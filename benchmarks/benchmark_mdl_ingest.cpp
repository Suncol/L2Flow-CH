#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace {

using namespace l2flow::ingest;

enum class ArrivalPattern : std::uint8_t {
    kOrdered,
    kLocalReverse,
};

struct Options final {
    std::uint64_t target_rate = 800'000U;
    std::uint64_t measurement_seconds = 5U;
    std::uint64_t warmup_seconds = 1U;
    std::size_t channels = 16U;
    std::size_t tick_lanes = 12U;
    std::size_t instrument_owners = 16U;
    std::uint64_t latency_sample_every = 1U;
    std::uint64_t gap_wait_ns = 500'000U;
    ArrivalPattern pattern = ArrivalPattern::kOrdered;
    std::size_t reorder_window = 1U;
    int producer_cpu = 0;
    int first_consumer_cpu = 1;
    int first_decoder_cpu = 17;
};

struct alignas(64) ConsumerState final {
    std::atomic<std::uint64_t> consumed{0U};
    std::uint64_t measured = 0U;
    std::uint64_t latency_samples = 0U;
    std::uint64_t maximum_latency_ns = 0U;
    std::uint64_t ordering_errors = 0U;
    std::uint64_t clock_errors = 0U;
    std::uint64_t finish_ns = 0U;
    int observed_cpu = -1;
    std::vector<std::uint64_t> latencies_ns;
    std::string error;
};

struct NumaPageCounts final {
    std::uint64_t anonymous_node0 = 0U;
    std::uint64_t anonymous_node1 = 0U;
    std::uint64_t anonymous_other = 0U;
    bool available = false;
};

struct SyntheticArrival final {
    std::size_t channel_index = 0U;
    std::uint64_t native_sequence = 0U;
};

class SyntheticMdlMessage final : public datayes::mdl::MDLMessage {
public:
    void Set(std::span<const std::byte> header,
             std::span<std::byte> body) noexcept {
        if (header.size() == sizeof(head_)) {
            std::memcpy(&head_, header.data(), sizeof(head_));
        }
        body_ = body.data();
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    datayes::mdl::MDLMessageHead* GetHead() const override {
        return const_cast<datayes::mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return reinterpret_cast<char*>(body_);
    }

protected:
    datayes::mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    datayes::mdl::MDLMessageHead head_{};
    std::byte* body_ = nullptr;
};

template <typename Unsigned>
void PutUnsigned(std::span<std::byte> bytes,
                 std::size_t offset,
                 Unsigned value) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto shifted = static_cast<std::uint64_t>(value) >>
                             static_cast<unsigned int>(index * 8U);
        bytes[offset + index] = static_cast<std::byte>(
            shifted & UINT64_C(0xff));
    }
}

void PutI32(std::span<std::byte> bytes,
            std::size_t offset,
            std::int32_t value) noexcept {
    PutUnsigned(bytes, offset, std::bit_cast<std::uint32_t>(value));
}

void PutI64(std::span<std::byte> bytes,
            std::size_t offset,
            std::int64_t value) noexcept {
    PutUnsigned(bytes, offset, std::bit_cast<std::uint64_t>(value));
}

void AppendText(std::vector<std::byte>* body,
                std::size_t descriptor_offset,
                std::string_view value) {
    const std::size_t start = body->size();
    body->insert(body->end(),
                 reinterpret_cast<const std::byte*>(value.data()),
                 reinterpret_cast<const std::byte*>(value.data()) +
                     value.size());
    PutUnsigned<std::uint16_t>(
        *body, descriptor_offset,
        static_cast<std::uint16_t>(value.size()));
    PutUnsigned<std::uint32_t>(
        *body, descriptor_offset + 2U,
        static_cast<std::uint32_t>(start - descriptor_offset));
}

[[nodiscard]] std::vector<std::byte> MakeBody(
    std::uint32_t channel,
    std::string_view security_id) {
    std::vector<std::byte> body(70U);
    PutI32(body, 8U, static_cast<std::int32_t>(channel));
    PutUnsigned<std::uint32_t>(body, 18U, 93'000'123U);
    PutI64(body, 28U, 101);
    PutI64(body, 36U, 202);
    PutI32(body, 44U, 12'345);
    PutI64(body, 48U, 100);
    PutI64(body, 56U, 1'234'500);
    AppendText(&body, 12U, security_id);
    AppendText(&body, 22U, "T");
    AppendText(&body, 64U, "B");
    return body;
}

[[nodiscard]] std::array<std::byte, kMdlHeaderBytes> MakeHeader(
    std::size_t body_size) noexcept {
    std::array<std::byte, kMdlHeaderBytes> header{};
    PutUnsigned<std::uint8_t>(header, 0U, 23U);
    PutUnsigned<std::uint32_t>(
        header, 1U, static_cast<std::uint32_t>(23U + body_size));
    PutUnsigned<std::uint8_t>(header, 5U, 1U);
    PutUnsigned<std::uint8_t>(header, 6U, 4U);
    PutUnsigned<std::uint16_t>(header, 7U, 101U);
    PutUnsigned<std::uint16_t>(header, 9U, 24U);
    PutUnsigned<std::uint32_t>(header, 11U, 93'000'000U);
    return header;
}

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    Integer parsed{};
    const auto result = std::from_chars(
        text.data(), text.data() + text.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != text.data() + text.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage: benchmark_mdl_ingest [options]\n"
        << "  --rate N                 target callbacks/s (default 800000)\n"
        << "  --seconds N              measured seconds (default 5)\n"
        << "  --warmup-seconds N       warm-up seconds (default 1)\n"
        << "  --pattern ordered|local-reverse\n"
        << "  --reorder-window N       per-Channel reverse window\n"
        << "  --channels N             default 16\n"
        << "  --tick-lanes N           default 12\n"
        << "  --owners N               must equal channels; default 16\n"
        << "  --sample-every N         latency percentile sample stride\n"
        << "  --gap-wait-ns N          FROM_OPEN reorder wait; default 500000\n"
        << "  --producer-cpu N         default 0\n"
        << "  --first-consumer-cpu N   default 1\n"
        << "  --first-decoder-cpu N    default 17\n";
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    Options parsed{};
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const auto next = [&](std::string_view name) -> std::string_view {
            if (index + 1 >= argc) {
                *error = std::string(name) + " requires a value";
                return {};
            }
            ++index;
            return argv[index];
        };
        if (argument == "--help") {
            PrintUsage();
            return false;
        }
        if (argument == "--rate") {
            if (!ParseInteger(next(argument), &parsed.target_rate)) {
                *error = "invalid --rate";
                return false;
            }
        } else if (argument == "--seconds") {
            if (!ParseInteger(next(argument),
                              &parsed.measurement_seconds)) {
                *error = "invalid --seconds";
                return false;
            }
        } else if (argument == "--warmup-seconds") {
            if (!ParseInteger(next(argument), &parsed.warmup_seconds)) {
                *error = "invalid --warmup-seconds";
                return false;
            }
        } else if (argument == "--channels") {
            if (!ParseInteger(next(argument), &parsed.channels)) {
                *error = "invalid --channels";
                return false;
            }
        } else if (argument == "--tick-lanes") {
            if (!ParseInteger(next(argument), &parsed.tick_lanes)) {
                *error = "invalid --tick-lanes";
                return false;
            }
        } else if (argument == "--owners") {
            if (!ParseInteger(next(argument),
                              &parsed.instrument_owners)) {
                *error = "invalid --owners";
                return false;
            }
        } else if (argument == "--sample-every") {
            if (!ParseInteger(next(argument),
                              &parsed.latency_sample_every)) {
                *error = "invalid --sample-every";
                return false;
            }
        } else if (argument == "--gap-wait-ns") {
            if (!ParseInteger(next(argument), &parsed.gap_wait_ns)) {
                *error = "invalid --gap-wait-ns";
                return false;
            }
        } else if (argument == "--reorder-window") {
            if (!ParseInteger(next(argument),
                              &parsed.reorder_window)) {
                *error = "invalid --reorder-window";
                return false;
            }
        } else if (argument == "--producer-cpu") {
            if (!ParseInteger(next(argument), &parsed.producer_cpu)) {
                *error = "invalid --producer-cpu";
                return false;
            }
        } else if (argument == "--first-consumer-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.first_consumer_cpu)) {
                *error = "invalid --first-consumer-cpu";
                return false;
            }
        } else if (argument == "--first-decoder-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.first_decoder_cpu)) {
                *error = "invalid --first-decoder-cpu";
                return false;
            }
        } else if (argument == "--pattern") {
            const std::string_view value = next(argument);
            if (value == "ordered") {
                parsed.pattern = ArrivalPattern::kOrdered;
                parsed.reorder_window = 1U;
            } else if (value == "local-reverse") {
                parsed.pattern = ArrivalPattern::kLocalReverse;
            } else {
                *error = "--pattern must be ordered or local-reverse";
                return false;
            }
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
        if (!error->empty()) {
            return false;
        }
    }

    if (parsed.target_rate == 0U || parsed.measurement_seconds == 0U ||
        parsed.measurement_seconds > 300U ||
        parsed.warmup_seconds > 60U || parsed.channels == 0U ||
        parsed.channels > 100'000U || parsed.tick_lanes == 0U ||
        parsed.instrument_owners != parsed.channels ||
        parsed.reorder_window == 0U ||
        parsed.latency_sample_every == 0U ||
        (parsed.pattern == ArrivalPattern::kLocalReverse &&
         parsed.reorder_window < 2U)) {
        *error = "invalid duration, topology, or reorder configuration";
        return false;
    }
    if (parsed.pattern == ArrivalPattern::kOrdered) {
        parsed.reorder_window = 1U;
    }
    if (parsed.pattern == ArrivalPattern::kLocalReverse &&
        parsed.latency_sample_every > 1U &&
        std::gcd(parsed.latency_sample_every,
                 static_cast<std::uint64_t>(parsed.reorder_window)) != 1U) {
        *error = "--sample-every must be coprime with --reorder-window "
                 "to avoid percentile sampling bias";
        return false;
    }
    if (parsed.channels >
            std::numeric_limits<std::uint64_t>::max() /
                parsed.reorder_window ||
        parsed.target_rate >
            std::numeric_limits<std::uint64_t>::max() /
                (parsed.measurement_seconds + parsed.warmup_seconds)) {
        *error = "message-count calculation overflows";
        return false;
    }
#if defined(__linux__)
    const std::size_t decoder_threads = parsed.tick_lanes + 1U;
    if (parsed.producer_cpu < 0 || parsed.first_consumer_cpu < 0 ||
        parsed.first_decoder_cpu < 0 ||
        static_cast<std::size_t>(parsed.first_consumer_cpu) +
                parsed.instrument_owners >
            static_cast<std::size_t>(CPU_SETSIZE) ||
        static_cast<std::size_t>(parsed.first_decoder_cpu) +
                decoder_threads >
            static_cast<std::size_t>(CPU_SETSIZE)) {
        *error = "CPU affinity range is invalid";
        return false;
    }
    std::vector<bool> used(static_cast<std::size_t>(CPU_SETSIZE), false);
    const auto claim = [&used](int cpu) {
        const std::size_t index = static_cast<std::size_t>(cpu);
        if (index >= used.size() || used[index]) {
            return false;
        }
        used[index] = true;
        return true;
    };
    if (!claim(parsed.producer_cpu)) {
        *error = "CPU affinity assignments overlap";
        return false;
    }
    for (std::size_t owner = 0U; owner < parsed.instrument_owners;
         ++owner) {
        if (!claim(parsed.first_consumer_cpu +
                   static_cast<int>(owner))) {
            *error = "CPU affinity assignments overlap";
            return false;
        }
    }
    for (std::size_t lane = 0U; lane < decoder_threads; ++lane) {
        if (!claim(parsed.first_decoder_cpu + static_cast<int>(lane))) {
            *error = "CPU affinity assignments overlap";
            return false;
        }
    }
#else
    *error = "this NUMA affinity benchmark requires Linux";
    return false;
#endif
    *output = parsed;
    error->clear();
    return true;
}

inline void CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

[[nodiscard]] bool PinCurrentThread(int cpu,
                                    std::string* error) noexcept {
#if defined(__linux__)
    if (cpu < 0 || cpu >= CPU_SETSIZE) {
        if (error != nullptr) {
            *error = "CPU index is outside CPU_SETSIZE";
        }
        return false;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    const int result = ::pthread_setaffinity_np(
        ::pthread_self(), sizeof(set), &set);
    if (result != 0) {
        if (error != nullptr) {
            try {
                *error = "pthread_setaffinity_np failed: " +
                         std::string(std::strerror(result));
            } catch (...) {
            }
        }
        return false;
    }
    return true;
#else
    static_cast<void>(cpu);
    if (error != nullptr) {
        *error = "CPU affinity is unavailable";
    }
    return false;
#endif
}

[[nodiscard]] int CurrentCpu() noexcept {
#if defined(__linux__)
    return ::sched_getcpu();
#else
    return -1;
#endif
}

[[nodiscard]] std::uint64_t RoundUp(
    std::uint64_t value,
    std::uint64_t quantum) noexcept {
    const std::uint64_t remainder = value % quantum;
    return remainder == 0U ? value : value + quantum - remainder;
}

[[nodiscard]] std::uint64_t ScheduledOffsetNs(
    std::uint64_t ordinal,
    std::uint64_t rate) noexcept {
    const std::uint64_t whole_seconds = ordinal / rate;
    const std::uint64_t remainder = ordinal % rate;
    return whole_seconds * UINT64_C(1'000'000'000) +
           remainder * UINT64_C(1'000'000'000) / rate;
}

[[nodiscard]] std::uint64_t WaitUntil(
    std::uint64_t deadline_ns) noexcept {
    for (;;) {
        const std::uint64_t now = MonotonicNowNs();
        if (now >= deadline_ns) {
            return now;
        }
        CpuRelax();
    }
}

[[nodiscard]] SyntheticArrival MakeArrival(
    std::uint64_t ordinal,
    const Options& options) noexcept {
    if (options.pattern == ArrivalPattern::kOrdered) {
        const std::uint64_t channels =
            static_cast<std::uint64_t>(options.channels);
        return {
            static_cast<std::size_t>(ordinal % channels),
            ordinal / channels + 1U};
    }
    const std::uint64_t window =
        static_cast<std::uint64_t>(options.reorder_window);
    const std::uint64_t channels =
        static_cast<std::uint64_t>(options.channels);
    const std::uint64_t global_block = ordinal / window;
    const std::uint64_t offset = ordinal % window;
    const std::uint64_t channel_block = global_block / channels;
    return {
        static_cast<std::size_t>(global_block % channels),
        channel_block * window + (window - offset)};
}

[[nodiscard]] std::uint64_t Percentile(
    const std::vector<std::uint64_t>& sorted,
    long double quantile) noexcept {
    if (sorted.empty()) {
        return 0U;
    }
    const long double raw = quantile *
        static_cast<long double>(sorted.size() - 1U);
    const std::size_t index = static_cast<std::size_t>(raw);
    return sorted[index];
}

[[nodiscard]] std::string PatternName(ArrivalPattern pattern) {
    return pattern == ArrivalPattern::kOrdered ? "ordered"
                                                : "local-reverse";
}

[[nodiscard]] NumaPageCounts ReadNumaPageCounts() {
    NumaPageCounts result{};
#if defined(__linux__)
    std::ifstream stream("/proc/self/numa_maps");
    std::string line;
    while (std::getline(stream, line)) {
        if (line.find("anon=") == std::string::npos) {
            continue;
        }
        std::size_t position = 0U;
        while (position < line.size()) {
            while (position < line.size() && line[position] == ' ') {
                ++position;
            }
            const std::size_t end = line.find(' ', position);
            const std::size_t token_end =
                end == std::string::npos ? line.size() : end;
            const std::string_view token(
                line.data() + position, token_end - position);
            if (token.size() >= 4U && token.front() == 'N') {
                const std::size_t equals = token.find('=');
                std::uint32_t node = 0U;
                std::uint64_t pages = 0U;
                if (equals != std::string_view::npos && equals > 1U &&
                    ParseInteger(token.substr(1U, equals - 1U), &node) &&
                    ParseInteger(token.substr(equals + 1U), &pages)) {
                    if (node == 0U) {
                        result.anonymous_node0 += pages;
                    } else if (node == 1U) {
                        result.anonymous_node1 += pages;
                    } else {
                        result.anonymous_other += pages;
                    }
                }
            }
            if (end == std::string::npos) {
                break;
            }
            position = end + 1U;
        }
    }
    result.available = stream.eof();
#endif
    return result;
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        if (!error.empty()) {
            std::cerr << "configuration error: " << error << '\n';
            PrintUsage();
            return 2;
        }
        return 0;
    }
    if (!PinCurrentThread(options.producer_cpu, &error)) {
        std::cerr << error << '\n';
        return 2;
    }

    const std::uint64_t quantum =
        static_cast<std::uint64_t>(options.channels) *
        static_cast<std::uint64_t>(options.reorder_window);
    const std::uint64_t warmup_count = RoundUp(
        options.target_rate * options.warmup_seconds, quantum);
    const std::uint64_t measured_count = RoundUp(
        options.target_rate * options.measurement_seconds, quantum);
    if (warmup_count >
        std::numeric_limits<std::uint64_t>::max() - measured_count) {
        std::cerr << "total message count overflows\n";
        return 2;
    }
    const std::uint64_t total_count = warmup_count + measured_count;
    const std::uint64_t warmup_per_channel =
        warmup_count / static_cast<std::uint64_t>(options.channels);
    const std::uint64_t total_per_channel =
        total_count / static_cast<std::uint64_t>(options.channels);
    const std::uint64_t measured_per_channel =
        measured_count / static_cast<std::uint64_t>(options.channels);
    const std::uint64_t latency_samples_per_channel =
        (measured_per_channel - 1U) / options.latency_sample_every + 1U;
    const std::uint64_t expected_latency_samples =
        latency_samples_per_channel *
        static_cast<std::uint64_t>(options.channels);
    if (measured_count >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max() /
            sizeof(std::uint64_t))) {
        std::cerr << "latency sample storage is too large\n";
        return 2;
    }

    std::vector<InstrumentDefinition> definitions;
    definitions.reserve(options.channels);
    std::vector<std::vector<std::byte>> bodies;
    bodies.reserve(options.channels);
    for (std::size_t index = 0U; index < options.channels; ++index) {
        const std::uint32_t numeric_security =
            600'000U + static_cast<std::uint32_t>(index);
        const std::string security_id = std::to_string(numeric_security);
        if (security_id.size() != 6U) {
            std::cerr << "synthetic security identity exceeded six digits\n";
            return 2;
        }
        definitions.push_back(
            {static_cast<std::uint32_t>(index + 1U),
             Market::kShanghai, {}, security_id});
        bodies.push_back(MakeBody(
            static_cast<std::uint32_t>(index + 1U), security_id));
    }

    InstrumentCatalog catalog;
    if (!InstrumentCatalog::Build(
            std::move(definitions), &catalog, &error)) {
        std::cerr << error << '\n';
        return 1;
    }
    EngineConfig config{};
    config.trade_date = 20260806U;
    config.start_mode = StartMode::kFromOpen;
    config.tick_decoder_lanes = options.tick_lanes;
    config.snapshot_decoder_lanes = 1U;
    config.instrument_workers = options.instrument_owners;
    config.tick_slots_per_lane = 16'384U;
    config.snapshot_slots_per_lane = 8U;
    config.maximum_tick_body_bytes = 256U;
    config.maximum_snapshot_body_bytes = 1'024U;
    config.dispatch_queue_capacity = 4'096U;
    config.diagnostic_queue_capacity = 256U;
    config.late_recovery_queue_capacity = 256U;
    config.maximum_channels_per_tick_lane =
        std::max<std::size_t>(
            8U, (options.channels + options.tick_lanes - 1U) /
                    options.tick_lanes + 4U);
    config.reorder_entries_per_channel = 256U;
    config.maximum_reorder_span = 255U;
    config.from_open_gap_wait_ns = options.gap_wait_ns;
    config.first_decoder_cpu = options.first_decoder_cpu;

    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, std::move(catalog), &error);
    if (engine == nullptr || !engine->Start(&error)) {
        std::cerr << error << '\n';
        return 1;
    }

    auto consumers = std::make_unique<ConsumerState[]>(
        options.instrument_owners);
    for (std::size_t owner = 0U; owner < options.instrument_owners;
         ++owner) {
        consumers[owner].latencies_ns.resize(
            static_cast<std::size_t>(latency_samples_per_channel));
    }
    std::atomic<bool> abort{false};
    std::atomic<std::size_t> ready{0U};
    std::vector<std::thread> consumer_threads;
    consumer_threads.reserve(options.instrument_owners);
    for (std::size_t owner = 0U; owner < options.instrument_owners;
         ++owner) {
        consumer_threads.emplace_back([&, owner] {
            ConsumerState& state = consumers[owner];
            if (!PinCurrentThread(
                    options.first_consumer_cpu +
                        static_cast<int>(owner),
                    &state.error)) {
                abort.store(true, std::memory_order_release);
                ready.fetch_add(1U, std::memory_order_release);
                return;
            }
            state.observed_cpu = CurrentCpu();
            ready.fetch_add(1U, std::memory_order_release);
            CanonicalTick tick{};
            std::uint64_t local_consumed = 0U;
            std::uint64_t expected_sequence = 1U;
            std::uint64_t measured_index = 0U;
            std::uint64_t latency_sample_index = 0U;
            std::size_t idle_spins = 0U;
            while (local_consumed < total_per_channel &&
                   !abort.load(std::memory_order_acquire)) {
                if (!engine->TryPollTick(owner, &tick)) {
                    ++idle_spins;
                    CpuRelax();
                    continue;
                }
                idle_spins = 0U;
                if (tick.common.instrument_ordinal != owner ||
                    tick.common.native_sequence != expected_sequence) {
                    ++state.ordering_errors;
                }
                if (tick.common.native_sequence > warmup_per_channel) {
                    const std::uint64_t now = MonotonicNowNs();
                    if (now < tick.common.receive_monotonic_ns ||
                        measured_index >= measured_per_channel) {
                        ++state.clock_errors;
                    } else {
                        const std::uint64_t latency =
                            now - tick.common.receive_monotonic_ns;
                        state.maximum_latency_ns = std::max(
                            state.maximum_latency_ns, latency);
                        if (measured_index %
                                options.latency_sample_every ==
                            0U) {
                            if (latency_sample_index >=
                                latency_samples_per_channel) {
                                ++state.clock_errors;
                            } else {
                                state.latencies_ns[static_cast<std::size_t>(
                                    latency_sample_index)] = latency;
                            }
                            ++latency_sample_index;
                        }
                    }
                    ++measured_index;
                }
                ++expected_sequence;
                ++local_consumed;
                if ((local_consumed & UINT64_C(255)) == 0U) {
                    state.consumed.store(
                        local_consumed, std::memory_order_release);
                }
            }
            state.measured = measured_index;
            state.latency_samples = latency_sample_index;
            state.finish_ns = MonotonicNowNs();
            state.consumed.store(
                local_consumed, std::memory_order_release);
        });
    }
    while (ready.load(std::memory_order_acquire) <
           options.instrument_owners) {
        std::this_thread::yield();
    }
    if (abort.load(std::memory_order_acquire)) {
        engine->Stop();
        for (std::thread& thread : consumer_threads) {
            thread.join();
        }
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            if (!consumers[owner].error.empty()) {
                std::cerr << consumers[owner].error << '\n';
            }
        }
        return 1;
    }

    std::array<std::byte, kMdlHeaderBytes> header =
        MakeHeader(bodies.front().size());
    MdlMessageHandler handler(engine.get());
    SyntheticMdlMessage synthetic_message;
    const std::uint64_t schedule_origin_ns =
        MonotonicNowNs() + UINT64_C(100'000'000);
    std::uint64_t first_measured_callback_ns = 0U;
    std::uint64_t producer_finish_ns = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    AdmissionResult admission_failure = AdmissionResult::kAccepted;
    std::uint64_t admitted_count = 0U;
    for (std::uint64_t ordinal = 0U; ordinal < total_count; ++ordinal) {
        const SyntheticArrival arrival = MakeArrival(ordinal, options);
        const std::size_t channel_index = arrival.channel_index;
        const std::uint64_t native_sequence = arrival.native_sequence;
        if (native_sequence > static_cast<std::uint64_t>(
                                  std::numeric_limits<std::int64_t>::max())) {
            admission_failure = AdmissionResult::kInternalFailure;
            break;
        }
        std::vector<std::byte>& body = bodies[channel_index];
        PutI64(body, 0U, static_cast<std::int64_t>(native_sequence));
        PutUnsigned<std::uint64_t>(header, 15U, ordinal + 1U);
        synthetic_message.Set(header, body);
        const std::uint64_t schedule_ordinal =
            options.pattern == ArrivalPattern::kLocalReverse
                ? ordinal - ordinal % static_cast<std::uint64_t>(
                                           options.reorder_window)
                : ordinal;
        const std::uint64_t deadline = schedule_origin_ns +
            ScheduledOffsetNs(schedule_ordinal, options.target_rate);
        const std::uint64_t callback_start_ns = WaitUntil(deadline);
        if (ordinal >= warmup_count) {
            maximum_schedule_lag_ns = std::max(
                maximum_schedule_lag_ns, callback_start_ns - deadline);
            if (ordinal == warmup_count) {
                first_measured_callback_ns = callback_start_ns;
            }
        }
        handler.OnMessage(nullptr, &synthetic_message);
        const AdmissionResult result = handler.last_result();
        if (result != AdmissionResult::kAccepted) {
            admission_failure = result;
            break;
        }
        ++admitted_count;
    }
    producer_finish_ns = MonotonicNowNs();

    bool completion_failed = false;
    if (admission_failure != AdmissionResult::kAccepted ||
        admitted_count != total_count) {
        abort.store(true, std::memory_order_release);
        engine->Stop();
        completion_failed = true;
    } else {
        const std::uint64_t completion_deadline =
            MonotonicNowNs() + UINT64_C(5'000'000'000);
        for (;;) {
            std::uint64_t progress = 0U;
            for (std::size_t owner = 0U;
                 owner < options.instrument_owners; ++owner) {
                progress += consumers[owner].consumed.load(
                    std::memory_order_acquire);
            }
            if (progress == total_count) {
                break;
            }
            const EngineStats progress_stats = engine->stats();
            if (!engine->healthy() || progress_stats.gaps_skipped != 0U ||
                progress_stats.late_recovery_dispatched != 0U ||
                progress_stats.from_open_channels_frozen != 0U ||
                MonotonicNowNs() >= completion_deadline) {
                completion_failed = true;
                abort.store(true, std::memory_order_release);
                engine->Stop();
                break;
            }
            std::this_thread::yield();
        }
    }
    for (std::thread& thread : consumer_threads) {
        thread.join();
    }
    if (!completion_failed) {
        engine->Stop();
    }

    std::uint64_t total_consumed = 0U;
    std::uint64_t total_measured = 0U;
    std::uint64_t ordering_errors = 0U;
    std::uint64_t clock_errors = 0U;
    std::uint64_t maximum_latency_ns = 0U;
    std::uint64_t dispatch_finish_ns = 0U;
    bool cpu_binding_valid = true;
    std::vector<std::uint64_t> latencies;
    latencies.reserve(
        static_cast<std::size_t>(expected_latency_samples));
    for (std::size_t owner = 0U; owner < options.instrument_owners;
         ++owner) {
        const ConsumerState& state = consumers[owner];
        total_consumed += state.consumed.load(std::memory_order_acquire);
        total_measured += state.measured;
        maximum_latency_ns = std::max(
            maximum_latency_ns, state.maximum_latency_ns);
        ordering_errors += state.ordering_errors;
        clock_errors += state.clock_errors;
        dispatch_finish_ns = std::max(
            dispatch_finish_ns, state.finish_ns);
        cpu_binding_valid = cpu_binding_valid &&
            state.observed_cpu ==
                options.first_consumer_cpu + static_cast<int>(owner);
        latencies.insert(latencies.end(), state.latencies_ns.begin(),
                         state.latencies_ns.begin() +
                             static_cast<std::ptrdiff_t>(
                                 std::min(state.latency_samples,
                                          latency_samples_per_channel)));
    }
    std::sort(latencies.begin(), latencies.end());

    std::uint64_t gap_controls = 0U;
    std::uint64_t late_controls = 0U;
    std::uint64_t fault_controls = 0U;
    ChannelGap gap{};
    LateRecoveryTick late{};
    ChannelFault fault{};
    while (engine->TryPollGap(&gap)) {
        ++gap_controls;
    }
    while (engine->TryPollLateRecovery(&late)) {
        ++late_controls;
    }
    while (engine->TryPollChannelFault(&fault)) {
        ++fault_controls;
    }
    const EngineStats stats = engine->stats();
    const NumaPageCounts numa_pages = ReadNumaPageCounts();
    const double producer_seconds =
        first_measured_callback_ns == 0U ||
                producer_finish_ns <= first_measured_callback_ns
            ? 0.0
            : static_cast<double>(producer_finish_ns -
                                  first_measured_callback_ns) /
                  1'000'000'000.0;
    const double dispatch_seconds =
        first_measured_callback_ns == 0U ||
                dispatch_finish_ns <= first_measured_callback_ns
            ? 0.0
            : static_cast<double>(dispatch_finish_ns -
                                  first_measured_callback_ns) /
                  1'000'000'000.0;
    const std::uint64_t admitted_measured =
        admitted_count > warmup_count ? admitted_count - warmup_count : 0U;
    const double producer_rate = producer_seconds == 0.0
        ? 0.0
        : static_cast<double>(admitted_measured) / producer_seconds;
    const double dispatch_rate = dispatch_seconds == 0.0
        ? 0.0
        : static_cast<double>(total_measured) / dispatch_seconds;
    const double minimum_pass_rate =
        static_cast<double>(options.target_rate) * 0.99;
    const bool valid =
        admission_failure == AdmissionResult::kAccepted &&
        admitted_count == total_count && total_consumed == total_count &&
        total_measured == measured_count &&
        latencies.size() ==
            static_cast<std::size_t>(expected_latency_samples) &&
        ordering_errors == 0U && clock_errors == 0U &&
        stats.lane_full == 0U && stats.decode_errors == 0U &&
        stats.dispatch_overflows == 0U && stats.gaps_skipped == 0U &&
        stats.late_recovery_dispatched == 0U &&
        stats.from_open_channels_frozen == 0U && gap_controls == 0U &&
        late_controls == 0U && fault_controls == 0U &&
        engine->healthy() && !handler.failed() && cpu_binding_valid &&
        producer_rate >= minimum_pass_rate &&
        dispatch_rate >= minimum_pass_rate;

    const auto to_us = [](std::uint64_t nanoseconds) {
        return static_cast<double>(nanoseconds) / 1'000.0;
    };
    std::cout << std::fixed << std::setprecision(3)
              << "config target_msg_s=" << options.target_rate
              << " measured_messages=" << measured_count
              << " warmup_messages=" << warmup_count
              << " channels=" << options.channels
              << " tick_lanes=" << options.tick_lanes
              << " owners=" << options.instrument_owners
              << " pattern=" << PatternName(options.pattern)
              << " reorder_window=" << options.reorder_window
              << " latency_sample_every="
              << options.latency_sample_every
              << " gap_wait_ns=" << options.gap_wait_ns << '\n'
              << "affinity producer_cpu=" << options.producer_cpu
              << " consumer_cpus=" << options.first_consumer_cpu << '-'
              << options.first_consumer_cpu +
                     static_cast<int>(options.instrument_owners) - 1
              << " decoder_cpus=" << options.first_decoder_cpu << '-'
              << options.first_decoder_cpu +
                     static_cast<int>(options.tick_lanes)
              << " observed_bindings_valid="
              << (cpu_binding_valid ? "true" : "false") << '\n'
              << "numa_anon_pages node0="
              << numa_pages.anonymous_node0 << " node1="
              << numa_pages.anonymous_node1 << " other="
              << numa_pages.anonymous_other << " available="
              << (numa_pages.available ? "true" : "false") << '\n'
              << "throughput producer_msg_s=" << producer_rate
              << " dispatch_msg_s=" << dispatch_rate
              << " producer_seconds=" << producer_seconds
              << " dispatch_seconds=" << dispatch_seconds
              << " max_schedule_lag_us="
              << to_us(maximum_schedule_lag_ns) << '\n'
              << "callback_entry_to_dispatch_us samples="
              << latencies.size()
              << " p50=" << to_us(Percentile(latencies, 0.50L))
              << " p90=" << to_us(Percentile(latencies, 0.90L))
              << " p99=" << to_us(Percentile(latencies, 0.99L))
              << " p999=" << to_us(Percentile(latencies, 0.999L))
              << " max_all=" << to_us(maximum_latency_ns) << '\n'
              << "quality admitted=" << stats.admitted
              << " consumed=" << total_consumed
              << " measured_dispatched=" << total_measured
              << " ordering_errors=" << ordering_errors
              << " lane_full=" << stats.lane_full
              << " decode_errors=" << stats.decode_errors
              << " dispatch_overflows=" << stats.dispatch_overflows
              << " gaps=" << stats.gaps_skipped
              << " late_recovery=" << stats.late_recovery_dispatched
              << " channel_faults=" << stats.from_open_channels_frozen
              << " healthy=" << (engine->healthy() ? "true" : "false")
              << '\n'
              << "status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (admission_failure != AdmissionResult::kAccepted) {
        std::cerr << "admission failed: "
                  << AdmissionResultName(admission_failure) << '\n';
    }
    if (!engine->healthy()) {
        std::cerr << "engine fatal: " << engine->fatal_error() << '\n';
    }
    return valid ? 0 : 1;
}
