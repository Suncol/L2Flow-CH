#include "l2flow/event/runtime.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/journal/fact_journal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace {

using l2flow::event::EventRevisionBatch;
using l2flow::event::EventRevisionSink;
using l2flow::event::EventRuntime;
using l2flow::event::EventRuntimeConfig;
using l2flow::event::EventRuntimeStats;
using l2flow::ingest::CanonicalKind;
using l2flow::ingest::CanonicalTick;
using l2flow::ingest::Market;
using l2flow::ingest::MessageKey;
using l2flow::ingest::MonotonicNowNs;
using l2flow::ingest::TickDispatch;
using l2flow::ingest::TickDispatchKind;
using l2flow::ingest::TickAction;
using l2flow::ingest::TradingPhase;
using l2flow::journal::CanonicalFactJournal;
using l2flow::journal::FactJournalConfig;
using l2flow::journal::FactJournalStats;

struct Options final {
    std::uint64_t target_rate = 800'000U;
    std::uint32_t seconds = 1U;
    std::size_t actors = 16U;
    std::size_t micro_batch_rows = 512U;
    std::uint64_t latency_sample_every = 100U;
    std::size_t pacing_burst = 64U;
    std::filesystem::path journal_directory;
    bool unpaced = false;
};

struct ProcessMemory final {
    std::uint64_t current_rss_kib = 0U;
    std::uint64_t peak_rss_kib = 0U;
    bool available = false;
};

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (text.empty() || output == nullptr) {
        return false;
    }
    Integer value{};
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage: benchmark_event_state [options]\n"
        << "  --rate N              scheduled facts/s; default 800000\n"
        << "  --seconds N           measured seconds; default 1, max 600\n"
        << "  --actors N            Event owner actors; default 16\n"
        << "  --micro-batch-rows N  journal-first cut; default 512\n"
        << "  --sample-every N       projection latency stride; default 100\n"
        << "  --pacing-burst N       owner drain burst; default 64\n"
        << "  --journal-dir DIR     journal directory; default system temp\n"
        << "  --unpaced             run the same count as fast as possible\n";
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
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
            if (!ParseInteger(next(argument), &parsed.seconds)) {
                *error = "invalid --seconds";
                return false;
            }
        } else if (argument == "--actors") {
            if (!ParseInteger(next(argument), &parsed.actors)) {
                *error = "invalid --actors";
                return false;
            }
        } else if (argument == "--micro-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.micro_batch_rows)) {
                *error = "invalid --micro-batch-rows";
                return false;
            }
        } else if (argument == "--sample-every") {
            if (!ParseInteger(next(argument),
                              &parsed.latency_sample_every)) {
                *error = "invalid --sample-every";
                return false;
            }
        } else if (argument == "--pacing-burst") {
            if (!ParseInteger(next(argument), &parsed.pacing_burst)) {
                *error = "invalid --pacing-burst";
                return false;
            }
        } else if (argument == "--journal-dir") {
            const std::string_view value = next(argument);
            if (value.empty()) {
                *error = "invalid --journal-dir";
                return false;
            }
            parsed.journal_directory = value;
        } else if (argument == "--unpaced") {
            parsed.unpaced = true;
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
        if (!error->empty()) {
            return false;
        }
    }
    if (parsed.target_rate == 0U || parsed.target_rate > 10'000'000U ||
        parsed.seconds == 0U || parsed.seconds > 600U ||
        parsed.actors == 0U || parsed.actors > 100'000U ||
        parsed.actors >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        parsed.micro_batch_rows == 0U ||
        parsed.micro_batch_rows > 1'048'576U ||
        parsed.latency_sample_every == 0U ||
        parsed.pacing_burst == 0U || parsed.pacing_burst > 256U ||
        parsed.target_rate >
            std::numeric_limits<std::uint64_t>::max() / parsed.seconds) {
        *error = "invalid rate, duration, actor, or micro-batch bound";
        return false;
    }
    *output = parsed;
    error->clear();
    return true;
}

[[nodiscard]] std::uint64_t Percentile(
    const std::vector<std::uint64_t>& sorted,
    long double fraction) noexcept {
    if (sorted.empty()) {
        return 0U;
    }
    const long double position = fraction *
        static_cast<long double>(sorted.size() - 1U);
    return sorted[static_cast<std::size_t>(position)];
}

class InMemoryRevisionSink final : public EventRevisionSink {
public:
    explicit InMemoryRevisionSink(std::size_t actors)
        : actors_(actors),
          last_sequence_(
              std::make_unique<std::atomic<std::uint64_t>[]>(actors)) {}

    [[nodiscard]] bool AppendRevisionGroup(
        std::vector<std::shared_ptr<const EventRevisionBatch>> group)
        noexcept override {
        if (group.empty() || !healthy_.load(std::memory_order_acquire)) {
            healthy_.store(false, std::memory_order_release);
            return false;
        }
        std::uint64_t rows = 0U;
        std::uint32_t owner = std::numeric_limits<std::uint32_t>::max();
        for (const auto& batch : group) {
            if (batch == nullptr || batch->revisions.empty() ||
                static_cast<std::size_t>(batch->owner) >= actors_ ||
                (owner != std::numeric_limits<std::uint32_t>::max() &&
                 batch->owner != owner)) {
                healthy_.store(false, std::memory_order_release);
                return false;
            }
            owner = batch->owner;
            std::atomic<std::uint64_t>& last = last_sequence_[owner];
            const std::uint64_t previous = last.load(
                std::memory_order_relaxed);
            if (batch->batch_sequence <= previous) {
                healthy_.store(false, std::memory_order_release);
                return false;
            }
            last.store(batch->batch_sequence, std::memory_order_release);
            rows += batch->revisions.size();
        }
        rows_.fetch_add(rows, std::memory_order_relaxed);
        batches_.fetch_add(group.size(), std::memory_order_relaxed);
        groups_.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t rows() const noexcept {
        return rows_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t batches() const noexcept {
        return batches_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t groups() const noexcept {
        return groups_.load(std::memory_order_relaxed);
    }

private:
    std::size_t actors_ = 0U;
    std::unique_ptr<std::atomic<std::uint64_t>[]> last_sequence_;
    std::atomic<std::uint64_t> rows_{0U};
    std::atomic<std::uint64_t> batches_{0U};
    std::atomic<std::uint64_t> groups_{0U};
    std::atomic<bool> healthy_{true};
};

[[nodiscard]] std::uint64_t ScheduledOffsetNs(
    std::uint64_t ordinal,
    std::uint64_t rate) noexcept {
    const std::uint64_t seconds = ordinal / rate;
    const std::uint64_t remainder = ordinal % rate;
    return seconds * UINT64_C(1'000'000'000) +
        remainder * UINT64_C(1'000'000'000) / rate;
}

[[nodiscard]] std::uint64_t WaitUntil(std::uint64_t deadline_ns) noexcept {
    for (;;) {
        const std::uint64_t now = MonotonicNowNs();
        if (now >= deadline_ns) {
            return now;
        }
        const std::uint64_t remaining = deadline_ns - now;
        if (remaining > 200'000U) {
            std::this_thread::sleep_for(
                std::chrono::nanoseconds(remaining - 100'000U));
        } else {
            std::this_thread::yield();
        }
    }
}

[[nodiscard]] CanonicalTick MakeStatusTick(
    std::size_t actor,
    std::uint64_t native_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t receive_ns) {
    CanonicalTick tick{};
    tick.common.trade_date = 20260807U;
    tick.common.instrument_id = static_cast<std::uint32_t>(actor + 1U);
    tick.common.instrument_ordinal = static_cast<std::uint32_t>(actor);
    tick.common.channel = static_cast<std::uint32_t>(actor + 1U);
    tick.common.native_sequence = native_sequence;
    tick.common.ingress_sequence = ingress_sequence;
    tick.common.vendor_sequence_id = ingress_sequence;
    tick.common.receive_monotonic_ns = receive_ns;
    tick.common.exchange_time_raw = 93'000'000U;
    tick.common.exchange_time_ns_from_midnight =
        UINT64_C(34'200'000'000'000) + native_sequence;
    tick.common.exchange_time_valid = true;
    tick.common.message_key = MessageKey{4U, 101U, 24U};
    tick.common.kind = CanonicalKind::kShanghaiTick;
    tick.common.identity.market = Market::kShanghai;
    const std::string security =
        std::to_string(600'000U + static_cast<std::uint32_t>(actor));
    tick.common.identity.security_id_size =
        static_cast<std::uint8_t>(security.size());
    std::copy(
        security.begin(), security.end(),
        reinterpret_cast<char*>(tick.common.identity.security_id.data()));
    tick.action = TickAction::kStatus;
    tick.phase = TradingPhase::kContinuous;
    tick.validity = l2flow::ingest::kTickPhaseValid |
        l2flow::ingest::kTickChannelHistoryValid;
    return tick;
}

[[nodiscard]] std::filesystem::path MakeUniqueJournalPath(
    const std::filesystem::path& directory,
    std::string* error) {
    static std::atomic<std::uint64_t> next_id{0U};
    std::string process = "portable";
#if defined(__linux__)
    process = std::to_string(static_cast<std::uint64_t>(::getpid()));
#endif
    for (std::size_t attempt = 0U; attempt < 1'024U; ++attempt) {
        const std::uint64_t id = next_id.fetch_add(
            1U, std::memory_order_relaxed);
        const std::filesystem::path candidate = directory /
            ("l2flow-event-state-" + process + '-' +
             std::to_string(MonotonicNowNs()) + '-' +
             std::to_string(id) + ".journal");
        std::error_code exists_error;
        const bool exists = std::filesystem::exists(
            candidate, exists_error);
        if (!exists_error && !exists) {
            error->clear();
            return candidate;
        }
        if (exists_error) {
            *error = "cannot inspect Event journal path: " +
                exists_error.message();
            return {};
        }
    }
    *error = "cannot allocate a unique Event journal path";
    return {};
}

class JournalFileCleanup final {
public:
    explicit JournalFileCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}

    ~JournalFileCleanup() {
        if (armed_) {
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove(path_, ignored));
        }
    }

    JournalFileCleanup(const JournalFileCleanup&) = delete;
    JournalFileCleanup& operator=(const JournalFileCleanup&) = delete;

    void Disarm() noexcept { armed_ = false; }

private:
    std::filesystem::path path_;
    bool armed_ = true;
};

[[nodiscard]] ProcessMemory ReadProcessMemory() {
    ProcessMemory result{};
#if defined(__linux__)
    std::ifstream status("/proc/self/status");
    std::string key;
    while (status >> key) {
        if (key == "VmRSS:" || key == "VmHWM:") {
            std::uint64_t value = 0U;
            std::string unit;
            if (!(status >> value >> unit) || unit != "kB") {
                return {};
            }
            if (key == "VmRSS:") {
                result.current_rss_kib = value;
            } else {
                result.peak_rss_kib = value;
            }
        } else {
            std::string remainder;
            std::getline(status, remainder);
        }
    }
    result.available = result.current_rss_kib != 0U &&
        result.peak_rss_kib != 0U;
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

    const std::uint64_t total =
        options.target_rate * static_cast<std::uint64_t>(options.seconds);
    if (total >
        (std::numeric_limits<std::uint64_t>::max() -
         l2flow::journal::kFactJournalFileHeaderBytes) /
            l2flow::journal::kFactJournalRecordBytes) {
        std::cerr << "journal byte size overflows uint64_t\n";
        return 2;
    }
    const std::uint64_t expected_record_bytes = total *
        l2flow::journal::kFactJournalRecordBytes;
    const std::uint64_t expected_file_bytes = expected_record_bytes +
        l2flow::journal::kFactJournalFileHeaderBytes;
    const std::uint64_t maximum_per_actor =
        (total + static_cast<std::uint64_t>(options.actors) - 1U) /
        static_cast<std::uint64_t>(options.actors);
    if (maximum_per_actor >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max() -
            options.micro_batch_rows - 1U)) {
        std::cerr << "per-actor capacity does not fit size_t\n";
        return 2;
    }

    std::error_code filesystem_error;
    std::filesystem::path journal_directory = options.journal_directory;
    if (journal_directory.empty()) {
        journal_directory = std::filesystem::temp_directory_path(
            filesystem_error);
    }
    if (filesystem_error || journal_directory.empty()) {
        std::cerr << "cannot resolve Event journal directory: "
                  << filesystem_error.message() << '\n';
        return 2;
    }
    std::filesystem::create_directories(
        journal_directory, filesystem_error);
    if (filesystem_error ||
        !std::filesystem::is_directory(
            journal_directory, filesystem_error) ||
        filesystem_error) {
        std::cerr << "cannot create Event journal directory: "
                  << filesystem_error.message() << '\n';
        return 2;
    }
    const std::filesystem::path journal_path = MakeUniqueJournalPath(
        journal_directory, &error);
    if (journal_path.empty()) {
        std::cerr << "Event journal path creation failed: " << error
                  << '\n';
        return 2;
    }
    JournalFileCleanup journal_cleanup(journal_path);
    FactJournalConfig journal_config{};
    journal_config.trade_date = 20260807U;
    journal_config.path = journal_path;
    journal_config.maximum_records = total;
    std::unique_ptr<CanonicalFactJournal> created_journal =
        CanonicalFactJournal::Create(std::move(journal_config), &error);
    if (created_journal == nullptr) {
        std::cerr << "FactJournal creation failed: " << error << '\n';
        return 1;
    }
    std::shared_ptr<CanonicalFactJournal> journal(
        std::move(created_journal));

    EventRuntimeConfig config{};
    config.feed_session_epoch = 1U;
    config.worker.trade_date = 20260807U;
    config.worker.feed_session_epoch = 1U;
    config.worker.owner_count = static_cast<std::uint32_t>(options.actors);
    config.worker.revision_epoch = 1U;
    config.worker.logic_version = 1U;
    config.worker.calculation_run_id.bytes[0U] = std::byte{1U};
    config.worker.fact_journal = journal;
    config.worker.maximum_carry_orders = 1U;
    config.worker.maximum_cached_events = static_cast<std::size_t>(
        maximum_per_actor + options.micro_batch_rows + 1U);
    config.worker.maximum_pending_commits = 1'024U;
    config.worker.maximum_acknowledged_raw_dependencies =
        std::max<std::size_t>(options.micro_batch_rows * 2U, 65'536U);
    config.micro_batch_rows = options.micro_batch_rows;
    config.micro_batch_max_delay_ns = UINT64_C(50'000'000);
    config.maximum_raw_ack_backlog_per_owner =
        config.worker.maximum_acknowledged_raw_dependencies;
    config.maximum_occurrence_join_entries_per_owner =
        config.worker.maximum_acknowledged_raw_dependencies;

    InMemoryRevisionSink sink(options.actors);
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(std::move(config), &sink, &error);
    if (runtime == nullptr) {
        std::cerr << "Event runtime creation failed: " << error << '\n';
        return 1;
    }

    std::atomic<std::size_t> ready{0U};
    std::atomic<bool> start{false};
    std::atomic<bool> abort{false};
    std::atomic<std::uint64_t> first_actual_ns{
        std::numeric_limits<std::uint64_t>::max()};
    std::atomic<std::uint64_t> maximum_schedule_lag_ns{0U};
    auto finish_ns = std::make_unique<std::atomic<std::uint64_t>[]>(
        options.actors);
    std::vector<std::vector<std::uint64_t>> actor_dispatch_latencies(
        options.actors);
    std::vector<std::vector<std::uint64_t>> actor_cut_latencies(
        options.actors);
    const std::uint64_t samples_per_actor =
        maximum_per_actor / options.latency_sample_every +
        (maximum_per_actor % options.latency_sample_every == 0U ? 0U : 1U);
    for (auto& samples : actor_dispatch_latencies) {
        samples.reserve(static_cast<std::size_t>(samples_per_actor));
    }
    const std::uint64_t cuts_per_actor =
        maximum_per_actor / options.micro_batch_rows + 1U;
    for (auto& samples : actor_cut_latencies) {
        samples.reserve(static_cast<std::size_t>(cuts_per_actor));
    }
    std::atomic<std::uint64_t> maximum_dispatch_latency_ns{0U};
    std::atomic<std::uint64_t> maximum_cut_latency_ns{0U};
    const std::uint64_t start_ns =
        MonotonicNowNs() + UINT64_C(100'000'000);
    std::vector<std::thread> threads;
    threads.reserve(options.actors);
    for (std::size_t actor = 0U; actor < options.actors; ++actor) {
        threads.emplace_back([&, actor] {
            std::vector<CanonicalTick> acknowledgements;
            acknowledgements.reserve(options.micro_batch_rows);
            std::uint64_t cut_oldest_origin_ns = 0U;
            std::size_t facts_in_cut = 0U;
            const auto publish_maximum = [](std::atomic<std::uint64_t>* target,
                                            std::uint64_t value) {
                std::uint64_t current = target->load(
                    std::memory_order_relaxed);
                while (value > current &&
                       !target->compare_exchange_weak(
                           current, value, std::memory_order_relaxed,
                           std::memory_order_relaxed)) {
                }
            };
            const auto service_until_pollable = [&]() {
                while (!runtime->CanPollDispatch(actor)) {
                    if (!runtime->FlushDue(actor, MonotonicNowNs())) {
                        return false;
                    }
                }
                return true;
            };
            const auto record_cut_latency = [&]() {
                const std::uint64_t completed_ns = MonotonicNowNs();
                const std::uint64_t latency =
                    completed_ns >= cut_oldest_origin_ns
                    ? completed_ns - cut_oldest_origin_ns
                    : 0U;
                actor_cut_latencies[actor].push_back(latency);
                publish_maximum(&maximum_cut_latency_ns, latency);
                cut_oldest_origin_ns = 0U;
                facts_in_cut = 0U;
            };
            ready.fetch_add(1U, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::uint64_t local_sequence = 0U;
            for (std::uint64_t ordinal = static_cast<std::uint64_t>(actor);
                 ordinal < total &&
                 !abort.load(std::memory_order_acquire);
                 ordinal += static_cast<std::uint64_t>(options.actors)) {
                const std::uint64_t deadline = start_ns +
                    ScheduledOffsetNs(ordinal, options.target_rate);
                if (!options.unpaced &&
                    local_sequence % options.pacing_burst == 0U) {
                    const std::uint64_t available_steps =
                        (total - 1U - ordinal) /
                        static_cast<std::uint64_t>(options.actors);
                    const std::uint64_t burst_steps = std::min(
                        available_steps,
                        static_cast<std::uint64_t>(
                            options.pacing_burst - 1U));
                    const std::uint64_t burst_last = ordinal + burst_steps *
                        static_cast<std::uint64_t>(options.actors);
                    static_cast<void>(WaitUntil(
                        start_ns + ScheduledOffsetNs(
                            burst_last, options.target_rate)));
                }
                const std::uint64_t now = MonotonicNowNs();
                std::uint64_t first = first_actual_ns.load(
                    std::memory_order_relaxed);
                while (now < first &&
                       !first_actual_ns.compare_exchange_weak(
                           first, now, std::memory_order_relaxed,
                           std::memory_order_relaxed)) {
                }
                const std::uint64_t lag = now >= deadline ? now - deadline
                                                           : 0U;
                std::uint64_t previous = maximum_schedule_lag_ns.load(
                    std::memory_order_relaxed);
                while (lag > previous &&
                       !maximum_schedule_lag_ns.compare_exchange_weak(
                           previous, lag, std::memory_order_relaxed,
                           std::memory_order_relaxed)) {
                }
                ++local_sequence;
                if (facts_in_cut == 0U) {
                    cut_oldest_origin_ns = options.unpaced ? now : deadline;
                }
                CanonicalTick tick = MakeStatusTick(
                    actor, local_sequence, ordinal + 1U,
                    options.unpaced ? now : deadline);
                TickDispatch dispatch{};
                dispatch.tick = tick;
                dispatch.feed_session_epoch = 1U;
                dispatch.expected_sequence = local_sequence;
                dispatch.admission_floor = 1U;
                dispatch.evict_before = local_sequence + 1U;
                dispatch.dispatch_fence = local_sequence;
                dispatch.channel = tick.common.channel;
                dispatch.owner = static_cast<std::uint32_t>(actor);
                dispatch.market = tick.common.identity.market;
                dispatch.kind = TickDispatchKind::kProjectOrdered;
                dispatch.catalog_match = true;
                if (!runtime->AppendDispatch(actor, dispatch)) {
                    abort.store(true, std::memory_order_release);
                    break;
                }
                ++facts_in_cut;
                if (ordinal % options.latency_sample_every == 0U) {
                    const std::uint64_t dispatched_ns = MonotonicNowNs();
                    const std::uint64_t latency_origin = options.unpaced
                        ? now
                        : deadline;
                    const std::uint64_t dispatch_latency =
                        dispatched_ns >= latency_origin
                        ? dispatched_ns - latency_origin
                        : 0U;
                    publish_maximum(
                        &maximum_dispatch_latency_ns, dispatch_latency);
                    actor_dispatch_latencies[actor].push_back(
                        dispatch_latency);
                }
                acknowledgements.push_back(std::move(tick));
                if (acknowledgements.size() ==
                    options.micro_batch_rows) {
                    if (!runtime->OnRawTickBatchAcknowledged(
                            acknowledgements)) {
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    acknowledgements.clear();
                }
                if (facts_in_cut == options.micro_batch_rows) {
                    if (!service_until_pollable()) {
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    record_cut_latency();
                } else if ((local_sequence & 255U) == 0U &&
                           !runtime->FlushDue(actor, MonotonicNowNs())) {
                    abort.store(true, std::memory_order_release);
                    break;
                }
            }
            if (!acknowledgements.empty() &&
                !runtime->OnRawTickBatchAcknowledged(acknowledgements)) {
                abort.store(true, std::memory_order_release);
            }
            if (!runtime->Flush(actor) || !service_until_pollable()) {
                abort.store(true, std::memory_order_release);
            } else if (facts_in_cut != 0U) {
                record_cut_latency();
            }
            finish_ns[actor].store(
                MonotonicNowNs(), std::memory_order_release);
        });
    }
    while (ready.load(std::memory_order_acquire) < options.actors) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) {
        thread.join();
    }

    std::uint64_t actors_completed_ns = 0U;
    for (std::size_t actor = 0U; actor < options.actors; ++actor) {
        actors_completed_ns = std::max(
            actors_completed_ns,
            finish_ns[actor].load(std::memory_order_acquire));
    }
    const std::uint64_t drain_start_ns = MonotonicNowNs();
    const bool drained = runtime->DrainAll();
    const std::uint64_t drain_finish_ns = MonotonicNowNs();
    const std::uint64_t journal_flush_start_ns = MonotonicNowNs();
    const bool journal_flushed = journal->Flush();
    const std::uint64_t durable_finish_ns = MonotonicNowNs();
    const EventRuntimeStats stats = runtime->stats();
    const FactJournalStats journal_stats = journal->stats();
    const ProcessMemory process_memory = ReadProcessMemory();
    std::vector<std::uint64_t> dispatch_latencies;
    std::vector<std::uint64_t> cut_latencies;
    for (const auto& actor : actor_dispatch_latencies) {
        dispatch_latencies.insert(
            dispatch_latencies.end(), actor.begin(), actor.end());
    }
    for (const auto& actor : actor_cut_latencies) {
        cut_latencies.insert(
            cut_latencies.end(), actor.begin(), actor.end());
    }
    std::sort(dispatch_latencies.begin(), dispatch_latencies.end());
    std::sort(cut_latencies.begin(), cut_latencies.end());
    std::error_code file_size_error;
    const std::uintmax_t observed_file_bytes =
        std::filesystem::file_size(journal_path, file_size_error);
    const std::uint64_t first_processing_ns = first_actual_ns.load(
        std::memory_order_acquire);
    const std::uint64_t measurement_origin_ns =
        options.unpaced &&
            first_processing_ns != std::numeric_limits<std::uint64_t>::max()
        ? first_processing_ns
        : start_ns;
    const double actors_elapsed_seconds =
        actors_completed_ns <= measurement_origin_ns
        ? 0.0
        : static_cast<double>(
              actors_completed_ns - measurement_origin_ns) /
              1'000'000'000.0;
    const double preflush_elapsed_seconds =
        drain_finish_ns <= measurement_origin_ns
        ? 0.0
        : static_cast<double>(drain_finish_ns - measurement_origin_ns) /
              1'000'000'000.0;
    const double durable_elapsed_seconds =
        durable_finish_ns <= measurement_origin_ns
        ? 0.0
        : static_cast<double>(durable_finish_ns - measurement_origin_ns) /
              1'000'000'000.0;
    const double preflush_rate = preflush_elapsed_seconds == 0.0
        ? 0.0
        : static_cast<double>(stats.workers.facts_journaled) /
              preflush_elapsed_seconds;
    const double steady_rate = actors_elapsed_seconds == 0.0
        ? 0.0
        : static_cast<double>(stats.workers.facts_journaled) /
              actors_elapsed_seconds;
    const double durable_rate = durable_elapsed_seconds == 0.0
        ? 0.0
        : static_cast<double>(stats.workers.facts_journaled) /
              durable_elapsed_seconds;
    const double drain_ms = drain_finish_ns < drain_start_ns
        ? 0.0
        : static_cast<double>(drain_finish_ns - drain_start_ns) /
              1'000'000.0;
    const double journal_flush_ms =
        durable_finish_ns < journal_flush_start_ns
        ? 0.0
        : static_cast<double>(
              durable_finish_ns - journal_flush_start_ns) /
              1'000'000.0;
    const double required_rate =
        static_cast<double>(options.target_rate) * 0.99;
    const double facts_per_micro_batch = stats.micro_batches_applied == 0U
        ? 0.0
        : static_cast<double>(stats.facts_in_micro_batches) /
              static_cast<double>(stats.micro_batches_applied);
    const double minimum_micro_batch_density =
        static_cast<double>(options.micro_batch_rows) * 0.80;
    const double revisions_per_persistence_group = sink.groups() == 0U
        ? 0.0
        : static_cast<double>(sink.rows()) /
              static_cast<double>(sink.groups());
    const bool runtime_healthy = runtime->healthy();
    const std::string runtime_fatal = runtime->fatal_error();
    const bool journal_healthy = journal->healthy();
    const std::string journal_fatal = journal->fatal_error();
    bool valid = !abort.load(std::memory_order_acquire) && drained &&
        journal_flushed && runtime_healthy && journal_healthy &&
        sink.healthy() &&
        stats.ordered_dispositions_received == total &&
        stats.hole_fill_dispositions_received == 0U &&
        stats.rejected_dispositions_received == 0U &&
        stats.occurrence_join_entries == 0U &&
        stats.raw_tick_acks_received == total &&
        stats.source_conflicts == 0U && stats.invalid_inputs == 0U &&
        stats.workers.facts_journaled == total &&
        stats.workers.duplicate_facts == 0U &&
        stats.workers.source_conflicts == 0U &&
        stats.workers.revisions_created == total &&
        stats.workers.pending_raw_commits == 0U &&
        stats.workers.acknowledged_raw_dependencies == 0U &&
        stats.workers.revision_batches_submitted == sink.batches() &&
        stats.workers.source_only_fast_path ==
            stats.micro_batches_applied &&
        stats.workers.unordered_batch_sorts == 0U &&
        sink.rows() == total && journal_stats.records == total &&
        journal_stats.record_bytes == expected_record_bytes &&
        journal_stats.file_bytes == expected_file_bytes &&
        journal_stats.write_bytes == expected_file_bytes &&
        journal_stats.partial_writes == 0U &&
        journal_stats.partial_reads == 0U &&
        journal_stats.consumer_new == total &&
        journal_stats.duplicates == 0U &&
        journal_stats.conflicts == 0U &&
        journal_stats.active_writes == 0U &&
        journal_stats.active_reads == 0U &&
        journal_stats.reserved_records == 0U &&
        journal_stats.waiting_admissions == 0U &&
        journal_stats.errors == 0U &&
        journal_stats.flush_calls >= 1U && !file_size_error &&
        observed_file_bytes == expected_file_bytes &&
        steady_rate >= required_rate &&
        facts_per_micro_batch >= minimum_micro_batch_density;

    runtime.reset();
    journal.reset();
    std::error_code cleanup_error;
    const bool journal_removed = std::filesystem::remove(
        journal_path, cleanup_error);
    const bool journal_cleanup_ok = journal_removed && !cleanup_error;
    if (journal_cleanup_ok) {
        journal_cleanup.Disarm();
    }
    valid = valid && journal_cleanup_ok;

    std::cout << std::fixed << std::setprecision(3)
              << "scope event_source_only_projection=true "
              << "shared_fact_journal=true clickhouse=false\n"
              << "event_state_config actors=" << options.actors
              << " target_msg_s=" << options.target_rate
              << " seconds=" << options.seconds
              << " micro_batch_rows=" << options.micro_batch_rows
              << " latency_sample_every=" << options.latency_sample_every
              << " pacing_burst=" << options.pacing_burst
              << " pacing=" << (options.unpaced ? "unpaced" : "scheduled")
              << " fact_payload_storage=disk_journal"
              << " journal_recovery=false"
              << " journal_path=" << journal_path.string()
              << " journal_cleanup="
              << (journal_cleanup_ok ? "removed" : "failed") << '\n'
              << "event_state_result expected_facts=" << total
              << " ordered_dispositions_received="
              << stats.ordered_dispositions_received
              << " raw_tick_acks_received="
              << stats.raw_tick_acks_received
              << " facts_journaled=" << stats.workers.facts_journaled
              << " revisions_created=" << stats.workers.revisions_created
              << " micro_batches=" << stats.micro_batches_applied
              << " facts_in_micro_batches="
              << stats.facts_in_micro_batches
              << " facts_per_micro_batch=" << facts_per_micro_batch
              << " required_facts_per_micro_batch="
              << minimum_micro_batch_density
              << " micro_batch_rows_max=" << stats.micro_batch_rows_max
              << " row_limit_flushes=" << stats.row_limit_flushes
              << " timer_flushes=" << stats.timer_flushes
              << " forced_active_flushes="
              << stats.forced_active_flushes
              << " empty_control_flushes="
              << stats.empty_control_flushes
              << " revision_batches=" << sink.batches()
              << " persistence_groups=" << sink.groups()
              << " revisions_per_persistence_group="
              << revisions_per_persistence_group
              << " revision_rows_acked_in_memory=" << sink.rows()
              << " pending_raw_commits="
              << stats.workers.pending_raw_commits
              << " pending_raw_acks="
              << stats.workers.acknowledged_raw_dependencies << '\n'
              << "event_state_journal records=" << journal_stats.records
              << " consumer_new=" << journal_stats.consumer_new
              << " record_bytes=" << journal_stats.record_bytes
              << " file_bytes=" << journal_stats.file_bytes
              << " observed_file_bytes=" << observed_file_bytes
              << " write_calls=" << journal_stats.write_calls
              << " write_bytes=" << journal_stats.write_bytes
              << " partial_writes=" << journal_stats.partial_writes
              << " read_calls=" << journal_stats.read_calls
              << " read_bytes=" << journal_stats.read_bytes
              << " partial_reads=" << journal_stats.partial_reads
              << " hot_cache_hits=" << journal_stats.hot_cache_hits
              << " flush_calls=" << journal_stats.flush_calls
              << " directory_pages=" << journal_stats.directory_pages
              << " active_writes=" << journal_stats.active_writes
              << " active_reads=" << journal_stats.active_reads
              << " reserved_records=" << journal_stats.reserved_records
              << " waiting_admissions="
              << journal_stats.waiting_admissions
              << " conflicts=" << journal_stats.conflicts
              << " errors=" << journal_stats.errors << '\n'
              << "event_state_throughput actors_elapsed_s="
              << actors_elapsed_seconds
              << " preflush_elapsed_s=" << preflush_elapsed_seconds
              << " durable_elapsed_s=" << durable_elapsed_seconds
              << " steady_fact_s=" << steady_rate
              << " preflush_fact_s=" << preflush_rate
              << " durable_fact_s=" << durable_rate
              << " required_fact_s=" << required_rate
              << " drain_all_ms=" << drain_ms
              << " journal_flush_ms=" << journal_flush_ms
              << " maximum_schedule_lag_us="
              << static_cast<double>(maximum_schedule_lag_ns.load(
                     std::memory_order_relaxed)) /
                     1'000.0
              << " dispatch_latency_samples="
              << dispatch_latencies.size()
              << " dispatch_latency_p50_us="
              << static_cast<double>(Percentile(
                     dispatch_latencies, 0.50L)) / 1'000.0
              << " dispatch_latency_p90_us="
              << static_cast<double>(Percentile(
                     dispatch_latencies, 0.90L)) / 1'000.0
              << " dispatch_latency_p99_us="
              << static_cast<double>(Percentile(
                     dispatch_latencies, 0.99L)) / 1'000.0
              << " dispatch_latency_p999_us="
              << static_cast<double>(Percentile(
                     dispatch_latencies, 0.999L)) / 1'000.0
              << " dispatch_latency_max_us="
              << static_cast<double>(maximum_dispatch_latency_ns.load(
                     std::memory_order_relaxed)) / 1'000.0
              << " projection_cut_latency_samples="
              << cut_latencies.size()
              << " projection_cut_latency_p50_us="
              << static_cast<double>(Percentile(
                     cut_latencies, 0.50L)) / 1'000.0
              << " projection_cut_latency_p90_us="
              << static_cast<double>(Percentile(
                     cut_latencies, 0.90L)) / 1'000.0
              << " projection_cut_latency_p99_us="
              << static_cast<double>(Percentile(
                     cut_latencies, 0.99L)) / 1'000.0
              << " projection_cut_latency_max_us="
              << static_cast<double>(maximum_cut_latency_ns.load(
                     std::memory_order_relaxed)) / 1'000.0
              << " ordered_batch_fast_path="
              << stats.workers.ordered_batch_fast_path
              << " unordered_batch_sorts="
              << stats.workers.unordered_batch_sorts
              << " source_only_fast_path="
              << stats.workers.source_only_fast_path
              << " runtime_healthy="
              << (runtime_healthy ? "true" : "false")
              << " journal_healthy="
              << (journal_healthy ? "true" : "false")
              << " sink_healthy="
              << (sink.healthy() ? "true" : "false") << '\n'
              << "event_state_memory current_rss_mib="
              << static_cast<double>(process_memory.current_rss_kib) / 1024.0
              << " peak_rss_mib="
              << static_cast<double>(process_memory.peak_rss_kib) / 1024.0
              << " available="
              << (process_memory.available ? "true" : "false") << '\n'
              << "status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (!runtime_healthy) {
        std::cerr << "Event runtime fatal: " << runtime_fatal << '\n';
    }
    if (!journal_healthy) {
        std::cerr << "FactJournal fatal: " << journal_fatal << '\n';
    }
    if (!journal_cleanup_ok) {
        std::cerr << "FactJournal cleanup failed: "
                  << cleanup_error.message() << '\n';
    }
    return valid ? 0 : 1;
}
