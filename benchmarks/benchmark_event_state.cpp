#include "l2flow/event/runtime.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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
using l2flow::ingest::TickAction;
using l2flow::ingest::TradingPhase;

struct Options final {
    std::uint64_t target_rate = 800'000U;
    std::uint32_t seconds = 1U;
    std::size_t actors = 16U;
    std::size_t micro_batch_rows = 256U;
    bool unpaced = false;
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
        << "  --seconds N           measured seconds; default 1, max 30\n"
        << "  --actors N            Event owner actors; default 16\n"
        << "  --micro-batch-rows N  journal-first cut; default 256\n"
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
        parsed.seconds == 0U || parsed.seconds > 30U ||
        parsed.actors == 0U || parsed.actors > 100'000U ||
        parsed.actors >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        parsed.micro_batch_rows == 0U ||
        parsed.target_rate >
            std::numeric_limits<std::uint64_t>::max() / parsed.seconds) {
        *error = "invalid rate, duration, actor, or micro-batch bound";
        return false;
    }
    *output = parsed;
    error->clear();
    return true;
}

class InMemoryRevisionSink final : public EventRevisionSink {
public:
    explicit InMemoryRevisionSink(std::size_t actors)
        : actors_(actors),
          last_sequence_(
              std::make_unique<std::atomic<std::uint64_t>[]>(actors)) {}

    [[nodiscard]] bool AppendRevisionBatch(
        std::shared_ptr<const EventRevisionBatch> batch) noexcept override {
        if (batch == nullptr || batch->revisions.empty() ||
            static_cast<std::size_t>(batch->owner) >= actors_ ||
            !healthy_.load(std::memory_order_acquire)) {
            healthy_.store(false, std::memory_order_release);
            return false;
        }
        std::atomic<std::uint64_t>& last = last_sequence_[batch->owner];
        std::uint64_t previous = last.load(std::memory_order_relaxed);
        if (batch->batch_sequence <= previous ||
            !last.compare_exchange_strong(
                previous, batch->batch_sequence,
                std::memory_order_release, std::memory_order_relaxed)) {
            healthy_.store(false, std::memory_order_release);
            return false;
        }
        rows_.fetch_add(
            static_cast<std::uint64_t>(batch->revisions.size()),
            std::memory_order_relaxed);
        batches_.fetch_add(1U, std::memory_order_relaxed);
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

private:
    std::size_t actors_ = 0U;
    std::unique_ptr<std::atomic<std::uint64_t>[]> last_sequence_;
    std::atomic<std::uint64_t> rows_{0U};
    std::atomic<std::uint64_t> batches_{0U};
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

    EventRuntimeConfig config{};
    config.worker.trade_date = 20260807U;
    config.worker.owner_count = static_cast<std::uint32_t>(options.actors);
    config.worker.revision_epoch = 1U;
    config.worker.logic_version = 1U;
    config.worker.calculation_run_id.bytes[0U] = std::byte{1U};
    config.worker.maximum_facts = static_cast<std::size_t>(
        maximum_per_actor + options.micro_batch_rows + 1U);
    config.worker.maximum_orders = 1U;
    config.worker.maximum_cached_events = config.worker.maximum_facts;
    config.worker.maximum_pending_commits = 1'024U;
    config.worker.maximum_acknowledged_raw_dependencies =
        std::max<std::size_t>(options.micro_batch_rows * 2U, 1'024U);
    config.micro_batch_rows = options.micro_batch_rows;
    config.micro_batch_max_delay_ns = 1'000'000U;
    config.maximum_raw_ack_backlog_per_owner =
        config.worker.maximum_acknowledged_raw_dependencies;
    config.maximum_late_backlog_per_owner = 1U;

    InMemoryRevisionSink sink(options.actors);
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
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
    const std::uint64_t start_ns =
        MonotonicNowNs() + UINT64_C(100'000'000);
    std::vector<std::thread> threads;
    threads.reserve(options.actors);
    for (std::size_t actor = 0U; actor < options.actors; ++actor) {
        threads.emplace_back([&, actor] {
            std::vector<CanonicalTick> acknowledgements;
            acknowledgements.reserve(options.micro_batch_rows);
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
                const std::uint64_t now = options.unpaced
                    ? MonotonicNowNs()
                    : WaitUntil(deadline);
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
                CanonicalTick tick = MakeStatusTick(
                    actor, local_sequence, ordinal + 1U, now);
                if (!runtime->AppendTick(actor, tick)) {
                    abort.store(true, std::memory_order_release);
                    break;
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
            }
            if (!acknowledgements.empty() &&
                !runtime->OnRawTickBatchAcknowledged(acknowledgements)) {
                abort.store(true, std::memory_order_release);
            }
            if (!runtime->Flush(actor)) {
                abort.store(true, std::memory_order_release);
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

    const bool drained = runtime->DrainAll();
    std::uint64_t completed_ns = start_ns;
    for (std::size_t actor = 0U; actor < options.actors; ++actor) {
        completed_ns = std::max(
            completed_ns,
            finish_ns[actor].load(std::memory_order_acquire));
    }
    const EventRuntimeStats stats = runtime->stats();
    const std::uint64_t first_processing_ns = first_actual_ns.load(
        std::memory_order_acquire);
    const std::uint64_t measurement_origin_ns =
        first_processing_ns == std::numeric_limits<std::uint64_t>::max()
            ? start_ns
            : first_processing_ns;
    const double elapsed_seconds = completed_ns <= measurement_origin_ns
        ? 0.0
        : static_cast<double>(completed_ns - measurement_origin_ns) /
              1'000'000'000.0;
    const double effective_rate = elapsed_seconds == 0.0
        ? 0.0
        : static_cast<double>(stats.workers.facts_journaled) /
              elapsed_seconds;
    const double required_rate =
        static_cast<double>(options.target_rate) * 0.99;
    const bool valid = !abort.load(std::memory_order_acquire) && drained &&
        runtime->healthy() && sink.healthy() &&
        stats.normal_ticks_received == total &&
        stats.raw_tick_acks_received == total &&
        stats.workers.facts_journaled == total &&
        stats.workers.revisions_created == total &&
        stats.workers.pending_raw_commits == 0U && sink.rows() == total &&
        effective_rate >= required_rate;

    std::cout << std::fixed << std::setprecision(3)
              << "event_state_config actors=" << options.actors
              << " target_msg_s=" << options.target_rate
              << " seconds=" << options.seconds
              << " micro_batch_rows=" << options.micro_batch_rows
              << " pacing=" << (options.unpaced ? "unpaced" : "scheduled")
              << " storage=bounded_memory wal=false disk_queue=false\n"
              << "event_state_result expected_facts=" << total
              << " facts_journaled=" << stats.workers.facts_journaled
              << " revisions_created=" << stats.workers.revisions_created
              << " revision_batches=" << sink.batches()
              << " revision_rows_acked_in_memory=" << sink.rows()
              << " pending_raw_commits="
              << stats.workers.pending_raw_commits << '\n'
              << "event_state_throughput elapsed_s=" << elapsed_seconds
              << " effective_fact_s=" << effective_rate
              << " maximum_schedule_lag_us="
              << static_cast<double>(maximum_schedule_lag_ns.load(
                     std::memory_order_relaxed)) /
                     1'000.0
              << " ordered_batch_fast_path="
              << stats.workers.ordered_batch_fast_path
              << " unordered_batch_sorts="
              << stats.workers.unordered_batch_sorts
              << " source_only_fast_path="
              << stats.workers.source_only_fast_path << '\n'
              << "status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (!runtime->healthy()) {
        std::cerr << "Event runtime fatal: " << runtime->fatal_error()
                  << '\n';
    }
    return valid ? 0 : 1;
}
