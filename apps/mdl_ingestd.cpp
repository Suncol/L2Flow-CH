#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/sdk_runtime.h"

#if defined(L2FLOW_CH_HAS_ARROW_RING)
#include "l2flow/arrow/egress.h"
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using l2flow::ingest::CanonicalSnapshot;
using l2flow::ingest::CanonicalTick;
using l2flow::ingest::ChannelGap;
using l2flow::ingest::ChannelFault;
using l2flow::ingest::EngineConfig;
using l2flow::ingest::EngineStats;
using l2flow::ingest::IngestEngine;
using l2flow::ingest::InstrumentCatalog;
using l2flow::ingest::LateRecoveryTick;
using l2flow::ingest::LoadStreamConfig;
using l2flow::ingest::MdlMessageHandler;
using l2flow::ingest::MdlConnectionBoundaryReason;
using l2flow::ingest::PhysicalSdkConfig;
using l2flow::ingest::PhysicalSdkSession;
using l2flow::ingest::StartMode;
using l2flow::ingest::StreamMask;

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleSignal(int signal_number) {
    static_cast<void>(signal_number);
    g_stop_requested = 1;
}

enum class OperationMode : std::uint8_t {
    kLive,
    kTest,
};

struct Options final {
    EngineConfig engine{};
    PhysicalSdkConfig sdk{};
    std::string catalog_path;
    std::string stream_config_path = "config/production.streams.conf";
    OperationMode operation_mode = OperationMode::kLive;
    std::uint32_t run_seconds = 0U;
    bool run_seconds_set = false;
    bool allow_discard_after_dispatch = false;
    bool validate_only = false;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    l2flow::arrow_hot::ArrowHotEgressConfig arrow{};
    bool arrow_enabled = false;
#endif
};

inline constexpr std::uint64_t kLatencySampleEvery = 64U;
inline constexpr std::size_t kLatencyRingCapacity = 4'096U;
inline constexpr std::size_t kLatencyHistogramMaximumUs = 100'000U;
inline constexpr std::size_t kDrainBurstMessages = 256U;

struct LatencySampleSlot final {
    std::atomic<std::uint64_t> sequence{0U};
    std::atomic<std::uint64_t> latency_ns{0U};
};

// Each sampler has one drain-thread writer. Sequence tags let the reporting
// thread detect a ring overwrite without blocking the hot path.
struct alignas(64) LatencySampler final {
    std::array<LatencySampleSlot, kLatencyRingCapacity> samples{};
    std::atomic<std::uint64_t> published{0U};
    std::atomic<std::uint64_t> clock_errors{0U};
    std::uint64_t next_sequence = 0U;
};

struct LatencyWindow final {
    std::vector<std::uint64_t> samples_ns;
    std::uint64_t overwritten = 0U;
    std::uint64_t clock_errors = 0U;
};

struct LatencySummary final {
    std::size_t count = 0U;
    double average_us = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double maximum_us = 0.0;
};

class LatencyAggregate final {
public:
    LatencyAggregate()
        : histogram_(kLatencyHistogramMaximumUs + 2U, 0U) {}

    void Add(const LatencyWindow& window) {
        overwritten_ += window.overwritten;
        clock_errors_ += window.clock_errors;
        for (const std::uint64_t latency_ns : window.samples_ns) {
            ++count_;
            sum_ns_ += static_cast<long double>(latency_ns);
            maximum_ns_ = std::max(maximum_ns_, latency_ns);
            const std::uint64_t latency_us = latency_ns / 1'000U;
            const std::size_t bucket = latency_us <=
                    kLatencyHistogramMaximumUs
                ? static_cast<std::size_t>(latency_us)
                : kLatencyHistogramMaximumUs + 1U;
            ++histogram_[bucket];
        }
    }

    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
    [[nodiscard]] std::uint64_t overwritten() const noexcept {
        return overwritten_;
    }
    [[nodiscard]] std::uint64_t clock_errors() const noexcept {
        return clock_errors_;
    }
    [[nodiscard]] double average_us() const noexcept {
        if (count_ == 0U) {
            return 0.0;
        }
        return static_cast<double>(
                   sum_ns_ / static_cast<long double>(count_)) /
               1'000.0;
    }
    [[nodiscard]] double maximum_us() const noexcept {
        return static_cast<double>(maximum_ns_) / 1'000.0;
    }
    [[nodiscard]] double PercentileUs(std::uint64_t percentile) const {
        if (count_ == 0U) {
            return 0.0;
        }
        const std::uint64_t target =
            (count_ * percentile + 99U) / 100U;
        std::uint64_t cumulative = 0U;
        for (std::size_t bucket = 0U; bucket < histogram_.size(); ++bucket) {
            cumulative += histogram_[bucket];
            if (cumulative >= target) {
                return static_cast<double>(bucket);
            }
        }
        return static_cast<double>(kLatencyHistogramMaximumUs + 1U);
    }

private:
    std::vector<std::uint64_t> histogram_;
    std::uint64_t count_ = 0U;
    std::uint64_t maximum_ns_ = 0U;
    std::uint64_t overwritten_ = 0U;
    std::uint64_t clock_errors_ = 0U;
    long double sum_ns_ = 0.0L;
};

void PrintUsage() {
    std::cout
        << "usage: mdl_ingestd --mode from-open|partial --trade-date YYYYMMDD "
           "--catalog FILE --sdk-library FILE "
           "--server ADDRESS --user USER "
           "[--arrow-ring-dir DIR --arrow-feed-epoch N | "
           "--allow-discard-after-dispatch] [options]\n"
        << "\nArrow output is a volatile recent-data transport, not a WAL or "
           "Kafka acknowledgement/recovery source. The explicit discard "
           "flag remains available for physical-path testing without output.\n"
        << "\noptions:\n"
        << "  --tick-lanes N             default 12\n"
        << "  --snapshot-lanes N         default 4\n"
        << "  --instrument-workers N     default 16\n"
        << "  --operation-mode live|test default live\n"
        << "  --stream-config FILE       default "
           "config/production.streams.conf\n"
        << "  --first-decoder-cpu N      default -1 (OS scheduling)\n"
        << "  --partial-initial-hold-ns N\n"
        << "  --partial-gap-wait-ns N    default 500000\n"
        << "  --from-open-gap-wait-ns N  default 500000\n"
        << "  --sdk-work-threads N       default 1\n"
        << "  --sdk-ready-timeout-seconds N default 30; range 1..3600\n"
        << "  --sdk-console-log\n"
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        << "  --arrow-ring-dir DIR       publish per-owner Arrow rings\n"
        << "  --arrow-feed-epoch N       required nonzero connection epoch\n"
        << "  --arrow-descriptors N      power of two; default 1024\n"
        << "  --arrow-segments N         default 1088\n"
        << "  --arrow-tick-segment-bytes N      default 262144\n"
        << "  --arrow-snapshot-segment-bytes N  default 262144\n"
        << "  --arrow-diagnostic-segment-bytes N default 131072\n"
        << "  --arrow-max-consumers N    default 16; maximum 64\n"
        << "  --arrow-tick-batch-rows N  default 256\n"
        << "  --arrow-snapshot-batch-rows N default 16\n"
        << "  --arrow-diagnostic-batch-rows N default 64\n"
        << "  --arrow-batch-max-delay-ns N default 1000000\n"
        << "  --arrow-heartbeat-interval-ns N default 1000000000\n"
#endif
        << "  --run-seconds N            test mode only; 0 means until signal, "
           "max 86400\n"
        << "  --validate-only            do not load or connect the SDK\n";
}

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    Integer value{};
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
    Options parsed{};
    bool mode_set = false;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    bool arrow_feed_epoch_set = false;
#endif
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
            std::exit(0);
        } else if (argument == "--mode") {
            const std::string_view value = next(argument);
            if (value == "from-open") {
                parsed.engine.start_mode = StartMode::kFromOpen;
            } else if (value == "partial") {
                parsed.engine.start_mode = StartMode::kPartial;
            } else {
                *error = "--mode must be from-open or partial";
                return false;
            }
            mode_set = true;
        } else if (argument == "--trade-date") {
            if (!ParseInteger(next(argument), &parsed.engine.trade_date)) {
                *error = "invalid --trade-date";
                return false;
            }
        } else if (argument == "--catalog") {
            parsed.catalog_path = next(argument);
        } else if (argument == "--operation-mode") {
            const std::string_view value = next(argument);
            if (value == "live") {
                parsed.operation_mode = OperationMode::kLive;
            } else if (value == "test") {
                parsed.operation_mode = OperationMode::kTest;
            } else {
                *error = "--operation-mode must be live or test";
                return false;
            }
        } else if (argument == "--stream-config") {
            parsed.stream_config_path = next(argument);
        } else if (argument == "--sdk-library") {
            parsed.sdk.shared_library = next(argument);
        } else if (argument == "--server") {
            parsed.sdk.server_address = next(argument);
        } else if (argument == "--user") {
            parsed.sdk.user_name = next(argument);
        } else if (argument == "--tick-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.tick_decoder_lanes)) {
                *error = "invalid --tick-lanes";
                return false;
            }
        } else if (argument == "--snapshot-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.snapshot_decoder_lanes)) {
                *error = "invalid --snapshot-lanes";
                return false;
            }
        } else if (argument == "--instrument-workers") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.instrument_workers)) {
                *error = "invalid --instrument-workers";
                return false;
            }
        } else if (argument == "--first-decoder-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.first_decoder_cpu)) {
                *error = "invalid --first-decoder-cpu";
                return false;
            }
        } else if (argument == "--partial-initial-hold-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.partial_initial_hold_ns)) {
                *error = "invalid --partial-initial-hold-ns";
                return false;
            }
        } else if (argument == "--partial-gap-wait-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.partial_gap_wait_ns)) {
                *error = "invalid --partial-gap-wait-ns";
                return false;
            }
        } else if (argument == "--from-open-gap-wait-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.from_open_gap_wait_ns)) {
                *error = "invalid --from-open-gap-wait-ns";
                return false;
            }
        } else if (argument == "--sdk-work-threads") {
            if (!ParseInteger(next(argument), &parsed.sdk.work_threads)) {
                *error = "invalid --sdk-work-threads";
                return false;
            }
        } else if (argument == "--sdk-ready-timeout-seconds") {
            if (!ParseInteger(next(argument),
                              &parsed.sdk.ready_timeout_seconds) ||
                parsed.sdk.ready_timeout_seconds == 0U ||
                parsed.sdk.ready_timeout_seconds > 3'600U) {
                *error =
                    "--sdk-ready-timeout-seconds must be from 1 through 3600";
                return false;
            }
        } else if (argument == "--run-seconds") {
            if (!ParseInteger(next(argument), &parsed.run_seconds) ||
                parsed.run_seconds > 86'400U) {
                *error = "--run-seconds must be from 0 through 86400";
                return false;
            }
            parsed.run_seconds_set = true;
        } else if (argument == "--sdk-console-log") {
            parsed.sdk.sdk_console_log = true;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        } else if (argument == "--arrow-ring-dir") {
            parsed.arrow.root_directory = next(argument);
            parsed.arrow_enabled = true;
        } else if (argument == "--arrow-feed-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.feed_session_epoch)) {
                *error = "invalid --arrow-feed-epoch";
                return false;
            }
            arrow_feed_epoch_set = true;
        } else if (argument == "--arrow-descriptors") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.descriptor_capacity)) {
                *error = "invalid --arrow-descriptors";
                return false;
            }
        } else if (argument == "--arrow-segments") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.segment_count)) {
                *error = "invalid --arrow-segments";
                return false;
            }
        } else if (argument == "--arrow-tick-segment-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.tick_segment_payload_bytes)) {
                *error = "invalid --arrow-tick-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-snapshot-segment-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.snapshot_segment_payload_bytes)) {
                *error = "invalid --arrow-snapshot-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-diagnostic-segment-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.arrow.diagnostic_segment_payload_bytes)) {
                *error = "invalid --arrow-diagnostic-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-max-consumers") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.maximum_consumers)) {
                *error = "invalid --arrow-max-consumers";
                return false;
            }
        } else if (argument == "--arrow-tick-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.tick_batch_rows)) {
                *error = "invalid --arrow-tick-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-snapshot-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.snapshot_batch_rows)) {
                *error = "invalid --arrow-snapshot-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-diagnostic-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.diagnostic_batch_rows)) {
                *error = "invalid --arrow-diagnostic-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.maximum_batch_delay_ns)) {
                *error = "invalid --arrow-batch-max-delay-ns";
                return false;
            }
        } else if (argument == "--arrow-heartbeat-interval-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.heartbeat_interval_ns)) {
                *error = "invalid --arrow-heartbeat-interval-ns";
                return false;
            }
#endif
        } else if (argument == "--allow-discard-after-dispatch") {
            parsed.allow_discard_after_dispatch = true;
        } else if (argument == "--validate-only") {
            parsed.validate_only = true;
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
        if (!error->empty()) {
            return false;
        }
    }
    if (!mode_set || parsed.engine.trade_date == 0U ||
        parsed.catalog_path.empty()) {
        *error = "--mode, --trade-date, and --catalog are required";
        return false;
    }
    if (!parsed.validate_only &&
        (parsed.sdk.shared_library.empty() ||
         parsed.sdk.server_address.empty() || parsed.sdk.user_name.empty())) {
        *error = "physical mode requires --sdk-library, --server, and --user";
        return false;
    }
    bool output_configured = parsed.allow_discard_after_dispatch;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    parsed.arrow.owner_count = parsed.engine.instrument_workers;
    if (parsed.arrow_enabled) {
        if (!l2flow::arrow_hot::ValidateArrowHotEgressConfig(
                parsed.arrow, error)) {
            return false;
        }
        output_configured = true;
    } else if (arrow_feed_epoch_set) {
        *error = "--arrow-feed-epoch requires --arrow-ring-dir";
        return false;
    }
#endif
    if (!parsed.validate_only && !output_configured) {
        *error =
            "physical mode requires --arrow-ring-dir or "
            "--allow-discard-after-dispatch";
        return false;
    }
    if (parsed.operation_mode == OperationMode::kLive &&
        parsed.run_seconds_set) {
        *error = "--run-seconds is available only in test operation mode";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

void PrintStats(const EngineStats& stats) {
    std::cout << "admitted=" << stats.admitted
              << " rejected=" << stats.rejected
              << " tick_out=" << stats.dispatched_ticks
              << " snapshot_out=" << stats.dispatched_snapshots
              << " decode_errors=" << stats.decode_errors
              << " catalog_misses=" << stats.catalog_misses
              << " gaps=" << stats.gaps_skipped
              << " frozen_channels="
              << stats.from_open_channels_frozen
              << " late_or_duplicate=" << stats.duplicates_or_late
              << " late_recovery_out="
              << stats.late_recovery_dispatched
              << " lane_full=" << stats.lane_full << '\n';
}

#if defined(L2FLOW_CH_HAS_ARROW_RING)
void PrintArrowStats(
    const l2flow::arrow_hot::ArrowHotEgressStats& stats) {
    std::cout << "arrow_tick_in=" << stats.tick_rows_received
              << " arrow_snapshot_in=" << stats.snapshot_rows_received
              << " arrow_late_in=" << stats.late_recovery_rows_received
              << " arrow_control_in=" << stats.control_rows_received
              << " arrow_batches=" << stats.published_batches
              << " arrow_rows=" << stats.published_rows
              << " arrow_no_segment_dropped_rows="
              << stats.no_segment_dropped_rows
              << " arrow_oversized_dropped_rows="
              << stats.oversized_dropped_rows
              << " arrow_control_dropped_rows="
              << stats.control_publish_dropped_rows
              << " arrow_internal_errors=" << stats.internal_errors << '\n';
}

void PublishMdlConnectionBoundary(
    l2flow::arrow_hot::ArrowHotEgress* egress,
    const MdlMessageHandler& handler) noexcept {
    if (egress == nullptr) {
        return;
    }
    const std::uint64_t observed =
        handler.connection_boundary_monotonic_ns();
    switch (handler.connection_boundary_reason()) {
        case MdlConnectionBoundaryReason::kNone:
            return;
        case MdlConnectionBoundaryReason::kConnectError:
            static_cast<void>(egress->MarkFeedConnectError(observed));
            return;
        case MdlConnectionBoundaryReason::kDisconnected:
            static_cast<void>(egress->MarkFeedDisconnected(observed));
            return;
        case MdlConnectionBoundaryReason::kServiceTimeout:
            static_cast<void>(egress->MarkFeedServiceTimeout(observed));
            return;
        case MdlConnectionBoundaryReason::kMessageDiscarded:
            static_cast<void>(egress->MarkFeedMessageDiscarded(observed));
            return;
        case MdlConnectionBoundaryReason::kSubscriptionRejected:
            static_cast<void>(
                egress->MarkFeedSubscriptionRejected(observed));
            return;
        case MdlConnectionBoundaryReason::kControlProtocolError:
            static_cast<void>(
                egress->MarkFeedControlProtocolError(observed));
            return;
        case MdlConnectionBoundaryReason::kReadyTimeout:
            static_cast<void>(egress->MarkFeedReadyTimeout(observed));
            return;
    }
}
#endif

void ObserveLatency(std::uint64_t ingress_sequence,
                    std::uint64_t receive_monotonic_ns,
                    LatencySampler* sampler) noexcept {
    if (sampler == nullptr ||
        ingress_sequence % kLatencySampleEvery != 0U) {
        return;
    }
    const std::uint64_t now = l2flow::ingest::MonotonicNowNs();
    if (now < receive_monotonic_ns) {
        sampler->clock_errors.fetch_add(1U, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t write_sequence = sampler->next_sequence;
    LatencySampleSlot& slot = sampler->samples[static_cast<std::size_t>(
        write_sequence % kLatencyRingCapacity)];
    slot.latency_ns.store(
        now - receive_monotonic_ns, std::memory_order_relaxed);
    slot.sequence.store(write_sequence + 1U, std::memory_order_release);
    sampler->next_sequence = write_sequence + 1U;
    sampler->published.store(write_sequence + 1U, std::memory_order_release);
}

LatencyWindow CollectLatency(
    LatencySampler* samplers,
    std::size_t sampler_count,
    std::vector<std::uint64_t>* cursors,
    std::vector<std::uint64_t>* clock_error_cursors) {
    LatencyWindow window;
    if (samplers == nullptr || cursors == nullptr ||
        clock_error_cursors == nullptr || cursors->size() != sampler_count ||
        clock_error_cursors->size() != sampler_count) {
        return window;
    }
    for (std::size_t index = 0U; index < sampler_count; ++index) {
        LatencySampler& sampler = samplers[index];
        const std::uint64_t end =
            sampler.published.load(std::memory_order_acquire);
        std::uint64_t begin = (*cursors)[index];
        if (end < begin) {
            begin = end;
        }
        if (end - begin > kLatencyRingCapacity) {
            window.overwritten += end - begin - kLatencyRingCapacity;
            begin = end - kLatencyRingCapacity;
        }
        for (std::uint64_t sequence = begin; sequence < end; ++sequence) {
            LatencySampleSlot& slot = sampler.samples[
                static_cast<std::size_t>(
                    sequence % kLatencyRingCapacity)];
            const std::uint64_t expected = sequence + 1U;
            const std::uint64_t first_tag =
                slot.sequence.load(std::memory_order_acquire);
            const std::uint64_t latency_ns =
                slot.latency_ns.load(std::memory_order_relaxed);
            const std::uint64_t second_tag =
                slot.sequence.load(std::memory_order_acquire);
            if (first_tag == expected && second_tag == expected) {
                window.samples_ns.push_back(latency_ns);
            } else {
                ++window.overwritten;
            }
        }
        (*cursors)[index] = end;

        const std::uint64_t clock_errors =
            sampler.clock_errors.load(std::memory_order_acquire);
        const std::uint64_t previous_clock_errors =
            (*clock_error_cursors)[index];
        if (clock_errors >= previous_clock_errors) {
            window.clock_errors += clock_errors - previous_clock_errors;
        }
        (*clock_error_cursors)[index] = clock_errors;
    }
    return window;
}

LatencySummary SummarizeLatency(
    std::vector<std::uint64_t>* samples_ns) {
    LatencySummary summary;
    if (samples_ns == nullptr || samples_ns->empty()) {
        return summary;
    }
    std::sort(samples_ns->begin(), samples_ns->end());
    summary.count = samples_ns->size();
    const long double sum = std::accumulate(
        samples_ns->begin(), samples_ns->end(), 0.0L);
    summary.average_us = static_cast<double>(
        sum / static_cast<long double>(summary.count)) / 1'000.0;
    const auto percentile = [samples_ns](std::size_t percent) {
        const std::size_t rank =
            (samples_ns->size() * percent + 99U) / 100U - 1U;
        return static_cast<double>((*samples_ns)[rank]) / 1'000.0;
    };
    summary.p50_us = percentile(50U);
    summary.p95_us = percentile(95U);
    summary.p99_us = percentile(99U);
    summary.maximum_us =
        static_cast<double>(samples_ns->back()) / 1'000.0;
    return summary;
}

[[nodiscard]] std::uint64_t CounterDelta(std::uint64_t current,
                                         std::uint64_t previous) noexcept {
    return current >= previous ? current - previous : 0U;
}

void PrintMonitor(const EngineStats& current,
                  const EngineStats& previous,
                  double interval_seconds,
                  const LatencySummary& latency,
                  const LatencyWindow& latency_window,
                  const LatencyAggregate& latency_aggregate) {
    const double safe_interval = interval_seconds > 0.0
        ? interval_seconds
        : 1.0;
    const std::uint64_t dispatch_current =
        current.dispatched_ticks + current.dispatched_snapshots;
    const std::uint64_t dispatch_previous =
        previous.dispatched_ticks + previous.dispatched_snapshots;
    std::cout << std::fixed << std::setprecision(3)
              << "monitor interval_s=" << safe_interval
              << " callback_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     current.callbacks, previous.callbacks)) /
                     safe_interval
              << " admit_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     current.admitted, previous.admitted)) /
                     safe_interval
              << " dispatch_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     dispatch_current, dispatch_previous)) /
                     safe_interval
              << " delay_sample_every=" << kLatencySampleEvery
              << " delay_samples=" << latency.count
              << " delay_avg_us=" << latency.average_us
              << " delay_p50_us=" << latency.p50_us
              << " delay_p95_us=" << latency.p95_us
              << " delay_p99_us=" << latency.p99_us
              << " delay_max_us=" << latency.maximum_us
              << " delay_overwritten=" << latency_window.overwritten
              << " delay_clock_errors=" << latency_window.clock_errors
              << " total_delay_samples=" << latency_aggregate.count()
              << " total_delay_avg_us=" << latency_aggregate.average_us()
              << " total_delay_p50_us="
              << latency_aggregate.PercentileUs(50U)
              << " total_delay_p95_us="
              << latency_aggregate.PercentileUs(95U)
              << " total_delay_p99_us="
              << latency_aggregate.PercentileUs(99U)
              << " total_delay_max_us="
              << latency_aggregate.maximum_us()
              << " total_delay_overwritten="
              << latency_aggregate.overwritten()
              << " total_delay_clock_errors="
              << latency_aggregate.clock_errors()
              << " admitted=" << current.admitted
              << " rejected=" << current.rejected
              << " tick_out=" << current.dispatched_ticks
              << " snapshot_out=" << current.dispatched_snapshots
              << " decode_errors=" << current.decode_errors
              << " catalog_misses=" << current.catalog_misses
              << " gaps=" << current.gaps_skipped
              << " lane_full=" << current.lane_full << '\n'
              << std::flush;
}

void PrintFinalLatency(const LatencyAggregate& latency) {
    std::cout << std::fixed << std::setprecision(3)
              << "final_delay basis=callback_to_dispatch"
              << " sample_every=" << kLatencySampleEvery
              << " samples=" << latency.count()
              << " avg_us=" << latency.average_us()
              << " p50_us=" << latency.PercentileUs(50U)
              << " p95_us=" << latency.PercentileUs(95U)
              << " p99_us=" << latency.PercentileUs(99U)
              << " max_us=" << latency.maximum_us()
              << " overwritten=" << latency.overwritten()
              << " clock_errors=" << latency.clock_errors() << '\n'
              << std::flush;
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << "configuration error: " << error << '\n';
        PrintUsage();
        return 2;
    }
    InstrumentCatalog catalog;
    StreamMask stream_mask = 0U;
    if (!LoadStreamConfig(
            options.stream_config_path, &stream_mask, &error)) {
        std::cerr << "stream config error: " << error << '\n';
        return 2;
    }
    options.engine.enabled_streams = stream_mask;
    options.sdk.enabled_streams = stream_mask;
    if (!InstrumentCatalog::LoadCsv(
            options.catalog_path, &catalog, &error)) {
        std::cerr << "catalog error: " << error << '\n';
        return 2;
    }
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        options.engine, std::move(catalog), &error);
    if (engine == nullptr) {
        std::cerr << "engine configuration error: " << error << '\n';
        return 2;
    }
    if (options.validate_only) {
        std::cout << "configuration valid\n";
        return 0;
    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    std::unique_ptr<l2flow::arrow_hot::ArrowHotEgress> arrow_egress;
    if (options.arrow_enabled) {
        arrow_egress = l2flow::arrow_hot::ArrowHotEgress::Create(
            options.arrow, &error);
        if (arrow_egress == nullptr) {
            std::cerr << "Arrow hot-egress start failed: " << error << '\n';
            return 1;
        }
        std::cout << "arrow_hot_run="
                  << arrow_egress->run_directory().string()
                  << " feed_epoch=" << options.arrow.feed_session_epoch
                  << " producer_instance="
                  << l2flow::arrow_hot::ProducerInstanceIdString(
                         arrow_egress->producer_instance())
                  << '\n';
    }
    const auto arrow_healthy = [&arrow_egress] {
        return arrow_egress == nullptr || arrow_egress->healthy();
    };
#else
    const auto arrow_healthy = [] { return true; };
#endif
    if (!engine->Start(&error)) {
        std::cerr << "engine start failed: " << error << '\n';
        return 1;
    }

    std::atomic<bool> drain_running{true};
    std::atomic<std::uint64_t> consumed_ticks{0U};
    std::atomic<std::uint64_t> consumed_snapshots{0U};
    std::atomic<std::uint64_t> consumed_gaps{0U};
    std::atomic<std::uint64_t> consumed_late_recovery{0U};
    std::atomic<std::uint64_t> consumed_faults{0U};
    std::unique_ptr<LatencySampler[]> latency_samplers;
    std::vector<std::uint64_t> latency_cursors;
    std::vector<std::uint64_t> latency_clock_error_cursors;
    std::unique_ptr<LatencyAggregate> latency_aggregate;
    if (options.operation_mode == OperationMode::kTest) {
        latency_samplers = std::make_unique<LatencySampler[]>(
            options.engine.instrument_workers);
        latency_cursors.resize(options.engine.instrument_workers, 0U);
        latency_clock_error_cursors.resize(
            options.engine.instrument_workers, 0U);
        latency_aggregate = std::make_unique<LatencyAggregate>();
    }
    std::vector<std::thread> drain_threads;
    drain_threads.reserve(options.engine.instrument_workers);
    for (std::size_t owner = 0U;
         owner < options.engine.instrument_workers; ++owner) {
        drain_threads.emplace_back([&, owner] {
            CanonicalTick tick{};
            CanonicalSnapshot snapshot{};
            ChannelGap gap{};
            LateRecoveryTick late_recovery{};
            ChannelFault fault{};
            while (drain_running.load(std::memory_order_acquire)) {
                bool progress = false;
                for (std::size_t drained = 0U;
                     drained < kDrainBurstMessages &&
                     engine->TryPollTick(owner, &tick);
                     ++drained) {
                    if (latency_samplers != nullptr) {
                        ObserveLatency(
                            tick.common.ingress_sequence,
                            tick.common.receive_monotonic_ns,
                            &latency_samplers[owner]);
                    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                    if (arrow_egress != nullptr) {
                        static_cast<void>(
                            arrow_egress->AppendTick(owner, tick));
                    }
#endif
                    consumed_ticks.fetch_add(1U, std::memory_order_relaxed);
                    progress = true;
                }
                for (std::size_t drained = 0U;
                     drained < kDrainBurstMessages &&
                     engine->TryPollSnapshot(owner, &snapshot);
                     ++drained) {
                    if (latency_samplers != nullptr) {
                        ObserveLatency(
                            snapshot.common.ingress_sequence,
                            snapshot.common.receive_monotonic_ns,
                            &latency_samplers[owner]);
                    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                    if (arrow_egress != nullptr) {
                        static_cast<void>(
                            arrow_egress->AppendSnapshot(owner, snapshot));
                    }
#endif
                    consumed_snapshots.fetch_add(
                        1U, std::memory_order_relaxed);
                    progress = true;
                }
                if (owner == 0U) {
                    for (std::size_t drained = 0U;
                         drained < kDrainBurstMessages &&
                         engine->TryPollGap(&gap);
                         ++drained) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                        if (arrow_egress != nullptr) {
                            static_cast<void>(arrow_egress->AppendGap(gap));
                        }
#endif
                        consumed_gaps.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                    for (std::size_t drained = 0U;
                         drained < kDrainBurstMessages &&
                         engine->TryPollLateRecovery(&late_recovery);
                         ++drained) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                        if (arrow_egress != nullptr) {
                            static_cast<void>(
                                arrow_egress->AppendLateRecovery(
                                    late_recovery));
                        }
#endif
                        consumed_late_recovery.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                    for (std::size_t drained = 0U;
                         drained < kDrainBurstMessages &&
                         engine->TryPollChannelFault(&fault);
                         ++drained) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                        if (arrow_egress != nullptr) {
                            static_cast<void>(
                                arrow_egress->AppendFault(fault));
                        }
#endif
                        consumed_faults.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                if (arrow_egress != nullptr) {
                    arrow_egress->FlushDue(
                        owner, l2flow::ingest::MonotonicNowNs());
                }
#endif
                if (!progress) {
                    std::this_thread::yield();
                }
            }
        });
    }

    const auto stop_engine_and_drain = [&] {
        engine->Stop();
        for (;;) {
            const EngineStats final_stats = engine->stats();
            if (consumed_ticks.load(std::memory_order_acquire) >=
                    final_stats.dispatched_ticks &&
                consumed_snapshots.load(std::memory_order_acquire) >=
                    final_stats.dispatched_snapshots &&
                consumed_late_recovery.load(std::memory_order_acquire) >=
                    final_stats.late_recovery_dispatched &&
                consumed_faults.load(std::memory_order_acquire) >=
                    final_stats.channel_faults_dispatched) {
                break;
            }
            std::this_thread::yield();
        }
        drain_running.store(false, std::memory_order_release);
        for (std::thread& thread : drain_threads) {
            thread.join();
        }
        // Gap delivery is a coalescing state mailbox, so event-count equality
        // is not meaningful. Once its producers and normal consumer stop,
        // collect the last dirty snapshots explicitly.
        ChannelGap final_gap{};
        while (engine->TryPollGap(&final_gap)) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            if (arrow_egress != nullptr) {
                static_cast<void>(arrow_egress->AppendGap(final_gap));
            }
#endif
            consumed_gaps.fetch_add(1U, std::memory_order_relaxed);
        }
    };

    MdlMessageHandler handler(engine.get(), stream_mask);
    std::unique_ptr<PhysicalSdkSession> sdk = PhysicalSdkSession::Connect(
        options.sdk, &handler, &error);
    if (sdk == nullptr) {
        stop_engine_and_drain();
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        if (arrow_egress != nullptr) {
            PublishMdlConnectionBoundary(arrow_egress.get(), handler);
            arrow_egress->FlushAll();
            arrow_egress->Seal(l2flow::ingest::MonotonicNowNs());
        }
#endif
        std::cerr << "SDK connect failed: " << error << '\n';
        return 1;
    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr && handler.feed_ready()) {
        static_cast<void>(arrow_egress->MarkFeedConnected(
            handler.feed_ready_monotonic_ns()));
    }
#endif

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    if (options.operation_mode == OperationMode::kLive) {
        while (g_stop_requested == 0 && engine->healthy() &&
               !handler.failed() &&
               handler.connection_boundary_reason() ==
                   l2flow::ingest::MdlConnectionBoundaryReason::kNone &&
               arrow_healthy()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            PrintStats(engine->stats());
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            if (arrow_egress != nullptr) {
                PrintArrowStats(arrow_egress->stats());
            }
#endif
        }
    } else {
        using SteadyClock = std::chrono::steady_clock;
        const SteadyClock::time_point monitor_started = SteadyClock::now();
        SteadyClock::time_point previous_report_time = monitor_started;
        SteadyClock::time_point next_report_time =
            monitor_started + std::chrono::seconds(1);
        const SteadyClock::time_point run_deadline = options.run_seconds == 0U
            ? SteadyClock::time_point::max()
            : monitor_started + std::chrono::seconds(options.run_seconds);
        EngineStats previous_stats = engine->stats();
        while (g_stop_requested == 0 && engine->healthy() &&
               !handler.failed() &&
               handler.connection_boundary_reason() ==
                   l2flow::ingest::MdlConnectionBoundaryReason::kNone &&
               arrow_healthy()) {
            std::this_thread::sleep_until(
                std::min(next_report_time, run_deadline));
            const SteadyClock::time_point now = SteadyClock::now();
            if (now >= next_report_time) {
                const EngineStats current_stats = engine->stats();
                LatencyWindow latency_window = CollectLatency(
                    latency_samplers.get(),
                    options.engine.instrument_workers,
                    &latency_cursors, &latency_clock_error_cursors);
                latency_aggregate->Add(latency_window);
                const LatencySummary latency_summary =
                    SummarizeLatency(&latency_window.samples_ns);
                const double interval_seconds =
                    std::chrono::duration<double>(
                        now - previous_report_time).count();
                PrintMonitor(current_stats, previous_stats, interval_seconds,
                             latency_summary, latency_window,
                             *latency_aggregate);
                previous_stats = current_stats;
                previous_report_time = now;
                next_report_time = now + std::chrono::seconds(1);
            }
            if (now >= run_deadline) {
                break;
            }
        }
    }

    // Shutdown order is contractual: quiesce SDK callbacks, drain decoder
    // lanes, then drain the instrument-dispatch queues.
    sdk->Shutdown();
    stop_engine_and_drain();
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr) {
        PublishMdlConnectionBoundary(arrow_egress.get(), handler);
        arrow_egress->FlushAll();
        arrow_egress->Seal(l2flow::ingest::MonotonicNowNs());
        PrintArrowStats(arrow_egress->stats());
    }
#endif
    const EngineStats final_stats = engine->stats();
    PrintStats(final_stats);
    if (options.operation_mode == OperationMode::kTest) {
        LatencyWindow final_latency_window = CollectLatency(
            latency_samplers.get(), options.engine.instrument_workers,
            &latency_cursors, &latency_clock_error_cursors);
        latency_aggregate->Add(final_latency_window);
        PrintFinalLatency(*latency_aggregate);
    }
    if (!engine->healthy()) {
        std::cerr << "fatal ingest error: " << engine->fatal_error() << '\n';
        return 1;
    }
    if (handler.failed()) {
        std::cerr << "fatal SDK callback adapter error\n";
        return 1;
    }
    if (handler.connection_boundary_reason() !=
        l2flow::ingest::MdlConnectionBoundaryReason::kNone) {
        const std::string boundary_detail =
            handler.connection_boundary_detail();
        std::cerr
            << "MDL connection boundary observed; post-reconnect data was "
               "rejected. Restart mdl_ingestd with a new Arrow feed epoch";
        if (!boundary_detail.empty()) {
            std::cerr << ": " << boundary_detail;
        }
        std::cerr << '\n';
        return 1;
    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr && !arrow_egress->healthy()) {
        std::cerr << "fatal Arrow hot-egress error: "
                  << arrow_egress->fatal_error() << '\n';
        return 1;
    }
#endif
    return 0;
}
