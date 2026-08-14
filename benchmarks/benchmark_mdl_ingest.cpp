#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/sdk_runtime.h"

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
#include "l2flow/clickhouse/event_sink.h"
#include "l2flow/clickhouse/kline_sink.h"
#include "l2flow/clickhouse/raw_sink.h"
#include "l2flow/event/runtime.h"
#include "l2flow/journal/fact_journal.h"
#include "l2flow/kline/runtime.h"
#endif

#if defined(L2FLOW_CH_HAS_ARROW_RING)
#include "l2flow/arrow/egress.h"
#include "l2flow/arrow/ring.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
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
#include <unistd.h>
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
    std::uint64_t target_rate = 1'000'000U;
    std::uint64_t measurement_seconds = 5U;
    std::uint64_t warmup_seconds = 1U;
    std::size_t channels = 16U;
    std::size_t tick_lanes = 12U;
    std::size_t instrument_owners = 16U;
    std::size_t dispatch_queue_capacity = 4'096U;
    std::size_t tick_consumer_count = 1U;
    std::uint64_t latency_sample_every = 1U;
    std::uint64_t gap_wait_ns = 500'000U;
    ArrivalPattern pattern = ArrivalPattern::kOrdered;
    std::size_t reorder_window = 1U;
    int producer_cpu = 0;
    int first_consumer_cpu = 1;
    int first_decoder_cpu = 17;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    l2flow::clickhouse::RawClickHouseConfig clickhouse{};
    l2flow::clickhouse::EventClickHouseConfig clickhouse_event{};
    l2flow::clickhouse::KLineClickHouseConfig clickhouse_kline{};
    l2flow::event::EventRuntimeConfig event{};
    l2flow::kline::KLineRuntimeConfig kline{};
    std::filesystem::path fact_journal_directory;
    bool clickhouse_enabled = false;
    bool clickhouse_configuration_set = false;
    bool event_enabled = false;
    bool event_configuration_set = false;
    bool event_construction_smoke = false;
    bool kline_enabled = false;
    bool kline_configuration_set = false;
    bool kline_interval_set = false;
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    std::filesystem::path arrow_ring_directory;
    std::uint64_t arrow_feed_session_epoch = 1U;
    std::size_t arrow_descriptor_capacity = 256U;
    std::size_t arrow_segment_count = 320U;
    std::size_t arrow_tick_segment_payload_bytes = 256U * 1'024U;
    std::size_t arrow_tick_batch_rows = 256U;
    std::uint64_t arrow_batch_max_delay_ns = 1'000'000U;
    int first_arrow_reader_cpu = -1;
    bool arrow_configuration_set = false;
#endif
};



#if defined(L2FLOW_CH_HAS_ARROW_RING)
[[nodiscard]] l2flow::arrow_hot::ArrowHotEgressConfig MakeArrowConfig(
    const Options& options) {
    l2flow::arrow_hot::ArrowHotEgressConfig config{};
    config.root_directory = options.arrow_ring_directory;
    config.owner_count = options.instrument_owners;
    config.feed_session_epoch = options.arrow_feed_session_epoch;
    config.descriptor_capacity = options.arrow_descriptor_capacity;
    config.segment_count = options.arrow_segment_count;
    config.tick_segment_payload_bytes =
        options.arrow_tick_segment_payload_bytes;
    // The benchmark emits only Tick rows. Keep unused rings small while still
    // exercising the same production ring-set creation and lifecycle path.
    config.snapshot_segment_payload_bytes = 1'024U;
    config.diagnostic_segment_payload_bytes = 16U * 1'024U;
    config.maximum_consumers = 1U;
    config.tick_batch_rows = options.arrow_tick_batch_rows;
    config.snapshot_batch_rows = 1U;
    config.diagnostic_batch_rows = 1U;
    config.maximum_batch_delay_ns = options.arrow_batch_max_delay_ns;
    return config;
}
#endif

struct alignas(64) ConsumerState final {
    std::atomic<std::uint64_t> consumed{0U};
    std::uint64_t secondary_consumed = 0U;
    std::uint64_t secondary_errors = 0U;
    std::uint64_t measured = 0U;
    std::uint64_t latency_samples = 0U;
    std::uint64_t maximum_latency_ns = 0U;
    std::uint64_t maximum_sink_operation_ns = 0U;
    std::uint64_t ordering_errors = 0U;
    std::uint64_t clock_errors = 0U;
    std::uint64_t finish_ns = 0U;
    int observed_cpu = -1;
    std::vector<std::uint64_t> latencies_ns;
    std::string error;
};

#if defined(L2FLOW_CH_HAS_ARROW_RING)
struct alignas(64) ArrowReaderState final {
    std::uint64_t rows = 0U;
    std::uint64_t batches = 0U;
    std::uint64_t latency_samples = 0U;
    std::uint64_t maximum_latency_ns = 0U;
    std::uint64_t ordering_errors = 0U;
    std::uint64_t clock_errors = 0U;
    std::uint64_t protocol_errors = 0U;
    std::uint64_t finish_ns = 0U;
    int observed_cpu = -1;
    std::vector<std::uint64_t> latencies_ns;
    std::string error;
};
#endif

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
    MessageKey key,
    std::size_t body_size) noexcept {
    std::array<std::byte, kMdlHeaderBytes> header{};
    PutUnsigned<std::uint8_t>(header, 0U, 23U);
    PutUnsigned<std::uint32_t>(
        header, 1U, static_cast<std::uint32_t>(23U + body_size));
    PutUnsigned<std::uint8_t>(header, 5U, 1U);
    PutUnsigned<std::uint8_t>(header, 6U, key.service_id);
    PutUnsigned<std::uint16_t>(header, 7U, key.service_version);
    PutUnsigned<std::uint16_t>(header, 9U, key.message_id);
    PutUnsigned<std::uint32_t>(header, 11U, 93'000'000U);
    return header;
}

[[nodiscard]] std::vector<std::byte> MakeReadyLogonResponse(
    MessageKey subscribed_key) {
    std::vector<std::byte> body(48U);
    // LogonResponse.Services is at 12; its sole item begins at 24.
    PutUnsigned<std::uint32_t>(body, 12U, 1U);
    PutUnsigned<std::uint32_t>(body, 16U, 12U);
    PutUnsigned<std::uint32_t>(body, 20U, 0U);
    PutUnsigned<std::uint32_t>(body, 24U, subscribed_key.service_id);
    PutUnsigned<std::uint32_t>(body, 28U, subscribed_key.service_version);
    // ServicesItem.Messages is at 32; its sole item begins at 40.
    PutUnsigned<std::uint32_t>(body, 32U, 1U);
    PutUnsigned<std::uint32_t>(body, 36U, 8U);
    PutUnsigned<std::uint32_t>(body, 40U, subscribed_key.message_id);
    PutUnsigned<std::uint32_t>(body, 44U, 0U);
    return body;
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
        << "  --rate N                 target callbacks/s (default 1000000)\n"
        << "  --seconds N              measured seconds (default 5)\n"
        << "  --warmup-seconds N       warm-up seconds (default 1)\n"
        << "  --pattern ordered|local-reverse\n"
        << "  --reorder-window N       per-Channel reverse window\n"
        << "  --channels N             default 16\n"
        << "  --tick-lanes N           default 12\n"
        << "  --owners N               must equal channels; default 16\n"
        << "  --dispatch-queue-capacity N default 4096\n"
        << "  --tick-consumers N       independent outbox cursors; default 1\n"
        << "  --sample-every N         latency percentile sample stride\n"
        << "  --gap-wait-ns N          FROM_OPEN reorder wait; default 500000\n"
        << "  --producer-cpu N         default 0\n"
        << "  --first-consumer-cpu N   default 1\n"
        << "  --first-decoder-cpu N    default 17\n";
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    std::cout
        << "  --clickhouse-url URL     enable durable raw writes\n"
        << "  --clickhouse-database NAME default l2flow\n"
        << "  --clickhouse-feed-epoch N required nonzero run epoch\n"
        << "  --clickhouse-writers N   default 2\n"
        << "  --clickhouse-tick-batch-rows N default 16384\n"
        << "  --clickhouse-tick-batch-bytes N default 16777216\n"
        << "  --clickhouse-tick-batch-max-delay-ns N default 5000000\n"
        << "  --clickhouse-queue-batches-per-lane N default 8\n"
        << "  --clickhouse-request-timeout-ms N default 10000\n"
        << "  --clickhouse-maximum-retry-ms N default 5000\n"
        << "  --clickhouse-shutdown-timeout-ms N default 30000\n"
        << "  --clickhouse-no-auto-create use pre-created tables\n"
        << "  --event-enable            enable Event worker + Event ClickHouse\n"
        << "  --event-micro-batch-rows N default 512\n"
        << "  --event-micro-batch-max-delay-ns N default 50000000\n"
        << "  --event-persistence-group-max-batches N default 1024\n"
        << "  --event-persistence-group-max-rows N default 16384\n"
        << "  --event-persistence-group-max-bytes N default 16777216\n"
        << "  --event-persistence-group-max-delay-ns N default 1000000000\n"
        << "  --event-insert-request-max-rows N default 1024\n"
        << "  --event-insert-request-max-bytes N default 1048576\n"
        << "  --event-physical-group-max-batches N default 256\n"
        << "  --event-physical-group-max-delay-ns N default 1000000\n"
        << "  --event-writer-lanes N (1,2,4,8,16,32) default 1\n"
        << "  --event-queue-revision-batches N default 1024\n"
        << "  --event-queue-revision-rows N default 1048576\n"
        << "  --event-maximum-pending-channel-seals N default 4096\n"
        << "  --fact-journal-dir DIR   unique shared Event/KLine file directory; default system temp\n"
        << "  --event-journal-dir DIR  deprecated alias for --fact-journal-dir\n"
        << "  --event-construction-smoke validate Event construction without network I/O\n";
    std::cout
        << "  --kline-enable           enable KLine worker + KLine ClickHouse\n"
        << "  --kline-interval-seconds N repeatable; default 1\n"
        << "  --kline-micro-batch-rows N default 256\n"
        << "  --kline-micro-batch-max-delay-ns N default 1000000\n"
        << "  --kline-insert-request-max-rows N default 1024\n"
        << "  --kline-insert-request-max-bytes N default 1048576\n"
        << "  --kline-physical-group-max-batches N default 256\n"
        << "  --kline-physical-group-max-delay-ns N default 1000000\n"
        << "  --kline-writer-lanes N (1,2,4,8,16,32) default 1\n"
        << "  --kline-queue-revision-batches N default 1024\n"
        << "  --kline-queue-revision-rows N default 1048576\n"
        << "  --kline-maximum-occurrence-join N default 65536\n";
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    std::cout
        << "  --arrow-ring-dir PATH    enable Arrow publish/read/decode\n"
        << "  --arrow-feed-epoch N     default 1\n"
        << "  --arrow-descriptors N    default 256, power of two\n"
        << "  --arrow-segments N       default 320\n"
        << "  --arrow-tick-bytes N     segment payload bytes; default 262144\n"
        << "  --arrow-batch-rows N     default 256\n"
        << "  --arrow-max-delay-ns N   default 1000000\n"
        << "  --first-arrow-reader-cpu N  default after decoder CPUs\n";
#endif
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
        } else if (argument == "--dispatch-queue-capacity") {
            if (!ParseInteger(next(argument),
                              &parsed.dispatch_queue_capacity)) {
                *error = "invalid --dispatch-queue-capacity";
                return false;
            }
        } else if (argument == "--tick-consumers") {
            if (!ParseInteger(next(argument),
                              &parsed.tick_consumer_count)) {
                *error = "invalid --tick-consumers";
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
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        } else if (argument == "--clickhouse-url") {
            const std::string_view value = next(argument);
            if (value.empty()) {
                if (error->empty()) {
                    *error = "invalid --clickhouse-url";
                }
                return false;
            }
            parsed.clickhouse.endpoint = value;
            parsed.clickhouse_enabled = true;
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-database") {
            parsed.clickhouse.database = next(argument);
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-feed-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.feed_session_epoch)) {
                *error = "invalid --clickhouse-feed-epoch";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-writers") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.writer_threads)) {
                *error = "invalid --clickhouse-writers";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-tick-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.tick_batch_rows)) {
                *error = "invalid --clickhouse-tick-batch-rows";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-tick-batch-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.tick_batch_bytes)) {
                *error = "invalid --clickhouse-tick-batch-bytes";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument ==
                   "--clickhouse-tick-batch-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse.tick_batch_max_delay_ns)) {
                *error = "invalid --clickhouse-tick-batch-max-delay-ns";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument ==
                   "--clickhouse-queue-batches-per-lane") {
            std::size_t capacity = 0U;
            if (!ParseInteger(next(argument), &capacity)) {
                *error = "invalid --clickhouse-queue-batches-per-lane";
                return false;
            }
            parsed.clickhouse.tick_queue_batches_per_lane = capacity;
            parsed.clickhouse.snapshot_queue_batches_per_lane = capacity;
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-request-timeout-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.request_timeout_ms)) {
                *error = "invalid --clickhouse-request-timeout-ms";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-maximum-retry-ms") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse.maximum_retry_elapsed_ms)) {
                *error = "invalid --clickhouse-maximum-retry-ms";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-shutdown-timeout-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.shutdown_timeout_ms)) {
                *error = "invalid --clickhouse-shutdown-timeout-ms";
                return false;
            }
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--clickhouse-no-auto-create") {
            parsed.clickhouse.ensure_local_tables = false;
            parsed.clickhouse_configuration_set = true;
        } else if (argument == "--event-enable") {
            parsed.event_enabled = true;
            parsed.event_configuration_set = true;
        } else if (argument == "--event-micro-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.event.micro_batch_rows)) {
                *error = "invalid --event-micro-batch-rows";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-micro-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.event.micro_batch_max_delay_ns)) {
                *error = "invalid --event-micro-batch-max-delay-ns";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument ==
                   "--event-persistence-group-max-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_batches)) {
                *error =
                    "invalid --event-persistence-group-max-batches";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-persistence-group-max-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_rows)) {
                *error = "invalid --event-persistence-group-max-rows";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-persistence-group-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_bytes)) {
                *error = "invalid --event-persistence-group-max-bytes";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument ==
                   "--event-persistence-group-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_delay_ns)) {
                *error =
                    "invalid --event-persistence-group-max-delay-ns";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-insert-request-max-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .insert_request_max_rows)) {
                *error = "invalid --event-insert-request-max-rows";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-insert-request-max-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .insert_request_max_bytes)) {
                *error = "invalid --event-insert-request-max-bytes";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-physical-group-max-batches") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .physical_group_max_batches)) {
                *error = "invalid --event-physical-group-max-batches";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-physical-group-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .physical_group_max_delay_ns)) {
                *error = "invalid --event-physical-group-max-delay-ns";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-writer-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event.writer_lanes)) {
                *error = "invalid --event-writer-lanes";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-queue-revision-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_event.queue_revision_batches)) {
                *error = "invalid --event-queue-revision-batches";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-queue-revision-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_event.queue_revision_rows)) {
                *error = "invalid --event-queue-revision-rows";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument ==
                   "--event-maximum-pending-channel-seals") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event
                         .maximum_pending_channel_seals_per_owner)) {
                *error =
                    "invalid --event-maximum-pending-channel-seals";
                return false;
            }
            parsed.event_configuration_set = true;
        } else if (argument == "--event-journal-dir" ||
                   argument == "--fact-journal-dir") {
            const std::string_view value = next(argument);
            if (value.empty()) {
                if (error->empty()) {
                    *error = "invalid --fact-journal-dir";
                }
                return false;
            }
            parsed.fact_journal_directory = value;
        } else if (argument == "--event-construction-smoke") {
            parsed.event_construction_smoke = true;
            parsed.event_configuration_set = true;
        } else if (argument == "--kline-enable") {
            parsed.kline_enabled = true;
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-interval-seconds") {
            std::uint32_t interval = 0U;
            if (!ParseInteger(next(argument), &interval)) {
                *error = "invalid --kline-interval-seconds";
                return false;
            }
            if (!parsed.kline_interval_set) {
                parsed.kline.worker.interval_seconds.clear();
                parsed.kline_interval_set = true;
            }
            parsed.kline.worker.interval_seconds.push_back(interval);
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-micro-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.micro_batch_rows)) {
                *error = "invalid --kline-micro-batch-rows";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-micro-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.micro_batch_max_delay_ns)) {
                *error = "invalid --kline-micro-batch-max-delay-ns";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-insert-request-max-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.insert_request_max_rows)) {
                *error = "invalid --kline-insert-request-max-rows";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-insert-request-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.insert_request_max_bytes)) {
                *error = "invalid --kline-insert-request-max-bytes";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-physical-group-max-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.physical_group_max_batches)) {
                *error = "invalid --kline-physical-group-max-batches";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-physical-group-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.physical_group_max_delay_ns)) {
                *error = "invalid --kline-physical-group-max-delay-ns";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-writer-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_kline.writer_lanes)) {
                *error = "invalid --kline-writer-lanes";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-queue-revision-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.queue_revision_batches)) {
                *error = "invalid --kline-queue-revision-batches";
                return false;
            }
            parsed.kline_configuration_set = true;
        } else if (argument == "--kline-queue-revision-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.queue_revision_rows)) {
                *error = "invalid --kline-queue-revision-rows";
                return false;
            }
            parsed.kline_configuration_set = true;
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        } else if (argument == "--arrow-ring-dir") {
            const std::string_view value = next(argument);
            if (value.empty()) {
                if (error->empty()) {
                    *error = "invalid --arrow-ring-dir";
                }
                return false;
            }
            parsed.arrow_ring_directory = std::string(value);
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-feed-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow_feed_session_epoch)) {
                *error = "invalid --arrow-feed-epoch";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-descriptors") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow_descriptor_capacity)) {
                *error = "invalid --arrow-descriptors";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-segments") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow_segment_count)) {
                *error = "invalid --arrow-segments";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-tick-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.arrow_tick_segment_payload_bytes)) {
                *error = "invalid --arrow-tick-bytes";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow_tick_batch_rows)) {
                *error = "invalid --arrow-batch-rows";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--arrow-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow_batch_max_delay_ns)) {
                *error = "invalid --arrow-max-delay-ns";
                return false;
            }
            parsed.arrow_configuration_set = true;
        } else if (argument == "--first-arrow-reader-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.first_arrow_reader_cpu)) {
                *error = "invalid --first-arrow-reader-cpu";
                return false;
            }
            parsed.arrow_configuration_set = true;
#endif
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
        parsed.dispatch_queue_capacity == 0U ||
        parsed.tick_consumer_count == 0U ||
        parsed.tick_consumer_count > kMaximumTickConsumers ||
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
#if defined(__linux__)
    if (parsed.tick_lanes >= static_cast<std::size_t>(CPU_SETSIZE)) {
        *error = "tick-lane count exceeds the affinity CPU set";
        return false;
    }
#endif
    if (parsed.channels >
            std::numeric_limits<std::uint64_t>::max() /
                parsed.reorder_window ||
        parsed.target_rate >
            std::numeric_limits<std::uint64_t>::max() /
                (parsed.measurement_seconds + parsed.warmup_seconds)) {
        *error = "message-count calculation overflows";
        return false;
    }
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (parsed.clickhouse_configuration_set &&
        !parsed.clickhouse_enabled) {
        *error = "ClickHouse benchmark options require --clickhouse-url";
        return false;
    }
    if (parsed.clickhouse_enabled) {
        if (parsed.clickhouse.feed_session_epoch == 0U) {
            *error = "--clickhouse-url requires a nonzero "
                     "--clickhouse-feed-epoch";
            return false;
        }
        parsed.clickhouse.tick_decoder_lanes = parsed.tick_lanes;
        parsed.clickhouse.snapshot_decoder_lanes = 1U;
        if (!l2flow::clickhouse::ValidateRawClickHouseConfig(
                parsed.clickhouse, error)) {
            return false;
        }
    }
    if (parsed.event_configuration_set && !parsed.event_enabled) {
        *error = "Event benchmark options require --event-enable";
        return false;
    }
    if (parsed.event_enabled && !parsed.clickhouse_enabled) {
        *error = "--event-enable requires --clickhouse-url";
        return false;
    }
    if (parsed.event_enabled) {
        parsed.clickhouse_event.endpoint = parsed.clickhouse.endpoint;
        parsed.clickhouse_event.database = parsed.clickhouse.database;
        parsed.clickhouse_event.username = parsed.clickhouse.username;
        parsed.clickhouse_event.password = parsed.clickhouse.password;
        parsed.clickhouse_event.no_proxy = parsed.clickhouse.no_proxy;
        parsed.clickhouse_event.connect_timeout_ms =
            parsed.clickhouse.connect_timeout_ms;
        parsed.clickhouse_event.request_timeout_ms =
            parsed.clickhouse.request_timeout_ms;
        parsed.clickhouse_event.retry_initial_backoff_ms =
            parsed.clickhouse.retry_initial_backoff_ms;
        parsed.clickhouse_event.retry_max_backoff_ms =
            parsed.clickhouse.retry_max_backoff_ms;
        parsed.clickhouse_event.maximum_retry_elapsed_ms =
            parsed.clickhouse.maximum_retry_elapsed_ms;
        parsed.clickhouse_event.shutdown_timeout_ms =
            parsed.clickhouse.shutdown_timeout_ms;
        parsed.clickhouse_event.insert_quorum = parsed.clickhouse.insert_quorum;
        parsed.clickhouse_event.insert_quorum_parallel =
            parsed.clickhouse.insert_quorum_parallel;
        parsed.clickhouse_event.ensure_local_tables =
            parsed.clickhouse.ensure_local_tables;
        parsed.clickhouse_event.tls_verify_peer =
            parsed.clickhouse.tls_verify_peer;
    }
    if (parsed.kline_configuration_set && !parsed.kline_enabled) {
        *error = "KLine benchmark options require --kline-enable";
        return false;
    }
    if (parsed.kline_enabled && !parsed.clickhouse_enabled) {
        *error = "--kline-enable requires --clickhouse-url";
        return false;
    }
    if (parsed.kline_enabled) {
        std::sort(parsed.kline.worker.interval_seconds.begin(),
                  parsed.kline.worker.interval_seconds.end());
        parsed.kline.worker.interval_seconds.erase(
            std::unique(parsed.kline.worker.interval_seconds.begin(),
                        parsed.kline.worker.interval_seconds.end()),
            parsed.kline.worker.interval_seconds.end());
        parsed.clickhouse_kline.endpoint = parsed.clickhouse.endpoint;
        parsed.clickhouse_kline.database = parsed.clickhouse.database;
        parsed.clickhouse_kline.username = parsed.clickhouse.username;
        parsed.clickhouse_kline.password = parsed.clickhouse.password;
        parsed.clickhouse_kline.no_proxy = parsed.clickhouse.no_proxy;
        parsed.clickhouse_kline.connect_timeout_ms =
            parsed.clickhouse.connect_timeout_ms;
        parsed.clickhouse_kline.request_timeout_ms =
            parsed.clickhouse.request_timeout_ms;
        parsed.clickhouse_kline.retry_initial_backoff_ms =
            parsed.clickhouse.retry_initial_backoff_ms;
        parsed.clickhouse_kline.retry_max_backoff_ms =
            parsed.clickhouse.retry_max_backoff_ms;
        parsed.clickhouse_kline.maximum_retry_elapsed_ms =
            parsed.clickhouse.maximum_retry_elapsed_ms;
        parsed.clickhouse_kline.shutdown_timeout_ms =
            parsed.clickhouse.shutdown_timeout_ms;
        parsed.clickhouse_kline.insert_quorum = parsed.clickhouse.insert_quorum;
        parsed.clickhouse_kline.insert_quorum_parallel =
            parsed.clickhouse.insert_quorum_parallel;
        parsed.clickhouse_kline.ensure_local_tables =
            parsed.clickhouse.ensure_local_tables;
        parsed.clickhouse_kline.tls_verify_peer =
            parsed.clickhouse.tls_verify_peer;
    }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    const bool arrow_enabled = !parsed.arrow_ring_directory.empty();
    if (parsed.arrow_configuration_set && !arrow_enabled) {
        *error = "Arrow benchmark options require --arrow-ring-dir";
        return false;
    }
    if (arrow_enabled) {
        const std::size_t decoder_threads = parsed.tick_lanes + 1U;
        if (parsed.first_arrow_reader_cpu == -1) {
            if (parsed.first_decoder_cpu < 0) {
                *error = "automatic Arrow reader CPU range is invalid";
                return false;
            }
            const std::size_t automatic_cpu =
                static_cast<std::size_t>(parsed.first_decoder_cpu) +
                decoder_threads;
            if (automatic_cpu > static_cast<std::size_t>(
                                    std::numeric_limits<int>::max())) {
                *error = "automatic Arrow reader CPU range overflows";
                return false;
            }
            parsed.first_arrow_reader_cpu =
                static_cast<int>(automatic_cpu);
        }
        const l2flow::arrow_hot::ArrowHotEgressConfig arrow_config =
            MakeArrowConfig(parsed);
        if (!l2flow::arrow_hot::ValidateArrowHotEgressConfig(
                arrow_config, error)) {
            return false;
        }
    }
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW) && \
    defined(L2FLOW_CH_HAS_ARROW_RING)
    if (parsed.clickhouse_enabled && arrow_enabled &&
        parsed.clickhouse.feed_session_epoch !=
            parsed.arrow_feed_session_epoch) {
        *error = "--clickhouse-feed-epoch and --arrow-feed-epoch must match";
        return false;
    }
#endif
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
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled &&
        (parsed.first_arrow_reader_cpu < 0 ||
         static_cast<std::size_t>(parsed.first_arrow_reader_cpu) +
                 parsed.instrument_owners >
             static_cast<std::size_t>(CPU_SETSIZE))) {
        *error = "Arrow reader CPU affinity range is invalid";
        return false;
    }
#endif
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
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled) {
        for (std::size_t owner = 0U;
             owner < parsed.instrument_owners; ++owner) {
            if (!claim(parsed.first_arrow_reader_cpu +
                       static_cast<int>(owner))) {
                *error = "CPU affinity assignments overlap";
                return false;
            }
        }
    }
#endif
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

#if defined(L2FLOW_CH_HAS_ARROW_RING) || \
    defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
void RecordMaximumElapsed(std::uint64_t start_ns,
                          std::uint64_t finish_ns,
                          std::uint64_t* maximum_ns) noexcept {
    if (maximum_ns != nullptr && finish_ns >= start_ns) {
        *maximum_ns = std::max(*maximum_ns, finish_ns - start_ns);
    }
}
#endif

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

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
[[nodiscard]] std::filesystem::path MakeUniqueEventJournalPath(
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
            ("l2flow-mdl-event-" + process + '-' +
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

class EventJournalFileCleanup final {
public:
    explicit EventJournalFileCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}

    ~EventJournalFileCleanup() {
        if (armed_) {
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove(path_, ignored));
        }
    }

    EventJournalFileCleanup(const EventJournalFileCleanup&) = delete;
    EventJournalFileCleanup& operator=(const EventJournalFileCleanup&) = delete;

    [[nodiscard]] bool Remove(std::error_code* error) noexcept {
        std::error_code local_error;
        const bool removed = std::filesystem::remove(path_, local_error);
        if (removed && !local_error) {
            armed_ = false;
        }
        if (error != nullptr) {
            *error = local_error;
        }
        return removed && !local_error;
    }

private:
    std::filesystem::path path_;
    bool armed_ = true;
};
#endif

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
    if (options.tick_consumer_count > 1U &&
        total_count > std::numeric_limits<std::uint64_t>::max() /
                          static_cast<std::uint64_t>(
                              options.tick_consumer_count - 1U)) {
        std::cerr << "secondary outbox read count overflows\n";
        return 2;
    }
    const std::uint64_t expected_secondary_consumed =
        total_count * static_cast<std::uint64_t>(
                          options.tick_consumer_count - 1U);
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
    config.feed_session_epoch =
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        options.clickhouse_enabled ? options.clickhouse.feed_session_epoch :
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        !options.arrow_ring_directory.empty()
            ? options.arrow_feed_session_epoch :
#endif
        1U;
    config.start_mode = StartMode::kFromOpen;
    config.tick_decoder_lanes = options.tick_lanes;
    config.snapshot_decoder_lanes = 1U;
    config.instrument_workers = options.instrument_owners;
    config.tick_slots_per_lane = 16'384U;
    config.snapshot_slots_per_lane = 8U;
    config.maximum_tick_body_bytes = 256U;
    config.maximum_snapshot_body_bytes = 1'024U;
    config.dispatch_queue_capacity = options.dispatch_queue_capacity;
    config.tick_consumer_count = options.tick_consumer_count;
    config.diagnostic_queue_capacity = 256U;
    config.maximum_channels_per_tick_lane =
        std::max<std::size_t>(
            8U, (options.channels + options.tick_lanes - 1U) /
                    options.tick_lanes + 4U);
    // Exercise the requested 4,096 x 64 per-Channel sequence window.
    config.reorder_entries_per_channel = 4'096U * 64U;
    config.maximum_reorder_span = 4'096U * 64U;
    config.from_open_gap_wait_ns = options.gap_wait_ns;
    config.first_decoder_cpu = options.first_decoder_cpu;
    constexpr MessageKey kBenchmarkStream{4U, 101U, 24U};
    config.enabled_streams = StreamBit(kBenchmarkStream);

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    std::filesystem::path fact_journal_path;
    std::unique_ptr<EventJournalFileCleanup> fact_journal_cleanup;
    std::shared_ptr<l2flow::journal::CanonicalFactJournal> fact_journal;
    std::unique_ptr<l2flow::clickhouse::EventClickHouseSink>
        clickhouse_event;
    std::unique_ptr<l2flow::event::EventRuntime> event_runtime;
    std::unique_ptr<l2flow::clickhouse::KLineClickHouseSink>
        clickhouse_kline;
    std::unique_ptr<l2flow::kline::KLineRuntime> kline_runtime;
    bool fact_journal_shared_by_all_owners = true;
    if (options.event_enabled || options.kline_enabled) {
        if (total_count >
            (std::numeric_limits<std::uint64_t>::max() -
             l2flow::journal::kFactJournalFileHeaderBytes) /
                l2flow::journal::kFactJournalRecordBytes) {
            std::cerr << "FactJournal byte size overflows uint64_t\n";
            return 2;
        }
        std::error_code filesystem_error;
        std::filesystem::path journal_directory =
            options.fact_journal_directory;
        if (journal_directory.empty()) {
            journal_directory = std::filesystem::temp_directory_path(
                filesystem_error);
        }
        if (filesystem_error || journal_directory.empty()) {
            std::cerr << "cannot resolve FactJournal directory: "
                      << filesystem_error.message() << '\n';
            return 2;
        }
        std::filesystem::create_directories(
            journal_directory, filesystem_error);
        if (filesystem_error ||
            !std::filesystem::is_directory(
                journal_directory, filesystem_error) ||
            filesystem_error) {
            std::cerr << "cannot create FactJournal directory: "
                      << filesystem_error.message() << '\n';
            return 2;
        }
        fact_journal_path = MakeUniqueEventJournalPath(
            journal_directory, &error);
        if (fact_journal_path.empty()) {
            std::cerr << "FactJournal path creation failed: " << error
                      << '\n';
            return 2;
        }
        fact_journal_cleanup =
            std::make_unique<EventJournalFileCleanup>(fact_journal_path);
        l2flow::journal::FactJournalConfig journal_config{};
        journal_config.trade_date = config.trade_date;
        journal_config.path = fact_journal_path;
        journal_config.maximum_records = total_count;
        std::unique_ptr<l2flow::journal::CanonicalFactJournal>
            created_journal =
                l2flow::journal::CanonicalFactJournal::Create(
                    std::move(journal_config), &error);
        if (created_journal == nullptr) {
            std::cerr << "FactJournal creation failed: " << error
                      << '\n';
            return 1;
        }
        fact_journal = std::shared_ptr<
            l2flow::journal::CanonicalFactJournal>(
                std::move(created_journal));
    }

    if (options.event_enabled) {
        options.event.worker.trade_date = config.trade_date;
        options.event.feed_session_epoch = config.feed_session_epoch;
        options.event.worker.feed_session_epoch =
            config.feed_session_epoch;
        options.event.worker.owner_count =
            static_cast<std::uint32_t>(options.instrument_owners);
        options.event.worker.revision_epoch = 1U;
        options.event.worker.logic_version = 1U;
        if (!l2flow::clickhouse::ParseIdentifier(
                "00000000000000000000000000000001",
                &options.event.worker.calculation_run_id)) {
            std::cerr << "failed to initialize Event calculation run ID\n";
            return 2;
        }
        l2flow::event::EventRuntimeConfig event_config = options.event;
        event_config.worker.fact_journal = fact_journal;
        if (!l2flow::event::ValidateEventRuntimeConfig(
                event_config, &error) ||
            !l2flow::clickhouse::ValidateEventClickHouseConfig(
                options.clickhouse_event, &error)) {
            std::cerr << "Event benchmark configuration invalid: "
                      << error << '\n';
            return 2;
        }
        clickhouse_event =
            l2flow::clickhouse::EventClickHouseSink::Create(
                options.clickhouse_event, &error);
        if (clickhouse_event == nullptr) {
            std::cerr << "ClickHouse Event sink creation failed: "
                      << error << '\n';
            return 1;
        }
        event_runtime = l2flow::event::EventRuntime::Create(
            std::move(event_config), clickhouse_event.get(), &error);
        if (event_runtime == nullptr) {
            std::cerr << "Event runtime creation failed: " << error << '\n';
            return 1;
        }
        fact_journal_shared_by_all_owners =
            event_runtime->config().worker.fact_journal ==
            fact_journal;
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            const l2flow::event::EventWorker* const worker =
                event_runtime->worker(owner);
            fact_journal_shared_by_all_owners =
                fact_journal_shared_by_all_owners && worker != nullptr &&
                worker->config().fact_journal == fact_journal;
        }
    }

    if (options.kline_enabled) {
        options.kline.worker.trade_date = config.trade_date;
        options.kline.feed_session_epoch = config.feed_session_epoch;
        options.kline.worker.feed_session_epoch = config.feed_session_epoch;
        options.kline.worker.owner_count =
            static_cast<std::uint32_t>(options.instrument_owners);
        options.kline.worker.revision_epoch = 2U;
        options.kline.worker.logic_version = 1U;
        if (!l2flow::clickhouse::ParseIdentifier(
                "00000000000000000000000000000002",
                &options.kline.worker.calculation_run_id)) {
            std::cerr << "failed to initialize KLine calculation run ID\n";
            return 2;
        }
        l2flow::kline::KLineRuntimeConfig kline_config = options.kline;
        kline_config.worker.fact_journal = fact_journal;
        if (!l2flow::kline::ValidateKLineRuntimeConfig(
                kline_config, &error) ||
            !l2flow::clickhouse::ValidateKLineClickHouseConfig(
                options.clickhouse_kline, &error)) {
            std::cerr << "KLine benchmark configuration invalid: "
                      << error << '\n';
            return 2;
        }
        clickhouse_kline =
            l2flow::clickhouse::KLineClickHouseSink::Create(
                options.clickhouse_kline, &error);
        if (clickhouse_kline == nullptr) {
            std::cerr << "ClickHouse KLine sink creation failed: "
                      << error << '\n';
            return 1;
        }
        kline_runtime = l2flow::kline::KLineRuntime::Create(
            std::move(kline_config), clickhouse_kline.get(), &error);
        if (kline_runtime == nullptr) {
            std::cerr << "KLine runtime creation failed: " << error << '\n';
            return 1;
        }
        fact_journal_shared_by_all_owners =
            fact_journal_shared_by_all_owners &&
            kline_runtime->config().worker.fact_journal == fact_journal;
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            const l2flow::kline::KLineWorker* const worker =
                kline_runtime->worker(owner);
            fact_journal_shared_by_all_owners =
                fact_journal_shared_by_all_owners && worker != nullptr &&
                worker->config().fact_journal == fact_journal;
        }
    }

    if (event_runtime != nullptr || kline_runtime != nullptr) {

    }

    if (options.event_enabled) {
        if (options.event_construction_smoke) {
            const bool runtime_healthy = event_runtime->healthy();
            const bool sink_healthy = clickhouse_event->healthy();
            const bool kline_runtime_healthy =
                kline_runtime == nullptr || kline_runtime->healthy();
            const bool kline_sink_healthy =
                clickhouse_kline == nullptr || clickhouse_kline->healthy();
            const bool journal_flushed = fact_journal->Flush();
            const l2flow::journal::FactJournalStats journal_stats =
                fact_journal->stats();
            const bool journal_healthy = fact_journal->healthy();
            std::error_code file_size_error;
            const std::uintmax_t observed_file_bytes =
                std::filesystem::file_size(
                    fact_journal_path, file_size_error);
            event_runtime.reset();
            kline_runtime.reset();
            clickhouse_event.reset();
            clickhouse_kline.reset();
            fact_journal.reset();
            std::error_code cleanup_error;
            const bool cleanup_ok =
                fact_journal_cleanup->Remove(&cleanup_error);
            const bool valid = runtime_healthy && sink_healthy &&
                kline_runtime_healthy && kline_sink_healthy &&
                fact_journal_shared_by_all_owners && journal_flushed &&
                journal_healthy && journal_stats.records == 0U &&
                journal_stats.file_bytes ==
                    l2flow::journal::kFactJournalFileHeaderBytes &&
                journal_stats.write_bytes ==
                    l2flow::journal::kFactJournalFileHeaderBytes &&
                journal_stats.partial_writes == 0U &&
                journal_stats.partial_reads == 0U &&
                journal_stats.active_writes == 0U &&
                journal_stats.active_reads == 0U &&
                journal_stats.reserved_records == 0U &&
                journal_stats.waiting_admissions == 0U &&
                journal_stats.errors == 0U &&
                journal_stats.flush_calls == 1U && !file_size_error &&
                observed_file_bytes ==
                    l2flow::journal::kFactJournalFileHeaderBytes &&
                cleanup_ok;
            std::cout
                << "event_construction_smoke shared_journal="
                << (fact_journal_shared_by_all_owners ? "true" : "false")
                << " records=" << journal_stats.records
                << " file_bytes=" << journal_stats.file_bytes
                << " observed_file_bytes=" << observed_file_bytes
                << " partial_writes=" << journal_stats.partial_writes
                << " partial_reads=" << journal_stats.partial_reads
                << " flush_calls=" << journal_stats.flush_calls
                << " runtime_healthy="
                << (runtime_healthy ? "true" : "false")
                << " journal_healthy="
                << (journal_healthy ? "true" : "false")
                << " sink_healthy=" << (sink_healthy ? "true" : "false")
                << " kline_runtime_healthy="
                << (kline_runtime_healthy ? "true" : "false")
                << " kline_sink_healthy="
                << (kline_sink_healthy ? "true" : "false")
                << " cleanup=" << (cleanup_ok ? "removed" : "failed")
                << " status=" << (valid ? "PASS" : "FAIL") << '\n';
            if (!cleanup_ok) {
                std::cerr << "FactJournal cleanup failed: "
                          << cleanup_error.message() << '\n';
            }
            return valid ? 0 : 1;
        }
    }
    std::unique_ptr<l2flow::clickhouse::RawClickHouseSink> clickhouse_raw;
    if (options.clickhouse_enabled) {
        clickhouse_raw = l2flow::clickhouse::RawClickHouseSink::Create(
            options.clickhouse, &error);
        if (clickhouse_raw == nullptr) {
            std::cerr << "ClickHouse raw sink creation failed: "
                      << error << '\n';
            return 1;
        }
        config.raw_record_tap = clickhouse_raw.get();
    }
#endif
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        config, std::move(catalog), &error);
    if (engine == nullptr) {
        std::cerr << error << '\n';
        return 1;
    }

#if defined(L2FLOW_CH_HAS_ARROW_RING)
    const bool arrow_enabled = !options.arrow_ring_directory.empty();
    std::unique_ptr<l2flow::arrow_hot::ArrowHotEgress> arrow_egress;
    std::vector<std::unique_ptr<l2flow::arrow_hot::SharedArrowRingReader>>
        arrow_readers;
    if (arrow_enabled) {
        arrow_egress = l2flow::arrow_hot::ArrowHotEgress::Create(
            MakeArrowConfig(options), &error);
        if (arrow_egress == nullptr) {
            std::cerr << "Arrow benchmark egress start failed: "
                      << error << '\n';
            return 1;
        }
        arrow_readers.reserve(options.instrument_owners);
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            const std::string ring_name =
                "tick-owner-" + std::to_string(owner);
            l2flow::arrow_hot::RingLocation location{
                arrow_egress->run_directory() /
                    (ring_name + ".arrow"),
                arrow_egress->run_directory() /
                    (ring_name + ".ctl")};
            std::unique_ptr<l2flow::arrow_hot::SharedArrowRingReader> reader =
                l2flow::arrow_hot::SharedArrowRingReader::Open(
                    std::move(location),
                    l2flow::arrow_hot::ReaderStart::kEarliestAvailable,
                    &error);
            if (reader == nullptr ||
                reader->stream_kind() !=
                    l2flow::arrow_hot::RingStreamKind::kOrderedTick ||
                reader->shard_id() != static_cast<std::uint32_t>(owner) ||
                reader->feed_session_epoch() !=
                    options.arrow_feed_session_epoch ||
                reader->producer_instance() !=
                    arrow_egress->producer_instance()) {
                std::cerr << "Arrow benchmark reader open failed for owner "
                          << owner;
                if (!error.empty()) {
                    std::cerr << ": " << error;
                }
                std::cerr << '\n';
                return 1;
            }
            arrow_readers.push_back(std::move(reader));
        }
    }
#endif

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (clickhouse_event != nullptr) {
        if (!clickhouse_event->Start(&error)) {
            std::cerr << "ClickHouse Event sink start failed: "
                      << error << '\n';
            return 1;
        }
        std::cout
            << "clickhouse_event_writer_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_event->writer_instance_id())
            << " calculation_run_id="
            << l2flow::clickhouse::IdentifierString(
                   options.event.worker.calculation_run_id)
            << " micro_batch_rows=" << options.event.micro_batch_rows
            << " micro_batch_max_delay_ns="
            << options.event.micro_batch_max_delay_ns
            << " maximum_pending_channel_seals="
            << options.event.maximum_pending_channel_seals_per_owner
            << " persistence_group_max_batches="
            << options.event.worker.persistence_group_max_batches
            << " persistence_group_max_rows="
            << options.event.worker.persistence_group_max_rows
            << " persistence_group_max_bytes="
            << options.event.worker.persistence_group_max_bytes
            << " persistence_group_max_delay_ns="
            << options.event.worker.persistence_group_max_delay_ns
            << " insert_request_max_rows="
            << options.clickhouse_event.insert_request_max_rows
            << " insert_request_max_bytes="
            << options.clickhouse_event.insert_request_max_bytes
            << " physical_group_max_batches="
            << options.clickhouse_event.physical_group_max_batches
            << " physical_group_max_delay_ns="
            << options.clickhouse_event.physical_group_max_delay_ns
            << " writer_lanes=" << options.clickhouse_event.writer_lanes
            << '\n';
    }
    if (clickhouse_kline != nullptr) {
        if (!clickhouse_kline->Start(&error)) {
            if (clickhouse_event != nullptr) {
                std::string event_stop_error;
                static_cast<void>(clickhouse_event->Stop(&event_stop_error));
            }
            std::cerr << "ClickHouse KLine sink start failed: "
                      << error << '\n';
            return 1;
        }
        std::cout
            << "clickhouse_kline_writer_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_kline->writer_instance_id())
            << " calculation_run_id="
            << l2flow::clickhouse::IdentifierString(
                   options.kline.worker.calculation_run_id)
            << " intervals=" << options.kline.worker.interval_seconds.size()
            << " micro_batch_rows=" << options.kline.micro_batch_rows
            << " micro_batch_max_delay_ns="
            << options.kline.micro_batch_max_delay_ns
            << " insert_request_max_rows="
            << options.clickhouse_kline.insert_request_max_rows
            << " insert_request_max_bytes="
            << options.clickhouse_kline.insert_request_max_bytes
            << " physical_group_max_batches="
            << options.clickhouse_kline.physical_group_max_batches
            << " physical_group_max_delay_ns="
            << options.clickhouse_kline.physical_group_max_delay_ns
            << " writer_lanes=" << options.clickhouse_kline.writer_lanes
            << '\n';
    }
    if (clickhouse_raw != nullptr) {
        if (!clickhouse_raw->Start(&error)) {
            if (clickhouse_event != nullptr) {
                std::string event_stop_error;
                static_cast<void>(clickhouse_event->Stop(&event_stop_error));
            }
            if (clickhouse_kline != nullptr) {
                std::string kline_stop_error;
                static_cast<void>(clickhouse_kline->Stop(&kline_stop_error));
            }
            std::cerr << "ClickHouse raw sink start failed: "
                      << error << '\n';
            return 1;
        }
        std::cout
            << "clickhouse_raw_writer_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_raw->writer_instance_id())
            << " source_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_raw->source_instance_id())
            << " feed_epoch=" << options.clickhouse.feed_session_epoch
            << " run_started_utc_ns="
            << clickhouse_raw->run_started_utc_ns()
            << " run_started_monotonic_ns="
            << clickhouse_raw->run_started_monotonic_ns() << '\n';
    }
#endif
    // ClickHouse writers inherit the caller affinity. Create them while the
    // main thread still has the benchmark's broad NUMA CPU mask, then isolate
    // this thread on the dedicated producer CPU before starting admission.
    if (!PinCurrentThread(options.producer_cpu, &error)) {
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_raw->Stop(&stop_error));
        }
        if (clickhouse_event != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_event->Stop(&stop_error));
        }
        if (clickhouse_kline != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_kline->Stop(&stop_error));
        }
#endif
        std::cerr << error << '\n';
        return 2;
    }
    if (!engine->Start(&error)) {
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_raw->Stop(&stop_error));
        }
        if (clickhouse_event != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_event->Stop(&stop_error));
        }
        if (clickhouse_kline != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_kline->Stop(&stop_error));
        }
#endif
        std::cerr << error << '\n';
        return 1;
    }

    MdlMessageHandler handler(engine.get(), config.enabled_streams);
    SyntheticMdlMessage synthetic_message;
    std::vector<std::byte> logon_body =
        MakeReadyLogonResponse(kBenchmarkStream);
    const auto logon_header =
        MakeHeader({2U, 101U, 2U}, logon_body.size());
    synthetic_message.Set(logon_header, logon_body);
    handler.OnMessage(nullptr, &synthetic_message);
    if (!handler.feed_ready() ||
        handler.last_result() != AdmissionResult::kAccepted) {
        engine->Stop();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_raw->Stop(&stop_error));
        }
#endif
        std::cerr << "synthetic MDL LogonResponse did not reach readiness\n";
        return 1;
    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr &&
        !arrow_egress->MarkFeedConnected(
            handler.feed_ready_monotonic_ns())) {
        engine->Stop();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_raw->Stop(&stop_error));
        }
#endif
        std::cerr << "Arrow benchmark could not publish feed readiness: "
                  << arrow_egress->fatal_error() << '\n';
        return 1;
    }
#endif

    auto consumers = std::make_unique<ConsumerState[]>(
        options.instrument_owners);
    for (std::size_t owner = 0U; owner < options.instrument_owners;
         ++owner) {
        consumers[owner].latencies_ns.resize(
            static_cast<std::size_t>(latency_samples_per_channel));
    }
    std::atomic<bool> abort{false};
    std::atomic<std::size_t> ready{0U};
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    std::unique_ptr<ArrowReaderState[]> arrow_reader_states;
    std::vector<std::thread> arrow_reader_threads;
    if (arrow_enabled) {
        arrow_reader_states = std::make_unique<ArrowReaderState[]>(
            options.instrument_owners);
        arrow_reader_threads.reserve(options.instrument_owners);
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            arrow_reader_states[owner].latencies_ns.resize(
                static_cast<std::size_t>(latency_samples_per_channel));
            arrow_reader_threads.emplace_back([&, owner] {
                ArrowReaderState& state = arrow_reader_states[owner];
                if (!PinCurrentThread(
                        options.first_arrow_reader_cpu +
                            static_cast<int>(owner),
                        &state.error)) {
                    abort.store(true, std::memory_order_release);
                    ready.fetch_add(1U, std::memory_order_release);
                    return;
                }
                state.observed_cpu = CurrentCpu();
                ready.fetch_add(1U, std::memory_order_release);
                l2flow::arrow_hot::SharedArrowRingReader* const reader =
                    arrow_readers[owner].get();
                while (state.rows < total_per_channel &&
                       !abort.load(std::memory_order_acquire)) {
                    l2flow::arrow_hot::ReadResult read = reader->TryRead();
                    if (read.code == l2flow::arrow_hot::ReadCode::kEmpty ||
                        read.code == l2flow::arrow_hot::ReadCode::kRetry) {
                        CpuRelax();
                        continue;
                    }
                    if (read.code !=
                        l2flow::arrow_hot::ReadCode::kBatch) {
                        ++state.protocol_errors;
                        state.error = read.code ==
                                l2flow::arrow_hot::ReadCode::kOverrun
                            ? "Arrow benchmark reader overrun"
                            : read.code ==
                                      l2flow::arrow_hot::ReadCode::kClosed
                                  ? "Arrow benchmark ring closed early"
                                  : "Arrow benchmark reader protocol error: " +
                                        read.error;
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    std::string decode_error;
                    const std::shared_ptr<arrow::RecordBatch> batch =
                        reader->Decode(read.lease, &decode_error);
                    const auto native_sequence = batch == nullptr
                        ? std::shared_ptr<arrow::UInt64Array>{}
                        : std::dynamic_pointer_cast<arrow::UInt64Array>(
                              batch->GetColumnByName("native_sequence"));
                    const auto receive_monotonic_ns = batch == nullptr
                        ? std::shared_ptr<arrow::UInt64Array>{}
                        : std::dynamic_pointer_cast<arrow::UInt64Array>(
                              batch->GetColumnByName(
                                  "receive_monotonic_ns"));
                    if (batch == nullptr || batch->num_rows() <= 0 ||
                        native_sequence == nullptr ||
                        receive_monotonic_ns == nullptr ||
                        native_sequence->length() != batch->num_rows() ||
                        receive_monotonic_ns->length() !=
                            batch->num_rows() ||
                        read.metadata.row_count !=
                            static_cast<std::uint32_t>(batch->num_rows()) ||
                        read.metadata.feed_session_epoch !=
                            options.arrow_feed_session_epoch ||
                        static_cast<std::uint64_t>(batch->num_rows()) >
                            total_per_channel - state.rows) {
                        ++state.protocol_errors;
                        state.error = decode_error.empty()
                            ? "Arrow benchmark decoded batch is invalid"
                            : "Arrow benchmark decode failed: " + decode_error;
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    const std::uint64_t decode_complete_ns =
                        MonotonicNowNs();
                    for (std::int64_t row = 0; row < batch->num_rows();
                         ++row) {
                        const std::uint64_t expected = state.rows +
                            static_cast<std::uint64_t>(row) + 1U;
                        if (native_sequence->IsNull(row) ||
                            native_sequence->Value(row) != expected) {
                            ++state.ordering_errors;
                        }
                        if (expected > warmup_per_channel) {
                            const std::uint64_t measured_index =
                                expected - warmup_per_channel - 1U;
                            if (receive_monotonic_ns->IsNull(row) ||
                                decode_complete_ns <
                                    receive_monotonic_ns->Value(row)) {
                                ++state.clock_errors;
                            } else {
                                const std::uint64_t latency =
                                    decode_complete_ns -
                                    receive_monotonic_ns->Value(row);
                                state.maximum_latency_ns = std::max(
                                    state.maximum_latency_ns, latency);
                                if (measured_index %
                                        options.latency_sample_every ==
                                    0U) {
                                    if (state.latency_samples >=
                                        latency_samples_per_channel) {
                                        ++state.clock_errors;
                                    } else {
                                        state.latencies_ns[
                                            static_cast<std::size_t>(
                                                state.latency_samples)] =
                                            latency;
                                    }
                                    ++state.latency_samples;
                                }
                            }
                        }
                    }
                    state.rows +=
                        static_cast<std::uint64_t>(batch->num_rows());
                    ++state.batches;
                    if ((state.batches & UINT64_C(4'095)) == 0U) {
                        reader->TouchHeartbeat(MonotonicNowNs());
                    }
                }
                state.finish_ns = MonotonicNowNs();
            });
        }
    }
#endif
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
            TickDispatch dispatch{};
            TickDispatch secondary_dispatch{};
            std::uint64_t local_consumed = 0U;
            std::uint64_t expected_sequence = 1U;
            std::uint64_t measured_index = 0U;
            std::uint64_t latency_sample_index = 0U;
            std::size_t idle_spins = 0U;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            std::size_t drain_burst = 0U;
#endif
            while (local_consumed < total_per_channel &&
                   !abort.load(std::memory_order_acquire)) {
                const bool can_poll_dispatch =
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                    event_runtime == nullptr ||
                    event_runtime->CanPollDispatch(owner);
#else
                    true;
#endif
                if (!can_poll_dispatch ||
                    !engine->TryPollTickDispatch(owner, &dispatch)) {
                    ++idle_spins;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                    if (arrow_egress != nullptr) {
                        const std::uint64_t operation_start_ns =
                            MonotonicNowNs();
                        arrow_egress->FlushDue(owner, operation_start_ns);
                        const std::uint64_t operation_finish_ns =
                            MonotonicNowNs();
                        RecordMaximumElapsed(
                            operation_start_ns, operation_finish_ns,
                            &state.maximum_sink_operation_ns);
                    }
                    drain_burst = 0U;
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                    if (event_runtime != nullptr) {
                        const std::uint64_t operation_start_ns =
                            MonotonicNowNs();
                        if (!event_runtime->FlushDue(
                                owner, operation_start_ns)) {
                            state.error =
                                "Event worker flush failed: " +
                                event_runtime->fatal_error();
                            abort.store(true, std::memory_order_release);
                            break;
                        }
                        const std::uint64_t operation_finish_ns =
                            MonotonicNowNs();
                        RecordMaximumElapsed(
                            operation_start_ns, operation_finish_ns,
                            &state.maximum_sink_operation_ns);
                    }
                    if (kline_runtime != nullptr) {
                        const std::uint64_t operation_start_ns =
                            MonotonicNowNs();
                        if (!kline_runtime->FlushDue(
                                owner, operation_start_ns)) {
                            state.error =
                                "KLine worker flush failed: " +
                                kline_runtime->fatal_error();
                            abort.store(true, std::memory_order_release);
                            break;
                        }
                        const std::uint64_t operation_finish_ns =
                            MonotonicNowNs();
                        RecordMaximumElapsed(
                            operation_start_ns, operation_finish_ns,
                            &state.maximum_sink_operation_ns);
                    }
#endif
                    CpuRelax();
                    continue;
                }
                idle_spins = 0U;
                if (dispatch.kind != TickDispatchKind::kProjectOrdered) {
                    state.error =
                        "unexpected non-ordered benchmark dispatch";
                    abort.store(true, std::memory_order_release);
                    break;
                }
                for (std::size_t consumer = 1U;
                     consumer < options.tick_consumer_count; ++consumer) {
                    if (!engine->TryPollTickDispatch(
                            consumer, owner, &secondary_dispatch)) {
                        state.error =
                            "secondary outbox cursor did not mirror primary";
                        ++state.secondary_errors;
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    ++state.secondary_consumed;
                    if (secondary_dispatch.kind != dispatch.kind ||
                        secondary_dispatch.owner != dispatch.owner ||
                        secondary_dispatch.outbox_lane !=
                            dispatch.outbox_lane ||
                        secondary_dispatch.outbox_lsn != dispatch.outbox_lsn ||
                        secondary_dispatch.tick.common.native_sequence !=
                            dispatch.tick.common.native_sequence ||
                        secondary_dispatch.tick.common.ingress_sequence !=
                            dispatch.tick.common.ingress_sequence) {
                        state.error =
                            "secondary outbox cursor diverged from primary";
                        ++state.secondary_errors;
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                }
                if (abort.load(std::memory_order_acquire)) {
                    break;
                }
                const CanonicalTick& tick = dispatch.tick;
                if (tick.common.instrument_ordinal != owner ||
                    tick.common.native_sequence != expected_sequence) {
                    ++state.ordering_errors;
                }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                if (arrow_egress != nullptr) {
                    const std::uint64_t operation_start_ns =
                        MonotonicNowNs();
                    const bool appended =
                        arrow_egress->AppendTickDispatch(owner, dispatch);
                    const std::uint64_t operation_finish_ns =
                        MonotonicNowNs();
                    RecordMaximumElapsed(
                        operation_start_ns, operation_finish_ns,
                        &state.maximum_sink_operation_ns);
                    if (!appended) {
                        state.error = "Arrow benchmark append failed: " +
                            arrow_egress->fatal_error();
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                }
                ++drain_burst;
                if (arrow_egress != nullptr && drain_burst == 256U) {
                    const std::uint64_t operation_start_ns =
                        MonotonicNowNs();
                    arrow_egress->FlushDue(owner, operation_start_ns);
                    const std::uint64_t operation_finish_ns =
                        MonotonicNowNs();
                    RecordMaximumElapsed(
                        operation_start_ns, operation_finish_ns,
                        &state.maximum_sink_operation_ns);
                    drain_burst = 0U;
                }
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                if (event_runtime != nullptr) {
                    const std::uint64_t operation_start_ns =
                        MonotonicNowNs();
                    if (!event_runtime->AppendDispatch(owner, dispatch)) {
                        state.error =
                            "Event worker append failed: " +
                            event_runtime->fatal_error();
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    const std::uint64_t operation_finish_ns =
                        MonotonicNowNs();
                    RecordMaximumElapsed(
                        operation_start_ns, operation_finish_ns,
                        &state.maximum_sink_operation_ns);
                }
                if (kline_runtime != nullptr) {
                    const std::uint64_t operation_start_ns =
                        MonotonicNowNs();
                    if (!kline_runtime->AppendDispatch(owner, dispatch)) {
                        state.error =
                            "KLine worker append failed: " +
                            kline_runtime->fatal_error();
                        abort.store(true, std::memory_order_release);
                        break;
                    }
                    const std::uint64_t operation_finish_ns =
                        MonotonicNowNs();
                    RecordMaximumElapsed(
                        operation_start_ns, operation_finish_ns,
                        &state.maximum_sink_operation_ns);
                }
#endif
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
            if (event_runtime != nullptr && !event_runtime->Flush(owner)) {
                state.error = "Event worker final flush failed: " +
                    event_runtime->fatal_error();
                abort.store(true, std::memory_order_release);
            }
            if (kline_runtime != nullptr && !kline_runtime->Flush(owner)) {
                state.error = "KLine worker final flush failed: " +
                    kline_runtime->fatal_error();
                abort.store(true, std::memory_order_release);
            }
            state.measured = measured_index;
            state.latency_samples = latency_sample_index;
            state.finish_ns = MonotonicNowNs();
            state.consumed.store(
                local_consumed, std::memory_order_release);
        });
    }
    std::size_t ready_target = options.instrument_owners;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled) {
        ready_target += options.instrument_owners;
    }
#endif
    while (ready.load(std::memory_order_acquire) <
           ready_target) {
        std::this_thread::yield();
    }
    if (abort.load(std::memory_order_acquire)) {
        engine->Stop();
        for (std::thread& thread : consumer_threads) {
            thread.join();
        }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        if (arrow_egress != nullptr) {
            arrow_egress->Seal(MonotonicNowNs());
        }
        for (std::thread& thread : arrow_reader_threads) {
            thread.join();
        }
#endif
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            if (!consumers[owner].error.empty()) {
                std::cerr << consumers[owner].error << '\n';
            }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            if (arrow_enabled &&
                !arrow_reader_states[owner].error.empty()) {
                std::cerr << arrow_reader_states[owner].error << '\n';
            }
#endif
        }
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (event_runtime != nullptr) {
            static_cast<void>(event_runtime->FlushAll());
        }
        if (kline_runtime != nullptr) {
            static_cast<void>(kline_runtime->FlushAll());
        }
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            if (!clickhouse_raw->Stop(&stop_error)) {
                std::cerr << "ClickHouse raw sink stop failed: "
                          << stop_error << '\n';
            }
        }
        if (event_runtime != nullptr) {
            static_cast<void>(event_runtime->DrainAll());
        }
        if (kline_runtime != nullptr) {
            static_cast<void>(kline_runtime->DrainAll());
        }
        if (clickhouse_event != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_event->Stop(&stop_error));
        }
        if (clickhouse_kline != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_kline->Stop(&stop_error));
        }
#endif
        return 1;
    }

    std::array<std::byte, kMdlHeaderBytes> header =
        MakeHeader(kBenchmarkStream, bodies.front().size());
    const std::uint64_t schedule_origin_ns =
        MonotonicNowNs() + UINT64_C(100'000'000);
    std::uint64_t first_measured_callback_ns = 0U;
    std::uint64_t producer_finish_ns = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    AdmissionResult admission_failure = AdmissionResult::kAccepted;
    std::uint64_t admitted_count = 0U;
    for (std::uint64_t ordinal = 0U;
         ordinal < total_count &&
         !abort.load(std::memory_order_acquire);
         ++ordinal) {
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
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    l2flow::clickhouse::RawClickHouseStats
        clickhouse_measurement_end_stats{};
    l2flow::clickhouse::EventClickHouseStats
        event_measurement_event_stats{};
    l2flow::clickhouse::KLineClickHouseStats
        kline_measurement_sink_stats{};
    if (clickhouse_raw != nullptr) {
        clickhouse_measurement_end_stats = clickhouse_raw->stats();
    }
    if (clickhouse_event != nullptr) {
        event_measurement_event_stats = clickhouse_event->stats();
    }
    if (clickhouse_kline != nullptr) {
        kline_measurement_sink_stats = clickhouse_kline->stats();
    }
#endif

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
            if (abort.load(std::memory_order_acquire) ||
                !engine->healthy() || progress_stats.gaps_skipped != 0U ||
                progress_stats.hole_fills_dispatched != 0U ||
                progress_stats.rejected_late_facts != 0U ||
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
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    bool clickhouse_stop_ok = true;
    std::string clickhouse_stop_error;
    std::uint64_t clickhouse_stop_start_ns = 0U;
    std::uint64_t clickhouse_stop_finish_ns = 0U;
    l2flow::clickhouse::RawClickHouseStats clickhouse_final_stats{};
    bool event_flush_ok = true;
    bool event_drain_ok = true;
    bool event_stop_ok = true;
    std::string event_stop_error;
    std::uint64_t event_stop_start_ns = 0U;
    std::uint64_t event_stop_finish_ns = 0U;
    l2flow::event::EventRuntimeStats event_final_runtime_stats{};
    l2flow::clickhouse::EventClickHouseStats event_final_stats{};
    const bool event_runtime_created = event_runtime != nullptr;
    bool event_runtime_healthy = true;
    std::string event_runtime_fatal;
    bool kline_flush_ok = true;
    bool kline_drain_ok = true;
    bool kline_stop_ok = true;
    std::string kline_stop_error;
    std::uint64_t kline_stop_start_ns = 0U;
    std::uint64_t kline_stop_finish_ns = 0U;
    l2flow::kline::KLineRuntimeStats kline_final_runtime_stats{};
    l2flow::clickhouse::KLineClickHouseStats kline_final_stats{};
    const bool kline_runtime_created = kline_runtime != nullptr;
    bool kline_runtime_healthy = true;
    std::string kline_runtime_fatal;
    bool event_journal_flush_ok = true;
    std::uint64_t event_journal_flush_start_ns = 0U;
    std::uint64_t event_journal_flush_finish_ns = 0U;
    l2flow::journal::FactJournalStats event_journal_stats{};
    bool event_journal_healthy = true;
    std::string event_journal_fatal;
    std::uintmax_t event_journal_observed_file_bytes = 0U;
    std::error_code event_journal_file_size_error;
    bool event_journal_cleanup_ok = true;
    std::error_code event_journal_cleanup_error;
    if (event_runtime != nullptr) {
        // Submit pending Event revisions independently of raw persistence.
        event_flush_ok = event_runtime->FlushAll();
    }
    if (kline_runtime != nullptr) {
        kline_flush_ok = kline_runtime->FlushAll();
    }
    if (clickhouse_raw != nullptr) {
        clickhouse_stop_start_ns = MonotonicNowNs();
        clickhouse_stop_ok =
            clickhouse_raw->Stop(&clickhouse_stop_error);
        clickhouse_stop_finish_ns = MonotonicNowNs();
        clickhouse_final_stats = clickhouse_raw->stats();
    }
    if (event_runtime != nullptr) {
        event_drain_ok = event_runtime->DrainAll();
        event_final_runtime_stats = event_runtime->stats();
    }
    if (kline_runtime != nullptr) {
        kline_drain_ok = kline_runtime->DrainAll();
        kline_final_runtime_stats = kline_runtime->stats();
    }
    if (clickhouse_event != nullptr) {
        event_stop_start_ns = MonotonicNowNs();
        event_stop_ok = clickhouse_event->Stop(&event_stop_error);
        event_stop_finish_ns = MonotonicNowNs();
        event_final_stats = clickhouse_event->stats();
    }
    if (clickhouse_kline != nullptr) {
        kline_stop_start_ns = MonotonicNowNs();
        kline_stop_ok = clickhouse_kline->Stop(&kline_stop_error);
        kline_stop_finish_ns = MonotonicNowNs();
        kline_final_stats = clickhouse_kline->stats();
    }
    if (event_runtime != nullptr) {
        event_runtime_healthy = event_runtime->healthy();
        event_runtime_fatal = event_runtime->fatal_error();
        event_runtime.reset();
    }
    if (kline_runtime != nullptr) {
        kline_runtime_healthy = kline_runtime->healthy();
        kline_runtime_fatal = kline_runtime->fatal_error();
        kline_runtime.reset();
    }
    if (fact_journal != nullptr) {
        event_journal_flush_start_ns = MonotonicNowNs();
        event_journal_flush_ok = fact_journal->Flush();
        event_journal_flush_finish_ns = MonotonicNowNs();
        event_journal_stats = fact_journal->stats();
        event_journal_healthy = fact_journal->healthy();
        event_journal_fatal = fact_journal->fatal_error();
        event_journal_observed_file_bytes = std::filesystem::file_size(
            fact_journal_path, event_journal_file_size_error);
        fact_journal.reset();
        event_journal_cleanup_ok = fact_journal_cleanup != nullptr &&
            fact_journal_cleanup->Remove(&event_journal_cleanup_error);
    } else if (options.event_enabled || options.kline_enabled) {
        event_journal_flush_ok = false;
        event_journal_healthy = false;
        event_journal_cleanup_ok = false;
    }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr) {
        arrow_egress->Seal(MonotonicNowNs());
    }
    for (std::thread& thread : arrow_reader_threads) {
        thread.join();
    }
#endif

    std::uint64_t total_consumed = 0U;
    std::uint64_t total_measured = 0U;
    std::uint64_t total_secondary_consumed = 0U;
    std::uint64_t secondary_errors = 0U;
    std::uint64_t ordering_errors = 0U;
    std::uint64_t clock_errors = 0U;
    std::uint64_t maximum_latency_ns = 0U;
    std::uint64_t maximum_sink_operation_ns = 0U;
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
        total_secondary_consumed += state.secondary_consumed;
        secondary_errors += state.secondary_errors;
        maximum_latency_ns = std::max(
            maximum_latency_ns, state.maximum_latency_ns);
        maximum_sink_operation_ns = std::max(
            maximum_sink_operation_ns,
            state.maximum_sink_operation_ns);
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

#if defined(L2FLOW_CH_HAS_ARROW_RING)
    l2flow::arrow_hot::ArrowHotEgressStats arrow_stats{};
    std::uint64_t arrow_rows_read = 0U;
    std::uint64_t arrow_batches_read = 0U;
    std::uint64_t arrow_clock_errors = 0U;
    std::uint64_t arrow_maximum_latency_ns = 0U;
    std::uint64_t arrow_ordering_errors = 0U;
    std::uint64_t arrow_protocol_errors = 0U;
    std::uint64_t arrow_finish_ns = 0U;
    bool arrow_reader_bindings_valid = true;
    std::vector<std::uint64_t> arrow_latencies;
    arrow_latencies.reserve(
        static_cast<std::size_t>(expected_latency_samples));
    if (arrow_egress != nullptr) {
        arrow_stats = arrow_egress->stats();
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            const ArrowReaderState& state = arrow_reader_states[owner];
            arrow_rows_read += state.rows;
            arrow_batches_read += state.batches;
            arrow_clock_errors += state.clock_errors;
            arrow_maximum_latency_ns = std::max(
                arrow_maximum_latency_ns, state.maximum_latency_ns);
            arrow_ordering_errors += state.ordering_errors;
            arrow_protocol_errors += state.protocol_errors;
            arrow_finish_ns = std::max(arrow_finish_ns, state.finish_ns);
            arrow_reader_bindings_valid = arrow_reader_bindings_valid &&
                state.observed_cpu ==
                    options.first_arrow_reader_cpu +
                        static_cast<int>(owner);
            arrow_latencies.insert(
                arrow_latencies.end(), state.latencies_ns.begin(),
                state.latencies_ns.begin() +
                    static_cast<std::ptrdiff_t>(
                        std::min(state.latency_samples,
                                 latency_samples_per_channel)));
        }
        std::sort(arrow_latencies.begin(), arrow_latencies.end());
    }
#endif

    std::uint64_t gap_controls = 0U;
    std::uint64_t fault_controls = 0U;
    ChannelGap gap{};
    ChannelFault fault{};
    while (engine->TryPollGap(&gap)) {
        ++gap_controls;
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
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    const double clickhouse_measurement_seconds =
        !options.clickhouse_enabled || producer_finish_ns <= schedule_origin_ns
            ? 0.0
            : static_cast<double>(producer_finish_ns - schedule_origin_ns) /
                  1'000'000'000.0;
    const double clickhouse_final_seconds =
        !options.clickhouse_enabled ||
                clickhouse_stop_finish_ns <= schedule_origin_ns
            ? 0.0
            : static_cast<double>(clickhouse_stop_finish_ns -
                                  schedule_origin_ns) /
                  1'000'000'000.0;
    const double clickhouse_measurement_ack_rate =
        clickhouse_measurement_seconds == 0.0
            ? 0.0
            : static_cast<double>(
                  clickhouse_measurement_end_stats.rows_acked) /
                  clickhouse_measurement_seconds;
    const double clickhouse_final_ack_rate =
        clickhouse_final_seconds == 0.0
            ? 0.0
            : static_cast<double>(clickhouse_final_stats.rows_acked) /
                  clickhouse_final_seconds;
    const std::uint64_t clickhouse_measurement_ingress_lag =
        clickhouse_measurement_end_stats.tick_rows_received >= total_count
            ? 0U
            : total_count -
                  clickhouse_measurement_end_stats.tick_rows_received;
    const std::uint64_t clickhouse_measurement_ack_lag =
        clickhouse_measurement_end_stats.rows_acked >= total_count
            ? 0U
            : total_count - clickhouse_measurement_end_stats.rows_acked;
    const std::uint64_t clickhouse_stop_duration_ns =
        clickhouse_stop_finish_ns >= clickhouse_stop_start_ns
            ? clickhouse_stop_finish_ns - clickhouse_stop_start_ns
            : 0U;
    const std::uint64_t clickhouse_total_ack_tail_ns =
        clickhouse_stop_finish_ns >= producer_finish_ns
            ? clickhouse_stop_finish_ns - producer_finish_ns
            : 0U;
    bool clickhouse_valid = true;
    if (options.clickhouse_enabled) {
        clickhouse_valid = clickhouse_stop_ok &&
            clickhouse_final_stats.tick_rows_received == total_count &&
            clickhouse_final_stats.snapshot_rows_received == 0U &&
            clickhouse_final_stats.rows_acked == total_count &&
            clickhouse_final_stats.batches_queued ==
                clickhouse_final_stats.batches_acked &&
            clickhouse_final_stats.batches_acked ==
                clickhouse_final_stats.batches_released &&
            clickhouse_final_stats.retry_attempts == 0U &&
            clickhouse_final_stats.unknown_outcomes == 0U &&
            clickhouse_final_stats.unacked_batches == 0U &&
            clickhouse_raw->healthy() &&
            clickhouse_final_ack_rate >= minimum_pass_rate;
    }
    const std::uint64_t event_processing_finish_ns = dispatch_finish_ns;
    const double event_processing_seconds =
        !options.event_enabled || first_measured_callback_ns == 0U ||
                event_processing_finish_ns <= first_measured_callback_ns
            ? 0.0
            : static_cast<double>(event_processing_finish_ns -
                                  first_measured_callback_ns) /
                  1'000'000'000.0;
    const std::uint64_t event_measured_facts =
        event_final_runtime_stats.workers.facts_journaled > warmup_count
            ? event_final_runtime_stats.workers.facts_journaled - warmup_count
            : 0U;
    const double event_worker_rate =
        event_processing_seconds == 0.0
            ? 0.0
            : static_cast<double>(event_measured_facts) /
                  event_processing_seconds;
    const double event_sink_seconds =
        !options.event_enabled || event_stop_finish_ns <= schedule_origin_ns
            ? 0.0
            : static_cast<double>(event_stop_finish_ns - schedule_origin_ns) /
                  1'000'000'000.0;
    const double event_revision_ack_rate =
        event_sink_seconds == 0.0
            ? 0.0
            : static_cast<double>(event_final_stats.revision_rows_acked) /
                  event_sink_seconds;
    const std::uint64_t event_measurement_rows_acked =
        event_measurement_event_stats.revision_rows_acked;
    const double event_measurement_ack_rate =
        clickhouse_measurement_seconds == 0.0
            ? 0.0
            : static_cast<double>(event_measurement_rows_acked) /
                  clickhouse_measurement_seconds;
    const std::uint64_t event_revision_lag_rows =
        event_measurement_event_stats.revision_rows_queued >=
                event_measurement_event_stats.revision_rows_acked
            ? event_measurement_event_stats.revision_rows_queued -
                  event_measurement_event_stats.revision_rows_acked
            : 0U;
    const std::uint64_t expected_event_journal_record_bytes = total_count *
        static_cast<std::uint64_t>(
            l2flow::journal::kFactJournalRecordBytes);
    const std::uint64_t expected_event_journal_file_bytes =
        expected_event_journal_record_bytes +
        l2flow::journal::kFactJournalFileHeaderBytes;
    const std::uint64_t derived_consumer_count =
        static_cast<std::uint64_t>(options.event_enabled) +
        static_cast<std::uint64_t>(options.kline_enabled);
    const std::uint64_t expected_consumer_new =
        total_count * derived_consumer_count;
    bool fact_journal_valid = true;
    if (derived_consumer_count != 0U) {
        fact_journal_valid = fact_journal_shared_by_all_owners &&
            event_journal_flush_ok && event_journal_healthy &&
            event_journal_cleanup_ok && !event_journal_file_size_error &&
            event_journal_stats.records == total_count &&
            event_journal_stats.record_bytes ==
                expected_event_journal_record_bytes &&
            event_journal_stats.file_bytes ==
                expected_event_journal_file_bytes &&
            event_journal_stats.write_bytes ==
                expected_event_journal_file_bytes &&
            event_journal_observed_file_bytes ==
                expected_event_journal_file_bytes &&
            event_journal_stats.consumer_new == expected_consumer_new &&
            event_journal_stats.duplicates == 0U &&
            event_journal_stats.conflicts == 0U &&
            event_journal_stats.partial_writes == 0U &&
            event_journal_stats.partial_reads == 0U &&
            event_journal_stats.active_writes == 0U &&
            event_journal_stats.active_reads == 0U &&
            event_journal_stats.reserved_records == 0U &&
            event_journal_stats.waiting_admissions == 0U &&
            event_journal_stats.errors == 0U &&
            event_journal_stats.flush_calls >= 1U;
    }
    bool event_valid = true;
    if (options.event_enabled) {
        event_valid = event_flush_ok && event_drain_ok && event_stop_ok &&
            event_runtime_created && event_runtime_healthy &&
            clickhouse_event != nullptr && clickhouse_event->healthy() &&
            fact_journal_valid &&
            event_final_runtime_stats.ordered_dispositions_received ==
                total_count &&
            event_final_runtime_stats.hole_fill_dispositions_received == 0U &&
            event_final_runtime_stats.rejected_dispositions_received == 0U &&
            event_final_runtime_stats.workers.facts_journaled == total_count &&
            event_final_runtime_stats.invalid_inputs == 0U &&
            event_final_runtime_stats.workers.pending_revision_batches ==
                0U &&
            event_final_stats.revision_rows_acked ==
                event_final_runtime_stats.workers.revisions_created &&
            event_final_stats.revision_batches_queued ==
                event_final_stats.revision_batches_acked &&
            event_final_stats.revision_batches_acked ==
                event_final_stats.revision_batches_released &&
            event_final_stats.revision_rows_queued ==
                event_final_stats.revision_rows_acked &&
            event_final_stats.retry_attempts == 0U &&
            event_final_stats.unknown_outcomes == 0U &&
            event_revision_ack_rate >= minimum_pass_rate;
    }
    const double kline_processing_seconds =
        !options.kline_enabled || first_measured_callback_ns == 0U ||
                dispatch_finish_ns <= first_measured_callback_ns
            ? 0.0
            : static_cast<double>(dispatch_finish_ns -
                                  first_measured_callback_ns) /
                  1'000'000'000.0;
    const std::uint64_t kline_measured_facts =
        kline_final_runtime_stats.workers.facts_journaled > warmup_count
            ? kline_final_runtime_stats.workers.facts_journaled - warmup_count
            : 0U;
    const double kline_worker_rate = kline_processing_seconds == 0.0
        ? 0.0
        : static_cast<double>(kline_measured_facts) /
              kline_processing_seconds;
    const double kline_sink_seconds =
        !options.kline_enabled || kline_stop_finish_ns <= schedule_origin_ns
            ? 0.0
            : static_cast<double>(kline_stop_finish_ns - schedule_origin_ns) /
                  1'000'000'000.0;
    const double kline_revision_ack_rate = kline_sink_seconds == 0.0
        ? 0.0
        : static_cast<double>(kline_final_stats.revision_rows_acked) /
              kline_sink_seconds;
    const double kline_measurement_ack_rate =
        clickhouse_measurement_seconds == 0.0
            ? 0.0
            : static_cast<double>(
                  kline_measurement_sink_stats.revision_rows_acked) /
                  clickhouse_measurement_seconds;
    const std::uint64_t kline_revision_lag_rows =
        kline_measurement_sink_stats.revision_rows_queued >=
                kline_measurement_sink_stats.revision_rows_acked
            ? kline_measurement_sink_stats.revision_rows_queued -
                  kline_measurement_sink_stats.revision_rows_acked
            : 0U;
    bool kline_valid = true;
    if (options.kline_enabled) {
        kline_valid = kline_flush_ok && kline_drain_ok && kline_stop_ok &&
            kline_runtime_created && kline_runtime_healthy &&
            clickhouse_kline != nullptr && clickhouse_kline->healthy() &&
            fact_journal_valid &&
            kline_final_runtime_stats.ordered_dispositions_received ==
                total_count &&
            kline_final_runtime_stats.hole_fill_dispositions_received == 0U &&
            kline_final_runtime_stats.rejected_dispositions_received == 0U &&
            kline_final_runtime_stats.workers.facts_journaled == total_count &&
            kline_final_runtime_stats.workers.trades_projected == total_count &&
            kline_final_runtime_stats.invalid_inputs == 0U &&
            kline_final_runtime_stats.workers.invalid_facts == 0U &&
            kline_final_runtime_stats.workers
                    .invalid_trade_exchange_times == 0U &&
            kline_final_runtime_stats.workers.pending_revision_batches ==
                0U &&
            kline_final_stats.revision_rows_acked ==
                kline_final_runtime_stats.workers.revisions_created &&
            kline_final_stats.revision_batches_queued ==
                kline_final_stats.revision_batches_acked &&
            kline_final_stats.revision_batches_acked ==
                kline_final_stats.revision_batches_released &&
            kline_final_stats.revision_rows_queued ==
                kline_final_stats.revision_rows_acked &&
            kline_final_stats.queued_revision_batches == 0U &&
            kline_final_stats.queued_revision_rows == 0U &&
            kline_final_stats.retry_attempts == 0U &&
            kline_final_stats.unknown_outcomes == 0U;
    }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    const double arrow_read_seconds =
        !arrow_enabled || first_measured_callback_ns == 0U ||
                arrow_finish_ns <= first_measured_callback_ns
            ? 0.0
            : static_cast<double>(arrow_finish_ns -
                                  first_measured_callback_ns) /
                  1'000'000'000.0;
    const double arrow_read_rate = arrow_read_seconds == 0.0
        ? 0.0
        : static_cast<double>(measured_count) / arrow_read_seconds;
    bool arrow_valid = true;
    if (arrow_enabled) {
        const std::uint64_t expected_published_rows =
            arrow_stats.tick_rows_received +
            arrow_stats.snapshot_rows_received +
            arrow_stats.control_rows_received;
        arrow_valid =
            arrow_stats.tick_rows_received == total_count &&
            arrow_stats.snapshot_rows_received == 0U &&
            arrow_stats.hole_fill_rows_received == 0U &&
            arrow_stats.published_rows == expected_published_rows &&
            arrow_stats.no_segment_dropped_rows == 0U &&
            arrow_stats.oversized_dropped_rows == 0U &&
            arrow_stats.control_publish_dropped_rows == 0U &&
            arrow_stats.internal_errors == 0U &&
            arrow_rows_read == total_count &&
            arrow_latencies.size() ==
                static_cast<std::size_t>(expected_latency_samples) &&
            arrow_clock_errors == 0U &&
            arrow_ordering_errors == 0U &&
            arrow_protocol_errors == 0U &&
            arrow_reader_bindings_valid && arrow_egress->healthy() &&
            arrow_read_rate >= minimum_pass_rate;
    }
#endif
    const bool base_valid =
        admission_failure == AdmissionResult::kAccepted &&
        admitted_count == total_count && total_consumed == total_count &&
        total_secondary_consumed == expected_secondary_consumed &&
        secondary_errors == 0U &&
        total_measured == measured_count &&
        latencies.size() ==
            static_cast<std::size_t>(expected_latency_samples) &&
        ordering_errors == 0U && clock_errors == 0U &&
        stats.lane_full == 0U && stats.decode_errors == 0U &&
        stats.dispatch_overflows == 0U && stats.gaps_skipped == 0U &&
        stats.hole_fills_dispatched == 0U &&
        stats.rejected_late_facts == 0U &&
        stats.from_open_channels_frozen == 0U && gap_controls == 0U &&
        fault_controls == 0U &&
        engine->healthy() && !handler.failed() && cpu_binding_valid &&
        producer_rate >= minimum_pass_rate &&
        dispatch_rate >= minimum_pass_rate;
    bool valid = base_valid;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    valid = valid && arrow_valid;
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    valid = valid && clickhouse_valid;
    if (options.event_enabled) {
        valid = valid && event_valid && event_worker_rate >= minimum_pass_rate;
    }
    if (options.kline_enabled) {
        valid = valid && kline_valid && kline_worker_rate >= minimum_pass_rate;
    }
#endif

    const auto to_us = [](std::uint64_t nanoseconds) {
        return static_cast<double>(nanoseconds) / 1'000.0;
    };
    std::string_view latency_name = "callback_entry_to_dispatch_us";
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled) {
        latency_name = "callback_entry_to_arrow_append_us";
    }
#endif
    std::cout << std::fixed << std::setprecision(3)
              << "config target_msg_s=" << options.target_rate
              << " measured_messages=" << measured_count
              << " warmup_messages=" << warmup_count
              << " channels=" << options.channels
              << " tick_lanes=" << options.tick_lanes
              << " owners=" << options.instrument_owners
              << " dispatch_queue_capacity="
              << options.dispatch_queue_capacity
              << " tick_consumers=" << options.tick_consumer_count
              << " pattern=" << PatternName(options.pattern)
              << " reorder_window=" << options.reorder_window
              << " sequence_entries_per_channel="
              << config.reorder_entries_per_channel
              << " maximum_reorder_span=" << config.maximum_reorder_span
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
              << latency_name << " samples="
              << latencies.size()
              << " p50=" << to_us(Percentile(latencies, 0.50L))
              << " p90=" << to_us(Percentile(latencies, 0.90L))
              << " p99=" << to_us(Percentile(latencies, 0.99L))
              << " p999=" << to_us(Percentile(latencies, 0.999L))
              << " max_all=" << to_us(maximum_latency_ns) << '\n'
              << "quality admitted=" << stats.admitted
              << " consumed=" << total_consumed
              << " secondary_consumed=" << total_secondary_consumed
              << " secondary_errors=" << secondary_errors
              << " measured_dispatched=" << total_measured
              << " ordering_errors=" << ordering_errors
              << " lane_full=" << stats.lane_full
              << " decode_errors=" << stats.decode_errors
              << " dispatch_overflows=" << stats.dispatch_overflows
              << " gaps=" << stats.gaps_skipped
              << " hole_fills=" << stats.hole_fills_dispatched
              << " rejected_late_facts=" << stats.rejected_late_facts
              << " channel_faults=" << stats.from_open_channels_frozen
              << " healthy=" << (engine->healthy() ? "true" : "false")
              << '\n';
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled) {
        std::cout
            << "arrow_config root="
            << options.arrow_ring_directory.string()
            << " epoch=" << options.arrow_feed_session_epoch
            << " descriptors=" << options.arrow_descriptor_capacity
            << " segments=" << options.arrow_segment_count
            << " tick_segment_bytes="
            << options.arrow_tick_segment_payload_bytes
            << " tick_batch_rows=" << options.arrow_tick_batch_rows
            << " max_delay_ns=" << options.arrow_batch_max_delay_ns
            << " reader_cpus=" << options.first_arrow_reader_cpu << '-'
            << options.first_arrow_reader_cpu +
                   static_cast<int>(options.instrument_owners) - 1
            << " observed_bindings_valid="
            << (arrow_reader_bindings_valid ? "true" : "false") << '\n'
            << "arrow_throughput read_msg_s=" << arrow_read_rate
            << " read_seconds=" << arrow_read_seconds
            << " published_batches=" << arrow_stats.published_batches
            << " reader_batches=" << arrow_batches_read
            << " max_sink_operation_us="
            << to_us(maximum_sink_operation_ns) << '\n'
            << "callback_entry_to_arrow_reader_us samples="
            << arrow_latencies.size()
            << " p50=" << to_us(Percentile(arrow_latencies, 0.50L))
            << " p90=" << to_us(Percentile(arrow_latencies, 0.90L))
            << " p99=" << to_us(Percentile(arrow_latencies, 0.99L))
            << " p999=" << to_us(Percentile(arrow_latencies, 0.999L))
            << " max_all=" << to_us(arrow_maximum_latency_ns) << '\n'
            << "arrow_quality tick_in=" << arrow_stats.tick_rows_received
            << " published_rows=" << arrow_stats.published_rows
            << " control_rows=" << arrow_stats.control_rows_received
            << " rows_read=" << arrow_rows_read
            << " ordering_errors=" << arrow_ordering_errors
            << " clock_errors=" << arrow_clock_errors
            << " protocol_errors=" << arrow_protocol_errors
            << " no_segment_dropped_rows="
            << arrow_stats.no_segment_dropped_rows
            << " oversized_dropped_rows="
            << arrow_stats.oversized_dropped_rows
            << " control_dropped_rows="
            << arrow_stats.control_publish_dropped_rows
            << " internal_errors=" << arrow_stats.internal_errors
            << " healthy="
            << (arrow_egress->healthy() ? "true" : "false") << '\n';
    }
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (options.clickhouse_enabled) {
        const std::string writer_instance =
            l2flow::clickhouse::IdentifierString(
                clickhouse_raw->writer_instance_id());
        std::cout
            << "clickhouse_config endpoint=" << options.clickhouse.endpoint
            << " database=" << options.clickhouse.database
            << " feed_epoch=" << options.clickhouse.feed_session_epoch
            << " writers=" << options.clickhouse.writer_threads
            << " tick_batch_rows=" << options.clickhouse.tick_batch_rows
            << " tick_batch_bytes=" << options.clickhouse.tick_batch_bytes
            << " tick_batch_max_delay_ns="
            << options.clickhouse.tick_batch_max_delay_ns
            << " queue_batches_per_lane="
            << options.clickhouse.tick_queue_batches_per_lane
            << " writer_instance=" << writer_instance
            << " query_id_prefix=l2flow/raw_tick/" << writer_instance
            << "/\n"
            << "clickhouse_measurement_end tick_in="
            << clickhouse_measurement_end_stats.tick_rows_received
            << " rows_acked="
            << clickhouse_measurement_end_stats.rows_acked
            << " ingress_lag_rows="
            << clickhouse_measurement_ingress_lag
            << " ack_lag_rows=" << clickhouse_measurement_ack_lag
            << " batches_queued="
            << clickhouse_measurement_end_stats.batches_queued
            << " batches_acked="
            << clickhouse_measurement_end_stats.batches_acked
            << " unacked_batches="
            << clickhouse_measurement_end_stats.unacked_batches << '\n'
            << "clickhouse_throughput measurement_ack_msg_s="
            << clickhouse_measurement_ack_rate
            << " effective_final_ack_msg_s="
            << clickhouse_final_ack_rate
            << " measurement_seconds=" << clickhouse_measurement_seconds
            << " final_seconds=" << clickhouse_final_seconds
            << " stop_drain_ms="
            << to_us(clickhouse_stop_duration_ns) / 1'000.0
            << " producer_to_final_ack_tail_ms="
            << to_us(clickhouse_total_ack_tail_ns) / 1'000.0 << '\n'
            << "clickhouse_final tick_in="
            << clickhouse_final_stats.tick_rows_received
            << " snapshot_in="
            << clickhouse_final_stats.snapshot_rows_received
            << " rows_acked=" << clickhouse_final_stats.rows_acked
            << " batches_queued="
            << clickhouse_final_stats.batches_queued
            << " batches_acked="
            << clickhouse_final_stats.batches_acked
            << " batches_released="
            << clickhouse_final_stats.batches_released
            << " retries=" << clickhouse_final_stats.retry_attempts
            << " unknown_outcomes="
            << clickhouse_final_stats.unknown_outcomes
            << " bytes_sent=" << clickhouse_final_stats.bytes_sent
            << " unacked_batches="
            << clickhouse_final_stats.unacked_batches
            << " preallocated_canonical_bytes="
            << clickhouse_final_stats.preallocated_canonical_bytes
            << " healthy="
            << (clickhouse_raw->healthy() ? "true" : "false") << '\n';
    }
    if (options.event_enabled) {
        std::cout
            << "event_config enabled=true micro_batch_rows="
            << options.event.micro_batch_rows
            << " micro_batch_max_delay_ns="
            << options.event.micro_batch_max_delay_ns
            << " insert_request_max_rows="
            << options.clickhouse_event.insert_request_max_rows
            << " insert_request_max_bytes="
            << options.clickhouse_event.insert_request_max_bytes
            << " physical_group_max_batches="
            << options.clickhouse_event.physical_group_max_batches
            << " physical_group_max_delay_ns="
            << options.clickhouse_event.physical_group_max_delay_ns
            << " writer_lanes="
            << options.clickhouse_event.writer_lanes
            << " calculation_run_id="
            << l2flow::clickhouse::IdentifierString(
                   options.event.worker.calculation_run_id) << '\n'
            << "event_throughput worker_facts_msg_s="
            << event_worker_rate
            << " event_revision_rows_ack_msg_s="
            << event_revision_ack_rate
            << " measurement_revision_ack_msg_s="
            << event_measurement_ack_rate
            << " processing_seconds=" << event_processing_seconds
            << " sink_seconds=" << event_sink_seconds
            << " revision_rows_per_callback="
            << (event_final_runtime_stats.workers.facts_journaled == 0U
                    ? 0.0
                    : static_cast<double>(
                          event_final_runtime_stats.workers.revisions_created) /
                          static_cast<double>(
                              event_final_runtime_stats.workers.facts_journaled))
            << '\n'
            << "event_final ordered_dispositions="
            << event_final_runtime_stats.ordered_dispositions_received
            << " facts_journaled="
            << event_final_runtime_stats.workers.facts_journaled
            << " revisions_created="
            << event_final_runtime_stats.workers.revisions_created
            << " micro_batches="
            << event_final_runtime_stats.micro_batches_applied
            << " facts_in_micro_batches="
            << event_final_runtime_stats.facts_in_micro_batches
            << " facts_per_micro_batch="
            << (event_final_runtime_stats.micro_batches_applied == 0U
                    ? 0.0
                    : static_cast<double>(
                          event_final_runtime_stats.facts_in_micro_batches) /
                          static_cast<double>(
                              event_final_runtime_stats
                                  .micro_batches_applied))
            << " micro_batch_rows_max="
            << event_final_runtime_stats.micro_batch_rows_max
            << " row_limit_flushes="
            << event_final_runtime_stats.row_limit_flushes
            << " timer_flushes="
            << event_final_runtime_stats.timer_flushes
            << " forced_active_flushes="
            << event_final_runtime_stats.forced_active_flushes
            << " empty_control_flushes="
            << event_final_runtime_stats.empty_control_flushes
            << " channel_seals_applied="
            << event_final_runtime_stats.channel_seals_applied
            << " channel_seals_coalesced="
            << event_final_runtime_stats.channel_seals_coalesced
            << " pending_channel_seals="
            << event_final_runtime_stats.pending_channel_seals
            << " pending_channel_seals_hwm="
            << event_final_runtime_stats.pending_channel_seals_high_water
            << " persistence_groups="
            << event_final_runtime_stats.workers
                   .persistence_groups_submitted
            << " persistence_group_batches_max="
            << event_final_runtime_stats.workers
                   .persistence_group_batches_max
            << " persistence_group_rows_max="
            << event_final_runtime_stats.workers.persistence_group_rows_max
            << " pending_revision_batches="
            << event_final_runtime_stats.workers.pending_revision_batches
            << " invalid_inputs="
            << event_final_runtime_stats.invalid_inputs
            << " source_conflicts="
            << event_final_runtime_stats.source_conflicts
            << " ordered_batch_fast_path="
            << event_final_runtime_stats.workers.ordered_batch_fast_path
            << " unordered_batch_sorts="
            << event_final_runtime_stats.workers.unordered_batch_sorts
            << " barrier_index_orders_visited="
            << event_final_runtime_stats.workers.barrier_index_orders_visited
            << " source_only_fast_path="
            << event_final_runtime_stats.workers.source_only_fast_path
            << '\n'
            << "clickhouse_event_final revision_rows_queued="
            << event_final_stats.revision_rows_queued
            << " revision_rows_acked="
            << event_final_stats.revision_rows_acked
            << " revision_batches_queued="
            << event_final_stats.revision_batches_queued
            << " revision_batches_acked="
            << event_final_stats.revision_batches_acked
            << " revision_batches_released="
            << event_final_stats.revision_batches_released
            << " submission_groups_queued="
            << event_final_stats.submission_groups_queued
            << " submission_groups_released="
            << event_final_stats.submission_groups_released
            << " sink_batches_per_second="
            << (event_processing_seconds == 0.0
                    ? 0.0
                    : static_cast<double>(
                          event_final_stats.submission_groups_queued) /
                          event_processing_seconds)
            << " revisions_per_sink_batch="
            << (event_final_stats.submission_groups_queued == 0U
                    ? 0.0
                    : static_cast<double>(
                          event_final_stats.revision_rows_queued) /
                          static_cast<double>(
                              event_final_stats.submission_groups_queued))
            << " revisions_per_insert="
            << (event_final_stats.revision_insert_requests_acked == 0U
                    ? 0.0
                    : static_cast<double>(
                          event_final_stats.revision_rows_acked) /
                          static_cast<double>(
                              event_final_stats
                                  .revision_insert_requests_acked))
            << " physical_groups="
            << event_final_stats.physical_groups_committed
            << " revision_insert_requests="
            << event_final_stats.revision_insert_requests_acked
            << " marker_insert_requests="
            << event_final_stats.marker_insert_requests_acked
            << " recovery_runs_committed="
            << event_final_stats.recovery_runs_committed
            << " retries=" << event_final_stats.retry_attempts
            << " unknown_outcomes=" << event_final_stats.unknown_outcomes
            << " queued_rows_at_measurement_end=" << event_revision_lag_rows
            << " queued_submission_groups_hwm="
            << event_final_stats.queued_submission_groups_high_water
            << " queued_logical_batches_hwm="
            << event_final_stats.queued_revision_batches_high_water
            << " queued_rows_hwm="
            << event_final_stats.queued_revision_rows_high_water
            << " bytes_sent=" << event_final_stats.bytes_sent
            << " stop_drain_ms="
            << to_us(event_stop_finish_ns >= event_stop_start_ns
                         ? event_stop_finish_ns - event_stop_start_ns
                         : 0U) /
                   1'000.0
            << " healthy="
            << (clickhouse_event->healthy() ? "true" : "false") << '\n';
    }
    if (options.kline_enabled) {
        std::cout
            << "kline_config enabled=true intervals="
            << options.kline.worker.interval_seconds.size()
            << " micro_batch_rows=" << options.kline.micro_batch_rows
            << " micro_batch_max_delay_ns="
            << options.kline.micro_batch_max_delay_ns
            << " insert_request_max_rows="
            << options.clickhouse_kline.insert_request_max_rows
            << " insert_request_max_bytes="
            << options.clickhouse_kline.insert_request_max_bytes
            << " physical_group_max_batches="
            << options.clickhouse_kline.physical_group_max_batches
            << " physical_group_max_delay_ns="
            << options.clickhouse_kline.physical_group_max_delay_ns
            << " writer_lanes=" << options.clickhouse_kline.writer_lanes
            << " calculation_run_id="
            << l2flow::clickhouse::IdentifierString(
                   options.kline.worker.calculation_run_id) << '\n'
            << "kline_throughput worker_facts_msg_s=" << kline_worker_rate
            << " kline_revision_rows_ack_msg_s=" << kline_revision_ack_rate
            << " measurement_revision_ack_msg_s="
            << kline_measurement_ack_rate
            << " processing_seconds=" << kline_processing_seconds
            << " sink_seconds=" << kline_sink_seconds
            << " revision_rows_per_callback="
            << (kline_final_runtime_stats.workers.facts_journaled == 0U
                    ? 0.0
                    : static_cast<double>(
                          kline_final_runtime_stats.workers.revisions_created) /
                          static_cast<double>(
                              kline_final_runtime_stats.workers.facts_journaled))
            << '\n'
            << "kline_final ordered_dispositions="
            << kline_final_runtime_stats.ordered_dispositions_received
            << " facts_journaled="
            << kline_final_runtime_stats.workers.facts_journaled
            << " trades_projected="
            << kline_final_runtime_stats.workers.trades_projected
            << " bars_created="
            << kline_final_runtime_stats.workers.bars_created
            << " bars_updated="
            << kline_final_runtime_stats.workers.bars_updated
            << " revisions_created="
            << kline_final_runtime_stats.workers.revisions_created
            << " micro_batches="
            << kline_final_runtime_stats.micro_batches_applied
            << " pending_revision_batches="
            << kline_final_runtime_stats.workers.pending_revision_batches
            << " invalid_inputs=" << kline_final_runtime_stats.invalid_inputs
            << " invalid_trade_times="
            << kline_final_runtime_stats.workers.invalid_trade_exchange_times
            << '\n'
            << "clickhouse_kline_final revision_rows_queued="
            << kline_final_stats.revision_rows_queued
            << " revision_rows_acked="
            << kline_final_stats.revision_rows_acked
            << " revision_batches_queued="
            << kline_final_stats.revision_batches_queued
            << " revision_batches_acked="
            << kline_final_stats.revision_batches_acked
            << " revision_batches_released="
            << kline_final_stats.revision_batches_released
            << " physical_groups="
            << kline_final_stats.physical_groups_committed
            << " revision_insert_requests="
            << kline_final_stats.revision_insert_requests_acked
            << " marker_insert_requests="
            << kline_final_stats.marker_insert_requests_acked
            << " recovery_runs_committed="
            << kline_final_stats.recovery_runs_committed
            << " retries=" << kline_final_stats.retry_attempts
            << " unknown_outcomes=" << kline_final_stats.unknown_outcomes
            << " queued_rows_at_measurement_end="
            << kline_revision_lag_rows
            << " queued_rows_hwm="
            << kline_final_stats.queued_revision_rows_high_water
            << " bytes_sent=" << kline_final_stats.bytes_sent
            << " stop_drain_ms="
            << to_us(kline_stop_finish_ns >= kline_stop_start_ns
                         ? kline_stop_finish_ns - kline_stop_start_ns
                         : 0U) /
                   1'000.0
            << " healthy="
            << (clickhouse_kline->healthy() ? "true" : "false") << '\n';
    }
    if (options.event_enabled || options.kline_enabled) {
        std::cout
            << "fact_journal path=" << fact_journal_path.string()
            << " shared_by_all_owners="
            << (fact_journal_shared_by_all_owners ? "true" : "false")
            << " records=" << event_journal_stats.records
            << " consumer_new=" << event_journal_stats.consumer_new
            << " expected_consumer_new=" << expected_consumer_new
            << " record_bytes=" << event_journal_stats.record_bytes
            << " file_bytes=" << event_journal_stats.file_bytes
            << " observed_file_bytes="
            << event_journal_observed_file_bytes
            << " write_calls=" << event_journal_stats.write_calls
            << " write_bytes=" << event_journal_stats.write_bytes
            << " partial_writes=" << event_journal_stats.partial_writes
            << " read_calls=" << event_journal_stats.read_calls
            << " read_bytes=" << event_journal_stats.read_bytes
            << " partial_reads=" << event_journal_stats.partial_reads
            << " directory_pages=" << event_journal_stats.directory_pages
            << " active_writes=" << event_journal_stats.active_writes
            << " active_reads=" << event_journal_stats.active_reads
            << " reserved_records=" << event_journal_stats.reserved_records
            << " waiting_admissions="
            << event_journal_stats.waiting_admissions
            << " errors=" << event_journal_stats.errors
            << " flush_ms="
            << to_us(event_journal_flush_finish_ns >=
                             event_journal_flush_start_ns
                         ? event_journal_flush_finish_ns -
                               event_journal_flush_start_ns
                         : 0U) /
                   1'000.0
            << " runtime_healthy="
            << ((event_runtime_healthy && kline_runtime_healthy)
                    ? "true" : "false")
            << " journal_healthy="
            << (event_journal_healthy ? "true" : "false")
            << " cleanup="
            << (event_journal_cleanup_ok ? "removed" : "failed") << '\n';
    }
#endif
    std::cout << "status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (admission_failure != AdmissionResult::kAccepted) {
        std::cerr << "admission failed: "
                  << AdmissionResultName(admission_failure) << '\n';
    }
    if (!engine->healthy()) {
        std::cerr << "engine fatal: " << engine->fatal_error() << '\n';
    }
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (options.clickhouse_enabled &&
        (!clickhouse_stop_ok || !clickhouse_raw->healthy())) {
        std::cerr << "ClickHouse raw sink fatal: "
                  << (clickhouse_stop_error.empty()
                          ? clickhouse_raw->fatal_error()
                          : clickhouse_stop_error)
                  << '\n';
    }
    if (options.event_enabled &&
        (!event_flush_ok || !event_drain_ok || !event_stop_ok ||
         !event_runtime_created || !event_runtime_healthy ||
         !fact_journal_valid || clickhouse_event == nullptr ||
         !clickhouse_event->healthy())) {
        std::cerr << "Event path fatal: ";
        if (!event_stop_error.empty()) {
            std::cerr << event_stop_error;
        } else if (!event_runtime_created) {
            std::cerr << "runtime unavailable";
        } else if (!event_runtime_fatal.empty()) {
            std::cerr << event_runtime_fatal;
        } else if (!event_journal_fatal.empty()) {
            std::cerr << event_journal_fatal;
        } else if (!fact_journal_shared_by_all_owners) {
            std::cerr << "derived owners do not share one FactJournal";
        } else if (!event_journal_cleanup_ok) {
            std::cerr << "FactJournal cleanup failed: "
                      << event_journal_cleanup_error.message();
        } else {
            std::cerr << clickhouse_event->fatal_error();
        }
        std::cerr << '\n';
    }
    if (options.kline_enabled &&
        (!kline_flush_ok || !kline_drain_ok || !kline_stop_ok ||
         !kline_runtime_created || !kline_runtime_healthy ||
         !fact_journal_valid || clickhouse_kline == nullptr ||
         !clickhouse_kline->healthy())) {
        std::cerr << "KLine path fatal: ";
        if (!kline_stop_error.empty()) {
            std::cerr << kline_stop_error;
        } else if (!kline_runtime_created) {
            std::cerr << "runtime unavailable";
        } else if (!kline_runtime_fatal.empty()) {
            std::cerr << kline_runtime_fatal;
        } else if (!event_journal_fatal.empty()) {
            std::cerr << event_journal_fatal;
        } else if (!fact_journal_shared_by_all_owners) {
            std::cerr << "derived owners do not share one FactJournal";
        } else if (!event_journal_cleanup_ok) {
            std::cerr << "FactJournal cleanup failed: "
                      << event_journal_cleanup_error.message();
        } else {
            std::cerr << clickhouse_kline->fatal_error();
        }
        std::cerr << '\n';
    }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_enabled) {
        for (std::size_t owner = 0U;
             owner < options.instrument_owners; ++owner) {
            if (!valid) {
                std::cerr << "owner " << owner << " consumed="
                          << consumers[owner].consumed.load(
                                 std::memory_order_acquire)
                          << " max_sink_operation_us="
                          << to_us(
                                 consumers[owner].maximum_sink_operation_ns)
                          << " arrow_rows="
                          << arrow_reader_states[owner].rows
                          << " arrow_batches="
                          << arrow_reader_states[owner].batches << '\n';
            }
            if (!consumers[owner].error.empty()) {
                std::cerr << "owner " << owner << ": "
                          << consumers[owner].error << '\n';
            }
            if (!arrow_reader_states[owner].error.empty()) {
                std::cerr << "Arrow reader " << owner << ": "
                          << arrow_reader_states[owner].error << '\n';
            }
        }
        if (!arrow_egress->healthy()) {
            std::cerr << "Arrow egress fatal: "
                      << arrow_egress->fatal_error() << '\n';
        }
    }
#endif
    return valid ? 0 : 1;
}
