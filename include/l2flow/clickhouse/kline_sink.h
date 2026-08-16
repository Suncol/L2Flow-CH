#pragma once

#include "l2flow/clickhouse/raw_consumer.h"
#include "l2flow/kline/types.h"
#include "l2flow/outbox/request_spool.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::clickhouse {

inline constexpr std::size_t kMaximumKLineWriterLanes = 32U;

struct KLineClickHouseConfig final {
    std::string endpoint = "http://127.0.0.1:8123";
    std::string database = "l2flow";
    std::string username = "default";
    std::string password;
    std::string no_proxy = "*";

    // Physical INSERT bounds are independent from logical KLine batches. A
    // logical batch larger than either request bound is split across multiple
    // immutable requests.
    std::size_t insert_request_max_rows = 1'024U;
    std::size_t insert_request_max_bytes = 1U * 1'024U * 1'024U;
    // Each lane combines only a consecutive FIFO prefix. The oldest queued
    // batch starts the linger deadline; stop bypasses it.
    std::size_t physical_group_max_batches = 256U;
    std::uint64_t physical_group_max_delay_ns = 1'000'000U;
    std::size_t writer_lanes = 1U;
    std::size_t queue_revision_batches = 1'024U;
    std::size_t queue_revision_rows = 1U * 1'024U * 1'024U;

    outbox::RequestSpoolConfig request_spool;
    outbox::ConsumerCompletionSink* completion_sink = nullptr;

    std::uint32_t connect_timeout_ms = 2'000U;
    std::uint32_t request_timeout_ms = 10'000U;
    std::uint32_t retry_initial_backoff_ms = 10U;
    std::uint32_t retry_max_backoff_ms = 1'000U;
    std::uint32_t shutdown_timeout_ms = 30'000U;

    std::uint32_t insert_quorum = 0U;
    bool insert_quorum_parallel = true;
    bool ensure_local_tables = true;
    bool tls_verify_peer = true;
};

[[nodiscard]] bool ValidateKLineClickHouseConfig(
    const KLineClickHouseConfig& config,
    std::string* error) noexcept;
[[nodiscard]] std::string KLineCurrentViewDdl(std::string_view database);

struct KLineClickHouseLaneStats final {
    std::uint64_t queued_revision_batches = 0U;
    std::uint64_t queued_revision_rows = 0U;
    std::uint64_t queued_revision_batches_high_water = 0U;
    std::uint64_t queued_revision_rows_high_water = 0U;
};

struct KLineClickHouseStats final {
    std::uint64_t revision_batches_queued = 0U;
    std::uint64_t revision_batches_acked = 0U;
    std::uint64_t revision_batches_released = 0U;
    std::uint64_t revision_rows_queued = 0U;
    std::uint64_t revision_rows_acked = 0U;
    std::uint64_t physical_groups_committed = 0U;
    std::uint64_t revision_insert_requests_acked = 0U;
    std::uint64_t marker_insert_requests_acked = 0U;
    std::uint64_t revision_insert_rows_acked = 0U;
    std::uint64_t revision_insert_bytes_acked = 0U;
    std::uint64_t marker_insert_rows_acked = 0U;
    std::uint64_t marker_insert_bytes_acked = 0U;
    std::uint64_t recovery_runs_committed = 0U;
    std::uint64_t retry_attempts = 0U;
    std::uint64_t unknown_outcomes = 0U;
    std::uint64_t bytes_sent = 0U;
    std::uint64_t physical_group_batches_max = 0U;
    std::uint64_t physical_group_rows_max = 0U;
    std::uint64_t revision_request_rows_max = 0U;
    std::uint64_t revision_request_bytes_max = 0U;
    std::uint64_t marker_request_rows_max = 0U;
    std::uint64_t marker_request_bytes_max = 0U;
    std::uint64_t revision_insert_latency_ns_total = 0U;
    std::uint64_t revision_insert_latency_ns_max = 0U;
    std::uint64_t marker_insert_latency_ns_total = 0U;
    std::uint64_t marker_insert_latency_ns_max = 0U;
    std::uint64_t queued_revision_batches = 0U;
    std::uint64_t queued_revision_rows = 0U;
    std::uint64_t queued_revision_batches_high_water = 0U;
    std::uint64_t queued_revision_rows_high_water = 0U;
    std::uint64_t request_spool_live_groups = 0U;
    std::uint64_t request_spool_bytes = 0U;
    std::size_t writer_lanes = 0U;
    std::array<KLineClickHouseLaneStats, kMaximumKLineWriterLanes> lanes{};
};

class KLineClickHouseSink final : public kline::KLineRevisionSink {
public:
    ~KLineClickHouseSink() override;
    KLineClickHouseSink(const KLineClickHouseSink&) = delete;
    KLineClickHouseSink& operator=(const KLineClickHouseSink&) = delete;

    [[nodiscard]] static std::unique_ptr<KLineClickHouseSink> Create(
        KLineClickHouseConfig config,
        std::string* error);

    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool Stop(std::string* error) noexcept;
    [[nodiscard]] bool AppendRevisionBatch(
        std::shared_ptr<const kline::KLineRevisionBatch> batch)
        noexcept override;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] KLineClickHouseStats stats() const noexcept;
    [[nodiscard]] Identifier128 writer_instance_id() const noexcept;
    [[nodiscard]] const KLineClickHouseConfig& config() const noexcept;

private:
    class Impl;
    explicit KLineClickHouseSink(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::clickhouse
