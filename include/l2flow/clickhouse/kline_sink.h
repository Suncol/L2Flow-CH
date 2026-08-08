#pragma once

#include "l2flow/clickhouse/raw_sink.h"
#include "l2flow/kline/types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::clickhouse {

struct KLineClickHouseConfig final {
    std::string endpoint = "http://127.0.0.1:8123";
    std::string database = "l2flow";
    std::string username = "default";
    std::string password;
    std::string no_proxy = "*";

    std::size_t insert_chunk_rows = 16'384U;
    std::size_t writer_lanes = 1U;
    std::size_t queue_revision_batches = 1'024U;
    std::size_t queue_revision_rows = 1U * 1'024U * 1'024U;

    std::uint32_t connect_timeout_ms = 2'000U;
    std::uint32_t request_timeout_ms = 10'000U;
    std::uint32_t retry_initial_backoff_ms = 10U;
    std::uint32_t retry_max_backoff_ms = 1'000U;
    std::uint32_t maximum_retry_elapsed_ms = 5'000U;
    std::uint32_t shutdown_timeout_ms = 30'000U;

    std::uint32_t insert_quorum = 0U;
    bool insert_quorum_parallel = true;
    bool ensure_local_tables = true;
    bool tls_verify_peer = true;
};

[[nodiscard]] bool ValidateKLineClickHouseConfig(
    const KLineClickHouseConfig& config,
    std::string* error) noexcept;

struct KLineClickHouseStats final {
    std::uint64_t revision_batches_queued = 0U;
    std::uint64_t revision_batches_acked = 0U;
    std::uint64_t revision_batches_released = 0U;
    std::uint64_t revision_rows_queued = 0U;
    std::uint64_t revision_rows_acked = 0U;
    std::uint64_t revision_chunks_acked = 0U;
    std::uint64_t recovery_runs_committed = 0U;
    std::uint64_t retry_attempts = 0U;
    std::uint64_t unknown_outcomes = 0U;
    std::uint64_t bytes_sent = 0U;
    std::uint64_t queued_revision_rows = 0U;
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
