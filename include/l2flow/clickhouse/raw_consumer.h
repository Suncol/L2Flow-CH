#pragma once

#include "l2flow/common/identifier.h"
#include "l2flow/ingest/canonical.h"
#include "l2flow/outbox/types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::clickhouse {

using Identifier128 = common::Identifier128;

inline constexpr std::uint32_t kRawTickSchemaVersion = 1U;
inline constexpr std::uint32_t kRawSnapshotSchemaVersion = 1U;

[[nodiscard]] std::string IdentifierString(Identifier128 identifier);
[[nodiscard]] bool ParseIdentifier(std::string_view text,
                                   Identifier128* output) noexcept;
[[nodiscard]] Identifier128 Blake3Hash128(
    std::span<const std::byte> input) noexcept;

enum class RawTableId : std::uint8_t {
    kRawTick = 1U,
    kRawSnapshot = 2U,
};

[[nodiscard]] Identifier128 RawBatchIdentifier(
    Identifier128 writer_instance_id,
    RawTableId table,
    std::uint32_t trade_date,
    std::uint64_t batch_sequence,
    std::uint32_t schema_version) noexcept;

[[nodiscard]] Identifier128 RawOccurrenceIdentifier(
    Identifier128 source_instance_id,
    std::uint64_t feed_session_epoch,
    std::uint64_t ingress_sequence,
    ingest::CanonicalKind kind) noexcept;

struct RawClickHouseConfig final {
    std::string endpoint = "http://127.0.0.1:8123";
    std::string database = "l2flow";
    std::string username = "default";
    std::string password;
    std::string no_proxy = "*";

    std::uint64_t feed_session_epoch = 0U;
    Identifier128 source_instance_id{};
    std::size_t tick_decoder_lanes = 0U;
    std::size_t snapshot_decoder_lanes = 0U;
    std::size_t writer_threads = 2U;

    std::size_t tick_batch_rows = 16'384U;
    std::size_t tick_batch_bytes = 16U * 1'024U * 1'024U;
    std::uint64_t tick_batch_max_delay_ns = 5'000'000U;
    std::size_t tick_queue_batches_per_lane = 8U;

    std::size_t snapshot_batch_rows = 256U;
    std::size_t snapshot_batch_bytes = 16U * 1'024U * 1'024U;
    std::uint64_t snapshot_batch_max_delay_ns = 20'000'000U;
    std::size_t snapshot_queue_batches_per_lane = 8U;

    // The raw consumer is the only component allowed to advance this cursor.
    // Event/KLine never appear on the raw ACK path.
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

[[nodiscard]] bool ValidateRawClickHouseConfig(
    const RawClickHouseConfig& config,
    std::string* error) noexcept;

struct RawClickHouseStats final {
    std::uint64_t tick_rows_received = 0U;
    std::uint64_t snapshot_rows_received = 0U;
    std::uint64_t batches_queued = 0U;
    std::uint64_t batches_acked = 0U;
    std::uint64_t batches_released = 0U;
    std::uint64_t rows_acked = 0U;
    std::uint64_t retry_attempts = 0U;
    std::uint64_t unknown_outcomes = 0U;
    std::uint64_t bytes_sent = 0U;
    std::uint64_t unacked_batches = 0U;
    std::uint64_t preallocated_canonical_bytes = 0U;
};

class RawClickHouseConsumer final {
public:
    ~RawClickHouseConsumer();
    RawClickHouseConsumer(const RawClickHouseConsumer&) = delete;
    RawClickHouseConsumer& operator=(const RawClickHouseConsumer&) = delete;

    [[nodiscard]] static std::unique_ptr<RawClickHouseConsumer> Create(
        RawClickHouseConfig config,
        std::string* error);

    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool Stop(std::string* error) noexcept;

    [[nodiscard]] bool AppendTick(
        std::size_t decoder_lane,
        outbox::WalPosition position,
        const ingest::CanonicalTick& tick) noexcept;
    [[nodiscard]] bool AppendSnapshot(
        std::size_t decoder_lane,
        outbox::WalPosition position,
        const ingest::CanonicalSnapshot& snapshot) noexcept;
    [[nodiscard]] bool CanAppendTick(std::size_t decoder_lane) noexcept;
    [[nodiscard]] bool CanAppendSnapshot(std::size_t decoder_lane) noexcept;
    [[nodiscard]] bool CanFlushTick(std::size_t decoder_lane) noexcept;
    [[nodiscard]] bool CanFlushSnapshot(std::size_t decoder_lane) noexcept;
    [[nodiscard]] bool PollTick(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept;
    [[nodiscard]] bool PollSnapshot(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept;
    [[nodiscard]] bool FlushTick(std::size_t decoder_lane) noexcept;
    [[nodiscard]] bool FlushSnapshot(std::size_t decoder_lane) noexcept;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] RawClickHouseStats stats() const noexcept;
    [[nodiscard]] Identifier128 writer_instance_id() const noexcept;
    [[nodiscard]] Identifier128 source_instance_id() const noexcept;
    [[nodiscard]] std::uint64_t run_started_monotonic_ns() const noexcept;
    [[nodiscard]] std::uint64_t run_started_utc_ns() const noexcept;
    [[nodiscard]] const RawClickHouseConfig& config() const noexcept;

private:
    class Impl;
    explicit RawClickHouseConsumer(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::clickhouse
