#pragma once

#include "l2flow/clickhouse/raw_consumer.h"
#include "l2flow/event/types.h"
#include "l2flow/outbox/request_spool.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::clickhouse {

inline constexpr std::size_t kMaximumEventWriterLanes = 32U;

struct EventClickHouseConfig final {
    std::string endpoint = "http://127.0.0.1:8123";
    std::string database = "l2flow";
    std::string username = "default";
    std::string password;
    std::string no_proxy = "*";

    // One physical RowBinary request is bounded independently from the
    // logical Event recovery batches that it carries. A logical batch larger
    // than either request bound is split across multiple immutable requests.
    std::size_t insert_request_max_rows = 1'024U;
    std::size_t insert_request_max_bytes = 1U * 1'024U * 1'024U;
    // A writer lane combines only a consecutive FIFO prefix. The delay is
    // measured from the oldest queued logical batch and is bypassed on stop.
    std::size_t physical_group_max_batches = 256U;
    std::size_t physical_group_max_rows = 16'384U;
    std::size_t physical_group_max_revision_bytes =
        16U * 1'024U * 1'024U;
    std::uint64_t physical_group_max_delay_ns = 1'000'000U;
    // Number of independent, ordered Event INSERT lanes.  The supported
    // production values are 1, 2, 4, 8, 16, and 32.  A batch is routed by its
    // logical Event owner, so all revisions for one owner stay FIFO on one
    // lane while different owners may be written concurrently.
    std::size_t writer_lanes = 1U;
    std::size_t queue_revision_batches = 1'024U;
    std::size_t queue_revision_rows = 1U * 1'024U * 1'024U;

    // The request spool is run-scoped and must live on a durable local
    // filesystem. Event WAL positions advance only after the recovery marker
    // is acknowledged and this completion sink accepts the exact input set.
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

[[nodiscard]] bool ValidateEventClickHouseConfig(
    const EventClickHouseConfig& config,
    std::string* error) noexcept;
[[nodiscard]] std::string EventCurrentViewDdl(std::string_view database);

struct EventClickHouseStats final {
    std::uint64_t submission_groups_queued = 0U;
    std::uint64_t submission_groups_released = 0U;
    std::uint64_t revision_batches_queued = 0U;
    std::uint64_t revision_batches_acked = 0U;
    std::uint64_t revision_batches_released = 0U;
    std::uint64_t revision_rows_queued = 0U;
    std::uint64_t revision_rows_acked = 0U;
    std::uint64_t physical_groups_committed = 0U;
    std::uint64_t revision_insert_requests_acked = 0U;
    std::uint64_t marker_insert_requests_acked = 0U;
    std::uint64_t recovery_runs_committed = 0U;
    std::uint64_t retry_attempts = 0U;
    std::uint64_t unknown_outcomes = 0U;
    std::uint64_t bytes_sent = 0U;
    std::uint64_t physical_group_batches_max = 0U;
    std::uint64_t physical_group_rows_max = 0U;
    std::uint64_t revision_request_rows_max = 0U;
    std::uint64_t revision_request_bytes_max = 0U;
    std::uint64_t admission_validation_ns = 0U;
    std::uint64_t queue_budget_wait_ns = 0U;
    std::uint64_t queue_budget_wait_count = 0U;
    std::uint64_t queue_budget_wait_ns_max = 0U;
    std::uint64_t group_wait_ns = 0U;
    std::uint64_t rowbinary_serialize_ns = 0U;
    std::uint64_t chunk_id_ns = 0U;
    std::uint64_t spool_checksum_ns = 0U;
    std::uint64_t spool_encode_copy_ns = 0U;
    std::uint64_t spool_write_ns = 0U;
    std::uint64_t spool_fdatasync_ns = 0U;
    std::uint64_t spool_registry_lock_wait_ns = 0U;
    std::uint64_t spool_entry_lock_wait_ns = 0U;
    std::uint64_t request_state_before_send_ns = 0U;
    std::uint64_t curl_easy_perform_ns = 0U;
    std::uint64_t request_state_after_ack_ns = 0U;
    std::uint64_t revision_http_ns = 0U;
    std::uint64_t marker_http_ns = 0U;
    std::uint64_t completion_ns = 0U;
    std::uint64_t retire_ns = 0U;
    std::uint64_t queued_revision_batches = 0U;
    std::uint64_t queued_revision_rows = 0U;
    std::uint64_t queued_submission_groups = 0U;
    std::uint64_t queued_revision_batches_high_water = 0U;
    std::uint64_t queued_revision_rows_high_water = 0U;
    std::uint64_t queued_submission_groups_high_water = 0U;
    std::uint64_t request_spool_live_groups = 0U;
    std::uint64_t request_spool_preparing_groups = 0U;
    std::uint64_t request_spool_bytes = 0U;
    std::uint64_t request_spool_reserved_bytes = 0U;
};

class EventClickHouseSink final : public event::EventRevisionSink {
public:
    ~EventClickHouseSink() override;
    EventClickHouseSink(const EventClickHouseSink&) = delete;
    EventClickHouseSink& operator=(const EventClickHouseSink&) = delete;

    [[nodiscard]] static std::unique_ptr<EventClickHouseSink> Create(
        EventClickHouseConfig config,
        std::string* error);

    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool Stop(std::string* error) noexcept;

    [[nodiscard]] bool AppendRevisionGroup(
        std::vector<std::shared_ptr<const event::EventRevisionBatch>> batches)
        noexcept override;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] EventClickHouseStats stats() const noexcept;
    [[nodiscard]] Identifier128 writer_instance_id() const noexcept;
    [[nodiscard]] const EventClickHouseConfig& config() const noexcept;

private:
    class Impl;
    explicit EventClickHouseSink(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::clickhouse
