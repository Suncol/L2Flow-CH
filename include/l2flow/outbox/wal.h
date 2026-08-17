#pragma once

#include "l2flow/outbox/types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace l2flow::outbox {

struct DurableOutboxConfig final {
    std::filesystem::path root_directory = "/tmp/l2flow-canonical-outbox";
    Identifier128 source_instance_id{};
    std::uint64_t feed_session_epoch = 0U;

    std::size_t producer_queue_records = 65'536U;
    std::size_t commit_batch_records = 256U;
    std::size_t commit_batch_bytes = 4U * 1'024U * 1'024U;
    std::uint64_t commit_max_delay_ns = 500'000U;
    std::uint64_t segment_max_bytes = 256U * 1'024U * 1'024U;
    std::uint64_t maximum_reservoir_bytes = 32ULL * 1'024ULL * 1'024ULL *
                                            1'024ULL;
    std::uint64_t cursor_checkpoint_interval_ns = 100'000'000U;
    std::size_t read_cache_batches = 8U;

    bool raw_consumer_enabled = true;
    bool event_consumer_enabled = true;
    bool kline_consumer_enabled = true;
};

[[nodiscard]] bool ValidateDurableOutboxConfig(
    const DurableOutboxConfig& config,
    std::string* error) noexcept;

struct ConsumerCursorStats final {
    WalPosition contiguous{};
    std::uint64_t out_of_order_completions = 0U;
    std::uint64_t lag_records = 0U;
    bool enabled = false;
};

struct DurableOutboxStats final {
    std::uint64_t records_accepted = 0U;
    std::uint64_t records_durable = 0U;
    std::uint64_t batches_durable = 0U;
    std::uint64_t bytes_durable = 0U;
    std::uint64_t segments_created = 0U;
    std::uint64_t segments_reclaimed = 0U;
    std::uint64_t reservoir_bytes = 0U;
    std::uint64_t queued_records = 0U;
    // Indexed records are disk-resident positions. Only cached_records have
    // decoded CanonicalRecord payloads resident inside the outbox.
    std::uint64_t indexed_records = 0U;
    std::uint64_t cached_records = 0U;
    std::uint64_t indexed_batches = 0U;
    // Successful cache-miss loads. Callers that join an in-flight same-batch
    // load increment coalesced_cold_read_waits without repeating the work.
    std::uint64_t cold_read_batches = 0U;
    std::uint64_t cold_read_bytes = 0U;
    std::uint64_t coalesced_cold_read_waits = 0U;
    // Disk read, validation, and decode execution time; semaphore queueing is
    // excluded.
    std::uint64_t cold_read_ns = 0U;
    std::uint64_t maximum_cold_read_ns = 0U;
    // Maximum delay from a successful WAL sync to acquisition of the state
    // mutex that publishes the durable batch index.
    std::uint64_t maximum_durable_publish_wait_ns = 0U;
    WalPosition durable_tail{};
    WalPosition latest_barrier_position{};
    FreshnessBarrier latest_barrier{};
    std::array<ConsumerCursorStats, 3U> consumers{};
};

class DurableOutbox final : public ConsumerCompletionSink {
public:
    ~DurableOutbox() override;
    DurableOutbox(const DurableOutbox&) = delete;
    DurableOutbox& operator=(const DurableOutbox&) = delete;

    [[nodiscard]] static std::unique_ptr<DurableOutbox> Create(
        DurableOutboxConfig config,
        std::string* error);

    // Start creates a new O_EXCL run directory. This implementation never
    // reopens an old run: crash recovery is intentionally delegated to the
    // external source reservoir, while durability decouples consumers during
    // the lifetime of this process.
    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool Flush(std::string* error) noexcept;
    [[nodiscard]] bool Stop(std::string* error) noexcept;

    // Multi-producer, bounded admission. The record is not visible to any
    // consumer until its containing batch has completed fdatasync.
    [[nodiscard]] bool Enqueue(const CanonicalRecord& record) noexcept;

    [[nodiscard]] bool TryRead(std::uint64_t lsn,
                               RecordView* output) const noexcept;
    [[nodiscard]] bool Complete(
        ConsumerKind consumer,
        std::span<const WalPosition> positions) noexcept override;
    [[nodiscard]] bool CompleteOne(ConsumerKind consumer,
                                   WalPosition position) noexcept;

    [[nodiscard]] WalPosition durable_tail() const noexcept;
    [[nodiscard]] std::uint64_t oldest_resident_lsn() const noexcept;
    [[nodiscard]] WalPosition consumer_cursor(
        ConsumerKind consumer) const noexcept;
    [[nodiscard]] bool consumer_enabled(ConsumerKind consumer) const noexcept;
    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] DurableOutboxStats stats() const noexcept;
    [[nodiscard]] Identifier128 run_id() const noexcept;
    [[nodiscard]] Identifier128 source_instance_id() const noexcept;
    [[nodiscard]] const std::filesystem::path& run_directory() const noexcept;
    [[nodiscard]] const DurableOutboxConfig& config() const noexcept;

private:
    class Impl;
    explicit DurableOutbox(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::outbox
