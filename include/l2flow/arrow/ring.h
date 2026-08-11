#pragma once

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace l2flow::arrow_hot {

inline constexpr std::uint32_t kArrowRingProtocolVersion = 2U;
inline constexpr std::size_t kMaximumArrowRingConsumers = 64U;
inline constexpr std::size_t kMaximumArrowRingSchemaBytes =
    16U * 1'024U * 1'024U;

enum class RingStreamKind : std::uint32_t {
    kOrderedTick = 1U,
    kSnapshot = 2U,
    kControl = 4U,
    kEvent = 5U,
    kKline = 6U,
};

enum class ProducerState : std::uint32_t {
    kInitializing = 0U,
    kActive = 1U,
    kSealed = 2U,
};

struct ProducerInstanceId final {
    std::uint64_t high = 0U;
    std::uint64_t low = 0U;

    friend constexpr bool operator==(const ProducerInstanceId&,
                                     const ProducerInstanceId&) = default;
};

struct RingLocation final {
    std::filesystem::path data_path;
    std::filesystem::path control_path;
};

struct RingWriterConfig final {
    RingLocation location;
    RingStreamKind stream_kind = RingStreamKind::kOrderedTick;
    std::uint32_t shard_id = 0U;
    std::size_t descriptor_capacity = 1'024U;
    std::size_t segment_count = 1'088U;
    std::size_t segment_payload_bytes = 256U * 1'024U;
    std::size_t maximum_consumers = 16U;
    std::uint64_t feed_session_epoch = 1U;
    ProducerInstanceId producer_instance{};
    bool unlink_on_destroy = false;
};

struct BatchMetadata final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t minimum_exchange_time_ns = 0U;
    std::uint64_t maximum_exchange_time_ns = 0U;
    std::uint64_t publish_monotonic_ns = 0U;
    std::uint32_t flags = 0U;
};

enum class PublishCode : std::uint8_t {
    kPublished,
    kNoReusableSegment,
    kPayloadTooLarge,
    kInvalidBatch,
    kInternalError,
};

struct PublishResult final {
    PublishCode code = PublishCode::kInternalError;
    std::uint64_t batch_sequence = 0U;
    std::size_t payload_bytes = 0U;
    std::string error;
};

struct RingWriterStats final {
    std::uint64_t published_batches = 0U;
    std::uint64_t published_rows = 0U;
    std::uint64_t no_segment_drops = 0U;
    std::uint64_t oversized_drops = 0U;
    std::uint64_t reaped_consumers = 0U;
};

class SharedArrowRingWriter final {
public:
    ~SharedArrowRingWriter();
    SharedArrowRingWriter(const SharedArrowRingWriter&) = delete;
    SharedArrowRingWriter& operator=(const SharedArrowRingWriter&) = delete;

    [[nodiscard]] static std::unique_ptr<SharedArrowRingWriter> Create(
        RingWriterConfig config,
        std::shared_ptr<arrow::Schema> schema,
        std::string* error);

    // TryPublish, ReapDeadConsumers, and Seal belong to one producer control
    // thread and must not overlap. Quiesce publication before Seal or
    // destruction. TouchHeartbeat and stats may be used by monitoring code,
    // but no method may be called after destruction begins.
    [[nodiscard]] PublishResult TryPublish(
        const arrow::RecordBatch& batch,
        const BatchMetadata& metadata) noexcept;

    void TouchHeartbeat(std::uint64_t monotonic_ns) noexcept;
    void Seal(std::uint64_t monotonic_ns) noexcept;
    void ReapDeadConsumers() noexcept;

    [[nodiscard]] RingWriterStats stats() const noexcept;
    [[nodiscard]] const RingWriterConfig& config() const noexcept;
    [[nodiscard]] const std::shared_ptr<arrow::Schema>& schema() const noexcept;
    [[nodiscard]] std::uint64_t schema_fingerprint() const noexcept;

private:
    class Impl;
    explicit SharedArrowRingWriter(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

enum class ReaderStart : std::uint8_t {
    kLatest,
    kEarliestAvailable,
};

enum class ReadCode : std::uint8_t {
    kBatch,
    kEmpty,
    kOverrun,
    kRetry,
    kCorrupt,
    kClosed,
};

struct ReadMetadata final {
    std::uint64_t batch_sequence = 0U;
    std::uint64_t oldest_available_sequence = 0U;
    std::uint64_t newest_available_sequence = 0U;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t minimum_exchange_time_ns = 0U;
    std::uint64_t maximum_exchange_time_ns = 0U;
    std::uint64_t publish_monotonic_ns = 0U;
    std::uint32_t row_count = 0U;
    std::uint32_t flags = 0U;
    ProducerInstanceId producer_instance{};
};

class ArrowBatchLease final {
public:
    ~ArrowBatchLease();
    ArrowBatchLease(const ArrowBatchLease&) = delete;
    ArrowBatchLease& operator=(const ArrowBatchLease&) = delete;

    [[nodiscard]] const std::uint8_t* data() const noexcept;
    [[nodiscard]] std::size_t size() const noexcept;
    [[nodiscard]] const ReadMetadata& metadata() const noexcept;

private:
    class Impl;
    explicit ArrowBatchLease(std::shared_ptr<Impl> impl) noexcept;
    std::shared_ptr<Impl> impl_;

    friend class SharedArrowRingReader;
};

struct ReadResult final {
    ReadCode code = ReadCode::kClosed;
    ReadMetadata metadata{};
    std::shared_ptr<ArrowBatchLease> lease;
    std::string error;
};

class SharedArrowRingReader final {
public:
    ~SharedArrowRingReader();
    SharedArrowRingReader(const SharedArrowRingReader&) = delete;
    SharedArrowRingReader& operator=(const SharedArrowRingReader&) = delete;

    [[nodiscard]] static std::unique_ptr<SharedArrowRingReader> Open(
        RingLocation location,
        ReaderStart start,
        std::string* error);

    // A reader is one mutable cursor: TryRead, seek, heartbeat, and Decode
    // calls must not overlap. Returned leases and decoded Arrow objects may be
    // retained or destroyed on other threads and may outlive this reader.
    [[nodiscard]] ReadResult TryRead() noexcept;
    [[nodiscard]] bool SeekToEarliestAvailable() noexcept;
    [[nodiscard]] bool SeekToLatest() noexcept;
    void TouchHeartbeat(std::uint64_t monotonic_ns) noexcept;

    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> Decode(
        const std::shared_ptr<ArrowBatchLease>& lease,
        std::string* error) const;

    [[nodiscard]] const std::shared_ptr<arrow::Schema>& schema() const noexcept;
    [[nodiscard]] RingStreamKind stream_kind() const noexcept;
    [[nodiscard]] std::uint32_t shard_id() const noexcept;
    [[nodiscard]] ProducerInstanceId producer_instance() const noexcept;
    [[nodiscard]] std::uint64_t feed_session_epoch() const noexcept;
    [[nodiscard]] ProducerState producer_state() const noexcept;
    [[nodiscard]] std::uint64_t producer_heartbeat_monotonic_ns()
        const noexcept;
    [[nodiscard]] std::uint64_t next_sequence() const noexcept;

private:
    class Impl;
    explicit SharedArrowRingReader(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] ProducerInstanceId GenerateProducerInstanceId(
    std::string* error) noexcept;
[[nodiscard]] std::string ProducerInstanceIdString(
    ProducerInstanceId value);

}  // namespace l2flow::arrow_hot
