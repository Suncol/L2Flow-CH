#pragma once

#include "l2flow/arrow/ring.h"
#include "l2flow/ingest/canonical.h"

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::arrow_hot {

inline constexpr std::uint32_t kTickArrowSchemaVersion = 1U;
inline constexpr std::uint32_t kSnapshotArrowSchemaVersion = 1U;
inline constexpr std::uint32_t kControlArrowSchemaVersion = 2U;

enum class TickStreamRole : std::uint8_t {
    kRealtimeOrdered = 0U,
    kLateRecovery = 1U,
};

enum class ControlKind : std::uint8_t {
    kProducerStarted = 1U,
    kFeedConnected = 2U,
    kFeedDisconnected = 3U,
    kProducerSealed = 4U,
    kChannelGap = 5U,
    kChannelFault = 6U,
    kHotPublishOverrun = 7U,
    kFeedConnectError = 8U,
    kFeedServiceTimeout = 9U,
    kFeedMessageDiscarded = 10U,
    kFeedSubscriptionRejected = 11U,
    kFeedControlProtocolError = 12U,
    kFeedReadyTimeout = 13U,
};

enum class ContinuityReason : std::uint8_t {
    kNone = 0U,
    kProcessStart = 1U,
    kMdlConnect = 2U,
    kMdlDisconnect = 3U,
    kProducerShutdown = 4U,
    kProducerCrashBoundary = 5U,
    kMdlConnectError = 6U,
    kMdlServiceTimeout = 7U,
    kMdlMessageDiscarded = 8U,
    kMdlSubscriptionRejected = 9U,
    kMdlControlProtocolError = 10U,
    kMdlReadyTimeout = 11U,
};

struct BuiltRecordBatch final {
    std::shared_ptr<arrow::RecordBatch> batch;
    BatchMetadata metadata{};
};

[[nodiscard]] std::shared_ptr<arrow::Schema> TickArrowSchema();
[[nodiscard]] std::shared_ptr<arrow::Schema> SnapshotArrowSchema();
[[nodiscard]] std::shared_ptr<arrow::Schema> ControlArrowSchema();

class TickRecordBatchBuilder final {
public:
    ~TickRecordBatchBuilder();
    TickRecordBatchBuilder(const TickRecordBatchBuilder&) = delete;
    TickRecordBatchBuilder& operator=(const TickRecordBatchBuilder&) = delete;

    [[nodiscard]] static std::unique_ptr<TickRecordBatchBuilder> Create(
        std::size_t initial_capacity,
        std::uint64_t feed_session_epoch,
        std::string* error);

    [[nodiscard]] bool AppendOrdered(
        const ingest::CanonicalTick& tick,
        std::string* error) noexcept;
    [[nodiscard]] bool AppendLateRecovery(
        const ingest::LateRecoveryTick& record,
        std::string* error) noexcept;
    [[nodiscard]] BuiltRecordBatch Finish(std::string* error) noexcept;

    [[nodiscard]] std::size_t rows() const noexcept;
    [[nodiscard]] std::uint64_t feed_session_epoch() const noexcept;

private:
    class Impl;
    explicit TickRecordBatchBuilder(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

class SnapshotRecordBatchBuilder final {
public:
    ~SnapshotRecordBatchBuilder();
    SnapshotRecordBatchBuilder(const SnapshotRecordBatchBuilder&) = delete;
    SnapshotRecordBatchBuilder& operator=(
        const SnapshotRecordBatchBuilder&) = delete;

    [[nodiscard]] static std::unique_ptr<SnapshotRecordBatchBuilder> Create(
        std::size_t initial_capacity,
        std::uint64_t feed_session_epoch,
        std::string* error);

    [[nodiscard]] bool Append(
        const ingest::CanonicalSnapshot& snapshot,
        std::string* error) noexcept;
    [[nodiscard]] BuiltRecordBatch Finish(std::string* error) noexcept;

    [[nodiscard]] std::size_t rows() const noexcept;
    [[nodiscard]] std::uint64_t feed_session_epoch() const noexcept;

private:
    class Impl;
    explicit SnapshotRecordBatchBuilder(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

struct LifecycleControl final {
    ControlKind kind = ControlKind::kProducerStarted;
    ContinuityReason reason = ContinuityReason::kNone;
    std::uint64_t observed_monotonic_ns = 0U;
};

struct HotPublishOverrun final {
    RingStreamKind affected_stream = RingStreamKind::kOrderedTick;
    std::uint32_t affected_shard = 0U;
    std::uint64_t dropped_rows = 0U;
    std::uint64_t observed_monotonic_ns = 0U;
};

class ControlRecordBatchBuilder final {
public:
    ~ControlRecordBatchBuilder();
    ControlRecordBatchBuilder(const ControlRecordBatchBuilder&) = delete;
    ControlRecordBatchBuilder& operator=(
        const ControlRecordBatchBuilder&) = delete;

    [[nodiscard]] static std::unique_ptr<ControlRecordBatchBuilder> Create(
        std::size_t initial_capacity,
        std::uint64_t feed_session_epoch,
        ProducerInstanceId producer_instance,
        std::string* error);

    [[nodiscard]] bool AppendLifecycle(
        const LifecycleControl& record,
        std::string* error) noexcept;
    [[nodiscard]] bool AppendGap(
        const ingest::ChannelGap& record,
        std::string* error) noexcept;
    [[nodiscard]] bool AppendFault(
        const ingest::ChannelFault& record,
        std::string* error) noexcept;
    [[nodiscard]] bool AppendOverrun(
        const HotPublishOverrun& record,
        std::string* error) noexcept;
    [[nodiscard]] BuiltRecordBatch Finish(std::string* error) noexcept;

    [[nodiscard]] std::size_t rows() const noexcept;

private:
    class Impl;
    explicit ControlRecordBatchBuilder(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::arrow_hot
