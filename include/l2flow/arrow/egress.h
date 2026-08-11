#pragma once

#include "l2flow/arrow/ring.h"
#include "l2flow/arrow/schemas.h"
#include "l2flow/ingest/canonical.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace l2flow::arrow_hot {

struct ArrowHotEgressConfig final {
    std::filesystem::path root_directory;
    std::size_t owner_count = 0U;
    std::uint64_t feed_session_epoch = 0U;

    std::size_t descriptor_capacity = 1'024U;
    std::size_t segment_count = 1'088U;
    std::size_t tick_segment_payload_bytes = 256U * 1'024U;
    std::size_t snapshot_segment_payload_bytes = 256U * 1'024U;
    std::size_t diagnostic_segment_payload_bytes = 128U * 1'024U;
    std::size_t maximum_consumers = 16U;

    std::size_t tick_batch_rows = 256U;
    std::size_t snapshot_batch_rows = 16U;
    std::size_t diagnostic_batch_rows = 64U;
    std::uint64_t maximum_batch_delay_ns = 1'000'000U;
    std::uint64_t heartbeat_interval_ns = 1'000'000'000U;
};

[[nodiscard]] bool ValidateArrowHotEgressConfig(
    const ArrowHotEgressConfig& config,
    std::string* error) noexcept;

struct ArrowHotEgressStats final {
    std::uint64_t tick_rows_received = 0U;
    std::uint64_t snapshot_rows_received = 0U;
    std::uint64_t hole_fill_rows_received = 0U;
    std::uint64_t control_rows_received = 0U;
    std::uint64_t published_batches = 0U;
    std::uint64_t published_rows = 0U;
    std::uint64_t no_segment_dropped_rows = 0U;
    std::uint64_t oversized_dropped_rows = 0U;
    std::uint64_t control_publish_dropped_rows = 0U;
    std::uint64_t internal_errors = 0U;
};

// This sink is called by the existing single consumer for each instrument
// owner. Methods for a given owner must not be called concurrently. Control
// methods are internally serialized because hot-path overruns can originate
// from every owner. FlushAll and Seal require all owner callers to be quiesced.
class ArrowHotEgress final {
public:
    ~ArrowHotEgress();
    ArrowHotEgress(const ArrowHotEgress&) = delete;
    ArrowHotEgress& operator=(const ArrowHotEgress&) = delete;

    [[nodiscard]] static std::unique_ptr<ArrowHotEgress> Create(
        ArrowHotEgressConfig config,
        std::string* error);

    [[nodiscard]] bool AppendTickDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept;
    [[nodiscard]] bool AppendSnapshot(
        std::size_t owner,
        const ingest::CanonicalSnapshot& snapshot) noexcept;
    [[nodiscard]] bool AppendGap(
        const ingest::ChannelGap& record) noexcept;
    [[nodiscard]] bool AppendFault(
        const ingest::ChannelFault& record) noexcept;

    // FeedConnected means that LogonResponse succeeded and every configured
    // subscription was confirmed. API events retain their MDL names; timeout,
    // discard, malformed-control, and readiness failures are conservative
    // local process/epoch boundaries.
    [[nodiscard]] bool MarkFeedConnected(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedDisconnected(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedConnectError(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedServiceTimeout(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedMessageDiscarded(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedSubscriptionRejected(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedControlProtocolError(
        std::uint64_t observed_monotonic_ns) noexcept;
    [[nodiscard]] bool MarkFeedReadyTimeout(
        std::uint64_t observed_monotonic_ns) noexcept;

    void FlushDue(std::size_t owner,
                  std::uint64_t now_monotonic_ns) noexcept;
    void FlushAll() noexcept;
    void Seal(std::uint64_t observed_monotonic_ns) noexcept;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] ArrowHotEgressStats stats() const noexcept;
    [[nodiscard]] const ArrowHotEgressConfig& config() const noexcept;
    [[nodiscard]] ProducerInstanceId producer_instance() const noexcept;
    [[nodiscard]] const std::filesystem::path& run_directory() const noexcept;
    [[nodiscard]] const std::filesystem::path& manifest_path() const noexcept;

private:
    class Impl;
    explicit ArrowHotEgress(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::arrow_hot
