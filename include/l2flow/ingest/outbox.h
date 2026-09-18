#pragma once

#include "l2flow/ingest/canonical.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

namespace l2flow::ingest {

inline constexpr std::uint32_t kOutboxBroadcastOwner =
    std::numeric_limits<std::uint32_t>::max();
inline constexpr std::size_t kMaximumTickConsumers = 8U;

enum class ContinuityMode : std::uint8_t {
    kCaughtUp = 0U,
    kDerivedCatchup,
    kFatalContinuity,
};

// Owner-local progress through all dispatches already removed from the outbox.
// Submitted means retained by the volatile revision sink, not acknowledged.
enum class DerivedProgress : std::uint8_t {
    kConsumed = 0U,
    kCalculated,
    kSubmitted,
};

struct DerivedFreshness final {
    bool consumed = false;
    bool calculated = false;
    bool submitted = false;
    bool acknowledged = false;
};

struct FreshnessFrontier final {
    ContinuityMode mode = ContinuityMode::kCaughtUp;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t published_monotonic_ns = 0U;
    std::uint64_t max_lag_lsn = 0U;
    std::uint64_t lag_warning_lsn = 0U;
    bool outbox_pressure = false;
    DerivedFreshness event{};
    DerivedFreshness kline{};
    // Each derived plane is authoritative independently; one lagging plane
    // does not clear the other plane's flag. Authoritative describes calculation
    // freshness in this feed epoch; durability requires acknowledged as well.
    bool event_authoritative = false;
    bool kline_authoritative = false;
};

struct DerivedContinuityInputs final {
    bool enabled = false;
    bool healthy = true;
    bool sink_healthy = true;
    // Read ACKs before submissions, after CaptureFreshness. These are cumulative
    // logical batch counts; physical revision requests alone are not commits.
    std::uint64_t revision_batches_acked = 0U;
    std::uint64_t revision_batches_submitted = 0U;
};

struct ContinuityInputs final {
    bool fatal = false;
    DerivedContinuityInputs event{};
    DerivedContinuityInputs kline{};
    std::uint64_t now_monotonic_ns = 0U;
    // Pressure warning only: strict catch-up always requires zero lag. Clamp
    // this ceiling to half the ring capacity, leaving room before exhaustion.
    std::uint64_t catchup_lsn_slack = 65'536U;
};

struct DispositionOutboxStats final {
    std::uint64_t records_appended = 0U;
    std::uint64_t records_read = 0U;
    std::uint64_t records_skipped = 0U;
    std::uint64_t append_rejected_full = 0U;
};

// Process-lifetime per-lane sequenced disposition log. One decoder lane
// appends; each (consumer, owner) pair has an independent cursor. A record
// is released only after every cursor has consumed or skipped it. The log
// is not a crash-restart boundary.
class DispositionOutbox final {
public:
    ~DispositionOutbox();
    DispositionOutbox(const DispositionOutbox&) = delete;
    DispositionOutbox& operator=(const DispositionOutbox&) = delete;

    [[nodiscard]] static std::unique_ptr<DispositionOutbox> Create(
        std::size_t lane_count,
        std::size_t owner_count,
        std::size_t consumer_count,
        std::size_t records_per_lane,
        std::uint64_t feed_session_epoch,
        std::string* error);

    // Single-writer per lane. Assigns the next LSN and stamps
    // dispatch_fence = LSN. Occurrence records must have a valid owner.
    // Control records must use kOutboxBroadcastOwner.
    [[nodiscard]] bool Append(std::size_t lane,
                              TickDispatch dispatch) noexcept;

    // Independent consumer cursor. Broadcast records are rewritten so
    // output->owner equals the calling owner.
    [[nodiscard]] bool TryRead(std::size_t consumer,
                               std::size_t owner,
                               TickDispatch* output) noexcept;

    [[nodiscard]] std::uint64_t head_lsn(std::size_t lane) const noexcept;
    [[nodiscard]] std::uint64_t consume_next_lsn(
        std::size_t consumer,
        std::size_t owner,
        std::size_t lane) const noexcept;
    [[nodiscard]] std::uint64_t max_consume_lag_lsn(
        std::size_t consumer) const noexcept;

    // Call on the consuming owner thread after a drain/service burst (also
    // after the final quiescent flush). No per-record progress counters needed.
    void PublishProgress(std::size_t consumer,
                         std::size_t owner,
                         DerivedProgress progress) noexcept;

    // Capture computation first, then obtain sink ACK/submission counters and
    // health, and call EvaluateFreshness. This ordering prevents a new computed
    // frontier from being paired with an older empty sink snapshot.
    [[nodiscard]] FreshnessFrontier CaptureFreshness(
        const ContinuityInputs& inputs) const noexcept;

    [[nodiscard]] std::size_t lane_count() const noexcept;
    [[nodiscard]] std::size_t owner_count() const noexcept;
    [[nodiscard]] std::size_t consumer_count() const noexcept;
    [[nodiscard]] std::size_t records_per_lane() const noexcept;
    [[nodiscard]] std::uint64_t feed_session_epoch() const noexcept;
    [[nodiscard]] DispositionOutboxStats stats() const noexcept;

private:
    class Impl;
    explicit DispositionOutbox(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] FreshnessFrontier EvaluateFreshness(
    FreshnessFrontier snapshot,
    const ContinuityInputs& inputs) noexcept;

[[nodiscard]] const char* ContinuityModeName(ContinuityMode mode) noexcept;

}  // namespace l2flow::ingest
