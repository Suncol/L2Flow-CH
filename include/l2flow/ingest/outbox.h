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
    kRawOnlyStale,
    kFatalContinuity,
};

struct FreshnessFrontier final {
    ContinuityMode mode = ContinuityMode::kCaughtUp;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t published_monotonic_ns = 0U;
    std::uint64_t max_lag_lsn = 0U;
    bool event_authoritative = false;
    bool kline_authoritative = false;
};

struct ContinuityInputs final {
    bool fatal = false;
    bool event_enabled = false;
    bool kline_enabled = false;
    bool event_healthy = true;
    bool kline_healthy = true;
    bool event_pending = false;
    bool kline_pending = false;
    std::uint64_t now_monotonic_ns = 0U;
    std::uint64_t catchup_lsn_slack = 65'536U;
    std::uint64_t stale_timeout_ns = UINT64_C(2'000'000'000);
    std::uint64_t derived_unhealthy_elapsed_ns = 0U;
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

    [[nodiscard]] FreshnessFrontier EvaluateFreshness(
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

[[nodiscard]] const char* ContinuityModeName(ContinuityMode mode) noexcept;

}  // namespace l2flow::ingest
