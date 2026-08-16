#pragma once

#include "l2flow/outbox/wal.h"

#include <cstdint>
#include <mutex>

namespace l2flow::outbox {

struct ContinuityConfig final {
    std::uint64_t derived_stale_after_ns = 30ULL * 1'000'000'000ULL;
};

struct ConsumerHealth final {
    bool raw = true;
    bool event = true;
    bool kline = true;
};

struct FreshnessSnapshot final {
    ContinuityState state = ContinuityState::kDerivedCatchup;
    std::uint64_t frontier_id = 0U;
    WalPosition barrier{};
    WalPosition canonical_tail{};
    WalPosition raw_cursor{};
    WalPosition event_cursor{};
    WalPosition kline_cursor{};
    std::uint64_t observed_monotonic_ns = 0U;
    bool event_current_authoritative = false;
    bool kline_current_authoritative = false;
};

class ContinuityController final {
public:
    ContinuityController(ContinuityConfig config,
                         const DurableOutbox* outbox) noexcept;

    [[nodiscard]] FreshnessSnapshot Evaluate(
        ConsumerHealth health,
        std::uint64_t now_monotonic_ns) noexcept;

private:
    ContinuityConfig config_{};
    const DurableOutbox* outbox_ = nullptr;
    std::mutex mutex_;
    WalPosition previous_event_cursor_{};
    WalPosition previous_kline_cursor_{};
    std::uint64_t last_event_progress_ns_ = 0U;
    std::uint64_t last_kline_progress_ns_ = 0U;
    std::uint64_t event_unhealthy_since_ns_ = 0U;
    std::uint64_t kline_unhealthy_since_ns_ = 0U;
};

}  // namespace l2flow::outbox
