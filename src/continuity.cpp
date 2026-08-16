#include "l2flow/outbox/continuity.h"

#include <algorithm>

namespace l2flow::outbox {

ContinuityController::ContinuityController(
    ContinuityConfig config,
    const DurableOutbox* outbox) noexcept
    : config_(config), outbox_(outbox) {}

FreshnessSnapshot ContinuityController::Evaluate(
    ConsumerHealth health,
    std::uint64_t now_monotonic_ns) noexcept {
    FreshnessSnapshot result{};
    result.observed_monotonic_ns = now_monotonic_ns;
    if (outbox_ == nullptr) {
        result.state = ContinuityState::kFatalContinuity;
        return result;
    }

    const DurableOutboxStats outbox_stats = outbox_->stats();
    result.canonical_tail = outbox_stats.durable_tail;
    result.raw_cursor = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kRaw)].contiguous;
    result.event_cursor = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kEvent)].contiguous;
    result.kline_cursor = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kKLine)].contiguous;
    result.frontier_id = outbox_stats.latest_barrier.frontier_id;
    result.barrier = outbox_stats.latest_barrier_position;

    std::lock_guard<std::mutex> lock(mutex_);
    if (result.event_cursor > previous_event_cursor_) {
        last_event_progress_ns_ = now_monotonic_ns;
        previous_event_cursor_ = result.event_cursor;
    } else if (last_event_progress_ns_ == 0U) {
        last_event_progress_ns_ = now_monotonic_ns;
    }
    if (result.kline_cursor > previous_kline_cursor_) {
        last_kline_progress_ns_ = now_monotonic_ns;
        previous_kline_cursor_ = result.kline_cursor;
    } else if (last_kline_progress_ns_ == 0U) {
        last_kline_progress_ns_ = now_monotonic_ns;
    }

    if (health.event) {
        event_unhealthy_since_ns_ = 0U;
    } else if (event_unhealthy_since_ns_ == 0U) {
        event_unhealthy_since_ns_ = now_monotonic_ns;
    }
    if (health.kline) {
        kline_unhealthy_since_ns_ = 0U;
    } else if (kline_unhealthy_since_ns_ == 0U) {
        kline_unhealthy_since_ns_ = now_monotonic_ns;
    }

    if (!outbox_->healthy()) {
        result.state = ContinuityState::kFatalContinuity;
        return result;
    }

    const bool raw_enabled = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kRaw)].enabled;
    const bool event_enabled = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kEvent)].enabled;
    const bool kline_enabled = outbox_stats.consumers[
        static_cast<std::size_t>(ConsumerKind::kKLine)].enabled;
    const bool have_barrier = result.barrier.lsn != 0U;
    const bool raw_at_barrier =
        !raw_enabled ||
        (have_barrier && result.raw_cursor >= result.barrier);
    const bool event_at_barrier =
        !event_enabled ||
        (have_barrier && result.event_cursor >= result.barrier);
    const bool kline_at_barrier =
        !kline_enabled ||
        (have_barrier && result.kline_cursor >= result.barrier);
    const bool derived_healthy =
        (!event_enabled || health.event) &&
        (!kline_enabled || health.kline);
    const bool derived_at_barrier = event_at_barrier && kline_at_barrier;
    const bool event_stalled =
        event_enabled && !event_at_barrier &&
        now_monotonic_ns >= last_event_progress_ns_ &&
        now_monotonic_ns - last_event_progress_ns_ >=
            config_.derived_stale_after_ns;
    const bool kline_stalled =
        kline_enabled && !kline_at_barrier &&
        now_monotonic_ns >= last_kline_progress_ns_ &&
        now_monotonic_ns - last_kline_progress_ns_ >=
            config_.derived_stale_after_ns;
    const bool event_unavailable_too_long =
        event_enabled && !health.event &&
        now_monotonic_ns >= event_unhealthy_since_ns_ &&
        now_monotonic_ns - event_unhealthy_since_ns_ >=
            config_.derived_stale_after_ns;
    const bool kline_unavailable_too_long =
        kline_enabled && !health.kline &&
        now_monotonic_ns >= kline_unhealthy_since_ns_ &&
        now_monotonic_ns - kline_unhealthy_since_ns_ >=
            config_.derived_stale_after_ns;
    const bool derived_stale = event_stalled || kline_stalled ||
                               event_unavailable_too_long ||
                               kline_unavailable_too_long;

    result.event_current_authoritative =
        event_enabled && health.event && have_barrier && event_at_barrier;
    result.kline_current_authoritative =
        kline_enabled && health.kline && have_barrier && kline_at_barrier;

    if (derived_stale) {
        result.state = ContinuityState::kRawOnlyStale;
    } else if (!derived_healthy || !derived_at_barrier) {
        result.state = ContinuityState::kDerivedCatchup;
    } else if ((raw_enabled && !health.raw) || !raw_at_barrier) {
        result.state = ContinuityState::kRawCatchup;
    } else {
        result.state = ContinuityState::kNormal;
    }
    return result;
}

}  // namespace l2flow::outbox
