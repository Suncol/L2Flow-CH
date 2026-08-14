#include "l2flow/ingest/outbox.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

namespace l2flow::ingest {
namespace {

[[nodiscard]] bool IsPowerOfTwo(std::size_t value) noexcept {
    return value != 0U && (value & (value - 1U)) == 0U;
}

[[nodiscard]] bool RoundUpPowerOfTwo(std::size_t requested,
                                     std::size_t* output) noexcept {
    if (output == nullptr || requested < 2U ||
        requested > (std::numeric_limits<std::size_t>::max() >> 1U)) {
        return false;
    }
    std::size_t value = 1U;
    while (value < requested) {
        value <<= 1U;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool IsBroadcastKind(TickDispatchKind kind) noexcept {
    return kind == TickDispatchKind::kGapOpen ||
           kind == TickDispatchKind::kChannelSeal;
}

[[nodiscard]] bool IsBroadcastOwner(std::uint32_t owner) noexcept {
    return owner == kOutboxBroadcastOwner;
}

}  // namespace

class DispositionOutbox::Impl final {
public:
    Impl(std::size_t lane_count,
         std::size_t owner_count,
         std::size_t consumer_count,
         std::size_t records_per_lane,
         std::uint64_t feed_session_epoch)
        : lane_count_(lane_count),
          owner_count_(owner_count),
          consumer_count_(consumer_count),
          records_per_lane_(records_per_lane),
          mask_(records_per_lane - 1U),
          feed_session_epoch_(feed_session_epoch),
          slots_(lane_count * records_per_lane),
          head_lsn_(lane_count),
          cursor_next_(consumer_count * owner_count * lane_count),
          lane_poll_(consumer_count * owner_count) {
        for (std::size_t index = 0U; index < slots_.size(); ++index) {
            slots_[index].sequence.store(0U, std::memory_order_relaxed);
        }
        for (std::size_t index = 0U; index < head_lsn_.size(); ++index) {
            head_lsn_[index].store(0U, std::memory_order_relaxed);
        }
        for (std::size_t index = 0U; index < cursor_next_.size(); ++index) {
            cursor_next_[index].store(1U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool Append(std::size_t lane,
                              TickDispatch dispatch) noexcept {
        if (lane >= lane_count_) {
            return false;
        }
        if (IsBroadcastKind(dispatch.kind)) {
            if (!IsBroadcastOwner(dispatch.owner)) {
                return false;
            }
        } else if (dispatch.owner >= owner_count_) {
            return false;
        }

        const std::uint64_t previous =
            head_lsn_[lane].load(std::memory_order_relaxed);
        if (previous == std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        const std::uint64_t lsn = previous + 1U;
        if (lsn - MinimumConsumeNext(lane) >= records_per_lane_) {
            stats_append_rejected_full_.fetch_add(
                1U, std::memory_order_relaxed);
            return false;
        }

        dispatch.outbox_lane = static_cast<std::uint16_t>(lane);
        dispatch.outbox_lsn = lsn;
        dispatch.dispatch_fence = lsn;

        Slot& slot = SlotAt(lane, lsn);
        slot.dispatch = dispatch;
        slot.sequence.store(lsn, std::memory_order_release);
        head_lsn_[lane].store(lsn, std::memory_order_release);
        stats_appended_.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool TryRead(std::size_t consumer,
                               std::size_t owner,
                               TickDispatch* output) noexcept {
        if (output == nullptr || consumer >= consumer_count_ ||
            owner >= owner_count_) {
            return false;
        }
        std::size_t& poll = lane_poll_[PollIndex(consumer, owner)];
        for (std::size_t count = 0U; count < lane_count_; ++count) {
            const std::size_t lane = (poll + count) % lane_count_;
            std::atomic<std::uint64_t>& next_cell =
                cursor_next_[CursorIndex(consumer, owner, lane)];
            std::uint64_t next = next_cell.load(std::memory_order_relaxed);
            const std::uint64_t head =
                head_lsn_[lane].load(std::memory_order_acquire);
            while (next <= head) {
                const Slot& slot = SlotAt(lane, next);
                if (slot.sequence.load(std::memory_order_acquire) != next) {
                    return false;
                }
                const TickDispatch& record = slot.dispatch;
                const bool deliver =
                    IsBroadcastOwner(record.owner) || record.owner == owner;
                ++next;
                next_cell.store(next, std::memory_order_release);
                if (deliver) {
                    *output = record;
                    output->owner = static_cast<std::uint32_t>(owner);
                    poll = (lane + 1U) % lane_count_;
                    stats_read_.fetch_add(1U, std::memory_order_relaxed);
                    return true;
                }
                stats_skipped_.fetch_add(1U, std::memory_order_relaxed);
            }
        }
        return false;
    }

    [[nodiscard]] std::uint64_t head_lsn(std::size_t lane) const noexcept {
        if (lane >= lane_count_) {
            return 0U;
        }
        return head_lsn_[lane].load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t consume_next_lsn(
        std::size_t consumer,
        std::size_t owner,
        std::size_t lane) const noexcept {
        if (consumer >= consumer_count_ || owner >= owner_count_ ||
            lane >= lane_count_) {
            return 0U;
        }
        return cursor_next_[CursorIndex(consumer, owner, lane)].load(
            std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t max_consume_lag_lsn(
        std::size_t consumer) const noexcept {
        if (consumer >= consumer_count_) {
            return 0U;
        }
        std::uint64_t max_lag = 0U;
        for (std::size_t owner = 0U; owner < owner_count_; ++owner) {
            for (std::size_t lane = 0U; lane < lane_count_; ++lane) {
                const std::uint64_t head =
                    head_lsn_[lane].load(std::memory_order_acquire);
                const std::uint64_t next = cursor_next_[CursorIndex(
                                               consumer, owner, lane)]
                                               .load(std::memory_order_acquire);
                const std::uint64_t consumed = next == 0U ? 0U : next - 1U;
                if (head > consumed) {
                    max_lag = std::max(max_lag, head - consumed);
                }
            }
        }
        return max_lag;
    }

    [[nodiscard]] FreshnessFrontier EvaluateFreshness(
        const ContinuityInputs& inputs) const noexcept {
        FreshnessFrontier frontier{};
        frontier.feed_session_epoch = feed_session_epoch_;
        frontier.published_monotonic_ns = inputs.now_monotonic_ns;
        if (inputs.fatal) {
            frontier.mode = ContinuityMode::kFatalContinuity;
            return frontier;
        }
        const std::uint64_t event_lag =
            inputs.event_enabled ? max_consume_lag_lsn(0U) : 0U;
        const std::uint64_t kline_lag = inputs.kline_enabled
            ? max_consume_lag_lsn(inputs.event_enabled ? 1U : 0U)
            : 0U;
        frontier.max_lag_lsn = std::max(event_lag, kline_lag);
        const bool event_behind = inputs.event_enabled &&
            (!inputs.event_healthy || inputs.event_pending ||
             event_lag > inputs.catchup_lsn_slack);
        const bool kline_behind = inputs.kline_enabled &&
            (!inputs.kline_healthy || inputs.kline_pending ||
             kline_lag > inputs.catchup_lsn_slack);
        const bool stale_elapsed =
            inputs.stale_timeout_ns != 0U &&
            inputs.derived_unhealthy_elapsed_ns >= inputs.stale_timeout_ns;
        const bool derived_unhealthy =
            (inputs.event_enabled && !inputs.event_healthy) ||
            (inputs.kline_enabled && !inputs.kline_healthy);
        if (derived_unhealthy && stale_elapsed) {
            frontier.mode = ContinuityMode::kRawOnlyStale;
        } else if (event_behind || kline_behind) {
            frontier.mode = ContinuityMode::kDerivedCatchup;
        } else {
            frontier.mode = ContinuityMode::kCaughtUp;
        }
        frontier.event_authoritative =
            inputs.event_enabled &&
            frontier.mode == ContinuityMode::kCaughtUp;
        frontier.kline_authoritative =
            inputs.kline_enabled &&
            frontier.mode == ContinuityMode::kCaughtUp;
        return frontier;
    }

    [[nodiscard]] std::size_t lane_count() const noexcept {
        return lane_count_;
    }
    [[nodiscard]] std::size_t owner_count() const noexcept {
        return owner_count_;
    }
    [[nodiscard]] std::size_t consumer_count() const noexcept {
        return consumer_count_;
    }
    [[nodiscard]] std::size_t records_per_lane() const noexcept {
        return records_per_lane_;
    }
    [[nodiscard]] std::uint64_t feed_session_epoch() const noexcept {
        return feed_session_epoch_;
    }

    [[nodiscard]] DispositionOutboxStats stats() const noexcept {
        DispositionOutboxStats result{};
        result.records_appended =
            stats_appended_.load(std::memory_order_relaxed);
        result.records_read = stats_read_.load(std::memory_order_relaxed);
        result.records_skipped =
            stats_skipped_.load(std::memory_order_relaxed);
        result.append_rejected_full =
            stats_append_rejected_full_.load(std::memory_order_relaxed);
        return result;
    }

private:
    struct Slot final {
        std::atomic<std::uint64_t> sequence{0U};
        TickDispatch dispatch{};
    };

    [[nodiscard]] std::size_t SlotOffset(
        std::size_t lane,
        std::uint64_t lsn) const noexcept {
        return lane * records_per_lane_ +
               (static_cast<std::size_t>(lsn - 1U) & mask_);
    }

    [[nodiscard]] Slot& SlotAt(std::size_t lane, std::uint64_t lsn) noexcept {
        return slots_[SlotOffset(lane, lsn)];
    }

    [[nodiscard]] const Slot& SlotAt(
        std::size_t lane,
        std::uint64_t lsn) const noexcept {
        return slots_[SlotOffset(lane, lsn)];
    }

    [[nodiscard]] std::size_t CursorIndex(std::size_t consumer,
                                          std::size_t owner,
                                          std::size_t lane) const noexcept {
        return ((consumer * owner_count_) + owner) * lane_count_ + lane;
    }

    [[nodiscard]] std::size_t PollIndex(std::size_t consumer,
                                        std::size_t owner) const noexcept {
        return consumer * owner_count_ + owner;
    }

    [[nodiscard]] std::uint64_t MinimumConsumeNext(
        std::size_t lane) const noexcept {
        std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
        for (std::size_t consumer = 0U; consumer < consumer_count_;
             ++consumer) {
            for (std::size_t owner = 0U; owner < owner_count_; ++owner) {
                const std::uint64_t next =
                    cursor_next_[CursorIndex(consumer, owner, lane)].load(
                        std::memory_order_acquire);
                minimum = std::min(minimum, next);
            }
        }
        return minimum;
    }

    std::size_t lane_count_ = 0U;
    std::size_t owner_count_ = 0U;
    std::size_t consumer_count_ = 0U;
    std::size_t records_per_lane_ = 0U;
    std::size_t mask_ = 0U;
    std::uint64_t feed_session_epoch_ = 0U;
    std::vector<Slot> slots_;
    std::vector<std::atomic<std::uint64_t>> head_lsn_;
    std::vector<std::atomic<std::uint64_t>> cursor_next_;
    std::vector<std::size_t> lane_poll_;
    std::atomic<std::uint64_t> stats_appended_{0U};
    std::atomic<std::uint64_t> stats_read_{0U};
    std::atomic<std::uint64_t> stats_skipped_{0U};
    std::atomic<std::uint64_t> stats_append_rejected_full_{0U};
};

DispositionOutbox::DispositionOutbox(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DispositionOutbox::~DispositionOutbox() = default;

std::unique_ptr<DispositionOutbox> DispositionOutbox::Create(
    std::size_t lane_count,
    std::size_t owner_count,
    std::size_t consumer_count,
    std::size_t records_per_lane,
    std::uint64_t feed_session_epoch,
    std::string* error) {
    const auto fail = [error](const char* message) {
        if (error != nullptr) {
            *error = message;
        }
        return nullptr;
    };
    if (lane_count == 0U || owner_count == 0U || consumer_count == 0U ||
        consumer_count > kMaximumTickConsumers ||
        feed_session_epoch == 0U) {
        return fail("outbox lane, owner, consumer, or epoch is invalid");
    }
    if (lane_count > static_cast<std::size_t>(
                         std::numeric_limits<std::uint16_t>::max())) {
        return fail("outbox lane count exceeds outbox_lane width");
    }
    if (owner_count >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max() - 1U)) {
        return fail("outbox owner count collides with broadcast owner");
    }
    std::size_t capacity = 0U;
    if (!RoundUpPowerOfTwo(records_per_lane, &capacity) ||
        !IsPowerOfTwo(capacity)) {
        return fail("outbox records_per_lane must be at least 2");
    }
    if (lane_count > std::numeric_limits<std::size_t>::max() / capacity) {
        return fail("outbox storage size overflows size_t");
    }
    try {
        auto impl = std::make_unique<Impl>(
            lane_count, owner_count, consumer_count, capacity,
            feed_session_epoch);
        return std::unique_ptr<DispositionOutbox>(
            new DispositionOutbox(std::move(impl)));
    } catch (...) {
        return fail("outbox allocation failed");
    }
}

bool DispositionOutbox::Append(std::size_t lane,
                               TickDispatch dispatch) noexcept {
    return impl_->Append(lane, std::move(dispatch));
}

bool DispositionOutbox::TryRead(std::size_t consumer,
                                std::size_t owner,
                                TickDispatch* output) noexcept {
    return impl_->TryRead(consumer, owner, output);
}

std::uint64_t DispositionOutbox::head_lsn(std::size_t lane) const noexcept {
    return impl_->head_lsn(lane);
}

std::uint64_t DispositionOutbox::consume_next_lsn(
    std::size_t consumer,
    std::size_t owner,
    std::size_t lane) const noexcept {
    return impl_->consume_next_lsn(consumer, owner, lane);
}

std::uint64_t DispositionOutbox::max_consume_lag_lsn(
    std::size_t consumer) const noexcept {
    return impl_->max_consume_lag_lsn(consumer);
}

FreshnessFrontier DispositionOutbox::EvaluateFreshness(
    const ContinuityInputs& inputs) const noexcept {
    return impl_->EvaluateFreshness(inputs);
}

std::size_t DispositionOutbox::lane_count() const noexcept {
    return impl_->lane_count();
}

std::size_t DispositionOutbox::owner_count() const noexcept {
    return impl_->owner_count();
}

std::size_t DispositionOutbox::consumer_count() const noexcept {
    return impl_->consumer_count();
}

std::size_t DispositionOutbox::records_per_lane() const noexcept {
    return impl_->records_per_lane();
}

std::uint64_t DispositionOutbox::feed_session_epoch() const noexcept {
    return impl_->feed_session_epoch();
}

DispositionOutboxStats DispositionOutbox::stats() const noexcept {
    return impl_->stats();
}

const char* ContinuityModeName(ContinuityMode mode) noexcept {
    switch (mode) {
        case ContinuityMode::kCaughtUp:
            return "CAUGHT_UP";
        case ContinuityMode::kDerivedCatchup:
            return "DERIVED_CATCHUP";
        case ContinuityMode::kRawOnlyStale:
            return "RAW_ONLY_STALE";
        case ContinuityMode::kFatalContinuity:
            return "FATAL_CONTINUITY";
    }
    return "UNKNOWN";
}

}  // namespace l2flow::ingest
