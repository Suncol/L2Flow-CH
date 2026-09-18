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
          lane_poll_(consumer_count * owner_count),
          cursor_stats_(consumer_count * owner_count) {
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
        const std::size_t poll_index = PollIndex(consumer, owner);
        std::size_t& poll = lane_poll_[poll_index];
        CursorStats& cursor_stats = cursor_stats_[poll_index];
        std::uint64_t skipped = 0U;
        const auto publish_skipped = [&cursor_stats, &skipped]() noexcept {
            if (skipped != 0U) {
                cursor_stats.records_skipped.fetch_add(
                    skipped, std::memory_order_relaxed);
                skipped = 0U;
            }
        };
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
                    publish_skipped();
                    return false;
                }
                const TickDispatch& record = slot.dispatch;
                const bool deliver =
                    IsBroadcastOwner(record.owner) || record.owner == owner;
                if (deliver) {
                    // Keep the slot pinned until the complete non-atomic
                    // record has been copied. Publishing the cursor first
                    // would let the producer wrap and overwrite this slot.
                    *output = record;
                    // Publish delivery before releasing the slot. A reporting
                    // thread that observes the advanced cursor must also see
                    // the dispatch awaiting the owner's calculation frontier.
                    cursor_stats.records_read.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                ++next;
                next_cell.store(next, std::memory_order_release);
                if (deliver) {
                    output->owner = static_cast<std::uint32_t>(owner);
                    poll = (lane + 1U) % lane_count_;
                    publish_skipped();
                    return true;
                }
                ++skipped;
            }
        }
        publish_skipped();
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
            max_lag = std::max(max_lag, OwnerConsumeLag(consumer, owner));
        }
        return max_lag;
    }

    void PublishProgress(std::size_t consumer,
                         std::size_t owner,
                         DerivedProgress progress) noexcept {
        if (consumer >= consumer_count_ || owner >= owner_count_ ||
            progress == DerivedProgress::kConsumed) {
            return;
        }
        CursorStats& cursor = cursor_stats_[PollIndex(consumer, owner)];
        const std::uint64_t delivered =
            cursor.records_read.load(std::memory_order_relaxed);
        if (cursor.records_calculated.load(std::memory_order_relaxed) !=
            delivered) {
            cursor.records_calculated.store(delivered,
                                            std::memory_order_release);
        }
        if (progress == DerivedProgress::kSubmitted &&
            cursor.records_submitted.load(std::memory_order_relaxed) !=
                delivered) {
            cursor.records_submitted.store(delivered,
                                           std::memory_order_release);
        }
    }

    [[nodiscard]] FreshnessFrontier CaptureFreshness(
        const ContinuityInputs& inputs) const noexcept {
        FreshnessFrontier frontier{};
        frontier.feed_session_epoch = feed_session_epoch_;
        frontier.published_monotonic_ns = inputs.now_monotonic_ns;
        frontier.lag_warning_lsn = std::max<std::uint64_t>(
            1U, std::min<std::uint64_t>(inputs.catchup_lsn_slack,
                                       records_per_lane_ / 2U));
        for (std::size_t consumer = 0U; consumer < consumer_count_;
             ++consumer) {
            const DerivedFreshness progress = CaptureConsumer(
                consumer, &frontier.max_lag_lsn);
            if (inputs.event.enabled && consumer == 0U) {
                frontier.event = progress;
            }
            if (inputs.kline.enabled &&
                consumer == (inputs.event.enabled ? 1U : 0U)) {
                frontier.kline = progress;
            }
        }
        frontier.outbox_pressure =
            frontier.max_lag_lsn >= frontier.lag_warning_lsn;
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
        for (const CursorStats& cursor : cursor_stats_) {
            result.records_read +=
                cursor.records_read.load(std::memory_order_relaxed);
            result.records_skipped +=
                cursor.records_skipped.load(std::memory_order_relaxed);
        }
        result.append_rejected_full =
            stats_append_rejected_full_.load(std::memory_order_relaxed);
        return result;
    }

private:
    struct Slot final {
        std::atomic<std::uint64_t> sequence{0U};
        TickDispatch dispatch{};
    };

    // TryRead already requires exclusive ownership of one (consumer, owner)
    // cursor because lane_poll_ is mutable non-atomic state. Keep its hot
    // observability counters on the same exclusive shard and a separate cache
    // line, then aggregate only on the cold stats() path.
    struct alignas(64) CursorStats final {
        std::atomic<std::uint64_t> records_read{0U};
        std::atomic<std::uint64_t> records_skipped{0U};
        std::atomic<std::uint64_t> records_calculated{0U};
        std::atomic<std::uint64_t> records_submitted{0U};
    };

    [[nodiscard]] std::uint64_t OwnerConsumeLag(
        std::size_t consumer,
        std::size_t owner) const noexcept {
        std::uint64_t lag = 0U;
        for (std::size_t lane = 0U; lane < lane_count_; ++lane) {
            const auto head = head_lsn_[lane].load(std::memory_order_acquire);
            const auto next = cursor_next_[CursorIndex(consumer, owner, lane)]
                                  .load(std::memory_order_acquire);
            const auto consumed = next == 0U ? 0U : next - 1U;
            if (head > consumed) {
                lag = std::max(lag, head - consumed);
            }
        }
        return lag;
    }

    [[nodiscard]] DerivedFreshness CaptureConsumer(
        std::size_t consumer,
        std::uint64_t* max_lag) const noexcept {
        DerivedFreshness result{true, true, true, false};
        for (std::size_t owner = 0U; owner < owner_count_; ++owner) {
            const CursorStats& cursor =
                cursor_stats_[PollIndex(consumer, owner)];
            // Read downstream progress first. New work between these reads
            // may conservatively report catch-up, but cannot look completed.
            const auto submitted =
                cursor.records_submitted.load(std::memory_order_acquire);
            const auto calculated =
                cursor.records_calculated.load(std::memory_order_acquire);
            const auto lag = OwnerConsumeLag(consumer, owner);
            *max_lag = std::max(*max_lag, lag);
            result.consumed = result.consumed && lag == 0U;
            const auto delivered =
                cursor.records_read.load(std::memory_order_relaxed);
            result.calculated = result.calculated && calculated == delivered;
            result.submitted = result.submitted && submitted == delivered;
        }
        result.calculated = result.calculated && result.consumed;
        result.submitted = result.submitted && result.calculated;
        return result;
    }

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
    std::vector<CursorStats> cursor_stats_;
    std::atomic<std::uint64_t> stats_appended_{0U};
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

void DispositionOutbox::PublishProgress(std::size_t consumer,
                                      std::size_t owner,
                                      DerivedProgress progress) noexcept {
    impl_->PublishProgress(consumer, owner, progress);
}

FreshnessFrontier DispositionOutbox::CaptureFreshness(
    const ContinuityInputs& inputs) const noexcept {
    return impl_->CaptureFreshness(inputs);
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

FreshnessFrontier EvaluateFreshness(
    FreshnessFrontier snapshot,
    const ContinuityInputs& inputs) noexcept {
    const auto complete = [&inputs](DerivedFreshness* progress,
                                    const DerivedContinuityInputs& plane) {
        if (!plane.enabled) {
            *progress = {};
            return false;
        }
        progress->calculated = progress->calculated && plane.healthy &&
            !inputs.fatal;
        progress->submitted = progress->submitted && progress->calculated &&
            plane.sink_healthy;
        progress->acknowledged = progress->submitted &&
            plane.revision_batches_acked == plane.revision_batches_submitted;
        return progress->calculated && plane.sink_healthy;
    };
    snapshot.event_authoritative = complete(&snapshot.event, inputs.event);
    snapshot.kline_authoritative = complete(&snapshot.kline, inputs.kline);
    if (inputs.fatal) {
        snapshot.mode = ContinuityMode::kFatalContinuity;
    } else if (snapshot.outbox_pressure ||
               (inputs.event.enabled && !snapshot.event.acknowledged) ||
               (inputs.kline.enabled && !snapshot.kline.acknowledged)) {
        snapshot.mode = ContinuityMode::kDerivedCatchup;
    } else {
        snapshot.mode = ContinuityMode::kCaughtUp;
    }
    return snapshot;
}

const char* ContinuityModeName(ContinuityMode mode) noexcept {
    switch (mode) {
        case ContinuityMode::kCaughtUp:
            return "CAUGHT_UP";
        case ContinuityMode::kDerivedCatchup:
            return "DERIVED_CATCHUP";
        case ContinuityMode::kFatalContinuity:
            return "FATAL_CONTINUITY";
    }
    return "UNKNOWN";
}

}  // namespace l2flow::ingest
