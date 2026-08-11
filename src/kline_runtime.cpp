#include "l2flow/kline/runtime.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <mutex>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace l2flow::kline {
namespace {

struct OccurrenceKey final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t ingress_sequence = 0U;
    ingest::CanonicalKind kind = ingest::CanonicalKind::kShanghaiTick;

    friend constexpr bool operator==(const OccurrenceKey&,
                                     const OccurrenceKey&) = default;
};

[[nodiscard]] std::uint64_t Mix(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

[[nodiscard]] std::size_t HashOccurrence(
    const OccurrenceKey& key) noexcept {
    std::uint64_t value = Mix(key.feed_session_epoch);
    value ^= Mix(key.ingress_sequence + UINT64_C(0x9e3779b97f4a7c15));
    value ^= Mix(static_cast<std::uint64_t>(key.kind) +
                 UINT64_C(0x517cc1b727220a95));
    if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
        value ^= value >> 32U;
    }
    return static_cast<std::size_t>(value);
}

enum class JoinSlotState : std::uint8_t {
    kEmpty = 0U,
    kAckOnly,
    kProjectOnly,
    kRejectOnly,
};

enum class JoinResult : std::uint8_t {
    kAckFirst = 0U,
    kDispositionFirst,
    kProjectResolved,
    kRejectResolved,
    kDuplicateSide,
    kCapacityExhausted,
};

class OccurrenceJoin final {
public:
    explicit OccurrenceJoin(std::size_t maximum_entries)
        : slots_(SlotCount(maximum_entries)),
          mask_(slots_.size() - 1U),
          maximum_entries_(maximum_entries) {}

    [[nodiscard]] JoinResult ObserveAck(const OccurrenceKey& key) noexcept {
        const std::size_t position = Find(key);
        if (position == slots_.size()) {
            return Insert(key, JoinSlotState::kAckOnly)
                ? JoinResult::kAckFirst
                : JoinResult::kCapacityExhausted;
        }
        switch (slots_[position].state) {
            case JoinSlotState::kAckOnly:
                return JoinResult::kDuplicateSide;
            case JoinSlotState::kProjectOnly:
                Erase(position);
                return JoinResult::kProjectResolved;
            case JoinSlotState::kRejectOnly:
                Erase(position);
                return JoinResult::kRejectResolved;
            case JoinSlotState::kEmpty:
                break;
        }
        return JoinResult::kDuplicateSide;
    }

    [[nodiscard]] JoinResult ObserveDisposition(
        const OccurrenceKey& key,
        bool project) noexcept {
        const std::size_t position = Find(key);
        if (position == slots_.size()) {
            return Insert(key, project ? JoinSlotState::kProjectOnly
                                       : JoinSlotState::kRejectOnly)
                ? JoinResult::kDispositionFirst
                : JoinResult::kCapacityExhausted;
        }
        if (slots_[position].state != JoinSlotState::kAckOnly) {
            return JoinResult::kDuplicateSide;
        }
        Erase(position);
        return project ? JoinResult::kProjectResolved
                       : JoinResult::kRejectResolved;
    }

private:
    struct Slot final {
        OccurrenceKey key{};
        JoinSlotState state = JoinSlotState::kEmpty;
    };

    [[nodiscard]] static std::size_t SlotCount(std::size_t maximum_entries) {
        if (maximum_entries == 0U ||
            maximum_entries >
                std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::length_error("KLine occurrence join capacity is invalid");
        }
        const std::size_t required = maximum_entries * 2U;
        std::size_t count = 1U;
        while (count < required) {
            if (count > std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::length_error(
                    "KLine occurrence join slot count overflow");
            }
            count <<= 1U;
        }
        return count;
    }

    [[nodiscard]] std::size_t Find(const OccurrenceKey& key) const noexcept {
        std::size_t position = HashOccurrence(key) & mask_;
        for (std::size_t probes = 0U; probes < slots_.size(); ++probes) {
            const Slot& slot = slots_[position];
            if (slot.state == JoinSlotState::kEmpty) {
                return slots_.size();
            }
            if (slot.key == key) {
                return position;
            }
            position = (position + 1U) & mask_;
        }
        return slots_.size();
    }

    [[nodiscard]] bool Insert(const OccurrenceKey& key,
                              JoinSlotState state) noexcept {
        if (entries_ >= maximum_entries_) {
            return false;
        }
        std::size_t position = HashOccurrence(key) & mask_;
        while (slots_[position].state != JoinSlotState::kEmpty) {
            position = (position + 1U) & mask_;
        }
        slots_[position] = Slot{key, state};
        ++entries_;
        return true;
    }

    void Erase(std::size_t position) noexcept {
        std::size_t hole = position;
        std::size_t current = (hole + 1U) & mask_;
        while (slots_[current].state != JoinSlotState::kEmpty) {
            const std::size_t home = HashOccurrence(slots_[current].key) & mask_;
            const std::size_t current_distance = (current - home) & mask_;
            const std::size_t hole_distance = (hole - home) & mask_;
            if (hole_distance < current_distance) {
                slots_[hole] = slots_[current];
                hole = current;
            }
            current = (current + 1U) & mask_;
        }
        slots_[hole] = Slot{};
        --entries_;
    }

    std::vector<Slot> slots_;
    std::size_t mask_ = 0U;
    std::size_t maximum_entries_ = 0U;
    std::size_t entries_ = 0U;
};

class RawAckInbox final {
public:
    explicit RawAckInbox(std::size_t maximum_entries)
        : physical_capacity_(std::max<std::size_t>(maximum_entries, 2U)),
          maximum_entries_(maximum_entries),
          slots_(std::make_unique<Slot[]>(physical_capacity_)) {
        for (std::size_t index = 0U; index < physical_capacity_; ++index) {
            slots_[index].sequence.store(index, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool TryPush(const OccurrenceKey& key) noexcept {
        std::size_t entries = entries_.load(std::memory_order_relaxed);
        do {
            if (entries >= maximum_entries_) {
                return false;
            }
        } while (!entries_.compare_exchange_weak(
            entries, entries + 1U, std::memory_order_acq_rel,
            std::memory_order_relaxed));

        const std::size_t position = enqueue_position_.fetch_add(
            1U, std::memory_order_relaxed);
        Slot& slot = slots_[position % physical_capacity_];
        while (slot.sequence.load(std::memory_order_acquire) != position) {
            std::atomic_signal_fence(std::memory_order_seq_cst);
        }
        slot.key = key;
        slot.sequence.store(position + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(OccurrenceKey* key) noexcept {
        Slot& slot = slots_[dequeue_position_ % physical_capacity_];
        if (slot.sequence.load(std::memory_order_acquire) !=
            dequeue_position_ + 1U) {
            return false;
        }
        *key = slot.key;
        slot.sequence.store(dequeue_position_ + physical_capacity_,
                            std::memory_order_release);
        ++dequeue_position_;
        entries_.fetch_sub(1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.load(std::memory_order_acquire);
    }

private:
    struct Slot final {
        std::atomic<std::size_t> sequence{0U};
        OccurrenceKey key{};
    };

    std::size_t physical_capacity_ = 0U;
    std::size_t maximum_entries_ = 0U;
    std::unique_ptr<Slot[]> slots_;
    std::atomic<std::size_t> enqueue_position_{0U};
    std::atomic<std::size_t> entries_{0U};
    std::size_t dequeue_position_ = 0U;
};

struct AtomicRuntimeStats final {
    std::atomic<std::uint64_t> ordered_dispositions_received{0U};
    std::atomic<std::uint64_t> hole_fill_dispositions_received{0U};
    std::atomic<std::uint64_t> rejected_dispositions_received{0U};
    std::atomic<std::uint64_t> gap_open_controls_received{0U};
    std::atomic<std::uint64_t> channel_seal_controls_received{0U};
    std::atomic<std::uint64_t> raw_tick_acks_received{0U};
    std::atomic<std::uint64_t> occurrence_join_entries{0U};
    std::atomic<std::uint64_t> occurrence_join_high_water{0U};
    std::atomic<std::uint64_t> occurrence_ack_first{0U};
    std::atomic<std::uint64_t> occurrence_disposition_first{0U};
    std::atomic<std::uint64_t> occurrence_projects_resolved{0U};
    std::atomic<std::uint64_t> occurrence_rejections_resolved{0U};
    std::atomic<std::uint64_t> occurrence_duplicate_sides{0U};
    std::atomic<std::uint64_t> micro_batches_applied{0U};
    std::atomic<std::uint64_t> facts_in_micro_batches{0U};
    std::atomic<std::uint64_t> micro_batch_rows_max{0U};
    std::atomic<std::uint64_t> micro_batch_source_age_ns_max{0U};
    std::atomic<std::uint64_t> row_limit_flushes{0U};
    std::atomic<std::uint64_t> timer_flushes{0U};
    std::atomic<std::uint64_t> explicit_flushes{0U};
    std::atomic<std::uint64_t> source_conflicts{0U};
    std::atomic<std::uint64_t> invalid_inputs{0U};
};

void PublishMaximum(std::atomic<std::uint64_t>* target,
                    std::uint64_t value) noexcept {
    std::uint64_t current = target->load(std::memory_order_relaxed);
    while (current < value &&
           !target->compare_exchange_weak(
               current, value, std::memory_order_relaxed)) {
    }
}

enum class MicroBatchFlushReason : std::uint8_t {
    kRowLimit = 0U,
    kTimer,
    kExplicit,
};

void AddWorkerStats(KLineWorkerStats* destination,
                    const KLineWorkerStats& source) noexcept {
    destination->facts_journaled += source.facts_journaled;
    destination->trades_projected += source.trades_projected;
    destination->duplicate_facts += source.duplicate_facts;
    destination->source_conflicts += source.source_conflicts;
    destination->invalid_facts += source.invalid_facts;
    destination->invalid_trade_exchange_times +=
        source.invalid_trade_exchange_times;
    destination->bars_created += source.bars_created;
    destination->bars_updated += source.bars_updated;
    destination->revisions_created += source.revisions_created;
    destination->pending_raw_commits += source.pending_raw_commits;
    destination->pending_revision_rows += source.pending_revision_rows;
    destination->pending_revision_rows_high_watermark = std::max(
        destination->pending_revision_rows_high_watermark,
        source.pending_revision_rows_high_watermark);
    destination->pending_revision_bytes += source.pending_revision_bytes;
    destination->pending_revision_bytes_high_watermark = std::max(
        destination->pending_revision_bytes_high_watermark,
        source.pending_revision_bytes_high_watermark);
    destination->acknowledged_raw_dependencies +=
        source.acknowledged_raw_dependencies;
    destination->revision_batches_submitted +=
        source.revision_batches_submitted;
}

}  // namespace

class KLineRuntime::Impl final {
public:
    struct OwnerState final {
        OwnerState(std::size_t join_entries, std::size_t ack_backlog)
            : occurrence_join(join_entries), ack_inbox(ack_backlog) {
            worker_ack_drain.reserve(ack_backlog);
        }

        std::unique_ptr<KLineWorker> worker;
        std::vector<KLineInput> active;
        std::uint64_t active_started_ns = 0U;
        OccurrenceJoin occurrence_join;
        RawAckInbox ack_inbox;
        std::vector<RawTickDependency> worker_ack_drain;
    };

    Impl(KLineRuntimeConfig config,
         std::vector<std::unique_ptr<OwnerState>> owners)
        : config_(std::move(config)), owners_(std::move(owners)) {}

    [[nodiscard]] bool AppendDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainAckInbox(owner)) {
            return false;
        }
        if (dispatch.owner != owner || dispatch.dispatch_fence == 0U ||
            dispatch.feed_session_epoch != config_.feed_session_epoch) {
            SetFatal("KLine TickDispatch owner, epoch, or fence is invalid");
            return false;
        }

        switch (dispatch.kind) {
            case ingest::TickDispatchKind::kProjectOrdered:
                stats_.ordered_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyProject(owner, dispatch,
                                    KLineSequenceClass::kOrdered);
            case ingest::TickDispatchKind::kProjectHoleFill:
                stats_.hole_fill_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyProject(owner, dispatch,
                                    KLineSequenceClass::kHoleFill);
            case ingest::TickDispatchKind::kRejectLateFact:
                stats_.rejected_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyRejection(owner, dispatch);
            case ingest::TickDispatchKind::kGapOpen:
                stats_.gap_open_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ValidControl(dispatch, true);
            case ingest::TickDispatchKind::kChannelSeal:
                stats_.channel_seal_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ValidControl(dispatch, false);
        }
        SetFatal("KLine TickDispatch kind is invalid");
        return false;
    }

    [[nodiscard]] bool FlushDue(std::size_t owner,
                                std::uint64_t monotonic_ns) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainAckInbox(owner)) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        if (!state.active.empty() &&
            monotonic_ns >= state.active_started_ns &&
            monotonic_ns - state.active_started_ns >=
                config_.micro_batch_max_delay_ns) {
            return FlushActive(owner, MicroBatchFlushReason::kTimer);
        }
        return ServiceWorker(owner);
    }

    [[nodiscard]] bool Flush(std::size_t owner) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainAckInbox(owner)) {
            return false;
        }
        return FlushActive(owner, MicroBatchFlushReason::kExplicit);
    }

    [[nodiscard]] bool FlushActive(
        std::size_t owner,
        MicroBatchFlushReason reason) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.active.empty()) {
            if (reason == MicroBatchFlushReason::kExplicit) {
                stats_.explicit_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
            }
            return ServiceWorker(owner);
        }
        const std::uint64_t rows = state.active.size();
        const std::uint64_t now = ingest::MonotonicNowNs();
        const std::uint64_t source_age = now >= state.active_started_ns
            ? now - state.active_started_ns
            : 0U;
        const KLineApplyResult applied = state.worker->ApplyBatch(
            std::span<const KLineInput>(state.active.data(),
                                        state.active.size()));
        state.active.clear();
        state.active_started_ns = 0U;
        stats_.micro_batches_applied.fetch_add(
            1U, std::memory_order_relaxed);
        stats_.facts_in_micro_batches.fetch_add(
            rows, std::memory_order_relaxed);
        PublishMaximum(&stats_.micro_batch_rows_max, rows);
        PublishMaximum(&stats_.micro_batch_source_age_ns_max, source_age);
        switch (reason) {
            case MicroBatchFlushReason::kRowLimit:
                stats_.row_limit_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
                break;
            case MicroBatchFlushReason::kTimer:
                stats_.timer_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
                break;
            case MicroBatchFlushReason::kExplicit:
                stats_.explicit_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
                break;
        }
        if (applied.code == KLineApplyCode::kSourceConflict) {
            stats_.source_conflicts.fetch_add(
                1U, std::memory_order_relaxed);
        } else if (applied.code == KLineApplyCode::kInvalidInput) {
            stats_.invalid_inputs.fetch_add(
                1U, std::memory_order_relaxed);
        } else if (applied.code == KLineApplyCode::kCapacityExhausted ||
                   applied.code == KLineApplyCode::kSinkFailed ||
                   applied.code == KLineApplyCode::kFailed) {
            SetFatal(state.worker->fatal_error().empty()
                         ? "KLine worker rejected a micro-batch"
                         : state.worker->fatal_error());
            return false;
        }
        return ServiceWorker(owner);
    }

    [[nodiscard]] bool FlushAll() noexcept {
        bool result = true;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            result = Flush(owner) && result;
        }
        return result;
    }

    [[nodiscard]] bool DrainAll() noexcept {
        bool result = FlushAll();
        bool pending_raw_dependencies = false;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            result = DrainAckInbox(owner) && result;
            KLineWorker* const worker = owners_[owner]->worker.get();
            result = worker->DrainDurableCommits() && result;
            if (worker->stats().pending_raw_commits != 0U) {
                pending_raw_dependencies = true;
                result = false;
            }
        }
        const bool ack_backlog = std::any_of(
            owners_.begin(), owners_.end(), [](const auto& owner) {
                return owner->ack_inbox.size() != 0U;
            });
        if (pending_raw_dependencies || ack_backlog ||
            stats_.occurrence_join_entries.load(std::memory_order_acquire) !=
                0U) {
            SetFatal(
                "KLine runtime drain has unresolved raw ACK/disposition "
                "dependencies");
            result = false;
        }
        return result;
    }

    [[nodiscard]] bool OnRawTickBatchAcknowledged(
        std::span<const ingest::CanonicalTick> ticks) noexcept {
        if (!healthy()) {
            return false;
        }
        for (const ingest::CanonicalTick& tick : ticks) {
            if (tick.common.instrument_ordinal ==
                ingest::kInvalidInstrumentOrdinal) {
                continue;
            }
            const std::size_t owner = owner_for_instrument(
                tick.common.instrument_ordinal);
            if (!ValidOwner(owner)) {
                SetFatal("KLine raw ACK owner is invalid");
                return false;
            }
            if (!owners_[owner]->ack_inbox.TryPush(OccurrenceKey{
                    config_.feed_session_epoch,
                    tick.common.ingress_sequence,
                    tick.common.kind})) {
                SetFatal("KLine raw ACK inbox capacity exhausted");
                return false;
            }
            stats_.raw_tick_acks_received.fetch_add(
                1U, std::memory_order_relaxed);
        }
        return true;
    }

    [[nodiscard]] bool healthy() const noexcept {
        if (!healthy_.load(std::memory_order_acquire)) {
            return false;
        }
        return std::all_of(owners_.begin(), owners_.end(),
                           [](const auto& owner) {
                               return owner->worker->healthy();
                           });
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        if (!fatal_error_.empty()) {
            return fatal_error_;
        }
        for (const auto& owner : owners_) {
            if (!owner->worker->healthy()) {
                return owner->worker->fatal_error();
            }
        }
        return {};
    }

    [[nodiscard]] KLineRuntimeStats stats() const noexcept {
        KLineRuntimeStats result{};
        result.ordered_dispositions_received =
            stats_.ordered_dispositions_received.load(
                std::memory_order_relaxed);
        result.hole_fill_dispositions_received =
            stats_.hole_fill_dispositions_received.load(
                std::memory_order_relaxed);
        result.rejected_dispositions_received =
            stats_.rejected_dispositions_received.load(
                std::memory_order_relaxed);
        result.gap_open_controls_received =
            stats_.gap_open_controls_received.load(std::memory_order_relaxed);
        result.channel_seal_controls_received =
            stats_.channel_seal_controls_received.load(
                std::memory_order_relaxed);
        result.raw_tick_acks_received = stats_.raw_tick_acks_received.load(
            std::memory_order_relaxed);
        for (const auto& owner : owners_) {
            result.raw_ack_inbox_backlog += static_cast<std::uint64_t>(
                owner->ack_inbox.size());
        }
        result.occurrence_join_entries =
            stats_.occurrence_join_entries.load(std::memory_order_acquire);
        result.occurrence_join_high_water =
            stats_.occurrence_join_high_water.load(std::memory_order_relaxed);
        result.occurrence_ack_first = stats_.occurrence_ack_first.load(
            std::memory_order_relaxed);
        result.occurrence_disposition_first =
            stats_.occurrence_disposition_first.load(
                std::memory_order_relaxed);
        result.occurrence_projects_resolved =
            stats_.occurrence_projects_resolved.load(
                std::memory_order_relaxed);
        result.occurrence_rejections_resolved =
            stats_.occurrence_rejections_resolved.load(
                std::memory_order_relaxed);
        result.occurrence_duplicate_sides =
            stats_.occurrence_duplicate_sides.load(
                std::memory_order_relaxed);
        result.micro_batches_applied = stats_.micro_batches_applied.load(
            std::memory_order_relaxed);
        result.facts_in_micro_batches = stats_.facts_in_micro_batches.load(
            std::memory_order_relaxed);
        result.micro_batch_rows_max = stats_.micro_batch_rows_max.load(
            std::memory_order_relaxed);
        result.micro_batch_source_age_ns_max =
            stats_.micro_batch_source_age_ns_max.load(
                std::memory_order_relaxed);
        result.row_limit_flushes = stats_.row_limit_flushes.load(
            std::memory_order_relaxed);
        result.timer_flushes = stats_.timer_flushes.load(
            std::memory_order_relaxed);
        result.explicit_flushes = stats_.explicit_flushes.load(
            std::memory_order_relaxed);
        result.source_conflicts = stats_.source_conflicts.load(
            std::memory_order_relaxed);
        result.invalid_inputs = stats_.invalid_inputs.load(
            std::memory_order_relaxed);
        for (const auto& owner : owners_) {
            AddWorkerStats(&result.workers, owner->worker->stats());
        }
        return result;
    }

    [[nodiscard]] KLineWorker* worker(std::size_t owner) noexcept {
        return ValidOwner(owner) ? owners_[owner]->worker.get() : nullptr;
    }

    [[nodiscard]] std::size_t owner_for_instrument(
        std::uint32_t instrument_ordinal) const noexcept {
        if (instrument_ordinal == ingest::kInvalidInstrumentOrdinal ||
            owners_.empty()) {
            return owners_.size();
        }
        return static_cast<std::size_t>(instrument_ordinal) % owners_.size();
    }

    [[nodiscard]] const KLineRuntimeConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool ValidOwner(std::size_t owner) const noexcept {
        return owner < owners_.size();
    }

    [[nodiscard]] bool ValidOccurrenceDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) const noexcept {
        const ingest::CanonicalTick& tick = dispatch.tick;
        const ingest::Market market = tick.common.identity.market;
        const bool tick_kind = market == ingest::Market::kShanghai
            ? tick.common.kind == ingest::CanonicalKind::kShanghaiTick
            : market == ingest::Market::kShenzhen &&
                  (tick.common.kind == ingest::CanonicalKind::kShenzhenOrder ||
                   tick.common.kind ==
                       ingest::CanonicalKind::kShenzhenTransaction);
        return dispatch.catalog_match &&
               tick.common.trade_date == config_.worker.trade_date &&
               tick.common.instrument_id != 0U &&
               tick.common.instrument_ordinal !=
                   ingest::kInvalidInstrumentOrdinal &&
               owner_for_instrument(tick.common.instrument_ordinal) == owner &&
               (market != ingest::Market::kShanghai ||
                tick.common.channel != 0U) &&
               tick.common.native_sequence != 0U &&
               tick.common.ingress_sequence != 0U &&
               dispatch.market == market &&
               dispatch.channel == tick.common.channel && tick_kind;
    }

    [[nodiscard]] OccurrenceKey DispatchOccurrence(
        const ingest::TickDispatch& dispatch) const noexcept {
        return OccurrenceKey{config_.feed_session_epoch,
                             dispatch.tick.common.ingress_sequence,
                             dispatch.tick.common.kind};
    }

    void JoinInserted(bool ack_first) noexcept {
        const std::uint64_t entries =
            stats_.occurrence_join_entries.fetch_add(
                1U, std::memory_order_acq_rel) + 1U;
        std::uint64_t high = stats_.occurrence_join_high_water.load(
            std::memory_order_relaxed);
        while (high < entries &&
               !stats_.occurrence_join_high_water.compare_exchange_weak(
                   high, entries, std::memory_order_relaxed)) {
        }
        (ack_first ? stats_.occurrence_ack_first
                   : stats_.occurrence_disposition_first)
            .fetch_add(1U, std::memory_order_relaxed);
    }

    void JoinResolved(bool project) noexcept {
        stats_.occurrence_join_entries.fetch_sub(
            1U, std::memory_order_acq_rel);
        (project ? stats_.occurrence_projects_resolved
                 : stats_.occurrence_rejections_resolved)
            .fetch_add(1U, std::memory_order_relaxed);
    }

    [[nodiscard]] bool JoinFailure(JoinResult result) noexcept {
        if (result == JoinResult::kDuplicateSide) {
            stats_.occurrence_duplicate_sides.fetch_add(
                1U, std::memory_order_relaxed);
            SetFatal("KLine occurrence join received a duplicate side");
        } else {
            SetFatal("KLine occurrence join capacity exhausted");
        }
        return false;
    }

    [[nodiscard]] bool ForwardRawAck(std::size_t owner,
                                     const OccurrenceKey& key) noexcept {
        const RawTickDependency dependency{key.ingress_sequence, key.kind};
        owners_[owner]->worker->AcknowledgeRawTicks(
            std::span<const RawTickDependency>(&dependency, 1U));
        if (!owners_[owner]->worker->healthy()) {
            SetFatal(owners_[owner]->worker->fatal_error());
            return false;
        }
        return true;
    }

    [[nodiscard]] bool ApplyProject(
        std::size_t owner,
        const ingest::TickDispatch& dispatch,
        KLineSequenceClass sequence_class) noexcept {
        if (!ValidOccurrenceDispatch(owner, dispatch)) {
            SetFatal("KLine project disposition is invalid");
            return false;
        }
        const std::uint64_t sequence = dispatch.tick.common.native_sequence;
        const bool eligible = dispatch.expected_sequence != 0U &&
            dispatch.admission_floor != 0U &&
            dispatch.admission_floor <= dispatch.expected_sequence &&
            dispatch.evict_before != 0U &&
            (sequence_class == KLineSequenceClass::kOrdered
                 ? sequence >= dispatch.expected_sequence
                 : dispatch.generation != 0U &&
                       sequence >= dispatch.admission_floor &&
                       sequence < dispatch.expected_sequence);
        if (!eligible) {
            SetFatal("KLine project admission token is invalid");
            return false;
        }
        const OccurrenceKey key = DispatchOccurrence(dispatch);
        const JoinResult joined = owners_[owner]->occurrence_join
            .ObserveDisposition(key, true);
        if (joined == JoinResult::kDispositionFirst) {
            JoinInserted(false);
        } else if (joined == JoinResult::kProjectResolved) {
            JoinResolved(true);
        } else if (joined != JoinResult::kDispositionFirst) {
            return JoinFailure(joined);
        }

        KLineInput input{};
        input.tick = dispatch.tick;
        input.admission.feed_session_epoch = dispatch.feed_session_epoch;
        input.admission.expected_sequence = dispatch.expected_sequence;
        input.admission.admission_floor = dispatch.admission_floor;
        input.admission.retention_floor = dispatch.evict_before;
        input.admission.generation = dispatch.generation;
        input.admission.dispatch_fence = dispatch.dispatch_fence;
        input.admission.sequence_class = sequence_class;
        input.catalog_match = dispatch.catalog_match;
        if (!AppendInput(owner, std::move(input),
                         dispatch.tick.common.receive_monotonic_ns)) {
            return false;
        }
        return joined != JoinResult::kProjectResolved ||
               ForwardRawAck(owner, key);
    }

    [[nodiscard]] bool ApplyRejection(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOccurrenceDispatch(owner, dispatch)) {
            SetFatal("KLine reject disposition is invalid");
            return false;
        }
        const JoinResult joined = owners_[owner]->occurrence_join
            .ObserveDisposition(DispatchOccurrence(dispatch), false);
        if (joined == JoinResult::kDispositionFirst) {
            JoinInserted(false);
            return true;
        }
        if (joined == JoinResult::kRejectResolved) {
            JoinResolved(false);
            return true;
        }
        return JoinFailure(joined);
    }

    [[nodiscard]] bool ValidControl(
        const ingest::TickDispatch& dispatch,
        bool gap_open) noexcept {
        const bool valid = dispatch.market != ingest::Market::kUnknown &&
            dispatch.generation != 0U &&
            (!gap_open ||
             (dispatch.first_missing != 0U &&
              dispatch.last_missing >= dispatch.first_missing));
        if (!valid) {
            SetFatal(gap_open ? "KLine GapOpen control is invalid"
                              : "KLine ChannelSeal control is invalid");
        }
        return valid;
    }

    [[nodiscard]] bool AppendInput(std::size_t owner,
                                   KLineInput input,
                                   std::uint64_t monotonic_ns) noexcept {
        OwnerState& state = *owners_[owner];
        try {
            if (state.active.size() >= config_.micro_batch_rows &&
                !FlushActive(owner, MicroBatchFlushReason::kRowLimit)) {
                return false;
            }
            if (state.active.empty()) {
                state.active_started_ns = monotonic_ns;
            }
            state.active.push_back(std::move(input));
            if (state.active.size() >= config_.micro_batch_rows) {
                return FlushActive(owner, MicroBatchFlushReason::kRowLimit);
            }
            return true;
        } catch (...) {
            SetFatal("KLine micro-batch allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool DrainAckInbox(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (!state.worker_ack_drain.empty()) {
            SetFatal("KLine owner ACK drain buffers are not empty");
            return false;
        }
        OccurrenceKey key{};
        std::size_t drained = 0U;
        while (drained < config_.maximum_raw_ack_backlog_per_owner &&
               state.ack_inbox.TryPop(&key)) {
            ++drained;
            const JoinResult joined = state.occurrence_join.ObserveAck(key);
            if (joined == JoinResult::kAckFirst) {
                JoinInserted(true);
            } else if (joined == JoinResult::kProjectResolved) {
                JoinResolved(true);
                state.worker_ack_drain.push_back(
                    RawTickDependency{key.ingress_sequence, key.kind});
            } else if (joined == JoinResult::kRejectResolved) {
                JoinResolved(false);
            } else {
                return JoinFailure(joined);
            }
        }
        if (!state.worker_ack_drain.empty()) {
            state.worker->AcknowledgeRawTicks(state.worker_ack_drain);
            if (!state.worker->healthy()) {
                SetFatal(state.worker->fatal_error());
                return false;
            }
        }
        state.worker_ack_drain.clear();
        return ServiceWorker(owner);
    }

    [[nodiscard]] bool ServiceWorker(std::size_t owner) noexcept {
        KLineWorker* const worker = owners_[owner]->worker.get();
        if (!worker->DrainDurableCommits()) {
            SetFatal(worker->fatal_error());
            return false;
        }
        return true;
    }

    void SetFatal(std::string message) noexcept {
        bool expected = true;
        if (!healthy_.compare_exchange_strong(
                expected, false, std::memory_order_acq_rel)) {
            return;
        }
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            fatal_error_ = std::move(message);
        } catch (...) {
        }
    }

    KLineRuntimeConfig config_{};
    std::vector<std::unique_ptr<OwnerState>> owners_;
    AtomicRuntimeStats stats_{};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

bool ValidateKLineRuntimeConfig(const KLineRuntimeConfig& config,
                                std::string* error) noexcept {
    if (!ValidateKLineWorkerConfig(config.worker, error)) {
        return false;
    }
    if (config.worker.owner != 0U || config.feed_session_epoch == 0U ||
        config.worker.feed_session_epoch != config.feed_session_epoch ||
        config.micro_batch_rows == 0U ||
        config.micro_batch_max_delay_ns == 0U ||
        config.maximum_raw_ack_backlog_per_owner == 0U ||
        config.maximum_occurrence_join_entries_per_owner == 0U ||
        config.maximum_occurrence_join_entries_per_owner >
            std::numeric_limits<std::size_t>::max() / 2U) {
        if (error != nullptr) {
            *error = "invalid KLine runtime configuration";
        }
        return false;
    }
    const std::size_t required_worker_ack_capacity = std::min(
        config.maximum_occurrence_join_entries_per_owner,
        std::max(config.maximum_raw_ack_backlog_per_owner,
                 config.micro_batch_rows));
    if (config.worker.maximum_acknowledged_raw_dependencies <
        required_worker_ack_capacity) {
        if (error != nullptr) {
            *error = "KLine worker raw ACK capacity is smaller than the "
                     "runtime ACK forwarding cut";
        }
        return false;
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

std::unique_ptr<KLineRuntime> KLineRuntime::Create(
    KLineRuntimeConfig config,
    KLineRevisionSink* sink,
    std::string* error) {
    if (!ValidateKLineRuntimeConfig(config, error)) {
        return nullptr;
    }
    if (sink == nullptr) {
        if (error != nullptr) {
            *error = "KLine revision sink is null";
        }
        return nullptr;
    }
    try {
        std::vector<std::unique_ptr<Impl::OwnerState>> owners;
        owners.reserve(config.worker.owner_count);
        for (std::uint32_t owner = 0U; owner < config.worker.owner_count;
             ++owner) {
            KLineWorkerConfig worker_config = config.worker;
            worker_config.owner = owner;
            auto state = std::make_unique<Impl::OwnerState>(
                config.maximum_occurrence_join_entries_per_owner,
                config.maximum_raw_ack_backlog_per_owner);
            state->worker = KLineWorker::Create(
                std::move(worker_config), sink, error);
            if (state->worker == nullptr) {
                return nullptr;
            }
            state->active.reserve(config.micro_batch_rows);
            owners.push_back(std::move(state));
        }
        return std::unique_ptr<KLineRuntime>(new KLineRuntime(
            std::make_unique<Impl>(std::move(config),
                                   std::move(owners))));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("KLine runtime creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

KLineRuntime::KLineRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineRuntime::~KLineRuntime() = default;

bool KLineRuntime::AppendDispatch(
    std::size_t owner,
    const ingest::TickDispatch& dispatch) noexcept {
    return impl_->AppendDispatch(owner, dispatch);
}

bool KLineRuntime::FlushDue(std::size_t owner,
                            std::uint64_t monotonic_ns) noexcept {
    return impl_->FlushDue(owner, monotonic_ns);
}

bool KLineRuntime::Flush(std::size_t owner) noexcept {
    return impl_->Flush(owner);
}

bool KLineRuntime::FlushAll() noexcept { return impl_->FlushAll(); }

bool KLineRuntime::DrainAll() noexcept { return impl_->DrainAll(); }

bool KLineRuntime::OnRawTickBatchAcknowledged(
    std::span<const ingest::CanonicalTick> ticks) noexcept {
    return impl_->OnRawTickBatchAcknowledged(ticks);
}

bool KLineRuntime::healthy() const noexcept { return impl_->healthy(); }

std::string KLineRuntime::fatal_error() const { return impl_->fatal_error(); }

KLineRuntimeStats KLineRuntime::stats() const noexcept {
    return impl_->stats();
}

KLineWorker* KLineRuntime::worker(std::size_t owner) noexcept {
    return impl_->worker(owner);
}

std::size_t KLineRuntime::owner_for_instrument(
    std::uint32_t instrument_ordinal) const noexcept {
    return impl_->owner_for_instrument(instrument_ordinal);
}

const KLineRuntimeConfig& KLineRuntime::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::kline
