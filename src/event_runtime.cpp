#include "l2flow/event/runtime.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <compare>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace l2flow::event {
namespace {

struct OccurrenceKey final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t ingress_sequence = 0U;
    ingest::CanonicalKind kind = ingest::CanonicalKind::kShanghaiTick;

    friend constexpr bool operator==(const OccurrenceKey&,
                                     const OccurrenceKey&) = default;
};

struct PendingSealKey final {
    ingest::Market market = ingest::Market::kUnknown;
    std::uint32_t channel = 0U;

    friend constexpr auto operator<=>(const PendingSealKey&,
                                      const PendingSealKey&) = default;
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
            throw std::length_error("Event occurrence join capacity is invalid");
        }
        const std::size_t required = maximum_entries * 2U;
        std::size_t count = 1U;
        while (count < required) {
            if (count > std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::length_error(
                    "Event occurrence join slot count overflow");
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
    std::atomic<std::uint64_t> channel_seals_applied{0U};
    std::atomic<std::uint64_t> channel_seals_coalesced{0U};
    std::atomic<std::uint64_t> pending_channel_seals{0U};
    std::atomic<std::uint64_t> pending_channel_seals_high_water{0U};
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
    std::atomic<std::uint64_t> forced_active_flushes{0U};
    std::atomic<std::uint64_t> empty_control_flushes{0U};
    std::atomic<std::uint64_t> explicit_flushes{0U};
    std::atomic<std::uint64_t> raw_ack_drain_slices{0U};
    std::atomic<std::uint64_t> raw_ack_entries_drained{0U};
    std::atomic<std::uint64_t> raw_ack_drain_entries_max{0U};
    std::atomic<std::uint64_t> raw_ack_drain_cpu_ns_max{0U};
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
    kControl,
    kExplicit,
};

void AddWorkerStats(EventWorkerStats* destination,
                    const EventWorkerStats& source) noexcept {
    destination->facts_journaled += source.facts_journaled;
    destination->duplicate_facts += source.duplicate_facts;
    destination->source_conflicts += source.source_conflicts;
    destination->live_order_uses += source.live_order_uses;
    destination->repaired_order_uses += source.repaired_order_uses;
    destination->repair_convergence_stops +=
        source.repair_convergence_stops;
    destination->repair_slices += source.repair_slices;
    destination->repair_commits += source.repair_commits;
    destination->repair_order_restarts += source.repair_order_restarts;
    destination->bundles_reassembled += source.bundles_reassembled;
    destination->revisions_created += source.revisions_created;
    destination->tombstones_created += source.tombstones_created;
    destination->pending_raw_commits += source.pending_raw_commits;
    destination->acknowledged_raw_dependencies +=
        source.acknowledged_raw_dependencies;
    destination->revision_batches_submitted +=
        source.revision_batches_submitted;
    destination->persistence_groups_submitted +=
        source.persistence_groups_submitted;
    destination->persistence_group_batches_max = std::max(
        destination->persistence_group_batches_max,
        source.persistence_group_batches_max);
    destination->persistence_group_rows_max = std::max(
        destination->persistence_group_rows_max,
        source.persistence_group_rows_max);
    destination->persistence_group_bytes_max = std::max(
        destination->persistence_group_bytes_max,
        source.persistence_group_bytes_max);
    destination->pending_revision_bytes += source.pending_revision_bytes;
    destination->pending_revision_bytes_high_watermark = std::max(
        destination->pending_revision_bytes_high_watermark,
        source.pending_revision_bytes_high_watermark);
    destination->active_repair_orders += source.active_repair_orders;
    destination->active_repair_bytes += source.active_repair_bytes;
    destination->active_repair_bytes_high_watermark = std::max(
        destination->active_repair_bytes_high_watermark,
        source.active_repair_bytes_high_watermark);
    destination->phase_normalization_slices +=
        source.phase_normalization_slices;
    destination->phase_facts_scanned += source.phase_facts_scanned;
    destination->phase_dirty_roles_discovered +=
        source.phase_dirty_roles_discovered;
    destination->pending_phase_bytes += source.pending_phase_bytes;
    destination->pending_phase_bytes_high_watermark = std::max(
        destination->pending_phase_bytes_high_watermark,
        source.pending_phase_bytes_high_watermark);
    destination->ordered_batch_fast_path += source.ordered_batch_fast_path;
    destination->unordered_batch_sorts += source.unordered_batch_sorts;
    destination->barrier_index_orders_visited +=
        source.barrier_index_orders_visited;
    destination->end_expansion_slices += source.end_expansion_slices;
    destination->end_candidates_processed += source.end_candidates_processed;
    destination->source_only_fast_path += source.source_only_fast_path;
    destination->order_uses_compacted += source.order_uses_compacted;
    destination->facts_evicted += source.facts_evicted;
    destination->eviction_slices += source.eviction_slices;
    destination->hot_facts += source.hot_facts;
    destination->hot_fact_bytes += source.hot_fact_bytes;
    destination->hot_fact_bytes_high_watermark = std::max(
        destination->hot_fact_bytes_high_watermark,
        source.hot_fact_bytes_high_watermark);
    destination->order_history_bytes += source.order_history_bytes;
    destination->order_history_bytes_high_watermark = std::max(
        destination->order_history_bytes_high_watermark,
        source.order_history_bytes_high_watermark);
}

}  // namespace

class EventRuntime::Impl final {
public:
    struct OwnerState final {
        OwnerState(std::size_t join_entries,
                   std::size_t ack_backlog,
                   std::size_t ack_drain_entries)
            : occurrence_join(join_entries), ack_inbox(ack_backlog) {
            worker_ack_drain.reserve(ack_drain_entries);
        }

        std::unique_ptr<EventWorker> worker;
        std::vector<EventInput> active;
        std::uint64_t active_started_ns = 0U;
        std::uint64_t active_oldest_receive_ns = 0U;
        // A control can already be removed from the engine FIFO when flushing
        // the preceding micro-batch starts a sliced phase-normalization cut.
        std::optional<ingest::TickDispatch> deferred_control;
        std::map<PendingSealKey, ingest::TickDispatch> pending_seals;
        OccurrenceJoin occurrence_join;
        RawAckInbox ack_inbox;
        std::vector<RawTickDependency> worker_ack_drain;
    };

    Impl(EventRuntimeConfig config,
         std::vector<std::unique_ptr<OwnerState>> owners)
        : config_(std::move(config)), owners_(std::move(owners)) {}

    [[nodiscard]] bool AppendDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOwner(owner) || !healthy()) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        if (state.deferred_control.has_value() ||
            state.worker->projection_input_fenced()) {
            SetFatal("Event TickDispatch appended while owner input is fenced");
            return false;
        }
        if (!DrainAckInbox(owner)) {
            return false;
        }
        if (dispatch.owner != owner || dispatch.dispatch_fence == 0U ||
            dispatch.feed_session_epoch != config_.feed_session_epoch) {
            SetFatal("Event TickDispatch owner, epoch, or fence is invalid");
            return false;
        }

        switch (dispatch.kind) {
            case ingest::TickDispatchKind::kProjectOrdered:
                stats_.ordered_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyProject(owner, dispatch,
                                    EventSequenceClass::kOrdered);
            case ingest::TickDispatchKind::kProjectHoleFill:
                stats_.hole_fill_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyProject(owner, dispatch,
                                    EventSequenceClass::kHoleFill);
            case ingest::TickDispatchKind::kRejectLateFact:
                stats_.rejected_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyRejection(owner, dispatch);
            case ingest::TickDispatchKind::kGapOpen:
                stats_.gap_open_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplyGapControl(owner, dispatch);
            case ingest::TickDispatchKind::kChannelSeal:
                stats_.channel_seal_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return ApplySealControl(owner, dispatch);
        }
        SetFatal("Event TickDispatch kind is invalid");
        return false;
    }

    [[nodiscard]] bool CanPollDispatch(std::size_t owner) const noexcept {
        if (!ValidOwner(owner)) {
            return false;
        }
        const OwnerState& state = *owners_[owner];
        return !state.deferred_control.has_value() &&
               !state.worker->projection_input_fenced() &&
               state.pending_seals.size() <
                   config_.maximum_pending_channel_seals_per_owner;
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
            if (!FlushActive(owner, MicroBatchFlushReason::kTimer)) {
                return false;
            }
        } else if (!ServiceWorkerOnce(owner)) {
            return false;
        }
        return ApplyPendingSeals(owner, 64U);
    }

    [[nodiscard]] bool Flush(std::size_t owner) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainAckInbox(owner)) {
            return false;
        }
        return FlushActive(owner, MicroBatchFlushReason::kExplicit) &&
            ApplyPendingSeals(owner,
                              std::numeric_limits<std::size_t>::max());
    }

    [[nodiscard]] bool FlushActive(
        std::size_t owner,
        MicroBatchFlushReason reason) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.active.empty()) {
            if (reason == MicroBatchFlushReason::kControl) {
                SetFatal("Event control flush reached an empty active batch");
                return false;
            }
            if (reason == MicroBatchFlushReason::kExplicit) {
                stats_.explicit_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
            }
            return ServiceWorkerOnce(owner);
        }
        const std::uint64_t rows = state.active.size();
        const std::uint64_t now = ingest::MonotonicNowNs();
        const std::uint64_t source_age =
            now >= state.active_oldest_receive_ns
            ? now - state.active_oldest_receive_ns
            : 0U;
        const EventApplyResult applied = state.worker->ApplyBatch(
            std::span<const EventInput>(state.active.data(),
                                        state.active.size()));
        state.active.clear();
        state.active_started_ns = 0U;
        state.active_oldest_receive_ns = 0U;
        stats_.micro_batches_applied.fetch_add(1U,
                                               std::memory_order_relaxed);
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
            case MicroBatchFlushReason::kControl:
                stats_.forced_active_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
                break;
            case MicroBatchFlushReason::kExplicit:
                stats_.explicit_flushes.fetch_add(
                    1U, std::memory_order_relaxed);
                break;
        }
        if (applied.code == EventApplyCode::kSourceConflict) {
            stats_.source_conflicts.fetch_add(1U,
                                              std::memory_order_relaxed);
        } else if (applied.code == EventApplyCode::kInvalidInput) {
            stats_.invalid_inputs.fetch_add(1U,
                                            std::memory_order_relaxed);
        } else if (applied.code == EventApplyCode::kCapacityExhausted ||
                   applied.code == EventApplyCode::kSinkFailed ||
                   applied.code == EventApplyCode::kFailed) {
            SetFatal(state.worker->fatal_error().empty()
                         ? "Event worker rejected a micro-batch"
                         : state.worker->fatal_error());
            return false;
        }
        return ServiceWorkerOnce(owner);
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
            while (result && owners_[owner]->ack_inbox.size() != 0U) {
                const std::size_t before = owners_[owner]->ack_inbox.size();
                if (!DrainAckInbox(owner) || !ServiceWorkerOnce(owner)) {
                    result = false;
                    break;
                }
                if (owners_[owner]->ack_inbox.size() >= before) {
                    SetFatal("Event owner ACK drain made no progress");
                    result = false;
                    break;
                }
            }
            OwnerState& state = *owners_[owner];
            EventWorker* const worker = state.worker.get();
            while (result &&
                   (worker->repair_pending() || worker->eviction_pending() ||
                    state.deferred_control.has_value())) {
                const EventWorkerStats before = worker->stats();
                const bool deferred_before =
                    state.deferred_control.has_value();
                if (!ServiceWorkerOnce(owner)) {
                    result = false;
                    break;
                }
                const EventWorkerStats after = worker->stats();
                const bool work_remains = worker->repair_pending() ||
                    worker->eviction_pending() ||
                    state.deferred_control.has_value();
                const bool deferred_applied = deferred_before &&
                    !state.deferred_control.has_value();
                if (work_remains && !deferred_applied &&
                    after.repaired_order_uses == before.repaired_order_uses &&
                    after.repair_order_restarts ==
                        before.repair_order_restarts &&
                    after.repair_commits == before.repair_commits &&
                    after.phase_normalization_slices ==
                        before.phase_normalization_slices &&
                    after.phase_facts_scanned == before.phase_facts_scanned &&
                    after.phase_dirty_roles_discovered ==
                        before.phase_dirty_roles_discovered &&
                    after.end_candidates_processed ==
                        before.end_candidates_processed &&
                    after.end_expansion_slices ==
                        before.end_expansion_slices &&
                    after.order_uses_compacted ==
                        before.order_uses_compacted &&
                    after.facts_evicted == before.facts_evicted &&
                    after.eviction_slices == before.eviction_slices) {
                    SetFatal("Event owner background work made no progress");
                    result = false;
                    break;
                }
            }
            if (result && !ApplyPendingSeals(
                              owner,
                              std::numeric_limits<std::size_t>::max())) {
                result = false;
            }
            while (result && worker->eviction_pending()) {
                const EventWorkerStats before = worker->stats();
                if (!ServiceWorkerOnce(owner)) {
                    result = false;
                    break;
                }
                const EventWorkerStats after = worker->stats();
                if (worker->eviction_pending() &&
                    after.facts_evicted == before.facts_evicted &&
                    after.eviction_slices == before.eviction_slices &&
                    after.order_uses_compacted ==
                        before.order_uses_compacted) {
                    SetFatal("Event owner seal eviction made no progress");
                    result = false;
                    break;
                }
            }
            if (!worker->FlushDurableCommits()) {
                SetFatal(worker->fatal_error().empty()
                             ? "Event owner persistence flush failed"
                             : worker->fatal_error());
                result = false;
            }
            if (worker->stats().pending_raw_commits != 0U) {
                pending_raw_dependencies = true;
                result = false;
            }
            if (worker->projection_input_fenced() ||
                state.deferred_control.has_value() ||
                !state.pending_seals.empty()) {
                SetFatal("Event owner input fence did not drain");
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
                "Event runtime drain has unresolved raw ACK/disposition "
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
                SetFatal("Event raw ACK owner is invalid");
                return false;
            }
            if (!owners_[owner]->ack_inbox.TryPush(OccurrenceKey{
                    config_.feed_session_epoch,
                    tick.common.ingress_sequence,
                    tick.common.kind})) {
                SetFatal("Event raw ACK inbox capacity exhausted");
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

    [[nodiscard]] EventRuntimeStats stats() const noexcept {
        EventRuntimeStats result{};
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
        result.channel_seals_applied = stats_.channel_seals_applied.load(
            std::memory_order_relaxed);
        result.channel_seals_coalesced = stats_.channel_seals_coalesced.load(
            std::memory_order_relaxed);
        result.pending_channel_seals = stats_.pending_channel_seals.load(
            std::memory_order_acquire);
        result.pending_channel_seals_high_water =
            stats_.pending_channel_seals_high_water.load(
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
        result.forced_active_flushes = stats_.forced_active_flushes.load(
            std::memory_order_relaxed);
        result.empty_control_flushes = stats_.empty_control_flushes.load(
            std::memory_order_relaxed);
        result.explicit_flushes = stats_.explicit_flushes.load(
            std::memory_order_relaxed);
        result.raw_ack_drain_slices = stats_.raw_ack_drain_slices.load(
            std::memory_order_relaxed);
        result.raw_ack_entries_drained = stats_.raw_ack_entries_drained.load(
            std::memory_order_relaxed);
        result.raw_ack_drain_entries_max =
            stats_.raw_ack_drain_entries_max.load(
                std::memory_order_relaxed);
        result.raw_ack_drain_cpu_ns_max =
            stats_.raw_ack_drain_cpu_ns_max.load(
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

    [[nodiscard]] EventWorker* worker(std::size_t owner) noexcept {
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

    [[nodiscard]] const EventRuntimeConfig& config() const noexcept {
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
            SetFatal("Event occurrence join received a duplicate side");
        } else {
            SetFatal("Event occurrence join capacity exhausted");
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
        EventSequenceClass sequence_class) noexcept {
        if (!ValidOccurrenceDispatch(owner, dispatch)) {
            SetFatal("Event project disposition is invalid");
            return false;
        }
        if (!ApplyPendingSealForChannel(
                owner, dispatch.market, dispatch.channel)) {
            return false;
        }
        const std::uint64_t sequence = dispatch.tick.common.native_sequence;
        const bool eligible = dispatch.expected_sequence != 0U &&
            dispatch.admission_floor != 0U &&
            dispatch.admission_floor <= dispatch.expected_sequence &&
            dispatch.evict_before != 0U &&
            (sequence_class == EventSequenceClass::kOrdered
                 ? sequence >= dispatch.expected_sequence
                 : dispatch.generation != 0U &&
                       sequence >= dispatch.admission_floor &&
                       sequence < dispatch.expected_sequence);
        if (!eligible) {
            SetFatal("Event project admission token is invalid");
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

        EventInput input{};
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
            SetFatal("Event reject disposition is invalid");
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

    [[nodiscard]] bool ApplyGapControl(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (dispatch.market == ingest::Market::kUnknown ||
            dispatch.generation == 0U || dispatch.first_missing == 0U ||
            dispatch.last_missing < dispatch.first_missing) {
            SetFatal("Event GapOpen control is invalid");
            return false;
        }
        return DeferControl(owner, dispatch,
                            "Event GapOpen control apply failed",
                            "Event GapOpen control flush failed");
    }

    [[nodiscard]] bool ApplySealControl(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (dispatch.market == ingest::Market::kUnknown ||
            dispatch.generation == 0U) {
            SetFatal("Event ChannelSeal control is invalid");
            return false;
        }
        return DeferControl(owner, dispatch,
                            "Event ChannelSeal control apply failed",
                            "Event ChannelSeal control flush failed");
    }

    [[nodiscard]] bool DeferControl(
        std::size_t owner,
        const ingest::TickDispatch& dispatch,
        const char* apply_error,
        const char* flush_error) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.deferred_control.has_value()) {
            SetFatal("Event deferred control slot is occupied");
            return false;
        }
        state.deferred_control = dispatch;
        if (state.active.empty()) {
            stats_.empty_control_flushes.fetch_add(
                1U, std::memory_order_relaxed);
            if (!ApplyDeferredControl(owner)) {
                if (healthy()) {
                    SetFatal(apply_error);
                }
                return false;
            }
            return true;
        }
        if (!FlushActive(owner, MicroBatchFlushReason::kControl)) {
            if (healthy()) {
                SetFatal(flush_error);
            }
            return false;
        }
        return true;
    }

    [[nodiscard]] bool ApplyDeferredControl(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (!state.deferred_control.has_value()) {
            return true;
        }
        if (state.worker->projection_input_fenced()) {
            return true;
        }

        const ingest::TickDispatch& dispatch = *state.deferred_control;
        bool applied = false;
        if (dispatch.kind == ingest::TickDispatchKind::kGapOpen) {
            // A GapOpen carries a generation-specific range needed by later
            // HoleFill. It is always applied individually; only the preceding
            // seal watermark may be drained from the owner mailbox.
            if (!ApplyPendingSealForChannel(
                    owner, dispatch.market, dispatch.channel)) {
                return false;
            }
            const GapOpen gap{dispatch.market,
                              dispatch.channel,
                              dispatch.feed_session_epoch,
                              dispatch.first_missing,
                              dispatch.last_missing,
                              dispatch.generation,
                              dispatch.dispatch_fence};
            applied = state.worker->ApplyGapOpen(gap);
        } else if (dispatch.kind == ingest::TickDispatchKind::kChannelSeal) {
            applied = StagePendingSeal(owner, dispatch);
        } else {
            SetFatal("Event deferred dispatch is not a control");
            return false;
        }
        if (!applied) {
            SetFatal(state.worker->fatal_error().empty()
                         ? "Event worker rejected deferred control"
                         : state.worker->fatal_error());
            return false;
        }
        state.deferred_control.reset();
        return true;
    }

    [[nodiscard]] bool StagePendingSeal(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        OwnerState& state = *owners_[owner];
        const PendingSealKey key{dispatch.market, dispatch.channel};
        try {
            auto position = state.pending_seals.find(key);
            if (position == state.pending_seals.end()) {
                if (state.pending_seals.size() >=
                    config_.maximum_pending_channel_seals_per_owner) {
                    SetFatal(
                        "Event ChannelSeal mailbox capacity exhausted");
                    return false;
                }
                state.pending_seals.emplace_hint(position, key, dispatch);
                const std::uint64_t pending =
                    stats_.pending_channel_seals.fetch_add(
                        1U, std::memory_order_release) + 1U;
                PublishMaximum(
                    &stats_.pending_channel_seals_high_water, pending);
                return true;
            }
            ingest::TickDispatch& pending = position->second;
            if (pending.feed_session_epoch != dispatch.feed_session_epoch ||
                pending.generation != dispatch.generation ||
                dispatch.dispatch_fence <= pending.dispatch_fence) {
                SetFatal(
                    "Event ChannelSeal mailbox generation/fence is stale");
                return false;
            }
            const std::uint64_t watermark = std::max(
                pending.evict_before, dispatch.evict_before);
            pending = dispatch;
            pending.evict_before = watermark;
            stats_.channel_seals_coalesced.fetch_add(
                1U, std::memory_order_relaxed);
            return true;
        } catch (...) {
            SetFatal("Event ChannelSeal mailbox allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool ApplyPendingSealForChannel(
        std::size_t owner,
        ingest::Market market,
        std::uint32_t channel) noexcept {
        OwnerState& state = *owners_[owner];
        const auto position = state.pending_seals.find(
            PendingSealKey{market, channel});
        if (position == state.pending_seals.end()) {
            return true;
        }
        const ingest::TickDispatch& dispatch = position->second;
        const ChannelSeal seal{dispatch.market,
                               dispatch.channel,
                               dispatch.feed_session_epoch,
                               dispatch.evict_before,
                               dispatch.generation,
                               dispatch.dispatch_fence};
        if (!state.worker->ApplyChannelSeal(seal)) {
            SetFatal(state.worker->fatal_error().empty()
                         ? "Event worker rejected ChannelSeal mailbox"
                         : state.worker->fatal_error());
            return false;
        }
        state.pending_seals.erase(position);
        stats_.pending_channel_seals.fetch_sub(
            1U, std::memory_order_release);
        stats_.channel_seals_applied.fetch_add(
            1U, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool ApplyPendingSeals(
        std::size_t owner,
        std::size_t maximum) noexcept {
        OwnerState& state = *owners_[owner];
        std::size_t applied = 0U;
        while (applied < maximum && !state.pending_seals.empty()) {
            const PendingSealKey key = state.pending_seals.begin()->first;
            if (!ApplyPendingSealForChannel(owner, key.market, key.channel)) {
                return false;
            }
            ++applied;
        }
        return true;
    }

    [[nodiscard]] bool AppendInput(std::size_t owner,
                                   EventInput input,
                                   std::uint64_t monotonic_ns) noexcept {
        OwnerState& state = *owners_[owner];
        try {
            if (state.active.size() >= config_.micro_batch_rows &&
                !FlushActive(owner, MicroBatchFlushReason::kRowLimit)) {
                return false;
            }
            if (state.active.empty()) {
                state.active_started_ns = ingest::MonotonicNowNs();
                state.active_oldest_receive_ns = monotonic_ns;
            } else {
                state.active_oldest_receive_ns = std::min(
                    state.active_oldest_receive_ns, monotonic_ns);
            }
            state.active.push_back(std::move(input));
            if (state.active.size() >= config_.micro_batch_rows) {
                return FlushActive(owner, MicroBatchFlushReason::kRowLimit);
            }
            return true;
        } catch (...) {
            SetFatal("Event micro-batch allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool DrainAckInbox(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (!state.worker_ack_drain.empty()) {
            SetFatal("Event owner ACK drain buffers are not empty");
            return false;
        }
        OccurrenceKey key{};
        std::size_t drained = 0U;
        const std::uint64_t started_ns = ingest::MonotonicNowNs();
        while (drained < config_.raw_ack_drain_max_entries &&
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
            if ((drained & 63U) == 0U) {
                const std::uint64_t now = ingest::MonotonicNowNs();
                if (now >= started_ns &&
                    now - started_ns >= config_.raw_ack_drain_max_cpu_ns) {
                    break;
                }
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
        if (drained != 0U) {
            const std::uint64_t completed_ns = ingest::MonotonicNowNs();
            const std::uint64_t elapsed = completed_ns >= started_ns
                ? completed_ns - started_ns
                : 0U;
            stats_.raw_ack_drain_slices.fetch_add(
                1U, std::memory_order_relaxed);
            stats_.raw_ack_entries_drained.fetch_add(
                drained, std::memory_order_relaxed);
            PublishMaximum(&stats_.raw_ack_drain_entries_max, drained);
            PublishMaximum(&stats_.raw_ack_drain_cpu_ns_max, elapsed);
        }
        return true;
    }

    [[nodiscard]] bool ServiceWorkerOnce(std::size_t owner) noexcept {
        EventWorker* const worker = owners_[owner]->worker.get();
        if (!worker->AdvanceDurableCommits() || !worker->AdvanceRepair() ||
            !worker->ContinueEviction() ||
            !worker->AdvanceDurableCommits()) {
            SetFatal(worker->fatal_error().empty()
                         ? "Event owner service failed"
                         : worker->fatal_error());
            return false;
        }
        return ApplyDeferredControl(owner);
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

    EventRuntimeConfig config_{};
    std::vector<std::unique_ptr<OwnerState>> owners_;
    AtomicRuntimeStats stats_{};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

bool ValidateEventRuntimeConfig(const EventRuntimeConfig& config,
                                std::string* error) noexcept {
    if (!ValidateEventWorkerConfig(config.worker, error)) {
        return false;
    }
    const auto fail = [error](const char* message) noexcept {
        if (error != nullptr) {
            try {
                *error = message;
            } catch (...) {
            }
        }
        return false;
    };
    if (config.worker.owner != 0U || config.feed_session_epoch == 0U ||
        config.worker.feed_session_epoch != config.feed_session_epoch ||
        config.micro_batch_rows == 0U ||
        config.micro_batch_max_delay_ns == 0U ||
        config.maximum_raw_ack_backlog_per_owner == 0U ||
        config.maximum_occurrence_join_entries_per_owner == 0U ||
        config.maximum_pending_channel_seals_per_owner == 0U ||
        config.raw_ack_drain_max_entries == 0U ||
        config.raw_ack_drain_max_cpu_ns == 0U ||
        config.maximum_occurrence_join_entries_per_owner >
            std::numeric_limits<std::size_t>::max() / 2U) {
        return fail("invalid Event runtime configuration");
    }
    const std::size_t required_worker_ack_capacity = std::min(
        config.maximum_occurrence_join_entries_per_owner,
        std::max(config.maximum_raw_ack_backlog_per_owner,
                 config.micro_batch_rows));
    if (config.worker.maximum_acknowledged_raw_dependencies <
        required_worker_ack_capacity) {
        return fail("Event worker raw ACK capacity is smaller than the "
                    "runtime ACK forwarding cut");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

std::unique_ptr<EventRuntime> EventRuntime::Create(
    EventRuntimeConfig config,
    EventRevisionSink* sink,
    std::string* error) {
    if (!ValidateEventRuntimeConfig(config, error)) {
        return nullptr;
    }
    if (sink == nullptr) {
        if (error != nullptr) {
            *error = "Event runtime revision sink is null";
        }
        return nullptr;
    }
    try {
        std::vector<std::unique_ptr<Impl::OwnerState>> owners;
        owners.reserve(config.worker.owner_count);
        for (std::uint32_t owner = 0U;
             owner < config.worker.owner_count; ++owner) {
            EventWorkerConfig worker_config = config.worker;
            worker_config.owner = owner;
            auto state = std::make_unique<Impl::OwnerState>(
                config.maximum_occurrence_join_entries_per_owner,
                config.maximum_raw_ack_backlog_per_owner,
                config.raw_ack_drain_max_entries);
            state->worker = EventWorker::Create(
                std::move(worker_config), sink, error);
            if (state->worker == nullptr) {
                return nullptr;
            }
            state->active.reserve(config.micro_batch_rows);
            owners.push_back(std::move(state));
        }
        return std::unique_ptr<EventRuntime>(new EventRuntime(
            std::make_unique<Impl>(std::move(config),
                                   std::move(owners))));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("Event runtime creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

EventRuntime::EventRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

EventRuntime::~EventRuntime() = default;

bool EventRuntime::AppendDispatch(
    std::size_t owner,
    const ingest::TickDispatch& dispatch) noexcept {
    return impl_->AppendDispatch(owner, dispatch);
}

bool EventRuntime::CanPollDispatch(std::size_t owner) const noexcept {
    return impl_->CanPollDispatch(owner);
}

bool EventRuntime::FlushDue(std::size_t owner,
                            std::uint64_t monotonic_ns) noexcept {
    return impl_->FlushDue(owner, monotonic_ns);
}

bool EventRuntime::Flush(std::size_t owner) noexcept {
    return impl_->Flush(owner);
}

bool EventRuntime::FlushAll() noexcept { return impl_->FlushAll(); }

bool EventRuntime::DrainAll() noexcept { return impl_->DrainAll(); }

bool EventRuntime::OnRawTickBatchAcknowledged(
    std::span<const ingest::CanonicalTick> ticks) noexcept {
    return impl_->OnRawTickBatchAcknowledged(ticks);
}

bool EventRuntime::healthy() const noexcept { return impl_->healthy(); }

std::string EventRuntime::fatal_error() const {
    return impl_->fatal_error();
}

EventRuntimeStats EventRuntime::stats() const noexcept {
    return impl_->stats();
}

EventWorker* EventRuntime::worker(std::size_t owner) noexcept {
    return impl_->worker(owner);
}

std::size_t EventRuntime::owner_for_instrument(
    std::uint32_t instrument_ordinal) const noexcept {
    return impl_->owner_for_instrument(instrument_ordinal);
}

const EventRuntimeConfig& EventRuntime::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::event
