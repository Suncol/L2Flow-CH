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

struct PendingSealKey final {
    ingest::Market market = ingest::Market::kUnknown;
    std::uint32_t channel = 0U;

    friend constexpr auto operator<=>(const PendingSealKey&,
                                      const PendingSealKey&) = default;
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
    std::atomic<std::uint64_t> micro_batches_applied{0U};
    std::atomic<std::uint64_t> facts_in_micro_batches{0U};
    std::atomic<std::uint64_t> micro_batch_rows_max{0U};
    std::atomic<std::uint64_t> micro_batch_source_age_ns_max{0U};
    std::atomic<std::uint64_t> row_limit_flushes{0U};
    std::atomic<std::uint64_t> timer_flushes{0U};
    std::atomic<std::uint64_t> forced_active_flushes{0U};
    std::atomic<std::uint64_t> empty_control_flushes{0U};
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
    destination->pending_revision_batches +=
        source.pending_revision_batches;
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
        std::unique_ptr<EventWorker> worker;
        std::vector<EventInput> active;
        std::uint64_t active_started_ns = 0U;
        std::uint64_t active_oldest_receive_ns = 0U;
        // A control can already be removed from the engine FIFO when flushing
        // the preceding micro-batch starts a sliced phase-normalization cut.
        std::optional<ingest::TickDispatch> deferred_control;
        std::map<PendingSealKey, ingest::TickDispatch> pending_seals;
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
        if (!ValidOwner(owner) || !healthy()) {
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
        if (!ValidOwner(owner) || !healthy()) {
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
        if (!ValidOwner(owner) || !healthy()) {
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
        bool pending_commits = false;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
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
            if (worker->stats().pending_revision_batches != 0U) {
                pending_commits = true;
                result = false;
            }
            if (worker->projection_input_fenced() ||
                state.deferred_control.has_value() ||
                !state.pending_seals.empty()) {
                SetFatal("Event owner input fence did not drain");
                result = false;
            }
        }
        if (pending_commits) {
            SetFatal(
                "Event runtime drain has unresolved pending revision batches");
            result = false;
        }
        return result;
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
        return AppendInput(owner, std::move(input),
                           dispatch.tick.common.receive_monotonic_ns);
    }

    [[nodiscard]] bool ApplyRejection(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOccurrenceDispatch(owner, dispatch)) {
            SetFatal("Event reject disposition is invalid");
            return false;
        }
        return true;
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
        config.maximum_pending_channel_seals_per_owner == 0U) {
        return fail("invalid Event runtime configuration");
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
            auto state = std::make_unique<Impl::OwnerState>();
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
