#include "l2flow/event/runtime.h"

#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace l2flow::event {
namespace {

struct AtomicRuntimeStats final {
    std::atomic<std::uint64_t> ordered_dispositions_received{0U};
    std::atomic<std::uint64_t> hole_fill_dispositions_received{0U};
    std::atomic<std::uint64_t> rejected_dispositions_received{0U};
    std::atomic<std::uint64_t> gap_open_controls_received{0U};
    std::atomic<std::uint64_t> channel_seal_controls_received{0U};
    std::atomic<std::uint64_t> channel_seals_applied{0U};
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

void AddWorkerStats(EventWorkerStats* destination,
                    const EventWorkerStats& source) noexcept {
    destination->facts_journaled += source.facts_journaled;
    destination->duplicate_facts += source.duplicate_facts;
    destination->source_conflicts += source.source_conflicts;
    destination->live_order_uses += source.live_order_uses;
    destination->repaired_order_uses += source.repaired_order_uses;
    destination->repair_convergence_stops += source.repair_convergence_stops;
    destination->repair_slices += source.repair_slices;
    destination->repair_commits += source.repair_commits;
    destination->repair_order_restarts += source.repair_order_restarts;
    destination->bundles_reassembled += source.bundles_reassembled;
    destination->revisions_created += source.revisions_created;
    destination->tombstones_created += source.tombstones_created;
    destination->pending_revision_commits +=
        source.pending_revision_commits;
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

[[nodiscard]] bool ApplySucceeded(EventApplyCode code) noexcept {
    return code == EventApplyCode::kApplied ||
           code == EventApplyCode::kDuplicateOnly ||
           code == EventApplyCode::kSourceConflict ||
           code == EventApplyCode::kInvalidInput;
}

}  // namespace

class EventRuntime::Impl final {
public:
    struct OwnerState final {
        std::unique_ptr<EventWorker> worker;
        std::vector<EventInput> active;
        std::uint64_t active_started_ns = 0U;
    };

    Impl(EventRuntimeConfig config,
         std::vector<std::unique_ptr<OwnerState>> owners)
        : config_(std::move(config)), owners_(std::move(owners)) {}

    [[nodiscard]] bool AppendDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch,
        outbox::WalPosition position) noexcept {
        if (!healthy() || owner >= owners_.size() ||
            dispatch.owner != owner ||
            dispatch.feed_session_epoch != config_.feed_session_epoch ||
            position.lsn == 0U || position.batch_sequence == 0U) {
            SetFatal("Event runtime received an invalid durable dispatch");
            return false;
        }
        OwnerState& state = *owners_[owner];
        switch (dispatch.kind) {
            case ingest::TickDispatchKind::kProjectOrdered:
            case ingest::TickDispatchKind::kProjectHoleFill: {
                if (!dispatch.catalog_match ||
                    state.worker->projection_input_fenced()) {
                    SetFatal("Event projection input overtook an owner fence");
                    return false;
                }
                EventInput input{};
                input.tick = dispatch.tick;
                input.outbox_position = position;
                input.catalog_match = true;
                input.admission.feed_session_epoch =
                    dispatch.feed_session_epoch;
                input.admission.expected_sequence =
                    dispatch.expected_sequence;
                input.admission.admission_floor = dispatch.admission_floor;
                input.admission.retention_floor = dispatch.evict_before;
                input.admission.generation = dispatch.generation;
                input.admission.dispatch_fence = dispatch.dispatch_fence;
                input.admission.sequence_class =
                    dispatch.kind ==
                            ingest::TickDispatchKind::kProjectHoleFill
                        ? EventSequenceClass::kHoleFill
                        : EventSequenceClass::kOrdered;
                if (state.active.empty()) {
                    state.active_started_ns =
                        dispatch.tick.common.receive_monotonic_ns;
                }
                state.active.push_back(std::move(input));
                if (dispatch.kind ==
                    ingest::TickDispatchKind::kProjectHoleFill) {
                    stats_.hole_fill_dispositions_received.fetch_add(
                        1U, std::memory_order_relaxed);
                } else {
                    stats_.ordered_dispositions_received.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                if (state.active.size() >= config_.micro_batch_rows) {
                    stats_.row_limit_flushes.fetch_add(
                        1U, std::memory_order_relaxed);
                    return FlushOwner(owner);
                }
                return true;
            }
            case ingest::TickDispatchKind::kRejectLateFact:
                stats_.rejected_dispositions_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return true;
            case ingest::TickDispatchKind::kGapOpen: {
                stats_.gap_open_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                if (!FlushForControl(owner)) {
                    return false;
                }
                GapOpen gap{};
                gap.market = dispatch.market;
                gap.channel = dispatch.channel;
                gap.feed_session_epoch = dispatch.feed_session_epoch;
                gap.first_missing = dispatch.first_missing;
                gap.last_missing = dispatch.last_missing;
                gap.generation = dispatch.generation;
                gap.dispatch_fence = dispatch.dispatch_fence;
                if (!state.worker->ApplyGapOpen(gap)) {
                    SetFatal("Event worker rejected durable GapOpen");
                    return false;
                }
                return ServiceOwner(owner);
            }
            case ingest::TickDispatchKind::kChannelSeal: {
                stats_.channel_seal_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                if (!FlushForControl(owner)) {
                    return false;
                }
                ChannelSeal seal{};
                seal.market = dispatch.market;
                seal.channel = dispatch.channel;
                seal.feed_session_epoch = dispatch.feed_session_epoch;
                seal.evict_before = dispatch.evict_before;
                seal.generation = dispatch.generation;
                seal.dispatch_fence = dispatch.dispatch_fence;
                if (!state.worker->ApplyChannelSeal(seal)) {
                    SetFatal("Event worker rejected durable ChannelSeal");
                    return false;
                }
                stats_.channel_seals_applied.fetch_add(
                    1U, std::memory_order_relaxed);
                return ServiceOwner(owner);
            }
        }
        SetFatal("Event runtime received an unknown disposition");
        return false;
    }

    [[nodiscard]] bool CanPollDispatch(std::size_t owner) const noexcept {
        return healthy() && owner < owners_.size() &&
               !owners_[owner]->worker->projection_input_fenced();
    }

    [[nodiscard]] bool FlushDue(std::size_t owner,
                                std::uint64_t monotonic_ns) noexcept {
        if (!healthy() || owner >= owners_.size() || !ServiceOwner(owner)) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        if (state.active.empty() || monotonic_ns < state.active_started_ns ||
            monotonic_ns - state.active_started_ns <
                config_.micro_batch_max_delay_ns) {
            return true;
        }
        stats_.timer_flushes.fetch_add(1U, std::memory_order_relaxed);
        return FlushOwner(owner);
    }

    [[nodiscard]] bool Flush(std::size_t owner) noexcept {
        if (!healthy() || owner >= owners_.size()) {
            return false;
        }
        stats_.explicit_flushes.fetch_add(1U, std::memory_order_relaxed);
        if (!FlushOwner(owner)) {
            return false;
        }
        constexpr std::size_t kMaximumShutdownSlices = 10'000'000U;
        for (std::size_t slice = 0U;
             slice < kMaximumShutdownSlices; ++slice) {
            if (!ServiceOwner(owner)) {
                return false;
            }
            if (!owners_[owner]->worker->projection_input_fenced() &&
                !owners_[owner]->worker->repair_pending() &&
                !owners_[owner]->worker->eviction_pending()) {
                return owners_[owner]->worker->FlushRevisionCommits();
            }
        }
        SetFatal("Event shutdown slicing made no bounded progress");
        return false;
    }

    [[nodiscard]] bool FlushAll() noexcept {
        bool result = true;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            result = Flush(owner) && result;
        }
        return result;
    }

    [[nodiscard]] bool DrainAll() noexcept {
        if (!FlushAll()) {
            return false;
        }
        bool result = true;
        for (const auto& owner : owners_) {
            result = owner->worker->FlushRevisionCommits() && result;
        }
        return result;
    }

    [[nodiscard]] bool healthy() const noexcept {
        if (!healthy_.load(std::memory_order_acquire)) {
            return false;
        }
        return std::all_of(
            owners_.begin(), owners_.end(),
            [](const auto& owner) { return owner->worker->healthy(); });
    }

    [[nodiscard]] std::string fatal_error() const {
        {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            if (!fatal_error_.empty()) {
                return fatal_error_;
            }
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
            stats_.gap_open_controls_received.load(
                std::memory_order_relaxed);
        result.channel_seal_controls_received =
            stats_.channel_seal_controls_received.load(
                std::memory_order_relaxed);
        result.channel_seals_applied = stats_.channel_seals_applied.load(
            std::memory_order_relaxed);
        result.pending_channel_seals = stats_.pending_channel_seals.load(
            std::memory_order_relaxed);
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
        return owner < owners_.size() ? owners_[owner]->worker.get() : nullptr;
    }

    [[nodiscard]] std::size_t owner_for_instrument(
        std::uint32_t instrument_ordinal) const noexcept {
        return static_cast<std::size_t>(instrument_ordinal) % owners_.size();
    }

    [[nodiscard]] const EventRuntimeConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool FlushForControl(std::size_t owner) noexcept {
        if (owners_[owner]->active.empty()) {
            stats_.empty_control_flushes.fetch_add(
                1U, std::memory_order_relaxed);
            return ServiceOwner(owner);
        }
        stats_.forced_active_flushes.fetch_add(
            1U, std::memory_order_relaxed);
        return FlushOwner(owner);
    }

    [[nodiscard]] bool FlushOwner(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.active.empty()) {
            return ServiceOwner(owner);
        }
        const std::uint64_t now = ingest::MonotonicNowNs();
        const std::uint64_t age = now >= state.active_started_ns
            ? now - state.active_started_ns
            : 0U;
        const std::size_t rows = state.active.size();
        const EventApplyResult applied = state.worker->ApplyBatch(state.active);
        if (!ApplySucceeded(applied.code)) {
            SetFatal("Event worker failed a durable outbox micro-batch");
            return false;
        }
        if (applied.code == EventApplyCode::kSourceConflict) {
            stats_.source_conflicts.fetch_add(1U, std::memory_order_relaxed);
        }
        if (applied.code == EventApplyCode::kInvalidInput) {
            stats_.invalid_inputs.fetch_add(1U, std::memory_order_relaxed);
        }
        state.active.clear();
        state.active_started_ns = 0U;
        stats_.micro_batches_applied.fetch_add(1U,
                                               std::memory_order_relaxed);
        stats_.facts_in_micro_batches.fetch_add(rows,
                                               std::memory_order_relaxed);
        PublishMaximum(&stats_.micro_batch_rows_max, rows);
        PublishMaximum(&stats_.micro_batch_source_age_ns_max, age);
        return ServiceOwner(owner);
    }

    [[nodiscard]] bool ServiceOwner(std::size_t owner) noexcept {
        EventWorker& worker = *owners_[owner]->worker;
        if (worker.projection_input_fenced() || worker.repair_pending()) {
            if (!worker.AdvanceRepair()) {
                SetFatal("Event repair failed while consuming durable WAL");
                return false;
            }
        }
        if (worker.eviction_pending() && !worker.ContinueEviction()) {
            SetFatal("Event eviction failed while consuming durable WAL");
            return false;
        }
        if (!worker.AdvanceRevisionCommits()) {
            SetFatal("Event revision submission failed");
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

    EventRuntimeConfig config_{};
    std::vector<std::unique_ptr<OwnerState>> owners_;
    AtomicRuntimeStats stats_{};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

bool ValidateEventRuntimeConfig(const EventRuntimeConfig& config,
                                std::string* error) noexcept {
    const auto fail = [error](const char* message) noexcept {
        if (error != nullptr) {
            try {
                *error = message;
            } catch (...) {
            }
        }
        return false;
    };
    if (!ValidateEventWorkerConfig(config.worker, error)) {
        return false;
    }
    if (config.feed_session_epoch == 0U ||
        config.feed_session_epoch != config.worker.feed_session_epoch ||
        config.micro_batch_rows == 0U ||
        config.micro_batch_max_delay_ns == 0U) {
        return fail("invalid Event runtime durable-outbox configuration");
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
            *error = "Event revision sink is null";
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
            state->active.reserve(config.micro_batch_rows);
            state->worker = EventWorker::Create(
                std::move(worker_config), sink, error);
            if (state->worker == nullptr) {
                return nullptr;
            }
            owners.push_back(std::move(state));
        }
        return std::unique_ptr<EventRuntime>(new EventRuntime(
            std::make_unique<Impl>(std::move(config), std::move(owners))));
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
    const ingest::TickDispatch& dispatch,
    outbox::WalPosition position) noexcept {
    return impl_->AppendDispatch(owner, dispatch, position);
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
