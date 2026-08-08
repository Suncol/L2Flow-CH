#include "l2flow/event/runtime.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

namespace l2flow::event {
namespace {

struct AtomicRuntimeStats final {
    std::atomic<std::uint64_t> normal_ticks_received{0U};
    std::atomic<std::uint64_t> late_ticks_received{0U};
    std::atomic<std::uint64_t> raw_tick_acks_received{0U};
    std::atomic<std::uint64_t> micro_batches_applied{0U};
    std::atomic<std::uint64_t> source_conflicts{0U};
    std::atomic<std::uint64_t> invalid_inputs{0U};
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
    destination->active_repair_orders += source.active_repair_orders;
    destination->ordered_batch_fast_path += source.ordered_batch_fast_path;
    destination->unordered_batch_sorts += source.unordered_batch_sorts;
    destination->barrier_index_orders_visited +=
        source.barrier_index_orders_visited;
    destination->source_only_fast_path += source.source_only_fast_path;
}

}  // namespace

class EventRuntime::Impl final {
public:
    struct OwnerState final {
        std::unique_ptr<EventWorker> worker;
        std::vector<EventInput> active;
        std::uint64_t active_started_ns = 0U;

        mutable std::mutex inbox_mutex;
        std::vector<EventInput> late_inbox;
        std::vector<RawTickDependency> ack_inbox;
    };

    Impl(EventRuntimeConfig config,
         std::vector<std::unique_ptr<OwnerState>> owners)
        : config_(std::move(config)), owners_(std::move(owners)) {}

    [[nodiscard]] bool AppendTick(std::size_t owner,
                                  const ingest::CanonicalTick& tick) noexcept {
        if (!ValidOwner(owner) || !healthy()) {
            return false;
        }
        if (!DrainInboxes(owner)) {
            return false;
        }
        stats_.normal_ticks_received.fetch_add(1U,
                                               std::memory_order_relaxed);
        EventInput input{tick};
        if (tick.common.native_sequence !=
            std::numeric_limits<std::uint64_t>::max()) {
            input.committed_next_sequence =
                tick.common.native_sequence + 1U;
        }
        input.observed_gap_epoch = tick.common.gap_epoch;
        return AppendInput(owner, std::move(input),
                           tick.common.receive_monotonic_ns);
    }

    [[nodiscard]] bool AppendLateRecovery(
        const ingest::LateRecoveryTick& late) noexcept {
        if (!healthy()) {
            return false;
        }
        if (!late.catalog_match ||
            late.tick.common.instrument_ordinal ==
                ingest::kInvalidInstrumentOrdinal) {
            return true;
        }
        const std::size_t owner = owner_for_instrument(
            late.tick.common.instrument_ordinal);
        EventInput input{};
        input.tick = late.tick;
        input.committed_next_sequence = late.committed_next_sequence;
        input.observed_gap_epoch = late.observed_gap_epoch;
        input.late_recovery = true;
        input.upstream_conflict =
            late.reason == ingest::LateRecoveryReason::
                               kPendingCanonicalConflict;
        input.catalog_match = late.catalog_match;
        OwnerState& state = *owners_[owner];
        try {
            std::lock_guard<std::mutex> lock(state.inbox_mutex);
            if (state.late_inbox.size() >=
                config_.maximum_late_backlog_per_owner) {
                SetFatal("Event LateRecovery inbox capacity exhausted");
                return false;
            }
            state.late_inbox.push_back(std::move(input));
            stats_.late_ticks_received.fetch_add(
                1U, std::memory_order_relaxed);
            return true;
        } catch (...) {
            SetFatal("Event LateRecovery inbox allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool FlushDue(std::size_t owner,
                                std::uint64_t monotonic_ns) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainInboxes(owner)) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        if (!state.active.empty() &&
            monotonic_ns >= state.active_started_ns &&
            monotonic_ns - state.active_started_ns >=
                config_.micro_batch_max_delay_ns) {
            return Flush(owner);
        }
        return ServiceWorkerOnce(owner);
    }

    [[nodiscard]] bool Flush(std::size_t owner) noexcept {
        if (!ValidOwner(owner) || !healthy() || !DrainInboxes(owner)) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        if (state.active.empty()) {
            return ServiceWorkerOnce(owner);
        }
        const EventApplyResult applied = state.worker->ApplyBatch(
            std::span<const EventInput>(state.active.data(),
                                        state.active.size()));
        state.active.clear();
        state.active_started_ns = 0U;
        stats_.micro_batches_applied.fetch_add(1U,
                                               std::memory_order_relaxed);
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
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            result = DrainInboxes(owner) && result;
            EventWorker* const worker = owners_[owner]->worker.get();
            while (result && worker->repair_pending()) {
                const EventWorkerStats before = worker->stats();
                if (!worker->AdvanceRepair()) {
                    SetFatal(worker->fatal_error());
                    result = false;
                    break;
                }
                const EventWorkerStats after = worker->stats();
                if (worker->repair_pending() &&
                    after.repaired_order_uses ==
                        before.repaired_order_uses &&
                    after.repair_order_restarts ==
                        before.repair_order_restarts &&
                    after.repair_commits == before.repair_commits) {
                    result = false;
                    break;
                }
            }
            result = worker->DrainDurableCommits() && result;
            if (worker->stats().pending_raw_commits != 0U) {
                result = false;
            }
        }
        return result;
    }

    [[nodiscard]] bool OnRawTickBatchAcknowledged(
        std::span<const ingest::CanonicalTick> ticks) noexcept {
        if (!healthy()) {
            return false;
        }
        try {
            std::vector<std::vector<RawTickDependency>> grouped(
                owners_.size());
            for (const ingest::CanonicalTick& tick : ticks) {
                if (tick.common.instrument_ordinal ==
                    ingest::kInvalidInstrumentOrdinal) {
                    continue;
                }
                const std::size_t owner = owner_for_instrument(
                    tick.common.instrument_ordinal);
                grouped[owner].push_back(RawTickDependency{
                    tick.common.ingress_sequence, tick.common.kind});
            }
            for (std::size_t owner = 0U; owner < grouped.size(); ++owner) {
                if (grouped[owner].empty()) {
                    continue;
                }
                OwnerState& state = *owners_[owner];
                std::lock_guard<std::mutex> lock(state.inbox_mutex);
                if (grouped[owner].size() >
                    config_.maximum_raw_ack_backlog_per_owner -
                        state.ack_inbox.size()) {
                    SetFatal("Event raw ACK inbox capacity exhausted");
                    return false;
                }
                state.ack_inbox.insert(state.ack_inbox.end(),
                                       grouped[owner].begin(),
                                       grouped[owner].end());
                stats_.raw_tick_acks_received.fetch_add(
                    static_cast<std::uint64_t>(grouped[owner].size()),
                    std::memory_order_relaxed);
            }
            return true;
        } catch (...) {
            SetFatal("Event raw ACK inbox allocation failed");
            return false;
        }
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
        result.normal_ticks_received = stats_.normal_ticks_received.load(
            std::memory_order_relaxed);
        result.late_ticks_received = stats_.late_ticks_received.load(
            std::memory_order_relaxed);
        result.raw_tick_acks_received = stats_.raw_tick_acks_received.load(
            std::memory_order_relaxed);
        result.micro_batches_applied = stats_.micro_batches_applied.load(
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

    [[nodiscard]] bool AppendInput(std::size_t owner,
                                   EventInput input,
                                   std::uint64_t monotonic_ns) noexcept {
        OwnerState& state = *owners_[owner];
        try {
            if (state.active.size() >= config_.micro_batch_rows &&
                !Flush(owner)) {
                return false;
            }
            if (state.active.empty()) {
                state.active_started_ns = monotonic_ns;
            }
            state.active.push_back(std::move(input));
            if (state.active.size() >= config_.micro_batch_rows) {
                return Flush(owner);
            }
            return true;
        } catch (...) {
            SetFatal("Event micro-batch allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool DrainInboxes(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        std::vector<EventInput> late;
        std::vector<RawTickDependency> acknowledgements;
        {
            std::lock_guard<std::mutex> lock(state.inbox_mutex);
            late.swap(state.late_inbox);
            acknowledgements.swap(state.ack_inbox);
        }
        if (!acknowledgements.empty()) {
            state.worker->AcknowledgeRawTicks(acknowledgements);
            if (!state.worker->healthy()) {
                SetFatal(state.worker->fatal_error());
                return false;
            }
        }
        for (EventInput& input : late) {
            const std::uint64_t received =
                input.tick.common.receive_monotonic_ns;
            if (!AppendInput(owner, std::move(input), received)) {
                return false;
            }
        }
        return ServiceWorkerOnce(owner);
    }

    [[nodiscard]] bool ServiceWorkerOnce(std::size_t owner) noexcept {
        EventWorker* const worker = owners_[owner]->worker.get();
        if (!worker->DrainDurableCommits() || !worker->AdvanceRepair() ||
            !worker->DrainDurableCommits()) {
            SetFatal(worker->fatal_error().empty()
                         ? "Event owner service failed"
                         : worker->fatal_error());
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
    if (config.worker.owner != 0U || config.micro_batch_rows == 0U ||
        config.micro_batch_max_delay_ns == 0U ||
        config.maximum_raw_ack_backlog_per_owner == 0U ||
        config.maximum_late_backlog_per_owner == 0U) {
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
        config.worker.maximum_acknowledged_raw_dependencies =
            config.maximum_raw_ack_backlog_per_owner;
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
            state->late_inbox.reserve(
                config.maximum_late_backlog_per_owner);
            state->ack_inbox.reserve(
                config.maximum_raw_ack_backlog_per_owner);
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

bool EventRuntime::AppendTick(std::size_t owner,
                              const ingest::CanonicalTick& tick) noexcept {
    return impl_->AppendTick(owner, tick);
}

bool EventRuntime::AppendLateRecovery(
    const ingest::LateRecoveryTick& late) noexcept {
    return impl_->AppendLateRecovery(late);
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
