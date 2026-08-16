#include "l2flow/kline/runtime.h"

#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace l2flow::kline {
namespace {

struct AtomicRuntimeStats final {
    std::atomic<std::uint64_t> ordered_dispositions_received{0U};
    std::atomic<std::uint64_t> hole_fill_dispositions_received{0U};
    std::atomic<std::uint64_t> rejected_dispositions_received{0U};
    std::atomic<std::uint64_t> gap_open_controls_received{0U};
    std::atomic<std::uint64_t> channel_seal_controls_received{0U};
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
    destination->pending_revision_commits +=
        source.pending_revision_commits;
    destination->pending_revision_rows += source.pending_revision_rows;
    destination->pending_revision_rows_high_watermark = std::max(
        destination->pending_revision_rows_high_watermark,
        source.pending_revision_rows_high_watermark);
    destination->pending_revision_bytes += source.pending_revision_bytes;
    destination->pending_revision_bytes_high_watermark = std::max(
        destination->pending_revision_bytes_high_watermark,
        source.pending_revision_bytes_high_watermark);
    destination->revision_batches_submitted +=
        source.revision_batches_submitted;
}

[[nodiscard]] bool ApplySucceeded(KLineApplyCode code) noexcept {
    return code == KLineApplyCode::kApplied ||
           code == KLineApplyCode::kDuplicateOnly ||
           code == KLineApplyCode::kSourceConflict ||
           code == KLineApplyCode::kInvalidInput;
}

}  // namespace

class KLineRuntime::Impl final {
public:
    struct OwnerState final {
        std::unique_ptr<KLineWorker> worker;
        std::vector<KLineInput> active;
        std::uint64_t active_started_ns = 0U;
    };

    Impl(KLineRuntimeConfig config,
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
            SetFatal("KLine runtime received an invalid durable dispatch");
            return false;
        }
        switch (dispatch.kind) {
            case ingest::TickDispatchKind::kProjectOrdered:
            case ingest::TickDispatchKind::kProjectHoleFill: {
                if (!dispatch.catalog_match) {
                    SetFatal("KLine received a non-catalog projection");
                    return false;
                }
                OwnerState& state = *owners_[owner];
                KLineInput input{};
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
                        ? KLineSequenceClass::kHoleFill
                        : KLineSequenceClass::kOrdered;
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
            case ingest::TickDispatchKind::kGapOpen:
                stats_.gap_open_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return FlushOwner(owner);
            case ingest::TickDispatchKind::kChannelSeal:
                stats_.channel_seal_controls_received.fetch_add(
                    1U, std::memory_order_relaxed);
                return FlushOwner(owner);
        }
        SetFatal("KLine runtime received an unknown disposition");
        return false;
    }

    [[nodiscard]] bool FlushDue(std::size_t owner,
                                std::uint64_t monotonic_ns) noexcept {
        if (!healthy() || owner >= owners_.size() ||
            !owners_[owner]->worker->DrainRevisionCommits()) {
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
        return FlushOwner(owner) &&
               owners_[owner]->worker->DrainRevisionCommits();
    }

    [[nodiscard]] bool FlushAll() noexcept {
        bool result = true;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            result = Flush(owner) && result;
        }
        return result;
    }

    [[nodiscard]] bool DrainAll() noexcept {
        return FlushAll();
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
            stats_.gap_open_controls_received.load(
                std::memory_order_relaxed);
        result.channel_seal_controls_received =
            stats_.channel_seal_controls_received.load(
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
        return owner < owners_.size() ? owners_[owner]->worker.get() : nullptr;
    }

    [[nodiscard]] std::size_t owner_for_instrument(
        std::uint32_t instrument_ordinal) const noexcept {
        return static_cast<std::size_t>(instrument_ordinal) % owners_.size();
    }

    [[nodiscard]] const KLineRuntimeConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool FlushOwner(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.active.empty()) {
            return state.worker->DrainRevisionCommits();
        }
        const std::uint64_t now = ingest::MonotonicNowNs();
        const std::uint64_t age = now >= state.active_started_ns
            ? now - state.active_started_ns
            : 0U;
        const std::size_t rows = state.active.size();
        const KLineApplyResult applied = state.worker->ApplyBatch(state.active);
        if (!ApplySucceeded(applied.code)) {
            SetFatal("KLine worker failed a durable outbox micro-batch");
            return false;
        }
        if (applied.code == KLineApplyCode::kSourceConflict) {
            stats_.source_conflicts.fetch_add(1U, std::memory_order_relaxed);
        }
        if (applied.code == KLineApplyCode::kInvalidInput) {
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
        return state.worker->DrainRevisionCommits();
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
    const auto fail = [error](const char* message) noexcept {
        if (error != nullptr) {
            try {
                *error = message;
            } catch (...) {
            }
        }
        return false;
    };
    if (!ValidateKLineWorkerConfig(config.worker, error)) {
        return false;
    }
    if (config.feed_session_epoch == 0U ||
        config.feed_session_epoch != config.worker.feed_session_epoch ||
        config.micro_batch_rows == 0U ||
        config.micro_batch_max_delay_ns == 0U) {
        return fail("invalid KLine runtime durable-outbox configuration");
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
        for (std::uint32_t owner = 0U;
             owner < config.worker.owner_count; ++owner) {
            KLineWorkerConfig worker_config = config.worker;
            worker_config.owner = owner;
            auto state = std::make_unique<Impl::OwnerState>();
            state->active.reserve(config.micro_batch_rows);
            state->worker = KLineWorker::Create(
                std::move(worker_config), sink, error);
            if (state->worker == nullptr) {
                return nullptr;
            }
            owners.push_back(std::move(state));
        }
        return std::unique_ptr<KLineRuntime>(new KLineRuntime(
            std::make_unique<Impl>(std::move(config), std::move(owners))));
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
    const ingest::TickDispatch& dispatch,
    outbox::WalPosition position) noexcept {
    return impl_->AppendDispatch(owner, dispatch, position);
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

bool KLineRuntime::healthy() const noexcept { return impl_->healthy(); }

std::string KLineRuntime::fatal_error() const {
    return impl_->fatal_error();
}

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
