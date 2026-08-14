#include "l2flow/kline/runtime.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <span>
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
    destination->revision_batches_submitted +=
        source.revision_batches_submitted;
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
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOwner(owner) || !healthy()) {
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
        if (!ValidOwner(owner) || !healthy()) {
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
        if (!ValidOwner(owner) || !healthy()) {
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
        bool pending_commits = false;
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            KLineWorker* const worker = owners_[owner]->worker.get();
            result = worker->DrainDurableCommits() && result;
            if (worker->stats().pending_raw_commits != 0U) {
                pending_commits = true;
                result = false;
            }
        }
        if (pending_commits) {
            SetFatal("KLine runtime drain has unresolved pending commits");
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
        return AppendInput(owner, std::move(input),
                           dispatch.tick.common.receive_monotonic_ns);
    }

    [[nodiscard]] bool ApplyRejection(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!ValidOccurrenceDispatch(owner, dispatch)) {
            SetFatal("KLine reject disposition is invalid");
            return false;
        }
        return true;
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
        config.micro_batch_max_delay_ns == 0U) {
        if (error != nullptr) {
            *error = "invalid KLine runtime configuration";
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
            auto state = std::make_unique<Impl::OwnerState>();
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
