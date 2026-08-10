#include "l2flow/kline/worker.h"

#include "l2flow/clickhouse/raw_sink.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace l2flow::kline {
namespace {

using ingest::CanonicalKind;
using ingest::CanonicalTick;
using ingest::Market;
using ingest::TickAction;

template <typename Value>
using HashRaw = std::conditional_t<
    std::is_enum_v<Value>,
    std::underlying_type<Value>,
    std::type_identity<Value>>;

template <typename Value>
void HashAppend(std::uint64_t* state, Value value) noexcept {
    static_assert(std::is_integral_v<Value> || std::is_enum_v<Value>);
    using Raw = typename HashRaw<Value>::type;
    using Unsigned = std::make_unsigned_t<Raw>;
    const auto encoded = static_cast<std::uint64_t>(
        static_cast<Unsigned>(static_cast<Raw>(value)));
    *state ^= encoded + UINT64_C(0x9e3779b97f4a7c15) + (*state << 6U) +
              (*state >> 2U);
}

struct KLineKeyHash final {
    [[nodiscard]] std::size_t operator()(const KLineKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0x13198a2e03707344);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.instrument_id);
        HashAppend(&state, key.interval_seconds);
        HashAppend(&state, key.bucket_start_ns_from_midnight);
        return static_cast<std::size_t>(state);
    }
};

struct RawTickDependencyHash final {
    [[nodiscard]] std::size_t operator()(
        const RawTickDependency& dependency) const noexcept {
        std::uint64_t state = UINT64_C(0xa4093822299f31d0);
        HashAppend(&state, dependency.ingress_sequence);
        HashAppend(&state, dependency.kind);
        return static_cast<std::size_t>(state);
    }
};

[[nodiscard]] bool IsZero(Identifier128 value) noexcept {
    return std::all_of(value.bytes.begin(), value.bytes.end(),
                       [](std::byte octet) {
                           return octet == std::byte{0U};
                       });
}

class HashInput final {
public:
    explicit HashInput(std::size_t reserve) { bytes_.reserve(reserve); }

    void AppendIdentifier(Identifier128 value) {
        bytes_.insert(bytes_.end(), value.bytes.begin(), value.bytes.end());
    }

    template <typename Value>
    void Append(Value value) {
        static_assert(std::is_integral_v<Value> || std::is_enum_v<Value>);
        if constexpr (std::is_enum_v<Value>) {
            Append(static_cast<std::underlying_type_t<Value>>(value));
        } else if constexpr (std::is_same_v<Value, bool>) {
            Append(static_cast<std::uint8_t>(value ? 1U : 0U));
        } else {
            using Unsigned = std::make_unsigned_t<Value>;
            const Unsigned encoded = static_cast<Unsigned>(value);
            for (std::size_t index = 0U; index < sizeof(Value); ++index) {
                bytes_.push_back(static_cast<std::byte>(
                    static_cast<std::uint64_t>(encoded) >> (index * 8U)));
            }
        }
    }

    [[nodiscard]] Identifier128 Finish() const noexcept {
        Identifier128 result = clickhouse::Blake3Hash128(bytes_);
        if (IsZero(result)) {
            result.bytes[0U] = std::byte{1U};
        }
        return result;
    }

private:
    std::vector<std::byte> bytes_;
};

void AppendKey(HashInput* hash, const KLineKey& key) {
    hash->Append(key.trade_date);
    hash->Append(key.market);
    hash->Append(key.instrument_id);
    hash->Append(key.interval_seconds);
    hash->Append(key.bucket_start_ns_from_midnight);
}

void AppendAnchor(HashInput* hash, const TradeAnchor& anchor) {
    hash->Append(anchor.exchange_time_ns_from_midnight);
    hash->Append(anchor.channel);
    hash->Append(anchor.native_sequence);
    hash->Append(anchor.ingress_sequence);
}

[[nodiscard]] Identifier128 HashPayload(const KLinePayload& payload) {
    HashInput hash(160U);
    hash.Append(payload.bucket_end_ns_from_midnight);
    hash.Append(payload.open_price_p6);
    hash.Append(payload.high_price_p6);
    hash.Append(payload.low_price_p6);
    hash.Append(payload.close_price_p6);
    hash.Append(payload.volume);
    hash.Append(payload.notional_p6);
    hash.Append(payload.trade_count);
    AppendAnchor(&hash, payload.first_trade);
    AppendAnchor(&hash, payload.last_trade);
    hash.Append(payload.source_quality_flags);
    hash.Append(payload.has_late_recovery);
    hash.Append(payload.provisional);
    return hash.Finish();
}

[[nodiscard]] Identifier128 HashContribution(const CanonicalTick& tick) {
    HashInput hash(128U);
    hash.Append(tick.common.trade_date);
    hash.Append(tick.common.identity.market);
    hash.Append(tick.common.instrument_id);
    hash.Append(tick.common.channel);
    hash.Append(tick.common.native_sequence);
    hash.Append(tick.common.exchange_time_ns_from_midnight);
    hash.Append(tick.price.p6);
    hash.Append(tick.quantity.raw);
    hash.Append(tick.common.quality_flags);
    return hash.Finish();
}

[[nodiscard]] Identifier128 HashInputSet(
    const KLineKey& key,
    const std::array<std::byte, 16U>& contribution_xor,
    std::uint64_t trade_count) {
    HashInput hash(64U);
    AppendKey(&hash, key);
    for (const std::byte octet : contribution_xor) {
        hash.Append(std::to_integer<std::uint8_t>(octet));
    }
    hash.Append(trade_count);
    return hash.Finish();
}

[[nodiscard]] Identifier128 RecoveryRunIdentifier(
    Identifier128 calculation_run_id,
    std::uint32_t owner,
    std::uint64_t batch_sequence) {
    HashInput hash(48U);
    hash.AppendIdentifier(calculation_run_id);
    hash.Append(owner);
    hash.Append(batch_sequence);
    hash.Append(kKLineSchemaVersion);
    hash.Append(std::uint8_t{1U});
    return hash.Finish();
}

[[nodiscard]] Identifier128 RevisionIdentifier(
    Identifier128 calculation_run_id,
    Identifier128 recovery_run_id,
    std::uint64_t version,
    Identifier128 payload_hash,
    RevisionOperation operation) {
    HashInput hash(64U);
    hash.AppendIdentifier(calculation_run_id);
    hash.AppendIdentifier(recovery_run_id);
    hash.Append(version);
    hash.AppendIdentifier(payload_hash);
    hash.Append(operation);
    return hash.Finish();
}

[[nodiscard]] bool StructurallyValid(const KLineWorkerConfig& config,
                                     const KLineInput& input) noexcept {
    const CanonicalTick& tick = input.tick;
    const Market market = tick.common.identity.market;
    if (!input.catalog_match || tick.common.trade_date != config.trade_date ||
        tick.common.instrument_id == 0U ||
        tick.common.native_sequence == 0U ||
        tick.common.ingress_sequence == 0U ||
        tick.common.instrument_ordinal == ingest::kInvalidInstrumentOrdinal ||
        tick.common.instrument_ordinal % config.owner_count != config.owner ||
        (market != Market::kShanghai && market != Market::kShenzhen)) {
        return false;
    }
    if (market == Market::kShanghai) {
        return tick.common.channel != 0U &&
               tick.common.kind == CanonicalKind::kShanghaiTick;
    }
    // The canonical decoder intentionally permits Shenzhen ChannelNo == 0.
    return tick.common.kind == CanonicalKind::kShenzhenOrder ||
           tick.common.kind == CanonicalKind::kShenzhenTransaction;
}

[[nodiscard]] bool ExchangeTimeValidForKLine(
    const CanonicalTick& tick) noexcept {
    return tick.common.exchange_time_valid &&
           (tick.validity & ingest::kTickExchangeTimeValid) != 0U &&
           tick.common.exchange_time_ns_from_midnight < kNanosecondsPerDay;
}

[[nodiscard]] bool TradeProjectionValid(const CanonicalTick& tick) noexcept {
    if (tick.action != TickAction::kTrade ||
        !ExchangeTimeValidForKLine(tick) ||
        (tick.validity & ingest::kTickPriceValid) == 0U ||
        !tick.price.p6_valid || tick.price.p6 <= 0 ||
        (tick.validity & ingest::kTickQuantityValid) == 0U ||
        !tick.quantity.valid || tick.quantity.scale != 0U ||
        tick.quantity.raw <= 0) {
        return false;
    }
    return tick.common.identity.market == Market::kShanghai
        ? tick.common.kind == CanonicalKind::kShanghaiTick
        : tick.common.kind == CanonicalKind::kShenzhenTransaction;
}

[[nodiscard]] bool AnchorLess(const TradeAnchor& left,
                              const TradeAnchor& right) noexcept {
    if (left.exchange_time_ns_from_midnight !=
        right.exchange_time_ns_from_midnight) {
        return left.exchange_time_ns_from_midnight <
               right.exchange_time_ns_from_midnight;
    }
    if (left.channel != right.channel) {
        return left.channel < right.channel;
    }
    return left.native_sequence < right.native_sequence;
}

[[nodiscard]] TradeAnchor MakeAnchor(const CanonicalTick& tick) noexcept {
    return TradeAnchor{tick.common.exchange_time_ns_from_midnight,
                       tick.common.channel,
                       tick.common.native_sequence,
                       tick.common.ingress_sequence};
}

[[nodiscard]] bool CheckedAdd(std::int64_t left,
                              std::int64_t right,
                              std::int64_t* output) noexcept {
    if (left < 0 || right < 0 ||
        left > std::numeric_limits<std::int64_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedMultiply(std::int64_t left,
                                   std::int64_t right,
                                   std::int64_t* output) noexcept {
    if (left <= 0 || right <= 0 ||
        left > std::numeric_limits<std::int64_t>::max() / right) {
        return false;
    }
    *output = left * right;
    return true;
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1970U || year > 9999U || month == 0U || month > 12U ||
        day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days[month - 1U];
    const bool leap = (year % 4U == 0U && year % 100U != 0U) ||
                      year % 400U == 0U;
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

struct AtomicStats final {
    std::atomic<std::uint64_t> facts_journaled{0U};
    std::atomic<std::uint64_t> trades_projected{0U};
    std::atomic<std::uint64_t> duplicate_facts{0U};
    std::atomic<std::uint64_t> source_conflicts{0U};
    std::atomic<std::uint64_t> invalid_facts{0U};
    std::atomic<std::uint64_t> invalid_trade_exchange_times{0U};
    std::atomic<std::uint64_t> bars_created{0U};
    std::atomic<std::uint64_t> bars_updated{0U};
    std::atomic<std::uint64_t> revisions_created{0U};
    std::atomic<std::uint64_t> pending_raw_commits{0U};
    std::atomic<std::uint64_t> acknowledged_raw_dependencies{0U};
    std::atomic<std::uint64_t> revision_batches_submitted{0U};
};

}  // namespace

class KLineWorker::Impl final {
public:
    struct FactRecord final {
        FactKey key{};
        CanonicalTick tick{};
        Identifier128 contribution_hash{};
        bool projected_trade = false;
        bool late = false;
    };

    struct BarState final {
        KLinePayload payload{};
        std::array<std::byte, 16U> contribution_xor{};
    };

    struct BarHead final {
        Identifier128 revision_id{};
        Identifier128 payload_hash{};
        bool deleted = false;
    };

    struct PendingCommit final {
        std::shared_ptr<const KLineRevisionBatch> batch;
        std::set<RawTickDependency> raw_dependencies;
    };

    Impl(KLineWorkerConfig config, KLineRevisionSink* sink)
        : config_(std::move(config)), sink_(sink) {
        bars_.reserve(config_.maximum_bars);
        heads_.reserve(config_.maximum_bars);
        acknowledged_raw_.reserve(
            config_.maximum_acknowledged_raw_dependencies);
    }

    [[nodiscard]] KLineApplyResult ApplyBatch(
        std::span<const KLineInput> inputs) noexcept {
        KLineApplyResult result{};
        if (!healthy_) {
            result.code = KLineApplyCode::kFailed;
            return result;
        }
        if (inputs.empty()) {
            static_cast<void>(DrainDurableCommits());
            return result;
        }
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            return CapacityFailure(&result,
                                   "KLine pending raw commit capacity exhausted");
        }

        try {
            std::set<RawTickDependency> dependencies;
            std::vector<CanonicalTick> admission_ticks;
            std::vector<const KLineInput*> admission_inputs;
            admission_ticks.reserve(inputs.size());
            admission_inputs.reserve(inputs.size());
            std::vector<FactRecord> inserted;
            inserted.reserve(inputs.size());
            bool saw_conflict = false;
            bool saw_invalid = false;

            // Journal the complete cut before any bar is projected.
            for (const KLineInput& input : inputs) {
                if (!StructurallyValid(config_, input)) {
                    saw_invalid = true;
                    stats_.invalid_facts.fetch_add(1U,
                                                   std::memory_order_relaxed);
                    continue;
                }
                dependencies.insert(RawTickDependency{
                    input.tick.common.ingress_sequence,
                    input.tick.common.kind});
                if (input.upstream_conflict) {
                    saw_conflict = true;
                    stats_.source_conflicts.fetch_add(
                        1U, std::memory_order_relaxed);
                    continue;
                }
                admission_ticks.push_back(input.tick);
                admission_inputs.push_back(&input);
            }

            std::vector<CanonicalTick> admission_winners(
                admission_ticks.size());
            const std::vector<journal::AdmitResult> admissions =
                config_.fact_journal->AdmitBatch(
                    journal::FactConsumer::kKLine, admission_ticks,
                    admission_winners);
            if (admissions.size() != admission_ticks.size() ||
                !config_.fact_journal->healthy()) {
                return Failure(
                    &result,
                    config_.fact_journal->fatal_error().empty()
                        ? "KLine FactJournal admission failed"
                        : config_.fact_journal->fatal_error());
            }
            for (std::size_t index = 0U; index < admissions.size(); ++index) {
                const journal::AdmitResult& admission = admissions[index];
                const KLineInput& input = *admission_inputs[index];
                if (admission.code == journal::AdmitCode::kDuplicate) {
                    ++result.duplicates;
                    stats_.duplicate_facts.fetch_add(
                        1U, std::memory_order_relaxed);
                    continue;
                }
                if (admission.code == journal::AdmitCode::kConflict) {
                    saw_conflict = true;
                    stats_.source_conflicts.fetch_add(
                        1U, std::memory_order_relaxed);
                    continue;
                }
                if (admission.code != journal::AdmitCode::kNew) {
                    return Failure(&result,
                                   "KLine FactJournal admission failed");
                }
                FactRecord record{};
                record.key = FactKey{
                    input.tick.common.trade_date,
                    input.tick.common.identity.market,
                    input.tick.common.channel,
                    input.tick.common.native_sequence};
                record.tick = admission_winners[index];
                record.late = input.late_recovery;
                record.projected_trade = TradeProjectionValid(record.tick);
                if (record.projected_trade) {
                    record.contribution_hash = HashContribution(record.tick);
                }
                if (record.tick.action == TickAction::kTrade &&
                    !record.projected_trade) {
                    saw_invalid = true;
                    stats_.invalid_facts.fetch_add(
                        1U, std::memory_order_relaxed);
                    if (!ExchangeTimeValidForKLine(record.tick)) {
                        stats_.invalid_trade_exchange_times.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                }
                inserted.push_back(std::move(record));
                ++result.facts_inserted;
                stats_.facts_journaled.fetch_add(
                    1U, std::memory_order_relaxed);
            }

            std::map<KLineKey, BarState> bar_patch;
            std::set<KLineKey> new_bars;
            bool batch_has_late_trade = false;
            for (const FactRecord& record : inserted) {
                if (!record.projected_trade) {
                    continue;
                }
                ++result.trades_inserted;
                stats_.trades_projected.fetch_add(
                    1U, std::memory_order_relaxed);
                batch_has_late_trade = batch_has_late_trade || record.late;
                for (const std::uint32_t interval : config_.interval_seconds) {
                    const std::uint64_t duration =
                        static_cast<std::uint64_t>(interval) *
                        kNanosecondsPerSecond;
                    const std::uint64_t start =
                        record.tick.common.exchange_time_ns_from_midnight /
                        duration * duration;
                    const KLineKey bar_key{
                        record.key.trade_date, record.key.market,
                        record.tick.common.instrument_id, interval, start};
                    auto patched = bar_patch.find(bar_key);
                    if (patched == bar_patch.end()) {
                        const auto live = bars_.find(bar_key);
                        if (live == bars_.end()) {
                            new_bars.insert(bar_key);
                            patched = bar_patch.emplace(
                                bar_key, BarState{}).first;
                        } else {
                            patched = bar_patch.emplace(
                                bar_key, live->second).first;
                        }
                    }
                    if (!AddTrade(bar_key, record, &patched->second)) {
                        return Failure(
                            &result,
                            "KLine volume or notional arithmetic overflow");
                    }
                }
            }
            if (new_bars.size() > config_.maximum_bars - bars_.size()) {
                return CapacityFailure(&result,
                                       "KLine bar capacity exhausted");
            }

            std::shared_ptr<KLineRevisionBatch> revision_batch;
            std::map<KLineKey, BarHead> head_patch;
            std::uint32_t next_counter = next_revision_counter_;
            if (!bar_patch.empty()) {
                const std::uint64_t sequence = next_batch_sequence_;
                if (sequence == 0U ||
                    sequence == std::numeric_limits<std::uint64_t>::max()) {
                    return Failure(&result,
                                   "KLine calculation batch sequence exhausted");
                }
                const Identifier128 recovery_run_id = RecoveryRunIdentifier(
                    config_.calculation_run_id, config_.owner, sequence);
                revision_batch = std::make_shared<KLineRevisionBatch>();
                revision_batch->calculation_run_id = config_.calculation_run_id;
                revision_batch->recovery_run_id = recovery_run_id;
                revision_batch->owner = config_.owner;
                revision_batch->batch_sequence = sequence;
                revision_batch->reason = batch_has_late_trade
                    ? RevisionReason::kLateRecovery
                    : RevisionReason::kLiveProjection;

                for (const auto& [bar_key, state] : bar_patch) {
                    const auto live = bars_.find(bar_key);
                    if (live != bars_.end() &&
                        live->second.payload == state.payload) {
                        continue;
                    }
                    KLineRevision revision{};
                    revision.key = bar_key;
                    if (!AllocateVersion(&next_counter, &revision.version)) {
                        return Failure(
                            &result, "KLine revision version space exhausted");
                    }
                    revision.recovery_run_id = recovery_run_id;
                    revision.operation = live == bars_.end()
                        ? RevisionOperation::kInsert
                        : RevisionOperation::kUpdate;
                    revision.reason = revision_batch->reason;
                    revision.calculation_run_id = config_.calculation_run_id;
                    revision.logic_version = config_.logic_version;
                    revision.input_set_hash = HashInputSet(
                        bar_key, state.contribution_xor,
                        state.payload.trade_count);
                    revision.payload_hash = HashPayload(state.payload);
                    revision.payload = state.payload;
                    if (const auto head = heads_.find(bar_key);
                        head != heads_.end()) {
                        revision.supersedes_revision_id =
                            head->second.revision_id;
                        revision.supersedes_revision_id_valid = true;
                    }
                    revision.revision_id = RevisionIdentifier(
                        config_.calculation_run_id, recovery_run_id,
                        revision.version, revision.payload_hash,
                        revision.operation);
                    head_patch[bar_key] = BarHead{
                        revision.revision_id, revision.payload_hash, false};
                    revision_batch->revisions.push_back(std::move(revision));
                    ++result.changed_bars;
                    ++result.revisions_created;
                    if (live == bars_.end()) {
                        stats_.bars_created.fetch_add(
                            1U, std::memory_order_relaxed);
                    } else {
                        stats_.bars_updated.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                }
                ++next_batch_sequence_;
            }

            for (auto& [key, state] : bar_patch) {
                bars_[key] = std::move(state);
            }
            for (const auto& [key, head] : head_patch) {
                heads_[key] = head;
            }
            next_revision_counter_ = next_counter;
            const std::size_t revision_count = revision_batch == nullptr
                ? 0U
                : revision_batch->revisions.size();
            if (revision_count != 0U || !dependencies.empty()) {
                pending_commits_.push_back(PendingCommit{
                    std::move(revision_batch), std::move(dependencies)});
                stats_.pending_raw_commits.store(
                    pending_commits_.size(), std::memory_order_relaxed);
            }
            stats_.revisions_created.fetch_add(
                static_cast<std::uint64_t>(revision_count),
                std::memory_order_relaxed);

            result.code = saw_conflict
                ? KLineApplyCode::kSourceConflict
                : (saw_invalid
                       ? KLineApplyCode::kInvalidInput
                       : (inserted.empty()
                              ? KLineApplyCode::kDuplicateOnly
                              : KLineApplyCode::kApplied));
            if (!DrainDurableCommits()) {
                result.code = KLineApplyCode::kSinkFailed;
            }
            return result;
        } catch (const std::bad_alloc&) {
            return CapacityFailure(&result, "KLine worker allocation failed");
        } catch (const std::exception& exception) {
            return Failure(&result,
                           std::string("KLine worker failed: ") +
                               exception.what());
        } catch (...) {
            return Failure(&result,
                           "KLine worker failed with unknown exception");
        }
    }

    void AcknowledgeRawTicks(
        std::span<const RawTickDependency> dependencies) noexcept {
        if (!healthy_) {
            return;
        }
        try {
            for (const RawTickDependency& dependency : dependencies) {
                if (acknowledged_raw_.contains(dependency)) {
                    continue;
                }
                if (acknowledged_raw_.size() >=
                    config_.maximum_acknowledged_raw_dependencies) {
                    SetFatal("KLine raw ACK index capacity exhausted");
                    return;
                }
                acknowledged_raw_.insert(dependency);
            }
            stats_.acknowledged_raw_dependencies.store(
                acknowledged_raw_.size(), std::memory_order_relaxed);
        } catch (...) {
            SetFatal("KLine raw ACK index allocation failed");
        }
    }

    [[nodiscard]] bool DrainDurableCommits() noexcept {
        if (!healthy_) {
            return false;
        }
        while (!pending_commits_.empty()) {
            PendingCommit& pending = pending_commits_.front();
            const bool durable = std::all_of(
                pending.raw_dependencies.begin(),
                pending.raw_dependencies.end(),
                [this](const RawTickDependency& dependency) {
                    return acknowledged_raw_.contains(dependency);
                });
            if (!durable) {
                break;
            }
            if (pending.batch != nullptr &&
                !pending.batch->revisions.empty()) {
                if (!sink_->AppendRevisionBatch(pending.batch)) {
                    SetFatal("KLine revision sink rejected an immutable batch");
                    return false;
                }
                stats_.revision_batches_submitted.fetch_add(
                    1U, std::memory_order_relaxed);
            }
            for (const RawTickDependency& dependency :
                 pending.raw_dependencies) {
                acknowledged_raw_.erase(dependency);
            }
            pending_commits_.pop_front();
            stats_.pending_raw_commits.store(
                pending_commits_.size(), std::memory_order_relaxed);
            stats_.acknowledged_raw_dependencies.store(
                acknowledged_raw_.size(), std::memory_order_relaxed);
        }
        return true;
    }

    [[nodiscard]] bool CopyBar(const KLineKey& key,
                               KLinePayload* output) const noexcept {
        if (output == nullptr) {
            return false;
        }
        const auto position = bars_.find(key);
        if (position == bars_.end()) {
            return false;
        }
        *output = position->second.payload;
        return true;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] KLineWorkerStats stats() const noexcept {
        KLineWorkerStats result{};
        result.facts_journaled = stats_.facts_journaled.load(
            std::memory_order_relaxed);
        result.trades_projected = stats_.trades_projected.load(
            std::memory_order_relaxed);
        result.duplicate_facts = stats_.duplicate_facts.load(
            std::memory_order_relaxed);
        result.source_conflicts = stats_.source_conflicts.load(
            std::memory_order_relaxed);
        result.invalid_facts = stats_.invalid_facts.load(
            std::memory_order_relaxed);
        result.invalid_trade_exchange_times =
            stats_.invalid_trade_exchange_times.load(
                std::memory_order_relaxed);
        result.bars_created = stats_.bars_created.load(
            std::memory_order_relaxed);
        result.bars_updated = stats_.bars_updated.load(
            std::memory_order_relaxed);
        result.revisions_created = stats_.revisions_created.load(
            std::memory_order_relaxed);
        result.pending_raw_commits = stats_.pending_raw_commits.load(
            std::memory_order_relaxed);
        result.acknowledged_raw_dependencies =
            stats_.acknowledged_raw_dependencies.load(
                std::memory_order_relaxed);
        result.revision_batches_submitted =
            stats_.revision_batches_submitted.load(
                std::memory_order_relaxed);
        return result;
    }

    [[nodiscard]] const KLineWorkerConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool AddTrade(const KLineKey& key,
                                const FactRecord& record,
                                BarState* state) noexcept {
        const CanonicalTick& tick = record.tick;
        const TradeAnchor anchor = MakeAnchor(tick);
        std::int64_t notional = 0;
        if (!CheckedMultiply(tick.price.p6, tick.quantity.raw, &notional)) {
            return false;
        }
        KLinePayload& payload = state->payload;
        if (payload.trade_count == 0U) {
            const std::uint64_t duration =
                static_cast<std::uint64_t>(key.interval_seconds) *
                kNanosecondsPerSecond;
            payload.bucket_end_ns_from_midnight =
                key.bucket_start_ns_from_midnight + duration;
            payload.open_price_p6 = tick.price.p6;
            payload.high_price_p6 = tick.price.p6;
            payload.low_price_p6 = tick.price.p6;
            payload.close_price_p6 = tick.price.p6;
            payload.volume = tick.quantity.raw;
            payload.notional_p6 = notional;
            payload.trade_count = 1U;
            payload.first_trade = anchor;
            payload.last_trade = anchor;
        } else {
            if (payload.trade_count ==
                std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }
            std::int64_t volume = 0;
            std::int64_t aggregate_notional = 0;
            if (!CheckedAdd(payload.volume, tick.quantity.raw, &volume) ||
                !CheckedAdd(payload.notional_p6, notional,
                            &aggregate_notional)) {
                return false;
            }
            payload.volume = volume;
            payload.notional_p6 = aggregate_notional;
            ++payload.trade_count;
            payload.high_price_p6 = std::max(payload.high_price_p6,
                                             tick.price.p6);
            payload.low_price_p6 = std::min(payload.low_price_p6,
                                            tick.price.p6);
            if (AnchorLess(anchor, payload.first_trade)) {
                payload.first_trade = anchor;
                payload.open_price_p6 = tick.price.p6;
            }
            if (AnchorLess(payload.last_trade, anchor)) {
                payload.last_trade = anchor;
                payload.close_price_p6 = tick.price.p6;
            }
        }
        payload.source_quality_flags |= tick.common.quality_flags;
        payload.has_late_recovery =
            payload.has_late_recovery || record.late;
        for (std::size_t index = 0U; index < state->contribution_xor.size();
             ++index) {
            state->contribution_xor[index] ^=
                record.contribution_hash.bytes[index];
        }
        return true;
    }

    [[nodiscard]] bool AllocateVersion(std::uint32_t* counter,
                                       std::uint64_t* output) const noexcept {
        if (counter == nullptr || output == nullptr || *counter == 0U ||
            *counter == std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
        *output = (static_cast<std::uint64_t>(config_.revision_epoch) << 32U) |
                  *counter;
        ++(*counter);
        return true;
    }

    [[nodiscard]] KLineApplyResult Failure(KLineApplyResult* result,
                                           std::string message) noexcept {
        SetFatal(std::move(message));
        result->code = KLineApplyCode::kFailed;
        return *result;
    }

    [[nodiscard]] KLineApplyResult CapacityFailure(
        KLineApplyResult* result,
        std::string message) noexcept {
        SetFatal(std::move(message));
        result->code = KLineApplyCode::kCapacityExhausted;
        return *result;
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

    KLineWorkerConfig config_{};
    KLineRevisionSink* sink_ = nullptr;
    std::unordered_map<KLineKey, BarState, KLineKeyHash> bars_;
    std::unordered_map<KLineKey, BarHead, KLineKeyHash> heads_;
    std::deque<PendingCommit> pending_commits_;
    std::unordered_set<RawTickDependency, RawTickDependencyHash>
        acknowledged_raw_;
    std::uint32_t next_revision_counter_ = 1U;
    std::uint64_t next_batch_sequence_ = 1U;
    AtomicStats stats_{};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

bool ValidateKLineWorkerConfig(const KLineWorkerConfig& config,
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
    if (!ValidTradeDate(config.trade_date) || config.owner_count == 0U ||
        config.owner >= config.owner_count || config.revision_epoch == 0U ||
        config.logic_version == 0U || IsZero(config.calculation_run_id) ||
        config.interval_seconds.empty() ||
        config.maximum_bars == 0U || config.maximum_pending_commits == 0U ||
        config.maximum_acknowledged_raw_dependencies == 0U) {
        return fail("invalid KLine worker configuration");
    }
    if (!std::is_sorted(config.interval_seconds.begin(),
                        config.interval_seconds.end()) ||
        std::adjacent_find(config.interval_seconds.begin(),
                           config.interval_seconds.end()) !=
            config.interval_seconds.end() ||
        std::any_of(config.interval_seconds.begin(),
                    config.interval_seconds.end(),
                    [](std::uint32_t interval) {
                        return interval == 0U || interval > 86'400U;
                    })) {
        return fail("KLine intervals must be sorted unique values in [1,86400]");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

std::unique_ptr<KLineWorker> KLineWorker::Create(
    KLineWorkerConfig config,
    KLineRevisionSink* sink,
    std::string* error) {
    if (!ValidateKLineWorkerConfig(config, error)) {
        return nullptr;
    }
    if (sink == nullptr) {
        if (error != nullptr) {
            *error = "KLine revision sink is null";
        }
        return nullptr;
    }
    if (config.fact_journal == nullptr) {
        if (error != nullptr) {
            *error = "KLine FactJournal is null";
        }
        return nullptr;
    }
    if (!config.fact_journal->healthy()) {
        if (error != nullptr) {
            *error = "KLine FactJournal is unhealthy: " +
                     config.fact_journal->fatal_error();
        }
        return nullptr;
    }
    try {
        return std::unique_ptr<KLineWorker>(new KLineWorker(
            std::make_unique<Impl>(std::move(config), sink)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("KLine worker creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

KLineWorker::KLineWorker(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KLineWorker::~KLineWorker() = default;

KLineApplyResult KLineWorker::ApplyBatch(
    std::span<const KLineInput> inputs) noexcept {
    return impl_->ApplyBatch(inputs);
}

void KLineWorker::AcknowledgeRawTicks(
    std::span<const RawTickDependency> dependencies) noexcept {
    impl_->AcknowledgeRawTicks(dependencies);
}

bool KLineWorker::DrainDurableCommits() noexcept {
    return impl_->DrainDurableCommits();
}

bool KLineWorker::CopyBar(const KLineKey& key,
                          KLinePayload* output) const noexcept {
    return impl_->CopyBar(key, output);
}

bool KLineWorker::healthy() const noexcept { return impl_->healthy(); }

std::string KLineWorker::fatal_error() const { return impl_->fatal_error(); }

KLineWorkerStats KLineWorker::stats() const noexcept {
    return impl_->stats();
}

const KLineWorkerConfig& KLineWorker::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::kline
