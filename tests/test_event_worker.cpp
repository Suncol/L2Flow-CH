#include "l2flow/event/runtime.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace l2flow::event;
using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

class RecordingSink final : public EventRevisionSink {
public:
    [[nodiscard]] bool AppendRevisionBatch(
        std::shared_ptr<const EventRevisionBatch> batch) noexcept override {
        try {
            batches.push_back(std::move(batch));
            return accept;
        } catch (...) {
            return false;
        }
    }

    bool accept = true;
    std::vector<std::shared_ptr<const EventRevisionBatch>> batches;
};

void SetIdentity(CanonicalCommon* common, Market market) {
    common->identity.market = market;
    const std::string security = market == Market::kShanghai
        ? "600000"
        : "000001";
    common->identity.security_id_size =
        static_cast<std::uint8_t>(security.size());
    std::copy(security.begin(), security.end(),
              reinterpret_cast<char*>(
                  common->identity.security_id.data()));
    if (market == Market::kShenzhen) {
        common->identity.security_id_source_size = 2U;
        common->identity.security_id_source[0U] = std::byte{'S'};
        common->identity.security_id_source[1U] = std::byte{'Z'};
    }
}

CanonicalTick BaseTick(Market market,
                       CanonicalKind kind,
                       std::uint64_t sequence,
                       std::uint64_t ingress) {
    CanonicalTick tick{};
    tick.common.trade_date = 20260807U;
    tick.common.instrument_id = 1U;
    tick.common.instrument_ordinal = 0U;
    tick.common.channel = 7U;
    tick.common.native_sequence = sequence;
    tick.common.ingress_sequence = ingress;
    tick.common.vendor_sequence_id = 10'000U + ingress;
    tick.common.receive_monotonic_ns = 20'000U + ingress;
    tick.common.exchange_time_raw = 93'000'000U;
    tick.common.exchange_time_ns_from_midnight =
        UINT64_C(34'200'000'000'000) + sequence;
    tick.common.exchange_time_valid = true;
    tick.common.kind = kind;
    tick.common.message_key = market == Market::kShanghai
        ? MessageKey{4U, 101U, 24U}
        : (kind == CanonicalKind::kShenzhenOrder
               ? MessageKey{6U, 101U, 33U}
               : MessageKey{6U, 101U, 36U});
    SetIdentity(&tick.common, market);
    return tick;
}

CanonicalTick ShenzhenAdd(std::uint64_t sequence,
                          std::uint64_t ingress,
                          std::int64_t quantity = 100) {
    CanonicalTick tick = BaseTick(
        Market::kShenzhen, CanonicalKind::kShenzhenOrder,
        sequence, ingress);
    tick.action = TickAction::kAdd;
    tick.primary_order_id = static_cast<std::int64_t>(sequence);
    tick.side = Side::kBuy;
    tick.order_type = OrderType::kLimit;
    tick.price = {100'000, 10'000'000, 4U, true, true};
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPrimaryOrderIdValid | kTickSideValid |
                    kTickOrderTypeValid | kTickPriceValid |
                    kTickQuantityValid | kTickChannelHistoryValid;
    return tick;
}

CanonicalTick ShenzhenTrade(std::uint64_t sequence,
                            std::uint64_t ingress,
                            std::int64_t buy_order,
                            std::int64_t sell_order,
                            std::int64_t quantity = 10) {
    CanonicalTick tick = BaseTick(
        Market::kShenzhen, CanonicalKind::kShenzhenTransaction,
        sequence, ingress);
    tick.action = TickAction::kTrade;
    tick.buy_order_id = buy_order;
    tick.sell_order_id = sell_order;
    tick.price = {100'000, 10'000'000, 4U, true, true};
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPriceValid | kTickQuantityValid |
                    kTickChannelHistoryValid;
    if (buy_order > 0) {
        tick.validity |= kTickBuyOrderIdValid;
    }
    if (sell_order > 0) {
        tick.validity |= kTickSellOrderIdValid;
    }
    return tick;
}

CanonicalTick ShenzhenCancel(std::uint64_t sequence,
                             std::uint64_t ingress,
                             std::int64_t order,
                             std::int64_t quantity = 10) {
    CanonicalTick tick = BaseTick(
        Market::kShenzhen, CanonicalKind::kShenzhenTransaction,
        sequence, ingress);
    tick.action = TickAction::kCancel;
    tick.primary_order_id = order;
    tick.buy_order_id = order;
    tick.side = Side::kBuy;
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPrimaryOrderIdValid | kTickBuyOrderIdValid |
                    kTickSideValid | kTickQuantityValid |
                    kTickChannelHistoryValid;
    return tick;
}

CanonicalTick ShanghaiStatus(std::uint64_t sequence,
                             std::uint64_t ingress,
                             TradingPhase phase) {
    CanonicalTick tick = BaseTick(
        Market::kShanghai, CanonicalKind::kShanghaiTick,
        sequence, ingress);
    tick.action = TickAction::kStatus;
    tick.phase = phase;
    tick.validity = kTickPhaseValid | kTickChannelHistoryValid;
    return tick;
}

CanonicalTick ShanghaiAdd(std::uint64_t sequence,
                          std::uint64_t ingress,
                          std::int64_t order,
                          std::int64_t quantity = 100) {
    CanonicalTick tick = BaseTick(
        Market::kShanghai, CanonicalKind::kShanghaiTick,
        sequence, ingress);
    tick.action = TickAction::kAdd;
    tick.primary_order_id = order;
    tick.buy_order_id = order;
    tick.side = Side::kBuy;
    tick.price = {10'000, 10'000'000, 3U, true, true};
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPrimaryOrderIdValid | kTickSideValid |
                    kTickPriceValid | kTickQuantityValid |
                    kTickChannelHistoryValid;
    return tick;
}

CanonicalTick ShanghaiCancel(std::uint64_t sequence,
                             std::uint64_t ingress,
                             std::int64_t order,
                             std::int64_t quantity) {
    CanonicalTick tick = BaseTick(
        Market::kShanghai, CanonicalKind::kShanghaiTick,
        sequence, ingress);
    tick.action = TickAction::kCancel;
    tick.primary_order_id = order;
    tick.buy_order_id = order;
    tick.side = Side::kBuy;
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPrimaryOrderIdValid | kTickSideValid |
                    kTickQuantityValid | kTickChannelHistoryValid;
    return tick;
}

EventWorkerConfig Config() {
    EventWorkerConfig config{};
    config.trade_date = 20260807U;
    config.owner = 0U;
    config.owner_count = 1U;
    config.revision_epoch = 9U;
    config.calculation_run_id.bytes[0U] = std::byte{1U};
    config.maximum_facts = 1'024U;
    config.maximum_orders = 1'024U;
    config.maximum_cached_events = 4'096U;
    config.maximum_pending_commits = 64U;
    return config;
}

void Ack(EventWorker* worker, const CanonicalTick& tick) {
    const RawTickDependency dependency{
        tick.common.ingress_sequence, tick.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&dependency, 1U));
    CHECK(worker->DrainDurableCommits());
}

void DrainRepair(EventWorker* worker) {
    std::size_t slices = 0U;
    while (worker->repair_pending()) {
        CHECK(worker->AdvanceRepair());
        CHECK(++slices < 1'024U);
    }
    CHECK(worker->DrainDurableCommits());
}

const EventPayload& FindPayload(
    const std::vector<std::pair<EventKey, EventPayload>>& bundle,
    EventKind kind,
    std::int64_t order_id = 0) {
    const auto found = std::find_if(
        bundle.begin(), bundle.end(),
        [kind, order_id](const auto& row) {
            return row.first.event_kind == kind &&
                   row.first.affected_order_id == order_id;
        });
    CHECK(found != bundle.end());
    return found->second;
}

void TestLateAddRepairsOnlyReferencedChainAndWaitsForRawAck() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick trade = ShenzhenTrade(102U, 1U, 101, 0);
    const EventInput trade_input{trade};
    EventApplyResult applied = worker->ApplyBatch(
        std::span<const EventInput>(&trade_input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(sink.batches.empty());
    Ack(worker.get(), trade);
    CHECK(sink.batches.size() == 1U);
    CHECK(sink.batches[0U]->revisions.size() == 1U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 102U}, &bundle));
    const EventPayload& old_trade = FindPayload(
        bundle, EventKind::kShenzhenTrade);
    CHECK((old_trade.event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownBuyOrderReference)) != 0U);

    const CanonicalTick add = ShenzhenAdd(101U, 2U);
    EventInput add_input{add};
    add_input.late_recovery = true;
    add_input.committed_next_sequence = 103U;
    applied = worker->ApplyBatch(
        std::span<const EventInput>(&add_input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(sink.batches.size() == 1U);
    Ack(worker.get(), add);
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[1U]->revisions.size() == 3U);

    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 102U}, &bundle));
    const EventPayload& repaired_trade = FindPayload(
        bundle, EventKind::kShenzhenTrade);
    CHECK((repaired_trade.event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownBuyOrderReference)) == 0U);
    CHECK((repaired_trade.event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownSellOrderReference)) != 0U);
    static_cast<void>(FindPayload(
        bundle, EventKind::kShenzhenOrderRevision, 101));

    const auto& revisions = sink.batches[1U]->revisions;
    CHECK(std::count_if(revisions.begin(), revisions.end(),
                        [](const EventRevision& revision) {
                            return revision.operation ==
                                   RevisionOperation::kUpdate;
                        }) == 1);
    CHECK(std::count_if(revisions.begin(), revisions.end(),
                        [](const EventRevision& revision) {
                            return revision.operation ==
                                   RevisionOperation::kInsert;
                        }) == 2);
    CHECK(std::all_of(revisions.begin(), revisions.end(),
                      [](const EventRevision& revision) {
                          return revision.reason ==
                                 RevisionReason::kLateRecovery;
                      }));
}

void TestJournalFirstSameBatchDoesNotPublishUnknownIntermediate() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick trade = ShenzhenTrade(102U, 50U, 101, 0);
    const CanonicalTick add = ShenzhenAdd(101U, 51U);
    const std::array<EventInput, 2U> inputs{
        EventInput{trade}, EventInput{add}};
    const EventApplyResult applied = worker->ApplyBatch(inputs);
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(sink.batches.empty());

    const std::array<RawTickDependency, 2U> dependencies{
        RawTickDependency{trade.common.ingress_sequence, trade.common.kind},
        RawTickDependency{add.common.ingress_sequence, add.common.kind}};
    worker->AcknowledgeRawTicks(dependencies);
    CHECK(worker->DrainDurableCommits());
    CHECK(sink.batches.size() == 1U);
    CHECK(sink.batches[0U]->revisions.size() == 3U);
    CHECK(std::all_of(
        sink.batches[0U]->revisions.begin(),
        sink.batches[0U]->revisions.end(),
        [](const EventRevision& revision) {
            return revision.operation == RevisionOperation::kInsert &&
                   revision.reason == RevisionReason::kLiveProjection;
        }));

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 102U}, &bundle));
    const EventPayload& projected = FindPayload(
        bundle, EventKind::kShenzhenTrade);
    CHECK((projected.event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownBuyOrderReference)) == 0U);
    static_cast<void>(FindPayload(
        bundle, EventKind::kShenzhenOrderRevision, 101));
}

void TestAckBeforeFactAndOutOfOrderAckPreserveCommitFifo() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick first = ShenzhenAdd(101U, 60U);
    const CanonicalTick second = ShenzhenAdd(102U, 61U);
    const RawTickDependency second_dependency{
        second.common.ingress_sequence, second.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&second_dependency, 1U));

    const EventInput first_input{first};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    const EventInput second_input{second};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&second_input, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(sink.batches.empty());
    CHECK(worker->DrainDurableCommits());
    CHECK(sink.batches.empty());

    const RawTickDependency first_dependency{
        first.common.ingress_sequence, first.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&first_dependency, 1U));
    CHECK(worker->DrainDurableCommits());
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[0U]->batch_sequence == 1U);
    CHECK(sink.batches[1U]->batch_sequence == 2U);
    CHECK(sink.batches[0U]->revisions.front().version <
          sink.batches[1U]->revisions.front().version);

    RecordingSink ack_first_sink;
    worker = EventWorker::Create(Config(), &ack_first_sink, &error);
    CHECK(worker != nullptr);
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&first_dependency, 1U));
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(ack_first_sink.batches.size() == 1U);
}

void TestUnresolvedLateCancelConvergesBeforeLaterTrade() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const CanonicalTick trade = ShenzhenTrade(102U, 10U, 101, 0);
    EventInput trade_input{trade};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);
    const EventWorkerStats before = worker->stats();

    const CanonicalTick cancel = ShenzhenCancel(101U, 11U, 101);
    EventInput cancel_input{cancel};
    cancel_input.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&cancel_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), cancel);
    const EventWorkerStats after = worker->stats();
    CHECK(after.repaired_order_uses - before.repaired_order_uses == 1U);
    CHECK(after.repair_convergence_stops -
              before.repair_convergence_stops == 1U);
    CHECK(sink.batches.back()->revisions.size() == 1U);
    CHECK(sink.batches.back()->revisions[0U].key.event_kind ==
          EventKind::kShenzhenCancel);
}

void TestShanghaiEndAddsOnlyLateOrdersFinalizeFragment() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const std::array<CanonicalTick, 3U> ticks{
        ShanghaiStatus(1U, 20U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 21U, 100),
        ShanghaiStatus(4U, 22U, TradingPhase::kEnded)};
    std::array<EventInput, 3U> inputs{
        EventInput{ticks[0U]}, EventInput{ticks[1U]}, EventInput{ticks[2U]}};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    std::array<RawTickDependency, 3U> dependencies{};
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        dependencies[index] = {ticks[index].common.ingress_sequence,
                               ticks[index].common.kind};
    }
    worker->AcknowledgeRawTicks(dependencies);
    CHECK(worker->DrainDurableCommits());
    const EventWorkerStats indexed_stats = worker->stats();
    CHECK(indexed_stats.ordered_batch_fast_path >= 1U);
    // Only the one order for this security/channel is visited by END; a
    // second unrelated order would not increase this counter.
    CHECK(indexed_stats.barrier_index_orders_visited == 1U);

    const CanonicalTick late = ShanghaiAdd(3U, 23U, 200);
    EventInput late_input{late};
    late_input.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), late);
    const auto& revisions = sink.batches.back()->revisions;
    CHECK(revisions.size() == 2U);
    CHECK(std::all_of(revisions.begin(), revisions.end(),
                      [](const EventRevision& revision) {
                          return revision.key.affected_order_id == 200 &&
                                 revision.operation ==
                                     RevisionOperation::kInsert;
                      }));
}

void TestShanghaiEndSourceOnlyCutRetainsBarrierForLateOrder() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    // There are no order histories when END is projected, so this exercises
    // the source-only status fast path.  The later Add is a late recovery
    // fact whose native position is before END and must be finalized by the
    // retained barrier.
    const std::array<CanonicalTick, 2U> statuses{
        ShanghaiStatus(1U, 40U, TradingPhase::kContinuous),
        ShanghaiStatus(3U, 42U, TradingPhase::kEnded)};
    std::array<EventInput, 2U> status_inputs{
        EventInput{statuses[0U]}, EventInput{statuses[1U]}};
    CHECK(worker->ApplyBatch(status_inputs).code ==
          EventApplyCode::kApplied);
    std::array<RawTickDependency, 2U> status_dependencies{
        RawTickDependency{statuses[0U].common.ingress_sequence,
                          statuses[0U].common.kind},
        RawTickDependency{statuses[1U].common.ingress_sequence,
                          statuses[1U].common.kind}};
    worker->AcknowledgeRawTicks(status_dependencies);
    CHECK(worker->DrainDurableCommits());

    const CanonicalTick late_add = ShanghaiAdd(2U, 41U, 300);
    EventInput late_input{late_add};
    late_input.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), late_add);

    OrderSnapshot snapshot{};
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 300},
        &snapshot));
    CHECK((snapshot.quality_flags &
           ShanghaiOrderQualityBit(
               ShanghaiOrderQualityFlag::kEndedWithObservedBalance)) != 0U);
    CHECK(std::any_of(
        sink.batches.back()->revisions.begin(),
        sink.batches.back()->revisions.end(),
        [](const EventRevision& revision) {
            return revision.payload.order_snapshot_valid &&
                   revision.payload.order.key.order_id == 300 &&
                   revision.payload.order_delta_operation ==
                       OrderDeltaOperation::kFinalize;
        }));
}

void TestShanghaiTerminalBeforeEndTombstonesOldFinalize() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const std::array<CanonicalTick, 3U> ticks{
        ShanghaiStatus(1U, 30U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 31U, 100, 10),
        ShanghaiStatus(4U, 32U, TradingPhase::kEnded)};
    std::array<EventInput, 3U> inputs{
        EventInput{ticks[0U]}, EventInput{ticks[1U]}, EventInput{ticks[2U]}};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    std::array<RawTickDependency, 3U> dependencies{};
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        dependencies[index] = {ticks[index].common.ingress_sequence,
                               ticks[index].common.kind};
    }
    worker->AcknowledgeRawTicks(dependencies);
    CHECK(worker->DrainDurableCommits());

    const CanonicalTick cancel = ShanghaiCancel(3U, 33U, 100, 10);
    EventInput late{cancel};
    late.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), cancel);
    const auto& revisions = sink.batches.back()->revisions;
    CHECK(revisions.size() == 3U);
    const auto tombstone = std::find_if(
        revisions.begin(), revisions.end(),
        [](const EventRevision& revision) {
            return revision.operation == RevisionOperation::kTombstone;
        });
    CHECK(tombstone != revisions.end());
    CHECK(tombstone->key.native_sequence == 4U);
    CHECK(tombstone->key.affected_order_id == 100);
    CHECK(tombstone->is_deleted);
}

void TestLateShanghaiStatusRepairsOnlyUntilNextStatus() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const std::array<CanonicalTick, 4U> ticks{
        ShanghaiStatus(1U, 70U, TradingPhase::kContinuous),
        ShanghaiAdd(3U, 71U, 100),
        ShanghaiStatus(5U, 72U, TradingPhase::kClosingCall),
        ShanghaiAdd(6U, 73U, 200)};
    const std::array<EventInput, 4U> inputs{
        EventInput{ticks[0U]}, EventInput{ticks[1U]},
        EventInput{ticks[2U]}, EventInput{ticks[3U]}};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    std::array<RawTickDependency, 4U> dependencies{};
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        dependencies[index] = {ticks[index].common.ingress_sequence,
                               ticks[index].common.kind};
    }
    worker->AcknowledgeRawTicks(dependencies);
    CHECK(worker->DrainDurableCommits());

    const EventWorkerStats before = worker->stats();
    const CanonicalTick status =
        ShanghaiStatus(2U, 74U, TradingPhase::kOpeningCall);
    EventInput late{status};
    late.late_recovery = true;
    late.committed_next_sequence = 7U;
    late.observed_gap_epoch = 2U;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), status);

    const EventWorkerStats after = worker->stats();
    CHECK(after.repaired_order_uses - before.repaired_order_uses == 1U);
    const auto& revisions = sink.batches.back()->revisions;
    CHECK(revisions.size() == 2U);
    CHECK(std::none_of(
        revisions.begin(), revisions.end(), [](const EventRevision& revision) {
            return revision.key.native_sequence >= 5U;
        }));

    OrderSnapshot first_order{};
    OrderSnapshot second_order{};
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 100},
        &first_order));
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 200},
        &second_order));
    CHECK(first_order.phase_at_add == TradingPhase::kOpeningCall);
    CHECK(first_order.sh_original_quantity_status ==
          ShanghaiOriginalQuantityStatus::kExact);
    CHECK(second_order.phase_at_add == TradingPhase::kClosingCall);

    EventChannelState channel{};
    CHECK(worker->CopyChannelState(Market::kShanghai, 7U, &channel));
    CHECK(channel.journal_tail == 6U);
    CHECK(channel.projected_frontier == 6U);
    CHECK(channel.committed_next_sequence == 7U);
    CHECK(channel.gap_epoch == 2U);
}

void TestSlicedRepairIsPrivateAndMatchesUnslicedProjection() {
    RecordingSink sliced_sink;
    EventWorkerConfig sliced_config = Config();
    sliced_config.repair_slice_max_order_uses = 1U;
    sliced_config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> sliced = EventWorker::Create(
        sliced_config, &sliced_sink, &error);
    CHECK(sliced != nullptr);

    const std::array<CanonicalTick, 2U> trades{
        ShenzhenTrade(102U, 100U, 101, 0),
        ShenzhenTrade(103U, 101U, 101, 0)};
    const std::array<EventInput, 2U> trade_inputs{
        EventInput{trades[0U]}, EventInput{trades[1U]}};
    CHECK(sliced->ApplyBatch(trade_inputs).code == EventApplyCode::kApplied);
    const std::array<RawTickDependency, 2U> trade_dependencies{
        RawTickDependency{trades[0U].common.ingress_sequence,
                          trades[0U].common.kind},
        RawTickDependency{trades[1U].common.ingress_sequence,
                          trades[1U].common.kind}};
    sliced->AcknowledgeRawTicks(trade_dependencies);
    CHECK(sliced->DrainDurableCommits());
    CHECK(sliced_sink.batches.size() == 1U);

    const CanonicalTick add = ShenzhenAdd(101U, 102U);
    EventInput late_add{add};
    late_add.late_recovery = true;
    EventApplyResult applied = sliced->ApplyBatch(
        std::span<const EventInput>(&late_add, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(applied.repair_pending);
    CHECK(sliced->repair_pending());
    CHECK(sliced_sink.batches.size() == 1U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(!sliced->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
    OrderSnapshot order{};
    CHECK(!sliced->CopyOrder(
        OrderKey{20260807U, Market::kShenzhen, 1U, 7U, 101}, &order));
    CHECK(sliced->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 102U}, &bundle));
    CHECK((FindPayload(bundle, EventKind::kShenzhenTrade)
               .event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownBuyOrderReference)) != 0U);

    const RawTickDependency add_dependency{
        add.common.ingress_sequence, add.common.kind};
    sliced->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&add_dependency, 1U));
    CHECK(sliced->DrainDurableCommits());
    CHECK(sliced_sink.batches.size() == 1U);
    DrainRepair(sliced.get());
    CHECK(sliced_sink.batches.size() == 2U);

    RecordingSink unsliced_sink;
    std::unique_ptr<EventWorker> unsliced = EventWorker::Create(
        Config(), &unsliced_sink, &error);
    CHECK(unsliced != nullptr);
    CHECK(unsliced->ApplyBatch(trade_inputs).code ==
          EventApplyCode::kApplied);
    unsliced->AcknowledgeRawTicks(trade_dependencies);
    CHECK(unsliced->DrainDurableCommits());
    CHECK(unsliced->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U)).code ==
          EventApplyCode::kApplied);
    unsliced->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&add_dependency, 1U));
    DrainRepair(unsliced.get());
    CHECK(unsliced_sink.batches.size() == 2U);
    CHECK(sliced_sink.batches[1U]->revisions ==
          unsliced_sink.batches[1U]->revisions);

    const EventWorkerStats stats = sliced->stats();
    CHECK(stats.repair_slices >= 3U);
    CHECK(stats.repair_commits == 1U);
    CHECK(stats.active_repair_orders == 0U);
}

void TestUnrelatedLiveOrderPublishesDuringRepair() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.repair_slice_max_order_uses = 1U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick trade = ShenzhenTrade(102U, 110U, 101, 0);
    const EventInput trade_input{trade};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);
    CHECK(sink.batches.size() == 1U);

    const CanonicalTick late_add = ShenzhenAdd(101U, 111U);
    EventInput late_input{late_add};
    late_input.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U))
              .repair_pending);
    const RawTickDependency late_dependency{
        late_add.common.ingress_sequence, late_add.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&late_dependency, 1U));

    const CanonicalTick unrelated = ShenzhenAdd(103U, 112U);
    const EventInput unrelated_input{unrelated};
    const EventApplyResult unrelated_result = worker->ApplyBatch(
        std::span<const EventInput>(&unrelated_input, 1U));
    CHECK(unrelated_result.code == EventApplyCode::kApplied);
    CHECK(unrelated_result.repair_pending);
    const RawTickDependency unrelated_dependency{
        unrelated.common.ingress_sequence, unrelated.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&unrelated_dependency, 1U));
    CHECK(worker->DrainDurableCommits());
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[1U]->reason == RevisionReason::kLiveProjection);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));
    static_cast<void>(FindPayload(
        bundle, EventKind::kShenzhenOrderRevision, 103));
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));

    DrainRepair(worker.get());
    CHECK(sink.batches.size() == 3U);
    CHECK(sink.batches[2U]->reason == RevisionReason::kLateRecovery);
}

void TestNewEarlierFactRestartsOnlyDirtyOrder() {
    RecordingSink sliced_sink;
    EventWorkerConfig config = Config();
    config.repair_slice_max_order_uses = 1U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> sliced = EventWorker::Create(
        config, &sliced_sink, &error);
    CHECK(sliced != nullptr);

    const CanonicalTick trade = ShenzhenTrade(103U, 120U, 101, 0);
    const EventInput trade_input{trade};
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(sliced.get(), trade);

    const CanonicalTick add = ShenzhenAdd(101U, 121U);
    EventInput late_add{add};
    late_add.late_recovery = true;
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U))
              .repair_pending);

    const CanonicalTick cancel = ShenzhenCancel(100U, 122U, 101);
    EventInput earlier_cancel{cancel};
    earlier_cancel.late_recovery = true;
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&earlier_cancel, 1U))
              .repair_pending);
    CHECK(sliced->stats().repair_order_restarts == 1U);

    const std::array<RawTickDependency, 2U> late_dependencies{
        RawTickDependency{add.common.ingress_sequence, add.common.kind},
        RawTickDependency{cancel.common.ingress_sequence,
                          cancel.common.kind}};
    sliced->AcknowledgeRawTicks(late_dependencies);
    DrainRepair(sliced.get());
    CHECK(sliced_sink.batches.size() == 2U);

    RecordingSink reference_sink;
    std::unique_ptr<EventWorker> reference = EventWorker::Create(
        Config(), &reference_sink, &error);
    CHECK(reference != nullptr);
    CHECK(reference->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(reference.get(), trade);
    const std::array<EventInput, 2U> reference_late{
        earlier_cancel, late_add};
    CHECK(reference->ApplyBatch(reference_late).code ==
          EventApplyCode::kApplied);
    reference->AcknowledgeRawTicks(late_dependencies);
    DrainRepair(reference.get());
    CHECK(reference_sink.batches.size() == 2U);
    CHECK(sliced_sink.batches[1U]->revisions ==
          reference_sink.batches[1U]->revisions);
}

void TestNewUseExtendsActiveRepairWorklist() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.repair_slice_max_order_uses = 1U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick trade = ShenzhenTrade(102U, 130U, 101, 0);
    const EventInput trade_input{trade};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);

    const CanonicalTick add = ShenzhenAdd(101U, 131U);
    EventInput late_add{add};
    late_add.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U))
              .repair_pending);

    const CanonicalTick later_trade =
        ShenzhenTrade(103U, 132U, 101, 0);
    const EventInput later_input{later_trade};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&later_input, 1U))
              .repair_pending);
    CHECK(worker->stats().repair_order_restarts == 1U);
    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));

    const std::array<RawTickDependency, 2U> dependencies{
        RawTickDependency{add.common.ingress_sequence, add.common.kind},
        RawTickDependency{later_trade.common.ingress_sequence,
                          later_trade.common.kind}};
    worker->AcknowledgeRawTicks(dependencies);
    DrainRepair(worker.get());
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));
    const EventPayload& projected = FindPayload(
        bundle, EventKind::kShenzhenTrade);
    CHECK((projected.event_quality_flags &
           ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kUnknownBuyOrderReference)) == 0U);
}

void TestMixedBatchPublishesOnlyDisjointComponent() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.repair_slice_max_order_uses = 1U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick old_trade = ShenzhenTrade(102U, 150U, 101, 0);
    const EventInput old_input{old_trade};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&old_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), old_trade);

    const CanonicalTick late_add = ShenzhenAdd(101U, 151U);
    EventInput late_input{late_add};
    late_input.late_recovery = true;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U))
              .repair_pending);
    const RawTickDependency late_dependency{
        late_add.common.ingress_sequence, late_add.common.kind};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&late_dependency, 1U));

    const CanonicalTick dirty_trade =
        ShenzhenTrade(103U, 152U, 101, 0);
    const CanonicalTick disjoint_add = ShenzhenAdd(104U, 153U);
    const std::array<EventInput, 2U> mixed{
        EventInput{dirty_trade}, EventInput{disjoint_add}};
    const EventApplyResult applied = worker->ApplyBatch(mixed);
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(applied.repair_pending);

    const std::array<RawTickDependency, 2U> mixed_dependencies{
        RawTickDependency{dirty_trade.common.ingress_sequence,
                          dirty_trade.common.kind},
        RawTickDependency{disjoint_add.common.ingress_sequence,
                          disjoint_add.common.kind}};
    worker->AcknowledgeRawTicks(mixed_dependencies);
    CHECK(worker->DrainDurableCommits());
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[1U]->reason == RevisionReason::kLiveProjection);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 104U}, &bundle));
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));

    DrainRepair(worker.get());
    CHECK(sink.batches.size() == 3U);
    CHECK(sink.batches[2U]->reason == RevisionReason::kLateRecovery);
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));
}

void TestRuntimeRoutesOwnersLateRecoveryAndRawAcks() {
    RecordingSink sink;
    EventRuntimeConfig config{};
    config.worker = Config();
    config.worker.owner_count = 2U;
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1U;
    config.maximum_raw_ack_backlog_per_owner = 8U;
    config.maximum_late_backlog_per_owner = 8U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    CHECK(runtime->owner_for_instrument(0U) == 0U);
    CHECK(runtime->owner_for_instrument(1U) == 1U);
    CHECK(runtime->owner_for_instrument(kInvalidInstrumentOrdinal) == 2U);

    CanonicalTick owner0 = ShenzhenAdd(101U, 80U);
    CanonicalTick owner1 = ShenzhenAdd(201U, 81U);
    owner1.common.instrument_id = 2U;
    owner1.common.instrument_ordinal = 1U;
    owner1.primary_order_id = 201;
    CHECK(runtime->AppendTick(0U, owner0));
    CHECK(runtime->AppendTick(1U, owner1));
    CHECK(runtime->FlushAll());
    const std::array<CanonicalTick, 2U> raw{owner1, owner0};
    CHECK(runtime->OnRawTickBatchAcknowledged(raw));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 2U);

    CanonicalTick late_tick = ShenzhenTrade(202U, 82U, 201, 0);
    late_tick.common.instrument_id = 2U;
    late_tick.common.instrument_ordinal = 1U;
    LateRecoveryTick late{};
    late.tick = late_tick;
    late.committed_next_sequence = 203U;
    late.observed_gap_epoch = 1U;
    late.catalog_match = true;
    CHECK(runtime->AppendLateRecovery(late));
    CHECK(runtime->FlushDue(1U, late_tick.common.receive_monotonic_ns + 2U));
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&late_tick, 1U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 3U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(runtime->worker(1U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 202U}, &bundle));
    CHECK(!runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 202U}, &bundle));
    const EventRuntimeStats stats = runtime->stats();
    CHECK(stats.normal_ticks_received == 2U);
    CHECK(stats.late_ticks_received == 1U);
    CHECK(stats.raw_tick_acks_received == 3U);
    CHECK(runtime->healthy());
}

void TestRuntimeDrainAllFlushesFinalPartialBatch() {
    RecordingSink sink;
    EventRuntimeConfig config{};
    config.worker = Config();
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1'000'000U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick tick = ShenzhenAdd(101U, 170U);
    CHECK(runtime->AppendTick(0U, tick));
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&tick, 1U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
}

void TestRuntimeFailsClosedWhenRevisionSinkRejects() {
    RecordingSink sink;
    sink.accept = false;
    EventRuntimeConfig config{};
    config.worker = Config();
    config.micro_batch_rows = 1U;
    config.micro_batch_max_delay_ns = 1U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    const CanonicalTick tick = ShenzhenAdd(101U, 90U);
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&tick, 1U)));
    CHECK(!runtime->AppendTick(0U, tick));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("revision sink rejected") !=
          std::string::npos);
}

void TestRuntimeInboxCapacityBoundariesFailClosed() {
    RecordingSink sink;
    EventRuntimeConfig config{};
    config.worker = Config();
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1U;
    config.maximum_raw_ack_backlog_per_owner = 1U;
    config.maximum_late_backlog_per_owner = 1U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick first = ShenzhenAdd(101U, 140U);
    const CanonicalTick second = ShenzhenAdd(102U, 141U);
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&first, 1U)));
    CHECK(!runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&second, 1U)));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("raw ACK inbox capacity") !=
          std::string::npos);

    runtime = EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    LateRecoveryTick first_late{};
    first_late.tick = first;
    first_late.committed_next_sequence = 103U;
    first_late.catalog_match = true;
    LateRecoveryTick second_late = first_late;
    second_late.tick = second;
    CHECK(runtime->AppendLateRecovery(first_late));
    CHECK(!runtime->AppendLateRecovery(second_late));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("LateRecovery inbox capacity") !=
          std::string::npos);
}

void TestPendingCanonicalConflictKeepsRetainedFactAuthoritative() {
    RecordingSink sink;
    EventRuntimeConfig config{};
    config.worker = Config();
    config.micro_batch_rows = 1U;
    config.micro_batch_max_delay_ns = 1U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick conflicting = ShenzhenAdd(101U, 180U, 999);
    LateRecoveryTick late{};
    late.tick = conflicting;
    late.committed_next_sequence = 101U;
    late.reason = LateRecoveryReason::kPendingCanonicalConflict;
    late.catalog_match = true;
    CHECK(runtime->AppendLateRecovery(late));
    CHECK(runtime->FlushDue(
        0U, conflicting.common.receive_monotonic_ns + 2U));
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&conflicting, 1U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.empty());

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(!runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
    EventRuntimeStats stats = runtime->stats();
    CHECK(stats.late_ticks_received == 1U);
    CHECK(stats.source_conflicts == 1U);
    CHECK(stats.workers.facts_journaled == 0U);
    CHECK(stats.workers.acknowledged_raw_dependencies == 0U);

    const CanonicalTick retained = ShenzhenAdd(101U, 181U, 100);
    CHECK(runtime->AppendTick(0U, retained));
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&retained, 1U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);
    CHECK(runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
    const EventPayload& projected = FindPayload(
        bundle, EventKind::kShenzhenOrderRevision, 101);
    CHECK(projected.order.original_quantity == 100);
}

void TestWorkerBoundsAcknowledgedRawIndex() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.maximum_acknowledged_raw_dependencies = 1U;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const RawTickDependency first{
        1'000U, CanonicalKind::kShenzhenOrder};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&first, 1U));
    CHECK(worker->healthy());
    CHECK(worker->stats().acknowledged_raw_dependencies == 1U);

    const RawTickDependency second{
        1'001U, CanonicalKind::kShenzhenOrder};
    worker->AcknowledgeRawTicks(
        std::span<const RawTickDependency>(&second, 1U));
    CHECK(!worker->healthy());
    CHECK(worker->fatal_error().find("raw ACK index capacity") !=
          std::string::npos);
}

void TestDuplicateAndConflictClassification() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const CanonicalTick add = ShenzhenAdd(1U, 40U);
    EventInput input{add};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), add);

    CanonicalTick duplicate = add;
    duplicate.common.ingress_sequence = 41U;
    EventInput duplicate_input{duplicate};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&duplicate_input, 1U)).code ==
          EventApplyCode::kDuplicateOnly);
    Ack(worker.get(), duplicate);
    CHECK(sink.batches.size() == 1U);

    CanonicalTick conflict = duplicate;
    conflict.common.ingress_sequence = 42U;
    conflict.quantity.raw = 101;
    EventInput conflict_input{conflict};
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&conflict_input, 1U)).code ==
          EventApplyCode::kSourceConflict);
    Ack(worker.get(), conflict);
    CHECK(worker->healthy());
    CHECK(sink.batches.size() == 1U);
}

}  // namespace

int main() {
    TestLateAddRepairsOnlyReferencedChainAndWaitsForRawAck();
    TestJournalFirstSameBatchDoesNotPublishUnknownIntermediate();
    TestAckBeforeFactAndOutOfOrderAckPreserveCommitFifo();
    TestUnresolvedLateCancelConvergesBeforeLaterTrade();
    TestShanghaiEndAddsOnlyLateOrdersFinalizeFragment();
    TestShanghaiEndSourceOnlyCutRetainsBarrierForLateOrder();
    TestShanghaiTerminalBeforeEndTombstonesOldFinalize();
    TestLateShanghaiStatusRepairsOnlyUntilNextStatus();
    TestSlicedRepairIsPrivateAndMatchesUnslicedProjection();
    TestUnrelatedLiveOrderPublishesDuringRepair();
    TestNewEarlierFactRestartsOnlyDirtyOrder();
    TestNewUseExtendsActiveRepairWorklist();
    TestMixedBatchPublishesOnlyDisjointComponent();
    TestRuntimeRoutesOwnersLateRecoveryAndRawAcks();
    TestRuntimeDrainAllFlushesFinalPartialBatch();
    TestRuntimeFailsClosedWhenRevisionSinkRejects();
    TestRuntimeInboxCapacityBoundariesFailClosed();
    TestPendingCanonicalConflictKeepsRetainedFactAuthoritative();
    TestWorkerBoundsAcknowledgedRawIndex();
    TestDuplicateAndConflictClassification();
    std::cout << "all Event worker tests passed\n";
    return 0;
}
