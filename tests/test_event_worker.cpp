#include "l2flow/event/runtime.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace {

using namespace l2flow::event;
using namespace l2flow::ingest;

constexpr std::uint64_t kFeedSessionEpoch = 37U;
std::uint64_t next_dispatch_fence = 1U;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

std::filesystem::path TestJournalPath(const std::string& label) {
    static std::uint64_t next_id = 0U;
    return std::filesystem::temp_directory_path() /
        ("l2flow-event-test-" + label + "-" +
         std::to_string(::getpid()) + "-" +
         std::to_string(next_id++) + ".journal");
}

std::shared_ptr<l2flow::journal::CanonicalFactJournal> MakeTestJournal(
    const std::filesystem::path& path,
    std::size_t hot_cache_entries = 64U) {
    l2flow::journal::FactJournalConfig config{};
    config.trade_date = 20260807U;
    config.path = path;
    config.hot_cache_entries = hot_cache_entries;
    std::string error;
    std::unique_ptr<l2flow::journal::CanonicalFactJournal> created =
        l2flow::journal::CanonicalFactJournal::Create(
            std::move(config), &error);
    CHECK(created != nullptr);
    auto* const raw = created.release();
    return std::shared_ptr<l2flow::journal::CanonicalFactJournal>(
        raw, [path](l2flow::journal::CanonicalFactJournal* journal) noexcept {
            delete journal;
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove(path, ignored));
        });
}

class RecordingSink final : public EventRevisionSink {
public:
    [[nodiscard]] bool AppendRevisionGroup(
        std::vector<std::shared_ptr<const EventRevisionBatch>> group)
        noexcept override {
        try {
            group_sizes.push_back(group.size());
            batches.insert(batches.end(),
                           std::make_move_iterator(group.begin()),
                           std::make_move_iterator(group.end()));
            return accept;
        } catch (...) {
            return false;
        }
    }

    bool accept = true;
    std::vector<std::size_t> group_sizes;
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

CanonicalTick ShanghaiTrade(std::uint64_t sequence,
                            std::uint64_t ingress,
                            std::int64_t buy_order,
                            std::int64_t sell_order,
                            Aggressor aggressor,
                            std::int64_t price_p6,
                            std::int64_t quantity) {
    CanonicalTick tick = BaseTick(
        Market::kShanghai, CanonicalKind::kShanghaiTick,
        sequence, ingress);
    tick.action = TickAction::kTrade;
    tick.buy_order_id = buy_order;
    tick.sell_order_id = sell_order;
    tick.aggressor = aggressor;
    tick.price = {price_p6 / 1'000, price_p6, 3U, true, true};
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickBuyOrderIdValid | kTickSellOrderIdValid |
                    kTickAggressorValid | kTickPriceValid |
                    kTickQuantityValid | kTickChannelHistoryValid;
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
    config.feed_session_epoch = kFeedSessionEpoch;
    config.calculation_run_id.bytes[0U] = std::byte{1U};
    config.fact_journal = MakeTestJournal(TestJournalPath("default"));
    config.maximum_carry_orders = 1'024U;
    config.maximum_cached_events = 4'096U;
    config.maximum_pending_commits = 64U;
    // Keep the common fixture deterministic under sanitizer overhead. Tests
    // for sliced work set explicit node/byte limits below.
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.phase_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    return config;
}

EventInput Ordered(const CanonicalTick& tick,
                   std::uint64_t generation = 0U,
                   std::uint64_t retention_floor = 1U) {
    EventInput input{};
    input.tick = tick;
    input.admission.feed_session_epoch = kFeedSessionEpoch;
    input.admission.expected_sequence = tick.common.native_sequence;
    input.admission.admission_floor = 1U;
    input.admission.retention_floor = retention_floor;
    input.admission.generation = generation;
    input.admission.dispatch_fence = next_dispatch_fence++;
    input.admission.sequence_class = EventSequenceClass::kOrdered;
    return input;
}

EventInput HoleFill(const CanonicalTick& tick,
                    std::uint64_t expected_sequence,
                    std::uint64_t generation = 1U,
                    std::uint64_t admission_floor = 1U,
                    std::uint64_t retention_floor = 1U) {
    static_cast<void>(next_dispatch_fence++);  // Reserved for GapOpen.
    EventInput input{};
    input.tick = tick;
    input.admission.feed_session_epoch = kFeedSessionEpoch;
    input.admission.expected_sequence = expected_sequence;
    input.admission.admission_floor = admission_floor;
    input.admission.retention_floor = retention_floor;
    input.admission.generation = generation;
    input.admission.dispatch_fence = next_dispatch_fence++;
    input.admission.sequence_class = EventSequenceClass::kHoleFill;
    return input;
}

void OpenGap(EventWorker* worker,
             const EventInput& fill,
             std::uint64_t first_missing,
             std::uint64_t last_missing) {
    const GapOpen gap{
        fill.tick.common.identity.market,
        fill.tick.common.channel,
        kFeedSessionEpoch,
        first_missing,
        last_missing,
        fill.admission.generation,
        fill.admission.dispatch_fence - 1U};
    CHECK(worker->ApplyGapOpen(gap));
}

void Seal(EventWorker* worker,
          Market market,
          std::uint32_t channel,
          std::uint64_t evict_before,
          std::uint64_t generation = 0U) {
    const ChannelSeal seal{
        market,
        channel,
        kFeedSessionEpoch,
        evict_before,
        generation,
        next_dispatch_fence++};
    CHECK(worker->ApplyChannelSeal(seal));
}

void DrainEviction(EventWorker* worker) {
    std::size_t slices = 0U;
    while (worker->eviction_pending()) {
        CHECK(worker->ContinueEviction());
        CHECK(++slices < 4'096U);
    }
}

EventRuntimeConfig RuntimeConfig() {
    EventRuntimeConfig config{};
    config.worker = Config();
    config.feed_session_epoch = kFeedSessionEpoch;
    return config;
}

TickDispatch RuntimeOrdered(const CanonicalTick& tick,
                            std::size_t owner,
                            std::uint64_t generation = 0U) {
    TickDispatch dispatch{};
    dispatch.tick = tick;
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.expected_sequence = tick.common.native_sequence;
    dispatch.admission_floor = 1U;
    dispatch.evict_before = 1U;
    dispatch.generation = generation;
    dispatch.dispatch_fence = next_dispatch_fence++;
    dispatch.channel = tick.common.channel;
    dispatch.owner = static_cast<std::uint32_t>(owner);
    dispatch.market = tick.common.identity.market;
    dispatch.kind = TickDispatchKind::kProjectOrdered;
    dispatch.catalog_match = true;
    return dispatch;
}

TickDispatch RuntimeHoleFill(const CanonicalTick& tick,
                             std::size_t owner,
                             std::uint64_t expected_sequence,
                             std::uint64_t generation) {
    TickDispatch dispatch = RuntimeOrdered(tick, owner, generation);
    dispatch.expected_sequence = expected_sequence;
    dispatch.kind = TickDispatchKind::kProjectHoleFill;
    return dispatch;
}

TickDispatch RuntimeReject(const CanonicalTick& tick, std::size_t owner) {
    TickDispatch dispatch{};
    dispatch.tick = tick;
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.dispatch_fence = next_dispatch_fence++;
    dispatch.channel = tick.common.channel;
    dispatch.owner = static_cast<std::uint32_t>(owner);
    dispatch.market = tick.common.identity.market;
    dispatch.kind = TickDispatchKind::kRejectLateFact;
    dispatch.catalog_match = true;
    return dispatch;
}

TickDispatch RuntimeGapOpen(const CanonicalTick& tick,
                            std::size_t owner,
                            std::uint64_t first_missing,
                            std::uint64_t last_missing,
                            std::uint64_t generation) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.generation = generation;
    dispatch.dispatch_fence = next_dispatch_fence++;
    dispatch.first_missing = first_missing;
    dispatch.last_missing = last_missing;
    dispatch.channel = tick.common.channel;
    dispatch.owner = static_cast<std::uint32_t>(owner);
    dispatch.market = tick.common.identity.market;
    dispatch.kind = TickDispatchKind::kGapOpen;
    return dispatch;
}

TickDispatch RuntimeChannelSeal(const CanonicalTick& tick,
                                std::size_t owner,
                                std::uint64_t evict_before,
                                std::uint64_t generation) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.generation = generation;
    dispatch.dispatch_fence = next_dispatch_fence++;
    dispatch.evict_before = evict_before;
    dispatch.channel = tick.common.channel;
    dispatch.owner = static_cast<std::uint32_t>(owner);
    dispatch.market = tick.common.identity.market;
    dispatch.kind = TickDispatchKind::kChannelSeal;
    return dispatch;
}

void Ack(EventWorker* worker, const CanonicalTick& tick) {
    static_cast<void>(tick);
    CHECK(worker->FlushDurableCommits());
}

void AckAll(EventWorker* worker,
            std::span<const CanonicalTick> ticks) {
    static_cast<void>(ticks);
    CHECK(worker->FlushDurableCommits());
}

void DrainRepair(EventWorker* worker) {
    std::size_t slices = 0U;
    while (worker->repair_pending()) {
        CHECK(worker->AdvanceRepair());
        CHECK(++slices < 1'024U);
    }
    CHECK(worker->FlushDurableCommits());
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
    const std::filesystem::path path = TestJournalPath("cold-repair");
    const auto fact_journal = MakeTestJournal(path);
    EventWorkerConfig config = Config();
    config.fact_journal = fact_journal;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick trade = ShenzhenTrade(102U, 1U, 101, 0);
    const EventInput trade_input = Ordered(trade);
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

    fact_journal->ClearHotCache();
    const std::uint64_t reads_before_repair =
        fact_journal->stats().read_calls;
    const CanonicalTick add = ShenzhenAdd(101U, 2U);
    EventInput add_input = HoleFill(add, 103U);
    OpenGap(worker.get(), add_input, 101U, 101U);
    applied = worker->ApplyBatch(
        std::span<const EventInput>(&add_input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(fact_journal->stats().read_calls > reads_before_repair);
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
                                 RevisionReason::kHoleFill;
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
        Ordered(trade), Ordered(add)};
    const EventApplyResult applied = worker->ApplyBatch(inputs);
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(sink.batches.empty());

    CHECK(worker->FlushDurableCommits());
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

    const EventInput first_input = Ordered(first);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    const EventInput second_input = Ordered(second);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&second_input, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(sink.batches.empty());
    CHECK(worker->FlushDurableCommits());
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[0U]->batch_sequence == 1U);
    CHECK(sink.batches[1U]->batch_sequence == 2U);
    CHECK(sink.batches[0U]->revisions.front().version <
          sink.batches[1U]->revisions.front().version);
}

void TestUnresolvedLateCancelConvergesBeforeLaterTrade() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);
    const CanonicalTick trade = ShenzhenTrade(102U, 10U, 101, 0);
    EventInput trade_input = Ordered(trade);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);
    const EventWorkerStats before = worker->stats();

    const CanonicalTick cancel = ShenzhenCancel(101U, 11U, 101);
    EventInput cancel_input = HoleFill(cancel, 103U);
    OpenGap(worker.get(), cancel_input, 101U, 101U);
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
        Ordered(ticks[0U]), Ordered(ticks[1U]), Ordered(ticks[2U])};
    const EventApplyResult applied = worker->ApplyBatch(inputs);
    CHECK(applied.code == EventApplyCode::kApplied);
    DrainRepair(worker.get());
    CHECK(worker->FlushDurableCommits());
    const EventWorkerStats indexed_stats = worker->stats();
    CHECK(indexed_stats.ordered_batch_fast_path >= 1U);
    // Only the one order for this security/channel is visited by END; a
    // second unrelated order would not increase this counter.
    CHECK(indexed_stats.barrier_index_orders_visited == 1U);

    const CanonicalTick late = ShanghaiAdd(3U, 23U, 200);
    EventInput late_input = HoleFill(late, 5U);
    OpenGap(worker.get(), late_input, 3U, 3U);
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

void TestShanghaiBarrierBulkRolesStayOrdered() {
    constexpr std::size_t kOrderCount = 128U;
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    std::vector<EventInput> inputs;
    inputs.reserve(kOrderCount + 2U);
    inputs.push_back(Ordered(
        ShanghaiStatus(1U, 500U, TradingPhase::kContinuous)));
    for (std::size_t index = 0U; index < kOrderCount; ++index) {
        const std::uint64_t sequence = 2U + index;
        const std::int64_t order = static_cast<std::int64_t>(
            kOrderCount - index);
        inputs.push_back(Ordered(
            ShanghaiAdd(sequence, 501U + index, order)));
    }
    const std::uint64_t end_sequence = kOrderCount + 2U;
    inputs.push_back(Ordered(ShanghaiStatus(
        end_sequence, 501U + kOrderCount, TradingPhase::kEnded)));

    const EventApplyResult applied = worker->ApplyBatch(inputs);
    CHECK(applied.code == EventApplyCode::kApplied);
    DrainRepair(worker.get());
    CHECK(worker->stats().barrier_index_orders_visited == kOrderCount);
    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShanghai, 7U, end_sequence}, &bundle));
    std::vector<std::int64_t> finalized_orders;
    for (const auto& [key, payload] : bundle) {
        static_cast<void>(payload);
        if (key.event_kind == EventKind::kShanghaiOrderRevision) {
            finalized_orders.push_back(key.affected_order_id);
        }
    }
    CHECK(finalized_orders.size() == kOrderCount);
    CHECK(std::is_sorted(finalized_orders.begin(), finalized_orders.end()));
}

void TestShanghaiEndSourceOnlyCutRetainsBarrierForLateOrder() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    // There are no order histories when END is projected, so this exercises
    // the source-only status fast path. The later Add is a hole fill
    // fact whose native position is before END and must be finalized by the
    // retained barrier.
    const std::array<CanonicalTick, 2U> statuses{
        ShanghaiStatus(1U, 40U, TradingPhase::kContinuous),
        ShanghaiStatus(3U, 42U, TradingPhase::kEnded)};
    std::array<EventInput, 2U> status_inputs{
        Ordered(statuses[0U]), Ordered(statuses[1U])};
    CHECK(worker->ApplyBatch(status_inputs).code ==
          EventApplyCode::kApplied);
    CHECK(worker->FlushDurableCommits());

    const CanonicalTick late_add = ShanghaiAdd(2U, 41U, 300);
    EventInput late_input = HoleFill(late_add, 4U);
    OpenGap(worker.get(), late_input, 2U, 2U);
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

void TestShanghaiEndDoesNotFinalizeOrderFirstSeenAfterEnd() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.eviction_slice_max_nodes = 1U;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 2U> ticks{
        ShanghaiStatus(1U, 43U, TradingPhase::kEnded),
        ShanghaiAdd(2U, 44U, 301)};
    const std::array<EventInput, 2U> inputs{
        Ordered(ticks[0U], 0U, 3U), Ordered(ticks[1U], 0U, 3U)};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    DrainRepair(worker.get());
    DrainEviction(worker.get());
    CHECK(worker->healthy());
    CHECK(worker->stats().hot_facts == 0U);

    OrderSnapshot snapshot{};
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 301},
        &snapshot));
    CHECK((snapshot.quality_flags &
           ShanghaiOrderQualityBit(
               ShanghaiOrderQualityFlag::kEndedWithObservedBalance)) == 0U);
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
        Ordered(ticks[0U]), Ordered(ticks[1U]), Ordered(ticks[2U])};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    CHECK(worker->FlushDurableCommits());

    const CanonicalTick cancel = ShanghaiCancel(3U, 33U, 100, 10);
    EventInput late = HoleFill(cancel, 5U);
    OpenGap(worker.get(), late, 3U, 3U);
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
        Ordered(ticks[0U]), Ordered(ticks[1U]),
        Ordered(ticks[2U]), Ordered(ticks[3U])};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    CHECK(worker->FlushDurableCommits());

    const EventWorkerStats before = worker->stats();
    const CanonicalTick status =
        ShanghaiStatus(2U, 74U, TradingPhase::kOpeningCall);
    EventInput late = HoleFill(status, 7U, 2U);
    OpenGap(worker.get(), late, 2U, 2U);
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
    CHECK(channel.expected_sequence == 7U);
    CHECK(channel.generation == 2U);
}

void TestSlicedShanghaiPhaseCutFencesAckAndMatchesReference() {
    const std::array<CanonicalTick, 5U> initial{
        ShanghaiStatus(1U, 7'000U, TradingPhase::kContinuous),
        ShanghaiAdd(3U, 7'003U, 100),
        ShanghaiAdd(4U, 7'004U, 101),
        ShanghaiStatus(6U, 7'006U, TradingPhase::kClosingCall),
        ShanghaiAdd(7U, 7'007U, 200)};
    const std::array<EventInput, 5U> initial_inputs{
        Ordered(initial[0U]), Ordered(initial[1U]), Ordered(initial[2U]),
        Ordered(initial[3U]), Ordered(initial[4U])};
    const CanonicalTick late_status =
        ShanghaiStatus(2U, 7'002U, TradingPhase::kOpeningCall);
    EventInput late = HoleFill(late_status, 8U);

    RecordingSink sliced_sink;
    const std::filesystem::path journal_path =
        TestJournalPath("sliced-phase-cursor");
    const auto fact_journal = MakeTestJournal(journal_path, 1U);
    EventWorkerConfig sliced_config = Config();
    sliced_config.fact_journal = fact_journal;
    sliced_config.phase_slice_max_nodes = 1U;
    sliced_config.phase_slice_max_bytes = 1U << 20U;
    sliced_config.phase_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> sliced = EventWorker::Create(
        sliced_config, &sliced_sink, &error);
    CHECK(sliced != nullptr);

    const EventApplyResult initial_result = sliced->ApplyBatch(initial_inputs);
    CHECK(initial_result.code == EventApplyCode::kApplied);
    CHECK(initial_result.repair_pending);
    CHECK(sliced->projection_input_fenced());
    DrainRepair(sliced.get());
    AckAll(sliced.get(), initial);
    CHECK(sliced_sink.batches.size() == 1U);

    OpenGap(sliced.get(), late, 2U, 2U);
    fact_journal->ClearHotCache();
    const std::uint64_t reads_before = fact_journal->stats().read_calls;
    const std::uint64_t repair_slices_before = sliced->stats().repair_slices;
    const EventApplyResult late_result = sliced->ApplyBatch(
        std::span<const EventInput>(&late, 1U));
    CHECK(late_result.code == EventApplyCode::kApplied);
    CHECK(late_result.repair_pending);
    CHECK(sliced->projection_input_fenced());

    CHECK(sliced->FlushDurableCommits());
    CHECK(sliced_sink.batches.size() == 1U);

    OrderSnapshot snapshot{};
    CHECK(sliced->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 100},
        &snapshot));
    CHECK(snapshot.phase_at_add == TradingPhase::kContinuous);

    std::size_t phase_calls = 0U;
    while (sliced->projection_input_fenced()) {
        CHECK(sliced->AdvanceRepair());
        CHECK(sliced->stats().repair_slices == repair_slices_before);
        CHECK(++phase_calls < 64U);
    }
    CHECK(phase_calls > 1U);
    CHECK(fact_journal->stats().read_calls >= reads_before + 2U);
    CHECK(sliced->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 100},
        &snapshot));
    CHECK(snapshot.phase_at_add == TradingPhase::kContinuous);

    DrainRepair(sliced.get());
    CHECK(sliced_sink.batches.size() == 2U);
    CHECK(sliced->stats().pending_phase_bytes == 0U);
    CHECK(sliced->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 100},
        &snapshot));
    CHECK(snapshot.phase_at_add == TradingPhase::kOpeningCall);
    CHECK(sliced->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 200},
        &snapshot));
    CHECK(snapshot.phase_at_add == TradingPhase::kClosingCall);

    RecordingSink reference_sink;
    std::unique_ptr<EventWorker> reference = EventWorker::Create(
        Config(), &reference_sink, &error);
    CHECK(reference != nullptr);
    CHECK(reference->ApplyBatch(initial_inputs).code ==
          EventApplyCode::kApplied);
    DrainRepair(reference.get());
    AckAll(reference.get(), initial);
    OpenGap(reference.get(), late, 2U, 2U);
    CHECK(reference->ApplyBatch(
              std::span<const EventInput>(&late, 1U)).code ==
          EventApplyCode::kApplied);
    DrainRepair(reference.get());
    CHECK(reference_sink.batches.size() == 2U);
    CHECK(sliced_sink.batches[1U]->revisions ==
          reference_sink.batches[1U]->revisions);
}

void TestInvalidShanghaiStatusKeepsSourcePhaseWithoutBecomingAnchor() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.phase_slice_max_nodes = 1U;
    config.phase_slice_max_bytes = 1U << 20U;
    config.phase_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 2U> initial{
        ShanghaiStatus(1U, 7'100U, TradingPhase::kContinuous),
        ShanghaiAdd(3U, 7'103U, 300)};
    const std::array<EventInput, 2U> initial_inputs{
        Ordered(initial[0U]), Ordered(initial[1U])};
    CHECK(worker->ApplyBatch(initial_inputs).code == EventApplyCode::kApplied);
    DrainRepair(worker.get());
    AckAll(worker.get(), initial);

    CanonicalTick invalid_status =
        ShanghaiStatus(2U, 7'102U, TradingPhase::kOpeningCall);
    invalid_status.validity = kTickChannelHistoryValid;
    EventInput late = HoleFill(invalid_status, 4U);
    OpenGap(worker.get(), late, 2U, 2U);
    const std::uint64_t source_only_before =
        worker->stats().source_only_fast_path;
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(worker->projection_input_fenced());
    DrainRepair(worker.get());
    CHECK(worker->stats().source_only_fast_path == source_only_before);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShanghai, 7U, 2U}, &bundle));
    CHECK(FindPayload(bundle, EventKind::kShanghaiStatus).phase ==
          TradingPhase::kOpeningCall);
    OrderSnapshot order{};
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 300}, &order));
    CHECK(order.phase_at_add == TradingPhase::kContinuous);
}

void TestPhaseSliceConfigRejectsZeroBudgets() {
    std::string error;
    EventWorkerConfig config = Config();
    config.phase_slice_max_nodes = 0U;
    CHECK(!ValidateEventWorkerConfig(config, &error));
    config = Config();
    config.phase_slice_max_bytes = 0U;
    CHECK(!ValidateEventWorkerConfig(config, &error));
    config = Config();
    config.phase_slice_max_cpu_ns = 0U;
    CHECK(!ValidateEventWorkerConfig(config, &error));
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
        Ordered(trades[0U]), Ordered(trades[1U])};
    CHECK(sliced->ApplyBatch(trade_inputs).code == EventApplyCode::kApplied);
    CHECK(sliced->FlushDurableCommits());
    CHECK(sliced_sink.batches.size() == 1U);

    const CanonicalTick add = ShenzhenAdd(101U, 102U);
    EventInput late_add = HoleFill(add, 104U);
    OpenGap(sliced.get(), late_add, 101U, 101U);
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

    CHECK(sliced->FlushDurableCommits());
    CHECK(sliced_sink.batches.size() == 1U);
    DrainRepair(sliced.get());
    CHECK(sliced_sink.batches.size() == 2U);

    RecordingSink unsliced_sink;
    std::unique_ptr<EventWorker> unsliced = EventWorker::Create(
        Config(), &unsliced_sink, &error);
    CHECK(unsliced != nullptr);
    CHECK(unsliced->ApplyBatch(trade_inputs).code ==
          EventApplyCode::kApplied);
    CHECK(unsliced->FlushDurableCommits());
    OpenGap(unsliced.get(), late_add, 101U, 101U);
    CHECK(unsliced->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U)).code ==
          EventApplyCode::kApplied);
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
    const EventInput trade_input = Ordered(trade);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);
    CHECK(sink.batches.size() == 1U);

    const CanonicalTick late_add = ShenzhenAdd(101U, 111U);
    EventInput late_input = HoleFill(late_add, 103U);
    OpenGap(worker.get(), late_input, 101U, 101U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U))
              .repair_pending);

    const CanonicalTick unrelated = ShenzhenAdd(103U, 112U);
    const EventInput unrelated_input = Ordered(unrelated, 1U);
    const EventApplyResult unrelated_result = worker->ApplyBatch(
        std::span<const EventInput>(&unrelated_input, 1U));
    CHECK(unrelated_result.code == EventApplyCode::kApplied);
    CHECK(unrelated_result.repair_pending);
    CHECK(worker->FlushDurableCommits());
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
    CHECK(sink.batches[2U]->reason == RevisionReason::kHoleFill);
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
    const EventInput trade_input = Ordered(trade);
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(sliced.get(), trade);

    const CanonicalTick add = ShenzhenAdd(101U, 121U);
    EventInput late_add = HoleFill(add, 104U);
    OpenGap(sliced.get(), late_add, 100U, 102U);
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U))
              .repair_pending);
    CHECK(sliced->stats().repaired_order_uses == 1U);

    const CanonicalTick cancel = ShenzhenCancel(100U, 122U, 101);
    EventInput earlier_cancel = HoleFill(cancel, 104U);
    CHECK(sliced->ApplyBatch(
              std::span<const EventInput>(&earlier_cancel, 1U))
              .repair_pending);
    CHECK(sliced->stats().repair_order_restarts == 1U);

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
    EventInput reference_cancel = HoleFill(cancel, 104U);
    EventInput reference_add = HoleFill(add, 104U);
    OpenGap(reference.get(), reference_cancel, 100U, 102U);
    const std::array<EventInput, 2U> reference_late{
        reference_cancel, reference_add};
    CHECK(reference->ApplyBatch(reference_late).code ==
          EventApplyCode::kApplied);
    DrainRepair(reference.get());
    CHECK(reference_sink.batches.size() == 2U);
    CHECK(sliced_sink.batches[1U]->revisions ==
          reference_sink.batches[1U]->revisions);
    OrderSnapshot sliced_order{};
    OrderSnapshot reference_order{};
    const OrderKey order{
        20260807U, Market::kShenzhen, 1U, 7U, 101};
    CHECK(sliced->CopyOrder(order, &sliced_order));
    CHECK(reference->CopyOrder(order, &reference_order));
    CHECK(sliced_order == reference_order);
}

void TestLargeSuffixRepairDoesNotCopyHistoryAndMatchesReference() {
    constexpr std::size_t kLargeSuffixUses = 256U;
    constexpr std::uint64_t kIngressBase = 20'000U;

    const auto make_history = [=](std::size_t use_count) {
        std::vector<CanonicalTick> ticks;
        ticks.reserve(use_count);
        for (std::size_t index = 0U; index < use_count; ++index) {
            ticks.push_back(ShenzhenTrade(
                2U + index, kIngressBase + index,
                1, 0, 1));
        }
        return ticks;
    };
    const auto project_history = [](
                                     EventWorker* worker,
                                     std::span<const CanonicalTick> ticks) {
        std::vector<EventInput> inputs;
        inputs.reserve(ticks.size());
        for (const CanonicalTick& tick : ticks) {
            inputs.push_back(Ordered(tick));
        }
        CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
        AckAll(worker, ticks);
    };
    const auto start_repair = [](
                                  EventWorker* worker,
                                  const CanonicalTick& add,
                                  std::uint64_t expected_sequence) {
        EventInput fill = HoleFill(add, expected_sequence);
        OpenGap(worker, fill, 1U, 1U);
        const EventApplyResult applied = worker->ApplyBatch(
            std::span<const EventInput>(&fill, 1U));
        CHECK(applied.code == EventApplyCode::kApplied);
        CHECK(applied.repair_pending);
        CHECK(applied.repaired_order_uses == 1U);
        return fill;
    };

    RecordingSink small_sink;
    EventWorkerConfig small_config = Config();
    small_config.repair_slice_max_order_uses = 1U;
    small_config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> small = EventWorker::Create(
        small_config, &small_sink, &error);
    CHECK(small != nullptr);
    const std::vector<CanonicalTick> small_history = make_history(1U);
    project_history(small.get(), small_history);
    const CanonicalTick small_add =
        ShenzhenAdd(1U, kIngressBase + kLargeSuffixUses + 1U, 1'000);
    static_cast<void>(start_repair(small.get(), small_add, 3U));
    const std::uint64_t one_use_repair_bytes =
        small->stats().active_repair_bytes_high_watermark;
    CHECK(one_use_repair_bytes != 0U);

    RecordingSink sliced_sink;
    EventWorkerConfig sliced_config = Config();
    sliced_config.repair_slice_max_order_uses = 1U;
    sliced_config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::unique_ptr<EventWorker> sliced = EventWorker::Create(
        sliced_config, &sliced_sink, &error);
    CHECK(sliced != nullptr);
    const std::vector<CanonicalTick> large_history =
        make_history(kLargeSuffixUses);
    project_history(sliced.get(), large_history);
    const CanonicalTick late_add =
        ShenzhenAdd(1U, kIngressBase + kLargeSuffixUses + 2U, 1'000);
    static_cast<void>(start_repair(
        sliced.get(), late_add, kLargeSuffixUses + 2U));
    CHECK(sliced->stats().active_repair_bytes_high_watermark ==
          one_use_repair_bytes);
    Ack(sliced.get(), late_add);
    DrainRepair(sliced.get());

    RecordingSink reference_sink;
    EventWorkerConfig reference_config = Config();
    reference_config.repair_slice_max_order_uses =
        kLargeSuffixUses + 1U;
    reference_config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::unique_ptr<EventWorker> reference = EventWorker::Create(
        reference_config, &reference_sink, &error);
    CHECK(reference != nullptr);
    project_history(reference.get(), large_history);
    EventInput reference_fill = HoleFill(
        late_add, kLargeSuffixUses + 2U);
    OpenGap(reference.get(), reference_fill, 1U, 1U);
    CHECK(reference->ApplyBatch(
              std::span<const EventInput>(&reference_fill, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(reference.get(), late_add);
    DrainRepair(reference.get());

    CHECK(sliced_sink.batches.size() == 2U);
    CHECK(reference_sink.batches.size() == 2U);
    CHECK(sliced_sink.batches[1U]->revisions ==
          reference_sink.batches[1U]->revisions);
    OrderSnapshot sliced_order{};
    OrderSnapshot reference_order{};
    const OrderKey order{
        20260807U, Market::kShenzhen, 1U, 7U, 1};
    CHECK(sliced->CopyOrder(order, &sliced_order));
    CHECK(reference->CopyOrder(order, &reference_order));
    CHECK(sliced_order == reference_order);
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
    const EventInput trade_input = Ordered(trade);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&trade_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), trade);

    const CanonicalTick add = ShenzhenAdd(101U, 131U);
    EventInput late_add = HoleFill(add, 103U);
    OpenGap(worker.get(), late_add, 101U, 101U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_add, 1U))
              .repair_pending);

    const CanonicalTick later_trade =
        ShenzhenTrade(103U, 132U, 101, 0);
    const EventInput later_input = Ordered(later_trade, 1U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&later_input, 1U))
              .repair_pending);
    CHECK(worker->stats().repair_order_restarts == 1U);
    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));

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
    const EventInput old_input = Ordered(old_trade);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&old_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), old_trade);

    const CanonicalTick late_add = ShenzhenAdd(101U, 151U);
    EventInput late_input = HoleFill(late_add, 103U);
    OpenGap(worker.get(), late_input, 101U, 101U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&late_input, 1U))
              .repair_pending);

    const CanonicalTick dirty_trade =
        ShenzhenTrade(103U, 152U, 101, 0);
    const CanonicalTick disjoint_add = ShenzhenAdd(104U, 153U);
    const std::array<EventInput, 2U> mixed{
        Ordered(dirty_trade, 1U), Ordered(disjoint_add, 1U)};
    const EventApplyResult applied = worker->ApplyBatch(mixed);
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(applied.repair_pending);

    CHECK(worker->FlushDurableCommits());
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[1U]->reason == RevisionReason::kLiveProjection);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 104U}, &bundle));
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));

    DrainRepair(worker.get());
    CHECK(sink.batches.size() == 3U);
    CHECK(sink.batches[2U]->reason == RevisionReason::kHoleFill);
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 103U}, &bundle));
}

void TestShenzhenChannelZeroProjectsAndSettlesAck() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    CanonicalTick tick = ShenzhenAdd(1U, 700U);
    tick.common.channel = 0U;
    EventInput input = Ordered(tick);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), tick);
    CHECK(sink.batches.size() == 1U);
    EventChannelState state{};
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 0U, &state));
}

void TestRetentionFloorAndStrictSealBoundary() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.eviction_slice_max_nodes = 64U;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick first = ShenzhenAdd(1U, 710U);
    EventInput first_input = Ordered(first, 0U, 2U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    DrainEviction(worker.get());
    EventChannelState state{};
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.sealed_before == 2U);

    const CanonicalTick at_cutoff = ShenzhenAdd(2U, 711U);
    EventInput cutoff_input = Ordered(at_cutoff, 0U, 3U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&cutoff_input, 1U)).code ==
          EventApplyCode::kApplied);
    DrainEviction(worker.get());
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.sealed_before == 3U);
    CHECK(worker->stats().hot_facts == 0U);

    RecordingSink rejected_sink;
    std::unique_ptr<EventWorker> rejected =
        EventWorker::Create(Config(), &rejected_sink, &error);
    CHECK(rejected != nullptr);
    const CanonicalTick retained = ShenzhenAdd(2U, 712U);
    EventInput retained_input = Ordered(retained, 0U, 3U);
    CHECK(rejected->ApplyBatch(
              std::span<const EventInput>(&retained_input, 1U)).code ==
          EventApplyCode::kApplied);
    const CanonicalTick expired = ShenzhenAdd(1U, 713U);
    EventInput expired_input = HoleFill(
        expired, 4U, 1U, 1U, 3U);
    OpenGap(rejected.get(), expired_input, 1U, 1U);
    CHECK(rejected->ApplyBatch(
              std::span<const EventInput>(&expired_input, 1U)).code ==
          EventApplyCode::kFailed);
    CHECK(!rejected->healthy());
}

void TestChannelSealRejectsStaleGeneration() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const GapOpen first{Market::kShenzhen, 7U, kFeedSessionEpoch,
                        1U, 1U, 1U, next_dispatch_fence++};
    const GapOpen second{Market::kShenzhen, 7U, kFeedSessionEpoch,
                         2U, 2U, 2U, next_dispatch_fence++};
    CHECK(worker->ApplyGapOpen(first));
    CHECK(worker->ApplyGapOpen(second));
    const ChannelSeal stale{Market::kShenzhen, 7U, kFeedSessionEpoch,
                            3U, 1U, next_dispatch_fence++};
    CHECK(!worker->ApplyChannelSeal(stale));
    CHECK(!worker->healthy());
    CHECK(worker->fatal_error().find("generation/fence is stale") !=
          std::string::npos);
}

void TestClaimedFillMayAdvanceRetentionBeyondSequence() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick fill = ShenzhenAdd(2U, 720U);
    EventInput input = HoleFill(fill, 5U, 1U, 1U, 4U);
    OpenGap(worker.get(), input, 2U, 2U);
    const EventApplyResult applied = worker->ApplyBatch(
        std::span<const EventInput>(&input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    DrainRepair(worker.get());
    DrainEviction(worker.get());
    EventChannelState state{};
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.sealed_before == 4U);
}

void TestMultipleChannelEvictionTargetsSurviveSmallSlices() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.eviction_slice_max_nodes = 1U;
    config.eviction_slice_max_bytes = 1U << 20U;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    CanonicalTick left = ShenzhenAdd(1U, 730U);
    CanonicalTick right = ShenzhenAdd(1U, 731U);
    right.common.channel = 8U;
    const std::array<EventInput, 2U> inputs{
        Ordered(left), Ordered(right)};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    CHECK(worker->FlushDurableCommits());

    Seal(worker.get(), Market::kShenzhen, 7U, 2U);
    Seal(worker.get(), Market::kShenzhen, 8U, 2U);
    DrainEviction(worker.get());
    EventChannelState left_state{};
    EventChannelState right_state{};
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 7U, &left_state));
    CHECK(worker->CopyChannelState(
        Market::kShenzhen, 8U, &right_state));
    CHECK(left_state.sealed_before == 2U);
    CHECK(right_state.sealed_before == 2U);
    CHECK(worker->stats().hot_facts == 0U);
}

void TestOrderBaselineAndPhaseAnchorPreserveHiddenState() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick add = ShenzhenAdd(1U, 740U, 10);
    const CanonicalTick cancel = ShenzhenCancel(2U, 741U, 1, 10);
    const std::array<EventInput, 2U> initial{
        Ordered(add, 0U, 2U), Ordered(cancel, 0U, 3U)};
    CHECK(worker->ApplyBatch(initial).code == EventApplyCode::kApplied);
    DrainEviction(worker.get());
    CHECK(worker->stats().hot_facts == 0U);

    const CanonicalTick after_terminal =
        ShenzhenTrade(3U, 742U, 1, 0, 1);
    EventInput terminal_input = Ordered(after_terminal, 0U, 3U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&terminal_input, 1U)).code ==
          EventApplyCode::kApplied);
    OrderSnapshot order{};
    CHECK(worker->CopyOrder(
        OrderKey{20260807U, Market::kShenzhen, 1U, 7U, 1}, &order));
    CHECK((order.quality_flags & ShenzhenEventQualityBit(
               ShenzhenEventQualityFlag::kQuantityConflict)) != 0U);
    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 3U}, &bundle));
    CHECK(FindPayload(
              bundle, EventKind::kShenzhenOrderRevision, 1)
              .order_delta_operation == OrderDeltaOperation::kUpdate);

    RecordingSink phase_sink;
    std::unique_ptr<EventWorker> phase_worker =
        EventWorker::Create(Config(), &phase_sink, &error);
    CHECK(phase_worker != nullptr);
    const CanonicalTick status =
        ShanghaiStatus(1U, 743U, TradingPhase::kContinuous);
    EventInput status_input = Ordered(status, 0U, 2U);
    CHECK(phase_worker->ApplyBatch(
              std::span<const EventInput>(&status_input, 1U)).code ==
          EventApplyCode::kApplied);
    DrainEviction(phase_worker.get());
    const CanonicalTick sh_add = ShanghaiAdd(2U, 744U, 100);
    EventInput sh_add_input = Ordered(sh_add, 0U, 2U);
    CHECK(phase_worker->ApplyBatch(
              std::span<const EventInput>(&sh_add_input, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(phase_worker->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 100}, &order));
    CHECK(order.phase_at_add == TradingPhase::kContinuous);
}

void TestOrderHistoryByteCapAndCompactionAccounting() {
    std::string error;
    RecordingSink invalid_sink;
    EventWorkerConfig invalid_config = Config();
    invalid_config.maximum_order_history_bytes = 0U;
    CHECK(EventWorker::Create(invalid_config, &invalid_sink, &error) ==
          nullptr);
    CHECK(error == "invalid Event worker configuration");

    RecordingSink probe_sink;
    EventWorkerConfig probe_config = Config();
    probe_config.maximum_carry_orders = 8U;
    std::unique_ptr<EventWorker> probe =
        EventWorker::Create(probe_config, &probe_sink, &error);
    CHECK(probe != nullptr);
    const EventWorkerStats bucket_baseline = probe->stats();
    CHECK(bucket_baseline.order_history_bytes != 0U);
    CHECK(bucket_baseline.order_history_bytes_high_watermark ==
          bucket_baseline.order_history_bytes);

    RecordingSink large_empty_sink;
    EventWorkerConfig large_empty_config = Config();
    large_empty_config.maximum_carry_orders = 1U << 20U;
    std::unique_ptr<EventWorker> large_empty = EventWorker::Create(
        large_empty_config, &large_empty_sink, &error);
    CHECK(large_empty != nullptr);
    const EventWorkerStats large_empty_stats = large_empty->stats();
    CHECK(large_empty_stats.order_history_bytes != 0U);
    CHECK(large_empty_stats.order_history_bytes <
          large_empty_config.maximum_carry_orders);
    CHECK(large_empty_stats.order_history_bytes_high_watermark ==
          large_empty_stats.order_history_bytes);

    EventWorkerConfig short_directory_config = large_empty_config;
    short_directory_config.maximum_order_history_bytes =
        large_empty_stats.order_history_bytes - 1U;
    CHECK(!ValidateEventWorkerConfig(short_directory_config, &error));
    CHECK(error.find("order-history directory") != std::string::npos);

    RecordingSink new_order_cap_sink;
    EventWorkerConfig new_order_cap_config = Config();
    new_order_cap_config.maximum_carry_orders = 8U;
    new_order_cap_config.maximum_order_history_bytes =
        bucket_baseline.order_history_bytes;
    std::unique_ptr<EventWorker> new_order_cap = EventWorker::Create(
        new_order_cap_config, &new_order_cap_sink, &error);
    CHECK(new_order_cap != nullptr);
    CHECK(new_order_cap->stats().order_history_bytes ==
          bucket_baseline.order_history_bytes);

    const CanonicalTick first = ShenzhenAdd(1U, 788U, 10);
    const EventInput rejected_new_order = Ordered(first);
    const EventApplyResult new_order_result = new_order_cap->ApplyBatch(
        std::span<const EventInput>(&rejected_new_order, 1U));
    CHECK(new_order_result.code == EventApplyCode::kCapacityExhausted);
    CHECK(!new_order_cap->healthy());
    CHECK(new_order_cap->fatal_error().find(
              "order-history byte capacity exhausted") !=
          std::string::npos);
    const EventWorkerStats after_new_order_rejection =
        new_order_cap->stats();
    CHECK(after_new_order_rejection.order_history_bytes ==
          bucket_baseline.order_history_bytes);
    CHECK(after_new_order_rejection.order_history_bytes_high_watermark ==
          bucket_baseline.order_history_bytes);
    OrderSnapshot rejected_snapshot{};
    CHECK(!new_order_cap->CopyOrder(
        OrderKey{20260807U, Market::kShenzhen, 1U, 7U, 1},
        &rejected_snapshot));

    RecordingSink multi_index_cap_sink;
    EventWorkerConfig multi_index_cap_config = Config();
    multi_index_cap_config.maximum_carry_orders = 8U;
    multi_index_cap_config.maximum_order_history_bytes =
        bucket_baseline.order_history_bytes + 1U;
    std::unique_ptr<EventWorker> multi_index_cap = EventWorker::Create(
        multi_index_cap_config, &multi_index_cap_sink, &error);
    CHECK(multi_index_cap != nullptr);
    const CanonicalTick rejected_shanghai =
        ShanghaiAdd(1U, 7'880U, 88);
    const EventInput rejected_shanghai_input = Ordered(rejected_shanghai);
    CHECK(multi_index_cap->ApplyBatch(std::span<const EventInput>(
              &rejected_shanghai_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    const EventWorkerStats after_multi_index_rejection =
        multi_index_cap->stats();
    CHECK(after_multi_index_rejection.order_history_bytes ==
          bucket_baseline.order_history_bytes);
    CHECK(after_multi_index_rejection.order_history_bytes_high_watermark ==
          bucket_baseline.order_history_bytes);
    CHECK(!multi_index_cap->CopyOrder(
        OrderKey{20260807U, Market::kShanghai, 1U, 7U, 88},
        &rejected_snapshot));

    const EventInput first_input = Ordered(first);
    CHECK(probe->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    const EventWorkerStats one_use = probe->stats();
    CHECK(one_use.order_history_bytes > bucket_baseline.order_history_bytes);
    CHECK(one_use.order_history_bytes_high_watermark ==
          one_use.order_history_bytes);

    RecordingSink suffix_cap_sink;
    EventWorkerConfig suffix_cap_config = Config();
    suffix_cap_config.maximum_carry_orders = 8U;
    suffix_cap_config.maximum_order_history_bytes =
        one_use.order_history_bytes;
    std::unique_ptr<EventWorker> suffix_cap = EventWorker::Create(
        suffix_cap_config, &suffix_cap_sink, &error);
    CHECK(suffix_cap != nullptr);
    CHECK(suffix_cap->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    const EventWorkerStats suffix_at_cap = suffix_cap->stats();
    CHECK(suffix_at_cap.order_history_bytes == one_use.order_history_bytes);
    CHECK(suffix_at_cap.order_history_bytes_high_watermark ==
          one_use.order_history_bytes);

    const CanonicalTick cancel = ShenzhenCancel(2U, 789U, 1, 10);
    const EventInput cancel_input = Ordered(cancel);
    const EventApplyResult suffix_result = suffix_cap->ApplyBatch(
        std::span<const EventInput>(&cancel_input, 1U));
    CHECK(suffix_result.code == EventApplyCode::kCapacityExhausted);
    CHECK(!suffix_cap->healthy());
    CHECK(suffix_cap->fatal_error().find(
              "order-history byte capacity exhausted") !=
          std::string::npos);
    const EventWorkerStats after_suffix_rejection = suffix_cap->stats();
    CHECK(after_suffix_rejection.order_history_bytes ==
          suffix_at_cap.order_history_bytes);
    CHECK(after_suffix_rejection.order_history_bytes_high_watermark ==
          suffix_at_cap.order_history_bytes_high_watermark);
    CHECK(suffix_cap->CopyOrder(
        OrderKey{20260807U, Market::kShenzhen, 1U, 7U, 1},
        &rejected_snapshot));
    CHECK(rejected_snapshot.total_cancel_quantity == 0);

    CHECK(probe->ApplyBatch(
              std::span<const EventInput>(&cancel_input, 1U)).code ==
          EventApplyCode::kApplied);
    const EventWorkerStats before_compaction = probe->stats();
    CHECK(before_compaction.order_history_bytes >
          one_use.order_history_bytes);
    Seal(probe.get(), Market::kShenzhen, 7U, 3U);
    DrainEviction(probe.get());
    const EventWorkerStats after_compaction = probe->stats();
    CHECK(after_compaction.order_history_bytes <
          before_compaction.order_history_bytes);
    CHECK(after_compaction.order_history_bytes_high_watermark ==
          before_compaction.order_history_bytes_high_watermark);
    CHECK(after_compaction.order_uses_compacted -
              before_compaction.order_uses_compacted ==
          2U);

    RecordingSink retirement_sink;
    EventWorkerConfig retirement_config = Config();
    retirement_config.maximum_carry_orders = 8U;
    std::unique_ptr<EventWorker> retirement = EventWorker::Create(
        retirement_config, &retirement_sink, &error);
    CHECK(retirement != nullptr);
    const std::size_t retirement_baseline =
        retirement->stats().order_history_bytes;
    const std::array<CanonicalTick, 2U> unknown_trades{
        ShenzhenTrade(1U, 7'881U, 901, 0),
        ShenzhenTrade(2U, 7'882U, 902, 0)};
    const std::array<EventInput, unknown_trades.size()> unknown_trade_inputs{
        Ordered(unknown_trades[0U]), Ordered(unknown_trades[1U])};
    CHECK(retirement->ApplyBatch(unknown_trade_inputs).code ==
          EventApplyCode::kApplied);
    const EventWorkerStats before_retirement = retirement->stats();
    CHECK(before_retirement.order_history_bytes > retirement_baseline);
    Seal(retirement.get(), Market::kShenzhen, 7U, 3U);
    DrainEviction(retirement.get());
    const EventWorkerStats after_retirement = retirement->stats();
    CHECK(after_retirement.order_history_bytes == retirement_baseline);
    CHECK(after_retirement.order_history_bytes_high_watermark ==
          before_retirement.order_history_bytes_high_watermark);
    CHECK(after_retirement.order_uses_compacted -
              before_retirement.order_uses_compacted ==
          2U);
}

void TestShanghaiFullPrivateBaselineMatchesUnsealedReference() {
    RecordingSink compacted_sink;
    RecordingSink reference_sink;
    std::string error;
    std::unique_ptr<EventWorker> compacted =
        EventWorker::Create(Config(), &compacted_sink, &error);
    CHECK(compacted != nullptr);
    std::unique_ptr<EventWorker> reference =
        EventWorker::Create(Config(), &reference_sink, &error);
    CHECK(reference != nullptr);

    const std::array<CanonicalTick, 3U> prefix{
        ShanghaiStatus(1U, 790U, TradingPhase::kContinuous),
        ShanghaiTrade(2U, 791U, 100, 900, Aggressor::kBuy,
                      12'000'000, 10),
        ShanghaiTrade(3U, 792U, 100, 901, Aggressor::kBuy,
                      11'000'000, 20)};
    const std::array<EventInput, prefix.size()> compacted_prefix{
        Ordered(prefix[0U]), Ordered(prefix[1U]), Ordered(prefix[2U])};
    const std::array<EventInput, prefix.size()> reference_prefix{
        Ordered(prefix[0U]), Ordered(prefix[1U]), Ordered(prefix[2U])};
    CHECK(compacted->ApplyBatch(compacted_prefix).code ==
          EventApplyCode::kApplied);
    CHECK(reference->ApplyBatch(reference_prefix).code ==
          EventApplyCode::kApplied);
    AckAll(compacted.get(), prefix);
    AckAll(reference.get(), prefix);

    const OrderKey order_key{
        20260807U, Market::kShanghai, 1U, 7U, 100};
    OrderSnapshot compacted_order{};
    OrderSnapshot reference_order{};
    CHECK(compacted->CopyOrder(order_key, &compacted_order));
    CHECK(reference->CopyOrder(order_key, &reference_order));
    CHECK(compacted_order == reference_order);
    CHECK(compacted_order.observed_pre_add_trade_quantity == 30);
    CHECK(compacted_order.execution_boundary_price_p6 == 12'000'000);

    const EventWorkerStats before_first_seal = compacted->stats();
    Seal(compacted.get(), Market::kShanghai, 7U, 4U);
    DrainEviction(compacted.get());
    const EventWorkerStats after_first_seal = compacted->stats();
    CHECK(after_first_seal.hot_facts == 0U);
    CHECK(after_first_seal.order_history_bytes <
          before_first_seal.order_history_bytes);
    CHECK(reference->stats().hot_facts == prefix.size());

    CanonicalTick add = ShanghaiAdd(4U, 793U, 100, 70);
    add.side = Side::kSell;
    add.buy_order_id = 0;
    add.sell_order_id = 100;
    const std::array<CanonicalTick, 3U> continuation{
        add,
        ShanghaiTrade(5U, 794U, 902, 100, Aggressor::kSell,
                      13'000'000, 20),
        ShanghaiStatus(6U, 795U, TradingPhase::kEnded)};
    const std::array<EventInput, continuation.size()> compacted_continuation{
        Ordered(continuation[0U], 0U, 4U),
        Ordered(continuation[1U], 0U, 4U),
        Ordered(continuation[2U], 0U, 4U)};
    const std::array<EventInput, continuation.size()> reference_continuation{
        Ordered(continuation[0U]), Ordered(continuation[1U]),
        Ordered(continuation[2U])};
    EventApplyResult compacted_result =
        compacted->ApplyBatch(compacted_continuation);
    EventApplyResult reference_result =
        reference->ApplyBatch(reference_continuation);
    CHECK(compacted_result.code == EventApplyCode::kApplied);
    CHECK(reference_result.code == EventApplyCode::kApplied);
    if (compacted_result.repair_pending) {
        DrainRepair(compacted.get());
    }
    if (reference_result.repair_pending) {
        DrainRepair(reference.get());
    }
    AckAll(compacted.get(), continuation);
    AckAll(reference.get(), continuation);

    const auto check_bundle = [&](std::uint64_t sequence) {
        std::vector<std::pair<EventKey, EventPayload>> compacted_bundle;
        std::vector<std::pair<EventKey, EventPayload>> reference_bundle;
        const FactKey key{
            20260807U, Market::kShanghai, 7U, sequence};
        CHECK(compacted->CopyBundle(key, &compacted_bundle));
        CHECK(reference->CopyBundle(key, &reference_bundle));
        CHECK(compacted_bundle == reference_bundle);
        return compacted_bundle;
    };
    const auto add_bundle = check_bundle(4U);
    const EventPayload& add_revision = FindPayload(
        add_bundle, EventKind::kShanghaiOrderRevision, 100);
    CHECK(add_revision.order_delta_operation ==
          OrderDeltaOperation::kUpdate);
    CHECK(add_revision.order.side == Side::kSell);
    CHECK(add_revision.order.original_quantity == 100);
    CHECK(add_revision.order.observed_pre_add_trade_quantity == 30);
    CHECK(add_revision.order.execution_boundary_price_p6 == 11'000'000);
    CHECK(add_revision.order.phase_at_add == TradingPhase::kContinuous);

    const auto trade_bundle = check_bundle(5U);
    const EventPayload& trade_revision = FindPayload(
        trade_bundle, EventKind::kShanghaiOrderRevision, 100);
    CHECK(trade_revision.order_delta_operation ==
          OrderDeltaOperation::kUpdate);
    CHECK(trade_revision.order.total_trade_quantity == 50);
    CHECK(trade_revision.order.post_add_trade_quantity == 20);
    CHECK(trade_revision.order.remaining_quantity == 50);
    CHECK(trade_revision.order.execution_boundary_price_p6 == 11'000'000);

    const auto end_bundle = check_bundle(6U);
    const EventPayload& end_revision = FindPayload(
        end_bundle, EventKind::kShanghaiOrderRevision, 100);
    CHECK(end_revision.order_delta_operation ==
          OrderDeltaOperation::kFinalize);
    CHECK((end_revision.order.quality_flags &
           ShanghaiOrderQualityBit(
               ShanghaiOrderQualityFlag::kEndedWithObservedBalance)) != 0U);

    Seal(compacted.get(), Market::kShanghai, 7U, 7U);
    DrainEviction(compacted.get());
    CHECK(compacted->stats().hot_facts == 0U);

    const CanonicalTick post_end_trade = ShanghaiTrade(
        7U, 796U, 903, 100, Aggressor::kSell, 10'000'000, 5);
    const EventInput compacted_post_end =
        Ordered(post_end_trade, 0U, 7U);
    const EventInput reference_post_end = Ordered(post_end_trade);
    CHECK(compacted->ApplyBatch(
              std::span<const EventInput>(&compacted_post_end, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(reference->ApplyBatch(
              std::span<const EventInput>(&reference_post_end, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(compacted.get(), post_end_trade);
    Ack(reference.get(), post_end_trade);

    std::vector<std::pair<EventKey, EventPayload>> compacted_post_bundle;
    std::vector<std::pair<EventKey, EventPayload>> reference_post_bundle;
    const FactKey post_key{
        20260807U, Market::kShanghai, 7U, 7U};
    CHECK(compacted->CopyBundle(post_key, &compacted_post_bundle));
    CHECK(reference->CopyBundle(post_key, &reference_post_bundle));
    CHECK(compacted_post_bundle == reference_post_bundle);
    const EventPayload& post_revision = FindPayload(
        compacted_post_bundle, EventKind::kShanghaiOrderRevision, 100);
    CHECK(post_revision.order_delta_operation ==
          OrderDeltaOperation::kUpdate);
    CHECK(post_revision.order.total_trade_quantity == 55);
    CHECK(post_revision.order.remaining_quantity == 45);
    CHECK(post_revision.order.execution_boundary_price_p6 == 10'000'000);
    CHECK((post_revision.order.quality_flags &
           ShanghaiOrderQualityBit(
               ShanghaiOrderQualityFlag::kQuantityConflict)) != 0U);
    CHECK(compacted->CopyOrder(order_key, &compacted_order));
    CHECK(reference->CopyOrder(order_key, &reference_order));
    CHECK(compacted_order == reference_order);

    CHECK(compacted_sink.batches.size() == reference_sink.batches.size());
    for (std::size_t index = 0U;
         index < compacted_sink.batches.size(); ++index) {
        const EventRevisionBatch& compacted_batch =
            *compacted_sink.batches[index];
        const EventRevisionBatch& reference_batch =
            *reference_sink.batches[index];
        CHECK(compacted_batch.calculation_run_id ==
              reference_batch.calculation_run_id);
        CHECK(compacted_batch.recovery_run_id ==
              reference_batch.recovery_run_id);
        CHECK(compacted_batch.owner == reference_batch.owner);
        CHECK(compacted_batch.batch_sequence ==
              reference_batch.batch_sequence);
        CHECK(compacted_batch.reason == reference_batch.reason);
        CHECK(compacted_batch.revisions == reference_batch.revisions);
    }
}

void TestShanghaiEndIsSlicedAndCapacityBounded() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.end_slice_max_candidates = 1U;
    config.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.repair_slice_max_order_uses = 64U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 4U> ticks{
        ShanghaiStatus(1U, 750U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 751U, 100),
        ShanghaiAdd(3U, 752U, 200),
        ShanghaiStatus(4U, 753U, TradingPhase::kEnded)};
    const std::array<EventInput, 4U> inputs{
        Ordered(ticks[0U]), Ordered(ticks[1U]),
        Ordered(ticks[2U]), Ordered(ticks[3U])};
    const EventApplyResult applied = worker->ApplyBatch(inputs);
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(applied.repair_pending);
    CHECK(worker->projection_input_fenced());
    CHECK(worker->stats().end_candidates_processed == 1U);
    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(!worker->CopyBundle(
        FactKey{20260807U, Market::kShanghai, 7U, 4U}, &bundle));
    DrainRepair(worker.get());
    CHECK(!worker->projection_input_fenced());
    CHECK(worker->stats().end_expansion_slices >= 2U);
    CHECK(worker->stats().end_candidates_processed == 2U);
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShanghai, 7U, 4U}, &bundle));
    CHECK(bundle.size() >= 2U);
    CHECK(worker->FlushDurableCommits());
    CHECK(!sink.batches.empty());
    CHECK(sink.batches.back()->reason == RevisionReason::kLiveProjection);

    RecordingSink bounded_sink;
    EventWorkerConfig bounded_config = Config();
    bounded_config.maximum_end_candidates = 1U;
    std::unique_ptr<EventWorker> bounded =
        EventWorker::Create(bounded_config, &bounded_sink, &error);
    CHECK(bounded != nullptr);
    const EventApplyResult rejected = bounded->ApplyBatch(inputs);
    CHECK(rejected.code == EventApplyCode::kCapacityExhausted);
    CHECK(!bounded->healthy());
}

void TestShanghaiEndExpansionRejectsDirectOvertake() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.end_slice_max_candidates = 1U;
    config.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker = EventWorker::Create(
        config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 4U> ticks{
        ShanghaiStatus(1U, 9'000U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 9'001U, 100),
        ShanghaiAdd(3U, 9'002U, 200),
        ShanghaiStatus(4U, 9'003U, TradingPhase::kEnded)};
    const std::array<EventInput, 4U> inputs{
        Ordered(ticks[0U]), Ordered(ticks[1U]),
        Ordered(ticks[2U]), Ordered(ticks[3U])};
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    CHECK(worker->projection_input_fenced());

    const CanonicalTick overtaking = ShanghaiAdd(5U, 9'004U, 300);
    const EventInput overtaking_input = Ordered(overtaking);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&overtaking_input, 1U)).code ==
          EventApplyCode::kFailed);
    CHECK(!worker->healthy());
    CHECK(worker->fatal_error().find("projection fence") !=
          std::string::npos);
}

void TestShanghaiFinalFastEndSkipsFinalizedCarryOrders() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.maximum_end_candidates = 1U;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 7U> ticks{
        ShanghaiStatus(1U, 780U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 781U, 100, 10),
        ShanghaiCancel(3U, 782U, 100, 10),
        ShanghaiAdd(4U, 783U, 200, 10),
        ShanghaiCancel(5U, 784U, 200, 10),
        ShanghaiAdd(6U, 785U, 300, 10),
        ShanghaiCancel(7U, 786U, 300, 10)};
    std::array<EventInput, ticks.size()> inputs{};
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        inputs[index] = Ordered(ticks[index]);
    }
    CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    Seal(worker.get(), Market::kShanghai, 7U, 8U);
    DrainEviction(worker.get());
    CHECK(worker->stats().hot_facts == 0U);

    const CanonicalTick end =
        ShanghaiStatus(8U, 787U, TradingPhase::kEnded);
    EventInput end_input = Ordered(end, 0U, 9U);
    const EventApplyResult applied = worker->ApplyBatch(
        std::span<const EventInput>(&end_input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(!applied.repair_pending);
    CHECK(worker->stats().end_candidates_processed == 0U);
    CHECK(worker->healthy());
}

void TestShanghaiEndExpansionMergesPreAttachedBarrier() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.end_slice_max_candidates = 1U;
    config.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.repair_slice_max_order_uses = 64U;
    config.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 3U> initial_ticks{
        ShanghaiStatus(1U, 770U, TradingPhase::kContinuous),
        ShanghaiAdd(2U, 771U, 100),
        ShanghaiAdd(3U, 772U, 200)};
    const std::array<EventInput, 3U> initial_inputs{
        Ordered(initial_ticks[0U]), Ordered(initial_ticks[1U]),
        Ordered(initial_ticks[2U])};
    CHECK(worker->ApplyBatch(initial_inputs).code == EventApplyCode::kApplied);

    const GapOpen gap{Market::kShanghai, 7U, kFeedSessionEpoch,
                      4U, 4U, 1U, next_dispatch_fence++};
    CHECK(worker->ApplyGapOpen(gap));
    const CanonicalTick end =
        ShanghaiStatus(5U, 774U, TradingPhase::kEnded);
    EventInput end_input = Ordered(end, 1U, 4U);
    const EventApplyResult end_applied = worker->ApplyBatch(
        std::span<const EventInput>(&end_input, 1U));
    CHECK(end_applied.code == EventApplyCode::kApplied);
    CHECK(end_applied.repair_pending);
    CHECK(worker->stats().end_candidates_processed == 1U);
    CHECK(worker->projection_input_fenced());
    while (worker->projection_input_fenced()) {
        CHECK(worker->AdvanceRepair());
    }
    CHECK(worker->stats().end_candidates_processed == 2U);

    const CanonicalTick fill = ShanghaiAdd(4U, 773U, 200);
    EventInput fill_input{};
    fill_input.tick = fill;
    fill_input.admission.feed_session_epoch = kFeedSessionEpoch;
    fill_input.admission.expected_sequence = 6U;
    fill_input.admission.admission_floor = 1U;
    fill_input.admission.retention_floor = 6U;
    fill_input.admission.generation = 1U;
    fill_input.admission.dispatch_fence = next_dispatch_fence++;
    fill_input.admission.sequence_class = EventSequenceClass::kHoleFill;
    const EventApplyResult fill_applied = worker->ApplyBatch(
        std::span<const EventInput>(&fill_input, 1U));
    CHECK(fill_applied.code == EventApplyCode::kApplied);
    CHECK(worker->healthy());
    DrainRepair(worker.get());
    DrainEviction(worker.get());
    CHECK(worker->stats().end_candidates_processed == 2U);
    CHECK(worker->stats().hot_facts == 0U);
}

void TestByteAndHotFactCapsFailClosed() {
    RecordingSink pending_probe_sink;
    std::string error;
    std::unique_ptr<EventWorker> pending_probe = EventWorker::Create(
        Config(), &pending_probe_sink, &error);
    CHECK(pending_probe != nullptr);
    const std::uint64_t pending_deque_baseline =
        pending_probe->stats().pending_revision_bytes;
    CHECK(pending_deque_baseline > 1U);
    const std::uint64_t hot_index_directory_baseline =
        pending_probe->stats().hot_fact_bytes;
    CHECK(hot_index_directory_baseline > 1U);

    RecordingSink invalid_hot_directory_sink;
    EventWorkerConfig invalid_hot_directory_config = Config();
    invalid_hot_directory_config.maximum_hot_fact_bytes =
        hot_index_directory_baseline - 1U;
    CHECK(EventWorker::Create(
              invalid_hot_directory_config,
              &invalid_hot_directory_sink, &error) == nullptr);
    CHECK(error.find("hot-index directory") != std::string::npos);

    RecordingSink invalid_revision_sink;
    EventWorkerConfig invalid_revision_config = Config();
    invalid_revision_config.maximum_pending_revision_bytes =
        pending_deque_baseline - 1U;
    CHECK(EventWorker::Create(
              invalid_revision_config, &invalid_revision_sink, &error) ==
          nullptr);

    RecordingSink revision_sink;
    EventWorkerConfig revision_config = Config();
    revision_config.maximum_pending_revision_bytes = pending_deque_baseline;
    std::unique_ptr<EventWorker> revision_worker =
        EventWorker::Create(revision_config, &revision_sink, &error);
    CHECK(revision_worker != nullptr);
    CHECK(revision_worker->stats().pending_revision_bytes ==
          pending_deque_baseline);
    const CanonicalTick revision_tick = ShenzhenAdd(1U, 760U);
    EventInput revision_input = Ordered(revision_tick);
    CHECK(revision_worker->ApplyBatch(
              std::span<const EventInput>(&revision_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    CHECK(!revision_worker->healthy());

    RecordingSink fact_sink;
    EventWorkerConfig fact_config = Config();
    fact_config.maximum_hot_facts = 1U;
    std::unique_ptr<EventWorker> fact_worker =
        EventWorker::Create(fact_config, &fact_sink, &error);
    CHECK(fact_worker != nullptr);
    const CanonicalTick first = ShenzhenAdd(1U, 761U);
    EventInput first_input = Ordered(first);
    CHECK(fact_worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    const CanonicalTick second = ShenzhenAdd(2U, 762U);
    EventInput second_input = Ordered(second);
    CHECK(fact_worker->ApplyBatch(
              std::span<const EventInput>(&second_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    CHECK(!fact_worker->healthy());
}

void TestProjectionCommitScratchExactBoundary() {
    const auto apply_once = [](std::size_t maximum_repair_bytes,
                               RecordingSink* sink,
                               std::string* error) {
        EventWorkerConfig config = Config();
        config.maximum_repair_bytes = maximum_repair_bytes;
        std::unique_ptr<EventWorker> worker = EventWorker::Create(
            config, sink, error);
        CHECK(worker != nullptr);
        const CanonicalTick tick = ShenzhenTrade(1U, 7'600U, 0, 0);
        const EventInput input = Ordered(tick);
        const EventApplyResult result = worker->ApplyBatch(
            std::span<const EventInput>(&input, 1U));
        return std::pair{std::move(worker), result};
    };

    std::string error;
    RecordingSink probe_sink;
    auto [probe, probe_result] = apply_once(
        Config().maximum_repair_bytes, &probe_sink, &error);
    CHECK(probe_result.code == EventApplyCode::kApplied);
    const std::uint64_t exact =
        probe->stats().pending_phase_bytes_high_watermark;
    CHECK(exact > 1U);

    RecordingSink exact_sink;
    auto [exact_worker, exact_result] = apply_once(
        exact, &exact_sink, &error);
    CHECK(exact_result.code == EventApplyCode::kApplied);
    CHECK(exact_worker->healthy());

    RecordingSink below_sink;
    auto [below_worker, below_result] = apply_once(
        exact - 1U, &below_sink, &error);
    CHECK(below_result.code == EventApplyCode::kCapacityExhausted);
    CHECK(!below_worker->healthy());
    CHECK(below_worker->fatal_error().find(
              "projection commit byte capacity exhausted") !=
          std::string::npos);
}

void TestPendingCommitDequeAccountingReturnsToBaseline() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker = EventWorker::Create(
        Config(), &sink, &error);
    CHECK(worker != nullptr);
    const std::uint64_t baseline = worker->stats().pending_revision_bytes;
    CHECK(baseline != 0U);

    constexpr std::size_t kCommitCount = 24U;
    std::array<CanonicalTick, kCommitCount> ticks{};
    std::uint64_t previous = baseline;
    std::uint64_t normal_growth = 0U;
    bool saw_deque_block_growth = false;
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        ticks[index] = ShenzhenAdd(
            index + 1U, 7'700U + index);
        const EventInput input = Ordered(ticks[index]);
        CHECK(worker->ApplyBatch(
                  std::span<const EventInput>(&input, 1U)).code ==
              EventApplyCode::kApplied);
        const std::uint64_t current =
            worker->stats().pending_revision_bytes;
        CHECK(current > previous);
        const std::uint64_t growth = current - previous;
        if (normal_growth == 0U) {
            normal_growth = growth;
        } else if (growth > normal_growth) {
            saw_deque_block_growth = true;
        }
        previous = current;
    }
    CHECK(saw_deque_block_growth);
    CHECK(worker->stats().pending_raw_commits == kCommitCount);
    AckAll(worker.get(), ticks);
    CHECK(worker->stats().pending_raw_commits == 0U);
    CHECK(worker->stats().pending_revision_bytes == baseline);
    CHECK(worker->stats().pending_revision_bytes_high_watermark > baseline);
}

void TestPhaseAnchorAccountingPlatformsWithoutHotFacts() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventWorker> worker = EventWorker::Create(
        Config(), &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick first = ShanghaiStatus(
        1U, 7'800U, TradingPhase::kContinuous);
    const EventInput first_input = Ordered(first, 0U, 2U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), first);
    DrainEviction(worker.get());
    const EventWorkerStats first_anchor = worker->stats();
    CHECK(first_anchor.hot_facts == 0U);
    CHECK(first_anchor.hot_fact_bytes != 0U);

    const CanonicalTick second = ShanghaiStatus(
        2U, 7'801U, TradingPhase::kClosingCall);
    const EventInput second_input = Ordered(second, 0U, 3U);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&second_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), second);
    DrainEviction(worker.get());
    const EventWorkerStats second_anchor = worker->stats();
    CHECK(second_anchor.hot_facts == 0U);
    CHECK(second_anchor.hot_fact_bytes == first_anchor.hot_fact_bytes);
}

void TestHotIndexSingleElementRangesAndChannelOuterAccounting() {
    const auto invalid_trade = [](std::uint64_t sequence,
                                  std::uint64_t ingress,
                                  std::uint32_t instrument_id,
                                  std::uint32_t channel) {
        CanonicalTick tick = ShenzhenTrade(sequence, ingress, 0, 0);
        tick.common.instrument_id = instrument_id;
        tick.common.channel = channel;
        tick.validity = kTickChannelHistoryValid;
        return tick;
    };
    const auto second_growth = [&invalid_trade](bool new_instrument,
                                                bool new_channel) {
        RecordingSink sink;
        EventWorkerConfig config = Config();
        config.maximum_hot_facts = 64U;
        std::string error;
        std::unique_ptr<EventWorker> worker = EventWorker::Create(
            config, &sink, &error);
        CHECK(worker != nullptr);
        const CanonicalTick first = invalid_trade(1U, 8'100U, 1U, 7U);
        const EventInput first_input = Ordered(first);
        CHECK(worker->ApplyBatch(
                  std::span<const EventInput>(&first_input, 1U)).code ==
              EventApplyCode::kInvalidInput);
        const std::uint64_t before = worker->stats().hot_fact_bytes;
        const CanonicalTick second = invalid_trade(
            new_channel ? 1U : 2U, 8'101U,
            new_instrument ? 2U : 1U, new_channel ? 8U : 7U);
        const EventInput second_input = Ordered(second);
        CHECK(worker->ApplyBatch(
                  std::span<const EventInput>(&second_input, 1U)).code ==
              EventApplyCode::kInvalidInput);
        return worker->stats().hot_fact_bytes - before;
    };

    const std::uint64_t existing_range_growth = second_growth(false, false);
    const std::uint64_t new_instrument_growth = second_growth(true, false);
    const std::uint64_t new_channel_growth = second_growth(false, true);
    CHECK(new_instrument_growth > existing_range_growth);
    CHECK(new_channel_growth > new_instrument_growth);

    constexpr std::size_t kRangeCount = 12U;
    RecordingSink probe_sink;
    EventWorkerConfig probe_config = Config();
    probe_config.maximum_hot_facts = 64U;
    std::string error;
    std::unique_ptr<EventWorker> probe = EventWorker::Create(
        probe_config, &probe_sink, &error);
    CHECK(probe != nullptr);
    const std::uint64_t directory_baseline = probe->stats().hot_fact_bytes;
    std::vector<CanonicalTick> ticks;
    std::vector<EventInput> inputs;
    ticks.reserve(kRangeCount);
    inputs.reserve(kRangeCount);
    for (std::size_t index = 0U; index < kRangeCount; ++index) {
        ticks.push_back(invalid_trade(
            index + 1U, 8'200U + index,
            static_cast<std::uint32_t>(index + 1U), 7U));
        inputs.push_back(Ordered(ticks.back()));
    }
    CHECK(probe->ApplyBatch(inputs).code == EventApplyCode::kInvalidInput);
    const std::uint64_t exact = probe->stats().hot_fact_bytes;
    CHECK(exact > directory_baseline);
    AckAll(probe.get(), ticks);
    Seal(probe.get(), Market::kShenzhen, 7U, kRangeCount + 1U);
    DrainEviction(probe.get());
    CHECK(probe->stats().hot_facts == 0U);
    CHECK(probe->stats().hot_fact_bytes == directory_baseline);

    RecordingSink single_sink;
    EventWorkerConfig single_config = Config();
    single_config.maximum_hot_facts = 64U;
    std::unique_ptr<EventWorker> single = EventWorker::Create(
        single_config, &single_sink, &error);
    CHECK(single != nullptr);
    const CanonicalTick first = invalid_trade(1U, 8'300U, 1U, 7U);
    const EventInput first_input = Ordered(first);
    CHECK(single->ApplyBatch(
              std::span<const EventInput>(&first_input, 1U)).code ==
          EventApplyCode::kInvalidInput);
    const std::uint64_t single_live = single->stats().hot_fact_bytes;

    RecordingSink capped_sink;
    EventWorkerConfig capped_config = Config();
    capped_config.maximum_hot_facts = 64U;
    capped_config.maximum_hot_fact_bytes =
        single_live + existing_range_growth;
    std::unique_ptr<EventWorker> capped = EventWorker::Create(
        capped_config, &capped_sink, &error);
    CHECK(capped != nullptr);
    const CanonicalTick capped_first = invalid_trade(1U, 8'400U, 1U, 7U);
    const EventInput capped_first_input = Ordered(capped_first);
    CHECK(capped->ApplyBatch(
              std::span<const EventInput>(&capped_first_input, 1U)).code ==
          EventApplyCode::kInvalidInput);
    const CanonicalTick new_range = invalid_trade(2U, 8'401U, 2U, 7U);
    const EventInput new_range_input = Ordered(new_range);
    CHECK(capped->ApplyBatch(
              std::span<const EventInput>(&new_range_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    CHECK(!capped->healthy());
    CHECK(capped->stats().hot_fact_bytes <=
          capped_config.maximum_hot_fact_bytes);
    CHECK(capped->fatal_error().find("hot-index byte capacity") !=
          std::string::npos);
}

void TestPhaseDequeBlockGrowthIsPreflightedAndCompactsToAnchor() {
    constexpr std::size_t kPhaseCount = 32U;
    RecordingSink probe_sink;
    EventWorkerConfig probe_config = Config();
    probe_config.maximum_hot_facts = 128U;
    std::string error;
    std::unique_ptr<EventWorker> probe = EventWorker::Create(
        probe_config, &probe_sink, &error);
    CHECK(probe != nullptr);
    const std::uint64_t directory_baseline = probe->stats().hot_fact_bytes;

    std::uint64_t bytes_after_31 = 0U;
    std::uint64_t growth_31 = 0U;
    std::uint64_t growth_32 = 0U;
    std::uint64_t previous = directory_baseline;
    for (std::size_t index = 0U; index < kPhaseCount; ++index) {
        const CanonicalTick tick = ShanghaiStatus(
            index + 1U, 8'400U + index, TradingPhase::kContinuous);
        const EventInput input = Ordered(tick);
        CHECK(probe->ApplyBatch(
                  std::span<const EventInput>(&input, 1U)).code ==
              EventApplyCode::kApplied);
        Ack(probe.get(), tick);
        const std::uint64_t current = probe->stats().hot_fact_bytes;
        if (index == 30U) {
            bytes_after_31 = current;
            growth_31 = current - previous;
        } else if (index == 31U) {
            growth_32 = current - previous;
        }
        previous = current;
    }
    CHECK(growth_31 != 0U);
    CHECK(growth_32 > growth_31);
    Seal(probe.get(), Market::kShanghai, 7U, kPhaseCount + 1U);
    DrainEviction(probe.get());
    const std::uint64_t anchor_baseline = probe->stats().hot_fact_bytes;
    CHECK(probe->stats().hot_facts == 0U);
    CHECK(anchor_baseline > directory_baseline);

    const CanonicalTick next = ShanghaiStatus(
        kPhaseCount + 1U, 8'500U, TradingPhase::kClosingCall);
    const EventInput next_input = Ordered(next, 0U, 1U);
    CHECK(probe->ApplyBatch(
              std::span<const EventInput>(&next_input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(probe.get(), next);
    Seal(probe.get(), Market::kShanghai, 7U, kPhaseCount + 2U);
    DrainEviction(probe.get());
    CHECK(probe->stats().hot_facts == 0U);
    CHECK(probe->stats().hot_fact_bytes == anchor_baseline);

    RecordingSink staging_sink;
    EventWorkerConfig staging_config = Config();
    staging_config.maximum_hot_facts = 128U;
    staging_config.maximum_hot_fact_bytes = bytes_after_31 + growth_31;
    std::unique_ptr<EventWorker> staging = EventWorker::Create(
        staging_config, &staging_sink, &error);
    CHECK(staging != nullptr);
    for (std::size_t index = 0U; index < kPhaseCount - 1U; ++index) {
        const CanonicalTick tick = ShanghaiStatus(
            index + 1U, 8'600U + index, TradingPhase::kContinuous);
        const EventInput input = Ordered(tick);
        CHECK(staging->ApplyBatch(
                  std::span<const EventInput>(&input, 1U)).code ==
              EventApplyCode::kApplied);
        Ack(staging.get(), tick);
    }
    CHECK(staging->stats().hot_fact_bytes == bytes_after_31);
    const CanonicalTick staged_crossing = ShanghaiStatus(
        kPhaseCount, 8'700U, TradingPhase::kContinuous);
    const EventInput staged_crossing_input = Ordered(staged_crossing);
    CHECK(staging->ApplyBatch(
              std::span<const EventInput>(&staged_crossing_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    const std::uint64_t staged_index_bytes =
        staging->stats().hot_fact_bytes;
    CHECK(staged_index_bytes > bytes_after_31);

    RecordingSink capped_sink;
    EventWorkerConfig capped_config = Config();
    capped_config.maximum_hot_facts = 128U;
    capped_config.maximum_hot_fact_bytes = staged_index_bytes - 1U;
    std::unique_ptr<EventWorker> capped = EventWorker::Create(
        capped_config, &capped_sink, &error);
    CHECK(capped != nullptr);
    for (std::size_t index = 0U; index < kPhaseCount - 1U; ++index) {
        const CanonicalTick tick = ShanghaiStatus(
            index + 1U, 8'800U + index, TradingPhase::kContinuous);
        const EventInput input = Ordered(tick);
        CHECK(capped->ApplyBatch(
                  std::span<const EventInput>(&input, 1U)).code ==
              EventApplyCode::kApplied);
        Ack(capped.get(), tick);
    }
    CHECK(capped->stats().hot_fact_bytes == bytes_after_31);
    const CanonicalTick crossing = ShanghaiStatus(
        kPhaseCount, 8'900U, TradingPhase::kContinuous);
    const EventInput crossing_input = Ordered(crossing);
    CHECK(capped->ApplyBatch(
              std::span<const EventInput>(&crossing_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    CHECK(!capped->healthy());
    CHECK(capped->stats().hot_fact_bytes <=
          capped_config.maximum_hot_fact_bytes);
    CHECK(capped->fatal_error().find("hot-index byte capacity") !=
          std::string::npos);
}

void TestEndBarrierIndexIsPreflightedAndReleased() {
    std::string error;
    EventWorkerConfig continuous_config = Config();
    continuous_config.maximum_hot_facts = 64U;
    RecordingSink continuous_sink;
    std::unique_ptr<EventWorker> continuous = EventWorker::Create(
        continuous_config, &continuous_sink, &error);
    CHECK(continuous != nullptr);
    const CanonicalTick continuous_tick = ShanghaiStatus(
        1U, 8'800U, TradingPhase::kContinuous);
    const EventInput continuous_input = Ordered(continuous_tick);
    CHECK(continuous->ApplyBatch(
              std::span<const EventInput>(&continuous_input, 1U)).code ==
          EventApplyCode::kApplied);
    const std::uint64_t continuous_live =
        continuous->stats().hot_fact_bytes;
    Ack(continuous.get(), continuous_tick);
    Seal(continuous.get(), Market::kShanghai, 7U, 2U);
    DrainEviction(continuous.get());
    const std::uint64_t anchor_baseline =
        continuous->stats().hot_fact_bytes;

    EventWorkerConfig end_config = Config();
    end_config.maximum_hot_facts = 64U;
    RecordingSink end_sink;
    std::unique_ptr<EventWorker> ended = EventWorker::Create(
        end_config, &end_sink, &error);
    CHECK(ended != nullptr);
    const CanonicalTick end_tick = ShanghaiStatus(
        1U, 8'900U, TradingPhase::kEnded);
    const EventInput end_input = Ordered(end_tick);
    CHECK(ended->ApplyBatch(
              std::span<const EventInput>(&end_input, 1U)).code ==
          EventApplyCode::kApplied);
    const std::uint64_t end_live = ended->stats().hot_fact_bytes;
    CHECK(end_live > continuous_live);
    Ack(ended.get(), end_tick);
    Seal(ended.get(), Market::kShanghai, 7U, 2U);
    DrainEviction(ended.get());
    CHECK(ended->stats().hot_facts == 0U);
    CHECK(ended->stats().hot_fact_bytes == anchor_baseline);

    EventWorkerConfig staging_config = Config();
    staging_config.maximum_hot_facts = 64U;
    staging_config.maximum_hot_fact_bytes = end_live - 1U;
    RecordingSink staging_sink;
    std::unique_ptr<EventWorker> staging = EventWorker::Create(
        staging_config, &staging_sink, &error);
    CHECK(staging != nullptr);
    const CanonicalTick staged_end = ShanghaiStatus(
        1U, 9'000U, TradingPhase::kEnded);
    const EventInput staged_input = Ordered(staged_end);
    CHECK(staging->ApplyBatch(
              std::span<const EventInput>(&staged_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    const std::uint64_t staged_index_bytes =
        staging->stats().hot_fact_bytes;
    CHECK(staged_index_bytes > anchor_baseline);

    EventWorkerConfig capped_config = Config();
    capped_config.maximum_hot_facts = 64U;
    capped_config.maximum_hot_fact_bytes = staged_index_bytes - 1U;
    RecordingSink capped_sink;
    std::unique_ptr<EventWorker> capped = EventWorker::Create(
        capped_config, &capped_sink, &error);
    CHECK(capped != nullptr);
    const CanonicalTick capped_end = ShanghaiStatus(
        1U, 9'100U, TradingPhase::kEnded);
    const EventInput capped_input = Ordered(capped_end);
    CHECK(capped->ApplyBatch(
              std::span<const EventInput>(&capped_input, 1U)).code ==
          EventApplyCode::kCapacityExhausted);
    CHECK(!capped->healthy());
    CHECK(capped->stats().hot_fact_bytes <=
          capped_config.maximum_hot_fact_bytes);
    CHECK(capped->fatal_error().find("hot-index byte capacity") !=
          std::string::npos);
}

void TestEvictionByteSliceIncludesCarryOrderRetirement() {
    const auto insert_two_orders = [](EventWorker* worker,
                                      std::uint64_t ingress_base) {
        std::array<CanonicalTick, 2U> ticks{
            ShenzhenAdd(1U, ingress_base),
            ShenzhenAdd(2U, ingress_base + 1U)};
        ticks[1U].common.instrument_id = 2U;
        const std::array<EventInput, 2U> inputs{
            Ordered(ticks[0U]), Ordered(ticks[1U])};
        CHECK(worker->ApplyBatch(inputs).code == EventApplyCode::kApplied);
    };

    RecordingSink probe_sink;
    EventWorkerConfig probe_config = Config();
    probe_config.eviction_slice_max_nodes = 1U;
    probe_config.eviction_slice_max_bytes =
        std::numeric_limits<std::size_t>::max();
    std::string error;
    std::unique_ptr<EventWorker> probe = EventWorker::Create(
        probe_config, &probe_sink, &error);
    CHECK(probe != nullptr);
    insert_two_orders(probe.get(), 7'900U);
    const EventWorkerStats before_probe = probe->stats();
    Seal(probe.get(), Market::kShenzhen, 7U, 3U);
    const EventWorkerStats after_probe = probe->stats();
    CHECK(before_probe.order_history_bytes >
          after_probe.order_history_bytes);
    const std::uint64_t retirement_bytes =
        before_probe.order_history_bytes -
        after_probe.order_history_bytes;
    CHECK(retirement_bytes != 0U);

    RecordingSink sliced_sink;
    EventWorkerConfig sliced_config = Config();
    sliced_config.eviction_slice_max_nodes = 64U;
    sliced_config.eviction_slice_max_bytes = retirement_bytes;
    std::unique_ptr<EventWorker> sliced = EventWorker::Create(
        sliced_config, &sliced_sink, &error);
    CHECK(sliced != nullptr);
    insert_two_orders(sliced.get(), 8'000U);
    const EventWorkerStats before_slice = sliced->stats();
    Seal(sliced.get(), Market::kShenzhen, 7U, 3U);
    const EventWorkerStats after_slice = sliced->stats();
    CHECK(after_slice.order_uses_compacted -
              before_slice.order_uses_compacted ==
          1U);
    CHECK(after_slice.hot_facts == 2U);
    CHECK(sliced->eviction_pending());
    DrainEviction(sliced.get());
    CHECK(sliced->stats().hot_facts == 0U);
}

void TestRuntimeRoutesOwnersAndJoinsRawAcks() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.worker.owner_count = 2U;
    config.worker.end_slice_max_candidates = 1U;
    config.worker.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1U;
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
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(owner0, 0U)));
    CHECK(runtime->AppendDispatch(1U, RuntimeOrdered(owner1, 1U)));
    CHECK(runtime->FlushAll());
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 2U);

    CanonicalTick fill = ShenzhenTrade(202U, 82U, 201, 0);
    fill.common.instrument_id = 2U;
    fill.common.instrument_ordinal = 1U;
    CHECK(runtime->AppendDispatch(
        1U, RuntimeGapOpen(fill, 1U, 202U, 202U, 1U)));
    CHECK(runtime->AppendDispatch(
        1U, RuntimeHoleFill(fill, 1U, 203U, 1U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 3U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(runtime->worker(1U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 202U}, &bundle));
    CHECK(!runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 202U}, &bundle));

    const std::array<CanonicalTick, 8U> end_ticks{
        ShanghaiStatus(301U, 83U, TradingPhase::kContinuous),
        ShanghaiAdd(302U, 84U, 300),
        ShanghaiAdd(303U, 85U, 301),
        ShanghaiAdd(304U, 86U, 302),
        ShanghaiAdd(305U, 87U, 303),
        ShanghaiAdd(306U, 88U, 304),
        ShanghaiAdd(307U, 89U, 305),
        ShanghaiStatus(308U, 90U, TradingPhase::kEnded)};
    for (const CanonicalTick& tick : end_ticks) {
        CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(tick, 0U)));
    }
    CHECK(runtime->DrainAll());

    const EventRuntimeStats stats = runtime->stats();
    CHECK(stats.ordered_dispositions_received == 10U);
    CHECK(stats.hole_fill_dispositions_received == 1U);
    CHECK(stats.gap_open_controls_received == 1U);
    const EventWorkerStats owner0_stats = runtime->worker(0U)->stats();
    const EventWorkerStats owner1_stats = runtime->worker(1U)->stats();
    CHECK(stats.workers.pending_revision_bytes ==
          owner0_stats.pending_revision_bytes +
              owner1_stats.pending_revision_bytes);
    CHECK(stats.workers.pending_revision_bytes_high_watermark ==
          std::max(owner0_stats.pending_revision_bytes_high_watermark,
                   owner1_stats.pending_revision_bytes_high_watermark));
    CHECK(stats.workers.pending_revision_bytes_high_watermark != 0U);
    CHECK(stats.workers.active_repair_orders ==
          owner0_stats.active_repair_orders +
              owner1_stats.active_repair_orders);
    CHECK(stats.workers.active_repair_bytes ==
          owner0_stats.active_repair_bytes + owner1_stats.active_repair_bytes);
    CHECK(stats.workers.active_repair_bytes_high_watermark ==
          std::max(owner0_stats.active_repair_bytes_high_watermark,
                   owner1_stats.active_repair_bytes_high_watermark));
    CHECK(stats.workers.active_repair_bytes_high_watermark != 0U);
    CHECK(stats.workers.end_expansion_slices ==
          owner0_stats.end_expansion_slices +
              owner1_stats.end_expansion_slices);
    CHECK(stats.workers.end_candidates_processed ==
          owner0_stats.end_candidates_processed +
              owner1_stats.end_candidates_processed);
    CHECK(stats.workers.end_expansion_slices >= 6U);
    CHECK(stats.workers.end_candidates_processed == 6U);
    CHECK(stats.workers.hot_facts ==
          owner0_stats.hot_facts + owner1_stats.hot_facts);
    CHECK(stats.workers.hot_fact_bytes ==
          owner0_stats.hot_fact_bytes + owner1_stats.hot_fact_bytes);
    CHECK(stats.workers.hot_fact_bytes_high_watermark ==
          std::max(owner0_stats.hot_fact_bytes_high_watermark,
                   owner1_stats.hot_fact_bytes_high_watermark));
    CHECK(stats.workers.hot_fact_bytes_high_watermark != 0U);
    CHECK(runtime->healthy());
}

void TestRuntimeDrainAllFlushesFinalPartialBatch() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1'000'000U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick tick = ShenzhenAdd(101U, 170U);
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(tick, 0U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
}

void TestPersistenceGroupingIsIndependentFromControlFences() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 32U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000'000);
    config.worker.persistence_group_max_batches = 8U;
    config.worker.persistence_group_max_rows = 1'024U;
    config.worker.persistence_group_max_bytes = 1U << 20U;
    config.worker.persistence_group_max_delay_ns =
        UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick tick = ShenzhenAdd(101U, 7'001U);
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(tick, 0U)));
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(tick, 0U, 102U, 102U, 1U)));
    CHECK(sink.batches.empty());
    CHECK(sink.group_sizes.empty());

    CHECK(runtime->AppendDispatch(
        0U, RuntimeChannelSeal(tick, 0U, 1U, 1U)));
    CHECK(sink.batches.empty());
    const EventRuntimeStats before_drain = runtime->stats();
    CHECK(before_drain.micro_batches_applied == 1U);
    CHECK(before_drain.facts_in_micro_batches == 1U);
    CHECK(before_drain.forced_active_flushes == 1U);
    CHECK(before_drain.empty_control_flushes == 1U);
    CHECK(before_drain.workers.pending_raw_commits == 1U);
    CHECK(before_drain.workers.persistence_groups_submitted == 0U);

    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);
    CHECK(sink.group_sizes.size() == 1U);
    CHECK(sink.group_sizes.front() == 1U);
    const EventRuntimeStats after_drain = runtime->stats();
    CHECK(after_drain.workers.persistence_groups_submitted == 1U);
    CHECK(after_drain.workers.revision_batches_submitted == 1U);
}

void TestOwnerPersistenceAggregatorClosesAtBatchBound() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.persistence_group_max_batches = 2U;
    config.persistence_group_max_rows = 1'024U;
    config.persistence_group_max_bytes = 1U << 20U;
    config.persistence_group_max_delay_ns =
        UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker = EventWorker::Create(
        config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 2U> ticks{
        ShenzhenAdd(101U, 7'101U), ShenzhenAdd(102U, 7'102U)};
    const EventInput first = Ordered(ticks[0U]);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&first, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(sink.batches.empty());
    const EventInput second = Ordered(ticks[1U]);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&second, 1U)).code ==
          EventApplyCode::kApplied);
    CHECK(sink.group_sizes.size() == 1U);
    CHECK(sink.group_sizes.front() == 2U);
    CHECK(sink.batches.size() == 2U);
    CHECK(sink.batches[0U]->batch_sequence == 1U);
    CHECK(sink.batches[1U]->batch_sequence == 2U);
    const EventWorkerStats stats = worker->stats();
    CHECK(stats.persistence_groups_submitted == 1U);
    CHECK(stats.persistence_group_batches_max == 2U);
    CHECK(stats.persistence_group_rows_max == 2U);
    CHECK(stats.revision_batches_submitted == 2U);
    CHECK(stats.pending_raw_commits == 0U);
}

void TestOwnerPersistenceAggregatorClosesAtPendingCapacity() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.maximum_pending_commits = 2U;
    config.persistence_group_max_batches = 8U;
    config.persistence_group_max_rows = 1'024U;
    config.persistence_group_max_bytes = 1U << 20U;
    config.persistence_group_max_delay_ns =
        UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventWorker> worker = EventWorker::Create(
        config, &sink, &error);
    CHECK(worker != nullptr);

    const std::array<CanonicalTick, 2U> ticks{
        ShenzhenAdd(101U, 7'201U), ShenzhenAdd(102U, 7'202U)};
    for (const CanonicalTick& tick : ticks) {
        const EventInput input = Ordered(tick);
        CHECK(worker->ApplyBatch(
                  std::span<const EventInput>(&input, 1U)).code ==
              EventApplyCode::kApplied);
    }

    CHECK(worker->healthy());
    CHECK(sink.group_sizes.size() == 1U);
    CHECK(sink.group_sizes.front() == 2U);
    CHECK(sink.batches.size() == 2U);
    CHECK(worker->stats().pending_raw_commits == 0U);
}

void TestRuntimeAckDrainIsBoundedAndSourceTimeDoesNotDriveTimer() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 8U;
    config.micro_batch_max_delay_ns = 1'000'000U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        config, &sink, &error);
    CHECK(runtime != nullptr);

    std::array<CanonicalTick, 4U> ticks{};
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        ticks[index] = ShenzhenAdd(101U + index, 7'201U + index);
        CHECK(runtime->AppendDispatch(
            0U, RuntimeOrdered(ticks[index], 0U)));
    }

    // The synthetic source timestamps are far behind the current clock. A
    // zero scheduling time must not make the active batch timer-expired.
    CHECK(runtime->FlushDue(0U, 0U));
    const EventRuntimeStats sliced = runtime->stats();
    CHECK(sliced.micro_batches_applied == 0U);
    CHECK(sliced.timer_flushes == 0U);

    CHECK(runtime->DrainAll());
    const EventRuntimeStats drained = runtime->stats();
    CHECK(drained.facts_in_micro_batches == ticks.size());
    CHECK(sink.batches.size() == 1U);
}

void TestRuntimeChannelSealMailboxCoalescesLatestWatermark() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 32U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick channel = ShenzhenAdd(1U, 7'301U);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(channel, 0U, 1U, 1U, 1U)));

    const TickDispatch first =
        RuntimeChannelSeal(channel, 0U, 4U, 1U);
    const TickDispatch second =
        RuntimeChannelSeal(channel, 0U, 8U, 1U);
    const TickDispatch latest =
        RuntimeChannelSeal(channel, 0U, 6U, 1U);
    CHECK(runtime->AppendDispatch(0U, first));
    CHECK(runtime->AppendDispatch(0U, second));
    CHECK(runtime->AppendDispatch(0U, latest));

    const EventRuntimeStats staged = runtime->stats();
    CHECK(staged.gap_open_controls_received == 1U);
    CHECK(staged.channel_seal_controls_received == 3U);
    CHECK(staged.channel_seals_applied == 0U);
    CHECK(staged.channel_seals_coalesced == 2U);
    CHECK(staged.pending_channel_seals == 1U);
    CHECK(staged.pending_channel_seals_high_water == 1U);

    CHECK(runtime->FlushDue(0U, 0U));
    const EventRuntimeStats applied = runtime->stats();
    CHECK(applied.channel_seals_applied == 1U);
    CHECK(applied.pending_channel_seals == 0U);

    EventChannelState state{};
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.generation == 1U);
    CHECK(state.sealed_before == 8U);
    CHECK(state.applied_dispatch_fence == latest.dispatch_fence);
    CHECK(runtime->DrainAll());
}

void TestRuntimeChannelSealMailboxPreservesChannelOrder() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 32U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick channel = ShenzhenAdd(1U, 7'401U);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(channel, 0U, 1U, 1U, 1U)));
    const TickDispatch seal =
        RuntimeChannelSeal(channel, 0U, 2U, 1U);
    CHECK(runtime->AppendDispatch(0U, seal));
    CHECK(runtime->stats().pending_channel_seals == 1U);

    const CanonicalTick following = ShenzhenAdd(2U, 7'402U);
    const TickDispatch fact = RuntimeOrdered(following, 0U, 1U);
    CHECK(runtime->AppendDispatch(0U, fact));
    const EventRuntimeStats after_fact = runtime->stats();
    CHECK(after_fact.channel_seals_applied == 1U);
    CHECK(after_fact.pending_channel_seals == 0U);

    EventChannelState state{};
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.sealed_before == 2U);
    CHECK(state.applied_dispatch_fence == seal.dispatch_fence);
    CHECK(runtime->Flush(0U));
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.applied_dispatch_fence == fact.dispatch_fence);
    CHECK(runtime->DrainAll());

    runtime.reset();
    runtime = EventRuntime::Create(RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(channel, 0U, 1U, 1U, 1U)));
    CHECK(runtime->AppendDispatch(
        0U, RuntimeChannelSeal(channel, 0U, 2U, 1U)));
    const TickDispatch next_gap =
        RuntimeGapOpen(channel, 0U, 2U, 2U, 2U);
    CHECK(runtime->AppendDispatch(0U, next_gap));
    const EventRuntimeStats after_gap = runtime->stats();
    CHECK(after_gap.gap_open_controls_received == 2U);
    CHECK(after_gap.channel_seals_applied == 1U);
    CHECK(after_gap.channel_seals_coalesced == 0U);
    CHECK(after_gap.pending_channel_seals == 0U);
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShenzhen, 7U, &state));
    CHECK(state.generation == 2U);
    CHECK(state.applied_dispatch_fence == next_gap.dispatch_fence);
    CHECK(runtime->DrainAll());
}

void TestRuntimeChannelSealMailboxRejectsProtocolMismatch() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);
    const CanonicalTick channel = ShenzhenAdd(1U, 7'501U);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(channel, 0U, 1U, 1U, 1U)));
    const TickDispatch first =
        RuntimeChannelSeal(channel, 0U, 2U, 1U);
    CHECK(runtime->AppendDispatch(0U, first));
    TickDispatch stale_fence =
        RuntimeChannelSeal(channel, 0U, 3U, 1U);
    stale_fence.dispatch_fence = first.dispatch_fence;
    CHECK(!runtime->AppendDispatch(0U, stale_fence));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("generation/fence is stale") !=
          std::string::npos);

    runtime.reset();
    runtime = EventRuntime::Create(RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(channel, 0U, 1U, 1U, 1U)));
    CHECK(runtime->AppendDispatch(
        0U, RuntimeChannelSeal(channel, 0U, 2U, 1U)));
    CHECK(!runtime->AppendDispatch(
        0U, RuntimeChannelSeal(channel, 0U, 3U, 2U)));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("generation/fence is stale") !=
          std::string::npos);
}

void TestRuntimeDefersPoppedControlBehindPhaseFence() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 32U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000'000);
    config.worker.phase_slice_max_nodes = 1U;
    config.worker.phase_slice_max_bytes = 1U << 20U;
    config.worker.phase_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    CHECK(runtime->CanPollDispatch(0U));
    CHECK(!runtime->CanPollDispatch(1U));

    const auto project_and_flush = [&](const CanonicalTick& tick,
                                       std::uint64_t generation) {
        CHECK(runtime->AppendDispatch(
            0U, RuntimeOrdered(tick, 0U, generation)));
        CHECK(runtime->Flush(0U));
        std::size_t service_calls = 0U;
        while (!runtime->CanPollDispatch(0U)) {
            CHECK(runtime->FlushDue(0U, 0U));
            CHECK(++service_calls < 128U);
        }
        CHECK(runtime->CanPollDispatch(0U));
    };

    const CanonicalTick opening =
        ShanghaiStatus(1U, 8'000U, TradingPhase::kContinuous);
    project_and_flush(opening, 0U);
    const TickDispatch gap = RuntimeGapOpen(opening, 0U, 2U, 2U, 1U);
    CHECK(runtime->AppendDispatch(0U, gap));

    for (std::uint64_t sequence = 3U; sequence <= 10U; ++sequence) {
        project_and_flush(
            ShanghaiAdd(sequence, 8'000U + sequence,
                         static_cast<std::int64_t>(100U + sequence)),
            1U);
    }
    project_and_flush(
        ShanghaiStatus(11U, 8'011U, TradingPhase::kClosingCall), 1U);

    const CanonicalTick late =
        ShanghaiStatus(2U, 8'012U, TradingPhase::kOpeningCall);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeHoleFill(late, 0U, 12U, 1U)));
    CHECK(runtime->CanPollDispatch(0U));

    const TickDispatch seal = RuntimeChannelSeal(late, 0U, 1U, 1U);
    CHECK(runtime->AppendDispatch(0U, seal));
    CHECK(!runtime->CanPollDispatch(0U));

    EventChannelState channel{};
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShanghai, 7U, &channel));
    CHECK(channel.applied_dispatch_fence < seal.dispatch_fence);

    std::size_t service_calls = 0U;
    while (!runtime->CanPollDispatch(0U)) {
        CHECK(runtime->FlushDue(0U, 0U));
        CHECK(++service_calls < 128U);
    }
    CHECK(service_calls != 0U);
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShanghai, 7U, &channel));
    CHECK(channel.applied_dispatch_fence == seal.dispatch_fence);
    CHECK(runtime->DrainAll());
    CHECK(runtime->stats().workers.phase_normalization_slices > 1U);

    CanonicalTick last{};
    for (std::uint64_t sequence = 12U; sequence <= 14U; ++sequence) {
        last = ShanghaiAdd(sequence, 8'100U + sequence,
                           static_cast<std::int64_t>(200U + sequence));
        CHECK(runtime->AppendDispatch(
            0U, RuntimeOrdered(last, 0U, 1U)));
    }
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(last, 0U, 15U, 15U, 2U)));
    CHECK(!runtime->CanPollDispatch(0U));

    const CanonicalTick overtaking = ShanghaiAdd(16U, 8'116U, 216);
    CHECK(!runtime->AppendDispatch(
        0U, RuntimeOrdered(overtaking, 0U, 2U)));
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("input is fenced") !=
          std::string::npos);
}

void TestRuntimeDefersControlBehindEndExpansionFence() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 32U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000'000);
    config.worker.end_slice_max_candidates = 1U;
    config.worker.end_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    config.worker.repair_slice_max_order_uses = 64U;
    config.worker.repair_slice_max_cpu_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventRuntime> runtime = EventRuntime::Create(
        config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick opening =
        ShanghaiStatus(1U, 9'100U, TradingPhase::kContinuous);
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(opening, 0U)));
    CHECK(runtime->Flush(0U));
    CHECK(runtime->AppendDispatch(
        0U, RuntimeGapOpen(opening, 0U, 2U, 2U, 1U)));

    const std::array<CanonicalTick, 5U> suffix{
        ShanghaiAdd(3U, 9'103U, 100),
        ShanghaiAdd(4U, 9'104U, 200),
        ShanghaiAdd(5U, 9'105U, 300),
        ShanghaiAdd(6U, 9'106U, 400),
        ShanghaiStatus(7U, 9'107U, TradingPhase::kEnded)};
    for (const CanonicalTick& tick : suffix) {
        CHECK(runtime->AppendDispatch(
            0U, RuntimeOrdered(tick, 0U, 1U)));
    }

    const TickDispatch seal = RuntimeChannelSeal(
        suffix.back(), 0U, 3U, 1U);
    CHECK(runtime->AppendDispatch(0U, seal));
    CHECK(!runtime->CanPollDispatch(0U));
    CHECK(runtime->worker(0U)->projection_input_fenced());
    CHECK(runtime->stats().workers.end_candidates_processed == 2U);

    EventChannelState channel{};
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShanghai, 7U, &channel));
    CHECK(channel.applied_dispatch_fence < seal.dispatch_fence);

    std::size_t service_calls = 0U;
    while (!runtime->CanPollDispatch(0U)) {
        CHECK(runtime->FlushDue(0U, 0U));
        CHECK(++service_calls < 128U);
    }
    CHECK(service_calls != 0U);
    CHECK(!runtime->worker(0U)->projection_input_fenced());
    CHECK(runtime->worker(0U)->CopyChannelState(
        Market::kShanghai, 7U, &channel));
    CHECK(channel.applied_dispatch_fence == seal.dispatch_fence);
    CHECK(runtime->stats().workers.end_candidates_processed == 4U);

    const CanonicalTick following = ShanghaiAdd(8U, 9'108U, 500);
    CHECK(runtime->AppendDispatch(
        0U, RuntimeOrdered(following, 0U, 1U)));
    CHECK(runtime->Flush(0U));
    CHECK(runtime->DrainAll());
    CHECK(runtime->healthy());
}

void TestRuntimeDrainAndEpochIsolation() {
    RecordingSink sink;
    const CanonicalTick tick = ShenzhenAdd(101U, 171U);

    EventRuntimeConfig config = RuntimeConfig();
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    CHECK(runtime->AppendDispatch(0U, RuntimeReject(tick, 0U)));
    CHECK(runtime->DrainAll());
    CHECK(runtime->healthy());

    runtime.reset();
    config = RuntimeConfig();
    config.feed_session_epoch = kFeedSessionEpoch + 1U;
    config.worker.feed_session_epoch = config.feed_session_epoch;
    runtime = EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    TickDispatch next_epoch = RuntimeReject(tick, 0U);
    next_epoch.feed_session_epoch = config.feed_session_epoch;
    CHECK(runtime->AppendDispatch(0U, next_epoch));
    CHECK(runtime->DrainAll());
}

void TestRuntimeFailsClosedWhenRevisionSinkRejects() {
    RecordingSink sink;
    sink.accept = false;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 1U;
    config.micro_batch_max_delay_ns = 1U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    const CanonicalTick tick = ShenzhenAdd(101U, 90U);
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(tick, 0U)));
    CHECK(!runtime->DrainAll());
    CHECK(!runtime->healthy());
    CHECK(runtime->fatal_error().find("revision sink rejected") !=
          std::string::npos);
}

void TestRejectedOccurrenceNeverReplacesFirstWinner() {
    RecordingSink sink;
    EventRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 1U;
    config.micro_batch_max_delay_ns = 1U;
    std::string error;
    std::unique_ptr<EventRuntime> runtime =
        EventRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);

    const CanonicalTick retained = ShenzhenAdd(101U, 180U, 100);
    CHECK(runtime->AppendDispatch(0U, RuntimeOrdered(retained, 0U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);

    const CanonicalTick conflicting = ShenzhenAdd(101U, 181U, 999);
    CHECK(runtime->AppendDispatch(0U, RuntimeReject(conflicting, 0U)));
    CHECK(runtime->DrainAll());
    CHECK(sink.batches.size() == 1U);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(runtime->worker(0U)->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 101U}, &bundle));
    const EventPayload& projected = FindPayload(
        bundle, EventKind::kShenzhenOrderRevision, 101);
    CHECK(projected.order.original_quantity == 100);
    const EventRuntimeStats stats = runtime->stats();
    CHECK(stats.rejected_dispositions_received == 1U);
    CHECK(stats.source_conflicts == 0U);
    CHECK(stats.workers.facts_journaled == 1U);
}

void TestDuplicateAndConflictClassification() {
    RecordingSink sink;
    const std::filesystem::path path = TestJournalPath("duplicate");
    const auto fact_journal = MakeTestJournal(path);
    EventWorkerConfig config = Config();
    config.fact_journal = fact_journal;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);
    const CanonicalTick add = ShenzhenAdd(1U, 40U);
    EventInput input = Ordered(add);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), add);
    const l2flow::journal::FactJournalStats new_stats =
        fact_journal->stats();
    CHECK(new_stats.records == 1U);
    CHECK(new_stats.read_calls == 0U);
    const std::vector<l2flow::journal::AdmitResult> kline_fanout =
        fact_journal->AdmitBatch(
            l2flow::journal::FactConsumer::kKLine,
            std::span<const CanonicalTick>(&add, 1U));
    CHECK(kline_fanout.size() == 1U);
    CHECK(kline_fanout[0U].code == l2flow::journal::AdmitCode::kNew);
    CHECK(fact_journal->stats().records == 1U);

    CanonicalTick duplicate = add;
    duplicate.common.ingress_sequence = 41U;
    EventInput duplicate_input = Ordered(duplicate);
    fact_journal->ClearHotCache();
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&duplicate_input, 1U)).code ==
          EventApplyCode::kDuplicateOnly);
    CHECK(fact_journal->stats().read_calls == new_stats.read_calls + 1U);
    Ack(worker.get(), duplicate);
    CHECK(sink.batches.size() == 1U);

    CanonicalTick conflict = duplicate;
    conflict.common.ingress_sequence = 42U;
    conflict.quantity.raw = 101;
    EventInput conflict_input = Ordered(conflict);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&conflict_input, 1U)).code ==
          EventApplyCode::kSourceConflict);
    Ack(worker.get(), conflict);
    CHECK(worker->healthy());
    CHECK(sink.batches.size() == 1U);
}

void TestSharedJournalUsesAuthoritativeFirstWinner() {
    RecordingSink sink;
    const std::filesystem::path path = TestJournalPath("shared-winner");
    const auto fact_journal = MakeTestJournal(path);

    const CanonicalTick winner = ShenzhenTrade(11U, 50U, 0, 0);
    const std::vector<l2flow::journal::AdmitResult> kline_admission =
        fact_journal->AdmitBatch(
            l2flow::journal::FactConsumer::kKLine,
            std::span<const CanonicalTick>(&winner, 1U));
    CHECK(kline_admission.size() == 1U);
    CHECK(kline_admission[0U].code ==
          l2flow::journal::AdmitCode::kNew);

    EventWorkerConfig config = Config();
    config.fact_journal = fact_journal;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    CanonicalTick event_occurrence = winner;
    event_occurrence.common.ingress_sequence = 51U;
    event_occurrence.common.vendor_sequence_id = 10'051U;
    event_occurrence.common.receive_monotonic_ns = 20'051U;
    EventInput input = Ordered(event_occurrence);
    const EventApplyResult applied = worker->ApplyBatch(
        std::span<const EventInput>(&input, 1U));
    CHECK(applied.code == EventApplyCode::kApplied);
    CHECK(applied.facts_inserted == 1U);
    Ack(worker.get(), event_occurrence);

    std::vector<std::pair<EventKey, EventPayload>> bundle;
    CHECK(worker->CopyBundle(
        FactKey{20260807U, Market::kShenzhen, 7U, 11U}, &bundle));
    const EventPayload& projected = FindPayload(
        bundle, EventKind::kShenzhenTrade);
    CHECK(projected.source_anchor.ingress_sequence ==
          winner.common.ingress_sequence);
    const l2flow::journal::FactJournalStats stats = fact_journal->stats();
    CHECK(stats.records == 1U);
    CHECK(stats.consumer_new == 2U);
    CHECK(stats.read_calls == 0U);
}

void TestWorkerRequiresExplicitFactJournal() {
    RecordingSink sink;
    EventWorkerConfig config = Config();
    config.fact_journal.reset();
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker == nullptr);
    CHECK(error.find("FactJournal is null") != std::string::npos);
}

void TestJournalCorruptionFailsClosed() {
    RecordingSink sink;
    const std::filesystem::path path = TestJournalPath("corruption");
    const auto fact_journal = MakeTestJournal(path);
    EventWorkerConfig config = Config();
    config.fact_journal = fact_journal;
    std::string error;
    std::unique_ptr<EventWorker> worker =
        EventWorker::Create(config, &sink, &error);
    CHECK(worker != nullptr);

    const CanonicalTick add = ShenzhenAdd(21U, 60U);
    EventInput input = Ordered(add);
    CHECK(worker->ApplyBatch(
              std::span<const EventInput>(&input, 1U)).code ==
          EventApplyCode::kApplied);
    Ack(worker.get(), add);
    CHECK(fact_journal->Flush());
    fact_journal->ClearHotCache();

    const int descriptor = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    CHECK(descriptor >= 0);
    const off_t payload_offset = static_cast<off_t>(
        l2flow::journal::kFactJournalFileHeaderBytes +
        l2flow::journal::kFactJournalRecordHeaderBytes + 7U);
    std::uint8_t octet = 0U;
    CHECK(::pread(descriptor, &octet, sizeof(octet), payload_offset) ==
          static_cast<ssize_t>(sizeof(octet)));
    octet ^= UINT8_C(0x5a);
    CHECK(::pwrite(descriptor, &octet, sizeof(octet), payload_offset) ==
          static_cast<ssize_t>(sizeof(octet)));
    CHECK(::close(descriptor) == 0);

    CanonicalTick duplicate = add;
    duplicate.common.ingress_sequence = 61U;
    EventInput duplicate_input = Ordered(duplicate);
    const EventApplyResult failed = worker->ApplyBatch(
        std::span<const EventInput>(&duplicate_input, 1U));
    CHECK(failed.code == EventApplyCode::kFailed);
    CHECK(!worker->healthy());
    CHECK(!fact_journal->healthy());
    CHECK(worker->fatal_error().find("FactJournal") != std::string::npos);
}

}  // namespace

int main() {
    TestLateAddRepairsOnlyReferencedChainAndWaitsForRawAck();
    TestJournalFirstSameBatchDoesNotPublishUnknownIntermediate();
    TestAckBeforeFactAndOutOfOrderAckPreserveCommitFifo();
    TestUnresolvedLateCancelConvergesBeforeLaterTrade();
    TestShanghaiEndAddsOnlyLateOrdersFinalizeFragment();
    TestShanghaiBarrierBulkRolesStayOrdered();
    TestShanghaiEndSourceOnlyCutRetainsBarrierForLateOrder();
    TestShanghaiEndDoesNotFinalizeOrderFirstSeenAfterEnd();
    TestShanghaiTerminalBeforeEndTombstonesOldFinalize();
    TestLateShanghaiStatusRepairsOnlyUntilNextStatus();
    TestSlicedShanghaiPhaseCutFencesAckAndMatchesReference();
    TestInvalidShanghaiStatusKeepsSourcePhaseWithoutBecomingAnchor();
    TestPhaseSliceConfigRejectsZeroBudgets();
    TestSlicedRepairIsPrivateAndMatchesUnslicedProjection();
    TestUnrelatedLiveOrderPublishesDuringRepair();
    TestNewEarlierFactRestartsOnlyDirtyOrder();
    TestLargeSuffixRepairDoesNotCopyHistoryAndMatchesReference();
    TestNewUseExtendsActiveRepairWorklist();
    TestMixedBatchPublishesOnlyDisjointComponent();
    TestShenzhenChannelZeroProjectsAndSettlesAck();
    TestRetentionFloorAndStrictSealBoundary();
    TestChannelSealRejectsStaleGeneration();
    TestClaimedFillMayAdvanceRetentionBeyondSequence();
    TestMultipleChannelEvictionTargetsSurviveSmallSlices();
    TestOrderBaselineAndPhaseAnchorPreserveHiddenState();
    TestOrderHistoryByteCapAndCompactionAccounting();
    TestShanghaiFullPrivateBaselineMatchesUnsealedReference();
    TestShanghaiEndIsSlicedAndCapacityBounded();
    TestShanghaiEndExpansionRejectsDirectOvertake();
    TestShanghaiFinalFastEndSkipsFinalizedCarryOrders();
    TestShanghaiEndExpansionMergesPreAttachedBarrier();
    TestByteAndHotFactCapsFailClosed();
    TestProjectionCommitScratchExactBoundary();
    TestPendingCommitDequeAccountingReturnsToBaseline();
    TestPhaseAnchorAccountingPlatformsWithoutHotFacts();
    TestHotIndexSingleElementRangesAndChannelOuterAccounting();
    TestPhaseDequeBlockGrowthIsPreflightedAndCompactsToAnchor();
    TestEndBarrierIndexIsPreflightedAndReleased();
    TestEvictionByteSliceIncludesCarryOrderRetirement();
    TestRuntimeRoutesOwnersAndJoinsRawAcks();
    TestRuntimeDrainAllFlushesFinalPartialBatch();
    TestPersistenceGroupingIsIndependentFromControlFences();
    TestOwnerPersistenceAggregatorClosesAtBatchBound();
    TestOwnerPersistenceAggregatorClosesAtPendingCapacity();
    TestRuntimeAckDrainIsBoundedAndSourceTimeDoesNotDriveTimer();
    TestRuntimeChannelSealMailboxCoalescesLatestWatermark();
    TestRuntimeChannelSealMailboxPreservesChannelOrder();
    TestRuntimeChannelSealMailboxRejectsProtocolMismatch();
    TestRuntimeDefersPoppedControlBehindPhaseFence();
    TestRuntimeDefersControlBehindEndExpansionFence();
    TestRuntimeDrainAndEpochIsolation();
    TestRuntimeFailsClosedWhenRevisionSinkRejects();


    TestRejectedOccurrenceNeverReplacesFirstWinner();

    TestDuplicateAndConflictClassification();
    TestSharedJournalUsesAuthoritativeFirstWinner();
    TestWorkerRequiresExplicitFactJournal();
    TestJournalCorruptionFailsClosed();
    std::cout << "all Event worker tests passed\n";
    return 0;
}
