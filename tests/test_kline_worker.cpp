#include "l2flow/kline/runtime.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace {

using namespace l2flow::ingest;
using namespace l2flow::kline;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

constexpr std::uint64_t kFeedSessionEpoch = 17U;

std::shared_ptr<l2flow::journal::CanonicalFactJournal> TestJournal() {
    static std::atomic<std::uint64_t> next_id{0U};
    const std::uint64_t id = next_id.fetch_add(1U, std::memory_order_relaxed);
    const std::uint64_t clock = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const std::filesystem::path path =
        std::filesystem::temp_directory_path() /
        ("l2flow-kline-worker-" + std::to_string(clock) + "-" +
         std::to_string(id) + ".fjn");
    std::string error;
    std::unique_ptr<l2flow::journal::CanonicalFactJournal> journal =
        l2flow::journal::CanonicalFactJournal::Create(
            l2flow::journal::FactJournalConfig{20260808U, path, 1'024U},
            &error);
    CHECK(journal != nullptr);
    return std::shared_ptr<l2flow::journal::CanonicalFactJournal>(
        journal.release(), [path](auto* value) {
            delete value;
            std::error_code ignored;
            static_cast<void>(std::filesystem::remove(path, ignored));
        });
}

class RecordingSink final : public KLineRevisionSink {
public:
    [[nodiscard]] bool AppendRevisionBatch(
        std::shared_ptr<const KLineRevisionBatch> batch) noexcept override {
        try {
            batches.push_back(std::move(batch));
            return accept;
        } catch (...) {
            return false;
        }
    }

    bool accept = true;
    std::vector<std::shared_ptr<const KLineRevisionBatch>> batches;
};

void SetIdentity(CanonicalCommon* common, Market market) {
    common->identity.market = market;
    const std::string security = market == Market::kShanghai
        ? "600000"
        : "000001";
    common->identity.security_id_size =
        static_cast<std::uint8_t>(security.size());
    std::copy(security.begin(), security.end(),
              reinterpret_cast<char*>(common->identity.security_id.data()));
    if (market == Market::kShenzhen) {
        common->identity.security_id_source_size = 2U;
        common->identity.security_id_source[0U] = std::byte{'S'};
        common->identity.security_id_source[1U] = std::byte{'Z'};
    }
}

CanonicalTick Trade(std::uint64_t sequence,
                    std::uint64_t ingress,
                    std::uint64_t exchange_time_ns,
                    std::uint64_t receive_monotonic_ns,
                    std::int64_t price_p6,
                    std::int64_t quantity,
                    std::uint32_t channel = 7U,
                    Market market = Market::kShenzhen) {
    CanonicalTick tick{};
    tick.common.trade_date = 20260808U;
    tick.common.instrument_id = 1U;
    tick.common.instrument_ordinal = 0U;
    tick.common.channel = channel;
    tick.common.native_sequence = sequence;
    tick.common.ingress_sequence = ingress;
    tick.common.vendor_sequence_id = 10'000U + ingress;
    tick.common.receive_monotonic_ns = receive_monotonic_ns;
    tick.common.exchange_time_ns_from_midnight = exchange_time_ns;
    tick.common.exchange_time_raw = 93'000'000U;
    tick.common.exchange_time_valid = true;
    tick.common.kind = market == Market::kShanghai
        ? CanonicalKind::kShanghaiTick
        : CanonicalKind::kShenzhenTransaction;
    tick.common.message_key = market == Market::kShanghai
        ? MessageKey{4U, 101U, 24U}
        : MessageKey{6U, 101U, 36U};
    SetIdentity(&tick.common, market);
    tick.action = TickAction::kTrade;
    tick.price = {price_p6 / 100, price_p6, 4U, true, true};
    tick.quantity = {quantity, 0U, true};
    tick.validity = kTickPriceValid | kTickQuantityValid |
                    kTickExchangeTimeValid | kTickChannelHistoryValid;
    return tick;
}

KLineWorkerConfig Config(std::vector<std::uint32_t> intervals = {5U}) {
    KLineWorkerConfig config{};
    config.trade_date = 20260808U;
    config.owner = 0U;
    config.owner_count = 1U;
    config.revision_epoch = 11U;
    config.feed_session_epoch = kFeedSessionEpoch;
    config.calculation_run_id.bytes[0U] = std::byte{1U};
    config.interval_seconds = std::move(intervals);
    config.fact_journal = TestJournal();
    config.maximum_bars = 1'024U;
    config.maximum_pending_commits = 64U;
    return config;
}

KLineInput OrderedInput(const CanonicalTick& tick) {
    KLineInput input{};
    input.tick = tick;
    input.admission.feed_session_epoch = kFeedSessionEpoch;
    input.admission.expected_sequence = tick.common.native_sequence;
    input.admission.admission_floor = 1U;
    input.admission.retention_floor = tick.common.native_sequence + 1U;
    input.admission.dispatch_fence = tick.common.ingress_sequence;
    input.admission.sequence_class = KLineSequenceClass::kOrdered;
    return input;
}

KLineInput HoleFillInput(const CanonicalTick& tick,
                         std::uint64_t expected_sequence,
                         std::uint64_t admission_floor,
                         std::uint64_t generation) {
    KLineInput input{};
    input.tick = tick;
    input.admission.feed_session_epoch = kFeedSessionEpoch;
    input.admission.expected_sequence = expected_sequence;
    input.admission.admission_floor = admission_floor;
    input.admission.retention_floor = admission_floor;
    input.admission.generation = generation;
    input.admission.dispatch_fence = tick.common.ingress_sequence;
    input.admission.sequence_class = KLineSequenceClass::kHoleFill;
    return input;
}

TickDispatch OrderedDispatch(const CanonicalTick& tick) {
    TickDispatch dispatch{};
    dispatch.tick = tick;
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.expected_sequence = tick.common.native_sequence;
    dispatch.admission_floor = 1U;
    dispatch.evict_before = tick.common.native_sequence + 1U;
    dispatch.dispatch_fence = tick.common.ingress_sequence;
    dispatch.channel = tick.common.channel;
    dispatch.owner = tick.common.instrument_ordinal;
    dispatch.market = tick.common.identity.market;
    dispatch.kind = TickDispatchKind::kProjectOrdered;
    dispatch.catalog_match = true;
    return dispatch;
}

TickDispatch HoleFillDispatch(const CanonicalTick& tick,
                              std::uint64_t expected_sequence,
                              std::uint64_t admission_floor,
                              std::uint64_t generation) {
    TickDispatch dispatch = OrderedDispatch(tick);
    dispatch.expected_sequence = expected_sequence;
    dispatch.admission_floor = admission_floor;
    dispatch.evict_before = admission_floor;
    dispatch.generation = generation;
    dispatch.kind = TickDispatchKind::kProjectHoleFill;
    return dispatch;
}

TickDispatch RejectDispatch(const CanonicalTick& tick) {
    TickDispatch dispatch = OrderedDispatch(tick);
    dispatch.kind = TickDispatchKind::kRejectLateFact;
    return dispatch;
}

TickDispatch GapOpenDispatch(std::uint64_t fence) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.generation = 1U;
    dispatch.dispatch_fence = fence;
    dispatch.first_missing = 10U;
    dispatch.last_missing = 11U;
    dispatch.channel = 7U;
    dispatch.owner = 0U;
    dispatch.market = Market::kShanghai;
    dispatch.kind = TickDispatchKind::kGapOpen;
    return dispatch;
}

TickDispatch ChannelSealDispatch(std::uint64_t fence) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = kFeedSessionEpoch;
    dispatch.generation = 1U;
    dispatch.dispatch_fence = fence;
    dispatch.evict_before = 12U;
    dispatch.channel = 7U;
    dispatch.owner = 0U;
    dispatch.market = Market::kShanghai;
    dispatch.kind = TickDispatchKind::kChannelSeal;
    return dispatch;
}

KLineRuntimeConfig RuntimeConfig() {
    KLineRuntimeConfig config{};
    config.worker = Config({1U});
    config.feed_session_epoch = kFeedSessionEpoch;
    config.micro_batch_rows = 1U;
    return config;
}

void Ack(KLineWorker* worker, std::span<const CanonicalTick> ticks) {
    static_cast<void>(ticks);
    CHECK(worker->DrainDurableCommits());
}

void TestSdkExchangeTimeDefinesWindowAndOhlc() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker =
        KLineWorker::Create(Config(), &sink, &error);
    CHECK(worker != nullptr);

    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);
    // Local receive order says 101 arrived first. SDK exchange time says 100 is
    // the opening trade and is the only clock allowed to define the bar.
    const CanonicalTick later_exchange = Trade(
        101U, 1U, base + UINT64_C(4'000'000'000), 1'000U,
        12'000'000, 20);
    const CanonicalTick earlier_exchange = Trade(
        100U, 2U, base + UINT64_C(1'000'000'000), 9'000U,
        10'000'000, 10);
    const std::array<KLineInput, 2U> inputs{
        OrderedInput(later_exchange), OrderedInput(earlier_exchange)};
    const KLineApplyResult result = worker->ApplyBatch(inputs);
    CHECK(result.code == KLineApplyCode::kApplied);
    CHECK(sink.batches.size() == 1U);
    CHECK(sink.batches[0U]->revisions.size() == 1U);

    KLinePayload bar{};
    CHECK(worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 5U, base}, &bar));
    CHECK(bar.open_price_p6 == 10'000'000);
    CHECK(bar.close_price_p6 == 12'000'000);
    CHECK(bar.high_price_p6 == 12'000'000);
    CHECK(bar.low_price_p6 == 10'000'000);
    CHECK(bar.volume == 30);
    CHECK(bar.notional_p6 == 340'000'000);
    CHECK(bar.trade_count == 2U);
    CHECK(bar.first_trade.native_sequence == 100U);
    CHECK(bar.last_trade.native_sequence == 101U);
}

void TestLateBackfillRevisesHistoricalSecondWindow() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker =
        KLineWorker::Create(Config({1U}), &sink, &error);
    CHECK(worker != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);

    const CanonicalTick live = Trade(
        102U, 10U, base + 800'000'000U, 100U, 11'000'000, 5);
    const KLineInput live_input = OrderedInput(live);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&live_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&live, 1U));

    const CanonicalTick next_window = Trade(
        103U, 11U, base + UINT64_C(1'100'000'000), 101U,
        13'000'000, 7);
    const KLineInput next_input = OrderedInput(next_window);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&next_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&next_window, 1U));

    const CanonicalTick recovered = Trade(
        101U, 12U, base + 100'000'000U, 9'999U, 9'000'000, 3);
    const KLineInput recovered_input =
        HoleFillInput(recovered, 104U, 100U, 1U);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&recovered_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&recovered, 1U));

    CHECK(sink.batches.size() == 3U);
    const KLineRevision& repair = sink.batches.back()->revisions.front();
    CHECK(repair.operation == RevisionOperation::kUpdate);
    CHECK(repair.reason == RevisionReason::kHoleFill);
    CHECK(repair.supersedes_revision_id_valid);
    KLinePayload bar{};
    CHECK(worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 1U, base}, &bar));
    CHECK(bar.open_price_p6 == 9'000'000);
    CHECK(bar.close_price_p6 == 11'000'000);
    CHECK(bar.volume == 8);
    CHECK(bar.has_hole_fill);
}

void TestInvalidExchangeTimeNeverFallsBackToLocalClock() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker =
        KLineWorker::Create(Config({1U}), &sink, &error);
    CHECK(worker != nullptr);
    CanonicalTick invalid = Trade(
        1U, 20U, 0U, UINT64_C(34'200'500'000'000), 10'000'000, 10);
    invalid.common.exchange_time_valid = false;
    invalid.common.vendor_local_time_raw = 93'000'500U;
    invalid.common.vendor_local_time_ns_from_midnight =
        UINT64_C(34'200'500'000'000);
    invalid.common.vendor_local_time_valid = true;
    CanonicalTick missing_validity = Trade(
        2U, 21U, UINT64_C(34'200'600'000'000),
        UINT64_C(34'200'600'000'000), 10'000'000, 10);
    missing_validity.validity &= ~kTickExchangeTimeValid;
    CanonicalTick out_of_day = Trade(
        3U, 22U, kNanosecondsPerDay,
        UINT64_C(34'200'700'000'000), 10'000'000, 10);
    const std::array<KLineInput, 3U> inputs{
        OrderedInput(invalid), OrderedInput(missing_validity),
        OrderedInput(out_of_day)};
    const KLineApplyResult result = worker->ApplyBatch(inputs);
    CHECK(result.code == KLineApplyCode::kInvalidInput);
    CHECK(result.trades_inserted == 0U);
    Ack(worker.get(),
        std::array{invalid, missing_validity, out_of_day});
    CHECK(sink.batches.empty());
    CHECK(worker->stats().invalid_trade_exchange_times == 3U);
    KLinePayload ignored{};
    CHECK(!worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 1U,
                 UINT64_C(34'200'000'000'000)},
        &ignored));
}

void TestHalfOpenWindowAndEqualTimestampTieBreak() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker =
        KLineWorker::Create(Config({5U}), &sink, &error);
    CHECK(worker != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);

    // Arrival order is deliberately opposite to the deterministic channel
    // tie-break. A trade exactly at +5 s belongs to the following window.
    const CanonicalTick channel_nine = Trade(
        1U, 23U, base + 1'000'000'000U, 10U, 12'000'000, 2, 9U);
    const CanonicalTick channel_two = Trade(
        1U, 24U, base + 1'000'000'000U, 20U, 10'000'000, 3, 2U);
    const CanonicalTick boundary = Trade(
        2U, 25U, base + 5'000'000'000U, 30U, 15'000'000, 4, 7U);
    const std::array<KLineInput, 3U> inputs{
        OrderedInput(channel_nine), OrderedInput(channel_two),
        OrderedInput(boundary)};
    const KLineApplyResult result = worker->ApplyBatch(inputs);
    CHECK(result.code == KLineApplyCode::kApplied);
    CHECK(result.changed_bars == 2U);
    Ack(worker.get(),
        std::array{channel_nine, channel_two, boundary});

    KLinePayload first{};
    CHECK(worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 5U, base}, &first));
    CHECK(first.open_price_p6 == 10'000'000);
    CHECK(first.close_price_p6 == 12'000'000);
    CHECK(first.first_trade.channel == 2U);
    CHECK(first.last_trade.channel == 9U);
    CHECK(first.trade_count == 2U);

    KLinePayload second{};
    CHECK(worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 5U,
                 base + 5'000'000'000U},
        &second));
    CHECK(second.open_price_p6 == 15'000'000);
    CHECK(second.close_price_p6 == 15'000'000);
    CHECK(second.trade_count == 1U);
}

void TestDuplicateConflictAndMultipleIntervals() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker =
        KLineWorker::Create(Config({1U, 5U}), &sink, &error);
    CHECK(worker != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);
    const CanonicalTick trade = Trade(
        1U, 30U, base + 100'000'000U, 1U, 10'000'000, 10, 0U);
    const KLineInput first = OrderedInput(trade);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&first, 1U)).revisions_created == 2U);
    Ack(worker.get(), std::span<const CanonicalTick>(&trade, 1U));

    CanonicalTick duplicate = trade;
    duplicate.common.ingress_sequence = 31U;
    duplicate.common.receive_monotonic_ns = 2U;
    const KLineInput duplicate_input = OrderedInput(duplicate);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&duplicate_input, 1U)).code ==
          KLineApplyCode::kDuplicateOnly);
    Ack(worker.get(), std::span<const CanonicalTick>(&duplicate, 1U));
    CHECK(sink.batches.size() == 1U);

    CanonicalTick conflict = trade;
    conflict.common.ingress_sequence = 32U;
    conflict.price.raw = 110'000;
    conflict.price.p6 = 11'000'000;
    const KLineInput conflict_input = OrderedInput(conflict);
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&conflict_input, 1U)).code ==
          KLineApplyCode::kSourceConflict);
    Ack(worker.get(), std::span<const CanonicalTick>(&conflict, 1U));
    CHECK(sink.batches.size() == 1U);
}

void TestRuntimeProjectJoinBothArrivalOrders() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineRuntime> runtime =
        KLineRuntime::Create(RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);

    const CanonicalTick ack_first = Trade(
        1U, 40U, base + 1U, 100U, 10'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, OrderedDispatch(ack_first)));
    CHECK(runtime->FlushAll());
    CHECK(sink.batches.size() == 1U);

    const CanonicalTick disposition_first = Trade(
        2U, 41U, base + 2U, 101U, 11'000'000, 2);
    CHECK(runtime->AppendDispatch(
        0U, HoleFillDispatch(disposition_first, 3U, 2U, 1U)));
    CHECK(runtime->FlushAll());
    CHECK(sink.batches.size() == 2U);

    const KLineRuntimeStats stats = runtime->stats();
    CHECK(stats.ordered_dispositions_received == 1U);
    CHECK(stats.hole_fill_dispositions_received == 1U);
    CHECK(runtime->DrainAll());
}

void TestRuntimeRejectJoinBothArrivalOrders() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineRuntime> runtime =
        KLineRuntime::Create(RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);

    const CanonicalTick disposition_first = Trade(
        10U, 50U, base + 1U, 100U, 10'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, RejectDispatch(disposition_first)));
    CHECK(runtime->Flush(0U));

    const CanonicalTick ack_first = Trade(
        11U, 51U, base + 2U, 101U, 10'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, RejectDispatch(ack_first)));

    const KLineRuntimeStats stats = runtime->stats();
    CHECK(stats.rejected_dispositions_received == 2U);
    CHECK(stats.workers.facts_journaled == 0U);
    CHECK(sink.batches.empty());
    CHECK(runtime->DrainAll());
}

void TestRuntimeRejectsInvalidEpochOwnerAndFence() {
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);
    const CanonicalTick trade = Trade(
        40U, 80U, base + 1U, 100U, 10'000'000, 1);
    std::string error;

    RecordingSink epoch_sink;
    std::unique_ptr<KLineRuntime> epoch =
        KLineRuntime::Create(RuntimeConfig(), &epoch_sink, &error);
    CHECK(epoch != nullptr);
    TickDispatch wrong_epoch = OrderedDispatch(trade);
    wrong_epoch.feed_session_epoch = kFeedSessionEpoch + 1U;
    CHECK(!epoch->AppendDispatch(0U, wrong_epoch));
    CHECK(!epoch->healthy());

    RecordingSink owner_sink;
    std::unique_ptr<KLineRuntime> owner =
        KLineRuntime::Create(RuntimeConfig(), &owner_sink, &error);
    CHECK(owner != nullptr);
    TickDispatch wrong_owner = OrderedDispatch(trade);
    wrong_owner.owner = 1U;
    CHECK(!owner->AppendDispatch(0U, wrong_owner));
    CHECK(!owner->healthy());

    RecordingSink fence_sink;
    std::unique_ptr<KLineRuntime> fence =
        KLineRuntime::Create(RuntimeConfig(), &fence_sink, &error);
    CHECK(fence != nullptr);
    TickDispatch missing_fence = OrderedDispatch(trade);
    missing_fence.dispatch_fence = 0U;
    CHECK(!fence->AppendDispatch(0U, missing_fence));
    CHECK(!fence->healthy());
}

void TestRuntimeGapAndSealControlsAreNoOps() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineRuntime> runtime =
        KLineRuntime::Create(RuntimeConfig(), &sink, &error);
    CHECK(runtime != nullptr);

    CHECK(runtime->AppendDispatch(0U, GapOpenDispatch(1U)));
    CHECK(runtime->AppendDispatch(0U, ChannelSealDispatch(2U)));
    CHECK(runtime->FlushAll());
    CHECK(runtime->DrainAll());

    const KLineRuntimeStats stats = runtime->stats();
    CHECK(stats.gap_open_controls_received == 1U);
    CHECK(stats.channel_seal_controls_received == 1U);
    CHECK(stats.micro_batches_applied == 0U);
    CHECK(stats.workers.facts_journaled == 0U);
    CHECK(sink.batches.empty());
}

void TestAdmissionTokenHalfOpenBoundary() {
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);

    RecordingSink accepted_sink;
    std::string error;
    std::unique_ptr<KLineWorker> accepted = KLineWorker::Create(
        Config({1U}), &accepted_sink, &error);
    CHECK(accepted != nullptr);
    const CanonicalTick at_floor = Trade(
        100U, 41U, base + 1U, 100U, 10'000'000, 1);
    const KLineInput accepted_input =
        HoleFillInput(at_floor, 101U, 100U, 1U);
    CHECK(accepted->ApplyBatch(
              std::span<const KLineInput>(&accepted_input, 1U)).code ==
          KLineApplyCode::kApplied);

    RecordingSink expired_sink;
    std::unique_ptr<KLineWorker> expired = KLineWorker::Create(
        Config({1U}), &expired_sink, &error);
    CHECK(expired != nullptr);
    const CanonicalTick below_floor = Trade(
        99U, 42U, base + 2U, 101U, 10'000'000, 1);
    const KLineInput expired_input =
        HoleFillInput(below_floor, 101U, 100U, 1U);
    CHECK(expired->ApplyBatch(
              std::span<const KLineInput>(&expired_input, 1U)).code ==
          KLineApplyCode::kFailed);
    CHECK(!expired->healthy());
    CHECK(expired->fatal_error().find("admission token") !=
          std::string::npos);
}

void TestChannelGlobalFirstWinnerAcrossInstrumentOwners() {
    RecordingSink owner_zero_sink;
    RecordingSink owner_one_sink;
    const auto journal = TestJournal();
    KLineWorkerConfig owner_zero_config = Config({1U});
    owner_zero_config.owner_count = 2U;
    owner_zero_config.owner = 0U;
    owner_zero_config.fact_journal = journal;
    KLineWorkerConfig owner_one_config = owner_zero_config;
    owner_one_config.owner = 1U;

    std::string error;
    std::unique_ptr<KLineWorker> owner_zero = KLineWorker::Create(
        owner_zero_config, &owner_zero_sink, &error);
    CHECK(owner_zero != nullptr);
    std::unique_ptr<KLineWorker> owner_one = KLineWorker::Create(
        owner_one_config, &owner_one_sink, &error);
    CHECK(owner_one != nullptr);

    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);
    CanonicalTick winner = Trade(
        77U, 50U, base + 1U, 100U, 10'000'000, 1);
    winner.common.instrument_ordinal = 0U;
    const KLineInput winner_input = OrderedInput(winner);
    CHECK(owner_zero->ApplyBatch(
              std::span<const KLineInput>(&winner_input, 1U)).code ==
          KLineApplyCode::kApplied);

    CanonicalTick conflicting_identity = winner;
    conflicting_identity.common.ingress_sequence = 51U;
    conflicting_identity.common.instrument_id = 2U;
    conflicting_identity.common.instrument_ordinal = 1U;
    conflicting_identity.common.identity.security_id[5U] = std::byte{'2'};
    const KLineInput conflict_input = OrderedInput(conflicting_identity);
    CHECK(owner_one->ApplyBatch(
              std::span<const KLineInput>(&conflict_input, 1U)).code ==
          KLineApplyCode::kSourceConflict);
    CHECK(owner_one->stats().facts_journaled == 0U);
    CHECK(journal->stats().records == 1U);
}

void TestWorkerRejectsUnhealthyFactJournal() {
    RecordingSink sink;
    const auto journal = TestJournal();
    CanonicalTick ignored{};
    CHECK(!journal->Read(
        l2flow::journal::FactHandle{
            l2flow::journal::kFactJournalFileHeaderBytes},
        &ignored));
    CHECK(!journal->healthy());

    KLineWorkerConfig config = Config({1U});
    config.fact_journal = journal;
    std::string error;
    CHECK(KLineWorker::Create(config, &sink, &error) == nullptr);
    CHECK(error.find("FactJournal is unhealthy") != std::string::npos);
}

void TestPendingRevisionRowsAndOwnedBytesAccounting() {
    RecordingSink sink;
    std::string error;
    std::unique_ptr<KLineWorker> worker = KLineWorker::Create(
        Config({1U, 5U}), &sink, &error);
    CHECK(worker != nullptr);
    constexpr std::uint64_t exchange = UINT64_C(34'200'000'000'000);
    const CanonicalTick trade = Trade(
        500U, 500U, exchange + 1U, MonotonicNowNs(),
        10'000'000, 1);
    const KLineInput input = OrderedInput(trade);
    const KLineApplyResult applied = worker->ApplyBatch(
        std::span<const KLineInput>(&input, 1U));
    CHECK(applied.code == KLineApplyCode::kApplied);
    CHECK(applied.revisions_created == 2U);

    KLineWorkerStats stats = worker->stats();
    CHECK(stats.pending_revision_batches == 0U);
    CHECK(stats.pending_revision_rows == 0U);
    CHECK(stats.pending_revision_bytes == 0U);
    CHECK(stats.pending_revision_rows_high_watermark == 2U);
    CHECK(stats.pending_revision_bytes_high_watermark >=
          2U * sizeof(KLineRevision));
    CHECK(sink.batches.size() == 1U);
    CHECK(sink.batches.size() == 1U);
}

void TestRuntimeMicroBatchFlushMetrics() {
    RecordingSink sink;
    KLineRuntimeConfig config = RuntimeConfig();
    config.micro_batch_rows = 2U;
    config.micro_batch_max_delay_ns = UINT64_C(1'000'000);
    std::string error;
    std::unique_ptr<KLineRuntime> runtime =
        KLineRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    constexpr std::uint64_t exchange = UINT64_C(34'200'000'000'000);

    const std::uint64_t row_now = MonotonicNowNs();
    const CanonicalTick row0 = Trade(
        600U, 600U, exchange + 1U, row_now - 10'000U,
        10'000'000, 1);
    const CanonicalTick row1 = Trade(
        601U, 601U, exchange + 2U, row_now - 5'000U,
        11'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, OrderedDispatch(row0)));
    CHECK(runtime->AppendDispatch(0U, OrderedDispatch(row1)));

    const std::uint64_t timer_now = MonotonicNowNs();
    const CanonicalTick timer = Trade(
        602U, 602U, exchange + 3U,
        timer_now - config.micro_batch_max_delay_ns, 12'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, OrderedDispatch(timer)));
    CHECK(runtime->FlushDue(0U, timer_now));

    const CanonicalTick explicit_tick = Trade(
        603U, 603U, exchange + 4U, MonotonicNowNs(), 13'000'000, 1);
    CHECK(runtime->AppendDispatch(0U, OrderedDispatch(explicit_tick)));
    CHECK(runtime->Flush(0U));
    CHECK(runtime->Flush(0U));

    const KLineRuntimeStats stats = runtime->stats();
    CHECK(stats.micro_batches_applied == 3U);
    CHECK(stats.facts_in_micro_batches == 4U);
    CHECK(stats.micro_batch_rows_max == 2U);
    CHECK(stats.micro_batch_source_age_ns_max > 0U);
    CHECK(stats.row_limit_flushes == 1U);
    CHECK(stats.timer_flushes == 1U);
    CHECK(stats.explicit_flushes == 2U);
    CHECK(stats.workers.pending_revision_batches == 0U);
    CHECK(stats.workers.pending_revision_rows == 0U);
    CHECK(stats.workers.pending_revision_bytes == 0U);
    CHECK(runtime->DrainAll());
}

}  // namespace

int main() {
    TestSdkExchangeTimeDefinesWindowAndOhlc();
    TestLateBackfillRevisesHistoricalSecondWindow();
    TestInvalidExchangeTimeNeverFallsBackToLocalClock();
    TestHalfOpenWindowAndEqualTimestampTieBreak();
    TestDuplicateConflictAndMultipleIntervals();
    TestRuntimeProjectJoinBothArrivalOrders();
    TestRuntimeRejectJoinBothArrivalOrders();
    TestRuntimeRejectsInvalidEpochOwnerAndFence();
    TestRuntimeGapAndSealControlsAreNoOps();
    TestAdmissionTokenHalfOpenBoundary();
    TestChannelGlobalFirstWinnerAcrossInstrumentOwners();
    TestWorkerRejectsUnhealthyFactJournal();
    TestPendingRevisionRowsAndOwnedBytesAccounting();
    TestRuntimeMicroBatchFlushMetrics();
    std::cout << "all KLine worker tests passed\n";
    return 0;
}
