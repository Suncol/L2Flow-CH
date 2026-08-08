#include "l2flow/kline/runtime.h"

#include <algorithm>
#include <array>
#include <cstdlib>
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
    config.calculation_run_id.bytes[0U] = std::byte{1U};
    config.interval_seconds = std::move(intervals);
    config.maximum_facts = 1'024U;
    config.maximum_bars = 1'024U;
    config.maximum_pending_commits = 64U;
    config.maximum_acknowledged_raw_dependencies = 1'024U;
    return config;
}

void Ack(KLineWorker* worker, std::span<const CanonicalTick> ticks) {
    std::vector<RawTickDependency> dependencies;
    dependencies.reserve(ticks.size());
    for (const CanonicalTick& tick : ticks) {
        dependencies.push_back(RawTickDependency{
            tick.common.ingress_sequence, tick.common.kind});
    }
    worker->AcknowledgeRawTicks(dependencies);
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
        KLineInput{later_exchange}, KLineInput{earlier_exchange}};
    const KLineApplyResult result = worker->ApplyBatch(inputs);
    CHECK(result.code == KLineApplyCode::kApplied);
    CHECK(sink.batches.empty());
    Ack(worker.get(), std::array{later_exchange, earlier_exchange});
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
    const KLineInput live_input{live};
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&live_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&live, 1U));

    const CanonicalTick next_window = Trade(
        103U, 11U, base + UINT64_C(1'100'000'000), 101U,
        13'000'000, 7);
    const KLineInput next_input{next_window};
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&next_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&next_window, 1U));

    const CanonicalTick recovered = Trade(
        101U, 12U, base + 100'000'000U, 9'999U, 9'000'000, 3);
    KLineInput recovered_input{recovered};
    recovered_input.late_recovery = true;
    recovered_input.committed_next_sequence = 104U;
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&recovered_input, 1U)).code ==
          KLineApplyCode::kApplied);
    Ack(worker.get(), std::span<const CanonicalTick>(&recovered, 1U));

    CHECK(sink.batches.size() == 3U);
    const KLineRevision& repair = sink.batches.back()->revisions.front();
    CHECK(repair.operation == RevisionOperation::kUpdate);
    CHECK(repair.reason == RevisionReason::kLateRecovery);
    CHECK(repair.supersedes_revision_id_valid);
    KLinePayload bar{};
    CHECK(worker->CopyBar(
        KLineKey{20260808U, Market::kShenzhen, 1U, 1U, base}, &bar));
    CHECK(bar.open_price_p6 == 9'000'000);
    CHECK(bar.close_price_p6 == 11'000'000);
    CHECK(bar.volume == 8);
    CHECK(bar.has_late_recovery);
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
        KLineInput{invalid}, KLineInput{missing_validity},
        KLineInput{out_of_day}};
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
        KLineInput{channel_nine}, KLineInput{channel_two},
        KLineInput{boundary}};
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
    const KLineInput first{trade};
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&first, 1U)).revisions_created == 2U);
    Ack(worker.get(), std::span<const CanonicalTick>(&trade, 1U));

    CanonicalTick duplicate = trade;
    duplicate.common.ingress_sequence = 31U;
    duplicate.common.receive_monotonic_ns = 2U;
    const KLineInput duplicate_input{duplicate};
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&duplicate_input, 1U)).code ==
          KLineApplyCode::kDuplicateOnly);
    Ack(worker.get(), std::span<const CanonicalTick>(&duplicate, 1U));
    CHECK(sink.batches.size() == 1U);

    CanonicalTick conflict = trade;
    conflict.common.ingress_sequence = 32U;
    conflict.price.raw = 110'000;
    conflict.price.p6 = 11'000'000;
    const KLineInput conflict_input{conflict};
    CHECK(worker->ApplyBatch(
              std::span<const KLineInput>(&conflict_input, 1U)).code ==
          KLineApplyCode::kSourceConflict);
    Ack(worker.get(), std::span<const CanonicalTick>(&conflict, 1U));
    CHECK(sink.batches.size() == 1U);
}

void TestRawAckBeforeFactAndRuntimeRouting() {
    RecordingSink sink;
    KLineRuntimeConfig config{};
    config.worker = Config({1U});
    config.micro_batch_rows = 1U;
    config.maximum_raw_ack_backlog_per_owner = 64U;
    config.worker.maximum_acknowledged_raw_dependencies = 64U;
    std::string error;
    std::unique_ptr<KLineRuntime> runtime =
        KLineRuntime::Create(config, &sink, &error);
    CHECK(runtime != nullptr);
    constexpr std::uint64_t base = UINT64_C(34'200'000'000'000);
    const CanonicalTick trade = Trade(
        1U, 40U, base + 1U, 100U, 10'000'000, 1);
    CHECK(runtime->OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick>(&trade, 1U)));
    CHECK(runtime->AppendTick(0U, trade));
    CHECK(runtime->FlushAll());
    CHECK(sink.batches.size() == 1U);
    CHECK(runtime->stats().raw_tick_acks_received == 1U);
}

}  // namespace

int main() {
    TestSdkExchangeTimeDefinesWindowAndOhlc();
    TestLateBackfillRevisesHistoricalSecondWindow();
    TestInvalidExchangeTimeNeverFallsBackToLocalClock();
    TestHalfOpenWindowAndEqualTimestampTieBreak();
    TestDuplicateConflictAndMultipleIntervals();
    TestRawAckBeforeFactAndRuntimeRouting();
    std::cout << "all KLine worker tests passed\n";
    return 0;
}
