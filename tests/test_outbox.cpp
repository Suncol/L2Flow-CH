#include "l2flow/ingest/outbox.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

TickDispatch Occurrence(std::uint32_t owner, std::uint32_t channel) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = 7U;
    dispatch.channel = channel;
    dispatch.owner = owner;
    dispatch.market = Market::kShenzhen;
    dispatch.kind = TickDispatchKind::kProjectOrdered;
    dispatch.catalog_match = true;
    dispatch.tick.common.channel = channel;
    dispatch.tick.common.identity.market = Market::kShenzhen;
    return dispatch;
}

TickDispatch Control(TickDispatchKind kind, std::uint32_t channel) {
    TickDispatch dispatch{};
    dispatch.feed_session_epoch = 7U;
    dispatch.channel = channel;
    dispatch.owner = kOutboxBroadcastOwner;
    dispatch.market = Market::kShenzhen;
    dispatch.kind = kind;
    dispatch.first_missing = 10U;
    dispatch.last_missing = 12U;
    return dispatch;
}

std::unique_ptr<DispositionOutbox> MakeOutbox(std::size_t lanes,
                                              std::size_t owners,
                                              std::size_t consumers,
                                              std::size_t records) {
    std::string error;
    auto outbox = DispositionOutbox::Create(
        lanes, owners, consumers, records, 7U, &error);
    CHECK(outbox != nullptr);
    return outbox;
}

void TestIndependentConsumersSeeTheSameRecords() {
    auto outbox = MakeOutbox(1U, 1U, 2U, 8U);
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    TickDispatch first{};
    TickDispatch second{};
    CHECK(outbox->TryRead(0U, 0U, &first));
    CHECK(outbox->TryRead(1U, 0U, &second));
    CHECK(first.outbox_lsn == 1U);
    CHECK(second.outbox_lsn == 1U);
    CHECK(first.dispatch_fence == 1U);
    CHECK(second.dispatch_fence == 1U);
    CHECK(outbox->TryRead(0U, 0U, &first));
    CHECK(outbox->TryRead(1U, 0U, &second));
    CHECK(first.outbox_lsn == 2U);
    CHECK(second.outbox_lsn == 2U);
    CHECK(!outbox->TryRead(0U, 0U, &first));
}

void TestSlowConsumerPinsTheRing() {
    auto outbox = MakeOutbox(1U, 1U, 2U, 4U);
    CHECK(outbox->records_per_lane() == 4U);
    for (std::uint32_t index = 0U; index < 4U; ++index) {
        CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    }
    TickDispatch dispatch{};
    CHECK(outbox->TryRead(0U, 0U, &dispatch));
    CHECK(!outbox->Append(0U, Occurrence(0U, 1U)));
    CHECK(outbox->stats().append_rejected_full == 1U);
    while (outbox->TryRead(1U, 0U, &dispatch)) {
    }
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    CHECK(outbox->head_lsn(0U) == 5U);
}

void TestBroadcastReachesEveryOwnerAndOccurrenceIsFiltered() {
    auto outbox = MakeOutbox(1U, 2U, 1U, 8U);
    CHECK(outbox->Append(0U, Occurrence(0U, 3U)));
    CHECK(outbox->Append(0U, Control(TickDispatchKind::kGapOpen, 3U)));
    CHECK(outbox->Append(0U, Occurrence(1U, 3U)));

    TickDispatch owner0{};
    CHECK(outbox->TryRead(0U, 0U, &owner0));
    CHECK(owner0.owner == 0U);
    CHECK(owner0.kind == TickDispatchKind::kProjectOrdered);
    CHECK(owner0.outbox_lsn == 1U);
    CHECK(outbox->TryRead(0U, 0U, &owner0));
    CHECK(owner0.owner == 0U);
    CHECK(owner0.kind == TickDispatchKind::kGapOpen);
    CHECK(owner0.outbox_lsn == 2U);
    CHECK(!outbox->TryRead(0U, 0U, &owner0));

    TickDispatch owner1{};
    CHECK(outbox->TryRead(0U, 1U, &owner1));
    CHECK(owner1.owner == 1U);
    CHECK(owner1.kind == TickDispatchKind::kGapOpen);
    CHECK(owner1.outbox_lsn == 2U);
    CHECK(outbox->TryRead(0U, 1U, &owner1));
    CHECK(owner1.owner == 1U);
    CHECK(owner1.kind == TickDispatchKind::kProjectOrdered);
    CHECK(owner1.outbox_lsn == 3U);

    const DispositionOutboxStats stats = outbox->stats();
    CHECK(stats.records_appended == 3U);
    CHECK(stats.records_read == 4U);
    CHECK(stats.records_skipped == 2U);
}

void TestLanesHaveIndependentLsns() {
    auto outbox = MakeOutbox(2U, 1U, 1U, 8U);
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    CHECK(outbox->Append(1U, Occurrence(0U, 2U)));
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    CHECK(outbox->head_lsn(0U) == 2U);
    CHECK(outbox->head_lsn(1U) == 1U);
    TickDispatch dispatch{};
    CHECK(outbox->TryRead(0U, 0U, &dispatch));
    CHECK(dispatch.outbox_lane == 0U || dispatch.outbox_lane == 1U);
    const std::uint16_t first_lane = dispatch.outbox_lane;
    CHECK(dispatch.outbox_lsn == 1U);
    CHECK(outbox->TryRead(0U, 0U, &dispatch));
    CHECK(dispatch.outbox_lane != first_lane || dispatch.outbox_lsn == 2U);
}

void TestFreshnessModes() {
    auto outbox = MakeOutbox(1U, 1U, 2U, 8U);
    CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    ContinuityInputs inputs{};
    inputs.event.enabled = true;
    inputs.kline.enabled = true;
    inputs.now_monotonic_ns = 10U;
    inputs.catchup_lsn_slack = 0U;
    const auto observe = [&] {
        return EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
    };
    FreshnessFrontier frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(!frontier.event_authoritative);
    CHECK(!frontier.kline_authoritative);

    TickDispatch dispatch{};
    CHECK(outbox->TryRead(0U, 0U, &dispatch));
    CHECK(outbox->TryRead(1U, 0U, &dispatch));
    // Polling releases the slot before either runtime has processed the record.
    frontier = observe();
    CHECK(frontier.event.consumed && frontier.kline.consumed);
    CHECK(!frontier.event.calculated && !frontier.kline.calculated);
    CHECK(!frontier.event_authoritative && !frontier.kline_authoritative);
    outbox->PublishProgress(0U, 0U, DerivedProgress::kCalculated);
    outbox->PublishProgress(1U, 0U, DerivedProgress::kSubmitted);
    frontier = observe();
    CHECK(frontier.event_authoritative && frontier.kline_authoritative);
    CHECK(!frontier.event.submitted && !frontier.event.acknowledged);
    CHECK(frontier.kline.acknowledged);
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);

    outbox->PublishProgress(0U, 0U, DerivedProgress::kSubmitted);
    inputs.event.revision_batches_submitted = 1U;
    frontier = observe();
    CHECK(frontier.event.submitted && !frontier.event.acknowledged);
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    inputs.event.revision_batches_acked = 1U;
    inputs.catchup_lsn_slack = 8U;
    frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kCaughtUp);
    CHECK(frontier.event_authoritative);
    CHECK(frontier.kline_authoritative);

    inputs.event.sink_healthy = false;
    frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(!frontier.event_authoritative && !frontier.event.acknowledged);
    CHECK(frontier.kline_authoritative);

    inputs.event.sink_healthy = true;
    inputs.event.healthy = false;
    frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(!frontier.event_authoritative);
    CHECK(frontier.kline_authoritative);
    CHECK(std::string(ContinuityModeName(frontier.mode)) ==
          "DERIVED_CATCHUP");

    inputs.event.healthy = true;
    inputs.kline.healthy = false;
    frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(frontier.event_authoritative);
    CHECK(!frontier.kline_authoritative);

    inputs.fatal = true;
    frontier = observe();
    CHECK(frontier.mode == ContinuityMode::kFatalContinuity);
    CHECK(!frontier.event_authoritative && !frontier.kline_authoritative);
    CHECK(!frontier.event.acknowledged && !frontier.kline.acknowledged);
    CHECK(std::string(ContinuityModeName(frontier.mode)) ==
          "FATAL_CONTINUITY");
}

void TestFreshnessWarnsBeforeCapacityAndNeverAllowsCalculationSlack() {
    for (const std::size_t capacity : {2U, 8U, 32'768U}) {
        auto outbox = MakeOutbox(1U, 1U, 1U, capacity);
        ContinuityInputs inputs{};
        inputs.kline.enabled = true;  // KLine-only uses consumer zero.
        CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
        auto frontier = EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
        CHECK(frontier.lag_warning_lsn == capacity / 2U);
        CHECK(!frontier.kline_authoritative);
        CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
        for (std::size_t row = 1U; row < capacity / 2U; ++row) {
            CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
        }
        frontier = EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
        CHECK(frontier.outbox_pressure);
        CHECK(frontier.max_lag_lsn < capacity);
        CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
    }
}

void TestFreshnessCoversBroadcastOwnersAndArrowPressure() {
    auto outbox = MakeOutbox(2U, 2U, 3U, 8U);
    ContinuityInputs inputs{};
    inputs.event.enabled = true;
    inputs.kline.enabled = true;
    CHECK(outbox->Append(1U, Control(TickDispatchKind::kGapOpen, 7U)));
    TickDispatch dispatch{};
    for (std::size_t consumer = 0U; consumer < 2U; ++consumer) {
        for (std::size_t owner = 0U; owner < 2U; ++owner) {
            CHECK(outbox->TryRead(consumer, owner, &dispatch));
            if (consumer != 0U || owner != 1U) {
                outbox->PublishProgress(consumer, owner, DerivedProgress::kSubmitted);
            }
        }
    }
    const auto before = outbox->CaptureFreshness(inputs);
    CHECK(!EvaluateFreshness(before, inputs).event_authoritative);
    CHECK(EvaluateFreshness(before, inputs).kline_authoritative);
    outbox->PublishProgress(0U, 1U, DerivedProgress::kSubmitted);
    // Finishing work later must not upgrade an earlier snapshot.
    CHECK(!EvaluateFreshness(before, inputs).event_authoritative);
    CHECK(EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs).event_authoritative);
    for (int row = 0; row < 3; ++row) {
        CHECK(outbox->Append(1U, Occurrence(0U, 7U)));
        for (std::size_t consumer = 0U; consumer < 2U; ++consumer) {
            CHECK(outbox->TryRead(consumer, 0U, &dispatch));
            CHECK(!outbox->TryRead(consumer, 1U, &dispatch));
            outbox->PublishProgress(consumer, 0U, DerivedProgress::kSubmitted);
        }
    }
    const auto frontier = EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
    CHECK(frontier.event_authoritative && frontier.kline_authoritative);
    CHECK(frontier.outbox_pressure);  // The Arrow cursor still pins this lane.
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
}

void TestConcurrentFreshnessWaitsForOwnerPublication() {
    auto outbox = MakeOutbox(1U, 1U, 1U, 2U);
    constexpr std::uint64_t kRounds = 2'000U;
    std::atomic<std::uint64_t> delivered{0U};
    std::atomic<std::uint64_t> may_publish{0U};
    std::atomic<std::uint64_t> published{0U};
    std::thread consumer([&] {
        TickDispatch dispatch{};
        for (std::uint64_t round = 1U; round <= kRounds; ++round) {
            while (!outbox->TryRead(0U, 0U, &dispatch)) {
                std::this_thread::yield();
            }
            delivered.store(round, std::memory_order_release);
            while (may_publish.load(std::memory_order_acquire) != round) {
                std::this_thread::yield();
            }
            outbox->PublishProgress(0U, 0U, DerivedProgress::kSubmitted);
            published.store(round, std::memory_order_release);
        }
    });
    ContinuityInputs inputs{};
    inputs.event.enabled = true;
    for (std::uint64_t round = 1U; round <= kRounds; ++round) {
        CHECK(outbox->Append(0U, Occurrence(0U, 1U)));
        while (delivered.load(std::memory_order_acquire) != round) {
            std::this_thread::yield();
        }
        auto frontier = EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
        CHECK(frontier.event.consumed);
        CHECK(!frontier.event.calculated && !frontier.event.acknowledged);
        may_publish.store(round, std::memory_order_release);
        while (published.load(std::memory_order_acquire) != round) {
            std::this_thread::yield();
        }
        frontier = EvaluateFreshness(outbox->CaptureFreshness(inputs), inputs);
        CHECK(frontier.mode == ContinuityMode::kCaughtUp);
    }
    consumer.join();
}

void TestRejectsInvalidCreate() {
    std::string error;
    CHECK(DispositionOutbox::Create(0U, 1U, 1U, 8U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 9U, 8U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 1U, 1U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 1U, 8U, 0U, &error) == nullptr);
}

void TestConcurrentWrapPreservesWholeRecords() {
    auto outbox = MakeOutbox(1U, 1U, 1U, 2U);
    constexpr std::uint64_t kRecords = 250'000U;
    std::atomic<bool> producer_done{false};
    std::thread producer([&] {
        for (std::uint64_t sequence = 1U;
             sequence <= kRecords; ++sequence) {
            TickDispatch dispatch = Occurrence(0U, 1U);
            dispatch.tick.common.ingress_sequence = sequence;
            dispatch.tick.common.vendor_sequence_id = sequence;
            dispatch.tick.common.receive_monotonic_ns = sequence;
            dispatch.tick.common.native_sequence = sequence;
            dispatch.tick.price.raw = static_cast<std::int64_t>(sequence);
            dispatch.tick.price.p6 = static_cast<std::int64_t>(sequence);
            dispatch.tick.quantity.raw = static_cast<std::int64_t>(sequence);
            while (!outbox->Append(0U, dispatch)) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::uint64_t expected = 1U;
    TickDispatch dispatch{};
    while (expected <= kRecords) {
        if (!outbox->TryRead(0U, 0U, &dispatch)) {
            CHECK(!producer_done.load(std::memory_order_acquire) ||
                  expected > outbox->head_lsn(0U));
            std::this_thread::yield();
            continue;
        }
        CHECK(dispatch.outbox_lsn == expected);
        CHECK(dispatch.dispatch_fence == expected);
        CHECK(dispatch.tick.common.ingress_sequence == expected);
        CHECK(dispatch.tick.common.vendor_sequence_id == expected);
        CHECK(dispatch.tick.common.receive_monotonic_ns == expected);
        CHECK(dispatch.tick.common.native_sequence == expected);
        CHECK(dispatch.tick.price.raw == static_cast<std::int64_t>(expected));
        CHECK(dispatch.tick.price.p6 == static_cast<std::int64_t>(expected));
        CHECK(dispatch.tick.quantity.raw ==
              static_cast<std::int64_t>(expected));
        ++expected;
    }
    producer.join();
    CHECK(producer_done.load(std::memory_order_acquire));
}

}  // namespace

int main() {
    TestIndependentConsumersSeeTheSameRecords();
    TestSlowConsumerPinsTheRing();
    TestBroadcastReachesEveryOwnerAndOccurrenceIsFiltered();
    TestLanesHaveIndependentLsns();
    TestFreshnessModes();
    TestFreshnessWarnsBeforeCapacityAndNeverAllowsCalculationSlack();
    TestFreshnessCoversBroadcastOwnersAndArrowPressure();
    TestConcurrentFreshnessWaitsForOwnerPublication();
    TestRejectsInvalidCreate();
    TestConcurrentWrapPreservesWholeRecords();
    std::cout << "test_outbox: ok\n";
    return 0;
}
