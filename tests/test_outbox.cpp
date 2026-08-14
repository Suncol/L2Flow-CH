#include "l2flow/ingest/outbox.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

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
    inputs.event_enabled = true;
    inputs.kline_enabled = true;
    inputs.now_monotonic_ns = 10U;
    inputs.catchup_lsn_slack = 0U;
    FreshnessFrontier frontier = outbox->EvaluateFreshness(inputs);
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(!frontier.event_authoritative);
    CHECK(!frontier.kline_authoritative);

    TickDispatch dispatch{};
    CHECK(outbox->TryRead(0U, 0U, &dispatch));
    CHECK(outbox->TryRead(1U, 0U, &dispatch));
    inputs.catchup_lsn_slack = 8U;
    frontier = outbox->EvaluateFreshness(inputs);
    CHECK(frontier.mode == ContinuityMode::kCaughtUp);
    CHECK(frontier.event_authoritative);
    CHECK(frontier.kline_authoritative);

    inputs.event_healthy = false;
    frontier = outbox->EvaluateFreshness(inputs);
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(!frontier.event_authoritative);
    CHECK(frontier.kline_authoritative);
    CHECK(std::string(ContinuityModeName(frontier.mode)) ==
          "DERIVED_CATCHUP");

    inputs.event_healthy = true;
    inputs.kline_healthy = false;
    frontier = outbox->EvaluateFreshness(inputs);
    CHECK(frontier.mode == ContinuityMode::kDerivedCatchup);
    CHECK(frontier.event_authoritative);
    CHECK(!frontier.kline_authoritative);

    inputs.fatal = true;
    frontier = outbox->EvaluateFreshness(inputs);
    CHECK(frontier.mode == ContinuityMode::kFatalContinuity);
    CHECK(std::string(ContinuityModeName(frontier.mode)) ==
          "FATAL_CONTINUITY");
}

void TestRejectsInvalidCreate() {
    std::string error;
    CHECK(DispositionOutbox::Create(0U, 1U, 1U, 8U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 9U, 8U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 1U, 1U, 1U, &error) == nullptr);
    CHECK(DispositionOutbox::Create(1U, 1U, 1U, 8U, 0U, &error) == nullptr);
}

}  // namespace

int main() {
    TestIndependentConsumersSeeTheSameRecords();
    TestSlowConsumerPinsTheRing();
    TestBroadcastReachesEveryOwnerAndOccurrenceIsFiltered();
    TestLanesHaveIndependentLsns();
    TestFreshnessModes();
    TestRejectsInvalidCreate();
    std::cout << "test_outbox: ok\n";
    return 0;
}
