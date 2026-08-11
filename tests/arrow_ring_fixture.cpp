#include "l2flow/arrow/egress.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

namespace {

void FillCommon(l2flow::ingest::CanonicalCommon* common,
                std::uint64_t ingress,
                std::uint64_t exchange_time) {
    common->ingress_sequence = ingress;
    common->vendor_sequence_id = ingress + 100U;
    common->receive_monotonic_ns = ingress + 1'000U;
    common->native_sequence = ingress + 200U;
    common->exchange_time_valid = true;
    common->exchange_time_ns_from_midnight = exchange_time;
    common->trade_date = 20'260'807U;
    common->instrument_id = 600'000U;
    common->instrument_ordinal = 3U;
    common->channel = 1U;
    common->identity.market = l2flow::ingest::Market::kShanghai;
}

[[nodiscard]] l2flow::ingest::TickDispatch MakeOrderedDispatch(
    l2flow::ingest::CanonicalTick tick,
    std::uint64_t fence) {
    l2flow::ingest::TickDispatch dispatch{};
    dispatch.tick = tick;
    dispatch.feed_session_epoch = 9'001U;
    dispatch.expected_sequence = tick.common.native_sequence;
    dispatch.admission_floor = 1U;
    dispatch.evict_before = tick.common.native_sequence + 1U;
    dispatch.dispatch_fence = fence;
    dispatch.channel = tick.common.channel;
    dispatch.owner = 0U;
    dispatch.market = tick.common.identity.market;
    dispatch.kind = l2flow::ingest::TickDispatchKind::kProjectOrdered;
    dispatch.catalog_match = true;
    return dispatch;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: arrow_ring_fixture ROOT_DIRECTORY\n";
        return 2;
    }
    l2flow::arrow_hot::ArrowHotEgressConfig config{};
    config.root_directory = std::filesystem::path(argv[1]);
    config.owner_count = 1U;
    config.feed_session_epoch = 9'001U;
    config.descriptor_capacity = 8U;
    config.segment_count = 10U;
    config.tick_segment_payload_bytes = 1024U * 1024U;
    config.snapshot_segment_payload_bytes = 1024U * 1024U;
    config.diagnostic_segment_payload_bytes = 1024U * 1024U;
    config.maximum_consumers = 4U;
    config.tick_batch_rows = 2U;
    config.snapshot_batch_rows = 2U;
    config.diagnostic_batch_rows = 4U;
    config.maximum_batch_delay_ns = 100U;

    std::string error;
    std::unique_ptr<l2flow::arrow_hot::ArrowHotEgress> egress =
        l2flow::arrow_hot::ArrowHotEgress::Create(config, &error);
    if (egress == nullptr) {
        std::cerr << error << '\n';
        return 1;
    }

    l2flow::ingest::CanonicalTick first{};
    FillCommon(&first.common, 1U, 0U);
    first.price = {12'345, 12'345'000, 3U, true, true};
    first.quantity = {100, 0U, true};
    first.action = l2flow::ingest::TickAction::kTrade;
    if (!egress->AppendTickDispatch(0U, MakeOrderedDispatch(first, 1U))) {
        std::cerr << egress->fatal_error() << '\n';
        return 1;
    }
    l2flow::ingest::CanonicalTick second = first;
    FillCommon(&second.common, 2U, 500U);
    second.price.raw = 12'346;
    second.price.p6 = 12'346'000;
    if (!egress->AppendTickDispatch(0U, MakeOrderedDispatch(second, 2U))) {
        std::cerr << egress->fatal_error() << '\n';
        return 1;
    }

    l2flow::ingest::CanonicalSnapshot snapshot{};
    FillCommon(&snapshot.common, 3U, 1'000U);
    snapshot.retained_bid_depth = 1U;
    snapshot.retained_ask_depth = 1U;
    snapshot.source_bid_depth = 10U;
    snapshot.source_ask_depth = 10U;
    snapshot.bids[0].price = {100, 100'000'000, 0U, true, true};
    snapshot.bids[0].quantity = {20, 0U, true};
    snapshot.asks[0].price = {101, 101'000'000, 0U, true, true};
    snapshot.asks[0].quantity = {30, 0U, true};
    if (!egress->AppendSnapshot(0U, snapshot)) {
        std::cerr << egress->fatal_error() << '\n';
        return 1;
    }

    l2flow::ingest::ChannelGap gap{};
    gap.market = l2flow::ingest::Market::kShanghai;
    gap.channel = 1U;
    gap.first_missing = 10U;
    gap.last_missing = 11U;
    gap.first_present_after_gap = 12U;
    gap.detected_monotonic_ns = 2'000U;
    gap.gap_epoch = 1U;
    gap.cumulative_missing_sequences = 2U;
    if (!egress->AppendGap(gap) ||
        !egress->MarkFeedConnected(2'100U)) {
        std::cerr << egress->fatal_error() << '\n';
        return 1;
    }
    egress->FlushAll();
    egress->Seal(2'200U);
    if (!egress->healthy()) {
        std::cerr << egress->fatal_error() << '\n';
        return 1;
    }
    std::cout << config.root_directory.string() << '\n';
    return 0;
}
