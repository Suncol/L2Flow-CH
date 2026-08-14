#pragma once

#include "l2flow/common/identifier.h"
#include "l2flow/ingest/canonical.h"

#include <compare>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace l2flow::event {

using l2flow::common::Identifier128;
using l2flow::ingest::Aggressor;
using l2flow::ingest::Market;
using l2flow::ingest::OrderType;
using l2flow::ingest::Side;
using l2flow::ingest::TickAction;
using l2flow::ingest::TradingPhase;

inline constexpr std::uint32_t kEventSchemaVersion = 1U;

struct FactKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t native_sequence = 0U;

    friend constexpr bool operator==(const FactKey&, const FactKey&) =
        default;
    friend constexpr auto operator<=>(const FactKey&, const FactKey&) =
        default;
};

struct OrderKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    std::int64_t order_id = 0;

    friend constexpr bool operator==(const OrderKey&, const OrderKey&) =
        default;
    friend constexpr auto operator<=>(const OrderKey&, const OrderKey&) =
        default;
};

enum class EventKind : std::uint8_t {
    kShanghaiOrderRevision = 1U,
    kShanghaiTrade,
    kShanghaiCancel,
    kShanghaiStatus,
    kShenzhenOrderRevision,
    kShenzhenTrade,
    kShenzhenCancel,
};

struct EventKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    std::uint64_t native_sequence = 0U;
    EventKind event_kind = EventKind::kShanghaiOrderRevision;
    std::int64_t affected_order_id = 0;
    std::uint32_t occurrence = 0U;

    friend constexpr bool operator==(const EventKey&, const EventKey&) =
        default;
    friend constexpr auto operator<=>(const EventKey&, const EventKey&) =
        default;
};

struct SourceAnchor final {
    std::uint64_t native_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint64_t receive_monotonic_ns = 0U;
    std::uint64_t exchange_time_ns_from_midnight = 0U;
    std::uint64_t vendor_local_time_ns_from_midnight = 0U;
    std::uint32_t exchange_time_raw = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    bool exchange_time_valid = false;
    bool vendor_local_time_valid = false;

    friend constexpr bool operator==(const SourceAnchor&,
                                     const SourceAnchor&) = default;
};

enum class OrderFinality : std::uint8_t {
    kProvisional = 0U,
    kFinal,
    kConflict,
};

enum class OrderDeltaOperation : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kFinalize,
};

enum class ShanghaiOriginalQuantityStatus : std::uint8_t {
    kUnknown = 0U,
    kExact,
    kLowerBound,
};

enum class ShanghaiOrderPriceSource : std::uint8_t {
    kUnknown = 0U,
    kSourceAdd,
    kBuyMaximumExecution,
    kSellMinimumExecution,
};

enum class ShanghaiOrderSource : std::uint8_t {
    kUnknown = 0U,
    kSourceAdd,
    kReconstructedFromTrades,
};

enum class ShanghaiOrderSideSource : std::uint8_t {
    kUnknown = 0U,
    kSourceAddFlag,
    kSourceAggressorFlag,
};

enum class ShanghaiOrderQualityFlag : std::uint8_t {
    kSyntheticOrder = 0U,
    kOriginalQuantityLowerBound,
    kExecutionBoundaryPrice,
    kPrematchQuantityMismatch,
    kPrematchQuantityUnavailable,
    kQuantityConflict,
    kPhaseUnknown,
    kUnexpectedPhase,
    kSideConflict,
    kDuplicateAdd,
    kCancelWithoutAdd,
    kEndedWithObservedBalance,
};

[[nodiscard]] constexpr std::uint64_t ShanghaiOrderQualityBit(
    ShanghaiOrderQualityFlag flag) noexcept {
    return UINT64_C(1) << static_cast<std::uint8_t>(flag);
}

enum class ShenzhenEventQualityFlag : std::uint8_t {
    kUnknownBuyOrderReference = 0U,
    kUnknownSellOrderReference,
    kUnknownCancelOrderReference,
    kQuantityConflict,
    kNumericOverflow,
    kSideConflict,
    kDuplicateOrder,
    kEndedWithObservedBalance,
    kAmbiguousTradeOrderReferences,
};

[[nodiscard]] constexpr std::uint64_t ShenzhenEventQualityBit(
    ShenzhenEventQualityFlag flag) noexcept {
    return UINT64_C(1) << static_cast<std::uint8_t>(flag);
}

// This is the complete published semantic order image. It deliberately has
// no continuously incrementing business revision: Event identity and
// ClickHouse replacement version are independent of source insertion order.
struct OrderSnapshot final {
    OrderKey key{};
    Side side = Side::kUnknown;
    OrderType order_type = OrderType::kUnknown;
    ShanghaiOrderSideSource sh_side_source =
        ShanghaiOrderSideSource::kUnknown;
    ShanghaiOrderSource sh_order_source = ShanghaiOrderSource::kUnknown;

    std::int64_t price_p6 = 0;
    bool price_valid = false;
    ShanghaiOrderPriceSource sh_price_source =
        ShanghaiOrderPriceSource::kUnknown;
    std::int64_t execution_boundary_price_p6 = 0;
    bool execution_boundary_price_valid = false;

    std::int64_t published_quantity = 0;
    bool published_quantity_valid = false;
    std::int64_t original_quantity = 0;
    bool original_quantity_valid = false;
    ShanghaiOriginalQuantityStatus sh_original_quantity_status =
        ShanghaiOriginalQuantityStatus::kUnknown;
    std::int64_t remaining_quantity = 0;
    bool remaining_quantity_valid = false;
    std::int64_t source_matched_quantity = 0;
    bool source_matched_quantity_valid = false;
    std::int64_t observed_pre_add_trade_quantity = 0;
    std::int64_t post_add_trade_quantity = 0;
    std::int64_t total_trade_quantity = 0;
    std::int64_t total_cancel_quantity = 0;
    std::uint64_t trade_count = 0U;
    std::uint64_t cancel_count = 0U;

    TradingPhase phase_at_first = TradingPhase::kUnknown;
    TradingPhase phase_at_add = TradingPhase::kUnknown;
    TradingPhase phase_at_last = TradingPhase::kUnknown;
    bool add_seen = false;
    bool apply_to_book = false;

    SourceAnchor first_anchor{};
    SourceAnchor last_anchor{};
    SourceAnchor add_anchor{};
    OrderFinality finality = OrderFinality::kProvisional;
    std::uint64_t quality_flags = 0U;
    std::uint64_t source_quality_flags = 0U;

    friend constexpr bool operator==(const OrderSnapshot&,
                                     const OrderSnapshot&) = default;
};

struct EventPayload final {
    SourceAnchor source_anchor{};
    TickAction action = TickAction::kUnknown;
    Side side = Side::kUnknown;
    Aggressor aggressor = Aggressor::kUnknown;
    OrderType order_type = OrderType::kUnknown;
    TradingPhase phase = TradingPhase::kUnknown;

    std::int64_t price_p6 = 0;
    bool price_valid = false;
    std::int64_t amount_p6 = 0;
    bool amount_valid = false;
    std::int64_t quantity = 0;
    bool quantity_valid = false;
    std::int64_t matched_quantity = 0;
    bool matched_quantity_valid = false;
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;

    std::uint64_t source_quality_flags = 0U;
    std::uint64_t event_quality_flags = 0U;
    bool referenced_order_found = false;
    bool referenced_order_found_valid = false;
    bool side_from_order = false;
    OrderDeltaOperation order_delta_operation =
        OrderDeltaOperation::kInsert;
    bool order_snapshot_valid = false;
    OrderSnapshot order{};

    friend constexpr bool operator==(const EventPayload&,
                                     const EventPayload&) = default;
};

enum class RevisionOperation : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kTombstone,
};

enum class RevisionReason : std::uint8_t {
    kLiveProjection = 0U,
    kHoleFill,
    kSessionFinalize,
};

struct EventRevision final {
    EventKey key{};
    std::uint64_t version = 0U;
    Identifier128 revision_id{};
    Identifier128 supersedes_revision_id{};
    bool supersedes_revision_id_valid = false;
    Identifier128 recovery_run_id{};
    RevisionOperation operation = RevisionOperation::kInsert;
    RevisionReason reason = RevisionReason::kLiveProjection;
    Identifier128 calculation_run_id{};
    std::uint32_t logic_version = 0U;
    Identifier128 input_set_hash{};
    Identifier128 payload_hash{};
    bool is_deleted = false;
    EventPayload payload{};

    friend constexpr bool operator==(const EventRevision&,
                                     const EventRevision&) = default;
};

struct EventRevisionBatch final {
    Identifier128 calculation_run_id{};
    Identifier128 recovery_run_id{};
    std::uint32_t owner = 0U;
    std::uint64_t batch_sequence = 0U;
    RevisionReason reason = RevisionReason::kLiveProjection;
    std::vector<EventRevision> revisions;
};

class EventRevisionSink {
public:
    virtual ~EventRevisionSink() = default;

    // One owner submits a consecutive FIFO group. Every contained batch stays
    // an independent recovery/commit boundary; the sink may combine their rows
    // physically but must retain all immutable objects until the whole group
    // is acknowledged or terminally failed.
    [[nodiscard]] virtual bool AppendRevisionGroup(
        std::vector<std::shared_ptr<const EventRevisionBatch>> batches)
        noexcept = 0;
};

}  // namespace l2flow::event
