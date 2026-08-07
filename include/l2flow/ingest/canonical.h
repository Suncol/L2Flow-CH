#pragma once

#include "l2flow/ingest/message.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace l2flow::ingest {

inline constexpr std::size_t kMaximumIdentityBytes = 32U;
inline constexpr std::size_t kCanonicalBookDepth = 10U;
inline constexpr std::uint32_t kInvalidInstrumentOrdinal =
    std::numeric_limits<std::uint32_t>::max();

enum class StartMode : std::uint8_t {
    kFromOpen,
    kPartial,
};

enum class Market : std::uint8_t {
    kUnknown,
    kShanghai,
    kShenzhen,
};

enum class CanonicalKind : std::uint8_t {
    kShanghaiTick,
    kShenzhenOrder,
    kShenzhenTransaction,
    kShanghaiSnapshot,
    kShenzhenSnapshot,
};

enum class TickAction : std::uint8_t {
    kUnknown,
    kAdd,
    kCancel,
    kTrade,
    kStatus,
};

enum class Side : std::uint8_t {
    kUnknown,
    kBuy,
    kSell,
    kBorrow,
    kLend,
};

enum class Aggressor : std::uint8_t {
    kUnknown,
    kBuy,
    kSell,
    kNeutral,
};

enum class OrderType : std::uint8_t {
    kUnknown,
    kMarket,
    kLimit,
    kSameSideBest,
};

enum class TradingPhase : std::uint8_t {
    kUnknown,
    kStart,
    kOpeningCall,
    kContinuous,
    kSuspended,
    kClosingCall,
    kClosed,
    kEnded,
};

enum QualityFlag : std::uint64_t {
    kQualityNone = 0U,
    kQualityNullSourceValue = UINT64_C(1) << 0U,
    kQualityInvalidTime = UINT64_C(1) << 1U,
    kQualityInvalidQuantity = UINT64_C(1) << 2U,
    kQualityInvalidPrice = UINT64_C(1) << 3U,
    kQualityUnknownEnum = UINT64_C(1) << 4U,
    kQualityAmbiguousOrderReference = UINT64_C(1) << 5U,
    kQualityDepthTruncated = UINT64_C(1) << 6U,
    kQualityInstrumentNotInCatalog = UINT64_C(1) << 7U,
    kQualitySequenceGapBefore = UINT64_C(1) << 8U,
    kQualityChannelHistoryIncomplete = UINT64_C(1) << 9U,
    kQualityInvalidAmount = UINT64_C(1) << 10U,
    kQualityInvalidOrderReference = UINT64_C(1) << 11U,
    kQualityNonIntegralMatchedQuantity = UINT64_C(1) << 12U,
    kQualityLateRecovery = UINT64_C(1) << 13U,
};

enum TickValidity : std::uint64_t {
    kTickPriceValid = UINT64_C(1) << 0U,
    kTickQuantityValid = UINT64_C(1) << 1U,
    kTickAmountValid = UINT64_C(1) << 2U,
    kTickPrimaryOrderIdValid = UINT64_C(1) << 3U,
    kTickBuyOrderIdValid = UINT64_C(1) << 4U,
    kTickSellOrderIdValid = UINT64_C(1) << 5U,
    kTickSideValid = UINT64_C(1) << 6U,
    kTickAggressorValid = UINT64_C(1) << 7U,
    kTickOrderTypeValid = UINT64_C(1) << 8U,
    kTickExchangeTimeValid = UINT64_C(1) << 9U,
    kTickPhaseValid = UINT64_C(1) << 10U,
    kTickMatchedQuantityValid = UINT64_C(1) << 11U,
    // Cleared on every PARTIAL output because the pre-start channel prefix is
    // unknown. Consumers must not synthesize state-dependent values when this
    // bit is absent.
    kTickChannelHistoryValid = UINT64_C(1) << 12U,
};

struct ExactIdentity final {
    Market market = Market::kUnknown;
    std::uint8_t security_id_source_size = 0U;
    std::uint8_t security_id_size = 0U;
    std::array<std::byte, kMaximumIdentityBytes> security_id_source{};
    std::array<std::byte, kMaximumIdentityBytes> security_id{};
};

struct FixedDecimal final {
    std::int64_t raw = 0;
    std::int64_t p6 = 0;
    std::uint8_t source_scale = 0U;
    bool raw_valid = false;
    bool p6_valid = false;
};

struct ScaledInteger final {
    std::int64_t raw = 0;
    std::uint8_t scale = 0U;
    bool valid = false;
};

struct CanonicalCommon final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint64_t receive_monotonic_ns = 0U;
    std::uint64_t native_sequence = 0U;
    std::uint64_t exchange_time_ns_from_midnight = 0U;
    std::uint64_t vendor_local_time_ns_from_midnight = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint64_t gap_epoch = 0U;
    std::uint64_t gap_before_first = 0U;
    std::uint64_t gap_before_last = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t instrument_ordinal = kInvalidInstrumentOrdinal;
    std::uint32_t channel = 0U;
    std::uint32_t exchange_time_raw = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    bool exchange_time_valid = false;
    bool vendor_local_time_valid = false;
    MessageKey message_key{};
    CanonicalKind kind = CanonicalKind::kShanghaiTick;
    ExactIdentity identity{};
    std::uint8_t md_stream_id_size = 0U;
    std::array<std::byte, kMaximumIdentityBytes> md_stream_id{};
};

struct CanonicalTick final {
    CanonicalCommon common{};
    FixedDecimal price{};
    FixedDecimal amount{};
    ScaledInteger quantity{};
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::int64_t sh_add_matched_quantity_raw = 0;
    std::uint64_t validity = kTickChannelHistoryValid;
    std::int32_t raw_type = 0;
    std::int32_t raw_side = 0;
    TickAction action = TickAction::kUnknown;
    Side side = Side::kUnknown;
    Aggressor aggressor = Aggressor::kUnknown;
    OrderType order_type = OrderType::kUnknown;
    TradingPhase phase = TradingPhase::kUnknown;
};

struct CanonicalBookLevel final {
    FixedDecimal price{};
    ScaledInteger quantity{};
    std::uint32_t source_order_count = 0U;
    bool order_count_valid = false;
};

struct CanonicalSnapshot final {
    CanonicalCommon common{};
    FixedDecimal previous_close{};
    FixedDecimal open{};
    FixedDecimal high{};
    FixedDecimal low{};
    FixedDecimal last{};
    FixedDecimal close{};
    FixedDecimal turnover{};
    ScaledInteger volume{};
    ScaledInteger total_bid_quantity{};
    ScaledInteger total_ask_quantity{};
    FixedDecimal weighted_average_bid{};
    FixedDecimal weighted_average_ask{};
    std::uint64_t trade_count = 0U;
    bool trade_count_valid = false;
    std::int32_t image_status = 0;
    bool image_status_valid = false;
    std::uint8_t instrument_status_code_size = 0U;
    std::array<std::byte, kMaximumIdentityBytes> instrument_status_code{};
    std::uint8_t trading_phase_code_size = 0U;
    std::array<std::byte, kMaximumIdentityBytes> trading_phase_code{};
    std::uint32_t source_bid_depth = 0U;
    std::uint32_t source_ask_depth = 0U;
    std::uint8_t retained_bid_depth = 0U;
    std::uint8_t retained_ask_depth = 0U;
    std::array<CanonicalBookLevel, kCanonicalBookDepth> bids{};
    std::array<CanonicalBookLevel, kCanonicalBookDepth> asks{};
};

struct ChannelGap final {
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t first_missing = 0U;
    std::uint64_t last_missing = 0U;
    std::uint64_t first_present_after_gap = 0U;
    std::uint64_t detected_monotonic_ns = 0U;
    // gap_epoch is the cumulative number of committed gap ranges for this
    // channel. A consumer that observes an epoch jump knows that mailbox
    // updates were coalesced and must use the durable/raw path for the
    // individual older ranges.
    std::uint64_t gap_epoch = 0U;
    std::uint64_t cumulative_missing_sequences = 0U;
};

enum class LateRecoveryReason : std::uint8_t {
    // The realtime ordered frontier has already advanced beyond this native
    // position. This can be a recovered gap member or a retransmission;
    // downstream reconciliation must decide using durable identity/history.
    kBehindCommittedFrontier,
    // PARTIAL retained the first canonical projection for a native position
    // and diverted a different projection instead of freezing the channel.
    kPendingCanonicalConflict,
};

struct LateRecoveryTick final {
    CanonicalTick tick{};
    std::uint64_t committed_next_sequence = 0U;
    std::uint64_t observed_gap_epoch = 0U;
    LateRecoveryReason reason =
        LateRecoveryReason::kBehindCommittedFrontier;
    bool catalog_match = false;
};

enum class ChannelFaultReason : std::uint8_t {
    kCanonicalConflict,
    kDecodeFailure,
};

struct ChannelFault final {
    Market market = Market::kUnknown;
    ChannelFaultReason reason =
        ChannelFaultReason::kCanonicalConflict;
    std::uint32_t channel = 0U;
    std::uint64_t expected_sequence = 0U;
    std::uint64_t observed_sequence = 0U;
    std::uint64_t detected_monotonic_ns = 0U;
};

static_assert(std::is_trivially_copyable_v<CanonicalTick>);
static_assert(std::is_trivially_copyable_v<CanonicalSnapshot>);
static_assert(std::is_trivially_copyable_v<ChannelGap>);
static_assert(std::is_trivially_copyable_v<LateRecoveryTick>);
static_assert(std::is_trivially_copyable_v<ChannelFault>);

}  // namespace l2flow::ingest
