#include "l2flow/event/worker.h"

#include "l2flow/clickhouse/raw_sink.h"
#include "l2flow/ingest/engine.h"

#include "event_exact_list.h"
#include "event_lazy_paged_hash.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace l2flow::event {
namespace {

using ingest::CanonicalKind;
using ingest::CanonicalTick;

struct ChannelKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;

    friend constexpr auto operator<=>(const ChannelKey&,
                                      const ChannelKey&) = default;
};

struct InstrumentChannelKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;

    friend constexpr auto operator<=>(const InstrumentChannelKey&,
                                      const InstrumentChannelKey&) = default;
};

[[nodiscard]] std::uint64_t HashMix(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

template <typename Value, bool IsEnum>
struct HashRawType final {
    using type = Value;
};

template <typename Value>
struct HashRawType<Value, true> final {
    using type = std::underlying_type_t<Value>;
};

template <typename Value>
void HashAppend(std::uint64_t* state, Value value) noexcept {
    using Raw = typename HashRawType<Value, std::is_enum_v<Value>>::type;
    using Unsigned = std::make_unsigned_t<Raw>;
    const std::uint64_t encoded = static_cast<std::uint64_t>(
        static_cast<Unsigned>(static_cast<Raw>(value)));
    *state = HashMix(*state ^ (encoded + UINT64_C(0x9e3779b97f4a7c15) +
                               (*state << 6U) + (*state >> 2U)));
}

struct ChannelKeyHash final {
    [[nodiscard]] std::size_t operator()(const ChannelKey& key) const
        noexcept {
        std::uint64_t state = UINT64_C(0x243f6a8885a308d3);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.channel);
        return static_cast<std::size_t>(state);
    }
};

struct InstrumentChannelKeyHash final {
    [[nodiscard]] std::size_t operator()(
        const InstrumentChannelKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0x13198a2e03707344);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.instrument_id);
        HashAppend(&state, key.channel);
        return static_cast<std::size_t>(state);
    }
};

struct FactKeyHash final {
    [[nodiscard]] std::size_t operator()(const FactKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0xa4093822299f31d0);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.channel);
        HashAppend(&state, key.native_sequence);
        return static_cast<std::size_t>(state);
    }
};

struct OrderKeyHash final {
    [[nodiscard]] std::size_t operator()(const OrderKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0x082efa98ec4e6c89);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.instrument_id);
        HashAppend(&state, key.channel);
        HashAppend(&state, key.order_id);
        return static_cast<std::size_t>(state);
    }
};

struct EventKeyHash final {
    [[nodiscard]] std::size_t operator()(const EventKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0xbe5466cf34e90c6c);
        HashAppend(&state, key.trade_date);
        HashAppend(&state, key.market);
        HashAppend(&state, key.instrument_id);
        HashAppend(&state, key.channel);
        HashAppend(&state, key.native_sequence);
        HashAppend(&state, key.event_kind);
        HashAppend(&state, key.affected_order_id);
        HashAppend(&state, key.occurrence);
        return static_cast<std::size_t>(state);
    }
};

enum class OrderRole : std::uint8_t {
    kPrimary = 0U,
    kBuy,
    kSell,
    kBarrier,
};

struct RoleCacheKey final {
    OrderKey order{};
    std::uint64_t native_sequence = 0U;

    friend constexpr auto operator<=>(const RoleCacheKey&,
                                      const RoleCacheKey&) = default;
};

struct RoleCacheKeyHash final {
    [[nodiscard]] std::size_t operator()(
        const RoleCacheKey& key) const noexcept {
        std::uint64_t state = UINT64_C(0x452821e638d01377);
        HashAppend(&state, OrderKeyHash{}(key.order));
        HashAppend(&state, key.native_sequence);
        return static_cast<std::size_t>(state);
    }
};

[[nodiscard]] bool IsZero(Identifier128 value) noexcept {
    return std::all_of(value.bytes.begin(), value.bytes.end(),
                       [](std::byte octet) {
                           return octet == std::byte{0U};
                       });
}

class SemanticHasher final {
public:
    SemanticHasher() noexcept
        : states_{UINT64_C(1469598103934665603),
                  UINT64_C(1099511628211),
                  UINT64_C(0x9e3779b97f4a7c15),
                  UINT64_C(0x6a09e667f3bcc909)} {}

    void AppendBytes(std::span<const std::byte> bytes) noexcept {
        for (const std::byte value : bytes) {
            const std::uint64_t octet =
                std::to_integer<std::uint8_t>(value);
            states_[0U] = (states_[0U] ^ octet) *
                          UINT64_C(1099511628211);
            states_[1U] = (states_[1U] + octet +
                           UINT64_C(0x9e3779b97f4a7c15)) *
                          UINT64_C(0xbf58476d1ce4e5b9);
            states_[2U] ^= octet + UINT64_C(0x9e3779b97f4a7c15) +
                           (states_[2U] << 6U) + (states_[2U] >> 2U);
            states_[3U] = (states_[3U] ^ (octet + states_[0U])) *
                          UINT64_C(0x94d049bb133111eb);
        }
        length_ += static_cast<std::uint64_t>(bytes.size());
    }

    template <typename Value>
    void Append(Value value) noexcept {
        static_assert(std::is_integral_v<Value> || std::is_enum_v<Value>);
        if constexpr (std::is_enum_v<Value>) {
            Append(static_cast<std::underlying_type_t<Value>>(value));
        } else if constexpr (std::is_same_v<Value, bool>) {
            Append(static_cast<std::uint8_t>(value ? 1U : 0U));
        } else {
            using Unsigned = std::make_unsigned_t<Value>;
            const Unsigned encoded = static_cast<Unsigned>(value);
            std::array<std::byte, sizeof(Unsigned)> bytes{};
            for (std::size_t index = 0U; index < bytes.size(); ++index) {
                bytes[index] = static_cast<std::byte>(
                    static_cast<std::uint64_t>(encoded) >> (index * 8U));
            }
            AppendBytes(bytes);
        }
    }

    [[nodiscard]] Identifier128 Finish() const noexcept {
        std::array<std::byte, 40U> input{};
        std::size_t offset = 0U;
        const auto append_u64 = [&input, &offset](std::uint64_t value) {
            for (std::size_t index = 0U; index < sizeof(value); ++index) {
                input[offset + index] = static_cast<std::byte>(
                    value >> (index * 8U));
            }
            offset += sizeof(value);
        };
        for (const std::uint64_t state : states_) {
            append_u64(state);
        }
        append_u64(length_);
        return clickhouse::Blake3Hash128(input);
    }

private:
    std::array<std::uint64_t, 4U> states_{};
    std::uint64_t length_ = 0U;
};

void HashAnchor(SemanticHasher* hasher,
                const SourceAnchor& value) noexcept {
    hasher->Append(value.native_sequence);
    hasher->Append(value.ingress_sequence);
    hasher->Append(value.vendor_sequence_id);
    hasher->Append(value.receive_monotonic_ns);
    hasher->Append(value.exchange_time_ns_from_midnight);
    hasher->Append(value.vendor_local_time_ns_from_midnight);
    hasher->Append(value.exchange_time_raw);
    hasher->Append(value.vendor_local_time_raw);
    hasher->Append(value.exchange_time_valid);
    hasher->Append(value.vendor_local_time_valid);
}

void HashOrderSnapshot(SemanticHasher* hasher,
                       const OrderSnapshot& value) noexcept {
    hasher->Append(value.key.trade_date);
    hasher->Append(value.key.market);
    hasher->Append(value.key.instrument_id);
    hasher->Append(value.key.channel);
    hasher->Append(value.key.order_id);
    hasher->Append(value.side);
    hasher->Append(value.order_type);
    hasher->Append(value.sh_side_source);
    hasher->Append(value.sh_order_source);
    hasher->Append(value.price_p6);
    hasher->Append(value.price_valid);
    hasher->Append(value.sh_price_source);
    hasher->Append(value.execution_boundary_price_p6);
    hasher->Append(value.execution_boundary_price_valid);
    hasher->Append(value.published_quantity);
    hasher->Append(value.published_quantity_valid);
    hasher->Append(value.original_quantity);
    hasher->Append(value.original_quantity_valid);
    hasher->Append(value.sh_original_quantity_status);
    hasher->Append(value.remaining_quantity);
    hasher->Append(value.remaining_quantity_valid);
    hasher->Append(value.source_matched_quantity);
    hasher->Append(value.source_matched_quantity_valid);
    hasher->Append(value.observed_pre_add_trade_quantity);
    hasher->Append(value.post_add_trade_quantity);
    hasher->Append(value.total_trade_quantity);
    hasher->Append(value.total_cancel_quantity);
    hasher->Append(value.trade_count);
    hasher->Append(value.cancel_count);
    hasher->Append(value.phase_at_first);
    hasher->Append(value.phase_at_add);
    hasher->Append(value.phase_at_last);
    hasher->Append(value.add_seen);
    hasher->Append(value.apply_to_book);
    HashAnchor(hasher, value.first_anchor);
    HashAnchor(hasher, value.last_anchor);
    HashAnchor(hasher, value.add_anchor);
    hasher->Append(value.finality);
    hasher->Append(value.quality_flags);
    hasher->Append(value.source_quality_flags);
}

[[nodiscard]] Identifier128 HashPayload(
    const EventPayload& value) noexcept {
    SemanticHasher hasher;
    HashAnchor(&hasher, value.source_anchor);
    hasher.Append(value.action);
    hasher.Append(value.side);
    hasher.Append(value.aggressor);
    hasher.Append(value.order_type);
    hasher.Append(value.phase);
    hasher.Append(value.price_p6);
    hasher.Append(value.price_valid);
    hasher.Append(value.amount_p6);
    hasher.Append(value.amount_valid);
    hasher.Append(value.quantity);
    hasher.Append(value.quantity_valid);
    hasher.Append(value.matched_quantity);
    hasher.Append(value.matched_quantity_valid);
    hasher.Append(value.primary_order_id);
    hasher.Append(value.buy_order_id);
    hasher.Append(value.sell_order_id);
    hasher.Append(value.source_quality_flags);
    hasher.Append(value.event_quality_flags);
    hasher.Append(value.referenced_order_found);
    hasher.Append(value.referenced_order_found_valid);
    hasher.Append(value.side_from_order);
    hasher.Append(value.order_delta_operation);
    hasher.Append(value.order_snapshot_valid);
    if (value.order_snapshot_valid) {
        HashOrderSnapshot(&hasher, value.order);
    }
    return hasher.Finish();
}

[[nodiscard]] Identifier128 CombineHashes(
    Identifier128 left,
    Identifier128 right,
    std::uint8_t tag) noexcept {
    std::array<std::byte, 33U> input{};
    std::copy(left.bytes.begin(), left.bytes.end(), input.begin());
    std::copy(right.bytes.begin(), right.bytes.end(), input.begin() + 16U);
    input.back() = static_cast<std::byte>(tag);
    return clickhouse::Blake3Hash128(input);
}

[[nodiscard]] SourceAnchor MakeAnchor(const CanonicalTick& tick) noexcept {
    SourceAnchor anchor{};
    anchor.native_sequence = tick.common.native_sequence;
    anchor.ingress_sequence = tick.common.ingress_sequence;
    anchor.vendor_sequence_id = tick.common.vendor_sequence_id;
    anchor.receive_monotonic_ns = tick.common.receive_monotonic_ns;
    anchor.exchange_time_ns_from_midnight =
        tick.common.exchange_time_ns_from_midnight;
    anchor.vendor_local_time_ns_from_midnight =
        tick.common.vendor_local_time_ns_from_midnight;
    anchor.exchange_time_raw = tick.common.exchange_time_raw;
    anchor.vendor_local_time_raw = tick.common.vendor_local_time_raw;
    anchor.exchange_time_valid = tick.common.exchange_time_valid;
    anchor.vendor_local_time_valid = tick.common.vendor_local_time_valid;
    return anchor;
}

[[nodiscard]] FactKey MakeFactKey(const CanonicalTick& tick) noexcept {
    const journal::FactKey key = journal::MakeFactKey(tick);
    return FactKey{key.trade_date, key.market, key.channel,
                   key.native_sequence};
}

[[nodiscard]] FactKey MakeFactKey(const EventKey& key) noexcept {
    return FactKey{key.trade_date, key.market, key.channel,
                   key.native_sequence};
}

[[nodiscard]] OrderKey MakeOrderKey(const CanonicalTick& tick,
                                    std::int64_t order_id) noexcept {
    return OrderKey{tick.common.trade_date,
                    tick.common.identity.market,
                    tick.common.instrument_id,
                    tick.common.channel,
                    order_id};
}

[[nodiscard]] Identifier128 HashFact(const CanonicalTick& tick) noexcept {
    SemanticHasher hasher;
    hasher.Append(tick.common.ingress_sequence);
    hasher.Append(tick.common.vendor_sequence_id);
    hasher.Append(tick.common.receive_monotonic_ns);
    hasher.Append(tick.common.trade_date);
    hasher.Append(tick.common.instrument_id);
    hasher.Append(tick.common.identity.market);
    hasher.Append(tick.common.channel);
    hasher.Append(tick.common.native_sequence);
    hasher.Append(tick.common.exchange_time_ns_from_midnight);
    hasher.Append(tick.common.vendor_local_time_ns_from_midnight);
    hasher.Append(tick.common.exchange_time_raw);
    hasher.Append(tick.common.vendor_local_time_raw);
    hasher.Append(tick.common.exchange_time_valid);
    hasher.Append(tick.common.vendor_local_time_valid);
    hasher.Append(tick.common.quality_flags);
    hasher.Append(tick.common.message_key.service_id);
    hasher.Append(tick.common.message_key.service_version);
    hasher.Append(tick.common.message_key.message_id);
    hasher.Append(tick.common.kind);
    hasher.Append(tick.common.identity.security_id_source_size);
    hasher.AppendBytes(tick.common.identity.security_id_source);
    hasher.Append(tick.common.identity.security_id_size);
    hasher.AppendBytes(tick.common.identity.security_id);
    hasher.Append(tick.price.raw);
    hasher.Append(tick.price.p6);
    hasher.Append(tick.price.source_scale);
    hasher.Append(tick.price.raw_valid);
    hasher.Append(tick.price.p6_valid);
    hasher.Append(tick.amount.raw);
    hasher.Append(tick.amount.p6);
    hasher.Append(tick.amount.source_scale);
    hasher.Append(tick.amount.raw_valid);
    hasher.Append(tick.amount.p6_valid);
    hasher.Append(tick.quantity.raw);
    hasher.Append(tick.quantity.scale);
    hasher.Append(tick.quantity.valid);
    hasher.Append(tick.primary_order_id);
    hasher.Append(tick.buy_order_id);
    hasher.Append(tick.sell_order_id);
    hasher.Append(tick.sh_add_matched_quantity_raw);
    hasher.Append(tick.validity);
    hasher.Append(tick.raw_type);
    hasher.Append(tick.raw_side);
    hasher.Append(tick.action);
    hasher.Append(tick.side);
    hasher.Append(tick.aggressor);
    hasher.Append(tick.order_type);
    hasher.Append(tick.phase);
    return hasher.Finish();
}

[[nodiscard]] bool CheckedAdd(std::int64_t left,
                              std::int64_t right,
                              std::int64_t* output) noexcept {
    if (output == nullptr ||
        (right > 0 &&
         left > std::numeric_limits<std::int64_t>::max() - right) ||
        (right < 0 &&
         left < std::numeric_limits<std::int64_t>::min() - right)) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedSubtract(std::int64_t left,
                                   std::int64_t right,
                                   std::int64_t* output) noexcept {
    if (output == nullptr ||
        (right > 0 &&
         left < std::numeric_limits<std::int64_t>::min() + right) ||
        (right < 0 &&
         left > std::numeric_limits<std::int64_t>::max() + right)) {
        return false;
    }
    *output = left - right;
    return true;
}

struct OrderState final {
    OrderSnapshot snapshot{};
    std::int64_t pre_add_active_trade_quantity = 0;
    std::int64_t minimum_execution_price_p6 = 0;
    std::int64_t maximum_execution_price_p6 = 0;
    bool execution_prices_seen = false;
    bool terminal = false;
    bool finalization_emitted = false;
    bool revision_emitted = false;

    friend constexpr bool operator==(const OrderState&,
                                     const OrderState&) = default;
};

[[nodiscard]] Identifier128 HashState(const OrderState& value) noexcept {
    SemanticHasher hasher;
    HashOrderSnapshot(&hasher, value.snapshot);
    hasher.Append(value.pre_add_active_trade_quantity);
    hasher.Append(value.minimum_execution_price_p6);
    hasher.Append(value.maximum_execution_price_p6);
    hasher.Append(value.execution_prices_seen);
    hasher.Append(value.terminal);
    hasher.Append(value.finalization_emitted);
    hasher.Append(value.revision_emitted);
    return hasher.Finish();
}

constexpr std::uint64_t kShanghaiConflictMask =
    ShanghaiOrderQualityBit(
        ShanghaiOrderQualityFlag::kPrematchQuantityMismatch) |
    ShanghaiOrderQualityBit(
        ShanghaiOrderQualityFlag::kPrematchQuantityUnavailable) |
    ShanghaiOrderQualityBit(
        ShanghaiOrderQualityFlag::kQuantityConflict) |
    ShanghaiOrderQualityBit(ShanghaiOrderQualityFlag::kPhaseUnknown) |
    ShanghaiOrderQualityBit(ShanghaiOrderQualityFlag::kUnexpectedPhase) |
    ShanghaiOrderQualityBit(ShanghaiOrderQualityFlag::kSideConflict) |
    ShanghaiOrderQualityBit(ShanghaiOrderQualityFlag::kDuplicateAdd) |
    ShanghaiOrderQualityBit(ShanghaiOrderQualityFlag::kCancelWithoutAdd);

constexpr std::uint64_t kShenzhenConflictMask =
    ShenzhenEventQualityBit(
        ShenzhenEventQualityFlag::kQuantityConflict) |
    ShenzhenEventQualityBit(ShenzhenEventQualityFlag::kNumericOverflow) |
    ShenzhenEventQualityBit(ShenzhenEventQualityFlag::kSideConflict) |
    ShenzhenEventQualityBit(ShenzhenEventQualityFlag::kDuplicateOrder);

void MarkMutation(const CanonicalTick& fact,
                  OrderState* state,
                  bool shanghai) noexcept {
    state->snapshot.last_anchor = MakeAnchor(fact);
    state->snapshot.source_quality_flags |= fact.common.quality_flags;
    if (shanghai) {
        state->snapshot.phase_at_last =
            (fact.validity & ingest::kTickPhaseValid) != 0U
                ? fact.phase
                : TradingPhase::kUnknown;
        if ((fact.validity & ingest::kTickPhaseValid) == 0U ||
            fact.phase == TradingPhase::kUnknown) {
            state->snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kPhaseUnknown);
        }
    }
}

void RefreshShanghaiState(OrderState* state) noexcept {
    if (state->execution_prices_seen) {
        if (state->snapshot.side == Side::kBuy) {
            state->snapshot.execution_boundary_price_p6 =
                state->maximum_execution_price_p6;
            state->snapshot.execution_boundary_price_valid = true;
        } else if (state->snapshot.side == Side::kSell) {
            state->snapshot.execution_boundary_price_p6 =
                state->minimum_execution_price_p6;
            state->snapshot.execution_boundary_price_valid = true;
        }
    }
    if (!state->snapshot.add_seen) {
        state->snapshot.sh_order_source =
            ShanghaiOrderSource::kReconstructedFromTrades;
        state->snapshot.original_quantity =
            state->snapshot.total_trade_quantity;
        state->snapshot.original_quantity_valid =
            state->snapshot.total_trade_quantity > 0;
        state->snapshot.sh_original_quantity_status =
            state->snapshot.original_quantity_valid
                ? ShanghaiOriginalQuantityStatus::kLowerBound
                : ShanghaiOriginalQuantityStatus::kUnknown;
        state->snapshot.observed_pre_add_trade_quantity =
            state->pre_add_active_trade_quantity;
        state->snapshot.quality_flags |=
            ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kSyntheticOrder) |
            ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kOriginalQuantityLowerBound);
        if (state->snapshot.execution_boundary_price_valid) {
            state->snapshot.price_p6 =
                state->snapshot.execution_boundary_price_p6;
            state->snapshot.price_valid = true;
            state->snapshot.sh_price_source =
                state->snapshot.side == Side::kBuy
                    ? ShanghaiOrderPriceSource::kBuyMaximumExecution
                    : ShanghaiOrderPriceSource::kSellMinimumExecution;
            state->snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kExecutionBoundaryPrice);
        }
    }
    if ((state->snapshot.quality_flags & kShanghaiConflictMask) != 0U) {
        state->snapshot.finality = OrderFinality::kConflict;
    } else if (state->terminal) {
        state->snapshot.finality = OrderFinality::kFinal;
    } else {
        state->snapshot.finality = OrderFinality::kProvisional;
    }
}

void RefreshShenzhenState(OrderState* state) noexcept {
    if ((state->snapshot.quality_flags & kShenzhenConflictMask) != 0U) {
        state->snapshot.finality = OrderFinality::kConflict;
    } else if (state->terminal) {
        state->snapshot.finality = OrderFinality::kFinal;
    } else {
        state->snapshot.finality = OrderFinality::kProvisional;
    }
}

[[nodiscard]] OrderState InitializeShanghaiState(
    const OrderKey& key,
    Side side,
    ShanghaiOrderSideSource side_source,
    const CanonicalTick& fact) noexcept {
    OrderState state{};
    state.snapshot.key = key;
    state.snapshot.side = side;
    state.snapshot.sh_side_source = side_source;
    state.snapshot.phase_at_first =
        (fact.validity & ingest::kTickPhaseValid) != 0U
            ? fact.phase
            : TradingPhase::kUnknown;
    state.snapshot.phase_at_last = state.snapshot.phase_at_first;
    state.snapshot.first_anchor = MakeAnchor(fact);
    state.snapshot.last_anchor = state.snapshot.first_anchor;
    state.snapshot.source_quality_flags = fact.common.quality_flags;
    if ((fact.validity & ingest::kTickPhaseValid) == 0U ||
        fact.phase == TradingPhase::kUnknown) {
        state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
            ShanghaiOrderQualityFlag::kPhaseUnknown);
    }
    return state;
}

[[nodiscard]] OrderDeltaOperation BeginOrderEmission(
    OrderState* state) noexcept {
    const bool first = !state->revision_emitted;
    const bool finalizing = state->terminal &&
                            !state->finalization_emitted;
    const OrderDeltaOperation operation =
        first ? OrderDeltaOperation::kInsert
              : (finalizing ? OrderDeltaOperation::kFinalize
                            : OrderDeltaOperation::kUpdate);
    state->revision_emitted = true;
    if (finalizing) {
        state->finalization_emitted = true;
    }
    return operation;
}

[[nodiscard]] EventPayload MakeOrderPayload(
    const CanonicalTick& fact,
    const OrderState& state,
    OrderDeltaOperation operation) noexcept {
    EventPayload payload{};
    payload.source_anchor = MakeAnchor(fact);
    payload.action = fact.action;
    payload.side = fact.side;
    payload.aggressor = fact.aggressor;
    payload.order_type = fact.order_type;
    payload.phase = fact.phase;
    payload.source_quality_flags = fact.common.quality_flags;
    payload.order_delta_operation = operation;
    payload.order_snapshot_valid = true;
    payload.order = state.snapshot;
    return payload;
}

struct RoleEval final {
    std::optional<OrderState> post_state;
    std::optional<OrderDeltaOperation> order_operation;
    std::uint64_t source_quality_contribution = 0U;
    bool referenced_order_found = false;
    Side resolved_side = Side::kUnknown;
    bool side_from_order = false;
    Identifier128 post_state_hash{};
    Identifier128 input_set_hash{};

    friend constexpr bool operator==(const RoleEval&,
                                     const RoleEval&) = default;
};

struct ApplyRoleResult final {
    RoleEval eval{};
    bool ok = true;
};

[[nodiscard]] bool ReferenceSideCompatible(Side reference,
                                           Side actual) noexcept {
    if (reference == Side::kBuy) {
        return actual == Side::kBuy || actual == Side::kBorrow;
    }
    if (reference == Side::kSell) {
        return actual == Side::kSell || actual == Side::kLend;
    }
    return false;
}

[[nodiscard]] ApplyRoleResult ApplyShenzhenRole(
    const std::optional<OrderState>& previous,
    const CanonicalTick& fact,
    OrderRole role,
    Identifier128 previous_input_hash,
    Identifier128 fact_hash) noexcept {
    ApplyRoleResult result{};
    result.eval.post_state = previous;
    const Side expected = role == OrderRole::kBuy
        ? Side::kBuy
        : role == OrderRole::kSell ? Side::kSell : fact.side;

    if (fact.action == TickAction::kAdd) {
        if (!previous.has_value()) {
            OrderState state{};
            state.snapshot.key = MakeOrderKey(fact, fact.primary_order_id);
            state.snapshot.side = fact.side;
            state.snapshot.order_type = fact.order_type;
            state.snapshot.price_p6 = fact.price.p6_valid
                ? fact.price.p6
                : 0;
            state.snapshot.price_valid = fact.price.p6_valid;
            state.snapshot.original_quantity = fact.quantity.raw;
            state.snapshot.original_quantity_valid = true;
            state.snapshot.remaining_quantity = fact.quantity.raw;
            state.snapshot.remaining_quantity_valid = true;
            state.snapshot.first_anchor = MakeAnchor(fact);
            state.snapshot.last_anchor = state.snapshot.first_anchor;
            state.snapshot.source_quality_flags =
                fact.common.quality_flags;
            const OrderDeltaOperation operation =
                BeginOrderEmission(&state);
            result.eval.order_operation = operation;
            result.eval.post_state = state;
        } else {
            OrderState state = *previous;
            state.snapshot.quality_flags |= ShenzhenEventQualityBit(
                ShenzhenEventQualityFlag::kDuplicateOrder);
            if (state.snapshot.side != fact.side) {
                state.snapshot.quality_flags |= ShenzhenEventQualityBit(
                    ShenzhenEventQualityFlag::kSideConflict);
            }
            if (state.snapshot.original_quantity != fact.quantity.raw) {
                state.snapshot.quality_flags |= ShenzhenEventQualityBit(
                    ShenzhenEventQualityFlag::kQuantityConflict);
            }
            MarkMutation(fact, &state, false);
            RefreshShenzhenState(&state);
            const OrderDeltaOperation operation =
                BeginOrderEmission(&state);
            result.eval.order_operation = operation;
            result.eval.post_state = state;
        }
    } else if (fact.action == TickAction::kTrade ||
               fact.action == TickAction::kCancel) {
        const bool trade = fact.action == TickAction::kTrade;
        if (!previous.has_value()) {
            result.eval.source_quality_contribution =
                ShenzhenEventQualityBit(
                    trade
                        ? (role == OrderRole::kBuy
                               ? ShenzhenEventQualityFlag::
                                     kUnknownBuyOrderReference
                               : ShenzhenEventQualityFlag::
                                     kUnknownSellOrderReference)
                        : ShenzhenEventQualityFlag::
                              kUnknownCancelOrderReference);
        } else {
            OrderState state = *previous;
            result.eval.referenced_order_found = true;
            result.eval.resolved_side = state.snapshot.side;
            result.eval.side_from_order = !trade;
            const std::uint64_t before = state.snapshot.quality_flags;
            if (!ReferenceSideCompatible(expected, state.snapshot.side)) {
                state.snapshot.quality_flags |= ShenzhenEventQualityBit(
                    ShenzhenEventQualityFlag::kSideConflict);
            }
            if (state.terminal) {
                state.snapshot.quality_flags |= ShenzhenEventQualityBit(
                    ShenzhenEventQualityFlag::kQuantityConflict);
            }
            std::int64_t next = 0;
            std::int64_t* total = trade
                ? &state.snapshot.total_trade_quantity
                : &state.snapshot.total_cancel_quantity;
            if (!CheckedAdd(*total, fact.quantity.raw, &next)) {
                state.snapshot.quality_flags |=
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kNumericOverflow) |
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kQuantityConflict);
            } else {
                *total = next;
            }
            std::uint64_t* count = trade ? &state.snapshot.trade_count
                                         : &state.snapshot.cancel_count;
            if (*count == std::numeric_limits<std::uint64_t>::max()) {
                state.snapshot.quality_flags |=
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kNumericOverflow) |
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kQuantityConflict);
            } else {
                ++(*count);
            }
            if (!CheckedSubtract(state.snapshot.remaining_quantity,
                                 fact.quantity.raw, &next)) {
                state.snapshot.remaining_quantity_valid = false;
                state.snapshot.quality_flags |=
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kNumericOverflow) |
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::kQuantityConflict);
            } else {
                state.snapshot.remaining_quantity = next;
                state.snapshot.remaining_quantity_valid = next >= 0;
                if (next < 0) {
                    state.snapshot.quality_flags |=
                        ShenzhenEventQualityBit(
                            ShenzhenEventQualityFlag::kQuantityConflict);
                } else if (next == 0) {
                    state.terminal = true;
                }
            }
            MarkMutation(fact, &state, false);
            RefreshShenzhenState(&state);
            result.eval.source_quality_contribution =
                state.snapshot.quality_flags & ~before;
            const OrderDeltaOperation operation =
                BeginOrderEmission(&state);
            result.eval.order_operation = operation;
            result.eval.post_state = state;
        }
    }

    if (result.eval.post_state.has_value()) {
        result.eval.post_state_hash = HashState(*result.eval.post_state);
        result.eval.input_set_hash = CombineHashes(
            previous.has_value() ? previous_input_hash : Identifier128{},
            fact_hash,
            static_cast<std::uint8_t>(role));
    } else {
        result.eval.input_set_hash = CombineHashes(
            fact_hash, Identifier128{}, static_cast<std::uint8_t>(role));
    }
    return result;
}

[[nodiscard]] bool IsDirectOriginalQuantityPhase(
    TradingPhase phase) noexcept {
    return phase == TradingPhase::kOpeningCall ||
           phase == TradingPhase::kSuspended ||
           phase == TradingPhase::kClosingCall;
}

[[nodiscard]] ApplyRoleResult ApplyShanghaiRole(
    const std::optional<OrderState>& previous,
    const CanonicalTick& fact,
    OrderRole role,
    Identifier128 previous_input_hash,
    Identifier128 fact_hash) noexcept {
    ApplyRoleResult result{};
    result.eval.post_state = previous;
    const Side expected = role == OrderRole::kBuy
        ? Side::kBuy
        : role == OrderRole::kSell ? Side::kSell : fact.side;

    if (role == OrderRole::kBarrier) {
        if (!previous.has_value() || previous->finalization_emitted) {
            result.eval.input_set_hash = previous.has_value()
                ? previous_input_hash
                : CombineHashes(fact_hash, Identifier128{},
                                static_cast<std::uint8_t>(role));
            if (previous.has_value()) {
                result.eval.post_state_hash = HashState(*previous);
            }
            return result;
        }
        OrderState state = *previous;
        state.terminal = true;
        if (state.snapshot.add_seen &&
            state.snapshot.remaining_quantity_valid &&
            state.snapshot.remaining_quantity > 0) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kEndedWithObservedBalance);
        }
        RefreshShanghaiState(&state);
        const OrderDeltaOperation operation = BeginOrderEmission(&state);
        result.eval.order_operation = operation;
        result.eval.post_state = state;
    } else if (fact.action == TickAction::kAdd) {
        OrderState state = previous.has_value()
            ? *previous
            : InitializeShanghaiState(
                  MakeOrderKey(fact, fact.primary_order_id), fact.side,
                  ShanghaiOrderSideSource::kSourceAddFlag, fact);
        if (state.snapshot.add_seen) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kDuplicateAdd);
            MarkMutation(fact, &state, true);
            RefreshShanghaiState(&state);
        } else {
            if (state.snapshot.side != fact.side) {
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    ShanghaiOrderQualityFlag::kSideConflict);
            }
            const bool was_terminal = state.terminal;
            if (was_terminal) {
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    ShanghaiOrderQualityFlag::kQuantityConflict);
            }
            state.snapshot.quality_flags &=
                ~(ShanghaiOrderQualityBit(
                      ShanghaiOrderQualityFlag::kSyntheticOrder) |
                  ShanghaiOrderQualityBit(
                      ShanghaiOrderQualityFlag::
                          kOriginalQuantityLowerBound) |
                  ShanghaiOrderQualityBit(
                      ShanghaiOrderQualityFlag::kExecutionBoundaryPrice));
            state.snapshot.side = fact.side;
            state.snapshot.sh_side_source =
                ShanghaiOrderSideSource::kSourceAddFlag;
            state.snapshot.sh_order_source =
                ShanghaiOrderSource::kSourceAdd;
            state.snapshot.add_seen = true;
            state.snapshot.apply_to_book = true;
            state.snapshot.phase_at_add =
                (fact.validity & ingest::kTickPhaseValid) != 0U
                    ? fact.phase
                    : TradingPhase::kUnknown;
            state.snapshot.add_anchor = MakeAnchor(fact);
            state.snapshot.price_p6 = fact.price.p6;
            state.snapshot.price_valid = true;
            state.snapshot.sh_price_source =
                ShanghaiOrderPriceSource::kSourceAdd;
            state.snapshot.published_quantity = fact.quantity.raw;
            state.snapshot.published_quantity_valid = true;
            state.snapshot.remaining_quantity = fact.quantity.raw;
            state.snapshot.remaining_quantity_valid = true;
            state.snapshot.source_matched_quantity =
                fact.sh_add_matched_quantity_raw;
            state.snapshot.source_matched_quantity_valid =
                (fact.validity & ingest::kTickMatchedQuantityValid) != 0U;
            state.snapshot.observed_pre_add_trade_quantity =
                state.pre_add_active_trade_quantity;
            state.snapshot.post_add_trade_quantity = 0;

            std::int64_t original = 0;
            const bool phase_valid =
                (fact.validity & ingest::kTickPhaseValid) != 0U;
            if (phase_valid && fact.phase == TradingPhase::kContinuous) {
                if (state.snapshot.source_matched_quantity_valid) {
                    if (!CheckedAdd(fact.quantity.raw,
                                    fact.sh_add_matched_quantity_raw,
                                    &original)) {
                        result.ok = false;
                        return result;
                    }
                    state.snapshot.original_quantity = original;
                    state.snapshot.original_quantity_valid = true;
                    state.snapshot.sh_original_quantity_status =
                        ShanghaiOriginalQuantityStatus::kExact;
                    if (fact.sh_add_matched_quantity_raw !=
                        state.pre_add_active_trade_quantity) {
                        state.snapshot.quality_flags |=
                            ShanghaiOrderQualityBit(
                                ShanghaiOrderQualityFlag::
                                    kPrematchQuantityMismatch);
                    }
                } else {
                    if (!CheckedAdd(fact.quantity.raw,
                                    state.pre_add_active_trade_quantity,
                                    &original)) {
                        result.ok = false;
                        return result;
                    }
                    state.snapshot.original_quantity = original;
                    state.snapshot.original_quantity_valid = true;
                    state.snapshot.sh_original_quantity_status =
                        ShanghaiOriginalQuantityStatus::kLowerBound;
                    state.snapshot.quality_flags |=
                        ShanghaiOrderQualityBit(
                            ShanghaiOrderQualityFlag::
                                kPrematchQuantityUnavailable) |
                        ShanghaiOrderQualityBit(
                            ShanghaiOrderQualityFlag::
                                kOriginalQuantityLowerBound);
                }
            } else if (phase_valid &&
                       IsDirectOriginalQuantityPhase(fact.phase)) {
                state.snapshot.original_quantity = fact.quantity.raw;
                state.snapshot.original_quantity_valid = true;
                state.snapshot.sh_original_quantity_status =
                    ShanghaiOriginalQuantityStatus::kExact;
            } else {
                state.snapshot.original_quantity = std::max(
                    fact.quantity.raw,
                    state.snapshot.total_trade_quantity);
                state.snapshot.original_quantity_valid = true;
                state.snapshot.sh_original_quantity_status =
                    ShanghaiOriginalQuantityStatus::kLowerBound;
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    ShanghaiOrderQualityFlag::
                        kOriginalQuantityLowerBound);
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    !phase_valid || fact.phase == TradingPhase::kUnknown
                        ? ShanghaiOrderQualityFlag::kPhaseUnknown
                        : ShanghaiOrderQualityFlag::kUnexpectedPhase);
            }
            if (!was_terminal) {
                state.terminal = false;
            }
            MarkMutation(fact, &state, true);
            RefreshShanghaiState(&state);
        }
        const OrderDeltaOperation operation = BeginOrderEmission(&state);
        result.eval.order_operation = operation;
        result.eval.post_state = state;
    } else if (fact.action == TickAction::kTrade) {
        const bool active_continuous =
            (fact.validity & ingest::kTickPhaseValid) != 0U &&
            fact.phase == TradingPhase::kContinuous &&
            ((role == OrderRole::kBuy &&
              fact.aggressor == Aggressor::kBuy) ||
             (role == OrderRole::kSell &&
              fact.aggressor == Aggressor::kSell));
        if (!previous.has_value() && !active_continuous) {
            result.eval.input_set_hash = CombineHashes(
                fact_hash, Identifier128{},
                static_cast<std::uint8_t>(role));
            return result;
        }
        OrderState state = previous.has_value()
            ? *previous
            : InitializeShanghaiState(
                  MakeOrderKey(
                      fact,
                      role == OrderRole::kBuy ? fact.buy_order_id
                                              : fact.sell_order_id),
                  expected,
                  ShanghaiOrderSideSource::kSourceAggressorFlag,
                  fact);
        if (state.snapshot.side != expected) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kSideConflict);
        }
        if (state.terminal) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kQuantityConflict);
        }
        std::int64_t next = 0;
        if (!CheckedAdd(state.snapshot.total_trade_quantity,
                        fact.quantity.raw, &next) ||
            state.snapshot.trade_count ==
                std::numeric_limits<std::uint64_t>::max()) {
            result.ok = false;
            return result;
        }
        state.snapshot.total_trade_quantity = next;
        ++state.snapshot.trade_count;
        if (!state.execution_prices_seen) {
            state.minimum_execution_price_p6 = fact.price.p6;
            state.maximum_execution_price_p6 = fact.price.p6;
            state.execution_prices_seen = true;
        } else {
            state.minimum_execution_price_p6 = std::min(
                state.minimum_execution_price_p6, fact.price.p6);
            state.maximum_execution_price_p6 = std::max(
                state.maximum_execution_price_p6, fact.price.p6);
        }
        if (state.snapshot.add_seen) {
            if (!CheckedAdd(state.snapshot.post_add_trade_quantity,
                            fact.quantity.raw, &next)) {
                result.ok = false;
                return result;
            }
            state.snapshot.post_add_trade_quantity = next;
            if (!CheckedSubtract(state.snapshot.remaining_quantity,
                                 fact.quantity.raw, &next)) {
                result.ok = false;
                return result;
            }
            state.snapshot.remaining_quantity = next;
            state.snapshot.remaining_quantity_valid = next >= 0;
            if (next < 0) {
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    ShanghaiOrderQualityFlag::kQuantityConflict);
            } else if (next == 0) {
                state.terminal = true;
            }
        } else if (active_continuous) {
            if (!CheckedAdd(state.pre_add_active_trade_quantity,
                            fact.quantity.raw, &next)) {
                result.ok = false;
                return result;
            }
            state.pre_add_active_trade_quantity = next;
        }
        MarkMutation(fact, &state, true);
        RefreshShanghaiState(&state);
        const OrderDeltaOperation operation = BeginOrderEmission(&state);
        result.eval.order_operation = operation;
        result.eval.post_state = state;
    } else if (fact.action == TickAction::kCancel) {
        if (!previous.has_value()) {
            result.eval.input_set_hash = CombineHashes(
                fact_hash, Identifier128{},
                static_cast<std::uint8_t>(role));
            return result;
        }
        OrderState state = *previous;
        result.eval.referenced_order_found = true;
        result.eval.resolved_side = state.snapshot.side;
        if (state.snapshot.side != fact.side) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kSideConflict);
        }
        if (state.terminal) {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kQuantityConflict);
        }
        std::int64_t next = 0;
        if (!CheckedAdd(state.snapshot.total_cancel_quantity,
                        fact.quantity.raw, &next)) {
            result.ok = false;
            return result;
        }
        state.snapshot.total_cancel_quantity = next;
        if (state.snapshot.add_seen) {
            if (!CheckedSubtract(state.snapshot.remaining_quantity,
                                 fact.quantity.raw, &next)) {
                result.ok = false;
                return result;
            }
            state.snapshot.remaining_quantity = next;
            state.snapshot.remaining_quantity_valid = next >= 0;
            if (next < 0) {
                state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                    ShanghaiOrderQualityFlag::kQuantityConflict);
            } else if (next == 0) {
                state.terminal = true;
            }
        } else {
            state.snapshot.quality_flags |= ShanghaiOrderQualityBit(
                ShanghaiOrderQualityFlag::kCancelWithoutAdd);
            state.terminal = true;
        }
        MarkMutation(fact, &state, true);
        RefreshShanghaiState(&state);
        const OrderDeltaOperation operation = BeginOrderEmission(&state);
        result.eval.order_operation = operation;
        result.eval.post_state = state;
    }

    if (result.eval.post_state.has_value()) {
        result.eval.post_state_hash = HashState(*result.eval.post_state);
        result.eval.input_set_hash = CombineHashes(
            previous.has_value() ? previous_input_hash : Identifier128{},
            fact_hash,
            static_cast<std::uint8_t>(role));
    } else if (IsZero(result.eval.input_set_hash)) {
        result.eval.input_set_hash = CombineHashes(
            fact_hash, Identifier128{},
            static_cast<std::uint8_t>(role));
    }
    return result;
}

[[nodiscard]] ApplyRoleResult ApplyOrderRole(
    const std::optional<OrderState>& previous,
    const CanonicalTick& fact,
    OrderRole role,
    Identifier128 previous_input_hash,
    Identifier128 fact_hash) noexcept {
    return fact.common.identity.market == Market::kShanghai
        ? ApplyShanghaiRole(previous, fact, role,
                            previous_input_hash, fact_hash)
        : ApplyShenzhenRole(previous, fact, role,
                            previous_input_hash, fact_hash);
}

[[nodiscard]] EventPayload SourcePayloadBase(
    const CanonicalTick& fact) noexcept {
    EventPayload payload{};
    payload.source_anchor = MakeAnchor(fact);
    payload.action = fact.action;
    payload.side = fact.side;
    payload.aggressor = fact.aggressor;
    payload.order_type = fact.order_type;
    payload.phase = fact.phase;
    payload.price_p6 = fact.price.p6;
    payload.price_valid = fact.price.p6_valid;
    payload.amount_p6 = fact.amount.p6;
    payload.amount_valid = fact.amount.p6_valid;
    payload.quantity = fact.quantity.raw;
    payload.quantity_valid = fact.quantity.valid;
    payload.matched_quantity = fact.sh_add_matched_quantity_raw;
    payload.matched_quantity_valid =
        (fact.validity & ingest::kTickMatchedQuantityValid) != 0U;
    payload.primary_order_id = fact.primary_order_id;
    payload.buy_order_id = fact.buy_order_id;
    payload.sell_order_id = fact.sell_order_id;
    payload.source_quality_flags = fact.common.quality_flags;
    return payload;
}

struct SourceFragment final {
    std::optional<EventKind> kind;
    EventPayload payload{};
};

// Source projection is independent of every order state.
[[nodiscard]] SourceFragment ProjectSource(
    const CanonicalTick& fact) noexcept {
    SourceFragment fragment{};
    fragment.payload = SourcePayloadBase(fact);
    if (fact.common.identity.market == Market::kShanghai) {
        switch (fact.action) {
            case TickAction::kTrade:
                fragment.kind = EventKind::kShanghaiTrade;
                break;
            case TickAction::kCancel:
                fragment.kind = EventKind::kShanghaiCancel;
                fragment.payload.referenced_order_found_valid = true;
                break;
            case TickAction::kStatus:
                fragment.kind = EventKind::kShanghaiStatus;
                if ((fact.validity & ingest::kTickPhaseValid) == 0U ||
                    fact.phase == TradingPhase::kUnknown) {
                    fragment.payload.event_quality_flags |=
                        ShanghaiOrderQualityBit(
                            ShanghaiOrderQualityFlag::kPhaseUnknown);
                }
                break;
            case TickAction::kAdd:
            case TickAction::kUnknown:
                break;
        }
    } else if (fact.common.identity.market == Market::kShenzhen) {
        if (fact.action == TickAction::kTrade) {
            fragment.kind = EventKind::kShenzhenTrade;
            fragment.payload.amount_p6 = 0;
            fragment.payload.amount_valid = false;
            if (fact.buy_order_id > 0 &&
                fact.buy_order_id == fact.sell_order_id) {
                fragment.payload.event_quality_flags |=
                    ShenzhenEventQualityBit(
                        ShenzhenEventQualityFlag::
                            kAmbiguousTradeOrderReferences);
            } else {
                if (fact.buy_order_id == 0) {
                    fragment.payload.event_quality_flags |=
                        ShenzhenEventQualityBit(
                            ShenzhenEventQualityFlag::
                                kUnknownBuyOrderReference);
                }
                if (fact.sell_order_id == 0) {
                    fragment.payload.event_quality_flags |=
                        ShenzhenEventQualityBit(
                            ShenzhenEventQualityFlag::
                                kUnknownSellOrderReference);
                }
            }
        } else if (fact.action == TickAction::kCancel) {
            fragment.kind = EventKind::kShenzhenCancel;
            fragment.payload.referenced_order_found_valid = true;
        }
    }
    return fragment;
}

}  // namespace

namespace {

struct StateVersion final {
    std::optional<OrderState> state;
    Identifier128 input_set_hash{};
};

struct OrderBaseline final {
    std::uint64_t compacted_before = 0U;
    std::uint64_t last_sequence = 0U;
    StateVersion version{};
};

struct OrderUseNode final {
    OrderRole role = OrderRole::kPrimary;
    RoleEval eval{};
    bool evaluated = false;
};

struct OrderHistory final {
    OrderBaseline baseline{};
    std::map<std::uint64_t, OrderUseNode> suffix;
    std::uint64_t generation = 0U;
    std::size_t instrument_order_slot = 0U;
    std::size_t active_end_slot = std::numeric_limits<std::size_t>::max();
};

using OrderHistoryTable = internal::LazyPagedHashMap<
    OrderKey, OrderHistory, OrderKeyHash>;
using OrderIndexList = internal::ExactList<OrderKey>;
using OrderRangeIndex = std::map<InstrumentChannelKey, OrderIndexList>;

struct FactRecord final {
    journal::FactHandle handle{};
    Identifier128 fact_hash{};
    std::deque<std::pair<OrderKey, OrderRole>> roles;
    std::uint32_t instrument_id = 0U;
    std::size_t accounted_bytes = 0U;
    TradingPhase projected_phase = TradingPhase::kUnknown;
    bool projectable = false;
    bool late = false;
    bool barrier = false;
    bool phase_status = false;
    bool projected_phase_valid = false;
    bool roles_ordered = true;
};

// Barrier callers establish uniqueness from OrderHistory creation: a new END
// sees each indexed order once, and a newly created order has never been added
// to any retained END. Appending keeps those paths amortized O(1).
void AppendUniqueFactRole(FactRecord* record,
                          const OrderKey& order,
                          OrderRole role) {
    const bool remains_ordered = record->roles_ordered &&
        (record->roles.empty() || record->roles.back().first < order);
    record->roles.emplace_back(order, role);
    record->roles_ordered = remains_ordered;
}

void UpsertFactRole(FactRecord* record,
                    const OrderKey& order,
                    OrderRole role) {
    const auto existing = std::find_if(
        record->roles.begin(), record->roles.end(),
        [&order](const auto& entry) { return entry.first == order; });
    if (existing != record->roles.end()) {
        existing->second = role;
        return;
    }
    AppendUniqueFactRole(record, order, role);
}

void EnsureFactRolesOrdered(FactRecord* record) {
    if (record->roles_ordered) {
        return;
    }
    std::sort(record->roles.begin(), record->roles.end(),
              [](const auto& left, const auto& right) {
                  return left.first < right.first;
              });
    record->roles_ordered = true;
}

struct Bundle final {
    std::map<EventKey, EventPayload> rows;
    std::map<EventKey, Identifier128> input_set_hashes;
};

[[nodiscard]] std::size_t BundleRevisionCount(
    const Bundle& old_bundle,
    const Bundle& new_bundle) noexcept {
    std::size_t count = 0U;
    auto old_row = old_bundle.rows.begin();
    auto new_row = new_bundle.rows.begin();
    while (old_row != old_bundle.rows.end() ||
           new_row != new_bundle.rows.end()) {
        if (new_row == new_bundle.rows.end() ||
            (old_row != old_bundle.rows.end() &&
             old_row->first < new_row->first)) {
            ++count;
            ++old_row;
            continue;
        }
        if (old_row == old_bundle.rows.end() ||
            new_row->first < old_row->first) {
            ++count;
            ++new_row;
            continue;
        }
        count += old_row->second == new_row->second ? 0U : 1U;
        ++old_row;
        ++new_row;
    }
    return count;
}

struct InstrumentFactIndex final {
    // Fact positions are append-ordered on the normal SequenceRecovery path.
    // A late insertion uses lower_bound and remains bounded by the per-
    // instrument journal; this avoids one tree node allocation per normal
    // fact while retaining ordered range traversal for phase repair.
    // The enclosing InstrumentChannelKey already owns every other FactKey
    // component.  Keeping only native_sequence avoids repeating 16 bytes of
    // partition identity for every fact retained during the trading day.
    std::deque<std::uint64_t> ordered;
};

struct PhaseIndex final {
    std::deque<std::pair<std::uint64_t, TradingPhase>> ordered;
    std::optional<std::pair<std::uint64_t, TradingPhase>> anchor;
};

void InsertInstrumentFact(InstrumentFactIndex* index,
                          std::uint64_t sequence) {
    auto& values = index->ordered;
    if (values.empty() || values.back() < sequence) {
        values.push_back(sequence);
        return;
    }
    const auto position = std::lower_bound(values.begin(), values.end(),
                                           sequence);
    if (position == values.end() || *position != sequence) {
        values.insert(position, sequence);
    }
}

void InsertPhase(PhaseIndex* index,
                 std::uint64_t sequence,
                 TradingPhase phase) {
    auto& values = index->ordered;
    if (values.empty() || values.back().first < sequence) {
        values.emplace_back(sequence, phase);
        return;
    }
    const auto position = std::lower_bound(
        values.begin(), values.end(), sequence,
        [](const auto& entry, std::uint64_t value) {
            return entry.first < value;
        });
    if (position == values.end() || position->first != sequence) {
        values.insert(position, std::pair{sequence, phase});
    } else {
        position->second = phase;
    }
}

struct EventHead final {
    Identifier128 revision_id{};
    Identifier128 payload_hash{};
    bool deleted = false;
};

[[nodiscard]] constexpr std::size_t SaturatingAdd(
    std::size_t left,
    std::size_t right) noexcept {
    return right > std::numeric_limits<std::size_t>::max() - left
        ? std::numeric_limits<std::size_t>::max()
        : left + right;
}

[[nodiscard]] constexpr std::size_t SaturatingMultiply(
    std::size_t left,
    std::size_t right) noexcept {
    return left != 0U &&
                   right > std::numeric_limits<std::size_t>::max() / left
        ? std::numeric_limits<std::size_t>::max()
        : left * right;
}

template <typename Value>
[[nodiscard]] constexpr std::size_t TreeNodeOwnedBytes() noexcept {
    // Three links, allocator bookkeeping and alignment are deliberately
    // included. This is a conservative logical-owned-byte accounting unit;
    // it is not an allocator-specific RSS measurement.
    return sizeof(Value) + 5U * sizeof(void*);
}

[[nodiscard]] constexpr std::size_t OrderUseNodeOwnedBytes() noexcept {
    return TreeNodeOwnedBytes<
        std::pair<const std::uint64_t, OrderUseNode>>();
}

[[nodiscard]] constexpr std::size_t OrderIndexTreeNodeOwnedBytes() noexcept {
    return TreeNodeOwnedBytes<
        std::pair<const InstrumentChannelKey, OrderIndexList>>();
}

template <typename Value>
[[nodiscard]] std::size_t DequeOwnedBytes(std::size_t count) noexcept {
    constexpr std::size_t kBlockBytes = 512U;
    constexpr std::size_t kValuesPerBlock = sizeof(Value) < kBlockBytes
        ? kBlockBytes / sizeof(Value)
        : 1U;
    const std::size_t blocks = SaturatingAdd(
        2U, count / kValuesPerBlock);
    const std::size_t map_slots = SaturatingMultiply(
        2U, SaturatingAdd(blocks, 8U));
    const std::size_t block_storage = SaturatingMultiply(
        SaturatingMultiply(blocks, kValuesPerBlock), sizeof(Value));
    return SaturatingAdd(
        block_storage,
        SaturatingMultiply(map_slots, sizeof(void*)));
}

using PhaseEntry = std::pair<std::uint64_t, TradingPhase>;
using BarrierRangeIndex = std::map<std::uint64_t, FactKey>;
using ChannelFactRangeIndex = std::map<std::uint64_t, FactKey>;
using BarrierTable = internal::LazyPagedHashMap<
    InstrumentChannelKey, BarrierRangeIndex, InstrumentChannelKeyHash>;
using InstrumentFactTable = internal::LazyPagedHashMap<
    InstrumentChannelKey, InstrumentFactIndex, InstrumentChannelKeyHash>;
using PhaseTable = internal::LazyPagedHashMap<
    InstrumentChannelKey, PhaseIndex, InstrumentChannelKeyHash>;
using ChannelFactTable = internal::LazyPagedHashMap<
    ChannelKey, ChannelFactRangeIndex, ChannelKeyHash>;

[[nodiscard]] std::size_t BarrierEntryOwnedBytes() noexcept {
    return TreeNodeOwnedBytes<BarrierRangeIndex::value_type>();
}

[[nodiscard]] std::size_t ChannelFactEntryOwnedBytes() noexcept {
    return TreeNodeOwnedBytes<ChannelFactRangeIndex::value_type>();
}

struct PendingCommit final {
    std::shared_ptr<const EventRevisionBatch> batch;
    std::size_t owned_bytes = 0U;
    std::uint64_t queued_monotonic_ns = 0U;
};

struct DirtyOrder final {
    std::set<std::uint64_t> new_sequences;
    bool late = false;
};

struct PhaseScanSpan final {
    InstrumentChannelKey range{};
    std::size_t next_index = 0U;
    std::size_t end_index = 0U;
};

struct EndExpansionTask final {
    FactKey fact{};
    InstrumentChannelKey range{};
    std::size_t candidate_count = 0U;
    std::size_t next_candidate = 0U;
    std::size_t staging_bytes = 0U;
    bool active_only = false;
};

enum class PendingCutStage : std::uint8_t {
    kNormalize = 0U,
    kRegisterInserted,
    kSeedOrders,
    kSeedEndOrders,
    kSeedBundles,
    kExpandComponents,
    kFinalize,
};

struct PendingProjectionCut final {
    std::vector<FactKey> inserted;
    std::map<ChannelKey, std::uint64_t> retention_requests;
    std::vector<PhaseScanSpan> phase_spans;
    std::map<OrderKey, DirtyOrder> phase_dirty_orders;
    std::map<FactKey, bool> phase_dirty_bundles;
    std::vector<EndExpansionTask> end_expansions;
    std::set<OrderKey> repair_orders;
    std::set<FactKey> repair_facts;
    std::deque<OrderKey> order_frontier;
    std::deque<FactKey> fact_frontier;
    std::optional<OrderKey> seed_order;
    std::optional<OrderKey> seed_end_order;
    std::optional<FactKey> seed_bundle;
    std::optional<OrderKey> active_order;
    std::optional<std::uint64_t> active_order_sequence;
    std::optional<FactKey> active_fact;
    std::size_t active_fact_role = 0U;
    std::size_t next_phase_span = 0U;
    std::size_t next_inserted = 0U;
    std::size_t seed_end_expansion = 0U;
    std::size_t accounted_bytes = 0U;
    PendingCutStage stage = PendingCutStage::kNormalize;
    bool source_only_fast = false;
    bool saw_conflict = false;
    bool saw_invalid = false;
};

struct RepairEvalPatch final {
    RoleEval eval{};
    std::uint64_t attempt_generation = 0U;
};

struct RepairOrderTask final {
    OrderKey key{};
    DirtyOrder dirty{};
    StateVersion previous{};
    std::optional<std::uint64_t> cursor;
    std::uint64_t observed_live_generation = 0U;
    std::uint64_t attempt_generation = 0U;
    bool initialized = false;
    bool complete = false;
};

struct RepairTransaction final {
    std::map<OrderKey, RepairOrderTask> tasks;
    // Only non-complete tasks are kept in this bounded work queue.  The old
    // implementation repeatedly scanned the whole task map after each slice,
    // which made a large repair fan-out proportional to completed work.
    std::deque<OrderKey> ready_orders;
    std::set<OrderKey> ready_order_set;
    std::map<RoleCacheKey, RepairEvalPatch> eval_patch;
    std::map<FactKey, bool> dirty_bundles;
    std::set<FactKey> inserted_facts;
    std::deque<EndExpansionTask> end_expansions;
    std::optional<OrderKey> next_order;
    std::size_t end_staging_bytes = 0U;
    std::size_t end_projected_rows = 0U;
    bool hole_fill = false;
};

struct EvictionTask final {
    ChannelKey channel{};
    std::uint64_t evict_before = 0U;
    std::optional<std::uint64_t> current_sequence;
};

[[nodiscard]] std::size_t PendingCommitPayloadOwnedBytes(
    std::size_t revision_capacity) noexcept {
    std::size_t bytes = sizeof(EventRevisionBatch) + 4U * sizeof(void*);
    return SaturatingAdd(
        bytes,
        SaturatingMultiply(revision_capacity, sizeof(EventRevision)));
}

[[nodiscard]] std::size_t RepairOwnedBytes(
    const RepairTransaction& repair) noexcept {
    std::size_t bytes = sizeof(RepairTransaction);
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            repair.tasks.size(),
            TreeNodeOwnedBytes<
                std::pair<const OrderKey, RepairOrderTask>>()));
    for (const auto& [key, task] : repair.tasks) {
        static_cast<void>(key);
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                task.dirty.new_sequences.size(),
                TreeNodeOwnedBytes<std::uint64_t>()));
    }
    bytes = SaturatingAdd(
        bytes, DequeOwnedBytes<OrderKey>(repair.ready_orders.size()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(repair.ready_order_set.size(),
                           TreeNodeOwnedBytes<OrderKey>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            repair.eval_patch.size(),
            TreeNodeOwnedBytes<
                std::pair<const RoleCacheKey, RepairEvalPatch>>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            repair.dirty_bundles.size(),
            TreeNodeOwnedBytes<std::pair<const FactKey, bool>>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(repair.inserted_facts.size(),
                           TreeNodeOwnedBytes<FactKey>()));
    bytes = SaturatingAdd(
        bytes,
        DequeOwnedBytes<EndExpansionTask>(repair.end_expansions.size()));
    return bytes;
}

[[nodiscard]] std::size_t PendingProjectionCutOwnedBytes(
    const PendingProjectionCut& cut) noexcept {
    std::size_t bytes = sizeof(PendingProjectionCut);
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(cut.inserted.capacity(), sizeof(FactKey)));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(cut.phase_spans.capacity(),
                           sizeof(PhaseScanSpan)));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(cut.end_expansions.capacity(),
                           sizeof(EndExpansionTask)));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            cut.retention_requests.size(),
            TreeNodeOwnedBytes<
                std::pair<const ChannelKey, std::uint64_t>>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            cut.phase_dirty_bundles.size(),
            TreeNodeOwnedBytes<std::pair<const FactKey, bool>>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(
            cut.phase_dirty_orders.size(),
            TreeNodeOwnedBytes<std::pair<const OrderKey, DirtyOrder>>()));
    for (const auto& [order, dirty] : cut.phase_dirty_orders) {
        static_cast<void>(order);
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                dirty.new_sequences.size(),
                TreeNodeOwnedBytes<std::uint64_t>()));
    }
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(cut.repair_orders.size(),
                           TreeNodeOwnedBytes<OrderKey>()));
    bytes = SaturatingAdd(
        bytes,
        SaturatingMultiply(cut.repair_facts.size(),
                           TreeNodeOwnedBytes<FactKey>()));
    bytes = SaturatingAdd(
        bytes, DequeOwnedBytes<OrderKey>(cut.order_frontier.size()));
    bytes = SaturatingAdd(
        bytes, DequeOwnedBytes<FactKey>(cut.fact_frontier.size()));
    return bytes;
}

[[nodiscard]] std::size_t PhaseFactWorkBytes(
    const FactRecord& record) noexcept {
    return SaturatingAdd(
        SaturatingAdd(sizeof(CanonicalTick), sizeof(FactRecord)),
        SaturatingMultiply(
            record.roles.size(), sizeof(std::pair<OrderKey, OrderRole>)));
}

[[nodiscard]] constexpr std::size_t EndCandidateStagingBytes() noexcept {
    return sizeof(OrderUseNode) +
        sizeof(std::pair<OrderKey, OrderRole>) +
        TreeNodeOwnedBytes<std::pair<const OrderKey, RepairOrderTask>>() +
        TreeNodeOwnedBytes<std::uint64_t>() +
        TreeNodeOwnedBytes<
            std::pair<const RoleCacheKey, RepairEvalPatch>>();
}

struct AtomicEventWorkerStats final {
    std::atomic<std::uint64_t> facts_journaled{0U};
    std::atomic<std::uint64_t> duplicate_facts{0U};
    std::atomic<std::uint64_t> source_conflicts{0U};
    std::atomic<std::uint64_t> live_order_uses{0U};
    std::atomic<std::uint64_t> repaired_order_uses{0U};
    std::atomic<std::uint64_t> repair_convergence_stops{0U};
    std::atomic<std::uint64_t> repair_slices{0U};
    std::atomic<std::uint64_t> repair_commits{0U};
    std::atomic<std::uint64_t> repair_order_restarts{0U};
    std::atomic<std::uint64_t> bundles_reassembled{0U};
    std::atomic<std::uint64_t> revisions_created{0U};
    std::atomic<std::uint64_t> tombstones_created{0U};
    std::atomic<std::uint64_t> pending_raw_commits{0U};
    std::atomic<std::uint64_t> revision_batches_submitted{0U};
    std::atomic<std::uint64_t> persistence_groups_submitted{0U};
    std::atomic<std::uint64_t> persistence_group_batches_max{0U};
    std::atomic<std::uint64_t> persistence_group_rows_max{0U};
    std::atomic<std::uint64_t> persistence_group_bytes_max{0U};
    std::atomic<std::uint64_t> pending_revision_bytes{0U};
    std::atomic<std::uint64_t> pending_revision_bytes_high_watermark{0U};
    std::atomic<std::uint64_t> active_repair_orders{0U};
    std::atomic<std::uint64_t> active_repair_bytes{0U};
    std::atomic<std::uint64_t> active_repair_bytes_high_watermark{0U};
    std::atomic<std::uint64_t> phase_normalization_slices{0U};
    std::atomic<std::uint64_t> phase_facts_scanned{0U};
    std::atomic<std::uint64_t> phase_dirty_roles_discovered{0U};
    std::atomic<std::uint64_t> pending_phase_bytes{0U};
    std::atomic<std::uint64_t> pending_phase_bytes_high_watermark{0U};
    std::atomic<std::uint64_t> ordered_batch_fast_path{0U};
    std::atomic<std::uint64_t> unordered_batch_sorts{0U};
    std::atomic<std::uint64_t> barrier_index_orders_visited{0U};
    std::atomic<std::uint64_t> end_expansion_slices{0U};
    std::atomic<std::uint64_t> end_candidates_processed{0U};
    std::atomic<std::uint64_t> source_only_fast_path{0U};
    std::atomic<std::uint64_t> order_uses_compacted{0U};
    std::atomic<std::uint64_t> facts_evicted{0U};
    std::atomic<std::uint64_t> eviction_slices{0U};
    std::atomic<std::uint64_t> hot_facts{0U};
    std::atomic<std::uint64_t> hot_fact_bytes{0U};
    std::atomic<std::uint64_t> hot_fact_bytes_high_watermark{0U};
    std::atomic<std::uint64_t> order_history_bytes{0U};
    std::atomic<std::uint64_t> order_history_bytes_high_watermark{0U};
    std::atomic<bool> repair_pending{false};
    std::atomic<bool> eviction_pending{false};
};

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
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

[[nodiscard]] bool StructurallyValid(
    const EventWorkerConfig& config,
    const EventInput& input) noexcept {
    const CanonicalTick& tick = input.tick;
    const Market market = tick.common.identity.market;
    if (!input.catalog_match || tick.common.trade_date != config.trade_date ||
        tick.common.instrument_id == 0U ||
        (market == Market::kShanghai && tick.common.channel == 0U) ||
        tick.common.native_sequence == 0U ||
        tick.common.ingress_sequence == 0U ||
        (market != Market::kShanghai && market != Market::kShenzhen) ||
        tick.common.instrument_ordinal == ingest::kInvalidInstrumentOrdinal ||
        tick.common.instrument_ordinal % config.owner_count != config.owner) {
        return false;
    }
    if (market == Market::kShanghai) {
        return tick.common.kind == CanonicalKind::kShanghaiTick;
    }
    return tick.common.kind == CanonicalKind::kShenzhenOrder ||
           tick.common.kind == CanonicalKind::kShenzhenTransaction;
}

[[nodiscard]] bool AdmissionValid(
    const EventWorkerConfig& config,
    const EventInput& input) noexcept {
    const EventAdmissionToken& token = input.admission;
    const std::uint64_t sequence = input.tick.common.native_sequence;
    if (token.feed_session_epoch != config.feed_session_epoch ||
        token.expected_sequence == 0U || token.admission_floor == 0U ||
        token.retention_floor == 0U || token.dispatch_fence == 0U ||
        token.admission_floor > token.expected_sequence ||
        sequence == std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    if (token.sequence_class == EventSequenceClass::kOrdered) {
        return sequence >= token.expected_sequence;
    }
    return token.sequence_class == EventSequenceClass::kHoleFill &&
           sequence >= token.admission_floor &&
           sequence < token.expected_sequence;
}

[[nodiscard]] bool DirectOrderSide(Side side) noexcept {
    return side == Side::kBuy || side == Side::kSell ||
           side == Side::kBorrow || side == Side::kLend;
}

[[nodiscard]] bool DirectOrderType(OrderType type) noexcept {
    return type == OrderType::kMarket || type == OrderType::kLimit ||
           type == OrderType::kSameSideBest;
}

[[nodiscard]] bool ProjectionInputValid(
    const CanonicalTick& fact) noexcept {
    const bool phase_valid =
        (fact.validity & ingest::kTickPhaseValid) != 0U;
    if (static_cast<std::uint8_t>(fact.phase) >
            static_cast<std::uint8_t>(TradingPhase::kEnded) ||
        (phase_valid && fact.phase == TradingPhase::kUnknown)) {
        return false;
    }
    const bool quantity_valid =
        (fact.validity & ingest::kTickQuantityValid) != 0U &&
        fact.quantity.valid && fact.quantity.scale == 0U &&
        fact.quantity.raw > 0;
    if (fact.common.identity.market == Market::kShanghai) {
        switch (fact.action) {
            case TickAction::kAdd:
                return (fact.side == Side::kBuy ||
                        fact.side == Side::kSell) &&
                       (fact.validity & ingest::kTickSideValid) != 0U &&
                       (fact.validity &
                        ingest::kTickPrimaryOrderIdValid) != 0U &&
                       fact.primary_order_id > 0 && quantity_valid &&
                       (fact.validity & ingest::kTickPriceValid) != 0U &&
                       fact.price.p6_valid && fact.price.p6 > 0 &&
                       ((fact.validity &
                         ingest::kTickMatchedQuantityValid) == 0U ||
                        fact.sh_add_matched_quantity_raw >= 0);
            case TickAction::kCancel:
                return (fact.side == Side::kBuy ||
                        fact.side == Side::kSell) &&
                       (fact.validity & ingest::kTickSideValid) != 0U &&
                       (fact.validity &
                        ingest::kTickPrimaryOrderIdValid) != 0U &&
                       fact.primary_order_id > 0 && quantity_valid;
            case TickAction::kTrade:
                return fact.buy_order_id > 0 && fact.sell_order_id > 0 &&
                       fact.buy_order_id != fact.sell_order_id &&
                       (fact.validity & ingest::kTickBuyOrderIdValid) != 0U &&
                       (fact.validity & ingest::kTickSellOrderIdValid) != 0U &&
                       quantity_valid && fact.price.p6_valid &&
                       (fact.validity & ingest::kTickPriceValid) != 0U &&
                       fact.price.p6 > 0 &&
                       static_cast<std::uint8_t>(fact.aggressor) <=
                           static_cast<std::uint8_t>(Aggressor::kNeutral);
            case TickAction::kStatus:
                return true;
            case TickAction::kUnknown:
                return false;
        }
    }

    switch (fact.action) {
        case TickAction::kAdd:
            if (fact.common.kind != CanonicalKind::kShenzhenOrder ||
                !quantity_valid || fact.primary_order_id <= 0 ||
                (fact.validity &
                 ingest::kTickPrimaryOrderIdValid) == 0U ||
                (fact.validity & ingest::kTickSideValid) == 0U ||
                (fact.validity & ingest::kTickOrderTypeValid) == 0U ||
                static_cast<std::uint64_t>(fact.primary_order_id) !=
                    fact.common.native_sequence ||
                fact.buy_order_id != 0 || fact.sell_order_id != 0 ||
                !DirectOrderSide(fact.side) ||
                !DirectOrderType(fact.order_type)) {
                return false;
            }
            return fact.order_type == OrderType::kLimit
                ? (fact.validity & ingest::kTickPriceValid) != 0U &&
                      fact.price.p6_valid && fact.price.p6 > 0
                : (fact.validity & ingest::kTickPriceValid) == 0U &&
                      !fact.price.p6_valid && fact.price.p6 == 0;
        case TickAction::kTrade:
            return fact.common.kind ==
                       CanonicalKind::kShenzhenTransaction &&
                   (fact.validity & ingest::kTickSideValid) == 0U &&
                   fact.side == Side::kUnknown &&
                   (fact.validity & ingest::kTickOrderTypeValid) == 0U &&
                   fact.order_type == OrderType::kUnknown &&
                   quantity_valid && fact.price.p6_valid &&
                   (fact.validity & ingest::kTickPriceValid) != 0U &&
                   fact.price.p6 > 0 &&
                   fact.primary_order_id == 0 &&
                   fact.buy_order_id >= 0 && fact.sell_order_id >= 0;
        case TickAction::kCancel: {
            const bool has_buy = fact.buy_order_id > 0;
            const bool has_sell = fact.sell_order_id > 0;
            return fact.common.kind ==
                       CanonicalKind::kShenzhenTransaction &&
                   quantity_valid && fact.primary_order_id > 0 &&
                   (fact.validity &
                    ingest::kTickPrimaryOrderIdValid) != 0U &&
                   (fact.validity & ingest::kTickSideValid) != 0U &&
                   (fact.side == Side::kBuy ||
                    fact.side == Side::kSell) &&
                   (fact.validity & ingest::kTickOrderTypeValid) == 0U &&
                   fact.order_type == OrderType::kUnknown &&
                   (fact.validity & ingest::kTickPriceValid) == 0U &&
                   !fact.price.p6_valid && fact.price.p6 == 0 &&
                   fact.buy_order_id >= 0 && fact.sell_order_id >= 0 &&
                   has_buy != has_sell &&
                   fact.primary_order_id ==
                       (has_buy ? fact.buy_order_id
                                : fact.sell_order_id) &&
                   fact.side ==
                       (has_buy ? Side::kBuy : Side::kSell);
        }
        case TickAction::kStatus:
        case TickAction::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] InstrumentChannelKey InstrumentChannel(
    const CanonicalTick& fact) noexcept {
    return InstrumentChannelKey{fact.common.trade_date,
                                fact.common.identity.market,
                                fact.common.instrument_id,
                                fact.common.channel};
}

[[nodiscard]] InstrumentChannelKey InstrumentChannel(
    const FactKey& key,
    const FactRecord& fact) noexcept {
    return InstrumentChannelKey{key.trade_date, key.market,
                                fact.instrument_id, key.channel};
}

[[nodiscard]] bool RoleObservableEqual(const RoleEval& left,
                                       const RoleEval& right) noexcept {
    return left.post_state == right.post_state &&
           left.order_operation == right.order_operation &&
           left.source_quality_contribution ==
               right.source_quality_contribution &&
           left.referenced_order_found == right.referenced_order_found &&
           left.resolved_side == right.resolved_side &&
           left.side_from_order == right.side_from_order;
}

[[nodiscard]] StateVersion LatestBefore(const OrderHistory& history,
                                        std::uint64_t sequence) noexcept {
    const auto position = history.suffix.lower_bound(sequence);
    if (position != history.suffix.begin()) {
        const OrderUseNode& node = std::prev(position)->second;
        if (node.evaluated) {
            return StateVersion{
                node.eval.post_state, node.eval.input_set_hash};
        }
    }
    if (history.baseline.last_sequence != 0U &&
        history.baseline.last_sequence < sequence) {
        return history.baseline.version;
    }
    return {};
}

[[nodiscard]] const OrderState* LatestOrderState(
    const OrderHistory& history) noexcept {
    for (auto use = history.suffix.rbegin();
         use != history.suffix.rend(); ++use) {
        if (use->second.evaluated) {
            return use->second.eval.post_state.has_value()
                ? &*use->second.eval.post_state
                : nullptr;
        }
    }
    return history.baseline.version.state.has_value()
        ? &*history.baseline.version.state
        : nullptr;
}

[[nodiscard]] bool SetOrderUse(OrderHistory* history,
                               std::uint64_t sequence,
                               OrderRole role) {
    if (sequence == 0U ||
        sequence < history->baseline.compacted_before) {
        return false;
    }
    auto existing = history->suffix.lower_bound(sequence);
    if (existing != history->suffix.end() &&
        existing->first == sequence &&
        existing->second.role == role) {
        return true;
    }
    if (history->generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    if (existing == history->suffix.end()) {
        history->suffix.emplace_hint(
            existing, sequence, OrderUseNode{role});
    } else if (existing->first != sequence) {
        history->suffix.emplace_hint(
            existing, sequence, OrderUseNode{role});
    } else {
        existing->second.role = role;
        existing->second.evaluated = false;
        existing->second.eval = {};
    }
    ++history->generation;
    return true;
}

[[nodiscard]] OrderUseNode* FindOrderUse(OrderHistory* history,
                                         std::uint64_t sequence) noexcept {
    auto position = history->suffix.find(sequence);
    return position != history->suffix.end()
        ? &position->second
        : nullptr;
}

[[nodiscard]] const OrderUseNode* FindOrderUse(
    const OrderHistory& history,
    std::uint64_t sequence) noexcept {
    const auto position = history.suffix.find(sequence);
    return position != history.suffix.end()
        ? &position->second
        : nullptr;
}

[[nodiscard]] Identifier128 RecoveryRunIdentifier(
    Identifier128 calculation_run_id,
    std::uint32_t owner,
    std::uint64_t batch_sequence) noexcept {
    std::array<std::byte, 28U> input{};
    std::copy(calculation_run_id.bytes.begin(),
              calculation_run_id.bytes.end(), input.begin());
    std::size_t offset = 16U;
    for (std::size_t index = 0U; index < sizeof(owner); ++index) {
        input[offset++] = static_cast<std::byte>(owner >> (index * 8U));
    }
    for (std::size_t index = 0U; index < sizeof(batch_sequence); ++index) {
        input[offset++] = static_cast<std::byte>(
            batch_sequence >> (index * 8U));
    }
    return clickhouse::Blake3Hash128(input);
}

[[nodiscard]] Identifier128 RevisionIdentifier(
    Identifier128 calculation_run_id,
    Identifier128 recovery_run_id,
    std::uint64_t version,
    Identifier128 payload_hash,
    RevisionOperation operation) noexcept {
    std::array<std::byte, 57U> input{};
    std::copy(calculation_run_id.bytes.begin(),
              calculation_run_id.bytes.end(), input.begin());
    std::copy(recovery_run_id.bytes.begin(), recovery_run_id.bytes.end(),
              input.begin() + 16U);
    std::size_t offset = 32U;
    for (std::size_t index = 0U; index < sizeof(version); ++index) {
        input[offset++] = static_cast<std::byte>(version >> (index * 8U));
    }
    std::copy(payload_hash.bytes.begin(), payload_hash.bytes.end(),
              input.begin() + static_cast<std::ptrdiff_t>(offset));
    offset += payload_hash.bytes.size();
    input[offset] = static_cast<std::byte>(operation);
    return clickhouse::Blake3Hash128(input);
}

}  // namespace

class EventWorker::Impl final {
public:
    Impl(EventWorkerConfig config, EventRevisionSink* sink)
        : config_(std::move(config)),
          sink_(sink),
          barriers_(config_.maximum_hot_facts),
          instrument_facts_(config_.maximum_hot_facts),
          phase_statuses_(config_.maximum_hot_facts),
          order_histories_(config_.maximum_carry_orders),
          channel_facts_(config_.maximum_hot_facts),
          order_history_bytes_(order_histories_.directory_owned_bytes()) {
        const auto reserve_bounded = [](auto* table, std::size_t limit) {
            constexpr std::size_t kMaximumInitialBuckets = 65'536U;
            table->max_load_factor(0.80F);
            table->reserve(std::min(limit, kMaximumInitialBuckets));
        };
        reserve_bounded(&facts_, 65'536U);
        batch_tick_cache_.reserve(65'536U);
        reserve_bounded(&channel_states_, 1'024U);
        reserve_bounded(&bundle_cache_, 65'536U);
        reserve_bounded(&event_heads_, 65'536U);
        reserve_bounded(&gap_ranges_, 1'024U);
        hot_index_bytes_ = SaturatingAdd(
            SaturatingAdd(barriers_.directory_owned_bytes(),
                          instrument_facts_.directory_owned_bytes()),
            SaturatingAdd(phase_statuses_.directory_owned_bytes(),
                          channel_facts_.directory_owned_bytes()));
        hot_fact_bytes_ = hot_index_bytes_;
        pending_revision_bytes_ = DequeOwnedBytes<PendingCommit>(0U);
        if (pending_revision_bytes_ >
            config_.maximum_pending_revision_bytes) {
            throw std::length_error(
                "Event pending-commit deque exceeds byte cap");
        }
        if (order_history_bytes_ >
            config_.maximum_order_history_bytes) {
            throw std::length_error(
                "Event order-history directory exceeds byte cap");
        }
        if (hot_fact_bytes_ > config_.maximum_hot_fact_bytes) {
            throw std::length_error(
                "Event hot-index directory exceeds byte cap");
        }
        PublishPendingRevisionBytes();
        PublishOrderHistoryBytes();
        PublishHotFactBytes();
    }

    [[nodiscard]] EventApplyResult ApplyBatch(
        std::span<const EventInput> inputs) noexcept {
        EventApplyResult result{};
        if (!healthy_) {
            result.code = EventApplyCode::kFailed;
            return result;
        }
        if (projection_input_fenced()) {
            return Failure(
                &result,
                "Event owner input overtook a projection fence");
        }
        if (inputs.empty()) {
            result.code = EventApplyCode::kDuplicateOnly;
            return result;
        }
        if (!AdvanceDurableCommits()) {
            result.code = EventApplyCode::kSinkFailed;
            return result;
        }
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            result.code = EventApplyCode::kCapacityExhausted;
            return result;
        }

        try {
            std::map<ChannelKey, std::pair<std::uint64_t, std::uint64_t>>
                protocol_cursors;
            for (const EventInput& input : inputs) {
                if (!StructurallyValid(config_, input)) {
                    return Failure(
                        &result, "Event input is structurally invalid");
                }
                if (!AdmissionValid(config_, input)) {
                    return Failure(&result,
                                   "Event admission token is invalid");
                }
                const ChannelKey channel{
                    config_.trade_date,
                    input.tick.common.identity.market,
                    input.tick.common.channel};
                const auto live = channel_states_.find(channel);
                auto [cursor, inserted_cursor] = protocol_cursors.try_emplace(
                    channel,
                    live == channel_states_.end()
                        ? std::pair<std::uint64_t, std::uint64_t>{0U, 0U}
                        : std::pair{
                              live->second.generation,
                              live->second.applied_dispatch_fence});
                static_cast<void>(inserted_cursor);
                bool generation_valid = false;
                if (input.admission.sequence_class ==
                    EventSequenceClass::kOrdered) {
                    generation_valid =
                        input.admission.generation == cursor->second.first;
                } else {
                    const auto gaps = gap_ranges_.find(channel);
                    if (gaps != gap_ranges_.end()) {
                        const auto gap = gaps->second.find(
                            input.admission.generation);
                        generation_valid = gap != gaps->second.end() &&
                            input.tick.common.native_sequence >=
                                gap->second.first &&
                            input.tick.common.native_sequence <=
                                gap->second.second;
                    }
                }
                if (!generation_valid ||
                    input.admission.dispatch_fence <= cursor->second.second ||
                    (live != channel_states_.end() &&
                     input.tick.common.native_sequence <
                         live->second.sealed_before)) {
                    return Failure(
                        &result,
                        "Event admission generation/fence is stale");
                }
                cursor->second.second = input.admission.dispatch_fence;
            }

            batch_tick_cache_.clear();
            std::vector<FactKey> inserted;
            inserted.reserve(inputs.size());
            std::vector<CanonicalTick> journal_ticks;
            journal_ticks.reserve(inputs.size());
            std::map<ChannelKey, std::uint64_t> cut_frontiers;
            std::map<ChannelKey, std::uint64_t> retention_requests;
            bool saw_conflict = false;
            bool saw_invalid = false;
            bool source_only_candidate = order_histories_.empty();

            // Capture every touched frontier before this cut changes it, then
            // journal all facts before registering or applying any role.
            for (const EventInput& input : inputs) {
                if (input.tick.common.identity.market != Market::kShanghai ||
                    input.tick.action != TickAction::kStatus ||
                    input.admission.sequence_class ==
                        EventSequenceClass::kHoleFill) {
                    source_only_candidate = false;
                }
                const FactKey key = MakeFactKey(input.tick);
                const ChannelKey channel{
                    key.trade_date, key.market, key.channel};
                const auto channel_state = channel_states_.find(channel);
                cut_frontiers.try_emplace(
                    channel,
                    channel_state == channel_states_.end()
                        ? 0U
                        : channel_state->second.projected_frontier);
                journal_ticks.push_back(input.tick);
            }

            std::vector<CanonicalTick> journal_winners(
                journal_ticks.size());
            const std::vector<journal::AdmitResult> admissions =
                config_.fact_journal->AdmitBatch(
                    journal::FactConsumer::kEvent, journal_ticks,
                    journal_winners);
            if (admissions.size() != journal_ticks.size()) {
                return Failure(
                    &result, "Event FactJournal batch admission failed: " +
                        config_.fact_journal->fatal_error());
            }

            std::size_t admission_index = 0U;
            for (const EventInput& input : inputs) {
                const FactKey key = MakeFactKey(input.tick);
                const ChannelKey channel{
                    key.trade_date, key.market, key.channel};
                EventChannelState& channel_state = channel_states_[channel];
                channel_state.feed_session_epoch =
                    config_.feed_session_epoch;
                channel_state.expected_sequence = std::max(
                    channel_state.expected_sequence,
                    std::max(input.admission.expected_sequence,
                             key.native_sequence + 1U));
                channel_state.generation = std::max(
                    channel_state.generation, input.admission.generation);
                channel_state.admission_floor = std::max(
                    channel_state.admission_floor,
                    input.admission.admission_floor);
                channel_state.applied_dispatch_fence =
                    input.admission.dispatch_fence;
                auto [request, new_request] = retention_requests.try_emplace(
                    channel, input.admission.retention_floor);
                if (!new_request) {
                    request->second = std::max(
                        request->second, input.admission.retention_floor);
                }
                const std::size_t current_admission = admission_index++;
                const journal::AdmitResult& admission =
                    admissions[current_admission];
                if (admission.code == journal::AdmitCode::kDuplicate) {
                    ++result.duplicates;
                    ++stats_.duplicate_facts;
                    continue;
                }
                if (admission.code == journal::AdmitCode::kConflict) {
                    ++stats_.source_conflicts;
                    saw_conflict = true;
                    continue;
                }
                if (admission.code == journal::AdmitCode::kFailed) {
                    return Failure(
                        &result,
                        "Event FactJournal admission failed: " +
                            config_.fact_journal->fatal_error());
                }
                if (facts_.contains(key)) {
                    return Failure(
                        &result,
                        "Event FactJournal consumer state diverged from "
                        "Event metadata");
                }
                if (facts_.size() >= config_.maximum_hot_facts) {
                    return CapacityFailure(
                        &result, "Event hot fact capacity exhausted");
                }
                const CanonicalTick& winner =
                    journal_winners[current_admission];
                if (MakeFactKey(winner) != key) {
                    return Failure(
                        &result,
                        "Event FactJournal returned a mismatched fact handle");
                }
                FactRecord record{};
                record.handle = admission.handle;
                record.fact_hash = HashFact(winner);
                record.instrument_id = winner.common.instrument_id;
                record.projected_phase = winner.phase;
                record.projected_phase_valid =
                    (winner.validity & ingest::kTickPhaseValid) != 0U;
                record.projectable = ProjectionInputValid(winner);
                record.phase_status =
                    key.market == Market::kShanghai &&
                    winner.action == TickAction::kStatus &&
                    record.projected_phase_valid;
                const InstrumentChannelKey instrument_range =
                    InstrumentChannel(winner);
                saw_invalid = saw_invalid || !record.projectable;
                source_only_candidate = source_only_candidate &&
                    record.projectable;
                record.late = input.admission.sequence_class ==
                                  EventSequenceClass::kHoleFill ||
                    key.native_sequence <= cut_frontiers.at(channel);
                if (record.projectable) {
                    record.barrier =
                        key.market == Market::kShanghai &&
                        winner.action == TickAction::kStatus &&
                        (winner.validity & ingest::kTickPhaseValid) != 0U &&
                        winner.phase == TradingPhase::kEnded;
                }
                record.accounted_bytes = FactOwnedBytes(
                    record, nullptr, 0U);
                if (record.accounted_bytes >
                        config_.maximum_hot_fact_bytes ||
                    hot_fact_bytes_ >
                        config_.maximum_hot_fact_bytes -
                            record.accounted_bytes) {
                    return CapacityFailure(
                        &result, "Event hot fact byte capacity exhausted");
                }
                batch_tick_cache_.emplace_back(key, winner);
                facts_.emplace(key, std::move(record));
                hot_fact_bytes_ += facts_.at(key).accounted_bytes;
                stats_.hot_facts.store(
                    facts_.size(), std::memory_order_relaxed);
                PublishHotFactBytes();
                if (!InsertChannelFact(channel, key, &result)) {
                    return result;
                }
                inserted.push_back(key);
                if (!InsertInstrumentFactIndex(
                        instrument_range, key.native_sequence, &result)) {
                    return result;
                }
                if (facts_.at(key).phase_status &&
                    !InsertPhaseIndex(
                        instrument_range, key.native_sequence,
                        winner.phase, &result)) {
                    return result;
                }
                channel_state.journal_tail = std::max(
                    channel_state.journal_tail, key.native_sequence);
                ++result.facts_inserted;
                ++stats_.facts_journaled;
            }
            if (admission_index != admissions.size()) {
                return Failure(
                    &result,
                    "Event FactJournal admission/result count diverged");
            }

            if (inserted.empty()) {
                static_cast<void>(AdvanceDurableCommits());
                for (const auto& [channel, floor] : retention_requests) {
                    QueueEviction(channel, floor);
                }
                if (!ContinueEviction()) {
                    result.code = EventApplyCode::kFailed;
                    return result;
                }
                result.code = saw_conflict
                    ? EventApplyCode::kSourceConflict
                    : (saw_invalid ? EventApplyCode::kInvalidInput
                                   : EventApplyCode::kDuplicateOnly);
                return result;
            }

            if (std::is_sorted(inserted.begin(), inserted.end())) {
                ++stats_.ordered_batch_fast_path;
            } else {
                std::sort(inserted.begin(), inserted.end());
                std::sort(
                    batch_tick_cache_.begin(), batch_tick_cache_.end(),
                    [](const auto& left, const auto& right) {
                        return left.first < right.first;
                    });
                ++stats_.unordered_batch_sorts;
            }
            const bool source_only_fast = source_only_candidate &&
                order_histories_.empty();
            if (source_only_fast) {
                ++stats_.source_only_fast_path;
            }
            PendingProjectionCut cut{};
            cut.inserted = std::move(inserted);
            cut.retention_requests = std::move(retention_requests);
            cut.source_only_fast = source_only_fast;
            cut.saw_conflict = saw_conflict;
            cut.saw_invalid = saw_invalid;
            if (!BuildPhaseScanSpans(&cut, &result)) {
                return result;
            }
            cut.accounted_bytes = PendingProjectionCutOwnedBytes(cut);
            if (!InstallPendingProjectionCut(std::move(cut), &result)) {
                return result;
            }
            if (!AdvancePendingProjectionCut(&result)) {
                return result;
            }
            if (pending_projection_cut_.has_value()) {
                result.code = saw_conflict
                    ? EventApplyCode::kSourceConflict
                    : (saw_invalid ? EventApplyCode::kInvalidInput
                                   : EventApplyCode::kApplied);
            }
            if (!pending_projection_cut_.has_value() &&
                active_repair_.has_value() &&
                !AdvanceRepairSlice(&result)) {
                return result;
            }
            result.repair_pending = repair_pending();
            return result;
        } catch (const std::bad_alloc&) {
            return CapacityFailure(&result,
                                   "Event worker allocation failed");
        } catch (const std::exception& exception) {
            return Failure(&result,
                           std::string("Event worker failed: ") +
                               exception.what());
        } catch (...) {
            return Failure(&result,
                           "Event worker failed with unknown exception");
        }
    }

    [[nodiscard]] bool ApplyGapOpen(const GapOpen& gap) noexcept {
        if (!healthy_) {
            return false;
        }
        if (projection_input_fenced()) {
            SetFatal("Event GapOpen overtook a projection fence");
            return false;
        }
        if (gap.feed_session_epoch != config_.feed_session_epoch ||
            (gap.market == Market::kShanghai && gap.channel == 0U) ||
            gap.first_missing == 0U ||
            gap.last_missing < gap.first_missing || gap.generation == 0U ||
            gap.dispatch_fence == 0U ||
            (gap.market != Market::kShanghai &&
             gap.market != Market::kShenzhen)) {
            SetFatal("Event GapOpen is invalid");
            return false;
        }
        try {
            const ChannelKey channel{
                config_.trade_date, gap.market, gap.channel};
            EventChannelState& state = channel_states_[channel];
            if (gap.dispatch_fence <= state.applied_dispatch_fence ||
                gap.generation <= state.generation) {
                SetFatal("Event GapOpen generation/fence is stale");
                return false;
            }
            auto& ranges = gap_ranges_[channel];
            if (!ranges.emplace(
                    gap.generation,
                    std::pair{gap.first_missing, gap.last_missing}).second) {
                SetFatal("Event GapOpen generation is duplicated");
                return false;
            }
            state.feed_session_epoch = config_.feed_session_epoch;
            state.generation = gap.generation;
            state.applied_dispatch_fence = gap.dispatch_fence;
            state.gap_open = true;
            return true;
        } catch (...) {
            SetFatal("Event GapOpen allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool ApplyChannelSeal(const ChannelSeal& seal) noexcept {
        if (!healthy_) {
            return false;
        }
        if (projection_input_fenced()) {
            SetFatal("Event ChannelSeal overtook a projection fence");
            return false;
        }
        if (seal.feed_session_epoch != config_.feed_session_epoch ||
            (seal.market == Market::kShanghai && seal.channel == 0U) ||
            seal.evict_before == 0U ||
            seal.dispatch_fence == 0U ||
            (seal.market != Market::kShanghai &&
             seal.market != Market::kShenzhen)) {
            SetFatal("Event ChannelSeal is invalid");
            return false;
        }
        try {
            const ChannelKey channel{
                config_.trade_date, seal.market, seal.channel};
            EventChannelState& state = channel_states_[channel];
            if (seal.dispatch_fence <= state.applied_dispatch_fence ||
                seal.generation != state.generation ||
                seal.evict_before < state.sealed_before) {
                SetFatal("Event ChannelSeal generation/fence is stale");
                return false;
            }
            state.feed_session_epoch = config_.feed_session_epoch;
            state.applied_dispatch_fence = seal.dispatch_fence;
            QueueEviction(channel, seal.evict_before);
            return ContinueEviction();
        } catch (...) {
            SetFatal("Event ChannelSeal allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool ContinueEviction() noexcept {
        if (!healthy_) {
            return false;
        }
        if (pending_projection_cut_.has_value() ||
            active_repair_.has_value() || eviction_ready_.empty()) {
            stats_.eviction_pending.store(
                !eviction_tasks_.empty(), std::memory_order_release);
            return true;
        }
        ++stats_.eviction_slices;
        std::size_t nodes = 0U;
        std::size_t bytes = 0U;
        try {
            while (!eviction_ready_.empty()) {
                const ChannelKey channel = eviction_ready_.front();
                eviction_ready_.pop_front();
                eviction_ready_set_.erase(channel);
                auto task_position = eviction_tasks_.find(channel);
                if (task_position == eviction_tasks_.end()) {
                    continue;
                }
                EvictionTask& task = task_position->second;
                ChannelFactRangeIndex* channel_facts =
                    channel_facts_.Find(channel);
                bool complete = channel_facts == nullptr ||
                    channel_facts->empty() ||
                    channel_facts->begin()->first >=
                        task.evict_before;
                while (!complete) {
                    auto fact_index = task.current_sequence.has_value()
                        ? channel_facts->find(*task.current_sequence)
                        : channel_facts->begin();
                    if (fact_index == channel_facts->end() ||
                        fact_index->first >= task.evict_before) {
                        complete = true;
                        break;
                    }
                    task.current_sequence = fact_index->first;
                    const FactKey key = fact_index->second;
                    const auto fact_position = facts_.find(key);
                    if (fact_position == facts_.end()) {
                        SetFatal("Event eviction FactIndex diverged");
                        return false;
                    }
                    FactRecord& record = fact_position->second;
                    while (!record.roles.empty()) {
                        const OrderKey order = record.roles.front().first;
                        const std::size_t order_bytes_before =
                            order_history_bytes_;
                        const std::size_t hot_bytes_before = hot_fact_bytes_;
                        if (!CompactOrderUse(order, key.native_sequence)) {
                            SetFatal("Event order baseline compaction failed");
                            return false;
                        }
                        if (order_history_bytes_ > order_bytes_before) {
                            SetFatal(
                                "Event order compaction accounting increased");
                            return false;
                        }
                        const std::size_t released_order_bytes =
                            order_bytes_before - order_history_bytes_;
                        record.roles.pop_front();
                        if (!RefreshHotFactAccounting(key)) {
                            SetFatal("Event eviction role accounting failed");
                            return false;
                        }
                        if (hot_fact_bytes_ > hot_bytes_before) {
                            SetFatal(
                                "Event role eviction accounting increased");
                            return false;
                        }
                        ++nodes;
                        bytes = SaturatingAdd(
                            bytes,
                            SaturatingAdd(
                                released_order_bytes,
                                hot_bytes_before - hot_fact_bytes_));
                        if (nodes >= config_.eviction_slice_max_nodes ||
                            bytes >= config_.eviction_slice_max_bytes) {
                            break;
                        }
                    }
                    if (!record.roles.empty()) {
                        break;
                    }
                    if (nodes >= config_.eviction_slice_max_nodes ||
                        bytes >= config_.eviction_slice_max_bytes) {
                        break;
                    }

                    auto bundle = bundle_cache_.find(key);
                    while (bundle != bundle_cache_.end() &&
                           !bundle->second.rows.empty()) {
                        const std::size_t hot_bytes_before = hot_fact_bytes_;
                        const EventKey event =
                            bundle->second.rows.begin()->first;
                        bundle->second.rows.erase(
                            bundle->second.rows.begin());
                        const std::size_t erased_hash =
                            bundle->second.input_set_hashes.erase(event);
                        if (erased_hash != 1U || cached_event_count_ == 0U ||
                            !RefreshHotFactAccounting(key)) {
                            SetFatal(
                                "Event eviction bundle accounting failed");
                            return false;
                        }
                        if (hot_fact_bytes_ > hot_bytes_before) {
                            SetFatal(
                                "Event bundle eviction accounting increased");
                            return false;
                        }
                        --cached_event_count_;
                        ++nodes;
                        bytes = SaturatingAdd(
                            bytes, hot_bytes_before - hot_fact_bytes_);
                        if (nodes >= config_.eviction_slice_max_nodes ||
                            bytes >= config_.eviction_slice_max_bytes) {
                            break;
                        }
                    }
                    if (bundle != bundle_cache_.end() &&
                        !bundle->second.rows.empty()) {
                        break;
                    }
                    if (bundle != bundle_cache_.end() &&
                        !bundle->second.input_set_hashes.empty()) {
                        SetFatal("Event eviction Bundle maps diverged");
                        return false;
                    }
                    if (nodes >= config_.eviction_slice_max_nodes ||
                        bytes >= config_.eviction_slice_max_bytes) {
                        break;
                    }

                    auto heads = event_heads_.find(key);
                    while (heads != event_heads_.end() &&
                           !heads->second.empty()) {
                        const std::size_t hot_bytes_before = hot_fact_bytes_;
                        heads->second.erase(heads->second.begin());
                        if (!RefreshHotFactAccounting(key)) {
                            SetFatal("Event eviction head accounting failed");
                            return false;
                        }
                        if (hot_fact_bytes_ > hot_bytes_before) {
                            SetFatal(
                                "Event head eviction accounting increased");
                            return false;
                        }
                        ++nodes;
                        bytes = SaturatingAdd(
                            bytes, hot_bytes_before - hot_fact_bytes_);
                        if (nodes >= config_.eviction_slice_max_nodes ||
                            bytes >= config_.eviction_slice_max_bytes) {
                            break;
                        }
                    }
                    if (heads != event_heads_.end() &&
                        !heads->second.empty()) {
                        break;
                    }
                    if (nodes >= config_.eviction_slice_max_nodes ||
                        bytes >= config_.eviction_slice_max_bytes) {
                        break;
                    }
                    const std::size_t hot_bytes_before = hot_fact_bytes_;
                    if (!EraseFactState(key, record)) {
                        SetFatal("Event fact eviction failed");
                        return false;
                    }
                    if (hot_fact_bytes_ > hot_bytes_before) {
                        SetFatal(
                            "Event fact eviction accounting increased");
                        return false;
                    }
                    if (!EraseChannelFact(channel, key.native_sequence)) {
                        SetFatal("Event channel fact eviction failed");
                        return false;
                    }
                    channel_facts = channel_facts_.Find(channel);
                    task.current_sequence.reset();
                    ++nodes;
                    bytes = SaturatingAdd(
                        bytes, hot_bytes_before - hot_fact_bytes_);
                    complete = channel_facts == nullptr ||
                        channel_facts->empty() ||
                        channel_facts->begin()->first >=
                            task.evict_before;
                    if (nodes >= config_.eviction_slice_max_nodes ||
                        bytes >= config_.eviction_slice_max_bytes) {
                        break;
                    }
                }

                if (complete) {
                    EventChannelState& state = channel_states_.at(channel);
                    state.sealed_before = std::max(
                        state.sealed_before, task.evict_before);
                    state.eviction_target = state.sealed_before;
                    const auto ranges = gap_ranges_.find(channel);
                    if (ranges != gap_ranges_.end()) {
                        auto range = ranges->second.begin();
                        while (range != ranges->second.end()) {
                            if (range->second.second < state.sealed_before) {
                                range = ranges->second.erase(range);
                            } else {
                                ++range;
                            }
                        }
                        state.gap_open = !ranges->second.empty();
                        if (ranges->second.empty()) {
                            gap_ranges_.erase(ranges);
                        }
                    } else {
                        state.gap_open = false;
                    }
                    eviction_tasks_.erase(task_position);
                } else if (eviction_ready_set_.insert(channel).second) {
                    eviction_ready_.push_back(channel);
                }

                if (nodes >= config_.eviction_slice_max_nodes ||
                    bytes >= config_.eviction_slice_max_bytes) {
                    break;
                }
            }
            stats_.eviction_pending.store(
                !eviction_tasks_.empty(), std::memory_order_release);
            return true;
        } catch (...) {
            SetFatal("Event eviction allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool AdvanceRepair() noexcept {
        return AdvanceRepairSlice(nullptr);
    }



    [[nodiscard]] bool PendingCommitDurable(
        const PendingCommit& commit) const noexcept {
        static_cast<void>(commit);
        return true;
    }

    [[nodiscard]] bool ReleaseFrontPendingCommit() noexcept {
        if (pending_commits_.empty()) {
            SetFatal("Event pending commit release underflow");
            return false;
        }
        PendingCommit& commit = pending_commits_.front();
        const std::size_t old_queue =
            DequeOwnedBytes<PendingCommit>(pending_commits_.size());
        const std::size_t new_queue = DequeOwnedBytes<PendingCommit>(
            pending_commits_.size() - 1U);
        if (new_queue > old_queue) {
            SetFatal("Event pending-commit deque accounting increased");
            return false;
        }
        const std::size_t released_bytes = SaturatingAdd(
            commit.owned_bytes, old_queue - new_queue);
        if (released_bytes > pending_revision_bytes_) {
            SetFatal("Event pending revision byte accounting diverged");
            return false;
        }
        pending_revision_bytes_ -= released_bytes;
        pending_commits_.pop_front();
        stats_.pending_raw_commits.store(
            pending_commits_.size(), std::memory_order_relaxed);
        PublishPendingRevisionBytes();
        return true;
    }

    [[nodiscard]] bool ServiceDurableCommits(bool flush_partial) noexcept {
        if (!healthy_) {
            return false;
        }
        try {
            while (!pending_commits_.empty()) {
                PendingCommit& front = pending_commits_.front();
                if (!PendingCommitDurable(front)) {
                    return true;
                }
                if (front.batch->revisions.empty()) {
                    if (!ReleaseFrontPendingCommit()) {
                        return false;
                    }
                    continue;
                }

                std::size_t batch_count = 0U;
                std::size_t row_count = 0U;
                std::size_t byte_count = 0U;
                bool closed = false;
                for (const PendingCommit& commit : pending_commits_) {
                    if (!PendingCommitDurable(commit)) {
                        break;
                    }
                    if (commit.batch->revisions.empty()) {
                        closed = true;
                        break;
                    }
                    const std::size_t rows = commit.batch->revisions.size();
                    const std::size_t bytes = commit.owned_bytes;
                    const bool first = batch_count == 0U;
                    const bool oversized =
                        rows > config_.persistence_group_max_rows ||
                        bytes > config_.persistence_group_max_bytes;
                    if (first && oversized) {
                        batch_count = 1U;
                        row_count = rows;
                        byte_count = bytes;
                        closed = true;
                        break;
                    }
                    if (batch_count >=
                            config_.persistence_group_max_batches ||
                        rows > config_.persistence_group_max_rows - row_count ||
                        bytes >
                            config_.persistence_group_max_bytes - byte_count) {
                        closed = true;
                        break;
                    }
                    ++batch_count;
                    row_count += rows;
                    byte_count += bytes;
                }
                if (batch_count == 0U) {
                    SetFatal(
                        "Event persistence group selection made no progress");
                    return false;
                }
                closed = closed ||
                    batch_count == config_.persistence_group_max_batches ||
                    batch_count == config_.maximum_pending_commits ||
                    row_count == config_.persistence_group_max_rows ||
                    byte_count == config_.persistence_group_max_bytes;
                if (!flush_partial && !closed) {
                    const std::uint64_t now = ingest::MonotonicNowNs();
                    const std::uint64_t queued =
                        pending_commits_.front().queued_monotonic_ns;
                    const std::uint64_t elapsed = now >= queued
                        ? now - queued
                        : 0U;
                    if (elapsed < config_.persistence_group_max_delay_ns) {
                        return true;
                    }
                }

                std::vector<std::shared_ptr<const EventRevisionBatch>> group;
                group.reserve(batch_count);
                auto commit = pending_commits_.begin();
                for (std::size_t index = 0U; index < batch_count;
                     ++index, ++commit) {
                    group.push_back(commit->batch);
                }
                if (!sink_->AppendRevisionGroup(std::move(group))) {
                    SetFatal(
                        "Event revision sink rejected an immutable group");
                    return false;
                }
                stats_.persistence_groups_submitted.fetch_add(
                    1U, std::memory_order_relaxed);
                stats_.revision_batches_submitted.fetch_add(
                    batch_count, std::memory_order_relaxed);
                UpdateHighWatermark(
                    &stats_.persistence_group_batches_max, batch_count);
                UpdateHighWatermark(
                    &stats_.persistence_group_rows_max, row_count);
                UpdateHighWatermark(
                    &stats_.persistence_group_bytes_max, byte_count);
                for (std::size_t index = 0U; index < batch_count; ++index) {
                    if (!ReleaseFrontPendingCommit()) {
                        return false;
                    }
                }
            }
            return true;
        } catch (...) {
            SetFatal("Event persistence group allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool AdvanceDurableCommits() noexcept {
        return ServiceDurableCommits(false);
    }

    [[nodiscard]] bool FlushDurableCommits() noexcept {
        return ServiceDurableCommits(true);
    }

    [[nodiscard]] bool CopyBundle(
        const FactKey& key,
        std::vector<std::pair<EventKey, EventPayload>>* output) const {
        if (output == nullptr) {
            return false;
        }
        output->clear();
        const auto position = bundle_cache_.find(key);
        if (position == bundle_cache_.end()) {
            return false;
        }
        output->reserve(position->second.rows.size());
        for (const auto& row : position->second.rows) {
            output->push_back(row);
        }
        return true;
    }

    [[nodiscard]] bool CopyOrder(const OrderKey& key,
                                 OrderSnapshot* output) const noexcept {
        if (output == nullptr) {
            return false;
        }
        const OrderHistory* const position = order_histories_.Find(key);
        if (position == nullptr) {
            return false;
        }
        const OrderHistory& history = *position;
        const std::optional<OrderState>* latest =
            &history.baseline.version.state;
        for (auto node = history.suffix.rbegin();
             node != history.suffix.rend(); ++node) {
            if (node->second.evaluated) {
                latest = &node->second.eval.post_state;
                break;
            }
        }
        if (!latest->has_value()) {
            return false;
        }
        *output = latest->value().snapshot;
        return true;
    }

    [[nodiscard]] bool CopyChannelState(
        Market market,
        std::uint32_t channel,
        EventChannelState* output) const noexcept {
        if (output == nullptr ||
            (market == Market::kShanghai && channel == 0U) ||
            (market != Market::kShanghai && market != Market::kShenzhen)) {
            return false;
        }
        const ChannelKey key{config_.trade_date, market, channel};
        const auto state = channel_states_.find(key);
        if (state == channel_states_.end()) {
            return false;
        }
        *output = state->second;
        return true;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }
    [[nodiscard]] EventWorkerStats stats() const noexcept {
        EventWorkerStats result{};
        result.facts_journaled = stats_.facts_journaled.load(
            std::memory_order_relaxed);
        result.duplicate_facts = stats_.duplicate_facts.load(
            std::memory_order_relaxed);
        result.source_conflicts = stats_.source_conflicts.load(
            std::memory_order_relaxed);
        result.live_order_uses = stats_.live_order_uses.load(
            std::memory_order_relaxed);
        result.repaired_order_uses = stats_.repaired_order_uses.load(
            std::memory_order_relaxed);
        result.repair_convergence_stops =
            stats_.repair_convergence_stops.load(
                std::memory_order_relaxed);
        result.repair_slices = stats_.repair_slices.load(
            std::memory_order_relaxed);
        result.repair_commits = stats_.repair_commits.load(
            std::memory_order_relaxed);
        result.repair_order_restarts = stats_.repair_order_restarts.load(
            std::memory_order_relaxed);
        result.bundles_reassembled = stats_.bundles_reassembled.load(
            std::memory_order_relaxed);
        result.revisions_created = stats_.revisions_created.load(
            std::memory_order_relaxed);
        result.tombstones_created = stats_.tombstones_created.load(
            std::memory_order_relaxed);
        result.pending_raw_commits = stats_.pending_raw_commits.load(
            std::memory_order_relaxed);
        result.revision_batches_submitted =
            stats_.revision_batches_submitted.load(
                std::memory_order_relaxed);
        result.persistence_groups_submitted =
            stats_.persistence_groups_submitted.load(
                std::memory_order_relaxed);
        result.persistence_group_batches_max =
            stats_.persistence_group_batches_max.load(
                std::memory_order_relaxed);
        result.persistence_group_rows_max =
            stats_.persistence_group_rows_max.load(
                std::memory_order_relaxed);
        result.persistence_group_bytes_max =
            stats_.persistence_group_bytes_max.load(
                std::memory_order_relaxed);
        result.pending_revision_bytes = stats_.pending_revision_bytes.load(
            std::memory_order_relaxed);
        result.pending_revision_bytes_high_watermark =
            stats_.pending_revision_bytes_high_watermark.load(
                std::memory_order_relaxed);
        result.active_repair_orders = stats_.active_repair_orders.load(
            std::memory_order_relaxed);
        result.active_repair_bytes = stats_.active_repair_bytes.load(
            std::memory_order_relaxed);
        result.active_repair_bytes_high_watermark =
            stats_.active_repair_bytes_high_watermark.load(
                std::memory_order_relaxed);
        result.phase_normalization_slices =
            stats_.phase_normalization_slices.load(
                std::memory_order_relaxed);
        result.phase_facts_scanned = stats_.phase_facts_scanned.load(
            std::memory_order_relaxed);
        result.phase_dirty_roles_discovered =
            stats_.phase_dirty_roles_discovered.load(
                std::memory_order_relaxed);
        result.pending_phase_bytes = stats_.pending_phase_bytes.load(
            std::memory_order_relaxed);
        result.pending_phase_bytes_high_watermark =
            stats_.pending_phase_bytes_high_watermark.load(
                std::memory_order_relaxed);
        result.ordered_batch_fast_path = stats_.ordered_batch_fast_path.load(
            std::memory_order_relaxed);
        result.unordered_batch_sorts = stats_.unordered_batch_sorts.load(
            std::memory_order_relaxed);
        result.barrier_index_orders_visited =
            stats_.barrier_index_orders_visited.load(
                std::memory_order_relaxed);
        result.end_expansion_slices = stats_.end_expansion_slices.load(
            std::memory_order_relaxed);
        result.end_candidates_processed =
            stats_.end_candidates_processed.load(
                std::memory_order_relaxed);
        result.source_only_fast_path = stats_.source_only_fast_path.load(
            std::memory_order_relaxed);
        result.order_uses_compacted = stats_.order_uses_compacted.load(
            std::memory_order_relaxed);
        result.facts_evicted = stats_.facts_evicted.load(
            std::memory_order_relaxed);
        result.eviction_slices = stats_.eviction_slices.load(
            std::memory_order_relaxed);
        result.hot_facts = stats_.hot_facts.load(
            std::memory_order_relaxed);
        result.hot_fact_bytes = stats_.hot_fact_bytes.load(
            std::memory_order_relaxed);
        result.hot_fact_bytes_high_watermark =
            stats_.hot_fact_bytes_high_watermark.load(
                std::memory_order_relaxed);
        result.order_history_bytes = stats_.order_history_bytes.load(
            std::memory_order_relaxed);
        result.order_history_bytes_high_watermark =
            stats_.order_history_bytes_high_watermark.load(
                std::memory_order_relaxed);
        return result;
    }
    [[nodiscard]] bool repair_pending() const noexcept {
        return pending_projection_cut_.has_value() ||
            stats_.repair_pending.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool eviction_pending() const noexcept {
        return stats_.eviction_pending.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool projection_input_fenced() const noexcept {
        return pending_projection_cut_.has_value() ||
            (active_repair_.has_value() &&
             !active_repair_->end_expansions.empty());
    }
    [[nodiscard]] const EventWorkerConfig& config() const noexcept {
        return config_;
    }

private:
    static void UpdateHighWatermark(
        std::atomic<std::uint64_t>* value,
        std::uint64_t candidate) noexcept {
        std::uint64_t observed = value->load(std::memory_order_relaxed);
        while (observed < candidate &&
               !value->compare_exchange_weak(
                   observed, candidate, std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
    }

    void PublishHotFactBytes() noexcept {
        stats_.hot_fact_bytes.store(
            hot_fact_bytes_, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.hot_fact_bytes_high_watermark,
            static_cast<std::uint64_t>(hot_fact_bytes_));
    }

    [[nodiscard]] bool ReserveHotIndexGrowth(
        std::size_t bytes,
        EventApplyResult* result) noexcept {
        if (bytes > config_.maximum_hot_fact_bytes ||
            hot_fact_bytes_ > config_.maximum_hot_fact_bytes - bytes ||
            hot_index_bytes_ >
                std::numeric_limits<std::size_t>::max() - bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event hot-index byte capacity exhausted"));
            return false;
        }
        hot_index_bytes_ += bytes;
        hot_fact_bytes_ += bytes;
        PublishHotFactBytes();
        return true;
    }

    [[nodiscard]] bool ReleaseHotIndexBytes(std::size_t bytes) noexcept {
        if (bytes > hot_index_bytes_ || bytes > hot_fact_bytes_) {
            return false;
        }
        hot_index_bytes_ -= bytes;
        hot_fact_bytes_ -= bytes;
        PublishHotFactBytes();
        return true;
    }

    [[nodiscard]] bool InsertChannelFact(
        const ChannelKey& channel,
        const FactKey& key,
        EventApplyResult* result) {
        if (channel_facts_.size() == channel_facts_.maximum_entries() &&
            channel_facts_.Find(channel) == nullptr) {
            static_cast<void>(CapacityFailure(
                result, "Event channel range capacity exhausted"));
            return false;
        }
        auto prepared = channel_facts_.PrepareInsert(channel);
        ChannelFactRangeIndex* const existing = prepared.existing();
        if (existing != nullptr &&
            existing->contains(key.native_sequence)) {
            static_cast<void>(Failure(
                result, "Event channel fact index is duplicated"));
            return false;
        }
        const std::size_t growth = SaturatingAdd(
            prepared.owned_byte_delta(), ChannelFactEntryOwnedBytes());
        if (!ReserveHotIndexGrowth(growth, result)) {
            return false;
        }

        bool outer_inserted = false;
        try {
            ChannelFactRangeIndex* range = existing;
            if (range == nullptr) {
                auto inserted = channel_facts_.CommitInsert(
                    std::move(prepared));
                range = inserted.value;
                outer_inserted = inserted.inserted;
            }
            const auto [position, inserted] = range->emplace(
                key.native_sequence, key);
            static_cast<void>(position);
            if (!inserted) {
                if (outer_inserted) {
                    static_cast<void>(channel_facts_.Erase(channel));
                }
                static_cast<void>(ReleaseHotIndexBytes(growth));
                static_cast<void>(Failure(
                    result, "Event channel fact index insertion diverged"));
                return false;
            }
            return true;
        } catch (...) {
            if (outer_inserted) {
                static_cast<void>(channel_facts_.Erase(channel));
            }
            static_cast<void>(ReleaseHotIndexBytes(growth));
            throw;
        }
    }

    [[nodiscard]] bool InsertInstrumentFactIndex(
        const InstrumentChannelKey& range,
        std::uint64_t sequence,
        EventApplyResult* result) {
        if (instrument_facts_.size() ==
                instrument_facts_.maximum_entries() &&
            instrument_facts_.Find(range) == nullptr) {
            static_cast<void>(CapacityFailure(
                result, "Event instrument range capacity exhausted"));
            return false;
        }
        auto prepared = instrument_facts_.PrepareInsert(range);
        InstrumentFactIndex* const existing = prepared.existing();
        const std::size_t before_count = existing == nullptr
            ? 0U
            : existing->ordered.size();
        if (existing != nullptr && std::binary_search(
                existing->ordered.begin(), existing->ordered.end(),
                sequence)) {
            static_cast<void>(Failure(
                result, "Event instrument fact index is duplicated"));
            return false;
        }
        const std::size_t before_bytes = existing == nullptr
            ? 0U
            : DequeOwnedBytes<std::uint64_t>(before_count);
        const std::size_t after_bytes = DequeOwnedBytes<std::uint64_t>(
            SaturatingAdd(before_count, 1U));
        if (after_bytes < before_bytes) {
            static_cast<void>(Failure(
                result, "Event instrument fact byte accounting overflowed"));
            return false;
        }
        const std::size_t growth = SaturatingAdd(
            prepared.owned_byte_delta(), after_bytes - before_bytes);
        if (!ReserveHotIndexGrowth(growth, result)) {
            return false;
        }

        bool outer_inserted = false;
        try {
            InstrumentFactIndex* index = existing;
            if (index == nullptr) {
                auto inserted = instrument_facts_.CommitInsert(
                    std::move(prepared));
                index = inserted.value;
                outer_inserted = inserted.inserted;
            }
            InsertInstrumentFact(index, sequence);
            return true;
        } catch (...) {
            if (outer_inserted) {
                static_cast<void>(instrument_facts_.Erase(range));
            }
            static_cast<void>(ReleaseHotIndexBytes(growth));
            throw;
        }
    }

    [[nodiscard]] bool InsertPhaseIndex(
        const InstrumentChannelKey& range,
        std::uint64_t sequence,
        TradingPhase phase,
        EventApplyResult* result) {
        if (phase_statuses_.size() == phase_statuses_.maximum_entries() &&
            phase_statuses_.Find(range) == nullptr) {
            static_cast<void>(CapacityFailure(
                result, "Event phase range capacity exhausted"));
            return false;
        }
        auto prepared = phase_statuses_.PrepareInsert(range);
        PhaseIndex* const existing = prepared.existing();
        const std::size_t before_count = existing == nullptr
            ? 0U
            : existing->ordered.size();
        if (existing != nullptr) {
            const auto position = std::lower_bound(
                existing->ordered.begin(), existing->ordered.end(), sequence,
                [](const PhaseEntry& entry, std::uint64_t value) {
                    return entry.first < value;
                });
            if (position != existing->ordered.end() &&
                position->first == sequence) {
                static_cast<void>(Failure(
                    result, "Event phase index is duplicated"));
                return false;
            }
        }
        const std::size_t before_bytes = existing == nullptr
            ? 0U
            : DequeOwnedBytes<PhaseEntry>(before_count);
        const std::size_t after_bytes = DequeOwnedBytes<PhaseEntry>(
            SaturatingAdd(before_count, 1U));
        if (after_bytes < before_bytes) {
            static_cast<void>(Failure(
                result, "Event phase index byte accounting overflowed"));
            return false;
        }
        const std::size_t growth = SaturatingAdd(
            prepared.owned_byte_delta(), after_bytes - before_bytes);
        if (!ReserveHotIndexGrowth(growth, result)) {
            return false;
        }

        bool outer_inserted = false;
        try {
            PhaseIndex* index = existing;
            if (index == nullptr) {
                auto inserted = phase_statuses_.CommitInsert(
                    std::move(prepared));
                index = inserted.value;
                outer_inserted = inserted.inserted;
            }
            InsertPhase(index, sequence, phase);
            return true;
        } catch (...) {
            if (outer_inserted) {
                static_cast<void>(phase_statuses_.Erase(range));
            }
            static_cast<void>(ReleaseHotIndexBytes(growth));
            throw;
        }
    }

    [[nodiscard]] bool InsertBarrier(
        const InstrumentChannelKey& range,
        const FactKey& key,
        EventApplyResult* result) {
        if (barriers_.size() == barriers_.maximum_entries() &&
            barriers_.Find(range) == nullptr) {
            static_cast<void>(CapacityFailure(
                result, "Event barrier range capacity exhausted"));
            return false;
        }
        auto prepared = barriers_.PrepareInsert(range);
        BarrierRangeIndex* const existing = prepared.existing();
        if (existing != nullptr &&
            existing->contains(key.native_sequence)) {
            static_cast<void>(Failure(
                result, "Event barrier index is duplicated"));
            return false;
        }
        const std::size_t growth = SaturatingAdd(
            prepared.owned_byte_delta(), BarrierEntryOwnedBytes());
        if (!ReserveHotIndexGrowth(growth, result)) {
            return false;
        }

        bool outer_inserted = false;
        try {
            BarrierRangeIndex* index = existing;
            if (index == nullptr) {
                auto inserted = barriers_.CommitInsert(std::move(prepared));
                index = inserted.value;
                outer_inserted = inserted.inserted;
            }
            const auto [position, inserted] = index->emplace(
                key.native_sequence, key);
            static_cast<void>(position);
            if (!inserted) {
                if (outer_inserted) {
                    static_cast<void>(barriers_.Erase(range));
                }
                static_cast<void>(ReleaseHotIndexBytes(growth));
                static_cast<void>(Failure(
                    result, "Event barrier index insertion diverged"));
                return false;
            }
            return true;
        } catch (...) {
            if (outer_inserted) {
                static_cast<void>(barriers_.Erase(range));
            }
            static_cast<void>(ReleaseHotIndexBytes(growth));
            throw;
        }
    }

    [[nodiscard]] bool EraseInstrumentFactIndex(
        const InstrumentChannelKey& range,
        std::uint64_t sequence) {
        InstrumentFactIndex* const index = instrument_facts_.Find(range);
        if (index == nullptr) {
            return false;
        }
        auto& ordered = index->ordered;
        const auto position = std::lower_bound(
            ordered.begin(), ordered.end(), sequence);
        if (position == ordered.end() || *position != sequence) {
            return false;
        }
        const std::size_t before =
            DequeOwnedBytes<std::uint64_t>(ordered.size());
        std::size_t released = 0U;
        if (ordered.size() == 1U) {
            const auto erased = instrument_facts_.Erase(range);
            if (!erased.erased) {
                return false;
            }
            released = SaturatingAdd(
                erased.released_owned_bytes, before);
        } else {
            ordered.erase(position);
            const std::size_t after =
                DequeOwnedBytes<std::uint64_t>(ordered.size());
            if (after > before) {
                return false;
            }
            released = before - after;
        }
        return ReleaseHotIndexBytes(released);
    }

    [[nodiscard]] bool ErasePhaseIndex(
        const InstrumentChannelKey& range,
        std::uint64_t sequence) {
        PhaseIndex* const index = phase_statuses_.Find(range);
        if (index == nullptr) {
            return false;
        }
        auto& ordered = index->ordered;
        const auto position = std::lower_bound(
            ordered.begin(), ordered.end(), sequence,
            [](const PhaseEntry& entry, std::uint64_t value) {
                return entry.first < value;
            });
        if (position == ordered.end() || position->first != sequence) {
            return false;
        }
        const std::size_t before =
            DequeOwnedBytes<PhaseEntry>(ordered.size());
        index->anchor = *position;
        ordered.erase(position);
        const std::size_t after =
            DequeOwnedBytes<PhaseEntry>(ordered.size());
        return after <= before && ReleaseHotIndexBytes(before - after);
    }

    [[nodiscard]] bool EraseBarrier(
        const InstrumentChannelKey& range,
        std::uint64_t sequence) {
        BarrierRangeIndex* const index = barriers_.Find(range);
        if (index == nullptr) {
            return false;
        }
        const auto position = index->find(sequence);
        if (position == index->end()) {
            return false;
        }
        std::size_t released = BarrierEntryOwnedBytes();
        if (index->size() == 1U) {
            const auto erased = barriers_.Erase(range);
            if (!erased.erased) {
                return false;
            }
            released = SaturatingAdd(
                released, erased.released_owned_bytes);
        } else {
            index->erase(position);
        }
        return ReleaseHotIndexBytes(released);
    }

    [[nodiscard]] bool EraseChannelFact(
        const ChannelKey& channel,
        std::uint64_t sequence) {
        ChannelFactRangeIndex* const index = channel_facts_.Find(channel);
        if (index == nullptr) {
            return false;
        }
        const auto position = index->find(sequence);
        if (position == index->end()) {
            return false;
        }
        std::size_t released = ChannelFactEntryOwnedBytes();
        if (index->size() == 1U) {
            const auto erased = channel_facts_.Erase(channel);
            if (!erased.erased) {
                return false;
            }
            released = SaturatingAdd(
                released, erased.released_owned_bytes);
        } else {
            index->erase(position);
        }
        return ReleaseHotIndexBytes(released);
    }

    void PublishPendingRevisionBytes() noexcept {
        stats_.pending_revision_bytes.store(
            pending_revision_bytes_, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.pending_revision_bytes_high_watermark,
            static_cast<std::uint64_t>(pending_revision_bytes_));
    }

    [[nodiscard]] std::size_t PendingCommitAdditionalBytes(
        std::size_t revision_capacity) const noexcept {
        const std::size_t old_queue =
            DequeOwnedBytes<PendingCommit>(pending_commits_.size());
        const std::size_t new_queue = DequeOwnedBytes<PendingCommit>(
            SaturatingAdd(pending_commits_.size(), 1U));
        const std::size_t queue_growth = new_queue > old_queue
            ? new_queue - old_queue
            : 0U;
        return SaturatingAdd(
            PendingCommitPayloadOwnedBytes(revision_capacity),
            queue_growth);
    }

    void PublishProjectionWorkspaceBytes() noexcept {
        const std::size_t active_bytes = SaturatingAdd(
            repair_accounted_bytes_,
            projection_scratch_is_repair_ ? projection_scratch_bytes_ : 0U);
        const std::size_t pending_bytes = SaturatingAdd(
            pending_projection_cut_accounted_bytes_,
            projection_scratch_is_repair_ ? 0U : projection_scratch_bytes_);
        stats_.active_repair_bytes.store(
            active_bytes, std::memory_order_relaxed);
        stats_.pending_phase_bytes.store(
            pending_bytes, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.active_repair_bytes_high_watermark,
            static_cast<std::uint64_t>(active_bytes));
        UpdateHighWatermark(
            &stats_.pending_phase_bytes_high_watermark,
            static_cast<std::uint64_t>(pending_bytes));
    }

    [[nodiscard]] bool ReserveProjectionScratch(
        std::size_t bytes,
        bool repair_commit,
        EventApplyResult* result) noexcept {
        if (projection_scratch_reserved_) {
            static_cast<void>(Failure(
                result, "Event projection scratch reservation is nested"));
            return false;
        }
        const std::size_t used = SaturatingAdd(
            repair_accounted_bytes_,
            pending_projection_cut_accounted_bytes_);
        if (bytes > config_.maximum_repair_bytes ||
            used > config_.maximum_repair_bytes - bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event projection commit byte capacity exhausted"));
            return false;
        }
        projection_scratch_bytes_ = bytes;
        projection_scratch_is_repair_ = repair_commit;
        projection_scratch_reserved_ = true;
        PublishProjectionWorkspaceBytes();
        return true;
    }

    void ReleaseProjectionScratch() noexcept {
        projection_scratch_bytes_ = 0U;
        projection_scratch_is_repair_ = false;
        projection_scratch_reserved_ = false;
        PublishProjectionWorkspaceBytes();
    }

    class ProjectionScratchGuard final {
    public:
        explicit ProjectionScratchGuard(Impl* owner) noexcept
            : owner_(owner) {}
        ~ProjectionScratchGuard() {
            if (owner_ != nullptr) {
                owner_->ReleaseProjectionScratch();
            }
        }

        ProjectionScratchGuard(const ProjectionScratchGuard&) = delete;
        ProjectionScratchGuard& operator=(
            const ProjectionScratchGuard&) = delete;

    private:
        Impl* owner_;
    };

    void PublishOrderHistoryBytes() noexcept {
        stats_.order_history_bytes.store(
            order_history_bytes_, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.order_history_bytes_high_watermark,
            static_cast<std::uint64_t>(order_history_bytes_));
    }

    [[nodiscard]] bool ReserveOrderHistoryGrowth(
        std::size_t bytes,
        EventApplyResult* result) noexcept {
        if (bytes > config_.maximum_order_history_bytes ||
            order_history_bytes_ >
                config_.maximum_order_history_bytes - bytes) {
            SetFatal("Event order-history byte capacity exhausted");
            if (result != nullptr) {
                result->code = EventApplyCode::kCapacityExhausted;
            }
            return false;
        }
        order_history_bytes_ += bytes;
        PublishOrderHistoryBytes();
        return true;
    }

    [[nodiscard]] bool ReleaseOrderHistoryBytes(
        std::size_t bytes) noexcept {
        if (bytes > order_history_bytes_) {
            return false;
        }
        order_history_bytes_ -= bytes;
        stats_.order_history_bytes.store(
            order_history_bytes_, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] bool SetAccountedOrderUse(
        OrderHistory* history,
        std::uint64_t sequence,
        OrderRole role,
        EventApplyResult* result) {
        const bool inserted = !history->suffix.contains(sequence);
        if (inserted &&
            !ReserveOrderHistoryGrowth(
                OrderUseNodeOwnedBytes(), result)) {
            return false;
        }
        try {
            if (SetOrderUse(history, sequence, role)) {
                return true;
            }
        } catch (...) {
            if (inserted) {
                static_cast<void>(ReleaseOrderHistoryBytes(
                    OrderUseNodeOwnedBytes()));
            }
            throw;
        }
        if (inserted) {
            static_cast<void>(ReleaseOrderHistoryBytes(
                OrderUseNodeOwnedBytes()));
        }
        return false;
    }

    [[nodiscard]] bool QueuePendingCommit(
        std::shared_ptr<const EventRevisionBatch> batch) {
        if (batch == nullptr) {
            SetFatal("Event pending revision batch is null");
            return false;
        }
        if (batch->revisions.empty()) {
            return true;
        }
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            SetFatal("Event pending raw commit capacity exhausted");
            return false;
        }
        const std::size_t payload_bytes = PendingCommitPayloadOwnedBytes(
            batch->revisions.capacity());
        const std::size_t additional_bytes = PendingCommitAdditionalBytes(
            batch->revisions.capacity());
        if (additional_bytes > config_.maximum_pending_revision_bytes ||
            pending_revision_bytes_ >
                config_.maximum_pending_revision_bytes - additional_bytes) {
            SetFatal("Event pending revision byte capacity exhausted");
            return false;
        }
        pending_commits_.push_back(PendingCommit{
            std::move(batch), payload_bytes, ingest::MonotonicNowNs()});
        pending_revision_bytes_ += additional_bytes;
        stats_.pending_raw_commits.store(
            pending_commits_.size(), std::memory_order_relaxed);
        PublishPendingRevisionBytes();
        return true;
    }

    [[nodiscard]] EventApplyResult CapacityFailure(
        EventApplyResult* result,
        std::string message) noexcept {
        SetFatal(std::move(message));
        result->code = EventApplyCode::kCapacityExhausted;
        return *result;
    }

    [[nodiscard]] EventApplyResult Failure(
        EventApplyResult* result,
        std::string message) noexcept {
        SetFatal(std::move(message));
        result->code = EventApplyCode::kFailed;
        return *result;
    }

    void SetFatal(std::string message) noexcept {
        if (!healthy_.load(std::memory_order_relaxed)) {
            return;
        }
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            fatal_error_ = std::move(message);
        } catch (...) {
        }
        healthy_.store(false, std::memory_order_release);
    }

    void QueueEviction(const ChannelKey& channel,
                       std::uint64_t evict_before) {
        EventChannelState& state = channel_states_[channel];
        state.feed_session_epoch = config_.feed_session_epoch;
        state.eviction_target = std::max(
            state.eviction_target, evict_before);
        if (evict_before <= state.sealed_before) {
            return;
        }
        auto [position, inserted] = eviction_tasks_.try_emplace(channel);
        EvictionTask& task = position->second;
        if (inserted) {
            task.channel = channel;
            task.evict_before = evict_before;
        } else {
            task.evict_before = std::max(task.evict_before, evict_before);
        }
        if (eviction_ready_set_.insert(channel).second) {
            eviction_ready_.push_back(channel);
        }
        stats_.eviction_pending.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool AddActiveEndOrder(
        const OrderKey& key,
        OrderHistory* history,
        EventApplyResult* result) {
        if (key.market != Market::kShanghai) {
            return true;
        }
        constexpr std::size_t kNoSlot =
            std::numeric_limits<std::size_t>::max();
        const InstrumentChannelKey range{
            key.trade_date, key.market, key.instrument_id, key.channel};
        if (history->active_end_slot != kNoSlot) {
            const auto indexed = active_end_orders_.find(range);
            return indexed != active_end_orders_.end() &&
                history->active_end_slot < indexed->second.size() &&
                indexed->second.values()[history->active_end_slot] == key;
        }
        auto indexed = active_end_orders_.find(range);
        const bool new_index = indexed == active_end_orders_.end();
        std::size_t reserved = 0U;
        if (new_index) {
            reserved = SaturatingAdd(
                OrderIndexTreeNodeOwnedBytes(),
                OrderIndexList::AllocationOwnedBytes(std::min(
                    OrderIndexList::kInitialCapacity,
                    config_.maximum_carry_orders)));
            if (!ReserveOrderHistoryGrowth(reserved, result)) {
                return false;
            }
            try {
                indexed = active_end_orders_.try_emplace(
                    range, config_.maximum_carry_orders).first;
            } catch (...) {
                static_cast<void>(ReleaseOrderHistoryBytes(reserved));
                throw;
            }
        }
        auto& orders = indexed->second;
        std::optional<OrderIndexList::PreparedAppend> prepared;
        try {
            prepared.emplace(orders.PrepareAppend());
        } catch (...) {
            if (new_index) {
                active_end_orders_.erase(indexed);
                static_cast<void>(ReleaseOrderHistoryBytes(reserved));
            }
            throw;
        }
        if (!new_index) {
            reserved = prepared->peak_allocation_owned_bytes();
            if (!ReserveOrderHistoryGrowth(reserved, result)) {
                return false;
            }
        }
        try {
            const auto appended = orders.CommitAppend(
                std::move(*prepared), key);
            if (new_index &&
                appended.peak_allocation_owned_bytes +
                        OrderIndexTreeNodeOwnedBytes() !=
                    reserved) {
                throw std::logic_error(
                    "Event active END index layout changed");
            }
            if (!ReleaseOrderHistoryBytes(
                    appended.released_owned_bytes)) {
                return false;
            }
            history->active_end_slot = appended.index;
        } catch (...) {
            if (new_index) {
                active_end_orders_.erase(indexed);
            }
            static_cast<void>(ReleaseOrderHistoryBytes(reserved));
            throw;
        }
        return true;
    }

    [[nodiscard]] bool RemoveActiveEndOrder(
        const OrderKey& key,
        OrderHistory* history) {
        constexpr std::size_t kNoSlot =
            std::numeric_limits<std::size_t>::max();
        if (history->active_end_slot == kNoSlot) {
            return true;
        }
        const InstrumentChannelKey range{
            key.trade_date, key.market, key.instrument_id, key.channel};
        const auto indexed = active_end_orders_.find(range);
        if (indexed == active_end_orders_.end()) {
            return false;
        }
        OrderIndexList& orders = indexed->second;
        const std::size_t slot = history->active_end_slot;
        if (slot >= orders.size() || orders.values()[slot] != key) {
            return false;
        }
        const auto erased = orders.EraseAtSwap(slot);
        if (erased.moved.has_value()) {
            OrderHistory* const moved =
                order_histories_.Find(erased.moved->value);
            if (moved == nullptr) {
                return false;
            }
            moved->active_end_slot = erased.moved->current_index;
        }
        history->active_end_slot = kNoSlot;
        if (orders.empty()) {
            const std::size_t released = SaturatingAdd(
                OrderIndexTreeNodeOwnedBytes(), orders.ReleaseStorage());
            active_end_orders_.erase(indexed);
            if (!ReleaseOrderHistoryBytes(released)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool SyncActiveEndOrder(
        const OrderKey& key,
        EventApplyResult* result) {
        OrderHistory* const history = order_histories_.Find(key);
        if (history == nullptr) {
            return false;
        }
        const OrderState* const state = LatestOrderState(*history);
        const bool active = key.market == Market::kShanghai &&
            state != nullptr && !state->finalization_emitted;
        return active
            ? AddActiveEndOrder(key, history, result)
            : RemoveActiveEndOrder(key, history);
    }

    [[nodiscard]] bool RetireEmptyOrder(const OrderKey& key) {
        OrderHistory* const history = order_histories_.Find(key);
        if (history == nullptr || !history->suffix.empty() ||
            history->baseline.version.state.has_value()) {
            return true;
        }
        if (!RemoveActiveEndOrder(key, history)) {
            return false;
        }
        const InstrumentChannelKey range{
            key.trade_date, key.market, key.instrument_id, key.channel};
        const auto indexed = orders_by_instrument_channel_.find(range);
        if (indexed == orders_by_instrument_channel_.end()) {
            return false;
        }
        OrderIndexList& orders = indexed->second;
        const std::size_t slot = history->instrument_order_slot;
        if (slot >= orders.size() || orders.values()[slot] != key) {
            return false;
        }
        const auto erased_index = orders.EraseAtSwap(slot);
        if (erased_index.moved.has_value()) {
            OrderHistory* const moved =
                order_histories_.Find(erased_index.moved->value);
            if (moved == nullptr) {
                return false;
            }
            moved->instrument_order_slot =
                erased_index.moved->current_index;
        }
        if (orders.empty()) {
            const std::size_t released = SaturatingAdd(
                OrderIndexTreeNodeOwnedBytes(), orders.ReleaseStorage());
            orders_by_instrument_channel_.erase(indexed);
            if (!ReleaseOrderHistoryBytes(released)) {
                return false;
            }
        }
        const auto erased_history = order_histories_.Erase(key);
        if (!erased_history.erased ||
            !ReleaseOrderHistoryBytes(
                erased_history.released_owned_bytes)) {
            return false;
        }
        return true;
    }

    [[nodiscard]] bool CompactOrderUse(const OrderKey& order,
                                       std::uint64_t sequence) {
        OrderHistory* const history_position = order_histories_.Find(order);
        if (history_position == nullptr) {
            return false;
        }
        OrderHistory& history = *history_position;
        auto use = history.suffix.find(sequence);
        if (use == history.suffix.end() ||
            !use->second.evaluated || use != history.suffix.begin()) {
            return false;
        }
        history.baseline.compacted_before = std::max(
            history.baseline.compacted_before, sequence + 1U);
        history.baseline.last_sequence = sequence;
        history.baseline.version = StateVersion{
            use->second.eval.post_state,
            use->second.eval.input_set_hash};
        history.suffix.erase(use);
        if (!ReleaseOrderHistoryBytes(OrderUseNodeOwnedBytes())) {
            return false;
        }
        ++stats_.order_uses_compacted;
        return RetireEmptyOrder(order);
    }

    [[nodiscard]] std::size_t FactOwnedBytes(
        const FactRecord& record,
        const Bundle* bundle,
        std::size_t head_count) const noexcept {
        using FactRole = std::pair<OrderKey, OrderRole>;
        std::size_t bytes =
            TreeNodeOwnedBytes<std::pair<const FactKey, FactRecord>>();
        bytes = SaturatingAdd(
            bytes, DequeOwnedBytes<FactRole>(record.roles.size()));
        if (bundle != nullptr) {
            bytes = SaturatingAdd(
                bytes,
                TreeNodeOwnedBytes<std::pair<const FactKey, Bundle>>());
            bytes = SaturatingAdd(
                bytes,
                SaturatingMultiply(
                    bundle->rows.size(),
                    TreeNodeOwnedBytes<
                        std::pair<const EventKey, EventPayload>>()));
            bytes = SaturatingAdd(
                bytes,
                SaturatingMultiply(
                    bundle->input_set_hashes.size(),
                    TreeNodeOwnedBytes<
                        std::pair<const EventKey, Identifier128>>()));
        }
        if (head_count != 0U) {
            bytes = SaturatingAdd(
                bytes,
                TreeNodeOwnedBytes<
                    std::pair<const FactKey,
                              std::map<EventKey, EventHead>>>());
            bytes = SaturatingAdd(
                bytes,
                SaturatingMultiply(
                    head_count,
                    TreeNodeOwnedBytes<
                        std::pair<const EventKey, EventHead>>()));
        }
        return bytes;
    }

    [[nodiscard]] std::size_t FactOwnedBytes(
        const FactKey& key,
        const FactRecord& record) const noexcept {
        const auto bundle = bundle_cache_.find(key);
        const auto heads = event_heads_.find(key);
        return FactOwnedBytes(
            record,
            bundle == bundle_cache_.end() ? nullptr : &bundle->second,
            heads == event_heads_.end() ? 0U : heads->second.size());
    }

    [[nodiscard]] bool RefreshHotFactAccounting(
        const FactKey& key) noexcept {
        const auto position = facts_.find(key);
        if (position == facts_.end()) {
            return false;
        }
        FactRecord& record = position->second;
        const std::size_t next = FactOwnedBytes(key, record);
        std::size_t projected = hot_fact_bytes_;
        if (record.accounted_bytes > projected) {
            return false;
        }
        projected -= record.accounted_bytes;
        projected = SaturatingAdd(projected, next);
        if (projected > config_.maximum_hot_fact_bytes) {
            SetFatal("Event hot fact byte capacity exhausted");
            return false;
        }
        record.accounted_bytes = next;
        hot_fact_bytes_ = projected;
        stats_.hot_fact_bytes.store(
            hot_fact_bytes_, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.hot_fact_bytes_high_watermark,
            static_cast<std::uint64_t>(hot_fact_bytes_));
        return true;
    }

    [[nodiscard]] bool EraseFactState(const FactKey& key,
                                      const FactRecord& record) {
        const InstrumentChannelKey range = InstrumentChannel(key, record);
        if (!EraseInstrumentFactIndex(range, key.native_sequence)) {
            return false;
        }

        if (record.phase_status &&
            !ErasePhaseIndex(range, key.native_sequence)) {
            return false;
        }

        if (record.barrier && !EraseBarrier(range, key.native_sequence)) {
            return false;
        }
        const auto bundle = bundle_cache_.find(key);
        if (bundle != bundle_cache_.end()) {
            if (cached_event_count_ < bundle->second.rows.size()) {
                return false;
            }
            cached_event_count_ -= bundle->second.rows.size();
            bundle_cache_.erase(bundle);
        }
        event_heads_.erase(key);
        if (record.accounted_bytes > hot_fact_bytes_) {
            return false;
        }
        hot_fact_bytes_ -= record.accounted_bytes;
        facts_.erase(key);
        stats_.hot_facts.store(facts_.size(), std::memory_order_relaxed);
        PublishHotFactBytes();
        ++stats_.facts_evicted;
        return true;
    }

    [[nodiscard]] bool LoadSourceTick(const FactKey& key,
                                      const FactRecord& record,
                                      CanonicalTick* output) {
        const auto hot = std::lower_bound(
            batch_tick_cache_.begin(), batch_tick_cache_.end(), key,
            [](const auto& entry, const FactKey& value) {
                return entry.first < value;
            });
        if (hot != batch_tick_cache_.end() && hot->first == key) {
            *output = hot->second;
            return true;
        }
        if (config_.fact_journal->Read(record.handle, output)) {
            return true;
        }
        SetFatal("Event FactJournal read failed: " +
                 config_.fact_journal->fatal_error());
        return false;
    }

    [[nodiscard]] bool LoadProjectedTick(const FactKey& key,
                                         const FactRecord& record,
                                         CanonicalTick* output) {
        if (!LoadSourceTick(key, record, output)) {
            return false;
        }
        output->phase = record.projected_phase;
        if (record.projected_phase_valid) {
            output->validity |= ingest::kTickPhaseValid;
        } else {
            output->validity &= ~ingest::kTickPhaseValid;
        }
        return true;
    }

    [[nodiscard]] bool RefreshRepairCapacity(
        EventApplyResult* result) noexcept {
        if (!active_repair_.has_value()) {
            repair_accounted_bytes_ = 0U;
            PublishProjectionWorkspaceBytes();
            return true;
        }
        repair_accounted_bytes_ = RepairOwnedBytes(*active_repair_);
        PublishProjectionWorkspaceBytes();
        if (SaturatingAdd(
                SaturatingAdd(
                    repair_accounted_bytes_,
                    pending_projection_cut_accounted_bytes_),
                projection_scratch_bytes_) <=
            config_.maximum_repair_bytes) {
            return true;
        }
        SetFatal("Event repair byte capacity exhausted");
        if (result != nullptr) {
            result->code = EventApplyCode::kCapacityExhausted;
        }
        return false;
    }

    [[nodiscard]] bool CheckRepairCapacity(
        EventApplyResult* result) noexcept {
        PublishProjectionWorkspaceBytes();
        if (SaturatingAdd(
                SaturatingAdd(
                    repair_accounted_bytes_,
                    pending_projection_cut_accounted_bytes_),
                projection_scratch_bytes_) <=
            config_.maximum_repair_bytes) {
            return true;
        }
        SetFatal("Event repair byte capacity exhausted");
        if (result != nullptr) {
            result->code = EventApplyCode::kCapacityExhausted;
        }
        return false;
    }

    [[nodiscard]] bool ReserveRepairGrowth(
        std::size_t bytes,
        EventApplyResult* result) noexcept {
        const std::size_t used = SaturatingAdd(
            SaturatingAdd(
                repair_accounted_bytes_,
                pending_projection_cut_accounted_bytes_),
            projection_scratch_bytes_);
        if (bytes > config_.maximum_repair_bytes ||
            used > config_.maximum_repair_bytes - bytes) {
            SetFatal("Event repair byte capacity exhausted");
            if (result != nullptr) {
                result->code = EventApplyCode::kCapacityExhausted;
            }
            return false;
        }
        repair_accounted_bytes_ += bytes;
        PublishProjectionWorkspaceBytes();
        return true;
    }

    [[nodiscard]] std::size_t RepairQueueGrowth(
        const RepairTransaction& repair,
        const OrderKey& order) const noexcept {
        if (repair.ready_order_set.contains(order)) {
            return 0U;
        }
        const std::size_t old_deque_bytes =
            DequeOwnedBytes<OrderKey>(repair.ready_orders.size());
        const std::size_t new_deque_bytes = DequeOwnedBytes<OrderKey>(
            SaturatingAdd(repair.ready_orders.size(), 1U));
        return SaturatingAdd(
            TreeNodeOwnedBytes<OrderKey>(),
            new_deque_bytes > old_deque_bytes
                ? new_deque_bytes - old_deque_bytes
                : 0U);
    }

    void QueueRepairOrder(RepairTransaction* repair,
                          const OrderKey& order) {
        auto [position, inserted] = repair->ready_order_set.insert(order);
        if (inserted) {
            try {
                repair->ready_orders.push_back(order);
            } catch (...) {
                repair->ready_order_set.erase(position);
                throw;
            }
        }
    }

    void ResetRepairOrder(RepairTransaction* repair,
                          RepairOrderTask* task,
                          bool count_restart) {
        if (count_restart && task->initialized) {
            ++stats_.repair_order_restarts;
        }
        task->previous = {};
        task->cursor.reset();
        task->observed_live_generation = 0U;
        task->initialized = false;
        task->complete = false;
        repair->next_order = task->key;
        QueueRepairOrder(repair, task->key);
    }

    [[nodiscard]] bool MergeRepair(
        const std::map<OrderKey, DirtyOrder>& dirty_orders,
        const std::map<FactKey, bool>& dirty_bundles,
        std::span<const FactKey> inserted,
        std::span<const EndExpansionTask> end_expansions,
        EventApplyResult* result) {
        const RepairTransaction* const current = active_repair_.has_value()
            ? &*active_repair_
            : nullptr;
        std::size_t projected_bytes = current != nullptr
            ? RepairOwnedBytes(*current)
            : SaturatingAdd(
                  sizeof(RepairTransaction),
                  SaturatingAdd(
                      DequeOwnedBytes<OrderKey>(0U),
                      DequeOwnedBytes<EndExpansionTask>(0U)));
        const auto add_nodes = [&projected_bytes](
                                   std::size_t count,
                                   std::size_t bytes) noexcept {
            projected_bytes = SaturatingAdd(
                projected_bytes, SaturatingMultiply(count, bytes));
        };

        for (const FactKey& key : inserted) {
            if (current == nullptr ||
                !current->inserted_facts.contains(key)) {
                add_nodes(1U, TreeNodeOwnedBytes<FactKey>());
            }
        }
        for (const auto& [key, late] : dirty_bundles) {
            static_cast<void>(late);
            if (current == nullptr ||
                !current->dirty_bundles.contains(key)) {
                add_nodes(
                    1U,
                    TreeNodeOwnedBytes<std::pair<const FactKey, bool>>());
            }
        }

        std::size_t new_ready_orders = 0U;
        for (const auto& [order, dirty] : dirty_orders) {
            const auto task = current == nullptr
                ? std::map<OrderKey, RepairOrderTask>::const_iterator{}
                : current->tasks.find(order);
            const bool new_task = current == nullptr ||
                task == current->tasks.end();
            if (new_task) {
                add_nodes(
                    1U,
                    TreeNodeOwnedBytes<std::pair<
                        const OrderKey, RepairOrderTask>>());
                add_nodes(
                    dirty.new_sequences.size(),
                    TreeNodeOwnedBytes<std::uint64_t>());
            } else {
                for (const std::uint64_t sequence :
                     dirty.new_sequences) {
                    if (!task->second.dirty.new_sequences.contains(
                            sequence)) {
                        add_nodes(
                            1U,
                            TreeNodeOwnedBytes<std::uint64_t>());
                    }
                }
            }
            if (current == nullptr ||
                !current->ready_order_set.contains(order)) {
                ++new_ready_orders;
                add_nodes(1U, TreeNodeOwnedBytes<OrderKey>());
            }
        }
        const std::size_t old_ready_count = current == nullptr
            ? 0U
            : current->ready_orders.size();
        const std::size_t old_ready_bytes =
            DequeOwnedBytes<OrderKey>(old_ready_count);
        const std::size_t next_ready_bytes = DequeOwnedBytes<OrderKey>(
            SaturatingAdd(old_ready_count, new_ready_orders));
        projected_bytes = SaturatingAdd(
            projected_bytes,
            next_ready_bytes >= old_ready_bytes
                ? next_ready_bytes - old_ready_bytes
                : 0U);

        const std::size_t old_end_count = current == nullptr
            ? 0U
            : current->end_expansions.size();
        const std::size_t old_end_bytes =
            DequeOwnedBytes<EndExpansionTask>(old_end_count);
        const std::size_t next_end_bytes =
            DequeOwnedBytes<EndExpansionTask>(SaturatingAdd(
                old_end_count, end_expansions.size()));
        projected_bytes = SaturatingAdd(
            projected_bytes,
            next_end_bytes >= old_end_bytes
                ? next_end_bytes - old_end_bytes
                : 0U);

        std::size_t projected_end_staging = current == nullptr
            ? 0U
            : current->end_staging_bytes;
        std::size_t projected_end_rows = current == nullptr
            ? 0U
            : current->end_projected_rows;
        for (const EndExpansionTask& expansion : end_expansions) {
            projected_end_staging = SaturatingAdd(
                projected_end_staging, expansion.staging_bytes);
            projected_end_rows = SaturatingAdd(
                projected_end_rows,
                SaturatingAdd(expansion.candidate_count, 1U));
        }
        if (SaturatingAdd(
                projected_bytes,
                pending_projection_cut_accounted_bytes_) >
            config_.maximum_repair_bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event repair byte capacity exhausted"));
            return false;
        }
        if (projected_end_staging >
                config_.maximum_end_staging_bytes ||
            projected_end_rows > config_.maximum_end_projected_rows) {
            static_cast<void>(CapacityFailure(
                result, "Event Shanghai END staging capacity exhausted"));
            return false;
        }

        if (!active_repair_.has_value()) {
            active_repair_.emplace();
        }
        stats_.repair_pending.store(true, std::memory_order_release);
        RepairTransaction& repair = *active_repair_;
        repair.inserted_facts.insert(inserted.begin(), inserted.end());
        for (const auto& [key, late] : dirty_bundles) {
            auto [position, was_inserted] =
                repair.dirty_bundles.try_emplace(key, late);
            if (!was_inserted) {
                position->second = position->second || late;
            }
            repair.hole_fill = repair.hole_fill || late;
        }
        for (const auto& [order, dirty] : dirty_orders) {
            auto [position, was_inserted] = repair.tasks.try_emplace(order);
            RepairOrderTask& task = position->second;
            if (was_inserted) {
                task.key = order;
            } else {
                ResetRepairOrder(&repair, &task, true);
            }
            QueueRepairOrder(&repair, order);
            task.dirty.new_sequences.insert(dirty.new_sequences.begin(),
                                            dirty.new_sequences.end());
            task.dirty.late = task.dirty.late || dirty.late;
            repair.hole_fill = repair.hole_fill || dirty.late;
        }
        for (const EndExpansionTask& expansion : end_expansions) {
            repair.end_staging_bytes += expansion.staging_bytes;
            repair.end_projected_rows += SaturatingAdd(
                expansion.candidate_count, 1U);
            repair.end_expansions.push_back(expansion);
        }
        if (!repair.next_order.has_value() && !repair.tasks.empty() &&
            repair.ready_orders.empty()) {
            QueueRepairOrder(&repair, repair.tasks.begin()->first);
            repair.next_order = repair.tasks.begin()->first;
        }
        stats_.active_repair_orders.store(
            repair.tasks.size(), std::memory_order_release);
        return RefreshRepairCapacity(result);
    }

    [[nodiscard]] bool AttachEndCandidate(
        RepairTransaction* repair,
        const EndExpansionTask& expansion,
        const OrderKey& order,
        EventApplyResult* result) {
        OrderHistory* const history_position = order_histories_.Find(order);
        const auto fact_position = facts_.find(expansion.fact);
        if (history_position == nullptr ||
            fact_position == facts_.end()) {
            return false;
        }

        auto repair_position = repair->tasks.find(order);
        std::size_t growth = 0U;
        if (repair_position == repair->tasks.end()) {
            growth = SaturatingAdd(
                growth,
                TreeNodeOwnedBytes<
                    std::pair<const OrderKey, RepairOrderTask>>());
            growth = SaturatingAdd(
                growth, TreeNodeOwnedBytes<std::uint64_t>());
        } else if (!repair_position->second.dirty.new_sequences.contains(
                       expansion.fact.native_sequence)) {
            growth = SaturatingAdd(
                growth, TreeNodeOwnedBytes<std::uint64_t>());
        }
        growth = SaturatingAdd(growth, RepairQueueGrowth(*repair, order));
        if (!ReserveRepairGrowth(growth, result)) {
            return false;
        }
        if (repair_position != repair->tasks.end() &&
            repair_position->second.initialized) {
            ResetRepairOrder(repair, &repair_position->second, true);
        }

        OrderHistory& history = *history_position;
        const ChannelKey channel{
            expansion.fact.trade_date, expansion.fact.market,
            expansion.fact.channel};
        const auto channel_state = channel_states_.find(channel);
        if (channel_state != channel_states_.end()) {
            history.baseline.compacted_before = std::max(
                history.baseline.compacted_before,
                channel_state->second.sealed_before);
        }
        OrderUseNode* const existing = FindOrderUse(
            &history, expansion.fact.native_sequence);
        if (existing != nullptr && existing->role != OrderRole::kBarrier) {
            return false;
        }
        if (existing == nullptr) {
            if (!SetAccountedOrderUse(
                    &history, expansion.fact.native_sequence,
                    OrderRole::kBarrier, result)) {
                return false;
            }
            AppendUniqueFactRole(
                &fact_position->second, order, OrderRole::kBarrier);
            if (!RefreshHotFactAccounting(expansion.fact)) {
                if (!healthy_) {
                    result->code = EventApplyCode::kCapacityExhausted;
                }
                return false;
            }
        }

        auto [task_position, task_inserted] =
            repair->tasks.try_emplace(order);
        RepairOrderTask& task = task_position->second;
        if (task_inserted) {
            task.key = order;
        }
        task.dirty.new_sequences.insert(
            expansion.fact.native_sequence);
        task.dirty.late = task.dirty.late || fact_position->second.late;
        QueueRepairOrder(repair, order);
        stats_.active_repair_orders.store(
            repair->tasks.size(), std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool AdvanceEndExpansionSlice(
        EventApplyResult* result) {
        RepairTransaction& repair = *active_repair_;
        if (repair.end_expansions.empty()) {
            return true;
        }

        ++stats_.end_expansion_slices;
        const auto started = std::chrono::steady_clock::now();
        std::size_t processed = 0U;
        while (!repair.end_expansions.empty()) {
            EndExpansionTask& expansion = repair.end_expansions.front();
            const auto& candidate_index = expansion.active_only
                ? active_end_orders_
                : orders_by_instrument_channel_;
            const auto indexed = candidate_index.find(expansion.range);
            if (indexed == candidate_index.end() ||
                indexed->second.size() < expansion.candidate_count ||
                expansion.next_candidate >= expansion.candidate_count) {
                SetFatal("Event Shanghai END candidate index diverged");
                result->code = EventApplyCode::kFailed;
                return false;
            }
            const OrderKey order = indexed->second.values()[
                expansion.next_candidate];
            if (!AttachEndCandidate(&repair, expansion, order, result)) {
                if (healthy_) {
                    SetFatal("Event Shanghai END expansion failed");
                    result->code = EventApplyCode::kFailed;
                }
                return false;
            }
            ++expansion.next_candidate;
            ++processed;
            ++stats_.barrier_index_orders_visited;
            ++stats_.end_candidates_processed;
            if (expansion.next_candidate == expansion.candidate_count) {
                repair.end_expansions.pop_front();
            }

            const auto elapsed_signed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started)
                    .count();
            const std::uint64_t elapsed = elapsed_signed <= 0
                ? 0U
                : static_cast<std::uint64_t>(elapsed_signed);
            if (!repair.end_expansions.empty() &&
                (processed >= config_.end_slice_max_candidates ||
                 elapsed >= config_.end_slice_max_cpu_ns)) {
                result->repair_pending = true;
                return CheckRepairCapacity(result);
            }
        }
        return CheckRepairCapacity(result);
    }

    [[nodiscard]] RepairOrderTask* NextRepairOrder(
        RepairTransaction* repair) noexcept {
        while (!repair->ready_orders.empty()) {
            const OrderKey key = repair->ready_orders.front();
            repair->ready_orders.pop_front();
            repair->ready_order_set.erase(key);
            const auto position = repair->tasks.find(key);
            if (position == repair->tasks.end() ||
                position->second.complete) {
                continue;
            }
            repair->next_order = key;
            return &position->second;
        }
        return nullptr;
    }

    [[nodiscard]] StateVersion LatestRepairBefore(
        const RepairTransaction& repair,
        const RepairOrderTask& task,
        const OrderHistory& history,
        std::uint64_t sequence) const noexcept {
        const auto position = history.suffix.lower_bound(sequence);
        if (position != history.suffix.begin()) {
            const auto previous = std::prev(position);
            const auto patched = repair.eval_patch.find(
                RoleCacheKey{task.key, previous->first});
            if (patched != repair.eval_patch.end() &&
                patched->second.attempt_generation ==
                    task.attempt_generation) {
                return StateVersion{
                    patched->second.eval.post_state,
                    patched->second.eval.input_set_hash};
            }
            if (previous->second.evaluated) {
                return StateVersion{
                    previous->second.eval.post_state,
                    previous->second.eval.input_set_hash};
            }
        }
        if (history.baseline.last_sequence != 0U &&
            history.baseline.last_sequence < sequence) {
            return history.baseline.version;
        }
        return {};
    }

    [[nodiscard]] bool InitializeRepairOrder(
        RepairTransaction* repair,
        RepairOrderTask* task,
        EventApplyResult* result) {
        const OrderHistory* const live = order_histories_.Find(task->key);
        if (live == nullptr ||
            task->dirty.new_sequences.empty()) {
            return false;
        }
        if (task->attempt_generation ==
            std::numeric_limits<std::uint64_t>::max()) {
            static_cast<void>(Failure(
                result, "Event repair attempt generation exhausted"));
            return false;
        }
        ++task->attempt_generation;
        task->observed_live_generation = live->generation;
        const std::uint64_t first = *task->dirty.new_sequences.begin();
        task->previous = LatestRepairBefore(
            *repair, *task, *live, first);
        const auto use = live->suffix.lower_bound(first);
        task->cursor = use == live->suffix.end()
            ? std::optional<std::uint64_t>{}
            : std::optional<std::uint64_t>{use->first};
        task->initialized = true;
        task->complete = !task->cursor.has_value();
        return true;
    }

    [[nodiscard]] bool AdvanceRepairOrder(
        RepairTransaction* repair,
        RepairOrderTask* task,
        EventApplyResult* result,
        bool* processed_use) {
        *processed_use = false;
        const OrderHistory* const live = order_histories_.Find(task->key);
        if (live == nullptr) {
            return false;
        }
        if (task->initialized &&
            task->observed_live_generation != live->generation) {
            if (!ReserveRepairGrowth(
                    RepairQueueGrowth(*repair, task->key), result)) {
                return false;
            }
            ResetRepairOrder(repair, task, true);
        }
        if (!task->initialized &&
            !InitializeRepairOrder(repair, task, result)) {
            return false;
        }
        if (task->complete) {
            return true;
        }

        const OrderHistory& history = *live;
        const OrderUseNode* const use = FindOrderUse(
            history, *task->cursor);
        if (use == nullptr) {
            return false;
        }
        const std::uint64_t sequence = *task->cursor;
        const FactKey fact_key{task->key.trade_date, task->key.market,
                               task->key.channel, sequence};
        const auto fact_position = facts_.find(fact_key);
        if (fact_position == facts_.end()) {
            return false;
        }
        const FactRecord& fact = fact_position->second;
        CanonicalTick projected_tick{};
        if (!LoadProjectedTick(fact_key, fact, &projected_tick)) {
            return false;
        }
        const RoleCacheKey cache_key{task->key, sequence};
        const RoleEval* old_eval = use->evaluated ? &use->eval : nullptr;
        const std::optional<OrderState> prior_state = task->previous.state;
        const ApplyRoleResult applied = ApplyOrderRole(
            task->previous.state, projected_tick, use->role,
            task->previous.input_set_hash, fact.fact_hash);
        if (!applied.ok) {
            return false;
        }

        const bool state_converged = old_eval != nullptr &&
            applied.eval.post_state == old_eval->post_state;
        const bool new_use = task->dirty.new_sequences.contains(sequence);
        const bool state_unchanged_new_use = new_use && old_eval == nullptr &&
            applied.eval.post_state == prior_state;
        const bool observable_changed = old_eval == nullptr ||
            !RoleObservableEqual(applied.eval, *old_eval);
        const bool cache_changed = old_eval == nullptr ||
            observable_changed ||
            old_eval->input_set_hash != applied.eval.input_set_hash ||
            old_eval->post_state_hash != applied.eval.post_state_hash;
        std::size_t patch_growth = 0U;
        if (cache_changed && !repair->eval_patch.contains(cache_key)) {
            patch_growth = SaturatingAdd(
                patch_growth,
                TreeNodeOwnedBytes<
                    std::pair<const RoleCacheKey, RepairEvalPatch>>());
        }
        if (observable_changed &&
            !repair->dirty_bundles.contains(fact_key)) {
            patch_growth = SaturatingAdd(
                patch_growth,
                TreeNodeOwnedBytes<std::pair<const FactKey, bool>>());
        }
        if (!ReserveRepairGrowth(patch_growth, result)) {
            return false;
        }
        if (cache_changed) {
            repair->eval_patch.insert_or_assign(
                cache_key,
                RepairEvalPatch{applied.eval, task->attempt_generation});
        }
        if (observable_changed) {
            auto [position, was_inserted] =
                repair->dirty_bundles.try_emplace(
                    fact_key,
                    repair->hole_fill || task->dirty.late);
            if (!was_inserted) {
                position->second = position->second ||
                    repair->hole_fill || task->dirty.late;
            }
        }
        task->previous = StateVersion{
            applied.eval.post_state, applied.eval.input_set_hash};
        ++stats_.repaired_order_uses;
        if (result != nullptr) {
            ++result->repaired_order_uses;
        }
        *processed_use = true;

        if (state_converged || state_unchanged_new_use) {
            const auto next_new =
                task->dirty.new_sequences.upper_bound(sequence);
            if (next_new == task->dirty.new_sequences.end()) {
                ++stats_.repair_convergence_stops;
                task->cursor.reset();
                task->complete = true;
                return true;
            }
            task->previous = LatestRepairBefore(
                *repair, *task, history, *next_new);
            const auto next_use = history.suffix.lower_bound(*next_new);
            task->cursor = next_use == history.suffix.end()
                ? std::optional<std::uint64_t>{}
                : std::optional<std::uint64_t>{next_use->first};
            task->complete = !task->cursor.has_value();
            return true;
        }

        const auto next_use = history.suffix.upper_bound(sequence);
        task->cursor = next_use == history.suffix.end()
            ? std::optional<std::uint64_t>{}
            : std::optional<std::uint64_t>{next_use->first};
        task->complete = !task->cursor.has_value();
        return true;
    }

    [[nodiscard]] bool RepairGenerationsStable(
        RepairTransaction* repair,
        EventApplyResult* result) {
        bool stable = true;
        for (auto& [order, task] : repair->tasks) {
            const OrderHistory* const live = order_histories_.Find(order);
            if (live == nullptr) {
                SetFatal("Event repair OrderUseIndex disappeared");
                return false;
            }
            if (task.initialized &&
                task.observed_live_generation != live->generation) {
                if (!ReserveRepairGrowth(
                        RepairQueueGrowth(*repair, order), result)) {
                    return false;
                }
                ResetRepairOrder(repair, &task, true);
                stable = false;
                continue;
            }
            if (!task.initialized || !task.complete) {
                if (!repair->ready_order_set.contains(order)) {
                    SetFatal("Event repair worklist diverged");
                    result->code = EventApplyCode::kFailed;
                    return false;
                }
                stable = false;
            }
        }
        return stable;
    }

    [[nodiscard]] bool ValidateRepairOverlay(
        const RepairTransaction& repair,
        EventApplyResult* result) {
        for (const auto& [order, task] : repair.tasks) {
            const OrderHistory* const live = order_histories_.Find(order);
            if (live == nullptr || !task.initialized ||
                !task.complete || task.attempt_generation == 0U ||
                task.observed_live_generation != live->generation) {
                static_cast<void>(Failure(
                    result, "Event repair generation changed before commit"));
                return false;
            }
        }
        for (const auto& [key, patch] : repair.eval_patch) {
            const auto task = repair.tasks.find(key.order);
            if (task == repair.tasks.end() ||
                patch.attempt_generation !=
                    task->second.attempt_generation) {
                continue;
            }
            const OrderHistory* const live = order_histories_.Find(key.order);
            if (live == nullptr ||
                FindOrderUse(*live, key.native_sequence) == nullptr) {
                static_cast<void>(Failure(
                    result, "Event repair eval patch target disappeared"));
                return false;
            }
        }
        return true;
    }

    void ApplyRepairOverlay(const RepairTransaction& repair) noexcept {
        for (const auto& [key, patch] : repair.eval_patch) {
            const auto task = repair.tasks.find(key.order);
            if (task == repair.tasks.end() ||
                patch.attempt_generation !=
                    task->second.attempt_generation) {
                continue;
            }
            OrderUseNode* const use = FindOrderUse(
                order_histories_.Find(key.order), key.native_sequence);
            use->eval = patch.eval;
            use->evaluated = true;
        }
    }

    [[nodiscard]] bool ProjectionCommitScratchUpperBound(
        const std::map<FactKey, bool>& dirty_bundles,
        std::size_t* output,
        EventApplyResult* result) noexcept {
        std::size_t new_rows = 0U;
        std::size_t old_rows = 0U;
        for (const auto& [key, late] : dirty_bundles) {
            static_cast<void>(late);
            const auto fact = facts_.find(key);
            if (fact == facts_.end()) {
                static_cast<void>(Failure(
                    result, "Event projection scratch fact is missing"));
                return false;
            }
            if (fact->second.projectable) {
                new_rows = SaturatingAdd(
                    new_rows,
                    SaturatingAdd(fact->second.roles.size(), 1U));
            }
            const auto old = bundle_cache_.find(key);
            if (old != bundle_cache_.end()) {
                old_rows = SaturatingAdd(
                    old_rows, old->second.rows.size());
            }
        }

        using BundlePatchNode = std::pair<const FactKey, Bundle>;
        using RowNode = std::pair<const EventKey, EventPayload>;
        using HashNode = std::pair<const EventKey, Identifier128>;
        using HeadNode = std::pair<const EventKey, EventHead>;
        using CountNode = std::pair<const FactKey, std::size_t>;
        std::size_t bytes = SaturatingMultiply(
            dirty_bundles.size(), TreeNodeOwnedBytes<BundlePatchNode>());
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                new_rows,
                SaturatingAdd(
                    TreeNodeOwnedBytes<RowNode>(),
                    TreeNodeOwnedBytes<HashNode>())));
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                SaturatingAdd(old_rows, new_rows),
                TreeNodeOwnedBytes<HeadNode>()));
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                dirty_bundles.size(),
                SaturatingMultiply(2U, TreeNodeOwnedBytes<CountNode>())));
        *output = bytes;
        return true;
    }

    [[nodiscard]] bool CommitProjection(
        std::span<const OrderKey> touched_orders,
        const std::map<RoleCacheKey, RoleEval>* live_eval_patch,
        RepairTransaction* repair_overlay,
        std::map<FactKey, bool>* dirty_bundles,
        std::span<const FactKey> inserted,
        bool repair_commit,
        EventApplyResult* result) {
        if ((live_eval_patch == nullptr) == (repair_overlay == nullptr)) {
            static_cast<void>(Failure(
                result, "Event projection eval source is ambiguous"));
            return false;
        }
        if (repair_overlay != nullptr &&
            !ValidateRepairOverlay(*repair_overlay, result)) {
            return false;
        }
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            static_cast<void>(CapacityFailure(
                result, "Event pending raw commit capacity exhausted"));
            return false;
        }

        std::size_t projection_scratch = 0U;
        if (!ProjectionCommitScratchUpperBound(
                *dirty_bundles, &projection_scratch, result) ||
            !ReserveProjectionScratch(
                projection_scratch, repair_commit, result)) {
            return false;
        }
        ProjectionScratchGuard projection_scratch_guard(this);

        std::map<FactKey, Bundle> bundle_patch;
        std::size_t projected_event_count = cached_event_count_;
        for (const auto& [key, late_reason] : *dirty_bundles) {
            static_cast<void>(late_reason);
            Bundle assembled{};
            if (!AssembleBundle(
                    key, live_eval_patch, repair_overlay, &assembled)) {
                static_cast<void>(Failure(
                    result, "Event bundle source fact read failed"));
                return false;
            }
            const auto old = bundle_cache_.find(key);
            const std::size_t old_size = old == bundle_cache_.end()
                ? 0U
                : old->second.rows.size();
            if (projected_event_count < old_size) {
                static_cast<void>(Failure(
                    result, "Event bundle cardinality invariant failed"));
                return false;
            }
            projected_event_count -= old_size;
            if (assembled.rows.size() >
                config_.maximum_cached_events - projected_event_count) {
                static_cast<void>(CapacityFailure(
                    result, "Event BundleCache capacity exhausted"));
                return false;
            }
            projected_event_count += assembled.rows.size();
            bundle_patch.emplace(key, std::move(assembled));
        }

        std::size_t pending_revision_count = 0U;
        for (const auto& [key, bundle] : bundle_patch) {
            const auto old = bundle_cache_.find(key);
            const Bundle empty{};
            pending_revision_count = SaturatingAdd(
                pending_revision_count,
                BundleRevisionCount(
                    old == bundle_cache_.end() ? empty : old->second,
                    bundle));
        }
        const std::size_t pending_owned_bytes = PendingCommitAdditionalBytes(
            pending_revision_count);
        if (pending_owned_bytes > config_.maximum_pending_revision_bytes ||
            pending_revision_bytes_ >
                config_.maximum_pending_revision_bytes -
                    pending_owned_bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event pending revision byte capacity exhausted"));
            return false;
        }

        const std::uint64_t calculation_batch_sequence =
            next_calculation_batch_sequence_;
        if (calculation_batch_sequence == 0U ||
            calculation_batch_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
            static_cast<void>(Failure(
                result, "Event calculation batch sequence exhausted"));
            return false;
        }
        const Identifier128 recovery_run_id = RecoveryRunIdentifier(
            config_.calculation_run_id, config_.owner,
            calculation_batch_sequence);
        auto revision_batch = std::make_shared<EventRevisionBatch>();
        revision_batch->calculation_run_id = config_.calculation_run_id;
        revision_batch->recovery_run_id = recovery_run_id;
        revision_batch->owner = config_.owner;
        revision_batch->batch_sequence = calculation_batch_sequence;
        revision_batch->revisions.reserve(pending_revision_count);

        std::map<EventKey, EventHead> head_patch;
        bool batch_has_late = false;
        std::uint32_t revision_counter = next_revision_counter_;
        for (const auto& [fact_key, new_bundle] : bundle_patch) {
            const auto old_position = bundle_cache_.find(fact_key);
            const Bundle empty{};
            const Bundle& old_bundle = old_position == bundle_cache_.end()
                ? empty
                : old_position->second;
            const bool late_reason = dirty_bundles->at(fact_key);
            batch_has_late = batch_has_late || late_reason;
            if (!DiffBundle(old_bundle, new_bundle, late_reason,
                            recovery_run_id, &revision_counter,
                            revision_batch.get(), &head_patch, result)) {
                static_cast<void>(Failure(
                    result, "Event revision version space exhausted"));
                return false;
            }
            ++result->changed_bundles;
            ++stats_.bundles_reassembled;
        }
        revision_batch->reason = batch_has_late
            ? RevisionReason::kHoleFill
            : RevisionReason::kLiveProjection;
        if (revision_batch->revisions.size() != pending_revision_count) {
            static_cast<void>(Failure(
                result, "Event revision byte preflight diverged"));
            return false;
        }

        std::map<FactKey, std::size_t> projected_head_counts;
        for (const auto& [fact_key, bundle] : bundle_patch) {
            static_cast<void>(bundle);
            const auto current = event_heads_.find(fact_key);
            projected_head_counts.emplace(
                fact_key,
                current == event_heads_.end() ? 0U : current->second.size());
        }
        for (const auto& [event_key, head] : head_patch) {
            static_cast<void>(head);
            const FactKey fact_key = MakeFactKey(event_key);
            auto count = projected_head_counts.find(fact_key);
            if (count == projected_head_counts.end()) {
                static_cast<void>(Failure(
                    result, "Event head patch fact diverged"));
                return false;
            }
            const auto current = event_heads_.find(fact_key);
            if (current == event_heads_.end() ||
                !current->second.contains(event_key)) {
                ++count->second;
            }
        }
        std::map<FactKey, std::size_t> projected_fact_bytes;
        std::size_t projected_hot_fact_bytes = hot_fact_bytes_;
        for (const auto& [fact_key, bundle] : bundle_patch) {
            const auto fact = facts_.find(fact_key);
            if (fact == facts_.end() ||
                fact->second.accounted_bytes > projected_hot_fact_bytes) {
                static_cast<void>(Failure(
                    result, "Event hot fact accounting diverged"));
                return false;
            }
            const std::size_t owned = FactOwnedBytes(
                fact->second, &bundle, projected_head_counts.at(fact_key));
            projected_hot_fact_bytes -= fact->second.accounted_bytes;
            projected_hot_fact_bytes = SaturatingAdd(
                projected_hot_fact_bytes, owned);
            projected_fact_bytes.emplace(fact_key, owned);
        }
        if (projected_hot_fact_bytes > config_.maximum_hot_fact_bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event hot fact byte capacity exhausted"));
            return false;
        }

        if (repair_overlay != nullptr) {
            ApplyRepairOverlay(*repair_overlay);
        }
        for (const OrderKey& order : touched_orders) {
            if (!SyncActiveEndOrder(order, result)) {
                static_cast<void>(Failure(
                    result, "Event active END order index diverged"));
                return false;
            }
        }
        for (auto& [key, bundle] : bundle_patch) {
            bundle_cache_[key] = std::move(bundle);
        }
        for (const auto& [key, head] : head_patch) {
            event_heads_[MakeFactKey(key)][key] = head;
        }
        for (const auto& [key, owned] : projected_fact_bytes) {
            facts_.at(key).accounted_bytes = owned;
        }
        hot_fact_bytes_ = projected_hot_fact_bytes;
        stats_.hot_fact_bytes.store(
            hot_fact_bytes_, std::memory_order_relaxed);
        UpdateHighWatermark(
            &stats_.hot_fact_bytes_high_watermark,
            static_cast<std::uint64_t>(hot_fact_bytes_));
        cached_event_count_ = projected_event_count;
        next_revision_counter_ = revision_counter;
        ++next_calculation_batch_sequence_;
        for (const FactKey& key : inserted) {
            const ChannelKey channel{
                key.trade_date, key.market, key.channel};
            EventChannelState& state = channel_states_[channel];
            state.projected_frontier = std::max(
                state.projected_frontier, key.native_sequence);
        }

        const std::size_t revision_count =
            revision_batch->revisions.size();
        if (!QueuePendingCommit(std::move(revision_batch))) {
            static_cast<void>(CapacityFailure(
                result, "Event pending revision byte capacity exhausted"));
            return false;
        }
        stats_.revisions_created += revision_count;
        if (repair_commit) {
            ++stats_.repair_commits;
        }
        return true;
    }

    [[nodiscard]] bool CommitRepair(EventApplyResult* result) {
        RepairTransaction& repair = *active_repair_;
        if (repair.hole_fill) {
            for (auto& [key, late] : repair.dirty_bundles) {
                static_cast<void>(key);
                late = true;
            }
        }
        const std::size_t commit_scratch = SaturatingAdd(
            SaturatingMultiply(repair.tasks.size(), sizeof(OrderKey)),
            SaturatingMultiply(
                repair.inserted_facts.size(), sizeof(FactKey)));
        if (!ReserveRepairGrowth(commit_scratch, result)) {
            return false;
        }
        std::vector<OrderKey> touched_orders;
        touched_orders.reserve(repair.tasks.size());
        for (const auto& [order, task] : repair.tasks) {
            static_cast<void>(task);
            touched_orders.push_back(order);
        }
        std::vector<FactKey> inserted(repair.inserted_facts.begin(),
                                      repair.inserted_facts.end());
        if (!CommitProjection(
                touched_orders, nullptr, &repair,
                &repair.dirty_bundles, inserted, true, result)) {
            return false;
        }
        active_repair_.reset();
        repair_accounted_bytes_ = 0U;
        stats_.active_repair_orders.store(0U, std::memory_order_release);
        stats_.active_repair_bytes.store(0U, std::memory_order_release);
        stats_.repair_pending.store(false, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool AdvanceRepairSlice(
        EventApplyResult* external_result) noexcept {
        EventApplyResult local_result{};
        EventApplyResult* result = external_result == nullptr
            ? &local_result
            : external_result;
        if (!healthy_) {
            result->code = EventApplyCode::kFailed;
            return false;
        }
        if (!AdvanceDurableCommits()) {
            result->code = EventApplyCode::kSinkFailed;
            return false;
        }
        if (pending_projection_cut_.has_value()) {
            try {
                const bool advanced = AdvancePendingProjectionCut(result);
                result->repair_pending = repair_pending();
                return advanced;
            } catch (const std::bad_alloc&) {
                static_cast<void>(CapacityFailure(
                    result, "Event phase normalization allocation failed"));
                return false;
            } catch (const std::exception& exception) {
                static_cast<void>(Failure(
                    result, std::string("Event phase normalization failed: ") +
                                exception.what()));
                return false;
            } catch (...) {
                static_cast<void>(Failure(
                    result,
                    "Event phase normalization failed with unknown exception"));
                return false;
            }
        }
        if (!active_repair_.has_value()) {
            result->repair_pending = false;
            return true;
        }
        if (!CheckRepairCapacity(result)) {
            return false;
        }

        ++stats_.repair_slices;
        const auto started = std::chrono::steady_clock::now();
        std::size_t processed_uses = 0U;
        try {
            if (!active_repair_->end_expansions.empty()) {
                if (!AdvanceEndExpansionSlice(result)) {
                    return false;
                }
                if (!active_repair_->end_expansions.empty()) {
                    result->repair_pending = true;
                    return true;
                }
            }
            while (active_repair_.has_value()) {
                RepairTransaction& repair = *active_repair_;
                RepairOrderTask* task = NextRepairOrder(&repair);
                if (task == nullptr) {
                    if (!RepairGenerationsStable(&repair, result)) {
                        if (!healthy_) {
                            if (result->code == EventApplyCode::kApplied) {
                                result->code = EventApplyCode::kFailed;
                            }
                            return false;
                        }
                        continue;
                    }
                    if (pending_commits_.size() >=
                        config_.maximum_pending_commits) {
                        result->repair_pending = true;
                        return true;
                    }
                    if (!CommitRepair(result)) {
                        return false;
                    }
                    result->repair_pending = false;
                    return AdvanceDurableCommits();
                }

                bool processed_use = false;
                if (!AdvanceRepairOrder(
                        &repair, task, result, &processed_use)) {
                    static_cast<void>(Failure(
                        result, "Event sliced order-role repair failed"));
                    return false;
                }
                if (!task->complete) {
                    QueueRepairOrder(&repair, task->key);
                }
                if (processed_use) {
                    ++processed_uses;
                }
                if (processed_uses == 0U) {
                    continue;
                }
                const auto elapsed_signed =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - started)
                        .count();
                const std::uint64_t elapsed = elapsed_signed <= 0
                    ? 0U
                    : static_cast<std::uint64_t>(elapsed_signed);
                if (processed_uses >=
                        config_.repair_slice_max_order_uses ||
                    elapsed >= config_.repair_slice_max_cpu_ns) {
                    result->repair_pending = true;
                    return true;
                }
            }
            result->repair_pending = false;
            return true;
        } catch (const std::bad_alloc&) {
            static_cast<void>(CapacityFailure(
                result, "Event repair allocation failed"));
            return false;
        } catch (const std::exception& exception) {
            static_cast<void>(Failure(
                result, std::string("Event repair failed: ") +
                            exception.what()));
            return false;
        } catch (...) {
            static_cast<void>(Failure(
                result, "Event repair failed with unknown exception"));
            return false;
        }
    }

    [[nodiscard]] std::optional<TradingPhase> PhaseAt(
        const InstrumentChannelKey& range,
        std::uint64_t sequence) const noexcept {
        const PhaseIndex* const statuses = phase_statuses_.Find(range);
        if (statuses == nullptr) {
            return std::nullopt;
        }
        const auto& values = statuses->ordered;
        const auto position = std::upper_bound(
            values.begin(), values.end(), sequence,
            [](std::uint64_t value, const auto& entry) {
                return value < entry.first;
            });
        if (position == values.begin()) {
            const auto& anchor = statuses->anchor;
            return anchor.has_value() && anchor->first <= sequence
                ? std::optional<TradingPhase>{anchor->second}
                : std::nullopt;
        }
        return std::prev(position)->second;
    }

    void PublishPendingPhaseBytes() noexcept {
        PublishProjectionWorkspaceBytes();
    }

    [[nodiscard]] bool ReservePendingProjectionGrowth(
        std::size_t bytes,
        EventApplyResult* result) noexcept {
        const std::size_t used = SaturatingAdd(
            SaturatingAdd(
                repair_accounted_bytes_,
                pending_projection_cut_accounted_bytes_),
            projection_scratch_bytes_);
        if (bytes > config_.maximum_repair_bytes ||
            used > config_.maximum_repair_bytes - bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event phase normalization byte capacity exhausted"));
            return false;
        }
        pending_projection_cut_accounted_bytes_ += bytes;
        if (pending_projection_cut_.has_value()) {
            pending_projection_cut_->accounted_bytes += bytes;
        }
        PublishPendingPhaseBytes();
        return true;
    }

    [[nodiscard]] bool InstallPendingProjectionCut(
        PendingProjectionCut cut,
        EventApplyResult* result) {
        if (pending_projection_cut_.has_value()) {
            static_cast<void>(Failure(
                result, "Event pending phase cut is duplicated"));
            return false;
        }
        const std::size_t used = SaturatingAdd(
            SaturatingAdd(repair_accounted_bytes_, cut.accounted_bytes),
            projection_scratch_bytes_);
        if (used > config_.maximum_repair_bytes) {
            static_cast<void>(CapacityFailure(
                result, "Event phase normalization byte capacity exhausted"));
            return false;
        }
        pending_projection_cut_accounted_bytes_ = cut.accounted_bytes;
        pending_projection_cut_.emplace(std::move(cut));
        PublishPendingPhaseBytes();
        stats_.repair_pending.store(true, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool BuildPhaseScanSpans(
        PendingProjectionCut* cut,
        EventApplyResult* result) {
        if (cut->source_only_fast) {
            return true;
        }
        cut->phase_spans.reserve(cut->inserted.size());
        cut->end_expansions.reserve(cut->inserted.size());
        for (const FactKey& key : cut->inserted) {
            const auto fact = facts_.find(key);
            if (fact == facts_.end()) {
                static_cast<void>(Failure(
                    result, "Event phase scan fact is missing"));
                return false;
            }
            if (key.market != Market::kShanghai) {
                continue;
            }
            CanonicalTick source{};
            if (!LoadSourceTick(key, fact->second, &source)) {
                static_cast<void>(Failure(
                    result, "Event phase scan source read failed"));
                return false;
            }
            const InstrumentChannelKey range =
                InstrumentChannel(key, fact->second);
            const InstrumentFactIndex* const journal =
                instrument_facts_.Find(range);
            if (journal == nullptr) {
                static_cast<void>(Failure(
                    result, "Event phase instrument index is missing"));
                return false;
            }
            const auto& facts = journal->ordered;
            std::size_t first_index = 0U;
            std::size_t end_index = 0U;
            if (source.action == TickAction::kStatus) {
                if ((source.validity & ingest::kTickPhaseValid) == 0U) {
                    continue;
                }
                const PhaseIndex* const statuses =
                    phase_statuses_.Find(range);
                if (statuses == nullptr) {
                    static_cast<void>(Failure(
                        result, "Event phase status index is missing"));
                    return false;
                }
                const auto& values = statuses->ordered;
                const auto next_status = std::upper_bound(
                    values.begin(), values.end(), key.native_sequence,
                    [](std::uint64_t value, const auto& entry) {
                        return value < entry.first;
                    });
                const auto first = std::upper_bound(
                    facts.begin(), facts.end(), key.native_sequence);
                const auto end = next_status == values.end()
                    ? facts.end()
                    : std::lower_bound(
                          facts.begin(), facts.end(), next_status->first);
                first_index = static_cast<std::size_t>(
                    std::distance(facts.begin(), first));
                end_index = static_cast<std::size_t>(
                    std::distance(facts.begin(), end));
            } else {
                const auto position = std::lower_bound(
                    facts.begin(), facts.end(), key.native_sequence);
                if (position == facts.end() ||
                    *position != key.native_sequence) {
                    static_cast<void>(Failure(
                        result, "Event phase fact index diverged"));
                    return false;
                }
                first_index = static_cast<std::size_t>(
                    std::distance(facts.begin(), position));
                end_index = first_index + 1U;
            }
            if (first_index < end_index) {
                cut->phase_spans.push_back(
                    PhaseScanSpan{range, first_index, end_index});
            }
        }

        std::sort(
            cut->phase_spans.begin(), cut->phase_spans.end(),
            [](const PhaseScanSpan& left, const PhaseScanSpan& right) {
                return std::tie(left.range, left.next_index,
                                left.end_index) <
                    std::tie(right.range, right.next_index,
                             right.end_index);
            });
        std::size_t merged = 0U;
        for (const PhaseScanSpan& span : cut->phase_spans) {
            if (merged != 0U &&
                cut->phase_spans[merged - 1U].range == span.range &&
                span.next_index <=
                    cut->phase_spans[merged - 1U].end_index) {
                cut->phase_spans[merged - 1U].end_index = std::max(
                    cut->phase_spans[merged - 1U].end_index,
                    span.end_index);
                continue;
            }
            cut->phase_spans[merged++] = span;
        }
        cut->phase_spans.resize(merged);
        return true;
    }

    [[nodiscard]] std::size_t PendingPhaseDirtyGrowth(
        const PendingProjectionCut& cut,
        const FactKey& key,
        const FactRecord& record) const noexcept {
        std::size_t bytes = cut.phase_dirty_bundles.contains(key)
            ? 0U
            : TreeNodeOwnedBytes<std::pair<const FactKey, bool>>();
        for (const auto& [order, role] : record.roles) {
            static_cast<void>(role);
            const auto dirty = cut.phase_dirty_orders.find(order);
            if (dirty == cut.phase_dirty_orders.end()) {
                bytes = SaturatingAdd(
                    bytes,
                    TreeNodeOwnedBytes<
                        std::pair<const OrderKey, DirtyOrder>>());
                bytes = SaturatingAdd(
                    bytes, TreeNodeOwnedBytes<std::uint64_t>());
            } else if (!dirty->second.new_sequences.contains(
                           key.native_sequence)) {
                bytes = SaturatingAdd(
                    bytes, TreeNodeOwnedBytes<std::uint64_t>());
            }
        }
        return bytes;
    }

    [[nodiscard]] bool NormalizePendingPhaseFact(
        const FactKey& key,
        EventApplyResult* result,
        std::size_t* work_bytes) {
        PendingProjectionCut& cut = *pending_projection_cut_;
        const auto fact = facts_.find(key);
        if (fact == facts_.end()) {
            static_cast<void>(Failure(
                result, "Event pending phase fact disappeared"));
            return false;
        }
        FactRecord& record = fact->second;
        *work_bytes = PhaseFactWorkBytes(record);
        CanonicalTick projected{};
        if (!LoadProjectedTick(key, record, &projected)) {
            static_cast<void>(Failure(
                result, "Event pending phase fact read failed"));
            return false;
        }
        if (projected.common.identity.market != Market::kShanghai ||
            projected.action == TickAction::kStatus) {
            return true;
        }
        const std::optional<TradingPhase> phase = PhaseAt(
            InstrumentChannel(key, record), key.native_sequence);
        const TradingPhase next = phase.value_or(TradingPhase::kUnknown);
        const bool next_valid = phase.has_value() &&
            next != TradingPhase::kUnknown;
        const bool old_valid =
            (projected.validity & ingest::kTickPhaseValid) != 0U;
        if (projected.phase == next && old_valid == next_valid) {
            return true;
        }

        const bool inserted = std::binary_search(
            cut.inserted.begin(), cut.inserted.end(), key);
        if (!inserted) {
            if (record.roles.size() > 2U) {
                static_cast<void>(Failure(
                    result,
                    "Event non-status phase fact has unbounded role fanout"));
                return false;
            }
            const std::size_t growth = PendingPhaseDirtyGrowth(
                cut, key, record);
            if (!ReservePendingProjectionGrowth(growth, result)) {
                return false;
            }
        }

        projected.phase = next;
        if (next_valid) {
            projected.validity |= ingest::kTickPhaseValid;
        } else {
            projected.validity &= ~ingest::kTickPhaseValid;
        }
        record.projected_phase = projected.phase;
        record.projected_phase_valid = next_valid;
        record.fact_hash = HashFact(projected);
        if (!inserted) {
            cut.phase_dirty_bundles[key] = true;
            for (const auto& [order, role] : record.roles) {
                static_cast<void>(role);
                DirtyOrder& dirty = cut.phase_dirty_orders[order];
                dirty.new_sequences.insert(key.native_sequence);
                dirty.late = true;
                ++stats_.phase_dirty_roles_discovered;
            }
        }
        return true;
    }

    [[nodiscard]] bool ReconcilePendingProjectionBytes(
        EventApplyResult* result) noexcept {
        const std::size_t actual = PendingProjectionCutOwnedBytes(
            *pending_projection_cut_);
        const std::size_t accounted =
            pending_projection_cut_->accounted_bytes;
        return actual <= accounted || ReservePendingProjectionGrowth(
            actual - accounted, result);
    }

    [[nodiscard]] bool QueuePendingRepairOrder(
        const OrderKey& order,
        EventApplyResult* result) {
        PendingProjectionCut& cut = *pending_projection_cut_;
        if (cut.repair_orders.contains(order)) {
            return true;
        }
        const std::size_t old_deque = DequeOwnedBytes<OrderKey>(
            cut.order_frontier.size());
        const std::size_t next_deque = DequeOwnedBytes<OrderKey>(
            cut.order_frontier.size() + 1U);
        std::size_t growth = TreeNodeOwnedBytes<OrderKey>();
        if (next_deque > old_deque) {
            growth = SaturatingAdd(growth, next_deque - old_deque);
        }
        if (!ReservePendingProjectionGrowth(growth, result)) {
            return false;
        }
        cut.repair_orders.insert(order);
        cut.order_frontier.push_back(order);
        return true;
    }

    [[nodiscard]] bool QueuePendingRepairFact(
        const FactKey& fact,
        EventApplyResult* result) {
        PendingProjectionCut& cut = *pending_projection_cut_;
        if (cut.repair_facts.contains(fact)) {
            return true;
        }
        const std::size_t old_deque = DequeOwnedBytes<FactKey>(
            cut.fact_frontier.size());
        const std::size_t next_deque = DequeOwnedBytes<FactKey>(
            cut.fact_frontier.size() + 1U);
        std::size_t growth = TreeNodeOwnedBytes<FactKey>();
        if (next_deque > old_deque) {
            growth = SaturatingAdd(growth, next_deque - old_deque);
        }
        if (!ReservePendingProjectionGrowth(growth, result)) {
            return false;
        }
        cut.repair_facts.insert(fact);
        cut.fact_frontier.push_back(fact);
        return true;
    }

    [[nodiscard]] bool PendingPhaseSliceExhausted(
        std::size_t nodes,
        std::size_t bytes,
        std::chrono::steady_clock::time_point started) const noexcept {
        if (nodes >= config_.phase_slice_max_nodes ||
            bytes >= config_.phase_slice_max_bytes) {
            return true;
        }
        const auto elapsed_signed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started).count();
        const std::uint64_t elapsed = elapsed_signed <= 0
            ? 0U
            : static_cast<std::uint64_t>(elapsed_signed);
        return elapsed >= config_.phase_slice_max_cpu_ns;
    }

    [[nodiscard]] bool AdvancePendingProjectionCut(
        EventApplyResult* result) {
        if (!pending_projection_cut_.has_value()) {
            return true;
        }
        ++stats_.phase_normalization_slices;
        const auto started = std::chrono::steady_clock::now();
        std::size_t processed_nodes = 0U;
        std::size_t processed_bytes = 0U;
        while (pending_projection_cut_.has_value()) {
            PendingProjectionCut& cut = *pending_projection_cut_;
            std::size_t one_bytes = 0U;
            if (cut.stage == PendingCutStage::kNormalize) {
                while (cut.next_phase_span < cut.phase_spans.size() &&
                       cut.phase_spans[cut.next_phase_span].next_index >=
                           cut.phase_spans[cut.next_phase_span].end_index) {
                    ++cut.next_phase_span;
                }
                if (cut.next_phase_span == cut.phase_spans.size()) {
                    cut.stage = PendingCutStage::kRegisterInserted;
                    continue;
                }
                PhaseScanSpan& span = cut.phase_spans[cut.next_phase_span];
                const InstrumentFactIndex* const journal =
                    instrument_facts_.Find(span.range);
                if (journal == nullptr ||
                    journal->ordered.size() < span.end_index) {
                    static_cast<void>(Failure(
                        result, "Event pending phase cursor diverged"));
                    return false;
                }
                const std::uint64_t sequence =
                    journal->ordered[span.next_index];
                const FactKey key{
                    span.range.trade_date, span.range.market,
                    span.range.channel, sequence};
                if (!NormalizePendingPhaseFact(
                        key, result, &one_bytes)) {
                    return false;
                }
                ++span.next_index;
                ++stats_.phase_facts_scanned;
            } else if (cut.stage ==
                       PendingCutStage::kRegisterInserted) {
                if (cut.next_inserted == cut.inserted.size()) {
                    cut.stage = PendingCutStage::kSeedOrders;
                    cut.seed_order = cut.phase_dirty_orders.empty()
                        ? std::optional<OrderKey>{}
                        : std::optional<OrderKey>{
                              cut.phase_dirty_orders.begin()->first};
                    continue;
                }
                const FactKey key = cut.inserted[cut.next_inserted++];
                FactRecord& record = facts_.at(key);
                cut.phase_dirty_bundles[key] = record.late;
                if (!record.projectable) {
                    cut.saw_invalid = true;
                } else if (!cut.source_only_fast) {
                    if (!RegisterFactUses(
                            key, &record, &cut.phase_dirty_orders,
                            &cut.end_expansions, result)) {
                        if (healthy_) {
                            static_cast<void>(CapacityFailure(
                                result,
                                "Event OrderUseIndex capacity exhausted"));
                        }
                        return false;
                    }
                } else if (record.barrier &&
                           !InsertBarrier(
                               InstrumentChannel(key, record), key,
                               result)) {
                    return false;
                }
                if (!RefreshHotFactAccounting(key)) {
                    if (healthy_) {
                        static_cast<void>(Failure(
                            result, "Event hot fact accounting failed"));
                    } else {
                        result->code = EventApplyCode::kCapacityExhausted;
                    }
                    return false;
                }
                if (!ReconcilePendingProjectionBytes(result)) {
                    return false;
                }
                one_bytes = PhaseFactWorkBytes(record);
            } else if (cut.stage == PendingCutStage::kSeedOrders) {
                if (!cut.seed_order.has_value()) {
                    cut.stage = PendingCutStage::kSeedEndOrders;
                    cut.seed_end_expansion = 0U;
                    cut.seed_end_order = cut.phase_dirty_orders.empty()
                        ? std::optional<OrderKey>{}
                        : std::optional<OrderKey>{
                              cut.phase_dirty_orders.begin()->first};
                    continue;
                }
                const auto dirty = cut.phase_dirty_orders.find(
                    *cut.seed_order);
                if (dirty == cut.phase_dirty_orders.end()) {
                    static_cast<void>(Failure(
                        result, "Event pending dirty-order cursor diverged"));
                    return false;
                }
                const auto next = std::next(dirty);
                cut.seed_order = next == cut.phase_dirty_orders.end()
                    ? std::optional<OrderKey>{}
                    : std::optional<OrderKey>{next->first};
                if (dirty->second.late ||
                    (active_repair_.has_value() &&
                     active_repair_->tasks.contains(dirty->first))) {
                    if (!QueuePendingRepairOrder(dirty->first, result)) {
                        return false;
                    }
                }
                one_bytes = TreeNodeOwnedBytes<
                    std::pair<const OrderKey, DirtyOrder>>();
            } else if (cut.stage == PendingCutStage::kSeedEndOrders) {
                if (cut.seed_end_expansion ==
                    cut.end_expansions.size()) {
                    cut.stage = PendingCutStage::kSeedBundles;
                    cut.seed_bundle = cut.phase_dirty_bundles.empty()
                        ? std::optional<FactKey>{}
                        : std::optional<FactKey>{
                              cut.phase_dirty_bundles.begin()->first};
                    continue;
                }
                const EndExpansionTask& expansion =
                    cut.end_expansions[cut.seed_end_expansion];
                if (!cut.seed_end_order.has_value()) {
                    if (!QueuePendingRepairFact(
                            expansion.fact, result)) {
                        return false;
                    }
                    ++cut.seed_end_expansion;
                    cut.seed_end_order = cut.phase_dirty_orders.empty()
                        ? std::optional<OrderKey>{}
                        : std::optional<OrderKey>{
                              cut.phase_dirty_orders.begin()->first};
                    one_bytes = sizeof(EndExpansionTask);
                } else {
                    const auto dirty = cut.phase_dirty_orders.find(
                        *cut.seed_end_order);
                    if (dirty == cut.phase_dirty_orders.end()) {
                        static_cast<void>(Failure(
                            result,
                            "Event END dirty-order cursor diverged"));
                        return false;
                    }
                    const auto next = std::next(dirty);
                    cut.seed_end_order =
                        next == cut.phase_dirty_orders.end()
                        ? std::optional<OrderKey>{}
                        : std::optional<OrderKey>{next->first};
                    const OrderKey& order = dirty->first;
                    const InstrumentChannelKey range{
                        order.trade_date, order.market,
                        order.instrument_id, order.channel};
                    const OrderHistory* const history =
                        order_histories_.Find(order);
                    const std::size_t candidate_slot =
                        expansion.active_only && history != nullptr
                            ? history->active_end_slot
                            : history != nullptr
                                  ? history->instrument_order_slot
                                  : std::numeric_limits<std::size_t>::max();
                    if (range == expansion.range &&
                        history != nullptr &&
                        candidate_slot < expansion.candidate_count &&
                        !QueuePendingRepairOrder(order, result)) {
                        return false;
                    }
                    one_bytes = sizeof(OrderKey);
                }
            } else if (cut.stage == PendingCutStage::kSeedBundles) {
                if (!cut.seed_bundle.has_value()) {
                    cut.stage = PendingCutStage::kExpandComponents;
                    continue;
                }
                const auto dirty = cut.phase_dirty_bundles.find(
                    *cut.seed_bundle);
                if (dirty == cut.phase_dirty_bundles.end()) {
                    static_cast<void>(Failure(
                        result, "Event pending dirty-bundle cursor diverged"));
                    return false;
                }
                const auto next = std::next(dirty);
                cut.seed_bundle = next == cut.phase_dirty_bundles.end()
                    ? std::optional<FactKey>{}
                    : std::optional<FactKey>{next->first};
                if (dirty->second &&
                    !QueuePendingRepairFact(dirty->first, result)) {
                    return false;
                }
                one_bytes = TreeNodeOwnedBytes<
                    std::pair<const FactKey, bool>>();
            } else if (cut.stage ==
                       PendingCutStage::kExpandComponents) {
                if (!cut.active_fact.has_value() &&
                    !cut.fact_frontier.empty()) {
                    cut.active_fact = cut.fact_frontier.front();
                    cut.fact_frontier.pop_front();
                    cut.active_fact_role = 0U;
                }
                if (cut.active_fact.has_value()) {
                    const FactRecord& record = facts_.at(*cut.active_fact);
                    if (cut.active_fact_role == record.roles.size()) {
                        cut.active_fact.reset();
                        continue;
                    }
                    const OrderKey order =
                        record.roles[cut.active_fact_role++].first;
                    if (cut.phase_dirty_orders.contains(order) &&
                        !QueuePendingRepairOrder(order, result)) {
                        return false;
                    }
                    one_bytes = sizeof(std::pair<OrderKey, OrderRole>);
                } else {
                    if (!cut.active_order.has_value() &&
                        !cut.order_frontier.empty()) {
                        cut.active_order = cut.order_frontier.front();
                        cut.order_frontier.pop_front();
                        const auto dirty = cut.phase_dirty_orders.find(
                            *cut.active_order);
                        cut.active_order_sequence =
                            dirty == cut.phase_dirty_orders.end() ||
                                    dirty->second.new_sequences.empty()
                                ? std::optional<std::uint64_t>{}
                                : std::optional<std::uint64_t>{
                                      *dirty->second.new_sequences.begin()};
                    }
                    if (cut.active_order.has_value() &&
                        cut.active_order_sequence.has_value()) {
                        const OrderKey order = *cut.active_order;
                        const std::uint64_t sequence =
                            *cut.active_order_sequence;
                        const DirtyOrder& dirty =
                            cut.phase_dirty_orders.at(order);
                        const auto next =
                            dirty.new_sequences.upper_bound(sequence);
                        cut.active_order_sequence =
                            next == dirty.new_sequences.end()
                                ? std::optional<std::uint64_t>{}
                                : std::optional<std::uint64_t>{*next};
                        const FactKey key{
                            order.trade_date, order.market,
                            order.channel, sequence};
                        if (cut.phase_dirty_bundles.contains(key) &&
                            !QueuePendingRepairFact(key, result)) {
                            return false;
                        }
                        one_bytes = sizeof(std::uint64_t);
                    } else if (cut.active_order.has_value()) {
                        cut.active_order.reset();
                        continue;
                    } else if (cut.fact_frontier.empty() &&
                               cut.order_frontier.empty()) {
                        cut.stage = PendingCutStage::kFinalize;
                        continue;
                    }
                }
            } else {
                return FinalizePendingProjectionCut(result);
            }

            ++processed_nodes;
            processed_bytes = SaturatingAdd(processed_bytes, one_bytes);
            if (PendingPhaseSliceExhausted(
                    processed_nodes, processed_bytes, started)) {
                result->repair_pending = true;
                return true;
            }
        }
        return true;
    }

    [[nodiscard]] bool ReservePendingFinalizeScratch(
        EventApplyResult* result) noexcept {
        const PendingProjectionCut& cut = *pending_projection_cut_;
        std::size_t live_order_count = 0U;
        std::size_t maximum_live_evals = 0U;
        for (const auto& [order, dirty] : cut.phase_dirty_orders) {
            if (cut.repair_orders.contains(order)) {
                continue;
            }
            const OrderHistory* const history = order_histories_.Find(order);
            if (history == nullptr ||
                dirty.new_sequences.empty()) {
                static_cast<void>(Failure(
                    result, "Event live projection order disappeared"));
                return false;
            }
            ++live_order_count;
            // RecomputeOrder can stop early on convergence. Charging the whole
            // retained suffix avoids an unbounded counting pass before the
            // one-shot projection commit.
            maximum_live_evals = SaturatingAdd(
                maximum_live_evals, history->suffix.size());
        }

        std::size_t bytes = SaturatingMultiply(
            cut.inserted.size(), sizeof(FactKey));
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(live_order_count, sizeof(OrderKey)));
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                maximum_live_evals,
                TreeNodeOwnedBytes<
                    std::pair<const RoleCacheKey, RoleEval>>()));
        bytes = SaturatingAdd(
            bytes,
            SaturatingMultiply(
                maximum_live_evals,
                TreeNodeOwnedBytes<std::pair<const FactKey, bool>>()));
        return ReservePendingProjectionGrowth(bytes, result);
    }

    [[nodiscard]] bool FinalizePendingProjectionCut(
        EventApplyResult* result) {
        PendingProjectionCut& cut = *pending_projection_cut_;
        if (!ReservePendingFinalizeScratch(result)) {
            return false;
        }
        std::map<OrderKey, DirtyOrder> dirty_orders =
            std::move(cut.phase_dirty_orders);
        std::map<FactKey, bool> dirty_bundles =
            std::move(cut.phase_dirty_bundles);
        std::vector<EndExpansionTask> end_expansions =
            std::move(cut.end_expansions);
        const std::set<OrderKey>& repair_orders = cut.repair_orders;
        const std::set<FactKey>& repair_facts = cut.repair_facts;

        std::map<OrderKey, DirtyOrder> repair_dirty_orders;
        std::map<OrderKey, DirtyOrder> live_dirty_orders;
        while (!dirty_orders.empty()) {
            auto node = dirty_orders.extract(dirty_orders.begin());
            auto& output = repair_orders.contains(node.key())
                ? repair_dirty_orders
                : live_dirty_orders;
            output.insert(output.end(), std::move(node));
        }
        std::map<FactKey, bool> repair_dirty_bundles;
        std::map<FactKey, bool> live_dirty_bundles;
        while (!dirty_bundles.empty()) {
            auto node = dirty_bundles.extract(dirty_bundles.begin());
            auto& output = repair_facts.contains(node.key())
                ? repair_dirty_bundles
                : live_dirty_bundles;
            output.insert(output.end(), std::move(node));
        }
        std::vector<FactKey> repair_inserted;
        std::vector<FactKey> live_inserted;
        const std::size_t repair_inserted_count =
            static_cast<std::size_t>(std::count_if(
                cut.inserted.begin(), cut.inserted.end(),
                [&repair_facts](const FactKey& key) {
                    return repair_facts.contains(key);
                }));
        repair_inserted.reserve(repair_inserted_count);
        live_inserted.reserve(cut.inserted.size() - repair_inserted_count);
        for (const FactKey& key : cut.inserted) {
            (repair_facts.contains(key)
                 ? repair_inserted
                 : live_inserted)
                .push_back(key);
        }

        const bool has_repair_work =
            !repair_dirty_orders.empty() ||
            !repair_dirty_bundles.empty() ||
            !end_expansions.empty();
        if (has_repair_work &&
            !MergeRepair(
                repair_dirty_orders, repair_dirty_bundles,
                repair_inserted, end_expansions, result)) {
            if (healthy_) {
                static_cast<void>(Failure(
                    result, "Event repair transaction merge failed"));
            }
            return false;
        }

        std::map<RoleCacheKey, RoleEval> eval_patch;
        std::vector<OrderKey> touched_orders;
        touched_orders.reserve(live_dirty_orders.size());
        for (const auto& [order, dirty] : live_dirty_orders) {
            if (!RecomputeOrder(
                    order, dirty, &eval_patch,
                    &live_dirty_bundles, result)) {
                static_cast<void>(Failure(
                    result, "Event order-role projection failed"));
                return false;
            }
            touched_orders.push_back(order);
        }
        if (!live_dirty_bundles.empty() || !live_inserted.empty()) {
            if (!CommitProjection(
                    touched_orders, &eval_patch, nullptr,
                    &live_dirty_bundles, live_inserted, false, result)) {
                return false;
            }
        }
        result->code = cut.saw_conflict
            ? EventApplyCode::kSourceConflict
            : (cut.saw_invalid ? EventApplyCode::kInvalidInput
                               : EventApplyCode::kApplied);
        if (!AdvanceDurableCommits()) {
            result->code = EventApplyCode::kSinkFailed;
            return false;
        }
        for (const auto& [channel, floor] : cut.retention_requests) {
            QueueEviction(channel, floor);
        }

        pending_projection_cut_.reset();
        pending_projection_cut_accounted_bytes_ = 0U;
        PublishPendingPhaseBytes();
        stats_.repair_pending.store(
            active_repair_.has_value(), std::memory_order_release);
        if (!ContinueEviction()) {
            result->code = EventApplyCode::kFailed;
            return false;
        }
        result->repair_pending = active_repair_.has_value();
        return true;
    }

    [[nodiscard]] bool EnsureOrder(
        const OrderKey& order,
        std::uint64_t first_use_sequence,
        std::map<OrderKey, DirtyOrder>* dirty_orders,
        EventApplyResult* result,
        OrderHistory** output) {
        OrderHistory* position = order_histories_.Find(order);
        if (position == nullptr) {
            if (order_histories_.size() >= config_.maximum_carry_orders) {
                return false;
            }

            auto prepared_history = order_histories_.PrepareInsert(order);
            if (prepared_history.is_duplicate()) {
                return false;
            }
            const std::size_t history_peak =
                prepared_history.owned_byte_delta();
            const InstrumentChannelKey range{
                order.trade_date, order.market, order.instrument_id,
                order.channel};
            auto instrument_index =
                orders_by_instrument_channel_.find(range);
            const bool new_instrument_index =
                instrument_index == orders_by_instrument_channel_.end();
            std::optional<OrderIndexList::PreparedAppend>
                prepared_instrument;
            const std::size_t initial_list_bytes =
                OrderIndexList::AllocationOwnedBytes(std::min(
                    OrderIndexList::kInitialCapacity,
                    config_.maximum_carry_orders));
            std::size_t instrument_peak = initial_list_bytes;
            if (!new_instrument_index) {
                prepared_instrument.emplace(
                    instrument_index->second.PrepareAppend());
                instrument_peak =
                    prepared_instrument->peak_allocation_owned_bytes();
            }

            auto active_index = active_end_orders_.end();
            const bool needs_active_index =
                order.market == Market::kShanghai;
            bool new_active_index = false;
            std::optional<OrderIndexList::PreparedAppend> prepared_active;
            std::size_t active_peak = 0U;
            if (needs_active_index) {
                active_index = active_end_orders_.find(range);
                new_active_index = active_index == active_end_orders_.end();
                if (new_active_index) {
                    active_peak = initial_list_bytes;
                } else {
                    prepared_active.emplace(
                        active_index->second.PrepareAppend());
                    active_peak =
                        prepared_active->peak_allocation_owned_bytes();
                }
            }

            std::size_t reserved = history_peak;
            reserved = SaturatingAdd(reserved, instrument_peak);
            if (new_instrument_index) {
                reserved = SaturatingAdd(
                    reserved, OrderIndexTreeNodeOwnedBytes());
            }
            reserved = SaturatingAdd(reserved, active_peak);
            if (new_active_index) {
                reserved = SaturatingAdd(
                    reserved, OrderIndexTreeNodeOwnedBytes());
            }
            if (!ReserveOrderHistoryGrowth(reserved, result)) {
                return false;
            }

            bool history_inserted = false;
            bool instrument_node_inserted = false;
            bool instrument_appended = false;
            bool active_node_inserted = false;
            bool active_appended = false;
            OrderIndexList::AppendResult instrument_result{};
            OrderIndexList::AppendResult active_result{};
            const auto rollback = [&]() noexcept {
                std::size_t retained_reserved = 0U;
                std::size_t released_old_storage = 0U;
                bool consistent = true;
                try {
                    if (active_appended) {
                        const auto erased =
                            active_index->second.EraseAtSwap(
                                active_result.index);
                        consistent = consistent && !erased.moved.has_value();
                        if (!new_active_index && active_result.grew) {
                            retained_reserved = SaturatingAdd(
                                retained_reserved,
                                active_result.peak_allocation_owned_bytes);
                            released_old_storage = SaturatingAdd(
                                released_old_storage,
                                active_result.released_owned_bytes);
                        }
                    }
                    if (active_node_inserted) {
                        consistent = consistent &&
                            active_index->second.empty();
                        static_cast<void>(
                            active_index->second.ReleaseStorage());
                        active_end_orders_.erase(active_index);
                    }
                    if (instrument_appended) {
                        const auto erased =
                            instrument_index->second.EraseAtSwap(
                                instrument_result.index);
                        consistent = consistent && !erased.moved.has_value();
                        if (!new_instrument_index &&
                            instrument_result.grew) {
                            retained_reserved = SaturatingAdd(
                                retained_reserved,
                                instrument_result.
                                    peak_allocation_owned_bytes);
                            released_old_storage = SaturatingAdd(
                                released_old_storage,
                                instrument_result.released_owned_bytes);
                        }
                    }
                    if (instrument_node_inserted) {
                        consistent = consistent &&
                            instrument_index->second.empty();
                        static_cast<void>(
                            instrument_index->second.ReleaseStorage());
                        orders_by_instrument_channel_.erase(
                            instrument_index);
                    }
                    if (history_inserted) {
                        const auto erased = order_histories_.Erase(order);
                        consistent = consistent && erased.erased &&
                            erased.released_owned_bytes ==
                                history_peak;
                    }
                    consistent = consistent &&
                        retained_reserved <= reserved;
                    const std::size_t release = SaturatingAdd(
                        retained_reserved <= reserved
                            ? reserved - retained_reserved
                            : 0U,
                        released_old_storage);
                    consistent = consistent &&
                        ReleaseOrderHistoryBytes(release);
                } catch (...) {
                    consistent = false;
                }
                if (!consistent) {
                    SetFatal("Event order insertion rollback diverged");
                }
            };

            try {
                const auto inserted_history = order_histories_.CommitInsert(
                    std::move(prepared_history));
                if (!inserted_history.inserted ||
                    inserted_history.owned_byte_delta == 0U) {
                    throw std::logic_error(
                        "Event order table insertion diverged");
                }
                position = inserted_history.value;
                history_inserted = true;

                if (new_instrument_index) {
                    const auto [inserted, was_inserted] =
                        orders_by_instrument_channel_.try_emplace(
                            range, config_.maximum_carry_orders);
                    if (!was_inserted) {
                        throw std::logic_error(
                            "Event instrument order index insertion diverged");
                    }
                    instrument_index = inserted;
                    instrument_node_inserted = true;
                    prepared_instrument.emplace(
                        instrument_index->second.PrepareAppend());
                }
                if (prepared_instrument->peak_allocation_owned_bytes() !=
                    instrument_peak) {
                    throw std::logic_error(
                        "Event instrument order index layout changed");
                }
                instrument_result =
                    instrument_index->second.CommitAppend(
                        std::move(*prepared_instrument), order);
                instrument_appended = true;
                position->instrument_order_slot = instrument_result.index;

                if (needs_active_index) {
                    if (new_active_index) {
                        const auto [inserted, was_inserted] =
                            active_end_orders_.try_emplace(
                                range, config_.maximum_carry_orders);
                        if (!was_inserted) {
                            throw std::logic_error(
                                "Event active END index insertion diverged");
                        }
                        active_index = inserted;
                        active_node_inserted = true;
                        prepared_active.emplace(
                            active_index->second.PrepareAppend());
                    }
                    if (prepared_active->peak_allocation_owned_bytes() !=
                        active_peak) {
                        throw std::logic_error(
                            "Event active END index layout changed");
                    }
                    active_result = active_index->second.CommitAppend(
                        std::move(*prepared_active), order);
                    active_appended = true;
                    position->active_end_slot = active_result.index;
                }
            } catch (...) {
                rollback();
                throw;
            }

            const std::size_t released_old_storage = SaturatingAdd(
                instrument_result.released_owned_bytes,
                active_result.released_owned_bytes);
            if (!ReleaseOrderHistoryBytes(released_old_storage)) {
                return false;
            }
            const ChannelKey channel{
                order.trade_date, order.market, order.channel};
            const auto channel_state = channel_states_.find(channel);
            if (channel_state != channel_states_.end()) {
                position->baseline.compacted_before =
                    channel_state->second.sealed_before;
            }
        }
        const ChannelKey channel{
            order.trade_date, order.market, order.channel};
        const auto channel_state = channel_states_.find(channel);
        if (channel_state != channel_states_.end()) {
            position->baseline.compacted_before = std::max(
                position->baseline.compacted_before,
                channel_state->second.sealed_before);
        }

        const InstrumentChannelKey range{
            order.trade_date, order.market, order.instrument_id,
            order.channel};
        const BarrierRangeIndex* const barriers = barriers_.Find(range);
        if (barriers != nullptr) {
            for (auto barrier = barriers->lower_bound(
                     first_use_sequence);
                 barrier != barriers->end(); ++barrier) {
                if (FindOrderUse(
                        position, barrier->first) != nullptr) {
                    continue;
                }
                if (!SetAccountedOrderUse(
                        position, barrier->first,
                        OrderRole::kBarrier, result)) {
                    return false;
                }
                FactRecord& barrier_fact = facts_.at(barrier->second);
                AppendUniqueFactRole(
                    &barrier_fact, order, OrderRole::kBarrier);
                if (!RefreshHotFactAccounting(barrier->second)) {
                    return false;
                }
                DirtyOrder& dirty = (*dirty_orders)[order];
                dirty.new_sequences.insert(barrier->first);
                dirty.late = dirty.late || barrier_fact.late;
            }
        }
        *output = position;
        return true;
    }

    [[nodiscard]] bool AddDirectRole(
        FactRecord* record,
        std::uint64_t native_sequence,
        const OrderKey& order,
        OrderRole role,
        std::map<OrderKey, DirtyOrder>* dirty_orders,
        EventApplyResult* result) {
        OrderHistory* history = nullptr;
        if (!EnsureOrder(
                order, native_sequence, dirty_orders, result, &history)) {
            return false;
        }
        if (!SetAccountedOrderUse(
                history, native_sequence, role, result)) {
            return false;
        }
        UpsertFactRole(record, order, role);
        DirtyOrder& dirty = (*dirty_orders)[order];
        dirty.new_sequences.insert(native_sequence);
        dirty.late = dirty.late || record->late;
        static_cast<void>(result);
        return true;
    }

    [[nodiscard]] bool RegisterFactUses(
        const FactKey& fact_key,
        FactRecord* record,
        std::map<OrderKey, DirtyOrder>* dirty_orders,
        std::vector<EndExpansionTask>* end_expansions,
        EventApplyResult* result) {
        CanonicalTick fact{};
        if (!LoadProjectedTick(fact_key, *record, &fact)) {
            return false;
        }
        if (fact.action == TickAction::kAdd ||
            fact.action == TickAction::kCancel) {
            if (!AddDirectRole(
                    record, fact_key.native_sequence,
                    MakeOrderKey(fact, fact.primary_order_id),
                    OrderRole::kPrimary, dirty_orders, result)) {
                return false;
            }
        } else if (fact.action == TickAction::kTrade) {
            if (fact.common.identity.market == Market::kShenzhen &&
                fact.buy_order_id > 0 &&
                fact.buy_order_id == fact.sell_order_id) {
                // Ambiguous 6.36 references deliberately mutate neither role.
            } else {
                if (fact.buy_order_id > 0) {
                    if (!AddDirectRole(
                            record, fact_key.native_sequence,
                            MakeOrderKey(fact, fact.buy_order_id),
                            OrderRole::kBuy, dirty_orders, result)) {
                        return false;
                    }
                }
                if (fact.sell_order_id > 0) {
                    if (!AddDirectRole(
                            record, fact_key.native_sequence,
                            MakeOrderKey(fact, fact.sell_order_id),
                            OrderRole::kSell, dirty_orders, result)) {
                        return false;
                    }
                }
            }
        }

        if (record->barrier) {
            const InstrumentChannelKey range = InstrumentChannel(fact);
            const ChannelKey channel{
                fact.common.trade_date, fact.common.identity.market,
                fact.common.channel};
            const auto channel_state = channel_states_.find(channel);
            const bool active_only = channel_state == channel_states_.end() ||
                !channel_state->second.gap_open;
            const auto& candidate_index = active_only
                ? active_end_orders_
                : orders_by_instrument_channel_;
            const auto indexed_orders = candidate_index.find(range);
            const std::size_t candidate_count =
                indexed_orders == candidate_index.end()
                ? 0U
                : indexed_orders->second.size();
            const std::size_t projected_rows = SaturatingAdd(
                candidate_count, 1U);
            const std::size_t staging_bytes = SaturatingMultiply(
                candidate_count, EndCandidateStagingBytes());
            if (candidate_count > config_.maximum_end_candidates ||
                projected_rows > config_.maximum_end_projected_rows ||
                staging_bytes > config_.maximum_end_staging_bytes) {
                static_cast<void>(CapacityFailure(
                    result, "Event Shanghai END capacity exhausted"));
                return false;
            }
            if (!InsertBarrier(range, fact_key, result)) {
                return false;
            }
            if (candidate_count != 0U) {
                end_expansions->push_back(EndExpansionTask{
                    fact_key, range, candidate_count, 0U, staging_bytes,
                    active_only});
            }
        }
        return order_histories_.size() <= config_.maximum_carry_orders;
    }

    [[nodiscard]] const RoleEval* FindEval(
        const RoleCacheKey& key,
        const std::map<RoleCacheKey, RoleEval>* live_patch,
        const RepairTransaction* repair_overlay) const noexcept {
        if (repair_overlay != nullptr) {
            const auto task = repair_overlay->tasks.find(key.order);
            const auto patched = repair_overlay->eval_patch.find(key);
            if (task != repair_overlay->tasks.end() &&
                patched != repair_overlay->eval_patch.end() &&
                patched->second.attempt_generation ==
                    task->second.attempt_generation) {
                return &patched->second.eval;
            }
        }
        if (live_patch != nullptr) {
            const auto patched = live_patch->find(key);
            if (patched != live_patch->end()) {
                return &patched->second;
            }
        }
        const OrderHistory* const history = order_histories_.Find(key.order);
        if (history == nullptr) {
            return nullptr;
        }
        const OrderUseNode* use = FindOrderUse(
            *history, key.native_sequence);
        return use != nullptr && use->evaluated ? &use->eval : nullptr;
    }

    [[nodiscard]] bool RecomputeOrder(
        const OrderKey& order,
        const DirtyOrder& dirty,
        std::map<RoleCacheKey, RoleEval>* eval_patch,
        std::map<FactKey, bool>* dirty_bundles,
        EventApplyResult* result) {
        OrderHistory* const live = order_histories_.Find(order);
        if (live == nullptr ||
            dirty.new_sequences.empty()) {
            return false;
        }
        OrderHistory& history = *live;
        const std::uint64_t first = *dirty.new_sequences.begin();
        StateVersion previous = LatestBefore(history, first);
        auto use = history.suffix.lower_bound(first);
        while (use != history.suffix.end()) {
            const std::uint64_t sequence = use->first;
            const FactKey fact_key{order.trade_date, order.market,
                                   order.channel, sequence};
            const auto fact_position = facts_.find(fact_key);
            if (fact_position == facts_.end()) {
                return false;
            }
            const FactRecord& fact = fact_position->second;
            CanonicalTick projected_tick{};
            if (!LoadProjectedTick(fact_key, fact, &projected_tick)) {
                return false;
            }
            const RoleCacheKey cache_key{order, sequence};
            OrderUseNode& node = use->second;
            const RoleEval* old_eval = node.evaluated ? &node.eval : nullptr;
            const ApplyRoleResult applied = ApplyOrderRole(
                previous.state, projected_tick, node.role,
                previous.input_set_hash, fact.fact_hash);
            if (!applied.ok) {
                return false;
            }
            const bool state_converged = old_eval != nullptr &&
                applied.eval.post_state == old_eval->post_state;
            const bool new_use = dirty.new_sequences.contains(sequence);
            const bool state_unchanged_new_use = new_use &&
                old_eval == nullptr &&
                applied.eval.post_state == previous.state;
            const bool observable_changed = old_eval == nullptr ||
                !RoleObservableEqual(applied.eval, *old_eval);
            const bool cache_changed = old_eval == nullptr ||
                observable_changed ||
                old_eval->input_set_hash != applied.eval.input_set_hash ||
                old_eval->post_state_hash != applied.eval.post_state_hash;
            if (cache_changed) {
                (*eval_patch)[cache_key] = applied.eval;
            }
            if (observable_changed) {
                auto [position, inserted] = dirty_bundles->try_emplace(
                    fact_key, dirty.late);
                if (!inserted) {
                    position->second = position->second || dirty.late;
                }
            }
            node.eval = applied.eval;
            node.evaluated = true;

            if (dirty.late) {
                ++stats_.repaired_order_uses;
                ++result->repaired_order_uses;
            } else {
                ++stats_.live_order_uses;
            }

            previous = StateVersion{
                node.eval.post_state, node.eval.input_set_hash};
            if (state_converged || state_unchanged_new_use) {
                const auto next_new = dirty.new_sequences.upper_bound(sequence);
                if (next_new == dirty.new_sequences.end()) {
                    if (dirty.late) {
                        ++stats_.repair_convergence_stops;
                    }
                    break;
                }
                previous = LatestBefore(history, *next_new);
                use = history.suffix.lower_bound(*next_new);
                continue;
            }
            ++use;
        }
        return true;
    }

    [[nodiscard]] bool AssembleBundle(
        const FactKey& fact_key,
        const std::map<RoleCacheKey, RoleEval>* live_eval_patch,
        const RepairTransaction* repair_overlay,
        Bundle* output) {
        *output = {};
        FactRecord& fact = facts_.at(fact_key);
        if (!fact.projectable) {
            return true;
        }
        CanonicalTick projected_tick{};
        if (!LoadProjectedTick(fact_key, fact, &projected_tick)) {
            return false;
        }
        Bundle bundle{};
        SourceFragment source = ProjectSource(projected_tick);
        Identifier128 source_input_hash = fact.fact_hash;
        EnsureFactRolesOrdered(&fact);
        for (const auto& [order, role] : fact.roles) {
            const RoleEval* eval = FindEval(
                RoleCacheKey{order, fact_key.native_sequence},
                live_eval_patch, repair_overlay);
            if (eval == nullptr) {
                continue;
            }
            source_input_hash = CombineHashes(
                source_input_hash, eval->input_set_hash,
                static_cast<std::uint8_t>(role) + 16U);
            if (source.kind.has_value()) {
                source.payload.event_quality_flags |=
                    eval->source_quality_contribution;
                if (projected_tick.action == TickAction::kCancel) {
                    source.payload.referenced_order_found =
                        eval->referenced_order_found;
                    if (fact_key.market == Market::kShenzhen &&
                        eval->referenced_order_found) {
                        source.payload.side = eval->resolved_side;
                        source.payload.side_from_order =
                            eval->side_from_order;
                    }
                }
            }
            if (eval->order_operation.has_value() &&
                eval->post_state.has_value()) {
                EventKey key{fact_key.trade_date,
                             fact_key.market,
                             fact.instrument_id,
                             fact_key.channel,
                             fact_key.native_sequence,
                             fact_key.market == Market::kShanghai
                                 ? EventKind::kShanghaiOrderRevision
                                 : EventKind::kShenzhenOrderRevision,
                             order.order_id,
                             0U};
                while (bundle.rows.contains(key)) {
                    ++key.occurrence;
                }
                bundle.rows.emplace(
                    key,
                    MakeOrderPayload(
                        projected_tick, *eval->post_state,
                        *eval->order_operation));
                bundle.input_set_hashes.emplace(
                    key, eval->input_set_hash);
            }
        }
        if (source.kind.has_value()) {
            EventKey key{fact_key.trade_date,
                         fact_key.market,
                         fact.instrument_id,
                         fact_key.channel,
                         fact_key.native_sequence,
                         *source.kind,
                         0,
                         0U};
            bundle.rows.emplace(key, source.payload);
            bundle.input_set_hashes.emplace(key, source_input_hash);
        }
        *output = std::move(bundle);
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

    [[nodiscard]] const EventHead* FindHead(
        const EventKey& key,
        const std::map<EventKey, EventHead>& patch) const noexcept {
        const auto patched = patch.find(key);
        if (patched != patch.end()) {
            return &patched->second;
        }
        const auto ledger = event_heads_.find(MakeFactKey(key));
        if (ledger == event_heads_.end()) {
            return nullptr;
        }
        const auto live = ledger->second.find(key);
        return live == ledger->second.end() ? nullptr : &live->second;
    }

    [[nodiscard]] bool AddRevision(
        const EventKey& key,
        const EventPayload& payload,
        Identifier128 input_set_hash,
        RevisionOperation operation,
        RevisionReason reason,
        Identifier128 recovery_run_id,
        std::uint32_t* revision_counter,
        EventRevisionBatch* batch,
        std::map<EventKey, EventHead>* head_patch,
        EventApplyResult* result) {
        EventRevision revision{};
        revision.key = key;
        if (!AllocateVersion(revision_counter, &revision.version)) {
            return false;
        }
        revision.recovery_run_id = recovery_run_id;
        revision.operation = operation;
        revision.reason = reason;
        revision.calculation_run_id = config_.calculation_run_id;
        revision.logic_version = config_.logic_version;
        revision.input_set_hash = input_set_hash;
        revision.payload_hash = HashPayload(payload);
        revision.is_deleted = operation == RevisionOperation::kTombstone;
        revision.payload = payload;
        if (const EventHead* head = FindHead(key, *head_patch);
            head != nullptr) {
            revision.supersedes_revision_id = head->revision_id;
            revision.supersedes_revision_id_valid = true;
        }
        revision.revision_id = RevisionIdentifier(
            config_.calculation_run_id, recovery_run_id,
            revision.version, revision.payload_hash, operation);
        (*head_patch)[key] = EventHead{
            revision.revision_id, revision.payload_hash,
            revision.is_deleted};
        batch->revisions.push_back(std::move(revision));
        ++result->revisions_created;
        if (operation == RevisionOperation::kTombstone) {
            ++stats_.tombstones_created;
        }
        return true;
    }

    [[nodiscard]] bool DiffBundle(
        const Bundle& old_bundle,
        const Bundle& new_bundle,
        bool late,
        Identifier128 recovery_run_id,
        std::uint32_t* revision_counter,
        EventRevisionBatch* batch,
        std::map<EventKey, EventHead>* head_patch,
        EventApplyResult* result) {
        const RevisionReason reason = late
            ? RevisionReason::kHoleFill
            : RevisionReason::kLiveProjection;
        auto old_row = old_bundle.rows.begin();
        auto new_row = new_bundle.rows.begin();
        while (old_row != old_bundle.rows.end() ||
               new_row != new_bundle.rows.end()) {
            if (new_row == new_bundle.rows.end() ||
                (old_row != old_bundle.rows.end() &&
                 old_row->first < new_row->first)) {
                const Identifier128 input_hash =
                    old_bundle.input_set_hashes.at(old_row->first);
                if (!AddRevision(old_row->first, old_row->second,
                                 input_hash,
                                 RevisionOperation::kTombstone, reason,
                                 recovery_run_id, revision_counter, batch,
                                 head_patch, result)) {
                    return false;
                }
                ++old_row;
                continue;
            }
            if (old_row == old_bundle.rows.end() ||
                new_row->first < old_row->first) {
                const Identifier128 input_hash =
                    new_bundle.input_set_hashes.at(new_row->first);
                if (!AddRevision(new_row->first, new_row->second,
                                 input_hash,
                                 RevisionOperation::kInsert, reason,
                                 recovery_run_id, revision_counter, batch,
                                 head_patch, result)) {
                    return false;
                }
                ++new_row;
                continue;
            }
            if (!(old_row->second == new_row->second)) {
                const Identifier128 input_hash =
                    new_bundle.input_set_hashes.at(new_row->first);
                if (!AddRevision(new_row->first, new_row->second,
                                 input_hash,
                                 RevisionOperation::kUpdate, reason,
                                 recovery_run_id, revision_counter, batch,
                                 head_patch, result)) {
                    return false;
                }
            }
            ++old_row;
            ++new_row;
        }
        return true;
    }

    EventWorkerConfig config_{};
    EventRevisionSink* sink_ = nullptr;
    std::unordered_map<FactKey, FactRecord, FactKeyHash> facts_;
    // At most one owner micro-batch. This keeps the normal new-fact projection
    // path off disk; historical repair and cold duplicate validation use the
    // shared journal synchronously in this first implementation.
    std::vector<std::pair<FactKey, CanonicalTick>> batch_tick_cache_;
    std::unordered_map<ChannelKey, EventChannelState, ChannelKeyHash>
        channel_states_;
    BarrierTable barriers_;
    InstrumentFactTable instrument_facts_;
    PhaseTable phase_statuses_;
    OrderRangeIndex orders_by_instrument_channel_;
    OrderRangeIndex active_end_orders_;
    OrderHistoryTable order_histories_;
    std::unordered_map<FactKey, Bundle, FactKeyHash> bundle_cache_;
    std::unordered_map<FactKey, std::map<EventKey, EventHead>, FactKeyHash>
        event_heads_;
    ChannelFactTable channel_facts_;
    std::unordered_map<ChannelKey,
                       std::map<std::uint64_t,
                                std::pair<std::uint64_t, std::uint64_t>>,
                       ChannelKeyHash>
        gap_ranges_;
    std::map<ChannelKey, EvictionTask> eviction_tasks_;
    std::deque<ChannelKey> eviction_ready_;
    std::set<ChannelKey> eviction_ready_set_;
    std::deque<PendingCommit> pending_commits_;
    std::optional<RepairTransaction> active_repair_;
    std::optional<PendingProjectionCut> pending_projection_cut_;
    std::size_t pending_revision_bytes_ = 0U;
    std::size_t repair_accounted_bytes_ = 0U;
    std::size_t pending_projection_cut_accounted_bytes_ = 0U;
    std::size_t projection_scratch_bytes_ = 0U;
    bool projection_scratch_is_repair_ = false;
    bool projection_scratch_reserved_ = false;
    std::size_t hot_fact_bytes_ = 0U;
    // Fixed directories, lazy hash pages/nodes and conservative inner
    // deque/map ownership. This is a logical cap, not allocator RSS.
    std::size_t hot_index_bytes_ = 0U;
    std::size_t order_history_bytes_ = 0U;
    std::size_t cached_event_count_ = 0U;
    std::uint32_t next_revision_counter_ = 1U;
    std::uint64_t next_calculation_batch_sequence_ = 1U;
    AtomicEventWorkerStats stats_{};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

bool ValidateEventWorkerConfig(const EventWorkerConfig& config,
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
        config.logic_version == 0U || config.feed_session_epoch == 0U ||
        IsZero(config.calculation_run_id) ||
        config.maximum_carry_orders == 0U ||
        config.maximum_order_history_bytes == 0U ||
        config.maximum_hot_facts == 0U ||
        config.maximum_hot_fact_bytes == 0U ||
        config.maximum_cached_events == 0U ||
        config.maximum_pending_commits == 0U ||
        config.maximum_pending_revision_bytes == 0U ||
        config.persistence_group_max_batches == 0U ||
        config.persistence_group_max_rows == 0U ||
        config.persistence_group_max_bytes == 0U ||
        config.persistence_group_max_delay_ns == 0U ||
        config.maximum_repair_bytes == 0U ||
        config.maximum_end_candidates == 0U ||
        config.maximum_end_projected_rows == 0U ||
        config.maximum_end_staging_bytes == 0U ||
        config.end_slice_max_candidates == 0U ||
        config.end_slice_max_cpu_ns == 0U ||
        config.eviction_slice_max_nodes == 0U ||
        config.eviction_slice_max_bytes == 0U ||
        config.repair_slice_max_order_uses == 0U ||
        config.repair_slice_max_cpu_ns == 0U ||
        config.phase_slice_max_nodes == 0U ||
        config.phase_slice_max_bytes == 0U ||
        config.phase_slice_max_cpu_ns == 0U) {
        return fail("invalid Event worker configuration");
    }
    try {
        const std::array<std::size_t, 4U> hot_index_directories{
            BarrierTable::RequiredDirectoryOwnedBytes(
                config.maximum_hot_facts),
            InstrumentFactTable::RequiredDirectoryOwnedBytes(
                config.maximum_hot_facts),
            PhaseTable::RequiredDirectoryOwnedBytes(
                config.maximum_hot_facts),
            ChannelFactTable::RequiredDirectoryOwnedBytes(
                config.maximum_hot_facts)};
        std::size_t hot_index_directory_bytes = 0U;
        for (const std::size_t directory : hot_index_directories) {
            if (directory > std::numeric_limits<std::size_t>::max() -
                    hot_index_directory_bytes) {
                return fail("invalid Event hot-index directory layout");
            }
            hot_index_directory_bytes += directory;
        }
        if (hot_index_directory_bytes >
            config.maximum_hot_fact_bytes) {
            return fail(
                "Event hot-index directory exceeds byte capacity");
        }
        if (OrderHistoryTable::RequiredDirectoryOwnedBytes(
                config.maximum_carry_orders) >
            config.maximum_order_history_bytes) {
            return fail(
                "Event order-history directory exceeds byte capacity");
        }
    } catch (...) {
        return fail("invalid Event fixed-table layout");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

std::unique_ptr<EventWorker> EventWorker::Create(
    EventWorkerConfig config,
    EventRevisionSink* sink,
    std::string* error) {
    if (!ValidateEventWorkerConfig(config, error)) {
        return nullptr;
    }
    if (sink == nullptr) {
        if (error != nullptr) {
            *error = "Event revision sink is null";
        }
        return nullptr;
    }
    try {
        if (config.fact_journal == nullptr) {
            if (error != nullptr) {
                *error = "Event FactJournal is null";
            }
            return nullptr;
        }
        if (!config.fact_journal->healthy()) {
            if (error != nullptr) {
                *error = "Event FactJournal is unhealthy: " +
                    config.fact_journal->fatal_error();
            }
            return nullptr;
        }
        return std::unique_ptr<EventWorker>(new EventWorker(
            std::make_unique<Impl>(std::move(config), sink)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("Event worker creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

EventWorker::EventWorker(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

EventWorker::~EventWorker() = default;

EventApplyResult EventWorker::ApplyBatch(
    std::span<const EventInput> inputs) noexcept {
    return impl_->ApplyBatch(inputs);
}

bool EventWorker::ApplyGapOpen(const GapOpen& gap) noexcept {
    return impl_->ApplyGapOpen(gap);
}

bool EventWorker::ApplyChannelSeal(const ChannelSeal& seal) noexcept {
    return impl_->ApplyChannelSeal(seal);
}

bool EventWorker::ContinueEviction() noexcept {
    return impl_->ContinueEviction();
}

bool EventWorker::AdvanceRepair() noexcept {
    return impl_->AdvanceRepair();
}

bool EventWorker::AdvanceDurableCommits() noexcept {
    return impl_->AdvanceDurableCommits();
}

bool EventWorker::FlushDurableCommits() noexcept {
    return impl_->FlushDurableCommits();
}

bool EventWorker::repair_pending() const noexcept {
    return impl_->repair_pending();
}

bool EventWorker::eviction_pending() const noexcept {
    return impl_->eviction_pending();
}

bool EventWorker::projection_input_fenced() const noexcept {
    return impl_->projection_input_fenced();
}

bool EventWorker::CopyBundle(
    const FactKey& key,
    std::vector<std::pair<EventKey, EventPayload>>* output) const {
    return impl_->CopyBundle(key, output);
}

bool EventWorker::CopyOrder(const OrderKey& key,
                            OrderSnapshot* output) const noexcept {
    return impl_->CopyOrder(key, output);
}

bool EventWorker::CopyChannelState(
    Market market,
    std::uint32_t channel,
    EventChannelState* output) const noexcept {
    return impl_->CopyChannelState(market, channel, output);
}

bool EventWorker::healthy() const noexcept { return impl_->healthy(); }

std::string EventWorker::fatal_error() const {
    return impl_->fatal_error();
}

EventWorkerStats EventWorker::stats() const noexcept {
    return impl_->stats();
}

const EventWorkerConfig& EventWorker::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::event
