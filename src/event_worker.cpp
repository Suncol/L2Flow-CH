#include "l2flow/event/worker.h"

#include "l2flow/clickhouse/raw_sink.h"

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
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace l2flow::event {
namespace {

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

struct RawTickDependencyHash final {
    [[nodiscard]] std::size_t operator()(
        const RawTickDependency& dependency) const noexcept {
        std::uint64_t value = dependency.ingress_sequence +
            UINT64_C(0x9e3779b97f4a7c15);
        value ^= value >> 30U;
        value *= UINT64_C(0xbf58476d1ce4e5b9);
        value ^= value >> 27U;
        value *= UINT64_C(0x94d049bb133111eb);
        value ^= value >> 31U;
        value ^= static_cast<std::uint64_t>(dependency.kind) *
            UINT64_C(0x517cc1b727220a95);
        if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
            value ^= value >> 32U;
        }
        return static_cast<std::size_t>(value);
    }
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
    return FactKey{tick.common.trade_date,
                   tick.common.identity.market,
                   tick.common.channel,
                   tick.common.native_sequence};
}

[[nodiscard]] OrderKey MakeOrderKey(const CanonicalTick& tick,
                                    std::int64_t order_id) noexcept {
    return OrderKey{tick.common.trade_date,
                    tick.common.identity.market,
                    tick.common.instrument_id,
                    tick.common.channel,
                    order_id};
}

[[nodiscard]] bool IdentityEqual(const ingest::ExactIdentity& left,
                                 const ingest::ExactIdentity& right) noexcept {
    return left.market == right.market &&
           left.security_id_source_size ==
               right.security_id_source_size &&
           left.security_id_size == right.security_id_size &&
           std::equal(left.security_id_source.begin(),
                      left.security_id_source.end(),
                      right.security_id_source.begin()) &&
           std::equal(left.security_id.begin(), left.security_id.end(),
                      right.security_id.begin());
}

[[nodiscard]] bool DecimalEqual(const ingest::FixedDecimal& left,
                                const ingest::FixedDecimal& right) noexcept {
    return left.raw == right.raw &&
           left.source_scale == right.source_scale &&
           left.raw_valid == right.raw_valid;
}

[[nodiscard]] bool QuantityEqual(const ingest::ScaledInteger& left,
                                 const ingest::ScaledInteger& right) noexcept {
    return left.raw == right.raw && left.scale == right.scale &&
           left.valid == right.valid;
}

// Arrival provenance and recovery annotations are intentionally excluded.
// This is the same canonical business-payload comparison used by upstream
// SequenceRecovery, with catalog identity added explicitly.
[[nodiscard]] bool FactPayloadEqual(const CanonicalTick& left,
                                    const CanonicalTick& right) noexcept {
    return left.common.trade_date == right.common.trade_date &&
           left.common.instrument_id == right.common.instrument_id &&
           left.common.message_key == right.common.message_key &&
           left.common.kind == right.common.kind &&
           left.common.channel == right.common.channel &&
           left.common.native_sequence == right.common.native_sequence &&
           left.common.exchange_time_raw == right.common.exchange_time_raw &&
           IdentityEqual(left.common.identity, right.common.identity) &&
           DecimalEqual(left.price, right.price) &&
           DecimalEqual(left.amount, right.amount) &&
           QuantityEqual(left.quantity, right.quantity) &&
           left.primary_order_id == right.primary_order_id &&
           left.buy_order_id == right.buy_order_id &&
           left.sell_order_id == right.sell_order_id &&
           left.sh_add_matched_quantity_raw ==
               right.sh_add_matched_quantity_raw &&
           left.raw_type == right.raw_type &&
           left.raw_side == right.raw_side &&
           left.action == right.action && left.side == right.side &&
           left.aggressor == right.aggressor &&
           left.order_type == right.order_type &&
           left.phase == right.phase;
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
    std::optional<EventPayload> order_fragment;
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
            result.eval.order_fragment =
                MakeOrderPayload(fact, state, operation);
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
            result.eval.order_fragment =
                MakeOrderPayload(fact, state, operation);
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
            result.eval.order_fragment =
                MakeOrderPayload(fact, state, operation);
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
        result.eval.order_fragment =
            MakeOrderPayload(fact, state, operation);
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
        result.eval.order_fragment =
            MakeOrderPayload(fact, state, operation);
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
        result.eval.order_fragment =
            MakeOrderPayload(fact, state, operation);
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
        result.eval.order_fragment =
            MakeOrderPayload(fact, state, operation);
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

struct OrderHistory final {
    std::map<std::uint64_t, OrderRole> uses;
    std::map<std::uint64_t, StateVersion> versions;
    std::uint64_t generation = 0U;
};

struct FactRecord final {
    EventInput input{};
    CanonicalTick source_tick{};
    Identifier128 fact_hash{};
    SourceFragment source{};
    std::map<OrderKey, OrderRole> roles;
    bool projectable = false;
    bool late = false;
    bool barrier = false;
};

struct Bundle final {
    std::map<EventKey, EventPayload> rows;
    std::map<EventKey, Identifier128> input_set_hashes;
};

struct InstrumentFactIndex final {
    // Fact positions are append-ordered on the normal SequenceRecovery path.
    // A late insertion uses lower_bound and remains bounded by the per-
    // instrument journal; this avoids one tree node allocation per normal
    // fact while retaining ordered range traversal for phase repair.
    std::vector<FactKey> ordered;
};

struct PhaseIndex final {
    std::vector<std::pair<std::uint64_t, TradingPhase>> ordered;
};

void InsertInstrumentFact(InstrumentFactIndex* index,
                          std::uint64_t sequence,
                          const FactKey& key) {
    auto& values = index->ordered;
    if (values.empty() || values.back().native_sequence < sequence) {
        values.push_back(key);
        return;
    }
    const auto position = std::lower_bound(
        values.begin(), values.end(), sequence,
        [](const FactKey& fact, std::uint64_t value) {
            return fact.native_sequence < value;
        });
    if (position == values.end() ||
        position->native_sequence != sequence) {
        values.insert(position, key);
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

struct PendingCommit final {
    std::shared_ptr<const EventRevisionBatch> batch;
    std::set<RawTickDependency> raw_dependencies;
};

struct DirtyOrder final {
    std::set<std::uint64_t> new_sequences;
    bool late = false;
};

struct OrderPatch final {
    OrderKey key{};
    OrderHistory history{};
};

struct RepairOrderTask final {
    OrderKey key{};
    DirtyOrder dirty{};
    OrderHistory history{};
    StateVersion previous{};
    std::optional<std::uint64_t> cursor;
    std::uint64_t observed_generation = 0U;
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
    std::map<RoleCacheKey, RoleEval> eval_patch;
    std::map<FactKey, bool> dirty_bundles;
    std::set<RawTickDependency> raw_dependencies;
    std::set<FactKey> inserted_facts;
    std::optional<OrderKey> next_order;
    bool late_recovery = false;
};

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
    std::atomic<std::uint64_t> acknowledged_raw_dependencies{0U};
    std::atomic<std::uint64_t> revision_batches_submitted{0U};
    std::atomic<std::uint64_t> active_repair_orders{0U};
    std::atomic<std::uint64_t> ordered_batch_fast_path{0U};
    std::atomic<std::uint64_t> unordered_batch_sorts{0U};
    std::atomic<std::uint64_t> barrier_index_orders_visited{0U};
    std::atomic<std::uint64_t> source_only_fast_path{0U};
    std::atomic<bool> repair_pending{false};
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
        tick.common.instrument_id == 0U || tick.common.channel == 0U ||
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

[[nodiscard]] bool RoleObservableEqual(const RoleEval& left,
                                       const RoleEval& right) noexcept {
    return left.post_state == right.post_state &&
           left.order_fragment == right.order_fragment &&
           left.source_quality_contribution ==
               right.source_quality_contribution &&
           left.referenced_order_found == right.referenced_order_found &&
           left.resolved_side == right.resolved_side &&
           left.side_from_order == right.side_from_order;
}

[[nodiscard]] StateVersion LatestBefore(const OrderHistory& history,
                                        std::uint64_t sequence) noexcept {
    const auto position = history.versions.lower_bound(sequence);
    if (position == history.versions.begin()) {
        return {};
    }
    return std::prev(position)->second;
}

[[nodiscard]] bool SetOrderUse(OrderHistory* history,
                               std::uint64_t sequence,
                               OrderRole role) {
    const auto existing = history->uses.find(sequence);
    if (existing != history->uses.end() && existing->second == role) {
        return true;
    }
    if (history->generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    if (existing == history->uses.end()) {
        history->uses.emplace(sequence, role);
    } else {
        existing->second = role;
    }
    ++history->generation;
    return true;
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
        : config_(std::move(config)), sink_(sink) {
        // Reserve the complete bounded ACK index up front.  This keeps the
        // owner hot path free of rehash pauses while preserving the explicit
        // in-memory capacity/fail-closed contract.
        acknowledged_raw_.reserve(
            config_.maximum_acknowledged_raw_dependencies);
        const auto reserve_bounded = [](auto* table, std::size_t limit) {
            constexpr std::size_t kMaximumInitialBuckets = 65'536U;
            table->max_load_factor(0.80F);
            table->reserve(std::min(limit, kMaximumInitialBuckets));
        };
        reserve_bounded(&facts_, config_.maximum_facts);
        reserve_bounded(&channel_states_, 1'024U);
        reserve_bounded(&barriers_, 4'096U);
        reserve_bounded(&instrument_facts_, 4'096U);
        reserve_bounded(&phase_statuses_, 4'096U);
        reserve_bounded(&orders_by_instrument_channel_,
                        4'096U);
        reserve_bounded(&order_histories_, config_.maximum_orders);
        reserve_bounded(&role_cache_, config_.maximum_orders);
        reserve_bounded(&bundle_cache_, config_.maximum_facts);
        reserve_bounded(&event_heads_, config_.maximum_cached_events);
    }

    [[nodiscard]] EventApplyResult ApplyBatch(
        std::span<const EventInput> inputs) noexcept {
        EventApplyResult result{};
        if (!healthy_) {
            result.code = EventApplyCode::kFailed;
            return result;
        }
        if (inputs.empty()) {
            result.code = EventApplyCode::kDuplicateOnly;
            return result;
        }
        if (!DrainDurableCommits()) {
            result.code = EventApplyCode::kSinkFailed;
            return result;
        }
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            result.code = EventApplyCode::kCapacityExhausted;
            return result;
        }

        try {
            std::vector<FactKey> inserted;
            inserted.reserve(inputs.size());
            std::map<ChannelKey, std::uint64_t> cut_frontiers;
            std::set<RawTickDependency> dependencies;
            bool saw_conflict = false;
            bool saw_invalid = false;
            bool source_only_candidate = order_histories_.empty();

            // Capture every touched frontier before this cut changes it, then
            // journal all facts before registering or applying any role.
            for (const EventInput& input : inputs) {
                if (!StructurallyValid(config_, input)) {
                    saw_invalid = true;
                    source_only_candidate = false;
                    continue;
                }
                if (input.upstream_conflict) {
                    continue;
                }
                if (input.tick.common.identity.market != Market::kShanghai ||
                    input.tick.action != TickAction::kStatus) {
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
            }

            for (const EventInput& input : inputs) {
                if (!StructurallyValid(config_, input)) {
                    continue;
                }
                dependencies.insert(RawTickDependency{
                    input.tick.common.ingress_sequence,
                    input.tick.common.kind});
                const FactKey key = MakeFactKey(input.tick);
                const ChannelKey channel{
                    key.trade_date, key.market, key.channel};
                EventChannelState& channel_state = channel_states_[channel];
                if (input.committed_next_sequence != 0U) {
                    channel_state.committed_next_sequence = std::max(
                        channel_state.committed_next_sequence,
                        input.committed_next_sequence);
                }
                channel_state.gap_epoch = std::max(
                    channel_state.gap_epoch, input.observed_gap_epoch);
                if (input.upstream_conflict) {
                    ++stats_.source_conflicts;
                    saw_conflict = true;
                    continue;
                }
                const auto existing = facts_.find(key);
                if (existing != facts_.end()) {
                    if (FactPayloadEqual(existing->second.source_tick,
                                         input.tick)) {
                        ++result.duplicates;
                        ++stats_.duplicate_facts;
                    } else {
                        ++stats_.source_conflicts;
                        saw_conflict = true;
                    }
                    continue;
                }
                if (facts_.size() >= config_.maximum_facts) {
                    return CapacityFailure(&result,
                                           "Event FactJournal capacity exhausted");
                }
                FactRecord record{};
                record.input = input;
                record.source_tick = input.tick;
                record.fact_hash = HashFact(input.tick);
                record.projectable = ProjectionInputValid(input.tick);
                source_only_candidate = source_only_candidate &&
                    record.projectable;
                record.late = input.late_recovery ||
                    key.native_sequence <= cut_frontiers.at(channel);
                if (record.projectable) {
                    record.source = ProjectSource(input.tick);
                    record.barrier =
                        key.market == Market::kShanghai &&
                        input.tick.action == TickAction::kStatus &&
                        (input.tick.validity & ingest::kTickPhaseValid) != 0U &&
                        input.tick.phase == TradingPhase::kEnded;
                }
                facts_.emplace(key, std::move(record));
                inserted.push_back(key);
                InsertInstrumentFact(
                    &instrument_facts_[InstrumentChannel(input.tick)],
                    key.native_sequence, key);
                if (key.market == Market::kShanghai &&
                    input.tick.action == TickAction::kStatus &&
                    (input.tick.validity & ingest::kTickPhaseValid) != 0U) {
                    InsertPhase(
                        &phase_statuses_[InstrumentChannel(input.tick)],
                        key.native_sequence, input.tick.phase);
                }
                channel_state.journal_tail = std::max(
                    channel_state.journal_tail, key.native_sequence);
                ++result.facts_inserted;
                ++stats_.facts_journaled;
            }

            if (inserted.empty()) {
                QueueNoopDependencies(std::move(dependencies));
                static_cast<void>(DrainDurableCommits());
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
                ++stats_.unordered_batch_sorts;
            }
            const bool source_only_fast = source_only_candidate &&
                order_histories_.empty();
            if (source_only_fast) {
                ++stats_.source_only_fast_path;
            }
            std::set<FactKey> phase_changed = source_only_fast
                ? std::set<FactKey>{}
                : NormalizeShanghaiPhases(inserted);
            std::map<OrderKey, DirtyOrder> dirty_orders;
            std::map<FactKey, bool> dirty_bundles;
            for (const FactKey& key : inserted) {
                FactRecord& record = facts_.at(key);
                dirty_bundles[key] = record.late;
                if (!record.projectable) {
                    saw_invalid = true;
                    continue;
                }
                if (!source_only_fast) {
                    if (!RegisterFactUses(key, &record, &dirty_orders,
                                          &result)) {
                        return CapacityFailure(
                            &result,
                            "Event OrderUseIndex capacity exhausted");
                    }
                } else if (record.barrier) {
                    // A source-only cut has no existing orders to walk, but
                    // END must still be retained for an order that arrives
                    // later through late recovery.  RegisterFactUses also
                    // applies the barrier to existing orders; this branch
                    // only needs the persistent in-memory barrier index.
                    barriers_[InstrumentChannel(record.source_tick)]
                        [key.native_sequence] = key;
                }
            }
            for (const FactKey& key : phase_changed) {
                if (std::binary_search(inserted.begin(), inserted.end(),
                                       key)) {
                    continue;
                }
                FactRecord& record = facts_.at(key);
                dirty_bundles[key] = true;
                for (const auto& [order, role] : record.roles) {
                    static_cast<void>(role);
                    DirtyOrder& dirty = dirty_orders[order];
                    dirty.new_sequences.insert(key.native_sequence);
                    dirty.late = true;
                }
            }

            // Split this journal cut by connected components in the
            // Fact-to-Order graph. A fact joins repair when it is itself late,
            // references an active dirty order, or is connected through its
            // other trade role. Disjoint components retain the append path.
            std::set<OrderKey> repair_orders;
            std::set<FactKey> repair_facts;
            for (const auto& [order, dirty] : dirty_orders) {
                if (dirty.late ||
                    (active_repair_.has_value() &&
                     active_repair_->tasks.contains(order))) {
                    repair_orders.insert(order);
                }
            }
            for (const auto& [key, late] : dirty_bundles) {
                if (late) {
                    repair_facts.insert(key);
                }
            }
            bool expanded = true;
            while (expanded) {
                expanded = false;
                for (const FactKey& key : repair_facts) {
                    const FactRecord& record = facts_.at(key);
                    for (const auto& [order, role] : record.roles) {
                        static_cast<void>(role);
                        if (dirty_orders.contains(order) &&
                            repair_orders.insert(order).second) {
                            expanded = true;
                        }
                    }
                }
                for (const OrderKey& order : repair_orders) {
                    const auto dirty = dirty_orders.find(order);
                    if (dirty == dirty_orders.end()) {
                        continue;
                    }
                    for (const std::uint64_t sequence :
                         dirty->second.new_sequences) {
                        const FactKey key{
                            order.trade_date, order.market,
                            order.channel, sequence};
                        if (dirty_bundles.contains(key) &&
                            repair_facts.insert(key).second) {
                            expanded = true;
                        }
                    }
                }
            }

            std::map<OrderKey, DirtyOrder> repair_dirty_orders;
            std::map<OrderKey, DirtyOrder> live_dirty_orders;
            for (const auto& [order, dirty] : dirty_orders) {
                (repair_orders.contains(order)
                     ? repair_dirty_orders
                     : live_dirty_orders)
                    .emplace(order, dirty);
            }
            std::map<FactKey, bool> repair_dirty_bundles;
            std::map<FactKey, bool> live_dirty_bundles;
            for (const auto& [key, late] : dirty_bundles) {
                (repair_facts.contains(key)
                     ? repair_dirty_bundles
                     : live_dirty_bundles)
                    .emplace(key, late);
            }
            std::vector<FactKey> repair_inserted;
            std::vector<FactKey> live_inserted;
            repair_inserted.reserve(inserted.size());
            live_inserted.reserve(inserted.size());
            for (const FactKey& key : inserted) {
                (repair_facts.contains(key)
                     ? repair_inserted
                     : live_inserted)
                    .push_back(key);
            }

            std::set<RawTickDependency> repair_dependencies;
            std::set<RawTickDependency> live_dependencies;
            const auto add_fact_dependency = [this](
                const FactKey& key,
                std::set<RawTickDependency>* output) {
                const CanonicalTick& tick = facts_.at(key).source_tick;
                output->insert(RawTickDependency{
                    tick.common.ingress_sequence, tick.common.kind});
            };
            for (const FactKey& key : repair_inserted) {
                add_fact_dependency(key, &repair_dependencies);
            }
            for (const FactKey& key : live_inserted) {
                add_fact_dependency(key, &live_dependencies);
            }
            for (const RawTickDependency& dependency : dependencies) {
                if (!repair_dependencies.contains(dependency) &&
                    !live_dependencies.contains(dependency)) {
                    (!live_inserted.empty()
                         ? live_dependencies
                         : repair_dependencies)
                        .insert(dependency);
                }
            }

            const bool has_repair_work =
                !repair_dirty_orders.empty() ||
                !repair_dirty_bundles.empty();
            if (has_repair_work &&
                !MergeRepair(
                    repair_dirty_orders, repair_dirty_bundles,
                    repair_inserted, std::move(repair_dependencies))) {
                return Failure(
                    &result, "Event repair transaction merge failed");
            }

            std::map<RoleCacheKey, RoleEval> eval_patch;
            std::vector<OrderPatch> order_patches;
            order_patches.reserve(live_dirty_orders.size());
            for (const auto& [order, dirty] : live_dirty_orders) {
                OrderPatch patch{};
                if (!RecomputeOrder(order, dirty, &patch, &eval_patch,
                                    &live_dirty_bundles, &result)) {
                    return Failure(&result,
                                   "Event order-role projection failed");
                }
                order_patches.push_back(std::move(patch));
            }
            if (!live_dirty_bundles.empty() || !live_inserted.empty()) {
                if (!CommitProjection(
                        &order_patches, &eval_patch,
                        &live_dirty_bundles, live_inserted,
                        std::move(live_dependencies), false, &result)) {
                    return result;
                }
            } else if (!live_dependencies.empty()) {
                QueueNoopDependencies(std::move(live_dependencies));
            }
            result.code = saw_conflict
                ? EventApplyCode::kSourceConflict
                : (saw_invalid ? EventApplyCode::kInvalidInput
                               : EventApplyCode::kApplied);
            if (!DrainDurableCommits()) {
                result.code = EventApplyCode::kSinkFailed;
                return result;
            }
            if (active_repair_.has_value() &&
                !AdvanceRepairSlice(has_repair_work ? &result : nullptr)) {
                return result;
            }
            result.repair_pending = active_repair_.has_value();
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

    [[nodiscard]] bool AdvanceRepair() noexcept {
        return AdvanceRepairSlice(nullptr);
    }

    void AcknowledgeRawTicks(
        std::span<const RawTickDependency> dependencies) noexcept {
        if (!healthy_) {
            return;
        }
        try {
            for (const RawTickDependency& dependency : dependencies) {
                if (acknowledged_raw_.contains(dependency)) {
                    continue;
                }
                if (acknowledged_raw_.size() >=
                    config_.maximum_acknowledged_raw_dependencies) {
                    SetFatal("Event raw ACK index capacity exhausted");
                    return;
                }
                acknowledged_raw_.insert(dependency);
            }
            stats_.acknowledged_raw_dependencies.store(
                acknowledged_raw_.size(), std::memory_order_relaxed);
        } catch (...) {
            SetFatal("Event raw ACK index allocation failed");
        }
    }

    [[nodiscard]] bool DrainDurableCommits() noexcept {
        if (!healthy_) {
            return false;
        }
        while (!pending_commits_.empty()) {
            PendingCommit& commit = pending_commits_.front();
            const bool durable = std::all_of(
                commit.raw_dependencies.begin(),
                commit.raw_dependencies.end(),
                [this](const RawTickDependency& dependency) {
                    return acknowledged_raw_.contains(dependency);
                });
            if (!durable) {
                break;
            }
            if (!commit.batch->revisions.empty()) {
                if (!sink_->AppendRevisionBatch(commit.batch)) {
                    SetFatal(
                        "Event revision sink rejected an immutable batch");
                    return false;
                }
                ++stats_.revision_batches_submitted;
            }
            for (const RawTickDependency& dependency :
                 commit.raw_dependencies) {
                acknowledged_raw_.erase(dependency);
            }
            stats_.acknowledged_raw_dependencies.store(
                acknowledged_raw_.size(), std::memory_order_relaxed);
            pending_commits_.pop_front();
            stats_.pending_raw_commits.store(
                pending_commits_.size(), std::memory_order_relaxed);
        }
        return true;
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
        const auto position = order_histories_.find(key);
        if (position == order_histories_.end() ||
            position->second.versions.empty()) {
            return false;
        }
        const StateVersion& latest =
            position->second.versions.rbegin()->second;
        if (!latest.state.has_value()) {
            return false;
        }
        *output = latest.state->snapshot;
        return true;
    }

    [[nodiscard]] bool CopyChannelState(
        Market market,
        std::uint32_t channel,
        EventChannelState* output) const noexcept {
        if (output == nullptr || channel == 0U ||
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
        result.acknowledged_raw_dependencies =
            stats_.acknowledged_raw_dependencies.load(
                std::memory_order_relaxed);
        result.revision_batches_submitted =
            stats_.revision_batches_submitted.load(
                std::memory_order_relaxed);
        result.active_repair_orders = stats_.active_repair_orders.load(
            std::memory_order_relaxed);
        result.ordered_batch_fast_path = stats_.ordered_batch_fast_path.load(
            std::memory_order_relaxed);
        result.unordered_batch_sorts = stats_.unordered_batch_sorts.load(
            std::memory_order_relaxed);
        result.barrier_index_orders_visited =
            stats_.barrier_index_orders_visited.load(
                std::memory_order_relaxed);
        result.source_only_fast_path = stats_.source_only_fast_path.load(
            std::memory_order_relaxed);
        return result;
    }
    [[nodiscard]] bool repair_pending() const noexcept {
        return stats_.repair_pending.load(std::memory_order_acquire);
    }
    [[nodiscard]] const EventWorkerConfig& config() const noexcept {
        return config_;
    }

private:
    void QueueNoopDependencies(
        std::set<RawTickDependency> dependencies) {
        if (dependencies.empty()) {
            return;
        }
        auto empty = std::make_shared<EventRevisionBatch>();
        empty->calculation_run_id = config_.calculation_run_id;
        empty->owner = config_.owner;
        pending_commits_.push_back(
            PendingCommit{std::move(empty), std::move(dependencies)});
        stats_.pending_raw_commits.store(
            pending_commits_.size(), std::memory_order_relaxed);
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

    void ClearRepairEvalPatch(RepairTransaction* repair,
                              const OrderKey& order) noexcept {
        auto position = repair->eval_patch.lower_bound(
            RoleCacheKey{order, 0U});
        while (position != repair->eval_patch.end() &&
               position->first.order == order) {
            position = repair->eval_patch.erase(position);
        }
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
        ClearRepairEvalPatch(repair, task->key);
        task->history = {};
        task->previous = {};
        task->cursor.reset();
        task->observed_generation = 0U;
        task->initialized = false;
        task->complete = false;
        repair->next_order = task->key;
        QueueRepairOrder(repair, task->key);
    }

    [[nodiscard]] bool MergeRepair(
        const std::map<OrderKey, DirtyOrder>& dirty_orders,
        const std::map<FactKey, bool>& dirty_bundles,
        std::span<const FactKey> inserted,
        std::set<RawTickDependency> dependencies) {
        if (!active_repair_.has_value()) {
            active_repair_.emplace();
        }
        stats_.repair_pending.store(true, std::memory_order_release);
        RepairTransaction& repair = *active_repair_;
        repair.raw_dependencies.insert(dependencies.begin(),
                                       dependencies.end());
        repair.inserted_facts.insert(inserted.begin(), inserted.end());
        for (const auto& [key, late] : dirty_bundles) {
            auto [position, was_inserted] =
                repair.dirty_bundles.try_emplace(key, late);
            if (!was_inserted) {
                position->second = position->second || late;
            }
            repair.late_recovery = repair.late_recovery || late;
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
            repair.late_recovery = repair.late_recovery || dirty.late;
        }
        if (!repair.next_order.has_value() && !repair.tasks.empty() &&
            repair.ready_orders.empty()) {
            QueueRepairOrder(&repair, repair.tasks.begin()->first);
            repair.next_order = repair.tasks.begin()->first;
        }
        stats_.active_repair_orders.store(
            repair.tasks.size(), std::memory_order_release);
        return true;
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

    [[nodiscard]] bool InitializeRepairOrder(
        RepairOrderTask* task) {
        const auto live = order_histories_.find(task->key);
        if (live == order_histories_.end() ||
            task->dirty.new_sequences.empty()) {
            return false;
        }
        task->history = live->second;
        task->observed_generation = live->second.generation;
        const std::uint64_t first = *task->dirty.new_sequences.begin();
        task->previous = LatestBefore(task->history, first);
        const auto use = task->history.uses.lower_bound(first);
        task->cursor = use == task->history.uses.end()
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
        const auto live = order_histories_.find(task->key);
        if (live == order_histories_.end()) {
            return false;
        }
        if (task->initialized &&
            task->observed_generation != live->second.generation) {
            ResetRepairOrder(repair, task, true);
        }
        if (!task->initialized && !InitializeRepairOrder(task)) {
            return false;
        }
        if (task->complete) {
            return true;
        }

        const auto use = task->history.uses.find(*task->cursor);
        if (use == task->history.uses.end()) {
            return false;
        }
        const std::uint64_t sequence = use->first;
        const FactKey fact_key{task->key.trade_date, task->key.market,
                               task->key.channel, sequence};
        const auto fact_position = facts_.find(fact_key);
        if (fact_position == facts_.end()) {
            return false;
        }
        const FactRecord& fact = fact_position->second;
        const RoleCacheKey cache_key{task->key, sequence};
        const auto old_position = role_cache_.find(cache_key);
        const RoleEval* old_eval = old_position == role_cache_.end()
            ? nullptr
            : &old_position->second;
        const std::optional<OrderState> prior_state = task->previous.state;
        const ApplyRoleResult applied = ApplyOrderRole(
            task->previous.state, fact.input.tick, use->second,
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
        if (cache_changed) {
            repair->eval_patch[cache_key] = applied.eval;
        }
        if (observable_changed) {
            auto [position, was_inserted] =
                repair->dirty_bundles.try_emplace(
                    fact_key,
                    repair->late_recovery || task->dirty.late);
            if (!was_inserted) {
                position->second = position->second ||
                    repair->late_recovery || task->dirty.late;
            }
        }
        task->history.versions[sequence] = StateVersion{
            applied.eval.post_state, applied.eval.input_set_hash};
        task->previous = task->history.versions.at(sequence);
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
            task->previous = LatestBefore(task->history, *next_new);
            const auto next_use =
                task->history.uses.lower_bound(*next_new);
            task->cursor = next_use == task->history.uses.end()
                ? std::optional<std::uint64_t>{}
                : std::optional<std::uint64_t>{next_use->first};
            task->complete = !task->cursor.has_value();
            return true;
        }

        const auto next_use = std::next(use);
        task->cursor = next_use == task->history.uses.end()
            ? std::optional<std::uint64_t>{}
            : std::optional<std::uint64_t>{next_use->first};
        task->complete = !task->cursor.has_value();
        return true;
    }

    [[nodiscard]] bool RepairGenerationsStable(
        RepairTransaction* repair) {
        bool stable = true;
        for (auto& [order, task] : repair->tasks) {
            const auto live = order_histories_.find(order);
            if (live == order_histories_.end()) {
                SetFatal("Event repair OrderUseIndex disappeared");
                return false;
            }
            if (!task.initialized || !task.complete ||
                task.observed_generation != live->second.generation) {
                if (task.initialized && task.complete &&
                    task.observed_generation != live->second.generation) {
                    ResetRepairOrder(repair, &task, true);
                }
                stable = false;
            }
        }
        return stable;
    }

    [[nodiscard]] bool CommitProjection(
        std::vector<OrderPatch>* order_patches,
        std::map<RoleCacheKey, RoleEval>* eval_patch,
        std::map<FactKey, bool>* dirty_bundles,
        std::span<const FactKey> inserted,
        std::set<RawTickDependency> dependencies,
        bool repair_commit,
        EventApplyResult* result) {
        if (pending_commits_.size() >= config_.maximum_pending_commits) {
            static_cast<void>(CapacityFailure(
                result, "Event pending raw commit capacity exhausted"));
            return false;
        }

        std::map<FactKey, Bundle> bundle_patch;
        std::size_t projected_event_count = cached_event_count_;
        for (const auto& [key, late_reason] : *dirty_bundles) {
            static_cast<void>(late_reason);
            Bundle assembled = AssembleBundle(key, *eval_patch);
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

        std::map<EventKey, EventHead> head_patch;
        bool batch_has_late = repair_commit;
        std::uint32_t revision_counter = next_revision_counter_;
        for (const auto& [fact_key, new_bundle] : bundle_patch) {
            const auto old_position = bundle_cache_.find(fact_key);
            const Bundle empty{};
            const Bundle& old_bundle = old_position == bundle_cache_.end()
                ? empty
                : old_position->second;
            const bool late_reason = repair_commit ||
                dirty_bundles->at(fact_key);
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
            ? RevisionReason::kLateRecovery
            : RevisionReason::kLiveProjection;

        for (OrderPatch& patch : *order_patches) {
            order_histories_[patch.key] = std::move(patch.history);
        }
        for (auto& [key, eval] : *eval_patch) {
            role_cache_[key] = std::move(eval);
        }
        for (auto& [key, bundle] : bundle_patch) {
            bundle_cache_[key] = std::move(bundle);
        }
        for (const auto& [key, head] : head_patch) {
            event_heads_[key] = head;
        }
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
        pending_commits_.push_back(PendingCommit{
            std::move(revision_batch), std::move(dependencies)});
        stats_.pending_raw_commits.store(
            pending_commits_.size(), std::memory_order_relaxed);
        stats_.revisions_created += revision_count;
        if (pending_commits_.back().raw_dependencies.empty() &&
            pending_commits_.back().batch->revisions.empty()) {
            pending_commits_.pop_back();
            stats_.pending_raw_commits.store(
                pending_commits_.size(), std::memory_order_relaxed);
        }
        if (repair_commit) {
            ++stats_.repair_commits;
        }
        return true;
    }

    [[nodiscard]] bool CommitRepair(EventApplyResult* result) {
        RepairTransaction& repair = *active_repair_;
        if (repair.late_recovery) {
            for (auto& [key, late] : repair.dirty_bundles) {
                static_cast<void>(key);
                late = true;
            }
        }
        std::vector<OrderPatch> order_patches;
        order_patches.reserve(repair.tasks.size());
        for (auto& [order, task] : repair.tasks) {
            order_patches.push_back(
                OrderPatch{order, std::move(task.history)});
        }
        std::vector<FactKey> inserted(repair.inserted_facts.begin(),
                                      repair.inserted_facts.end());
        if (!CommitProjection(
                &order_patches, &repair.eval_patch,
                &repair.dirty_bundles, inserted,
                std::move(repair.raw_dependencies), true, result)) {
            return false;
        }
        active_repair_.reset();
        stats_.active_repair_orders.store(0U, std::memory_order_release);
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
        if (!DrainDurableCommits()) {
            result->code = EventApplyCode::kSinkFailed;
            return false;
        }
        if (!active_repair_.has_value()) {
            result->repair_pending = false;
            return true;
        }

        ++stats_.repair_slices;
        const auto started = std::chrono::steady_clock::now();
        std::size_t processed_uses = 0U;
        try {
            while (active_repair_.has_value()) {
                RepairTransaction& repair = *active_repair_;
                RepairOrderTask* task = NextRepairOrder(&repair);
                if (task == nullptr) {
                    if (!RepairGenerationsStable(&repair)) {
                        if (!healthy_) {
                            result->code = EventApplyCode::kFailed;
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
                    return DrainDurableCommits();
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
        const auto statuses = phase_statuses_.find(range);
        if (statuses == phase_statuses_.end()) {
            return std::nullopt;
        }
        const auto& values = statuses->second.ordered;
        const auto position = std::upper_bound(
            values.begin(), values.end(), sequence,
            [](std::uint64_t value, const auto& entry) {
                return value < entry.first;
            });
        if (position == values.begin()) {
            return std::nullopt;
        }
        return std::prev(position)->second;
    }

    [[nodiscard]] bool NormalizeOneShanghaiPhase(
        const FactKey& key) {
        FactRecord& record = facts_.at(key);
        CanonicalTick& projected = record.input.tick;
        if (projected.common.identity.market != Market::kShanghai ||
            projected.action == TickAction::kStatus) {
            return false;
        }
        const std::optional<TradingPhase> phase = PhaseAt(
            InstrumentChannel(record.source_tick), key.native_sequence);
        const TradingPhase next = phase.value_or(TradingPhase::kUnknown);
        const bool next_valid = phase.has_value() &&
                                next != TradingPhase::kUnknown;
        const bool old_valid =
            (projected.validity & ingest::kTickPhaseValid) != 0U;
        if (projected.phase == next && old_valid == next_valid) {
            return false;
        }
        projected.phase = next;
        if (next_valid) {
            projected.validity |= ingest::kTickPhaseValid;
        } else {
            projected.validity &= ~ingest::kTickPhaseValid;
        }
        record.fact_hash = HashFact(projected);
        record.source = ProjectSource(projected);
        return true;
    }

    [[nodiscard]] std::set<FactKey> NormalizeShanghaiPhases(
        std::span<const FactKey> inserted) {
        std::set<FactKey> candidates;
        for (const FactKey& key : inserted) {
            const FactRecord& record = facts_.at(key);
            if (key.market != Market::kShanghai) {
                continue;
            }
            const InstrumentChannelKey range =
                InstrumentChannel(record.source_tick);
            const auto journal = instrument_facts_.find(range);
            if (journal == instrument_facts_.end()) {
                continue;
            }
            if (record.source_tick.action != TickAction::kStatus ||
                (record.source_tick.validity &
                 ingest::kTickPhaseValid) == 0U) {
                candidates.insert(key);
                continue;
            }
            const auto statuses = phase_statuses_.find(range);
            std::uint64_t end =
                std::numeric_limits<std::uint64_t>::max();
            if (statuses != phase_statuses_.end()) {
                const auto& values = statuses->second.ordered;
                const auto next = std::upper_bound(
                    values.begin(), values.end(), key.native_sequence,
                    [](std::uint64_t value, const auto& entry) {
                        return value < entry.first;
                    });
                if (next != values.end()) {
                    end = next->first;
                }
            }
            const auto& values = journal->second.ordered;
            const auto position = std::upper_bound(
                values.begin(), values.end(), key.native_sequence,
                [](std::uint64_t value, const FactKey& fact) {
                    return value < fact.native_sequence;
                });
            for (auto cursor = position;
                 cursor != values.end() && cursor->native_sequence < end;
                 ++cursor) {
                candidates.insert(*cursor);
            }
        }
        std::set<FactKey> changed;
        for (const FactKey& key : candidates) {
            if (NormalizeOneShanghaiPhase(key)) {
                changed.insert(key);
            }
        }
        return changed;
    }

    [[nodiscard]] bool EnsureOrder(
        const OrderKey& order,
        OrderHistory** output) {
        auto position = order_histories_.find(order);
        if (position == order_histories_.end()) {
            if (order_histories_.size() >= config_.maximum_orders) {
                return false;
            }
            auto inserted = order_histories_.try_emplace(order);
            position = inserted.first;
            const InstrumentChannelKey range{
                order.trade_date, order.market, order.instrument_id,
                order.channel};
            // Orders never leave the intraday state.  Keep a direct
            // instrument/channel index so a Shanghai END barrier visits only
            // the security it actually closes, instead of every order owned
            // by this actor.
            orders_by_instrument_channel_[range].push_back(order);
            const auto barriers = barriers_.find(range);
            if (barriers != barriers_.end()) {
                for (const auto& [sequence, fact_key] : barriers->second) {
                    if (!SetOrderUse(
                            &position->second, sequence,
                            OrderRole::kBarrier)) {
                        return false;
                    }
                    facts_.at(fact_key).roles.emplace(
                        order, OrderRole::kBarrier);
                }
            }
        }
        *output = &position->second;
        return true;
    }

    [[nodiscard]] bool AddDirectRole(
        FactRecord* record,
        const OrderKey& order,
        OrderRole role,
        std::map<OrderKey, DirtyOrder>* dirty_orders,
        EventApplyResult* result) {
        OrderHistory* history = nullptr;
        if (!EnsureOrder(order, &history)) {
            return false;
        }
        if (!SetOrderUse(
                history, record->input.tick.common.native_sequence, role)) {
            return false;
        }
        record->roles[order] = role;
        DirtyOrder& dirty = (*dirty_orders)[order];
        dirty.new_sequences.insert(
            record->input.tick.common.native_sequence);
        dirty.late = dirty.late || record->late;
        static_cast<void>(result);
        return true;
    }

    [[nodiscard]] bool RegisterFactUses(
        const FactKey& fact_key,
        FactRecord* record,
        std::map<OrderKey, DirtyOrder>* dirty_orders,
        EventApplyResult* result) {
        const CanonicalTick& fact = record->input.tick;
        if (fact.action == TickAction::kAdd ||
            fact.action == TickAction::kCancel) {
            if (!AddDirectRole(
                    record, MakeOrderKey(fact, fact.primary_order_id),
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
                            record,
                            MakeOrderKey(fact, fact.buy_order_id),
                            OrderRole::kBuy, dirty_orders, result)) {
                        return false;
                    }
                }
                if (fact.sell_order_id > 0) {
                    if (!AddDirectRole(
                            record,
                            MakeOrderKey(fact, fact.sell_order_id),
                            OrderRole::kSell, dirty_orders, result)) {
                        return false;
                    }
                }
            }
        }

        if (record->barrier) {
            const InstrumentChannelKey range = InstrumentChannel(fact);
            barriers_[range][fact_key.native_sequence] = fact_key;
            const auto indexed_orders =
                orders_by_instrument_channel_.find(range);
            if (indexed_orders == orders_by_instrument_channel_.end()) {
                return order_histories_.size() <= config_.maximum_orders;
            }
            stats_.barrier_index_orders_visited.fetch_add(
                static_cast<std::uint64_t>(indexed_orders->second.size()),
                std::memory_order_relaxed);
            for (const OrderKey& order : indexed_orders->second) {
                auto history_position = order_histories_.find(order);
                if (history_position == order_histories_.end()) {
                    return false;
                }
                OrderHistory& history = history_position->second;
                if (!SetOrderUse(
                        &history, fact_key.native_sequence,
                        OrderRole::kBarrier)) {
                    return false;
                }
                record->roles[order] = OrderRole::kBarrier;
                DirtyOrder& dirty = (*dirty_orders)[order];
                dirty.new_sequences.insert(fact_key.native_sequence);
                dirty.late = dirty.late || record->late;
            }
        }
        return order_histories_.size() <= config_.maximum_orders;
    }

    [[nodiscard]] const RoleEval* FindEval(
        const RoleCacheKey& key,
        const std::map<RoleCacheKey, RoleEval>& patch) const noexcept {
        const auto patched = patch.find(key);
        if (patched != patch.end()) {
            return &patched->second;
        }
        const auto existing = role_cache_.find(key);
        return existing == role_cache_.end() ? nullptr : &existing->second;
    }

    [[nodiscard]] bool RecomputeOrder(
        const OrderKey& order,
        const DirtyOrder& dirty,
        OrderPatch* output,
        std::map<RoleCacheKey, RoleEval>* eval_patch,
        std::map<FactKey, bool>* dirty_bundles,
        EventApplyResult* result) {
        const auto live = order_histories_.find(order);
        if (live == order_histories_.end() ||
            dirty.new_sequences.empty()) {
            return false;
        }
        output->key = order;
        output->history = live->second;
        const std::uint64_t first = *dirty.new_sequences.begin();
        StateVersion previous = LatestBefore(output->history, first);
        auto use = output->history.uses.lower_bound(first);
        while (use != output->history.uses.end()) {
            const std::uint64_t sequence = use->first;
            const FactKey fact_key{order.trade_date, order.market,
                                   order.channel, sequence};
            const auto fact_position = facts_.find(fact_key);
            if (fact_position == facts_.end()) {
                return false;
            }
            const FactRecord& fact = fact_position->second;
            const RoleCacheKey cache_key{order, sequence};
            const auto old_position = role_cache_.find(cache_key);
            const RoleEval* old_eval = old_position == role_cache_.end()
                ? nullptr
                : &old_position->second;
            const ApplyRoleResult applied = ApplyOrderRole(
                previous.state, fact.input.tick, use->second,
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
            output->history.versions[sequence] = StateVersion{
                applied.eval.post_state, applied.eval.input_set_hash};

            if (dirty.late) {
                ++stats_.repaired_order_uses;
                ++result->repaired_order_uses;
            } else {
                ++stats_.live_order_uses;
            }

            previous = output->history.versions.at(sequence);
            if (state_converged || state_unchanged_new_use) {
                const auto next_new = dirty.new_sequences.upper_bound(sequence);
                if (next_new == dirty.new_sequences.end()) {
                    if (dirty.late) {
                        ++stats_.repair_convergence_stops;
                    }
                    break;
                }
                previous = LatestBefore(output->history, *next_new);
                use = output->history.uses.lower_bound(*next_new);
                continue;
            }
            ++use;
        }
        return true;
    }

    [[nodiscard]] Bundle AssembleBundle(
        const FactKey& fact_key,
        const std::map<RoleCacheKey, RoleEval>& eval_patch) const {
        Bundle bundle{};
        const FactRecord& fact = facts_.at(fact_key);
        if (!fact.projectable) {
            return bundle;
        }
        SourceFragment source = fact.source;
        Identifier128 source_input_hash = fact.fact_hash;
        for (const auto& [order, role] : fact.roles) {
            const RoleEval* eval = FindEval(
                RoleCacheKey{order, fact_key.native_sequence}, eval_patch);
            if (eval == nullptr) {
                continue;
            }
            source_input_hash = CombineHashes(
                source_input_hash, eval->input_set_hash,
                static_cast<std::uint8_t>(role) + 16U);
            if (source.kind.has_value()) {
                source.payload.event_quality_flags |=
                    eval->source_quality_contribution;
                if (fact.input.tick.action == TickAction::kCancel) {
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
            if (eval->order_fragment.has_value()) {
                EventKey key{fact_key.trade_date,
                             fact_key.market,
                             fact.input.tick.common.instrument_id,
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
                bundle.rows.emplace(key, *eval->order_fragment);
                bundle.input_set_hashes.emplace(
                    key, eval->input_set_hash);
            }
        }
        if (source.kind.has_value()) {
            EventKey key{fact_key.trade_date,
                         fact_key.market,
                         fact.input.tick.common.instrument_id,
                         fact_key.channel,
                         fact_key.native_sequence,
                         *source.kind,
                         0,
                         0U};
            bundle.rows.emplace(key, source.payload);
            bundle.input_set_hashes.emplace(key, source_input_hash);
        }
        return bundle;
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
        const auto live = event_heads_.find(key);
        return live == event_heads_.end() ? nullptr : &live->second;
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
            ? RevisionReason::kLateRecovery
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
    std::unordered_map<ChannelKey, EventChannelState, ChannelKeyHash>
        channel_states_;
    std::unordered_map<InstrumentChannelKey,
                       std::map<std::uint64_t, FactKey>,
                       InstrumentChannelKeyHash>
        barriers_;
    std::unordered_map<InstrumentChannelKey,
                       InstrumentFactIndex,
                       InstrumentChannelKeyHash>
        instrument_facts_;
    std::unordered_map<InstrumentChannelKey,
                       PhaseIndex,
                       InstrumentChannelKeyHash>
        phase_statuses_;
    std::unordered_map<InstrumentChannelKey, std::vector<OrderKey>,
                       InstrumentChannelKeyHash>
        orders_by_instrument_channel_;
    std::unordered_map<OrderKey, OrderHistory, OrderKeyHash>
        order_histories_;
    std::unordered_map<RoleCacheKey, RoleEval, RoleCacheKeyHash>
        role_cache_;
    std::unordered_map<FactKey, Bundle, FactKeyHash> bundle_cache_;
    std::unordered_map<EventKey, EventHead, EventKeyHash> event_heads_;
    std::unordered_set<RawTickDependency, RawTickDependencyHash>
        acknowledged_raw_;
    std::deque<PendingCommit> pending_commits_;
    std::optional<RepairTransaction> active_repair_;
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
        config.logic_version == 0U || IsZero(config.calculation_run_id) ||
        config.maximum_facts == 0U || config.maximum_orders == 0U ||
        config.maximum_cached_events == 0U ||
        config.maximum_pending_commits == 0U ||
        config.maximum_acknowledged_raw_dependencies == 0U ||
        config.repair_slice_max_order_uses == 0U ||
        config.repair_slice_max_cpu_ns == 0U) {
        return fail("invalid Event worker configuration");
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

bool EventWorker::AdvanceRepair() noexcept {
    return impl_->AdvanceRepair();
}

void EventWorker::AcknowledgeRawTicks(
    std::span<const RawTickDependency> dependencies) noexcept {
    impl_->AcknowledgeRawTicks(dependencies);
}

bool EventWorker::DrainDurableCommits() noexcept {
    return impl_->DrainDurableCommits();
}

bool EventWorker::repair_pending() const noexcept {
    return impl_->repair_pending();
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
