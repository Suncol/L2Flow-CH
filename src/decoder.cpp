#include "decoder_internal.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace l2flow::ingest::internal {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

static_assert(sizeof(sh::SHL2MarketData) == 248U);
static_assert(offsetof(sh::SHL2MarketData, SecurityID) == 4U);
static_assert(offsetof(sh::SHL2MarketData, BidLevels) == 228U);
static_assert(offsetof(sh::SHL2MarketData, SellLevels) == 236U);
static_assert(sizeof(sh::SHL2MarketData::BidLevelsItem) == 28U);
static_assert(sizeof(sh::NGTSTick) == 70U);
static_assert(offsetof(sh::NGTSTick, BizIndex) == 0U);
static_assert(offsetof(sh::NGTSTick, Channel) == 8U);
static_assert(offsetof(sh::NGTSTick, SecurityID) == 12U);
static_assert(offsetof(sh::NGTSTick, TickTime) == 18U);
static_assert(sizeof(sz::Snapshot300111_v2) == 224U);
static_assert(offsetof(sz::Snapshot300111_v2, SecurityID) == 14U);
static_assert(offsetof(sz::Snapshot300111_v2, SecurityIDSource) == 20U);
static_assert(sizeof(sz::Snapshot300111_v2::BidPriceLevelItem) == 28U);
static_assert(sizeof(sz::Order300192_v2) == 58U);
static_assert(offsetof(sz::Order300192_v2, ChannelNo) == 0U);
static_assert(offsetof(sz::Order300192_v2, ApplSeqNum) == 4U);
static_assert(offsetof(sz::Order300192_v2, TransactTime) == 50U);
static_assert(sizeof(sz::Transaction300191_v2) == 70U);
static_assert(offsetof(sz::Transaction300191_v2, ChannelNo) == 0U);
static_assert(offsetof(sz::Transaction300191_v2, ApplSeqNum) == 4U);
static_assert(offsetof(sz::Transaction300191_v2, TransactTime) == 66U);

inline constexpr std::int32_t kNullI32 =
    std::numeric_limits<std::int32_t>::min();
inline constexpr std::int64_t kNullI64 =
    std::numeric_limits<std::int64_t>::min();
inline constexpr std::uint32_t kNullTime = 1'000'000'000U;

struct ProtectedRange final {
    std::size_t begin = 0U;
    std::size_t end = 0U;
};

thread_local std::vector<ProtectedRange> g_protected_ranges;
thread_local bool g_protect_dynamic_ranges = false;

class DynamicRangeScope final {
public:
    explicit DynamicRangeScope(std::size_t maximum_ranges) noexcept {
        g_protected_ranges.clear();
        try {
            if (g_protected_ranges.capacity() < maximum_ranges) {
                g_protected_ranges.reserve(maximum_ranges);
            }
            g_protect_dynamic_ranges = true;
            valid_ = true;
        } catch (const std::bad_alloc&) {
            g_protect_dynamic_ranges = false;
        }
    }

    ~DynamicRangeScope() { g_protect_dynamic_ranges = false; }
    [[nodiscard]] bool valid() const noexcept { return valid_; }

private:
    bool valid_ = false;
};

[[nodiscard]] DecodeError ProtectDynamicRange(
    std::size_t begin,
    std::size_t size) noexcept {
    if (!g_protect_dynamic_ranges || size == 0U) {
        return DecodeError::kNone;
    }
    if (begin > std::numeric_limits<std::size_t>::max() - size) {
        return DecodeError::kInvalidOffset;
    }
    const std::size_t end = begin + size;
    for (const ProtectedRange& existing : g_protected_ranges) {
        if (begin < existing.end && existing.begin < end) {
            return DecodeError::kRangeOverlap;
        }
    }
    try {
        g_protected_ranges.push_back({begin, end});
    } catch (const std::bad_alloc&) {
        return DecodeError::kResourceExhausted;
    }
    return DecodeError::kNone;
}

template <typename Unsigned>
[[nodiscard]] bool LoadUnsigned(
    std::span<const std::byte> body,
    std::size_t offset,
    Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr || offset > body.size() ||
        sizeof(Unsigned) > body.size() - offset) {
        return false;
    }
    Unsigned value = 0U;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto octet = static_cast<Unsigned>(
            std::to_integer<unsigned char>(body[offset + index]));
        const auto shift = static_cast<unsigned int>(index * 8U);
        value |= static_cast<Unsigned>(octet << shift);
    }
    *output = value;
    return true;
}

[[nodiscard]] bool LoadI32(std::span<const std::byte> body,
                           std::size_t offset,
                           std::int32_t* output) noexcept {
    std::uint32_t wire = 0U;
    if (output == nullptr || !LoadUnsigned(body, offset, &wire)) {
        return false;
    }
    *output = std::bit_cast<std::int32_t>(wire);
    return true;
}

[[nodiscard]] bool LoadI64(std::span<const std::byte> body,
                           std::size_t offset,
                           std::int64_t* output) noexcept {
    std::uint64_t wire = 0U;
    if (output == nullptr || !LoadUnsigned(body, offset, &wire)) {
        return false;
    }
    *output = std::bit_cast<std::int64_t>(wire);
    return true;
}

struct TextView final {
    std::span<const std::byte> bytes{};
};

[[nodiscard]] bool IsPrintableAscii(
    std::span<const std::byte> value) noexcept {
    return std::all_of(value.begin(), value.end(), [](std::byte item) {
        const auto octet = std::to_integer<unsigned char>(item);
        return octet >= 0x20U && octet <= 0x7eU;
    });
}

[[nodiscard]] DecodeError ReadText(
    std::span<const std::byte> body,
    std::size_t descriptor_offset,
    std::size_t fixed_bytes,
    std::size_t maximum_bytes,
    bool require_nonempty,
    TextView* output) noexcept {
    if (output == nullptr || body.size() < fixed_bytes) {
        return DecodeError::kTruncated;
    }
    std::uint16_t length = 0U;
    std::uint32_t relative_offset = 0U;
    if (!LoadUnsigned(body, descriptor_offset, &length) ||
        !LoadUnsigned(body, descriptor_offset + 2U, &relative_offset)) {
        return DecodeError::kTruncated;
    }
    const std::size_t size = static_cast<std::size_t>(length);
    if (size > maximum_bytes) {
        return DecodeError::kCountExceeded;
    }
    if (size == 0U) {
        if (relative_offset != 0U &&
            (static_cast<std::size_t>(relative_offset) >
                 std::numeric_limits<std::size_t>::max() -
                     descriptor_offset ||
             descriptor_offset +
                     static_cast<std::size_t>(relative_offset) >
                 body.size())) {
            return DecodeError::kInvalidOffset;
        }
        output->bytes = {};
        return require_nonempty ? DecodeError::kInvalidText
                                : DecodeError::kNone;
    }
    if (relative_offset < 6U ||
        static_cast<std::size_t>(relative_offset) >
            std::numeric_limits<std::size_t>::max() - descriptor_offset) {
        return DecodeError::kInvalidOffset;
    }
    const std::size_t start =
        descriptor_offset + static_cast<std::size_t>(relative_offset);
    if (start < fixed_bytes || start > body.size() ||
        size > body.size() - start) {
        return DecodeError::kInvalidOffset;
    }
    const auto value = body.subspan(start, size);
    if (!IsPrintableAscii(value)) {
        return DecodeError::kInvalidText;
    }
    const DecodeError protection = ProtectDynamicRange(start, size);
    if (protection != DecodeError::kNone) {
        return protection;
    }
    output->bytes = value;
    return DecodeError::kNone;
}

struct ListView final {
    std::size_t start = 0U;
    std::size_t count = 0U;
    std::size_t item_size = 0U;
};

[[nodiscard]] DecodeError ReadList(
    std::span<const std::byte> body,
    std::size_t descriptor_offset,
    std::size_t item_size,
    std::size_t fixed_bytes,
    std::size_t maximum_items,
    ListView* output) noexcept {
    if (output == nullptr || body.size() < fixed_bytes) {
        return DecodeError::kTruncated;
    }
    std::uint32_t count = 0U;
    std::uint32_t relative_offset = 0U;
    if (!LoadUnsigned(body, descriptor_offset, &count) ||
        !LoadUnsigned(body, descriptor_offset + 4U, &relative_offset)) {
        return DecodeError::kTruncated;
    }
    const std::size_t count_size = static_cast<std::size_t>(count);
    if (count_size > maximum_items) {
        return DecodeError::kCountExceeded;
    }
    if (count_size == 0U) {
        if (relative_offset != 0U &&
            (static_cast<std::size_t>(relative_offset) >
                 std::numeric_limits<std::size_t>::max() -
                     descriptor_offset ||
             descriptor_offset +
                     static_cast<std::size_t>(relative_offset) >
                 body.size())) {
            return DecodeError::kInvalidOffset;
        }
        *output = ListView{0U, 0U, item_size};
        return DecodeError::kNone;
    }
    if (relative_offset < 8U || item_size == 0U ||
        count_size > std::numeric_limits<std::size_t>::max() / item_size ||
        static_cast<std::size_t>(relative_offset) >
            std::numeric_limits<std::size_t>::max() - descriptor_offset) {
        return DecodeError::kInvalidOffset;
    }
    const std::size_t start =
        descriptor_offset + static_cast<std::size_t>(relative_offset);
    const std::size_t bytes = count_size * item_size;
    if (start < fixed_bytes || start > body.size() ||
        bytes > body.size() - start) {
        return DecodeError::kInvalidOffset;
    }
    const DecodeError protection = ProtectDynamicRange(start, bytes);
    if (protection != DecodeError::kNone) {
        return protection;
    }
    *output = ListView{start, count_size, item_size};
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError NormalizeP6(FixedDecimal* value) noexcept {
    if (value == nullptr || value->source_scale > 6U) {
        return DecodeError::kFixedPointOverflow;
    }
    if (!value->raw_valid) {
        return DecodeError::kNone;
    }
    constexpr std::array<std::int64_t, 7U> powers{
        1LL, 10LL, 100LL, 1'000LL, 10'000LL, 100'000LL, 1'000'000LL};
    const std::int64_t multiplier = powers[6U - value->source_scale];
    if ((value->raw > 0 &&
         value->raw > std::numeric_limits<std::int64_t>::max() /
                          multiplier) ||
        (value->raw < 0 &&
         value->raw < std::numeric_limits<std::int64_t>::min() /
                          multiplier)) {
        return DecodeError::kFixedPointOverflow;
    }
    value->p6 = value->raw * multiplier;
    value->p6_valid = true;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError LoadDecimal32Raw(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality,
    FixedDecimal* output) noexcept {
    std::int32_t raw = 0;
    if (quality == nullptr || output == nullptr ||
        !LoadI32(body, offset, &raw)) {
        return DecodeError::kTruncated;
    }
    output->raw = raw;
    output->source_scale = scale;
    if (raw == kNullI32) {
        *quality |= kQualityNullSourceValue;
        return DecodeError::kNone;
    }
    output->raw_valid = true;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError LoadDecimal64Raw(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality,
    FixedDecimal* output) noexcept {
    std::int64_t raw = 0;
    if (quality == nullptr || output == nullptr ||
        !LoadI64(body, offset, &raw)) {
        return DecodeError::kTruncated;
    }
    output->raw = raw;
    output->source_scale = scale;
    if (raw == kNullI64) {
        *quality |= kQualityNullSourceValue;
        return DecodeError::kNone;
    }
    output->raw_valid = true;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError DecodePrice32(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality,
    FixedDecimal* output) noexcept {
    DecodeError error =
        LoadDecimal32Raw(body, offset, scale, quality, output);
    if (error != DecodeError::kNone || !output->raw_valid) {
        return error;
    }
    if (output->raw <= 0) {
        *quality |= kQualityInvalidPrice;
        return DecodeError::kNone;
    }
    return NormalizeP6(output);
}

[[nodiscard]] DecodeError DecodePrice64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality,
    FixedDecimal* output) noexcept {
    DecodeError error =
        LoadDecimal64Raw(body, offset, scale, quality, output);
    if (error != DecodeError::kNone || !output->raw_valid) {
        return error;
    }
    if (output->raw <= 0) {
        *quality |= kQualityInvalidPrice;
        return DecodeError::kNone;
    }
    return NormalizeP6(output);
}

[[nodiscard]] DecodeError DecodeDecimal64(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    std::uint64_t* quality,
    FixedDecimal* output) noexcept {
    const DecodeError error =
        LoadDecimal64Raw(body, offset, scale, quality, output);
    if (error != DecodeError::kNone) {
        return error;
    }
    return NormalizeP6(output);
}

[[nodiscard]] DecodeError DecodeQuantity(
    std::span<const std::byte> body,
    std::size_t offset,
    std::uint8_t scale,
    bool nullable,
    std::uint64_t* quality,
    ScaledInteger* output) noexcept {
    std::int64_t raw = 0;
    if (quality == nullptr || output == nullptr ||
        !LoadI64(body, offset, &raw)) {
        return DecodeError::kTruncated;
    }
    output->raw = raw;
    output->scale = scale;
    if (nullable && raw == kNullI64) {
        *quality |= kQualityNullSourceValue;
        return DecodeError::kNone;
    }
    if (raw < 0) {
        *quality |= kQualityInvalidQuantity;
        return DecodeError::kNone;
    }
    output->valid = true;
    return DecodeError::kNone;
}

[[nodiscard]] bool DecodeTime(std::uint32_t raw,
                              std::uint64_t* quality,
                              std::uint64_t* ns) noexcept {
    if (quality == nullptr || ns == nullptr) {
        return false;
    }
    if (raw == kNullTime) {
        *quality |= kQualityNullSourceValue | kQualityInvalidTime;
        return false;
    }
    const std::uint32_t hour = raw / 10'000'000U;
    const std::uint32_t minute = (raw / 100'000U) % 100U;
    const std::uint32_t second = (raw / 1'000U) % 100U;
    const std::uint32_t millisecond = raw % 1'000U;
    if (hour >= 24U || minute >= 60U || second >= 60U) {
        *quality |= kQualityInvalidTime;
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(hour) * 3'600U +
        static_cast<std::uint64_t>(minute) * 60U + second;
    *ns = seconds * UINT64_C(1'000'000'000) +
          static_cast<std::uint64_t>(millisecond) * 1'000'000U;
    return true;
}

void CopyText(std::span<const std::byte> source,
              std::array<std::byte, kMaximumIdentityBytes>* destination,
              std::uint8_t* destination_size) noexcept {
    std::copy(source.begin(), source.end(), destination->begin());
    *destination_size = static_cast<std::uint8_t>(source.size());
}

void InitializeCommon(const OwnedMessageView& message,
                      std::uint32_t trade_date,
                      Market market,
                      CanonicalKind kind,
                      CanonicalCommon* common) noexcept {
    common->ingress_sequence = message.ingress_sequence;
    common->vendor_sequence_id = message.header.vendor_sequence_id;
    common->receive_monotonic_ns = message.receive_monotonic_ns;
    common->trade_date = trade_date;
    common->message_key = message.header.key;
    common->kind = kind;
    common->identity.market = market;
    common->vendor_local_time_raw = message.header.local_time_raw;
    common->vendor_local_time_valid = DecodeTime(
        message.header.local_time_raw, &common->quality_flags,
        &common->vendor_local_time_ns_from_midnight);
}

[[nodiscard]] DecodeError ReadIdentity(
    std::span<const std::byte> body,
    Market market,
    std::size_t fixed_bytes,
    std::size_t stream_offset,
    std::size_t security_offset,
    std::size_t source_offset,
    const DecoderLimits& limits,
    CanonicalCommon* common) noexcept {
    TextView stream;
    if (stream_offset != std::numeric_limits<std::size_t>::max()) {
        const DecodeError stream_error = ReadText(
            body, stream_offset, fixed_bytes, limits.maximum_text_bytes,
            true, &stream);
        if (stream_error != DecodeError::kNone) {
            return stream_error;
        }
        CopyText(stream.bytes, &common->md_stream_id,
                 &common->md_stream_id_size);
    }
    TextView security;
    const DecodeError security_error = ReadText(
        body, security_offset, fixed_bytes, limits.maximum_text_bytes,
        true, &security);
    if (security_error != DecodeError::kNone) {
        return security_error;
    }
    TextView source;
    if (source_offset != std::numeric_limits<std::size_t>::max()) {
        const DecodeError source_error = ReadText(
            body, source_offset, fixed_bytes, limits.maximum_text_bytes,
            true, &source);
        if (source_error != DecodeError::kNone) {
            return source_error;
        }
    }
    common->identity.market = market;
    CopyText(security.bytes, &common->identity.security_id,
             &common->identity.security_id_size);
    CopyText(source.bytes, &common->identity.security_id_source,
             &common->identity.security_id_source_size);
    return DecodeError::kNone;
}

[[nodiscard]] bool ApplyCatalog(const InstrumentCatalog& catalog,
                                CanonicalCommon* common) noexcept {
    const auto source = std::span<const std::byte>(
        common->identity.security_id_source.data(),
        common->identity.security_id_source_size);
    const auto security = std::span<const std::byte>(
        common->identity.security_id.data(),
        common->identity.security_id_size);
    InstrumentMatch match{};
    if (!catalog.Find(common->identity.market, source, security, &match)) {
        common->quality_flags |= kQualityInstrumentNotInCatalog;
        return false;
    }
    common->instrument_id = match.instrument_id;
    common->instrument_ordinal = match.ordinal;
    return true;
}

[[nodiscard]] std::string_view AsString(TextView text) noexcept {
    return {reinterpret_cast<const char*>(text.bytes.data()),
            text.bytes.size()};
}

[[nodiscard]] TradingPhase ParseShPhase(std::string_view value) noexcept {
    if (value == "START") {
        return TradingPhase::kStart;
    }
    if (value == "OCALL") {
        return TradingPhase::kOpeningCall;
    }
    if (value == "TRADE") {
        return TradingPhase::kContinuous;
    }
    if (value == "SUSP") {
        return TradingPhase::kSuspended;
    }
    if (value == "CCALL") {
        return TradingPhase::kClosingCall;
    }
    if (value == "CLOSE") {
        return TradingPhase::kClosed;
    }
    if (value == "ENDTR") {
        return TradingPhase::kEnded;
    }
    return TradingPhase::kUnknown;
}

[[nodiscard]] Side DecodeSzSide(std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return Side::kBuy;
        case 50:
            return Side::kSell;
        case 71:
            return Side::kBorrow;
        case 70:
            return Side::kLend;
        default:
            return Side::kUnknown;
    }
}

[[nodiscard]] OrderType DecodeSzOrderType(std::int32_t raw) noexcept {
    switch (raw) {
        case 49:
            return OrderType::kMarket;
        case 50:
            return OrderType::kLimit;
        case 85:
            return OrderType::kSameSideBest;
        default:
            return OrderType::kUnknown;
    }
}

[[nodiscard]] DecodeError DecodeShanghaiTick(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    TickDecodeResult* output) noexcept {
    if (message.body.size() < sizeof(sh::NGTSTick)) {
        return DecodeError::kTruncated;
    }
    CanonicalTick tick{};
    InitializeCommon(message, trade_date, Market::kShanghai,
                     CanonicalKind::kShanghaiTick, &tick.common);
    DecodeError error = ReadIdentity(
        message.body, Market::kShanghai, sizeof(sh::NGTSTick),
        std::numeric_limits<std::size_t>::max(), 12U,
        std::numeric_limits<std::size_t>::max(), limits, &tick.common);
    if (error != DecodeError::kNone) {
        return error;
    }

    std::int64_t sequence = 0;
    std::int32_t channel = 0;
    if (!LoadI64(message.body, 0U, &sequence) || sequence <= 0 ||
        !LoadI32(message.body, 8U, &channel) || channel <= 0 ||
        !LoadI64(message.body, 28U, &tick.buy_order_id) ||
        !LoadI64(message.body, 36U, &tick.sell_order_id) ||
        !LoadI64(message.body, 48U, &tick.quantity.raw)) {
        return sequence <= 0 || channel <= 0
                   ? DecodeError::kInvalidNativeSequence
                   : DecodeError::kTruncated;
    }
    tick.common.native_sequence = static_cast<std::uint64_t>(sequence);
    tick.common.channel = static_cast<std::uint32_t>(channel);
    if (tick.buy_order_id < 0 || tick.sell_order_id < 0) {
        tick.common.quality_flags |= kQualityInvalidOrderReference;
    }
    tick.quantity.scale = 0U;
    if (tick.quantity.raw >= 0) {
        tick.quantity.valid = true;
    } else {
        tick.common.quality_flags |= kQualityInvalidQuantity;
    }
    std::uint32_t exchange_time = 0U;
    if (!LoadUnsigned(message.body, 18U, &exchange_time)) {
        return DecodeError::kTruncated;
    }
    tick.common.exchange_time_raw = exchange_time;
    if (DecodeTime(exchange_time, &tick.common.quality_flags,
                   &tick.common.exchange_time_ns_from_midnight)) {
        tick.common.exchange_time_valid = true;
        tick.validity |= kTickExchangeTimeValid;
    }
    error = LoadDecimal32Raw(message.body, 44U, 3U,
                             &tick.common.quality_flags, &tick.price);
    if (error != DecodeError::kNone) {
        return error;
    }
    error = LoadDecimal64Raw(message.body, 56U, 3U,
                             &tick.common.quality_flags, &tick.amount);
    if (error != DecodeError::kNone) {
        return error;
    }
    TextView type;
    TextView flag;
    error = ReadText(message.body, 22U, sizeof(sh::NGTSTick),
                     limits.maximum_text_bytes, true, &type);
    if (error != DecodeError::kNone) {
        return error;
    }
    error = ReadText(message.body, 64U, sizeof(sh::NGTSTick),
                     limits.maximum_text_bytes, true, &flag);
    if (error != DecodeError::kNone) {
        return error;
    }
    const std::string_view type_text = AsString(type);
    const std::string_view flag_text = AsString(flag);
    tick.raw_type = type_text.size() == 1U
                        ? static_cast<std::int32_t>(
                              static_cast<unsigned char>(type_text[0]))
                        : 0;
    tick.raw_side = flag_text.size() == 1U
                        ? static_cast<std::int32_t>(
                              static_cast<unsigned char>(flag_text[0]))
                        : 0;

    const auto set_order_side = [&tick, flag_text]() {
        if (flag_text == "B") {
            tick.side = Side::kBuy;
            tick.primary_order_id = tick.buy_order_id;
        } else if (flag_text == "S") {
            tick.side = Side::kSell;
            tick.primary_order_id = tick.sell_order_id;
        } else {
            tick.common.quality_flags |= kQualityUnknownEnum;
            return;
        }
        tick.validity |= kTickSideValid;
        if (tick.primary_order_id > 0) {
            tick.validity |= kTickPrimaryOrderIdValid;
        }
    };

    if (type_text == "A") {
        tick.action = TickAction::kAdd;
        if (tick.price.raw_valid && tick.price.raw > 0) {
            error = NormalizeP6(&tick.price);
        } else if (tick.price.raw_valid) {
            tick.common.quality_flags |= kQualityInvalidPrice;
        }
        if (error != DecodeError::kNone) {
            return error;
        }
        if (tick.price.p6_valid) {
            tick.validity |= kTickPriceValid;
        }
        if (tick.quantity.valid) {
            tick.validity |= kTickQuantityValid;
        }
        set_order_side();
        if (tick.amount.raw_valid && tick.amount.raw >= 0 &&
            tick.amount.raw % 1'000 == 0) {
            tick.sh_add_matched_quantity_raw = tick.amount.raw / 1'000;
            tick.validity |= kTickMatchedQuantityValid;
        } else if (tick.amount.raw_valid && tick.amount.raw < 0) {
            tick.common.quality_flags |= kQualityInvalidQuantity;
        } else if (tick.amount.raw_valid) {
            tick.common.quality_flags |=
                kQualityNonIntegralMatchedQuantity;
        }
        // For SH add records this wire field is matched quantity, not amount.
        tick.amount.p6_valid = false;
    } else if (type_text == "D") {
        tick.action = TickAction::kCancel;
        if (tick.quantity.valid) {
            tick.validity |= kTickQuantityValid;
        }
        set_order_side();
    } else if (type_text == "T") {
        tick.action = TickAction::kTrade;
        if (tick.price.raw_valid && tick.price.raw > 0) {
            error = NormalizeP6(&tick.price);
        } else if (tick.price.raw_valid) {
            tick.common.quality_flags |= kQualityInvalidPrice;
        }
        if (error != DecodeError::kNone) {
            return error;
        }
        if (tick.amount.raw_valid && tick.amount.raw >= 0) {
            error = NormalizeP6(&tick.amount);
        } else if (tick.amount.raw_valid) {
            tick.common.quality_flags |= kQualityInvalidAmount;
        }
        if (error != DecodeError::kNone) {
            return error;
        }
        if (tick.price.p6_valid) {
            tick.validity |= kTickPriceValid;
        }
        if (tick.quantity.valid) {
            tick.validity |= kTickQuantityValid;
        }
        if (tick.amount.p6_valid) {
            tick.validity |= kTickAmountValid;
        }
        if (tick.buy_order_id > 0) {
            tick.validity |= kTickBuyOrderIdValid;
        }
        if (tick.sell_order_id > 0) {
            tick.validity |= kTickSellOrderIdValid;
        }
        if (flag_text == "B") {
            tick.aggressor = Aggressor::kBuy;
        } else if (flag_text == "S") {
            tick.aggressor = Aggressor::kSell;
        } else if (flag_text == "N") {
            tick.aggressor = Aggressor::kNeutral;
        } else {
            tick.common.quality_flags |= kQualityUnknownEnum;
        }
        if (tick.aggressor != Aggressor::kUnknown) {
            tick.validity |= kTickAggressorValid;
        }
    } else if (type_text == "S") {
        tick.action = TickAction::kStatus;
        tick.quantity.valid = false;
        tick.phase = ParseShPhase(flag_text);
        if (tick.phase == TradingPhase::kUnknown) {
            tick.common.quality_flags |= kQualityUnknownEnum;
        } else {
            tick.validity |= kTickPhaseValid;
        }
    } else {
        tick.quantity.valid = false;
        tick.common.quality_flags |= kQualityUnknownEnum;
    }
    output->catalog_match = ApplyCatalog(catalog, &tick.common);
    output->tick = tick;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError DecodeShenzhenOrder(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    TickDecodeResult* output) noexcept {
    if (message.body.size() < sizeof(sz::Order300192_v2)) {
        return DecodeError::kTruncated;
    }
    CanonicalTick tick{};
    InitializeCommon(message, trade_date, Market::kShenzhen,
                     CanonicalKind::kShenzhenOrder, &tick.common);
    DecodeError error = ReadIdentity(
        message.body, Market::kShenzhen, sizeof(sz::Order300192_v2),
        12U, 18U, 24U, limits, &tick.common);
    if (error != DecodeError::kNone) {
        return error;
    }
    std::int64_t sequence = 0;
    if (!LoadUnsigned(message.body, 0U, &tick.common.channel) ||
        !LoadI64(message.body, 4U, &sequence) || sequence <= 0 ||
        !LoadI64(message.body, 38U, &tick.quantity.raw) ||
        !LoadI32(message.body, 46U, &tick.raw_side) ||
        !LoadI32(message.body, 54U, &tick.raw_type)) {
        return sequence <= 0 ? DecodeError::kInvalidNativeSequence
                             : DecodeError::kTruncated;
    }
    tick.common.native_sequence = static_cast<std::uint64_t>(sequence);
    tick.primary_order_id = sequence;
    tick.action = TickAction::kAdd;
    tick.validity |= kTickPrimaryOrderIdValid;
    tick.quantity.scale = 0U;
    if (tick.quantity.raw >= 0) {
        tick.quantity.valid = true;
        tick.validity |= kTickQuantityValid;
    } else {
        tick.common.quality_flags |= kQualityInvalidQuantity;
    }
    std::uint32_t exchange_time = 0U;
    if (!LoadUnsigned(message.body, 50U, &exchange_time)) {
        return DecodeError::kTruncated;
    }
    tick.common.exchange_time_raw = exchange_time;
    if (DecodeTime(exchange_time, &tick.common.quality_flags,
                   &tick.common.exchange_time_ns_from_midnight)) {
        tick.common.exchange_time_valid = true;
        tick.validity |= kTickExchangeTimeValid;
    }
    error = LoadDecimal64Raw(message.body, 30U, 4U,
                             &tick.common.quality_flags, &tick.price);
    if (error != DecodeError::kNone) {
        return error;
    }
    tick.side = DecodeSzSide(tick.raw_side);
    if (tick.side == Side::kUnknown) {
        tick.common.quality_flags |= kQualityUnknownEnum;
    } else {
        tick.validity |= kTickSideValid;
    }
    tick.order_type = DecodeSzOrderType(tick.raw_type);
    if (tick.order_type == OrderType::kUnknown) {
        tick.common.quality_flags |= kQualityUnknownEnum;
    } else {
        tick.validity |= kTickOrderTypeValid;
    }
    if (tick.order_type == OrderType::kLimit) {
        if (tick.price.raw_valid && tick.price.raw > 0) {
            error = NormalizeP6(&tick.price);
        } else if (tick.price.raw_valid) {
            tick.common.quality_flags |= kQualityInvalidPrice;
        }
        if (error != DecodeError::kNone) {
            return error;
        }
        if (tick.price.p6_valid) {
            tick.validity |= kTickPriceValid;
        }
    }
    output->catalog_match = ApplyCatalog(catalog, &tick.common);
    output->tick = tick;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError DecodeShenzhenTransaction(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    TickDecodeResult* output) noexcept {
    if (message.body.size() < sizeof(sz::Transaction300191_v2)) {
        return DecodeError::kTruncated;
    }
    CanonicalTick tick{};
    InitializeCommon(message, trade_date, Market::kShenzhen,
                     CanonicalKind::kShenzhenTransaction, &tick.common);
    DecodeError error = ReadIdentity(
        message.body, Market::kShenzhen,
        sizeof(sz::Transaction300191_v2), 12U, 34U, 40U, limits,
        &tick.common);
    if (error != DecodeError::kNone) {
        return error;
    }
    std::int64_t sequence = 0;
    if (!LoadUnsigned(message.body, 0U, &tick.common.channel) ||
        !LoadI64(message.body, 4U, &sequence) || sequence <= 0 ||
        !LoadI64(message.body, 18U, &tick.buy_order_id) ||
        !LoadI64(message.body, 26U, &tick.sell_order_id) ||
        !LoadI64(message.body, 54U, &tick.quantity.raw) ||
        !LoadI32(message.body, 62U, &tick.raw_type)) {
        return sequence <= 0 ? DecodeError::kInvalidNativeSequence
                             : DecodeError::kTruncated;
    }
    tick.common.native_sequence = static_cast<std::uint64_t>(sequence);
    if (tick.buy_order_id < 0 || tick.sell_order_id < 0) {
        tick.common.quality_flags |= kQualityInvalidOrderReference;
    }
    tick.quantity.scale = 0U;
    if (tick.quantity.raw >= 0) {
        tick.quantity.valid = true;
    } else {
        tick.common.quality_flags |= kQualityInvalidQuantity;
    }
    std::uint32_t exchange_time = 0U;
    if (!LoadUnsigned(message.body, 66U, &exchange_time)) {
        return DecodeError::kTruncated;
    }
    tick.common.exchange_time_raw = exchange_time;
    if (DecodeTime(exchange_time, &tick.common.quality_flags,
                   &tick.common.exchange_time_ns_from_midnight)) {
        tick.common.exchange_time_valid = true;
        tick.validity |= kTickExchangeTimeValid;
    }
    error = LoadDecimal64Raw(message.body, 46U, 4U,
                             &tick.common.quality_flags, &tick.price);
    if (error != DecodeError::kNone) {
        return error;
    }
    const auto publish_order_ids = [&tick]() {
        if (tick.buy_order_id > 0) {
            tick.validity |= kTickBuyOrderIdValid;
        }
        if (tick.sell_order_id > 0) {
            tick.validity |= kTickSellOrderIdValid;
        }
    };
    if (tick.raw_type == 70) {
        tick.action = TickAction::kTrade;
        publish_order_ids();
        if (tick.price.raw_valid && tick.price.raw > 0) {
            error = NormalizeP6(&tick.price);
        } else if (tick.price.raw_valid) {
            tick.common.quality_flags |= kQualityInvalidPrice;
        }
        if (error != DecodeError::kNone) {
            return error;
        }
        if (tick.price.p6_valid) {
            tick.validity |= kTickPriceValid;
        }
        if (tick.quantity.valid) {
            tick.validity |= kTickQuantityValid;
        }
    } else if (tick.raw_type == 52) {
        tick.action = TickAction::kCancel;
        publish_order_ids();
        if (tick.quantity.valid) {
            tick.validity |= kTickQuantityValid;
        }
        const bool has_bid = tick.buy_order_id > 0;
        const bool has_ask = tick.sell_order_id > 0;
        if (has_bid != has_ask) {
            tick.primary_order_id =
                has_bid ? tick.buy_order_id : tick.sell_order_id;
            tick.side = has_bid ? Side::kBuy : Side::kSell;
            tick.validity |= kTickPrimaryOrderIdValid | kTickSideValid;
        } else {
            tick.common.quality_flags |=
                kQualityAmbiguousOrderReference;
        }
    } else {
        tick.quantity.valid = false;
        tick.common.quality_flags |= kQualityUnknownEnum;
    }
    output->catalog_match = ApplyCatalog(catalog, &tick.common);
    output->tick = tick;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError ValidateAndDecodeShLevel(
    std::span<const std::byte> body,
    const ListView& levels,
    std::size_t index,
    const DecoderLimits& limits,
    std::size_t* total_queue_items,
    std::uint64_t* quality,
    CanonicalBookLevel* retained) noexcept {
    const std::size_t level_offset =
        levels.start + index * levels.item_size;
    CanonicalBookLevel level{};
    DecodeError error = DecodePrice32(
        body, level_offset + 4U, 3U, quality, &level.price);
    if (error != DecodeError::kNone) {
        return error;
    }
    error = DecodeQuantity(body, level_offset + 8U, 3U, true,
                           quality, &level.quantity);
    if (error != DecodeError::kNone ||
        !LoadUnsigned(body, level_offset + 16U,
                      &level.source_order_count)) {
        return error == DecodeError::kNone ? DecodeError::kTruncated
                                            : error;
    }
    level.order_count_valid = true;
    ListView queue;
    error = ReadList(body, level_offset + 20U, 16U,
                     sizeof(sh::SHL2MarketData),
                     limits.maximum_queue_items, &queue);
    if (error != DecodeError::kNone) {
        return error;
    }
    if (queue.count > level.source_order_count) {
        return DecodeError::kCountMismatch;
    }
    if (queue.count > limits.maximum_queue_items - *total_queue_items) {
        return DecodeError::kCountExceeded;
    }
    *total_queue_items += queue.count;
    for (std::size_t queue_index = 0U; queue_index < queue.count;
         ++queue_index) {
        ScaledInteger ignored{};
        error = DecodeQuantity(
            body, queue.start + queue_index * queue.item_size + 8U,
            3U, true, quality, &ignored);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    if (retained != nullptr) {
        *retained = level;
    }
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError DecodeShanghaiSnapshot(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    SnapshotDecodeResult* output) noexcept {
    if (message.body.size() < sizeof(sh::SHL2MarketData)) {
        return DecodeError::kTruncated;
    }
    CanonicalSnapshot snapshot{};
    InitializeCommon(message, trade_date, Market::kShanghai,
                     CanonicalKind::kShanghaiSnapshot, &snapshot.common);
    DecodeError error = ReadIdentity(
        message.body, Market::kShanghai, sizeof(sh::SHL2MarketData),
        std::numeric_limits<std::size_t>::max(), 4U,
        std::numeric_limits<std::size_t>::max(), limits, &snapshot.common);
    if (error != DecodeError::kNone) {
        return error;
    }
    TextView status;
    error = ReadText(message.body, 38U, sizeof(sh::SHL2MarketData),
                     limits.maximum_text_bytes, false, &status);
    if (error != DecodeError::kNone) {
        return error;
    }
    CopyText(status.bytes, &snapshot.instrument_status_code,
             &snapshot.instrument_status_code_size);
    if (!LoadI32(message.body, 10U, &snapshot.image_status)) {
        return DecodeError::kTruncated;
    }
    snapshot.image_status_valid = true;
    std::uint32_t exchange_time = 0U;
    if (!LoadUnsigned(message.body, 0U, &exchange_time)) {
        return DecodeError::kTruncated;
    }
    snapshot.common.exchange_time_raw = exchange_time;
    snapshot.common.exchange_time_valid = DecodeTime(
        exchange_time, &snapshot.common.quality_flags,
        &snapshot.common.exchange_time_ns_from_midnight);

    const auto price = [&](std::size_t offset, FixedDecimal* value) {
        return DecodePrice32(message.body, offset, 3U,
                             &snapshot.common.quality_flags, value);
    };
    error = price(14U, &snapshot.previous_close);
    if (error == DecodeError::kNone) {
        error = price(18U, &snapshot.open);
    }
    if (error == DecodeError::kNone) {
        error = price(22U, &snapshot.high);
    }
    if (error == DecodeError::kNone) {
        error = price(26U, &snapshot.low);
    }
    if (error == DecodeError::kNone) {
        error = price(30U, &snapshot.last);
    }
    if (error == DecodeError::kNone) {
        error = price(34U, &snapshot.close);
    }
    if (error != DecodeError::kNone) {
        return error;
    }
    std::uint32_t trade_count = 0U;
    if (!LoadUnsigned(message.body, 44U, &trade_count)) {
        return DecodeError::kTruncated;
    }
    snapshot.trade_count = trade_count;
    snapshot.trade_count_valid = true;
    error = DecodeQuantity(message.body, 48U, 3U, true,
                           &snapshot.common.quality_flags, &snapshot.volume);
    if (error == DecodeError::kNone) {
        error = DecodeDecimal64(message.body, 56U, 5U,
                                &snapshot.common.quality_flags,
                                &snapshot.turnover);
    }
    if (error == DecodeError::kNone) {
        error = DecodeQuantity(message.body, 64U, 3U, true,
                               &snapshot.common.quality_flags,
                               &snapshot.total_bid_quantity);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice32(message.body, 72U, 3U,
                              &snapshot.common.quality_flags,
                              &snapshot.weighted_average_bid);
    }
    if (error == DecodeError::kNone) {
        error = DecodeQuantity(message.body, 80U, 3U, true,
                               &snapshot.common.quality_flags,
                               &snapshot.total_ask_quantity);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice32(message.body, 88U, 3U,
                              &snapshot.common.quality_flags,
                              &snapshot.weighted_average_ask);
    }
    if (error != DecodeError::kNone) {
        return error;
    }

    ListView bids;
    ListView asks;
    error = ReadList(message.body, 228U, 28U,
                     sizeof(sh::SHL2MarketData),
                     limits.maximum_depth_items, &bids);
    if (error == DecodeError::kNone) {
        error = ReadList(message.body, 236U, 28U,
                         sizeof(sh::SHL2MarketData),
                         limits.maximum_depth_items, &asks);
    }
    if (error != DecodeError::kNone) {
        return error;
    }
    snapshot.source_bid_depth = static_cast<std::uint32_t>(bids.count);
    snapshot.source_ask_depth = static_cast<std::uint32_t>(asks.count);
    snapshot.retained_bid_depth = static_cast<std::uint8_t>(
        std::min(bids.count, kCanonicalBookDepth));
    snapshot.retained_ask_depth = static_cast<std::uint8_t>(
        std::min(asks.count, kCanonicalBookDepth));
    if (bids.count > kCanonicalBookDepth ||
        asks.count > kCanonicalBookDepth) {
        snapshot.common.quality_flags |= kQualityDepthTruncated;
    }
    std::size_t total_queue_items = 0U;
    for (std::size_t index = 0U; index < bids.count; ++index) {
        CanonicalBookLevel* retained =
            index < kCanonicalBookDepth ? &snapshot.bids[index] : nullptr;
        error = ValidateAndDecodeShLevel(
            message.body, bids, index, limits, &total_queue_items,
            &snapshot.common.quality_flags, retained);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    for (std::size_t index = 0U; index < asks.count; ++index) {
        CanonicalBookLevel* retained =
            index < kCanonicalBookDepth ? &snapshot.asks[index] : nullptr;
        error = ValidateAndDecodeShLevel(
            message.body, asks, index, limits, &total_queue_items,
            &snapshot.common.quality_flags, retained);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    output->catalog_match = ApplyCatalog(catalog, &snapshot.common);
    output->snapshot = snapshot;
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError ValidateAndDecodeSzLevel(
    std::span<const std::byte> body,
    const ListView& levels,
    std::size_t index,
    const DecoderLimits& limits,
    std::size_t* total_queue_items,
    std::uint64_t* quality,
    CanonicalBookLevel* retained) noexcept {
    const std::size_t level_offset =
        levels.start + index * levels.item_size;
    CanonicalBookLevel level{};
    DecodeError error = DecodeQuantity(
        body, level_offset, 0U, false, quality, &level.quantity);
    if (error == DecodeError::kNone) {
        error = DecodePrice64(body, level_offset + 8U, 6U,
                              quality, &level.price);
    }
    if (error != DecodeError::kNone ||
        !LoadUnsigned(body, level_offset + 16U,
                      &level.source_order_count)) {
        return error == DecodeError::kNone ? DecodeError::kTruncated
                                            : error;
    }
    level.order_count_valid = true;
    ListView queue;
    error = ReadList(body, level_offset + 20U, 8U,
                     sizeof(sz::Snapshot300111_v2),
                     limits.maximum_queue_items, &queue);
    if (error != DecodeError::kNone) {
        return error;
    }
    if (queue.count > level.source_order_count) {
        return DecodeError::kCountMismatch;
    }
    if (queue.count > limits.maximum_queue_items - *total_queue_items) {
        return DecodeError::kCountExceeded;
    }
    *total_queue_items += queue.count;
    for (std::size_t queue_index = 0U; queue_index < queue.count;
         ++queue_index) {
        ScaledInteger ignored{};
        error = DecodeQuantity(
            body, queue.start + queue_index * queue.item_size,
            0U, false, quality, &ignored);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    if (retained != nullptr) {
        *retained = level;
    }
    return DecodeError::kNone;
}

[[nodiscard]] DecodeError DecodeShenzhenSnapshot(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    SnapshotDecodeResult* output) noexcept {
    if (message.body.size() < sizeof(sz::Snapshot300111_v2)) {
        return DecodeError::kTruncated;
    }
    CanonicalSnapshot snapshot{};
    InitializeCommon(message, trade_date, Market::kShenzhen,
                     CanonicalKind::kShenzhenSnapshot, &snapshot.common);
    DecodeError error = ReadIdentity(
        message.body, Market::kShenzhen, sizeof(sz::Snapshot300111_v2),
        8U, 14U, 20U, limits, &snapshot.common);
    if (error != DecodeError::kNone) {
        return error;
    }
    TextView phase;
    error = ReadText(message.body, 26U, sizeof(sz::Snapshot300111_v2),
                     limits.maximum_text_bytes, true, &phase);
    if (error != DecodeError::kNone) {
        return error;
    }
    CopyText(phase.bytes, &snapshot.trading_phase_code,
             &snapshot.trading_phase_code_size);
    if (!LoadUnsigned(message.body, 4U, &snapshot.common.channel)) {
        return DecodeError::kTruncated;
    }
    std::uint32_t exchange_time = 0U;
    if (!LoadUnsigned(message.body, 0U, &exchange_time)) {
        return DecodeError::kTruncated;
    }
    snapshot.common.exchange_time_raw = exchange_time;
    snapshot.common.exchange_time_valid = DecodeTime(
        exchange_time, &snapshot.common.quality_flags,
        &snapshot.common.exchange_time_ns_from_midnight);
    std::int64_t trade_count = 0;
    if (!LoadI64(message.body, 40U, &trade_count)) {
        return DecodeError::kTruncated;
    }
    if (trade_count >= 0) {
        snapshot.trade_count = static_cast<std::uint64_t>(trade_count);
        snapshot.trade_count_valid = true;
    } else {
        snapshot.common.quality_flags |= kQualityInvalidQuantity;
    }

    error = DecodePrice64(message.body, 32U, 4U,
                          &snapshot.common.quality_flags,
                          &snapshot.previous_close);
    if (error == DecodeError::kNone) {
        error = DecodeQuantity(message.body, 48U, 0U, false,
                               &snapshot.common.quality_flags,
                               &snapshot.volume);
    }
    if (error == DecodeError::kNone) {
        error = DecodeDecimal64(message.body, 56U, 4U,
                                &snapshot.common.quality_flags,
                                &snapshot.turnover);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 64U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.last);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 72U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.open);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 80U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.high);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 88U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.low);
    }
    if (error == DecodeError::kNone) {
        error = DecodeQuantity(message.body, 144U, 0U, false,
                               &snapshot.common.quality_flags,
                               &snapshot.total_ask_quantity);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 152U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.weighted_average_ask);
    }
    if (error == DecodeError::kNone) {
        error = DecodeQuantity(message.body, 160U, 0U, false,
                               &snapshot.common.quality_flags,
                               &snapshot.total_bid_quantity);
    }
    if (error == DecodeError::kNone) {
        error = DecodePrice64(message.body, 168U, 6U,
                              &snapshot.common.quality_flags,
                              &snapshot.weighted_average_bid);
    }
    if (error != DecodeError::kNone) {
        return error;
    }

    ListView bids;
    ListView asks;
    error = ReadList(message.body, 208U, 28U,
                     sizeof(sz::Snapshot300111_v2),
                     limits.maximum_depth_items, &bids);
    if (error == DecodeError::kNone) {
        error = ReadList(message.body, 216U, 28U,
                         sizeof(sz::Snapshot300111_v2),
                         limits.maximum_depth_items, &asks);
    }
    if (error != DecodeError::kNone) {
        return error;
    }
    snapshot.source_bid_depth = static_cast<std::uint32_t>(bids.count);
    snapshot.source_ask_depth = static_cast<std::uint32_t>(asks.count);
    snapshot.retained_bid_depth = static_cast<std::uint8_t>(
        std::min(bids.count, kCanonicalBookDepth));
    snapshot.retained_ask_depth = static_cast<std::uint8_t>(
        std::min(asks.count, kCanonicalBookDepth));
    if (bids.count > kCanonicalBookDepth ||
        asks.count > kCanonicalBookDepth) {
        snapshot.common.quality_flags |= kQualityDepthTruncated;
    }
    std::size_t total_queue_items = 0U;
    for (std::size_t index = 0U; index < bids.count; ++index) {
        CanonicalBookLevel* retained =
            index < kCanonicalBookDepth ? &snapshot.bids[index] : nullptr;
        error = ValidateAndDecodeSzLevel(
            message.body, bids, index, limits, &total_queue_items,
            &snapshot.common.quality_flags, retained);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    for (std::size_t index = 0U; index < asks.count; ++index) {
        CanonicalBookLevel* retained =
            index < kCanonicalBookDepth ? &snapshot.asks[index] : nullptr;
        error = ValidateAndDecodeSzLevel(
            message.body, asks, index, limits, &total_queue_items,
            &snapshot.common.quality_flags, retained);
        if (error != DecodeError::kNone) {
            return error;
        }
    }
    output->catalog_match = ApplyCatalog(catalog, &snapshot.common);
    output->snapshot = snapshot;
    return DecodeError::kNone;
}

[[nodiscard]] std::uint64_t HashRouteBytes(
    std::span<const std::byte> first,
    std::span<const std::byte> second) noexcept {
    std::uint64_t hash = UINT64_C(1469598103934665603);
    const auto append = [&hash](std::byte item) {
        hash ^= std::to_integer<unsigned char>(item);
        hash *= UINT64_C(1099511628211);
    };
    for (const std::byte item : first) {
        append(item);
    }
    append(std::byte{0xffU});
    for (const std::byte item : second) {
        append(item);
    }
    return hash;
}

}  // namespace

DecodeError DecodeTick(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    TickDecodeResult* output) noexcept {
    if (output == nullptr) {
        return DecodeError::kTruncated;
    }
    DynamicRangeScope ranges(8U);
    if (!ranges.valid()) {
        return DecodeError::kResourceExhausted;
    }
    const MessageClass message_class = ClassifyMessage(message.header.key);
    switch (message_class) {
        case MessageClass::kShanghaiTick:
            return DecodeShanghaiTick(
                message, trade_date, limits, catalog, output);
        case MessageClass::kShenzhenOrder:
            return DecodeShenzhenOrder(
                message, trade_date, limits, catalog, output);
        case MessageClass::kShenzhenTransaction:
            return DecodeShenzhenTransaction(
                message, trade_date, limits, catalog, output);
        default:
            return DecodeError::kUnsupported;
    }
}

DecodeError DecodeSnapshot(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    SnapshotDecodeResult* output) noexcept {
    if (output == nullptr) {
        return DecodeError::kTruncated;
    }
    if (limits.maximum_depth_items >
        (std::numeric_limits<std::size_t>::max() - 8U) / 2U) {
        return DecodeError::kResourceExhausted;
    }
    DynamicRangeScope ranges(limits.maximum_depth_items * 2U + 8U);
    if (!ranges.valid()) {
        return DecodeError::kResourceExhausted;
    }
    const MessageClass message_class = ClassifyMessage(message.header.key);
    switch (message_class) {
        case MessageClass::kShanghaiSnapshot:
            return DecodeShanghaiSnapshot(
                message, trade_date, limits, catalog, output);
        case MessageClass::kShenzhenSnapshot:
            return DecodeShenzhenSnapshot(
                message, trade_date, limits, catalog, output);
        default:
            return DecodeError::kUnsupported;
    }
}

bool ExtractAdmissionRoute(
    MessageClass message_class,
    std::span<const std::byte> body,
    std::uint64_t* route_key) noexcept {
    if (route_key == nullptr) {
        return false;
    }
    if (message_class == MessageClass::kShanghaiTick) {
        std::int32_t channel = 0;
        if (body.size() < sizeof(sh::NGTSTick) ||
            !LoadI32(body, 8U, &channel) || channel <= 0) {
            return false;
        }
        *route_key = UINT64_C(0x0400000000) |
                     static_cast<std::uint32_t>(channel);
        return true;
    }
    if (message_class == MessageClass::kShenzhenOrder ||
        message_class == MessageClass::kShenzhenTransaction) {
        const std::size_t fixed =
            message_class == MessageClass::kShenzhenOrder
                ? sizeof(sz::Order300192_v2)
                : sizeof(sz::Transaction300191_v2);
        std::uint32_t channel = 0U;
        if (body.size() < fixed || !LoadUnsigned(body, 0U, &channel)) {
            return false;
        }
        *route_key = UINT64_C(0x0600000000) | channel;
        return true;
    }
    if (message_class == MessageClass::kShanghaiSnapshot) {
        TextView security;
        if (ReadText(body, 4U, sizeof(sh::SHL2MarketData),
                     kMaximumIdentityBytes, true, &security) !=
            DecodeError::kNone) {
            return false;
        }
        *route_key = HashRouteBytes({}, security.bytes);
        return true;
    }
    if (message_class == MessageClass::kShenzhenSnapshot) {
        TextView security;
        TextView source;
        if (ReadText(body, 14U, sizeof(sz::Snapshot300111_v2),
                     kMaximumIdentityBytes, true, &security) !=
                DecodeError::kNone ||
            ReadText(body, 20U, sizeof(sz::Snapshot300111_v2),
                     kMaximumIdentityBytes, true, &source) !=
                DecodeError::kNone) {
            return false;
        }
        *route_key = HashRouteBytes(source.bytes, security.bytes);
        return true;
    }
    return false;
}

bool ExtractTickNativeDescriptor(
    MessageClass message_class,
    std::span<const std::byte> body,
    Market* market,
    std::uint32_t* channel,
    std::uint64_t* sequence) noexcept {
    if (market == nullptr || channel == nullptr || sequence == nullptr) {
        return false;
    }
    if (message_class == MessageClass::kShanghaiTick) {
        std::int64_t raw_sequence = 0;
        std::int32_t raw_channel = 0;
        if (body.size() < sizeof(sh::NGTSTick) ||
            !LoadI64(body, 0U, &raw_sequence) || raw_sequence <= 0 ||
            !LoadI32(body, 8U, &raw_channel) || raw_channel <= 0) {
            return false;
        }
        *market = Market::kShanghai;
        *channel = static_cast<std::uint32_t>(raw_channel);
        *sequence = static_cast<std::uint64_t>(raw_sequence);
        return true;
    }
    if (message_class == MessageClass::kShenzhenOrder ||
        message_class == MessageClass::kShenzhenTransaction) {
        std::int64_t raw_sequence = 0;
        const std::size_t fixed =
            message_class == MessageClass::kShenzhenOrder
                ? sizeof(sz::Order300192_v2)
                : sizeof(sz::Transaction300191_v2);
        if (body.size() < fixed || !LoadUnsigned(body, 0U, channel) ||
            !LoadI64(body, 4U, &raw_sequence) || raw_sequence <= 0) {
            return false;
        }
        *market = Market::kShenzhen;
        *sequence = static_cast<std::uint64_t>(raw_sequence);
        return true;
    }
    return false;
}

}  // namespace l2flow::ingest::internal
