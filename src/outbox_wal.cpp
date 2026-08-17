#include "l2flow/outbox/wal.h"

#include "l2flow/checksum/crc32c.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <semaphore>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/random.h>
#endif

namespace l2flow::outbox {
namespace {

constexpr std::array<std::byte, 8U> kSegmentMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'O'}, std::byte{'B'},
    std::byte{'S'}, std::byte{'E'}, std::byte{'G'}, std::byte{1U}};
constexpr std::array<std::byte, 8U> kBatchMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'O'}, std::byte{'B'},
    std::byte{'B'}, std::byte{'A'}, std::byte{'T'}, std::byte{1U}};
constexpr std::array<std::byte, 8U> kFooterMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'O'}, std::byte{'B'},
    std::byte{'E'}, std::byte{'N'}, std::byte{'D'}, std::byte{1U}};
constexpr std::array<std::byte, 8U> kCursorMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'O'}, std::byte{'B'},
    std::byte{'C'}, std::byte{'U'}, std::byte{'R'}, std::byte{1U}};
constexpr std::uint32_t kWalFormatVersion = 2U;
constexpr std::size_t kBatchHeaderBytes = 100U;
constexpr std::size_t kBatchFooterBytes = 28U;

[[nodiscard]] std::size_t ConsumerIndex(ConsumerKind consumer) noexcept {
    return static_cast<std::size_t>(consumer);
}

[[nodiscard]] bool IsZero(Identifier128 identifier) noexcept {
    return std::all_of(
        identifier.bytes.begin(), identifier.bytes.end(),
        [](std::byte value) { return value == std::byte{0U}; });
}

[[nodiscard]] Identifier128 GenerateIdentifier() {
    Identifier128 identifier{};
#if defined(__linux__)
    std::byte* destination = identifier.bytes.data();
    std::size_t remaining = identifier.bytes.size();
    while (remaining != 0U) {
        const ssize_t count = ::getrandom(destination, remaining, 0U);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("getrandom failed for outbox run ID");
        }
        if (count == 0) {
            throw std::runtime_error(
                "getrandom returned no bytes for outbox run ID");
        }
        const std::size_t produced = static_cast<std::size_t>(count);
        destination += produced;
        remaining -= produced;
    }
#else
    const std::uint64_t seed = ingest::MonotonicNowNs();
    for (std::size_t index = 0U; index < identifier.bytes.size(); ++index) {
        identifier.bytes[index] = static_cast<std::byte>(
            (seed >> ((index % sizeof(seed)) * 8U)) ^
            static_cast<std::uint64_t>(index * 37U + 11U));
    }
#endif
    if (IsZero(identifier)) {
        identifier.bytes.back() = std::byte{1U};
    }
    return identifier;
}

[[nodiscard]] std::string IdentifierHex(Identifier128 identifier) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string output;
    output.resize(identifier.bytes.size() * 2U);
    for (std::size_t index = 0U; index < identifier.bytes.size(); ++index) {
        const auto octet = std::to_integer<unsigned int>(
            identifier.bytes[index]);
        output[index * 2U] = digits[octet >> 4U];
        output[index * 2U + 1U] = digits[octet & 0x0fU];
    }
    return output;
}

[[nodiscard]] std::uint64_t UtcNowNs() noexcept {
    const auto value = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return value > 0 ? static_cast<std::uint64_t>(value) : 0U;
}

template <typename Value>
void AppendLe(std::vector<std::byte>* output, Value value) {
    static_assert(std::is_integral_v<Value> || std::is_enum_v<Value>);
    if constexpr (std::is_enum_v<Value>) {
        AppendLe(output, static_cast<std::underlying_type_t<Value>>(value));
    } else {
        using Unsigned = std::make_unsigned_t<Value>;
        const Unsigned encoded = static_cast<Unsigned>(value);
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            output->push_back(static_cast<std::byte>(
                encoded >> static_cast<unsigned int>(index * 8U)));
        }
    }
}

void AppendBytes(std::vector<std::byte>* output,
                 std::span<const std::byte> bytes) {
    output->insert(output->end(), bytes.begin(), bytes.end());
}

void AppendBool(std::vector<std::byte>* output, bool value) {
    AppendLe(output, static_cast<std::uint8_t>(value ? 1U : 0U));
}

class LittleEndianReader final {
public:
    explicit LittleEndianReader(std::span<const std::byte> input) noexcept
        : input_(input) {}

    template <typename Value>
    [[nodiscard]] bool Read(Value* output) noexcept {
        static_assert(std::is_integral_v<Value>);
        if (output == nullptr || offset_ > input_.size() ||
            sizeof(Value) > input_.size() - offset_) {
            good_ = false;
            return false;
        }
        using Unsigned = std::make_unsigned_t<Value>;
        Unsigned encoded = 0U;
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            const Unsigned octet = static_cast<Unsigned>(
                std::to_integer<unsigned int>(input_[offset_ + index]));
            const Unsigned shifted = static_cast<Unsigned>(
                octet << static_cast<unsigned int>(index * 8U));
            encoded = static_cast<Unsigned>(encoded | shifted);
        }
        offset_ += sizeof(Value);
        if constexpr (std::is_signed_v<Value>) {
            *output = std::bit_cast<Value>(encoded);
        } else {
            *output = encoded;
        }
        return true;
    }

    [[nodiscard]] bool ReadBool(bool* output) noexcept {
        std::uint8_t encoded = 0U;
        if (!Read(&encoded) || encoded > 1U || output == nullptr) {
            good_ = false;
            return false;
        }
        *output = encoded != 0U;
        return true;
    }

    [[nodiscard]] bool ReadBytes(std::span<std::byte> output) noexcept {
        if (offset_ > input_.size() ||
            output.size() > input_.size() - offset_) {
            good_ = false;
            return false;
        }
        std::copy_n(input_.begin() + static_cast<std::ptrdiff_t>(offset_),
                    output.size(), output.begin());
        offset_ += output.size();
        return true;
    }

    [[nodiscard]] bool ReadSpan(
        std::size_t size,
        std::span<const std::byte>* output) noexcept {
        if (output == nullptr || offset_ > input_.size() ||
            size > input_.size() - offset_) {
            good_ = false;
            return false;
        }
        *output = input_.subspan(offset_, size);
        offset_ += size;
        return true;
    }

    [[nodiscard]] bool complete() const noexcept {
        return good_ && offset_ == input_.size();
    }

private:
    std::span<const std::byte> input_;
    std::size_t offset_ = 0U;
    bool good_ = true;
};

void EncodeFixedDecimal(std::vector<std::byte>* output,
                        const ingest::FixedDecimal& value) {
    AppendLe(output, value.raw);
    AppendLe(output, value.p6);
    AppendLe(output, value.source_scale);
    AppendBool(output, value.raw_valid);
    AppendBool(output, value.p6_valid);
}

[[nodiscard]] bool DecodeFixedDecimal(LittleEndianReader* reader,
                                      ingest::FixedDecimal* value) noexcept {
    return reader != nullptr && value != nullptr &&
           reader->Read(&value->raw) && reader->Read(&value->p6) &&
           reader->Read(&value->source_scale) &&
           reader->ReadBool(&value->raw_valid) &&
           reader->ReadBool(&value->p6_valid);
}

void EncodeScaledInteger(std::vector<std::byte>* output,
                         const ingest::ScaledInteger& value) {
    AppendLe(output, value.raw);
    AppendLe(output, value.scale);
    AppendBool(output, value.valid);
}

[[nodiscard]] bool DecodeScaledInteger(
    LittleEndianReader* reader,
    ingest::ScaledInteger* value) noexcept {
    return reader != nullptr && value != nullptr &&
           reader->Read(&value->raw) && reader->Read(&value->scale) &&
           reader->ReadBool(&value->valid);
}

void EncodeCanonicalCommon(std::vector<std::byte>* output,
                           const ingest::CanonicalCommon& value) {
    AppendLe(output, value.ingress_sequence);
    AppendLe(output, value.vendor_sequence_id);
    AppendLe(output, value.receive_monotonic_ns);
    AppendLe(output, value.native_sequence);
    AppendLe(output, value.exchange_time_ns_from_midnight);
    AppendLe(output, value.vendor_local_time_ns_from_midnight);
    AppendLe(output, value.quality_flags);
    AppendLe(output, value.gap_epoch);
    AppendLe(output, value.gap_before_first);
    AppendLe(output, value.gap_before_last);
    AppendLe(output, value.trade_date);
    AppendLe(output, value.instrument_id);
    AppendLe(output, value.instrument_ordinal);
    AppendLe(output, value.channel);
    AppendLe(output, value.exchange_time_raw);
    AppendLe(output, value.vendor_local_time_raw);
    AppendBool(output, value.exchange_time_valid);
    AppendBool(output, value.vendor_local_time_valid);
    AppendLe(output, value.message_key.service_id);
    AppendLe(output, value.message_key.service_version);
    AppendLe(output, value.message_key.message_id);
    AppendLe(output, value.kind);
    AppendLe(output, value.identity.market);
    AppendLe(output, value.identity.security_id_source_size);
    AppendLe(output, value.identity.security_id_size);
    AppendBytes(output, value.identity.security_id_source);
    AppendBytes(output, value.identity.security_id);
    AppendLe(output, value.md_stream_id_size);
    AppendBytes(output, value.md_stream_id);
}

[[nodiscard]] bool DecodeCanonicalCommon(
    LittleEndianReader* reader,
    ingest::CanonicalCommon* value) noexcept {
    if (reader == nullptr || value == nullptr ||
        !reader->Read(&value->ingress_sequence) ||
        !reader->Read(&value->vendor_sequence_id) ||
        !reader->Read(&value->receive_monotonic_ns) ||
        !reader->Read(&value->native_sequence) ||
        !reader->Read(&value->exchange_time_ns_from_midnight) ||
        !reader->Read(&value->vendor_local_time_ns_from_midnight) ||
        !reader->Read(&value->quality_flags) ||
        !reader->Read(&value->gap_epoch) ||
        !reader->Read(&value->gap_before_first) ||
        !reader->Read(&value->gap_before_last) ||
        !reader->Read(&value->trade_date) ||
        !reader->Read(&value->instrument_id) ||
        !reader->Read(&value->instrument_ordinal) ||
        !reader->Read(&value->channel) ||
        !reader->Read(&value->exchange_time_raw) ||
        !reader->Read(&value->vendor_local_time_raw) ||
        !reader->ReadBool(&value->exchange_time_valid) ||
        !reader->ReadBool(&value->vendor_local_time_valid) ||
        !reader->Read(&value->message_key.service_id) ||
        !reader->Read(&value->message_key.service_version) ||
        !reader->Read(&value->message_key.message_id)) {
        return false;
    }
    std::uint8_t kind = 0U;
    std::uint8_t market = 0U;
    if (!reader->Read(&kind) || !reader->Read(&market) ||
        kind > static_cast<std::uint8_t>(
                   ingest::CanonicalKind::kShenzhenSnapshot) ||
        market > static_cast<std::uint8_t>(ingest::Market::kShenzhen) ||
        !reader->Read(&value->identity.security_id_source_size) ||
        !reader->Read(&value->identity.security_id_size) ||
        value->identity.security_id_source_size >
            ingest::kMaximumIdentityBytes ||
        value->identity.security_id_size > ingest::kMaximumIdentityBytes ||
        !reader->ReadBytes(value->identity.security_id_source) ||
        !reader->ReadBytes(value->identity.security_id) ||
        !reader->Read(&value->md_stream_id_size) ||
        value->md_stream_id_size > ingest::kMaximumIdentityBytes ||
        !reader->ReadBytes(value->md_stream_id)) {
        return false;
    }
    value->kind = static_cast<ingest::CanonicalKind>(kind);
    value->identity.market = static_cast<ingest::Market>(market);
    return true;
}

void EncodeCanonicalTick(std::vector<std::byte>* output,
                         const ingest::CanonicalTick& value) {
    EncodeCanonicalCommon(output, value.common);
    EncodeFixedDecimal(output, value.price);
    EncodeFixedDecimal(output, value.amount);
    EncodeScaledInteger(output, value.quantity);
    AppendLe(output, value.primary_order_id);
    AppendLe(output, value.buy_order_id);
    AppendLe(output, value.sell_order_id);
    AppendLe(output, value.sh_add_matched_quantity_raw);
    AppendLe(output, value.validity);
    AppendLe(output, value.raw_type);
    AppendLe(output, value.raw_side);
    AppendLe(output, value.action);
    AppendLe(output, value.side);
    AppendLe(output, value.aggressor);
    AppendLe(output, value.order_type);
    AppendLe(output, value.phase);
}

[[nodiscard]] bool DecodeCanonicalTick(LittleEndianReader* reader,
                                       ingest::CanonicalTick* value) noexcept {
    if (reader == nullptr || value == nullptr ||
        !DecodeCanonicalCommon(reader, &value->common) ||
        !DecodeFixedDecimal(reader, &value->price) ||
        !DecodeFixedDecimal(reader, &value->amount) ||
        !DecodeScaledInteger(reader, &value->quantity) ||
        !reader->Read(&value->primary_order_id) ||
        !reader->Read(&value->buy_order_id) ||
        !reader->Read(&value->sell_order_id) ||
        !reader->Read(&value->sh_add_matched_quantity_raw) ||
        !reader->Read(&value->validity) ||
        !reader->Read(&value->raw_type) ||
        !reader->Read(&value->raw_side)) {
        return false;
    }
    std::uint8_t action = 0U;
    std::uint8_t side = 0U;
    std::uint8_t aggressor = 0U;
    std::uint8_t order_type = 0U;
    std::uint8_t phase = 0U;
    if (!reader->Read(&action) || !reader->Read(&side) ||
        !reader->Read(&aggressor) || !reader->Read(&order_type) ||
        !reader->Read(&phase) ||
        action > static_cast<std::uint8_t>(ingest::TickAction::kStatus) ||
        side > static_cast<std::uint8_t>(ingest::Side::kLend) ||
        aggressor > static_cast<std::uint8_t>(ingest::Aggressor::kNeutral) ||
        order_type >
            static_cast<std::uint8_t>(ingest::OrderType::kSameSideBest) ||
        phase > static_cast<std::uint8_t>(ingest::TradingPhase::kEnded)) {
        return false;
    }
    value->action = static_cast<ingest::TickAction>(action);
    value->side = static_cast<ingest::Side>(side);
    value->aggressor = static_cast<ingest::Aggressor>(aggressor);
    value->order_type = static_cast<ingest::OrderType>(order_type);
    value->phase = static_cast<ingest::TradingPhase>(phase);
    return true;
}

void EncodeBookLevel(std::vector<std::byte>* output,
                     const ingest::CanonicalBookLevel& value) {
    EncodeFixedDecimal(output, value.price);
    EncodeScaledInteger(output, value.quantity);
    AppendLe(output, value.source_order_count);
    AppendBool(output, value.order_count_valid);
}

[[nodiscard]] bool DecodeBookLevel(
    LittleEndianReader* reader,
    ingest::CanonicalBookLevel* value) noexcept {
    return reader != nullptr && value != nullptr &&
           DecodeFixedDecimal(reader, &value->price) &&
           DecodeScaledInteger(reader, &value->quantity) &&
           reader->Read(&value->source_order_count) &&
           reader->ReadBool(&value->order_count_valid);
}

void EncodeCanonicalSnapshot(std::vector<std::byte>* output,
                             const ingest::CanonicalSnapshot& value) {
    EncodeCanonicalCommon(output, value.common);
    EncodeFixedDecimal(output, value.previous_close);
    EncodeFixedDecimal(output, value.open);
    EncodeFixedDecimal(output, value.high);
    EncodeFixedDecimal(output, value.low);
    EncodeFixedDecimal(output, value.last);
    EncodeFixedDecimal(output, value.close);
    EncodeFixedDecimal(output, value.turnover);
    EncodeScaledInteger(output, value.volume);
    EncodeScaledInteger(output, value.total_bid_quantity);
    EncodeScaledInteger(output, value.total_ask_quantity);
    EncodeFixedDecimal(output, value.weighted_average_bid);
    EncodeFixedDecimal(output, value.weighted_average_ask);
    AppendLe(output, value.trade_count);
    AppendBool(output, value.trade_count_valid);
    AppendLe(output, value.image_status);
    AppendBool(output, value.image_status_valid);
    AppendLe(output, value.instrument_status_code_size);
    AppendBytes(output, value.instrument_status_code);
    AppendLe(output, value.trading_phase_code_size);
    AppendBytes(output, value.trading_phase_code);
    AppendLe(output, value.source_bid_depth);
    AppendLe(output, value.source_ask_depth);
    AppendLe(output, value.retained_bid_depth);
    AppendLe(output, value.retained_ask_depth);
    for (const ingest::CanonicalBookLevel& level : value.bids) {
        EncodeBookLevel(output, level);
    }
    for (const ingest::CanonicalBookLevel& level : value.asks) {
        EncodeBookLevel(output, level);
    }
}

[[nodiscard]] bool DecodeCanonicalSnapshot(
    LittleEndianReader* reader,
    ingest::CanonicalSnapshot* value) noexcept {
    if (reader == nullptr || value == nullptr ||
        !DecodeCanonicalCommon(reader, &value->common) ||
        !DecodeFixedDecimal(reader, &value->previous_close) ||
        !DecodeFixedDecimal(reader, &value->open) ||
        !DecodeFixedDecimal(reader, &value->high) ||
        !DecodeFixedDecimal(reader, &value->low) ||
        !DecodeFixedDecimal(reader, &value->last) ||
        !DecodeFixedDecimal(reader, &value->close) ||
        !DecodeFixedDecimal(reader, &value->turnover) ||
        !DecodeScaledInteger(reader, &value->volume) ||
        !DecodeScaledInteger(reader, &value->total_bid_quantity) ||
        !DecodeScaledInteger(reader, &value->total_ask_quantity) ||
        !DecodeFixedDecimal(reader, &value->weighted_average_bid) ||
        !DecodeFixedDecimal(reader, &value->weighted_average_ask) ||
        !reader->Read(&value->trade_count) ||
        !reader->ReadBool(&value->trade_count_valid) ||
        !reader->Read(&value->image_status) ||
        !reader->ReadBool(&value->image_status_valid) ||
        !reader->Read(&value->instrument_status_code_size) ||
        value->instrument_status_code_size > ingest::kMaximumIdentityBytes ||
        !reader->ReadBytes(value->instrument_status_code) ||
        !reader->Read(&value->trading_phase_code_size) ||
        value->trading_phase_code_size > ingest::kMaximumIdentityBytes ||
        !reader->ReadBytes(value->trading_phase_code) ||
        !reader->Read(&value->source_bid_depth) ||
        !reader->Read(&value->source_ask_depth) ||
        !reader->Read(&value->retained_bid_depth) ||
        !reader->Read(&value->retained_ask_depth) ||
        value->retained_bid_depth > ingest::kCanonicalBookDepth ||
        value->retained_ask_depth > ingest::kCanonicalBookDepth) {
        return false;
    }
    for (ingest::CanonicalBookLevel& level : value->bids) {
        if (!DecodeBookLevel(reader, &level)) {
            return false;
        }
    }
    for (ingest::CanonicalBookLevel& level : value->asks) {
        if (!DecodeBookLevel(reader, &level)) {
            return false;
        }
    }
    return true;
}

void EncodeTickDispatch(std::vector<std::byte>* output,
                        const ingest::TickDispatch& value) {
    EncodeCanonicalTick(output, value.tick);
    AppendLe(output, value.feed_session_epoch);
    AppendLe(output, value.expected_sequence);
    AppendLe(output, value.admission_floor);
    AppendLe(output, value.generation);
    AppendLe(output, value.dispatch_fence);
    AppendLe(output, value.first_missing);
    AppendLe(output, value.last_missing);
    AppendLe(output, value.evict_before);
    AppendLe(output, value.channel);
    AppendLe(output, value.owner);
    AppendLe(output, value.market);
    AppendLe(output, value.kind);
    AppendBool(output, value.catalog_match);
}

[[nodiscard]] bool DecodeTickDispatch(LittleEndianReader* reader,
                                      ingest::TickDispatch* value) noexcept {
    if (reader == nullptr || value == nullptr ||
        !DecodeCanonicalTick(reader, &value->tick) ||
        !reader->Read(&value->feed_session_epoch) ||
        !reader->Read(&value->expected_sequence) ||
        !reader->Read(&value->admission_floor) ||
        !reader->Read(&value->generation) ||
        !reader->Read(&value->dispatch_fence) ||
        !reader->Read(&value->first_missing) ||
        !reader->Read(&value->last_missing) ||
        !reader->Read(&value->evict_before) ||
        !reader->Read(&value->channel) || !reader->Read(&value->owner)) {
        return false;
    }
    std::uint8_t market = 0U;
    std::uint8_t kind = 0U;
    if (!reader->Read(&market) || !reader->Read(&kind) ||
        market > static_cast<std::uint8_t>(ingest::Market::kShenzhen) ||
        kind > static_cast<std::uint8_t>(
                   ingest::TickDispatchKind::kChannelSeal) ||
        !reader->ReadBool(&value->catalog_match)) {
        return false;
    }
    value->market = static_cast<ingest::Market>(market);
    value->kind = static_cast<ingest::TickDispatchKind>(kind);
    return true;
}

void EncodeChannelGap(std::vector<std::byte>* output,
                      const ingest::ChannelGap& value) {
    AppendLe(output, value.market);
    AppendLe(output, value.channel);
    AppendLe(output, value.first_missing);
    AppendLe(output, value.last_missing);
    AppendLe(output, value.first_present_after_gap);
    AppendLe(output, value.detected_monotonic_ns);
    AppendLe(output, value.gap_epoch);
    AppendLe(output, value.cumulative_missing_sequences);
    AppendLe(output, value.feed_session_epoch);
}

[[nodiscard]] bool DecodeChannelGap(LittleEndianReader* reader,
                                    ingest::ChannelGap* value) noexcept {
    std::uint8_t market = 0U;
    if (reader == nullptr || value == nullptr || !reader->Read(&market) ||
        market > static_cast<std::uint8_t>(ingest::Market::kShenzhen) ||
        !reader->Read(&value->channel) ||
        !reader->Read(&value->first_missing) ||
        !reader->Read(&value->last_missing) ||
        !reader->Read(&value->first_present_after_gap) ||
        !reader->Read(&value->detected_monotonic_ns) ||
        !reader->Read(&value->gap_epoch) ||
        !reader->Read(&value->cumulative_missing_sequences) ||
        !reader->Read(&value->feed_session_epoch)) {
        return false;
    }
    value->market = static_cast<ingest::Market>(market);
    return true;
}

void EncodeChannelFault(std::vector<std::byte>* output,
                        const ingest::ChannelFault& value) {
    AppendLe(output, value.market);
    AppendLe(output, value.reason);
    AppendLe(output, value.channel);
    AppendLe(output, value.expected_sequence);
    AppendLe(output, value.observed_sequence);
    AppendLe(output, value.detected_monotonic_ns);
    AppendLe(output, value.feed_session_epoch);
}

[[nodiscard]] bool DecodeChannelFault(LittleEndianReader* reader,
                                      ingest::ChannelFault* value) noexcept {
    std::uint8_t market = 0U;
    std::uint8_t reason = 0U;
    if (reader == nullptr || value == nullptr || !reader->Read(&market) ||
        !reader->Read(&reason) ||
        market > static_cast<std::uint8_t>(ingest::Market::kShenzhen) ||
        reason > static_cast<std::uint8_t>(
                     ingest::ChannelFaultReason::kDecodeFailure) ||
        !reader->Read(&value->channel) ||
        !reader->Read(&value->expected_sequence) ||
        !reader->Read(&value->observed_sequence) ||
        !reader->Read(&value->detected_monotonic_ns) ||
        !reader->Read(&value->feed_session_epoch)) {
        return false;
    }
    value->market = static_cast<ingest::Market>(market);
    value->reason = static_cast<ingest::ChannelFaultReason>(reason);
    return true;
}

[[nodiscard]] Identifier128 BatchIdentifier(
    Identifier128 run_id,
    std::uint64_t sequence,
    std::uint32_t checksum) noexcept {
    std::uint64_t first = UINT64_C(1469598103934665603);
    std::uint64_t second = UINT64_C(1099511628211) ^ sequence;
    const auto mix = [](std::uint64_t* state, std::uint8_t octet) {
        *state ^= octet;
        *state *= UINT64_C(1099511628211);
    };
    for (const std::byte value : run_id.bytes) {
        const auto octet = static_cast<std::uint8_t>(
            std::to_integer<unsigned int>(value));
        mix(&first, octet);
        mix(&second, static_cast<std::uint8_t>(octet ^ UINT8_C(0xa5)));
    }
    for (std::size_t index = 0U; index < sizeof(sequence); ++index) {
        mix(&first, static_cast<std::uint8_t>(sequence >> (index * 8U)));
    }
    for (std::size_t index = 0U; index < sizeof(checksum); ++index) {
        mix(&second, static_cast<std::uint8_t>(checksum >> (index * 8U)));
    }
    Identifier128 result{};
    for (std::size_t index = 0U; index < sizeof(first); ++index) {
        result.bytes[index] =
            static_cast<std::byte>(first >> (index * 8U));
        result.bytes[sizeof(first) + index] =
            static_cast<std::byte>(second >> (index * 8U));
    }
    if (IsZero(result)) {
        result.bytes.back() = std::byte{1U};
    }
    return result;
}

[[nodiscard]] bool WriteAllAt(int descriptor,
                              std::span<const std::byte> bytes,
                              std::uint64_t offset,
                              std::string* error) noexcept {
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const std::uint64_t maximum_offset = static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max());
        if (offset > maximum_offset ||
            written > maximum_offset - offset) {
            if (error != nullptr) {
                *error = "WAL offset exceeds off_t";
            }
            return false;
        }
        const std::uint64_t target = offset + written;
        const ssize_t count = ::pwrite(
            descriptor, bytes.data() + written, bytes.size() - written,
            static_cast<off_t>(target));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (error != nullptr) {
                *error = std::string("pwrite failed: ") +
                         std::strerror(errno);
            }
            return false;
        }
        if (count == 0) {
            if (error != nullptr) {
                *error = "pwrite made no progress";
            }
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool ReadAllAt(int descriptor,
                             std::span<std::byte> bytes,
                             std::uint64_t offset,
                             std::string* error) noexcept {
    std::size_t read = 0U;
    while (read < bytes.size()) {
        const std::uint64_t maximum_offset = static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max());
        if (offset > maximum_offset || read > maximum_offset - offset) {
            if (error != nullptr) {
                *error = "WAL read offset exceeds off_t";
            }
            return false;
        }
        const std::uint64_t target = offset + read;
        const ssize_t count = ::pread(
            descriptor, bytes.data() + read, bytes.size() - read,
            static_cast<off_t>(target));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (error != nullptr) {
                *error = std::string("pread failed: ") +
                         std::strerror(errno);
            }
            return false;
        }
        if (count == 0) {
            if (error != nullptr) {
                *error = "pread reached an incomplete WAL frame";
            }
            return false;
        }
        read += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool SyncDescriptor(int descriptor,
                                  std::string* error) noexcept {
    for (;;) {
        if (::fdatasync(descriptor) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (error != nullptr) {
            *error = std::string("fdatasync failed: ") +
                     std::strerror(errno);
        }
        return false;
    }
}

[[nodiscard]] bool CloseDescriptor(int* descriptor,
                                   std::string* error) noexcept {
    if (descriptor == nullptr || *descriptor < 0) {
        return true;
    }
    const int value = *descriptor;
    *descriptor = -1;
    for (;;) {
        if (::close(value) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (error != nullptr) {
            *error = std::string("close failed: ") + std::strerror(errno);
        }
        return false;
    }
}

[[nodiscard]] bool IsRecordValid(const CanonicalRecord& record) noexcept;

[[nodiscard]] std::vector<std::byte> SerializeRecord(
    const CanonicalRecord& record) {
    std::vector<std::byte> payload;
    payload.reserve(1'280U);
    AppendLe(&payload, record.kind);
    AppendLe(&payload, record.producer_lane);
    AppendLe(&payload, record.owner);
    switch (record.kind) {
        case RecordKind::kTickOccurrence:
            EncodeCanonicalTick(&payload, record.raw_tick);
            EncodeTickDispatch(&payload, record.disposition);
            break;
        case RecordKind::kSnapshot:
            EncodeCanonicalSnapshot(&payload, record.raw_snapshot);
            AppendBool(&payload, record.catalog_match);
            break;
        case RecordKind::kTickControl:
            EncodeTickDispatch(&payload, record.disposition);
            break;
        case RecordKind::kGapDiagnostic:
            EncodeChannelGap(&payload, record.gap);
            break;
        case RecordKind::kChannelFault:
            EncodeChannelFault(&payload, record.fault);
            break;
        case RecordKind::kFreshnessBarrier:
        case RecordKind::kFinalBarrier:
            AppendLe(&payload, record.barrier.frontier_id);
            AppendLe(&payload, record.barrier.created_monotonic_ns);
            AppendLe(&payload, record.barrier.created_utc_ns);
            break;
    }
    return payload;
}

[[nodiscard]] bool DeserializeRecord(
    std::span<const std::byte> payload,
    std::shared_ptr<const CanonicalRecord>* output) {
    if (output == nullptr) {
        return false;
    }
    auto decoded = std::make_shared<CanonicalRecord>();
    LittleEndianReader reader(payload);
    std::uint8_t kind = 0U;
    if (!reader.Read(&kind) ||
        kind < static_cast<std::uint8_t>(RecordKind::kTickOccurrence) ||
        kind > static_cast<std::uint8_t>(RecordKind::kFinalBarrier) ||
        !reader.Read(&decoded->producer_lane) ||
        !reader.Read(&decoded->owner)) {
        return false;
    }
    decoded->kind = static_cast<RecordKind>(kind);
    bool valid = false;
    switch (decoded->kind) {
        case RecordKind::kTickOccurrence:
            valid = DecodeCanonicalTick(&reader, &decoded->raw_tick) &&
                    DecodeTickDispatch(&reader, &decoded->disposition);
            decoded->catalog_match = decoded->disposition.catalog_match;
            break;
        case RecordKind::kSnapshot:
            valid = DecodeCanonicalSnapshot(&reader, &decoded->raw_snapshot) &&
                    reader.ReadBool(&decoded->catalog_match);
            break;
        case RecordKind::kTickControl:
            valid = DecodeTickDispatch(&reader, &decoded->disposition);
            break;
        case RecordKind::kGapDiagnostic:
            valid = DecodeChannelGap(&reader, &decoded->gap);
            break;
        case RecordKind::kChannelFault:
            valid = DecodeChannelFault(&reader, &decoded->fault);
            break;
        case RecordKind::kFreshnessBarrier:
        case RecordKind::kFinalBarrier:
            valid = reader.Read(&decoded->barrier.frontier_id) &&
                    reader.Read(&decoded->barrier.created_monotonic_ns) &&
                    reader.Read(&decoded->barrier.created_utc_ns);
            break;
    }
    if (!valid || !reader.complete() || !IsRecordValid(*decoded)) {
        return false;
    }
    *output = std::move(decoded);
    return true;
}

[[nodiscard]] bool IsRecordValid(const CanonicalRecord& record) noexcept {
    switch (record.kind) {
        case RecordKind::kTickOccurrence:
            return record.raw_tick.common.ingress_sequence != 0U &&
                   record.disposition.tick.common.ingress_sequence ==
                       record.raw_tick.common.ingress_sequence &&
                   record.disposition.feed_session_epoch != 0U;
        case RecordKind::kSnapshot:
            return record.raw_snapshot.common.ingress_sequence != 0U;
        case RecordKind::kTickControl:
            return record.disposition.feed_session_epoch != 0U &&
                   (record.disposition.kind ==
                        ingest::TickDispatchKind::kGapOpen ||
                    record.disposition.kind ==
                        ingest::TickDispatchKind::kChannelSeal);
        case RecordKind::kGapDiagnostic:
            return record.gap.feed_session_epoch != 0U;
        case RecordKind::kChannelFault:
            return record.fault.feed_session_epoch != 0U;
        case RecordKind::kFreshnessBarrier:
        case RecordKind::kFinalBarrier:
            return record.barrier.frontier_id != 0U;
    }
    return false;
}

[[nodiscard]] bool RecordMatchesFeedEpoch(
    const CanonicalRecord& record,
    std::uint64_t feed_session_epoch) noexcept {
    switch (record.kind) {
        case RecordKind::kTickOccurrence:
        case RecordKind::kTickControl:
            return record.disposition.feed_session_epoch ==
                   feed_session_epoch;
        case RecordKind::kGapDiagnostic:
            return record.gap.feed_session_epoch == feed_session_epoch;
        case RecordKind::kChannelFault:
            return record.fault.feed_session_epoch == feed_session_epoch;
        case RecordKind::kSnapshot:
        case RecordKind::kFreshnessBarrier:
        case RecordKind::kFinalBarrier:
            return true;
    }
    return false;
}

}  // namespace

bool ValidateDurableOutboxConfig(const DurableOutboxConfig& config,
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
    if (config.root_directory.empty() || config.feed_session_epoch == 0U) {
        return fail("outbox directory and feed epoch are required");
    }
    if (config.producer_queue_records < 2U ||
        config.commit_batch_records == 0U ||
        config.commit_batch_records > config.producer_queue_records ||
        config.commit_batch_records >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.commit_batch_bytes < 4U * 1'024U ||
        config.commit_max_delay_ns == 0U ||
        config.commit_batch_bytes > config.segment_max_bytes / 2U ||
        config.segment_max_bytes > config.maximum_reservoir_bytes / 2U ||
        config.cursor_checkpoint_interval_ns == 0U ||
        config.read_cache_batches == 0U ||
        config.read_cache_batches > 1'024U) {
        return fail("outbox queue, batch, segment, or reservoir bound is invalid");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

class DurableOutbox::Impl final {
public:
    // Raw, Event, and KLine are the three independent durable WAL readers.
    // Give each reader one cold-decode slot while bounding simultaneous frame
    // allocation, checksum work, and deserialization.
    static constexpr std::ptrdiff_t kColdReadConcurrency = 3;

    struct PendingRecord final {
        std::shared_ptr<const CanonicalRecord> record;
        std::vector<std::byte> payload;
        std::uint32_t checksum = 0U;
    };

    struct StoredRecord final {
        WalPosition position{};
        Identifier128 batch_id{};
        std::uint32_t payload_checksum = 0U;
        std::shared_ptr<const CanonicalRecord> record;
    };

    struct BatchIndex final {
        std::uint64_t first_lsn = 0U;
        std::uint64_t sequence = 0U;
        std::uint64_t segment_sequence = 0U;
        std::uint64_t offset = 0U;
        std::uint64_t frame_bytes = 0U;
        std::uint32_t row_count = 0U;
        std::uint32_t payload_checksum = 0U;
        Identifier128 batch_id{};

        [[nodiscard]] std::uint64_t last_lsn() const noexcept {
            return first_lsn + static_cast<std::uint64_t>(row_count) - 1U;
        }
    };

    struct CachedBatch final {
        std::uint64_t sequence = 0U;
        std::uint64_t first_lsn = 0U;
        std::vector<StoredRecord> records;

        [[nodiscard]] std::uint64_t last_lsn() const noexcept {
            return records.empty()
                ? first_lsn
                : first_lsn +
                      static_cast<std::uint64_t>(records.size()) - 1U;
        }
    };

    struct ReservoirAccounting final {
        std::atomic<std::uint64_t> bytes{0U};
    };

    struct SegmentFile final {
        explicit SegmentFile(
            std::shared_ptr<ReservoirAccounting> accounting) noexcept
            : reservoir(std::move(accounting)) {}

        ~SegmentFile() {
            static_cast<void>(CloseDescriptor(&descriptor, nullptr));
            if (release_bytes_on_close != 0U) {
                reservoir->bytes.fetch_sub(release_bytes_on_close,
                                           std::memory_order_relaxed);
            }
        }

        SegmentFile(const SegmentFile&) = delete;
        SegmentFile& operator=(const SegmentFile&) = delete;

        std::shared_ptr<ReservoirAccounting> reservoir;
        // Reclaimed files are unlinked while an in-flight pread may still
        // hold this object. Their blocks remain charged until the final lease
        // closes the descriptor and the filesystem can release those blocks.
        std::uint64_t release_bytes_on_close = 0U;
        int descriptor = -1;
    };

    struct Segment final {
        std::filesystem::path path;
        std::uint64_t sequence = 0U;
        std::uint64_t first_lsn = 0U;
        std::uint64_t last_lsn = 0U;
        std::uint64_t bytes = 0U;
        std::shared_ptr<SegmentFile> file;
        bool closed = false;
    };

    struct RetiredSegment final {
        std::filesystem::path path;
        std::shared_ptr<SegmentFile> file;
        std::uint64_t bytes = 0U;
    };

    struct BatchReadLease final {
        // Both values are immutable after publication. The shared file handle
        // keeps pread valid even if cursor progress concurrently reclaims and
        // unlinks the segment.
        BatchIndex index{};
        std::shared_ptr<SegmentFile> file;
    };

    struct BatchLoadState final {
        std::mutex mutex;
        std::condition_variable wake;
        std::shared_ptr<const CachedBatch> batch;
        std::exception_ptr failure;
        bool ready = false;
    };

    struct ColdReadPermit final {
        explicit ColdReadPermit(
            std::counting_semaphore<kColdReadConcurrency>* permits) noexcept
            : slots(permits) {
            slots->acquire();
        }

        ~ColdReadPermit() { slots->release(); }

        ColdReadPermit(const ColdReadPermit&) = delete;
        ColdReadPermit& operator=(const ColdReadPermit&) = delete;

        std::counting_semaphore<kColdReadConcurrency>* slots;
    };

    struct Cursor final {
        WalPosition contiguous{};
        std::map<std::uint64_t, WalPosition> completed;
        int descriptor = -1;
        bool enabled = false;
        bool dirty = true;
    };

    explicit Impl(DurableOutboxConfig config)
        : config_(std::move(config)),
          source_instance_id_(IsZero(config_.source_instance_id)
                                  ? GenerateIdentifier()
                                  : config_.source_instance_id),
          run_id_(GenerateIdentifier()) {
        read_cache_.reserve(config_.read_cache_batches);
        cursors_[ConsumerIndex(ConsumerKind::kRaw)].enabled =
            config_.raw_consumer_enabled;
        cursors_[ConsumerIndex(ConsumerKind::kEvent)].enabled =
            config_.event_consumer_enabled;
        cursors_[ConsumerIndex(ConsumerKind::kKLine)].enabled =
            config_.kline_consumer_enabled;
    }

    ~Impl() {
        std::string ignored;
        static_cast<void>(Stop(&ignored));
    }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "outbox can be started exactly once";
            }
            return false;
        }
        try {
            std::filesystem::create_directories(config_.root_directory);
            bool created = false;
            for (std::size_t attempt = 0U; attempt < 8U; ++attempt) {
                run_directory_ = config_.root_directory /
                    ("run-" + IdentifierHex(run_id_));
                if (::mkdir(run_directory_.c_str(), S_IRWXU | S_IRGRP |
                                                     S_IXGRP) == 0) {
                    created = true;
                    break;
                }
                if (errno != EEXIST) {
                    throw std::runtime_error(
                        std::string("outbox run directory creation failed: ") +
                        std::strerror(errno));
                }
                run_id_ = GenerateIdentifier();
            }
            if (!created) {
                throw std::runtime_error(
                    "outbox could not allocate a unique run directory");
            }
            OpenCursorFiles();
            CheckpointCursors(true);
            accepting_.store(true, std::memory_order_release);
            writer_ = std::thread([this] { WriterLoop(); });
        } catch (const std::exception& exception) {
            SetFatal(std::string("outbox start failed: ") + exception.what());
            accepting_.store(false, std::memory_order_release);
            stopping_.store(true, std::memory_order_release);
            CloseFiles();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool Enqueue(const CanonicalRecord& record) noexcept {
        if (!IsRecordValid(record) ||
            !RecordMatchesFeedEpoch(
                record, config_.feed_session_epoch)) {
            SetFatal("outbox rejected a canonical record outside its run");
            return false;
        }
        if (!accepting_.load(std::memory_order_acquire) || !healthy()) {
            return false;
        }
        try {
            auto owned = std::make_shared<CanonicalRecord>(record);
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                if (queue_.size() >= config_.producer_queue_records) {
                    SetFatal("outbox producer queue capacity exhausted");
                    return false;
                }
                queue_.push_back(std::move(owned));
                ++accepted_count_;
                queued_records_.store(queue_.size(),
                                      std::memory_order_relaxed);
            }
            records_accepted_.fetch_add(1U, std::memory_order_relaxed);
            queue_wake_.notify_one();
            return true;
        } catch (...) {
            SetFatal("outbox producer allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool Flush(std::string* error) noexcept {
        std::uint64_t target = 0U;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            target = accepted_count_;
            flush_target_ = std::max(flush_target_, target);
        }
        queue_wake_.notify_one();
        std::unique_lock<std::mutex> lock(flush_mutex_);
        flush_wake_.wait(lock, [this, target] {
            return durable_count_.load(std::memory_order_acquire) >= target ||
                   !healthy();
        });
        if (!healthy()) {
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        try {
            CheckpointCursors(true);
        } catch (const std::exception& exception) {
            SetFatal(std::string("outbox cursor flush failed: ") +
                     exception.what());
        }
        if (error != nullptr) {
            *error = healthy() ? std::string{} : fatal_error();
        }
        return healthy();
    }

    [[nodiscard]] bool Stop(std::string* error) noexcept {
        if (!started_.load(std::memory_order_acquire)) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        bool expected = false;
        if (!stop_called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            JoinWriter();
            if (error != nullptr) {
                *error = healthy() ? std::string{} : fatal_error();
            }
            return healthy();
        }
        accepting_.store(false, std::memory_order_release);
        const bool flushed = Flush(error);
        stopping_.store(true, std::memory_order_release);
        queue_wake_.notify_all();
        JoinWriter();
        try {
            CheckpointCursors(true);
        } catch (const std::exception& exception) {
            SetFatal(std::string("outbox final cursor checkpoint failed: ") +
                     exception.what());
        }
        CloseFiles();
        if (error != nullptr) {
            *error = healthy() ? std::string{} : fatal_error();
        }
        return flushed && healthy();
    }

    [[nodiscard]] bool TryRead(std::uint64_t lsn,
                               RecordView* output) noexcept {
        if (output == nullptr || lsn == 0U) {
            return false;
        }
        try {
            std::shared_ptr<const CachedBatch> cached;
            std::shared_ptr<BatchLoadState> load;
            BatchReadLease lease{};
            bool load_owner = false;
            {
                std::lock_guard<std::mutex> lock(data_mutex_);
                const CachedBatch* const hot =
                    CachedBatchForLsnLocked(lsn);
                if (hot != nullptr) {
                    CopyRecordView(*hot, lsn, output);
                    return true;
                }
                const BatchIndex* const batch = BatchForLsnLocked(lsn);
                if (batch == nullptr) {
                    return false;
                }
                const auto existing = inflight_loads_.find(batch->sequence);
                if (existing != inflight_loads_.end()) {
                    load = existing->second;
                    coalesced_cold_read_waits_.fetch_add(
                        1U, std::memory_order_relaxed);
                } else {
                    Segment* const segment = SegmentBySequenceLocked(
                        batch->segment_sequence);
                    if (segment == nullptr || segment->file == nullptr ||
                        segment->file->descriptor < 0) {
                        throw std::runtime_error(
                            "WAL batch references an unavailable segment");
                    }
                    load = std::make_shared<BatchLoadState>();
                    inflight_loads_.emplace(batch->sequence, load);
                    lease.index = *batch;
                    lease.file = segment->file;
                    load_owner = true;
                }
            }

            if (load_owner) {
                try {
                    ColdReadPermit permit(&cold_read_slots_);
                    const std::uint64_t started_ns =
                        ingest::MonotonicNowNs();
                    cached = LoadBatch(lease);
                    const std::uint64_t finished_ns =
                        ingest::MonotonicNowNs();
                    const std::uint64_t elapsed_ns =
                        finished_ns >= started_ns
                        ? finished_ns - started_ns
                        : 0U;
                    cold_read_batches_.fetch_add(
                        1U, std::memory_order_relaxed);
                    cold_read_bytes_.fetch_add(
                        lease.index.frame_bytes, std::memory_order_relaxed);
                    cold_read_ns_.fetch_add(
                        elapsed_ns, std::memory_order_relaxed);
                    UpdateMaximum(&maximum_cold_read_ns_, elapsed_ns);
                    FinishBatchLoad(lease, load, cached);
                } catch (...) {
                    FailBatchLoad(lease.index.sequence, load,
                                  std::current_exception());
                    throw;
                }
            } else {
                cached = WaitForBatchLoad(load);
            }
            CopyRecordView(*cached, lsn, output);
            return true;
        } catch (const std::exception& exception) {
            SetFatal(std::string("WAL read failed: ") + exception.what());
            return false;
        } catch (...) {
            SetFatal("WAL read failed with unknown exception");
            return false;
        }
    }

    [[nodiscard]] bool Complete(
        ConsumerKind consumer,
        std::span<const WalPosition> positions) noexcept {
        const std::size_t index = ConsumerIndex(consumer);
        if (index >= cursors_.size() || !healthy()) {
            return false;
        }
        try {
            std::lock_guard<std::mutex> lock(data_mutex_);
            Cursor& cursor = cursors_[index];
            if (!cursor.enabled) {
                return positions.empty();
            }
            for (const WalPosition position : positions) {
                if (position.lsn == 0U ||
                    position.lsn > durable_tail_.lsn) {
                    SetFatal("consumer completed a non-durable WAL position");
                    return false;
                }
                if (position.lsn <= cursor.contiguous.lsn) {
                    continue;
                }
                if (!MatchesPositionLocked(position)) {
                    SetFatal("consumer WAL position metadata mismatched");
                    return false;
                }
                cursor.completed.emplace(position.lsn, position);
            }
            for (;;) {
                const std::uint64_t next = cursor.contiguous.lsn + 1U;
                const auto completed = cursor.completed.find(next);
                if (completed == cursor.completed.end()) {
                    break;
                }
                cursor.contiguous = completed->second;
                cursor.completed.erase(completed);
                cursor.dirty = true;
            }
            data_changed_.store(true, std::memory_order_release);
            queue_wake_.notify_one();
            return true;
        } catch (...) {
            SetFatal("consumer completion tracking allocation failed");
            return false;
        }
    }

    [[nodiscard]] WalPosition durable_tail() const noexcept {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return durable_tail_;
    }

    [[nodiscard]] std::uint64_t oldest_resident_lsn() const noexcept {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return batches_.empty()
            ? (durable_tail_.lsn == std::numeric_limits<std::uint64_t>::max()
                   ? durable_tail_.lsn
                   : durable_tail_.lsn + 1U)
            : batches_.front().first_lsn;
    }

    [[nodiscard]] WalPosition consumer_cursor(
        ConsumerKind consumer) const noexcept {
        const std::size_t index = ConsumerIndex(consumer);
        if (index >= cursors_.size()) {
            return {};
        }
        std::lock_guard<std::mutex> lock(data_mutex_);
        return cursors_[index].contiguous;
    }

    [[nodiscard]] bool consumer_enabled(ConsumerKind consumer) const noexcept {
        const std::size_t index = ConsumerIndex(consumer);
        if (index >= cursors_.size()) {
            return false;
        }
        std::lock_guard<std::mutex> lock(data_mutex_);
        return cursors_[index].enabled;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] DurableOutboxStats stats() const noexcept {
        DurableOutboxStats result{};
        result.records_accepted = records_accepted_.load(
            std::memory_order_relaxed);
        result.records_durable = records_durable_.load(
            std::memory_order_relaxed);
        result.batches_durable = batches_durable_.load(
            std::memory_order_relaxed);
        result.bytes_durable = bytes_durable_.load(
            std::memory_order_relaxed);
        result.segments_created = segments_created_.load(
            std::memory_order_relaxed);
        result.segments_reclaimed = segments_reclaimed_.load(
            std::memory_order_relaxed);
        result.queued_records = queued_records_.load(
            std::memory_order_relaxed);
        result.reservoir_bytes = reservoir_accounting_->bytes.load(
            std::memory_order_relaxed);
        result.cold_read_batches = cold_read_batches_.load(
            std::memory_order_relaxed);
        result.cold_read_bytes = cold_read_bytes_.load(
            std::memory_order_relaxed);
        result.coalesced_cold_read_waits = coalesced_cold_read_waits_.load(
            std::memory_order_relaxed);
        result.cold_read_ns = cold_read_ns_.load(std::memory_order_relaxed);
        result.maximum_cold_read_ns = maximum_cold_read_ns_.load(
            std::memory_order_relaxed);
        result.maximum_durable_publish_wait_ns =
            maximum_durable_publish_wait_ns_.load(std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(data_mutex_);
        result.indexed_records = retained_records_;
        result.indexed_batches =
            static_cast<std::uint64_t>(batches_.size());
        for (const std::shared_ptr<const CachedBatch>& cached : read_cache_) {
            result.cached_records +=
                static_cast<std::uint64_t>(cached->records.size());
        }
        result.durable_tail = durable_tail_;
        result.latest_barrier_position = latest_barrier_position_;
        result.latest_barrier = latest_barrier_;
        for (std::size_t index = 0U; index < cursors_.size(); ++index) {
            const Cursor& cursor = cursors_[index];
            result.consumers[index].contiguous = cursor.contiguous;
            result.consumers[index].out_of_order_completions =
                cursor.completed.size();
            result.consumers[index].lag_records =
                durable_tail_.lsn >= cursor.contiguous.lsn
                    ? durable_tail_.lsn - cursor.contiguous.lsn
                    : 0U;
            result.consumers[index].enabled = cursor.enabled;
        }
        return result;
    }

    [[nodiscard]] Identifier128 run_id() const noexcept { return run_id_; }

    [[nodiscard]] Identifier128 source_instance_id() const noexcept {
        return source_instance_id_;
    }

    [[nodiscard]] const std::filesystem::path& run_directory() const noexcept {
        return run_directory_;
    }

    [[nodiscard]] const DurableOutboxConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] const BatchIndex* BatchBySequenceLocked(
        std::uint64_t sequence) const noexcept {
        if (batches_.empty() || sequence < batches_.front().sequence) {
            return nullptr;
        }
        const std::uint64_t relative =
            sequence - batches_.front().sequence;
        if (relative >= batches_.size()) {
            return nullptr;
        }
        const BatchIndex& batch =
            batches_[static_cast<std::size_t>(relative)];
        return batch.sequence == sequence ? &batch : nullptr;
    }

    [[nodiscard]] const BatchIndex* BatchForLsnLocked(
        std::uint64_t lsn) const noexcept {
        if (batches_.empty() || lsn < batches_.front().first_lsn ||
            lsn > batches_.back().last_lsn()) {
            return nullptr;
        }
        auto upper = std::upper_bound(
            batches_.begin(), batches_.end(), lsn,
            [](std::uint64_t target, const BatchIndex& batch) {
                return target < batch.first_lsn;
            });
        if (upper == batches_.begin()) {
            return nullptr;
        }
        --upper;
        return lsn <= upper->last_lsn() ? std::addressof(*upper) : nullptr;
    }

    [[nodiscard]] bool MatchesPositionLocked(
        WalPosition position) const noexcept {
        const BatchIndex* const batch =
            BatchBySequenceLocked(position.batch_sequence);
        return batch != nullptr && position.row_index < batch->row_count &&
               batch->first_lsn + position.row_index == position.lsn;
    }

    [[nodiscard]] Segment* SegmentBySequenceLocked(
        std::uint64_t sequence) noexcept {
        if (segments_.empty() || sequence < segments_.front().sequence) {
            return nullptr;
        }
        const std::uint64_t relative =
            sequence - segments_.front().sequence;
        if (relative >= segments_.size()) {
            return nullptr;
        }
        Segment& segment = segments_[static_cast<std::size_t>(relative)];
        return segment.sequence == sequence ? &segment : nullptr;
    }

    [[nodiscard]] const CachedBatch*
    PromoteCachedBatchLocked(std::size_t index) noexcept {
        std::shared_ptr<const CachedBatch> promoted =
            std::move(read_cache_[index]);
        for (std::size_t current = index;
             current + 1U < read_cache_.size(); ++current) {
            read_cache_[current] = std::move(read_cache_[current + 1U]);
        }
        read_cache_.back() = std::move(promoted);
        return read_cache_.back().get();
    }

    [[nodiscard]] const CachedBatch* CachedBatchForLsnLocked(
        std::uint64_t lsn) noexcept {
        for (std::size_t index = 0U; index < read_cache_.size(); ++index) {
            const std::shared_ptr<const CachedBatch>& cached =
                read_cache_[index];
            if (lsn < cached->first_lsn || lsn > cached->last_lsn()) {
                continue;
            }
            return index + 1U == read_cache_.size()
                ? cached.get()
                : PromoteCachedBatchLocked(index);
        }
        return nullptr;
    }

    [[nodiscard]] bool CachedBatchBySequenceLocked(
        std::uint64_t sequence) noexcept {
        for (std::size_t index = 0U; index < read_cache_.size(); ++index) {
            if (read_cache_[index]->sequence != sequence) {
                continue;
            }
            if (index + 1U != read_cache_.size()) {
                static_cast<void>(PromoteCachedBatchLocked(index));
            }
            return true;
        }
        return false;
    }

    [[nodiscard]] std::shared_ptr<const CachedBatch>
    InsertCachedBatchLocked(
        const std::shared_ptr<const CachedBatch>& decoded) noexcept {
        if (CachedBatchBySequenceLocked(decoded->sequence)) {
            return nullptr;
        }
        if (read_cache_.size() < config_.read_cache_batches) {
            read_cache_.push_back(decoded);
            return nullptr;
        }
        std::shared_ptr<const CachedBatch> retired =
            std::move(read_cache_.front());
        for (std::size_t index = 0U; index + 1U < read_cache_.size(); ++index) {
            read_cache_[index] = std::move(read_cache_[index + 1U]);
        }
        read_cache_.back() = decoded;
        return retired;
    }

    void RetireCachedBatchesThroughLocked(
        std::uint64_t last_lsn,
        std::vector<std::shared_ptr<const CachedBatch>>* retired) {
        std::size_t retained = 0U;
        for (std::size_t index = 0U; index < read_cache_.size(); ++index) {
            if (read_cache_[index]->last_lsn() <= last_lsn) {
                retired->push_back(std::move(read_cache_[index]));
                continue;
            }
            if (retained != index) {
                read_cache_[retained] = std::move(read_cache_[index]);
            }
            ++retained;
        }
        read_cache_.resize(retained);
    }

    static void UpdateMaximum(std::atomic<std::uint64_t>* maximum,
                              std::uint64_t value) noexcept {
        std::uint64_t observed = maximum->load(std::memory_order_relaxed);
        while (observed < value &&
               !maximum->compare_exchange_weak(
                   observed, value, std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }
    }

    [[nodiscard]] static bool SameBatchIndex(
        const BatchIndex& left,
        const BatchIndex& right) noexcept {
        return left.first_lsn == right.first_lsn &&
               left.sequence == right.sequence &&
               left.segment_sequence == right.segment_sequence &&
               left.offset == right.offset &&
               left.frame_bytes == right.frame_bytes &&
               left.row_count == right.row_count &&
               left.payload_checksum == right.payload_checksum &&
               left.batch_id == right.batch_id;
    }

    [[nodiscard]] bool BatchReadLeaseStillResidentLocked(
        const BatchReadLease& lease) noexcept {
        const BatchIndex* const resident =
            BatchBySequenceLocked(lease.index.sequence);
        Segment* const segment = SegmentBySequenceLocked(
            lease.index.segment_sequence);
        return resident != nullptr && SameBatchIndex(*resident, lease.index) &&
               segment != nullptr && segment->file == lease.file;
    }

    static void CopyRecordView(const CachedBatch& cached,
                               std::uint64_t lsn,
                               RecordView* output) {
        if (lsn < cached.first_lsn) {
            throw std::runtime_error("WAL cache position preceded its batch");
        }
        const std::uint64_t relative = lsn - cached.first_lsn;
        if (relative >= cached.records.size()) {
            throw std::runtime_error(
                "WAL cache row offset exceeded its batch");
        }
        const StoredRecord& stored =
            cached.records[static_cast<std::size_t>(relative)];
        if (stored.position.lsn != lsn ||
            stored.position.batch_sequence != cached.sequence ||
            stored.position.row_index != relative) {
            throw std::runtime_error("WAL cache position lost contiguity");
        }
        output->position = stored.position;
        output->batch_id = stored.batch_id;
        output->payload_checksum = stored.payload_checksum;
        output->record = stored.record;
    }

    [[nodiscard]] static std::shared_ptr<const CachedBatch>
    WaitForBatchLoad(const std::shared_ptr<BatchLoadState>& load) {
        std::unique_lock<std::mutex> lock(load->mutex);
        load->wake.wait(lock, [&load] { return load->ready; });
        if (load->failure != nullptr) {
            std::rethrow_exception(load->failure);
        }
        if (load->batch == nullptr) {
            throw std::runtime_error(
                "WAL batch load completed without a decoded batch");
        }
        return load->batch;
    }

    void FinishBatchLoad(
        const BatchReadLease& lease,
        const std::shared_ptr<BatchLoadState>& load,
        const std::shared_ptr<const CachedBatch>& decoded) {
        std::shared_ptr<const CachedBatch> retired;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            if (BatchReadLeaseStillResidentLocked(lease)) {
                retired = InsertCachedBatchLocked(decoded);
            }
            {
                std::lock_guard<std::mutex> state_lock(load->mutex);
                load->batch = decoded;
                load->ready = true;
            }
            const auto inflight = inflight_loads_.find(lease.index.sequence);
            if (inflight != inflight_loads_.end() &&
                inflight->second == load) {
                inflight_loads_.erase(inflight);
            }
        }
        load->wake.notify_all();
    }

    void FailBatchLoad(std::uint64_t sequence,
                       const std::shared_ptr<BatchLoadState>& load,
                       std::exception_ptr failure) noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(data_mutex_);
                {
                    std::lock_guard<std::mutex> state_lock(load->mutex);
                    if (!load->ready) {
                        load->failure = std::move(failure);
                        load->ready = true;
                    }
                }
                const auto inflight = inflight_loads_.find(sequence);
                if (inflight != inflight_loads_.end() &&
                    inflight->second == load) {
                    inflight_loads_.erase(inflight);
                }
            }
            load->wake.notify_all();
        } catch (...) {
            load->wake.notify_all();
        }
    }

    [[nodiscard]] std::shared_ptr<const CachedBatch> LoadBatch(
        const BatchReadLease& lease) {
        const BatchIndex& index = lease.index;
        if (lease.file == nullptr || lease.file->descriptor < 0) {
            throw std::runtime_error(
                "WAL batch references an unavailable segment");
        }
        if (index.frame_bytes < kBatchHeaderBytes + kBatchFooterBytes ||
            index.frame_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
            throw std::runtime_error("WAL batch frame size is invalid");
        }
        std::vector<std::byte> frame(
            static_cast<std::size_t>(index.frame_bytes));
        std::string error;
        if (!ReadAllAt(lease.file->descriptor, frame, index.offset, &error)) {
            throw std::runtime_error(error);
        }

        LittleEndianReader checksum_reader(
            std::span<const std::byte>(frame).last(sizeof(std::uint32_t)));
        std::uint32_t stored_frame_checksum = 0U;
        if (!checksum_reader.Read(&stored_frame_checksum) ||
            !checksum_reader.complete() ||
            checksum::Crc32c(std::span<const std::byte>(frame).first(
                frame.size() - sizeof(std::uint32_t))) !=
                stored_frame_checksum) {
            throw std::runtime_error("WAL batch frame checksum mismatched");
        }

        LittleEndianReader reader(frame);
        std::array<std::byte, kBatchMagic.size()> magic{};
        std::array<std::byte, kFooterMagic.size()> footer_magic{};
        Identifier128 source{};
        Identifier128 run{};
        Identifier128 batch_id{};
        std::uint32_t format = 0U;
        std::uint64_t feed_epoch = 0U;
        std::uint64_t sequence = 0U;
        std::uint64_t first_lsn = 0U;
        std::uint32_t row_count = 0U;
        std::uint64_t payload_bytes = 0U;
        std::uint32_t payload_checksum = 0U;
        std::span<const std::byte> payload;
        std::uint64_t footer_sequence = 0U;
        std::uint32_t footer_rows = 0U;
        std::uint32_t footer_payload_checksum = 0U;
        std::uint32_t footer_frame_checksum = 0U;
        if (!reader.ReadBytes(magic) || magic != kBatchMagic ||
            !reader.Read(&format) || !reader.ReadBytes(source.bytes) ||
            !reader.ReadBytes(run.bytes) || !reader.Read(&feed_epoch) ||
            !reader.Read(&sequence) || !reader.Read(&first_lsn) ||
            !reader.Read(&row_count) || !reader.Read(&payload_bytes) ||
            !reader.Read(&payload_checksum) ||
            !reader.ReadBytes(batch_id.bytes) ||
            payload_bytes >
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
            !reader.ReadSpan(static_cast<std::size_t>(payload_bytes),
                             &payload) ||
            !reader.ReadBytes(footer_magic) ||
            footer_magic != kFooterMagic ||
            !reader.Read(&footer_sequence) || !reader.Read(&footer_rows) ||
            !reader.Read(&footer_payload_checksum) ||
            !reader.Read(&footer_frame_checksum) || !reader.complete()) {
            throw std::runtime_error("WAL batch frame structure is invalid");
        }
        if (format != kWalFormatVersion || source != source_instance_id_ ||
            run != run_id_ || feed_epoch != config_.feed_session_epoch ||
            sequence != index.sequence || first_lsn != index.first_lsn ||
            row_count != index.row_count ||
            payload_checksum != index.payload_checksum ||
            batch_id != index.batch_id || footer_sequence != sequence ||
            footer_rows != row_count ||
            footer_payload_checksum != payload_checksum ||
            footer_frame_checksum != stored_frame_checksum ||
            checksum::Crc32c(payload) != payload_checksum) {
            throw std::runtime_error("WAL batch metadata mismatched");
        }

        auto decoded = std::make_shared<CachedBatch>();
        decoded->sequence = sequence;
        decoded->first_lsn = first_lsn;
        decoded->records.reserve(row_count);
        LittleEndianReader payload_reader(payload);
        for (std::uint32_t row = 0U; row < row_count; ++row) {
            std::uint32_t record_bytes = 0U;
            std::uint32_t record_checksum = 0U;
            std::span<const std::byte> encoded;
            if (!payload_reader.Read(&record_bytes) ||
                !payload_reader.Read(&record_checksum) ||
                !payload_reader.ReadSpan(record_bytes, &encoded) ||
                checksum::Crc32c(encoded) != record_checksum) {
                throw std::runtime_error(
                    "WAL record frame checksum mismatched");
            }
            std::shared_ptr<const CanonicalRecord> record;
            if (!DeserializeRecord(encoded, &record) ||
                !RecordMatchesFeedEpoch(
                    *record, config_.feed_session_epoch)) {
                throw std::runtime_error("WAL record payload is invalid");
            }
            decoded->records.push_back(StoredRecord{
                WalPosition{first_lsn + row, sequence, row}, batch_id,
                record_checksum, std::move(record)});
        }
        if (!payload_reader.complete()) {
            throw std::runtime_error("WAL batch payload has trailing bytes");
        }
        return decoded;
    }

    void SetFatal(std::string message) noexcept {
        accepting_.store(false, std::memory_order_release);
        if (!fatal_claimed_.test_and_set(std::memory_order_acq_rel)) {
            try {
                std::lock_guard<std::mutex> lock(fatal_mutex_);
                fatal_error_ = std::move(message);
            } catch (...) {
            }
            healthy_.store(false, std::memory_order_release);
        }
        queue_wake_.notify_all();
        flush_wake_.notify_all();
    }

    void OpenCursorFiles() {
        for (std::size_t index = 0U; index < cursors_.size(); ++index) {
            const auto consumer = static_cast<ConsumerKind>(index);
            const std::filesystem::path path = run_directory_ /
                (std::string("cursor-") + ConsumerName(consumer) + ".bin");
            const int descriptor = ::open(
                path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                S_IRUSR | S_IWUSR | S_IRGRP);
            if (descriptor < 0) {
                throw std::runtime_error(
                    std::string("cursor file creation failed: ") +
                    std::strerror(errno));
            }
            cursors_[index].descriptor = descriptor;
        }
    }

    [[nodiscard]] std::vector<std::byte> CursorBytes(
        ConsumerKind consumer,
        const Cursor& cursor) const {
        std::vector<std::byte> bytes;
        bytes.reserve(96U);
        AppendBytes(&bytes, kCursorMagic);
        AppendLe(&bytes, kWalFormatVersion);
        AppendLe(&bytes, consumer);
        AppendLe(&bytes,
                 static_cast<std::uint8_t>(cursor.enabled ? 1U : 0U));
        AppendLe(&bytes, static_cast<std::uint16_t>(0U));
        AppendBytes(&bytes, source_instance_id_.bytes);
        AppendBytes(&bytes, run_id_.bytes);
        AppendLe(&bytes, config_.feed_session_epoch);
        AppendLe(&bytes, cursor.contiguous.lsn);
        AppendLe(&bytes, cursor.contiguous.batch_sequence);
        AppendLe(&bytes, cursor.contiguous.row_index);
        const std::uint32_t checksum = checksum::Crc32c(bytes);
        AppendLe(&bytes, checksum);
        return bytes;
    }

    void CheckpointCursors(bool force) {
        std::lock_guard<std::mutex> checkpoint_lock(checkpoint_mutex_);
        std::array<std::vector<std::byte>, 3U> checkpoints;
        std::array<int, 3U> descriptors{};
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            for (std::size_t index = 0U; index < cursors_.size(); ++index) {
                Cursor& cursor = cursors_[index];
                descriptors[index] = cursor.descriptor;
                if (force || cursor.dirty) {
                    checkpoints[index] = CursorBytes(
                        static_cast<ConsumerKind>(index), cursor);
                    cursor.dirty = false;
                }
            }
        }
        for (std::size_t index = 0U; index < checkpoints.size(); ++index) {
            if (checkpoints[index].empty()) {
                continue;
            }
            std::string error;
            if (!WriteAllAt(descriptors[index], checkpoints[index], 0U,
                            &error) ||
                !SyncDescriptor(descriptors[index], &error)) {
                throw std::runtime_error(error);
            }
        }
        last_cursor_checkpoint_ns_.store(
            ingest::MonotonicNowNs(), std::memory_order_release);
    }

    [[nodiscard]] std::vector<std::byte> SegmentHeaderBytes(
        std::uint64_t segment_sequence,
        std::uint64_t first_lsn) const {
        std::vector<std::byte> bytes;
        bytes.reserve(80U);
        AppendBytes(&bytes, kSegmentMagic);
        AppendLe(&bytes, kWalFormatVersion);
        AppendBytes(&bytes, source_instance_id_.bytes);
        AppendBytes(&bytes, run_id_.bytes);
        AppendLe(&bytes, config_.feed_session_epoch);
        AppendLe(&bytes, segment_sequence);
        AppendLe(&bytes, first_lsn);
        AppendLe(&bytes, UtcNowNs());
        const std::uint32_t checksum = checksum::Crc32c(bytes);
        AppendLe(&bytes, checksum);
        return bytes;
    }

    void OpenSegment(std::uint64_t first_lsn) {
        ++next_segment_sequence_;
        std::array<char, 64U> name{};
        const int count = std::snprintf(
            name.data(), name.size(), "segment-%020llu.wal",
            static_cast<unsigned long long>(next_segment_sequence_));
        if (count <= 0 || static_cast<std::size_t>(count) >= name.size()) {
            throw std::runtime_error("WAL segment name formatting failed");
        }
        Segment segment{};
        segment.path = run_directory_ / name.data();
        segment.sequence = next_segment_sequence_;
        segment.first_lsn = first_lsn;
        std::shared_ptr<SegmentFile> file =
            std::make_shared<SegmentFile>(reservoir_accounting_);
        const int descriptor = ::open(
            segment.path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            S_IRUSR | S_IWUSR | S_IRGRP);
        if (descriptor < 0) {
            throw std::runtime_error(
                std::string("WAL segment creation failed: ") +
                std::strerror(errno));
        }
        file->descriptor = descriptor;
        const std::vector<std::byte> header = SegmentHeaderBytes(
            segment.sequence, first_lsn);
        std::string error;
        if (!WriteAllAt(descriptor, header, 0U, &error) ||
            !SyncDescriptor(descriptor, &error)) {
            throw std::runtime_error(error);
        }
        segment.bytes = header.size();
        segment.file = std::move(file);
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            const std::uint64_t reservoir_bytes =
                reservoir_accounting_->bytes.load(std::memory_order_relaxed);
            if (reservoir_bytes >
                config_.maximum_reservoir_bytes - segment.bytes) {
                throw std::runtime_error(
                    "outbox reservoir exhausted by a segment header");
            }
            reservoir_accounting_->bytes.fetch_add(
                segment.bytes, std::memory_order_relaxed);
            segments_.push_back(std::move(segment));
            current_segment_descriptor_ = descriptor;
        }
        segments_created_.fetch_add(1U, std::memory_order_relaxed);
    }

    void CloseCurrentSegment() {
        if (current_segment_descriptor_ < 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (segments_.empty() ||
            segments_.back().file == nullptr ||
            segments_.back().file->descriptor != current_segment_descriptor_) {
            throw std::runtime_error(
                "outbox active segment descriptor diverged");
        }
        segments_.back().closed = true;
        current_segment_descriptor_ = -1;
    }

    [[nodiscard]] std::uint64_t MinimumCursorLocked() const noexcept {
        std::uint64_t minimum = durable_tail_.lsn;
        bool found = false;
        for (const Cursor& cursor : cursors_) {
            if (!cursor.enabled) {
                continue;
            }
            minimum = found ? std::min(minimum, cursor.contiguous.lsn)
                            : cursor.contiguous.lsn;
            found = true;
        }
        return found ? minimum : durable_tail_.lsn;
    }

    void Reclaim() {
        std::vector<RetiredSegment> retired_segments;
        retired_segments.reserve(segments_.size());
        std::vector<std::shared_ptr<const CachedBatch>> retired_cache;
        retired_cache.reserve(config_.read_cache_batches);
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            const std::uint64_t minimum = MinimumCursorLocked();
            while (!segments_.empty() && segments_.front().closed &&
                   segments_.front().last_lsn != 0U &&
                   segments_.front().last_lsn <= minimum) {
                Segment& segment = segments_.front();
                if (segment.bytes > reservoir_accounting_->bytes.load(
                                        std::memory_order_relaxed)) {
                    throw std::runtime_error(
                        "outbox segment byte accounting diverged");
                }
                const std::uint64_t reclaimed_last_lsn = segment.last_lsn;
                while (!batches_.empty() &&
                       batches_.front().segment_sequence ==
                           segment.sequence) {
                    const std::uint64_t row_count =
                        batches_.front().row_count;
                    if (row_count > retained_records_) {
                        throw std::runtime_error(
                            "outbox retained record accounting diverged");
                    }
                    retained_records_ -= row_count;
                    batches_.pop_front();
                }
                RetireCachedBatchesThroughLocked(reclaimed_last_lsn,
                                                 &retired_cache);
                retired_segments.push_back(RetiredSegment{
                    std::move(segment.path), std::move(segment.file),
                    segment.bytes});
                segments_.pop_front();
            }
        }
        for (RetiredSegment& segment : retired_segments) {
            std::error_code error;
            const bool removed = std::filesystem::remove(segment.path, error);
            if (!removed || error) {
                throw std::runtime_error(
                    "outbox could not reclaim completed WAL segment: " +
                    error.message());
            }
            if (segment.file == nullptr ||
                segment.file->release_bytes_on_close != 0U) {
                throw std::runtime_error(
                    "outbox reclaimed segment file accounting diverged");
            }
            segment.file->release_bytes_on_close = segment.bytes;
            segment.file.reset();
            segments_reclaimed_.fetch_add(1U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] std::vector<std::byte> BuildBatchBytes(
        const std::vector<PendingRecord>& batch,
        std::uint64_t batch_sequence,
        std::uint64_t first_lsn,
        Identifier128* batch_id,
        std::uint32_t* batch_checksum) const {
        std::vector<std::byte> payload;
        std::size_t payload_size = 0U;
        for (const PendingRecord& record : batch) {
            payload_size += sizeof(std::uint32_t) * 2U +
                            record.payload.size();
        }
        payload.reserve(payload_size);
        for (const PendingRecord& record : batch) {
            AppendLe(&payload,
                     static_cast<std::uint32_t>(record.payload.size()));
            AppendLe(&payload, record.checksum);
            AppendBytes(&payload, record.payload);
        }
        *batch_checksum = checksum::Crc32c(payload);
        *batch_id = BatchIdentifier(run_id_, batch_sequence, *batch_checksum);

        std::vector<std::byte> bytes;
        bytes.reserve(128U + payload.size());
        AppendBytes(&bytes, kBatchMagic);
        AppendLe(&bytes, kWalFormatVersion);
        AppendBytes(&bytes, source_instance_id_.bytes);
        AppendBytes(&bytes, run_id_.bytes);
        AppendLe(&bytes, config_.feed_session_epoch);
        AppendLe(&bytes, batch_sequence);
        AppendLe(&bytes, first_lsn);
        AppendLe(&bytes, static_cast<std::uint32_t>(batch.size()));
        AppendLe(&bytes, static_cast<std::uint64_t>(payload.size()));
        AppendLe(&bytes, *batch_checksum);
        AppendBytes(&bytes, batch_id->bytes);
        AppendBytes(&bytes, payload);
        AppendBytes(&bytes, kFooterMagic);
        AppendLe(&bytes, batch_sequence);
        AppendLe(&bytes, static_cast<std::uint32_t>(batch.size()));
        AppendLe(&bytes, *batch_checksum);
        const std::uint32_t frame_checksum = checksum::Crc32c(bytes);
        AppendLe(&bytes, frame_checksum);
        return bytes;
    }

    void CommitBatch(std::vector<PendingRecord> batch) {
        if (batch.empty()) {
            return;
        }
        if (next_batch_sequence_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            next_lsn_ > std::numeric_limits<std::uint64_t>::max() -
                            batch.size()) {
            throw std::runtime_error("outbox WAL position space exhausted");
        }
        const std::uint64_t batch_sequence = next_batch_sequence_++;
        const std::uint64_t first_lsn = next_lsn_;
        Identifier128 batch_id{};
        std::uint32_t batch_checksum = 0U;
        const std::vector<std::byte> bytes = BuildBatchBytes(
            batch, batch_sequence, first_lsn, &batch_id, &batch_checksum);
        static_cast<void>(batch_checksum);

        Reclaim();
        bool rotate_segment = false;
        if (current_segment_descriptor_ >= 0) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            rotate_segment = !segments_.empty() &&
                segments_.back().bytes != 0U &&
                bytes.size() > config_.segment_max_bytes -
                                   std::min(config_.segment_max_bytes,
                                            segments_.back().bytes);
        }
        if (rotate_segment) {
            CloseCurrentSegment();
            Reclaim();
        }
        if (current_segment_descriptor_ < 0) {
            OpenSegment(first_lsn);
        }
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            const std::uint64_t reservoir_bytes =
                reservoir_accounting_->bytes.load(std::memory_order_relaxed);
            if (bytes.size() > config_.maximum_reservoir_bytes ||
                reservoir_bytes >
                    config_.maximum_reservoir_bytes - bytes.size()) {
                throw std::runtime_error(
                    "outbox durable reservoir capacity exhausted");
            }
        }
        std::uint64_t offset = 0U;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            if (segments_.empty()) {
                throw std::runtime_error("outbox has no active WAL segment");
            }
            offset = segments_.back().bytes;
        }
        std::string error;
        if (!WriteAllAt(current_segment_descriptor_, bytes, offset, &error) ||
            !SyncDescriptor(current_segment_descriptor_, &error)) {
            throw std::runtime_error(error);
        }

        const std::uint64_t publish_wait_started_ns =
            ingest::MonotonicNowNs();
        std::uint64_t publish_wait_ns = 0U;
        {
            std::unique_lock<std::mutex> lock(data_mutex_);
            const std::uint64_t publish_lock_acquired_ns =
                ingest::MonotonicNowNs();
            publish_wait_ns =
                publish_lock_acquired_ns >= publish_wait_started_ns
                ? publish_lock_acquired_ns - publish_wait_started_ns
                : 0U;
            if (!batches_.empty() &&
                (batches_.back().sequence + 1U != batch_sequence ||
                 batches_.back().last_lsn() + 1U != first_lsn)) {
                throw std::runtime_error(
                    "outbox durable batch index lost contiguity");
            }
            if (segments_.empty() ||
                segments_.back().file == nullptr ||
                segments_.back().file->descriptor !=
                    current_segment_descriptor_) {
                throw std::runtime_error(
                    "outbox has no writable WAL segment");
            }
            Segment& segment = segments_.back();
            batches_.push_back(BatchIndex{
                first_lsn,
                batch_sequence,
                segment.sequence,
                offset,
                static_cast<std::uint64_t>(bytes.size()),
                static_cast<std::uint32_t>(batch.size()),
                batch_checksum,
                batch_id});
            retained_records_ +=
                static_cast<std::uint64_t>(batch.size());
            for (std::size_t row = 0U; row < batch.size(); ++row) {
                const WalPosition position{
                    first_lsn + row, batch_sequence,
                    static_cast<std::uint32_t>(row)};
                if (batch[row].record->kind ==
                        RecordKind::kFreshnessBarrier ||
                    batch[row].record->kind == RecordKind::kFinalBarrier) {
                    latest_barrier_position_ = position;
                    latest_barrier_ = batch[row].record->barrier;
                }
                durable_tail_ = position;
            }
            segment.last_lsn = durable_tail_.lsn;
            segment.bytes += bytes.size();
            reservoir_accounting_->bytes.fetch_add(
                bytes.size(), std::memory_order_relaxed);
            for (Cursor& cursor : cursors_) {
                if (!cursor.enabled) {
                    cursor.contiguous = durable_tail_;
                    cursor.dirty = true;
                }
            }
        }
        UpdateMaximum(&maximum_durable_publish_wait_ns_, publish_wait_ns);
        next_lsn_ += batch.size();
        records_durable_.fetch_add(batch.size(), std::memory_order_relaxed);
        batches_durable_.fetch_add(1U, std::memory_order_relaxed);
        bytes_durable_.fetch_add(bytes.size(), std::memory_order_relaxed);
        durable_count_.fetch_add(batch.size(), std::memory_order_release);
        flush_wake_.notify_all();
    }

    [[nodiscard]] bool FlushRequestedLocked() const noexcept {
        return durable_count_.load(std::memory_order_acquire) < flush_target_;
    }

    void WriterLoop() noexcept {
        try {
            for (;;) {
                std::vector<std::shared_ptr<const CanonicalRecord>> owned;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);
                    queue_wake_.wait_for(
                        lock,
                        std::chrono::nanoseconds(
                            config_.cursor_checkpoint_interval_ns),
                        [this] {
                            return !queue_.empty() ||
                                   stopping_.load(std::memory_order_acquire) ||
                                   FlushRequestedLocked() ||
                                   data_changed_.load(
                                       std::memory_order_acquire);
                        });
                    if (!queue_.empty() && !FlushRequestedLocked() &&
                        queue_.size() < config_.commit_batch_records) {
                        queue_wake_.wait_for(
                            lock,
                            std::chrono::nanoseconds(
                                config_.commit_max_delay_ns),
                            [this] {
                                return queue_.size() >=
                                           config_.commit_batch_records ||
                                       stopping_.load(
                                           std::memory_order_acquire) ||
                                       FlushRequestedLocked();
                            });
                    }
                    const std::size_t count = std::min(
                        queue_.size(), config_.commit_batch_records);
                    owned.reserve(count);
                    for (std::size_t index = 0U; index < count; ++index) {
                        owned.push_back(std::move(queue_.front()));
                        queue_.pop_front();
                    }
                    queued_records_.store(queue_.size(),
                                          std::memory_order_relaxed);
                    if (owned.empty() &&
                        stopping_.load(std::memory_order_acquire)) {
                        break;
                    }
                }

                if (!owned.empty()) {
                    std::vector<PendingRecord> batch;
                    batch.reserve(owned.size());
                    std::size_t batch_bytes = 0U;
                    std::size_t consumed = 0U;
                    for (; consumed < owned.size(); ++consumed) {
                        PendingRecord pending{};
                        pending.record = owned[consumed];
                        pending.payload = SerializeRecord(*pending.record);
                        pending.checksum = checksum::Crc32c(pending.payload);
                        const std::size_t framed =
                            pending.payload.size() + 2U * sizeof(std::uint32_t);
                        if (!batch.empty() &&
                            framed > config_.commit_batch_bytes -
                                         std::min(config_.commit_batch_bytes,
                                                  batch_bytes)) {
                            break;
                        }
                        batch_bytes += framed;
                        batch.push_back(std::move(pending));
                    }
                    if (consumed < owned.size()) {
                        std::lock_guard<std::mutex> lock(queue_mutex_);
                        for (std::size_t index = owned.size();
                             index > consumed; --index) {
                            queue_.push_front(std::move(owned[index - 1U]));
                        }
                        queued_records_.store(queue_.size(),
                                              std::memory_order_relaxed);
                    }
                    CommitBatch(std::move(batch));
                }

                const std::uint64_t now = ingest::MonotonicNowNs();
                if (data_changed_.exchange(false,
                                           std::memory_order_acq_rel) ||
                    now - last_cursor_checkpoint_ns_.load(
                              std::memory_order_acquire) >=
                        config_.cursor_checkpoint_interval_ns) {
                    CheckpointCursors(false);
                    Reclaim();
                }
            }
            CheckpointCursors(true);
            CloseCurrentSegment();
        } catch (const std::exception& exception) {
            SetFatal(std::string("outbox writer failed: ") + exception.what());
        } catch (...) {
            SetFatal("outbox writer failed with unknown exception");
        }
        flush_wake_.notify_all();
    }

    void JoinWriter() noexcept {
        if (writer_.joinable()) {
            writer_.join();
        }
    }

    void CloseFiles() noexcept {
        std::deque<Segment> retired_segments;
        std::array<int, 3U> cursor_descriptors{-1, -1, -1};
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            current_segment_descriptor_ = -1;
            retired_segments.swap(segments_);
            for (std::size_t index = 0U; index < cursors_.size(); ++index) {
                cursor_descriptors[index] = cursors_[index].descriptor;
                cursors_[index].descriptor = -1;
            }
        }
        retired_segments.clear();
        for (int& descriptor : cursor_descriptors) {
            static_cast<void>(CloseDescriptor(&descriptor, nullptr));
        }
    }

    DurableOutboxConfig config_{};
    Identifier128 source_instance_id_{};
    Identifier128 run_id_{};
    std::filesystem::path run_directory_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_wake_;
    std::deque<std::shared_ptr<const CanonicalRecord>> queue_;
    std::uint64_t accepted_count_ = 0U;
    std::uint64_t flush_target_ = 0U;

    mutable std::mutex data_mutex_;
    std::condition_variable flush_wake_;
    std::mutex flush_mutex_;
    std::deque<BatchIndex> batches_;
    std::vector<std::shared_ptr<const CachedBatch>> read_cache_;
    std::map<std::uint64_t, std::shared_ptr<BatchLoadState>> inflight_loads_;
    std::counting_semaphore<kColdReadConcurrency> cold_read_slots_{
        kColdReadConcurrency};
    std::uint64_t retained_records_ = 0U;
    WalPosition durable_tail_{};
    WalPosition latest_barrier_position_{};
    FreshnessBarrier latest_barrier_{};
    std::array<Cursor, 3U> cursors_{};
    std::deque<Segment> segments_;
    std::shared_ptr<ReservoirAccounting> reservoir_accounting_ =
        std::make_shared<ReservoirAccounting>();

    std::thread writer_;
    int current_segment_descriptor_ = -1;
    std::uint64_t next_segment_sequence_ = 0U;
    std::uint64_t next_batch_sequence_ = 1U;
    std::uint64_t next_lsn_ = 1U;
    std::atomic<std::uint64_t> last_cursor_checkpoint_ns_{0U};
    std::mutex checkpoint_mutex_;

    std::atomic<std::uint64_t> durable_count_{0U};
    std::atomic<std::uint64_t> records_accepted_{0U};
    std::atomic<std::uint64_t> records_durable_{0U};
    std::atomic<std::uint64_t> batches_durable_{0U};
    std::atomic<std::uint64_t> bytes_durable_{0U};
    std::atomic<std::uint64_t> segments_created_{0U};
    std::atomic<std::uint64_t> segments_reclaimed_{0U};
    std::atomic<std::uint64_t> queued_records_{0U};
    std::atomic<std::uint64_t> cold_read_batches_{0U};
    std::atomic<std::uint64_t> cold_read_bytes_{0U};
    std::atomic<std::uint64_t> coalesced_cold_read_waits_{0U};
    std::atomic<std::uint64_t> cold_read_ns_{0U};
    std::atomic<std::uint64_t> maximum_cold_read_ns_{0U};
    std::atomic<std::uint64_t> maximum_durable_publish_wait_ns_{0U};
    std::atomic<bool> data_changed_{false};
    std::atomic<bool> started_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stop_called_{false};
    std::atomic<bool> healthy_{true};
    std::atomic_flag fatal_claimed_ = ATOMIC_FLAG_INIT;
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

std::unique_ptr<DurableOutbox> DurableOutbox::Create(
    DurableOutboxConfig config,
    std::string* error) {
    if (!ValidateDurableOutboxConfig(config, error)) {
        return nullptr;
    }
    try {
        return std::unique_ptr<DurableOutbox>(new DurableOutbox(
            std::make_unique<Impl>(std::move(config))));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("outbox creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

DurableOutbox::DurableOutbox(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DurableOutbox::~DurableOutbox() = default;

bool DurableOutbox::Start(std::string* error) { return impl_->Start(error); }

bool DurableOutbox::Flush(std::string* error) noexcept {
    return impl_->Flush(error);
}

bool DurableOutbox::Stop(std::string* error) noexcept {
    return impl_->Stop(error);
}

bool DurableOutbox::Enqueue(const CanonicalRecord& record) noexcept {
    return impl_->Enqueue(record);
}

bool DurableOutbox::TryRead(std::uint64_t lsn,
                            RecordView* output) const noexcept {
    return impl_->TryRead(lsn, output);
}

bool DurableOutbox::Complete(
    ConsumerKind consumer,
    std::span<const WalPosition> positions) noexcept {
    return impl_->Complete(consumer, positions);
}

bool DurableOutbox::CompleteOne(ConsumerKind consumer,
                                WalPosition position) noexcept {
    return impl_->Complete(
        consumer, std::span<const WalPosition>(&position, 1U));
}

WalPosition DurableOutbox::durable_tail() const noexcept {
    return impl_->durable_tail();
}

std::uint64_t DurableOutbox::oldest_resident_lsn() const noexcept {
    return impl_->oldest_resident_lsn();
}

WalPosition DurableOutbox::consumer_cursor(
    ConsumerKind consumer) const noexcept {
    return impl_->consumer_cursor(consumer);
}

bool DurableOutbox::consumer_enabled(ConsumerKind consumer) const noexcept {
    return impl_->consumer_enabled(consumer);
}

bool DurableOutbox::healthy() const noexcept { return impl_->healthy(); }

std::string DurableOutbox::fatal_error() const {
    return impl_->fatal_error();
}

DurableOutboxStats DurableOutbox::stats() const noexcept {
    return impl_->stats();
}

Identifier128 DurableOutbox::run_id() const noexcept {
    return impl_->run_id();
}

Identifier128 DurableOutbox::source_instance_id() const noexcept {
    return impl_->source_instance_id();
}

const std::filesystem::path& DurableOutbox::run_directory() const noexcept {
    return impl_->run_directory();
}

const DurableOutboxConfig& DurableOutbox::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::outbox
