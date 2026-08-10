#include "l2flow/journal/fact_journal.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <fcntl.h>
#include <functional>
#include <limits>
#include <list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <sys/file.h>
#include <unistd.h>

namespace l2flow::journal {
namespace {

constexpr std::uint32_t kFileMagic = UINT32_C(0x314a464c);  // LFJ1
constexpr std::uint32_t kRecordMagic = UINT32_C(0x3152464c);  // LFR1
constexpr std::uint16_t kFormatVersion = 1U;
constexpr std::uint64_t kEventSeen = UINT64_C(1) << 0U;
constexpr std::uint64_t kKLineSeen = UINT64_C(1) << 1U;
constexpr std::uint64_t kWriting = UINT64_C(1) << 2U;
constexpr std::uint64_t kOffsetMask = ~UINT64_C(7);
constexpr std::uint32_t kCrc32cPolynomial = UINT32_C(0x82f63b78);

static_assert(kFactJournalFileHeaderBytes % 8U == 0U);
static_assert(kFactJournalRecordBytes % 8U == 0U);
static_assert(kFactJournalRecordHeaderBytes + kCanonicalTickEncodedBytes <=
              kFactJournalRecordBytes);

class LittleEndianWriter final {
public:
    explicit LittleEndianWriter(std::span<std::byte> output) noexcept
        : output_(output) {}

    void U8(std::uint8_t value) noexcept { Unsigned(value, 1U); }
    void U16(std::uint16_t value) noexcept { Unsigned(value, 2U); }
    void U32(std::uint32_t value) noexcept { Unsigned(value, 4U); }
    void U64(std::uint64_t value) noexcept { Unsigned(value, 8U); }
    void I32(std::int32_t value) noexcept {
        U32(std::bit_cast<std::uint32_t>(value));
    }
    void I64(std::int64_t value) noexcept {
        U64(std::bit_cast<std::uint64_t>(value));
    }
    void Bool(bool value) noexcept { U8(value ? 1U : 0U); }

    void Bytes(std::span<const std::byte> bytes) noexcept {
        if (bytes.size() > output_.size() - std::min(position_, output_.size())) {
            good_ = false;
            return;
        }
        std::copy(bytes.begin(), bytes.end(), output_.begin() +
                  static_cast<std::ptrdiff_t>(position_));
        position_ += bytes.size();
    }

    [[nodiscard]] bool complete() const noexcept {
        return good_ && position_ == output_.size();
    }

private:
    void Unsigned(std::uint64_t value, std::size_t bytes) noexcept {
        if (bytes > output_.size() - std::min(position_, output_.size())) {
            good_ = false;
            return;
        }
        for (std::size_t index = 0U; index < bytes; ++index) {
            output_[position_ + index] = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                UINT64_C(0xff));
        }
        position_ += bytes;
    }

    std::span<std::byte> output_;
    std::size_t position_ = 0U;
    bool good_ = true;
};

class LittleEndianReader final {
public:
    explicit LittleEndianReader(std::span<const std::byte> input) noexcept
        : input_(input) {}

    [[nodiscard]] std::uint8_t U8() noexcept {
        return static_cast<std::uint8_t>(Unsigned(1U));
    }
    [[nodiscard]] std::uint16_t U16() noexcept {
        return static_cast<std::uint16_t>(Unsigned(2U));
    }
    [[nodiscard]] std::uint32_t U32() noexcept {
        return static_cast<std::uint32_t>(Unsigned(4U));
    }
    [[nodiscard]] std::uint64_t U64() noexcept { return Unsigned(8U); }
    [[nodiscard]] std::int32_t I32() noexcept {
        return std::bit_cast<std::int32_t>(U32());
    }
    [[nodiscard]] std::int64_t I64() noexcept {
        return std::bit_cast<std::int64_t>(U64());
    }
    [[nodiscard]] bool Bool() noexcept {
        const std::uint8_t value = U8();
        if (value > 1U) {
            good_ = false;
        }
        return value != 0U;
    }

    void Bytes(std::span<std::byte> output) noexcept {
        if (output.size() > input_.size() -
                                std::min(position_, input_.size())) {
            good_ = false;
            return;
        }
        std::copy_n(input_.begin() + static_cast<std::ptrdiff_t>(position_),
                    output.size(), output.begin());
        position_ += output.size();
    }

    [[nodiscard]] bool complete() const noexcept {
        return good_ && position_ == input_.size();
    }

private:
    [[nodiscard]] std::uint64_t Unsigned(std::size_t bytes) noexcept {
        if (bytes > input_.size() - std::min(position_, input_.size())) {
            good_ = false;
            return 0U;
        }
        std::uint64_t value = 0U;
        for (std::size_t index = 0U; index < bytes; ++index) {
            value |= static_cast<std::uint64_t>(
                         std::to_integer<std::uint8_t>(
                             input_[position_ + index]))
                     << static_cast<unsigned int>(index * 8U);
        }
        position_ += bytes;
        return value;
    }

    std::span<const std::byte> input_;
    std::size_t position_ = 0U;
    bool good_ = true;
};

[[nodiscard]] constexpr auto MakeCrc32cTables() noexcept {
    std::array<std::array<std::uint32_t, 256U>, 8U> tables{};
    for (std::size_t index = 0U; index < tables[0U].size(); ++index) {
        std::uint32_t value = static_cast<std::uint32_t>(index);
        for (std::size_t bit = 0U; bit < 8U; ++bit) {
            value = (value >> 1U) ^
                    ((value & 1U) != 0U ? kCrc32cPolynomial : 0U);
        }
        tables[0U][index] = value;
    }
    for (std::size_t table = 1U; table < tables.size(); ++table) {
        for (std::size_t index = 0U; index < tables[table].size(); ++index) {
            const std::uint32_t previous = tables[table - 1U][index];
            tables[table][index] =
                tables[0U][previous & UINT32_C(0xff)] ^ (previous >> 8U);
        }
    }
    return tables;
}

constexpr auto kCrc32cTables = MakeCrc32cTables();

[[nodiscard]] std::uint32_t LoadLittleEndian32(
    const std::byte* input) noexcept {
    return static_cast<std::uint32_t>(
               std::to_integer<std::uint8_t>(input[0U])) |
           (static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(input[1U]))
            << 8U) |
           (static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(input[2U]))
            << 16U) |
           (static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(input[3U]))
            << 24U);
}

[[nodiscard]] std::uint32_t Crc32c(
    std::span<const std::byte> input) noexcept {
    std::uint32_t crc = UINT32_MAX;
    while (input.size() >= 8U) {
        const std::uint32_t first = LoadLittleEndian32(input.data()) ^ crc;
        crc = kCrc32cTables[7U][first & UINT32_C(0xff)] ^
              kCrc32cTables[6U][(first >> 8U) & UINT32_C(0xff)] ^
              kCrc32cTables[5U][(first >> 16U) & UINT32_C(0xff)] ^
              kCrc32cTables[4U][first >> 24U] ^
              kCrc32cTables[3U][std::to_integer<std::uint8_t>(input[4U])] ^
              kCrc32cTables[2U][std::to_integer<std::uint8_t>(input[5U])] ^
              kCrc32cTables[1U][std::to_integer<std::uint8_t>(input[6U])] ^
              kCrc32cTables[0U][std::to_integer<std::uint8_t>(input[7U])];
        input = input.subspan(8U);
    }
    for (const std::byte value : input) {
        crc = kCrc32cTables[0U][
                  (crc ^ std::to_integer<std::uint8_t>(value)) &
                  UINT32_C(0xff)] ^
              (crc >> 8U);
    }
    return ~crc;
}

class FingerprintBuilder final {
public:
    void U8(std::uint8_t value) noexcept { Byte(value); }
    void U16(std::uint16_t value) noexcept { Unsigned(value, 2U); }
    void U32(std::uint32_t value) noexcept { Unsigned(value, 4U); }
    void U64(std::uint64_t value) noexcept { Unsigned(value, 8U); }
    void I32(std::int32_t value) noexcept {
        U32(std::bit_cast<std::uint32_t>(value));
    }
    void I64(std::int64_t value) noexcept {
        U64(std::bit_cast<std::uint64_t>(value));
    }
    void Bool(bool value) noexcept { U8(value ? 1U : 0U); }
    void Bytes(std::span<const std::byte> values) noexcept {
        for (const std::byte value : values) {
            Byte(std::to_integer<std::uint8_t>(value));
        }
    }
    [[nodiscard]] std::uint64_t Finish() const noexcept { return state_; }

private:
    void Unsigned(std::uint64_t value, std::size_t bytes) noexcept {
        for (std::size_t index = 0U; index < bytes; ++index) {
            Byte(static_cast<std::uint8_t>(
                value >> static_cast<unsigned int>(index * 8U)));
        }
    }
    void Byte(std::uint8_t value) noexcept {
        state_ ^= value;
        state_ *= UINT64_C(1099511628211);
    }

    std::uint64_t state_ = UINT64_C(14695981039346656037);
};

void EncodeDecimal(LittleEndianWriter* writer,
                   const ingest::FixedDecimal& value) noexcept {
    writer->I64(value.raw);
    writer->I64(value.p6);
    writer->U8(value.source_scale);
    writer->Bool(value.raw_valid);
    writer->Bool(value.p6_valid);
}

void DecodeDecimal(LittleEndianReader* reader,
                   ingest::FixedDecimal* value) noexcept {
    value->raw = reader->I64();
    value->p6 = reader->I64();
    value->source_scale = reader->U8();
    value->raw_valid = reader->Bool();
    value->p6_valid = reader->Bool();
}

[[nodiscard]] bool EncodeCanonicalTick(
    const ingest::CanonicalTick& tick,
    std::span<std::byte> output) noexcept {
    if (output.size() != kCanonicalTickEncodedBytes) {
        return false;
    }
    LittleEndianWriter writer(output);
    const ingest::CanonicalCommon& common = tick.common;
    writer.U64(common.ingress_sequence);
    writer.U64(common.vendor_sequence_id);
    writer.U64(common.receive_monotonic_ns);
    writer.U64(common.native_sequence);
    writer.U64(common.exchange_time_ns_from_midnight);
    writer.U64(common.vendor_local_time_ns_from_midnight);
    writer.U64(common.quality_flags);
    writer.U64(common.gap_epoch);
    writer.U64(common.gap_before_first);
    writer.U64(common.gap_before_last);
    writer.U32(common.trade_date);
    writer.U32(common.instrument_id);
    writer.U32(common.instrument_ordinal);
    writer.U32(common.channel);
    writer.U32(common.exchange_time_raw);
    writer.U32(common.vendor_local_time_raw);
    writer.Bool(common.exchange_time_valid);
    writer.Bool(common.vendor_local_time_valid);
    writer.U8(common.message_key.service_id);
    writer.U16(common.message_key.service_version);
    writer.U16(common.message_key.message_id);
    writer.U8(static_cast<std::uint8_t>(common.kind));
    writer.U8(static_cast<std::uint8_t>(common.identity.market));
    writer.U8(common.identity.security_id_source_size);
    writer.U8(common.identity.security_id_size);
    writer.Bytes(common.identity.security_id_source);
    writer.Bytes(common.identity.security_id);
    writer.U8(common.md_stream_id_size);
    writer.Bytes(common.md_stream_id);
    EncodeDecimal(&writer, tick.price);
    EncodeDecimal(&writer, tick.amount);
    writer.I64(tick.quantity.raw);
    writer.U8(tick.quantity.scale);
    writer.Bool(tick.quantity.valid);
    writer.I64(tick.primary_order_id);
    writer.I64(tick.buy_order_id);
    writer.I64(tick.sell_order_id);
    writer.I64(tick.sh_add_matched_quantity_raw);
    writer.U64(tick.validity);
    writer.I32(tick.raw_type);
    writer.I32(tick.raw_side);
    writer.U8(static_cast<std::uint8_t>(tick.action));
    writer.U8(static_cast<std::uint8_t>(tick.side));
    writer.U8(static_cast<std::uint8_t>(tick.aggressor));
    writer.U8(static_cast<std::uint8_t>(tick.order_type));
    writer.U8(static_cast<std::uint8_t>(tick.phase));
    return writer.complete();
}

[[nodiscard]] bool DecodeCanonicalTick(
    std::span<const std::byte> input,
    ingest::CanonicalTick* output) noexcept {
    if (input.size() != kCanonicalTickEncodedBytes || output == nullptr) {
        return false;
    }
    ingest::CanonicalTick tick{};
    LittleEndianReader reader(input);
    ingest::CanonicalCommon& common = tick.common;
    common.ingress_sequence = reader.U64();
    common.vendor_sequence_id = reader.U64();
    common.receive_monotonic_ns = reader.U64();
    common.native_sequence = reader.U64();
    common.exchange_time_ns_from_midnight = reader.U64();
    common.vendor_local_time_ns_from_midnight = reader.U64();
    common.quality_flags = reader.U64();
    common.gap_epoch = reader.U64();
    common.gap_before_first = reader.U64();
    common.gap_before_last = reader.U64();
    common.trade_date = reader.U32();
    common.instrument_id = reader.U32();
    common.instrument_ordinal = reader.U32();
    common.channel = reader.U32();
    common.exchange_time_raw = reader.U32();
    common.vendor_local_time_raw = reader.U32();
    common.exchange_time_valid = reader.Bool();
    common.vendor_local_time_valid = reader.Bool();
    common.message_key.service_id = reader.U8();
    common.message_key.service_version = reader.U16();
    common.message_key.message_id = reader.U16();
    const std::uint8_t kind = reader.U8();
    const std::uint8_t market = reader.U8();
    common.identity.security_id_source_size = reader.U8();
    common.identity.security_id_size = reader.U8();
    reader.Bytes(common.identity.security_id_source);
    reader.Bytes(common.identity.security_id);
    common.md_stream_id_size = reader.U8();
    reader.Bytes(common.md_stream_id);
    DecodeDecimal(&reader, &tick.price);
    DecodeDecimal(&reader, &tick.amount);
    tick.quantity.raw = reader.I64();
    tick.quantity.scale = reader.U8();
    tick.quantity.valid = reader.Bool();
    tick.primary_order_id = reader.I64();
    tick.buy_order_id = reader.I64();
    tick.sell_order_id = reader.I64();
    tick.sh_add_matched_quantity_raw = reader.I64();
    tick.validity = reader.U64();
    tick.raw_type = reader.I32();
    tick.raw_side = reader.I32();
    const std::uint8_t action = reader.U8();
    const std::uint8_t side = reader.U8();
    const std::uint8_t aggressor = reader.U8();
    const std::uint8_t order_type = reader.U8();
    const std::uint8_t phase = reader.U8();
    if (!reader.complete() ||
        kind > static_cast<std::uint8_t>(
                   ingest::CanonicalKind::kShenzhenSnapshot) ||
        market > static_cast<std::uint8_t>(ingest::Market::kShenzhen) ||
        common.identity.security_id_source_size >
            ingest::kMaximumIdentityBytes ||
        common.identity.security_id_size > ingest::kMaximumIdentityBytes ||
        common.md_stream_id_size > ingest::kMaximumIdentityBytes ||
        action > static_cast<std::uint8_t>(ingest::TickAction::kStatus) ||
        side > static_cast<std::uint8_t>(ingest::Side::kLend) ||
        aggressor >
            static_cast<std::uint8_t>(ingest::Aggressor::kNeutral) ||
        order_type >
            static_cast<std::uint8_t>(ingest::OrderType::kSameSideBest) ||
        phase > static_cast<std::uint8_t>(ingest::TradingPhase::kEnded)) {
        return false;
    }
    common.kind = static_cast<ingest::CanonicalKind>(kind);
    common.identity.market = static_cast<ingest::Market>(market);
    tick.action = static_cast<ingest::TickAction>(action);
    tick.side = static_cast<ingest::Side>(side);
    tick.aggressor = static_cast<ingest::Aggressor>(aggressor);
    tick.order_type = static_cast<ingest::OrderType>(order_type);
    tick.phase = static_cast<ingest::TradingPhase>(phase);
    *output = tick;
    return true;
}

[[nodiscard]] bool EncodeRecord(const ingest::CanonicalTick& tick,
                                std::span<std::byte> output) noexcept {
    if (output.size() != kFactJournalRecordBytes) {
        return false;
    }
    std::fill(output.begin(), output.end(), std::byte{0U});
    const auto payload = output.subspan(kFactJournalRecordHeaderBytes,
                                        kCanonicalTickEncodedBytes);
    if (!EncodeCanonicalTick(tick, payload)) {
        return false;
    }
    LittleEndianWriter header(
        output.first(kFactJournalRecordHeaderBytes));
    header.U32(kRecordMagic);
    header.U16(kFormatVersion);
    header.U16(static_cast<std::uint16_t>(kFactJournalRecordHeaderBytes));
    header.U32(static_cast<std::uint32_t>(kCanonicalTickEncodedBytes));
    header.U32(static_cast<std::uint32_t>(kFactJournalRecordBytes));
    header.U32(Crc32c(payload));
    header.U32(0U);
    return header.complete();
}

[[nodiscard]] bool DecodeRecord(std::span<const std::byte> input,
                                ingest::CanonicalTick* output,
                                std::string* error) {
    if (input.size() != kFactJournalRecordBytes) {
        *error = "FactJournal record has an invalid byte length";
        return false;
    }
    LittleEndianReader header(
        input.first(kFactJournalRecordHeaderBytes));
    const std::uint32_t magic = header.U32();
    const std::uint16_t version = header.U16();
    const std::uint16_t header_bytes = header.U16();
    const std::uint32_t payload_bytes = header.U32();
    const std::uint32_t record_bytes = header.U32();
    const std::uint32_t expected_crc = header.U32();
    const std::uint32_t reserved = header.U32();
    if (!header.complete() || magic != kRecordMagic ||
        version != kFormatVersion ||
        header_bytes != kFactJournalRecordHeaderBytes ||
        payload_bytes != kCanonicalTickEncodedBytes ||
        record_bytes != kFactJournalRecordBytes || reserved != 0U) {
        *error = "FactJournal record header is corrupt or unsupported";
        return false;
    }
    const auto payload = input.subspan(kFactJournalRecordHeaderBytes,
                                       kCanonicalTickEncodedBytes);
    if (Crc32c(payload) != expected_crc) {
        *error = "FactJournal record CRC32C mismatch";
        return false;
    }
    const auto padding = input.subspan(kFactJournalRecordHeaderBytes +
                                       kCanonicalTickEncodedBytes);
    if (!std::all_of(padding.begin(), padding.end(),
                     [](std::byte value) { return value == std::byte{0U}; })) {
        *error = "FactJournal record padding is corrupt";
        return false;
    }
    if (!DecodeCanonicalTick(payload, output)) {
        *error = "FactJournal canonical Tick payload is invalid";
        return false;
    }
    return true;
}

[[nodiscard]] std::array<std::byte, kFactJournalFileHeaderBytes>
EncodeFileHeader(std::uint32_t trade_date) noexcept {
    std::array<std::byte, kFactJournalFileHeaderBytes> header{};
    LittleEndianWriter writer(header);
    writer.U32(kFileMagic);
    writer.U16(kFormatVersion);
    writer.U16(static_cast<std::uint16_t>(kFactJournalFileHeaderBytes));
    writer.U32(trade_date);
    writer.U32(static_cast<std::uint32_t>(kFactJournalRecordHeaderBytes));
    writer.U32(static_cast<std::uint32_t>(kCanonicalTickEncodedBytes));
    writer.U32(static_cast<std::uint32_t>(kFactJournalRecordBytes));
    writer.U64(0U);
    return header;
}

[[nodiscard]] bool IdentityEqual(const ingest::ExactIdentity& left,
                                 const ingest::ExactIdentity& right) noexcept {
    return left.market == right.market &&
           left.security_id_source_size == right.security_id_source_size &&
           left.security_id_size == right.security_id_size &&
           left.security_id_source == right.security_id_source &&
           left.security_id == right.security_id;
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

void FingerprintDecimal(FingerprintBuilder* hash,
                        const ingest::FixedDecimal& value) noexcept {
    hash->I64(value.raw);
    hash->U8(value.source_scale);
    hash->Bool(value.raw_valid);
}

[[nodiscard]] std::string ErrnoMessage(std::string_view operation,
                                       int error_number) {
    return std::string(operation) + ": " +
           std::system_category().message(error_number);
}

void StoreMessageBestEffort(std::string* output,
                            std::string_view message,
                            std::string_view detail = {}) noexcept {
    try {
        output->assign(message.data(), message.size());
        if (!detail.empty()) {
            output->append(": ");
            output->append(detail.data(), detail.size());
        }
    } catch (...) {
        try {
            output->assign(message.data(), message.size());
        } catch (...) {
        }
    }
}

void StoreErrnoBestEffort(std::string* output,
                          std::string_view operation,
                          int error_number) noexcept {
    try {
        *output = ErrnoMessage(operation, error_number);
    } catch (...) {
        StoreMessageBestEffort(output, operation);
    }
}

struct DirectoryEntry final {
    std::uint64_t offset_and_flags = 0U;
    std::uint64_t fingerprint = 0U;
};

static_assert(sizeof(DirectoryEntry) == 16U);

struct PreparedFact final {
    FactKey key{};
    std::uint64_t fingerprint = 0U;
    std::array<std::byte, kFactJournalRecordBytes> record{};
};

struct IoCounters final {
    std::uint64_t calls = 0U;
    std::uint64_t bytes = 0U;
    std::uint64_t partial = 0U;
};

struct WriteResult final {
    IoCounters counters{};
    int error_number = 0;
    bool no_progress = false;

    [[nodiscard]] bool ok() const noexcept {
        return error_number == 0 && !no_progress;
    }
};

struct ReadResult final {
    IoCounters counters{};
    ingest::CanonicalTick tick{};
    std::string error;

    [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct PendingRange final {
    std::uint64_t begin = 0U;
    std::uint64_t end = 0U;
};

struct Page final {
    std::array<DirectoryEntry, kFactJournalPageEntries> entries{};
};

static_assert(sizeof(Page) == 64U * 1'024U);

struct ChannelKey final {
    ingest::Market market = ingest::Market::kUnknown;
    std::uint32_t channel = 0U;

    friend bool operator==(const ChannelKey&, const ChannelKey&) = default;
};

struct ChannelKeyHash final {
    [[nodiscard]] std::size_t operator()(const ChannelKey& key) const noexcept {
        const std::uint64_t value =
            (static_cast<std::uint64_t>(key.channel) << 8U) |
            static_cast<std::uint8_t>(key.market);
        return static_cast<std::size_t>(value ^ (value >> 33U));
    }
};

struct ChannelDirectory final {
    std::unordered_map<std::uint64_t, std::unique_ptr<Page>> pages;
};

[[nodiscard]] std::uint64_t ConsumerFlag(FactConsumer consumer) noexcept {
    switch (consumer) {
        case FactConsumer::kEvent:
            return kEventSeen;
        case FactConsumer::kKLine:
            return kKLineSeen;
    }
    return 0U;
}

[[nodiscard]] bool ValidJournalTick(const ingest::CanonicalTick& tick,
                                    std::uint32_t trade_date) noexcept {
    const FactKey key = MakeFactKey(tick);
    return key.trade_date == trade_date &&
           (key.market == ingest::Market::kShanghai ||
            key.market == ingest::Market::kShenzhen) &&
           (key.market != ingest::Market::kShanghai || key.channel != 0U) &&
           key.native_sequence != 0U &&
           static_cast<std::uint8_t>(tick.common.kind) <=
               static_cast<std::uint8_t>(
                   ingest::CanonicalKind::kShenzhenSnapshot) &&
           tick.common.identity.security_id_source_size <=
               ingest::kMaximumIdentityBytes &&
           tick.common.identity.security_id_size <=
               ingest::kMaximumIdentityBytes &&
           tick.common.md_stream_id_size <= ingest::kMaximumIdentityBytes &&
           static_cast<std::uint8_t>(tick.action) <=
               static_cast<std::uint8_t>(ingest::TickAction::kStatus) &&
           static_cast<std::uint8_t>(tick.side) <=
               static_cast<std::uint8_t>(ingest::Side::kLend) &&
           static_cast<std::uint8_t>(tick.aggressor) <=
               static_cast<std::uint8_t>(ingest::Aggressor::kNeutral) &&
           static_cast<std::uint8_t>(tick.order_type) <=
               static_cast<std::uint8_t>(
                   ingest::OrderType::kSameSideBest) &&
           static_cast<std::uint8_t>(tick.phase) <=
               static_cast<std::uint8_t>(ingest::TradingPhase::kEnded);
}

}  // namespace

FactKey MakeFactKey(const ingest::CanonicalTick& tick) noexcept {
    return FactKey{tick.common.trade_date, tick.common.identity.market,
                   tick.common.channel, tick.common.native_sequence};
}

bool FactPayloadEqual(const ingest::CanonicalTick& left,
                      const ingest::CanonicalTick& right) noexcept {
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
           left.raw_type == right.raw_type && left.raw_side == right.raw_side &&
           left.action == right.action && left.side == right.side &&
           left.aggressor == right.aggressor &&
           left.order_type == right.order_type && left.phase == right.phase;
}

std::uint64_t FactPayloadFingerprint(
    const ingest::CanonicalTick& tick) noexcept {
    FingerprintBuilder hash;
    hash.U32(tick.common.trade_date);
    hash.U32(tick.common.instrument_id);
    hash.U8(tick.common.message_key.service_id);
    hash.U16(tick.common.message_key.service_version);
    hash.U16(tick.common.message_key.message_id);
    hash.U8(static_cast<std::uint8_t>(tick.common.kind));
    hash.U32(tick.common.channel);
    hash.U64(tick.common.native_sequence);
    hash.U32(tick.common.exchange_time_raw);
    hash.U8(static_cast<std::uint8_t>(tick.common.identity.market));
    hash.U8(tick.common.identity.security_id_source_size);
    hash.U8(tick.common.identity.security_id_size);
    hash.Bytes(tick.common.identity.security_id_source);
    hash.Bytes(tick.common.identity.security_id);
    FingerprintDecimal(&hash, tick.price);
    FingerprintDecimal(&hash, tick.amount);
    hash.I64(tick.quantity.raw);
    hash.U8(tick.quantity.scale);
    hash.Bool(tick.quantity.valid);
    hash.I64(tick.primary_order_id);
    hash.I64(tick.buy_order_id);
    hash.I64(tick.sell_order_id);
    hash.I64(tick.sh_add_matched_quantity_raw);
    hash.I32(tick.raw_type);
    hash.I32(tick.raw_side);
    hash.U8(static_cast<std::uint8_t>(tick.action));
    hash.U8(static_cast<std::uint8_t>(tick.side));
    hash.U8(static_cast<std::uint8_t>(tick.aggressor));
    hash.U8(static_cast<std::uint8_t>(tick.order_type));
    hash.U8(static_cast<std::uint8_t>(tick.phase));
    return hash.Finish();
}

struct CanonicalFactJournal::Impl final {
    struct CachedTick final {
        ingest::CanonicalTick tick{};
        std::list<std::uint64_t>::iterator position;
    };

    explicit Impl(FactJournalConfig input_config, int input_fd)
        : config(std::move(input_config)), fd(input_fd),
          hot_cache_capacity(config.hot_cache_entries) {}

    ~Impl() {
        if (fd >= 0) {
            static_cast<void>(::close(fd));
        }
    }

    [[nodiscard]] bool BeginFailureLocked() noexcept {
        if (!is_healthy) {
            return false;
        }
        is_healthy = false;
        ++statistics.errors;
        return true;
    }

    void FailLocked(std::string_view message,
                    std::string_view detail = {}) noexcept {
        if (BeginFailureLocked()) {
            StoreMessageBestEffort(&fatal, message, detail);
        }
        write_condition.notify_all();
    }

    void FailErrnoLocked(std::string_view operation,
                         int error_number) noexcept {
        if (BeginFailureLocked()) {
            StoreErrnoBestEffort(&fatal, operation, error_number);
        }
        write_condition.notify_all();
    }

    [[nodiscard]] WriteResult PwriteAll(
        std::span<const std::byte> bytes,
        std::uint64_t offset) const noexcept {
        WriteResult result{};
        std::size_t completed = 0U;
        while (completed < bytes.size()) {
            const std::size_t remaining = bytes.size() - completed;
            const std::size_t request = std::min(
                remaining, static_cast<std::size_t>(SSIZE_MAX));
            ++result.counters.calls;
            const ssize_t written = ::pwrite(
                fd, bytes.data() + static_cast<std::ptrdiff_t>(completed),
                request, static_cast<off_t>(offset + completed));
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                result.error_number = errno;
                return result;
            }
            if (written == 0) {
                result.no_progress = true;
                return result;
            }
            const auto count = static_cast<std::size_t>(written);
            if (count < request) {
                ++result.counters.partial;
            }
            completed += count;
            result.counters.bytes += count;
        }
        return result;
    }

    [[nodiscard]] bool PreadAll(std::span<std::byte> bytes,
                               std::uint64_t offset,
                               IoCounters* counters,
                               std::string* error) const {
        std::size_t completed = 0U;
        while (completed < bytes.size()) {
            const std::size_t remaining = bytes.size() - completed;
            const std::size_t request = std::min(
                remaining, static_cast<std::size_t>(SSIZE_MAX));
            ++counters->calls;
            const ssize_t read_bytes = ::pread(
                fd, bytes.data() + static_cast<std::ptrdiff_t>(completed),
                request, static_cast<off_t>(offset + completed));
            if (read_bytes < 0) {
                if (errno == EINTR) {
                    continue;
                }
                *error = ErrnoMessage("FactJournal pread failed", errno);
                return false;
            }
            if (read_bytes == 0) {
                *error = "FactJournal pread reached an incomplete record";
                return false;
            }
            const auto count = static_cast<std::size_t>(read_bytes);
            if (count < request) {
                ++counters->partial;
            }
            completed += count;
            counters->bytes += count;
        }
        return true;
    }

    [[nodiscard]] ReadResult ReadRecord(std::uint64_t offset) const {
        ReadResult result{};
        std::array<std::byte, kFactJournalRecordBytes> record{};
        if (!PreadAll(record, offset, &result.counters, &result.error) ||
            !DecodeRecord(record, &result.tick, &result.error)) {
            return result;
        }
        return result;
    }

    [[nodiscard]] DirectoryEntry* FindEntryLocked(const FactKey& key) {
        const auto channel = directories.find(
            ChannelKey{key.market, key.channel});
        if (channel == directories.end()) {
            return nullptr;
        }
        const std::uint64_t page_number =
            key.native_sequence / kFactJournalPageEntries;
        const auto page = channel->second.pages.find(page_number);
        if (page == channel->second.pages.end()) {
            return nullptr;
        }
        const std::size_t index = static_cast<std::size_t>(
            key.native_sequence % kFactJournalPageEntries);
        DirectoryEntry* const entry = &page->second->entries[index];
        return entry->offset_and_flags == 0U ? nullptr : entry;
    }

    [[nodiscard]] DirectoryEntry* CreateEntryLocked(const FactKey& key) {
        const ChannelKey channel_key{key.market, key.channel};
        const std::uint64_t page_number =
            key.native_sequence / kFactJournalPageEntries;
        auto channel = directories.find(channel_key);
        if (channel != directories.end()) {
            const auto page = channel->second.pages.find(page_number);
            if (page != channel->second.pages.end()) {
                const std::size_t index = static_cast<std::size_t>(
                    key.native_sequence % kFactJournalPageEntries);
                return &page->second->entries[index];
            }
        }
        if (statistics.directory_pages >= config.maximum_directory_pages) {
            return nullptr;
        }
        if (channel == directories.end()) {
            const auto inserted = directories.try_emplace(channel_key);
            channel = inserted.first;
            if (inserted.second) {
                ++statistics.directory_channels;
            }
        }
        auto page = channel->second.pages.find(page_number);
        if (page == channel->second.pages.end()) {
            auto new_page = std::make_unique<Page>();
            page = channel->second.pages.emplace(
                page_number, std::move(new_page)).first;
            ++statistics.directory_pages;
        }
        const std::size_t index = static_cast<std::size_t>(
            key.native_sequence % kFactJournalPageEntries);
        return &page->second->entries[index];
    }

    [[nodiscard]] bool HasCachedTickLocked(std::uint64_t offset) const {
        return hot_cache.find(offset) != hot_cache.end();
    }

    [[nodiscard]] bool TryLoadCachedTickLocked(
        std::uint64_t offset,
        ingest::CanonicalTick* output) {
        const auto cached = hot_cache.find(offset);
        if (cached == hot_cache.end()) {
            return false;
        }
        *output = cached->second.tick;
        hot_lru.splice(hot_lru.begin(), hot_lru,
                       cached->second.position);
        ++statistics.hot_cache_hits;
        return true;
    }

    void InsertCacheBestEffortLocked(
        std::uint64_t offset,
        const ingest::CanonicalTick& tick) noexcept {
        if (hot_cache_capacity == 0U) {
            return;
        }
        try {
            const auto existing = hot_cache.find(offset);
            if (existing != hot_cache.end()) {
                existing->second.tick = tick;
                hot_lru.splice(hot_lru.begin(), hot_lru,
                               existing->second.position);
                return;
            }
            hot_lru.push_front(offset);
            try {
                hot_cache.emplace(
                    offset, CachedTick{tick, hot_lru.begin()});
            } catch (...) {
                hot_lru.pop_front();
                throw;
            }
            while (hot_cache.size() > hot_cache_capacity) {
                const std::uint64_t evicted = hot_lru.back();
                hot_cache.erase(evicted);
                hot_lru.pop_back();
            }
        } catch (...) {
            hot_cache.clear();
            hot_lru.clear();
            hot_cache_capacity = 0U;
        }
    }

    [[nodiscard]] bool IsPendingOffsetLocked(std::uint64_t offset) const {
        return std::any_of(
            pending_ranges.begin(), pending_ranges.end(),
            [offset](const PendingRange& range) {
                return offset >= range.begin && offset < range.end;
            });
    }

    void AddWriteStatsLocked(const IoCounters& counters) noexcept {
        statistics.write_calls += counters.calls;
        statistics.write_bytes += counters.bytes;
        statistics.partial_writes += counters.partial;
    }

    void AddReadStatsLocked(const IoCounters& counters) noexcept {
        statistics.read_calls += counters.calls;
        statistics.read_bytes += counters.bytes;
        statistics.partial_reads += counters.partial;
    }

    FactJournalConfig config;
    int fd = -1;
    mutable std::mutex mutex;
    std::condition_variable write_condition;
    bool is_healthy = true;
    bool dirty = true;
    std::string fatal;
    std::uint64_t append_offset = kFactJournalFileHeaderBytes;
    std::uint64_t durable_offset = 0U;
    std::uint64_t active_writes = 0U;
    std::uint64_t active_reads = 0U;
    std::uint64_t reserved_records = 0U;
    std::uint64_t waiting_admissions = 0U;
    FactJournalStats statistics{};
    std::unordered_map<ChannelKey, ChannelDirectory, ChannelKeyHash>
        directories;
    std::size_t hot_cache_capacity = 0U;
    std::list<std::uint64_t> hot_lru;
    std::unordered_map<std::uint64_t, CachedTick> hot_cache;
    std::vector<PendingRange> pending_ranges;
};

CanonicalFactJournal::CanonicalFactJournal(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

CanonicalFactJournal::~CanonicalFactJournal() = default;

std::unique_ptr<CanonicalFactJournal> CanonicalFactJournal::Create(
    FactJournalConfig config,
    std::string* error) {
    if (error != nullptr) {
        error->clear();
    }
    if (error == nullptr) {
        return nullptr;
    }
    if (config.trade_date == 0U) {
        StoreMessageBestEffort(error,
                               "FactJournal trade_date must be nonzero");
        return nullptr;
    }
    if (config.path.empty()) {
        StoreMessageBestEffort(error,
                               "FactJournal path must not be empty");
        return nullptr;
    }
    if (config.maximum_records == 0U) {
        StoreMessageBestEffort(
            error, "FactJournal maximum_records must be nonzero");
        return nullptr;
    }
    if (config.maximum_directory_pages == 0U) {
        StoreMessageBestEffort(
            error,
            "FactJournal maximum_directory_pages must be nonzero");
        return nullptr;
    }
    const int fd = ::open(config.path.c_str(),
                          O_RDWR | O_CREAT | O_CLOEXEC,
                          S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        StoreErrnoBestEffort(error, "FactJournal open failed", errno);
        return nullptr;
    }
    int lock_result = 0;
    do {
        lock_result = ::flock(fd, LOCK_EX | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        const int error_number = errno;
        static_cast<void>(::close(fd));
        bool already_locked = error_number == EWOULDBLOCK;
#if EAGAIN != EWOULDBLOCK
        already_locked = already_locked || error_number == EAGAIN;
#endif
        const std::string_view operation =
            already_locked
            ? "FactJournal path is already locked by another instance"
            : "FactJournal flock failed";
        StoreErrnoBestEffort(error, operation, error_number);
        return nullptr;
    }
    if (::ftruncate(fd, 0) != 0) {
        const int error_number = errno;
        static_cast<void>(::close(fd));
        StoreErrnoBestEffort(error, "FactJournal ftruncate failed",
                             error_number);
        return nullptr;
    }
    bool fd_owned_by_impl = false;
    try {
        auto impl = std::make_unique<Impl>(std::move(config), fd);
        fd_owned_by_impl = true;
        const auto header = EncodeFileHeader(impl->config.trade_date);
        const WriteResult write = impl->PwriteAll(header, 0U);
        if (!write.ok()) {
            if (write.error_number != 0) {
                impl->FailErrnoLocked("FactJournal pwrite failed",
                                      write.error_number);
            } else {
                impl->FailLocked("FactJournal pwrite made no progress");
            }
            StoreMessageBestEffort(
                error, impl->fatal.empty()
                    ? std::string_view{"FactJournal creation failed"}
                    : std::string_view{impl->fatal});
            return nullptr;
        }
        impl->AddWriteStatsLocked(write.counters);
        impl->statistics.file_bytes = kFactJournalFileHeaderBytes;
        return std::unique_ptr<CanonicalFactJournal>(
            new CanonicalFactJournal(std::move(impl)));
    } catch (const std::exception& exception) {
        if (!fd_owned_by_impl) {
            static_cast<void>(::close(fd));
        }
        StoreMessageBestEffort(error, "FactJournal creation failed",
                               exception.what());
        return nullptr;
    } catch (...) {
        if (!fd_owned_by_impl) {
            static_cast<void>(::close(fd));
        }
        StoreMessageBestEffort(error, "FactJournal creation failed");
        return nullptr;
    }
}

std::vector<AdmitResult> CanonicalFactJournal::AdmitBatch(
    FactConsumer consumer,
    std::span<const ingest::CanonicalTick> ticks,
    std::span<ingest::CanonicalTick> authoritative_winners) {
    std::vector<AdmitResult> results(ticks.size());
    if (!authoritative_winners.empty()) {
        if (authoritative_winners.size() != ticks.size()) {
            std::lock_guard lock(impl_->mutex);
            impl_->FailLocked(
                "FactJournal authoritative winner output size mismatch");
            return results;
        }
        const ingest::CanonicalTick* const input_begin = ticks.data();
        const ingest::CanonicalTick* const input_end =
            input_begin + ticks.size();
        const ingest::CanonicalTick* const output_begin =
            authoritative_winners.data();
        const ingest::CanonicalTick* const output_end =
            output_begin + authoritative_winners.size();
        const std::less<const ingest::CanonicalTick*> before;
        if (before(input_begin, output_end) &&
            before(output_begin, input_end)) {
            std::lock_guard lock(impl_->mutex);
            impl_->FailLocked(
                "FactJournal authoritative winner output overlaps input");
            return results;
        }
    }
    std::vector<PreparedFact> prepared;
    try {
        if (ticks.size() >
            std::numeric_limits<std::size_t>::max() /
                kFactJournalRecordBytes) {
            std::lock_guard lock(impl_->mutex);
            impl_->FailLocked("FactJournal batch byte size overflows size_t");
            return results;
        }
        prepared.reserve(ticks.size());
        for (const ingest::CanonicalTick& tick : ticks) {
            if (!ValidJournalTick(tick, impl_->config.trade_date)) {
                std::lock_guard lock(impl_->mutex);
                impl_->FailLocked(
                    "FactJournal received a Tick that cannot be decoded "
                    "losslessly or has an invalid key/date");
                return results;
            }
            prepared.emplace_back();
            PreparedFact& fact = prepared.back();
            fact.key = MakeFactKey(tick);
            fact.fingerprint = FactPayloadFingerprint(tick);
            if (!EncodeRecord(tick, fact.record)) {
                std::lock_guard lock(impl_->mutex);
                impl_->FailLocked(
                    "FactJournal could not encode a canonical Tick");
                return results;
            }
        }
    } catch (const std::exception& exception) {
        std::lock_guard lock(impl_->mutex);
        impl_->FailLocked("FactJournal fingerprint batch failed",
                          exception.what());
        return results;
    } catch (...) {
        std::lock_guard lock(impl_->mutex);
        impl_->FailLocked("FactJournal fingerprint batch failed");
        return results;
    }
    std::unique_lock lock(impl_->mutex);
    if (!impl_->is_healthy || ticks.empty()) {
        return results;
    }
    const std::uint64_t consumer_flag = ConsumerFlag(consumer);
    if (consumer_flag == 0U) {
        impl_->FailLocked("FactJournal received an invalid consumer");
        return results;
    }
    std::uint64_t consumer_new = 0U;
    std::uint64_t duplicates = 0U;
    std::uint64_t conflicts = 0U;
    try {
        std::vector<std::byte> append;
        append.reserve(ticks.size() * kFactJournalRecordBytes);
        std::vector<const ingest::CanonicalTick*> pending;
        pending.reserve(ticks.size());
        std::vector<DirectoryEntry*> publishing_entries;
        publishing_entries.reserve(ticks.size());
        std::unordered_map<std::uint64_t, ingest::CanonicalTick>
            cold_winners;

        // Resolve every pre-existing fingerprint match before this batch
        // reserves directory entries. Published offsets are immutable, so a
        // cache miss can be read and decoded without holding the directory
        // mutex. Re-scan after every unlocked read to cover a winner that a
        // concurrent batch may have published in the meantime.
        for (;;) {
            DirectoryEntry* writing = nullptr;
            for (const PreparedFact& fact : prepared) {
                DirectoryEntry* const entry =
                    impl_->FindEntryLocked(fact.key);
                if (entry != nullptr &&
                    (entry->offset_and_flags & kWriting) != 0U) {
                    writing = entry;
                    break;
                }
            }
            if (writing != nullptr) {
                ++impl_->waiting_admissions;
                try {
                    impl_->write_condition.wait(lock, [&] {
                        return !impl_->is_healthy ||
                               (writing->offset_and_flags & kWriting) == 0U;
                    });
                } catch (...) {
                    --impl_->waiting_admissions;
                    throw;
                }
                --impl_->waiting_admissions;
                if (!impl_->is_healthy) {
                    return results;
                }
                continue;
            }

            std::vector<std::uint64_t> cold_offsets;
            std::unordered_set<std::uint64_t> scheduled;
            for (const PreparedFact& fact : prepared) {
                DirectoryEntry* const entry =
                    impl_->FindEntryLocked(fact.key);
                if (entry == nullptr ||
                    entry->fingerprint != fact.fingerprint) {
                    continue;
                }
                const std::uint64_t offset =
                    entry->offset_and_flags & kOffsetMask;
                if (cold_winners.contains(offset) ||
                    impl_->HasCachedTickLocked(offset) ||
                    !scheduled.insert(offset).second) {
                    continue;
                }
                cold_offsets.push_back(offset);
            }
            if (cold_offsets.empty()) {
                break;
            }

            impl_->active_reads += cold_offsets.size();
            lock.unlock();
            std::vector<std::pair<std::uint64_t, ReadResult>> reads;
            try {
                reads.reserve(cold_offsets.size());
                for (const std::uint64_t offset : cold_offsets) {
                    reads.emplace_back(offset, impl_->ReadRecord(offset));
                    if (!reads.back().second.ok()) {
                        break;
                    }
                }
            } catch (...) {
                lock.lock();
                impl_->active_reads -= cold_offsets.size();
                throw;
            }
            lock.lock();
            impl_->active_reads -= cold_offsets.size();
            for (const auto& read : reads) {
                impl_->AddReadStatsLocked(read.second.counters);
            }
            if (!impl_->is_healthy) {
                return results;
            }
            const auto failed = std::find_if(
                reads.begin(), reads.end(),
                [](const auto& read) { return !read.second.ok(); });
            if (failed != reads.end()) {
                impl_->FailLocked(failed->second.error);
                return results;
            }
            if (reads.size() != cold_offsets.size()) {
                impl_->FailLocked(
                    "FactJournal comparison pread did not complete");
                return results;
            }
            for (auto& read : reads) {
                cold_winners.try_emplace(
                    read.first, std::move(read.second.tick));
            }
        }

        const std::uint64_t pending_start = impl_->append_offset;
        for (std::size_t index = 0U; index < ticks.size(); ++index) {
            const ingest::CanonicalTick& tick = ticks[index];
            const PreparedFact& fact = prepared[index];
            const FactKey& key = fact.key;
            const std::uint64_t fingerprint = fact.fingerprint;
            DirectoryEntry* entry = impl_->FindEntryLocked(key);
            if (entry != nullptr) {
                const std::uint64_t offset =
                    entry->offset_and_flags & kOffsetMask;
                results[index].handle.offset = offset;
                if (entry->fingerprint != fingerprint) {
                    results[index].code = AdmitCode::kConflict;
                    ++conflicts;
                    continue;
                }
                ingest::CanonicalTick existing{};
                if (offset >= pending_start) {
                    const std::uint64_t relative = offset - pending_start;
                    if (relative % kFactJournalRecordBytes != 0U ||
                        relative / kFactJournalRecordBytes >= pending.size()) {
                        impl_->FailLocked(
                            "FactJournal pending comparison offset is invalid");
                        std::fill(
                            results.begin(), results.end(), AdmitResult{});
                        return results;
                    }
                    existing = *pending[static_cast<std::size_t>(
                        relative / kFactJournalRecordBytes)];
                } else {
                    const auto cold = cold_winners.find(offset);
                    if (cold != cold_winners.end()) {
                        existing = cold->second;
                    } else if (!impl_->TryLoadCachedTickLocked(
                                   offset, &existing)) {
                        impl_->FailLocked(
                            "FactJournal authoritative comparison winner "
                            "was not prefetched");
                        std::fill(
                            results.begin(), results.end(), AdmitResult{});
                        return results;
                    }
                }
                if (!FactPayloadEqual(existing, tick)) {
                    results[index].code = AdmitCode::kConflict;
                    ++conflicts;
                    continue;
                }
                if (!authoritative_winners.empty()) {
                    authoritative_winners[index] = existing;
                }
                if ((entry->offset_and_flags & consumer_flag) != 0U) {
                    results[index].code = AdmitCode::kDuplicate;
                    ++duplicates;
                } else {
                    // Publish a consumer's first observation only after every
                    // physical append in this batch has succeeded.
                    entry->offset_and_flags |= consumer_flag | kWriting;
                    publishing_entries.push_back(entry);
                    results[index].code = AdmitCode::kNew;
                    ++consumer_new;
                }
                continue;
            }

            const std::uint64_t occupied =
                impl_->statistics.records + impl_->reserved_records;
            if (occupied >= impl_->config.maximum_records ||
                pending.size() >=
                    impl_->config.maximum_records - occupied) {
                impl_->FailLocked(
                    "FactJournal maximum_records would be exceeded");
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            if (append.size() >
                std::numeric_limits<std::uint64_t>::max() -
                    impl_->append_offset ||
                impl_->append_offset + append.size() >
                    static_cast<std::uint64_t>(
                        std::numeric_limits<off_t>::max()) -
                        kFactJournalRecordBytes) {
                impl_->FailLocked("FactJournal file offset is exhausted");
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            const std::uint64_t offset =
                impl_->append_offset + append.size();
            const std::size_t old_size = append.size();
            append.resize(old_size + kFactJournalRecordBytes);
            std::copy(fact.record.begin(), fact.record.end(),
                      append.begin() + static_cast<std::ptrdiff_t>(old_size));
            pending.push_back(&tick);
            entry = impl_->CreateEntryLocked(key);
            if (entry == nullptr) {
                impl_->FailLocked(
                    "FactJournal maximum_directory_pages would be exceeded");
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            if (entry->offset_and_flags != 0U) {
                impl_->FailLocked(
                    "FactJournal directory insertion was not unique");
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            entry->offset_and_flags =
                offset | consumer_flag | kWriting;
            entry->fingerprint = fingerprint;
            publishing_entries.push_back(entry);
            results[index] = AdmitResult{AdmitCode::kNew,
                                          FactHandle{offset}};
            if (!authoritative_winners.empty()) {
                authoritative_winners[index] = tick;
            }
            ++consumer_new;
        }

        for (const auto& winner : cold_winners) {
            impl_->InsertCacheBestEffortLocked(winner.first, winner.second);
        }

        if (!append.empty()) {
            const std::uint64_t pending_end =
                pending_start + append.size();
            impl_->pending_ranges.push_back(
                PendingRange{pending_start, pending_end});
            impl_->append_offset = pending_end;
            impl_->reserved_records += pending.size();
            ++impl_->active_writes;

            lock.unlock();
            const WriteResult write =
                impl_->PwriteAll(append, pending_start);
            lock.lock();

            impl_->AddWriteStatsLocked(write.counters);
            const auto range = std::find_if(
                impl_->pending_ranges.begin(), impl_->pending_ranges.end(),
                [pending_start](const PendingRange& value) {
                    return value.begin == pending_start;
                });
            if (range != impl_->pending_ranges.end()) {
                impl_->pending_ranges.erase(range);
            }
            --impl_->active_writes;
            impl_->reserved_records -= pending.size();

            if (!write.ok()) {
                if (write.error_number != 0) {
                    impl_->FailErrnoLocked("FactJournal pwrite failed",
                                           write.error_number);
                } else {
                    impl_->FailLocked(
                        "FactJournal pwrite made no progress");
                }
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            if (!impl_->is_healthy) {
                impl_->write_condition.notify_all();
                std::fill(results.begin(), results.end(), AdmitResult{});
                return results;
            }
            for (DirectoryEntry* const entry : publishing_entries) {
                entry->offset_and_flags &= ~kWriting;
            }
            impl_->dirty = true;
            impl_->statistics.records += pending.size();
            impl_->statistics.record_bytes += append.size();
            impl_->statistics.file_bytes = std::max(
                impl_->statistics.file_bytes, pending_end);
            for (std::size_t index = 0U; index < pending.size(); ++index) {
                impl_->InsertCacheBestEffortLocked(
                    pending_start + index * kFactJournalRecordBytes,
                    *pending[index]);
            }
            impl_->write_condition.notify_all();
        } else {
            for (DirectoryEntry* const entry : publishing_entries) {
                entry->offset_and_flags &= ~kWriting;
            }
        }
        impl_->statistics.consumer_new += consumer_new;
        impl_->statistics.duplicates += duplicates;
        impl_->statistics.conflicts += conflicts;
        return results;
    } catch (const std::exception& exception) {
        impl_->FailLocked("FactJournal admission failed", exception.what());
    } catch (...) {
        impl_->FailLocked("FactJournal admission failed");
    }
    std::fill(results.begin(), results.end(), AdmitResult{});
    return results;
}

bool CanonicalFactJournal::Read(FactHandle handle,
                                ingest::CanonicalTick* output) {
    std::unique_lock lock(impl_->mutex);
    impl_->write_condition.wait(lock, [&] {
        return !impl_->is_healthy ||
               !impl_->IsPendingOffsetLocked(handle.offset);
    });
    if (!impl_->is_healthy) {
        return false;
    }
    if (output == nullptr || impl_->statistics.records == 0U ||
        handle.offset < kFactJournalFileHeaderBytes ||
        handle.offset % 8U != 0U ||
        (handle.offset - kFactJournalFileHeaderBytes) %
                kFactJournalRecordBytes !=
            0U ||
        handle.offset > impl_->append_offset - kFactJournalRecordBytes) {
        impl_->FailLocked("FactJournal received an invalid FactHandle");
        return false;
    }
    ingest::CanonicalTick cached{};
    if (impl_->TryLoadCachedTickLocked(handle.offset, &cached)) {
        *output = cached;
        return true;
    }

    ++impl_->active_reads;
    lock.unlock();
    ReadResult read{};
    try {
        read = impl_->ReadRecord(handle.offset);
    } catch (const std::exception& exception) {
        lock.lock();
        --impl_->active_reads;
        impl_->FailLocked("FactJournal read failed", exception.what());
        return false;
    } catch (...) {
        lock.lock();
        --impl_->active_reads;
        impl_->FailLocked("FactJournal read failed");
        return false;
    }
    lock.lock();
    --impl_->active_reads;
    impl_->AddReadStatsLocked(read.counters);
    if (!impl_->is_healthy) {
        return false;
    }
    if (!read.ok()) {
        impl_->FailLocked(read.error);
        return false;
    }
    impl_->InsertCacheBestEffortLocked(handle.offset, read.tick);
    *output = read.tick;
    return true;
}

bool CanonicalFactJournal::Flush() {
    std::unique_lock lock(impl_->mutex);
    impl_->write_condition.wait(lock, [&] {
        return !impl_->is_healthy || impl_->active_writes == 0U;
    });
    if (!impl_->is_healthy) {
        return false;
    }
    for (;;) {
        ++impl_->statistics.flush_calls;
        if (::fdatasync(impl_->fd) == 0) {
            impl_->dirty = false;
            impl_->durable_offset = impl_->append_offset;
            return true;
        }
        if (errno != EINTR) {
            impl_->FailErrnoLocked("FactJournal fdatasync failed", errno);
            return false;
        }
    }
}

void CanonicalFactJournal::ClearHotCache() {
    std::lock_guard lock(impl_->mutex);
    impl_->hot_cache.clear();
    impl_->hot_lru.clear();
}

void CanonicalFactJournal::EvictHotCache(FactHandle handle) {
    std::lock_guard lock(impl_->mutex);
    const auto cached = impl_->hot_cache.find(handle.offset);
    if (cached == impl_->hot_cache.end()) {
        return;
    }
    impl_->hot_lru.erase(cached->second.position);
    impl_->hot_cache.erase(cached);
}

bool CanonicalFactJournal::DropFileCache() {
    std::lock_guard lock(impl_->mutex);
    if (!impl_->is_healthy || impl_->active_writes != 0U ||
        impl_->active_reads != 0U || impl_->dirty ||
        impl_->durable_offset != impl_->append_offset) {
        return false;
    }
#if defined(POSIX_FADV_DONTNEED)
    return ::posix_fadvise(impl_->fd, 0,
                           static_cast<off_t>(impl_->append_offset),
                           POSIX_FADV_DONTNEED) == 0;
#else
    return false;
#endif
}

bool CanonicalFactJournal::healthy() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->is_healthy;
}

std::string CanonicalFactJournal::fatal_error() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->fatal;
}

FactJournalStats CanonicalFactJournal::stats() const {
    std::lock_guard lock(impl_->mutex);
    FactJournalStats result = impl_->statistics;
    result.active_writes = impl_->active_writes;
    result.active_reads = impl_->active_reads;
    result.reserved_records = impl_->reserved_records;
    result.waiting_admissions = impl_->waiting_admissions;
    return result;
}

}  // namespace l2flow::journal
