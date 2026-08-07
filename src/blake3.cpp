#include "l2flow/clickhouse/raw_sink.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>

namespace l2flow::clickhouse {
namespace {

constexpr std::array<std::uint32_t, 8U> kIv{
    UINT32_C(0x6A09E667), UINT32_C(0xBB67AE85),
    UINT32_C(0x3C6EF372), UINT32_C(0xA54FF53A),
    UINT32_C(0x510E527F), UINT32_C(0x9B05688C),
    UINT32_C(0x1F83D9AB), UINT32_C(0x5BE0CD19)};
constexpr std::array<std::uint8_t, 16U> kPermutation{
    2U, 6U, 3U, 10U, 7U, 0U, 4U, 13U,
    1U, 11U, 12U, 5U, 9U, 14U, 15U, 8U};
constexpr std::uint32_t kChunkStart = 1U;
constexpr std::uint32_t kChunkEnd = 2U;
constexpr std::uint32_t kRoot = 8U;

[[nodiscard]] constexpr std::uint32_t RotateRight(std::uint32_t value,
                                                   unsigned int amount) {
    return (value >> amount) | (value << (32U - amount));
}

void Mix(std::array<std::uint32_t, 16U>* state,
         std::size_t a,
         std::size_t b,
         std::size_t c,
         std::size_t d,
         std::uint32_t x,
         std::uint32_t y) noexcept {
    auto& words = *state;
    words[a] = words[a] + words[b] + x;
    words[d] = RotateRight(words[d] ^ words[a], 16U);
    words[c] += words[d];
    words[b] = RotateRight(words[b] ^ words[c], 12U);
    words[a] = words[a] + words[b] + y;
    words[d] = RotateRight(words[d] ^ words[a], 8U);
    words[c] += words[d];
    words[b] = RotateRight(words[b] ^ words[c], 7U);
}

void Round(std::array<std::uint32_t, 16U>* state,
           const std::array<std::uint32_t, 16U>& message) noexcept {
    Mix(state, 0U, 4U, 8U, 12U, message[0U], message[1U]);
    Mix(state, 1U, 5U, 9U, 13U, message[2U], message[3U]);
    Mix(state, 2U, 6U, 10U, 14U, message[4U], message[5U]);
    Mix(state, 3U, 7U, 11U, 15U, message[6U], message[7U]);
    Mix(state, 0U, 5U, 10U, 15U, message[8U], message[9U]);
    Mix(state, 1U, 6U, 11U, 12U, message[10U], message[11U]);
    Mix(state, 2U, 7U, 8U, 13U, message[12U], message[13U]);
    Mix(state, 3U, 4U, 9U, 14U, message[14U], message[15U]);
}

[[nodiscard]] std::array<std::uint32_t, 16U> Compress(
    const std::array<std::uint32_t, 8U>& chaining_value,
    const std::array<std::uint32_t, 16U>& block,
    std::uint32_t block_length,
    std::uint32_t flags) noexcept {
    std::array<std::uint32_t, 16U> state{};
    std::copy(chaining_value.begin(), chaining_value.end(), state.begin());
    std::copy_n(kIv.begin(), 4U, state.begin() + 8U);
    state[14U] = block_length;
    state[15U] = flags;

    std::array<std::uint32_t, 16U> message = block;
    for (std::size_t round = 0U; round < 7U; ++round) {
        Round(&state, message);
        if (round != 6U) {
            std::array<std::uint32_t, 16U> permuted{};
            for (std::size_t index = 0U; index < message.size(); ++index) {
                permuted[index] = message[kPermutation[index]];
            }
            message = permuted;
        }
    }
    for (std::size_t index = 0U; index < 8U; ++index) {
        state[index] ^= state[index + 8U];
        state[index + 8U] ^= chaining_value[index];
    }
    return state;
}

[[nodiscard]] std::uint8_t HexValue(char value) noexcept {
    if (value >= '0' && value <= '9') {
        return static_cast<std::uint8_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
        return static_cast<std::uint8_t>(value - 'a' + 10);
    }
    if (value >= 'A' && value <= 'F') {
        return static_cast<std::uint8_t>(value - 'A' + 10);
    }
    return UINT8_MAX;
}

template <typename Integer>
void AppendLittleEndian(Integer value,
                        std::span<std::byte> destination,
                        std::size_t* offset) noexcept {
    for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
        destination[*offset + index] = static_cast<std::byte>(
            static_cast<std::uint64_t>(value) >> (index * 8U));
    }
    *offset += sizeof(Integer);
}

}  // namespace

Identifier128 Blake3Hash128(std::span<const std::byte> input) noexcept {
    // Every identifier input in this module fits one 64-byte BLAKE3 block.
    // Rejecting larger accidental inputs is deterministic and cannot affect
    // the fixed RawBatch/RawOccurrence call sites.
    if (input.size() > 64U) {
        return {};
    }
    std::array<std::uint32_t, 16U> block{};
    for (std::size_t index = 0U; index < input.size(); ++index) {
        block[index / 4U] |=
            static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(input[index]))
            << static_cast<unsigned int>((index % 4U) * 8U);
    }
    const auto output = Compress(
        kIv, block, static_cast<std::uint32_t>(input.size()),
        kChunkStart | kChunkEnd | kRoot);
    Identifier128 identifier{};
    for (std::size_t index = 0U; index < identifier.bytes.size(); ++index) {
        identifier.bytes[index] = static_cast<std::byte>(
            output[index / 4U] >> ((index % 4U) * 8U));
    }
    return identifier;
}

std::string IdentifierString(Identifier128 identifier) {
    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (const std::byte value : identifier.bytes) {
        stream << std::setw(2)
               << static_cast<unsigned int>(
                      std::to_integer<std::uint8_t>(value));
    }
    return stream.str();
}

bool ParseIdentifier(std::string_view text, Identifier128* output) noexcept {
    if (output == nullptr || text.size() != 32U) {
        return false;
    }
    Identifier128 parsed{};
    for (std::size_t index = 0U; index < parsed.bytes.size(); ++index) {
        const std::uint8_t high = HexValue(text[index * 2U]);
        const std::uint8_t low = HexValue(text[index * 2U + 1U]);
        if (high == UINT8_MAX || low == UINT8_MAX) {
            return false;
        }
        parsed.bytes[index] =
            static_cast<std::byte>((high << 4U) | low);
    }
    *output = parsed;
    return true;
}

Identifier128 RawBatchIdentifier(Identifier128 writer_instance_id,
                                 RawTableId table,
                                 std::uint32_t trade_date,
                                 std::uint64_t batch_sequence,
                                 std::uint32_t schema_version) noexcept {
    std::array<std::byte, 33U> input{};
    std::copy(writer_instance_id.bytes.begin(), writer_instance_id.bytes.end(),
              input.begin());
    std::size_t offset = writer_instance_id.bytes.size();
    input[offset++] =
        static_cast<std::byte>(static_cast<std::uint8_t>(table));
    AppendLittleEndian(trade_date, input, &offset);
    AppendLittleEndian(batch_sequence, input, &offset);
    AppendLittleEndian(schema_version, input, &offset);
    return Blake3Hash128(input);
}

Identifier128 RawOccurrenceIdentifier(
    Identifier128 source_instance_id,
    std::uint64_t feed_session_epoch,
    std::uint64_t ingress_sequence,
    ingest::CanonicalKind kind) noexcept {
    std::array<std::byte, 33U> input{};
    std::copy(source_instance_id.bytes.begin(), source_instance_id.bytes.end(),
              input.begin());
    std::size_t offset = source_instance_id.bytes.size();
    AppendLittleEndian(feed_session_epoch, input, &offset);
    AppendLittleEndian(ingress_sequence, input, &offset);
    input[offset] =
        static_cast<std::byte>(static_cast<std::uint8_t>(kind));
    return Blake3Hash128(input);
}

}  // namespace l2flow::clickhouse
