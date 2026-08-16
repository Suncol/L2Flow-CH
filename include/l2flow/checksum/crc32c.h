#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::checksum {
namespace detail {

inline constexpr std::uint32_t kCrc32cPolynomial = UINT32_C(0x82f63b78);

[[nodiscard]] constexpr auto MakeCrc32cTables() noexcept {
    std::array<std::array<std::uint32_t, 256U>, 8U> tables{};
    for (std::size_t index = 0U; index < tables[0U].size(); ++index) {
        std::uint32_t value = static_cast<std::uint32_t>(index);
        for (std::size_t bit = 0U; bit < 8U; ++bit) {
            value = (value >> 1U) ^
                ((value & UINT32_C(1)) != 0U ? kCrc32cPolynomial : 0U);
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

inline constexpr auto kCrc32cTables = MakeCrc32cTables();

[[nodiscard]] inline std::uint32_t LoadLittleEndian32(
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

}  // namespace detail

class Crc32cAccumulator final {
public:
    void Update(std::span<const std::byte> input) noexcept {
        while (input.size() >= 8U) {
            const std::uint32_t first =
                detail::LoadLittleEndian32(input.data()) ^ crc_;
            crc_ =
                detail::kCrc32cTables[7U][first & UINT32_C(0xff)] ^
                detail::kCrc32cTables[6U][
                    (first >> 8U) & UINT32_C(0xff)] ^
                detail::kCrc32cTables[5U][
                    (first >> 16U) & UINT32_C(0xff)] ^
                detail::kCrc32cTables[4U][first >> 24U] ^
                detail::kCrc32cTables[3U][
                    std::to_integer<std::uint8_t>(input[4U])] ^
                detail::kCrc32cTables[2U][
                    std::to_integer<std::uint8_t>(input[5U])] ^
                detail::kCrc32cTables[1U][
                    std::to_integer<std::uint8_t>(input[6U])] ^
                detail::kCrc32cTables[0U][
                    std::to_integer<std::uint8_t>(input[7U])];
            input = input.subspan(8U);
        }
        for (const std::byte value : input) {
            crc_ = detail::kCrc32cTables[0U][
                       (crc_ ^ std::to_integer<std::uint8_t>(value)) &
                       UINT32_C(0xff)] ^
                   (crc_ >> 8U);
        }
    }

    [[nodiscard]] std::uint32_t Finish() const noexcept { return ~crc_; }

private:
    std::uint32_t crc_ = UINT32_MAX;
};

[[nodiscard]] inline std::uint32_t Crc32c(
    std::span<const std::byte> input) noexcept {
    Crc32cAccumulator accumulator;
    accumulator.Update(input);
    return accumulator.Finish();
}

}  // namespace l2flow::checksum
