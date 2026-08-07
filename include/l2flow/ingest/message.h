#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::ingest {

inline constexpr std::size_t kMdlHeaderBytes = 23U;
inline constexpr std::uint8_t kBinaryEncoding = 1U;

struct MessageKey final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;

    friend constexpr bool operator==(const MessageKey&,
                                     const MessageKey&) = default;
};

// Decoders present in this milestone. The default deployment selects all
// five Shanghai/Shenzhen L2 message families used for A-share processing.
// This is a message-family selection; the exact A-share instrument universe
// is owned by the catalog.
inline constexpr std::array<MessageKey, 5U> kSupportedMessageKeys{{
    {4U, 101U, 4U},
    {4U, 101U, 24U},
    {6U, 101U, 28U},
    {6U, 101U, 33U},
    {6U, 101U, 36U},
}};

using StreamMask = std::uint32_t;
inline constexpr StreamMask kSupportedStreamMask =
    (StreamMask{1U} << kSupportedMessageKeys.size()) - 1U;
inline constexpr StreamMask kDefaultAShareL2StreamMask =
    kSupportedStreamMask;

enum class MessageClass : std::uint8_t {
    kShanghaiSnapshot,
    kShanghaiTick,
    kShenzhenSnapshot,
    kShenzhenOrder,
    kShenzhenTransaction,
    kUnsupported,
};

[[nodiscard]] constexpr StreamMask StreamBit(
    const MessageKey& key) noexcept {
    for (std::size_t index = 0U; index < kSupportedMessageKeys.size();
         ++index) {
        if (kSupportedMessageKeys[index] == key) {
            return StreamMask{1U} << index;
        }
    }
    return 0U;
}

[[nodiscard]] constexpr bool StreamEnabled(
    StreamMask mask,
    const MessageKey& key) noexcept {
    const StreamMask bit = StreamBit(key);
    return bit != 0U && (mask & bit) != 0U;
}

[[nodiscard]] constexpr MessageClass ClassifyMessage(
    const MessageKey& key) noexcept {
    if (key == MessageKey{4U, 101U, 4U}) {
        return MessageClass::kShanghaiSnapshot;
    }
    if (key == MessageKey{4U, 101U, 24U}) {
        return MessageClass::kShanghaiTick;
    }
    if (key == MessageKey{6U, 101U, 28U}) {
        return MessageClass::kShenzhenSnapshot;
    }
    if (key == MessageKey{6U, 101U, 33U}) {
        return MessageClass::kShenzhenOrder;
    }
    if (key == MessageKey{6U, 101U, 36U}) {
        return MessageClass::kShenzhenTransaction;
    }
    return MessageClass::kUnsupported;
}

// Text format: one exact supported tuple per line (for example 4.101.24).
// Empty lines and lines beginning with '#' are ignored. Duplicates and tuples
// without a decoder in this build are configuration errors.
[[nodiscard]] bool LoadStreamConfig(
    const std::filesystem::path& path,
    StreamMask* output,
    std::string* error);

struct ParsedHeader final {
    std::uint8_t head_size = 0U;
    std::uint32_t message_size = 0U;
    std::uint8_t encoding = 0U;
    MessageKey key{};
    std::uint32_t local_time_raw = 0U;
    std::uint64_t vendor_sequence_id = 0U;
};

enum class AdmissionResult : std::uint8_t {
    kAccepted,
    kNotRunning,
    kConcurrentCallback,
    kNullMessage,
    kHeaderTruncated,
    kHeaderInvalid,
    kBodySizeMismatch,
    kNonBinaryEncoding,
    kUnsupportedMessage,
    kDisabledMessage,
    kBodyTruncated,
    kBodyTooLarge,
    kRouteInvalid,
    kLaneFull,
    kFeedNotReady,
    kInternalFailure,
};

[[nodiscard]] std::string_view AdmissionResultName(
    AdmissionResult value) noexcept;

[[nodiscard]] bool ParseMdlHeader(
    std::span<const std::byte> bytes,
    ParsedHeader* output) noexcept;

}  // namespace l2flow::ingest
