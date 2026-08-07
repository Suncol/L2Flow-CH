#pragma once

#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/canonical.h"
#include "l2flow/ingest/message.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::ingest::internal {

struct OwnedMessageView final {
    ParsedHeader header{};
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t receive_monotonic_ns = 0U;
    std::span<const std::byte> body{};
};

struct DecoderLimits final {
    std::size_t maximum_text_bytes = kMaximumIdentityBytes;
    std::size_t maximum_depth_items = 4'096U;
    std::size_t maximum_queue_items = 1'000'000U;
};

enum class DecodeError : std::uint8_t {
    kNone,
    kUnsupported,
    kTruncated,
    kInvalidOffset,
    kRangeOverlap,
    kCountExceeded,
    kCountMismatch,
    kInvalidText,
    kInvalidNativeSequence,
    kFixedPointOverflow,
    kResourceExhausted,
};

struct TickDecodeResult final {
    CanonicalTick tick{};
    bool catalog_match = false;
};

struct SnapshotDecodeResult final {
    CanonicalSnapshot snapshot{};
    bool catalog_match = false;
};

[[nodiscard]] DecodeError DecodeTick(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    TickDecodeResult* output) noexcept;

[[nodiscard]] DecodeError DecodeSnapshot(
    const OwnedMessageView& message,
    std::uint32_t trade_date,
    const DecoderLimits& limits,
    const InstrumentCatalog& catalog,
    SnapshotDecodeResult* output) noexcept;

[[nodiscard]] bool ExtractAdmissionRoute(
    MessageClass message_class,
    std::span<const std::byte> body,
    std::uint64_t* route_key) noexcept;

[[nodiscard]] bool ExtractTickNativeDescriptor(
    MessageClass message_class,
    std::span<const std::byte> body,
    Market* market,
    std::uint32_t* channel,
    std::uint64_t* sequence) noexcept;

}  // namespace l2flow::ingest::internal
