#pragma once

#include "l2flow/ingest/canonical.h"

#include <cstddef>
#include <cstdint>

namespace l2flow::ingest {

// A RawRecordTap is owned by the process boundary and called only by the
// corresponding decoder lane. Implementations must keep Append/Poll bounded
// and noexcept; returning false creates a fatal continuity boundary.
class RawRecordTap {
public:
    virtual ~RawRecordTap() = default;

    [[nodiscard]] virtual bool AppendTick(
        std::size_t decoder_lane,
        const CanonicalTick& tick) noexcept = 0;
    [[nodiscard]] virtual bool AppendSnapshot(
        std::size_t decoder_lane,
        const CanonicalSnapshot& snapshot) noexcept = 0;

    [[nodiscard]] virtual bool PollTick(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept = 0;
    [[nodiscard]] virtual bool PollSnapshot(
        std::size_t decoder_lane,
        std::uint64_t monotonic_ns) noexcept = 0;

    // Flush is called by the producer lane after its admission queue is empty
    // and before that decoder thread exits.
    [[nodiscard]] virtual bool FlushTick(
        std::size_t decoder_lane) noexcept = 0;
    [[nodiscard]] virtual bool FlushSnapshot(
        std::size_t decoder_lane) noexcept = 0;
};

}  // namespace l2flow::ingest
