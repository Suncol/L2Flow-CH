#pragma once

#include "l2flow/common/identifier.h"
#include "l2flow/ingest/canonical.h"

#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace l2flow::outbox {

using Identifier128 = common::Identifier128;

enum class RecordKind : std::uint8_t {
    kTickOccurrence = 1U,
    kSnapshot = 2U,
    kTickControl = 3U,
    kGapDiagnostic = 4U,
    kChannelFault = 5U,
    kFreshnessBarrier = 6U,
    kFinalBarrier = 7U,
};

enum class ConsumerKind : std::uint8_t {
    kRaw = 0U,
    kEvent = 1U,
    kKLine = 2U,
};

enum class ContinuityState : std::uint8_t {
    kNormal = 0U,
    kDerivedCatchup,
    kRawOnlyStale,
    kRawCatchup,
    kFatalContinuity,
};

// lsn is the total order assigned by the single WAL committer.  The pair
// (batch_sequence,row_index) is retained so a checkpoint can be audited
// against the exact durable batch; ingress_sequence is deliberately absent.
struct WalPosition final {
    std::uint64_t lsn = 0U;
    std::uint64_t batch_sequence = 0U;
    std::uint32_t row_index = 0U;

    friend constexpr bool operator==(const WalPosition&,
                                     const WalPosition&) = default;
    friend constexpr auto operator<=>(const WalPosition&,
                                      const WalPosition&) = default;
};

struct FreshnessBarrier final {
    std::uint64_t frontier_id = 0U;
    std::uint64_t created_monotonic_ns = 0U;
    std::uint64_t created_utc_ns = 0U;
};

// One zero-initialized, run-scoped logical record.  Only the payload selected
// by kind is serialized.  A Tick occurrence contains both representations:
// raw_tick is the unmodified decoded fact, while disposition.tick contains
// the final SequenceRecovery overlay used by Event/KLine.
struct CanonicalRecord final {
    RecordKind kind = RecordKind::kTickOccurrence;
    std::uint32_t producer_lane = 0U;
    std::uint32_t owner = 0U;
    ingest::CanonicalTick raw_tick{};
    ingest::CanonicalSnapshot raw_snapshot{};
    ingest::TickDispatch disposition{};
    ingest::ChannelGap gap{};
    ingest::ChannelFault fault{};
    FreshnessBarrier barrier{};
    bool catalog_match = false;
};

struct RecordView final {
    WalPosition position{};
    Identifier128 batch_id{};
    std::uint32_t payload_checksum = 0U;
    std::shared_ptr<const CanonicalRecord> record;
};

class ConsumerCompletionSink {
public:
    virtual ~ConsumerCompletionSink() = default;

    // Completion may arrive out of order. Implementations must advance only
    // the largest gap-free prefix and must never infer completion from an
    // ingress_sequence maximum.
    [[nodiscard]] virtual bool Complete(
        ConsumerKind consumer,
        std::span<const WalPosition> positions) noexcept = 0;
};

[[nodiscard]] constexpr const char* ConsumerName(
    ConsumerKind consumer) noexcept {
    switch (consumer) {
        case ConsumerKind::kRaw:
            return "raw";
        case ConsumerKind::kEvent:
            return "event";
        case ConsumerKind::kKLine:
            return "kline";
    }
    return "unknown";
}

[[nodiscard]] constexpr const char* ContinuityStateName(
    ContinuityState state) noexcept {
    switch (state) {
        case ContinuityState::kNormal:
            return "NORMAL";
        case ContinuityState::kDerivedCatchup:
            return "DERIVED_CATCHUP";
        case ContinuityState::kRawOnlyStale:
            return "RAW_ONLY_STALE";
        case ContinuityState::kRawCatchup:
            return "RAW_CATCHUP";
        case ContinuityState::kFatalContinuity:
            return "FATAL_CONTINUITY";
    }
    return "FATAL_CONTINUITY";
}

}  // namespace l2flow::outbox
