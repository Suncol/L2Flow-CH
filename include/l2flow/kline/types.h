#pragma once

#include "l2flow/common/identifier.h"
#include "l2flow/ingest/canonical.h"
#include "l2flow/outbox/types.h"

#include <compare>
#include <cstdint>
#include <memory>
#include <vector>

namespace l2flow::kline {

using l2flow::common::Identifier128;
using l2flow::ingest::CanonicalKind;
using l2flow::ingest::Market;

inline constexpr std::uint32_t kKLineSchemaVersion = 1U;
inline constexpr std::uint64_t kNanosecondsPerSecond = UINT64_C(1'000'000'000);
inline constexpr std::uint64_t kNanosecondsPerDay =
    UINT64_C(86'400) * kNanosecondsPerSecond;

struct FactKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t native_sequence = 0U;

    friend constexpr bool operator==(const FactKey&, const FactKey&) = default;
    friend constexpr auto operator<=>(const FactKey&, const FactKey&) = default;
};

struct KLineKey final {
    std::uint32_t trade_date = 0U;
    Market market = Market::kUnknown;
    std::uint32_t instrument_id = 0U;
    std::uint32_t interval_seconds = 0U;
    std::uint64_t bucket_start_ns_from_midnight = 0U;

    friend constexpr bool operator==(const KLineKey&, const KLineKey&) = default;
    friend constexpr auto operator<=>(const KLineKey&, const KLineKey&) = default;
};

// Exchange time is the first ordering member. Channel/native sequence is only
// a deterministic tie-break for equal SDK timestamps; it is not presented as
// an exchange-provided total order across Channels.
struct TradeAnchor final {
    std::uint64_t exchange_time_ns_from_midnight = 0U;
    std::uint32_t channel = 0U;
    std::uint64_t native_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;

    friend constexpr bool operator==(const TradeAnchor&,
                                     const TradeAnchor&) = default;
    friend constexpr auto operator<=>(const TradeAnchor&,
                                      const TradeAnchor&) = default;
};

struct KLinePayload final {
    std::uint64_t bucket_end_ns_from_midnight = 0U;
    std::int64_t open_price_p6 = 0;
    std::int64_t high_price_p6 = 0;
    std::int64_t low_price_p6 = 0;
    std::int64_t close_price_p6 = 0;
    std::int64_t volume = 0;
    // Canonical computed notional: sum(price_p6 * integer quantity). This is
    // deliberately independent of the optional Shanghai source amount field.
    std::int64_t notional_p6 = 0;
    std::uint64_t trade_count = 0U;
    TradeAnchor first_trade{};
    TradeAnchor last_trade{};
    std::uint64_t source_quality_flags = 0U;
    bool has_hole_fill = false;
    // Realtime sequence frontiers do not close historical hole fills, so a
    // live row remains provisional until an explicit reconciliation/finalize.
    bool provisional = true;

    friend constexpr bool operator==(const KLinePayload&,
                                     const KLinePayload&) = default;
};

enum class RevisionOperation : std::uint8_t {
    kInsert = 0U,
    kUpdate,
    kTombstone,
};

enum class RevisionReason : std::uint8_t {
    kLiveProjection = 0U,
    kHoleFill,
    kReconciliation,
    kSessionFinalize,
};

struct KLineRevision final {
    KLineKey key{};
    std::uint64_t version = 0U;
    Identifier128 revision_id{};
    Identifier128 supersedes_revision_id{};
    bool supersedes_revision_id_valid = false;
    Identifier128 recovery_run_id{};
    RevisionOperation operation = RevisionOperation::kInsert;
    RevisionReason reason = RevisionReason::kLiveProjection;
    Identifier128 calculation_run_id{};
    std::uint32_t logic_version = 0U;
    Identifier128 input_set_hash{};
    Identifier128 payload_hash{};
    bool is_deleted = false;
    KLinePayload payload{};

    friend constexpr bool operator==(const KLineRevision&,
                                     const KLineRevision&) = default;
};

struct KLineRevisionBatch final {
    Identifier128 calculation_run_id{};
    Identifier128 recovery_run_id{};
    std::uint32_t owner = 0U;
    std::uint64_t batch_sequence = 0U;
    RevisionReason reason = RevisionReason::kLiveProjection;
    std::vector<outbox::WalPosition> input_positions;
    std::vector<KLineRevision> revisions;
};

class KLineRevisionSink {
public:
    virtual ~KLineRevisionSink() = default;

    [[nodiscard]] virtual bool AppendRevisionBatch(
        std::shared_ptr<const KLineRevisionBatch> batch) noexcept = 0;
};

}  // namespace l2flow::kline
