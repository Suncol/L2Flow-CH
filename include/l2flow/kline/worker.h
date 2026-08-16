#pragma once

#include "l2flow/journal/fact_journal.h"
#include "l2flow/kline/types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace l2flow::kline {

enum class KLineSequenceClass : std::uint8_t {
    kOrdered = 0U,
    kHoleFill,
};

struct KLineAdmissionToken final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t expected_sequence = 0U;
    std::uint64_t admission_floor = 0U;
    std::uint64_t retention_floor = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t dispatch_fence = 0U;
    KLineSequenceClass sequence_class = KLineSequenceClass::kOrdered;
};

struct KLineInput final {
    ingest::CanonicalTick tick{};
    KLineAdmissionToken admission{};
    outbox::WalPosition outbox_position{};
    bool catalog_match = true;
};

struct KLineWorkerConfig final {
    std::uint32_t trade_date = 0U;
    std::uint32_t owner = 0U;
    std::uint32_t owner_count = 0U;
    std::uint32_t revision_epoch = 0U;
    std::uint32_t logic_version = 1U;
    std::uint64_t feed_session_epoch = 0U;
    Identifier128 calculation_run_id{};
    // Intervals are immutable for one calculation run and must be sorted,
    // unique, nonzero intraday second counts.
    std::vector<std::uint32_t> interval_seconds{1U};

    // Shared across every Event/KLine owner for one trade date. Fact identity
    // is channel-global, while mutable projection state remains owner-local.
    std::shared_ptr<journal::CanonicalFactJournal> fact_journal;
    outbox::ConsumerCompletionSink* completion_sink = nullptr;

    std::size_t maximum_bars = 4U * 1'024U * 1'024U;
    std::size_t maximum_pending_commits = 1'024U;
};

enum class KLineApplyCode : std::uint8_t {
    kApplied = 0U,
    kDuplicateOnly,
    kSourceConflict,
    kInvalidInput,
    kCapacityExhausted,
    kSinkFailed,
    kFailed,
};

struct KLineApplyResult final {
    KLineApplyCode code = KLineApplyCode::kApplied;
    std::uint64_t facts_inserted = 0U;
    std::uint64_t trades_inserted = 0U;
    std::uint64_t duplicates = 0U;
    std::uint64_t changed_bars = 0U;
    std::uint64_t revisions_created = 0U;
};

struct KLineWorkerStats final {
    std::uint64_t facts_journaled = 0U;
    std::uint64_t trades_projected = 0U;
    std::uint64_t duplicate_facts = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t invalid_facts = 0U;
    std::uint64_t invalid_trade_exchange_times = 0U;
    std::uint64_t bars_created = 0U;
    std::uint64_t bars_updated = 0U;
    std::uint64_t revisions_created = 0U;
    std::uint64_t pending_revision_commits = 0U;
    // Conservative logical-owned bytes for immutable revision batches,
    // revision vector capacity, and raw-dependency nodes. This excludes the
    // deque's implementation storage and is not allocator RSS.
    std::uint64_t pending_revision_rows = 0U;
    std::uint64_t pending_revision_rows_high_watermark = 0U;
    std::uint64_t pending_revision_bytes = 0U;
    std::uint64_t pending_revision_bytes_high_watermark = 0U;
    std::uint64_t revision_batches_submitted = 0U;
};

[[nodiscard]] bool ValidateKLineWorkerConfig(
    const KLineWorkerConfig& config,
    std::string* error) noexcept;

class KLineWorker final {
public:
    ~KLineWorker();
    KLineWorker(const KLineWorker&) = delete;
    KLineWorker& operator=(const KLineWorker&) = delete;

    [[nodiscard]] static std::unique_ptr<KLineWorker> Create(
        KLineWorkerConfig config,
        KLineRevisionSink* sink,
        std::string* error);

    [[nodiscard]] KLineApplyResult ApplyBatch(
        std::span<const KLineInput> inputs) noexcept;
    [[nodiscard]] bool DrainRevisionCommits() noexcept;

    [[nodiscard]] bool CopyBar(const KLineKey& key,
                               KLinePayload* output) const noexcept;
    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] KLineWorkerStats stats() const noexcept;
    [[nodiscard]] const KLineWorkerConfig& config() const noexcept;

private:
    class Impl;
    explicit KLineWorker(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::kline
