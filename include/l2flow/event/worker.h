#pragma once

#include "l2flow/event/types.h"
#include "l2flow/journal/fact_journal.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace l2flow::event {

enum class EventSequenceClass : std::uint8_t {
    kOrdered = 0U,
    kHoleFill,
};

struct EventAdmissionToken final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t expected_sequence = 0U;
    std::uint64_t admission_floor = 0U;
    std::uint64_t retention_floor = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t dispatch_fence = 0U;
    EventSequenceClass sequence_class = EventSequenceClass::kOrdered;
};

struct EventInput final {
    ingest::CanonicalTick tick{};
    EventAdmissionToken admission{};
    bool catalog_match = true;
};

struct GapOpen final {
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t first_missing = 0U;
    std::uint64_t last_missing = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t dispatch_fence = 0U;
};

struct ChannelSeal final {
    Market market = Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t evict_before = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t dispatch_fence = 0U;
};

struct EventWorkerConfig final {
    std::uint32_t trade_date = 0U;
    std::uint32_t owner = 0U;
    std::uint32_t owner_count = 0U;
    std::uint32_t revision_epoch = 0U;
    std::uint32_t logic_version = 1U;
    std::uint64_t feed_session_epoch = 0U;
    Identifier128 calculation_run_id{};

    // Event and KLine must share this instance so one canonical payload is
    // appended once while each consumer retains independent first-seen state.
    // Worker creation rejects a null journal.
    std::shared_ptr<journal::CanonicalFactJournal> fact_journal;

    std::size_t maximum_carry_orders = 2U * 1'024U * 1'024U;
    // Conservative logical owned bytes for carry baselines and retained
    // order-use suffixes. Allocator-resident RSS is outside this bound.
    std::size_t maximum_order_history_bytes =
        4ULL * 1'024ULL * 1'024ULL * 1'024ULL;
    std::size_t maximum_hot_facts = 16U * 1'024U * 1'024U;
    // Conservative logical owned bytes for fact/Bundle/head state and the
    // channel, instrument, phase, and barrier indexes. Allocator bucket/cache
    // retention and process RSS are outside this bound.
    std::size_t maximum_hot_fact_bytes =
        4ULL * 1'024ULL * 1'024ULL * 1'024ULL;
    std::size_t maximum_cached_events = 16U * 1'024U * 1'024U;
    // Maximum immutable logical revision batches awaiting submission to the
    // derived Event sink; the unit is batches, not rows or raw ACKs.
    std::size_t maximum_pending_commits = 1'024U;
    std::size_t maximum_pending_revision_bytes =
        256U * 1'024U * 1'024U;

    // Correctness cuts create immutable logical recovery batches. Durable
    // consecutive batches remain owner-local until one of these independent
    // persistence bounds closes the group; controls do not close it.
    std::size_t persistence_group_max_batches = 1'024U;
    std::size_t persistence_group_max_rows = 16'384U;
    std::size_t persistence_group_max_bytes = 16U * 1'024U * 1'024U;
    std::uint64_t persistence_group_max_delay_ns =
        UINT64_C(1'000'000'000);

    // Repair owns generation-tagged eval patches and revision staging until
    // its atomic commit. This hard bound is independent of the read-only live
    // repairable suffix.
    std::size_t maximum_repair_bytes = 512U * 1'024U * 1'024U;

    // Shanghai END expansion is independent capacity: one END may touch every
    // active order for an instrument. Candidate/row/byte limits are checked
    // before expansion starts, then work is sliced on the owner thread.
    std::size_t maximum_end_candidates = 2U * 1'024U * 1'024U;
    std::size_t maximum_end_projected_rows =
        2U * 1'024U * 1'024U + 1U;
    std::size_t maximum_end_staging_bytes =
        2ULL * 1'024ULL * 1'024ULL * 1'024ULL;
    std::size_t end_slice_max_candidates = 2'048U;
    std::uint64_t end_slice_max_cpu_ns = 500'000U;

    // Seal work is incremental. A call processes at most this many order-use
    // or fact nodes and this many estimated owned bytes before returning.
    std::size_t eviction_slice_max_nodes = 4'096U;
    std::size_t eviction_slice_max_bytes = 4U * 1'024U * 1'024U;

    // A repair slice always executes at least one order-use node. Both limits
    // are then enforced before control returns to the owner loop.
    std::size_t repair_slice_max_order_uses = 4'096U;
    std::uint64_t repair_slice_max_cpu_ns = 500'000U;

    // A Shanghai phase cut owns its journal-cut metadata until every affected
    // fact has been normalized and its dirty order roles have been recorded.
    // The owned state shares maximum_repair_bytes with an active order repair.
    std::size_t phase_slice_max_nodes = 4'096U;
    std::size_t phase_slice_max_bytes = 4U * 1'024U * 1'024U;
    std::uint64_t phase_slice_max_cpu_ns = 500'000U;
};

enum class EventApplyCode : std::uint8_t {
    kApplied = 0U,
    kDuplicateOnly,
    kSourceConflict,
    kInvalidInput,
    kCapacityExhausted,
    kSinkFailed,
    kFailed,
};

struct EventApplyResult final {
    EventApplyCode code = EventApplyCode::kApplied;
    std::uint64_t facts_inserted = 0U;
    std::uint64_t duplicates = 0U;
    std::uint64_t repaired_order_uses = 0U;
    std::uint64_t changed_bundles = 0U;
    std::uint64_t revisions_created = 0U;
    bool repair_pending = false;
};

struct EventWorkerStats final {
    std::uint64_t facts_journaled = 0U;
    std::uint64_t duplicate_facts = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t live_order_uses = 0U;
    std::uint64_t repaired_order_uses = 0U;
    std::uint64_t repair_convergence_stops = 0U;
    std::uint64_t repair_slices = 0U;
    std::uint64_t repair_commits = 0U;
    std::uint64_t repair_order_restarts = 0U;
    std::uint64_t bundles_reassembled = 0U;
    std::uint64_t revisions_created = 0U;
    std::uint64_t tombstones_created = 0U;
    // Immutable logical revision batches waiting for submission to the Event
    // revision sink. This count has no raw-persistence or raw-ACK dependency.
    std::uint64_t pending_revision_batches = 0U;
    std::uint64_t revision_batches_submitted = 0U;
    std::uint64_t persistence_groups_submitted = 0U;
    std::uint64_t persistence_group_batches_max = 0U;
    std::uint64_t persistence_group_rows_max = 0U;
    std::uint64_t persistence_group_bytes_max = 0U;
    std::uint64_t pending_revision_bytes = 0U;
    std::uint64_t pending_revision_bytes_high_watermark = 0U;
    std::uint64_t active_repair_orders = 0U;
    std::uint64_t active_repair_bytes = 0U;
    std::uint64_t active_repair_bytes_high_watermark = 0U;
    std::uint64_t phase_normalization_slices = 0U;
    std::uint64_t phase_facts_scanned = 0U;
    std::uint64_t phase_dirty_roles_discovered = 0U;
    std::uint64_t pending_phase_bytes = 0U;
    std::uint64_t pending_phase_bytes_high_watermark = 0U;
    // Hot-path counters used to distinguish ordered append work from the
    // bounded late/乱序 fallback and to verify Shanghai END index coverage.
    std::uint64_t ordered_batch_fast_path = 0U;
    std::uint64_t unordered_batch_sorts = 0U;
    std::uint64_t barrier_index_orders_visited = 0U;
    std::uint64_t end_expansion_slices = 0U;
    std::uint64_t end_candidates_processed = 0U;
    std::uint64_t source_only_fast_path = 0U;
    std::uint64_t order_uses_compacted = 0U;
    std::uint64_t order_history_bytes = 0U;
    std::uint64_t order_history_bytes_high_watermark = 0U;
    std::uint64_t facts_evicted = 0U;
    std::uint64_t eviction_slices = 0U;
    std::uint64_t hot_facts = 0U;
    std::uint64_t hot_fact_bytes = 0U;
    std::uint64_t hot_fact_bytes_high_watermark = 0U;
};

// Owner-local view of one upstream Channel. journal_tail includes every
// accepted fact in the worker. projected_frontier advances only after the
// corresponding journal cut has been projected into the live caches.
// expected_sequence and generation are monotone observations of the
// SequenceRecovery classifier, not claims that historical repair is closed.
struct EventChannelState final {
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t journal_tail = 0U;
    std::uint64_t projected_frontier = 0U;
    std::uint64_t expected_sequence = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t admission_floor = 0U;
    std::uint64_t sealed_before = 0U;
    std::uint64_t eviction_target = 0U;
    std::uint64_t applied_dispatch_fence = 0U;
    bool gap_open = false;
};

[[nodiscard]] bool ValidateEventWorkerConfig(
    const EventWorkerConfig& config,
    std::string* error) noexcept;

class EventWorker final {
public:
    ~EventWorker();
    EventWorker(const EventWorker&) = delete;
    EventWorker& operator=(const EventWorker&) = delete;

    [[nodiscard]] static std::unique_ptr<EventWorker> Create(
        EventWorkerConfig config,
        EventRevisionSink* sink,
        std::string* error);

    // One owner thread calls all methods. ApplyBatch journals every accepted
    // fact before projecting any fact from this cut.
    [[nodiscard]] EventApplyResult ApplyBatch(
        std::span<const EventInput> inputs) noexcept;
    [[nodiscard]] bool ApplyGapOpen(const GapOpen& gap) noexcept;
    [[nodiscard]] bool ApplyChannelSeal(const ChannelSeal& seal) noexcept;
    [[nodiscard]] bool ContinueEviction() noexcept;
    [[nodiscard]] bool AdvanceRepair() noexcept;
    // Advance closes only full or timed-out derived persistence groups. Flush
    // also submits a final partial group and is reserved for quiescent
    // shutdown and explicit derived-persistence barriers. Neither method
    // consults raw persistence state or waits for a raw ACK.
    [[nodiscard]] bool AdvanceDurableCommits() noexcept;
    [[nodiscard]] bool FlushDurableCommits() noexcept;
    [[nodiscard]] bool repair_pending() const noexcept;
    [[nodiscard]] bool eviction_pending() const noexcept;
    // True while a journal cut is being phase-normalized or a sliced Shanghai
    // END still indexes its fixed candidate prefix. Ordinary order repair does
    // not fence later owner input.
    [[nodiscard]] bool projection_input_fenced() const noexcept;

    [[nodiscard]] bool CopyBundle(
        const FactKey& key,
        std::vector<std::pair<EventKey, EventPayload>>* output) const;
    [[nodiscard]] bool CopyOrder(
        const OrderKey& key,
        OrderSnapshot* output) const noexcept;
    [[nodiscard]] bool CopyChannelState(
        Market market,
        std::uint32_t channel,
        EventChannelState* output) const noexcept;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] EventWorkerStats stats() const noexcept;
    [[nodiscard]] const EventWorkerConfig& config() const noexcept;

private:
    class Impl;
    explicit EventWorker(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::event
