#pragma once

#include "l2flow/event/types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace l2flow::event {

struct EventInput final {
    ingest::CanonicalTick tick{};
    std::uint64_t committed_next_sequence = 0U;
    std::uint64_t observed_gap_epoch = 0U;
    bool late_recovery = false;
    // SequenceRecovery has already classified this body as conflicting with
    // the retained pending canonical fact. Keep its raw durability dependency
    // ordered, but never let it become the authoritative Event fact.
    bool upstream_conflict = false;
    bool catalog_match = true;
};

struct EventWorkerConfig final {
    std::uint32_t trade_date = 0U;
    std::uint32_t owner = 0U;
    std::uint32_t owner_count = 0U;
    std::uint32_t revision_epoch = 0U;
    std::uint32_t logic_version = 1U;
    Identifier128 calculation_run_id{};

    std::size_t maximum_facts = 4U * 1'024U * 1'024U;
    std::size_t maximum_orders = 2U * 1'024U * 1'024U;
    std::size_t maximum_cached_events = 16U * 1'024U * 1'024U;
    std::size_t maximum_pending_commits = 1'024U;
    std::size_t maximum_acknowledged_raw_dependencies = 65'536U;

    // A repair slice always executes at least one order-use node. Both limits
    // are then enforced before control returns to the owner loop.
    std::size_t repair_slice_max_order_uses = 4'096U;
    std::uint64_t repair_slice_max_cpu_ns = 500'000U;
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
    std::uint64_t pending_raw_commits = 0U;
    std::uint64_t acknowledged_raw_dependencies = 0U;
    std::uint64_t revision_batches_submitted = 0U;
    std::uint64_t active_repair_orders = 0U;
};

// Owner-local view of one upstream Channel. journal_tail includes every
// accepted fact in the worker. projected_frontier advances only after the
// corresponding journal cut has been projected into the live caches.
// committed_next_sequence and gap_epoch are monotone observations of the
// SequenceRecovery boundary, not claims that historical repair is closed.
struct EventChannelState final {
    std::uint64_t journal_tail = 0U;
    std::uint64_t projected_frontier = 0U;
    std::uint64_t committed_next_sequence = 0U;
    std::uint64_t gap_epoch = 0U;
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
    [[nodiscard]] bool AdvanceRepair() noexcept;
    void AcknowledgeRawTicks(
        std::span<const RawTickDependency> dependencies) noexcept;
    [[nodiscard]] bool DrainDurableCommits() noexcept;
    [[nodiscard]] bool repair_pending() const noexcept;

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
