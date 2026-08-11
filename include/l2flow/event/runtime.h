#pragma once

#include "l2flow/event/worker.h"
#include "l2flow/ingest/raw_tap.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::event {

struct EventRuntimeConfig final {
    // `worker.owner_count` is the number of single-writer Event state actors.
    // The ingest engine's instrument_workers must use the same count so one
    // catalog-resolved security (instrument_ordinal) has exactly one mutable
    // owner. Increase that count to add actors; do not add locks around one
    // worker or split an order chain across threads.
    EventWorkerConfig worker{};
    std::uint64_t feed_session_epoch = 0U;
    std::size_t micro_batch_rows = 512U;
    std::uint64_t micro_batch_max_delay_ns = UINT64_C(50'000'000);
    // The worker raw-ACK index must cover the largest forwarding cut formed
    // by the inbox, occurrence join, and micro-batch limits.
    std::size_t maximum_raw_ack_backlog_per_owner = 65'536U;
    std::size_t maximum_occurrence_join_entries_per_owner = 65'536U;
    // ChannelSeal is an eviction watermark, so consecutive seals for one
    // channel/generation may share one owner-local mailbox entry. GapOpen is
    // never coalesced because the FactJournal does not persist gap controls.
    std::size_t maximum_pending_channel_seals_per_owner = 4'096U;
    // Capacity and online service work are separate bounds. The owner checks
    // the CPU deadline in coarse blocks to avoid one clock read per ACK.
    std::size_t raw_ack_drain_max_entries = 1'024U;
    std::uint64_t raw_ack_drain_max_cpu_ns = 150'000U;
};

struct EventRuntimeStats final {
    std::uint64_t ordered_dispositions_received = 0U;
    std::uint64_t hole_fill_dispositions_received = 0U;
    std::uint64_t rejected_dispositions_received = 0U;
    std::uint64_t gap_open_controls_received = 0U;
    std::uint64_t channel_seal_controls_received = 0U;
    std::uint64_t channel_seals_applied = 0U;
    std::uint64_t channel_seals_coalesced = 0U;
    std::uint64_t pending_channel_seals = 0U;
    std::uint64_t pending_channel_seals_high_water = 0U;
    std::uint64_t raw_tick_acks_received = 0U;
    std::uint64_t raw_ack_inbox_backlog = 0U;
    std::uint64_t occurrence_join_entries = 0U;
    std::uint64_t occurrence_join_high_water = 0U;
    std::uint64_t occurrence_ack_first = 0U;
    std::uint64_t occurrence_disposition_first = 0U;
    std::uint64_t occurrence_projects_resolved = 0U;
    std::uint64_t occurrence_rejections_resolved = 0U;
    std::uint64_t occurrence_duplicate_sides = 0U;
    std::uint64_t micro_batches_applied = 0U;
    std::uint64_t facts_in_micro_batches = 0U;
    std::uint64_t micro_batch_rows_max = 0U;
    std::uint64_t micro_batch_source_age_ns_max = 0U;
    std::uint64_t row_limit_flushes = 0U;
    std::uint64_t timer_flushes = 0U;
    // Controls that closed a nonempty computation cut, and controls that found
    // no active cut and were applied/staged directly, respectively.
    std::uint64_t forced_active_flushes = 0U;
    std::uint64_t empty_control_flushes = 0U;
    std::uint64_t explicit_flushes = 0U;
    std::uint64_t raw_ack_drain_slices = 0U;
    std::uint64_t raw_ack_entries_drained = 0U;
    std::uint64_t raw_ack_drain_entries_max = 0U;
    std::uint64_t raw_ack_drain_cpu_ns_max = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t invalid_inputs = 0U;
    EventWorkerStats workers{};
};

[[nodiscard]] bool ValidateEventRuntimeConfig(
    const EventRuntimeConfig& config,
    std::string* error) noexcept;

// AppendDispatch for owner i is single-caller and belongs to owner i's drain
// thread. Raw ACK admission is a concurrency-safe producer endpoint; ACKs are
// joined with SequenceRecovery dispositions only by the destination owner.
class EventRuntime final : public ingest::RawTickBatchAckListener {
public:
    ~EventRuntime() override;
    EventRuntime(const EventRuntime&) = delete;
    EventRuntime& operator=(const EventRuntime&) = delete;

    [[nodiscard]] static std::unique_ptr<EventRuntime> Create(
        EventRuntimeConfig config,
        EventRevisionSink* sink,
        std::string* error);

    [[nodiscard]] bool AppendDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept;
    // False while the owner must finish a projection cut or apply a control
    // already removed from the decoder-to-owner FIFO. The owner drain thread
    // must not poll another TickDispatch until this returns true.
    [[nodiscard]] bool CanPollDispatch(std::size_t owner) const noexcept;
    [[nodiscard]] bool FlushDue(
        std::size_t owner,
        std::uint64_t monotonic_ns) noexcept;
    [[nodiscard]] bool Flush(std::size_t owner) noexcept;

    // Call with owner threads quiesced. DrainAll is useful after the raw sink
    // has stopped and delivered the final partial-batch ACKs.
    [[nodiscard]] bool FlushAll() noexcept;
    [[nodiscard]] bool DrainAll() noexcept;

    [[nodiscard]] bool OnRawTickBatchAcknowledged(
        std::span<const ingest::CanonicalTick> ticks) noexcept override;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] EventRuntimeStats stats() const noexcept;
    [[nodiscard]] EventWorker* worker(std::size_t owner) noexcept;
    // Runtime routing helper.  The ordinal is catalog-local and is only used
    // for the current actor assignment; Event/ClickHouse identity remains
    // instrument_id.
    [[nodiscard]] std::size_t owner_for_instrument(
        std::uint32_t instrument_ordinal) const noexcept;
    [[nodiscard]] const EventRuntimeConfig& config() const noexcept;

private:
    class Impl;
    explicit EventRuntime(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::event
