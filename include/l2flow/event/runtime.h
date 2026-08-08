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
    std::size_t micro_batch_rows = 256U;
    std::uint64_t micro_batch_max_delay_ns = 1'000'000U;
    std::size_t maximum_raw_ack_backlog_per_owner = 65'536U;
    std::size_t maximum_late_backlog_per_owner = 4'096U;
};

struct EventRuntimeStats final {
    std::uint64_t normal_ticks_received = 0U;
    std::uint64_t late_ticks_received = 0U;
    std::uint64_t raw_tick_acks_received = 0U;
    std::uint64_t micro_batches_applied = 0U;
    std::uint64_t source_conflicts = 0U;
    std::uint64_t invalid_inputs = 0U;
    EventWorkerStats workers{};
};

[[nodiscard]] bool ValidateEventRuntimeConfig(
    const EventRuntimeConfig& config,
    std::string* error) noexcept;

// Normal methods for owner i are single-caller and belong to owner i's drain
// thread. LateRecovery and raw ACK admission are concurrency-safe producer
// endpoints; they are copied into the destination owner's private inbox and
// applied only by that owner.
class EventRuntime final : public ingest::RawTickBatchAckListener {
public:
    ~EventRuntime() override;
    EventRuntime(const EventRuntime&) = delete;
    EventRuntime& operator=(const EventRuntime&) = delete;

    [[nodiscard]] static std::unique_ptr<EventRuntime> Create(
        EventRuntimeConfig config,
        EventRevisionSink* sink,
        std::string* error);

    [[nodiscard]] bool AppendTick(
        std::size_t owner,
        const ingest::CanonicalTick& tick) noexcept;
    [[nodiscard]] bool AppendLateRecovery(
        const ingest::LateRecoveryTick& late) noexcept;
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
