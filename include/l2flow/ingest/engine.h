#pragma once

#include "l2flow/ingest/canonical.h"
#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/message.h"
#include "l2flow/outbox/wal.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

namespace l2flow::ingest {

struct EngineConfig final {
    std::uint32_t trade_date = 0U;
    // One engine instance owns exactly one externally allocated feed epoch.
    // SequenceRecovery state is never shared across epochs.
    std::uint64_t feed_session_epoch = 0U;
    StartMode start_mode = StartMode::kFromOpen;
    StreamMask enabled_streams = kDefaultAShareL2StreamMask;

    std::size_t tick_decoder_lanes = 12U;
    std::size_t snapshot_decoder_lanes = 4U;
    std::size_t instrument_workers = 16U;

    std::size_t tick_slots_per_lane = 32'768U;
    std::size_t snapshot_slots_per_lane = 512U;
    std::size_t maximum_tick_body_bytes = 512U;
    std::size_t maximum_snapshot_body_bytes = 64U * 1024U;
    std::size_t maximum_channels_per_tick_lane = 256U;
    std::size_t reorder_entries_per_channel = 4'096U;
    std::uint64_t maximum_reorder_span = 4'096U;

    // These are bounded-reorder policies, not MDL protocol constants.
    // The initial hold applies only to PARTIAL. Both gap waits default to
    // 20 milliseconds, but remain separate deployment policies so measured
    // upstream backfill distributions can tune them independently.
    std::uint64_t partial_initial_hold_ns = 200'000U;
    std::uint64_t partial_gap_wait_ns = 20'000'000U;
    std::uint64_t from_open_gap_wait_ns = 20'000'000U;
    std::uint64_t recovery_timer_scan_ns = 25'000U;

    // Mandatory durability boundary. Decoder lanes enqueue only finalized
    // canonical/disposition records; no raw/Event/KLine sink is called from a
    // decoder thread.
    outbox::DurableOutbox* outbox = nullptr;

    std::size_t maximum_text_bytes = kMaximumIdentityBytes;
    std::size_t maximum_depth_items = 4'096U;
    std::size_t maximum_queue_items = 1'000'000U;

    // -1 leaves scheduling to the OS. Otherwise lane i is pinned to
    // first_decoder_cpu + i on Linux.
    int first_decoder_cpu = -1;
};

struct EngineStats final {
    std::uint64_t callbacks = 0U;
    std::uint64_t admitted = 0U;
    std::uint64_t rejected = 0U;
    std::uint64_t lane_full = 0U;
    std::uint64_t decoded_ticks = 0U;
    std::uint64_t decoded_snapshots = 0U;
    std::uint64_t decode_errors = 0U;
    std::uint64_t catalog_misses = 0U;
    std::uint64_t dispatched_ticks = 0U;
    std::uint64_t dispatched_snapshots = 0U;
    std::uint64_t rejected_late_facts = 0U;
    std::uint64_t hole_fills_dispatched = 0U;
    std::uint64_t source_channel_controls = 0U;
    std::uint64_t owner_control_deliveries = 0U;
    std::uint64_t expired_hole_sequences = 0U;
    std::uint64_t gaps_skipped = 0U;
    std::uint64_t from_open_channels_frozen = 0U;
    std::uint64_t channel_faults_dispatched = 0U;
    std::uint64_t outbox_overflows = 0U;
};

class IngestEngine final {
public:
    ~IngestEngine();
    IngestEngine(const IngestEngine&) = delete;
    IngestEngine& operator=(const IngestEngine&) = delete;

    [[nodiscard]] static std::unique_ptr<IngestEngine> Create(
        EngineConfig config,
        InstrumentCatalog catalog,
        std::string* error);

    [[nodiscard]] bool Start(std::string* error);
    void Stop() noexcept;

    // Exactly one producer thread may call AdmitMdlMessage. Production uses
    // an MDL Subscriber created with multithread_callback=false. A concurrent
    // call is detected and rejected instead of being serialized silently.
    [[nodiscard]] AdmissionResult AdmitMdlMessage(
        std::span<const std::byte> header,
        std::span<const std::byte> body,
        std::uint64_t receive_monotonic_ns) noexcept;

    // Establishes a cut across every decoder lane without stopping SDK
    // admission. On success, every message admitted before the cut has either
    // produced its final WAL record/control set or reached a terminal decode
    // outcome. A freshness barrier may be enqueued only after this succeeds.
    [[nodiscard]] bool FenceAcceptedInputs(
        std::uint64_t timeout_ns,
        std::string* error) noexcept;

    [[nodiscard]] EngineStats stats() const noexcept;
    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] const EngineConfig& config() const noexcept;

private:
    class Impl;
    explicit IngestEngine(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept;

}  // namespace l2flow::ingest
