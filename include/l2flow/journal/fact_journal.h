#pragma once

#include "l2flow/ingest/canonical.h"

#include <compare>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace l2flow::journal {

inline constexpr std::size_t kFactJournalPageEntries = 4'096U;
inline constexpr std::size_t kFactJournalFileHeaderBytes = 32U;
inline constexpr std::size_t kFactJournalRecordHeaderBytes = 24U;
inline constexpr std::size_t kCanonicalTickEncodedBytes = 313U;
inline constexpr std::size_t kFactJournalRecordBytes = 344U;

struct FactKey final {
    std::uint32_t trade_date = 0U;
    ingest::Market market = ingest::Market::kUnknown;
    std::uint32_t channel = 0U;
    std::uint64_t native_sequence = 0U;

    friend constexpr bool operator==(const FactKey&, const FactKey&) =
        default;
    friend constexpr auto operator<=>(const FactKey&, const FactKey&) =
        default;
};

[[nodiscard]] FactKey MakeFactKey(
    const ingest::CanonicalTick& tick) noexcept;

// This is the authoritative Event/KLine fact identity predicate. Arrival
// provenance, recovery annotations, and derived canonical values are
// intentionally excluded. A matching fingerprint is only a lookup filter;
// callers must use FactPayloadEqual for the final collision-safe decision.
[[nodiscard]] bool FactPayloadEqual(
    const ingest::CanonicalTick& left,
    const ingest::CanonicalTick& right) noexcept;
[[nodiscard]] std::uint64_t FactPayloadFingerprint(
    const ingest::CanonicalTick& tick) noexcept;

enum class FactConsumer : std::uint8_t {
    kEvent,
    kKLine,
};

enum class AdmitCode : std::uint8_t {
    // First observation by this consumer. The canonical record may already
    // have been physically appended by the other consumer.
    kNew,
    kDuplicate,
    kConflict,
    kFailed,
};

struct FactHandle final {
    std::uint64_t offset = 0U;

    friend constexpr bool operator==(const FactHandle&, const FactHandle&) =
        default;
};

struct AdmitResult final {
    AdmitCode code = AdmitCode::kFailed;
    FactHandle handle{};
};

struct FactJournalConfig final {
    std::uint32_t trade_date = 0U;
    std::filesystem::path path;
    std::size_t hot_cache_entries = 65'536U;
    // Global physical-record limit for the configured trading day. Consumer
    // seen bits do not consume additional records.
    std::uint64_t maximum_records = 600'000'000U;
    // Each sparse (market, channel, native_sequence / 4096) range allocates a
    // 64 KiB directory page. Bound pages separately so a sparse feed cannot
    // exhaust RAM before maximum_records is reached. 262,144 pages cap the
    // page payload at 16 GiB; production must size this from observed channel
    // sequence-page occupancy, not only from record count.
    std::uint64_t maximum_directory_pages = 262'144U;
};

struct FactJournalStats final {
    std::uint64_t records = 0U;
    std::uint64_t record_bytes = 0U;
    std::uint64_t file_bytes = 0U;
    std::uint64_t consumer_new = 0U;
    std::uint64_t duplicates = 0U;
    std::uint64_t conflicts = 0U;
    std::uint64_t write_calls = 0U;
    std::uint64_t write_bytes = 0U;
    std::uint64_t partial_writes = 0U;
    std::uint64_t read_calls = 0U;
    std::uint64_t read_bytes = 0U;
    std::uint64_t partial_reads = 0U;
    std::uint64_t hot_cache_hits = 0U;
    std::uint64_t flush_calls = 0U;
    std::uint64_t directory_channels = 0U;
    std::uint64_t directory_pages = 0U;
    std::uint64_t active_writes = 0U;
    std::uint64_t active_reads = 0U;
    std::uint64_t reserved_records = 0U;
    std::uint64_t waiting_admissions = 0U;
    std::uint64_t errors = 0U;
};

class CanonicalFactJournal final {
public:
    // Create takes a nonblocking exclusive file lock before truncating path
    // and starts an empty, process-local directory for exactly one trade date.
    // A concurrent process using the same path is rejected without truncating
    // the live journal. This API does not reopen or replay an earlier journal
    // and therefore is not a crash-recovery boundary.
    [[nodiscard]] static std::unique_ptr<CanonicalFactJournal> Create(
        FactJournalConfig config,
        std::string* error);

    ~CanonicalFactJournal();

    CanonicalFactJournal(const CanonicalFactJournal&) = delete;
    CanonicalFactJournal& operator=(const CanonicalFactJournal&) = delete;
    CanonicalFactJournal(CanonicalFactJournal&&) = delete;
    CanonicalFactJournal& operator=(CanonicalFactJournal&&) = delete;

    // The batch is serialized into one contiguous append and normally issued
    // with one pwrite. No per-record sync is performed. Within and across
    // batches, the first canonical business payload wins for a FactKey. When
    // authoritative_winners is nonempty it must not overlap ticks and must
    // match ticks.size(); entries classified kNew or kDuplicate receive the
    // complete first winner, including arrival provenance ignored by
    // FactPayloadEqual.
    [[nodiscard]] std::vector<AdmitResult> AdmitBatch(
        FactConsumer consumer,
        std::span<const ingest::CanonicalTick> ticks,
        std::span<ingest::CanonicalTick> authoritative_winners = {});

    // Read checks the bounded hot cache first, then performs and validates a
    // complete record pread. Any I/O or integrity error makes the journal
    // permanently unhealthy and all later operations fail closed.
    [[nodiscard]] bool Read(FactHandle handle,
                            ingest::CanonicalTick* output);

    // Establishes the explicit local durability boundary with fdatasync.
    // Durable raw storage remains the source of truth unless the caller gates
    // publication on a successful Flush.
    [[nodiscard]] bool Flush();

    // Benchmark/test controls. EvictHotCache removes one decoded record and
    // ClearHotCache removes the entire decoded-record cache. DropFileCache
    // issues POSIX_FADV_DONTNEED after a successful Flush; it is advisory and
    // cannot prove that a later pread reached the physical storage device.
    void EvictHotCache(FactHandle handle);
    void ClearHotCache();
    [[nodiscard]] bool DropFileCache();

    [[nodiscard]] bool healthy() const;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] FactJournalStats stats() const;

private:
    struct Impl;
    explicit CanonicalFactJournal(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::journal
