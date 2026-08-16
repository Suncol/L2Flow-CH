#pragma once

#include "l2flow/outbox/types.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace l2flow::outbox {

enum class RequestKind : std::uint8_t {
    kEventRevision = 1U,
    kEventMarker = 2U,
    kKLineRevision = 3U,
    kKLineMarker = 4U,
};

enum class RequestState : std::uint8_t {
    kPrepared = 1U,
    kSent = 2U,
    kUnknown = 3U,
    kAcked = 4U,
    kBlocked = 5U,
};

struct RequestSpoolConfig final {
    std::filesystem::path directory;
    std::uint64_t maximum_bytes = 4ULL * 1'024ULL * 1'024ULL * 1'024ULL;
};

struct RequestPayload final {
    RequestKind kind = RequestKind::kEventRevision;
    std::uint64_t rows = 0U;
    std::string_view query_id;
    std::string_view dedup_token;
    std::span<const std::byte> payload;
};

struct RequestGroupHandle final {
    std::uint64_t sequence = 0U;
    std::uint32_t request_count = 0U;
};

struct RequestSpoolStats final {
    std::uint64_t groups_prepared = 0U;
    std::uint64_t groups_retired = 0U;
    std::uint64_t state_updates = 0U;
    std::uint64_t preparing_groups = 0U;
    std::uint64_t live_groups = 0U;
    std::uint64_t reserved_bytes = 0U;
    std::uint64_t live_bytes = 0U;
    std::uint64_t checksum_ns = 0U;
    std::uint64_t encode_copy_ns = 0U;
    std::uint64_t write_ns = 0U;
    std::uint64_t fdatasync_ns = 0U;
    std::uint64_t registry_lock_wait_ns = 0U;
    std::uint64_t entry_lock_wait_ns = 0U;
};

[[nodiscard]] bool ValidateRequestSpoolConfig(
    const RequestSpoolConfig& config,
    std::string* error) noexcept;

// Run-scoped request journal. A whole physical INSERT group is serialized and
// fdatasync'ed before its first request may be sent. The immutable section
// retains the exact RowBinary bytes, query_id, dedup token, payload checksum,
// row count, and causal WAL positions. State bytes are updated in place for
// live-run diagnosis, but are intentionally not fdatasync'ed individually:
// this spool is not reopened after a process crash, and the canonical WAL is
// the durable handoff boundary.
class RequestSpool final {
public:
    ~RequestSpool();
    RequestSpool(const RequestSpool&) = delete;
    RequestSpool& operator=(const RequestSpool&) = delete;

    [[nodiscard]] static std::unique_ptr<RequestSpool> Create(
        RequestSpoolConfig config,
        std::string* error);

    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool Stop(std::string* error) noexcept;

    [[nodiscard]] bool PrepareGroup(
        ConsumerKind consumer,
        std::span<const WalPosition> input_positions,
        std::span<const RequestPayload> requests,
        RequestGroupHandle* handle,
        std::string* error) noexcept;

    [[nodiscard]] bool SetState(RequestGroupHandle handle,
                                std::uint32_t request_index,
                                RequestState state,
                                std::string* error) noexcept;

    // Retire is legal only after every request is ACKED. The file is unlinked
    // only after the owning consumer cursor has accepted all causal inputs.
    [[nodiscard]] bool Retire(RequestGroupHandle handle,
                              std::string* error) noexcept;

    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] RequestSpoolStats stats() const noexcept;
    [[nodiscard]] const RequestSpoolConfig& config() const noexcept;

private:
    class Impl;
    explicit RequestSpool(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::outbox
