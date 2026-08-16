#include "l2flow/outbox/request_spool.h"

#include "l2flow/checksum/crc32c.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace l2flow::outbox {
namespace {

constexpr std::array<std::byte, 8U> kSpoolMagic{
    std::byte{'L'}, std::byte{'2'}, std::byte{'R'}, std::byte{'E'},
    std::byte{'Q'}, std::byte{'G'}, std::byte{'R'}, std::byte{1U}};
constexpr std::array<std::byte, 8U> kSpoolFooter{
    std::byte{'L'}, std::byte{'2'}, std::byte{'R'}, std::byte{'E'},
    std::byte{'Q'}, std::byte{'E'}, std::byte{'N'}, std::byte{1U}};
constexpr std::uint32_t kSpoolFormatVersion = 1U;
constexpr std::uint64_t kMinimumSpoolBytes = 1U * 1'024U * 1'024U;
constexpr std::uint64_t kHeaderBytes = 40U;
constexpr std::uint64_t kStateBytes = 2U;
constexpr std::uint64_t kPositionBytes = 20U;
constexpr std::uint64_t kRequestHeaderBytes = 36U;
constexpr std::uint64_t kFooterBytes = 24U;

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return count > 0 ? static_cast<std::uint64_t>(count) : 0U;
}

[[nodiscard]] std::uint64_t ElapsedSince(std::uint64_t started) noexcept {
    const std::uint64_t completed = MonotonicNowNs();
    return completed >= started ? completed - started : 0U;
}

template <typename Function>
class ScopeExit final {
public:
    explicit ScopeExit(Function function) noexcept
        : function_(std::move(function)) {}
    ~ScopeExit() {
        if (active_) {
            function_();
        }
    }
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;

    void Release() noexcept { active_ = false; }

private:
    Function function_;
    bool active_ = true;
};

template <typename Function>
ScopeExit(Function) -> ScopeExit<Function>;

template <typename Value>
void AppendLe(std::vector<std::byte>* output, Value value) {
    static_assert(std::is_integral_v<Value> || std::is_enum_v<Value>);
    if constexpr (std::is_enum_v<Value>) {
        AppendLe(output, static_cast<std::underlying_type_t<Value>>(value));
    } else {
        using Unsigned = std::make_unsigned_t<Value>;
        const Unsigned encoded = static_cast<Unsigned>(value);
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            output->push_back(static_cast<std::byte>(
                encoded >> static_cast<unsigned int>(index * 8U)));
        }
    }
}

void AppendBytes(std::vector<std::byte>* output,
                 std::span<const std::byte> bytes) {
    output->insert(output->end(), bytes.begin(), bytes.end());
}

void AppendString(std::vector<std::byte>* output, std::string_view value) {
    AppendBytes(output, std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(value.data()), value.size()));
}

[[nodiscard]] bool CheckedAdd(std::uint64_t value,
                              std::uint64_t* total) noexcept {
    if (total == nullptr || value > UINT64_MAX - *total) {
        return false;
    }
    *total += value;
    return true;
}

[[nodiscard]] bool CheckedMultiply(std::uint64_t left,
                                   std::uint64_t right,
                                   std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (left != 0U && right > UINT64_MAX / left)) {
        return false;
    }
    *output = left * right;
    return true;
}

[[nodiscard]] bool EncodedFileBytes(
    std::span<const WalPosition> input_positions,
    std::span<const RequestPayload> requests,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        input_positions.size() > UINT64_MAX ||
        requests.size() > UINT64_MAX) {
        return false;
    }
    std::uint64_t bytes = kHeaderBytes;
    std::uint64_t section = 0U;
    if (!CheckedMultiply(static_cast<std::uint64_t>(requests.size()),
                         kStateBytes, &section) ||
        !CheckedAdd(section, &bytes) ||
        !CheckedMultiply(static_cast<std::uint64_t>(input_positions.size()),
                         kPositionBytes, &section) ||
        !CheckedAdd(section, &bytes)) {
        return false;
    }
    for (const RequestPayload& request : requests) {
        if (request.query_id.size() > UINT64_MAX ||
            request.dedup_token.size() > UINT64_MAX ||
            request.payload.size() > UINT64_MAX ||
            !CheckedAdd(kRequestHeaderBytes, &bytes) ||
            !CheckedAdd(static_cast<std::uint64_t>(request.query_id.size()),
                        &bytes) ||
            !CheckedAdd(
                static_cast<std::uint64_t>(request.dedup_token.size()),
                &bytes) ||
            !CheckedAdd(static_cast<std::uint64_t>(request.payload.size()),
                        &bytes)) {
            return false;
        }
    }
    if (!CheckedAdd(kFooterBytes, &bytes) ||
        bytes > static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
        bytes > static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
        return false;
    }
    *output = bytes;
    return true;
}

[[nodiscard]] bool WriteAllAt(int descriptor,
                              std::span<const std::byte> bytes,
                              std::uint64_t offset,
                              std::string* error) noexcept {
    std::size_t written = 0U;
    while (written < bytes.size()) {
        const std::uint64_t maximum_offset = static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max());
        if (offset > maximum_offset || written > maximum_offset - offset) {
            if (error != nullptr) {
                *error = "request spool offset exceeds off_t";
            }
            return false;
        }
        const std::uint64_t target = offset + written;
        const std::size_t remaining = bytes.size() - written;
        const std::size_t request_bytes = std::min(
            remaining,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pwrite(
            descriptor, bytes.data() + written, request_bytes,
            static_cast<off_t>(target));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (error != nullptr) {
                *error = std::string("request spool pwrite failed: ") +
                         std::strerror(errno);
            }
            return false;
        }
        if (count == 0) {
            if (error != nullptr) {
                *error = "request spool pwrite made no progress";
            }
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool SyncFile(int descriptor, std::string* error) noexcept {
    for (;;) {
        if (::fdatasync(descriptor) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (error != nullptr) {
            *error = std::string("request spool fdatasync failed: ") +
                     std::strerror(errno);
        }
        return false;
    }
}

[[nodiscard]] bool SyncDirectory(int descriptor,
                                 std::string* error) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (error != nullptr) {
            *error = std::string("request spool directory fsync failed: ") +
                     std::strerror(errno);
        }
        return false;
    }
}

void CloseNoThrow(int* descriptor) noexcept {
    if (descriptor == nullptr || *descriptor < 0) {
        return;
    }
    const int value = *descriptor;
    *descriptor = -1;
    // On Linux an EINTR return may already have released the descriptor;
    // retrying the same integer can close an unrelated concurrently opened
    // file after descriptor reuse.
    static_cast<void>(::close(value));
}

[[nodiscard]] bool ValidRequestKind(RequestKind kind,
                                    ConsumerKind consumer) noexcept {
    if (consumer == ConsumerKind::kEvent) {
        return kind == RequestKind::kEventRevision ||
               kind == RequestKind::kEventMarker;
    }
    if (consumer == ConsumerKind::kKLine) {
        return kind == RequestKind::kKLineRevision ||
               kind == RequestKind::kKLineMarker;
    }
    return false;
}

[[nodiscard]] bool ValidTransition(RequestState from,
                                   RequestState to) noexcept {
    switch (from) {
        case RequestState::kPrepared:
            return to == RequestState::kSent ||
                   to == RequestState::kBlocked;
        case RequestState::kSent:
            return to == RequestState::kUnknown ||
                   to == RequestState::kAcked ||
                   to == RequestState::kBlocked;
        case RequestState::kUnknown:
            return to == RequestState::kSent ||
                   to == RequestState::kBlocked;
        case RequestState::kAcked:
        case RequestState::kBlocked:
            return false;
    }
    return false;
}

}  // namespace

bool ValidateRequestSpoolConfig(const RequestSpoolConfig& config,
                                std::string* error) noexcept {
    const auto fail = [error](const char* message) noexcept {
        if (error != nullptr) {
            try {
                *error = message;
            } catch (...) {
            }
        }
        return false;
    };
    if (config.directory.empty() ||
        config.maximum_bytes < kMinimumSpoolBytes) {
        return fail("request spool directory or byte bound is invalid");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

class RequestSpool::Impl final {
public:
    struct Entry final {
        std::mutex mutex;
        int descriptor = -1;
        std::filesystem::path path;
        std::uint64_t bytes = 0U;
        std::vector<std::uint64_t> state_offsets;
        std::vector<RequestState> states;
        bool retiring = false;
    };

    explicit Impl(RequestSpoolConfig config) : config_(std::move(config)) {}

    ~Impl() {
        std::string ignored;
        static_cast<void>(Stop(&ignored));
    }

    [[nodiscard]] bool Start(std::string* error) {
        std::unique_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return Fail("request spool can be started exactly once", error);
        }
        try {
            std::filesystem::create_directories(config_.directory);
            directory_descriptor_ = ::open(
                config_.directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (directory_descriptor_ < 0) {
                return Fail(
                    std::string("request spool directory open failed: ") +
                        std::strerror(errno),
                    error);
            }
            if (!SyncDirectory(directory_descriptor_, error)) {
                return Fail(error == nullptr
                                ? "request spool directory sync failed"
                                : *error,
                            error);
            }
            accepting_.store(true, std::memory_order_release);
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (const std::exception& exception) {
            return Fail(std::string("request spool start failed: ") +
                            exception.what(),
                        error);
        } catch (...) {
            return Fail("request spool start failed", error);
        }
    }

    [[nodiscard]] bool Stop(std::string* error) noexcept {
        accepting_.store(false, std::memory_order_release);
        std::unique_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
        {
            std::lock_guard<std::mutex> entries_lock(entries_mutex_);
            for (auto& [sequence, entry] : entries_) {
                static_cast<void>(sequence);
                CloseNoThrow(&entry->descriptor);
            }
        }
        CloseNoThrow(&directory_descriptor_);
        if (error != nullptr) {
            try {
                *error = healthy() ? std::string{} : fatal_error();
            } catch (...) {
            }
        }
        return healthy();
    }

    [[nodiscard]] bool PrepareGroup(
        ConsumerKind consumer,
        std::span<const WalPosition> input_positions,
        std::span<const RequestPayload> requests,
        RequestGroupHandle* handle,
        std::string* error) noexcept {
        std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
        if (!accepting_.load(std::memory_order_acquire) || !healthy() ||
            handle == nullptr || input_positions.empty() || requests.empty() ||
            requests.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
            return Fail("invalid request spool group", error);
        }
        *handle = RequestGroupHandle{};
        for (std::size_t index = 0U; index < input_positions.size(); ++index) {
            if (input_positions[index].lsn == 0U ||
                (index != 0U &&
                 !(input_positions[index - 1U] < input_positions[index]))) {
                return Fail(
                    "request spool positions must be nonzero, unique, and ordered",
                    error);
            }
        }
        for (const RequestPayload& request : requests) {
            if (!ValidRequestKind(request.kind, consumer) || request.rows == 0U ||
                request.query_id.empty() || request.dedup_token.empty() ||
                request.payload.empty() ||
                request.query_id.size() >
                    static_cast<std::size_t>(
                        std::numeric_limits<std::uint32_t>::max()) ||
                request.dedup_token.size() >
                    static_cast<std::size_t>(
                        std::numeric_limits<std::uint32_t>::max())) {
                return Fail("invalid durable ClickHouse request", error);
            }
        }

        std::uint64_t file_bytes = 0U;
        if (!EncodedFileBytes(input_positions, requests, &file_bytes)) {
            return Fail("request spool encoded size overflow", error);
        }
        if (!Reserve(file_bytes, error)) {
            return false;
        }
        ScopeExit reservation_guard([this, file_bytes] {
            preparing_groups_.fetch_sub(1U, std::memory_order_relaxed);
            reserved_bytes_.fetch_sub(file_bytes, std::memory_order_relaxed);
        });

        const std::uint64_t sequence = next_sequence_.fetch_add(
            1U, std::memory_order_relaxed);
        if (sequence == 0U) {
            return Fail("request spool sequence exhausted", error);
        }

        int descriptor = -1;
        bool cleanup_file = false;
        std::filesystem::path path;
        ScopeExit file_guard([&descriptor, &cleanup_file, &path] {
            CloseNoThrow(&descriptor);
            if (cleanup_file) {
                static_cast<void>(::unlink(path.c_str()));
            }
        });

        try {
            std::uint64_t checksum_ns = 0U;
            const auto crc32c = [&checksum_ns](
                                    std::span<const std::byte> bytes) noexcept {
                const std::uint64_t started = MonotonicNowNs();
                const std::uint32_t result = checksum::Crc32c(bytes);
                checksum_ns += ElapsedSince(started);
                return result;
            };
            const std::uint64_t encode_started = MonotonicNowNs();
            std::vector<std::byte> bytes;
            std::vector<std::uint64_t> state_offsets;
            bytes.reserve(static_cast<std::size_t>(file_bytes));
            state_offsets.reserve(requests.size());
            AppendBytes(&bytes, kSpoolMagic);
            AppendLe(&bytes, kSpoolFormatVersion);
            AppendLe(&bytes, consumer);
            AppendLe(&bytes, static_cast<std::uint8_t>(0U));
            AppendLe(&bytes, static_cast<std::uint16_t>(0U));
            AppendLe(&bytes, sequence);
            AppendLe(&bytes,
                     static_cast<std::uint64_t>(input_positions.size()));
            AppendLe(&bytes, static_cast<std::uint32_t>(requests.size()));
            AppendLe(&bytes, crc32c(bytes));

            for (std::size_t index = 0U; index < requests.size(); ++index) {
                state_offsets.push_back(
                    static_cast<std::uint64_t>(bytes.size()));
                AppendLe(&bytes, RequestState::kPrepared);
                AppendLe(&bytes, static_cast<std::uint8_t>(
                    ~static_cast<std::uint8_t>(RequestState::kPrepared)));
            }
            const std::size_t immutable_offset = bytes.size();

            for (const WalPosition position : input_positions) {
                AppendLe(&bytes, position.lsn);
                AppendLe(&bytes, position.batch_sequence);
                AppendLe(&bytes, position.row_index);
            }
            for (const RequestPayload& request : requests) {
                AppendLe(&bytes, request.kind);
                AppendLe(&bytes, static_cast<std::uint8_t>(0U));
                AppendLe(&bytes, static_cast<std::uint16_t>(0U));
                AppendLe(&bytes, request.rows);
                AppendLe(&bytes,
                         static_cast<std::uint32_t>(request.query_id.size()));
                AppendLe(
                    &bytes,
                    static_cast<std::uint32_t>(request.dedup_token.size()));
                AppendLe(&bytes,
                         static_cast<std::uint64_t>(request.payload.size()));
                AppendLe(&bytes, crc32c(request.payload));
                AppendLe(&bytes, static_cast<std::uint32_t>(0U));
                AppendString(&bytes, request.query_id);
                AppendString(&bytes, request.dedup_token);
                AppendBytes(&bytes, request.payload);
            }
            const std::uint32_t group_checksum = crc32c(
                std::span<const std::byte>(bytes).subspan(immutable_offset));
            AppendBytes(&bytes, kSpoolFooter);
            AppendLe(&bytes, group_checksum);
            AppendLe(&bytes, static_cast<std::uint32_t>(0U));
            AppendLe(&bytes, file_bytes);
            if (bytes.size() != static_cast<std::size_t>(file_bytes)) {
                return Fail("request spool encoded size invariant failed",
                            error);
            }
            const std::uint64_t encode_elapsed =
                ElapsedSince(encode_started);
            checksum_ns_.fetch_add(checksum_ns, std::memory_order_relaxed);
            encode_copy_ns_.fetch_add(
                encode_elapsed >= checksum_ns
                    ? encode_elapsed - checksum_ns
                    : 0U,
                std::memory_order_relaxed);

            auto entry = std::make_shared<Entry>();
            entry->path = config_.directory /
                ("group-" + std::to_string(sequence) + ".wal");
            entry->bytes = file_bytes;
            entry->state_offsets = std::move(state_offsets);
            entry->states.assign(requests.size(), RequestState::kPrepared);
            path = entry->path;

            descriptor = ::open(path.c_str(),
                                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                                S_IRUSR | S_IWUSR | S_IRGRP);
            if (descriptor < 0) {
                return Fail(
                    std::string("request spool file create failed: ") +
                        std::strerror(errno),
                    error);
            }
            cleanup_file = true;

            const std::uint64_t write_started = MonotonicNowNs();
            const bool wrote = WriteAllAt(descriptor, bytes, 0U, error);
            write_ns_.fetch_add(ElapsedSince(write_started),
                                std::memory_order_relaxed);
            if (!wrote) {
                return Fail(error == nullptr
                                ? "request spool write failed"
                                : *error,
                            error);
            }
            const std::uint64_t sync_started = MonotonicNowNs();
            const bool synced = SyncFile(descriptor, error);
            fdatasync_ns_.fetch_add(ElapsedSince(sync_started),
                                    std::memory_order_relaxed);
            if (!synced) {
                return Fail(error == nullptr
                                ? "request spool fdatasync failed"
                                : *error,
                            error);
            }
            if (!healthy()) {
                return ReturnFatal(error);
            }

            entry->descriptor = descriptor;
            const std::uint64_t lock_started = MonotonicNowNs();
            std::unique_lock<std::mutex> entries_lock(entries_mutex_);
            registry_lock_wait_ns_.fetch_add(
                ElapsedSince(lock_started), std::memory_order_relaxed);
            if (!healthy()) {
                return ReturnFatal(error);
            }
            const auto [found, inserted] = entries_.emplace(sequence, entry);
            static_cast<void>(found);
            if (!inserted) {
                return Fail("request spool sequence collision", error);
            }
            descriptor = -1;
            cleanup_file = false;
            entries_lock.unlock();

            live_bytes_.fetch_add(file_bytes, std::memory_order_relaxed);
            live_groups_.fetch_add(1U, std::memory_order_relaxed);
            groups_prepared_.fetch_add(1U, std::memory_order_relaxed);
            preparing_groups_.fetch_sub(1U, std::memory_order_relaxed);
            reservation_guard.Release();
            handle->sequence = sequence;
            handle->request_count =
                static_cast<std::uint32_t>(requests.size());
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (const std::exception& exception) {
            return Fail(std::string("request spool prepare failed: ") +
                            exception.what(),
                        error);
        } catch (...) {
            return Fail("request spool prepare allocation failed", error);
        }
    }

    [[nodiscard]] bool SetState(RequestGroupHandle handle,
                                std::uint32_t request_index,
                                RequestState state,
                                std::string* error) noexcept {
        std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
        if (!accepting_.load(std::memory_order_acquire) || !healthy() ||
            handle.sequence == 0U || request_index >= handle.request_count) {
            return Fail("invalid request spool state update", error);
        }
        const std::shared_ptr<Entry> entry = FindEntry(handle.sequence);
        if (entry == nullptr || entry->states.size() != handle.request_count) {
            return Fail("request spool handle does not exist", error);
        }
        const std::uint64_t lock_started = MonotonicNowNs();
        std::unique_lock<std::mutex> entry_lock(entry->mutex);
        entry_lock_wait_ns_.fetch_add(ElapsedSince(lock_started),
                                      std::memory_order_relaxed);
        const std::size_t index = static_cast<std::size_t>(request_index);
        if (entry->retiring || !ValidTransition(entry->states[index], state)) {
            return Fail("invalid request spool state transition", error);
        }
        const std::array<std::byte, 2U> encoded{
            static_cast<std::byte>(state),
            static_cast<std::byte>(~static_cast<std::uint8_t>(state))};
        if (!WriteAllAt(entry->descriptor, encoded,
                        entry->state_offsets[index], error)) {
            return Fail(error == nullptr
                            ? "request spool state update failed"
                            : *error,
                        error);
        }
        entry->states[index] = state;
        state_updates_.fetch_add(1U, std::memory_order_relaxed);
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool Retire(RequestGroupHandle handle,
                              std::string* error) noexcept {
        std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
        if (!accepting_.load(std::memory_order_acquire) || !healthy() ||
            handle.sequence == 0U) {
            return Fail("invalid request spool retire", error);
        }
        const std::shared_ptr<Entry> entry = FindEntry(handle.sequence);
        if (entry == nullptr || entry->states.size() != handle.request_count) {
            return Fail("request spool handle does not exist", error);
        }
        const std::uint64_t entry_lock_started = MonotonicNowNs();
        std::unique_lock<std::mutex> entry_lock(entry->mutex);
        entry_lock_wait_ns_.fetch_add(
            ElapsedSince(entry_lock_started), std::memory_order_relaxed);
        if (entry->retiring ||
            !std::all_of(entry->states.begin(), entry->states.end(),
                         [](RequestState request_state) {
                             return request_state == RequestState::kAcked;
                         })) {
            return Fail("request spool group is not fully acknowledged",
                        error);
        }
        entry->retiring = true;
        CloseNoThrow(&entry->descriptor);
        if (::unlink(entry->path.c_str()) != 0) {
            return Fail(std::string("request spool unlink failed: ") +
                            std::strerror(errno),
                        error);
        }

        const std::uint64_t registry_lock_started = MonotonicNowNs();
        std::unique_lock<std::mutex> entries_lock(entries_mutex_);
        registry_lock_wait_ns_.fetch_add(
            ElapsedSince(registry_lock_started), std::memory_order_relaxed);
        const auto found = entries_.find(handle.sequence);
        if (found == entries_.end() || found->second != entry) {
            return Fail("request spool registry invariant failed", error);
        }
        entries_.erase(found);
        entries_lock.unlock();

        live_groups_.fetch_sub(1U, std::memory_order_relaxed);
        live_bytes_.fetch_sub(entry->bytes, std::memory_order_relaxed);
        reserved_bytes_.fetch_sub(entry->bytes, std::memory_order_relaxed);
        groups_retired_.fetch_add(1U, std::memory_order_relaxed);
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] RequestSpoolStats stats() const noexcept {
        RequestSpoolStats result{};
        result.groups_prepared =
            groups_prepared_.load(std::memory_order_relaxed);
        result.groups_retired =
            groups_retired_.load(std::memory_order_relaxed);
        result.state_updates = state_updates_.load(std::memory_order_relaxed);
        result.preparing_groups =
            preparing_groups_.load(std::memory_order_relaxed);
        result.live_groups = live_groups_.load(std::memory_order_relaxed);
        result.reserved_bytes =
            reserved_bytes_.load(std::memory_order_relaxed);
        result.live_bytes = live_bytes_.load(std::memory_order_relaxed);
        result.checksum_ns = checksum_ns_.load(std::memory_order_relaxed);
        result.encode_copy_ns =
            encode_copy_ns_.load(std::memory_order_relaxed);
        result.write_ns = write_ns_.load(std::memory_order_relaxed);
        result.fdatasync_ns = fdatasync_ns_.load(std::memory_order_relaxed);
        result.registry_lock_wait_ns =
            registry_lock_wait_ns_.load(std::memory_order_relaxed);
        result.entry_lock_wait_ns =
            entry_lock_wait_ns_.load(std::memory_order_relaxed);
        return result;
    }

    [[nodiscard]] const RequestSpoolConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool Reserve(std::uint64_t bytes,
                               std::string* error) noexcept {
        std::uint64_t current =
            reserved_bytes_.load(std::memory_order_relaxed);
        for (;;) {
            if (current > config_.maximum_bytes ||
                bytes > config_.maximum_bytes - current) {
                return Fail("request spool capacity exhausted", error);
            }
            if (reserved_bytes_.compare_exchange_weak(
                    current, current + bytes, std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                preparing_groups_.fetch_add(1U, std::memory_order_relaxed);
                return true;
            }
        }
    }

    [[nodiscard]] std::shared_ptr<Entry> FindEntry(
        std::uint64_t sequence) noexcept {
        const std::uint64_t lock_started = MonotonicNowNs();
        std::lock_guard<std::mutex> lock(entries_mutex_);
        registry_lock_wait_ns_.fetch_add(
            ElapsedSince(lock_started), std::memory_order_relaxed);
        const auto found = entries_.find(sequence);
        return found == entries_.end() ? nullptr : found->second;
    }

    [[nodiscard]] bool ReturnFatal(std::string* error) const noexcept {
        if (error != nullptr) {
            try {
                *error = fatal_error();
            } catch (...) {
            }
        }
        return false;
    }

    [[nodiscard]] bool Fail(std::string message,
                            std::string* error) noexcept {
        SetFatal(std::move(message));
        return ReturnFatal(error);
    }

    void SetFatal(std::string message) noexcept {
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            if (!healthy_.load(std::memory_order_relaxed)) {
                accepting_.store(false, std::memory_order_release);
                return;
            }
            fatal_error_ = std::move(message);
            healthy_.store(false, std::memory_order_release);
        } catch (...) {
            healthy_.store(false, std::memory_order_release);
        }
        accepting_.store(false, std::memory_order_release);
    }

    RequestSpoolConfig config_{};
    mutable std::shared_mutex lifecycle_mutex_;
    mutable std::mutex entries_mutex_;
    std::map<std::uint64_t, std::shared_ptr<Entry>> entries_;
    std::atomic<std::uint64_t> next_sequence_{1U};
    std::atomic<std::uint64_t> reserved_bytes_{0U};
    std::atomic<std::uint64_t> live_bytes_{0U};
    std::atomic<std::uint64_t> preparing_groups_{0U};
    std::atomic<std::uint64_t> live_groups_{0U};
    int directory_descriptor_ = -1;
    std::atomic<std::uint64_t> groups_prepared_{0U};
    std::atomic<std::uint64_t> groups_retired_{0U};
    std::atomic<std::uint64_t> state_updates_{0U};
    std::atomic<std::uint64_t> checksum_ns_{0U};
    std::atomic<std::uint64_t> encode_copy_ns_{0U};
    std::atomic<std::uint64_t> write_ns_{0U};
    std::atomic<std::uint64_t> fdatasync_ns_{0U};
    std::atomic<std::uint64_t> registry_lock_wait_ns_{0U};
    std::atomic<std::uint64_t> entry_lock_wait_ns_{0U};
    std::atomic<bool> started_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

std::unique_ptr<RequestSpool> RequestSpool::Create(
    RequestSpoolConfig config,
    std::string* error) {
    if (!ValidateRequestSpoolConfig(config, error)) {
        return nullptr;
    }
    try {
        return std::unique_ptr<RequestSpool>(
            new RequestSpool(std::make_unique<Impl>(std::move(config))));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("request spool creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

RequestSpool::RequestSpool(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RequestSpool::~RequestSpool() = default;

bool RequestSpool::Start(std::string* error) {
    return impl_->Start(error);
}

bool RequestSpool::Stop(std::string* error) noexcept {
    return impl_->Stop(error);
}

bool RequestSpool::PrepareGroup(
    ConsumerKind consumer,
    std::span<const WalPosition> input_positions,
    std::span<const RequestPayload> requests,
    RequestGroupHandle* handle,
    std::string* error) noexcept {
    return impl_->PrepareGroup(consumer, input_positions, requests, handle,
                               error);
}

bool RequestSpool::SetState(RequestGroupHandle handle,
                            std::uint32_t request_index,
                            RequestState state,
                            std::string* error) noexcept {
    return impl_->SetState(handle, request_index, state, error);
}

bool RequestSpool::Retire(RequestGroupHandle handle,
                          std::string* error) noexcept {
    return impl_->Retire(handle, error);
}

bool RequestSpool::healthy() const noexcept {
    return impl_->healthy();
}

std::string RequestSpool::fatal_error() const {
    return impl_->fatal_error();
}

RequestSpoolStats RequestSpool::stats() const noexcept {
    return impl_->stats();
}

const RequestSpoolConfig& RequestSpool::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::outbox
