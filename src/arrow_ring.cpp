#include "l2flow/arrow/ring.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/crc32.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <new>
#include <signal.h>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::arrow_hot {
namespace {

constexpr std::size_t kCacheLineBytes = 64U;
constexpr std::size_t kHeaderRegionBytes = 4'096U;
constexpr std::uint64_t kConsumerStateMask = 3U;
constexpr std::uint64_t kConsumerFree = 0U;
constexpr std::uint64_t kConsumerInitializing = 1U;
constexpr std::uint64_t kConsumerActive = 2U;
constexpr std::uint64_t kConsumerReaping = 3U;
constexpr std::array<char, 8U> kDataMagic{
    'L', '2', 'A', 'R', 'R', 'O', 'W', '1'};
constexpr std::array<char, 8U> kControlMagic{
    'L', '2', 'A', 'C', 'T', 'R', 'L', '1'};

static_assert(__atomic_always_lock_free(sizeof(std::uint64_t), nullptr),
              "the shared-memory protocol requires lock-free uint64 atomics");
static_assert(__atomic_always_lock_free(sizeof(std::uint32_t), nullptr),
              "the shared-memory protocol requires lock-free uint32 atomics");

struct alignas(kCacheLineBytes) DataHeader final {
    std::array<char, 8U> magic{};
    std::uint32_t protocol_version = 0U;
    std::uint32_t header_region_bytes = 0U;
    std::uint32_t stream_kind = 0U;
    std::uint32_t shard_id = 0U;
    std::uint32_t descriptor_capacity = 0U;
    std::uint32_t segment_count = 0U;
    std::uint32_t segment_payload_bytes = 0U;
    std::uint32_t maximum_consumers = 0U;
    std::uint64_t schema_offset = 0U;
    std::uint64_t schema_bytes = 0U;
    std::uint64_t descriptors_offset = 0U;
    std::uint64_t segments_offset = 0U;
    std::uint64_t segment_stride = 0U;
    std::uint64_t file_bytes = 0U;
    std::uint64_t producer_instance_high = 0U;
    std::uint64_t producer_instance_low = 0U;
    std::uint64_t ring_instance_high = 0U;
    std::uint64_t ring_instance_low = 0U;
    std::uint64_t initial_feed_session_epoch = 0U;
    std::uint64_t schema_fingerprint = 0U;
    std::uint64_t created_utc_ns = 0U;
    alignas(kCacheLineBytes) std::uint64_t published_sequence = 0U;
    std::uint64_t producer_heartbeat_monotonic_ns = 0U;
    std::uint64_t producer_state = 0U;
    std::uint64_t published_batches = 0U;
    std::uint64_t published_rows = 0U;
};

struct alignas(kCacheLineBytes) BatchDescriptor final {
    alignas(kCacheLineBytes) std::uint64_t publication_sequence = 0U;
    std::uint64_t segment_generation = 0U;
    std::uint64_t feed_session_epoch = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t minimum_exchange_time_ns = 0U;
    std::uint64_t maximum_exchange_time_ns = 0U;
    std::uint64_t publish_monotonic_ns = 0U;
    std::uint64_t payload_bytes = 0U;
    std::uint32_t segment_index = 0U;
    std::uint32_t row_count = 0U;
    std::uint32_t payload_crc32 = 0U;
    std::uint32_t flags = 0U;
    std::array<std::uint64_t, 3U> reserved{};
};

struct alignas(kCacheLineBytes) SegmentDataHeader final {
    std::uint64_t generation = 0U;
    std::uint64_t ipc_payload_bytes = 0U;
    std::uint32_t row_count = 0U;
    std::uint32_t payload_crc32 = 0U;
    std::array<std::uint64_t, 5U> reserved{};
};

struct alignas(kCacheLineBytes) ControlHeader final {
    std::array<char, 8U> magic{};
    std::uint32_t protocol_version = 0U;
    std::uint32_t header_region_bytes = 0U;
    std::uint32_t maximum_consumers = 0U;
    std::uint32_t segment_count = 0U;
    std::uint64_t consumer_entries_offset = 0U;
    std::uint64_t segment_controls_offset = 0U;
    std::uint64_t file_bytes = 0U;
    std::uint64_t producer_instance_high = 0U;
    std::uint64_t producer_instance_low = 0U;
    std::uint64_t ring_instance_high = 0U;
    std::uint64_t ring_instance_low = 0U;
};

struct alignas(kCacheLineBytes) ConsumerEntry final {
    alignas(kCacheLineBytes) std::uint64_t state = 0U;
    std::uint64_t pid = 0U;
    std::uint64_t process_start_ticks = 0U;
    std::uint64_t heartbeat_monotonic_ns = 0U;
    std::uint64_t next_sequence = 0U;
    std::uint64_t overrun_count = 0U;
    std::uint64_t nonce = 0U;
    std::uint64_t reserved = 0U;
};

struct alignas(kCacheLineBytes) SegmentControl final {
    // Even values are published generations. Odd values reserve the segment
    // for its single producer and prevent new readers from acquiring it.
    alignas(kCacheLineBytes) std::uint64_t state_generation = 0U;
    std::uint64_t reader_mask = 0U;
    std::array<std::uint64_t, 6U> reserved{};
};

static_assert(sizeof(BatchDescriptor) == 128U);
static_assert(sizeof(SegmentDataHeader) == kCacheLineBytes);
static_assert(sizeof(ConsumerEntry) == kCacheLineBytes);
static_assert(sizeof(SegmentControl) == kCacheLineBytes);
static_assert(sizeof(DataHeader) == 256U);
static_assert(sizeof(ControlHeader) == 128U);
static_assert(alignof(DataHeader) == kCacheLineBytes);
static_assert(alignof(BatchDescriptor) == kCacheLineBytes);
static_assert(alignof(ControlHeader) == kCacheLineBytes);
static_assert(offsetof(DataHeader, schema_bytes) == 48U);
static_assert(offsetof(DataHeader, published_sequence) == 192U);
static_assert(offsetof(DataHeader, producer_state) == 208U);
static_assert(std::is_standard_layout_v<DataHeader>);
static_assert(std::is_standard_layout_v<BatchDescriptor>);
static_assert(std::is_standard_layout_v<SegmentDataHeader>);
static_assert(std::is_standard_layout_v<ControlHeader>);
static_assert(std::is_standard_layout_v<ConsumerEntry>);
static_assert(std::is_standard_layout_v<SegmentControl>);
static_assert(std::is_trivially_copyable_v<DataHeader>);
static_assert(std::is_trivially_copyable_v<BatchDescriptor>);
static_assert(std::is_trivially_copyable_v<SegmentDataHeader>);
static_assert(std::is_trivially_copyable_v<ControlHeader>);
static_assert(std::is_trivially_copyable_v<ConsumerEntry>);
static_assert(std::is_trivially_copyable_v<SegmentControl>);

[[nodiscard]] constexpr bool IsRingStreamKind(
    RingStreamKind kind) noexcept {
    switch (kind) {
        case RingStreamKind::kOrderedTick:
        case RingStreamKind::kSnapshot:
        case RingStreamKind::kControl:
        case RingStreamKind::kEvent:
        case RingStreamKind::kKline:
            return true;
    }
    return false;
}

[[nodiscard]] std::uint64_t AtomicLoadAcquire(
    const std::uint64_t* value) noexcept {
    return __atomic_load_n(value, __ATOMIC_ACQUIRE);
}

[[nodiscard]] std::uint64_t AtomicLoadRelaxed(
    const std::uint64_t* value) noexcept {
    return __atomic_load_n(value, __ATOMIC_RELAXED);
}

[[nodiscard]] std::uint32_t AtomicLoadRelaxed(
    const std::uint32_t* value) noexcept {
    return __atomic_load_n(value, __ATOMIC_RELAXED);
}

[[nodiscard]] std::uint64_t AtomicLoadSequential(
    const std::uint64_t* value) noexcept {
    return __atomic_load_n(value, __ATOMIC_SEQ_CST);
}

void AtomicStoreRelease(std::uint64_t* target,
                        std::uint64_t value) noexcept {
    __atomic_store_n(target, value, __ATOMIC_RELEASE);
}

void AtomicStoreRelaxed(std::uint64_t* target,
                        std::uint64_t value) noexcept {
    __atomic_store_n(target, value, __ATOMIC_RELAXED);
}

void AtomicStoreRelaxed(std::uint32_t* target,
                        std::uint32_t value) noexcept {
    __atomic_store_n(target, value, __ATOMIC_RELAXED);
}

void AtomicStoreMaximumRelease(std::uint64_t* target,
                               std::uint64_t value) noexcept {
    std::uint64_t current = AtomicLoadRelaxed(target);
    while (current < value &&
           !__atomic_compare_exchange_n(
               target, &current, value, true,
               __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
    }
}

[[nodiscard]] bool AtomicCompareExchange(std::uint64_t* target,
                                         std::uint64_t* expected,
                                         std::uint64_t desired) noexcept {
    return __atomic_compare_exchange_n(target, expected, desired, false,
                                       __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

[[nodiscard]] bool AtomicCompareExchangeSequential(
    std::uint64_t* target,
    std::uint64_t* expected,
    std::uint64_t desired) noexcept {
    return __atomic_compare_exchange_n(target, expected, desired, false,
                                       __ATOMIC_SEQ_CST,
                                       __ATOMIC_SEQ_CST);
}

[[nodiscard]] std::uint64_t AtomicFetchOrSequential(
    std::uint64_t* target,
    std::uint64_t value) noexcept {
    return __atomic_fetch_or(target, value, __ATOMIC_SEQ_CST);
}

void AtomicFetchAnd(std::uint64_t* target, std::uint64_t value) noexcept {
    static_cast<void>(
        __atomic_fetch_and(target, value, __ATOMIC_ACQ_REL));
}

[[nodiscard]] bool IsPowerOfTwo(std::size_t value) noexcept {
    return value != 0U && (value & (value - 1U)) == 0U;
}

[[nodiscard]] bool AlignUp(std::size_t value,
                           std::size_t alignment,
                           std::size_t* output) noexcept {
    if (output == nullptr || !IsPowerOfTwo(alignment) ||
        value > std::numeric_limits<std::size_t>::max() -
                    (alignment - 1U)) {
        return false;
    }
    *output = (value + alignment - 1U) & ~(alignment - 1U);
    return true;
}

[[nodiscard]] bool CheckedAdd(std::size_t left,
                              std::size_t right,
                              std::size_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedMultiply(std::size_t left,
                                   std::size_t right,
                                   std::size_t* output) noexcept {
    if (output == nullptr ||
        (left != 0U &&
         right > std::numeric_limits<std::size_t>::max() / left)) {
        return false;
    }
    *output = left * right;
    return true;
}

[[nodiscard]] bool ToSize(std::uint64_t value,
                          std::size_t* output) noexcept {
    if (output == nullptr ||
        value > static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    *output = static_cast<std::size_t>(value);
    return true;
}

void SetError(std::string* error, std::string value) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(value);
    } catch (...) {
    }
}

[[nodiscard]] std::string ErrnoText(std::string_view operation,
                                    int value) {
    return std::string(operation) + ": " + std::strerror(value);
}

[[nodiscard]] std::uint64_t ClockNowNs(clockid_t clock) noexcept {
    timespec value{};
    if (::clock_gettime(clock, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(value.tv_sec) *
               UINT64_C(1'000'000'000) +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] std::uint64_t Fnv1a64(const std::uint8_t* data,
                                    std::size_t size) noexcept {
    std::uint64_t hash = UINT64_C(14695981039346656037);
    for (std::size_t index = 0U; index < size; ++index) {
        hash ^= data[index];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

[[nodiscard]] bool ContainsDictionary(
    const std::shared_ptr<arrow::DataType>& type) noexcept {
    if (type == nullptr || type->id() == arrow::Type::DICTIONARY) {
        return type != nullptr;
    }
    for (int index = 0; index < type->num_fields(); ++index) {
        if (ContainsDictionary(type->field(index)->type())) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool SchemaContainsDictionary(
    const arrow::Schema& schema) noexcept {
    for (const std::shared_ptr<arrow::Field>& field : schema.fields()) {
        if (ContainsDictionary(field->type())) {
            return true;
        }
    }
    return false;
}

class MappedFile final {
public:
    ~MappedFile() {
        if (address_ != MAP_FAILED) {
            static_cast<void>(::munmap(address_, size_));
        }
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
    }

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    [[nodiscard]] static std::unique_ptr<MappedFile> Create(
        const std::filesystem::path& path,
        std::size_t size,
        std::string* error) {
        const int fd = ::open(path.c_str(),
                              O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
                              S_IRUSR | S_IWUSR | S_IRGRP);
        if (fd < 0) {
            SetError(error, ErrnoText("open " + path.string(), errno));
            return nullptr;
        }
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<off_t>::max()) ||
            ::ftruncate(fd, static_cast<off_t>(size)) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(path.c_str()));
            SetError(error,
                     size > static_cast<std::size_t>(
                                std::numeric_limits<off_t>::max())
                         ? "mapped file exceeds off_t"
                         : ErrnoText("ftruncate " + path.string(),
                                     saved_errno));
            return nullptr;
        }
        int allocation_error = 0;
        do {
            allocation_error = ::posix_fallocate(
                fd, 0, static_cast<off_t>(size));
        } while (allocation_error == EINTR);
        if (allocation_error != 0) {
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(path.c_str()));
            SetError(error, ErrnoText(
                "posix_fallocate " + path.string(), allocation_error));
            return nullptr;
        }
        void* const address = ::mmap(
            nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (address == MAP_FAILED) {
            const int saved_errno = errno;
            static_cast<void>(::close(fd));
            static_cast<void>(::unlink(path.c_str()));
            SetError(error,
                     ErrnoText("mmap " + path.string(), saved_errno));
            return nullptr;
        }
        return std::unique_ptr<MappedFile>(
            new MappedFile(fd, address, size));
    }

    [[nodiscard]] static std::unique_ptr<MappedFile> Open(
        const std::filesystem::path& path,
        bool writable,
        std::string* error) {
        const int flags = writable ? O_RDWR | O_CLOEXEC
                                   : O_RDONLY | O_CLOEXEC;
        const int fd = ::open(path.c_str(), flags);
        if (fd < 0) {
            SetError(error, ErrnoText("open " + path.string(), errno));
            return nullptr;
        }
        struct stat status {};
        if (::fstat(fd, &status) != 0) {
            const int saved_errno = errno;
            static_cast<void>(::close(fd));
            SetError(error,
                     ErrnoText("fstat " + path.string(), saved_errno));
            return nullptr;
        }
        if (status.st_size <= 0) {
            static_cast<void>(::close(fd));
            SetError(error, "mapped file is empty: " + path.string());
            return nullptr;
        }
        const auto unsigned_size = static_cast<std::uintmax_t>(status.st_size);
        if (unsigned_size > std::numeric_limits<std::size_t>::max()) {
            static_cast<void>(::close(fd));
            SetError(error, "mapped file is too large for size_t");
            return nullptr;
        }
        const std::size_t size = static_cast<std::size_t>(unsigned_size);
        const int protection = writable ? PROT_READ | PROT_WRITE : PROT_READ;
        void* const address =
            ::mmap(nullptr, size, protection, MAP_SHARED, fd, 0);
        if (address == MAP_FAILED) {
            const int saved_errno = errno;
            static_cast<void>(::close(fd));
            SetError(error,
                     ErrnoText("mmap " + path.string(), saved_errno));
            return nullptr;
        }
        return std::unique_ptr<MappedFile>(
            new MappedFile(fd, address, size));
    }

    [[nodiscard]] std::byte* bytes() noexcept {
        return static_cast<std::byte*>(address_);
    }
    [[nodiscard]] const std::byte* bytes() const noexcept {
        return static_cast<const std::byte*>(address_);
    }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }

private:
    MappedFile(int fd, void* address, std::size_t size) noexcept
        : fd_(fd), address_(address), size_(size) {}

    int fd_ = -1;
    void* address_ = MAP_FAILED;
    std::size_t size_ = 0U;
};

class CreatedFileCleanup final {
public:
    explicit CreatedFileCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}
    ~CreatedFileCleanup() {
        if (active_) {
            static_cast<void>(::unlink(path_.c_str()));
        }
    }

    CreatedFileCleanup(const CreatedFileCleanup&) = delete;
    CreatedFileCleanup& operator=(const CreatedFileCleanup&) = delete;

    void Arm() noexcept { active_ = true; }

    void Release() noexcept { active_ = false; }

private:
    std::filesystem::path path_;
    bool active_ = false;
};

struct Layout final {
    std::size_t schema_offset = 0U;
    std::size_t descriptors_offset = 0U;
    std::size_t segments_offset = 0U;
    std::size_t segment_stride = 0U;
    std::size_t data_bytes = 0U;
    std::size_t consumer_entries_offset = 0U;
    std::size_t segment_controls_offset = 0U;
    std::size_t control_bytes = 0U;
};

[[nodiscard]] bool ComputeLayout(std::size_t schema_bytes,
                                 const RingWriterConfig& config,
                                 Layout* output,
                                 std::string* error) noexcept {
    if (output == nullptr) {
        SetError(error, "layout output is null");
        return false;
    }
    Layout layout{};
    layout.schema_offset = kHeaderRegionBytes;
    std::size_t after_schema = 0U;
    std::size_t descriptor_bytes = 0U;
    std::size_t after_descriptors = 0U;
    std::size_t segment_unaligned = 0U;
    std::size_t all_segment_bytes = 0U;
    std::size_t consumer_bytes = 0U;
    std::size_t after_consumers = 0U;
    std::size_t control_segment_bytes = 0U;
    if (!CheckedAdd(layout.schema_offset, schema_bytes, &after_schema) ||
        !AlignUp(after_schema, kCacheLineBytes,
                 &layout.descriptors_offset) ||
        !CheckedMultiply(config.descriptor_capacity,
                         sizeof(BatchDescriptor), &descriptor_bytes) ||
        !CheckedAdd(layout.descriptors_offset, descriptor_bytes,
                    &after_descriptors) ||
        !AlignUp(after_descriptors, kHeaderRegionBytes,
                 &layout.segments_offset) ||
        !CheckedAdd(sizeof(SegmentDataHeader),
                    config.segment_payload_bytes, &segment_unaligned) ||
        !AlignUp(segment_unaligned, kCacheLineBytes,
                 &layout.segment_stride) ||
        !CheckedMultiply(config.segment_count, layout.segment_stride,
                         &all_segment_bytes) ||
        !CheckedAdd(layout.segments_offset, all_segment_bytes,
                    &layout.data_bytes) ||
        !CheckedMultiply(config.maximum_consumers,
                         sizeof(ConsumerEntry), &consumer_bytes) ||
        !CheckedAdd(kHeaderRegionBytes, consumer_bytes,
                    &after_consumers) ||
        !AlignUp(after_consumers, kHeaderRegionBytes,
                 &layout.segment_controls_offset) ||
        !CheckedMultiply(config.segment_count, sizeof(SegmentControl),
                         &control_segment_bytes) ||
        !CheckedAdd(layout.segment_controls_offset,
                    control_segment_bytes, &layout.control_bytes)) {
        SetError(error, "shared-memory ring layout overflows size_t");
        return false;
    }
    layout.consumer_entries_offset = kHeaderRegionBytes;
    *output = layout;
    return true;
}

[[nodiscard]] std::uint64_t ReadProcessStartTicks(pid_t pid) noexcept {
    try {
        std::ifstream stream("/proc/" + std::to_string(pid) + "/stat");
        std::string line;
        if (!stream || !std::getline(stream, line)) {
            return 0U;
        }
        const std::size_t close = line.rfind(')');
        if (close == std::string::npos || close + 2U >= line.size()) {
            return 0U;
        }
        std::istringstream fields(line.substr(close + 2U));
        std::string token;
        // The substring starts at field 3. Process start time is field 22.
        for (std::size_t field = 3U; field <= 22U; ++field) {
            if (!(fields >> token)) {
                return 0U;
            }
            if (field == 22U) {
                std::size_t consumed = 0U;
                const std::uint64_t value = std::stoull(token, &consumed);
                return consumed == token.size() ? value : 0U;
            }
        }
    } catch (...) {
    }
    return 0U;
}

[[nodiscard]] std::uint64_t EncodeConsumerState(
    pid_t pid,
    std::uint64_t state) noexcept {
    return (static_cast<std::uint64_t>(pid) << 2U) | state;
}

[[nodiscard]] std::uint64_t ConsumerStateKind(
    std::uint64_t state) noexcept {
    return state & kConsumerStateMask;
}

[[nodiscard]] std::uint64_t ConsumerStatePid(
    std::uint64_t state) noexcept {
    return state >> 2U;
}

[[nodiscard]] bool ProcessIdConfirmedDead(std::uint64_t raw_pid) noexcept {
    if (raw_pid == 0U ||
        raw_pid > static_cast<std::uint64_t>(
                      std::numeric_limits<pid_t>::max())) {
        return false;
    }
    const pid_t pid = static_cast<pid_t>(raw_pid);
    if (::kill(pid, 0) == 0 || errno == EPERM) {
        return false;
    }
    return errno == ESRCH;
}

[[nodiscard]] bool ProcessInstanceConfirmedDead(
    std::uint64_t raw_pid,
    std::uint64_t start_ticks) noexcept {
    if (raw_pid == 0U ||
        raw_pid > static_cast<std::uint64_t>(
                      std::numeric_limits<pid_t>::max()) ||
        start_ticks == 0U) {
        return false;
    }
    const pid_t pid = static_cast<pid_t>(raw_pid);
    const std::uint64_t observed_start = ReadProcessStartTicks(pid);
    if (observed_start != 0U) {
        return observed_start != start_ticks;
    }
    return ProcessIdConfirmedDead(raw_pid);
}

struct ReaderMappings final {
    std::unique_ptr<MappedFile> data;
    std::unique_ptr<MappedFile> control;
    const DataHeader* data_header = nullptr;
    ControlHeader* control_header = nullptr;
    const BatchDescriptor* descriptors = nullptr;
    const std::byte* segments = nullptr;
    ConsumerEntry* consumers = nullptr;
    SegmentControl* segment_controls = nullptr;
};

class ConsumerRegistration final {
    struct LocalSegmentPins final {
        std::atomic_flag lock = ATOMIC_FLAG_INIT;
        std::uint32_t count = 0U;
    };

public:
    ConsumerRegistration(std::shared_ptr<ReaderMappings> mappings,
                         std::size_t consumer_index)
        : mappings_(std::move(mappings)),
          consumer_index_(consumer_index),
          bit_(UINT64_C(1) <<
               static_cast<unsigned int>(consumer_index)),
          local_pins_(std::make_unique<LocalSegmentPins[]>(
              mappings_->data_header->segment_count)) {}

    ~ConsumerRegistration() {
        if (mappings_ == nullptr) {
            return;
        }
        const std::size_t segment_count =
            mappings_->data_header->segment_count;
        for (std::size_t index = 0U; index < segment_count; ++index) {
            AtomicFetchAnd(&mappings_->segment_controls[index].reader_mask,
                           ~bit_);
        }
        ConsumerEntry& entry = mappings_->consumers[consumer_index_];
        AtomicStoreRelease(&entry.state, kConsumerFree);
    }

    [[nodiscard]] std::uint64_t bit() const noexcept { return bit_; }
    [[nodiscard]] std::shared_ptr<ReaderMappings> mappings() const noexcept {
        return mappings_;
    }
    [[nodiscard]] ConsumerEntry* entry() const noexcept {
        return &mappings_->consumers[consumer_index_];
    }

    [[nodiscard]] bool Pin(std::size_t segment_index) noexcept {
        LocalSegmentPins& pins = local_pins_[segment_index];
        Lock(pins);
        if (pins.count == std::numeric_limits<std::uint32_t>::max()) {
            Unlock(pins);
            return false;
        }
        if (pins.count == 0U) {
            static_cast<void>(AtomicFetchOrSequential(
                &mappings_->segment_controls[segment_index].reader_mask,
                bit_));
        }
        ++pins.count;
        Unlock(pins);
        return true;
    }

    void Unpin(std::size_t segment_index) noexcept {
        LocalSegmentPins& pins = local_pins_[segment_index];
        Lock(pins);
        if (pins.count == 0U) {
            Unlock(pins);
            return;
        }
        --pins.count;
        if (pins.count == 0U) {
            AtomicFetchAnd(
                &mappings_->segment_controls[segment_index].reader_mask,
                ~bit_);
        }
        Unlock(pins);
    }

private:
    static void Lock(LocalSegmentPins& pins) noexcept {
        while (pins.lock.test_and_set(std::memory_order_acquire)) {
            pins.lock.wait(true, std::memory_order_relaxed);
        }
    }

    static void Unlock(LocalSegmentPins& pins) noexcept {
        pins.lock.clear(std::memory_order_release);
        pins.lock.notify_one();
    }

    std::shared_ptr<ReaderMappings> mappings_;
    std::size_t consumer_index_ = 0U;
    std::uint64_t bit_ = 0U;
    std::unique_ptr<LocalSegmentPins[]> local_pins_;
};

class SegmentPinGuard final {
public:
    SegmentPinGuard(ConsumerRegistration* registration,
                    std::size_t segment_index) noexcept
        : registration_(registration), segment_index_(segment_index) {}

    ~SegmentPinGuard() {
        if (registration_ != nullptr) {
            registration_->Unpin(segment_index_);
        }
    }

    SegmentPinGuard(const SegmentPinGuard&) = delete;
    SegmentPinGuard& operator=(const SegmentPinGuard&) = delete;

    void Release() noexcept { registration_ = nullptr; }

private:
    ConsumerRegistration* registration_ = nullptr;
    std::size_t segment_index_ = 0U;
};

class ConsumerSlotReservation final {
public:
    ConsumerSlotReservation() = default;
    ~ConsumerSlotReservation() {
        if (entry_ != nullptr) {
            AtomicStoreRelease(&entry_->state, kConsumerFree);
        }
    }

    ConsumerSlotReservation(const ConsumerSlotReservation&) = delete;
    ConsumerSlotReservation& operator=(const ConsumerSlotReservation&) =
        delete;

    void Arm(ConsumerEntry* entry) noexcept { entry_ = entry; }
    void Release() noexcept { entry_ = nullptr; }

private:
    ConsumerEntry* entry_ = nullptr;
};

class LeaseArrowBuffer final : public arrow::Buffer {
public:
    LeaseArrowBuffer(const std::uint8_t* data,
                     std::int64_t size,
                     std::shared_ptr<ArrowBatchLease> lease)
        : arrow::Buffer(data, size), lease_(std::move(lease)) {}

private:
    std::shared_ptr<ArrowBatchLease> lease_;
};

[[nodiscard]] bool ValidateDataHeader(const DataHeader& header,
                                      std::size_t file_size,
                                      std::string* error) noexcept {
    const std::uint64_t producer_state =
        AtomicLoadAcquire(&header.producer_state);
    if (producer_state !=
            static_cast<std::uint64_t>(ProducerState::kActive) &&
        producer_state !=
            static_cast<std::uint64_t>(ProducerState::kSealed)) {
        SetError(error, "Arrow ring producer is not fully initialized");
        return false;
    }
    if (header.magic != kDataMagic ||
        header.protocol_version != kArrowRingProtocolVersion ||
        header.header_region_bytes != kHeaderRegionBytes) {
        SetError(error, "data file is not a supported L2Flow Arrow ring");
        return false;
    }
    if (!IsRingStreamKind(static_cast<RingStreamKind>(header.stream_kind)) ||
        !IsPowerOfTwo(header.descriptor_capacity) ||
        header.descriptor_capacity < 2U ||
        header.segment_count < header.descriptor_capacity ||
        header.segment_payload_bytes == 0U ||
        header.maximum_consumers == 0U ||
        header.maximum_consumers > kMaximumArrowRingConsumers ||
        header.initial_feed_session_epoch == 0U ||
        (header.producer_instance_high == 0U &&
         header.producer_instance_low == 0U) ||
        (header.ring_instance_high == 0U &&
         header.ring_instance_low == 0U) ||
        header.schema_offset < kHeaderRegionBytes ||
        header.schema_bytes == 0U ||
        header.schema_bytes > kMaximumArrowRingSchemaBytes) {
        SetError(error, "Arrow ring data header contains invalid bounds");
        return false;
    }
    std::size_t schema_offset = 0U;
    std::size_t schema_bytes = 0U;
    std::size_t descriptors_offset = 0U;
    std::size_t segments_offset = 0U;
    std::size_t segment_stride = 0U;
    std::size_t declared_file_bytes = 0U;
    std::size_t schema_end = 0U;
    std::size_t descriptor_bytes = 0U;
    std::size_t descriptors_end = 0U;
    std::size_t minimum_segment_stride = 0U;
    std::size_t segment_bytes = 0U;
    std::size_t segments_end = 0U;
    if (!ToSize(header.schema_offset, &schema_offset) ||
        !ToSize(header.schema_bytes, &schema_bytes) ||
        !ToSize(header.descriptors_offset, &descriptors_offset) ||
        !ToSize(header.segments_offset, &segments_offset) ||
        !ToSize(header.segment_stride, &segment_stride) ||
        !ToSize(header.file_bytes, &declared_file_bytes) ||
        declared_file_bytes != file_size ||
        !CheckedAdd(schema_offset, schema_bytes, &schema_end) ||
        descriptors_offset < schema_end ||
        descriptors_offset % kCacheLineBytes != 0U ||
        !CheckedMultiply(header.descriptor_capacity,
                         sizeof(BatchDescriptor), &descriptor_bytes) ||
        !CheckedAdd(descriptors_offset, descriptor_bytes,
                    &descriptors_end) ||
        segments_offset < descriptors_end ||
        segments_offset % kHeaderRegionBytes != 0U ||
        !CheckedAdd(sizeof(SegmentDataHeader),
                    header.segment_payload_bytes,
                    &minimum_segment_stride) ||
        segment_stride < minimum_segment_stride ||
        segment_stride % kCacheLineBytes != 0U ||
        !CheckedMultiply(header.segment_count,
                         segment_stride,
                         &segment_bytes) ||
        !CheckedAdd(segments_offset, segment_bytes, &segments_end) ||
        schema_end > file_size || descriptors_end > file_size ||
        segments_end != file_size) {
        SetError(error, "Arrow ring data regions exceed the mapped file");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateControlHeader(const ControlHeader& header,
                                         const DataHeader& data,
                                         std::size_t file_size,
                                         std::string* error) noexcept {
    if (header.magic != kControlMagic ||
        header.protocol_version != kArrowRingProtocolVersion ||
        header.header_region_bytes != kHeaderRegionBytes ||
        header.maximum_consumers != data.maximum_consumers ||
        header.segment_count != data.segment_count ||
        header.producer_instance_high != data.producer_instance_high ||
        header.producer_instance_low != data.producer_instance_low ||
        header.ring_instance_high != data.ring_instance_high ||
        header.ring_instance_low != data.ring_instance_low ||
        header.file_bytes != file_size) {
        SetError(error, "control file does not match the Arrow data ring");
        return false;
    }
    std::size_t consumer_bytes = 0U;
    std::size_t consumer_end = 0U;
    std::size_t consumer_offset = 0U;
    std::size_t segment_bytes = 0U;
    std::size_t segment_end = 0U;
    std::size_t segment_offset = 0U;
    std::size_t declared_file_bytes = 0U;
    if (!ToSize(header.consumer_entries_offset, &consumer_offset) ||
        !ToSize(header.segment_controls_offset, &segment_offset) ||
        !ToSize(header.file_bytes, &declared_file_bytes) ||
        declared_file_bytes != file_size ||
        consumer_offset < kHeaderRegionBytes ||
        consumer_offset % kCacheLineBytes != 0U ||
        !CheckedMultiply(header.maximum_consumers, sizeof(ConsumerEntry),
                         &consumer_bytes) ||
        !CheckedAdd(consumer_offset, consumer_bytes, &consumer_end) ||
        segment_offset < consumer_end ||
        segment_offset % kCacheLineBytes != 0U ||
        !CheckedMultiply(header.segment_count, sizeof(SegmentControl),
                         &segment_bytes) ||
        !CheckedAdd(segment_offset, segment_bytes, &segment_end) ||
        consumer_end > file_size || segment_end != file_size) {
        SetError(error, "Arrow ring control regions exceed the mapped file");
        return false;
    }
    return true;
}

[[nodiscard]] std::uint64_t OldestAvailable(std::uint64_t newest,
                                            std::uint64_t capacity) noexcept {
    if (newest == 0U) {
        return 1U;
    }
    return newest > capacity ? newest - capacity + 1U : 1U;
}

}  // namespace

class SharedArrowRingWriter::Impl final {
public:
    Impl(RingWriterConfig config,
         std::shared_ptr<arrow::Schema> schema,
         std::unique_ptr<MappedFile> data,
         std::unique_ptr<MappedFile> control,
         Layout layout,
         std::uint64_t schema_fingerprint) noexcept
        : config_(std::move(config)),
          schema_(std::move(schema)),
          data_(std::move(data)),
          control_(std::move(control)),
          layout_(layout),
          schema_fingerprint_(schema_fingerprint),
          segment_last_sequence_(config_.segment_count, 0U) {
        data_header_ = reinterpret_cast<DataHeader*>(data_->bytes());
        control_header_ =
            reinterpret_cast<ControlHeader*>(control_->bytes());
        descriptors_ = reinterpret_cast<BatchDescriptor*>(
            data_->bytes() + layout_.descriptors_offset);
        segments_ = data_->bytes() + layout_.segments_offset;
        consumers_ = reinterpret_cast<ConsumerEntry*>(
            control_->bytes() + layout_.consumer_entries_offset);
        segment_controls_ = reinterpret_cast<SegmentControl*>(
            control_->bytes() + layout_.segment_controls_offset);
    }

    ~Impl() {
        Seal(ClockNowNs(CLOCK_MONOTONIC));
        if (config_.unlink_on_destroy) {
            static_cast<void>(::unlink(
                config_.location.data_path.c_str()));
            static_cast<void>(::unlink(
                config_.location.control_path.c_str()));
        }
    }

    [[nodiscard]] PublishResult TryPublish(
        const arrow::RecordBatch& batch,
        const BatchMetadata& metadata) noexcept {
        PublishResult result{};
        try {
            if (AtomicLoadAcquire(&data_header_->producer_state) !=
                static_cast<std::uint64_t>(ProducerState::kActive)) {
                result.code = PublishCode::kInternalError;
                result.error = "Arrow ring writer is sealed";
                return result;
            }
            if (batch.num_rows() <= 0 || batch.num_rows() >
                    static_cast<std::int64_t>(
                        std::numeric_limits<std::uint32_t>::max()) ||
                !batch.schema()->Equals(*schema_, true) ||
                (metadata.feed_session_epoch != 0U &&
                 metadata.feed_session_epoch != config_.feed_session_epoch)) {
                result.code = PublishCode::kInvalidBatch;
                result.error =
                    "record batch is empty, has the wrong schema, or belongs "
                    "to a different feed epoch";
                return result;
            }
            arrow::ipc::IpcWriteOptions options =
                arrow::ipc::IpcWriteOptions::Defaults();
            options.alignment = 64;
            options.use_threads = false;
            options.metadata_version = arrow::ipc::MetadataVersion::V5;
            options.write_legacy_ipc_format = false;
            options.codec.reset();
            const arrow::Result<std::shared_ptr<arrow::Buffer>> serialized =
                arrow::ipc::SerializeRecordBatch(batch, options);
            if (!serialized.ok()) {
                result.code = PublishCode::kInternalError;
                result.error = serialized.status().ToString();
                return result;
            }
            const std::shared_ptr<arrow::Buffer>& payload = *serialized;
            if (payload->size() <= 0) {
                result.code = PublishCode::kInternalError;
                result.error =
                    "Arrow serialized a nonempty batch to an empty payload";
                return result;
            }
            if (static_cast<std::uint64_t>(payload->size()) >
                    config_.segment_payload_bytes) {
                oversized_drops_.fetch_add(1U, std::memory_order_relaxed);
                result.code = PublishCode::kPayloadTooLarge;
                result.payload_bytes = payload->size() < 0
                    ? 0U
                    : static_cast<std::size_t>(payload->size());
                result.error = "serialized Arrow batch exceeds segment payload";
                return result;
            }

            const std::uint64_t current =
                AtomicLoadAcquire(&data_header_->published_sequence);
            if (current == std::numeric_limits<std::uint64_t>::max()) {
                result.code = PublishCode::kInternalError;
                result.error = "Arrow ring batch sequence exhausted";
                return result;
            }
            const std::uint64_t sequence = current + 1U;
            std::size_t segment_index = 0U;
            std::uint64_t generation = 0U;
            if (!ReserveSegment(sequence, &segment_index, &generation)) {
                ReapDeadConsumers();
                if (!ReserveSegment(sequence, &segment_index, &generation)) {
                    no_segment_drops_.fetch_add(1U,
                                                std::memory_order_relaxed);
                    result.code = PublishCode::kNoReusableSegment;
                    result.payload_bytes =
                        static_cast<std::size_t>(payload->size());
                    return result;
                }
            }

            std::byte* const segment =
                segments_ + segment_index * layout_.segment_stride;
            auto* const segment_header =
                reinterpret_cast<SegmentDataHeader*>(segment);
            std::byte* const destination = segment + sizeof(*segment_header);
            std::memcpy(destination, payload->data(),
                        static_cast<std::size_t>(payload->size()));
            const std::uint32_t crc = arrow::internal::crc32(
                0U, destination, static_cast<std::size_t>(payload->size()));
            segment_header->generation = generation;
            segment_header->ipc_payload_bytes =
                static_cast<std::uint64_t>(payload->size());
            segment_header->row_count =
                static_cast<std::uint32_t>(batch.num_rows());
            segment_header->payload_crc32 = crc;
            AtomicStoreRelease(
                &segment_controls_[segment_index].state_generation,
                generation << 1U);

            BatchDescriptor& descriptor = descriptors_[
                static_cast<std::size_t>(sequence - 1U) &
                (config_.descriptor_capacity - 1U)];
            AtomicStoreRelaxed(&descriptor.publication_sequence, 0U);
            // Writer side of the descriptor seqlock: no replacement field may
            // become visible before readers can observe the invalid marker.
            std::atomic_thread_fence(std::memory_order_release);
            AtomicStoreRelaxed(&descriptor.segment_generation, generation);
            AtomicStoreRelaxed(&descriptor.feed_session_epoch,
                metadata.feed_session_epoch == 0U
                    ? config_.feed_session_epoch
                    : metadata.feed_session_epoch);
            AtomicStoreRelaxed(&descriptor.first_ingress_sequence,
                               metadata.first_ingress_sequence);
            AtomicStoreRelaxed(&descriptor.last_ingress_sequence,
                               metadata.last_ingress_sequence);
            AtomicStoreRelaxed(&descriptor.minimum_exchange_time_ns,
                               metadata.minimum_exchange_time_ns);
            AtomicStoreRelaxed(&descriptor.maximum_exchange_time_ns,
                               metadata.maximum_exchange_time_ns);
            AtomicStoreRelaxed(&descriptor.publish_monotonic_ns,
                metadata.publish_monotonic_ns == 0U
                    ? ClockNowNs(CLOCK_MONOTONIC)
                    : metadata.publish_monotonic_ns);
            AtomicStoreRelaxed(
                &descriptor.payload_bytes,
                static_cast<std::uint64_t>(payload->size()));
            AtomicStoreRelaxed(
                &descriptor.segment_index,
                static_cast<std::uint32_t>(segment_index));
            AtomicStoreRelaxed(
                &descriptor.row_count,
                static_cast<std::uint32_t>(batch.num_rows()));
            AtomicStoreRelaxed(&descriptor.payload_crc32, crc);
            AtomicStoreRelaxed(&descriptor.flags, metadata.flags);
            AtomicStoreRelease(&descriptor.publication_sequence, sequence);
            segment_last_sequence_[segment_index] = sequence;
            AtomicStoreRelease(&data_header_->published_sequence, sequence);
            AtomicStoreMaximumRelease(
                &data_header_->producer_heartbeat_monotonic_ns,
                AtomicLoadRelaxed(&descriptor.publish_monotonic_ns));
            AtomicStoreRelease(&data_header_->published_batches, sequence);
            const std::uint64_t rows_before =
                AtomicLoadRelaxed(&data_header_->published_rows);
            AtomicStoreRelease(
                &data_header_->published_rows,
                rows_before + static_cast<std::uint64_t>(batch.num_rows()));
            published_batches_.fetch_add(1U, std::memory_order_relaxed);
            published_rows_.fetch_add(
                static_cast<std::uint64_t>(batch.num_rows()),
                std::memory_order_relaxed);
            result.code = PublishCode::kPublished;
            result.batch_sequence = sequence;
            result.payload_bytes =
                static_cast<std::size_t>(payload->size());
            return result;
        } catch (const std::exception& exception) {
            result.code = PublishCode::kInternalError;
            result.error = std::string("Arrow ring publish failed: ") +
                           exception.what();
            return result;
        } catch (...) {
            result.code = PublishCode::kInternalError;
            result.error = "Arrow ring publish failed unexpectedly";
            return result;
        }
    }

    void TouchHeartbeat(std::uint64_t monotonic_ns) noexcept {
        AtomicStoreMaximumRelease(
            &data_header_->producer_heartbeat_monotonic_ns, monotonic_ns);
    }

    void Seal(std::uint64_t monotonic_ns) noexcept {
        TouchHeartbeat(monotonic_ns);
        AtomicStoreRelease(
            &data_header_->producer_state,
            static_cast<std::uint64_t>(ProducerState::kSealed));
    }

    void ReapDeadConsumers() noexcept {
        for (std::size_t index = 0U;
             index < config_.maximum_consumers; ++index) {
            ConsumerEntry& entry = consumers_[index];
            const std::uint64_t observed_state =
                AtomicLoadAcquire(&entry.state);
            const std::uint64_t kind = ConsumerStateKind(observed_state);
            if (kind != kConsumerActive &&
                kind != kConsumerInitializing) {
                continue;
            }
            const std::uint64_t state_pid =
                ConsumerStatePid(observed_state);
            const bool confirmed_dead = kind == kConsumerActive
                ? ProcessInstanceConfirmedDead(
                      state_pid, entry.process_start_ticks)
                : ProcessIdConfirmedDead(state_pid);
            if (!confirmed_dead) {
                continue;
            }
            std::uint64_t expected = observed_state;
            if (!AtomicCompareExchange(&entry.state, &expected,
                                       EncodeConsumerState(
                                           static_cast<pid_t>(state_pid),
                                           kConsumerReaping))) {
                continue;
            }
            const std::uint64_t bit =
                UINT64_C(1) << static_cast<unsigned int>(index);
            for (std::size_t segment = 0U;
                 segment < config_.segment_count; ++segment) {
                AtomicFetchAnd(&segment_controls_[segment].reader_mask,
                               ~bit);
            }
            AtomicStoreRelease(&entry.state, kConsumerFree);
            reaped_consumers_.fetch_add(1U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] RingWriterStats Stats() const noexcept {
        RingWriterStats result{};
        result.published_batches = published_batches_.load(
            std::memory_order_relaxed);
        result.published_rows = published_rows_.load(
            std::memory_order_relaxed);
        result.no_segment_drops = no_segment_drops_.load(
            std::memory_order_relaxed);
        result.oversized_drops = oversized_drops_.load(
            std::memory_order_relaxed);
        result.reaped_consumers = reaped_consumers_.load(
            std::memory_order_relaxed);
        return result;
    }

    [[nodiscard]] bool ReserveSegment(std::uint64_t sequence,
                                      std::size_t* output_index,
                                      std::uint64_t* output_generation) noexcept {
        const std::uint64_t eviction_threshold =
            sequence > config_.descriptor_capacity
                ? sequence - config_.descriptor_capacity
                : 0U;
        for (std::size_t count = 0U; count < config_.segment_count; ++count) {
            const std::size_t index =
                (next_segment_cursor_ + count) % config_.segment_count;
            const std::uint64_t last = segment_last_sequence_[index];
            if (last != 0U && last > eviction_threshold) {
                continue;
            }
            SegmentControl& control = segment_controls_[index];
            std::uint64_t state =
                AtomicLoadAcquire(&control.state_generation);
            if ((state & UINT64_C(1)) != 0U) {
                continue;
            }
            const std::uint64_t old_generation = state >> 1U;
            if (old_generation ==
                (std::numeric_limits<std::uint64_t>::max() >> 1U)) {
                continue;
            }
            const std::uint64_t generation = old_generation + 1U;
            const std::uint64_t writing_state =
                (generation << 1U) | UINT64_C(1);
            if (!AtomicCompareExchangeSequential(
                    &control.state_generation, &state, writing_state)) {
                continue;
            }
            if (AtomicLoadSequential(&control.reader_mask) != 0U) {
                AtomicStoreRelease(&control.state_generation,
                                   old_generation << 1U);
                continue;
            }
            *output_index = index;
            *output_generation = generation;
            next_segment_cursor_ = (index + 1U) % config_.segment_count;
            return true;
        }
        return false;
    }

    RingWriterConfig config_;
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<MappedFile> data_;
    std::unique_ptr<MappedFile> control_;
    Layout layout_{};
    std::uint64_t schema_fingerprint_ = 0U;
    DataHeader* data_header_ = nullptr;
    ControlHeader* control_header_ = nullptr;
    BatchDescriptor* descriptors_ = nullptr;
    std::byte* segments_ = nullptr;
    ConsumerEntry* consumers_ = nullptr;
    SegmentControl* segment_controls_ = nullptr;
    std::vector<std::uint64_t> segment_last_sequence_;
    std::size_t next_segment_cursor_ = 0U;
    std::atomic<std::uint64_t> published_batches_{0U};
    std::atomic<std::uint64_t> published_rows_{0U};
    std::atomic<std::uint64_t> no_segment_drops_{0U};
    std::atomic<std::uint64_t> oversized_drops_{0U};
    std::atomic<std::uint64_t> reaped_consumers_{0U};
};

class ArrowBatchLease::Impl final {
public:
    Impl(std::shared_ptr<ConsumerRegistration> registration,
         std::size_t segment_index,
         const std::uint8_t* data,
         std::size_t size,
         ReadMetadata metadata) noexcept
        : registration_(std::move(registration)),
          segment_index_(segment_index),
          data_(data),
          size_(size),
          metadata_(metadata) {}

    ~Impl() {
        if (registration_ == nullptr) {
            return;
        }
        const std::shared_ptr<ReaderMappings> mappings =
            registration_->mappings();
        registration_->Unpin(segment_index_);
    }

    std::shared_ptr<ConsumerRegistration> registration_;
    std::size_t segment_index_ = 0U;
    const std::uint8_t* data_ = nullptr;
    std::size_t size_ = 0U;
    ReadMetadata metadata_{};
};

class SharedArrowRingReader::Impl final {
public:
    Impl(RingLocation location,
         std::shared_ptr<ReaderMappings> mappings,
         std::shared_ptr<ConsumerRegistration> registration,
         std::shared_ptr<arrow::Schema> schema,
         std::unique_ptr<arrow::ipc::DictionaryMemo> dictionary_memo,
         std::uint64_t next_sequence,
         bool sequence_exhausted) noexcept
        : location_(std::move(location)),
          mappings_(std::move(mappings)),
          registration_(std::move(registration)),
          schema_(std::move(schema)),
          dictionary_memo_(std::move(dictionary_memo)),
          next_sequence_(next_sequence),
          sequence_exhausted_(sequence_exhausted) {}

    [[nodiscard]] ReadResult TryRead() noexcept {
        ReadResult result{};
        try {
            const DataHeader& header = *mappings_->data_header;
            if (sequence_exhausted_) {
                result.code = ReadCode::kClosed;
                return result;
            }
            const std::uint64_t newest =
                AtomicLoadAcquire(&header.published_sequence);
            const std::uint64_t oldest = OldestAvailable(
                newest, header.descriptor_capacity);
            result.metadata.oldest_available_sequence = oldest;
            result.metadata.newest_available_sequence = newest;
            result.metadata.producer_instance = {
                header.producer_instance_high,
                header.producer_instance_low};
            if (next_sequence_ < oldest) {
                result.code = ReadCode::kOverrun;
                RecordOverrun();
                return result;
            }
            if (next_sequence_ > newest || newest == 0U) {
                result.code =
                    AtomicLoadAcquire(&header.producer_state) ==
                            static_cast<std::uint64_t>(ProducerState::kSealed)
                        ? ReadCode::kClosed
                        : ReadCode::kEmpty;
                return result;
            }
            const std::uint64_t expected_sequence = next_sequence_;
            const std::size_t descriptor_index =
                static_cast<std::size_t>(expected_sequence - 1U) &
                (static_cast<std::size_t>(header.descriptor_capacity) - 1U);
            const BatchDescriptor& descriptor =
                mappings_->descriptors[descriptor_index];
            const std::uint64_t observed_sequence =
                AtomicLoadAcquire(&descriptor.publication_sequence);
            if (observed_sequence != expected_sequence) {
                const std::uint64_t refreshed =
                    AtomicLoadAcquire(&header.published_sequence);
                const std::uint64_t refreshed_oldest = OldestAvailable(
                    refreshed, header.descriptor_capacity);
                result.metadata.oldest_available_sequence = refreshed_oldest;
                result.metadata.newest_available_sequence = refreshed;
                if (next_sequence_ < refreshed_oldest) {
                    result.code = ReadCode::kOverrun;
                    RecordOverrun();
                } else {
                    result.code = ReadCode::kRetry;
                }
                return result;
            }
            BatchDescriptor snapshot{};
            snapshot.segment_generation =
                AtomicLoadRelaxed(&descriptor.segment_generation);
            snapshot.feed_session_epoch =
                AtomicLoadRelaxed(&descriptor.feed_session_epoch);
            snapshot.first_ingress_sequence =
                AtomicLoadRelaxed(&descriptor.first_ingress_sequence);
            snapshot.last_ingress_sequence =
                AtomicLoadRelaxed(&descriptor.last_ingress_sequence);
            snapshot.minimum_exchange_time_ns =
                AtomicLoadRelaxed(&descriptor.minimum_exchange_time_ns);
            snapshot.maximum_exchange_time_ns =
                AtomicLoadRelaxed(&descriptor.maximum_exchange_time_ns);
            snapshot.publish_monotonic_ns =
                AtomicLoadRelaxed(&descriptor.publish_monotonic_ns);
            snapshot.payload_bytes =
                AtomicLoadRelaxed(&descriptor.payload_bytes);
            snapshot.segment_index =
                AtomicLoadRelaxed(&descriptor.segment_index);
            snapshot.row_count = AtomicLoadRelaxed(&descriptor.row_count);
            snapshot.payload_crc32 =
                AtomicLoadRelaxed(&descriptor.payload_crc32);
            snapshot.flags = AtomicLoadRelaxed(&descriptor.flags);
            if (snapshot.segment_index >= header.segment_count ||
                snapshot.segment_generation == 0U ||
                snapshot.segment_generation >
                    (std::numeric_limits<std::uint64_t>::max() >> 1U) ||
                snapshot.feed_session_epoch !=
                    header.initial_feed_session_epoch ||
                snapshot.payload_bytes == 0U ||
                snapshot.payload_bytes > header.segment_payload_bytes ||
                snapshot.row_count == 0U) {
                result.code = ReadCode::kCorrupt;
                result.error = "Arrow ring descriptor contains invalid bounds";
                return result;
            }
            const std::size_t segment_index = snapshot.segment_index;
            SegmentControl& control =
                mappings_->segment_controls[segment_index];
            if (!registration_->Pin(segment_index)) {
                result.code = ReadCode::kCorrupt;
                result.error = "Arrow ring reader pin count exhausted";
                return result;
            }
            SegmentPinGuard pin_guard(registration_.get(), segment_index);
            const std::uint64_t state =
                AtomicLoadSequential(&control.state_generation);
            // Reader side of the descriptor seqlock: finish the snapshot
            // loads before checking whether the producer invalidated the slot.
            std::atomic_thread_fence(std::memory_order_acquire);
            const std::uint64_t descriptor_after =
                AtomicLoadAcquire(&descriptor.publication_sequence);
            if (state != snapshot.segment_generation << 1U ||
                descriptor_after != expected_sequence) {
                result.code = ReadCode::kRetry;
                return result;
            }
            const std::byte* const segment =
                mappings_->segments +
                segment_index * static_cast<std::size_t>(header.segment_stride);
            const auto* const segment_header =
                reinterpret_cast<const SegmentDataHeader*>(segment);
            if (segment_header->generation !=
                    snapshot.segment_generation ||
                segment_header->ipc_payload_bytes !=
                    snapshot.payload_bytes ||
                segment_header->row_count != snapshot.row_count ||
                segment_header->payload_crc32 != snapshot.payload_crc32) {
                result.code = ReadCode::kCorrupt;
                result.error = "Arrow ring segment header mismatch";
                return result;
            }
            const auto* const payload = reinterpret_cast<const std::uint8_t*>(
                segment + sizeof(SegmentDataHeader));
            const std::uint32_t crc = arrow::internal::crc32(
                0U, payload,
                static_cast<std::size_t>(snapshot.payload_bytes));
            if (crc != snapshot.payload_crc32) {
                result.code = ReadCode::kCorrupt;
                result.error = "Arrow ring payload CRC32 mismatch";
                return result;
            }
            ReadMetadata metadata{};
            metadata.batch_sequence = expected_sequence;
            metadata.oldest_available_sequence = oldest;
            metadata.newest_available_sequence = newest;
            metadata.feed_session_epoch = snapshot.feed_session_epoch;
            metadata.first_ingress_sequence =
                snapshot.first_ingress_sequence;
            metadata.last_ingress_sequence =
                snapshot.last_ingress_sequence;
            metadata.minimum_exchange_time_ns =
                snapshot.minimum_exchange_time_ns;
            metadata.maximum_exchange_time_ns =
                snapshot.maximum_exchange_time_ns;
            metadata.publish_monotonic_ns =
                snapshot.publish_monotonic_ns;
            metadata.row_count = snapshot.row_count;
            metadata.flags = snapshot.flags;
            metadata.producer_instance = {
                header.producer_instance_high,
                header.producer_instance_low};
            std::shared_ptr<ArrowBatchLease::Impl> lease_impl =
                std::make_shared<ArrowBatchLease::Impl>(
                    registration_, segment_index, payload,
                    static_cast<std::size_t>(snapshot.payload_bytes),
                    metadata);
            pin_guard.Release();
            result.lease = std::shared_ptr<ArrowBatchLease>(
                new ArrowBatchLease(std::move(lease_impl)));
            result.metadata = metadata;
            result.code = ReadCode::kBatch;
            if (next_sequence_ ==
                std::numeric_limits<std::uint64_t>::max()) {
                sequence_exhausted_ = true;
            } else {
                ++next_sequence_;
            }
            AtomicStoreRelease(&registration_->entry()->next_sequence,
                               next_sequence_);
            return result;
        } catch (const std::exception& exception) {
            result.code = ReadCode::kCorrupt;
            result.error = std::string("Arrow ring read failed: ") +
                           exception.what();
            return result;
        } catch (...) {
            result.code = ReadCode::kCorrupt;
            result.error = "Arrow ring read failed unexpectedly";
            return result;
        }
    }

    void RecordOverrun() noexcept {
        ConsumerEntry* const entry = registration_->entry();
        const std::uint64_t count =
            AtomicLoadRelaxed(&entry->overrun_count);
        if (count != std::numeric_limits<std::uint64_t>::max()) {
            AtomicStoreRelease(&entry->overrun_count, count + 1U);
        }
    }

    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> Decode(
        const std::shared_ptr<ArrowBatchLease>& lease,
        std::string* error) const {
        if (lease == nullptr || lease->data() == nullptr ||
            lease->size() == 0U) {
            SetError(error, "Arrow batch lease is empty");
            return nullptr;
        }
        if (lease->size() > static_cast<std::size_t>(
                                std::numeric_limits<std::int64_t>::max())) {
            SetError(error, "Arrow batch lease exceeds int64 size");
            return nullptr;
        }
        try {
            auto parent = std::make_shared<LeaseArrowBuffer>(
                lease->data(), static_cast<std::int64_t>(lease->size()),
                lease);
            auto input = std::make_shared<arrow::io::BufferReader>(parent);
            arrow::ipc::IpcReadOptions options =
                arrow::ipc::IpcReadOptions::Defaults();
            options.use_threads = false;
            const arrow::Result<std::shared_ptr<arrow::RecordBatch>> decoded =
                arrow::ipc::ReadRecordBatch(
                    schema_, dictionary_memo_.get(), options, input.get());
            if (!decoded.ok()) {
                SetError(error, decoded.status().ToString());
                return nullptr;
            }
            if ((*decoded)->num_rows() != lease->metadata().row_count ||
                !(*decoded)->schema()->Equals(*schema_, true)) {
                SetError(error,
                         "decoded Arrow batch does not match ring metadata");
                return nullptr;
            }
            SetError(error, {});
            return *decoded;
        } catch (const std::exception& exception) {
            SetError(error, std::string("Arrow batch decode failed: ") +
                                exception.what());
            return nullptr;
        }
    }

    RingLocation location_;
    std::shared_ptr<ReaderMappings> mappings_;
    std::shared_ptr<ConsumerRegistration> registration_;
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<arrow::ipc::DictionaryMemo> dictionary_memo_;
    std::uint64_t next_sequence_ = 1U;
    bool sequence_exhausted_ = false;
};

std::unique_ptr<SharedArrowRingWriter> SharedArrowRingWriter::Create(
    RingWriterConfig config,
    std::shared_ptr<arrow::Schema> schema,
    std::string* error) {
    if constexpr (std::endian::native != std::endian::little) {
        SetError(error, "Arrow ring protocol currently requires little endian");
        return nullptr;
    }
    if (schema == nullptr || schema->num_fields() == 0 ||
        SchemaContainsDictionary(*schema)) {
        SetError(error,
                 "Arrow ring schema must be nonempty and dictionary-free");
        return nullptr;
    }
    if (config.location.data_path.empty() ||
        config.location.control_path.empty() ||
        config.location.data_path == config.location.control_path ||
        !IsRingStreamKind(config.stream_kind) ||
        !IsPowerOfTwo(config.descriptor_capacity) ||
        config.descriptor_capacity < 2U ||
        config.descriptor_capacity >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.segment_count < config.descriptor_capacity ||
        config.segment_count >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.segment_payload_bytes < 1'024U ||
        config.segment_payload_bytes >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.maximum_consumers == 0U ||
        config.maximum_consumers > kMaximumArrowRingConsumers ||
        config.feed_session_epoch == 0U) {
        SetError(error, "invalid Arrow ring capacity or path configuration");
        return nullptr;
    }
    if (config.producer_instance == ProducerInstanceId{}) {
        config.producer_instance = GenerateProducerInstanceId(error);
        if (config.producer_instance == ProducerInstanceId{}) {
            return nullptr;
        }
    }
    const ProducerInstanceId ring_instance = GenerateProducerInstanceId(error);
    if (ring_instance == ProducerInstanceId{}) {
        return nullptr;
    }
    try {
        const arrow::Result<std::shared_ptr<arrow::Buffer>> serialized_schema =
            arrow::ipc::SerializeSchema(*schema);
        if (!serialized_schema.ok()) {
            SetError(error, serialized_schema.status().ToString());
            return nullptr;
        }
        const std::shared_ptr<arrow::Buffer>& schema_buffer =
            *serialized_schema;
        if (schema_buffer->size() <= 0) {
            SetError(error, "serialized Arrow schema is empty");
            return nullptr;
        }
        if (static_cast<std::uint64_t>(schema_buffer->size()) >
                kMaximumArrowRingSchemaBytes) {
            SetError(error,
                     "serialized Arrow schema exceeds the ring limit");
            return nullptr;
        }
        const std::size_t schema_bytes =
            static_cast<std::size_t>(schema_buffer->size());
        Layout layout{};
        if (!ComputeLayout(schema_bytes, config, &layout, error)) {
            return nullptr;
        }
        if (!config.location.data_path.parent_path().empty()) {
            std::filesystem::create_directories(
                config.location.data_path.parent_path());
        }
        if (config.location.control_path.parent_path() !=
                config.location.data_path.parent_path() &&
            !config.location.control_path.parent_path().empty()) {
            std::filesystem::create_directories(
                config.location.control_path.parent_path());
        }
        CreatedFileCleanup data_cleanup(config.location.data_path);
        CreatedFileCleanup control_cleanup(config.location.control_path);
        std::unique_ptr<MappedFile> data = MappedFile::Create(
            config.location.data_path, layout.data_bytes, error);
        if (data == nullptr) {
            return nullptr;
        }
        data_cleanup.Arm();
        std::unique_ptr<MappedFile> control = MappedFile::Create(
            config.location.control_path, layout.control_bytes, error);
        if (control == nullptr) {
            return nullptr;
        }
        control_cleanup.Arm();
        auto* const data_header =
            ::new (static_cast<void*>(data->bytes())) DataHeader{};
        auto* const control_header =
            ::new (static_cast<void*>(control->bytes())) ControlHeader{};
        data_header->magic = kDataMagic;
        data_header->protocol_version = kArrowRingProtocolVersion;
        data_header->header_region_bytes = kHeaderRegionBytes;
        data_header->stream_kind =
            static_cast<std::uint32_t>(config.stream_kind);
        data_header->shard_id = config.shard_id;
        data_header->descriptor_capacity =
            static_cast<std::uint32_t>(config.descriptor_capacity);
        data_header->segment_count =
            static_cast<std::uint32_t>(config.segment_count);
        data_header->segment_payload_bytes =
            static_cast<std::uint32_t>(config.segment_payload_bytes);
        data_header->maximum_consumers =
            static_cast<std::uint32_t>(config.maximum_consumers);
        data_header->schema_offset = layout.schema_offset;
        data_header->schema_bytes = schema_bytes;
        data_header->descriptors_offset = layout.descriptors_offset;
        data_header->segments_offset = layout.segments_offset;
        data_header->segment_stride = layout.segment_stride;
        data_header->file_bytes = layout.data_bytes;
        data_header->producer_instance_high =
            config.producer_instance.high;
        data_header->producer_instance_low = config.producer_instance.low;
        data_header->ring_instance_high = ring_instance.high;
        data_header->ring_instance_low = ring_instance.low;
        data_header->initial_feed_session_epoch =
            config.feed_session_epoch;
        data_header->created_utc_ns = ClockNowNs(CLOCK_REALTIME);
        std::memcpy(data->bytes() + layout.schema_offset,
                    schema_buffer->data(), schema_bytes);
        data_header->schema_fingerprint = Fnv1a64(
            reinterpret_cast<const std::uint8_t*>(
                data->bytes() + layout.schema_offset),
            schema_bytes);

        control_header->magic = kControlMagic;
        control_header->protocol_version = kArrowRingProtocolVersion;
        control_header->header_region_bytes = kHeaderRegionBytes;
        control_header->maximum_consumers =
            static_cast<std::uint32_t>(config.maximum_consumers);
        control_header->segment_count =
            static_cast<std::uint32_t>(config.segment_count);
        control_header->consumer_entries_offset =
            layout.consumer_entries_offset;
        control_header->segment_controls_offset =
            layout.segment_controls_offset;
        control_header->file_bytes = layout.control_bytes;
        control_header->producer_instance_high =
            config.producer_instance.high;
        control_header->producer_instance_low = config.producer_instance.low;
        control_header->ring_instance_high = ring_instance.high;
        control_header->ring_instance_low = ring_instance.low;

        auto* const descriptors = reinterpret_cast<BatchDescriptor*>(
            data->bytes() + layout.descriptors_offset);
        for (std::size_t index = 0U;
             index < config.descriptor_capacity; ++index) {
            ::new (static_cast<void*>(&descriptors[index]))
                BatchDescriptor{};
        }
        auto* const consumers = reinterpret_cast<ConsumerEntry*>(
            control->bytes() + layout.consumer_entries_offset);
        for (std::size_t index = 0U;
             index < config.maximum_consumers; ++index) {
            ::new (static_cast<void*>(&consumers[index])) ConsumerEntry{};
        }
        auto* const segment_controls = reinterpret_cast<SegmentControl*>(
            control->bytes() + layout.segment_controls_offset);
        for (std::size_t index = 0U; index < config.segment_count; ++index) {
            std::byte* const segment =
                data->bytes() + layout.segments_offset +
                index * layout.segment_stride;
            ::new (static_cast<void*>(segment)) SegmentDataHeader{};
            ::new (static_cast<void*>(&segment_controls[index]))
                SegmentControl{};
        }

        AtomicStoreRelease(
            &data_header->producer_heartbeat_monotonic_ns,
            ClockNowNs(CLOCK_MONOTONIC));
        AtomicStoreRelease(
            &data_header->producer_state,
            static_cast<std::uint64_t>(ProducerState::kActive));
        auto impl = std::make_unique<Impl>(
            std::move(config), std::move(schema), std::move(data),
            std::move(control), layout, data_header->schema_fingerprint);
        auto writer = std::unique_ptr<SharedArrowRingWriter>(
            new SharedArrowRingWriter(std::move(impl)));
        data_cleanup.Release();
        control_cleanup.Release();
        SetError(error, {});
        return writer;
    } catch (const std::exception& exception) {
        SetError(error, std::string("Arrow ring creation failed: ") +
                            exception.what());
        return nullptr;
    }
}

SharedArrowRingWriter::SharedArrowRingWriter(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SharedArrowRingWriter::~SharedArrowRingWriter() = default;

PublishResult SharedArrowRingWriter::TryPublish(
    const arrow::RecordBatch& batch,
    const BatchMetadata& metadata) noexcept {
    return impl_->TryPublish(batch, metadata);
}

void SharedArrowRingWriter::TouchHeartbeat(
    std::uint64_t monotonic_ns) noexcept {
    impl_->TouchHeartbeat(monotonic_ns);
}

void SharedArrowRingWriter::Seal(std::uint64_t monotonic_ns) noexcept {
    impl_->Seal(monotonic_ns);
}

void SharedArrowRingWriter::ReapDeadConsumers() noexcept {
    impl_->ReapDeadConsumers();
}

RingWriterStats SharedArrowRingWriter::stats() const noexcept {
    return impl_->Stats();
}

const RingWriterConfig& SharedArrowRingWriter::config() const noexcept {
    return impl_->config_;
}

const std::shared_ptr<arrow::Schema>& SharedArrowRingWriter::schema()
    const noexcept {
    return impl_->schema_;
}

std::uint64_t SharedArrowRingWriter::schema_fingerprint() const noexcept {
    return impl_->schema_fingerprint_;
}

ArrowBatchLease::ArrowBatchLease(std::shared_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ArrowBatchLease::~ArrowBatchLease() = default;

const std::uint8_t* ArrowBatchLease::data() const noexcept {
    return impl_->data_;
}

std::size_t ArrowBatchLease::size() const noexcept { return impl_->size_; }

const ReadMetadata& ArrowBatchLease::metadata() const noexcept {
    return impl_->metadata_;
}

std::unique_ptr<SharedArrowRingReader> SharedArrowRingReader::Open(
    RingLocation location,
    ReaderStart start,
    std::string* error) {
    if constexpr (std::endian::native != std::endian::little) {
        SetError(error, "Arrow ring protocol currently requires little endian");
        return nullptr;
    }
    if (start != ReaderStart::kLatest &&
        start != ReaderStart::kEarliestAvailable) {
        SetError(error, "invalid Arrow ring reader start mode");
        return nullptr;
    }
    try {
        auto mappings = std::make_shared<ReaderMappings>();
        mappings->data = MappedFile::Open(location.data_path, false, error);
        if (mappings->data == nullptr ||
            mappings->data->size() < kHeaderRegionBytes) {
            if (mappings->data != nullptr) {
                SetError(error, "Arrow data file is smaller than its header");
            }
            return nullptr;
        }
        mappings->control =
            MappedFile::Open(location.control_path, true, error);
        if (mappings->control == nullptr ||
            mappings->control->size() < kHeaderRegionBytes) {
            if (mappings->control != nullptr) {
                SetError(error,
                         "Arrow control file is smaller than its header");
            }
            return nullptr;
        }
        mappings->data_header =
            reinterpret_cast<const DataHeader*>(mappings->data->bytes());
        mappings->control_header =
            reinterpret_cast<ControlHeader*>(mappings->control->bytes());
        if (!ValidateDataHeader(*mappings->data_header,
                                mappings->data->size(), error) ||
            !ValidateControlHeader(*mappings->control_header,
                                   *mappings->data_header,
                                   mappings->control->size(), error)) {
            return nullptr;
        }
        mappings->descriptors =
            reinterpret_cast<const BatchDescriptor*>(
                mappings->data->bytes() +
                mappings->data_header->descriptors_offset);
        mappings->segments = mappings->data->bytes() +
                             mappings->data_header->segments_offset;
        mappings->consumers = reinterpret_cast<ConsumerEntry*>(
            mappings->control->bytes() +
            mappings->control_header->consumer_entries_offset);
        mappings->segment_controls = reinterpret_cast<SegmentControl*>(
            mappings->control->bytes() +
            mappings->control_header->segment_controls_offset);

        auto dictionary_memo =
            std::make_unique<arrow::ipc::DictionaryMemo>();
        const auto* const schema_data =
            reinterpret_cast<const std::uint8_t*>(mappings->data->bytes() +
                mappings->data_header->schema_offset);
        if (Fnv1a64(
                schema_data,
                static_cast<std::size_t>(
                    mappings->data_header->schema_bytes)) !=
            mappings->data_header->schema_fingerprint) {
            SetError(error, "Arrow ring schema fingerprint mismatch");
            return nullptr;
        }
        auto schema_buffer = std::make_shared<arrow::Buffer>(
            schema_data,
            static_cast<std::int64_t>(
                mappings->data_header->schema_bytes));
        arrow::io::BufferReader schema_input(schema_buffer);
        const arrow::Result<std::unique_ptr<arrow::ipc::Message>> message =
            arrow::ipc::ReadMessage(&schema_input);
        if (!message.ok() || *message == nullptr) {
            SetError(error, message.ok()
                ? "Arrow ring schema message is empty"
                : message.status().ToString());
            return nullptr;
        }
        const arrow::Result<std::shared_ptr<arrow::Schema>> schema_result =
            arrow::ipc::ReadSchema(**message, dictionary_memo.get());
        if (!schema_result.ok()) {
            SetError(error, schema_result.status().ToString());
            return nullptr;
        }
        const std::shared_ptr<arrow::Schema>& schema = *schema_result;
        if (SchemaContainsDictionary(*schema)) {
            SetError(error, "Arrow ring schema is unsupported or corrupt");
            return nullptr;
        }

        const pid_t pid = ::getpid();
        const std::uint64_t start_ticks = ReadProcessStartTicks(pid);
        if (start_ticks == 0U) {
            SetError(error, "cannot read consumer process start time");
            return nullptr;
        }
        std::size_t consumer_index =
            mappings->data_header->maximum_consumers;
        ConsumerSlotReservation reservation;
        for (std::size_t index = 0U;
             index < mappings->data_header->maximum_consumers; ++index) {
            ConsumerEntry& entry = mappings->consumers[index];
            std::uint64_t expected = kConsumerFree;
            if (AtomicCompareExchange(&entry.state, &expected,
                                      EncodeConsumerState(
                                          pid, kConsumerInitializing))) {
                reservation.Arm(&entry);
                consumer_index = index;
                entry.pid = static_cast<std::uint64_t>(pid);
                entry.process_start_ticks = start_ticks;
                entry.heartbeat_monotonic_ns = ClockNowNs(CLOCK_MONOTONIC);
                entry.nonce = ClockNowNs(CLOCK_REALTIME) ^
                              static_cast<std::uint64_t>(pid) ^
                              static_cast<std::uint64_t>(index);
                break;
            }
        }
        if (consumer_index >=
            mappings->data_header->maximum_consumers) {
            SetError(error, "Arrow ring consumer registry is full");
            return nullptr;
        }
        const std::uint64_t newest = AtomicLoadAcquire(
            &mappings->data_header->published_sequence);
        const bool sequence_exhausted =
            start == ReaderStart::kLatest &&
            newest == std::numeric_limits<std::uint64_t>::max();
        const std::uint64_t next =
            start == ReaderStart::kLatest
                ? (sequence_exhausted ? newest : newest + 1U)
                : OldestAvailable(
                      newest, mappings->data_header->descriptor_capacity);
        ConsumerEntry& entry = mappings->consumers[consumer_index];
        entry.overrun_count = 0U;
        entry.next_sequence = next;
        auto registration = std::make_shared<ConsumerRegistration>(
            mappings, consumer_index);
        AtomicStoreRelease(&entry.state,
                           EncodeConsumerState(pid, kConsumerActive));
        auto impl = std::make_unique<Impl>(
            std::move(location), std::move(mappings),
            std::move(registration), schema,
            std::move(dictionary_memo), next, sequence_exhausted);
        auto reader = std::unique_ptr<SharedArrowRingReader>(
            new SharedArrowRingReader(std::move(impl)));
        reservation.Release();
        SetError(error, {});
        return reader;
    } catch (const std::exception& exception) {
        SetError(error, std::string("Arrow ring open failed: ") +
                            exception.what());
        return nullptr;
    }
}

SharedArrowRingReader::SharedArrowRingReader(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SharedArrowRingReader::~SharedArrowRingReader() = default;

ReadResult SharedArrowRingReader::TryRead() noexcept {
    return impl_->TryRead();
}

bool SharedArrowRingReader::SeekToEarliestAvailable() noexcept {
    const std::uint64_t newest = AtomicLoadAcquire(
        &impl_->mappings_->data_header->published_sequence);
    impl_->next_sequence_ = OldestAvailable(
        newest, impl_->mappings_->data_header->descriptor_capacity);
    impl_->sequence_exhausted_ = false;
    AtomicStoreRelease(&impl_->registration_->entry()->next_sequence,
                       impl_->next_sequence_);
    return true;
}

bool SharedArrowRingReader::SeekToLatest() noexcept {
    const std::uint64_t newest = AtomicLoadAcquire(
        &impl_->mappings_->data_header->published_sequence);
    impl_->sequence_exhausted_ =
        newest == std::numeric_limits<std::uint64_t>::max();
    impl_->next_sequence_ = impl_->sequence_exhausted_
        ? newest
        : newest + 1U;
    AtomicStoreRelease(&impl_->registration_->entry()->next_sequence,
                       impl_->next_sequence_);
    return true;
}

void SharedArrowRingReader::TouchHeartbeat(
    std::uint64_t monotonic_ns) noexcept {
    AtomicStoreRelease(
        &impl_->registration_->entry()->heartbeat_monotonic_ns,
        monotonic_ns);
}

std::shared_ptr<arrow::RecordBatch> SharedArrowRingReader::Decode(
    const std::shared_ptr<ArrowBatchLease>& lease,
    std::string* error) const {
    return impl_->Decode(lease, error);
}

const std::shared_ptr<arrow::Schema>& SharedArrowRingReader::schema()
    const noexcept {
    return impl_->schema_;
}

RingStreamKind SharedArrowRingReader::stream_kind() const noexcept {
    return static_cast<RingStreamKind>(
        impl_->mappings_->data_header->stream_kind);
}

std::uint32_t SharedArrowRingReader::shard_id() const noexcept {
    return impl_->mappings_->data_header->shard_id;
}

ProducerInstanceId SharedArrowRingReader::producer_instance() const noexcept {
    return {impl_->mappings_->data_header->producer_instance_high,
            impl_->mappings_->data_header->producer_instance_low};
}

std::uint64_t SharedArrowRingReader::feed_session_epoch() const noexcept {
    return impl_->mappings_->data_header->initial_feed_session_epoch;
}

ProducerState SharedArrowRingReader::producer_state() const noexcept {
    return static_cast<ProducerState>(AtomicLoadAcquire(
        &impl_->mappings_->data_header->producer_state));
}

std::uint64_t SharedArrowRingReader::producer_heartbeat_monotonic_ns()
    const noexcept {
    return AtomicLoadAcquire(
        &impl_->mappings_->data_header->producer_heartbeat_monotonic_ns);
}

std::uint64_t SharedArrowRingReader::next_sequence() const noexcept {
    return impl_->next_sequence_;
}

ProducerInstanceId GenerateProducerInstanceId(std::string* error) noexcept {
    ProducerInstanceId id{};
    std::byte* destination = reinterpret_cast<std::byte*>(&id);
    std::size_t remaining = sizeof(id);
    while (remaining != 0U) {
        const ssize_t count = ::getrandom(destination, remaining, 0U);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            SetError(error, ErrnoText("getrandom", errno));
            return {};
        }
        if (count == 0) {
            SetError(error, "getrandom returned zero bytes");
            return {};
        }
        const std::size_t produced = static_cast<std::size_t>(count);
        destination += produced;
        remaining -= produced;
    }
    if (id == ProducerInstanceId{}) {
        id.low = 1U;
    }
    SetError(error, {});
    return id;
}

std::string ProducerInstanceIdString(ProducerInstanceId value) {
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << value.high
           << std::setw(16) << value.low;
    return stream.str();
}

}  // namespace l2flow::arrow_hot
