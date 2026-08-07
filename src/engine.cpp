#include "l2flow/ingest/engine.h"

#include "l2flow/ingest/raw_tap.h"

#include "decoder_internal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace l2flow::ingest {
namespace {

template <typename Unsigned>
[[nodiscard]] bool LoadHeaderUnsigned(
    std::span<const std::byte> bytes,
    std::size_t offset,
    Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr || offset > bytes.size() ||
        sizeof(Unsigned) > bytes.size() - offset) {
        return false;
    }
    Unsigned value = 0U;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        const auto octet = static_cast<Unsigned>(
            std::to_integer<unsigned char>(bytes[offset + index]));
        value |= static_cast<Unsigned>(
            octet << static_cast<unsigned int>(index * 8U));
    }
    *output = value;
    return true;
}

[[nodiscard]] bool IsPowerOfTwo(std::size_t value) noexcept {
    return value != 0U && (value & (value - 1U)) == 0U;
}

[[nodiscard]] bool RoundUpPowerOfTwo(std::size_t requested,
                                     std::size_t* output) noexcept {
    if (output == nullptr || requested == 0U ||
        requested > (std::numeric_limits<std::size_t>::max() >> 1U)) {
        return false;
    }
    std::size_t value = 1U;
    while (value < requested) {
        value <<= 1U;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool IsLeapYear(std::uint32_t year) noexcept {
    return year % 4U == 0U && (year % 100U != 0U || year % 400U == 0U);
}

[[nodiscard]] bool IsTradeDateValid(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U || month > 12U ||
        day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days[month - 1U];
    if (month == 2U && IsLeapYear(year)) {
        maximum = 29U;
    }
    return day <= maximum;
}

inline void CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

template <typename T>
class UninitializedArray final {
public:
    // C++20 memcpy starts the lifetime of an implicit-lifetime T in allocator
    // storage. Keeping this wrapper limited to trivial records avoids eager
    // construction/page touching without adding object-lifetime ambiguity.
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::is_trivially_destructible_v<T>);

    explicit UninitializedArray(std::size_t size)
        : size_(size), data_(allocator_.allocate(size)) {}

    ~UninitializedArray() {
        if (data_ != nullptr) {
            allocator_.deallocate(data_, size_);
        }
    }

    UninitializedArray(const UninitializedArray&) = delete;
    UninitializedArray& operator=(const UninitializedArray&) = delete;

    void Store(std::size_t index, const T& value) noexcept {
        std::memcpy(static_cast<void*>(data_ + index),
                    static_cast<const void*>(std::addressof(value)),
                    sizeof(T));
    }

    void Load(std::size_t index, T* output) const noexcept {
        std::memcpy(static_cast<void*>(output),
                    static_cast<const void*>(data_ + index), sizeof(T));
    }

    [[nodiscard]] const T& Get(std::size_t index) const noexcept {
        return data_[index];
    }

private:
    std::allocator<T> allocator_;
    std::size_t size_ = 0U;
    T* data_ = nullptr;
};

[[nodiscard]] std::size_t CheckedRingCapacity(std::size_t requested) {
    std::size_t capacity = 0U;
    if (!RoundUpPowerOfTwo(requested, &capacity) ||
        capacity > static_cast<std::size_t>(
                       std::numeric_limits<std::uint64_t>::max())) {
        throw std::invalid_argument("invalid SPSC ring capacity");
    }
    return capacity;
}

template <typename T>
class SpscRing final {
public:
    explicit SpscRing(std::size_t requested_capacity)
        : capacity_(CheckedRingCapacity(requested_capacity)),
          storage_(capacity_),
          mask_(capacity_ - 1U) {}

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    [[nodiscard]] bool TryPush(const T& value) noexcept {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head - cached_tail_ >=
            static_cast<std::uint64_t>(capacity_)) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (head - cached_tail_ >=
                static_cast<std::uint64_t>(capacity_)) {
                return false;
            }
        }
        storage_.Store(static_cast<std::size_t>(head) & mask_, value);
        head_.store(head + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(T* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (cached_head_ == tail) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (cached_head_ == tail) {
                return false;
            }
        }
        storage_.Load(static_cast<std::size_t>(tail) & mask_, output);
        tail_.store(tail + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return tail_.load(std::memory_order_acquire) ==
               head_.load(std::memory_order_acquire);
    }

private:
    std::size_t capacity_ = 0U;
    UninitializedArray<T> storage_;
    std::size_t mask_ = 0U;
    alignas(64) std::atomic<std::uint64_t> head_{0U};
    std::uint64_t cached_tail_ = 0U;
    alignas(64) std::atomic<std::uint64_t> tail_{0U};
    std::uint64_t cached_head_ = 0U;
};

struct LaneMessageMetadata final {
    ParsedHeader header{};
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t receive_monotonic_ns = 0U;
    std::uint32_t body_size = 0U;
};

class LaneStorage final {
public:
    LaneStorage(std::size_t slot_count, std::size_t maximum_body_bytes)
        : slot_count_(slot_count),
          maximum_body_bytes_(maximum_body_bytes),
          metadata_(slot_count),
          arena_(std::make_unique_for_overwrite<std::byte[]>(
              slot_count * maximum_body_bytes)),
          ready_(slot_count),
          recycled_(slot_count) {
        free_.reserve(slot_count);
        for (std::size_t index = slot_count; index > 0U; --index) {
            free_.push_back(static_cast<std::uint32_t>(index - 1U));
        }
    }

    [[nodiscard]] bool TryPublish(
        const ParsedHeader& header,
        std::span<const std::byte> body,
        std::uint64_t ingress_sequence,
        std::uint64_t receive_monotonic_ns) noexcept {
        if (body.size() > maximum_body_bytes_ ||
            body.size() > std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
        DrainRecycled();
        if (free_.empty()) {
            return false;
        }
        const std::uint32_t index = free_.back();
        free_.pop_back();
        LaneMessageMetadata metadata{};
        metadata.header = header;
        metadata.ingress_sequence = ingress_sequence;
        metadata.receive_monotonic_ns = receive_monotonic_ns;
        metadata.body_size = static_cast<std::uint32_t>(body.size());
        metadata_.Store(index, metadata);
        std::memcpy(arena_.get() +
                        static_cast<std::size_t>(index) *
                            maximum_body_bytes_,
                    body.data(), body.size());
        if (!ready_.TryPush(index)) {
            free_.push_back(index);
            return false;
        }
        return true;
    }

    [[nodiscard]] bool TryConsume(std::uint32_t* index,
                                  internal::OwnedMessageView* view) noexcept {
        if (index == nullptr || view == nullptr || !ready_.TryPop(index)) {
            return false;
        }
        const LaneMessageMetadata& metadata = metadata_.Get(*index);
        view->header = metadata.header;
        view->ingress_sequence = metadata.ingress_sequence;
        view->receive_monotonic_ns = metadata.receive_monotonic_ns;
        view->body = std::span<const std::byte>(
            arena_.get() + static_cast<std::size_t>(*index) *
                               maximum_body_bytes_,
            metadata.body_size);
        return true;
    }

    [[nodiscard]] bool Release(std::uint32_t index) noexcept {
        return index < slot_count_ && recycled_.TryPush(index);
    }

    [[nodiscard]] bool empty() const noexcept { return ready_.empty(); }

private:
    void DrainRecycled() noexcept {
        std::uint32_t index = 0U;
        while (recycled_.TryPop(&index)) {
            free_.push_back(index);
        }
    }

    std::size_t slot_count_ = 0U;
    std::size_t maximum_body_bytes_ = 0U;
    UninitializedArray<LaneMessageMetadata> metadata_;
    std::unique_ptr<std::byte[]> arena_;
    SpscRing<std::uint32_t> ready_;
    SpscRing<std::uint32_t> recycled_;
    std::vector<std::uint32_t> free_;
};

struct alignas(64) AdmissionStats final {
    std::atomic<std::uint64_t> callbacks{0U};
    std::atomic<std::uint64_t> admitted{0U};
    std::atomic<std::uint64_t> rejected{0U};
    std::atomic<std::uint64_t> lane_full{0U};
};

struct alignas(64) TickLaneStats final {
    std::atomic<std::uint64_t> decoded_ticks{0U};
    std::atomic<std::uint64_t> decode_errors{0U};
    std::atomic<std::uint64_t> catalog_misses{0U};
    std::atomic<std::uint64_t> dispatched_ticks{0U};
    std::atomic<std::uint64_t> duplicates_or_late{0U};
    std::atomic<std::uint64_t> late_recovery_dispatched{0U};
    std::atomic<std::uint64_t> gaps_skipped{0U};
    std::atomic<std::uint64_t> from_open_channels_frozen{0U};
    std::atomic<std::uint64_t> channel_faults_dispatched{0U};
    std::atomic<std::uint64_t> dispatch_overflows{0U};
};

struct alignas(64) SnapshotLaneStats final {
    std::atomic<std::uint64_t> decoded_snapshots{0U};
    std::atomic<std::uint64_t> decode_errors{0U};
    std::atomic<std::uint64_t> catalog_misses{0U};
    std::atomic<std::uint64_t> dispatched_snapshots{0U};
    std::atomic<std::uint64_t> dispatch_overflows{0U};
};

class GapMailboxSlot final {
public:
    enum class SnapshotResult : std::uint8_t {
        kReady,
        kNoChange,
        kBusy,
    };

    void Publish(const ChannelGap& gap) noexcept {
        // Three immutable-at-publication buffers let the single producer
        // avoid both waiting and overwriting a buffer held by a preempted
        // consumer. The versioned token prevents an index ABA during the
        // consumer's claim/recheck handshake.
        const std::uint64_t current_token =
            published_token_.load(std::memory_order_seq_cst);
        const std::size_t current_index =
            static_cast<std::size_t>(current_token & UINT64_C(3));
        const std::size_t reader_index = static_cast<std::size_t>(
            reader_index_.load(std::memory_order_seq_cst));
        std::size_t target = (current_index + 1U) % buffers_.size();
        if (target == reader_index) {
            target = (target + 1U) % buffers_.size();
        }
        buffers_[target] = gap;
        ++producer_generation_;
        const std::uint64_t token =
            (producer_generation_ << 2U) |
            static_cast<std::uint64_t>(target);
        published_token_.store(token, std::memory_order_seq_cst);
    }

    [[nodiscard]] SnapshotResult TrySnapshot(
        ChannelGap* output) noexcept {
        if (output == nullptr) {
            return SnapshotResult::kNoChange;
        }
        constexpr std::size_t kMaximumAttempts = 8U;
        for (std::size_t attempt = 0U; attempt < kMaximumAttempts;
             ++attempt) {
            const std::uint64_t before =
                published_token_.load(std::memory_order_seq_cst);
            const std::uint8_t index = static_cast<std::uint8_t>(
                before & UINT64_C(3));
            if (index >= buffers_.size()) {
                return SnapshotResult::kNoChange;
            }
            reader_index_.store(index, std::memory_order_seq_cst);
            const std::uint64_t after =
                published_token_.load(std::memory_order_seq_cst);
            if (before != after) {
                reader_index_.store(kNoReader,
                                    std::memory_order_seq_cst);
                CpuRelax();
                continue;
            }
            const ChannelGap snapshot = buffers_[index];
            reader_index_.store(kNoReader, std::memory_order_seq_cst);
            if (delivered_token_ == before) {
                return SnapshotResult::kNoChange;
            }
            delivered_token_ = before;
            *output = snapshot;
            return SnapshotResult::kReady;
        }
        return SnapshotResult::kBusy;
    }

private:
    static constexpr std::uint8_t kNoReader = 3U;
    std::array<ChannelGap, 3U> buffers_{};
    std::atomic<std::uint64_t> published_token_{UINT64_C(3)};
    std::atomic<std::uint8_t> reader_index_{kNoReader};
    std::uint64_t producer_generation_ = 0U;
    std::uint64_t delivered_token_ = UINT64_C(3);
};

class Dispatcher final {
public:
    Dispatcher(std::size_t tick_producers,
               std::size_t snapshot_producers,
               std::size_t owners,
               std::size_t queue_capacity,
               std::size_t fault_capacity,
               std::size_t late_recovery_capacity,
               std::size_t maximum_channels_per_tick_producer)
        : tick_producers_(tick_producers),
          snapshot_producers_(snapshot_producers),
          owners_(owners),
          gap_channels_per_producer_(maximum_channels_per_tick_producer),
          gap_words_per_producer_(
              (maximum_channels_per_tick_producer + 63U) / 64U),
          gap_slots_(std::make_unique<GapMailboxSlot[]>(
              tick_producers * maximum_channels_per_tick_producer)),
          gap_dirty_words_(std::make_unique<std::atomic<std::uint64_t>[]>(
              tick_producers * gap_words_per_producer_)),
          gap_consumer_pending_(
              tick_producers * gap_words_per_producer_, 0U),
          tick_consumer_cursor_(owners, 0U),
          snapshot_consumer_cursor_(owners, 0U) {
        tick_queues_.reserve(tick_producers * owners);
        for (std::size_t index = 0U; index < tick_producers * owners;
             ++index) {
            tick_queues_.push_back(
                std::make_unique<SpscRing<CanonicalTick>>(queue_capacity));
        }
        snapshot_queues_.reserve(snapshot_producers * owners);
        for (std::size_t index = 0U; index < snapshot_producers * owners;
             ++index) {
            snapshot_queues_.push_back(
                std::make_unique<SpscRing<CanonicalSnapshot>>(
                    queue_capacity));
        }
        fault_queues_.reserve(tick_producers);
        late_recovery_queues_.reserve(tick_producers);
        for (std::size_t index = 0U; index < tick_producers; ++index) {
            fault_queues_.push_back(
                std::make_unique<SpscRing<ChannelFault>>(fault_capacity));
            late_recovery_queues_.push_back(
                std::make_unique<SpscRing<LateRecoveryTick>>(
                    late_recovery_capacity));
        }
        const std::size_t dirty_word_count =
            tick_producers * gap_words_per_producer_;
        for (std::size_t index = 0U; index < dirty_word_count; ++index) {
            gap_dirty_words_[index].store(0U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool PublishTick(std::size_t producer,
                                   std::size_t owner,
                                   const CanonicalTick& tick) noexcept {
        if (producer >= tick_producers_ || owner >= owners_) {
            return false;
        }
        return tick_queues_[producer * owners_ + owner]->TryPush(tick);
    }

    [[nodiscard]] bool PublishSnapshot(
        std::size_t producer,
        std::size_t owner,
        const CanonicalSnapshot& snapshot) noexcept {
        if (producer >= snapshot_producers_ || owner >= owners_) {
            return false;
        }
        return snapshot_queues_[producer * owners_ + owner]->TryPush(
            snapshot);
    }

    [[nodiscard]] bool PublishGap(std::size_t producer,
                                  std::size_t channel_slot,
                                  const ChannelGap& gap) noexcept {
        if (producer >= tick_producers_ ||
            channel_slot >= gap_channels_per_producer_) {
            return false;
        }
        const std::size_t slot_index =
            producer * gap_channels_per_producer_ + channel_slot;
        gap_slots_[slot_index].Publish(gap);
        const std::size_t word_index =
            producer * gap_words_per_producer_ + channel_slot / 64U;
        const std::uint64_t bit =
            UINT64_C(1) << static_cast<unsigned int>(channel_slot % 64U);
        gap_dirty_words_[word_index].fetch_or(
            bit, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool PublishFault(std::size_t producer,
                                    const ChannelFault& fault) noexcept {
        return producer < fault_queues_.size() &&
               fault_queues_[producer]->TryPush(fault);
    }

    [[nodiscard]] bool PublishLateRecovery(
        std::size_t producer,
        const LateRecoveryTick& record) noexcept {
        return producer < late_recovery_queues_.size() &&
               late_recovery_queues_[producer]->TryPush(record);
    }

    [[nodiscard]] bool TryPollTick(std::size_t owner,
                                   CanonicalTick* output) noexcept {
        if (owner >= owners_ || output == nullptr) {
            return false;
        }
        std::size_t& cursor = tick_consumer_cursor_[owner];
        for (std::size_t count = 0U; count < tick_producers_; ++count) {
            const std::size_t producer = (cursor + count) % tick_producers_;
            if (tick_queues_[producer * owners_ + owner]->TryPop(output)) {
                cursor = (producer + 1U) % tick_producers_;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool TryPollSnapshot(
        std::size_t owner,
        CanonicalSnapshot* output) noexcept {
        if (owner >= owners_ || output == nullptr) {
            return false;
        }
        std::size_t& cursor = snapshot_consumer_cursor_[owner];
        for (std::size_t count = 0U; count < snapshot_producers_; ++count) {
            const std::size_t producer =
                (cursor + count) % snapshot_producers_;
            if (snapshot_queues_[producer * owners_ + owner]->TryPop(
                    output)) {
                cursor = (producer + 1U) % snapshot_producers_;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool TryPollGap(ChannelGap* output) noexcept {
        if (output == nullptr || gap_consumer_pending_.empty()) {
            return false;
        }
        const std::size_t word_count = gap_consumer_pending_.size();
        for (std::size_t count = 0U; count < word_count; ++count) {
            const std::size_t word_index =
                (gap_consumer_word_cursor_ + count) % word_count;
            std::uint64_t& pending = gap_consumer_pending_[word_index];
            if (pending == 0U) {
                pending = gap_dirty_words_[word_index].exchange(
                    0U, std::memory_order_acq_rel);
            }
            while (pending != 0U) {
                const unsigned int bit_index =
                    std::countr_zero(pending);
                const std::uint64_t bit = UINT64_C(1) << bit_index;
                pending &= ~bit;
                const std::size_t producer =
                    word_index / gap_words_per_producer_;
                const std::size_t local_word =
                    word_index % gap_words_per_producer_;
                const std::size_t channel_slot =
                    local_word * 64U + bit_index;
                if (channel_slot >= gap_channels_per_producer_) {
                    continue;
                }
                GapMailboxSlot& slot = gap_slots_[
                    producer * gap_channels_per_producer_ + channel_slot];
                ChannelGap snapshot{};
                const GapMailboxSlot::SnapshotResult result =
                    slot.TrySnapshot(&snapshot);
                if (result == GapMailboxSlot::SnapshotResult::kBusy) {
                    pending |= bit;
                    return false;
                }
                if (result == GapMailboxSlot::SnapshotResult::kNoChange) {
                    continue;
                }
                *output = snapshot;
                gap_consumer_word_cursor_ =
                    (word_index + 1U) % word_count;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool TryPollLateRecovery(
        LateRecoveryTick* output) noexcept {
        if (output == nullptr || late_recovery_queues_.empty()) {
            return false;
        }
        for (std::size_t count = 0U;
             count < late_recovery_queues_.size(); ++count) {
            const std::size_t producer =
                (late_recovery_consumer_cursor_ + count) %
                late_recovery_queues_.size();
            if (late_recovery_queues_[producer]->TryPop(output)) {
                late_recovery_consumer_cursor_ =
                    (producer + 1U) % late_recovery_queues_.size();
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool TryPollFault(ChannelFault* output) noexcept {
        if (output == nullptr || fault_queues_.empty()) {
            return false;
        }
        for (std::size_t count = 0U; count < fault_queues_.size(); ++count) {
            const std::size_t producer =
                (fault_consumer_cursor_ + count) % fault_queues_.size();
            if (fault_queues_[producer]->TryPop(output)) {
                fault_consumer_cursor_ =
                    (producer + 1U) % fault_queues_.size();
                return true;
            }
        }
        return false;
    }

private:
    std::size_t tick_producers_ = 0U;
    std::size_t snapshot_producers_ = 0U;
    std::size_t owners_ = 0U;
    std::vector<std::unique_ptr<SpscRing<CanonicalTick>>> tick_queues_;
    std::vector<std::unique_ptr<SpscRing<CanonicalSnapshot>>>
        snapshot_queues_;
    std::vector<std::unique_ptr<SpscRing<ChannelFault>>> fault_queues_;
    std::vector<std::unique_ptr<SpscRing<LateRecoveryTick>>>
        late_recovery_queues_;
    std::size_t gap_channels_per_producer_ = 0U;
    std::size_t gap_words_per_producer_ = 0U;
    std::unique_ptr<GapMailboxSlot[]> gap_slots_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> gap_dirty_words_;
    std::vector<std::uint64_t> gap_consumer_pending_;
    std::vector<std::size_t> tick_consumer_cursor_;
    std::vector<std::size_t> snapshot_consumer_cursor_;
    std::size_t gap_consumer_word_cursor_ = 0U;
    std::size_t late_recovery_consumer_cursor_ = 0U;
    std::size_t fault_consumer_cursor_ = 0U;
};

[[nodiscard]] bool IdentityEqual(const ExactIdentity& left,
                                 const ExactIdentity& right) noexcept {
    return left.market == right.market &&
           left.security_id_source_size ==
               right.security_id_source_size &&
           left.security_id_size == right.security_id_size &&
           std::equal(left.security_id_source.begin(),
                      left.security_id_source.end(),
                      right.security_id_source.begin()) &&
           std::equal(left.security_id.begin(), left.security_id.end(),
                      right.security_id.begin());
}

[[nodiscard]] bool DecimalEqual(const FixedDecimal& left,
                                const FixedDecimal& right) noexcept {
    return left.raw == right.raw && left.source_scale == right.source_scale &&
           left.raw_valid == right.raw_valid;
}

[[nodiscard]] bool QuantityEqual(const ScaledInteger& left,
                                 const ScaledInteger& right) noexcept {
    return left.raw == right.raw && left.scale == right.scale &&
           left.valid == right.valid;
}

[[nodiscard]] bool CanonicalPayloadEqual(const CanonicalTick& left,
                                         const CanonicalTick& right) noexcept {
    return left.common.message_key == right.common.message_key &&
           left.common.kind == right.common.kind &&
           left.common.channel == right.common.channel &&
           left.common.native_sequence == right.common.native_sequence &&
           left.common.exchange_time_raw == right.common.exchange_time_raw &&
           IdentityEqual(left.common.identity, right.common.identity) &&
           DecimalEqual(left.price, right.price) &&
           DecimalEqual(left.amount, right.amount) &&
           QuantityEqual(left.quantity, right.quantity) &&
           left.primary_order_id == right.primary_order_id &&
           left.buy_order_id == right.buy_order_id &&
           left.sell_order_id == right.sell_order_id &&
           left.sh_add_matched_quantity_raw ==
               right.sh_add_matched_quantity_raw &&
           left.raw_type == right.raw_type &&
           left.raw_side == right.raw_side && left.action == right.action &&
           left.side == right.side && left.aggressor == right.aggressor &&
           left.order_type == right.order_type && left.phase == right.phase;
}

class SequenceRecovery final {
public:
    SequenceRecovery(StartMode mode,
                     std::size_t maximum_channels,
                     std::size_t entries_per_channel,
                     std::uint64_t maximum_reorder_span,
                     std::uint64_t initial_hold_ns,
                     std::uint64_t gap_wait_ns,
                     TickLaneStats* stats)
        : mode_(mode),
          entries_per_channel_(entries_per_channel),
          slot_mask_(entries_per_channel - 1U),
          maximum_reorder_span_(maximum_reorder_span),
          initial_hold_ns_(initial_hold_ns),
          gap_wait_ns_(gap_wait_ns),
          channels_(maximum_channels),
          stats_(stats) {
        std::size_t table_capacity = 1U;
        while (table_capacity < maximum_channels * 2U) {
            table_capacity <<= 1U;
        }
        channel_table_.resize(table_capacity, 0U);
        channel_table_mask_ = table_capacity - 1U;
    }

    template <typename EmitTick, typename EmitGap, typename EmitFault,
              typename EmitLate, typename Fatal>
    void Process(const internal::TickDecodeResult& decoded,
                 std::uint64_t now,
                 EmitTick&& emit_tick,
                 EmitGap&& emit_gap,
                 EmitFault&& emit_fault,
                 EmitLate&& emit_late,
                 Fatal&& fatal) noexcept {
        ChannelState* state = FindOrCreate(
            decoded.tick.common.identity.market,
            decoded.tick.common.channel);
        if (state == nullptr) {
            fatal("native-sequence channel capacity or allocation exhausted");
            return;
        }
        if (state->frozen) {
            stats_->duplicates_or_late.fetch_add(1U,
                                                 std::memory_order_relaxed);
            return;
        }
        if (mode_ == StartMode::kPartial && state->discovering) {
            const InsertResult inserted = Insert(*state, decoded);
            if (inserted == InsertResult::kDuplicate) {
                stats_->duplicates_or_late.fetch_add(
                    1U, std::memory_order_relaxed);
                return;
            }
            if (inserted == InsertResult::kConflict) {
                // PARTIAL deterministically keeps the first canonical
                // projection at that native position and diverts the other
                // body for reconciliation instead of freezing the channel.
                stats_->duplicates_or_late.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(DivertLateRecovery(
                    *state, decoded,
                    LateRecoveryReason::kPendingCanonicalConflict,
                    emit_late, fatal));
                return;
            }
            if (inserted == InsertResult::kCollision) {
                FinishDiscovery(*state, now, emit_tick, fatal);
                ProcessActive(*state, decoded, now, emit_tick, emit_gap,
                              emit_fault, emit_late, fatal);
                return;
            }
            if (state->first_seen_ns == 0U) {
                state->first_seen_ns = now;
                if (initial_hold_ns_ != 0U) {
                    ScheduleDeadline(now, initial_hold_ns_);
                }
            }
            if (initial_hold_ns_ == 0U ||
                DeadlineReached(
                    now, state->first_seen_ns, initial_hold_ns_)) {
                FinishDiscovery(*state, now, emit_tick, fatal);
            }
            return;
        }
        ProcessActive(*state, decoded, now, emit_tick, emit_gap,
                      emit_fault, emit_late, fatal);
    }

    template <typename EmitTick, typename EmitGap, typename Fatal>
    void Poll(std::uint64_t now,
              EmitTick&& emit_tick,
              EmitGap&& emit_gap,
              Fatal&& fatal) noexcept {
        if (next_deadline_ns_ == 0U || now < next_deadline_ns_) {
            return;
        }
        next_deadline_ns_ = 0U;
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelState& state = channels_[index];
            if (state.frozen || state.pending_count == 0U) {
                continue;
            }
            if (state.discovering) {
                if (DeadlineReached(
                        now, state.first_seen_ns, initial_hold_ns_)) {
                    FinishDiscovery(state, now, emit_tick, fatal);
                } else {
                    ScheduleDeadline(state.first_seen_ns,
                                     initial_hold_ns_);
                }
                continue;
            }
            if (state.gap_open_ns != 0U &&
                DeadlineReached(now, state.gap_open_ns, gap_wait_ns_)) {
                ForceGapToMinimum(
                    state, now, emit_tick, emit_gap, fatal);
            } else if (state.gap_open_ns != 0U) {
                ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
            }
        }
    }

    template <typename EmitTick, typename EmitGap, typename Fatal>
    void Flush(std::uint64_t now,
               EmitTick&& emit_tick,
               EmitGap&& emit_gap,
               Fatal&& fatal) noexcept {
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelState& state = channels_[index];
            if (state.frozen) {
                continue;
            }
            if (state.discovering) {
                FinishDiscovery(state, now, emit_tick, fatal);
            }
            while (state.pending_count > 0U) {
                ForceGapToMinimum(
                    state, now, emit_tick, emit_gap, fatal);
            }
        }
    }

    template <typename EmitFault, typename Fatal>
    void FreezeDecodeFailure(Market market,
                             std::uint32_t channel,
                             std::uint64_t sequence,
                             std::uint64_t now,
                             EmitFault&& emit_fault,
                             Fatal&& fatal) noexcept {
        if (mode_ != StartMode::kFromOpen) {
            return;
        }
        ChannelState* state = FindOrCreate(market, channel);
        if (state == nullptr) {
            fatal("native-sequence channel capacity or allocation exhausted "
                  "while recording a decode failure");
            return;
        }
        FreezeFromOpen(*state, ChannelFaultReason::kDecodeFailure,
                       sequence, now, emit_fault, fatal);
    }

private:
    struct PendingSlot final {
        CanonicalTick tick{};
        bool occupied = false;
        bool emit = false;
    };

    struct ChannelState final {
        Market market = Market::kUnknown;
        std::uint32_t channel = 0U;
        bool used = false;
        bool frozen = false;
        bool discovering = false;
        bool history_complete = true;
        bool gap_metadata_pending = false;
        std::uint64_t expected = 1U;
        std::uint64_t first_seen_ns = 0U;
        std::uint64_t gap_open_ns = 0U;
        std::uint64_t gap_epoch = 0U;
        std::uint64_t cumulative_missing_sequences = 0U;
        std::uint64_t pending_gap_first = 0U;
        std::uint64_t pending_gap_last = 0U;
        std::size_t mailbox_slot = 0U;
        std::size_t pending_count = 0U;
        std::unique_ptr<PendingSlot[]> pending;
    };

    enum class InsertResult : std::uint8_t {
        kInserted,
        kDuplicate,
        kConflict,
        kCollision,
    };

    [[nodiscard]] static bool DeadlineReached(
        std::uint64_t now,
        std::uint64_t start,
        std::uint64_t delay) noexcept {
        return now >= start && now - start >= delay;
    }

    void ScheduleDeadline(std::uint64_t start,
                          std::uint64_t delay) noexcept {
        const std::uint64_t maximum =
            std::numeric_limits<std::uint64_t>::max();
        const std::uint64_t deadline =
            maximum - start < delay ? maximum : start + delay;
        if (next_deadline_ns_ == 0U || deadline < next_deadline_ns_) {
            next_deadline_ns_ = deadline;
        }
    }

    [[nodiscard]] static std::uint64_t HashChannel(
        Market market,
        std::uint32_t channel) noexcept {
        std::uint64_t value =
            (static_cast<std::uint64_t>(market) << 32U) | channel;
        value ^= value >> 30U;
        value *= UINT64_C(0xbf58476d1ce4e5b9);
        value ^= value >> 27U;
        value *= UINT64_C(0x94d049bb133111eb);
        return value ^ (value >> 31U);
    }

    [[nodiscard]] ChannelState* FindOrCreate(Market market,
                                              std::uint32_t channel) noexcept {
        std::size_t slot =
            static_cast<std::size_t>(HashChannel(market, channel)) &
            channel_table_mask_;
        for (std::size_t probe = 0U; probe < channel_table_.size(); ++probe) {
            const std::uint32_t index_plus_one = channel_table_[slot];
            if (index_plus_one == 0U) {
                if (channel_count_ >= channels_.size()) {
                    return nullptr;
                }
                ChannelState& state = channels_[channel_count_];
                state.pending.reset(
                    new (std::nothrow) PendingSlot[entries_per_channel_]);
                if (state.pending == nullptr) {
                    return nullptr;
                }
                state.market = market;
                state.channel = channel;
                state.mailbox_slot = channel_count_;
                state.used = true;
                state.discovering = mode_ == StartMode::kPartial;
                state.history_complete = mode_ == StartMode::kFromOpen;
                state.expected = mode_ == StartMode::kFromOpen ? 1U : 0U;
                channel_table_[slot] =
                    static_cast<std::uint32_t>(channel_count_ + 1U);
                ++channel_count_;
                return &state;
            }
            ChannelState& state = channels_[index_plus_one - 1U];
            if (state.market == market && state.channel == channel) {
                return &state;
            }
            slot = (slot + 1U) & channel_table_mask_;
        }
        return nullptr;
    }

    [[nodiscard]] InsertResult Insert(
        ChannelState& state,
        const internal::TickDecodeResult& decoded) noexcept {
        const std::uint64_t sequence = decoded.tick.common.native_sequence;
        PendingSlot& slot = state.pending[
            static_cast<std::size_t>(sequence) & slot_mask_];
        if (slot.occupied) {
            if (slot.tick.common.native_sequence != sequence) {
                return InsertResult::kCollision;
            }
            return CanonicalPayloadEqual(slot.tick, decoded.tick) &&
                           slot.emit == decoded.catalog_match
                       ? InsertResult::kDuplicate
                       : InsertResult::kConflict;
        }
        slot.tick = decoded.tick;
        slot.emit = decoded.catalog_match;
        slot.occupied = true;
        ++state.pending_count;
        return InsertResult::kInserted;
    }

    [[nodiscard]] PendingSlot* Find(ChannelState& state,
                                    std::uint64_t sequence) noexcept {
        PendingSlot& slot = state.pending[
            static_cast<std::size_t>(sequence) & slot_mask_];
        return slot.occupied && slot.tick.common.native_sequence == sequence
                   ? &slot
                   : nullptr;
    }

    [[nodiscard]] std::uint64_t MinimumPending(
        const ChannelState& state) const noexcept {
        std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
        for (std::size_t index = 0U; index < entries_per_channel_; ++index) {
            const PendingSlot& slot = state.pending[index];
            if (slot.occupied) {
                minimum =
                    std::min(minimum, slot.tick.common.native_sequence);
            }
        }
        return minimum;
    }

    template <typename EmitLate, typename Fatal>
    [[nodiscard]] bool DivertLateRecovery(
        const ChannelState& state,
        const internal::TickDecodeResult& decoded,
        LateRecoveryReason reason,
        EmitLate&& emit_late,
        Fatal&& fatal) noexcept {
        LateRecoveryTick record{};
        record.tick = decoded.tick;
        record.tick.validity &= ~kTickChannelHistoryValid;
        record.tick.common.quality_flags |=
            kQualityChannelHistoryIncomplete | kQualityLateRecovery;
        record.tick.common.gap_epoch = state.gap_epoch;
        record.committed_next_sequence = state.expected;
        record.observed_gap_epoch = state.gap_epoch;
        record.reason = reason;
        record.catalog_match = decoded.catalog_match;
        if (!emit_late(record)) {
            fatal("LateRecovery canonical record queue overflow");
            return false;
        }
        return true;
    }

    template <typename EmitTick, typename Fatal>
    [[nodiscard]] bool EmitOne(ChannelState& state,
                               PendingSlot& slot,
                               EmitTick&& emit_tick,
                               Fatal&& fatal) noexcept {
        CanonicalTick tick = slot.tick;
        const bool should_emit = slot.emit;
        slot.occupied = false;
        --state.pending_count;
        if (!state.history_complete) {
            tick.validity &= ~kTickChannelHistoryValid;
            tick.common.quality_flags |= kQualityChannelHistoryIncomplete;
        }
        if (should_emit && state.gap_metadata_pending) {
            tick.common.quality_flags |= kQualitySequenceGapBefore;
            tick.common.gap_epoch = state.gap_epoch;
            tick.common.gap_before_first = state.pending_gap_first;
            tick.common.gap_before_last = state.pending_gap_last;
            state.gap_metadata_pending = false;
        }
        if (should_emit && !emit_tick(tick)) {
            fatal("instrument tick dispatch queue overflow");
            return false;
        }
        return true;
    }

    template <typename EmitTick, typename Fatal>
    void DrainContiguous(ChannelState& state,
                         std::uint64_t now,
                         EmitTick&& emit_tick,
                         Fatal&& fatal) noexcept {
        for (;;) {
            PendingSlot* slot = Find(state, state.expected);
            if (slot == nullptr) {
                break;
            }
            if (!EmitOne(state, *slot, emit_tick, fatal)) {
                return;
            }
            ++state.expected;
        }
        state.gap_open_ns = state.pending_count == 0U ? 0U : now;
        if (state.gap_open_ns != 0U) {
            ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
        }
    }

    template <typename EmitGap, typename Fatal>
    [[nodiscard]] bool RecordGap(ChannelState& state,
                                 std::uint64_t first,
                                 std::uint64_t last,
                                 std::uint64_t next,
                                 std::uint64_t now,
                                 EmitGap&& emit_gap,
                                 Fatal&& fatal) noexcept {
        if (last < first) {
            return true;
        }
        state.history_complete = false;
        if (state.gap_epoch !=
            std::numeric_limits<std::uint64_t>::max()) {
            ++state.gap_epoch;
        }
        const std::uint64_t missing_count = next - first;
        if (std::numeric_limits<std::uint64_t>::max() -
                state.cumulative_missing_sequences <
            missing_count) {
            state.cumulative_missing_sequences =
                std::numeric_limits<std::uint64_t>::max();
        } else {
            state.cumulative_missing_sequences += missing_count;
        }
        state.pending_gap_first = first;
        state.pending_gap_last = last;
        state.gap_metadata_pending = true;
        ChannelGap gap{};
        gap.market = state.market;
        gap.channel = state.channel;
        gap.first_missing = first;
        gap.last_missing = last;
        gap.first_present_after_gap = next;
        gap.detected_monotonic_ns = now;
        gap.gap_epoch = state.gap_epoch;
        gap.cumulative_missing_sequences =
            state.cumulative_missing_sequences;
        if (!emit_gap(state.mailbox_slot, gap)) {
            fatal("channel gap mailbox invariant failed");
            return false;
        }
        stats_->gaps_skipped.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    template <typename EmitFault, typename Fatal>
    void FreezeFromOpen(ChannelState& state,
                        ChannelFaultReason reason,
                        std::uint64_t observed_sequence,
                        std::uint64_t now,
                        EmitFault&& emit_fault,
                        Fatal&& fatal) noexcept {
        if (!state.frozen) {
            state.frozen = true;
            stats_->from_open_channels_frozen.fetch_add(
                1U, std::memory_order_relaxed);
            ChannelFault fault{};
            fault.market = state.market;
            fault.reason = reason;
            fault.channel = state.channel;
            fault.expected_sequence = state.expected;
            fault.observed_sequence = observed_sequence;
            fault.detected_monotonic_ns = now;
            if (!emit_fault(fault)) {
                fatal("channel fault dispatch queue overflow");
            }
        }
    }

    template <typename EmitTick, typename Fatal>
    void FinishDiscovery(ChannelState& state,
                         std::uint64_t now,
                         EmitTick&& emit_tick,
                         Fatal&& fatal) noexcept {
        if (!state.discovering || state.pending_count == 0U) {
            state.discovering = false;
            return;
        }
        state.discovering = false;
        state.expected = MinimumPending(state);
        DrainContiguous(state, now, emit_tick, fatal);
    }

    template <typename EmitTick, typename EmitGap, typename Fatal>
    void ForceGapToMinimum(ChannelState& state,
                           std::uint64_t now,
                           EmitTick&& emit_tick,
                           EmitGap&& emit_gap,
                           Fatal&& fatal) noexcept {
        if (state.pending_count == 0U) {
            state.gap_open_ns = 0U;
            return;
        }
        const std::uint64_t minimum = MinimumPending(state);
        if (minimum > state.expected &&
            !RecordGap(state, state.expected, minimum - 1U, minimum, now,
                       emit_gap, fatal)) {
            return;
        }
        state.expected = minimum;
        DrainContiguous(state, now, emit_tick, fatal);
    }

    template <typename EmitTick, typename EmitGap, typename EmitFault,
              typename EmitLate, typename Fatal>
    void ProcessActive(ChannelState& state,
                       const internal::TickDecodeResult& decoded,
                       std::uint64_t now,
                       EmitTick&& emit_tick,
                       EmitGap&& emit_gap,
                       EmitFault&& emit_fault,
                       EmitLate&& emit_late,
                       Fatal&& fatal) noexcept {
        const std::uint64_t sequence = decoded.tick.common.native_sequence;
        for (;;) {
            if (sequence < state.expected) {
                stats_->duplicates_or_late.fetch_add(
                    1U, std::memory_order_relaxed);
                static_cast<void>(DivertLateRecovery(
                    state, decoded,
                    LateRecoveryReason::kBehindCommittedFrontier,
                    emit_late, fatal));
                return;
            }
            if (sequence == state.expected) {
                PendingSlot temporary{};
                temporary.tick = decoded.tick;
                temporary.emit = decoded.catalog_match;
                temporary.occupied = true;
                ++state.pending_count;
                if (!EmitOne(state, temporary, emit_tick, fatal)) {
                    return;
                }
                ++state.expected;
                DrainContiguous(state, now, emit_tick, fatal);
                return;
            }

            const std::uint64_t span = sequence - state.expected;
            const InsertResult inserted = Insert(state, decoded);
            if (inserted == InsertResult::kDuplicate) {
                stats_->duplicates_or_late.fetch_add(
                    1U, std::memory_order_relaxed);
                return;
            }
            if (inserted == InsertResult::kConflict) {
                stats_->duplicates_or_late.fetch_add(
                    1U, std::memory_order_relaxed);
                if (mode_ == StartMode::kFromOpen) {
                    FreezeFromOpen(
                        state, ChannelFaultReason::kCanonicalConflict,
                        sequence, now, emit_fault, fatal);
                } else {
                    static_cast<void>(DivertLateRecovery(
                        state, decoded,
                        LateRecoveryReason::kPendingCanonicalConflict,
                        emit_late, fatal));
                }
                return;
            }
            if (inserted == InsertResult::kCollision ||
                span > maximum_reorder_span_) {
                if (inserted == InsertResult::kInserted) {
                    // The far record is already retained; advancing to the
                    // smallest retained native position preserves order.
                    ForceGapToMinimum(
                        state, now, emit_tick, emit_gap, fatal);
                    return;
                }
                const std::uint64_t minimum = MinimumPending(state);
                if (state.pending_count == 0U || sequence < minimum) {
                    if (!RecordGap(state, state.expected, sequence - 1U,
                                   sequence, now, emit_gap, fatal)) {
                        return;
                    }
                    state.expected = sequence;
                } else {
                    ForceGapToMinimum(
                        state, now, emit_tick, emit_gap, fatal);
                }
                // Retry iteratively after freeing at least one colliding
                // position; adversarial sequence patterns cannot grow the
                // worker stack.
                continue;
            }
            if (state.gap_open_ns == 0U) {
                state.gap_open_ns = now;
                ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
            }
            if (gap_wait_ns_ == 0U) {
                ForceGapToMinimum(state, now, emit_tick, emit_gap, fatal);
            }
            return;
        }
    }

    StartMode mode_ = StartMode::kFromOpen;
    std::size_t entries_per_channel_ = 0U;
    std::size_t slot_mask_ = 0U;
    std::uint64_t maximum_reorder_span_ = 0U;
    std::uint64_t initial_hold_ns_ = 0U;
    std::uint64_t gap_wait_ns_ = 0U;
    std::vector<ChannelState> channels_;
    std::vector<std::uint32_t> channel_table_;
    std::size_t channel_table_mask_ = 0U;
    std::size_t channel_count_ = 0U;
    std::uint64_t next_deadline_ns_ = 0U;
    TickLaneStats* stats_ = nullptr;
};

[[nodiscard]] bool ValidateConfig(const EngineConfig& config,
                                  const InstrumentCatalog& catalog,
                                  std::string* error) {
    const auto fail = [error](const char* message) {
        if (error != nullptr) {
            *error = message;
        }
        return false;
    };
    if (!IsTradeDateValid(config.trade_date)) {
        return fail("trade_date is not a supported valid calendar date");
    }
    if (catalog.size() == 0U) {
        return fail("instrument catalog is empty");
    }
    if (config.enabled_streams == 0U ||
        (config.enabled_streams & ~kSupportedStreamMask) != 0U) {
        return fail("enabled_streams contains no stream or an unknown bit");
    }
    if (config.tick_decoder_lanes == 0U ||
        config.snapshot_decoder_lanes == 0U ||
        config.instrument_workers == 0U) {
        return fail("lane and instrument worker counts must be positive");
    }
    if (config.tick_slots_per_lane < 2U ||
        config.snapshot_slots_per_lane < 2U ||
        config.maximum_tick_body_bytes < 70U ||
        config.maximum_snapshot_body_bytes < 248U ||
        config.dispatch_queue_capacity < 2U ||
        config.diagnostic_queue_capacity < 2U ||
        config.late_recovery_queue_capacity < 2U) {
        return fail("slot, body, or dispatch capacity is too small");
    }
    if (config.maximum_channels_per_tick_lane == 0U) {
        return fail("maximum_channels_per_tick_lane must be positive");
    }
    if (config.tick_slots_per_lane >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.snapshot_slots_per_lane >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.tick_slots_per_lane >
            std::numeric_limits<std::size_t>::max() /
                config.maximum_tick_body_bytes ||
        config.snapshot_slots_per_lane >
            std::numeric_limits<std::size_t>::max() /
                config.maximum_snapshot_body_bytes ||
        config.tick_decoder_lanes >
            std::numeric_limits<std::size_t>::max() /
                config.instrument_workers ||
        config.snapshot_decoder_lanes >
            std::numeric_limits<std::size_t>::max() /
                config.instrument_workers ||
        config.tick_decoder_lanes >
            std::numeric_limits<std::size_t>::max() /
                config.maximum_channels_per_tick_lane) {
        return fail("configured bounded storage size overflows size_t");
    }
    if (config.maximum_channels_per_tick_lane >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max() - 1U) ||
        config.maximum_channels_per_tick_lane >
            std::numeric_limits<std::size_t>::max() / 2U ||
        !IsPowerOfTwo(config.reorder_entries_per_channel) ||
        config.reorder_entries_per_channel < 2U ||
        config.maximum_reorder_span >
            config.reorder_entries_per_channel) {
        return fail("reorder entries must be a power of two and be at least "
                    "the maximum reorder span");
    }
    if (config.maximum_text_bytes == 0U ||
        config.maximum_text_bytes > kMaximumIdentityBytes ||
        config.maximum_depth_items == 0U ||
        config.maximum_queue_items == 0U ||
        config.recovery_timer_scan_ns == 0U) {
        return fail("decoder or recovery timer limits are invalid");
    }
    if (config.first_decoder_cpu < -1) {
        return fail("first_decoder_cpu must be -1 or nonnegative");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace

class IngestEngine::Impl final {
public:
    Impl(EngineConfig config, InstrumentCatalog catalog)
        : config_(config),
          catalog_(std::move(catalog)),
          tick_stats_(std::make_unique<TickLaneStats[]>(
              config.tick_decoder_lanes)),
          snapshot_stats_(std::make_unique<SnapshotLaneStats[]>(
              config.snapshot_decoder_lanes)),
          instrument_owner_mask_(
              IsPowerOfTwo(config.instrument_workers)
                  ? config.instrument_workers - 1U
                  : 0U),
          dispatcher_(config.tick_decoder_lanes,
                      config.snapshot_decoder_lanes,
                      config.instrument_workers,
                      config.dispatch_queue_capacity,
                      config.diagnostic_queue_capacity,
                      config.late_recovery_queue_capacity,
                      config.maximum_channels_per_tick_lane) {
        tick_lanes_.reserve(config_.tick_decoder_lanes);
        tick_recovery_.reserve(config_.tick_decoder_lanes);
        for (std::size_t index = 0U; index < config_.tick_decoder_lanes;
             ++index) {
            tick_lanes_.push_back(std::make_unique<LaneStorage>(
                config_.tick_slots_per_lane,
                config_.maximum_tick_body_bytes));
            tick_recovery_.push_back(std::make_unique<SequenceRecovery>(
                config_.start_mode,
                config_.maximum_channels_per_tick_lane,
                config_.reorder_entries_per_channel,
                config_.maximum_reorder_span,
                config_.partial_initial_hold_ns,
                config_.start_mode == StartMode::kFromOpen
                    ? config_.from_open_gap_wait_ns
                    : config_.partial_gap_wait_ns,
                &tick_stats_[index]));
        }
        snapshot_lanes_.reserve(config_.snapshot_decoder_lanes);
        for (std::size_t index = 0U;
             index < config_.snapshot_decoder_lanes; ++index) {
            snapshot_lanes_.push_back(std::make_unique<LaneStorage>(
                config_.snapshot_slots_per_lane,
                config_.maximum_snapshot_body_bytes));
        }
    }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "engine can be started exactly once";
            }
            return false;
        }
        accepting_.store(false, std::memory_order_release);
        running_.store(true, std::memory_order_release);
        try {
            threads_.reserve(config_.tick_decoder_lanes +
                             config_.snapshot_decoder_lanes);
            for (std::size_t index = 0U;
                 index < config_.tick_decoder_lanes; ++index) {
                threads_.emplace_back([this, index] { TickWorker(index); });
            }
            for (std::size_t index = 0U;
                 index < config_.snapshot_decoder_lanes; ++index) {
                threads_.emplace_back(
                    [this, index] { SnapshotWorker(index); });
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("decoder thread creation failed: ") +
                     exception.what());
            Stop();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }

#if defined(__linux__)
        if (config_.first_decoder_cpu >= 0) {
            for (std::size_t index = 0U; index < threads_.size(); ++index) {
                const std::size_t cpu =
                    static_cast<std::size_t>(config_.first_decoder_cpu) +
                    index;
                if (cpu >= static_cast<std::size_t>(CPU_SETSIZE)) {
                    SetFatal("requested decoder CPU exceeds CPU_SETSIZE");
                    break;
                }
                cpu_set_t set;
                CPU_ZERO(&set);
                CPU_SET(static_cast<int>(cpu), &set);
                if (::pthread_setaffinity_np(
                        threads_[index].native_handle(), sizeof(set),
                        &set) != 0) {
                    SetFatal("failed to set decoder thread affinity");
                    break;
                }
            }
            if (!healthy()) {
                Stop();
                if (error != nullptr) {
                    *error = fatal_error();
                }
                return false;
            }
        }
#else
        if (config_.first_decoder_cpu >= 0) {
            SetFatal("decoder CPU affinity is supported only on Linux");
            Stop();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
#endif
        accepting_.store(true, std::memory_order_release);
        if (!healthy()) {
            accepting_.store(false, std::memory_order_release);
            Stop();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    void Stop() noexcept {
        accepting_.store(false, std::memory_order_release);
        // A producer that entered before the gate closed may still own a
        // lane slot. Let that bounded admission finish before consumers are
        // told that no more records can arrive.
        while (callback_active_.test(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        running_.store(false, std::memory_order_release);
        for (std::thread& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        threads_.clear();
    }

    [[nodiscard]] AdmissionResult Admit(
        std::span<const std::byte> header_bytes,
        std::span<const std::byte> body,
        std::uint64_t receive_monotonic_ns) noexcept {
        admission_stats_.callbacks.fetch_add(
            1U, std::memory_order_relaxed);
        if (callback_active_.test_and_set(std::memory_order_acquire)) {
            admission_stats_.rejected.fetch_add(
                1U, std::memory_order_relaxed);
            SetFatal("concurrent SDK callback violated the single-producer "
                     "admission contract");
            return AdmissionResult::kConcurrentCallback;
        }
        struct CallbackGuard final {
            std::atomic_flag* flag;
            ~CallbackGuard() { flag->clear(std::memory_order_release); }
        } guard{&callback_active_};

        const auto reject = [this](AdmissionResult result) {
            admission_stats_.rejected.fetch_add(
                1U, std::memory_order_relaxed);
            if (result == AdmissionResult::kLaneFull) {
                admission_stats_.lane_full.fetch_add(
                    1U, std::memory_order_relaxed);
            }
            return result;
        };
        if (!accepting_.load(std::memory_order_acquire) ||
            !running_.load(std::memory_order_acquire)) {
            return reject(AdmissionResult::kNotRunning);
        }
        if (!healthy()) {
            return reject(AdmissionResult::kInternalFailure);
        }
        ParsedHeader header{};
        if (!ParseMdlHeader(header_bytes, &header)) {
            return reject(header_bytes.size() < kMdlHeaderBytes
                              ? AdmissionResult::kHeaderTruncated
                              : AdmissionResult::kHeaderInvalid);
        }
        if (header.head_size != kMdlHeaderBytes ||
            header.message_size < kMdlHeaderBytes) {
            return reject(AdmissionResult::kHeaderInvalid);
        }
        if (body.size() !=
            static_cast<std::size_t>(header.message_size -
                                     kMdlHeaderBytes)) {
            return reject(AdmissionResult::kBodySizeMismatch);
        }
        if (header.encoding != kBinaryEncoding) {
            return reject(AdmissionResult::kNonBinaryEncoding);
        }
        const MessageClass message_class = ClassifyMessage(header.key);
        if (message_class == MessageClass::kUnsupported) {
            return reject(AdmissionResult::kUnsupportedMessage);
        }
        if (!StreamEnabled(config_.enabled_streams, header.key)) {
            return reject(AdmissionResult::kDisabledMessage);
        }
        const bool is_tick =
            message_class == MessageClass::kShanghaiTick ||
            message_class == MessageClass::kShenzhenOrder ||
            message_class == MessageClass::kShenzhenTransaction;
        const std::size_t maximum_body =
            is_tick ? config_.maximum_tick_body_bytes
                    : config_.maximum_snapshot_body_bytes;
        if (body.size() > maximum_body) {
            SetFatal("required MDL message exceeded its configured body "
                     "capacity");
            return reject(AdmissionResult::kBodyTooLarge);
        }
        std::uint64_t route_key = 0U;
        if (!internal::ExtractAdmissionRoute(
                message_class, body, &route_key)) {
            std::size_t fixed_bytes = 0U;
            switch (message_class) {
                case MessageClass::kShanghaiSnapshot:
                    fixed_bytes = 248U;
                    break;
                case MessageClass::kShanghaiTick:
                    fixed_bytes = 70U;
                    break;
                case MessageClass::kShenzhenSnapshot:
                    fixed_bytes = 224U;
                    break;
                case MessageClass::kShenzhenOrder:
                    fixed_bytes = 58U;
                    break;
                case MessageClass::kShenzhenTransaction:
                    fixed_bytes = 70U;
                    break;
                case MessageClass::kUnsupported:
                    break;
            }
            return reject(body.size() < fixed_bytes
                              ? AdmissionResult::kBodyTruncated
                              : AdmissionResult::kRouteInvalid);
        }
        const std::size_t lane_count =
            is_tick ? tick_lanes_.size() : snapshot_lanes_.size();
        const std::size_t lane_index =
            static_cast<std::size_t>(route_key % lane_count);
        LaneStorage& lane = is_tick ? *tick_lanes_[lane_index]
                                    : *snapshot_lanes_[lane_index];
        const std::uint64_t ingress_sequence = next_ingress_sequence_ + 1U;
        if (!lane.TryPublish(header, body, ingress_sequence,
                             receive_monotonic_ns)) {
            SetFatal("decoder admission lane exhausted; no message was "
                     "silently overwritten");
            return reject(AdmissionResult::kLaneFull);
        }
        next_ingress_sequence_ = ingress_sequence;
        admission_stats_.admitted.fetch_add(
            1U, std::memory_order_relaxed);
        return AdmissionResult::kAccepted;
    }

    [[nodiscard]] bool TryPollTick(std::size_t owner,
                                   CanonicalTick* output) noexcept {
        return dispatcher_.TryPollTick(owner, output);
    }

    [[nodiscard]] bool TryPollSnapshot(
        std::size_t owner,
        CanonicalSnapshot* output) noexcept {
        return dispatcher_.TryPollSnapshot(owner, output);
    }

    [[nodiscard]] bool TryPollGap(ChannelGap* output) noexcept {
        return dispatcher_.TryPollGap(output);
    }

    [[nodiscard]] bool TryPollLateRecovery(
        LateRecoveryTick* output) noexcept {
        return dispatcher_.TryPollLateRecovery(output);
    }

    [[nodiscard]] bool TryPollChannelFault(
        ChannelFault* output) noexcept {
        return dispatcher_.TryPollFault(output);
    }

    [[nodiscard]] EngineStats Stats() const noexcept {
        EngineStats result{};
        result.callbacks = admission_stats_.callbacks.load(
            std::memory_order_relaxed);
        result.admitted = admission_stats_.admitted.load(
            std::memory_order_relaxed);
        result.rejected = admission_stats_.rejected.load(
            std::memory_order_relaxed);
        result.lane_full = admission_stats_.lane_full.load(
            std::memory_order_relaxed);
        for (std::size_t index = 0U;
             index < config_.tick_decoder_lanes; ++index) {
            const TickLaneStats& lane = tick_stats_[index];
            result.decoded_ticks += lane.decoded_ticks.load(
                std::memory_order_relaxed);
            result.decode_errors += lane.decode_errors.load(
                std::memory_order_relaxed);
            result.catalog_misses += lane.catalog_misses.load(
                std::memory_order_relaxed);
            result.dispatched_ticks += lane.dispatched_ticks.load(
                std::memory_order_relaxed);
            result.duplicates_or_late += lane.duplicates_or_late.load(
                std::memory_order_relaxed);
            result.late_recovery_dispatched +=
                lane.late_recovery_dispatched.load(
                    std::memory_order_relaxed);
            result.gaps_skipped += lane.gaps_skipped.load(
                std::memory_order_relaxed);
            result.from_open_channels_frozen +=
                lane.from_open_channels_frozen.load(
                    std::memory_order_relaxed);
            result.channel_faults_dispatched +=
                lane.channel_faults_dispatched.load(
                    std::memory_order_relaxed);
            result.dispatch_overflows += lane.dispatch_overflows.load(
                std::memory_order_relaxed);
        }
        for (std::size_t index = 0U;
             index < config_.snapshot_decoder_lanes; ++index) {
            const SnapshotLaneStats& lane = snapshot_stats_[index];
            result.decoded_snapshots += lane.decoded_snapshots.load(
                std::memory_order_relaxed);
            result.decode_errors += lane.decode_errors.load(
                std::memory_order_relaxed);
            result.catalog_misses += lane.catalog_misses.load(
                std::memory_order_relaxed);
            result.dispatched_snapshots +=
                lane.dispatched_snapshots.load(
                    std::memory_order_relaxed);
            result.dispatch_overflows += lane.dispatch_overflows.load(
                std::memory_order_relaxed);
        }
        return result;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] const EngineConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] std::size_t InstrumentOwner(
        std::uint32_t ordinal) const noexcept {
        const std::size_t value = static_cast<std::size_t>(ordinal);
        return instrument_owner_mask_ != 0U
                   ? value & instrument_owner_mask_
                   : value % config_.instrument_workers;
    }

    void SetFatal(std::string message) noexcept {
        if (fatal_claimed_.test_and_set(std::memory_order_acq_rel)) {
            return;
        }
        accepting_.store(false, std::memory_order_release);
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            fatal_error_ = std::move(message);
        } catch (...) {
        }
        // Publish the health transition only after the diagnostic is ready.
        healthy_.store(false, std::memory_order_release);
    }

    template <typename Record, typename Publish>
    [[nodiscard]] bool PublishBounded(const Record& record,
                                      Publish&& publish,
                                      std::atomic<std::uint64_t>*
                                          overflow_counter) noexcept {
        constexpr std::size_t kMaximumSpins = 2'048U;
        for (std::size_t spin = 0U; spin < kMaximumSpins; ++spin) {
            if (publish(record)) {
                return true;
            }
            CpuRelax();
        }
        overflow_counter->fetch_add(1U, std::memory_order_relaxed);
        return false;
    }

    void TickWorker(std::size_t lane_index) noexcept {
        try {
            LaneStorage& lane = *tick_lanes_[lane_index];
            SequenceRecovery& recovery = *tick_recovery_[lane_index];
            TickLaneStats& lane_stats = tick_stats_[lane_index];
            const internal::DecoderLimits limits{
                config_.maximum_text_bytes,
                config_.maximum_depth_items,
                config_.maximum_queue_items};
            const auto emit_tick = [this, lane_index, &lane_stats](
                                       const CanonicalTick& tick) {
                const std::size_t owner = InstrumentOwner(
                    tick.common.instrument_ordinal);
                const bool published = PublishBounded(
                    tick, [this, lane_index, owner](
                              const CanonicalTick& value) {
                        return dispatcher_.PublishTick(
                            lane_index, owner, value);
                    },
                    &lane_stats.dispatch_overflows);
                if (published) {
                    lane_stats.dispatched_ticks.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                return published;
            };
            const auto emit_gap = [this, lane_index](
                                      std::size_t channel_slot,
                                      const ChannelGap& gap) {
                return dispatcher_.PublishGap(
                    lane_index, channel_slot, gap);
            };
            const auto emit_fault = [this, lane_index, &lane_stats](
                                        const ChannelFault& fault) {
                const bool published = PublishBounded(
                    fault,
                    [this, lane_index](const ChannelFault& value) {
                        return dispatcher_.PublishFault(lane_index, value);
                    },
                    &lane_stats.dispatch_overflows);
                if (published) {
                    lane_stats.channel_faults_dispatched.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                return published;
            };
            const auto emit_late = [this, lane_index, &lane_stats](
                                       const LateRecoveryTick& record) {
                const bool published = PublishBounded(
                    record,
                    [this, lane_index](const LateRecoveryTick& value) {
                        return dispatcher_.PublishLateRecovery(
                            lane_index, value);
                    },
                    &lane_stats.dispatch_overflows);
                if (published) {
                    lane_stats.late_recovery_dispatched.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                return published;
            };
            const auto fatal = [this](const char* message) {
                SetFatal(message);
            };

            std::uint64_t next_timer = MonotonicNowNs();
            std::size_t idle_spins = 0U;
            for (;;) {
                std::uint32_t slot = 0U;
                internal::OwnedMessageView message;
                if (lane.TryConsume(&slot, &message)) {
                    idle_spins = 0U;
                    internal::TickDecodeResult decoded;
                    const internal::DecodeError error = internal::DecodeTick(
                        message, config_.trade_date, limits, catalog_,
                        &decoded);
                    if (error == internal::DecodeError::kNone) {
                        lane_stats.decoded_ticks.fetch_add(
                            1U, std::memory_order_relaxed);
                        if (!decoded.catalog_match) {
                            lane_stats.catalog_misses.fetch_add(
                                1U, std::memory_order_relaxed);
                        }
                        const bool raw_accepted =
                            config_.raw_record_tap == nullptr ||
                            config_.raw_record_tap->AppendTick(
                                lane_index, decoded.tick);
                        if (!raw_accepted) {
                            SetFatal(
                                "raw Tick batch queue exhausted or failed; "
                                "admission stopped at a continuity boundary");
                        }
                        if (raw_accepted) {
                            recovery.Process(decoded,
                                             message.receive_monotonic_ns,
                                             emit_tick, emit_gap, emit_fault,
                                             emit_late, fatal);
                        }
                    } else {
                        lane_stats.decode_errors.fetch_add(
                            1U, std::memory_order_relaxed);
                        if (config_.start_mode == StartMode::kFromOpen) {
                            Market market = Market::kUnknown;
                            std::uint32_t channel = 0U;
                            std::uint64_t sequence = 0U;
                            if (internal::ExtractTickNativeDescriptor(
                                    ClassifyMessage(message.header.key),
                                    message.body, &market, &channel,
                                    &sequence)) {
                                recovery.FreezeDecodeFailure(
                                    market, channel, sequence,
                                    message.receive_monotonic_ns,
                                    emit_fault, fatal);
                            } else {
                                SetFatal(
                                    "from-open tick decode failed and its "
                                    "native channel position was invalid");
                            }
                        }
                    }
                    if (!lane.Release(slot)) {
                        SetFatal("tick lane recycle queue invariant failed");
                    }
                    if (message.receive_monotonic_ns >= next_timer) {
                        recovery.Poll(message.receive_monotonic_ns,
                                      emit_tick, emit_gap, fatal);
                        if (config_.raw_record_tap != nullptr &&
                            !config_.raw_record_tap->PollTick(
                                lane_index,
                                message.receive_monotonic_ns)) {
                            SetFatal("raw Tick batch flush failed");
                        }
                        next_timer = message.receive_monotonic_ns +
                                     config_.recovery_timer_scan_ns;
                    }
                } else {
                    const std::uint64_t now = MonotonicNowNs();
                    if (now >= next_timer) {
                        recovery.Poll(now, emit_tick, emit_gap, fatal);
                        if (config_.raw_record_tap != nullptr &&
                            !config_.raw_record_tap->PollTick(
                                lane_index, now)) {
                            SetFatal("raw Tick batch flush failed");
                        }
                        next_timer = now + config_.recovery_timer_scan_ns;
                    }
                    if (!running_.load(std::memory_order_acquire) &&
                        lane.empty()) {
                        recovery.Flush(
                            now, emit_tick, emit_gap, fatal);
                        if (config_.raw_record_tap != nullptr &&
                            !config_.raw_record_tap->FlushTick(lane_index)) {
                            SetFatal("final raw Tick batch flush failed");
                        }
                        break;
                    }
                    if (config_.first_decoder_cpu >= 0) {
                        CpuRelax();
                    } else if (idle_spins < 4'096U) {
                        ++idle_spins;
                        CpuRelax();
                    } else {
                        std::this_thread::yield();
                    }
                }
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("tick decoder lane failed: ") +
                     exception.what());
        } catch (...) {
            SetFatal("tick decoder lane failed with unknown exception");
        }
    }

    void SnapshotWorker(std::size_t lane_index) noexcept {
        try {
            LaneStorage& lane = *snapshot_lanes_[lane_index];
            SnapshotLaneStats& lane_stats = snapshot_stats_[lane_index];
            const internal::DecoderLimits limits{
                config_.maximum_text_bytes,
                config_.maximum_depth_items,
                config_.maximum_queue_items};
            std::size_t idle_spins = 0U;
            std::size_t timer_check_spins = 0U;
            std::uint64_t next_raw_timer =
                config_.raw_record_tap == nullptr
                    ? std::numeric_limits<std::uint64_t>::max()
                    : MonotonicNowNs() + config_.recovery_timer_scan_ns;
            for (;;) {
                std::uint32_t slot = 0U;
                internal::OwnedMessageView message;
                if (lane.TryConsume(&slot, &message)) {
                    idle_spins = 0U;
                    internal::SnapshotDecodeResult decoded;
                    const internal::DecodeError error =
                        internal::DecodeSnapshot(
                            message, config_.trade_date, limits, catalog_,
                            &decoded);
                    if (error == internal::DecodeError::kNone) {
                        lane_stats.decoded_snapshots.fetch_add(
                            1U, std::memory_order_relaxed);
                        if (!decoded.catalog_match) {
                            lane_stats.catalog_misses.fetch_add(
                                1U, std::memory_order_relaxed);
                        }
                        const bool raw_accepted =
                            config_.raw_record_tap == nullptr ||
                            config_.raw_record_tap->AppendSnapshot(
                                lane_index, decoded.snapshot);
                        if (!raw_accepted) {
                            SetFatal(
                                "raw Snapshot batch queue exhausted or "
                                "failed; admission stopped at a continuity "
                                "boundary");
                        }
                        if (raw_accepted && decoded.catalog_match) {
                            const std::size_t owner = InstrumentOwner(
                                decoded.snapshot.common.instrument_ordinal);
                            const bool published = PublishBounded(
                                decoded.snapshot,
                                [this, lane_index, owner](
                                    const CanonicalSnapshot& value) {
                                    return dispatcher_.PublishSnapshot(
                                        lane_index, owner, value);
                                },
                                &lane_stats.dispatch_overflows);
                            if (published) {
                                lane_stats.dispatched_snapshots.fetch_add(
                                    1U, std::memory_order_relaxed);
                            } else {
                                SetFatal(
                                    "instrument snapshot dispatch queue "
                                    "overflow");
                            }
                        }
                    } else {
                        lane_stats.decode_errors.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                    if (!lane.Release(slot)) {
                        SetFatal(
                            "snapshot lane recycle queue invariant failed");
                    }
                    if (config_.raw_record_tap != nullptr &&
                        message.receive_monotonic_ns >= next_raw_timer &&
                        !config_.raw_record_tap->PollSnapshot(
                            lane_index, message.receive_monotonic_ns)) {
                        SetFatal("raw Snapshot batch flush failed");
                    }
                    if (message.receive_monotonic_ns >= next_raw_timer) {
                        next_raw_timer = message.receive_monotonic_ns +
                                         config_.recovery_timer_scan_ns;
                    }
                } else {
                    if (!running_.load(std::memory_order_acquire) &&
                        lane.empty()) {
                        if (config_.raw_record_tap != nullptr &&
                            !config_.raw_record_tap->FlushSnapshot(
                                lane_index)) {
                            SetFatal(
                                "final raw Snapshot batch flush failed");
                        }
                        break;
                    }
                    if (config_.raw_record_tap != nullptr) {
                        ++timer_check_spins;
                    }
                    if (config_.raw_record_tap != nullptr &&
                        timer_check_spins >= 1'024U) {
                        timer_check_spins = 0U;
                        const std::uint64_t now = MonotonicNowNs();
                        if (now >= next_raw_timer) {
                            if (config_.raw_record_tap != nullptr &&
                                !config_.raw_record_tap->PollSnapshot(
                                    lane_index, now)) {
                                SetFatal("raw Snapshot batch flush failed");
                            }
                            next_raw_timer = now +
                                             config_.recovery_timer_scan_ns;
                        }
                    }
                    if (config_.first_decoder_cpu >= 0) {
                        CpuRelax();
                    } else if (idle_spins < 4'096U) {
                        ++idle_spins;
                        CpuRelax();
                    } else {
                        std::this_thread::yield();
                    }
                }
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("snapshot decoder lane failed: ") +
                     exception.what());
        } catch (...) {
            SetFatal("snapshot decoder lane failed with unknown exception");
        }
    }

    EngineConfig config_{};
    InstrumentCatalog catalog_;
    AdmissionStats admission_stats_{};
    std::unique_ptr<TickLaneStats[]> tick_stats_;
    std::unique_ptr<SnapshotLaneStats[]> snapshot_stats_;
    std::size_t instrument_owner_mask_ = 0U;
    Dispatcher dispatcher_;
    std::vector<std::unique_ptr<LaneStorage>> tick_lanes_;
    std::vector<std::unique_ptr<LaneStorage>> snapshot_lanes_;
    std::vector<std::unique_ptr<SequenceRecovery>> tick_recovery_;
    std::vector<std::thread> threads_;
    std::atomic<bool> started_{false};
    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> healthy_{true};
    std::atomic_flag callback_active_ = ATOMIC_FLAG_INIT;
    std::atomic_flag fatal_claimed_ = ATOMIC_FLAG_INIT;
    std::uint64_t next_ingress_sequence_ = 0U;
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
};

std::string_view AdmissionResultName(AdmissionResult value) noexcept {
    switch (value) {
        case AdmissionResult::kAccepted:
            return "accepted";
        case AdmissionResult::kNotRunning:
            return "not_running";
        case AdmissionResult::kConcurrentCallback:
            return "concurrent_callback";
        case AdmissionResult::kNullMessage:
            return "null_message";
        case AdmissionResult::kHeaderTruncated:
            return "header_truncated";
        case AdmissionResult::kHeaderInvalid:
            return "header_invalid";
        case AdmissionResult::kBodySizeMismatch:
            return "body_size_mismatch";
        case AdmissionResult::kNonBinaryEncoding:
            return "non_binary_encoding";
        case AdmissionResult::kUnsupportedMessage:
            return "unsupported_message";
        case AdmissionResult::kDisabledMessage:
            return "disabled_message";
        case AdmissionResult::kBodyTruncated:
            return "body_truncated";
        case AdmissionResult::kBodyTooLarge:
            return "body_too_large";
        case AdmissionResult::kRouteInvalid:
            return "route_invalid";
        case AdmissionResult::kLaneFull:
            return "lane_full";
        case AdmissionResult::kFeedNotReady:
            return "feed_not_ready";
        case AdmissionResult::kInternalFailure:
            return "internal_failure";
    }
    return "unknown";
}

bool ParseMdlHeader(std::span<const std::byte> bytes,
                    ParsedHeader* output) noexcept {
    if (output == nullptr || bytes.size() < kMdlHeaderBytes) {
        return false;
    }
    ParsedHeader parsed{};
    if (!LoadHeaderUnsigned(bytes, 0U, &parsed.head_size) ||
        !LoadHeaderUnsigned(bytes, 1U, &parsed.message_size) ||
        !LoadHeaderUnsigned(bytes, 5U, &parsed.encoding) ||
        !LoadHeaderUnsigned(bytes, 6U, &parsed.key.service_id) ||
        !LoadHeaderUnsigned(bytes, 7U, &parsed.key.service_version) ||
        !LoadHeaderUnsigned(bytes, 9U, &parsed.key.message_id) ||
        !LoadHeaderUnsigned(bytes, 11U, &parsed.local_time_raw) ||
        !LoadHeaderUnsigned(bytes, 15U, &parsed.vendor_sequence_id)) {
        return false;
    }
    *output = parsed;
    return true;
}

std::unique_ptr<IngestEngine> IngestEngine::Create(
    EngineConfig config,
    InstrumentCatalog catalog,
    std::string* error) {
    if (!ValidateConfig(config, catalog, error)) {
        return nullptr;
    }
    try {
        return std::unique_ptr<IngestEngine>(new IngestEngine(
            std::make_unique<Impl>(config, std::move(catalog))));
    } catch (const std::bad_alloc&) {
        if (error != nullptr) {
            *error = "engine preallocation failed";
        }
        return nullptr;
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("engine creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

IngestEngine::IngestEngine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IngestEngine::~IngestEngine() { impl_->Stop(); }

bool IngestEngine::Start(std::string* error) { return impl_->Start(error); }

void IngestEngine::Stop() noexcept { impl_->Stop(); }

AdmissionResult IngestEngine::AdmitMdlMessage(
    std::span<const std::byte> header,
    std::span<const std::byte> body,
    std::uint64_t receive_monotonic_ns) noexcept {
    return impl_->Admit(header, body, receive_monotonic_ns);
}

bool IngestEngine::TryPollTick(std::size_t owner,
                               CanonicalTick* output) noexcept {
    return impl_->TryPollTick(owner, output);
}

bool IngestEngine::TryPollSnapshot(std::size_t owner,
                                   CanonicalSnapshot* output) noexcept {
    return impl_->TryPollSnapshot(owner, output);
}

bool IngestEngine::TryPollGap(ChannelGap* output) noexcept {
    return impl_->TryPollGap(output);
}

bool IngestEngine::TryPollLateRecovery(
    LateRecoveryTick* output) noexcept {
    return impl_->TryPollLateRecovery(output);
}

bool IngestEngine::TryPollChannelFault(ChannelFault* output) noexcept {
    return impl_->TryPollChannelFault(output);
}

EngineStats IngestEngine::stats() const noexcept { return impl_->Stats(); }

bool IngestEngine::healthy() const noexcept { return impl_->healthy(); }

std::string IngestEngine::fatal_error() const { return impl_->fatal_error(); }

const EngineConfig& IngestEngine::config() const noexcept {
    return impl_->config();
}

std::uint64_t MonotonicNowNs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

}  // namespace l2flow::ingest
