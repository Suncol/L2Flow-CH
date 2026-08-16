#include "l2flow/ingest/engine.h"

#include "decoder_internal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
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
        if (!CanPush()) {
            return false;
        }
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        storage_.Store(static_cast<std::size_t>(head) & mask_, value);
        head_.store(head + 1U, std::memory_order_release);
        return true;
    }

    // Producer-only capacity probe. After it succeeds, this producer can push
    // one element without another thread consuming capacity.
    [[nodiscard]] bool CanPush() noexcept {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head - cached_tail_ < static_cast<std::uint64_t>(capacity_)) {
            return true;
        }
        cached_tail_ = tail_.load(std::memory_order_acquire);
        return head - cached_tail_ < static_cast<std::uint64_t>(capacity_);
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
    std::atomic<std::uint64_t> rejected_late_facts{0U};
    std::atomic<std::uint64_t> hole_fills_dispatched{0U};
    std::atomic<std::uint64_t> source_channel_controls{0U};
    std::atomic<std::uint64_t> owner_control_deliveries{0U};
    std::atomic<std::uint64_t> expired_hole_sequences{0U};
    std::atomic<std::uint64_t> gaps_skipped{0U};
    std::atomic<std::uint64_t> from_open_channels_frozen{0U};
    std::atomic<std::uint64_t> channel_faults_dispatched{0U};
    std::atomic<std::uint64_t> outbox_overflows{0U};
};

struct alignas(64) SnapshotLaneStats final {
    std::atomic<std::uint64_t> decoded_snapshots{0U};
    std::atomic<std::uint64_t> decode_errors{0U};
    std::atomic<std::uint64_t> catalog_misses{0U};
    std::atomic<std::uint64_t> dispatched_snapshots{0U};
    std::atomic<std::uint64_t> outbox_overflows{0U};
};

class SequenceRecovery final {
public:
    SequenceRecovery(StartMode mode,
                     std::uint64_t feed_session_epoch,
                     std::size_t maximum_channels,
                     std::size_t entries_per_channel,
                     std::uint64_t maximum_reorder_span,
                     std::uint64_t initial_hold_ns,
                     std::uint64_t gap_wait_ns,
                     TickLaneStats* stats)
        : mode_(mode),
          feed_session_epoch_(feed_session_epoch),
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

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitGap, typename Fatal>
    void Process(const internal::TickDecodeResult& decoded,
                 std::uint64_t now,
                 EmitOccurrence&& emit_occurrence,
                 EmitControl&& emit_control,
                 EmitGap&& emit_gap,
                 Fatal&& fatal) noexcept {
        ChannelState* state = FindOrCreate(
            decoded.tick.common.identity.market,
            decoded.tick.common.channel);
        if (state == nullptr) {
            fatal("native-sequence channel capacity or allocation exhausted");
            return;
        }
        if (state->frozen) {
            const std::uint64_t expected = RejectionExpected(
                *state, decoded.tick.common.native_sequence);
            Reject(*state, decoded, expected,
                   RejectionFloor(*state, expected),
                   emit_occurrence, fatal);
            return;
        }
        if (mode_ == StartMode::kPartial && state->discovering) {
            const InsertResult inserted = Insert(*state, decoded, 0U, 0U);
            if (inserted == InsertResult::kDuplicate) {
                const std::uint64_t expected = RejectionExpected(
                    *state, decoded.tick.common.native_sequence);
                Reject(*state, decoded, expected,
                       RejectionFloor(*state, expected),
                       emit_occurrence, fatal);
                return;
            }
            if (inserted == InsertResult::kCollision) {
                const std::uint64_t sequence =
                    decoded.tick.common.native_sequence;
                if (sequence < MinimumPending(*state)) {
                    ActivateDiscoveredOrigin(*state, sequence);
                } else {
                    FinishDiscovery(*state, now, emit_occurrence, fatal);
                }
                ProcessActive(*state, decoded, now, emit_occurrence,
                              emit_control, emit_gap, fatal);
                return;
            }
            if (state->first_seen_ns == 0U) {
                state->first_seen_ns = now;
                if (initial_hold_ns_ != 0U) {
                    ScheduleDeadline(now, initial_hold_ns_);
                }
            }
            if (initial_hold_ns_ == 0U ||
                DeadlineReached(now, state->first_seen_ns,
                                initial_hold_ns_)) {
                FinishDiscovery(*state, now, emit_occurrence, fatal);
            }
            return;
        }
        ProcessActive(*state, decoded, now, emit_occurrence, emit_control,
                      emit_gap, fatal);
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitGap, typename Fatal>
    void Poll(std::uint64_t now,
              EmitOccurrence&& emit_occurrence,
              EmitControl&& emit_control,
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
                if (DeadlineReached(now, state.first_seen_ns,
                                    initial_hold_ns_)) {
                    FinishDiscovery(state, now, emit_occurrence, fatal);
                } else {
                    ScheduleDeadline(state.first_seen_ns, initial_hold_ns_);
                }
                continue;
            }
            if (state.gap_open_ns != 0U &&
                DeadlineReached(now, state.gap_open_ns, gap_wait_ns_)) {
                ForceGapToMinimum(state, now, emit_occurrence, emit_control,
                                  emit_gap, fatal);
            } else if (state.gap_open_ns != 0U) {
                ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
            }
        }
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitGap, typename Fatal>
    void Flush(std::uint64_t now,
               EmitOccurrence&& emit_occurrence,
               EmitControl&& emit_control,
               EmitGap&& emit_gap,
               Fatal&& fatal) noexcept {
        for (std::size_t index = 0U; index < channel_count_; ++index) {
            ChannelState& state = channels_[index];
            if (state.frozen) {
                continue;
            }
            if (state.discovering) {
                FinishDiscovery(state, now, emit_occurrence, fatal);
            }
            while (state.pending_count > 0U) {
                const std::size_t pending_before = state.pending_count;
                ForceGapToMinimum(state, now, emit_occurrence, emit_control,
                                  emit_gap, fatal);
                if (state.pending_count >= pending_before) {
                    fatal("native-sequence flush made no progress");
                    break;
                }
            }
        }
    }

    [[nodiscard]] std::uint64_t MinimumPendingIngress() const noexcept {
        std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
        for (std::size_t channel = 0U; channel < channel_count_; ++channel) {
            const ChannelState& state = channels_[channel];
            if (state.pending_count == 0U) {
                continue;
            }
            for (std::size_t index = 0U; index < entries_per_channel_;
                 ++index) {
                const PendingSlot& slot = state.pending[index];
                if (slot.occupied) {
                    minimum = std::min(
                        minimum, slot.tick.common.ingress_sequence);
                }
            }
        }
        return minimum;
    }

    [[nodiscard]] std::uint64_t pending_revision() const noexcept {
        return pending_revision_;
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitFault, typename Fatal>
    void FreezeDecodeFailure(Market market,
                             std::uint32_t channel,
                             std::uint64_t sequence,
                             std::uint64_t now,
                             EmitOccurrence&& emit_occurrence,
                             EmitControl&& emit_control,
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
        RejectPendingAndFreeze(*state, sequence, now, emit_occurrence,
                               emit_control, emit_fault, fatal);
    }

private:
    struct PendingSlot final {
        CanonicalTick tick{};
        std::uint64_t arrival_expected = 0U;
        std::uint64_t arrival_floor = 0U;
        bool occupied = false;
        bool catalog_match = false;
    };

    struct HoleInterval final {
        std::uint64_t first = 0U;
        std::uint64_t last = 0U;
        std::uint64_t generation = 0U;
    };

    struct ChannelState final {
        Market market = Market::kUnknown;
        std::uint32_t channel = 0U;
        bool frozen = false;
        bool discovering = false;
        bool expired_loss = false;
        bool gap_metadata_pending = false;
        std::uint64_t origin = 1U;
        std::uint64_t expected = 1U;
        std::uint64_t first_seen_ns = 0U;
        std::uint64_t gap_open_ns = 0U;
        std::uint64_t gap_epoch = 0U;
        std::uint64_t cumulative_missing_sequences = 0U;
        std::uint64_t pending_gap_first = 0U;
        std::uint64_t pending_gap_last = 0U;
        std::size_t mailbox_slot = 0U;
        std::size_t pending_count = 0U;
        std::size_t hole_count = 0U;
        std::unique_ptr<PendingSlot[]> pending;
        std::unique_ptr<HoleInterval[]> holes;
    };

    enum class InsertResult : std::uint8_t {
        kInserted,
        kDuplicate,
        kCollision,
    };

    enum class ClaimResult : std::uint8_t {
        kNotFound,
        kClaimed,
        kCapacityExhausted,
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

    [[nodiscard]] std::uint64_t AdmissionFloorFor(
        const ChannelState& state,
        std::uint64_t expected) const noexcept {
        if (expected <= state.origin ||
            expected - state.origin <= maximum_reorder_span_) {
            return state.origin;
        }
        return expected - maximum_reorder_span_;
    }

    [[nodiscard]] std::uint64_t AdmissionFloor(
        const ChannelState& state) const noexcept {
        return AdmissionFloorFor(state, state.expected);
    }

    [[nodiscard]] static std::uint64_t RejectionExpected(
        const ChannelState& state,
        std::uint64_t sequence) noexcept {
        if (state.expected != 0U) {
            return state.expected;
        }
        if (state.origin != 0U) {
            return state.origin;
        }
        return sequence;
    }

    [[nodiscard]] std::uint64_t RejectionFloor(
        const ChannelState& state,
        std::uint64_t expected) const noexcept {
        return state.expected == 0U || state.origin == 0U
            ? expected
            : AdmissionFloor(state);
    }

    [[nodiscard]] std::uint64_t RetentionFrontier(
        const ChannelState& state) const noexcept {
        return state.hole_count == 0U ? state.expected
                                     : state.holes[0U].first;
    }

    [[nodiscard]] std::uint64_t ProjectedRetentionFrontier(
        const ChannelState& state,
        std::uint64_t projected_expected) const noexcept {
        const std::uint64_t floor =
            AdmissionFloorFor(state, projected_expected);
        for (std::size_t index = 0U; index < state.hole_count; ++index) {
            const HoleInterval& hole = state.holes[index];
            if (hole.last >= floor) {
                return std::max(hole.first, floor);
            }
        }
        return projected_expected;
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
                state.holes.reset(
                    new (std::nothrow) HoleInterval[entries_per_channel_]);
                if (state.pending == nullptr || state.holes == nullptr) {
                    return nullptr;
                }
                state.market = market;
                state.channel = channel;
                state.mailbox_slot = channel_count_;
                state.discovering = mode_ == StartMode::kPartial;
                state.expired_loss = mode_ == StartMode::kPartial;
                state.origin = mode_ == StartMode::kFromOpen ? 1U : 0U;
                state.expected = state.origin;
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
        const internal::TickDecodeResult& decoded,
        std::uint64_t arrival_expected,
        std::uint64_t arrival_floor) noexcept {
        const std::uint64_t sequence = decoded.tick.common.native_sequence;
        PendingSlot& slot = state.pending[
            static_cast<std::size_t>(sequence) & slot_mask_];
        if (slot.occupied) {
            return slot.tick.common.native_sequence == sequence
                       ? InsertResult::kDuplicate
                       : InsertResult::kCollision;
        }
        slot.tick = decoded.tick;
        slot.arrival_expected = arrival_expected;
        slot.arrival_floor = arrival_floor;
        slot.catalog_match = decoded.catalog_match;
        slot.occupied = true;
        ++state.pending_count;
        ++pending_revision_;
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

    [[nodiscard]] TickDispatch MakeOccurrence(
        const ChannelState& state,
        const CanonicalTick& tick,
        bool catalog_match,
        TickDispatchKind kind,
        std::uint64_t expected_at_arrival,
        std::uint64_t floor_at_arrival,
        std::uint64_t generation) const noexcept {
        TickDispatch dispatch{};
        dispatch.tick = tick;
        dispatch.feed_session_epoch = feed_session_epoch_;
        dispatch.expected_sequence = expected_at_arrival;
        dispatch.admission_floor = floor_at_arrival;
        dispatch.generation = generation;
        dispatch.channel = state.channel;
        dispatch.market = state.market;
        dispatch.kind = kind;
        dispatch.catalog_match = catalog_match;
        return dispatch;
    }

    template <typename EmitOccurrence, typename Fatal>
    void Reject(const ChannelState& state,
                const internal::TickDecodeResult& decoded,
                std::uint64_t expected_at_arrival,
                std::uint64_t floor_at_arrival,
                EmitOccurrence&& emit_occurrence,
                Fatal&& fatal) noexcept {
        stats_->rejected_late_facts.fetch_add(1U,
                                             std::memory_order_relaxed);
        const TickDispatch dispatch = MakeOccurrence(
            state, decoded.tick, decoded.catalog_match,
            TickDispatchKind::kRejectLateFact,
            expected_at_arrival, floor_at_arrival, state.gap_epoch);
        if (!emit_occurrence(decoded.tick, dispatch)) {
            fatal("canonical outbox rejected a late-fact occurrence");
        }
    }

    template <typename EmitOccurrence, typename Fatal>
    [[nodiscard]] bool RejectPending(ChannelState& state,
                                     EmitOccurrence&& emit_occurrence,
                                     Fatal&& fatal) noexcept {
        while (state.pending_count != 0U) {
            const std::uint64_t sequence = MinimumPending(state);
            PendingSlot* const slot = Find(state, sequence);
            if (slot == nullptr) {
                fatal("pending first-wins state is inconsistent");
                return false;
            }
            CanonicalTick tick = slot->tick;
            const bool catalog_match = slot->catalog_match;
            const std::uint64_t expected_at_arrival =
                slot->arrival_expected != 0U
                    ? slot->arrival_expected
                    : RejectionExpected(state,
                                        tick.common.native_sequence);
            const std::uint64_t floor_at_arrival =
                slot->arrival_floor != 0U
                    ? slot->arrival_floor
                    : RejectionFloor(state, expected_at_arrival);
            slot->occupied = false;
            --state.pending_count;
            ++pending_revision_;
            stats_->rejected_late_facts.fetch_add(
                1U, std::memory_order_relaxed);
            TickDispatch dispatch = MakeOccurrence(
                state, tick, catalog_match,
                TickDispatchKind::kRejectLateFact,
                expected_at_arrival, floor_at_arrival, state.gap_epoch);
            dispatch.evict_before = RetentionFrontier(state);
            if (!emit_occurrence(tick, dispatch)) {
                fatal("canonical outbox rejected a pending late fact");
                return false;
            }
        }
        state.gap_open_ns = 0U;
        return true;
    }

    template <typename EmitOccurrence, typename Fatal>
    [[nodiscard]] bool EmitOne(ChannelState& state,
                               PendingSlot& slot,
                               bool tracked_pending,
                               EmitOccurrence&& emit_occurrence,
                               Fatal&& fatal) noexcept {
        CanonicalTick tick = slot.tick;
        const CanonicalTick original_tick = slot.tick;
        const bool catalog_match = slot.catalog_match;
        const std::uint64_t expected_at_arrival = slot.arrival_expected;
        const std::uint64_t floor_at_arrival = slot.arrival_floor;
        slot.occupied = false;
        --state.pending_count;
        if (tracked_pending) {
            ++pending_revision_;
        }
        if (state.expired_loss || state.hole_count != 0U) {
            tick.validity &= ~kTickChannelHistoryValid;
            tick.common.quality_flags |= kQualityChannelHistoryIncomplete;
        }
        if (catalog_match && state.gap_metadata_pending) {
            tick.common.quality_flags |= kQualitySequenceGapBefore;
            tick.common.gap_epoch = state.gap_epoch;
            tick.common.gap_before_first = state.pending_gap_first;
            tick.common.gap_before_last = state.pending_gap_last;
            state.gap_metadata_pending = false;
        }
        if (expected_at_arrival == 0U || floor_at_arrival == 0U ||
            floor_at_arrival > expected_at_arrival) {
            fatal("ordered arrival token is not initialized");
            return false;
        }
        const std::uint64_t projected_expected =
            tick.common.native_sequence + 1U;
        TickDispatch dispatch = MakeOccurrence(
            state, tick, catalog_match, TickDispatchKind::kProjectOrdered,
            expected_at_arrival, floor_at_arrival,
            state.gap_epoch);
        dispatch.evict_before =
            ProjectedRetentionFrontier(state, projected_expected);
        if (!emit_occurrence(original_tick, dispatch)) {
            fatal("canonical outbox occurrence queue overflow");
            return false;
        }
        return true;
    }

    template <typename EmitOccurrence, typename Fatal>
    void DrainContiguous(ChannelState& state,
                         std::uint64_t now,
                         EmitOccurrence&& emit_occurrence,
                         Fatal&& fatal) noexcept {
        for (;;) {
            PendingSlot* slot = Find(state, state.expected);
            if (slot == nullptr) {
                break;
            }
            if (!EmitOne(state, *slot, true, emit_occurrence, fatal)) {
                return;
            }
            ++state.expected;
        }
        state.gap_open_ns = state.pending_count == 0U ? 0U : now;
        if (state.gap_open_ns != 0U) {
            ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
        }
    }

    [[nodiscard]] bool AddHole(ChannelState& state,
                               std::uint64_t first,
                               std::uint64_t last,
                               std::uint64_t generation) noexcept {
        if (last < first) {
            return true;
        }
        if (state.hole_count >= entries_per_channel_) {
            return false;
        }
        if (state.hole_count != 0U &&
            state.holes[state.hole_count - 1U].last >= first) {
            return false;
        }
        state.holes[state.hole_count++] =
            HoleInterval{first, last, generation};
        return true;
    }

    [[nodiscard]] ClaimResult ClaimHole(ChannelState& state,
                                         std::uint64_t sequence,
                                         std::uint64_t* generation) noexcept {
        std::size_t lower = 0U;
        std::size_t upper = state.hole_count;
        while (lower < upper) {
            const std::size_t middle = lower + (upper - lower) / 2U;
            if (state.holes[middle].last < sequence) {
                lower = middle + 1U;
            } else {
                upper = middle;
            }
        }
        if (lower == state.hole_count ||
            sequence < state.holes[lower].first) {
            return ClaimResult::kNotFound;
        }
        const HoleInterval claimed = state.holes[lower];
        *generation = claimed.generation;
        if (claimed.first == sequence && claimed.last == sequence) {
            std::move(state.holes.get() + lower + 1U,
                      state.holes.get() + state.hole_count,
                      state.holes.get() + lower);
            --state.hole_count;
        } else if (claimed.first == sequence) {
            state.holes[lower].first = sequence + 1U;
        } else if (claimed.last == sequence) {
            state.holes[lower].last = sequence - 1U;
        } else {
            if (state.hole_count >= entries_per_channel_) {
                return ClaimResult::kCapacityExhausted;
            }
            std::move_backward(state.holes.get() + lower + 1U,
                               state.holes.get() + state.hole_count,
                               state.holes.get() + state.hole_count + 1U);
            state.holes[lower].last = sequence - 1U;
            state.holes[lower + 1U] =
                HoleInterval{sequence + 1U, claimed.last,
                             claimed.generation};
            ++state.hole_count;
        }
        return ClaimResult::kClaimed;
    }

    void ExpireOpenHoles(ChannelState& state,
                         std::uint64_t floor) noexcept {
        std::size_t expired = 0U;
        std::uint64_t expired_sequences = 0U;
        while (expired < state.hole_count &&
               state.holes[expired].last < floor) {
            expired_sequences += state.holes[expired].last -
                                 state.holes[expired].first + 1U;
            ++expired;
        }
        if (expired != 0U) {
            std::move(state.holes.get() + expired,
                      state.holes.get() + state.hole_count,
                      state.holes.get());
            state.hole_count -= expired;
        }
        if (state.hole_count != 0U && state.holes[0U].first < floor) {
            expired_sequences += floor - state.holes[0U].first;
            state.holes[0U].first = floor;
        }
        stats_->expired_hole_sequences.fetch_add(
            expired_sequences, std::memory_order_relaxed);
        state.expired_loss = state.expired_loss || expired_sequences != 0U;
    }

    template <typename EmitControl, typename Fatal>
    [[nodiscard]] bool EmitSeal(const ChannelState& state,
                                EmitControl&& emit_control,
                                Fatal&& fatal) noexcept {
        TickDispatch seal{};
        seal.feed_session_epoch = feed_session_epoch_;
        seal.expected_sequence = state.expected;
        seal.admission_floor = AdmissionFloor(state);
        seal.generation = state.gap_epoch;
        seal.evict_before = RetentionFrontier(state);
        seal.channel = state.channel;
        seal.market = state.market;
        seal.kind = TickDispatchKind::kChannelSeal;
        if (!emit_control(seal)) {
            fatal("canonical outbox rejected ChannelSeal");
            return false;
        }
        return true;
    }

    template <typename EmitControl, typename Fatal>
    [[nodiscard]] bool ExpireAndMaybeSeal(
        ChannelState& state,
        std::uint64_t previous_retention,
        bool previously_had_holes,
        EmitControl&& emit_control,
        Fatal&& fatal) noexcept {
        ExpireOpenHoles(state, AdmissionFloor(state));
        return !previously_had_holes ||
                       RetentionFrontier(state) <= previous_retention
                   ? true
                   : EmitSeal(state, emit_control, fatal);
    }

    template <typename EmitControl, typename EmitGap, typename Fatal>
    [[nodiscard]] bool RecordGap(ChannelState& state,
                                 std::uint64_t first,
                                 std::uint64_t last,
                                 std::uint64_t next,
                                 std::uint64_t now,
                                 EmitControl&& emit_control,
                                 EmitGap&& emit_gap,
                                 Fatal&& fatal) noexcept {
        if (last < first) {
            return true;
        }
        if (state.gap_epoch ==
            std::numeric_limits<std::uint64_t>::max()) {
            fatal("channel gap generation exhausted");
            return false;
        }
        ++state.gap_epoch;
        if (!AddHole(state, first, last, state.gap_epoch)) {
            fatal("open-hole interval capacity exhausted");
            return false;
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

        TickDispatch opened{};
        opened.feed_session_epoch = feed_session_epoch_;
        opened.expected_sequence = state.expected;
        opened.admission_floor = AdmissionFloor(state);
        opened.generation = state.gap_epoch;
        opened.first_missing = first;
        opened.last_missing = last;
        opened.channel = state.channel;
        opened.market = state.market;
        opened.kind = TickDispatchKind::kGapOpen;
        if (!emit_control(opened)) {
            fatal("canonical outbox rejected GapOpen");
            return false;
        }

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
        gap.feed_session_epoch = feed_session_epoch_;
        if (!emit_gap(state.mailbox_slot, gap)) {
            fatal("channel gap mailbox invariant failed");
            return false;
        }
        stats_->gaps_skipped.fetch_add(1U, std::memory_order_relaxed);
        return true;
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitFault, typename Fatal>
    void RejectPendingAndFreeze(ChannelState& state,
                                std::uint64_t observed_sequence,
                                std::uint64_t now,
                                EmitOccurrence&& emit_occurrence,
                                EmitControl&& emit_control,
                                EmitFault&& emit_fault,
                                Fatal&& fatal) noexcept {
        if (state.frozen) {
            return;
        }
        if (!RejectPending(state, emit_occurrence, fatal)) {
            return;
        }
        const bool had_open_holes = state.hole_count != 0U;
        ExpireOpenHoles(state, state.expected);
        state.frozen = true;
        stats_->from_open_channels_frozen.fetch_add(
            1U, std::memory_order_relaxed);
        ChannelFault fault{};
        fault.market = state.market;
        fault.reason = ChannelFaultReason::kDecodeFailure;
        fault.channel = state.channel;
        fault.expected_sequence = state.expected;
        fault.observed_sequence = observed_sequence;
        fault.detected_monotonic_ns = now;
        fault.feed_session_epoch = feed_session_epoch_;
        if (had_open_holes && !EmitSeal(state, emit_control, fatal)) {
            return;
        }
        if (!emit_fault(fault)) {
            fatal("canonical outbox rejected channel fault");
        }
    }

    template <typename EmitOccurrence, typename Fatal>
    void FinishDiscovery(ChannelState& state,
                         std::uint64_t now,
                         EmitOccurrence&& emit_occurrence,
                         Fatal&& fatal) noexcept {
        if (!state.discovering || state.pending_count == 0U) {
            state.discovering = false;
            return;
        }
        ActivateDiscoveredOrigin(state, MinimumPending(state));
        DrainContiguous(state, now, emit_occurrence, fatal);
    }

    void ActivateDiscoveredOrigin(ChannelState& state,
                                  std::uint64_t origin) noexcept {
        state.discovering = false;
        state.origin = origin;
        state.expected = origin;
        for (std::size_t index = 0U; index < entries_per_channel_; ++index) {
            PendingSlot& slot = state.pending[index];
            if (slot.occupied) {
                slot.arrival_expected = origin;
                slot.arrival_floor = origin;
            }
        }
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitGap, typename Fatal>
    void ForceGapToMinimum(ChannelState& state,
                           std::uint64_t now,
                           EmitOccurrence&& emit_occurrence,
                           EmitControl&& emit_control,
                           EmitGap&& emit_gap,
                           Fatal&& fatal) noexcept {
        if (state.pending_count == 0U) {
            state.gap_open_ns = 0U;
            return;
        }
        const std::uint64_t previous_retention = RetentionFrontier(state);
        const bool previously_had_holes = state.hole_count != 0U;
        const std::uint64_t minimum = MinimumPending(state);
        const bool opened_gap = minimum > state.expected;
        if (opened_gap &&
            !RecordGap(state, state.expected, minimum - 1U, minimum, now,
                       emit_control, emit_gap, fatal)) {
            return;
        }
        state.expected = minimum;
        DrainContiguous(state, now, emit_occurrence, fatal);
        static_cast<void>(ExpireAndMaybeSeal(
            state, previous_retention,
            previously_had_holes || opened_gap,
            emit_control, fatal));
    }

    template <typename EmitOccurrence, typename EmitControl,
              typename EmitGap, typename Fatal>
    void ProcessActive(ChannelState& state,
                       const internal::TickDecodeResult& decoded,
                       std::uint64_t now,
                       EmitOccurrence&& emit_occurrence,
                       EmitControl&& emit_control,
                       EmitGap&& emit_gap,
                       Fatal&& fatal) noexcept {
        const std::uint64_t sequence = decoded.tick.common.native_sequence;
        const std::uint64_t arrival_expected = state.expected;
        const std::uint64_t arrival_floor = AdmissionFloor(state);
        if (sequence == std::numeric_limits<std::uint64_t>::max()) {
            Reject(state, decoded, arrival_expected, arrival_floor,
                   emit_occurrence, fatal);
            fatal("native sequence exhausted");
            return;
        }
        for (;;) {
            if (sequence < state.expected) {
                if (sequence < arrival_floor) {
                    Reject(state, decoded, arrival_expected, arrival_floor,
                           emit_occurrence, fatal);
                    return;
                }
                const std::uint64_t previous_retention =
                    RetentionFrontier(state);
                std::uint64_t hole_generation = 0U;
                const ClaimResult claimed =
                    ClaimHole(state, sequence, &hole_generation);
                if (claimed == ClaimResult::kCapacityExhausted) {
                    fatal("open-hole split capacity exhausted");
                    return;
                }
                if (claimed == ClaimResult::kNotFound) {
                    Reject(state, decoded, arrival_expected, arrival_floor,
                           emit_occurrence, fatal);
                    return;
                }

                CanonicalTick tick = decoded.tick;
                tick.validity &= ~kTickChannelHistoryValid;
                tick.common.quality_flags |=
                    kQualityChannelHistoryIncomplete |
                    kQualityHoleFill;
                tick.common.gap_epoch = state.gap_epoch;
                const TickDispatch dispatch = MakeOccurrence(
                    state, tick, decoded.catalog_match,
                    TickDispatchKind::kProjectHoleFill,
                    arrival_expected, arrival_floor, hole_generation);
                TickDispatch pinned = dispatch;
                pinned.evict_before = RetentionFrontier(state);
                if (!emit_occurrence(decoded.tick, pinned)) {
                    fatal("hole-fill canonical outbox queue overflow");
                    return;
                }
                if (RetentionFrontier(state) > previous_retention) {
                    static_cast<void>(EmitSeal(state, emit_control, fatal));
                }
                return;
            }
            if (sequence == state.expected) {
                const std::uint64_t previous_retention =
                    RetentionFrontier(state);
                const bool previously_had_holes = state.hole_count != 0U;
                PendingSlot temporary{};
                temporary.tick = decoded.tick;
                temporary.arrival_expected = arrival_expected;
                temporary.arrival_floor = arrival_floor;
                temporary.catalog_match = decoded.catalog_match;
                temporary.occupied = true;
                ++state.pending_count;
                if (!EmitOne(state, temporary, false, emit_occurrence,
                             fatal)) {
                    return;
                }
                ++state.expected;
                DrainContiguous(state, now, emit_occurrence, fatal);
                static_cast<void>(ExpireAndMaybeSeal(
                    state, previous_retention, previously_had_holes,
                    emit_control, fatal));
                return;
            }

            const std::uint64_t span = sequence - state.expected;
            const InsertResult inserted = Insert(
                state, decoded, arrival_expected, arrival_floor);
            if (inserted == InsertResult::kDuplicate) {
                Reject(state, decoded, arrival_expected, arrival_floor,
                       emit_occurrence, fatal);
                return;
            }
            if (inserted == InsertResult::kCollision ||
                span > maximum_reorder_span_) {
                if (inserted == InsertResult::kInserted) {
                    ForceGapToMinimum(state, now, emit_occurrence,
                                      emit_control, emit_gap, fatal);
                    return;
                }
                const std::uint64_t minimum = MinimumPending(state);
                if (state.pending_count == 0U || sequence < minimum) {
                    if (!RecordGap(state, state.expected, sequence - 1U,
                                   sequence, now, emit_control, emit_gap,
                                   fatal)) {
                        return;
                    }
                    state.expected = sequence;
                } else {
                    ForceGapToMinimum(state, now, emit_occurrence,
                                      emit_control, emit_gap, fatal);
                }
                continue;
            }
            if (state.gap_open_ns == 0U) {
                state.gap_open_ns = now;
                ScheduleDeadline(state.gap_open_ns, gap_wait_ns_);
            }
            if (gap_wait_ns_ == 0U) {
                ForceGapToMinimum(state, now, emit_occurrence, emit_control,
                                  emit_gap, fatal);
            }
            return;
        }
    }

    StartMode mode_ = StartMode::kFromOpen;
    std::uint64_t feed_session_epoch_ = 0U;
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
    std::uint64_t pending_revision_ = 0U;
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
    if (!IsTradeDateValid(config.trade_date) ||
        config.feed_session_epoch == 0U) {
        return fail("trade_date and feed_session_epoch must identify a valid "
                    "feed run");
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
        config.maximum_snapshot_body_bytes < 248U) {
        return fail("slot or body capacity is too small");
    }
    if (config.outbox == nullptr || !config.outbox->healthy()) {
        return fail("a healthy durable canonical outbox is required");
    }
    if (config.outbox->config().feed_session_epoch !=
        config.feed_session_epoch) {
        return fail("engine and canonical outbox feed epochs differ");
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
        config.instrument_workers >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
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
          tick_dispatch_fences_(
              config.tick_decoder_lanes * config.instrument_workers, 0U),
          tick_admitted_through_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.tick_decoder_lanes)),
          snapshot_admitted_through_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.snapshot_decoder_lanes)),
          tick_fence_targets_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.tick_decoder_lanes)),
          snapshot_fence_targets_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.snapshot_decoder_lanes)),
          tick_fence_completed_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.tick_decoder_lanes)),
          snapshot_fence_completed_(
              std::make_unique<std::atomic<std::uint64_t>[]>(
                  config.snapshot_decoder_lanes)),
          tick_fence_capture_(config.tick_decoder_lanes, 0U),
          snapshot_fence_capture_(config.snapshot_decoder_lanes, 0U) {
        for (std::size_t index = 0U; index < config_.tick_decoder_lanes;
             ++index) {
            tick_admitted_through_[index].store(0U,
                                                 std::memory_order_relaxed);
            tick_fence_targets_[index].store(0U,
                                             std::memory_order_relaxed);
            tick_fence_completed_[index].store(0U,
                                               std::memory_order_relaxed);
        }
        for (std::size_t index = 0U;
             index < config_.snapshot_decoder_lanes; ++index) {
            snapshot_admitted_through_[index].store(
                0U, std::memory_order_relaxed);
            snapshot_fence_targets_[index].store(
                0U, std::memory_order_relaxed);
            snapshot_fence_completed_[index].store(
                0U, std::memory_order_relaxed);
        }
        tick_lanes_.reserve(config_.tick_decoder_lanes);
        tick_recovery_.reserve(config_.tick_decoder_lanes);
        for (std::size_t index = 0U; index < config_.tick_decoder_lanes;
             ++index) {
            tick_lanes_.push_back(std::make_unique<LaneStorage>(
                config_.tick_slots_per_lane,
                config_.maximum_tick_body_bytes));
            tick_recovery_.push_back(std::make_unique<SequenceRecovery>(
                config_.start_mode,
                config_.feed_session_epoch,
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
        fence_wake_.notify_all();
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
        admission_epoch_.fetch_add(1U, std::memory_order_acq_rel);
        struct CallbackGuard final {
            std::atomic_flag* flag;
            std::atomic<std::uint64_t>* epoch;
            ~CallbackGuard() {
                epoch->fetch_add(1U, std::memory_order_release);
                flag->clear(std::memory_order_release);
            }
        } guard{&callback_active_, &admission_epoch_};

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
        if (is_tick) {
            tick_admitted_through_[lane_index].store(
                ingress_sequence, std::memory_order_release);
        } else {
            snapshot_admitted_through_[lane_index].store(
                ingress_sequence, std::memory_order_release);
        }
        next_ingress_sequence_ = ingress_sequence;
        admission_stats_.admitted.fetch_add(
            1U, std::memory_order_relaxed);
        return AdmissionResult::kAccepted;
    }

    [[nodiscard]] bool FenceAcceptedInputs(
        std::uint64_t timeout_ns,
        std::string* error) noexcept {
        const auto fail = [error](std::string message) noexcept {
            if (error != nullptr) {
                try {
                    *error = std::move(message);
                } catch (...) {
                }
            }
            return false;
        };
        if (timeout_ns == 0U) {
            return fail("decoder fence timeout must be positive");
        }
        if (!running_.load(std::memory_order_acquire) || !healthy()) {
            return fail("decoder fence requires a healthy running engine");
        }
        const std::uint64_t started_ns = MonotonicNowNs();
        const std::uint64_t deadline_ns =
            timeout_ns > std::numeric_limits<std::uint64_t>::max() -
                             started_ns
            ? std::numeric_limits<std::uint64_t>::max()
            : started_ns + timeout_ns;

        try {
            std::unique_lock<std::mutex> issue_lock(fence_issue_mutex_);
            for (;;) {
                const std::uint64_t before =
                    admission_epoch_.load(std::memory_order_acquire);
                if ((before & 1U) != 0U) {
                    if (!running_.load(std::memory_order_acquire) ||
                        !healthy()) {
                        return fail(
                            "decoder fence interrupted while capturing "
                            "admission");
                    }
                    if (MonotonicNowNs() >= deadline_ns) {
                        return fail(
                            "decoder fence timed out while capturing "
                            "admission");
                    }
                    std::this_thread::yield();
                    continue;
                }
                for (std::size_t index = 0U;
                     index < config_.tick_decoder_lanes; ++index) {
                    tick_fence_capture_[index] =
                        tick_admitted_through_[index].load(
                            std::memory_order_acquire);
                }
                for (std::size_t index = 0U;
                     index < config_.snapshot_decoder_lanes; ++index) {
                    snapshot_fence_capture_[index] =
                        snapshot_admitted_through_[index].load(
                            std::memory_order_acquire);
                }
                const std::uint64_t after =
                    admission_epoch_.load(std::memory_order_acquire);
                if (before == after && (after & 1U) == 0U) {
                    break;
                }
                if (MonotonicNowNs() >= deadline_ns) {
                    return fail(
                        "decoder fence timed out while capturing admission");
                }
            }

            const std::uint64_t previous =
                fence_generation_.load(std::memory_order_relaxed);
            if (previous == std::numeric_limits<std::uint64_t>::max()) {
                SetFatal("decoder fence generation exhausted");
                return fail(fatal_error());
            }
            const std::uint64_t generation = previous + 1U;
            for (std::size_t index = 0U;
                 index < config_.tick_decoder_lanes; ++index) {
                tick_fence_targets_[index].store(
                    tick_fence_capture_[index], std::memory_order_relaxed);
            }
            for (std::size_t index = 0U;
                 index < config_.snapshot_decoder_lanes; ++index) {
                snapshot_fence_targets_[index].store(
                    snapshot_fence_capture_[index],
                    std::memory_order_relaxed);
            }
            fence_generation_.store(generation, std::memory_order_release);

            const auto completed = [this, generation] {
                if (!healthy() ||
                    !running_.load(std::memory_order_acquire)) {
                    return true;
                }
                for (std::size_t index = 0U;
                     index < config_.tick_decoder_lanes; ++index) {
                    if (tick_fence_completed_[index].load(
                            std::memory_order_acquire) < generation) {
                        return false;
                    }
                }
                for (std::size_t index = 0U;
                     index < config_.snapshot_decoder_lanes; ++index) {
                    if (snapshot_fence_completed_[index].load(
                            std::memory_order_acquire) < generation) {
                        return false;
                    }
                }
                return true;
            };
            const std::uint64_t now_ns = MonotonicNowNs();
            const std::uint64_t remaining_ns = now_ns < deadline_ns
                ? deadline_ns - now_ns
                : 0U;
            const std::uint64_t bounded_timeout = std::min(
                remaining_ns,
                static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()));
            if (!fence_wake_.wait_for(
                    issue_lock,
                    std::chrono::nanoseconds(
                        static_cast<std::int64_t>(bounded_timeout)),
                    completed)) {
                return fail("decoder fence timed out before every lane "
                            "settled its pre-cut inputs");
            }
            if (!healthy() ||
                !running_.load(std::memory_order_acquire)) {
                return fail(fatal_error().empty()
                                ? "decoder fence interrupted by engine stop"
                                : fatal_error());
            }
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (const std::exception& exception) {
            return fail(std::string("decoder fence failed: ") +
                        exception.what());
        } catch (...) {
            return fail("decoder fence failed with unknown exception");
        }
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
            result.rejected_late_facts += lane.rejected_late_facts.load(
                std::memory_order_relaxed);
            result.hole_fills_dispatched +=
                lane.hole_fills_dispatched.load(std::memory_order_relaxed);
            result.source_channel_controls +=
                lane.source_channel_controls.load(std::memory_order_relaxed);
            result.owner_control_deliveries +=
                lane.owner_control_deliveries.load(
                    std::memory_order_relaxed);
            result.expired_hole_sequences +=
                lane.expired_hole_sequences.load(
                    std::memory_order_relaxed);
            result.gaps_skipped += lane.gaps_skipped.load(
                std::memory_order_relaxed);
            result.from_open_channels_frozen +=
                lane.from_open_channels_frozen.load(
                    std::memory_order_relaxed);
            result.channel_faults_dispatched +=
                lane.channel_faults_dispatched.load(
                    std::memory_order_relaxed);
            result.outbox_overflows += lane.outbox_overflows.load(
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
            result.outbox_overflows += lane.outbox_overflows.load(
                std::memory_order_relaxed);
        }
        return result;
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire) &&
               config_.outbox != nullptr && config_.outbox->healthy();
    }

    [[nodiscard]] std::string fatal_error() const {
        if (config_.outbox != nullptr && !config_.outbox->healthy()) {
            return "FATAL_CONTINUITY: " + config_.outbox->fatal_error();
        }
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
        fence_wake_.notify_all();
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
            const auto emit_occurrence = [this, lane_index, &lane_stats](
                                       const CanonicalTick& original,
                                       const TickDispatch& dispatch) {
                outbox::CanonicalRecord record{};
                record.kind = outbox::RecordKind::kTickOccurrence;
                record.producer_lane = static_cast<std::uint32_t>(lane_index);
                record.raw_tick = original;
                record.disposition = dispatch;
                record.catalog_match = dispatch.catalog_match;
                if (dispatch.catalog_match) {
                    const std::size_t owner = InstrumentOwner(
                        dispatch.tick.common.instrument_ordinal);
                    const std::size_t fence_index =
                        lane_index * config_.instrument_workers + owner;
                    std::uint64_t& fence =
                        tick_dispatch_fences_[fence_index];
                    if (fence == std::numeric_limits<std::uint64_t>::max()) {
                        return false;
                    }
                    record.owner = static_cast<std::uint32_t>(owner);
                    record.disposition.owner = record.owner;
                    record.disposition.dispatch_fence = ++fence;
                }
                const bool published = config_.outbox->Enqueue(record);
                if (!published) {
                    lane_stats.outbox_overflows.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                if (published) {
                    if (dispatch.catalog_match &&
                        (dispatch.kind ==
                            TickDispatchKind::kProjectOrdered ||
                        dispatch.kind ==
                            TickDispatchKind::kProjectHoleFill)) {
                        lane_stats.dispatched_ticks.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                    if (dispatch.catalog_match && dispatch.kind ==
                        TickDispatchKind::kProjectHoleFill) {
                        lane_stats.hole_fills_dispatched.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                }
                return published;
            };
            const auto emit_control = [this, lane_index, &lane_stats](
                                          const TickDispatch& control) {
                for (std::size_t owner = 0U;
                     owner < config_.instrument_workers; ++owner) {
                    const std::size_t fence_index =
                        lane_index * config_.instrument_workers + owner;
                    std::uint64_t& fence =
                        tick_dispatch_fences_[fence_index];
                    if (fence == std::numeric_limits<std::uint64_t>::max()) {
                        return false;
                    }
                    outbox::CanonicalRecord record{};
                    record.kind = outbox::RecordKind::kTickControl;
                    record.producer_lane =
                        static_cast<std::uint32_t>(lane_index);
                    record.owner = static_cast<std::uint32_t>(owner);
                    record.disposition = control;
                    record.disposition.owner = record.owner;
                    record.disposition.dispatch_fence = fence + 1U;
                    if (!config_.outbox->Enqueue(record)) {
                        lane_stats.outbox_overflows.fetch_add(
                            1U, std::memory_order_relaxed);
                        return false;
                    }
                    ++fence;
                }
                lane_stats.source_channel_controls.fetch_add(
                    1U, std::memory_order_relaxed);
                lane_stats.owner_control_deliveries.fetch_add(
                    config_.instrument_workers,
                    std::memory_order_relaxed);
                return true;
            };
            const auto emit_gap = [this, lane_index](
                                      std::size_t channel_slot,
                                      const ChannelGap& gap) {
                static_cast<void>(channel_slot);
                outbox::CanonicalRecord record{};
                record.kind = outbox::RecordKind::kGapDiagnostic;
                record.producer_lane = static_cast<std::uint32_t>(lane_index);
                record.gap = gap;
                return healthy() && config_.outbox->Enqueue(record);
            };
            const auto emit_fault = [this, lane_index, &lane_stats](
                                        const ChannelFault& fault) {
                outbox::CanonicalRecord record{};
                record.kind = outbox::RecordKind::kChannelFault;
                record.producer_lane = static_cast<std::uint32_t>(lane_index);
                record.fault = fault;
                const bool published = config_.outbox->Enqueue(record);
                if (!published) {
                    lane_stats.outbox_overflows.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                if (published) {
                    lane_stats.channel_faults_dispatched.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                return published;
            };
            const auto fatal = [this](const char* message) {
                SetFatal(message);
            };

            std::uint64_t next_timer = MonotonicNowNs();
            std::uint64_t last_consumed_ingress = 0U;
            std::uint64_t checked_pending_revision =
                std::numeric_limits<std::uint64_t>::max();
            std::uint64_t minimum_pending_ingress =
                std::numeric_limits<std::uint64_t>::max();
            const auto service_fence = [&] {
                const std::uint64_t generation =
                    fence_generation_.load(std::memory_order_acquire);
                if (generation == 0U ||
                    tick_fence_completed_[lane_index].load(
                        std::memory_order_relaxed) >= generation) {
                    return;
                }
                const std::uint64_t target =
                    tick_fence_targets_[lane_index].load(
                        std::memory_order_acquire);
                if (last_consumed_ingress < target) {
                    return;
                }
                if (target != 0U) {
                    const std::uint64_t revision =
                        recovery.pending_revision();
                    if (revision != checked_pending_revision) {
                        minimum_pending_ingress =
                            recovery.MinimumPendingIngress();
                        checked_pending_revision = revision;
                    }
                    if (minimum_pending_ingress <= target) {
                        return;
                    }
                }
                tick_fence_completed_[lane_index].store(
                    generation, std::memory_order_release);
                fence_wake_.notify_all();
            };
            std::size_t idle_spins = 0U;
            for (;;) {
                service_fence();
                std::uint32_t slot = 0U;
                internal::OwnedMessageView message;
                if (lane.TryConsume(&slot, &message)) {
                    idle_spins = 0U;
                    if (!healthy()) {
                        if (!lane.Release(slot)) {
                            SetFatal(
                                "tick lane recycle queue invariant failed");
                        }
                        continue;
                    }
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
                        recovery.Process(
                            decoded, message.receive_monotonic_ns,
                            emit_occurrence, emit_control, emit_gap,
                            fatal);
                    } else {
                        lane_stats.decode_errors.fetch_add(
                            1U, std::memory_order_relaxed);
                        if (healthy() &&
                            config_.start_mode == StartMode::kFromOpen) {
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
                                    emit_occurrence, emit_control, emit_fault,
                                    fatal);
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
                    last_consumed_ingress = message.ingress_sequence;
                    if (healthy() &&
                        message.receive_monotonic_ns >= next_timer) {
                        recovery.Poll(
                            message.receive_monotonic_ns, emit_occurrence,
                            emit_control, emit_gap, fatal);
                        next_timer = message.receive_monotonic_ns +
                                     config_.recovery_timer_scan_ns;
                    }
                    service_fence();
                } else {
                    const std::uint64_t now = MonotonicNowNs();
                    if (healthy() && now >= next_timer) {
                        recovery.Poll(now, emit_occurrence, emit_control,
                                      emit_gap, fatal);
                        next_timer = now + config_.recovery_timer_scan_ns;
                        service_fence();
                    }
                    if (!running_.load(std::memory_order_acquire) &&
                        lane.empty()) {
                        if (healthy()) {
                            recovery.Flush(now, emit_occurrence, emit_control,
                                           emit_gap, fatal);
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
            std::uint64_t last_consumed_ingress = 0U;
            const auto service_fence = [&] {
                const std::uint64_t generation =
                    fence_generation_.load(std::memory_order_acquire);
                if (generation == 0U ||
                    snapshot_fence_completed_[lane_index].load(
                        std::memory_order_relaxed) >= generation) {
                    return;
                }
                const std::uint64_t target =
                    snapshot_fence_targets_[lane_index].load(
                        std::memory_order_acquire);
                if (last_consumed_ingress < target) {
                    return;
                }
                snapshot_fence_completed_[lane_index].store(
                    generation, std::memory_order_release);
                fence_wake_.notify_all();
            };
            for (;;) {
                service_fence();
                std::uint32_t slot = 0U;
                internal::OwnedMessageView message;
                if (lane.TryConsume(&slot, &message)) {
                    idle_spins = 0U;
                    if (!healthy()) {
                        if (!lane.Release(slot)) {
                            SetFatal(
                                "snapshot lane recycle queue invariant "
                                "failed");
                        }
                        continue;
                    }
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
                        outbox::CanonicalRecord record{};
                        record.kind = outbox::RecordKind::kSnapshot;
                        record.producer_lane =
                            static_cast<std::uint32_t>(lane_index);
                        record.raw_snapshot = decoded.snapshot;
                        record.catalog_match = decoded.catalog_match;
                        if (decoded.catalog_match) {
                            record.owner = static_cast<std::uint32_t>(
                                InstrumentOwner(decoded.snapshot.common
                                                    .instrument_ordinal));
                        }
                        if (config_.outbox->Enqueue(record)) {
                            if (decoded.catalog_match) {
                                lane_stats.dispatched_snapshots.fetch_add(
                                    1U, std::memory_order_relaxed);
                            }
                        } else {
                            lane_stats.outbox_overflows.fetch_add(
                                1U, std::memory_order_relaxed);
                            SetFatal("canonical snapshot outbox exhausted");
                        }
                    } else {
                        lane_stats.decode_errors.fetch_add(
                            1U, std::memory_order_relaxed);
                    }
                    if (!lane.Release(slot)) {
                        SetFatal(
                            "snapshot lane recycle queue invariant failed");
                    }
                    last_consumed_ingress = message.ingress_sequence;
                    service_fence();
                } else {
                    if (!running_.load(std::memory_order_acquire) &&
                        lane.empty()) {
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
    std::vector<std::uint64_t> tick_dispatch_fences_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> tick_admitted_through_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> snapshot_admitted_through_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> tick_fence_targets_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> snapshot_fence_targets_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> tick_fence_completed_;
    std::unique_ptr<std::atomic<std::uint64_t>[]> snapshot_fence_completed_;
    std::vector<std::uint64_t> tick_fence_capture_;
    std::vector<std::uint64_t> snapshot_fence_capture_;
    std::vector<std::unique_ptr<LaneStorage>> tick_lanes_;
    std::vector<std::unique_ptr<LaneStorage>> snapshot_lanes_;
    std::vector<std::unique_ptr<SequenceRecovery>> tick_recovery_;
    std::vector<std::thread> threads_;
    std::atomic<bool> started_{false};
    std::atomic<bool> running_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> healthy_{true};
    std::atomic_flag callback_active_ = ATOMIC_FLAG_INIT;
    std::atomic<std::uint64_t> admission_epoch_{0U};
    std::atomic<std::uint64_t> fence_generation_{0U};
    std::mutex fence_issue_mutex_;
    std::condition_variable fence_wake_;
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

bool IngestEngine::FenceAcceptedInputs(
    std::uint64_t timeout_ns,
    std::string* error) noexcept {
    return impl_->FenceAcceptedInputs(timeout_ns, error);
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
