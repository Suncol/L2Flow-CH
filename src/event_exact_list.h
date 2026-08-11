#ifndef L2FLOW_EVENT_EXACT_LIST_H_
#define L2FLOW_EVENT_EXACT_LIST_H_

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace l2flow::event::internal {

template <typename T>
struct ExactListDefaultArrayAllocation final {
    [[nodiscard]] static std::unique_ptr<T[]> Allocate(std::size_t count) {
        return std::unique_ptr<T[]>(new T[count]);
    }
};

// A single-owner contiguous list with exact, observable allocation capacity.
// It is intentionally narrower than std::vector: callers may append, inspect
// a read-only span, erase by swap, and explicitly release empty storage.
//
// Logical owned-byte charges include conservative per-allocation overhead.
// They support hard-cap accounting but do not claim that delete[] returns RSS
// to the operating system.
template <typename T,
          typename ArrayAllocation = ExactListDefaultArrayAllocation<T>>
class ExactList final {
private:
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::is_nothrow_default_constructible_v<T>);
    static_assert(std::is_nothrow_copy_constructible_v<T>);
    static_assert(std::is_nothrow_copy_assignable_v<T>);
    static_assert(std::is_nothrow_destructible_v<T>);
    static_assert(std::is_same_v<
                  decltype(ArrayAllocation::Allocate(std::size_t{})),
                  std::unique_ptr<T[]>>);

    [[nodiscard]] static constexpr std::size_t AllocationAlignment() noexcept {
        return std::max(alignof(T), alignof(std::max_align_t));
    }

    [[nodiscard]] static std::size_t CheckedAdd(std::size_t left,
                                                std::size_t right) {
        if (left > std::numeric_limits<std::size_t>::max() - right) {
            throw std::length_error("exact list byte size overflow");
        }
        return left + right;
    }

    [[nodiscard]] static std::size_t CheckedMultiply(std::size_t left,
                                                     std::size_t right) {
        if (left != 0U &&
            right > std::numeric_limits<std::size_t>::max() / left) {
            throw std::length_error("exact list allocation size overflow");
        }
        return left * right;
    }

public:
    using value_type = T;
    using size_type = std::size_t;

    static constexpr size_type kInitialCapacity = 8U;

    class PreparedAppend final {
    public:
        PreparedAppend(const PreparedAppend&) = delete;
        PreparedAppend& operator=(const PreparedAppend&) = delete;

        PreparedAppend(PreparedAppend&& other) noexcept {
            MoveFrom(&other);
        }

        PreparedAppend& operator=(PreparedAppend&& other) noexcept {
            if (this != &other) {
                MoveFrom(&other);
            }
            return *this;
        }

        [[nodiscard]] bool grows() const noexcept {
            return target_capacity_ != prepared_capacity_;
        }

        [[nodiscard]] size_type append_index() const noexcept {
            return prepared_size_;
        }

        [[nodiscard]] size_type target_capacity() const noexcept {
            return target_capacity_;
        }

        // The full new allocation coexists with the old allocation until
        // commit. A hard-cap owner must reserve this before CommitAppend.
        [[nodiscard]] size_type peak_allocation_owned_bytes() const noexcept {
            return peak_allocation_owned_bytes_;
        }

        // Net retained growth after the old allocation has been released.
        [[nodiscard]] size_type steady_owned_byte_delta() const noexcept {
            return steady_owned_byte_delta_;
        }

    private:
        friend class ExactList;

        PreparedAppend(ExactList* owner,
                       size_type mutation_generation,
                       size_type prepared_size,
                       size_type prepared_capacity,
                       size_type target_capacity,
                       size_type peak_allocation_owned_bytes,
                       size_type steady_owned_byte_delta) noexcept
            : owner_(owner),
              mutation_generation_(mutation_generation),
              prepared_size_(prepared_size),
              prepared_capacity_(prepared_capacity),
              target_capacity_(target_capacity),
              peak_allocation_owned_bytes_(peak_allocation_owned_bytes),
              steady_owned_byte_delta_(steady_owned_byte_delta) {}

        void MoveFrom(PreparedAppend* other) noexcept {
            owner_ = other->owner_;
            mutation_generation_ = other->mutation_generation_;
            prepared_size_ = other->prepared_size_;
            prepared_capacity_ = other->prepared_capacity_;
            target_capacity_ = other->target_capacity_;
            peak_allocation_owned_bytes_ =
                other->peak_allocation_owned_bytes_;
            steady_owned_byte_delta_ = other->steady_owned_byte_delta_;
            other->owner_ = nullptr;
            other->prepared_size_ = 0U;
            other->prepared_capacity_ = 0U;
            other->target_capacity_ = 0U;
            other->peak_allocation_owned_bytes_ = 0U;
            other->steady_owned_byte_delta_ = 0U;
        }

        ExactList* owner_ = nullptr;
        size_type mutation_generation_ = 0U;
        size_type prepared_size_ = 0U;
        size_type prepared_capacity_ = 0U;
        size_type target_capacity_ = 0U;
        size_type peak_allocation_owned_bytes_ = 0U;
        size_type steady_owned_byte_delta_ = 0U;
    };

    struct AppendResult final {
        value_type* value = nullptr;
        size_type index = 0U;
        bool grew = false;
        size_type peak_allocation_owned_bytes = 0U;
        size_type released_owned_bytes = 0U;
        size_type steady_owned_byte_delta = 0U;
    };

    struct MovedElement final {
        value_type value{};
        size_type previous_index = 0U;
        size_type current_index = 0U;
    };

    struct EraseResult final {
        std::optional<MovedElement> moved;
    };

    [[nodiscard]] static size_type AllocationOwnedBytes(size_type capacity) {
        if (capacity == 0U) {
            return 0U;
        }
        const size_type payload = CheckedMultiply(capacity, sizeof(value_type));
        const size_type alignment = AllocationAlignment();
        const size_type with_rounding = CheckedAdd(payload, alignment - 1U);
        const size_type rounded =
            (with_rounding / alignment) * alignment;
        return CheckedAdd(rounded, CheckedMultiply(2U, alignment));
    }

    explicit ExactList(size_type maximum_entries)
        : maximum_entries_(ValidateMaximumEntries(maximum_entries)) {}

    ~ExactList() noexcept = default;

    ExactList(const ExactList&) = delete;
    ExactList& operator=(const ExactList&) = delete;
    ExactList(ExactList&&) = delete;
    ExactList& operator=(ExactList&&) = delete;

    [[nodiscard]] PreparedAppend PrepareAppend() {
        ValidateState();
        if (size_ >= maximum_entries_) {
            throw std::length_error("exact list capacity exceeded");
        }
        if (mutation_generation_ ==
            std::numeric_limits<size_type>::max()) {
            throw std::length_error("exact list mutation generation overflow");
        }

        if (size_ < capacity_) {
            return PreparedAppend(this, mutation_generation_, size_, capacity_,
                                  capacity_, 0U, 0U);
        }

        const size_type target_capacity = NextCapacity();
        const size_type new_owned_bytes =
            AllocationOwnedBytes(target_capacity);
        if (new_owned_bytes < owned_bytes_) {
            throw std::logic_error(
                "exact list owned-byte accounting corrupted");
        }
        return PreparedAppend(
            this, mutation_generation_, size_, capacity_, target_capacity,
            new_owned_bytes, new_owned_bytes - owned_bytes_);
    }

    [[nodiscard]] AppendResult CommitAppend(PreparedAppend&& prepared,
                                            const value_type& value) {
        ValidatePreparation(prepared);

        // Stage an aliased source while the old array is still authoritative.
        const value_type staged_value = value;
        const size_type append_index = size_;
        const size_type next_size = CheckedAdd(size_, 1U);
        const size_type next_generation =
            CheckedAdd(mutation_generation_, 1U);

        if (!prepared.grows()) {
            data_[append_index] = staged_value;
            size_ = next_size;
            mutation_generation_ = next_generation;
            ConsumePreparation(&prepared);
            return AppendResult{
                &data_[append_index], append_index, false, 0U, 0U, 0U};
        }

        std::unique_ptr<value_type[]> pending =
            ArrayAllocation::Allocate(prepared.target_capacity_);
        if (pending == nullptr) {
            throw std::bad_alloc();
        }
        if (size_ != 0U) {
            std::memcpy(pending.get(), data_.get(),
                        size_ * sizeof(value_type));
        }
        pending[append_index] = staged_value;

        const size_type released_owned_bytes = owned_bytes_;
        data_.swap(pending);
        capacity_ = prepared.target_capacity_;
        size_ = next_size;
        owned_bytes_ = prepared.peak_allocation_owned_bytes_;
        mutation_generation_ = next_generation;
        const size_type peak = prepared.peak_allocation_owned_bytes_;
        const size_type steady = prepared.steady_owned_byte_delta_;
        ConsumePreparation(&prepared);
        return AppendResult{&data_[append_index], append_index, true, peak,
                            released_owned_bytes, steady};
    }

    [[nodiscard]] EraseResult EraseAtSwap(size_type index) {
        ValidateState();
        if (index >= size_) {
            throw std::out_of_range("exact list erase index is out of range");
        }
        if (mutation_generation_ ==
            std::numeric_limits<size_type>::max()) {
            throw std::length_error("exact list mutation generation overflow");
        }

        const size_type last_index = size_ - 1U;
        EraseResult result;
        if (index != last_index) {
            result.moved.emplace(MovedElement{
                data_[last_index], last_index, index});
            data_[index] = data_[last_index];
        }
        --size_;
        ++mutation_generation_;
        return result;
    }

    // Swap erases retain storage so the hot path remains O(1). The owner may
    // explicitly release it once the list is empty.
    [[nodiscard]] size_type ReleaseStorage() {
        ValidateState();
        if (size_ != 0U) {
            throw std::logic_error(
                "exact list cannot release storage while non-empty");
        }
        if (capacity_ == 0U) {
            return 0U;
        }
        if (mutation_generation_ ==
            std::numeric_limits<size_type>::max()) {
            throw std::length_error("exact list mutation generation overflow");
        }

        const size_type released = owned_bytes_;
        data_.reset();
        capacity_ = 0U;
        owned_bytes_ = 0U;
        ++mutation_generation_;
        return released;
    }

    [[nodiscard]] std::span<const value_type> values() const noexcept {
        return std::span<const value_type>(data_.get(), size_);
    }

    [[nodiscard]] size_type size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0U; }
    [[nodiscard]] size_type capacity() const noexcept { return capacity_; }
    [[nodiscard]] size_type maximum_entries() const noexcept {
        return maximum_entries_;
    }
    [[nodiscard]] size_type owned_bytes() const noexcept {
        return owned_bytes_;
    }

private:
    [[nodiscard]] static size_type ValidateMaximumEntries(
        size_type maximum_entries) {
        if (maximum_entries == 0U) {
            throw std::invalid_argument(
                "exact list maximum_entries must be non-zero");
        }
        static_cast<void>(AllocationOwnedBytes(maximum_entries));
        return maximum_entries;
    }

    [[nodiscard]] size_type NextCapacity() const {
        if (capacity_ == 0U) {
            return std::min(kInitialCapacity, maximum_entries_);
        }
        const size_type doubled = capacity_ > maximum_entries_ - capacity_
            ? maximum_entries_
            : capacity_ * 2U;
        const size_type target = std::min(doubled, maximum_entries_);
        if (target <= capacity_ || target <= size_) {
            throw std::logic_error("exact list cannot grow its capacity");
        }
        return target;
    }

    void ValidateState() const {
        if (size_ > capacity_ || capacity_ > maximum_entries_ ||
            (capacity_ == 0U) != (data_ == nullptr) ||
            owned_bytes_ != AllocationOwnedBytes(capacity_)) {
            throw std::logic_error("exact list state is corrupted");
        }
    }

    void ValidatePreparation(const PreparedAppend& prepared) const {
        if (prepared.owner_ == nullptr) {
            throw std::invalid_argument(
                "exact list append preparation is empty or consumed");
        }
        if (prepared.owner_ != this) {
            throw std::invalid_argument(
                "exact list append preparation has a different owner");
        }
        ValidateState();
        if (prepared.mutation_generation_ != mutation_generation_ ||
            prepared.prepared_size_ != size_ ||
            prepared.prepared_capacity_ != capacity_) {
            throw std::logic_error("exact list append preparation is stale");
        }
        if (size_ >= maximum_entries_ ||
            mutation_generation_ ==
                std::numeric_limits<size_type>::max()) {
            throw std::length_error("exact list append is no longer possible");
        }

        const bool should_grow = size_ == capacity_;
        const size_type expected_target =
            should_grow ? NextCapacity() : capacity_;
        const size_type expected_peak = should_grow
            ? AllocationOwnedBytes(expected_target)
            : 0U;
        const size_type expected_steady = should_grow
            ? expected_peak - owned_bytes_
            : 0U;
        if (prepared.target_capacity_ != expected_target ||
            prepared.peak_allocation_owned_bytes_ != expected_peak ||
            prepared.steady_owned_byte_delta_ != expected_steady) {
            throw std::logic_error(
                "exact list append preparation does not match layout");
        }
    }

    static void ConsumePreparation(PreparedAppend* prepared) noexcept {
        prepared->owner_ = nullptr;
        prepared->prepared_size_ = 0U;
        prepared->prepared_capacity_ = 0U;
        prepared->target_capacity_ = 0U;
        prepared->peak_allocation_owned_bytes_ = 0U;
        prepared->steady_owned_byte_delta_ = 0U;
    }

    std::unique_ptr<value_type[]> data_;
    size_type size_ = 0U;
    size_type capacity_ = 0U;
    size_type maximum_entries_ = 0U;
    size_type owned_bytes_ = 0U;
    size_type mutation_generation_ = 0U;
};

}  // namespace l2flow::event::internal

#endif  // L2FLOW_EVENT_EXACT_LIST_H_
