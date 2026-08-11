#include "event_exact_list.h"
#include "l2flow/event/types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {

using l2flow::event::internal::ExactList;
using l2flow::event::OrderKey;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

template <typename Exception, typename Callable>
bool ThrowsAs(Callable&& callable) {
    try {
        std::forward<Callable>(callable)();
    } catch (const Exception&) {
        return true;
    } catch (...) {
        return false;
    }
    return false;
}

struct TestOrderKey final {
    std::uint32_t instrument = 0U;
    std::uint32_t channel = 0U;
    std::int64_t order_id = 0;

    friend constexpr bool operator==(const TestOrderKey&,
                                     const TestOrderKey&) = default;
};

static_assert(std::is_trivially_copyable_v<TestOrderKey>);
static_assert(std::is_nothrow_default_constructible_v<TestOrderKey>);
static_assert(std::is_nothrow_copy_constructible_v<TestOrderKey>);
static_assert(std::is_nothrow_copy_assignable_v<TestOrderKey>);
static_assert(std::is_nothrow_destructible_v<TestOrderKey>);
static_assert(std::is_trivially_copyable_v<OrderKey>);
static_assert(std::is_nothrow_default_constructible_v<OrderKey>);
static_assert(std::is_nothrow_copy_constructible_v<OrderKey>);
static_assert(std::is_nothrow_copy_assignable_v<OrderKey>);
static_assert(std::is_nothrow_destructible_v<OrderKey>);

template <typename T>
struct ControlledArrayAllocation final {
    [[nodiscard]] static std::unique_ptr<T[]> Allocate(std::size_t count) {
        ++calls;
        last_count = count;
        if (fail_next) {
            fail_next = false;
            throw std::bad_alloc();
        }
        if (return_null_next) {
            return_null_next = false;
            return nullptr;
        }
        return std::unique_ptr<T[]>(new T[count]);
    }

    static void Reset() noexcept {
        calls = 0U;
        last_count = 0U;
        fail_next = false;
        return_null_next = false;
    }

    static inline std::size_t calls = 0U;
    static inline std::size_t last_count = 0U;
    static inline bool fail_next = false;
    static inline bool return_null_next = false;
};

template <typename List>
typename List::AppendResult Append(
    List* list, const typename List::value_type& value) {
    auto prepared = list->PrepareAppend();
    return list->CommitAppend(std::move(prepared), value);
}

void TestExactCapacityAndGrowthCharges() {
    using List = ExactList<TestOrderKey>;
    List list(20U);
    CHECK(list.empty());
    CHECK(list.size() == 0U);
    CHECK(list.capacity() == 0U);
    CHECK(list.maximum_entries() == 20U);
    CHECK(list.owned_bytes() == 0U);

    auto first = list.PrepareAppend();
    CHECK(first.grows());
    CHECK(first.append_index() == 0U);
    CHECK(first.target_capacity() == List::kInitialCapacity);
    CHECK(first.peak_allocation_owned_bytes() ==
          List::AllocationOwnedBytes(List::kInitialCapacity));
    CHECK(first.steady_owned_byte_delta() ==
          first.peak_allocation_owned_bytes());
    const auto first_result = list.CommitAppend(
        std::move(first), TestOrderKey{1U, 2U, 10});
    CHECK(first_result.grew);
    CHECK(first_result.index == 0U);
    CHECK(first_result.value == list.values().data());
    CHECK(first_result.value->order_id == 10);
    CHECK(first_result.released_owned_bytes == 0U);
    CHECK(list.capacity() == 8U);
    CHECK(list.owned_bytes() == List::AllocationOwnedBytes(8U));

    const TestOrderKey* const initial_data = list.values().data();
    for (std::int64_t order_id = 11; order_id <= 17; ++order_id) {
        auto prepared = list.PrepareAppend();
        CHECK(!prepared.grows());
        CHECK(prepared.peak_allocation_owned_bytes() == 0U);
        CHECK(prepared.steady_owned_byte_delta() == 0U);
        const auto appended = list.CommitAppend(
            std::move(prepared), TestOrderKey{1U, 2U, order_id});
        CHECK(!appended.grew);
        CHECK(appended.peak_allocation_owned_bytes == 0U);
        CHECK(appended.released_owned_bytes == 0U);
        CHECK(appended.steady_owned_byte_delta == 0U);
        CHECK(list.values().data() == initial_data);
    }
    CHECK(list.size() == 8U);

    const std::size_t old_owned = list.owned_bytes();
    auto ninth = list.PrepareAppend();
    const std::size_t new_owned = List::AllocationOwnedBytes(16U);
    CHECK(ninth.grows());
    CHECK(ninth.target_capacity() == 16U);
    CHECK(ninth.peak_allocation_owned_bytes() == new_owned);
    CHECK(ninth.steady_owned_byte_delta() == new_owned - old_owned);
    const auto ninth_result = list.CommitAppend(
        std::move(ninth), TestOrderKey{1U, 2U, 18});
    CHECK(ninth_result.grew);
    CHECK(ninth_result.peak_allocation_owned_bytes == new_owned);
    CHECK(ninth_result.released_owned_bytes == old_owned);
    CHECK(ninth_result.steady_owned_byte_delta == new_owned - old_owned);
    CHECK(list.owned_bytes() == new_owned);
    CHECK(list.values().data() != initial_data);
    for (std::size_t index = 0U; index < list.size(); ++index) {
        CHECK(list.values()[index].order_id ==
              static_cast<std::int64_t>(10U + index));
    }

    while (list.size() < 16U) {
        const auto order_id = static_cast<std::int64_t>(
            10U + list.size());
        static_cast<void>(Append(
            &list, TestOrderKey{1U, 2U, order_id}));
    }
    auto capped_growth = list.PrepareAppend();
    CHECK(capped_growth.grows());
    CHECK(capped_growth.target_capacity() == 20U);
    static_cast<void>(list.CommitAppend(
        std::move(capped_growth), TestOrderKey{1U, 2U, 26}));
    CHECK(list.capacity() == 20U);
    while (list.size() < list.maximum_entries()) {
        static_cast<void>(Append(
            &list, TestOrderKey{1U, 2U,
                                static_cast<std::int64_t>(
                                    10U + list.size())}));
    }
    CHECK(ThrowsAs<std::length_error>([&list] {
        static_cast<void>(list.PrepareAppend());
    }));
    CHECK(list.size() == 20U);
}

void TestPeakChargeIncludesBothArrays() {
    using List = ExactList<std::uint64_t>;
    List list(10U);
    for (std::uint64_t value = 0U; value < 8U; ++value) {
        static_cast<void>(Append(&list, value));
    }
    const std::size_t old_owned = list.owned_bytes();
    auto prepared = list.PrepareAppend();
    CHECK(prepared.target_capacity() == 10U);
    const std::size_t full_new_allocation =
        List::AllocationOwnedBytes(10U);
    CHECK(prepared.peak_allocation_owned_bytes() == full_new_allocation);
    CHECK(prepared.steady_owned_byte_delta() ==
          full_new_allocation - old_owned);
    CHECK(old_owned + prepared.peak_allocation_owned_bytes() >
          old_owned + prepared.steady_owned_byte_delta());
}

void TestProductionOrderKeyAndAliasedGrowth() {
    using List = ExactList<OrderKey>;
    List list(16U);
    for (std::int64_t order_id = 10; order_id < 18; ++order_id) {
        static_cast<void>(Append(
            &list, OrderKey{20260810U, l2flow::event::Market::kShanghai,
                            600000U, 7U, order_id}));
    }
    CHECK(list.size() == 8U);
    const OrderKey source = list.values()[3U];
    auto prepared = list.PrepareAppend();
    CHECK(prepared.grows());
    const auto appended = list.CommitAppend(
        std::move(prepared), list.values()[3U]);
    CHECK(appended.grew);
    CHECK(list.size() == 9U);
    CHECK(list.values()[3U] == source);
    CHECK(list.values()[8U] == source);
}

void TestStaleWrongOwnerAndConsumedPlans() {
    using List = ExactList<int>;
    List first(16U);
    List second(16U);

    auto wrong_owner = first.PrepareAppend();
    CHECK(ThrowsAs<std::invalid_argument>(
        [&second, &wrong_owner] {
            static_cast<void>(
                second.CommitAppend(std::move(wrong_owner), 1));
        }));
    CHECK(first.empty());
    CHECK(second.empty());

    auto stale_growth = first.PrepareAppend();
    static_cast<void>(Append(&first, 10));
    const auto before_stale = first.values();
    CHECK(ThrowsAs<std::logic_error>([&first, &stale_growth] {
        static_cast<void>(
            first.CommitAppend(std::move(stale_growth), 20));
    }));
    CHECK(first.size() == 1U);
    CHECK(first.values().data() == before_stale.data());
    CHECK(first.values()[0] == 10);

    auto stale_without_growth = first.PrepareAppend();
    static_cast<void>(Append(&first, 20));
    CHECK(ThrowsAs<std::logic_error>([&first, &stale_without_growth] {
        static_cast<void>(
            first.CommitAppend(std::move(stale_without_growth), 30));
    }));
    CHECK(first.size() == 2U);
    CHECK(first.values()[0] == 10);
    CHECK(first.values()[1] == 20);

    auto consumed = first.PrepareAppend();
    static_cast<void>(first.CommitAppend(std::move(consumed), 30));
    CHECK(ThrowsAs<std::invalid_argument>([&first, &consumed] {
        static_cast<void>(first.CommitAppend(std::move(consumed), 40));
    }));
    CHECK(first.size() == 3U);
}

void TestSwapEraseAndStorageRelease() {
    using List = ExactList<TestOrderKey>;
    List list(8U);
    static_cast<void>(Append(&list, TestOrderKey{1U, 2U, 10}));
    static_cast<void>(Append(&list, TestOrderKey{1U, 2U, 20}));
    static_cast<void>(Append(&list, TestOrderKey{1U, 2U, 30}));
    const std::size_t retained_owned = list.owned_bytes();

    CHECK(ThrowsAs<std::logic_error>([&list] {
        static_cast<void>(list.ReleaseStorage());
    }));
    CHECK(list.size() == 3U);
    CHECK(list.owned_bytes() == retained_owned);

    auto pending = list.PrepareAppend();
    const auto middle = list.EraseAtSwap(1U);
    CHECK(middle.moved.has_value());
    CHECK(middle.moved->value.order_id == 30);
    CHECK(middle.moved->previous_index == 2U);
    CHECK(middle.moved->current_index == 1U);
    CHECK(list.size() == 2U);
    CHECK(list.values()[0].order_id == 10);
    CHECK(list.values()[1].order_id == 30);
    CHECK(list.capacity() == 8U);
    CHECK(list.owned_bytes() == retained_owned);
    CHECK(ThrowsAs<std::logic_error>([&list, &pending] {
        static_cast<void>(
            list.CommitAppend(std::move(pending),
                              TestOrderKey{1U, 2U, 40}));
    }));

    const auto last = list.EraseAtSwap(1U);
    CHECK(!last.moved.has_value());
    CHECK(list.size() == 1U);
    CHECK(list.values()[0].order_id == 10);
    CHECK(ThrowsAs<std::out_of_range>([&list] {
        static_cast<void>(list.EraseAtSwap(1U));
    }));
    CHECK(list.size() == 1U);

    static_cast<void>(list.EraseAtSwap(0U));
    CHECK(list.empty());
    CHECK(list.capacity() == 8U);
    auto stale_after_release = list.PrepareAppend();
    CHECK(!stale_after_release.grows());
    const std::size_t released = list.ReleaseStorage();
    CHECK(released == retained_owned);
    CHECK(list.capacity() == 0U);
    CHECK(list.owned_bytes() == 0U);
    CHECK(list.values().empty());
    CHECK(list.ReleaseStorage() == 0U);
    CHECK(ThrowsAs<std::logic_error>([&list, &stale_after_release] {
        static_cast<void>(list.CommitAppend(
            std::move(stale_after_release), TestOrderKey{1U, 2U, 50}));
    }));

    const auto restarted = Append(&list, TestOrderKey{1U, 2U, 60});
    CHECK(restarted.grew);
    CHECK(list.size() == 1U);
    CHECK(list.capacity() == 8U);
}

void TestOverflowAndSmallCapacity() {
    using List = ExactList<std::uint64_t>;
    CHECK(ThrowsAs<std::invalid_argument>([] {
        List invalid(0U);
        static_cast<void>(invalid);
    }));
    CHECK(ThrowsAs<std::length_error>([] {
        List overflow(std::numeric_limits<std::size_t>::max());
        static_cast<void>(overflow);
    }));
    CHECK(ThrowsAs<std::length_error>([] {
        static_cast<void>(List::AllocationOwnedBytes(
            std::numeric_limits<std::size_t>::max()));
    }));

    List one(1U);
    auto prepared = one.PrepareAppend();
    CHECK(prepared.target_capacity() == 1U);
    CHECK(prepared.peak_allocation_owned_bytes() ==
          List::AllocationOwnedBytes(1U));
    static_cast<void>(one.CommitAppend(std::move(prepared), 7U));
    CHECK(one.size() == 1U);
    CHECK(one.capacity() == 1U);
    CHECK(ThrowsAs<std::length_error>([&one] {
        static_cast<void>(one.PrepareAppend());
    }));
    CHECK(one.values()[0] == 7U);
}

void TestAllocationFailureRollbackAndRetry() {
    using Allocation = ControlledArrayAllocation<TestOrderKey>;
    using List = ExactList<TestOrderKey, Allocation>;
    Allocation::Reset();
    List list(16U);

    auto initial = list.PrepareAppend();
    Allocation::fail_next = true;
    CHECK(ThrowsAs<std::bad_alloc>([&list, &initial] {
        static_cast<void>(list.CommitAppend(
            std::move(initial), TestOrderKey{1U, 2U, 10}));
    }));
    CHECK(list.empty());
    CHECK(list.capacity() == 0U);
    CHECK(list.owned_bytes() == 0U);
    CHECK(Allocation::calls == 1U);
    CHECK(Allocation::last_count == 8U);

    const auto retry = list.CommitAppend(
        std::move(initial), TestOrderKey{1U, 2U, 10});
    CHECK(retry.grew);
    CHECK(list.size() == 1U);
    CHECK(list.values()[0].order_id == 10);
    CHECK(Allocation::calls == 2U);

    for (std::int64_t order_id = 11; order_id <= 17; ++order_id) {
        static_cast<void>(Append(
            &list, TestOrderKey{1U, 2U, order_id}));
    }
    CHECK(list.size() == 8U);
    CHECK(Allocation::calls == 2U);
    const TestOrderKey* const old_data = list.values().data();
    const std::size_t old_owned = list.owned_bytes();
    const auto old_values = std::array<std::int64_t, 8U>{
        10, 11, 12, 13, 14, 15, 16, 17};
    auto growth = list.PrepareAppend();
    Allocation::fail_next = true;
    CHECK(ThrowsAs<std::bad_alloc>([&list, &growth] {
        static_cast<void>(list.CommitAppend(
            std::move(growth), TestOrderKey{1U, 2U, 18}));
    }));
    CHECK(list.size() == 8U);
    CHECK(list.capacity() == 8U);
    CHECK(list.owned_bytes() == old_owned);
    CHECK(list.values().data() == old_data);
    for (std::size_t index = 0U; index < old_values.size(); ++index) {
        CHECK(list.values()[index].order_id == old_values[index]);
    }

    const auto growth_retry = list.CommitAppend(
        std::move(growth), TestOrderKey{1U, 2U, 18});
    CHECK(growth_retry.grew);
    CHECK(list.size() == 9U);
    CHECK(list.capacity() == 16U);
    CHECK(list.values()[8U].order_id == 18);

    List null_list(8U);
    auto null_plan = null_list.PrepareAppend();
    Allocation::return_null_next = true;
    CHECK(ThrowsAs<std::bad_alloc>([&null_list, &null_plan] {
        static_cast<void>(null_list.CommitAppend(
            std::move(null_plan), TestOrderKey{3U, 4U, 50}));
    }));
    CHECK(null_list.empty());
    CHECK(null_list.capacity() == 0U);
    CHECK(null_list.owned_bytes() == 0U);
}

}  // namespace

int main() {
    TestExactCapacityAndGrowthCharges();
    TestPeakChargeIncludesBothArrays();
    TestProductionOrderKeyAndAliasedGrowth();
    TestStaleWrongOwnerAndConsumedPlans();
    TestSwapEraseAndStorageRelease();
    TestOverflowAndSmallCapacity();
    TestAllocationFailureRollbackAndRetry();
    std::cout << "event exact list tests passed\n";
    return 0;
}
