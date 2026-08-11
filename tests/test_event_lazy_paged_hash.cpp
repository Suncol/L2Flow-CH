#include "event_lazy_paged_hash.h"

#include <array>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using l2flow::event::internal::LazyPagedHashMap;

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

struct IdentityHash final {
    [[nodiscard]] std::size_t operator()(int value) const noexcept {
        return static_cast<std::size_t>(value);
    }
};

struct ConstantHash final {
    [[nodiscard]] std::size_t operator()(int) const noexcept { return 0U; }
};

template <typename Map, typename... Args>
typename Map::InsertResult Insert(Map* map,
                                  const typename Map::key_type& key,
                                  Args&&... args) {
    auto prepared = map->PrepareInsert(key);
    return map->CommitInsert(
        std::move(prepared), std::forward<Args>(args)...);
}

void TestLazyPagesAndOwnedBytes() {
    using Map = LazyPagedHashMap<int, std::string, IdentityHash>;
    Map map(129U);
    CHECK(map.maximum_entries() == 129U);
    CHECK(map.bucket_count() == 256U);
    CHECK(map.directory_page_count() == 2U);
    CHECK(Map::RequiredDirectoryOwnedBytes(129U) ==
          map.directory_owned_bytes());
    CHECK(map.empty());
    CHECK(map.allocated_page_count() == 0U);
    CHECK(map.owned_bytes() == map.directory_owned_bytes());

    const int first_key = 1;
    auto first = map.PrepareInsert(first_key);
    CHECK(!first.is_duplicate());
    CHECK(first.existing() == nullptr);
    CHECK(first.owned_byte_delta() ==
          Map::kPageOwnedBytes + Map::kNodeOwnedBytes);
    const std::size_t initial_owned = map.owned_bytes();
    const auto first_result =
        map.CommitInsert(std::move(first), "first");
    CHECK(first_result.inserted);
    CHECK(first_result.owned_byte_delta ==
          Map::kPageOwnedBytes + Map::kNodeOwnedBytes);
    CHECK(*first_result.value == "first");
    CHECK(map.allocated_page_count() == 1U);
    CHECK(map.owned_bytes() == initial_owned +
          Map::kPageOwnedBytes + Map::kNodeOwnedBytes);

    const int same_page_key = 127;
    auto same_page = map.PrepareInsert(same_page_key);
    CHECK(same_page.owned_byte_delta() == Map::kNodeOwnedBytes);
    const auto same_page_result =
        map.CommitInsert(std::move(same_page), "same-page");
    CHECK(same_page_result.inserted);
    CHECK(map.allocated_page_count() == 1U);

    const int next_page_key = 128;
    auto next_page = map.PrepareInsert(next_page_key);
    CHECK(next_page.owned_byte_delta() ==
          Map::kPageOwnedBytes + Map::kNodeOwnedBytes);
    const auto next_page_result =
        map.CommitInsert(std::move(next_page), "next-page");
    CHECK(next_page_result.inserted);
    CHECK(map.allocated_page_count() == 2U);
    CHECK(map.size() == 3U);
}

void TestCollisionErasePositionsAndPageRelease() {
    using Map = LazyPagedHashMap<int, int, ConstantHash>;
    Map map(8U);
    for (int key = 1; key <= 4; ++key) {
        const auto inserted = Insert(&map, key, key * 10);
        CHECK(inserted.inserted);
    }
    CHECK(map.size() == 4U);
    CHECK(map.allocated_page_count() == 1U);

    const auto head = map.Erase(4);
    CHECK(head.erased);
    CHECK(head.released_owned_bytes == Map::kNodeOwnedBytes);
    CHECK(map.Find(4) == nullptr);
    CHECK(map.Find(3) != nullptr);

    const auto middle = map.Erase(2);
    CHECK(middle.erased);
    CHECK(middle.released_owned_bytes == Map::kNodeOwnedBytes);
    CHECK(map.Find(2) == nullptr);
    CHECK(map.Find(1) != nullptr);

    const auto tail = map.Erase(1);
    CHECK(tail.erased);
    CHECK(tail.released_owned_bytes == Map::kNodeOwnedBytes);
    CHECK(map.Find(1) == nullptr);
    CHECK(map.allocated_page_count() == 1U);

    const auto last = map.Erase(3);
    CHECK(last.erased);
    CHECK(last.released_owned_bytes ==
          Map::kNodeOwnedBytes + Map::kPageOwnedBytes);
    CHECK(map.empty());
    CHECK(map.allocated_page_count() == 0U);
    CHECK(map.owned_bytes() == map.directory_owned_bytes());

    const auto absent = map.Erase(99);
    CHECK(!absent.erased);
    CHECK(absent.released_owned_bytes == 0U);
}

struct StablePayload final {
    explicit StablePayload(int source) : value(source) {
        padding.fill(source);
    }

    int value = 0;
    std::array<int, 8U> padding{};
};

void TestPointerStability() {
    using Map = LazyPagedHashMap<int, StablePayload, IdentityHash>;
    Map map(512U);
    const int anchor_key = 7;
    const auto anchor_insert = Insert(&map, anchor_key, 700);
    CHECK(anchor_insert.inserted);
    StablePayload* const anchor = anchor_insert.value;

    for (int key = 0; key < 300; ++key) {
        if (key == anchor_key) {
            continue;
        }
        const auto inserted = Insert(&map, key, key);
        CHECK(inserted.inserted);
        CHECK(map.Find(anchor_key) == anchor);
        CHECK(anchor->value == 700);
    }

    for (int key = 0; key < 150; ++key) {
        if (key == anchor_key) {
            continue;
        }
        const auto erased = map.Erase(key);
        CHECK(erased.erased);
        CHECK(map.Find(anchor_key) == anchor);
    }
}

struct ThrowingValue final {
    explicit ThrowingValue(int source) : value(source) {
        if (throw_on_construct) {
            throw std::runtime_error("requested value construction failure");
        }
    }

    static bool throw_on_construct;
    int value = 0;
};

bool ThrowingValue::throw_on_construct = false;

void TestCapacityAndDuplicateZeroGrowth() {
    using Map = LazyPagedHashMap<int, ThrowingValue, IdentityHash>;
    Map map(2U);
    const int first_key = 0;
    const int second_key = 1;
    const int overflow_key = 2;
    CHECK(Insert(&map, first_key, 10).inserted);
    CHECK(Insert(&map, second_key, 20).inserted);
    CHECK(map.size() == 2U);
    CHECK(ThrowsAs<std::length_error>([&map, &overflow_key] {
        static_cast<void>(map.PrepareInsert(overflow_key));
    }));

    const std::size_t before_duplicate = map.owned_bytes();
    auto duplicate = map.PrepareInsert(first_key);
    CHECK(duplicate.is_duplicate());
    CHECK(duplicate.existing() != nullptr);
    CHECK(duplicate.existing()->value == 10);
    CHECK(duplicate.owned_byte_delta() == 0U);
    ThrowingValue::throw_on_construct = true;
    const auto duplicate_result =
        map.CommitInsert(std::move(duplicate), 999);
    ThrowingValue::throw_on_construct = false;
    CHECK(!duplicate_result.inserted);
    CHECK(duplicate_result.owned_byte_delta == 0U);
    CHECK(duplicate_result.value->value == 10);
    CHECK(map.owned_bytes() == before_duplicate);
    CHECK(map.size() == 2U);

    CHECK(map.Erase(second_key).erased);
    CHECK(Insert(&map, overflow_key, 30).inserted);
    CHECK(map.Find(overflow_key)->value == 30);
}

void TestIndependentPageRelease() {
    using Map = LazyPagedHashMap<int, int, IdentityHash>;
    Map map(256U);
    const int first = 1;
    const int same_page = 2;
    const int other_page = 129;
    CHECK(Insert(&map, first, first).inserted);
    CHECK(Insert(&map, same_page, same_page).inserted);
    CHECK(Insert(&map, other_page, other_page).inserted);
    CHECK(map.allocated_page_count() == 2U);

    CHECK(map.Erase(first).released_owned_bytes == Map::kNodeOwnedBytes);
    const auto page_release = map.Erase(same_page);
    CHECK(page_release.released_owned_bytes ==
          Map::kNodeOwnedBytes + Map::kPageOwnedBytes);
    CHECK(map.allocated_page_count() == 1U);
    CHECK(map.Find(other_page) != nullptr);

    const auto final_release = map.Erase(other_page);
    CHECK(final_release.released_owned_bytes ==
          Map::kNodeOwnedBytes + Map::kPageOwnedBytes);
    CHECK(map.allocated_page_count() == 0U);
}

void TestInvalidAndOverflowingLayouts() {
    using Map = LazyPagedHashMap<int, int, IdentityHash>;
    CHECK(ThrowsAs<std::invalid_argument>([] {
        Map invalid(0U);
        static_cast<void>(invalid);
    }));
    CHECK(ThrowsAs<std::length_error>([] {
        Map overflow(std::numeric_limits<std::size_t>::max());
        static_cast<void>(overflow);
    }));
    constexpr int kDigits = std::numeric_limits<std::size_t>::digits;
    constexpr std::size_t kLargestPowerOfTwo =
        std::size_t{1U} << (kDigits - 1);
    CHECK(ThrowsAs<std::length_error>([] {
        Map overflow(kLargestPowerOfTwo + 1U);
        static_cast<void>(overflow);
    }));
}

struct ThrowingKey final {
    explicit ThrowingKey(int source) noexcept : value(source) {}
    ThrowingKey(const ThrowingKey& other) : value(other.value) {
        if (throw_on_copy) {
            throw std::runtime_error("requested key copy failure");
        }
    }
    ThrowingKey& operator=(const ThrowingKey&) = default;

    static bool throw_on_copy;
    int value = 0;
};

bool ThrowingKey::throw_on_copy = false;

struct ThrowingKeyHash final {
    [[nodiscard]] std::size_t operator()(const ThrowingKey& key) const {
        if (throw_on_hash) {
            throw std::runtime_error("requested hash failure");
        }
        return static_cast<std::size_t>(key.value);
    }

    static bool throw_on_hash;
};

bool ThrowingKeyHash::throw_on_hash = false;

struct ThrowingKeyEqual final {
    [[nodiscard]] bool operator()(const ThrowingKey& left,
                                  const ThrowingKey& right) const {
        if (throw_on_equal) {
            throw std::runtime_error("requested equality failure");
        }
        return left.value == right.value;
    }

    static bool throw_on_equal;
};

bool ThrowingKeyEqual::throw_on_equal = false;

void TestExceptionRollback() {
    using ValueMap = LazyPagedHashMap<int, ThrowingValue, IdentityHash>;
    ValueMap value_map(256U);
    const int first_key = 1;
    auto first = value_map.PrepareInsert(first_key);
    const std::size_t empty_owned = value_map.owned_bytes();
    ThrowingValue::throw_on_construct = true;
    CHECK(ThrowsAs<std::runtime_error>([&value_map, &first] {
        static_cast<void>(value_map.CommitInsert(std::move(first), 10));
    }));
    ThrowingValue::throw_on_construct = false;
    CHECK(value_map.empty());
    CHECK(value_map.allocated_page_count() == 0U);
    CHECK(value_map.owned_bytes() == empty_owned);
    CHECK(value_map.Find(first_key) == nullptr);

    const auto retry = value_map.CommitInsert(std::move(first), 10);
    CHECK(retry.inserted);
    CHECK(retry.value->value == 10);
    CHECK(value_map.size() == 1U);

    const int second_key = 2;
    auto second = value_map.PrepareInsert(second_key);
    const std::size_t one_node_owned = value_map.owned_bytes();
    ThrowingValue::throw_on_construct = true;
    CHECK(ThrowsAs<std::runtime_error>([&value_map, &second] {
        static_cast<void>(value_map.CommitInsert(std::move(second), 20));
    }));
    ThrowingValue::throw_on_construct = false;
    CHECK(value_map.size() == 1U);
    CHECK(value_map.allocated_page_count() == 1U);
    CHECK(value_map.owned_bytes() == one_node_owned);
    CHECK(value_map.Find(second_key) == nullptr);

    using KeyMap = LazyPagedHashMap<
        ThrowingKey, int, ThrowingKeyHash, ThrowingKeyEqual>;
    KeyMap key_map(8U);
    const ThrowingKey key(3);
    auto key_preparation = key_map.PrepareInsert(key);
    const std::size_t key_map_owned = key_map.owned_bytes();
    ThrowingKey::throw_on_copy = true;
    CHECK(ThrowsAs<std::runtime_error>([&key_map, &key_preparation] {
        static_cast<void>(
            key_map.CommitInsert(std::move(key_preparation), 30));
    }));
    ThrowingKey::throw_on_copy = false;
    CHECK(key_map.empty());
    CHECK(key_map.allocated_page_count() == 0U);
    CHECK(key_map.owned_bytes() == key_map_owned);
    CHECK(key_map.CommitInsert(std::move(key_preparation), 30).inserted);

    const ThrowingKey hash_key(4);
    const std::size_t before_hash_failure = key_map.owned_bytes();
    ThrowingKeyHash::throw_on_hash = true;
    CHECK(ThrowsAs<std::runtime_error>([&key_map, &hash_key] {
        static_cast<void>(key_map.PrepareInsert(hash_key));
    }));
    ThrowingKeyHash::throw_on_hash = false;
    CHECK(key_map.size() == 1U);
    CHECK(key_map.owned_bytes() == before_hash_failure);

    const ThrowingKey equal_key(11);
    ThrowingKeyEqual::throw_on_equal = true;
    CHECK(ThrowsAs<std::runtime_error>([&key_map, &equal_key] {
        static_cast<void>(key_map.PrepareInsert(equal_key));
    }));
    ThrowingKeyEqual::throw_on_equal = false;
    CHECK(key_map.size() == 1U);
    CHECK(key_map.owned_bytes() == before_hash_failure);
}

void TestPreparationOwnershipAndStaleness() {
    using Map = LazyPagedHashMap<int, int, IdentityHash>;
    Map first_map(8U);
    Map second_map(8U);
    const int first_key = 1;
    const int second_key = 2;
    auto wrong_owner = first_map.PrepareInsert(first_key);
    CHECK(ThrowsAs<std::invalid_argument>([&second_map, &wrong_owner] {
        static_cast<void>(
            second_map.CommitInsert(std::move(wrong_owner), 10));
    }));
    CHECK(first_map.empty());
    CHECK(second_map.empty());

    auto stale = first_map.PrepareInsert(first_key);
    CHECK(Insert(&first_map, second_key, 20).inserted);
    CHECK(ThrowsAs<std::logic_error>([&first_map, &stale] {
        static_cast<void>(first_map.CommitInsert(std::move(stale), 10));
    }));
    CHECK(first_map.size() == 1U);

    auto consumed = first_map.PrepareInsert(first_key);
    CHECK(first_map.CommitInsert(std::move(consumed), 10).inserted);
    CHECK(ThrowsAs<std::invalid_argument>([&first_map, &consumed] {
        static_cast<void>(
            first_map.CommitInsert(std::move(consumed), 100));
    }));
}

struct ModuloHash final {
    explicit ModuloHash(int source) noexcept : modulus(source) {}
    [[nodiscard]] std::size_t operator()(int value) const noexcept {
        return static_cast<std::size_t>(value % modulus);
    }
    int modulus = 1;
};

struct ModuloEqual final {
    explicit ModuloEqual(int source) noexcept : modulus(source) {}
    [[nodiscard]] bool operator()(int left, int right) const noexcept {
        return left % modulus == right % modulus;
    }
    int modulus = 1;
};

void TestStatefulHashAndEquality() {
    using Map = LazyPagedHashMap<int, std::string, ModuloHash, ModuloEqual>;
    Map map(16U, ModuloHash{10}, ModuloEqual{10});
    const int first_key = 1;
    const int equivalent_key = 11;
    CHECK(Insert(&map, first_key, "winner").inserted);
    auto duplicate = map.PrepareInsert(equivalent_key);
    CHECK(duplicate.is_duplicate());
    CHECK(duplicate.owned_byte_delta() == 0U);
    const auto result =
        map.CommitInsert(std::move(duplicate), "replacement");
    CHECK(!result.inserted);
    CHECK(*result.value == "winner");
    CHECK(map.size() == 1U);
    CHECK(*map.Find(equivalent_key) == "winner");
}

}  // namespace

int main() {
    TestLazyPagesAndOwnedBytes();
    TestCollisionErasePositionsAndPageRelease();
    TestPointerStability();
    TestCapacityAndDuplicateZeroGrowth();
    TestIndependentPageRelease();
    TestInvalidAndOverflowingLayouts();
    TestExceptionRollback();
    TestPreparationOwnershipAndStaleness();
    TestStatefulHashAndEquality();
    std::cout << "event lazy paged hash tests passed\n";
    return 0;
}
