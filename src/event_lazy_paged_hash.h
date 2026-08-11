#ifndef L2FLOW_EVENT_LAZY_PAGED_HASH_H_
#define L2FLOW_EVENT_LAZY_PAGED_HASH_H_

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace l2flow::event::internal {

// A single-owner fixed-bucket hash table. The directory is allocated once,
// while groups of 128 bucket heads and individual nodes are allocated lazily.
// Preparations are invalidated by a successful insert or erase; their key
// references must remain alive and unmodified until CommitInsert returns.
template <typename Key,
          typename Value,
          typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class LazyPagedHashMap final {
private:
    struct Node final {
        template <typename... ValueArgs>
        explicit Node(const Key& source_key, ValueArgs&&... value_args)
            : key(source_key),
              value(std::forward<ValueArgs>(value_args)...) {}

        Node* next = nullptr;
        Key key;
        Value value;
    };

    static constexpr std::size_t kHeadsPerPage = 128U;

    struct Page final {
        std::array<Node*, kHeadsPerPage> heads{};
        std::size_t live_nodes = 0U;
    };

    struct Layout final {
        std::size_t bucket_count = 0U;
        std::size_t page_count = 0U;
        std::size_t directory_owned_bytes = 0U;
    };

    static_assert(std::has_single_bit(kHeadsPerPage));
    static_assert(std::is_nothrow_destructible_v<Key>);
    static_assert(std::is_nothrow_destructible_v<Value>);

    [[nodiscard]] static constexpr std::size_t AllocationAlignment(
        std::size_t object_alignment) noexcept {
        return std::max(object_alignment, alignof(std::max_align_t));
    }

    [[nodiscard]] static constexpr std::size_t FixedAllocationOwnedBytes(
        std::size_t payload_bytes,
        std::size_t object_alignment) noexcept {
        const std::size_t alignment = AllocationAlignment(object_alignment);
        const std::size_t rounded =
            ((payload_bytes + alignment - 1U) / alignment) * alignment;
        return rounded + 2U * alignment;
    }

    [[nodiscard]] static std::size_t CheckedAdd(std::size_t left,
                                                std::size_t right) {
        if (left > std::numeric_limits<std::size_t>::max() - right) {
            throw std::length_error("lazy paged hash byte size overflow");
        }
        return left + right;
    }

    [[nodiscard]] static std::size_t CheckedMultiply(std::size_t left,
                                                     std::size_t right) {
        if (left != 0U &&
            right > std::numeric_limits<std::size_t>::max() / left) {
            throw std::length_error("lazy paged hash layout overflow");
        }
        return left * right;
    }

    [[nodiscard]] static std::size_t CheckedAllocationOwnedBytes(
        std::size_t payload_bytes,
        std::size_t object_alignment) {
        const std::size_t alignment = AllocationAlignment(object_alignment);
        const std::size_t rounding = alignment - 1U;
        const std::size_t with_rounding = CheckedAdd(payload_bytes, rounding);
        const std::size_t rounded =
            (with_rounding / alignment) * alignment;
        return CheckedAdd(rounded, CheckedMultiply(2U, alignment));
    }

    [[nodiscard]] static Layout MakeLayout(std::size_t maximum_entries) {
        if (maximum_entries == 0U) {
            throw std::invalid_argument(
                "lazy paged hash maximum_entries must be non-zero");
        }
        constexpr int kDigits = std::numeric_limits<std::size_t>::digits;
        constexpr std::size_t kLargestPowerOfTwo =
            std::size_t{1U} << (kDigits - 1);
        if (maximum_entries > kLargestPowerOfTwo) {
            throw std::length_error(
                "lazy paged hash bucket count is not representable");
        }

        const std::size_t bucket_count = std::bit_ceil(maximum_entries);
        const std::size_t whole_pages = bucket_count / kHeadsPerPage;
        const std::size_t partial_page =
            bucket_count % kHeadsPerPage == 0U ? 0U : 1U;
        const std::size_t page_count = CheckedAdd(whole_pages, partial_page);
        if (page_count == 0U) {
            throw std::length_error("lazy paged hash has no directory pages");
        }
        const std::size_t directory_payload =
            CheckedMultiply(page_count, sizeof(Page*));
        return Layout{
            bucket_count,
            page_count,
            CheckedAllocationOwnedBytes(directory_payload, alignof(Page*))};
    }

public:
    using key_type = Key;
    using mapped_type = Value;
    using size_type = std::size_t;

    // Logical hard-cap charges include alignment rounding and two additional
    // alignment units per allocation. They deliberately exceed object size;
    // they are not a claim about allocator RSS release.
    static constexpr size_type kPageOwnedBytes =
        FixedAllocationOwnedBytes(sizeof(Page), alignof(Page));
    static constexpr size_type kNodeOwnedBytes =
        FixedAllocationOwnedBytes(sizeof(Node), alignof(Node));

    class PreparedInsert final {
    public:
        PreparedInsert(const PreparedInsert&) = delete;
        PreparedInsert& operator=(const PreparedInsert&) = delete;
        PreparedInsert(PreparedInsert&& other) noexcept {
            MoveFrom(&other);
        }

        PreparedInsert& operator=(PreparedInsert&& other) noexcept {
            if (this != &other) {
                MoveFrom(&other);
            }
            return *this;
        }

        [[nodiscard]] mapped_type* existing() const noexcept {
            return existing_ == nullptr ? nullptr : &existing_->value;
        }

        [[nodiscard]] bool is_duplicate() const noexcept {
            return existing_ != nullptr;
        }

        [[nodiscard]] size_type owned_byte_delta() const noexcept {
            return owned_byte_delta_;
        }

    private:
        friend class LazyPagedHashMap;

        PreparedInsert(LazyPagedHashMap* owner,
                       const key_type* key,
                       size_type bucket_index,
                       size_type mutation_epoch,
                       Node* existing,
                       size_type owned_byte_delta) noexcept
            : owner_(owner),
              key_(key),
              bucket_index_(bucket_index),
              mutation_epoch_(mutation_epoch),
              existing_(existing),
              owned_byte_delta_(owned_byte_delta) {}

        void MoveFrom(PreparedInsert* other) noexcept {
            owner_ = other->owner_;
            key_ = other->key_;
            bucket_index_ = other->bucket_index_;
            mutation_epoch_ = other->mutation_epoch_;
            existing_ = other->existing_;
            owned_byte_delta_ = other->owned_byte_delta_;
            other->owner_ = nullptr;
            other->key_ = nullptr;
            other->existing_ = nullptr;
            other->owned_byte_delta_ = 0U;
        }

        LazyPagedHashMap* owner_ = nullptr;
        const key_type* key_ = nullptr;
        size_type bucket_index_ = 0U;
        size_type mutation_epoch_ = 0U;
        Node* existing_ = nullptr;
        size_type owned_byte_delta_ = 0U;
    };

    struct InsertResult final {
        mapped_type* value = nullptr;
        bool inserted = false;
        size_type owned_byte_delta = 0U;
    };

    struct EraseResult final {
        bool erased = false;
        size_type released_owned_bytes = 0U;
    };

    [[nodiscard]] static size_type RequiredDirectoryOwnedBytes(
        size_type maximum_entries) {
        return MakeLayout(maximum_entries).directory_owned_bytes;
    }

    explicit LazyPagedHashMap(size_type maximum_entries,
                              Hash hash = Hash{},
                              KeyEqual equal = KeyEqual{})
        : LazyPagedHashMap(
              MakeLayout(maximum_entries), maximum_entries,
              std::move(hash), std::move(equal)) {}

    ~LazyPagedHashMap() noexcept {
        for (size_type page_index = 0U; page_index < page_count_;
             ++page_index) {
            Page* const page = directory_[page_index];
            if (page == nullptr) {
                continue;
            }
            for (Node* head : page->heads) {
                while (head != nullptr) {
                    Node* const next = head->next;
                    delete head;
                    head = next;
                }
            }
            delete page;
        }
    }

    LazyPagedHashMap(const LazyPagedHashMap&) = delete;
    LazyPagedHashMap& operator=(const LazyPagedHashMap&) = delete;
    LazyPagedHashMap(LazyPagedHashMap&&) = delete;
    LazyPagedHashMap& operator=(LazyPagedHashMap&&) = delete;

    [[nodiscard]] mapped_type* Find(const key_type& key) {
        Node* const node = FindNode(key);
        return node == nullptr ? nullptr : &node->value;
    }

    [[nodiscard]] const mapped_type* Find(const key_type& key) const {
        const Node* const node = FindNode(key);
        return node == nullptr ? nullptr : &node->value;
    }

    [[nodiscard]] PreparedInsert PrepareInsert(const key_type& key) {
        const size_type bucket_index = BucketIndex(key);
        Node* const existing = FindNodeInBucket(bucket_index, key);
        if (existing != nullptr) {
            return PreparedInsert(
                this, &key, bucket_index, mutation_epoch_, existing, 0U);
        }
        if (size_ >= maximum_entries_ ||
            size_ == std::numeric_limits<size_type>::max()) {
            throw std::length_error("lazy paged hash capacity exceeded");
        }

        const Page* const page = directory_[PageIndex(bucket_index)];
        const size_type delta = page == nullptr
            ? CheckedAdd(kPageOwnedBytes, kNodeOwnedBytes)
            : kNodeOwnedBytes;
        static_cast<void>(CheckedAdd(owned_bytes_, delta));
        return PreparedInsert(
            this, &key, bucket_index, mutation_epoch_, nullptr, delta);
    }

    PreparedInsert PrepareInsert(key_type&&) = delete;
    PreparedInsert PrepareInsert(const key_type&&) = delete;

    template <typename... ValueArgs>
    [[nodiscard]] InsertResult CommitInsert(
        PreparedInsert&& prepared,
        ValueArgs&&... value_args) {
        ValidatePreparation(prepared);
        if (prepared.existing_ != nullptr) {
            mapped_type* const existing = &prepared.existing_->value;
            ConsumePreparation(&prepared);
            return InsertResult{existing, false, 0U};
        }

        if (size_ >= maximum_entries_ ||
            size_ == std::numeric_limits<size_type>::max()) {
            throw std::length_error("lazy paged hash capacity exceeded");
        }
        if (mutation_epoch_ == std::numeric_limits<size_type>::max()) {
            throw std::length_error("lazy paged hash mutation overflow");
        }

        const size_type page_index = PageIndex(prepared.bucket_index_);
        const size_type head_index = HeadIndex(prepared.bucket_index_);
        Page* const existing_page = directory_[page_index];
        const bool needs_page = existing_page == nullptr;
        const size_type actual_delta = needs_page
            ? CheckedAdd(kPageOwnedBytes, kNodeOwnedBytes)
            : kNodeOwnedBytes;
        if (actual_delta != prepared.owned_byte_delta_) {
            throw std::logic_error(
                "lazy paged hash preparation no longer matches layout");
        }
        const size_type next_size = CheckedAdd(size_, 1U);
        const size_type next_owned_bytes =
            CheckedAdd(owned_bytes_, actual_delta);
        const size_type next_epoch = CheckedAdd(mutation_epoch_, 1U);
        const size_type next_page_live_nodes = needs_page
            ? 1U
            : CheckedAdd(existing_page->live_nodes, 1U);

        std::unique_ptr<Page> pending_page;
        if (needs_page) {
            pending_page = std::make_unique<Page>();
        }
        std::unique_ptr<Node> pending_node = std::make_unique<Node>(
            *prepared.key_, std::forward<ValueArgs>(value_args)...);

        Page* const target_page = needs_page
            ? pending_page.get()
            : existing_page;
        Node* const inserted_node = pending_node.get();
        inserted_node->next = target_page->heads[head_index];
        target_page->heads[head_index] = inserted_node;
        target_page->live_nodes = next_page_live_nodes;
        if (needs_page) {
            directory_[page_index] = pending_page.release();
            ++allocated_page_count_;
        }
        size_ = next_size;
        owned_bytes_ = next_owned_bytes;
        mutation_epoch_ = next_epoch;
        static_cast<void>(pending_node.release());
        ConsumePreparation(&prepared);
        return InsertResult{&inserted_node->value, true, actual_delta};
    }

    [[nodiscard]] EraseResult Erase(const key_type& key) {
        const size_type bucket_index = BucketIndex(key);
        const size_type page_index = PageIndex(bucket_index);
        Page* const page = directory_[page_index];
        if (page == nullptr) {
            return EraseResult{};
        }

        Node** link = &page->heads[HeadIndex(bucket_index)];
        while (*link != nullptr &&
               !std::invoke(equal_, (*link)->key, key)) {
            link = &(*link)->next;
        }
        if (*link == nullptr) {
            return EraseResult{};
        }
        if (mutation_epoch_ == std::numeric_limits<size_type>::max()) {
            throw std::length_error("lazy paged hash mutation overflow");
        }

        const bool releases_page = page->live_nodes == 1U;
        const size_type released = releases_page
            ? CheckedAdd(kNodeOwnedBytes, kPageOwnedBytes)
            : kNodeOwnedBytes;
        if (size_ == 0U || page->live_nodes == 0U ||
            owned_bytes_ < released) {
            throw std::logic_error("lazy paged hash accounting corrupted");
        }
        const size_type next_epoch = CheckedAdd(mutation_epoch_, 1U);

        Node* const erased_node = *link;
        *link = erased_node->next;
        --page->live_nodes;
        --size_;
        owned_bytes_ -= released;
        mutation_epoch_ = next_epoch;
        delete erased_node;
        if (releases_page) {
            directory_[page_index] = nullptr;
            --allocated_page_count_;
            delete page;
        }
        return EraseResult{true, released};
    }

    [[nodiscard]] size_type size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0U; }
    [[nodiscard]] size_type maximum_entries() const noexcept {
        return maximum_entries_;
    }
    [[nodiscard]] size_type bucket_count() const noexcept {
        return bucket_count_;
    }
    [[nodiscard]] size_type directory_page_count() const noexcept {
        return page_count_;
    }
    [[nodiscard]] size_type allocated_page_count() const noexcept {
        return allocated_page_count_;
    }
    [[nodiscard]] size_type directory_owned_bytes() const noexcept {
        return directory_owned_bytes_;
    }
    [[nodiscard]] size_type owned_bytes() const noexcept {
        return owned_bytes_;
    }

private:
    LazyPagedHashMap(Layout layout,
                     size_type maximum_entries,
                     Hash hash,
                     KeyEqual equal)
        : hash_(std::move(hash)),
          equal_(std::move(equal)),
          directory_(std::make_unique<Page*[]>(layout.page_count)),
          maximum_entries_(maximum_entries),
          bucket_count_(layout.bucket_count),
          bucket_mask_(layout.bucket_count - 1U),
          page_count_(layout.page_count),
          directory_owned_bytes_(layout.directory_owned_bytes),
          owned_bytes_(layout.directory_owned_bytes) {}

    [[nodiscard]] size_type BucketIndex(const key_type& key) const {
        return static_cast<size_type>(std::invoke(hash_, key)) & bucket_mask_;
    }

    [[nodiscard]] static constexpr size_type PageIndex(
        size_type bucket_index) noexcept {
        return bucket_index / kHeadsPerPage;
    }

    [[nodiscard]] static constexpr size_type HeadIndex(
        size_type bucket_index) noexcept {
        return bucket_index & (kHeadsPerPage - 1U);
    }

    [[nodiscard]] Node* FindNodeInBucket(size_type bucket_index,
                                         const key_type& key) {
        Page* const page = directory_[PageIndex(bucket_index)];
        if (page == nullptr) {
            return nullptr;
        }
        Node* node = page->heads[HeadIndex(bucket_index)];
        while (node != nullptr &&
               !std::invoke(equal_, node->key, key)) {
            node = node->next;
        }
        return node;
    }

    [[nodiscard]] const Node* FindNodeInBucket(size_type bucket_index,
                                               const key_type& key) const {
        const Page* const page = directory_[PageIndex(bucket_index)];
        if (page == nullptr) {
            return nullptr;
        }
        const Node* node = page->heads[HeadIndex(bucket_index)];
        while (node != nullptr &&
               !std::invoke(equal_, node->key, key)) {
            node = node->next;
        }
        return node;
    }

    [[nodiscard]] Node* FindNode(const key_type& key) {
        const size_type bucket_index = BucketIndex(key);
        return FindNodeInBucket(bucket_index, key);
    }

    [[nodiscard]] const Node* FindNode(const key_type& key) const {
        const size_type bucket_index = BucketIndex(key);
        return FindNodeInBucket(bucket_index, key);
    }

    void ValidatePreparation(const PreparedInsert& prepared) const {
        if (prepared.owner_ != this || prepared.key_ == nullptr) {
            throw std::invalid_argument(
                "lazy paged hash preparation does not belong to this table");
        }
        if (prepared.mutation_epoch_ != mutation_epoch_) {
            throw std::logic_error("lazy paged hash preparation is stale");
        }
    }

    static void ConsumePreparation(PreparedInsert* prepared) noexcept {
        prepared->owner_ = nullptr;
        prepared->key_ = nullptr;
        prepared->existing_ = nullptr;
        prepared->owned_byte_delta_ = 0U;
    }

    [[no_unique_address]] Hash hash_;
    [[no_unique_address]] KeyEqual equal_;
    std::unique_ptr<Page*[]> directory_;
    const size_type maximum_entries_;
    const size_type bucket_count_;
    const size_type bucket_mask_;
    const size_type page_count_;
    const size_type directory_owned_bytes_;
    size_type size_ = 0U;
    size_type allocated_page_count_ = 0U;
    size_type owned_bytes_ = 0U;
    size_type mutation_epoch_ = 0U;
};

}  // namespace l2flow::event::internal

#endif  // L2FLOW_EVENT_LAZY_PAGED_HASH_H_
