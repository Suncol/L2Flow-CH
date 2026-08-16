#include "l2flow/clickhouse/raw_sink.h"

#include "l2flow/arrow/schemas.h"
#include "l2flow/ingest/engine.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <cerrno>
#include <sys/random.h>
#endif

namespace l2flow::clickhouse {
namespace {

using arrow_hot::BuiltRecordBatch;
using arrow_hot::SnapshotRecordBatchBuilder;
using arrow_hot::TickRecordBatchBuilder;
using ingest::CanonicalSnapshot;
using ingest::CanonicalTick;

constexpr std::size_t kMaximumHttpResponseBytes = 64U * 1'024U;

[[nodiscard]] bool IsZero(Identifier128 identifier) noexcept {
    return std::all_of(identifier.bytes.begin(), identifier.bytes.end(),
                       [](std::byte value) { return value == std::byte{0U}; });
}

[[nodiscard]] Identifier128 GenerateIdentifier() {
    Identifier128 identifier{};
#if defined(__linux__)
    std::byte* destination = identifier.bytes.data();
    std::size_t remaining = identifier.bytes.size();
    while (remaining != 0U) {
        const ssize_t count = ::getrandom(destination, remaining, 0U);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("getrandom failed while creating writer ID");
        }
        if (count == 0) {
            throw std::runtime_error(
                "getrandom returned zero bytes while creating writer ID");
        }
        const std::size_t produced = static_cast<std::size_t>(count);
        destination += produced;
        remaining -= produced;
    }
#else
    std::random_device random;
    for (std::byte& value : identifier.bytes) {
        value = static_cast<std::byte>(random() & 0xffU);
    }
#endif
    if (IsZero(identifier)) {
        identifier.bytes.back() = std::byte{1U};
    }
    return identifier;
}

[[nodiscard]] std::uint64_t SystemUtcNowNs() noexcept {
    const auto duration =
        std::chrono::system_clock::now().time_since_epoch();
    const auto value =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    return value > 0 ? static_cast<std::uint64_t>(value) : 0U;
}

[[nodiscard]] bool IsIdentifierName(std::string_view value) noexcept {
    if (value.empty() ||
        !((value.front() >= 'A' && value.front() <= 'Z') ||
          (value.front() >= 'a' && value.front() <= 'z') ||
          value.front() == '_')) {
        return false;
    }
    return std::all_of(
        value.begin() + 1, value.end(), [](char character) {
            return (character >= 'A' && character <= 'Z') ||
                   (character >= 'a' && character <= 'z') ||
                   (character >= '0' && character <= '9') ||
                   character == '_';
        });
}

[[nodiscard]] bool CheckedRoundUpPowerOfTwo(std::size_t requested,
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

class IndexSpscRing final {
public:
    explicit IndexSpscRing(std::size_t requested_capacity) {
        if (!CheckedRoundUpPowerOfTwo(requested_capacity, &capacity_)) {
            throw std::invalid_argument("invalid raw batch queue capacity");
        }
        mask_ = capacity_ - 1U;
        storage_.resize(capacity_);
    }

    IndexSpscRing(const IndexSpscRing&) = delete;
    IndexSpscRing& operator=(const IndexSpscRing&) = delete;

    [[nodiscard]] bool TryPush(std::uint32_t value) noexcept {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head - cached_tail_ >=
            static_cast<std::uint64_t>(capacity_)) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (head - cached_tail_ >=
                static_cast<std::uint64_t>(capacity_)) {
                return false;
            }
        }
        storage_[static_cast<std::size_t>(head) & mask_] = value;
        head_.store(head + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(std::uint32_t* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == cached_head_) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (tail == cached_head_) {
                return false;
            }
        }
        *output = storage_[static_cast<std::size_t>(tail) & mask_];
        tail_.store(tail + 1U, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return tail_.load(std::memory_order_acquire) ==
               head_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

private:
    std::size_t capacity_ = 0U;
    std::size_t mask_ = 0U;
    std::vector<std::uint32_t> storage_;
    alignas(64) std::atomic<std::uint64_t> head_{0U};
    std::uint64_t cached_tail_ = 0U;
    alignas(64) std::atomic<std::uint64_t> tail_{0U};
    std::uint64_t cached_head_ = 0U;
};

struct RawBatchMetadata final {
    std::uint32_t trade_date = 0U;
    std::uint64_t created_monotonic_ns = 0U;
    std::uint64_t minimum_ingress_sequence = 0U;
    std::uint64_t maximum_ingress_sequence = 0U;
    std::size_t row_count = 0U;
    std::size_t byte_count = 0U;
};

template <typename Record>
struct RecordStorageDeleter final {
    void operator()(Record* pointer) const noexcept {
        ::operator delete[](pointer, std::align_val_t{alignof(Record)});
    }
};

template <typename Record>
using RecordStorage = std::unique_ptr<Record, RecordStorageDeleter<Record>>;

template <typename Record>
[[nodiscard]] RecordStorage<Record> AllocateRecordStorage(std::size_t rows) {
    static_assert(std::is_trivially_copyable_v<Record>);
    static_assert(std::is_trivially_destructible_v<Record>);
    if (rows > std::numeric_limits<std::size_t>::max() / sizeof(Record)) {
        throw std::bad_array_new_length();
    }
    void* const memory = ::operator new[](
        rows * sizeof(Record), std::align_val_t{alignof(Record)});
    return RecordStorage<Record>(static_cast<Record*>(memory));
}

template <typename Record>
struct RawCanonicalBatch final {
    RawBatchMetadata metadata{};
    RecordStorage<Record> rows;
};

enum class LaneAppendCode : std::uint8_t {
    kOk,
    kInvalidRecord,
    kQueueFull,
    kInvariantFailure,
};

template <typename Record>
class RawBatchLane final {
public:
    RawBatchLane(std::size_t rows_per_batch,
                 std::size_t bytes_per_batch,
                 std::uint64_t maximum_delay_ns,
                 std::size_t queue_batches)
        : rows_per_batch_(rows_per_batch),
          bytes_per_batch_(bytes_per_batch),
          maximum_delay_ns_(maximum_delay_ns),
          ready_(queue_batches),
          recycled_(ready_.capacity() + 1U) {
        const std::size_t pool_size = ready_.capacity() + 1U;
        if (pool_size > static_cast<std::size_t>(
                            std::numeric_limits<std::uint32_t>::max())) {
            throw std::invalid_argument("raw batch pool is too large");
        }
        slots_.reserve(pool_size);
        free_.reserve(pool_size);
        for (std::size_t index = 0U; index < pool_size; ++index) {
            RawCanonicalBatch<Record> slot{};
            slot.rows = AllocateRecordStorage<Record>(rows_per_batch_);
            slots_.push_back(std::move(slot));
            free_.push_back(static_cast<std::uint32_t>(index));
        }
        active_index_ = free_.back();
        free_.pop_back();
    }

    RawBatchLane(const RawBatchLane&) = delete;
    RawBatchLane& operator=(const RawBatchLane&) = delete;

    template <typename Published>
    [[nodiscard]] LaneAppendCode Append(const Record& record,
                                        Published&& published) noexcept {
        RawCanonicalBatch<Record>& active = slots_[active_index_];
        if (active.metadata.row_count != 0U &&
            active.metadata.trade_date != record.common.trade_date) {
            return LaneAppendCode::kInvalidRecord;
        }
        if (active.metadata.row_count >= rows_per_batch_) {
            const LaneAppendCode code = Publish(published);
            if (code != LaneAppendCode::kOk) {
                return code;
            }
        }

        RawCanonicalBatch<Record>& destination = slots_[active_index_];
        const std::size_t row = destination.metadata.row_count;
        if (row >= rows_per_batch_) {
            return LaneAppendCode::kInvariantFailure;
        }
        std::memcpy(static_cast<void*>(destination.rows.get() + row),
                    static_cast<const void*>(std::addressof(record)),
                    sizeof(Record));
        if (row == 0U) {
            destination.metadata.trade_date = record.common.trade_date;
            destination.metadata.created_monotonic_ns =
                record.common.receive_monotonic_ns;
            destination.metadata.minimum_ingress_sequence =
                record.common.ingress_sequence;
            destination.metadata.maximum_ingress_sequence =
                record.common.ingress_sequence;
            active_batch_.store(true, std::memory_order_release);
        } else {
            destination.metadata.minimum_ingress_sequence = std::min(
                destination.metadata.minimum_ingress_sequence,
                record.common.ingress_sequence);
            destination.metadata.maximum_ingress_sequence = std::max(
                destination.metadata.maximum_ingress_sequence,
                record.common.ingress_sequence);
        }
        ++destination.metadata.row_count;
        destination.metadata.byte_count += sizeof(Record);

        if (destination.metadata.row_count >= rows_per_batch_ ||
            destination.metadata.byte_count >= bytes_per_batch_) {
            return Publish(published);
        }
        return LaneAppendCode::kOk;
    }

    template <typename Published>
    [[nodiscard]] LaneAppendCode Poll(std::uint64_t monotonic_ns,
                                      Published&& published) noexcept {
        const RawBatchMetadata& metadata = slots_[active_index_].metadata;
        if (metadata.row_count == 0U ||
            metadata.created_monotonic_ns == 0U ||
            monotonic_ns < metadata.created_monotonic_ns ||
            monotonic_ns - metadata.created_monotonic_ns <
                maximum_delay_ns_) {
            return LaneAppendCode::kOk;
        }
        return Publish(published);
    }

    template <typename Published>
    [[nodiscard]] LaneAppendCode Flush(Published&& published) noexcept {
        return slots_[active_index_].metadata.row_count == 0U
                   ? LaneAppendCode::kOk
                   : Publish(published);
    }

    [[nodiscard]] bool TryPop(std::uint32_t* index,
                              RawCanonicalBatch<Record>** batch) noexcept {
        if (index == nullptr || batch == nullptr || !ready_.TryPop(index) ||
            *index >= slots_.size()) {
            return false;
        }
        *batch = &slots_[*index];
        return true;
    }

    [[nodiscard]] bool Recycle(std::uint32_t index) noexcept {
        if (index >= slots_.size()) {
            return false;
        }
        slots_[index].metadata = {};
        return recycled_.TryPush(index);
    }

    [[nodiscard]] bool empty() const noexcept { return ready_.empty(); }

    [[nodiscard]] bool has_active_rows() const noexcept {
        return active_batch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t preallocated_bytes() const noexcept {
        return static_cast<std::uint64_t>(slots_.size()) *
               static_cast<std::uint64_t>(rows_per_batch_) *
               static_cast<std::uint64_t>(sizeof(Record));
    }

private:
    void DrainRecycled() noexcept {
        std::uint32_t index = 0U;
        while (recycled_.TryPop(&index)) {
            free_.push_back(index);
        }
    }

    template <typename Published>
    [[nodiscard]] LaneAppendCode Publish(Published&& published) noexcept {
        DrainRecycled();
        if (free_.empty()) {
            return LaneAppendCode::kQueueFull;
        }
        const std::uint32_t next_active = free_.back();
        free_.pop_back();
        const std::size_t published_rows =
            slots_[active_index_].metadata.row_count;
        if (!ready_.TryPush(active_index_)) {
            free_.push_back(next_active);
            return LaneAppendCode::kInvariantFailure;
        }
        active_index_ = next_active;
        slots_[active_index_].metadata = {};
        active_batch_.store(false, std::memory_order_release);
        published(published_rows);
        return LaneAppendCode::kOk;
    }

    std::size_t rows_per_batch_ = 0U;
    std::size_t bytes_per_batch_ = 0U;
    std::uint64_t maximum_delay_ns_ = 0U;
    std::vector<RawCanonicalBatch<Record>> slots_;
    IndexSpscRing ready_;
    IndexSpscRing recycled_;
    std::vector<std::uint32_t> free_;
    std::uint32_t active_index_ = 0U;
    std::atomic<bool> active_batch_{false};
};

struct AtomicStats final {
    std::atomic<std::uint64_t> tick_rows_received{0U};
    std::atomic<std::uint64_t> snapshot_rows_received{0U};
    std::atomic<std::uint64_t> batches_queued{0U};
    std::atomic<std::uint64_t> batches_acked{0U};
    std::atomic<std::uint64_t> batches_released{0U};
    std::atomic<std::uint64_t> rows_acked{0U};
    std::atomic<std::uint64_t> retry_attempts{0U};
    std::atomic<std::uint64_t> unknown_outcomes{0U};
    std::atomic<std::uint64_t> bytes_sent{0U};
};

struct FieldMapping final {
    std::string_view source;
    std::string_view output;
};

constexpr std::array<FieldMapping, 21U> kCommonFields{{
    {"feed_session_epoch", "feed_session_epoch"},
    {"ingress_sequence", "ingress_sequence"},
    {"vendor_sequence_id", "vendor_sequence_id"},
    {"receive_monotonic_ns", "receive_monotonic_ns"},
    {"native_sequence", "native_sequence"},
    {"exchange_time_ns_from_midnight", "exchange_time_ns_from_midnight"},
    {"vendor_local_time_ns_from_midnight",
     "vendor_local_time_ns_from_midnight"},
    {"quality_flags", "quality_flags"},
    {"instrument_id", "instrument_id"},
    {"instrument_ordinal", "instrument_ordinal"},
    {"channel", "channel"},
    {"exchange_time_raw", "exchange_time_raw"},
    {"vendor_local_time_raw", "vendor_local_time_raw"},
    {"service_id", "service_id"},
    {"service_version", "service_version"},
    {"message_id", "message_id"},
    {"canonical_kind", "canonical_kind"},
    {"market", "market"},
    {"security_id_source", "security_id_source"},
    {"security_id", "security_id"},
    {"md_stream_id", "md_stream_id"},
}};

constexpr std::array<FieldMapping, 20U> kTickFields{{
    {"price_raw", "price_raw"},
    {"price_p6", "price_p6"},
    {"price_source_scale", "price_source_scale"},
    {"amount_raw", "amount_raw"},
    {"amount_p6", "amount_p6"},
    {"amount_source_scale", "amount_source_scale"},
    {"quantity_raw", "quantity_raw"},
    {"quantity_scale", "quantity_scale"},
    {"primary_order_id", "primary_order_id"},
    {"buy_order_id", "buy_order_id"},
    {"sell_order_id", "sell_order_id"},
    {"sh_add_matched_quantity_raw", "sh_add_matched_quantity_raw"},
    {"validity", "validity"},
    {"raw_type", "raw_type"},
    {"raw_side", "raw_side"},
    {"action", "action"},
    {"side", "side"},
    {"aggressor", "aggressor"},
    {"order_type", "order_type"},
    {"phase", "phase"},
}};

constexpr std::array<FieldMapping, 43U> kSnapshotFields{{
    {"previous_close_raw", "previous_close_raw"},
    {"previous_close_p6", "previous_close_p6"},
    {"previous_close_source_scale", "previous_close_source_scale"},
    {"open_raw", "open_raw"},
    {"open_p6", "open_p6"},
    {"open_source_scale", "open_source_scale"},
    {"high_raw", "high_raw"},
    {"high_p6", "high_p6"},
    {"high_source_scale", "high_source_scale"},
    {"low_raw", "low_raw"},
    {"low_p6", "low_p6"},
    {"low_source_scale", "low_source_scale"},
    {"last_raw", "last_raw"},
    {"last_p6", "last_p6"},
    {"last_source_scale", "last_source_scale"},
    {"close_raw", "close_raw"},
    {"close_p6", "close_p6"},
    {"close_source_scale", "close_source_scale"},
    {"turnover_raw", "turnover_raw"},
    {"turnover_p6", "turnover_p6"},
    {"turnover_source_scale", "turnover_source_scale"},
    {"volume_raw", "volume_raw"},
    {"volume_scale", "volume_scale"},
    {"total_bid_quantity_raw", "total_bid_quantity_raw"},
    {"total_bid_quantity_scale", "total_bid_quantity_scale"},
    {"total_ask_quantity_raw", "total_ask_quantity_raw"},
    {"total_ask_quantity_scale", "total_ask_quantity_scale"},
    {"weighted_average_bid_raw", "weighted_average_bid_raw"},
    {"weighted_average_bid_p6", "weighted_average_bid_p6"},
    {"weighted_average_bid_source_scale",
     "weighted_average_bid_source_scale"},
    {"weighted_average_ask_raw", "weighted_average_ask_raw"},
    {"weighted_average_ask_p6", "weighted_average_ask_p6"},
    {"weighted_average_ask_source_scale",
     "weighted_average_ask_source_scale"},
    {"trade_count", "trade_count"},
    {"image_status", "image_status"},
    {"instrument_status_code", "instrument_status_code"},
    {"trading_phase_code", "trading_phase_code"},
    {"source_bid_depth", "source_bid_depth"},
    {"source_ask_depth", "source_ask_depth"},
    {"retained_bid_depth", "retained_bid_depth"},
    {"retained_ask_depth", "retained_ask_depth"},
    {"bids", "bids"},
    {"asks", "asks"},
}};

struct InFlightMetadata final {
    RawTableId table = RawTableId::kRawTick;
    std::uint32_t schema_version = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t batch_sequence = 0U;
    Identifier128 batch_id{};
};

[[nodiscard]] bool TradeDateToDays(std::uint32_t value,
                                   std::int32_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::int64_t year = static_cast<std::int64_t>(value / 10'000U);
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1970 || year > 2200 || month == 0U || month > 12U ||
        day == 0U || day > 31U) {
        return false;
    }
    year -= month <= 2U ? 1 : 0;
    const std::int64_t era = year / 400;
    const std::uint32_t year_of_era = static_cast<std::uint32_t>(
        year - era * 400);
    const std::uint32_t adjusted_month =
        month > 2U ? month - 3U : month + 9U;
    const std::uint32_t day_of_year =
        (153U * adjusted_month + 2U) / 5U + day - 1U;
    const std::uint32_t day_of_era =
        year_of_era * 365U + year_of_era / 4U - year_of_era / 100U +
        day_of_year;
    const std::int64_t days = era * 146'097 +
                              static_cast<std::int64_t>(day_of_era) -
                              719'468;
    if (days < std::numeric_limits<std::int32_t>::min() ||
        days > std::numeric_limits<std::int32_t>::max()) {
        return false;
    }
    *output = static_cast<std::int32_t>(days);
    return true;
}

[[nodiscard]] arrow::Status AppendMappedFields(
    const arrow::RecordBatch& source,
    std::span<const FieldMapping> mappings,
    arrow::FieldVector* fields,
    arrow::ArrayVector* columns) {
    for (const FieldMapping& mapping : mappings) {
        const int index = source.schema()->GetFieldIndex(mapping.source);
        if (index < 0) {
            return arrow::Status::Invalid(
                "source Arrow batch is missing field ", mapping.source);
        }
        const std::shared_ptr<arrow::Field>& source_field =
            source.schema()->field(index);
        fields->push_back(arrow::field(
            std::string(mapping.output), source_field->type(),
            source_field->nullable()));
        columns->push_back(source.column(index));
    }
    return arrow::Status::OK();
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::Array>> BuildTradeDate(
    const arrow::RecordBatch& source) {
    const int index = source.schema()->GetFieldIndex("trade_date");
    if (index < 0 || source.column(index)->type_id() != arrow::Type::UINT32) {
        return arrow::Status::Invalid(
            "source Arrow batch has no UInt32 trade_date");
    }
    const auto values =
        std::static_pointer_cast<arrow::UInt32Array>(source.column(index));
    arrow::Date32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(source.num_rows()));
    for (std::int64_t row = 0; row < source.num_rows(); ++row) {
        std::int32_t days = 0;
        if (values->IsNull(row) ||
            !TradeDateToDays(values->Value(row), &days)) {
            return arrow::Status::Invalid("invalid raw trade_date");
        }
        ARROW_RETURN_NOT_OK(builder.Append(days));
    }
    return builder.Finish();
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::Array>> BuildCatalogMatch(
    const arrow::RecordBatch& source) {
    const int index = source.schema()->GetFieldIndex("quality_flags");
    if (index < 0 || source.column(index)->type_id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid(
            "source Arrow batch has no UInt64 quality_flags");
    }
    const auto values =
        std::static_pointer_cast<arrow::UInt64Array>(source.column(index));
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(source.num_rows()));
    for (std::int64_t row = 0; row < source.num_rows(); ++row) {
        const bool matched = !values->IsNull(row) &&
            (values->Value(row) &
             static_cast<std::uint64_t>(
                 ingest::kQualityInstrumentNotInCatalog)) == 0U;
        ARROW_RETURN_NOT_OK(builder.Append(matched));
    }
    return builder.Finish();
}

[[nodiscard]] arrow::Status AppendFixedIdentifier(
    arrow::FixedSizeBinaryBuilder* builder,
    Identifier128 identifier) {
    return builder->Append(
        reinterpret_cast<const std::uint8_t*>(identifier.bytes.data()));
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildPersistentBatch(const arrow::RecordBatch& source,
                     const InFlightMetadata& metadata,
                     Identifier128 source_instance_id,
                     Identifier128 writer_instance_id) {
    arrow::FieldVector fields;
    arrow::ArrayVector columns;
    fields.reserve(static_cast<std::size_t>(source.num_columns()) + 8U);
    columns.reserve(static_cast<std::size_t>(source.num_columns()) + 8U);

    constexpr std::size_t kLeadingCommonCount = 8U;
    ARROW_RETURN_NOT_OK(AppendMappedFields(
        source,
        std::span<const FieldMapping>(kCommonFields).first(
            kLeadingCommonCount),
        &fields, &columns));
    ARROW_ASSIGN_OR_RAISE(auto trade_date, BuildTradeDate(source));
    fields.push_back(arrow::field("trade_date", arrow::date32(), false));
    columns.push_back(std::move(trade_date));
    ARROW_RETURN_NOT_OK(AppendMappedFields(
        source,
        std::span<const FieldMapping>(kCommonFields).subspan(
            kLeadingCommonCount),
        &fields, &columns));
    if (metadata.table == RawTableId::kRawTick) {
        ARROW_RETURN_NOT_OK(AppendMappedFields(
            source, kTickFields, &fields, &columns));
    } else {
        ARROW_RETURN_NOT_OK(AppendMappedFields(
            source, kSnapshotFields, &fields, &columns));
    }

    ARROW_ASSIGN_OR_RAISE(auto catalog_match, BuildCatalogMatch(source));
    fields.push_back(arrow::field("catalog_match", arrow::boolean(), false));
    columns.push_back(std::move(catalog_match));

    const auto fixed_identifier_type = arrow::fixed_size_binary(16);
    arrow::FixedSizeBinaryBuilder source_ids(fixed_identifier_type);
    arrow::FixedSizeBinaryBuilder writer_ids(fixed_identifier_type);
    arrow::FixedSizeBinaryBuilder batch_ids(fixed_identifier_type);
    arrow::FixedSizeBinaryBuilder occurrence_ids(fixed_identifier_type);
    arrow::UInt64Builder batch_sequences;
    arrow::UInt32Builder row_indices;
    arrow::UInt32Builder schema_versions;
    const std::int64_t row_count = source.num_rows();
    ARROW_RETURN_NOT_OK(source_ids.Reserve(row_count));
    ARROW_RETURN_NOT_OK(writer_ids.Reserve(row_count));
    ARROW_RETURN_NOT_OK(batch_ids.Reserve(row_count));
    ARROW_RETURN_NOT_OK(occurrence_ids.Reserve(row_count));
    ARROW_RETURN_NOT_OK(batch_sequences.Reserve(row_count));
    ARROW_RETURN_NOT_OK(row_indices.Reserve(row_count));
    ARROW_RETURN_NOT_OK(schema_versions.Reserve(row_count));

    const int ingress_index =
        source.schema()->GetFieldIndex("ingress_sequence");
    const int kind_index = source.schema()->GetFieldIndex("canonical_kind");
    const int epoch_index =
        source.schema()->GetFieldIndex("feed_session_epoch");
    if (ingress_index < 0 || kind_index < 0 || epoch_index < 0 ||
        source.column(ingress_index)->type_id() != arrow::Type::UINT64 ||
        source.column(kind_index)->type_id() != arrow::Type::UINT8 ||
        source.column(epoch_index)->type_id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid(
            "raw source batch lacks occurrence identity columns");
    }
    const auto ingress = std::static_pointer_cast<arrow::UInt64Array>(
        source.column(ingress_index));
    const auto kinds = std::static_pointer_cast<arrow::UInt8Array>(
        source.column(kind_index));
    const auto epochs = std::static_pointer_cast<arrow::UInt64Array>(
        source.column(epoch_index));
    for (std::int64_t row = 0; row < row_count; ++row) {
        if (ingress->IsNull(row) || kinds->IsNull(row) ||
            epochs->IsNull(row) ||
            row > static_cast<std::int64_t>(
                      std::numeric_limits<std::uint32_t>::max())) {
            return arrow::Status::Invalid(
                "invalid raw occurrence identity input");
        }
        const auto kind = static_cast<ingest::CanonicalKind>(
            kinds->Value(row));
        if (kind > ingest::CanonicalKind::kShenzhenSnapshot) {
            return arrow::Status::Invalid(
                "invalid canonical kind in raw occurrence identity");
        }
        const Identifier128 occurrence = RawOccurrenceIdentifier(
            source_instance_id, epochs->Value(row),
            ingress->Value(row), kind);
        ARROW_RETURN_NOT_OK(
            AppendFixedIdentifier(&source_ids, source_instance_id));
        ARROW_RETURN_NOT_OK(
            AppendFixedIdentifier(&writer_ids, writer_instance_id));
        ARROW_RETURN_NOT_OK(
            AppendFixedIdentifier(&batch_ids, metadata.batch_id));
        ARROW_RETURN_NOT_OK(
            AppendFixedIdentifier(&occurrence_ids, occurrence));
        ARROW_RETURN_NOT_OK(
            batch_sequences.Append(metadata.batch_sequence));
        ARROW_RETURN_NOT_OK(
            row_indices.Append(static_cast<std::uint32_t>(row)));
        ARROW_RETURN_NOT_OK(
            schema_versions.Append(metadata.schema_version));
    }

    const auto append_finished = [&fields, &columns](
        std::string name,
        std::shared_ptr<arrow::DataType> type,
        arrow::Result<std::shared_ptr<arrow::Array>> result)
        -> arrow::Status {
        if (!result.ok()) {
            return result.status();
        }
        fields.push_back(arrow::field(std::move(name), std::move(type), false));
        columns.push_back(std::move(*result));
        return arrow::Status::OK();
    };
    ARROW_RETURN_NOT_OK(append_finished(
        "source_instance_id", fixed_identifier_type, source_ids.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "writer_instance_id", fixed_identifier_type, writer_ids.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "batch_id", fixed_identifier_type, batch_ids.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "batch_sequence", arrow::uint64(), batch_sequences.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "row_index", arrow::uint32(), row_indices.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "occurrence_id", fixed_identifier_type, occurrence_ids.Finish()));
    ARROW_RETURN_NOT_OK(append_finished(
        "schema_version", arrow::uint32(), schema_versions.Finish()));

    auto batch = arrow::RecordBatch::Make(
        arrow::schema(std::move(fields)), row_count, std::move(columns));
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
    return batch;
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeBatch(
    const arrow::RecordBatch& batch) {
    ARROW_ASSIGN_OR_RAISE(auto output,
                          arrow::io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          arrow::ipc::MakeStreamWriter(
                              output, batch.schema()));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(batch));
    ARROW_RETURN_NOT_OK(writer->Close());
    return output->Finish();
}

[[nodiscard]] std::string RawCommonDdl() {
    return R"SQL(
    feed_session_epoch UInt64,
    ingress_sequence UInt64,
    vendor_sequence_id UInt64,
    receive_monotonic_ns UInt64,
    native_sequence Nullable(UInt64),
    exchange_time_ns_from_midnight Nullable(UInt64),
    vendor_local_time_ns_from_midnight Nullable(UInt64),
    quality_flags UInt64,
    trade_date Date,
    instrument_id Nullable(UInt32),
    instrument_ordinal Nullable(UInt32),
    channel UInt32,
    exchange_time_raw UInt32,
    vendor_local_time_raw UInt32,
    service_id UInt8,
    service_version UInt16,
    message_id UInt16,
    canonical_kind UInt8,
    market UInt8,
    security_id_source String,
    security_id String,
    md_stream_id String,
)SQL";
}

[[nodiscard]] std::string ProvenanceDdl() {
    return R"SQL(
    catalog_match Bool,
    source_instance_id FixedString(16),
    writer_instance_id FixedString(16),
    batch_id FixedString(16),
    batch_sequence UInt64,
    row_index UInt32,
    occurrence_id FixedString(16),
    schema_version UInt32
)SQL";
}

[[nodiscard]] std::string RawTickDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
        R"SQL(.raw_tick
(
)SQL" + RawCommonDdl() + R"SQL(
    price_raw Nullable(Int64),
    price_p6 Nullable(Int64),
    price_source_scale UInt8,
    amount_raw Nullable(Int64),
    amount_p6 Nullable(Int64),
    amount_source_scale UInt8,
    quantity_raw Nullable(Int64),
    quantity_scale UInt8,
    primary_order_id Nullable(Int64),
    buy_order_id Nullable(Int64),
    sell_order_id Nullable(Int64),
    sh_add_matched_quantity_raw Nullable(Int64),
    validity UInt64,
    raw_type Int32,
    raw_side Int32,
    action UInt8,
    side UInt8,
    aggressor UInt8,
    order_type UInt8,
    phase UInt8,
)SQL" + ProvenanceDdl() + R"SQL(
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    exchange_time_ns_from_midnight,
    channel,
    native_sequence,
    source_instance_id,
    feed_session_epoch,
    ingress_sequence
)
SETTINGS
    allow_nullable_key = 1,
    non_replicated_deduplication_window = 10000
)SQL";
}

[[nodiscard]] std::string RawSnapshotDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
        R"SQL(.raw_snapshot
(
)SQL" + RawCommonDdl() + R"SQL(
    previous_close_raw Nullable(Int64),
    previous_close_p6 Nullable(Int64),
    previous_close_source_scale UInt8,
    open_raw Nullable(Int64),
    open_p6 Nullable(Int64),
    open_source_scale UInt8,
    high_raw Nullable(Int64),
    high_p6 Nullable(Int64),
    high_source_scale UInt8,
    low_raw Nullable(Int64),
    low_p6 Nullable(Int64),
    low_source_scale UInt8,
    last_raw Nullable(Int64),
    last_p6 Nullable(Int64),
    last_source_scale UInt8,
    close_raw Nullable(Int64),
    close_p6 Nullable(Int64),
    close_source_scale UInt8,
    turnover_raw Nullable(Int64),
    turnover_p6 Nullable(Int64),
    turnover_source_scale UInt8,
    volume_raw Nullable(Int64),
    volume_scale UInt8,
    total_bid_quantity_raw Nullable(Int64),
    total_bid_quantity_scale UInt8,
    total_ask_quantity_raw Nullable(Int64),
    total_ask_quantity_scale UInt8,
    weighted_average_bid_raw Nullable(Int64),
    weighted_average_bid_p6 Nullable(Int64),
    weighted_average_bid_source_scale UInt8,
    weighted_average_ask_raw Nullable(Int64),
    weighted_average_ask_p6 Nullable(Int64),
    weighted_average_ask_source_scale UInt8,
    trade_count Nullable(UInt64),
    image_status Nullable(Int32),
    instrument_status_code Nullable(String),
    trading_phase_code Nullable(String),
    source_bid_depth UInt32,
    source_ask_depth UInt32,
    retained_bid_depth UInt8,
    retained_ask_depth UInt8,
    bids Array(Tuple(
        price_raw Nullable(Int64),
        price_p6 Nullable(Int64),
        price_source_scale UInt8,
        quantity_raw Nullable(Int64),
        quantity_scale UInt8,
        source_order_count Nullable(UInt32))),
    asks Array(Tuple(
        price_raw Nullable(Int64),
        price_p6 Nullable(Int64),
        price_source_scale UInt8,
        quantity_raw Nullable(Int64),
        quantity_scale UInt8,
        source_order_count Nullable(UInt32))),
)SQL" + ProvenanceDdl() + R"SQL(
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    exchange_time_ns_from_midnight,
    source_instance_id,
    feed_session_epoch,
    ingress_sequence
)
SETTINGS
    allow_nullable_key = 1,
    non_replicated_deduplication_window = 10000
)SQL";
}

[[nodiscard]] std::string RawColumnProbe(
    std::string_view database,
    std::string_view table,
    std::span<const FieldMapping> table_fields) {
    std::string sql = "SELECT ";
    const auto append_name = [&sql](std::string_view name) {
        if (!sql.ends_with(' ')) {
            sql += ',';
        }
        sql += name;
    };
    for (const FieldMapping& mapping : kCommonFields) {
        append_name(mapping.output);
    }
    append_name("trade_date");
    for (const FieldMapping& mapping : table_fields) {
        append_name(mapping.output);
    }
    constexpr std::array<std::string_view, 8U> provenance{{
        "catalog_match",
        "source_instance_id",
        "writer_instance_id",
        "batch_id",
        "batch_sequence",
        "row_index",
        "occurrence_id",
        "schema_version",
    }};
    for (std::string_view name : provenance) {
        append_name(name);
    }
    sql += " FROM ";
    sql += database;
    sql += '.';
    sql += table;
    sql += " LIMIT 0 FORMAT Null";
    return sql;
}

[[nodiscard]] std::string RawTableContractProbe(std::string_view database) {
    return "SELECT throwIf(count() != 2 OR "
           "countIf(engine NOT IN ('MergeTree', 'ReplicatedMergeTree')) != 0 "
           "OR countIf(partition_key != 'trade_date') != 0 "
           "OR countIf(engine = 'MergeTree' AND "
           "toUInt64OrZero(extract(engine_full, "
           "'non_replicated_deduplication_window[[:space:]]*="
           "[[:space:]]*([0-9]+)')) < 10000) != 0 "
           "OR countIf(engine = 'ReplicatedMergeTree' AND "
           "toUInt64OrZero(extract(engine_full, "
           "'replicated_deduplication_window[[:space:]]*="
           "[[:space:]]*([0-9]+)')) < 10000) != 0, "
           "'raw tables must be MergeTree/ReplicatedMergeTree partitioned "
           "by trade_date with a deduplication window of at least 10000') "
           "FROM system.tables WHERE database = '" +
           std::string(database) +
           "' AND name IN ('raw_tick', 'raw_snapshot') FORMAT Null";
}

struct HttpResult final {
    CURLcode curl_code = CURLE_OK;
    long status_code = 0;
    std::string response;
    std::string transport_error;
};

size_t CaptureResponse(char* data,
                       size_t size,
                       size_t count,
                       void* context) noexcept {
    if (context == nullptr || data == nullptr ||
        (size != 0U && count >
             std::numeric_limits<std::size_t>::max() / size)) {
        return 0U;
    }
    const std::size_t bytes = size * count;
    auto* response = static_cast<std::string*>(context);
    if (bytes > kMaximumHttpResponseBytes -
                    std::min(response->size(), kMaximumHttpResponseBytes)) {
        return 0U;
    }
    try {
        response->append(data, bytes);
        return bytes;
    } catch (...) {
        return 0U;
    }
}

class CurlHeaders final {
public:
    ~CurlHeaders() {
        if (headers_ != nullptr) {
            ::curl_slist_free_all(headers_);
        }
    }

    [[nodiscard]] bool Append(const char* value) noexcept {
        curl_slist* const next = ::curl_slist_append(headers_, value);
        if (next == nullptr) {
            return false;
        }
        headers_ = next;
        return true;
    }

    [[nodiscard]] curl_slist* get() const noexcept { return headers_; }

private:
    curl_slist* headers_ = nullptr;
};

class HttpClient final {
public:
    explicit HttpClient(const RawClickHouseConfig& config)
        : config_(config), handle_(::curl_easy_init()) {
        if (handle_ == nullptr) {
            throw std::runtime_error("curl_easy_init failed");
        }
    }

    ~HttpClient() {
        if (handle_ != nullptr) {
            ::curl_easy_cleanup(handle_);
        }
    }

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;

    [[nodiscard]] HttpResult ExecuteSql(std::string_view sql) {
        return Perform(config_.endpoint + "/?wait_end_of_query=1",
                       std::span<const std::byte>(
                           reinterpret_cast<const std::byte*>(sql.data()),
                           sql.size()),
                       "Content-Type: text/plain");
    }

    [[nodiscard]] HttpResult Insert(
        const InFlightMetadata& metadata,
        std::string_view writer_id,
        std::span<const std::byte> payload) {
        const std::string table = metadata.table == RawTableId::kRawTick
            ? "raw_tick"
            : "raw_snapshot";
        const std::string query = "INSERT INTO " + config_.database + "." +
                                  table + " FORMAT ArrowStream";
        const std::string query_id = "l2flow/" + table + "/" +
                                     std::string(writer_id) + "/" +
                                     std::to_string(metadata.batch_sequence);
        const std::string token = "l2flow/" + table + "/" +
            std::to_string(metadata.trade_date) + "/" +
            IdentifierString(metadata.batch_id) + "/" +
            std::to_string(metadata.schema_version);
        const std::string url = config_.endpoint + "/?query=" +
            Escape(query) + "&query_id=" + Escape(query_id) +
            "&async_insert=0&wait_end_of_query=1&insert_deduplicate=1" +
            "&insert_deduplication_token=" + Escape(token) +
            "&insert_quorum=" + std::to_string(config_.insert_quorum) +
            "&insert_quorum_parallel=" +
            (config_.insert_quorum_parallel ? "1" : "0");
        return Perform(url, payload, "Content-Type: application/octet-stream");
    }

private:
    [[nodiscard]] std::string Escape(std::string_view value) {
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<int>::max())) {
            throw std::length_error("ClickHouse URL value is too large");
        }
        char* const escaped = ::curl_easy_escape(
            handle_, value.data(), static_cast<int>(value.size()));
        if (escaped == nullptr) {
            throw std::bad_alloc();
        }
        try {
            std::string result(escaped);
            ::curl_free(escaped);
            return result;
        } catch (...) {
            ::curl_free(escaped);
            throw;
        }
    }

    [[nodiscard]] HttpResult Perform(std::string_view url,
                                     std::span<const std::byte> payload,
                                     const char* content_type) {
        HttpResult result{};
        if (payload.size() > static_cast<std::size_t>(
                                 std::numeric_limits<curl_off_t>::max())) {
            result.curl_code = CURLE_FILESIZE_EXCEEDED;
            result.transport_error = "ClickHouse request payload is too large";
            return result;
        }
        std::array<char, CURL_ERROR_SIZE> error_buffer{};
        CurlHeaders headers;
        if (!headers.Append(content_type) || !headers.Append("Expect:")) {
            result.curl_code = CURLE_OUT_OF_MEMORY;
            result.transport_error = "failed to allocate HTTP headers";
            return result;
        }
        ::curl_easy_reset(handle_);
        const auto set = [this](CURLoption option, auto value) {
            return ::curl_easy_setopt(handle_, option, value) == CURLE_OK;
        };
        const bool configured =
            set(CURLOPT_URL, url.data()) &&
            set(CURLOPT_POST, 1L) &&
            set(CURLOPT_POSTFIELDS,
                reinterpret_cast<const char*>(payload.data())) &&
            set(CURLOPT_POSTFIELDSIZE_LARGE,
                static_cast<curl_off_t>(payload.size())) &&
            set(CURLOPT_HTTPHEADER, headers.get()) &&
            set(CURLOPT_USERNAME, config_.username.c_str()) &&
            set(CURLOPT_PASSWORD, config_.password.c_str()) &&
            set(CURLOPT_NOPROXY, config_.no_proxy.c_str()) &&
            set(CURLOPT_CONNECTTIMEOUT_MS,
                static_cast<long>(config_.connect_timeout_ms)) &&
            set(CURLOPT_TIMEOUT_MS,
                static_cast<long>(config_.request_timeout_ms)) &&
            set(CURLOPT_NOSIGNAL, 1L) &&
            set(CURLOPT_TCP_KEEPALIVE, 1L) &&
            set(CURLOPT_SSL_VERIFYPEER,
                config_.tls_verify_peer ? 1L : 0L) &&
            set(CURLOPT_SSL_VERIFYHOST,
                config_.tls_verify_peer ? 2L : 0L) &&
            set(CURLOPT_WRITEFUNCTION, &CaptureResponse) &&
            set(CURLOPT_WRITEDATA, &result.response) &&
            set(CURLOPT_ERRORBUFFER, error_buffer.data()) &&
            set(CURLOPT_USERAGENT, "l2flow-clickhouse-raw/1");
        if (!configured) {
            result.curl_code = CURLE_FAILED_INIT;
            result.transport_error = "failed to configure libcurl request";
            return result;
        }
        result.curl_code = ::curl_easy_perform(handle_);
        if (result.curl_code != CURLE_OK) {
            result.transport_error = error_buffer.front() != '\0'
                ? error_buffer.data()
                : ::curl_easy_strerror(result.curl_code);
            return result;
        }
        static_cast<void>(::curl_easy_getinfo(
            handle_, CURLINFO_RESPONSE_CODE, &result.status_code));
        return result;
    }

    const RawClickHouseConfig& config_;
    CURL* handle_ = nullptr;
};

[[nodiscard]] bool HttpSucceeded(const HttpResult& result) noexcept {
    return result.curl_code == CURLE_OK && result.status_code >= 200L &&
           result.status_code < 300L;
}

[[nodiscard]] bool IsRetryable(const HttpResult& result) noexcept {
    if (result.curl_code != CURLE_OK) {
        switch (result.curl_code) {
            case CURLE_UNSUPPORTED_PROTOCOL:
            case CURLE_FAILED_INIT:
            case CURLE_URL_MALFORMAT:
            case CURLE_OUT_OF_MEMORY:
            case CURLE_WRITE_ERROR:
            case CURLE_BAD_FUNCTION_ARGUMENT:
            case CURLE_TOO_MANY_REDIRECTS:
            case CURLE_FILESIZE_EXCEEDED:
            case CURLE_LOGIN_DENIED:
            case CURLE_PEER_FAILED_VERIFICATION:
                return false;
            default:
                return true;
        }
    }
    if (result.status_code == 408L || result.status_code == 429L ||
        result.status_code == 502L || result.status_code == 503L ||
        result.status_code == 504L) {
        return true;
    }
    if (result.status_code != 500L) {
        return false;
    }
    constexpr std::array<std::string_view, 10U> retryable_errors{{
        "TIMEOUT_EXCEEDED",
        "SOCKET_TIMEOUT",
        "NETWORK_ERROR",
        "ALL_CONNECTION_TRIES_FAILED",
        "NO_ACTIVE_REPLICAS",
        "TOO_FEW_LIVE_REPLICAS",
        "QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING",
        "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE",
        "UNKNOWN_STATUS_OF_INSERT",
        "TOO_MANY_SIMULTANEOUS_QUERIES",
    }};
    return std::any_of(
        retryable_errors.begin(), retryable_errors.end(),
        [&result](std::string_view value) {
            return result.response.find(value) != std::string::npos;
        });
}

[[nodiscard]] std::string HttpErrorText(const HttpResult& result) {
    if (result.curl_code != CURLE_OK) {
        return "transport error: " + result.transport_error;
    }
    return "HTTP " + std::to_string(result.status_code) + ": " +
           result.response;
}

void InitializeCurl() {
    static std::once_flag once;
    static CURLcode initialization = CURLE_FAILED_INIT;
    std::call_once(once, [] {
        initialization = ::curl_global_init(CURL_GLOBAL_DEFAULT);
    });
    if (initialization != CURLE_OK) {
        throw std::runtime_error("curl_global_init failed");
    }
}

}  // namespace

bool ValidateRawClickHouseConfig(const RawClickHouseConfig& config,
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
    const std::string_view endpoint = config.endpoint;
    const bool http = endpoint.starts_with("http://");
    const bool https = endpoint.starts_with("https://");
    if ((!http && !https) || endpoint.find('?') != std::string::npos ||
        endpoint.find('#') != std::string::npos) {
        return fail("ClickHouse endpoint must be an HTTP(S) base URL");
    }
    const std::size_t authority_begin = endpoint.find("://") + 3U;
    const std::size_t authority_end =
        endpoint.find('/', authority_begin);
    const std::size_t authority_size =
        (authority_end == std::string::npos ? endpoint.size()
                                            : authority_end) -
        authority_begin;
    if (authority_size == 0U) {
        return fail("ClickHouse endpoint authority may not be empty");
    }
    if (endpoint.substr(authority_begin, authority_size).find('@') !=
        std::string::npos) {
        return fail(
            "ClickHouse endpoint may not contain embedded credentials");
    }
    if (!IsIdentifierName(config.database)) {
        return fail("ClickHouse database is not a valid identifier");
    }
    if (config.username.empty()) {
        return fail("ClickHouse username may not be empty");
    }
    if (config.feed_session_epoch == 0U ||
        config.tick_decoder_lanes == 0U ||
        config.snapshot_decoder_lanes == 0U ||
        config.writer_threads == 0U || config.tick_batch_rows == 0U ||
        config.snapshot_batch_rows == 0U ||
        config.tick_batch_rows >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.snapshot_batch_rows >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.tick_batch_bytes == 0U ||
        config.snapshot_batch_bytes == 0U ||
        config.tick_batch_max_delay_ns == 0U ||
        config.snapshot_batch_max_delay_ns == 0U ||
        config.tick_queue_batches_per_lane == 0U ||
        config.snapshot_queue_batches_per_lane == 0U) {
        return fail("invalid ClickHouse raw batch or lane configuration");
    }
    if (config.connect_timeout_ms == 0U ||
        config.request_timeout_ms == 0U ||
        config.retry_initial_backoff_ms == 0U ||
        config.retry_max_backoff_ms < config.retry_initial_backoff_ms ||
        config.maximum_retry_elapsed_ms == 0U ||
        config.shutdown_timeout_ms < config.request_timeout_ms) {
        return fail("invalid ClickHouse timeout/retry configuration");
    }
    std::size_t tick_queue_capacity = 0U;
    std::size_t snapshot_queue_capacity = 0U;
    if (!CheckedRoundUpPowerOfTwo(
            config.tick_queue_batches_per_lane, &tick_queue_capacity) ||
        !CheckedRoundUpPowerOfTwo(
            config.snapshot_queue_batches_per_lane,
            &snapshot_queue_capacity)) {
        return fail("ClickHouse raw queue capacity is too large");
    }
    const auto pool_overflows = [](std::size_t lanes,
                                   std::size_t queue_capacity,
                                   std::size_t rows,
                                   std::size_t row_bytes) {
        const std::size_t slots = queue_capacity + 1U;
        return lanes > std::numeric_limits<std::size_t>::max() / slots ||
               lanes * slots >
                   std::numeric_limits<std::size_t>::max() / rows ||
               lanes * slots * rows >
                   std::numeric_limits<std::size_t>::max() / row_bytes;
    };
    if (pool_overflows(config.tick_decoder_lanes, tick_queue_capacity,
                       config.tick_batch_rows, sizeof(CanonicalTick)) ||
        pool_overflows(config.snapshot_decoder_lanes,
                       snapshot_queue_capacity,
                       config.snapshot_batch_rows,
                       sizeof(CanonicalSnapshot))) {
        return fail("ClickHouse raw canonical pool size overflows size_t");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

class RawClickHouseSink::Impl final {
public:
    Impl(RawClickHouseConfig config,
         Identifier128 writer_instance_id,
         Identifier128 source_instance_id)
        : config_(std::move(config)),
          writer_instance_id_(writer_instance_id),
          source_instance_id_(source_instance_id) {}

    [[nodiscard]] bool Initialize(std::string* error) {
        try {
            tick_lanes_.reserve(config_.tick_decoder_lanes);
            for (std::size_t lane = 0U;
                 lane < config_.tick_decoder_lanes; ++lane) {
                tick_lanes_.push_back(
                    std::make_unique<RawBatchLane<CanonicalTick>>(
                        config_.tick_batch_rows,
                        config_.tick_batch_bytes,
                        config_.tick_batch_max_delay_ns,
                        config_.tick_queue_batches_per_lane));
            }
            snapshot_lanes_.reserve(config_.snapshot_decoder_lanes);
            for (std::size_t lane = 0U;
                 lane < config_.snapshot_decoder_lanes; ++lane) {
                snapshot_lanes_.push_back(
                    std::make_unique<RawBatchLane<CanonicalSnapshot>>(
                        config_.snapshot_batch_rows,
                        config_.snapshot_batch_bytes,
                        config_.snapshot_batch_max_delay_ns,
                        config_.snapshot_queue_batches_per_lane));
            }
            InitializeCurl();
        } catch (const std::exception& exception) {
            if (error != nullptr) {
                *error = std::string("ClickHouse raw sink preallocation failed: ") +
                         exception.what();
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "ClickHouse raw sink can be started exactly once";
            }
            return false;
        }
        try {
            HttpClient client(config_);
            const auto execute = [&client, error](std::string sql) {
                const HttpResult result = client.ExecuteSql(sql);
                if (!HttpSucceeded(result)) {
                    if (error != nullptr) {
                        *error = HttpErrorText(result);
                    }
                    return false;
                }
                return true;
            };
            if (config_.ensure_local_tables) {
                if (!execute("CREATE DATABASE IF NOT EXISTS " +
                             config_.database) ||
                    !execute(RawTickDdl(config_.database)) ||
                    !execute(RawSnapshotDdl(config_.database))) {
                    SetFatal("ClickHouse raw schema initialization failed", true);
                    return false;
                }
            }
            if (!execute(RawTableContractProbe(config_.database)) ||
                !execute(RawColumnProbe(
                    config_.database, "raw_tick", kTickFields)) ||
                !execute(RawColumnProbe(
                    config_.database, "raw_snapshot", kSnapshotFields))) {
                SetFatal("ClickHouse raw schema validation failed", true);
                return false;
            }

            run_started_monotonic_ns_ = ingest::MonotonicNowNs();
            run_started_utc_ns_ = SystemUtcNowNs();
            const std::size_t lane_count = tick_lanes_.size() +
                                           snapshot_lanes_.size();
            writer_count_ = std::min(config_.writer_threads, lane_count);
            writer_threads_.reserve(writer_count_);
            for (std::size_t worker = 0U; worker < writer_count_; ++worker) {
                writer_threads_.emplace_back(
                    [this, worker] { WriterLoop(worker); });
            }
            accepting_.store(true, std::memory_order_release);
        } catch (const std::exception& exception) {
            SetFatal(std::string("ClickHouse raw sink start failed: ") +
                         exception.what(),
                     true);
            stopping_.store(true, std::memory_order_release);
            wake_.notify_all();
            JoinWriters();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        if (!healthy()) {
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

    [[nodiscard]] bool Stop(std::string* error) noexcept {
        if (!started_.load(std::memory_order_acquire)) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        bool expected = false;
        if (!stop_called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            JoinWriters();
            if (error != nullptr) {
                *error = healthy() ? std::string{} : fatal_error();
            }
            return healthy();
        }
        accepting_.store(false, std::memory_order_release);
        const std::uint64_t now = ingest::MonotonicNowNs();
        const std::uint64_t timeout_ns =
            static_cast<std::uint64_t>(config_.shutdown_timeout_ms) *
            UINT64_C(1'000'000);
        shutdown_deadline_ns_.store(
            std::numeric_limits<std::uint64_t>::max() - now < timeout_ns
                ? std::numeric_limits<std::uint64_t>::max()
                : now + timeout_ns,
            std::memory_order_release);

        // Decoder producers are contractually quiesced before Stop. This final
        // pass makes the sink robust if a caller omitted the explicit lane
        // Flush after joining the engine.
        while (HasActiveRows()) {
            if (FlushAllActive()) {
                break;
            }
            if (ingest::MonotonicNowNs() >=
                shutdown_deadline_ns_.load(std::memory_order_acquire)) {
                SetFatal(
                    "ClickHouse shutdown timed out while publishing partial "
                    "raw batches",
                    false);
                break;
            }
            std::unique_lock<std::mutex> lock(wake_mutex_);
            wake_.wait_for(lock, std::chrono::milliseconds(1));
        }

        stopping_.store(true, std::memory_order_release);
        wake_.notify_all();
        JoinWriters();
        if (PendingBatchCount() != 0U || HasActiveRows()) {
            SetFatal("ClickHouse raw sink stopped with unacknowledged batches",
                     false);
        }
        if (error != nullptr) {
            *error = healthy() ? std::string{} : fatal_error();
        }
        return healthy();
    }

    [[nodiscard]] bool AppendTick(std::size_t decoder_lane,
                                  const CanonicalTick& tick) noexcept {
        if (!CanAppend() || decoder_lane >= tick_lanes_.size()) {
            return false;
        }
        const LaneAppendCode code = tick_lanes_[decoder_lane]->Append(
            tick, [this](std::size_t rows) {
                BatchPublished(RawTableId::kRawTick, rows);
            });
        return HandleLaneCode(code, RawTableId::kRawTick, decoder_lane);
    }

    [[nodiscard]] bool AppendSnapshot(
        std::size_t decoder_lane,
        const CanonicalSnapshot& snapshot) noexcept {
        if (!CanAppend() || decoder_lane >= snapshot_lanes_.size()) {
            return false;
        }
        const LaneAppendCode code = snapshot_lanes_[decoder_lane]->Append(
            snapshot, [this](std::size_t rows) {
                BatchPublished(RawTableId::kRawSnapshot, rows);
            });
        return HandleLaneCode(
            code, RawTableId::kRawSnapshot, decoder_lane);
    }

    [[nodiscard]] bool PollTick(std::size_t decoder_lane,
                                std::uint64_t monotonic_ns) noexcept {
        if (!started_.load(std::memory_order_acquire) ||
            decoder_lane >= tick_lanes_.size()) {
            return false;
        }
        const LaneAppendCode code = tick_lanes_[decoder_lane]->Poll(
            monotonic_ns, [this](std::size_t rows) {
                BatchPublished(RawTableId::kRawTick, rows);
            });
        return HandleLaneCode(code, RawTableId::kRawTick, decoder_lane) &&
               healthy();
    }

    [[nodiscard]] bool PollSnapshot(std::size_t decoder_lane,
                                    std::uint64_t monotonic_ns) noexcept {
        if (!started_.load(std::memory_order_acquire) ||
            decoder_lane >= snapshot_lanes_.size()) {
            return false;
        }
        const LaneAppendCode code = snapshot_lanes_[decoder_lane]->Poll(
            monotonic_ns, [this](std::size_t rows) {
                BatchPublished(RawTableId::kRawSnapshot, rows);
            });
        return HandleLaneCode(
                   code, RawTableId::kRawSnapshot, decoder_lane) &&
               healthy();
    }

    [[nodiscard]] bool FlushTick(std::size_t decoder_lane) noexcept {
        if (!started_.load(std::memory_order_acquire) ||
            decoder_lane >= tick_lanes_.size()) {
            return false;
        }
        return HandleLaneCode(
            tick_lanes_[decoder_lane]->Flush(
                [this](std::size_t rows) {
                    BatchPublished(RawTableId::kRawTick, rows);
                }),
            RawTableId::kRawTick, decoder_lane);
    }

    [[nodiscard]] bool FlushSnapshot(std::size_t decoder_lane) noexcept {
        if (!started_.load(std::memory_order_acquire) ||
            decoder_lane >= snapshot_lanes_.size()) {
            return false;
        }
        return HandleLaneCode(
            snapshot_lanes_[decoder_lane]->Flush(
                [this](std::size_t rows) {
                    BatchPublished(RawTableId::kRawSnapshot, rows);
                }),
            RawTableId::kRawSnapshot, decoder_lane);
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] RawClickHouseStats Stats() const noexcept {
        RawClickHouseStats result{};
        result.tick_rows_received = stats_.tick_rows_received.load(
            std::memory_order_relaxed);
        result.snapshot_rows_received = stats_.snapshot_rows_received.load(
            std::memory_order_relaxed);
        result.batches_queued = stats_.batches_queued.load(
            std::memory_order_relaxed);
        result.batches_acked = stats_.batches_acked.load(
            std::memory_order_relaxed);
        result.batches_released = stats_.batches_released.load(
            std::memory_order_relaxed);
        result.rows_acked = stats_.rows_acked.load(
            std::memory_order_relaxed);
        result.retry_attempts = stats_.retry_attempts.load(
            std::memory_order_relaxed);
        result.unknown_outcomes = stats_.unknown_outcomes.load(
            std::memory_order_relaxed);
        result.bytes_sent = stats_.bytes_sent.load(
            std::memory_order_relaxed);
        result.unacked_batches = UnackedBatchCount();
        for (const auto& lane : tick_lanes_) {
            if (lane->has_active_rows()) {
                ++result.unacked_batches;
            }
        }
        for (const auto& lane : snapshot_lanes_) {
            if (lane->has_active_rows()) {
                ++result.unacked_batches;
            }
        }
        for (const auto& lane : tick_lanes_) {
            result.preallocated_canonical_bytes +=
                lane->preallocated_bytes();
        }
        for (const auto& lane : snapshot_lanes_) {
            result.preallocated_canonical_bytes +=
                lane->preallocated_bytes();
        }
        return result;
    }

    [[nodiscard]] Identifier128 writer_instance_id() const noexcept {
        return writer_instance_id_;
    }

    [[nodiscard]] Identifier128 source_instance_id() const noexcept {
        return source_instance_id_;
    }

    [[nodiscard]] std::uint64_t run_started_monotonic_ns() const noexcept {
        return run_started_monotonic_ns_;
    }

    [[nodiscard]] std::uint64_t run_started_utc_ns() const noexcept {
        return run_started_utc_ns_;
    }

    [[nodiscard]] const RawClickHouseConfig& config() const noexcept {
        return config_;
    }

private:
    class WriterContext final {
    public:
        WriterContext(const RawClickHouseConfig& config,
                      std::string* error)
            : http(config) {
            tick_builder = TickRecordBatchBuilder::Create(
                config.tick_batch_rows, config.feed_session_epoch, error);
            if (tick_builder == nullptr) {
                return;
            }
            snapshot_builder = SnapshotRecordBatchBuilder::Create(
                config.snapshot_batch_rows, config.feed_session_epoch, error);
        }

        [[nodiscard]] bool valid() const noexcept {
            return tick_builder != nullptr && snapshot_builder != nullptr;
        }

        HttpClient http;
        std::unique_ptr<TickRecordBatchBuilder> tick_builder;
        std::unique_ptr<SnapshotRecordBatchBuilder> snapshot_builder;
    };

    [[nodiscard]] bool CanAppend() const noexcept {
        return started_.load(std::memory_order_acquire) &&
               accepting_.load(std::memory_order_acquire) && healthy();
    }

    void BatchPublished(RawTableId table, std::size_t rows) noexcept {
        if (table == RawTableId::kRawTick) {
            stats_.tick_rows_received.fetch_add(
                static_cast<std::uint64_t>(rows),
                std::memory_order_relaxed);
        } else {
            stats_.snapshot_rows_received.fetch_add(
                static_cast<std::uint64_t>(rows),
                std::memory_order_relaxed);
        }
        stats_.batches_queued.fetch_add(1U, std::memory_order_relaxed);
        wake_.notify_one();
    }

    [[nodiscard]] bool HandleLaneCode(LaneAppendCode code,
                                      RawTableId table,
                                      std::size_t lane) noexcept {
        if (code == LaneAppendCode::kOk) {
            return true;
        }
        const std::string table_name = table == RawTableId::kRawTick
            ? "raw_tick"
            : "raw_snapshot";
        const char* reason = code == LaneAppendCode::kQueueFull
            ? "preallocated batch pool exhausted"
            : code == LaneAppendCode::kInvalidRecord
            ? "trade_date changed inside one batch"
            : "SPSC batch queue invariant failed";
        SetFatal(table_name + " lane " + std::to_string(lane) + ": " +
                     reason,
                 code == LaneAppendCode::kInvariantFailure);
        return false;
    }

    void SetFatal(std::string message, bool halt_writers) noexcept {
        if (halt_writers) {
            halt_writers_.store(true, std::memory_order_release);
        }
        accepting_.store(false, std::memory_order_release);
        if (!fatal_claimed_.test_and_set(std::memory_order_acq_rel)) {
            try {
                std::lock_guard<std::mutex> lock(fatal_mutex_);
                fatal_error_ = std::move(message);
            } catch (...) {
            }
            healthy_.store(false, std::memory_order_release);
        }
        wake_.notify_all();
    }

    [[nodiscard]] std::uint64_t PendingBatchCount() const noexcept {
        const std::uint64_t queued = stats_.batches_queued.load(
            std::memory_order_acquire);
        const std::uint64_t released = stats_.batches_released.load(
            std::memory_order_acquire);
        return queued >= released ? queued - released : 0U;
    }

    [[nodiscard]] std::uint64_t UnackedBatchCount() const noexcept {
        const std::uint64_t queued = stats_.batches_queued.load(
            std::memory_order_acquire);
        const std::uint64_t acked = stats_.batches_acked.load(
            std::memory_order_acquire);
        return queued >= acked ? queued - acked : 0U;
    }

    [[nodiscard]] bool HasActiveRows() const noexcept {
        return std::any_of(
                   tick_lanes_.begin(), tick_lanes_.end(),
                   [](const auto& lane) { return lane->has_active_rows(); }) ||
               std::any_of(snapshot_lanes_.begin(), snapshot_lanes_.end(),
                           [](const auto& lane) {
                               return lane->has_active_rows();
                           });
    }

    [[nodiscard]] bool FlushAllActive() noexcept {
        bool flushed = true;
        for (std::size_t lane = 0U; lane < tick_lanes_.size(); ++lane) {
            const LaneAppendCode code = tick_lanes_[lane]->Flush(
                [this](std::size_t rows) {
                    BatchPublished(RawTableId::kRawTick, rows);
                });
            flushed = flushed && code == LaneAppendCode::kOk;
        }
        for (std::size_t lane = 0U; lane < snapshot_lanes_.size(); ++lane) {
            const LaneAppendCode code = snapshot_lanes_[lane]->Flush(
                [this](std::size_t rows) {
                    BatchPublished(RawTableId::kRawSnapshot, rows);
                });
            flushed = flushed && code == LaneAppendCode::kOk;
        }
        return flushed;
    }

    [[nodiscard]] bool IsAssigned(std::size_t global_lane,
                                  std::size_t worker) const noexcept {
        return global_lane % writer_count_ == worker;
    }

    [[nodiscard]] bool TryTakeTick(
        std::size_t worker,
        std::size_t* cursor,
        std::size_t* lane_index,
        std::uint32_t* batch_index,
        RawCanonicalBatch<CanonicalTick>** batch) noexcept {
        for (std::size_t attempt = 0U; attempt < tick_lanes_.size();
             ++attempt) {
            const std::size_t lane = (*cursor + attempt) % tick_lanes_.size();
            if (IsAssigned(lane, worker) &&
                tick_lanes_[lane]->TryPop(batch_index, batch)) {
                *cursor = (lane + 1U) % tick_lanes_.size();
                *lane_index = lane;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool TryTakeSnapshot(
        std::size_t worker,
        std::size_t* cursor,
        std::size_t* lane_index,
        std::uint32_t* batch_index,
        RawCanonicalBatch<CanonicalSnapshot>** batch) noexcept {
        const std::size_t offset = tick_lanes_.size();
        for (std::size_t attempt = 0U; attempt < snapshot_lanes_.size();
             ++attempt) {
            const std::size_t lane =
                (*cursor + attempt) % snapshot_lanes_.size();
            if (IsAssigned(offset + lane, worker) &&
                snapshot_lanes_[lane]->TryPop(batch_index, batch)) {
                *cursor = (lane + 1U) % snapshot_lanes_.size();
                *lane_index = lane;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool AssignedQueuesEmpty(std::size_t worker) const noexcept {
        for (std::size_t lane = 0U; lane < tick_lanes_.size(); ++lane) {
            if (IsAssigned(lane, worker) && !tick_lanes_[lane]->empty()) {
                return false;
            }
        }
        const std::size_t offset = tick_lanes_.size();
        for (std::size_t lane = 0U; lane < snapshot_lanes_.size(); ++lane) {
            if (IsAssigned(offset + lane, worker) &&
                !snapshot_lanes_[lane]->empty()) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] std::uint64_t AllocateBatchSequence() noexcept {
        const std::uint64_t sequence = next_batch_sequence_.fetch_add(
            1U, std::memory_order_relaxed);
        if (sequence == 0U ||
            sequence == std::numeric_limits<std::uint64_t>::max()) {
            SetFatal("ClickHouse raw batch_sequence exhausted", true);
            return 0U;
        }
        return sequence;
    }

    template <typename Record>
    [[nodiscard]] bool BuildPayload(
        RawCanonicalBatch<Record>& batch,
        WriterContext* context,
        InFlightMetadata* metadata,
        std::shared_ptr<arrow::Buffer>* payload,
        std::string* error) {
        BuiltRecordBatch built{};
        if constexpr (std::is_same_v<Record, CanonicalTick>) {
            for (std::size_t row = 0U;
                 row < batch.metadata.row_count; ++row) {
                if (!context->tick_builder->AppendOrdered(
                        batch.rows.get()[row], error)) {
                    return false;
                }
            }
            built = context->tick_builder->Finish(error);
        } else {
            for (std::size_t row = 0U;
                 row < batch.metadata.row_count; ++row) {
                if (!context->snapshot_builder->Append(
                        batch.rows.get()[row], error)) {
                    return false;
                }
            }
            built = context->snapshot_builder->Finish(error);
        }
        if (built.batch == nullptr) {
            return false;
        }
        auto persistent = BuildPersistentBatch(
            *built.batch, *metadata, source_instance_id_,
            writer_instance_id_);
        if (!persistent.ok()) {
            *error = persistent.status().ToString();
            return false;
        }
        auto serialized = SerializeBatch(**persistent);
        if (!serialized.ok()) {
            *error = serialized.status().ToString();
            return false;
        }
        *payload = std::move(*serialized);
        return true;
    }

    template <typename Record>
    [[nodiscard]] bool ProcessBatch(
        RawCanonicalBatch<Record>& batch,
        RawTableId table,
        WriterContext* context) {
        InFlightMetadata metadata{};
        metadata.table = table;
        metadata.schema_version = table == RawTableId::kRawTick
            ? kRawTickSchemaVersion
            : kRawSnapshotSchemaVersion;
        metadata.trade_date = batch.metadata.trade_date;
        metadata.batch_sequence = AllocateBatchSequence();
        if (metadata.batch_sequence == 0U) {
            return false;
        }
        metadata.batch_id = RawBatchIdentifier(
            writer_instance_id_, table, metadata.trade_date,
            metadata.batch_sequence, metadata.schema_version);

        std::shared_ptr<arrow::Buffer> payload;
        std::string error;
        try {
            if (!BuildPayload(batch, context, &metadata, &payload, &error)) {
                SetFatal("raw Arrow columnization failed: " + error, true);
                return false;
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("raw Arrow columnization threw: ") +
                         exception.what(),
                     true);
            return false;
        }

        std::uint64_t retry_started_ns = 0U;
        std::uint32_t backoff_ms = config_.retry_initial_backoff_ms;
        const std::string writer_id = IdentifierString(writer_instance_id_);
        for (;;) {
            if (halt_writers_.load(std::memory_order_acquire)) {
                return false;
            }
            const HttpResult result = context->http.Insert(
                metadata, writer_id,
                std::span<const std::byte>(
                    reinterpret_cast<const std::byte*>(payload->data()),
                    static_cast<std::size_t>(payload->size())));
            stats_.bytes_sent.fetch_add(
                static_cast<std::uint64_t>(payload->size()),
                std::memory_order_relaxed);
            if (HttpSucceeded(result)) {
                stats_.batches_acked.fetch_add(1U,
                                               std::memory_order_release);
                stats_.rows_acked.fetch_add(
                    static_cast<std::uint64_t>(batch.metadata.row_count),
                    std::memory_order_relaxed);
                wake_.notify_all();
                return true;
            }
            if (!IsRetryable(result)) {
                SetFatal("ClickHouse INSERT failed permanently for batch " +
                             IdentifierString(metadata.batch_id) + ": " +
                             HttpErrorText(result),
                         true);
                return false;
            }
            stats_.unknown_outcomes.fetch_add(1U,
                                              std::memory_order_relaxed);
            const std::uint64_t now = ingest::MonotonicNowNs();
            if (retry_started_ns == 0U) {
                retry_started_ns = now;
            }
            const std::uint64_t retry_limit_ns =
                static_cast<std::uint64_t>(
                    config_.maximum_retry_elapsed_ms) *
                UINT64_C(1'000'000);
            const std::uint64_t shutdown_deadline =
                shutdown_deadline_ns_.load(std::memory_order_acquire);
            if ((now >= retry_started_ns &&
                 now - retry_started_ns >= retry_limit_ns) ||
                (stopping_.load(std::memory_order_acquire) &&
                 now >= shutdown_deadline)) {
                SetFatal("ClickHouse INSERT retry budget exhausted for batch " +
                             IdentifierString(metadata.batch_id) + ": " +
                             HttpErrorText(result),
                         true);
                return false;
            }
            stats_.retry_attempts.fetch_add(1U,
                                            std::memory_order_relaxed);
            std::unique_lock<std::mutex> lock(wake_mutex_);
            wake_.wait_for(
                lock, std::chrono::milliseconds(backoff_ms),
                [this] {
                    return halt_writers_.load(std::memory_order_acquire);
                });
            backoff_ms = std::min(
                config_.retry_max_backoff_ms,
                backoff_ms > config_.retry_max_backoff_ms / 2U
                    ? config_.retry_max_backoff_ms
                    : backoff_ms * 2U);
        }
    }

    void WriterLoop(std::size_t worker) noexcept {
        try {
            std::string error;
            WriterContext context(config_, &error);
            if (!context.valid()) {
                SetFatal("ClickHouse writer Arrow builder failed: " + error,
                         true);
                return;
            }
            std::size_t tick_cursor = 0U;
            std::size_t snapshot_cursor = 0U;
            for (;;) {
                if (halt_writers_.load(std::memory_order_acquire)) {
                    return;
                }
                bool progress = false;
                for (std::size_t burst = 0U; burst < 4U; ++burst) {
                    std::size_t lane = 0U;
                    std::uint32_t batch_index = 0U;
                    RawCanonicalBatch<CanonicalTick>* batch = nullptr;
                    if (!TryTakeTick(worker, &tick_cursor, &lane,
                                     &batch_index, &batch)) {
                        break;
                    }
                    progress = true;
                    if (!ProcessBatch(
                            *batch, RawTableId::kRawTick, &context)) {
                        return;
                    }
                    if (!tick_lanes_[lane]->Recycle(batch_index)) {
                        SetFatal("raw_tick recycled SPSC queue overflow", true);
                        return;
                    }
                    stats_.batches_released.fetch_add(
                        1U, std::memory_order_release);
                    wake_.notify_all();
                }

                std::size_t lane = 0U;
                std::uint32_t batch_index = 0U;
                RawCanonicalBatch<CanonicalSnapshot>* batch = nullptr;
                if (TryTakeSnapshot(worker, &snapshot_cursor, &lane,
                                    &batch_index, &batch)) {
                    progress = true;
                    if (!ProcessBatch(
                            *batch, RawTableId::kRawSnapshot, &context)) {
                        return;
                    }
                    if (!snapshot_lanes_[lane]->Recycle(batch_index)) {
                        SetFatal(
                            "raw_snapshot recycled SPSC queue overflow", true);
                        return;
                    }
                    stats_.batches_released.fetch_add(
                        1U, std::memory_order_release);
                    wake_.notify_all();
                }

                if (!progress) {
                    if (stopping_.load(std::memory_order_acquire) &&
                        AssignedQueuesEmpty(worker)) {
                        return;
                    }
                    std::unique_lock<std::mutex> lock(wake_mutex_);
                    wake_.wait_for(lock, std::chrono::milliseconds(1));
                }
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("ClickHouse writer thread failed: ") +
                         exception.what(),
                     true);
        } catch (...) {
            SetFatal("ClickHouse writer thread failed with unknown exception",
                     true);
        }
    }

    void JoinWriters() noexcept {
        for (std::thread& thread : writer_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        writer_threads_.clear();
    }

    RawClickHouseConfig config_;
    Identifier128 writer_instance_id_{};
    Identifier128 source_instance_id_{};
    std::uint64_t run_started_monotonic_ns_ = 0U;
    std::uint64_t run_started_utc_ns_ = 0U;
    std::vector<std::unique_ptr<RawBatchLane<CanonicalTick>>> tick_lanes_;
    std::vector<std::unique_ptr<RawBatchLane<CanonicalSnapshot>>>
        snapshot_lanes_;
    AtomicStats stats_{};
    std::size_t writer_count_ = 0U;
    std::vector<std::thread> writer_threads_;
    std::atomic<std::uint64_t> next_batch_sequence_{1U};
    std::atomic<std::uint64_t> shutdown_deadline_ns_{
        std::numeric_limits<std::uint64_t>::max()};
    std::atomic<bool> started_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stop_called_{false};
    std::atomic<bool> healthy_{true};
    std::atomic<bool> halt_writers_{false};
    std::atomic_flag fatal_claimed_ = ATOMIC_FLAG_INIT;
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
    std::mutex wake_mutex_;
    std::condition_variable wake_;
};

std::unique_ptr<RawClickHouseSink> RawClickHouseSink::Create(
    RawClickHouseConfig config,
    std::string* error) {
    if (!ValidateRawClickHouseConfig(config, error)) {
        return nullptr;
    }
    try {
        while (config.endpoint.size() > std::string_view("http://").size() &&
               config.endpoint.ends_with('/')) {
            config.endpoint.pop_back();
        }
        const Identifier128 writer_instance_id = GenerateIdentifier();
        const Identifier128 source_instance_id =
            IsZero(config.source_instance_id) ? writer_instance_id
                                              : config.source_instance_id;
        auto impl = std::make_unique<Impl>(
            std::move(config), writer_instance_id, source_instance_id);
        if (!impl->Initialize(error)) {
            return nullptr;
        }
        return std::unique_ptr<RawClickHouseSink>(
            new RawClickHouseSink(std::move(impl)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("ClickHouse raw sink creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

RawClickHouseSink::RawClickHouseSink(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RawClickHouseSink::~RawClickHouseSink() {
    std::string ignored;
    static_cast<void>(impl_->Stop(&ignored));
}

bool RawClickHouseSink::Start(std::string* error) {
    return impl_->Start(error);
}

bool RawClickHouseSink::Stop(std::string* error) noexcept {
    return impl_->Stop(error);
}

bool RawClickHouseSink::AppendTick(
    std::size_t decoder_lane,
    const CanonicalTick& tick) noexcept {
    return impl_->AppendTick(decoder_lane, tick);
}

bool RawClickHouseSink::AppendSnapshot(
    std::size_t decoder_lane,
    const CanonicalSnapshot& snapshot) noexcept {
    return impl_->AppendSnapshot(decoder_lane, snapshot);
}

bool RawClickHouseSink::PollTick(std::size_t decoder_lane,
                                 std::uint64_t monotonic_ns) noexcept {
    return impl_->PollTick(decoder_lane, monotonic_ns);
}

bool RawClickHouseSink::PollSnapshot(std::size_t decoder_lane,
                                     std::uint64_t monotonic_ns) noexcept {
    return impl_->PollSnapshot(decoder_lane, monotonic_ns);
}

bool RawClickHouseSink::FlushTick(std::size_t decoder_lane) noexcept {
    return impl_->FlushTick(decoder_lane);
}

bool RawClickHouseSink::FlushSnapshot(std::size_t decoder_lane) noexcept {
    return impl_->FlushSnapshot(decoder_lane);
}

bool RawClickHouseSink::healthy() const noexcept { return impl_->healthy(); }

std::string RawClickHouseSink::fatal_error() const {
    return impl_->fatal_error();
}

RawClickHouseStats RawClickHouseSink::stats() const noexcept {
    return impl_->Stats();
}

Identifier128 RawClickHouseSink::writer_instance_id() const noexcept {
    return impl_->writer_instance_id();
}

Identifier128 RawClickHouseSink::source_instance_id() const noexcept {
    return impl_->source_instance_id();
}

std::uint64_t RawClickHouseSink::run_started_monotonic_ns() const noexcept {
    return impl_->run_started_monotonic_ns();
}

std::uint64_t RawClickHouseSink::run_started_utc_ns() const noexcept {
    return impl_->run_started_utc_ns();
}

const RawClickHouseConfig& RawClickHouseSink::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::clickhouse
