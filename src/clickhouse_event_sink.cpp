#include "l2flow/clickhouse/event_sink.h"

#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <curl/curl.h>
#include <deque>
#include <limits>
#include <mutex>
#include <random>
#include <shared_mutex>
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

using event::EventPayload;
using event::EventRevision;
using event::EventRevisionBatch;
using event::Identifier128;
using event::OrderSnapshot;
using event::SourceAnchor;

constexpr std::size_t kMaximumHttpResponseBytes = 64U * 1'024U;

[[nodiscard]] bool IsZero(Identifier128 identifier) noexcept {
    return std::all_of(identifier.bytes.begin(), identifier.bytes.end(),
                       [](std::byte value) {
                           return value == std::byte{0U};
                       });
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
            throw std::runtime_error(
                "getrandom failed while creating Event writer ID");
        }
        if (count == 0) {
            throw std::runtime_error(
                "getrandom returned zero bytes for Event writer ID");
        }
        destination += count;
        remaining -= static_cast<std::size_t>(count);
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

[[nodiscard]] std::uint64_t SystemUtcNowNs() noexcept {
    const auto value = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return value > 0 ? static_cast<std::uint64_t>(value) : 0U;
}

[[nodiscard]] Identifier128 ChunkIdentifier(
    Identifier128 writer,
    Identifier128 recovery_run,
    std::uint32_t chunk,
    std::uint32_t schema_version,
    std::uint8_t table_tag) noexcept {
    std::array<std::byte, 41U> input{};
    std::copy(writer.bytes.begin(), writer.bytes.end(), input.begin());
    std::copy(recovery_run.bytes.begin(), recovery_run.bytes.end(),
              input.begin() + 16U);
    std::size_t offset = 32U;
    for (std::size_t index = 0U; index < sizeof(chunk); ++index) {
        input[offset++] = static_cast<std::byte>(chunk >> (index * 8U));
    }
    for (std::size_t index = 0U; index < sizeof(schema_version); ++index) {
        input[offset++] = static_cast<std::byte>(
            schema_version >> (index * 8U));
    }
    input[offset] = static_cast<std::byte>(table_tag);
    return Blake3Hash128(input);
}

[[nodiscard]] bool TradeDateToDays(std::uint32_t value,
                                   std::uint16_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const int year = static_cast<int>(value / 10'000U);
    const unsigned int month = (value / 100U) % 100U;
    const unsigned int day = value % 100U;
    using namespace std::chrono;
    const year_month_day date{
        std::chrono::year{year}, std::chrono::month{month},
        std::chrono::day{day}};
    if (!date.ok()) {
        return false;
    }
    const auto count = sys_days{date}.time_since_epoch().count();
    if (count < 0 ||
        count > std::numeric_limits<std::uint16_t>::max()) {
        return false;
    }
    *output = static_cast<std::uint16_t>(count);
    return true;
}

class RowBinaryWriter final {
public:
    explicit RowBinaryWriter(std::size_t reserve_bytes) {
        bytes_.reserve(reserve_bytes);
    }

    template <typename Value>
    void Append(Value value) {
        static_assert(std::is_integral_v<Value> &&
                      !std::is_same_v<Value, bool>);
        using Unsigned = std::make_unsigned_t<Value>;
        const Unsigned encoded = std::bit_cast<Unsigned>(value);
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            bytes_.push_back(static_cast<std::byte>(
                static_cast<std::uint64_t>(encoded) >> (index * 8U)));
        }
    }

    template <typename Enum>
    void AppendEnum(Enum value) {
        static_assert(std::is_enum_v<Enum>);
        Append(static_cast<std::underlying_type_t<Enum>>(value));
    }

    void AppendBool(bool value) {
        Append<std::uint8_t>(value ? 1U : 0U);
    }

    void AppendIdentifier(Identifier128 value) {
        bytes_.insert(bytes_.end(), value.bytes.begin(), value.bytes.end());
    }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept {
        return bytes_;
    }

private:
    std::vector<std::byte> bytes_;
};

void AppendAnchor(RowBinaryWriter* writer,
                  const SourceAnchor& value) {
    writer->Append(value.native_sequence);
    writer->Append(value.ingress_sequence);
    writer->Append(value.vendor_sequence_id);
    writer->Append(value.receive_monotonic_ns);
    writer->Append(value.exchange_time_ns_from_midnight);
    writer->Append(value.vendor_local_time_ns_from_midnight);
    writer->Append(value.exchange_time_raw);
    writer->Append(value.vendor_local_time_raw);
    writer->AppendBool(value.exchange_time_valid);
    writer->AppendBool(value.vendor_local_time_valid);
}

void AppendOrder(RowBinaryWriter* writer,
                 const OrderSnapshot& value) {
    writer->Append(value.key.trade_date);
    writer->AppendEnum(value.key.market);
    writer->Append(value.key.instrument_id);
    writer->Append(value.key.channel);
    writer->Append(value.key.order_id);
    writer->AppendEnum(value.side);
    writer->AppendEnum(value.order_type);
    writer->AppendEnum(value.sh_side_source);
    writer->AppendEnum(value.sh_order_source);
    writer->Append(value.price_p6);
    writer->AppendBool(value.price_valid);
    writer->AppendEnum(value.sh_price_source);
    writer->Append(value.execution_boundary_price_p6);
    writer->AppendBool(value.execution_boundary_price_valid);
    writer->Append(value.published_quantity);
    writer->AppendBool(value.published_quantity_valid);
    writer->Append(value.original_quantity);
    writer->AppendBool(value.original_quantity_valid);
    writer->AppendEnum(value.sh_original_quantity_status);
    writer->Append(value.remaining_quantity);
    writer->AppendBool(value.remaining_quantity_valid);
    writer->Append(value.source_matched_quantity);
    writer->AppendBool(value.source_matched_quantity_valid);
    writer->Append(value.observed_pre_add_trade_quantity);
    writer->Append(value.post_add_trade_quantity);
    writer->Append(value.total_trade_quantity);
    writer->Append(value.total_cancel_quantity);
    writer->Append(value.trade_count);
    writer->Append(value.cancel_count);
    writer->AppendEnum(value.phase_at_first);
    writer->AppendEnum(value.phase_at_add);
    writer->AppendEnum(value.phase_at_last);
    writer->AppendBool(value.add_seen);
    writer->AppendBool(value.apply_to_book);
    AppendAnchor(writer, value.first_anchor);
    AppendAnchor(writer, value.last_anchor);
    AppendAnchor(writer, value.add_anchor);
    writer->AppendEnum(value.finality);
    writer->Append(value.quality_flags);
    writer->Append(value.source_quality_flags);
}

void AppendPayload(RowBinaryWriter* writer,
                   const EventPayload& value) {
    AppendAnchor(writer, value.source_anchor);
    writer->AppendEnum(value.action);
    writer->AppendEnum(value.side);
    writer->AppendEnum(value.aggressor);
    writer->AppendEnum(value.order_type);
    writer->AppendEnum(value.phase);
    writer->Append(value.price_p6);
    writer->AppendBool(value.price_valid);
    writer->Append(value.amount_p6);
    writer->AppendBool(value.amount_valid);
    writer->Append(value.quantity);
    writer->AppendBool(value.quantity_valid);
    writer->Append(value.matched_quantity);
    writer->AppendBool(value.matched_quantity_valid);
    writer->Append(value.primary_order_id);
    writer->Append(value.buy_order_id);
    writer->Append(value.sell_order_id);
    writer->Append(value.source_quality_flags);
    writer->Append(value.event_quality_flags);
    writer->AppendBool(value.referenced_order_found);
    writer->AppendBool(value.referenced_order_found_valid);
    writer->AppendBool(value.side_from_order);
    writer->AppendEnum(value.order_delta_operation);
    writer->AppendBool(value.order_snapshot_valid);
    AppendOrder(writer, value.order);
}

struct RevisionChunkMetadata final {
    std::uint64_t sink_batch_sequence = 0U;
    std::uint32_t chunk_index = 0U;
    Identifier128 batch_id{};
    std::uint32_t trade_date = 0U;
};

[[nodiscard]] bool AppendRevisionRow(
    RowBinaryWriter* writer,
    const EventRevision& revision,
    Identifier128 writer_instance_id,
    const RevisionChunkMetadata& metadata,
    std::uint32_t row_index) {
    std::uint16_t days = 0U;
    if (!TradeDateToDays(revision.key.trade_date, &days)) {
        return false;
    }
    writer->Append(days);
    writer->AppendEnum(revision.key.market);
    writer->Append(revision.key.instrument_id);
    writer->Append(revision.key.channel);
    writer->Append(revision.key.native_sequence);
    writer->AppendEnum(revision.key.event_kind);
    writer->Append(revision.key.affected_order_id);
    writer->Append(revision.key.occurrence);
    writer->Append(revision.version);
    writer->AppendIdentifier(revision.revision_id);
    writer->AppendIdentifier(revision.supersedes_revision_id);
    writer->AppendBool(revision.supersedes_revision_id_valid);
    writer->AppendIdentifier(revision.recovery_run_id);
    writer->AppendEnum(revision.operation);
    writer->AppendEnum(revision.reason);
    writer->AppendIdentifier(revision.calculation_run_id);
    writer->Append(revision.logic_version);
    writer->AppendIdentifier(revision.input_set_hash);
    writer->AppendIdentifier(revision.payload_hash);
    writer->AppendBool(revision.is_deleted);
    AppendPayload(writer, revision.payload);
    writer->AppendIdentifier(writer_instance_id);
    writer->AppendIdentifier(metadata.batch_id);
    writer->Append(metadata.sink_batch_sequence);
    writer->Append(metadata.chunk_index);
    writer->Append(row_index);
    writer->Append(event::kEventSchemaVersion);
    return true;
}

[[nodiscard]] std::string AnchorType() {
    return "Tuple(native_sequence UInt64, ingress_sequence UInt64, "
           "vendor_sequence_id UInt64, receive_monotonic_ns UInt64, "
           "exchange_time_ns_from_midnight UInt64, "
           "vendor_local_time_ns_from_midnight UInt64, "
           "exchange_time_raw UInt32, vendor_local_time_raw UInt32, "
           "exchange_time_valid Bool, vendor_local_time_valid Bool)";
}

[[nodiscard]] std::string OrderType() {
    const std::string anchor = AnchorType();
    return "Tuple(trade_date UInt32, market UInt8, instrument_id UInt32, "
           "channel UInt32, order_id Int64, side UInt8, order_type UInt8, "
           "sh_side_source UInt8, sh_order_source UInt8, price_p6 Int64, "
           "price_valid Bool, sh_price_source UInt8, "
           "execution_boundary_price_p6 Int64, "
           "execution_boundary_price_valid Bool, published_quantity Int64, "
           "published_quantity_valid Bool, original_quantity Int64, "
           "original_quantity_valid Bool, "
           "sh_original_quantity_status UInt8, remaining_quantity Int64, "
           "remaining_quantity_valid Bool, source_matched_quantity Int64, "
           "source_matched_quantity_valid Bool, "
           "observed_pre_add_trade_quantity Int64, "
           "post_add_trade_quantity Int64, total_trade_quantity Int64, "
           "total_cancel_quantity Int64, trade_count UInt64, "
           "cancel_count UInt64, phase_at_first UInt8, phase_at_add UInt8, "
           "phase_at_last UInt8, add_seen Bool, apply_to_book Bool, "
           "first_anchor " + anchor + ", last_anchor " + anchor +
           ", add_anchor " + anchor + ", finality UInt8, "
           "quality_flags UInt64, source_quality_flags UInt64)";
}

[[nodiscard]] std::string PayloadType() {
    return "Tuple(source_anchor " + AnchorType() +
           ", action UInt8, side UInt8, aggressor UInt8, order_type UInt8, "
           "phase UInt8, price_p6 Int64, price_valid Bool, amount_p6 Int64, "
           "amount_valid Bool, quantity Int64, quantity_valid Bool, "
           "matched_quantity Int64, matched_quantity_valid Bool, "
           "primary_order_id Int64, buy_order_id Int64, "
           "sell_order_id Int64, source_quality_flags UInt64, "
           "event_quality_flags UInt64, referenced_order_found Bool, "
           "referenced_order_found_valid Bool, side_from_order Bool, "
           "order_delta_operation UInt8, order_snapshot_valid Bool, order " +
           OrderType() + ")";
}

[[nodiscard]] std::string RevisionColumns() {
    return "trade_date Date, market UInt8, instrument_id UInt32, "
           "channel UInt32, native_sequence UInt64, event_kind UInt8, "
           "affected_order_id Int64, occurrence UInt32, version UInt64, "
           "revision_id FixedString(16), "
           "supersedes_revision_id FixedString(16), "
           "supersedes_revision_id_valid Bool, "
           "recovery_run_id FixedString(16), revision_operation UInt8, "
           "revision_reason UInt8, calculation_run_id FixedString(16), "
           "logic_version UInt32, input_set_hash FixedString(16), "
           "payload_hash FixedString(16), is_deleted Bool, payload " +
           PayloadType() + ", writer_instance_id FixedString(16), "
           "batch_id FixedString(16), batch_sequence UInt64, "
           "chunk_index UInt32, row_index UInt32, schema_version UInt32";
}

[[nodiscard]] std::string RevisionColumnNames() {
    return "trade_date,market,instrument_id,channel,native_sequence,event_kind,"
           "affected_order_id,occurrence,version,revision_id,"
           "supersedes_revision_id,supersedes_revision_id_valid,"
           "recovery_run_id,revision_operation,"
           "revision_reason,calculation_run_id,logic_version,input_set_hash,"
           "payload_hash,is_deleted,payload,writer_instance_id,batch_id,"
           "batch_sequence,chunk_index,row_index,schema_version";
}

[[nodiscard]] std::string RevisionLogDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
           ".event_revision_log (" + RevisionColumns() +
           ") ENGINE = MergeTree PARTITION BY trade_date ORDER BY "
           "(market,instrument_id,channel,native_sequence,event_kind,"
           "affected_order_id,occurrence,version) SETTINGS "
           "non_replicated_deduplication_window=10000";
}

[[nodiscard]] std::string CurrentDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
           ".event (" + RevisionColumns() +
           ") ENGINE = ReplacingMergeTree(version) PARTITION BY trade_date "
           "ORDER BY (market,instrument_id,channel,native_sequence,event_kind,"
           "affected_order_id,occurrence)";
}

[[nodiscard]] std::string CurrentMvDdl(std::string_view database) {
    return "CREATE MATERIALIZED VIEW IF NOT EXISTS " +
           std::string(database) + ".event_current_mv TO " +
           std::string(database) + ".event AS SELECT " +
           RevisionColumnNames() + " FROM " + std::string(database) +
           ".event_revision_log";
}

[[nodiscard]] std::string RecoveryRunDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
           ".event_recovery_run (trade_date Date, "
           "calculation_run_id FixedString(16), "
           "recovery_run_id FixedString(16), owner UInt32, "
           "calculation_batch_sequence UInt64, revision_reason UInt8, "
           "minimum_version UInt64, maximum_version UInt64, "
           "revision_count UInt64, chunk_count UInt32, committed Bool, "
           "committed_utc_ns UInt64, writer_instance_id FixedString(16), "
           "commit_id FixedString(16), schema_version UInt32) "
           "ENGINE = MergeTree PARTITION BY trade_date ORDER BY "
           "(calculation_run_id,recovery_run_id) SETTINGS "
           "non_replicated_deduplication_window=10000";
}

[[nodiscard]] std::string EventTableProbe(std::string_view database) {
    constexpr std::string_view revision_sort =
        "market, instrument_id, channel, native_sequence, event_kind, "
        "affected_order_id, occurrence, version";
    constexpr std::string_view current_sort =
        "market, instrument_id, channel, native_sequence, event_kind, "
        "affected_order_id, occurrence";
    constexpr std::string_view recovery_sort =
        "calculation_run_id, recovery_run_id";
    const std::string current_target =
        " TO " + std::string(database) + ".event ";
    const std::string current_source =
        " FROM " + std::string(database) + ".event_revision_log";
    return "SELECT throwIf(count() != 4 OR "
           "countIf(name='event_revision_log' AND engine NOT IN "
           "('MergeTree','ReplicatedMergeTree')) != 0 OR "
           "countIf(name='event' AND engine NOT IN "
           "('ReplacingMergeTree','ReplicatedReplacingMergeTree')) != 0 OR "
           "countIf(name='event_recovery_run' AND engine NOT IN "
           "('MergeTree','ReplicatedMergeTree')) != 0 OR "
           "countIf(name='event_current_mv' AND "
           "engine!='MaterializedView') != 0 OR "
           "countIf(name IN ('event_revision_log','event',"
           "'event_recovery_run') AND partition_key!='trade_date') != 0 OR "
           "countIf(name='event_revision_log' AND sorting_key!='" +
           std::string(revision_sort) + "') != 0 OR "
           "countIf(name='event' AND (sorting_key!='" +
           std::string(current_sort) +
           "' OR position(engine_full,'version)')=0)) != 0 OR "
           "countIf(name='event_recovery_run' AND sorting_key!='" +
           std::string(recovery_sort) + "') != 0 OR "
           "countIf(name='event_current_mv' AND ("
           "position(create_table_query,'" + current_target +
           "')=0 OR position(create_table_query,'" + current_source +
           "')=0)) != 0, "
           "'Event table engine or partition contract mismatch') FROM "
           "system.tables WHERE database='" + std::string(database) +
           "' AND name IN ('event_revision_log','event',"
           "'event_recovery_run','event_current_mv') FORMAT Null";
}

void AppendColumnTypeCondition(std::string_view name,
                               std::string_view type,
                               std::string* sql) {
    *sql += " OR countIf(name='";
    sql->append(name);
    *sql += "' AND type='";
    sql->append(type);
    *sql += "') != 1";
}

[[nodiscard]] std::string RevisionColumnProbe(
    std::string_view database,
    std::string_view table) {
    std::string sql = "SELECT throwIf(count() != 27";
    AppendColumnTypeCondition("trade_date", "Date", &sql);
    AppendColumnTypeCondition("market", "UInt8", &sql);
    AppendColumnTypeCondition("instrument_id", "UInt32", &sql);
    AppendColumnTypeCondition("channel", "UInt32", &sql);
    AppendColumnTypeCondition("native_sequence", "UInt64", &sql);
    AppendColumnTypeCondition("event_kind", "UInt8", &sql);
    AppendColumnTypeCondition("affected_order_id", "Int64", &sql);
    AppendColumnTypeCondition("occurrence", "UInt32", &sql);
    AppendColumnTypeCondition("version", "UInt64", &sql);
    AppendColumnTypeCondition("revision_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition(
        "supersedes_revision_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition(
        "supersedes_revision_id_valid", "Bool", &sql);
    AppendColumnTypeCondition(
        "recovery_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("revision_operation", "UInt8", &sql);
    AppendColumnTypeCondition("revision_reason", "UInt8", &sql);
    AppendColumnTypeCondition(
        "calculation_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("logic_version", "UInt32", &sql);
    AppendColumnTypeCondition("input_set_hash", "FixedString(16)", &sql);
    AppendColumnTypeCondition("payload_hash", "FixedString(16)", &sql);
    AppendColumnTypeCondition("is_deleted", "Bool", &sql);
    AppendColumnTypeCondition("payload", PayloadType(), &sql);
    AppendColumnTypeCondition(
        "writer_instance_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("batch_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("batch_sequence", "UInt64", &sql);
    AppendColumnTypeCondition("chunk_index", "UInt32", &sql);
    AppendColumnTypeCondition("row_index", "UInt32", &sql);
    AppendColumnTypeCondition("schema_version", "UInt32", &sql);
    sql += ", 'Event revision column contract mismatch') FROM "
           "system.columns WHERE database='";
    sql.append(database);
    sql += "' AND table='";
    sql.append(table);
    sql += "' FORMAT Null";
    return sql;
}

[[nodiscard]] std::string RecoveryRunColumnProbe(
    std::string_view database) {
    std::string sql = "SELECT throwIf(count() != 15";
    AppendColumnTypeCondition("trade_date", "Date", &sql);
    AppendColumnTypeCondition(
        "calculation_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition(
        "recovery_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("owner", "UInt32", &sql);
    AppendColumnTypeCondition(
        "calculation_batch_sequence", "UInt64", &sql);
    AppendColumnTypeCondition("revision_reason", "UInt8", &sql);
    AppendColumnTypeCondition("minimum_version", "UInt64", &sql);
    AppendColumnTypeCondition("maximum_version", "UInt64", &sql);
    AppendColumnTypeCondition("revision_count", "UInt64", &sql);
    AppendColumnTypeCondition("chunk_count", "UInt32", &sql);
    AppendColumnTypeCondition("committed", "Bool", &sql);
    AppendColumnTypeCondition("committed_utc_ns", "UInt64", &sql);
    AppendColumnTypeCondition(
        "writer_instance_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("commit_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("schema_version", "UInt32", &sql);
    sql += ", 'Event recovery-run column contract mismatch') FROM "
           "system.columns WHERE database='";
    sql.append(database);
    sql += "' AND table='event_recovery_run' FORMAT Null";
    return sql;
}

struct HttpResult final {
    CURLcode curl_code = CURLE_OK;
    long status_code = 0L;
    std::string response;
    std::string transport_error;
};

size_t CaptureResponse(char* data,
                       std::size_t size,
                       std::size_t count,
                       void* context) noexcept {
    if (data == nullptr || context == nullptr || size == 0U ||
        count > std::numeric_limits<std::size_t>::max() / size) {
        return 0U;
    }
    const std::size_t bytes = size * count;
    std::string* response = static_cast<std::string*>(context);
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
    ~CurlHeaders() { ::curl_slist_free_all(head_); }
    [[nodiscard]] bool Append(const char* value) noexcept {
        curl_slist* const next = ::curl_slist_append(head_, value);
        if (next == nullptr) {
            return false;
        }
        head_ = next;
        return true;
    }
    [[nodiscard]] curl_slist* get() const noexcept { return head_; }

private:
    curl_slist* head_ = nullptr;
};

class HttpClient final {
public:
    explicit HttpClient(const EventClickHouseConfig& config)
        : config_(config), handle_(::curl_easy_init()) {
        if (handle_ == nullptr) {
            throw std::runtime_error("curl_easy_init failed");
        }
    }
    ~HttpClient() { ::curl_easy_cleanup(handle_); }

    [[nodiscard]] HttpResult ExecuteSql(std::string_view sql) {
        const std::string url = config_.endpoint + "/?query=" + Escape(sql) +
                                "&wait_end_of_query=1";
        return Perform(url, {}, "Content-Type: application/octet-stream");
    }

    [[nodiscard]] HttpResult Insert(
        std::string_view table,
        std::string_view columns,
        std::string_view query_id,
        std::string_view dedup_token,
        std::span<const std::byte> payload) {
        const std::string query = "INSERT INTO " + config_.database + "." +
                                  std::string(table) + " (" +
                                  std::string(columns) + ") FORMAT RowBinary";
        const std::string url = config_.endpoint + "/?query=" + Escape(query) +
            "&query_id=" + Escape(query_id) +
            "&async_insert=0&wait_end_of_query=1&insert_deduplicate=1" +
            "&insert_deduplication_token=" + Escape(dedup_token) +
            "&insert_quorum=" + std::to_string(config_.insert_quorum) +
            "&insert_quorum_parallel=" +
            (config_.insert_quorum_parallel ? "1" : "0");
        return Perform(url, payload,
                       "Content-Type: application/octet-stream");
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
        std::string result(escaped);
        ::curl_free(escaped);
        return result;
    }

    [[nodiscard]] HttpResult Perform(std::string_view url,
                                     std::span<const std::byte> payload,
                                     const char* content_type) {
        HttpResult result{};
        if (payload.size() > static_cast<std::size_t>(
                                 std::numeric_limits<curl_off_t>::max())) {
            result.curl_code = CURLE_FILESIZE_EXCEEDED;
            result.transport_error = "ClickHouse Event payload is too large";
            return result;
        }
        std::array<char, CURL_ERROR_SIZE> error_buffer{};
        CurlHeaders headers;
        if (!headers.Append(content_type) || !headers.Append("Expect:")) {
            result.curl_code = CURLE_OUT_OF_MEMORY;
            return result;
        }
        ::curl_easy_reset(handle_);
        const auto set = [this](CURLoption option, auto value) {
            return ::curl_easy_setopt(handle_, option, value) == CURLE_OK;
        };
        const bool configured =
            set(CURLOPT_URL, url.data()) && set(CURLOPT_POST, 1L) &&
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
            set(CURLOPT_USERAGENT, "l2flow-clickhouse-event/1");
        if (!configured) {
            result.curl_code = CURLE_FAILED_INIT;
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

    const EventClickHouseConfig& config_;
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
    constexpr std::array<std::string_view, 10U> retryable{{
        "TIMEOUT_EXCEEDED", "SOCKET_TIMEOUT", "NETWORK_ERROR",
        "ALL_CONNECTION_TRIES_FAILED", "NO_ACTIVE_REPLICAS",
        "TOO_FEW_LIVE_REPLICAS", "QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING",
        "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE", "UNKNOWN_STATUS_OF_INSERT",
        "TOO_MANY_SIMULTANEOUS_QUERIES"}};
    return std::any_of(retryable.begin(), retryable.end(),
                       [&result](std::string_view value) {
                           return result.response.find(value) !=
                                  std::string::npos;
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

struct AtomicStats final {
    std::atomic<std::uint64_t> revision_batches_queued{0U};
    std::atomic<std::uint64_t> revision_batches_acked{0U};
    std::atomic<std::uint64_t> revision_batches_released{0U};
    std::atomic<std::uint64_t> revision_rows_queued{0U};
    std::atomic<std::uint64_t> revision_rows_acked{0U};
    std::atomic<std::uint64_t> revision_chunks_acked{0U};
    std::atomic<std::uint64_t> recovery_runs_committed{0U};
    std::atomic<std::uint64_t> retry_attempts{0U};
    std::atomic<std::uint64_t> unknown_outcomes{0U};
    std::atomic<std::uint64_t> bytes_sent{0U};
};

[[nodiscard]] bool ValidRevisionReason(
    event::RevisionReason value) noexcept {
    return static_cast<std::uint8_t>(value) <=
           static_cast<std::uint8_t>(
               event::RevisionReason::kSessionFinalize);
}

[[nodiscard]] bool ValidRevisionOperation(
    event::RevisionOperation value) noexcept {
    return static_cast<std::uint8_t>(value) <=
           static_cast<std::uint8_t>(
               event::RevisionOperation::kTombstone);
}

[[nodiscard]] bool ValidEventKeyAndPayload(
    const EventRevision& revision) noexcept {
    const event::EventKey& key = revision.key;
    if (key.instrument_id == 0U || key.channel == 0U ||
        key.native_sequence == 0U ||
        (key.market != event::Market::kShanghai &&
         key.market != event::Market::kShenzhen)) {
        return false;
    }
    const bool shanghai_kind =
        key.event_kind == event::EventKind::kShanghaiOrderRevision ||
        key.event_kind == event::EventKind::kShanghaiTrade ||
        key.event_kind == event::EventKind::kShanghaiCancel ||
        key.event_kind == event::EventKind::kShanghaiStatus;
    const bool shenzhen_kind =
        key.event_kind == event::EventKind::kShenzhenOrderRevision ||
        key.event_kind == event::EventKind::kShenzhenTrade ||
        key.event_kind == event::EventKind::kShenzhenCancel;
    if ((key.market == event::Market::kShanghai && !shanghai_kind) ||
        (key.market == event::Market::kShenzhen && !shenzhen_kind)) {
        return false;
    }
    const bool order_revision =
        key.event_kind == event::EventKind::kShanghaiOrderRevision ||
        key.event_kind == event::EventKind::kShenzhenOrderRevision;
    if (order_revision != revision.payload.order_snapshot_valid ||
        (order_revision && key.affected_order_id <= 0) ||
        (!order_revision && key.affected_order_id != 0)) {
        return false;
    }
    if (!order_revision) {
        return true;
    }
    const event::OrderKey& order = revision.payload.order.key;
    return order.trade_date == key.trade_date &&
           order.market == key.market &&
           order.instrument_id == key.instrument_id &&
           order.channel == key.channel &&
           order.order_id == key.affected_order_id;
}

[[nodiscard]] bool ValidRevisionBatch(const EventRevisionBatch& batch)
    noexcept {
    if (IsZero(batch.calculation_run_id) || IsZero(batch.recovery_run_id) ||
        batch.batch_sequence == 0U || batch.revisions.empty() ||
        !ValidRevisionReason(batch.reason)) {
        return false;
    }
    std::uint64_t previous_version = 0U;
    const std::uint32_t trade_date = batch.revisions.front().key.trade_date;
    std::uint16_t ignored_days = 0U;
    if (!TradeDateToDays(trade_date, &ignored_days)) {
        return false;
    }
    for (const EventRevision& revision : batch.revisions) {
        if (revision.calculation_run_id != batch.calculation_run_id ||
            revision.recovery_run_id != batch.recovery_run_id ||
            revision.reason != batch.reason ||
            !ValidRevisionReason(revision.reason) ||
            !ValidRevisionOperation(revision.operation) ||
            !ValidEventKeyAndPayload(revision) ||
            revision.version == 0U || IsZero(revision.revision_id) ||
            IsZero(revision.input_set_hash) ||
            IsZero(revision.payload_hash) ||
            revision.logic_version == 0U ||
            revision.key.trade_date != trade_date ||
            revision.version <= previous_version ||
            revision.is_deleted !=
                (revision.operation ==
                 event::RevisionOperation::kTombstone)) {
            return false;
        }
        previous_version = revision.version;
    }
    return true;
}

}  // namespace

bool ValidateEventClickHouseConfig(const EventClickHouseConfig& config,
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
        return fail("ClickHouse Event endpoint must be an HTTP(S) base URL");
    }
    const std::size_t authority_begin = endpoint.find("://") + 3U;
    const std::size_t authority_end = endpoint.find('/', authority_begin);
    const std::size_t authority_size =
        (authority_end == std::string::npos ? endpoint.size()
                                            : authority_end) -
        authority_begin;
    if (authority_size == 0U ||
        endpoint.substr(authority_begin, authority_size).find('@') !=
            std::string::npos ||
        !IsIdentifierName(config.database) || config.username.empty()) {
        return fail("invalid ClickHouse Event endpoint, database, or user");
    }
    const bool valid_writer_lanes =
        config.writer_lanes == 1U || config.writer_lanes == 2U ||
        config.writer_lanes == 4U || config.writer_lanes == 8U;
    if (!valid_writer_lanes || config.insert_chunk_rows == 0U ||
        config.queue_revision_batches == 0U ||
        config.queue_revision_rows == 0U ||
        config.insert_chunk_rows > config.queue_revision_rows ||
        config.queue_revision_rows >=
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.connect_timeout_ms == 0U ||
        config.request_timeout_ms == 0U ||
        config.retry_initial_backoff_ms == 0U ||
        config.retry_max_backoff_ms < config.retry_initial_backoff_ms ||
        config.maximum_retry_elapsed_ms == 0U ||
        config.shutdown_timeout_ms < config.request_timeout_ms) {
        return fail("invalid ClickHouse Event queue or retry configuration");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

class EventClickHouseSink::Impl final {
public:
    struct Lane final {
        mutable std::mutex mutex;
        std::condition_variable wake;
        std::deque<std::shared_ptr<const EventRevisionBatch>> queue;
        std::thread thread;
    };

    Impl(EventClickHouseConfig config, Identifier128 writer_instance_id)
        : config_(std::move(config)),
          writer_instance_id_(writer_instance_id) {
        lanes_.reserve(config_.writer_lanes);
        for (std::size_t lane = 0U; lane < config_.writer_lanes; ++lane) {
            lanes_.push_back(std::make_unique<Lane>());
        }
    }

    [[nodiscard]] bool Initialize(std::string* error) {
        try {
            InitializeCurl();
        } catch (const std::exception& exception) {
            if (error != nullptr) {
                *error = exception.what();
            }
            return false;
        }
        return true;
    }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "ClickHouse Event sink can be started once";
            }
            return false;
        }
        try {
            HttpClient client(config_);
            const auto execute = [&client, error](const std::string& sql) {
                const HttpResult result = client.ExecuteSql(sql);
                if (!HttpSucceeded(result)) {
                    if (error != nullptr) {
                        *error = HttpErrorText(result);
                    }
                    return false;
                }
                return true;
            };
            if (config_.ensure_local_tables &&
                (!execute("CREATE DATABASE IF NOT EXISTS " +
                          config_.database) ||
                 !execute(RevisionLogDdl(config_.database)) ||
                 !execute(CurrentDdl(config_.database)) ||
                 !execute(CurrentMvDdl(config_.database)) ||
                 !execute(RecoveryRunDdl(config_.database)))) {
                SetFatal("ClickHouse Event schema initialization failed");
                return false;
            }
            if (!execute(EventTableProbe(config_.database)) ||
                !execute(RevisionColumnProbe(
                    config_.database, "event_revision_log")) ||
                !execute(RevisionColumnProbe(config_.database, "event")) ||
                !execute(RecoveryRunColumnProbe(config_.database))) {
                SetFatal("ClickHouse Event schema validation failed");
                return false;
            }
            for (std::size_t lane = 0U; lane < lanes_.size(); ++lane) {
                lanes_[lane]->thread = std::thread(
                    [this, lane] { WriterLoop(lane); });
            }
            accepting_.store(true, std::memory_order_release);
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (const std::exception& exception) {
            accepting_.store(false, std::memory_order_release);
            stopping_.store(true, std::memory_order_release);
            for (const auto& lane : lanes_) {
                lane->wake.notify_all();
            }
            for (auto& lane : lanes_) {
                if (lane->thread.joinable()) {
                    lane->thread.join();
                }
            }
            SetFatal(std::string("ClickHouse Event sink start failed: ") +
                     exception.what());
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
    }

    [[nodiscard]] bool Stop(std::string* error) noexcept {
        std::unique_lock<std::shared_mutex> lifecycle_lock(
            lifecycle_mutex_);
        if (!started_.load(std::memory_order_acquire)) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        bool expected = false;
        if (stop_called_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            accepting_.store(false, std::memory_order_release);
            const std::uint64_t now = ingest::MonotonicNowNs();
            const std::uint64_t timeout =
                static_cast<std::uint64_t>(config_.shutdown_timeout_ms) *
                UINT64_C(1'000'000);
            shutdown_deadline_ns_.store(
                std::numeric_limits<std::uint64_t>::max() - now < timeout
                    ? std::numeric_limits<std::uint64_t>::max()
                    : now + timeout,
                std::memory_order_release);
            stopping_.store(true, std::memory_order_release);
            wake_.notify_all();
            for (const auto& lane : lanes_) {
                lane->wake.notify_all();
            }
        }
        for (auto& lane : lanes_) {
            if (lane->thread.joinable()) {
                lane->thread.join();
            }
        }
        for (const auto& lane : lanes_) {
            std::lock_guard<std::mutex> lock(lane->mutex);
            if (!lane->queue.empty()) {
                SetFatal("ClickHouse Event sink stopped with queued batches");
                break;
            }
        }
        {
            std::lock_guard<std::mutex> budget_lock(queue_budget_mutex_);
            if (queued_batches_ != 0U || queued_rows_ != 0U) {
                SetFatal("ClickHouse Event sink queue budget not empty");
            }
        }
        if (error != nullptr) {
            *error = healthy() ? std::string{} : fatal_error();
        }
        return healthy();
    }

    [[nodiscard]] bool AppendRevisionBatch(
        std::shared_ptr<const EventRevisionBatch> batch) noexcept {
        std::shared_lock<std::shared_mutex> lifecycle_lock(
            lifecycle_mutex_);
        if (!accepting_.load(std::memory_order_acquire) || !healthy() ||
            batch == nullptr || !ValidRevisionBatch(*batch)) {
            return false;
        }
        try {
            const std::size_t rows = batch->revisions.size();
            const std::size_t lane_index =
                static_cast<std::size_t>(batch->owner) % lanes_.size();
            {
                std::lock_guard<std::mutex> budget_lock(queue_budget_mutex_);
                if (queued_batches_ >= config_.queue_revision_batches ||
                    queued_rows_ > config_.queue_revision_rows ||
                    rows > config_.queue_revision_rows - queued_rows_) {
                    SetFatal(
                        "ClickHouse Event revision queue capacity exhausted");
                    return false;
                }
                ++queued_batches_;
                queued_rows_ += rows;
            }
            try {
                Lane& lane = *lanes_[lane_index];
                {
                    std::lock_guard<std::mutex> lane_lock(lane.mutex);
                    lane.queue.push_back(std::move(batch));
                }
                stats_.revision_batches_queued.fetch_add(
                    1U, std::memory_order_relaxed);
                stats_.revision_rows_queued.fetch_add(
                    static_cast<std::uint64_t>(rows),
                    std::memory_order_relaxed);
                lane.wake.notify_one();
                return true;
            } catch (...) {
                std::lock_guard<std::mutex> budget_lock(queue_budget_mutex_);
                --queued_batches_;
                queued_rows_ -= rows;
                throw;
            }
        } catch (...) {
            SetFatal("ClickHouse Event revision queue allocation failed");
            return false;
        }
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] EventClickHouseStats stats() const noexcept {
        EventClickHouseStats result{};
        result.revision_batches_queued =
            stats_.revision_batches_queued.load(std::memory_order_relaxed);
        result.revision_batches_acked =
            stats_.revision_batches_acked.load(std::memory_order_relaxed);
        result.revision_batches_released =
            stats_.revision_batches_released.load(std::memory_order_relaxed);
        result.revision_rows_queued =
            stats_.revision_rows_queued.load(std::memory_order_relaxed);
        result.revision_rows_acked =
            stats_.revision_rows_acked.load(std::memory_order_relaxed);
        result.revision_chunks_acked =
            stats_.revision_chunks_acked.load(std::memory_order_relaxed);
        result.recovery_runs_committed =
            stats_.recovery_runs_committed.load(std::memory_order_relaxed);
        result.retry_attempts =
            stats_.retry_attempts.load(std::memory_order_relaxed);
        result.unknown_outcomes =
            stats_.unknown_outcomes.load(std::memory_order_relaxed);
        result.bytes_sent =
            stats_.bytes_sent.load(std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(queue_budget_mutex_);
        result.queued_revision_rows = queued_rows_;
        return result;
    }

    [[nodiscard]] Identifier128 writer_instance_id() const noexcept {
        return writer_instance_id_;
    }

    [[nodiscard]] const EventClickHouseConfig& config() const noexcept {
        return config_;
    }

private:
    [[nodiscard]] bool ExactInsert(
        HttpClient* client,
        std::string_view table,
        std::string_view columns,
        const std::string& query_id,
        const std::string& token,
        std::span<const std::byte> payload) {
        std::uint64_t retry_started_ns = 0U;
        std::uint32_t backoff = config_.retry_initial_backoff_ms;
        for (;;) {
            const HttpResult result = client->Insert(
                table, columns, query_id, token, payload);
            stats_.bytes_sent.fetch_add(
                static_cast<std::uint64_t>(payload.size()),
                std::memory_order_relaxed);
            if (HttpSucceeded(result)) {
                return true;
            }
            if (!IsRetryable(result)) {
                SetFatal("ClickHouse Event INSERT failed permanently: " +
                         HttpErrorText(result));
                return false;
            }
            stats_.unknown_outcomes.fetch_add(1U,
                                              std::memory_order_relaxed);
            const std::uint64_t now = ingest::MonotonicNowNs();
            if (retry_started_ns == 0U) {
                retry_started_ns = now;
            }
            const std::uint64_t retry_limit =
                static_cast<std::uint64_t>(
                    config_.maximum_retry_elapsed_ms) *
                UINT64_C(1'000'000);
            if ((now >= retry_started_ns &&
                 now - retry_started_ns >= retry_limit) ||
                (stopping_.load(std::memory_order_acquire) &&
                 now >= shutdown_deadline_ns_.load(
                     std::memory_order_acquire))) {
                SetFatal("ClickHouse Event INSERT retry budget exhausted: " +
                         HttpErrorText(result));
                return false;
            }
            stats_.retry_attempts.fetch_add(1U,
                                            std::memory_order_relaxed);
            std::unique_lock<std::mutex> lock(wake_mutex_);
            wake_.wait_for(lock, std::chrono::milliseconds(backoff));
            backoff = std::min(
                config_.retry_max_backoff_ms,
                backoff > config_.retry_max_backoff_ms / 2U
                    ? config_.retry_max_backoff_ms
                    : backoff * 2U);
        }
    }

    [[nodiscard]] bool ProcessBatch(const EventRevisionBatch& batch,
                                    HttpClient* client,
                                    std::size_t lane_index) {
        const std::uint64_t sink_sequence =
            next_sink_batch_sequence_.fetch_add(1U, std::memory_order_relaxed);
        if (sink_sequence == 0U) {
            SetFatal("ClickHouse Event sink batch sequence exhausted");
            return false;
        }
        const std::string writer = IdentifierString(writer_instance_id_);
        const std::string lane_text = std::to_string(lane_index);
        std::uint32_t chunk_index = 0U;
        for (std::size_t begin = 0U; begin < batch.revisions.size();
             begin += config_.insert_chunk_rows, ++chunk_index) {
            const std::size_t end = std::min(
                batch.revisions.size(), begin + config_.insert_chunk_rows);
            RevisionChunkMetadata metadata{};
            metadata.sink_batch_sequence = sink_sequence;
            metadata.chunk_index = chunk_index;
            metadata.trade_date = batch.revisions[begin].key.trade_date;
            metadata.batch_id = ChunkIdentifier(
                writer_instance_id_, batch.recovery_run_id, chunk_index,
                event::kEventSchemaVersion, 1U);
            RowBinaryWriter payload((end - begin) * 512U);
            for (std::size_t row = begin; row < end; ++row) {
                if (!AppendRevisionRow(
                        &payload, batch.revisions[row], writer_instance_id_,
                        metadata, static_cast<std::uint32_t>(row))) {
                    SetFatal("ClickHouse Event revision serialization failed");
                    return false;
                }
            }
            const std::string query_id =
                "l2flow/event_revision_log/" + writer + "/" +
                lane_text + "/" +
                std::to_string(sink_sequence) + "/" +
                std::to_string(chunk_index);
            const std::string token =
                "l2flow/event_revision_log/" +
                IdentifierString(metadata.batch_id) + "/" +
                std::to_string(event::kEventSchemaVersion);
            if (!ExactInsert(client, "event_revision_log",
                             RevisionColumnNames(), query_id, token,
                             payload.bytes())) {
                return false;
            }
            stats_.revision_chunks_acked.fetch_add(
                1U, std::memory_order_relaxed);
        }

        const EventRevision& first = batch.revisions.front();
        const EventRevision& last = batch.revisions.back();
        std::uint16_t days = 0U;
        if (!TradeDateToDays(first.key.trade_date, &days)) {
            SetFatal("ClickHouse Event recovery marker date is invalid");
            return false;
        }
        const Identifier128 commit_id = ChunkIdentifier(
            writer_instance_id_, batch.recovery_run_id, 0U,
            event::kEventSchemaVersion, 2U);
        RowBinaryWriter marker(160U);
        marker.Append(days);
        marker.AppendIdentifier(batch.calculation_run_id);
        marker.AppendIdentifier(batch.recovery_run_id);
        marker.Append(batch.owner);
        marker.Append(batch.batch_sequence);
        marker.AppendEnum(batch.reason);
        marker.Append(first.version);
        marker.Append(last.version);
        marker.Append(static_cast<std::uint64_t>(batch.revisions.size()));
        marker.Append(chunk_index);
        marker.AppendBool(true);
        marker.Append(SystemUtcNowNs());
        marker.AppendIdentifier(writer_instance_id_);
        marker.AppendIdentifier(commit_id);
        marker.Append(event::kEventSchemaVersion);
        constexpr std::string_view marker_columns =
            "trade_date,calculation_run_id,recovery_run_id,owner,"
            "calculation_batch_sequence,revision_reason,minimum_version,"
            "maximum_version,revision_count,chunk_count,committed,"
            "committed_utc_ns,writer_instance_id,commit_id,schema_version";
        const std::string marker_query_id =
            "l2flow/event_recovery_run/" + writer + "/" +
            lane_text + "/" +
            IdentifierString(batch.recovery_run_id);
        const std::string marker_token =
            "l2flow/event_recovery_run/" + IdentifierString(commit_id) +
            "/" + std::to_string(event::kEventSchemaVersion);
        if (!ExactInsert(client, "event_recovery_run", marker_columns,
                         marker_query_id, marker_token, marker.bytes())) {
            return false;
        }
        stats_.recovery_runs_committed.fetch_add(
            1U, std::memory_order_relaxed);
        stats_.revision_rows_acked.fetch_add(
            static_cast<std::uint64_t>(batch.revisions.size()),
            std::memory_order_relaxed);
        stats_.revision_batches_acked.fetch_add(
            1U, std::memory_order_release);
        return true;
    }

    void WriterLoop(std::size_t lane_index) noexcept {
        try {
            HttpClient client(config_);
            Lane& lane = *lanes_[lane_index];
            for (;;) {
                std::shared_ptr<const EventRevisionBatch> batch;
                {
                    std::unique_lock<std::mutex> lock(lane.mutex);
                    lane.wake.wait_for(
                        lock, std::chrono::milliseconds(1), [this, &lane] {
                            return !lane.queue.empty() ||
                                stopping_.load(std::memory_order_acquire) ||
                                !healthy();
                        });
                    if (lane.queue.empty()) {
                        if (stopping_.load(std::memory_order_acquire) ||
                            !healthy()) {
                            return;
                        }
                        continue;
                    }
                    batch = lane.queue.front();
                }
                if (!healthy() ||
                    !ProcessBatch(*batch, &client, lane_index)) {
                    return;
                }
                {
                    std::lock_guard<std::mutex> lock(lane.mutex);
                    if (lane.queue.empty() || lane.queue.front() != batch) {
                        SetFatal("ClickHouse Event queue invariant failed");
                        return;
                    }
                    lane.queue.pop_front();
                }
                {
                    std::lock_guard<std::mutex> budget_lock(
                        queue_budget_mutex_);
                    if (queued_batches_ == 0U ||
                        queued_rows_ < batch->revisions.size()) {
                        SetFatal("ClickHouse Event queue budget invariant failed");
                        return;
                    }
                    --queued_batches_;
                    queued_rows_ -= batch->revisions.size();
                }
                stats_.revision_batches_released.fetch_add(
                    1U, std::memory_order_release);
                lane.wake.notify_all();
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("ClickHouse Event writer failed: ") +
                     exception.what());
        } catch (...) {
            SetFatal("ClickHouse Event writer failed with unknown exception");
        }
    }

    void SetFatal(std::string message) noexcept {
        accepting_.store(false, std::memory_order_release);
        bool expected = true;
        if (!healthy_.compare_exchange_strong(
                expected, false, std::memory_order_acq_rel)) {
            return;
        }
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            fatal_error_ = std::move(message);
        } catch (...) {
        }
        wake_.notify_all();
        for (const auto& lane : lanes_) {
            lane->wake.notify_all();
        }
    }

    EventClickHouseConfig config_{};
    Identifier128 writer_instance_id_{};
    AtomicStats stats_{};
    std::vector<std::unique_ptr<Lane>> lanes_;
    mutable std::mutex queue_budget_mutex_;
    mutable std::shared_mutex lifecycle_mutex_;
    std::size_t queued_batches_ = 0U;
    std::size_t queued_rows_ = 0U;
    std::atomic<std::uint64_t> next_sink_batch_sequence_{1U};
    std::atomic<std::uint64_t> shutdown_deadline_ns_{
        std::numeric_limits<std::uint64_t>::max()};
    std::atomic<bool> started_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stop_called_{false};
    std::atomic<bool> healthy_{true};
    mutable std::mutex fatal_mutex_;
    std::string fatal_error_;
    std::mutex wake_mutex_;
    std::condition_variable wake_;
};

std::unique_ptr<EventClickHouseSink> EventClickHouseSink::Create(
    EventClickHouseConfig config,
    std::string* error) {
    if (!ValidateEventClickHouseConfig(config, error)) {
        return nullptr;
    }
    try {
        while (config.endpoint.size() >
                   std::string_view("http://").size() &&
               config.endpoint.ends_with('/')) {
            config.endpoint.pop_back();
        }
        auto impl = std::make_unique<Impl>(
            std::move(config), GenerateIdentifier());
        if (!impl->Initialize(error)) {
            return nullptr;
        }
        return std::unique_ptr<EventClickHouseSink>(
            new EventClickHouseSink(std::move(impl)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("ClickHouse Event sink creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

EventClickHouseSink::EventClickHouseSink(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

EventClickHouseSink::~EventClickHouseSink() {
    std::string ignored;
    static_cast<void>(impl_->Stop(&ignored));
}

bool EventClickHouseSink::Start(std::string* error) {
    return impl_->Start(error);
}

bool EventClickHouseSink::Stop(std::string* error) noexcept {
    return impl_->Stop(error);
}

bool EventClickHouseSink::AppendRevisionBatch(
    std::shared_ptr<const EventRevisionBatch> batch) noexcept {
    return impl_->AppendRevisionBatch(std::move(batch));
}

bool EventClickHouseSink::healthy() const noexcept {
    return impl_->healthy();
}

std::string EventClickHouseSink::fatal_error() const {
    return impl_->fatal_error();
}

EventClickHouseStats EventClickHouseSink::stats() const noexcept {
    return impl_->stats();
}

Identifier128 EventClickHouseSink::writer_instance_id() const noexcept {
    return impl_->writer_instance_id();
}

const EventClickHouseConfig& EventClickHouseSink::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::clickhouse
