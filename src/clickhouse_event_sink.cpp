#include "l2flow/clickhouse/event_sink.h"

#include "l2flow/clickhouse/freshness.h"
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
#include <set>
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
// Event RowBinary contains only fixed-width scalar, FixedString, and Tuple
// fields. These constants are checked against every serialized row before a
// request can be sent, so a schema edit cannot silently invalidate grouping.
constexpr std::size_t kRevisionRowBinaryBytes = 665U;
constexpr std::size_t kRecoveryMarkerRowBinaryBytes = 140U;

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

[[nodiscard]] Identifier128 PhysicalRequestIdentifier(
    Identifier128 writer,
    std::uint64_t group_sequence,
    std::uint32_t lane,
    std::uint32_t request,
    std::uint32_t schema_version,
    std::uint8_t table_tag) noexcept {
    std::array<std::byte, 37U> input{};
    std::copy(writer.bytes.begin(), writer.bytes.end(), input.begin());
    std::size_t offset = writer.bytes.size();
    const auto append = [&input, &offset](auto value) {
        using Value = decltype(value);
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            input[offset++] = static_cast<std::byte>(
                static_cast<std::uint64_t>(value) >> (index * 8U));
        }
    };
    append(group_sequence);
    append(lane);
    append(request);
    append(schema_version);
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

    [[nodiscard]] std::size_t size() const noexcept { return bytes_.size(); }

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
    std::uint16_t trade_date_days = 0U;
};

[[nodiscard]] bool AppendRevisionRow(
    RowBinaryWriter* writer,
    const EventRevision& revision,
    Identifier128 writer_instance_id,
    const RevisionChunkMetadata& metadata,
    std::uint32_t row_index) {
    writer->Append(metadata.trade_date_days);
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

[[nodiscard]] bool AppendRecoveryMarkerRow(
    RowBinaryWriter* writer,
    const EventRevisionBatch& batch,
    Identifier128 writer_instance_id,
    std::uint16_t trade_date_days,
    Identifier128 commit_id,
    std::uint32_t chunk_count,
    std::uint64_t committed_utc_ns) {
    if (batch.revisions.empty() || chunk_count == 0U) {
        return false;
    }
    writer->Append(trade_date_days);
    writer->AppendIdentifier(batch.calculation_run_id);
    writer->AppendIdentifier(batch.recovery_run_id);
    writer->Append(batch.owner);
    writer->Append(batch.batch_sequence);
    const outbox::WalPosition input_max = batch.input_positions.back();
    writer->Append(input_max.lsn);
    writer->Append(input_max.batch_sequence);
    writer->Append(input_max.row_index);
    writer->AppendEnum(batch.reason);
    writer->Append(batch.revisions.front().version);
    writer->Append(batch.revisions.back().version);
    writer->Append(static_cast<std::uint64_t>(batch.revisions.size()));
    writer->Append(chunk_count);
    writer->AppendBool(true);
    writer->Append(committed_utc_ns);
    writer->AppendIdentifier(writer_instance_id);
    writer->AppendIdentifier(commit_id);
    writer->Append(event::kEventSchemaVersion);
    return true;
}

constexpr std::string_view kRecoveryMarkerColumns =
    "trade_date,calculation_run_id,recovery_run_id,owner,"
    "calculation_batch_sequence,input_max_lsn,input_max_batch_sequence,"
    "input_max_row_index,revision_reason,minimum_version,"
    "maximum_version,revision_count,chunk_count,committed,"
    "committed_utc_ns,writer_instance_id,commit_id,schema_version";

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
    const std::string qualified = std::string(database) + ".";
    return "CREATE VIEW IF NOT EXISTS " + qualified +
           "event AS SELECT " + RevisionColumnNames() +
           " FROM (SELECT r.*, row_number() OVER (PARTITION BY "
           "r.trade_date,r.market,r.instrument_id,r.channel,"
           "r.native_sequence,r.event_kind,r.affected_order_id,"
           "r.occurrence ORDER BY r.version DESC,"
           "r.batch_sequence DESC,r.chunk_index DESC,r.row_index DESC,"
           "r.revision_id DESC) AS _l2flow_rank FROM " + qualified +
           "event_revision_log AS r INNER JOIN (SELECT trade_date AS "
           "committed_trade_date,calculation_run_id AS "
           "committed_calculation_run_id,recovery_run_id AS "
           "committed_recovery_run_id,input_max_lsn,"
           "input_max_batch_sequence,input_max_row_index FROM " + qualified +
           "event_recovery_run WHERE committed GROUP BY trade_date,"
           "calculation_run_id,recovery_run_id,input_max_lsn,"
           "input_max_batch_sequence,input_max_row_index) AS c ON "
           "r.trade_date=c.committed_trade_date AND "
           "r.calculation_run_id=c.committed_calculation_run_id AND "
           "r.recovery_run_id=c.committed_recovery_run_id INNER JOIN "
           "(SELECT calculation_run_id AS freshness_calculation_run_id,"
           "tupleElement(latest,3) AS cursor_lsn,"
           "tupleElement(latest,4) AS cursor_batch_sequence,"
           "tupleElement(latest,5) AS cursor_row_index FROM (SELECT "
           "calculation_run_id,latest,count() OVER () AS active_run_count "
           "FROM (SELECT calculation_run_id,argMax(tuple(authoritative,"
           "valid_until_utc_ns,event_lsn,event_batch_sequence,event_row_index),"
           "tuple(publication_sequence,observed_utc_ns,"
           "publisher_instance_id)) AS latest FROM " + qualified +
           "derived_freshness_log WHERE domain=1 GROUP BY "
           "calculation_run_id) WHERE tupleElement(latest,1)=1 AND "
           "tupleElement(latest,2)>=toUInt64(toUnixTimestamp64Nano("
           "now64(9)))) WHERE active_run_count=1) AS f ON "
           "r.calculation_run_id=f.freshness_calculation_run_id AND tuple("
           "c.input_max_lsn,c.input_max_batch_sequence,c.input_max_row_index)"
           "<=tuple(f.cursor_lsn,f.cursor_batch_sequence,f.cursor_row_index)) "
           "WHERE "
           "_l2flow_rank=1 AND is_deleted=0";
}

[[nodiscard]] std::string RecoveryRunDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
           ".event_recovery_run (trade_date Date, "
           "calculation_run_id FixedString(16), "
           "recovery_run_id FixedString(16), owner UInt32, "
           "calculation_batch_sequence UInt64, input_max_lsn UInt64, "
           "input_max_batch_sequence UInt64, input_max_row_index UInt32, "
           "revision_reason UInt8, "
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
    constexpr std::string_view recovery_sort =
        "calculation_run_id, recovery_run_id";
    const std::string revision_source =
        std::string(database) + ".event_revision_log";
    const std::string marker_source =
        std::string(database) + ".event_recovery_run";
    const std::string freshness_source =
        std::string(database) + ".derived_freshness_log";
    return "SELECT throwIf(count() != 3 OR "
           "countIf(name='event_revision_log' AND engine NOT IN "
           "('MergeTree','ReplicatedMergeTree')) != 0 OR "
           "countIf(name='event' AND engine!='View') != 0 OR "
           "countIf(name='event_recovery_run' AND engine NOT IN "
           "('MergeTree','ReplicatedMergeTree')) != 0 OR "
           "countIf(name IN ('event_revision_log',"
           "'event_recovery_run') AND partition_key!='trade_date') != 0 OR "
           "countIf(name='event_revision_log' AND sorting_key!='" +
           std::string(revision_sort) + "') != 0 OR "
           "countIf(name='event_recovery_run' AND sorting_key!='" +
           std::string(recovery_sort) + "') != 0 OR "
           "countIf(name='event' AND ("
           "position(create_table_query,'" + revision_source +
           "')=0 OR position(create_table_query,'" + marker_source +
           "')=0 OR position(create_table_query,'" + freshness_source +
           "')=0 OR position(create_table_query,'input_max_lsn')=0 OR "
           "position(create_table_query,'event_lsn')=0 OR "
           "position(create_table_query,'valid_until_utc_ns')=0 OR "
           "position(create_table_query,'active_run_count')=0)) != 0, "
           "'Event table engine or partition contract mismatch') FROM "
           "system.tables WHERE database='" + std::string(database) +
           "' AND name IN ('event_revision_log','event',"
           "'event_recovery_run') FORMAT Null";
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
    std::string sql = "SELECT throwIf(count() != 18";
    AppendColumnTypeCondition("trade_date", "Date", &sql);
    AppendColumnTypeCondition(
        "calculation_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition(
        "recovery_run_id", "FixedString(16)", &sql);
    AppendColumnTypeCondition("owner", "UInt32", &sql);
    AppendColumnTypeCondition(
        "calculation_batch_sequence", "UInt64", &sql);
    AppendColumnTypeCondition("input_max_lsn", "UInt64", &sql);
    AppendColumnTypeCondition(
        "input_max_batch_sequence", "UInt64", &sql);
    AppendColumnTypeCondition("input_max_row_index", "UInt32", &sql);
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
    std::uint64_t perform_ns = 0U;
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
        const std::uint64_t perform_started_ns = ingest::MonotonicNowNs();
        result.curl_code = ::curl_easy_perform(handle_);
        const std::uint64_t perform_completed_ns = ingest::MonotonicNowNs();
        result.perform_ns = perform_completed_ns >= perform_started_ns
            ? perform_completed_ns - perform_started_ns
            : 0U;
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
    std::atomic<std::uint64_t> submission_groups_queued{0U};
    std::atomic<std::uint64_t> submission_groups_released{0U};
    std::atomic<std::uint64_t> revision_batches_queued{0U};
    std::atomic<std::uint64_t> revision_batches_acked{0U};
    std::atomic<std::uint64_t> revision_batches_released{0U};
    std::atomic<std::uint64_t> revision_rows_queued{0U};
    std::atomic<std::uint64_t> revision_rows_acked{0U};
    std::atomic<std::uint64_t> physical_groups_committed{0U};
    std::atomic<std::uint64_t> revision_insert_requests_acked{0U};
    std::atomic<std::uint64_t> marker_insert_requests_acked{0U};
    std::atomic<std::uint64_t> recovery_runs_committed{0U};
    std::atomic<std::uint64_t> retry_attempts{0U};
    std::atomic<std::uint64_t> unknown_outcomes{0U};
    std::atomic<std::uint64_t> bytes_sent{0U};
    std::atomic<std::uint64_t> physical_group_batches_max{0U};
    std::atomic<std::uint64_t> physical_group_rows_max{0U};
    std::atomic<std::uint64_t> revision_request_rows_max{0U};
    std::atomic<std::uint64_t> revision_request_bytes_max{0U};
    std::atomic<std::uint64_t> admission_validation_ns{0U};
    std::atomic<std::uint64_t> queue_budget_wait_ns{0U};
    std::atomic<std::uint64_t> queue_budget_wait_count{0U};
    std::atomic<std::uint64_t> queue_budget_wait_ns_max{0U};
    std::atomic<std::uint64_t> group_wait_ns{0U};
    std::atomic<std::uint64_t> rowbinary_serialize_ns{0U};
    std::atomic<std::uint64_t> chunk_id_ns{0U};
    std::atomic<std::uint64_t> request_state_before_send_ns{0U};
    std::atomic<std::uint64_t> curl_easy_perform_ns{0U};
    std::atomic<std::uint64_t> request_state_after_ack_ns{0U};
    std::atomic<std::uint64_t> revision_http_ns{0U};
    std::atomic<std::uint64_t> marker_http_ns{0U};
    std::atomic<std::uint64_t> completion_ns{0U};
    std::atomic<std::uint64_t> retire_ns{0U};
};

void PublishMaximum(std::atomic<std::uint64_t>* target,
                    std::uint64_t value) noexcept {
    std::uint64_t current = target->load(std::memory_order_relaxed);
    while (current < value &&
           !target->compare_exchange_weak(
               current, value, std::memory_order_relaxed)) {
    }
}

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
    if (key.instrument_id == 0U || key.native_sequence == 0U ||
        (key.market != event::Market::kShanghai &&
         key.market != event::Market::kShenzhen)) {
        return false;
    }
    if (key.market == event::Market::kShanghai && key.channel == 0U) {
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
        batch.input_positions.empty() ||
        !ValidRevisionReason(batch.reason)) {
        return false;
    }
    for (std::size_t index = 0U; index < batch.input_positions.size();
         ++index) {
        if (batch.input_positions[index].lsn == 0U ||
            (index != 0U &&
             !(batch.input_positions[index - 1U] <
               batch.input_positions[index]))) {
            return false;
        }
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

std::string EventCurrentViewDdl(std::string_view database) {
    return CurrentDdl(database);
}

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
        config.writer_lanes == 4U || config.writer_lanes == 8U ||
        config.writer_lanes == 16U || config.writer_lanes == 32U;
    std::string spool_error;
    if (!valid_writer_lanes ||
        config.writer_lanes > kMaximumEventWriterLanes ||
        config.insert_request_max_rows == 0U ||
        config.insert_request_max_bytes < kRevisionRowBinaryBytes ||
        config.physical_group_max_batches == 0U ||
        config.physical_group_max_rows == 0U ||
        config.physical_group_max_revision_bytes <
            kRevisionRowBinaryBytes ||
        config.physical_group_max_delay_ns == 0U ||
        config.physical_group_max_delay_ns > UINT64_C(1'000'000'000) ||
        config.queue_revision_batches == 0U ||
        config.queue_revision_rows == 0U ||
        config.insert_request_max_rows > config.queue_revision_rows ||
        config.queue_revision_rows >=
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.connect_timeout_ms == 0U ||
        config.request_timeout_ms == 0U ||
        config.retry_initial_backoff_ms == 0U ||
        config.retry_max_backoff_ms < config.retry_initial_backoff_ms ||
        config.shutdown_timeout_ms < config.request_timeout_ms ||
        config.completion_sink == nullptr ||
        !outbox::ValidateRequestSpoolConfig(
            config.request_spool, &spool_error)) {
        return fail("invalid ClickHouse Event queue or retry configuration");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

class EventClickHouseSink::Impl final {
public:
    struct QueuedSubmission final {
        std::vector<std::shared_ptr<const EventRevisionBatch>> batches;
        std::size_t first_batch = 0U;
        std::uint64_t enqueued_monotonic_ns = 0U;
    };

    struct PreparedBatch final {
        std::shared_ptr<const EventRevisionBatch> batch;
        std::uint64_t sink_batch_sequence = 0U;
        std::uint32_t chunk_count = 0U;
        std::uint16_t trade_date_days = 0U;
    };

    struct InsertRequest final {
        explicit InsertRequest(std::size_t reserve_bytes)
            : payload(reserve_bytes) {}

        RowBinaryWriter payload;
        std::string query_id;
        std::string dedup_token;
        std::size_t rows = 0U;
    };

    struct PhysicalInsertGroup final {
        explicit PhysicalInsertGroup(std::uint64_t sequence,
                                     std::size_t marker_reserve)
            : group_sequence(sequence), marker_payload(marker_reserve) {}

        std::uint64_t group_sequence = 0U;
        std::vector<PreparedBatch> batches;
        std::vector<InsertRequest> revision_requests;
        RowBinaryWriter marker_payload;
        std::string marker_query_id;
        std::string marker_dedup_token;
        std::vector<outbox::WalPosition> input_positions;
        outbox::RequestGroupHandle spool_handle{};
        std::size_t revision_rows = 0U;
    };

    struct Lane final {
        mutable std::mutex mutex;
        std::condition_variable wake;
        std::deque<QueuedSubmission> queue;
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
            request_spool_ = outbox::RequestSpool::Create(
                config_.request_spool, error);
            if (request_spool_ == nullptr) {
                return false;
            }
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
                 !execute(RecoveryRunDdl(config_.database)) ||
                 !execute(DerivedFreshnessLogDdl(config_.database)) ||
                 !execute(EventCurrentViewDdl(config_.database)))) {
                SetFatal("ClickHouse Event schema initialization failed");
                return false;
            }
            if (!execute(EventTableProbe(config_.database)) ||
                !execute(DerivedFreshnessTableProbe(config_.database)) ||
                !execute(DerivedFreshnessColumnProbe(config_.database)) ||
                !execute(RevisionColumnProbe(
                    config_.database, "event_revision_log")) ||
                !execute(RevisionColumnProbe(config_.database, "event")) ||
                !execute(RecoveryRunColumnProbe(config_.database))) {
                SetFatal("ClickHouse Event schema validation failed");
                return false;
            }
            if (!request_spool_->Start(error)) {
                SetFatal("ClickHouse Event request spool start failed: " +
                         request_spool_->fatal_error());
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
            std::string ignored;
            static_cast<void>(request_spool_->Stop(&ignored));
            SetFatal(std::string("ClickHouse Event sink start failed: ") +
                     exception.what());
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
    }

    [[nodiscard]] bool Stop(std::string* error) noexcept {
        accepting_.store(false, std::memory_order_release);
        queue_budget_available_.notify_all();
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
            if (queued_batches_ != 0U || queued_rows_ != 0U ||
                queued_submission_groups_ != 0U) {
                SetFatal("ClickHouse Event sink queue budget not empty");
            }
        }
        std::string spool_error;
        if (!request_spool_->Stop(&spool_error)) {
            SetFatal("ClickHouse Event request spool stop failed: " +
                     spool_error);
        }
        if (error != nullptr) {
            *error = healthy() ? std::string{} : fatal_error();
        }
        return healthy();
    }

    [[nodiscard]] bool AppendRevisionGroup(
        std::vector<std::shared_ptr<const EventRevisionBatch>> batches)
        noexcept {
        std::shared_lock<std::shared_mutex> lifecycle_lock(
            lifecycle_mutex_);
        if (!accepting_.load(std::memory_order_acquire) || !healthy() ||
            batches.empty()) {
            return false;
        }
        try {
            const std::uint64_t validation_started_ns =
                ingest::MonotonicNowNs();
            const std::size_t batch_count = batches.size();
            const std::uint32_t owner = batches.front() == nullptr
                ? std::numeric_limits<std::uint32_t>::max()
                : batches.front()->owner;
            std::size_t rows = 0U;
            bool valid = true;
            for (const auto& batch : batches) {
                if (batch == nullptr || batch->owner != owner ||
                    !ValidRevisionBatch(*batch) ||
                    batch->revisions.size() >
                        std::numeric_limits<std::size_t>::max() - rows) {
                    valid = false;
                    break;
                }
                rows += batch->revisions.size();
            }
            const std::uint64_t validation_completed_ns =
                ingest::MonotonicNowNs();
            stats_.admission_validation_ns.fetch_add(
                validation_completed_ns >= validation_started_ns
                    ? validation_completed_ns - validation_started_ns
                    : 0U,
                std::memory_order_relaxed);
            if (!valid) {
                return false;
            }
            const std::size_t lane_index =
                static_cast<std::size_t>(owner) % lanes_.size();
            {
                if (batch_count > config_.queue_revision_batches ||
                    rows > config_.queue_revision_rows) {
                    SetFatal(
                        "ClickHouse Event immutable group exceeds queue bound");
                    return false;
                }
                std::unique_lock<std::mutex> budget_lock(
                    queue_budget_mutex_);
                const auto ready = [this, batch_count, rows] {
                    return !accepting_.load(std::memory_order_acquire) ||
                           !healthy() ||
                           (queued_batches_ <=
                                config_.queue_revision_batches - batch_count &&
                            queued_rows_ <=
                                config_.queue_revision_rows - rows);
                };
                if (!ready()) {
                    const std::uint64_t wait_started_ns =
                        ingest::MonotonicNowNs();
                    queue_budget_available_.wait(budget_lock, ready);
                    const std::uint64_t wait_completed_ns =
                        ingest::MonotonicNowNs();
                    const std::uint64_t waited_ns =
                        wait_completed_ns >= wait_started_ns
                            ? wait_completed_ns - wait_started_ns
                            : 0U;
                    stats_.queue_budget_wait_ns.fetch_add(
                        waited_ns, std::memory_order_relaxed);
                    stats_.queue_budget_wait_count.fetch_add(
                        1U, std::memory_order_relaxed);
                    PublishMaximum(
                        &stats_.queue_budget_wait_ns_max, waited_ns);
                }
                if (!accepting_.load(std::memory_order_acquire) ||
                    !healthy()) {
                    return false;
                }
                queued_batches_ += batch_count;
                queued_rows_ += rows;
                ++queued_submission_groups_;
                queued_batches_high_water_ = std::max(
                    queued_batches_high_water_, queued_batches_);
                queued_rows_high_water_ = std::max(
                    queued_rows_high_water_, queued_rows_);
                queued_submission_groups_high_water_ = std::max(
                    queued_submission_groups_high_water_,
                    queued_submission_groups_);
            }
            try {
                Lane& lane = *lanes_[lane_index];
                {
                    std::lock_guard<std::mutex> lane_lock(lane.mutex);
                    lane.queue.push_back(QueuedSubmission{
                        std::move(batches), 0U, ingest::MonotonicNowNs()});
                }
                stats_.submission_groups_queued.fetch_add(
                    1U, std::memory_order_relaxed);
                stats_.revision_batches_queued.fetch_add(
                    batch_count, std::memory_order_relaxed);
                stats_.revision_rows_queued.fetch_add(
                    static_cast<std::uint64_t>(rows),
                    std::memory_order_relaxed);
                lane.wake.notify_one();
                return true;
            } catch (...) {
                std::lock_guard<std::mutex> budget_lock(queue_budget_mutex_);
                queued_batches_ -= batch_count;
                queued_rows_ -= rows;
                --queued_submission_groups_;
                queue_budget_available_.notify_all();
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
        result.submission_groups_queued =
            stats_.submission_groups_queued.load(std::memory_order_relaxed);
        result.submission_groups_released =
            stats_.submission_groups_released.load(
                std::memory_order_relaxed);
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
        result.physical_groups_committed =
            stats_.physical_groups_committed.load(std::memory_order_relaxed);
        result.revision_insert_requests_acked =
            stats_.revision_insert_requests_acked.load(
                std::memory_order_relaxed);
        result.marker_insert_requests_acked =
            stats_.marker_insert_requests_acked.load(
                std::memory_order_relaxed);
        result.recovery_runs_committed =
            stats_.recovery_runs_committed.load(std::memory_order_relaxed);
        result.retry_attempts =
            stats_.retry_attempts.load(std::memory_order_relaxed);
        result.unknown_outcomes =
            stats_.unknown_outcomes.load(std::memory_order_relaxed);
        result.bytes_sent =
            stats_.bytes_sent.load(std::memory_order_relaxed);
        result.physical_group_batches_max =
            stats_.physical_group_batches_max.load(std::memory_order_relaxed);
        result.physical_group_rows_max =
            stats_.physical_group_rows_max.load(std::memory_order_relaxed);
        result.revision_request_rows_max =
            stats_.revision_request_rows_max.load(std::memory_order_relaxed);
        result.revision_request_bytes_max =
            stats_.revision_request_bytes_max.load(std::memory_order_relaxed);
        result.admission_validation_ns =
            stats_.admission_validation_ns.load(std::memory_order_relaxed);
        result.queue_budget_wait_ns =
            stats_.queue_budget_wait_ns.load(std::memory_order_relaxed);
        result.queue_budget_wait_count =
            stats_.queue_budget_wait_count.load(std::memory_order_relaxed);
        result.queue_budget_wait_ns_max =
            stats_.queue_budget_wait_ns_max.load(std::memory_order_relaxed);
        result.group_wait_ns =
            stats_.group_wait_ns.load(std::memory_order_relaxed);
        result.rowbinary_serialize_ns =
            stats_.rowbinary_serialize_ns.load(std::memory_order_relaxed);
        result.chunk_id_ns =
            stats_.chunk_id_ns.load(std::memory_order_relaxed);
        result.request_state_before_send_ns =
            stats_.request_state_before_send_ns.load(
                std::memory_order_relaxed);
        result.curl_easy_perform_ns =
            stats_.curl_easy_perform_ns.load(std::memory_order_relaxed);
        result.request_state_after_ack_ns =
            stats_.request_state_after_ack_ns.load(
                std::memory_order_relaxed);
        result.revision_http_ns =
            stats_.revision_http_ns.load(std::memory_order_relaxed);
        result.marker_http_ns =
            stats_.marker_http_ns.load(std::memory_order_relaxed);
        result.completion_ns =
            stats_.completion_ns.load(std::memory_order_relaxed);
        result.retire_ns = stats_.retire_ns.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(queue_budget_mutex_);
            result.queued_revision_batches = queued_batches_;
            result.queued_revision_rows = queued_rows_;
            result.queued_submission_groups = queued_submission_groups_;
            result.queued_revision_batches_high_water =
                queued_batches_high_water_;
            result.queued_revision_rows_high_water = queued_rows_high_water_;
            result.queued_submission_groups_high_water =
                queued_submission_groups_high_water_;
        }
        const outbox::RequestSpoolStats spool_stats =
            request_spool_->stats();
        result.spool_checksum_ns = spool_stats.checksum_ns;
        result.spool_encode_copy_ns = spool_stats.encode_copy_ns;
        result.spool_write_ns = spool_stats.write_ns;
        result.spool_fdatasync_ns = spool_stats.fdatasync_ns;
        result.spool_registry_lock_wait_ns =
            spool_stats.registry_lock_wait_ns;
        result.spool_entry_lock_wait_ns = spool_stats.entry_lock_wait_ns;
        result.request_spool_live_groups = spool_stats.live_groups;
        result.request_spool_preparing_groups =
            spool_stats.preparing_groups;
        result.request_spool_bytes = spool_stats.live_bytes;
        result.request_spool_reserved_bytes = spool_stats.reserved_bytes;
        return result;
    }

    [[nodiscard]] Identifier128 writer_instance_id() const noexcept {
        return writer_instance_id_;
    }

    [[nodiscard]] const EventClickHouseConfig& config() const noexcept {
        return config_;
    }

private:
    enum class InsertKind : std::uint8_t {
        kRevision,
        kMarker,
    };

    [[nodiscard]] bool SetRequestState(
        outbox::RequestGroupHandle handle,
        std::uint32_t request_index,
        outbox::RequestState state) noexcept {
        std::string error;
        if (request_spool_->SetState(
                handle, request_index, state, &error)) {
            return true;
        }
        SetFatal("ClickHouse Event request spool state failed: " + error);
        return false;
    }

    [[nodiscard]] bool ExactInsert(
        HttpClient* client,
        InsertKind kind,
        outbox::RequestGroupHandle spool_handle,
        std::uint32_t request_index,
        std::string_view table,
        std::string_view columns,
        const std::string& query_id,
        const std::string& token,
        std::span<const std::byte> payload) {
        std::uint32_t backoff = config_.retry_initial_backoff_ms;
        for (;;) {
            if (!healthy()) {
                return false;
            }
            const std::uint64_t state_before_started_ns =
                ingest::MonotonicNowNs();
            if (!SetRequestState(spool_handle, request_index,
                                 outbox::RequestState::kSent)) {
                return false;
            }
            const std::uint64_t state_before_completed_ns =
                ingest::MonotonicNowNs();
            stats_.request_state_before_send_ns.fetch_add(
                state_before_completed_ns >= state_before_started_ns
                    ? state_before_completed_ns - state_before_started_ns
                    : 0U,
                std::memory_order_relaxed);
            HttpResult result{};
            try {
                result = client->Insert(
                    table, columns, query_id, token, payload);
            } catch (...) {
                static_cast<void>(SetRequestState(
                    spool_handle, request_index,
                    outbox::RequestState::kUnknown));
                throw;
            }
            stats_.bytes_sent.fetch_add(
                static_cast<std::uint64_t>(payload.size()),
                std::memory_order_relaxed);
            stats_.curl_easy_perform_ns.fetch_add(
                result.perform_ns, std::memory_order_relaxed);
            (kind == InsertKind::kRevision
                 ? stats_.revision_http_ns
                 : stats_.marker_http_ns)
                .fetch_add(result.perform_ns, std::memory_order_relaxed);
            if (HttpSucceeded(result)) {
                const std::uint64_t state_after_started_ns =
                    ingest::MonotonicNowNs();
                if (!SetRequestState(spool_handle, request_index,
                                     outbox::RequestState::kAcked)) {
                    return false;
                }
                const std::uint64_t state_after_completed_ns =
                    ingest::MonotonicNowNs();
                stats_.request_state_after_ack_ns.fetch_add(
                    state_after_completed_ns >= state_after_started_ns
                        ? state_after_completed_ns - state_after_started_ns
                        : 0U,
                    std::memory_order_relaxed);
                if (kind == InsertKind::kRevision) {
                    stats_.revision_insert_requests_acked.fetch_add(
                        1U, std::memory_order_relaxed);
                } else {
                    stats_.marker_insert_requests_acked.fetch_add(
                        1U, std::memory_order_relaxed);
                }
                return true;
            }
            if (!IsRetryable(result)) {
                static_cast<void>(SetRequestState(
                    spool_handle, request_index,
                    outbox::RequestState::kBlocked));
                SetFatal("ClickHouse Event INSERT failed permanently: " +
                         HttpErrorText(result));
                return false;
            }
            if (!SetRequestState(spool_handle, request_index,
                                 outbox::RequestState::kUnknown)) {
                return false;
            }
            stats_.unknown_outcomes.fetch_add(1U,
                                              std::memory_order_relaxed);
            const std::uint64_t now = ingest::MonotonicNowNs();
            if (stopping_.load(std::memory_order_acquire) &&
                now >= shutdown_deadline_ns_.load(
                    std::memory_order_acquire)) {
                static_cast<void>(SetRequestState(
                    spool_handle, request_index,
                    outbox::RequestState::kBlocked));
                SetFatal("ClickHouse Event INSERT shutdown deadline reached: " +
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

    struct PrefixSelection final {
        std::size_t batches = 0U;
        std::size_t rows = 0U;
        bool closed = false;
    };

    [[nodiscard]] PrefixSelection SelectPrefix(const Lane& lane) const noexcept {
        PrefixSelection result{};
        std::size_t revision_bytes = 0U;
        const std::size_t marker_row_capacity = std::min(
            config_.insert_request_max_rows,
            config_.insert_request_max_bytes /
                kRecoveryMarkerRowBinaryBytes);
        for (const QueuedSubmission& submission : lane.queue) {
            for (std::size_t batch_index = submission.first_batch;
                 batch_index < submission.batches.size(); ++batch_index) {
                const std::size_t rows =
                    submission.batches[batch_index]->revisions.size();
                const bool first = result.batches == 0U;
                const bool oversized =
                    rows > config_.physical_group_max_rows ||
                    rows > config_.physical_group_max_revision_bytes /
                               kRevisionRowBinaryBytes;
                if (first && oversized) {
                    result.batches = 1U;
                    result.rows = rows;
                    result.closed = true;
                    return result;
                }
                const bool batch_limit =
                    result.batches >= config_.physical_group_max_batches;
                const bool marker_row_limit =
                    result.batches >= marker_row_capacity;
                const bool row_limit = oversized ||
                    rows > config_.physical_group_max_rows - result.rows;
                const bool byte_limit = oversized ||
                    rows > (config_.physical_group_max_revision_bytes -
                            revision_bytes) /
                               kRevisionRowBinaryBytes;
                if (batch_limit || marker_row_limit ||
                    row_limit || byte_limit) {
                    result.closed = true;
                    break;
                }
                ++result.batches;
                result.rows += rows;
                revision_bytes += rows * kRevisionRowBinaryBytes;
            }
            if (result.closed) {
                break;
            }
        }
        if (result.batches == config_.physical_group_max_batches ||
            result.batches == marker_row_capacity ||
            result.rows == config_.physical_group_max_rows ||
            revision_bytes == config_.physical_group_max_revision_bytes) {
            result.closed = true;
        }
        return result;
    }

    [[nodiscard]] bool CollectGroupPrefix(
        Lane* lane,
        std::vector<std::shared_ptr<const EventRevisionBatch>>* batches) {
        batches->clear();
        std::unique_lock<std::mutex> lock(lane->mutex);
        for (;;) {
            lane->wake.wait(lock, [this, lane] {
                return !lane->queue.empty() ||
                    stopping_.load(std::memory_order_acquire) || !healthy();
            });
            if (lane->queue.empty()) {
                return false;
            }
            if (!healthy()) {
                return false;
            }
            const PrefixSelection selection = SelectPrefix(*lane);
            if (selection.batches == 0U) {
                SetFatal("ClickHouse Event group selection made no progress");
                return false;
            }
            if (!stopping_.load(std::memory_order_acquire) &&
                !selection.closed) {
                const std::uint64_t now = ingest::MonotonicNowNs();
                const std::uint64_t enqueued =
                    lane->queue.front().enqueued_monotonic_ns;
                const std::uint64_t elapsed = now >= enqueued
                    ? now - enqueued
                    : 0U;
                if (elapsed < config_.physical_group_max_delay_ns) {
                    lane->wake.wait_for(
                        lock,
                        std::chrono::nanoseconds(
                            config_.physical_group_max_delay_ns - elapsed));
                    continue;
                }
            }
            batches->reserve(selection.batches);
            std::size_t remaining = selection.batches;
            for (const QueuedSubmission& submission : lane->queue) {
                for (std::size_t batch_index = submission.first_batch;
                     batch_index < submission.batches.size() && remaining != 0U;
                     ++batch_index, --remaining) {
                    batches->push_back(submission.batches[batch_index]);
                }
                if (remaining == 0U) {
                    break;
                }
            }
            if (remaining != 0U) {
                SetFatal("ClickHouse Event queue prefix shortened");
                return false;
            }
            const std::uint64_t selected_ns = ingest::MonotonicNowNs();
            const std::uint64_t enqueued_ns =
                lane->queue.front().enqueued_monotonic_ns;
            stats_.group_wait_ns.fetch_add(
                selected_ns >= enqueued_ns ? selected_ns - enqueued_ns : 0U,
                std::memory_order_relaxed);
            return true;
        }
    }

    [[nodiscard]] bool FinalizeRevisionRequest(
        std::size_t lane_index,
        PhysicalInsertGroup* group,
        InsertRequest* current) {
        if (current->rows == 0U || current->payload.size() == 0U) {
            SetFatal("ClickHouse Event revision request is empty");
            return false;
        }
        if (current->rows > config_.insert_request_max_rows ||
            current->payload.size() > config_.insert_request_max_bytes ||
            group->revision_requests.size() >=
                std::numeric_limits<std::uint32_t>::max()) {
            SetFatal("ClickHouse Event revision request bound was exceeded");
            return false;
        }
        const std::uint32_t request_index = static_cast<std::uint32_t>(
            group->revision_requests.size());
        const Identifier128 request_id = PhysicalRequestIdentifier(
            writer_instance_id_, group->group_sequence,
            static_cast<std::uint32_t>(lane_index), request_index,
            event::kEventSchemaVersion, 1U);
        const std::string writer = IdentifierString(writer_instance_id_);
        current->query_id =
            "l2flow/event_revision_log/" + writer + "/" +
            std::to_string(lane_index) + "/" +
            std::to_string(group->group_sequence) + "/" +
            std::to_string(request_index);
        current->dedup_token =
            "l2flow/event_revision_log/" + IdentifierString(request_id) +
            "/" + std::to_string(event::kEventSchemaVersion);
        group->revision_requests.push_back(std::move(*current));
        return true;
    }

    [[nodiscard]] bool PrepareGroup(
        std::span<const std::shared_ptr<const EventRevisionBatch>> batches,
        std::size_t lane_index,
        PhysicalInsertGroup* group) {
        if (batches.empty()) {
            SetFatal("ClickHouse Event physical group is empty");
            return false;
        }
        group->batches.reserve(batches.size());
        for (const auto& batch : batches) {
            const std::uint64_t sink_sequence =
                next_sink_batch_sequence_.fetch_add(
                    1U, std::memory_order_relaxed);
            if (sink_sequence == 0U) {
                SetFatal("ClickHouse Event sink batch sequence exhausted");
                return false;
            }
            group->revision_rows += batch->revisions.size();
            std::uint16_t trade_date_days = 0U;
            if (!TradeDateToDays(batch->revisions.front().key.trade_date,
                                 &trade_date_days)) {
                SetFatal("ClickHouse Event trade date became invalid");
                return false;
            }
            group->batches.push_back(PreparedBatch{
                batch, sink_sequence, 0U, trade_date_days});
        }

        const std::uint64_t serialize_started_ns = ingest::MonotonicNowNs();
        std::uint64_t chunk_id_ns = 0U;
        const auto measured_chunk_identifier =
            [this, &chunk_id_ns](Identifier128 recovery_run,
                                 std::uint32_t chunk,
                                 std::uint8_t table_tag) noexcept {
                const std::uint64_t started_ns = ingest::MonotonicNowNs();
                const Identifier128 result = ChunkIdentifier(
                    writer_instance_id_, recovery_run, chunk,
                    event::kEventSchemaVersion, table_tag);
                const std::uint64_t completed_ns = ingest::MonotonicNowNs();
                chunk_id_ns += completed_ns >= started_ns
                    ? completed_ns - started_ns
                    : 0U;
                return result;
            };
        const std::size_t request_row_capacity = std::min(
            config_.insert_request_max_rows,
            config_.insert_request_max_bytes / kRevisionRowBinaryBytes);
        const std::size_t initial_reserve = std::max(
            kRevisionRowBinaryBytes,
            std::min(group->revision_rows, request_row_capacity) *
                kRevisionRowBinaryBytes);
        InsertRequest current(initial_reserve);
        for (PreparedBatch& prepared : group->batches) {
            const EventRevisionBatch& batch = *prepared.batch;
            std::uint32_t chunk_index = 0U;
            bool logical_chunk_has_rows = false;
            RevisionChunkMetadata metadata{};
            metadata.sink_batch_sequence = prepared.sink_batch_sequence;
            metadata.chunk_index = chunk_index;
            metadata.batch_id = measured_chunk_identifier(
                batch.recovery_run_id, chunk_index, 1U);
            metadata.trade_date_days = prepared.trade_date_days;
            for (std::size_t row = 0U; row < batch.revisions.size(); ++row) {
                if (current.rows == request_row_capacity) {
                    if (!FinalizeRevisionRequest(
                            lane_index, group, &current)) {
                        return false;
                    }
                    current = InsertRequest(initial_reserve);
                    if (logical_chunk_has_rows) {
                        ++chunk_index;
                        logical_chunk_has_rows = false;
                        metadata.chunk_index = chunk_index;
                        metadata.batch_id = measured_chunk_identifier(
                            batch.recovery_run_id, chunk_index, 1U);
                    }
                }

                const std::size_t checkpoint = current.payload.size();
                if (!AppendRevisionRow(
                        &current.payload, batch.revisions[row],
                        writer_instance_id_, metadata,
                        static_cast<std::uint32_t>(row)) ||
                    current.payload.size() - checkpoint !=
                        kRevisionRowBinaryBytes) {
                    SetFatal(
                        "ClickHouse Event revision serialization size changed");
                    return false;
                }
                ++current.rows;
                logical_chunk_has_rows = true;
            }
            prepared.chunk_count = chunk_index + 1U;
        }
        if (current.rows != 0U &&
            !FinalizeRevisionRequest(lane_index, group, &current)) {
            return false;
        }
        if (group->revision_requests.empty()) {
            SetFatal("ClickHouse Event group has no revision requests");
            return false;
        }

        const std::uint64_t committed_utc_ns = SystemUtcNowNs();
        for (const PreparedBatch& prepared : group->batches) {
            const std::size_t checkpoint = group->marker_payload.size();
            const Identifier128 commit_id = measured_chunk_identifier(
                prepared.batch->recovery_run_id, 0U, 2U);
            if (!AppendRecoveryMarkerRow(
                    &group->marker_payload, *prepared.batch,
                    writer_instance_id_, prepared.trade_date_days,
                    commit_id,
                    prepared.chunk_count,
                    committed_utc_ns) ||
                group->marker_payload.size() - checkpoint !=
                    kRecoveryMarkerRowBinaryBytes) {
                SetFatal(
                    "ClickHouse Event recovery marker serialization changed");
                return false;
            }
        }
        const std::uint64_t serialize_completed_ns = ingest::MonotonicNowNs();
        const std::uint64_t serialize_elapsed_ns =
            serialize_completed_ns >= serialize_started_ns
                ? serialize_completed_ns - serialize_started_ns
                : 0U;
        stats_.chunk_id_ns.fetch_add(chunk_id_ns,
                                     std::memory_order_relaxed);
        stats_.rowbinary_serialize_ns.fetch_add(
            serialize_elapsed_ns >= chunk_id_ns
                ? serialize_elapsed_ns - chunk_id_ns
                : 0U,
            std::memory_order_relaxed);
        if (group->batches.size() > config_.insert_request_max_rows ||
            group->marker_payload.size() >
                config_.insert_request_max_bytes) {
            SetFatal("ClickHouse Event marker request bound was exceeded");
            return false;
        }
        const Identifier128 marker_id = PhysicalRequestIdentifier(
            writer_instance_id_, group->group_sequence,
            static_cast<std::uint32_t>(lane_index), 0U,
            event::kEventSchemaVersion, 2U);
        const std::string writer = IdentifierString(writer_instance_id_);
        group->marker_query_id =
            "l2flow/event_recovery_run/" + writer + "/" +
            std::to_string(lane_index) + "/" +
            std::to_string(group->group_sequence);
        group->marker_dedup_token =
            "l2flow/event_recovery_run/" + IdentifierString(marker_id) +
            "/" + std::to_string(event::kEventSchemaVersion);

        std::set<outbox::WalPosition> positions;
        for (const PreparedBatch& prepared : group->batches) {
            positions.insert(prepared.batch->input_positions.begin(),
                             prepared.batch->input_positions.end());
        }
        group->input_positions.assign(positions.begin(), positions.end());
        if (group->input_positions.empty()) {
            SetFatal("ClickHouse Event group has no WAL inputs");
            return false;
        }
        std::vector<outbox::RequestPayload> durable_requests;
        durable_requests.reserve(group->revision_requests.size() + 1U);
        for (const InsertRequest& request : group->revision_requests) {
            durable_requests.push_back(outbox::RequestPayload{
                outbox::RequestKind::kEventRevision,
                static_cast<std::uint64_t>(request.rows),
                request.query_id,
                request.dedup_token,
                request.payload.bytes()});
        }
        durable_requests.push_back(outbox::RequestPayload{
            outbox::RequestKind::kEventMarker,
            static_cast<std::uint64_t>(group->batches.size()),
            group->marker_query_id,
            group->marker_dedup_token,
            group->marker_payload.bytes()});
        std::string spool_error;
        if (!request_spool_->PrepareGroup(
                outbox::ConsumerKind::kEvent, group->input_positions,
                durable_requests, &group->spool_handle, &spool_error)) {
            SetFatal("ClickHouse Event request spool prepare failed: " +
                     spool_error);
            return false;
        }
        return true;
    }

    [[nodiscard]] bool ProcessGroup(PhysicalInsertGroup* group,
                                    HttpClient* client) {
        for (std::size_t index = 0U;
             index < group->revision_requests.size(); ++index) {
            InsertRequest& request = group->revision_requests[index];
            if (!ExactInsert(client, InsertKind::kRevision,
                             group->spool_handle,
                             static_cast<std::uint32_t>(index),
                             "event_revision_log", RevisionColumnNames(),
                             request.query_id, request.dedup_token,
                             request.payload.bytes())) {
                return false;
            }
            PublishMaximum(
                &stats_.revision_request_rows_max,
                static_cast<std::uint64_t>(request.rows));
            PublishMaximum(
                &stats_.revision_request_bytes_max,
                static_cast<std::uint64_t>(request.payload.size()));
        }
        if (!ExactInsert(client, InsertKind::kMarker,
                         group->spool_handle,
                         static_cast<std::uint32_t>(
                             group->revision_requests.size()),
                         "event_recovery_run", kRecoveryMarkerColumns,
                         group->marker_query_id, group->marker_dedup_token,
                         group->marker_payload.bytes())) {
            return false;
        }
        const std::uint64_t completion_started_ns = ingest::MonotonicNowNs();
        const bool completed = config_.completion_sink->Complete(
            outbox::ConsumerKind::kEvent, group->input_positions);
        const std::uint64_t completion_completed_ns =
            ingest::MonotonicNowNs();
        stats_.completion_ns.fetch_add(
            completion_completed_ns >= completion_started_ns
                ? completion_completed_ns - completion_started_ns
                : 0U,
            std::memory_order_relaxed);
        if (!completed) {
            SetFatal("ClickHouse Event marker ACK could not advance the WAL cursor");
            return false;
        }
        std::string spool_error;
        const std::uint64_t retire_started_ns = ingest::MonotonicNowNs();
        const bool retired = request_spool_->Retire(
            group->spool_handle, &spool_error);
        const std::uint64_t retire_completed_ns = ingest::MonotonicNowNs();
        stats_.retire_ns.fetch_add(
            retire_completed_ns >= retire_started_ns
                ? retire_completed_ns - retire_started_ns
                : 0U,
            std::memory_order_relaxed);
        if (!retired) {
            SetFatal("ClickHouse Event request spool retire failed: " +
                     spool_error);
            return false;
        }
        const std::uint64_t batch_count =
            static_cast<std::uint64_t>(group->batches.size());
        const std::uint64_t row_count =
            static_cast<std::uint64_t>(group->revision_rows);
        stats_.physical_groups_committed.fetch_add(
            1U, std::memory_order_relaxed);
        stats_.recovery_runs_committed.fetch_add(
            batch_count, std::memory_order_relaxed);
        stats_.revision_rows_acked.fetch_add(
            row_count, std::memory_order_relaxed);
        stats_.revision_batches_acked.fetch_add(
            batch_count, std::memory_order_release);
        PublishMaximum(&stats_.physical_group_batches_max, batch_count);
        PublishMaximum(&stats_.physical_group_rows_max, row_count);
        return true;
    }

    [[nodiscard]] bool ReleaseGroup(
        Lane* lane,
        const PhysicalInsertGroup& group) noexcept {
        std::size_t released_submissions = 0U;
        {
            std::scoped_lock lock(lane->mutex, queue_budget_mutex_);
            if (queued_batches_ < group.batches.size() ||
                queued_rows_ < group.revision_rows) {
                SetFatal("ClickHouse Event queue budget invariant failed");
                return false;
            }
            std::size_t expected = 0U;
            for (const QueuedSubmission& submission : lane->queue) {
                for (std::size_t batch_index = submission.first_batch;
                     batch_index < submission.batches.size() &&
                         expected < group.batches.size();
                     ++batch_index, ++expected) {
                    if (submission.batches[batch_index] !=
                        group.batches[expected].batch) {
                        SetFatal("ClickHouse Event queue prefix changed");
                        return false;
                    }
                }
                if (expected == group.batches.size()) {
                    break;
                }
            }
            if (expected != group.batches.size()) {
                SetFatal("ClickHouse Event queue prefix disappeared");
                return false;
            }

            std::size_t remaining = group.batches.size();
            while (remaining != 0U) {
                QueuedSubmission& submission = lane->queue.front();
                const std::size_t available =
                    submission.batches.size() - submission.first_batch;
                const std::size_t released = std::min(remaining, available);
                submission.first_batch += released;
                remaining -= released;
                if (submission.first_batch == submission.batches.size()) {
                    lane->queue.pop_front();
                    ++released_submissions;
                }
            }
            queued_batches_ -= group.batches.size();
            queued_rows_ -= group.revision_rows;
            if (queued_submission_groups_ < released_submissions) {
                SetFatal("ClickHouse Event submission budget invariant failed");
                return false;
            }
            queued_submission_groups_ -= released_submissions;
        }
        stats_.submission_groups_released.fetch_add(
            released_submissions, std::memory_order_release);
        stats_.revision_batches_released.fetch_add(
            static_cast<std::uint64_t>(group.batches.size()),
            std::memory_order_release);
        queue_budget_available_.notify_all();
        lane->wake.notify_all();
        return true;
    }

    void WriterLoop(std::size_t lane_index) noexcept {
        try {
            HttpClient client(config_);
            Lane& lane = *lanes_[lane_index];
            std::vector<std::shared_ptr<const EventRevisionBatch>> batches;
            const std::size_t maximum_selected_batches = std::min(
                {config_.physical_group_max_batches,
                 config_.insert_request_max_rows,
                 config_.insert_request_max_bytes /
                     kRecoveryMarkerRowBinaryBytes,
                 config_.queue_revision_batches,
                 config_.queue_revision_rows});
            batches.reserve(maximum_selected_batches);
            for (;;) {
                if (!CollectGroupPrefix(&lane, &batches)) {
                    return;
                }
                const std::uint64_t group_sequence =
                    next_physical_group_sequence_.fetch_add(
                        1U, std::memory_order_relaxed);
                if (group_sequence == 0U) {
                    SetFatal(
                        "ClickHouse Event physical group sequence exhausted");
                    return;
                }
                PhysicalInsertGroup group(
                    group_sequence,
                    batches.size() * kRecoveryMarkerRowBinaryBytes);
                if (!PrepareGroup(batches, lane_index, &group) ||
                    !ProcessGroup(&group, &client) ||
                    !ReleaseGroup(&lane, group)) {
                    return;
                }
            }
        } catch (const std::exception& exception) {
            SetFatal(std::string("ClickHouse Event writer failed: ") +
                     exception.what());
        } catch (...) {
            SetFatal("ClickHouse Event writer failed with unknown exception");
        }
    }

    void SetFatal(std::string message) noexcept {
        bool first_failure = false;
        try {
            std::lock_guard<std::mutex> lock(fatal_mutex_);
            if (healthy_.load(std::memory_order_relaxed)) {
                fatal_error_ = std::move(message);
                healthy_.store(false, std::memory_order_release);
                first_failure = true;
            }
        } catch (...) {
            first_failure = healthy_.exchange(
                false, std::memory_order_acq_rel);
        }
        accepting_.store(false, std::memory_order_release);
        if (!first_failure) {
            return;
        }
        wake_.notify_all();
        queue_budget_available_.notify_all();
        for (const auto& lane : lanes_) {
            lane->wake.notify_all();
        }
    }

    EventClickHouseConfig config_{};
    Identifier128 writer_instance_id_{};
    std::unique_ptr<outbox::RequestSpool> request_spool_;
    AtomicStats stats_{};
    std::vector<std::unique_ptr<Lane>> lanes_;
    mutable std::mutex queue_budget_mutex_;
    std::condition_variable queue_budget_available_;
    mutable std::shared_mutex lifecycle_mutex_;
    std::size_t queued_batches_ = 0U;
    std::size_t queued_rows_ = 0U;
    std::size_t queued_submission_groups_ = 0U;
    std::size_t queued_batches_high_water_ = 0U;
    std::size_t queued_rows_high_water_ = 0U;
    std::size_t queued_submission_groups_high_water_ = 0U;
    std::atomic<std::uint64_t> next_sink_batch_sequence_{1U};
    std::atomic<std::uint64_t> next_physical_group_sequence_{1U};
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

bool EventClickHouseSink::AppendRevisionGroup(
    std::vector<std::shared_ptr<const EventRevisionBatch>> batches) noexcept {
    return impl_->AppendRevisionGroup(std::move(batches));
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
