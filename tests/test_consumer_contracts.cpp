#include "l2flow/clickhouse/event_sink.h"
#include "l2flow/clickhouse/freshness.h"
#include "l2flow/clickhouse/kline_sink.h"
#include "l2flow/clickhouse/raw_consumer.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <string>
#include <vector>

namespace {

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::abort();                                                    \
        }                                                                    \
    } while (false)

class CompletionRecorder final
    : public l2flow::outbox::ConsumerCompletionSink {
public:
    [[nodiscard]] bool Complete(
        l2flow::outbox::ConsumerKind consumer,
        std::span<const l2flow::outbox::WalPosition> positions)
        noexcept override {
        last_consumer = consumer;
        completed.insert(completed.end(), positions.begin(), positions.end());
        return accept;
    }

    l2flow::outbox::ConsumerKind last_consumer =
        l2flow::outbox::ConsumerKind::kRaw;
    std::vector<l2flow::outbox::WalPosition> completed;
    bool accept = true;
};

void TestRawConfigRequiresCursorSink() {
    l2flow::clickhouse::RawClickHouseConfig config{};
    config.feed_session_epoch = 9U;
    config.tick_decoder_lanes = 2U;
    config.snapshot_decoder_lanes = 1U;
    CompletionRecorder completion;
    std::string error;
    CHECK(!l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
    config.completion_sink = &completion;
    CHECK(l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
}

void TestDerivedConfigsRequireSpoolAndExactCursorSink() {
    CompletionRecorder completion;
    std::string error;

    l2flow::clickhouse::EventClickHouseConfig event{};
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(event, &error));
    event.completion_sink = &completion;
    event.request_spool.directory = "/tmp/l2flow-event-contract-spool";
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(event, &error));
    event.physical_group_max_rows = 0U;
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(event, &error));
    event.physical_group_max_rows = 16'384U;
    event.physical_group_max_revision_bytes = 664U;
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(event, &error));
    event.physical_group_max_revision_bytes = 16U * 1'024U * 1'024U;
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(event, &error));

    l2flow::clickhouse::KLineClickHouseConfig kline{};
    CHECK(!l2flow::clickhouse::ValidateKLineClickHouseConfig(kline, &error));
    kline.completion_sink = &completion;
    kline.request_spool.directory = "/tmp/l2flow-kline-contract-spool";
    CHECK(l2flow::clickhouse::ValidateKLineClickHouseConfig(kline, &error));
}

void TestRawIdentifiersSeparateBatchAndOccurrenceIdentity() {
    l2flow::clickhouse::Identifier128 source{};
    source.bytes[0U] = std::byte{1U};
    const auto batch = l2flow::clickhouse::RawBatchIdentifier(
        source, l2flow::clickhouse::RawTableId::kRawTick,
        20260814U, 7U, l2flow::clickhouse::kRawTickSchemaVersion);
    const auto occurrence = l2flow::clickhouse::RawOccurrenceIdentifier(
        source, 9U, 7U,
        l2flow::ingest::CanonicalKind::kShanghaiTick);
    CHECK(batch != occurrence);
    CHECK(batch == l2flow::clickhouse::RawBatchIdentifier(
        source, l2flow::clickhouse::RawTableId::kRawTick,
        20260814U, 7U, l2flow::clickhouse::kRawTickSchemaVersion));
    CHECK(occurrence == l2flow::clickhouse::RawOccurrenceIdentifier(
        source, 9U, 7U,
        l2flow::ingest::CanonicalKind::kShanghaiTick));
}

void TestFreshnessContractIsFailClosed() {
    l2flow::clickhouse::FreshnessClickHouseConfig config{};
    config.feed_session_epoch = 9U;
    config.event_enabled = true;
    config.source_instance_id.bytes[0U] = std::byte{1U};
    config.publisher_instance_id.bytes[0U] = std::byte{2U};
    std::string error;
    CHECK(!l2flow::clickhouse::ValidateFreshnessClickHouseConfig(
        config, &error));
    config.event_calculation_run_id.bytes[0U] = std::byte{3U};
    CHECK(l2flow::clickhouse::ValidateFreshnessClickHouseConfig(
        config, &error));

    const std::string ddl =
        l2flow::clickhouse::DerivedFreshnessLogDdl("l2flow");
    CHECK(ddl.find("valid_until_utc_ns UInt64") != std::string::npos);
    CHECK(ddl.find("authoritative Bool") != std::string::npos);
    CHECK(ddl.find("calculation_run_id,domain,publication_sequence") !=
          std::string::npos);

    const std::string event_view =
        l2flow::clickhouse::EventCurrentViewDdl("l2flow");
    CHECK(event_view.find("event_recovery_run") != std::string::npos);
    CHECK(event_view.find("derived_freshness_log") != std::string::npos);
    CHECK(event_view.find("count() OVER () AS active_run_count") !=
          std::string::npos);
    CHECK(event_view.find("WHERE active_run_count=1") != std::string::npos);
    CHECK(event_view.find(
              "tuple(publication_sequence,observed_utc_ns,"
              "publisher_instance_id)") != std::string::npos);
    CHECK(event_view.find("row_number()") != std::string::npos);
    CHECK(event_view.find("PARTITION BY r.trade_date,r.market") !=
          std::string::npos);
    CHECK(event_view.find("committed_trade_date") != std::string::npos);
    CHECK(event_view.find("freshness_calculation_run_id") !=
          std::string::npos);
    CHECK(event_view.find("input_max_lsn") != std::string::npos);
    CHECK(event_view.find("event_lsn") != std::string::npos);
    CHECK(event_view.find("<=tuple(f.cursor_lsn") != std::string::npos);
    CHECK(event_view.find("is_deleted=0") != std::string::npos);

    const std::string kline_view =
        l2flow::clickhouse::KLineCurrentViewDdl("l2flow");
    CHECK(kline_view.find("kline_recovery_run") != std::string::npos);
    CHECK(kline_view.find("derived_freshness_log") != std::string::npos);
    CHECK(kline_view.find("count() OVER () AS active_run_count") !=
          std::string::npos);
    CHECK(kline_view.find("WHERE active_run_count=1") != std::string::npos);
    CHECK(kline_view.find(
              "tuple(publication_sequence,observed_utc_ns,"
              "publisher_instance_id)") != std::string::npos);
    CHECK(kline_view.find("row_number()") != std::string::npos);
    CHECK(kline_view.find("PARTITION BY r.trade_date,r.market") !=
          std::string::npos);
    CHECK(kline_view.find("committed_trade_date") != std::string::npos);
    CHECK(kline_view.find("freshness_calculation_run_id") !=
          std::string::npos);
    CHECK(kline_view.find("input_max_lsn") != std::string::npos);
    CHECK(kline_view.find("kline_lsn") != std::string::npos);
    CHECK(kline_view.find("<=tuple(f.cursor_lsn") != std::string::npos);
    CHECK(kline_view.find("is_deleted=0") != std::string::npos);
}

void TestFreshnessLeaseUsesClickHouseClock() {
    constexpr std::uint64_t kLeaseNs = 1'234'567'890U;
    const std::string insert =
        l2flow::clickhouse::DerivedFreshnessInsertSql("l2flow", kLeaseNs);
    CHECK(insert.find(
              "WITH toUInt64(toUnixTimestamp64Nano(now64(9))) AS "
              "_l2flow_server_utc_ns") != std::string::npos);
    CHECK(insert.find(
              "toUInt64(least(toUInt128(_l2flow_server_utc_ns)+"
              "toUInt128('1234567890'),"
              "toUInt128('18446744073709551615')))") !=
          std::string::npos);

    const std::size_t input_begin = insert.find("FROM input('");
    CHECK(input_begin != std::string::npos);
    const std::size_t input_end = insert.find(
        "') FORMAT RowBinary", input_begin);
    CHECK(input_end != std::string::npos);
    const std::string input_structure = insert.substr(
        input_begin, input_end - input_begin);
    CHECK(input_structure.find("observed_utc_ns") == std::string::npos);
    CHECK(input_structure.find("valid_until_utc_ns") == std::string::npos);
}

}  // namespace

int main() {
    TestRawConfigRequiresCursorSink();
    TestDerivedConfigsRequireSpoolAndExactCursorSink();
    TestRawIdentifiersSeparateBatchAndOccurrenceIdentity();
    TestFreshnessContractIsFailClosed();
    TestFreshnessLeaseUsesClickHouseClock();
    std::cout << "all consumer contract tests passed\n";
    return 0;
}
