#pragma once

#include "l2flow/common/identifier.h"
#include "l2flow/outbox/continuity.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::clickhouse {

struct FreshnessClickHouseConfig final {
    std::string endpoint = "http://127.0.0.1:8123";
    std::string database = "l2flow";
    std::string username = "default";
    std::string password;
    std::string no_proxy = "*";

    common::Identifier128 source_instance_id{};
    std::uint64_t feed_session_epoch = 0U;
    common::Identifier128 publisher_instance_id{};
    common::Identifier128 event_calculation_run_id{};
    common::Identifier128 kline_calculation_run_id{};
    bool event_enabled = false;
    bool kline_enabled = false;

    // ClickHouse starts this lease from its own query clock when it executes
    // the INSERT. This must be longer than the application's publication
    // period.
    std::uint64_t lease_ns = 3'000'000'000ULL;
    std::uint32_t connect_timeout_ms = 2'000U;
    std::uint32_t request_timeout_ms = 10'000U;
    std::uint32_t insert_quorum = 0U;
    bool insert_quorum_parallel = true;
    bool ensure_local_tables = true;
    bool tls_verify_peer = true;
};

[[nodiscard]] bool ValidateFreshnessClickHouseConfig(
    const FreshnessClickHouseConfig& config,
    std::string* error) noexcept;

// Shared schema functions are used by the Event/KLine sinks as part of their
// fail-closed table contract and by the heartbeat publisher itself.
[[nodiscard]] std::string DerivedFreshnessLogDdl(
    std::string_view database);
[[nodiscard]] std::string DerivedFreshnessTableProbe(
    std::string_view database);
[[nodiscard]] std::string DerivedFreshnessColumnProbe(
    std::string_view database);
[[nodiscard]] std::string DerivedFreshnessInsertSql(
    std::string_view database,
    std::uint64_t lease_ns);

class FreshnessClickHousePublisher final {
public:
    ~FreshnessClickHousePublisher();
    FreshnessClickHousePublisher(const FreshnessClickHousePublisher&) = delete;
    FreshnessClickHousePublisher& operator=(
        const FreshnessClickHousePublisher&) = delete;

    [[nodiscard]] static std::unique_ptr<FreshnessClickHousePublisher> Create(
        FreshnessClickHouseConfig config,
        std::string* error);

    // Publish is deliberately recoverable: a failed request does not poison
    // the publisher. The previous lease expires and later calls can reconnect.
    [[nodiscard]] bool Publish(
        const outbox::FreshnessSnapshot& snapshot,
        std::string* error) noexcept;

    [[nodiscard]] const FreshnessClickHouseConfig& config() const noexcept;

private:
    class Impl;
    explicit FreshnessClickHousePublisher(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::clickhouse
