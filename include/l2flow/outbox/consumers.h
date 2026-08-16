#pragma once

#include "l2flow/clickhouse/raw_consumer.h"
#include "l2flow/event/runtime.h"
#include "l2flow/kline/runtime.h"
#include "l2flow/outbox/wal.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::outbox {

struct RawOutboxConsumerConfig final {
    DurableOutbox* outbox = nullptr;
    clickhouse::RawClickHouseConsumer* sink = nullptr;
    std::size_t tick_lanes = 0U;
    std::size_t snapshot_lanes = 0U;
};

class RawOutboxConsumer final {
public:
    ~RawOutboxConsumer();
    RawOutboxConsumer(const RawOutboxConsumer&) = delete;
    RawOutboxConsumer& operator=(const RawOutboxConsumer&) = delete;

    [[nodiscard]] static std::unique_ptr<RawOutboxConsumer> Create(
        RawOutboxConsumerConfig config,
        std::string* error);
    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool DrainThrough(std::uint64_t lsn,
                                    std::uint64_t timeout_ns) noexcept;
    void Stop() noexcept;
    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] std::uint64_t reader_lsn() const noexcept;

private:
    class Impl;
    explicit RawOutboxConsumer(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

enum class DerivedDomain : std::uint8_t {
    kEvent,
    kKLine,
};

struct DerivedOutboxConsumerConfig final {
    DurableOutbox* outbox = nullptr;
    DerivedDomain domain = DerivedDomain::kEvent;
    event::EventRuntime* event_runtime = nullptr;
    kline::KLineRuntime* kline_runtime = nullptr;
    std::size_t owner_count = 0U;
    std::size_t queue_records_per_owner = 4'096U;
};

class DerivedOutboxConsumer final {
public:
    ~DerivedOutboxConsumer();
    DerivedOutboxConsumer(const DerivedOutboxConsumer&) = delete;
    DerivedOutboxConsumer& operator=(const DerivedOutboxConsumer&) = delete;

    [[nodiscard]] static std::unique_ptr<DerivedOutboxConsumer> Create(
        DerivedOutboxConsumerConfig config,
        std::string* error);
    [[nodiscard]] bool Start(std::string* error);
    [[nodiscard]] bool DrainThrough(std::uint64_t lsn,
                                    std::uint64_t timeout_ns) noexcept;
    void Stop() noexcept;
    [[nodiscard]] bool healthy() const noexcept;
    [[nodiscard]] std::string fatal_error() const;
    [[nodiscard]] std::uint64_t reader_lsn() const noexcept;

private:
    class Impl;
    explicit DerivedOutboxConsumer(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::outbox
