#include "l2flow/checksum/crc32c.h"
#include "l2flow/outbox/continuity.h"
#include "l2flow/outbox/request_spool.h"
#include "l2flow/outbox/wal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <unistd.h>

#if defined(L2FLOW_TEST_WRAP_PREAD)
namespace {

std::atomic<bool> g_intercept_pread{false};
std::atomic<bool> g_pread_entered{false};
std::atomic<bool> g_release_pread{false};
std::atomic<std::uint64_t> g_intercepted_preads{0U};

}  // namespace

extern "C" ssize_t __real_pread(int descriptor,
                                 void* buffer,
                                 std::size_t count,
                                 off_t offset);

extern "C" ssize_t __wrap_pread(int descriptor,
                                 void* buffer,
                                 std::size_t count,
                                 off_t offset) {
    if (g_intercept_pread.load(std::memory_order_acquire)) {
        g_intercepted_preads.fetch_add(1U, std::memory_order_relaxed);
        g_pread_entered.store(true, std::memory_order_release);
        while (!g_release_pread.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    return __real_pread(descriptor, buffer, count, offset);
}
#endif

namespace {

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::abort();                                                    \
        }                                                                    \
    } while (false)

template <typename Predicate>
[[nodiscard]] bool WaitFor(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(5);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

#if defined(L2FLOW_TEST_WRAP_PREAD)
void BlockWalReads() noexcept {
    g_intercepted_preads.store(0U, std::memory_order_relaxed);
    g_pread_entered.store(false, std::memory_order_relaxed);
    g_release_pread.store(false, std::memory_order_relaxed);
    g_intercept_pread.store(true, std::memory_order_release);
}

void ReleaseWalReads() noexcept {
    g_release_pread.store(true, std::memory_order_release);
}

void StopInterceptingWalReads() noexcept {
    ReleaseWalReads();
    g_intercept_pread.store(false, std::memory_order_release);
}
#endif

using l2flow::outbox::CanonicalRecord;
using l2flow::outbox::ConsumerKind;
using l2flow::outbox::ContinuityController;
using l2flow::outbox::ContinuityState;
using l2flow::outbox::DurableOutbox;
using l2flow::outbox::DurableOutboxConfig;
using l2flow::outbox::FreshnessBarrier;
using l2flow::outbox::RecordKind;
using l2flow::outbox::RecordView;
using l2flow::outbox::RequestGroupHandle;
using l2flow::outbox::RequestKind;
using l2flow::outbox::RequestPayload;
using l2flow::outbox::RequestSpool;
using l2flow::outbox::RequestSpoolConfig;
using l2flow::outbox::RequestState;
using l2flow::outbox::WalPosition;

[[nodiscard]] std::uint32_t ReferenceCrc32c(
    std::span<const std::byte> bytes) noexcept {
    std::uint32_t crc = UINT32_MAX;
    for (const std::byte value : bytes) {
        crc ^= std::to_integer<std::uint32_t>(value);
        for (unsigned int bit = 0U; bit < 8U; ++bit) {
            const std::uint32_t mask = static_cast<std::uint32_t>(
                -static_cast<std::int32_t>(crc & UINT32_C(1)));
            crc = (crc >> 1U) ^ (UINT32_C(0x82f63b78) & mask);
        }
    }
    return ~crc;
}

void TestSharedCrc32c() {
    CHECK(l2flow::checksum::Crc32c({}) == 0U);
    constexpr std::string_view known = "123456789";
    const auto known_bytes = std::as_bytes(
        std::span<const char>(known.data(), known.size()));
    CHECK(l2flow::checksum::Crc32c(known_bytes) == UINT32_C(0xe3069283));

    std::vector<std::byte> random(64U * 1'024U);
    std::uint64_t state = UINT64_C(0x9e3779b97f4a7c15);
    for (std::byte& value : random) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        value = static_cast<std::byte>(state);
    }
    const std::uint32_t expected = ReferenceCrc32c(random);
    CHECK(l2flow::checksum::Crc32c(random) == expected);

    l2flow::checksum::Crc32cAccumulator segmented;
    segmented.Update({});
    std::size_t offset = 0U;
    std::size_t segment = 1U;
    while (offset < random.size()) {
        const std::size_t count = std::min(segment, random.size() - offset);
        segmented.Update(std::span<const std::byte>(random).subspan(
            offset, count));
        offset += count;
        segment = segment == 97U ? 1U : segment + 1U;
    }
    CHECK(segmented.Finish() == expected);
}

template <std::size_t Size>
void FillBytes(std::array<std::byte, Size>* output,
               std::uint8_t seed) {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<unsigned int>(seed) +
            static_cast<unsigned int>(index));
    }
}

[[nodiscard]] l2flow::ingest::FixedDecimal Decimal(
    std::int64_t raw,
    std::int64_t p6,
    std::uint8_t scale) {
    return l2flow::ingest::FixedDecimal{raw, p6, scale, true, true};
}

[[nodiscard]] l2flow::ingest::ScaledInteger Scaled(
    std::int64_t raw,
    std::uint8_t scale) {
    return l2flow::ingest::ScaledInteger{raw, scale, true};
}

[[nodiscard]] l2flow::ingest::CanonicalCommon RichCommon(
    std::uint64_t ingress,
    l2flow::ingest::CanonicalKind kind,
    l2flow::ingest::Market market) {
    l2flow::ingest::CanonicalCommon value{};
    value.ingress_sequence = ingress;
    value.vendor_sequence_id = ingress + 1U;
    value.receive_monotonic_ns = ingress + 2U;
    value.native_sequence = ingress + 3U;
    value.exchange_time_ns_from_midnight = ingress + 4U;
    value.vendor_local_time_ns_from_midnight = ingress + 5U;
    value.quality_flags = ingress + 6U;
    value.gap_epoch = ingress + 7U;
    value.gap_before_first = ingress + 8U;
    value.gap_before_last = ingress + 9U;
    value.trade_date = 20'260'814U;
    value.instrument_id = 600'001U;
    value.instrument_ordinal = 19U;
    value.channel = 7U;
    value.exchange_time_raw = 93'000'000U;
    value.vendor_local_time_raw = 93'000'001U;
    value.exchange_time_valid = true;
    value.vendor_local_time_valid = true;
    value.message_key.service_id = 11U;
    value.message_key.service_version = 12U;
    value.message_key.message_id = 13U;
    value.kind = kind;
    value.identity.market = market;
    value.identity.security_id_source_size = 4U;
    value.identity.security_id_size = 6U;
    FillBytes(&value.identity.security_id_source, 17U);
    FillBytes(&value.identity.security_id, 33U);
    value.md_stream_id_size = 5U;
    FillBytes(&value.md_stream_id, 49U);
    return value;
}

[[nodiscard]] l2flow::ingest::CanonicalTick RichTick(
    std::uint64_t ingress) {
    l2flow::ingest::CanonicalTick value{};
    value.common = RichCommon(
        ingress, l2flow::ingest::CanonicalKind::kShanghaiTick,
        l2flow::ingest::Market::kShanghai);
    value.price = Decimal(-12'345, -12'345'000, 3U);
    value.amount = Decimal(98'765, 987'650, 5U);
    value.quantity = Scaled(-456, 2U);
    value.primary_order_id = -101;
    value.buy_order_id = 102;
    value.sell_order_id = -103;
    value.sh_add_matched_quantity_raw = 104;
    value.validity = UINT64_C(0x1fed);
    value.raw_type = -7;
    value.raw_side = 8;
    value.action = l2flow::ingest::TickAction::kTrade;
    value.side = l2flow::ingest::Side::kBorrow;
    value.aggressor = l2flow::ingest::Aggressor::kSell;
    value.order_type = l2flow::ingest::OrderType::kSameSideBest;
    value.phase = l2flow::ingest::TradingPhase::kClosingCall;
    return value;
}

[[nodiscard]] l2flow::ingest::CanonicalSnapshot RichSnapshot(
    std::uint64_t ingress) {
    l2flow::ingest::CanonicalSnapshot value{};
    value.common = RichCommon(
        ingress, l2flow::ingest::CanonicalKind::kShenzhenSnapshot,
        l2flow::ingest::Market::kShenzhen);
    value.previous_close = Decimal(1, 10, 1U);
    value.open = Decimal(2, 20, 2U);
    value.high = Decimal(3, 30, 3U);
    value.low = Decimal(4, 40, 4U);
    value.last = Decimal(5, 50, 5U);
    value.close = Decimal(6, 60, 6U);
    value.turnover = Decimal(7, 70, 7U);
    value.volume = Scaled(8, 1U);
    value.total_bid_quantity = Scaled(9, 2U);
    value.total_ask_quantity = Scaled(10, 3U);
    value.weighted_average_bid = Decimal(11, 110, 1U);
    value.weighted_average_ask = Decimal(12, 120, 2U);
    value.trade_count = 13U;
    value.trade_count_valid = true;
    value.image_status = -14;
    value.image_status_valid = true;
    value.instrument_status_code_size = 4U;
    FillBytes(&value.instrument_status_code, 65U);
    value.trading_phase_code_size = 3U;
    FillBytes(&value.trading_phase_code, 81U);
    value.source_bid_depth = 12U;
    value.source_ask_depth = 13U;
    value.retained_bid_depth =
        static_cast<std::uint8_t>(value.bids.size());
    value.retained_ask_depth =
        static_cast<std::uint8_t>(value.asks.size());
    for (std::size_t index = 0U; index < value.bids.size(); ++index) {
        const std::int64_t scalar = static_cast<std::int64_t>(index + 1U);
        value.bids[index].price = Decimal(scalar, scalar * 10, 2U);
        value.bids[index].quantity = Scaled(scalar * 100, 1U);
        value.bids[index].source_order_count =
            static_cast<std::uint32_t>(index + 20U);
        value.bids[index].order_count_valid = true;
        value.asks[index].price = Decimal(-scalar, -scalar * 10, 3U);
        value.asks[index].quantity = Scaled(-scalar * 100, 2U);
        value.asks[index].source_order_count =
            static_cast<std::uint32_t>(index + 40U);
        value.asks[index].order_count_valid = (index % 2U) == 0U;
    }
    return value;
}

[[nodiscard]] l2flow::ingest::TickDispatch RichDispatch(
    std::uint64_t ingress,
    l2flow::ingest::TickDispatchKind kind) {
    l2flow::ingest::TickDispatch value{};
    value.tick = RichTick(ingress);
    value.feed_session_epoch = 17U;
    value.expected_sequence = 201U;
    value.admission_floor = 202U;
    value.generation = 203U;
    value.dispatch_fence = 204U;
    value.first_missing = 205U;
    value.last_missing = 206U;
    value.evict_before = 207U;
    value.channel = 7U;
    value.owner = 9U;
    value.market = l2flow::ingest::Market::kShanghai;
    value.kind = kind;
    value.catalog_match = true;
    return value;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::FixedDecimal& left,
    const l2flow::ingest::FixedDecimal& right) {
    return left.raw == right.raw && left.p6 == right.p6 &&
           left.source_scale == right.source_scale &&
           left.raw_valid == right.raw_valid &&
           left.p6_valid == right.p6_valid;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::ScaledInteger& left,
    const l2flow::ingest::ScaledInteger& right) {
    return left.raw == right.raw && left.scale == right.scale &&
           left.valid == right.valid;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::CanonicalCommon& left,
    const l2flow::ingest::CanonicalCommon& right) {
    return left.ingress_sequence == right.ingress_sequence &&
           left.vendor_sequence_id == right.vendor_sequence_id &&
           left.receive_monotonic_ns == right.receive_monotonic_ns &&
           left.native_sequence == right.native_sequence &&
           left.exchange_time_ns_from_midnight ==
               right.exchange_time_ns_from_midnight &&
           left.vendor_local_time_ns_from_midnight ==
               right.vendor_local_time_ns_from_midnight &&
           left.quality_flags == right.quality_flags &&
           left.gap_epoch == right.gap_epoch &&
           left.gap_before_first == right.gap_before_first &&
           left.gap_before_last == right.gap_before_last &&
           left.trade_date == right.trade_date &&
           left.instrument_id == right.instrument_id &&
           left.instrument_ordinal == right.instrument_ordinal &&
           left.channel == right.channel &&
           left.exchange_time_raw == right.exchange_time_raw &&
           left.vendor_local_time_raw == right.vendor_local_time_raw &&
           left.exchange_time_valid == right.exchange_time_valid &&
           left.vendor_local_time_valid == right.vendor_local_time_valid &&
           left.message_key.service_id == right.message_key.service_id &&
           left.message_key.service_version ==
               right.message_key.service_version &&
           left.message_key.message_id == right.message_key.message_id &&
           left.kind == right.kind &&
           left.identity.market == right.identity.market &&
           left.identity.security_id_source_size ==
               right.identity.security_id_source_size &&
           left.identity.security_id_size ==
               right.identity.security_id_size &&
           left.identity.security_id_source ==
               right.identity.security_id_source &&
           left.identity.security_id == right.identity.security_id &&
           left.md_stream_id_size == right.md_stream_id_size &&
           left.md_stream_id == right.md_stream_id;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::CanonicalTick& left,
    const l2flow::ingest::CanonicalTick& right) {
    return Equal(left.common, right.common) &&
           Equal(left.price, right.price) &&
           Equal(left.amount, right.amount) &&
           Equal(left.quantity, right.quantity) &&
           left.primary_order_id == right.primary_order_id &&
           left.buy_order_id == right.buy_order_id &&
           left.sell_order_id == right.sell_order_id &&
           left.sh_add_matched_quantity_raw ==
               right.sh_add_matched_quantity_raw &&
           left.validity == right.validity &&
           left.raw_type == right.raw_type &&
           left.raw_side == right.raw_side && left.action == right.action &&
           left.side == right.side && left.aggressor == right.aggressor &&
           left.order_type == right.order_type && left.phase == right.phase;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::CanonicalBookLevel& left,
    const l2flow::ingest::CanonicalBookLevel& right) {
    return Equal(left.price, right.price) &&
           Equal(left.quantity, right.quantity) &&
           left.source_order_count == right.source_order_count &&
           left.order_count_valid == right.order_count_valid;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::CanonicalSnapshot& left,
    const l2flow::ingest::CanonicalSnapshot& right) {
    if (!Equal(left.common, right.common) ||
        !Equal(left.previous_close, right.previous_close) ||
        !Equal(left.open, right.open) || !Equal(left.high, right.high) ||
        !Equal(left.low, right.low) || !Equal(left.last, right.last) ||
        !Equal(left.close, right.close) ||
        !Equal(left.turnover, right.turnover) ||
        !Equal(left.volume, right.volume) ||
        !Equal(left.total_bid_quantity, right.total_bid_quantity) ||
        !Equal(left.total_ask_quantity, right.total_ask_quantity) ||
        !Equal(left.weighted_average_bid, right.weighted_average_bid) ||
        !Equal(left.weighted_average_ask, right.weighted_average_ask) ||
        left.trade_count != right.trade_count ||
        left.trade_count_valid != right.trade_count_valid ||
        left.image_status != right.image_status ||
        left.image_status_valid != right.image_status_valid ||
        left.instrument_status_code_size !=
            right.instrument_status_code_size ||
        left.instrument_status_code != right.instrument_status_code ||
        left.trading_phase_code_size != right.trading_phase_code_size ||
        left.trading_phase_code != right.trading_phase_code ||
        left.source_bid_depth != right.source_bid_depth ||
        left.source_ask_depth != right.source_ask_depth ||
        left.retained_bid_depth != right.retained_bid_depth ||
        left.retained_ask_depth != right.retained_ask_depth) {
        return false;
    }
    for (std::size_t index = 0U; index < left.bids.size(); ++index) {
        if (!Equal(left.bids[index], right.bids[index]) ||
            !Equal(left.asks[index], right.asks[index])) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool Equal(
    const l2flow::ingest::TickDispatch& left,
    const l2flow::ingest::TickDispatch& right) {
    return Equal(left.tick, right.tick) &&
           left.feed_session_epoch == right.feed_session_epoch &&
           left.expected_sequence == right.expected_sequence &&
           left.admission_floor == right.admission_floor &&
           left.generation == right.generation &&
           left.dispatch_fence == right.dispatch_fence &&
           left.first_missing == right.first_missing &&
           left.last_missing == right.last_missing &&
           left.evict_before == right.evict_before &&
           left.channel == right.channel && left.owner == right.owner &&
           left.market == right.market && left.kind == right.kind &&
           left.catalog_match == right.catalog_match;
}

[[nodiscard]] bool Equal(const CanonicalRecord& left,
                         const CanonicalRecord& right) {
    if (left.kind != right.kind ||
        left.producer_lane != right.producer_lane ||
        left.owner != right.owner) {
        return false;
    }
    switch (left.kind) {
        case RecordKind::kTickOccurrence:
            return Equal(left.raw_tick, right.raw_tick) &&
                   Equal(left.disposition, right.disposition) &&
                   left.catalog_match == right.catalog_match;
        case RecordKind::kSnapshot:
            return Equal(left.raw_snapshot, right.raw_snapshot) &&
                   left.catalog_match == right.catalog_match;
        case RecordKind::kTickControl:
            return Equal(left.disposition, right.disposition);
        case RecordKind::kGapDiagnostic:
            return left.gap.market == right.gap.market &&
                   left.gap.channel == right.gap.channel &&
                   left.gap.first_missing == right.gap.first_missing &&
                   left.gap.last_missing == right.gap.last_missing &&
                   left.gap.first_present_after_gap ==
                       right.gap.first_present_after_gap &&
                   left.gap.detected_monotonic_ns ==
                       right.gap.detected_monotonic_ns &&
                   left.gap.gap_epoch == right.gap.gap_epoch &&
                   left.gap.cumulative_missing_sequences ==
                       right.gap.cumulative_missing_sequences &&
                   left.gap.feed_session_epoch ==
                       right.gap.feed_session_epoch;
        case RecordKind::kChannelFault:
            return left.fault.market == right.fault.market &&
                   left.fault.reason == right.fault.reason &&
                   left.fault.channel == right.fault.channel &&
                   left.fault.expected_sequence ==
                       right.fault.expected_sequence &&
                   left.fault.observed_sequence ==
                       right.fault.observed_sequence &&
                   left.fault.detected_monotonic_ns ==
                       right.fault.detected_monotonic_ns &&
                   left.fault.feed_session_epoch ==
                       right.fault.feed_session_epoch;
        case RecordKind::kFreshnessBarrier:
        case RecordKind::kFinalBarrier:
            return left.barrier.frontier_id == right.barrier.frontier_id &&
                   left.barrier.created_monotonic_ns ==
                       right.barrier.created_monotonic_ns &&
                   left.barrier.created_utc_ns ==
                       right.barrier.created_utc_ns;
    }
    return false;
}

class TemporaryDirectory final {
public:
    explicit TemporaryDirectory(std::string_view name) {
        const std::uint64_t stamp = static_cast<std::uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
            (std::string(name) + "-" + std::to_string(::getpid()) + "-" +
             std::to_string(stamp));
        CHECK(std::filesystem::create_directories(path_));
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

[[nodiscard]] DurableOutboxConfig Config(
    const std::filesystem::path& root) {
    DurableOutboxConfig config{};
    config.root_directory = root;
    config.feed_session_epoch = 17U;
    config.producer_queue_records = 1'024U;
    config.commit_batch_records = 32U;
    config.commit_batch_bytes = 4U * 1'024U;
    config.commit_max_delay_ns = 100'000U;
    config.segment_max_bytes = 8U * 1'024U;
    config.maximum_reservoir_bytes = 64U * 1'024U;
    config.cursor_checkpoint_interval_ns = 100'000U;
    return config;
}

[[nodiscard]] CanonicalRecord Barrier(std::uint64_t frontier) {
    CanonicalRecord record{};
    record.kind = RecordKind::kFreshnessBarrier;
    record.barrier.frontier_id = frontier;
    record.barrier.created_monotonic_ns = frontier * 10U;
    record.barrier.created_utc_ns = frontier * 100U;
    return record;
}

[[nodiscard]] std::unique_ptr<DurableOutbox> StartedOutbox(
    const std::filesystem::path& root) {
    std::string error;
    std::unique_ptr<DurableOutbox> outbox = DurableOutbox::Create(
        Config(root), &error);
    CHECK(outbox != nullptr);
    CHECK(outbox->Start(&error));
    return outbox;
}

void TestDurableVisibilityAndExactCursors() {
    TemporaryDirectory directory("l2flow-outbox-order");
    std::unique_ptr<DurableOutbox> outbox = StartedOutbox(directory.path());
    CHECK(outbox->Enqueue(Barrier(1U)));
    CHECK(outbox->Enqueue(Barrier(2U)));
    CHECK(outbox->Enqueue(Barrier(3U)));
    std::string error;
    CHECK(outbox->Flush(&error));

    std::array<RecordView, 3U> records{};
    for (std::size_t index = 0U; index < records.size(); ++index) {
        CHECK(outbox->TryRead(index + 1U, &records[index]));
        CHECK(records[index].position.lsn == index + 1U);
        CHECK(records[index].position.batch_sequence != 0U);
        CHECK(records[index].payload_checksum != 0U);
        CHECK(records[index].record->barrier.frontier_id == index + 1U);
    }

    const std::array<WalPosition, 2U> later{
        records[1U].position, records[2U].position};
    CHECK(outbox->Complete(ConsumerKind::kEvent, later));
    CHECK(outbox->consumer_cursor(ConsumerKind::kEvent).lsn == 0U);
    CHECK(outbox->stats().consumers[1U].out_of_order_completions == 2U);
    CHECK(outbox->CompleteOne(ConsumerKind::kEvent,
                              records[0U].position));
    CHECK(outbox->consumer_cursor(ConsumerKind::kEvent).lsn == 3U);

    std::array<WalPosition, 3U> all{
        records[2U].position, records[0U].position, records[1U].position};
    CHECK(outbox->Complete(ConsumerKind::kRaw, all));
    CHECK(outbox->Complete(ConsumerKind::kKLine, all));
    CHECK(outbox->consumer_cursor(ConsumerKind::kRaw).lsn == 3U);
    CHECK(outbox->consumer_cursor(ConsumerKind::kKLine).lsn == 3U);
    CHECK(outbox->Stop(&error));
}

void TestConcurrentProducerOrderDoesNotUseIngressMaximum() {
    TemporaryDirectory directory("l2flow-outbox-mpsc");
    std::unique_ptr<DurableOutbox> outbox = StartedOutbox(directory.path());
    constexpr std::size_t kThreads = 4U;
    constexpr std::size_t kPerThread = 50U;
    std::array<std::thread, kThreads> producers;
    for (std::size_t producer = 0U; producer < kThreads; ++producer) {
        producers[producer] = std::thread([&, producer] {
            for (std::size_t index = 0U; index < kPerThread; ++index) {
                const std::uint64_t deliberately_nonmonotone =
                    static_cast<std::uint64_t>(
                        (kThreads - producer) * 1'000U +
                        (kPerThread - index));
                CHECK(outbox->Enqueue(Barrier(deliberately_nonmonotone)));
            }
        });
    }
    for (std::thread& producer : producers) {
        producer.join();
    }
    std::string error;
    CHECK(outbox->Flush(&error));
    CHECK(outbox->durable_tail().lsn == kThreads * kPerThread);

    std::vector<WalPosition> positions;
    positions.reserve(kThreads * kPerThread);
    for (std::uint64_t lsn = 1U;
         lsn <= static_cast<std::uint64_t>(kThreads * kPerThread); ++lsn) {
        RecordView view{};
        CHECK(outbox->TryRead(lsn, &view));
        CHECK(view.position.lsn == lsn);
        positions.push_back(view.position);
    }
    std::reverse(positions.begin(), positions.end());
    CHECK(outbox->Complete(ConsumerKind::kEvent, positions));
    CHECK(outbox->consumer_cursor(ConsumerKind::kEvent).lsn ==
          kThreads * kPerThread);
    CHECK(outbox->Complete(ConsumerKind::kRaw, positions));
    CHECK(outbox->Complete(ConsumerKind::kKLine, positions));
    CHECK(outbox->Stop(&error));
}

void TestEveryCanonicalRecordKindRoundTripsFromDisk() {
    TemporaryDirectory directory("l2flow-outbox-codec");
    std::unique_ptr<DurableOutbox> outbox = StartedOutbox(directory.path());

    std::array<CanonicalRecord, 7U> expected{};
    expected[0U].kind = RecordKind::kTickOccurrence;
    expected[0U].producer_lane = 3U;
    expected[0U].owner = 4U;
    expected[0U].raw_tick = RichTick(101U);
    expected[0U].disposition = RichDispatch(
        101U, l2flow::ingest::TickDispatchKind::kProjectHoleFill);
    expected[0U].catalog_match = true;

    expected[1U].kind = RecordKind::kSnapshot;
    expected[1U].producer_lane = 5U;
    expected[1U].owner = 6U;
    expected[1U].raw_snapshot = RichSnapshot(102U);
    expected[1U].catalog_match = true;

    expected[2U].kind = RecordKind::kTickControl;
    expected[2U].producer_lane = 7U;
    expected[2U].owner = 8U;
    expected[2U].disposition = RichDispatch(
        103U, l2flow::ingest::TickDispatchKind::kGapOpen);

    expected[3U].kind = RecordKind::kGapDiagnostic;
    expected[3U].producer_lane = 9U;
    expected[3U].owner = 10U;
    expected[3U].gap.market = l2flow::ingest::Market::kShenzhen;
    expected[3U].gap.channel = 11U;
    expected[3U].gap.first_missing = 12U;
    expected[3U].gap.last_missing = 13U;
    expected[3U].gap.first_present_after_gap = 14U;
    expected[3U].gap.detected_monotonic_ns = 15U;
    expected[3U].gap.gap_epoch = 16U;
    expected[3U].gap.cumulative_missing_sequences = 17U;
    expected[3U].gap.feed_session_epoch = 17U;

    expected[4U].kind = RecordKind::kChannelFault;
    expected[4U].producer_lane = 12U;
    expected[4U].owner = 13U;
    expected[4U].fault.market = l2flow::ingest::Market::kShanghai;
    expected[4U].fault.reason =
        l2flow::ingest::ChannelFaultReason::kDecodeFailure;
    expected[4U].fault.channel = 14U;
    expected[4U].fault.expected_sequence = 15U;
    expected[4U].fault.observed_sequence = 16U;
    expected[4U].fault.detected_monotonic_ns = 17U;
    expected[4U].fault.feed_session_epoch = 17U;

    expected[5U] = Barrier(19U);
    expected[5U].producer_lane = 15U;
    expected[5U].owner = 16U;
    expected[6U] = Barrier(20U);
    expected[6U].kind = RecordKind::kFinalBarrier;
    expected[6U].producer_lane = 17U;
    expected[6U].owner = 18U;

    for (const CanonicalRecord& record : expected) {
        CHECK(outbox->Enqueue(record));
    }
    std::string error;
    CHECK(outbox->Flush(&error));

    std::vector<WalPosition> positions;
    positions.reserve(expected.size());
    for (std::size_t index = 0U; index < expected.size(); ++index) {
        RecordView actual{};
        CHECK(outbox->TryRead(index + 1U, &actual));
        CHECK(actual.record != nullptr);
        CHECK(Equal(expected[index], *actual.record));
        positions.push_back(actual.position);
    }
    CHECK(outbox->Complete(ConsumerKind::kRaw, positions));
    CHECK(outbox->Complete(ConsumerKind::kEvent, positions));
    CHECK(outbox->Complete(ConsumerKind::kKLine, positions));
    CHECK(outbox->Stop(&error));
}

void TestDecodedReadCacheHasConfiguredHardBound() {
    TemporaryDirectory directory("l2flow-outbox-cache-bound");
    DurableOutboxConfig config = Config(directory.path());
    config.commit_batch_records = 1U;
    config.read_cache_batches = 2U;
    std::string error;
    std::unique_ptr<DurableOutbox> outbox = DurableOutbox::Create(
        config, &error);
    CHECK(outbox != nullptr);
    CHECK(outbox->Start(&error));

    constexpr std::uint64_t kRecords = 64U;
    for (std::uint64_t frontier = 1U; frontier <= kRecords; ++frontier) {
        CHECK(outbox->Enqueue(Barrier(frontier)));
    }
    CHECK(outbox->Flush(&error));
    const auto before_reads = outbox->stats();
    CHECK(before_reads.indexed_records == kRecords);
    CHECK(before_reads.indexed_batches == kRecords);
    CHECK(before_reads.cached_records == 0U);

    std::vector<WalPosition> positions;
    positions.reserve(static_cast<std::size_t>(kRecords));
    for (std::uint64_t lsn = 1U; lsn <= kRecords; ++lsn) {
        RecordView view{};
        CHECK(outbox->TryRead(lsn, &view));
        CHECK(view.record->barrier.frontier_id == lsn);
        positions.push_back(view.position);
        CHECK(outbox->stats().cached_records <=
              config.read_cache_batches);
    }
    const auto after_reads = outbox->stats();
    CHECK(after_reads.cached_records == config.read_cache_batches);
    CHECK(after_reads.indexed_records == kRecords);

    CHECK(outbox->Complete(ConsumerKind::kRaw, positions));
    CHECK(outbox->Complete(ConsumerKind::kEvent, positions));
    CHECK(outbox->Complete(ConsumerKind::kKLine, positions));
    const auto reclaim_deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(2);
    while (outbox->stats().segments_reclaimed == 0U &&
           std::chrono::steady_clock::now() < reclaim_deadline) {
        std::this_thread::yield();
    }
    const auto reclaimed = outbox->stats();
    CHECK(reclaimed.segments_reclaimed != 0U);
    CHECK(reclaimed.indexed_records < kRecords);
    RecordView reclaimed_view{};
    CHECK(!outbox->TryRead(1U, &reclaimed_view));
    CHECK(outbox->healthy());
    CHECK(outbox->Stop(&error));
}

void TestColdReadDoesNotBlockWalState() {
#if defined(L2FLOW_TEST_WRAP_PREAD)
    TemporaryDirectory directory("l2flow-outbox-cold-read-isolation");
    DurableOutboxConfig config = Config(directory.path());
    config.commit_batch_records = 1U;
    config.read_cache_batches = 1U;
    std::string error;
    std::unique_ptr<DurableOutbox> outbox = DurableOutbox::Create(
        config, &error);
    CHECK(outbox != nullptr);
    CHECK(outbox->Start(&error));

    CHECK(outbox->Enqueue(Barrier(1U)));
    CHECK(outbox->Flush(&error));
    RecordView first{};
    CHECK(outbox->TryRead(1U, &first));

    CHECK(outbox->Enqueue(Barrier(2U)));
    CHECK(outbox->Flush(&error));
    RecordView second{};
    CHECK(outbox->TryRead(2U, &second));

    constexpr std::size_t kReaders = 8U;
    std::array<std::thread, kReaders> readers;
    std::array<std::atomic<bool>, kReaders> results{};
    std::atomic<std::size_t> ready{0U};
    std::atomic<bool> begin{false};
    BlockWalReads();
    for (std::size_t index = 0U; index < readers.size(); ++index) {
        results[index].store(false, std::memory_order_relaxed);
        readers[index] = std::thread([&, index] {
            ready.fetch_add(1U, std::memory_order_release);
            while (!begin.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            RecordView view{};
            results[index].store(
                outbox->TryRead(1U, &view) && view.position == first.position,
                std::memory_order_release);
        });
    }
    CHECK(WaitFor([&ready] {
        return ready.load(std::memory_order_acquire) == kReaders;
    }));
    begin.store(true, std::memory_order_release);
    CHECK(WaitFor([] {
        return g_pread_entered.load(std::memory_order_acquire);
    }));

    auto completion = std::async(std::launch::async, [&] {
        return outbox->CompleteOne(ConsumerKind::kEvent, first.position);
    });
    const bool completion_ready =
        completion.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (!completion_ready) {
        ReleaseWalReads();
    }
    CHECK(completion_ready);
    CHECK(completion.get());

    CHECK(outbox->Enqueue(Barrier(3U)));
    auto flush = std::async(std::launch::async, [&] {
        std::string flush_error;
        return outbox->Flush(&flush_error);
    });
    const bool flush_ready = flush.wait_for(std::chrono::seconds(2)) ==
                             std::future_status::ready;
    if (!flush_ready) {
        ReleaseWalReads();
    }
    CHECK(flush_ready);
    CHECK(flush.get());
    CHECK(outbox->durable_tail().lsn == 3U);

    auto cache_hit = std::async(std::launch::async, [&] {
        RecordView view{};
        return outbox->TryRead(2U, &view) && view.position == second.position;
    });
    const bool cache_hit_ready =
        cache_hit.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (!cache_hit_ready) {
        ReleaseWalReads();
    }
    CHECK(cache_hit_ready);
    CHECK(cache_hit.get());

    ReleaseWalReads();
    for (std::thread& reader : readers) {
        reader.join();
    }
    StopInterceptingWalReads();
    for (const std::atomic<bool>& result : results) {
        CHECK(result.load(std::memory_order_acquire));
    }
    CHECK(g_intercepted_preads.load(std::memory_order_relaxed) == 1U);
    CHECK(outbox->healthy());
    CHECK(outbox->Stop(&error));
#endif
}

void TestReclaimPreservesInFlightColdReadLease() {
#if defined(L2FLOW_TEST_WRAP_PREAD)
    TemporaryDirectory directory("l2flow-outbox-cold-read-reclaim");
    DurableOutboxConfig config = Config(directory.path());
    config.commit_batch_records = 1U;
    config.read_cache_batches = 1U;
    std::string error;
    std::unique_ptr<DurableOutbox> outbox = DurableOutbox::Create(
        config, &error);
    CHECK(outbox != nullptr);
    CHECK(outbox->Start(&error));

    constexpr std::uint64_t kRecords = 128U;
    for (std::uint64_t frontier = 1U; frontier <= kRecords; ++frontier) {
        CHECK(outbox->Enqueue(Barrier(frontier)));
    }
    CHECK(outbox->Flush(&error));
    CHECK(outbox->stats().segments_created > 1U);

    const auto wal_segments = [&outbox] {
        std::vector<std::filesystem::path> paths;
        for (const auto& entry :
             std::filesystem::directory_iterator(outbox->run_directory())) {
            if (entry.path().extension() == ".wal") {
                paths.push_back(entry.path());
            }
        }
        std::sort(paths.begin(), paths.end());
        return paths;
    };
    const auto wal_bytes = [](const auto& paths) {
        std::uint64_t bytes = 0U;
        for (const std::filesystem::path& path : paths) {
            bytes += static_cast<std::uint64_t>(
                std::filesystem::file_size(path));
        }
        return bytes;
    };
    const std::vector<std::filesystem::path> initial_segments =
        wal_segments();
    CHECK(initial_segments.size() > 1U);
    const std::uint64_t first_segment_bytes =
        static_cast<std::uint64_t>(
            std::filesystem::file_size(initial_segments.front()));
    const auto before_reclaim = outbox->stats();
    CHECK(before_reclaim.reservoir_bytes == wal_bytes(initial_segments));
    const std::uint64_t expected_reclaimed =
        before_reclaim.segments_created - 1U;

    std::vector<WalPosition> positions;
    positions.reserve(static_cast<std::size_t>(kRecords));
    for (std::uint64_t lsn = 1U; lsn <= kRecords; ++lsn) {
        RecordView view{};
        CHECK(outbox->TryRead(lsn, &view));
        positions.push_back(view.position);
    }

    BlockWalReads();
    std::atomic<bool> read_succeeded{false};
    std::thread reader([&] {
        RecordView view{};
        read_succeeded.store(
            outbox->TryRead(1U, &view) && view.position == positions.front(),
            std::memory_order_release);
    });
    CHECK(WaitFor([] {
        return g_pread_entered.load(std::memory_order_acquire);
    }));

    auto complete = std::async(std::launch::async, [&] {
        return outbox->Complete(ConsumerKind::kRaw, positions) &&
               outbox->Complete(ConsumerKind::kEvent, positions) &&
               outbox->Complete(ConsumerKind::kKLine, positions);
    });
    const bool complete_ready = complete.wait_for(std::chrono::seconds(2)) ==
                                std::future_status::ready;
    if (!complete_ready) {
        ReleaseWalReads();
    }
    CHECK(complete_ready);
    CHECK(complete.get());

    auto reclaimed = std::async(std::launch::async, [&] {
        return WaitFor([&] {
            return outbox->stats().segments_reclaimed ==
                   expected_reclaimed;
        });
    });
    const bool reclaim_ready = reclaimed.wait_for(std::chrono::seconds(6)) ==
                               std::future_status::ready;
    if (!reclaim_ready) {
        ReleaseWalReads();
    }
    CHECK(reclaim_ready);
    CHECK(reclaimed.get());
    const std::vector<std::filesystem::path> linked_segments =
        wal_segments();
    CHECK(linked_segments.size() == 1U);
    CHECK(outbox->stats().reservoir_bytes ==
          wal_bytes(linked_segments) + first_segment_bytes);

    ReleaseWalReads();
    reader.join();
    StopInterceptingWalReads();
    CHECK(read_succeeded.load(std::memory_order_acquire));
    CHECK(g_intercepted_preads.load(std::memory_order_relaxed) == 1U);
    CHECK(outbox->stats().reservoir_bytes == wal_bytes(linked_segments));
    RecordView reclaimed_view{};
    CHECK(!outbox->TryRead(1U, &reclaimed_view));
    CHECK(outbox->healthy());
    CHECK(outbox->Stop(&error));
#endif
}

void TestDiskReadRejectsCorruptFrame() {
    TemporaryDirectory directory("l2flow-outbox-corrupt-frame");
    std::unique_ptr<DurableOutbox> outbox = StartedOutbox(directory.path());
    CHECK(outbox->Enqueue(Barrier(1U)));
    std::string error;
    CHECK(outbox->Flush(&error));

    std::filesystem::path segment;
    for (const auto& entry :
         std::filesystem::directory_iterator(outbox->run_directory())) {
        if (entry.path().extension() == ".wal") {
            CHECK(segment.empty());
            segment = entry.path();
        }
    }
    CHECK(!segment.empty());
    std::fstream file(segment, std::ios::binary | std::ios::in |
                                   std::ios::out);
    CHECK(file.good());
    file.seekg(-1, std::ios::end);
    char value = 0;
    file.read(&value, 1);
    CHECK(file.good());
    value = static_cast<char>(
        static_cast<unsigned char>(value) ^ UINT8_C(0x80));
    file.seekp(-1, std::ios::end);
    file.write(&value, 1);
    file.flush();
    CHECK(file.good());

    RecordView view{};
    CHECK(!outbox->TryRead(1U, &view));
    CHECK(!outbox->healthy());
    CHECK(outbox->fatal_error().find("checksum") != std::string::npos);
    CHECK(!outbox->Stop(&error));
}

void TestRequestSpoolPersistsBytesAndStateMachine() {
    TemporaryDirectory directory("l2flow-request-spool");
    RequestSpoolConfig config{};
    config.directory = directory.path() / "event";
    config.maximum_bytes = 2U * 1'024U * 1'024U;
    std::string error;
    std::unique_ptr<RequestSpool> spool = RequestSpool::Create(config, &error);
    CHECK(spool != nullptr);
    CHECK(spool->Start(&error));

    const std::array<WalPosition, 2U> positions{
        WalPosition{11U, 2U, 0U}, WalPosition{12U, 2U, 1U}};
    const std::array<std::byte, 5U> revision{
        std::byte{1U}, std::byte{2U}, std::byte{3U},
        std::byte{4U}, std::byte{5U}};
    const std::array<std::byte, 3U> marker{
        std::byte{9U}, std::byte{8U}, std::byte{7U}};
    const std::array<RequestPayload, 2U> requests{
        RequestPayload{RequestKind::kEventRevision, 2U, "revision-query",
                       "revision-token", revision},
        RequestPayload{RequestKind::kEventMarker, 1U, "marker-query",
                       "marker-token", marker}};
    RequestGroupHandle handle{};
    CHECK(spool->PrepareGroup(ConsumerKind::kEvent, positions, requests,
                              &handle, &error));
    CHECK(handle.request_count == 2U);
    CHECK(spool->stats().live_groups == 1U);
    CHECK(spool->stats().live_bytes > revision.size() + marker.size());
    CHECK(spool->stats().reserved_bytes == spool->stats().live_bytes);

    const std::filesystem::path file = config.directory /
        ("group-" + std::to_string(handle.sequence) + ".wal");
    std::ifstream input(file, std::ios::binary);
    CHECK(input.good());
    const std::string bytes((std::istreambuf_iterator<char>(input)),
                            std::istreambuf_iterator<char>());
    CHECK(bytes.find("revision-query") != std::string::npos);
    CHECK(bytes.find("revision-token") != std::string::npos);
    CHECK(bytes.find("marker-query") != std::string::npos);
    CHECK(bytes.find("marker-token") != std::string::npos);

    CHECK(spool->SetState(handle, 0U, RequestState::kSent, &error));
    CHECK(spool->SetState(handle, 0U, RequestState::kUnknown, &error));
    CHECK(spool->SetState(handle, 0U, RequestState::kSent, &error));
    CHECK(spool->SetState(handle, 0U, RequestState::kAcked, &error));
    CHECK(spool->SetState(handle, 1U, RequestState::kSent, &error));
    CHECK(spool->SetState(handle, 1U, RequestState::kAcked, &error));
    CHECK(spool->Retire(handle, &error));
    CHECK(spool->stats().live_groups == 0U);
    CHECK(spool->stats().live_bytes == 0U);
    CHECK(spool->stats().reserved_bytes == 0U);
    CHECK(!std::filesystem::exists(file));
    CHECK(spool->Stop(&error));
}

void TestRequestSpoolRejectsIllegalLifecycleTransitions() {
    const std::array<WalPosition, 1U> positions{
        WalPosition{1U, 1U, 0U}};
    const std::array<std::byte, 4U> payload{
        std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
    const std::array<RequestPayload, 1U> requests{
        RequestPayload{RequestKind::kEventRevision, 1U, "query", "token",
                       payload}};

    {
        TemporaryDirectory directory("l2flow-request-spool-retire-state");
        RequestSpoolConfig config{};
        config.directory = directory.path() / "event";
        config.maximum_bytes = 1U * 1'024U * 1'024U;
        std::string error;
        std::unique_ptr<RequestSpool> spool =
            RequestSpool::Create(config, &error);
        CHECK(spool != nullptr);
        CHECK(spool->Start(&error));
        RequestGroupHandle handle{};
        CHECK(spool->PrepareGroup(ConsumerKind::kEvent, positions, requests,
                                  &handle, &error));
        CHECK(!spool->Retire(handle, &error));
        CHECK(!spool->healthy());
        CHECK(error.find("not fully acknowledged") != std::string::npos);
        CHECK(spool->stats().live_groups == 1U);
        CHECK(spool->stats().reserved_bytes == spool->stats().live_bytes);
        CHECK(!spool->Stop(&error));
    }

    {
        TemporaryDirectory directory("l2flow-request-spool-illegal-state");
        RequestSpoolConfig config{};
        config.directory = directory.path() / "event";
        config.maximum_bytes = 1U * 1'024U * 1'024U;
        std::string error;
        std::unique_ptr<RequestSpool> spool =
            RequestSpool::Create(config, &error);
        CHECK(spool != nullptr);
        CHECK(spool->Start(&error));
        RequestGroupHandle handle{};
        CHECK(spool->PrepareGroup(ConsumerKind::kEvent, positions, requests,
                                  &handle, &error));
        CHECK(!spool->SetState(handle, 0U, RequestState::kAcked, &error));
        CHECK(!spool->healthy());
        CHECK(error.find("state transition") != std::string::npos);
        CHECK(spool->stats().state_updates == 0U);
        CHECK(spool->stats().live_groups == 1U);
        CHECK(!spool->Stop(&error));
    }
}

void TestRequestSpoolEnforcesGlobalCapacity() {
    TemporaryDirectory directory("l2flow-request-spool-capacity");
    RequestSpoolConfig config{};
    config.directory = directory.path() / "event";
    config.maximum_bytes = 1U * 1'024U * 1'024U;
    std::string error;
    std::unique_ptr<RequestSpool> spool = RequestSpool::Create(config, &error);
    CHECK(spool != nullptr);
    CHECK(spool->Start(&error));

    std::vector<std::byte> payload(600U * 1'024U, std::byte{0x5aU});
    const std::array<RequestPayload, 1U> requests{
        RequestPayload{RequestKind::kEventRevision, 1U, "query", "token",
                       payload}};
    const std::array<WalPosition, 1U> first_positions{
        WalPosition{1U, 1U, 0U}};
    const std::array<WalPosition, 1U> second_positions{
        WalPosition{2U, 2U, 0U}};
    RequestGroupHandle first{};
    CHECK(spool->PrepareGroup(ConsumerKind::kEvent, first_positions, requests,
                              &first, &error));
    const auto before = spool->stats();
    CHECK(before.live_groups == 1U);
    CHECK(before.reserved_bytes == before.live_bytes);
    CHECK(before.reserved_bytes <= config.maximum_bytes);

    RequestGroupHandle second{};
    CHECK(!spool->PrepareGroup(ConsumerKind::kEvent, second_positions,
                               requests, &second, &error));
    CHECK(second.sequence == 0U);
    CHECK(error.find("capacity exhausted") != std::string::npos);
    const auto after = spool->stats();
    CHECK(after.preparing_groups == 0U);
    CHECK(after.live_groups == 1U);
    CHECK(after.live_bytes == before.live_bytes);
    CHECK(after.reserved_bytes == before.reserved_bytes);
    CHECK(after.reserved_bytes <= config.maximum_bytes);
    CHECK(!spool->healthy());
    CHECK(!spool->Stop(&error));
}

[[nodiscard]] std::uint32_t ReadLe32(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    CHECK(offset <= bytes.size());
    CHECK(sizeof(std::uint32_t) <= bytes.size() - offset);
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        value |= static_cast<std::uint32_t>(
                     std::to_integer<std::uint8_t>(bytes[offset + index]))
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

[[nodiscard]] std::uint64_t ReadLe64(
    std::span<const std::byte> bytes,
    std::size_t offset) {
    CHECK(offset <= bytes.size());
    CHECK(sizeof(std::uint64_t) <= bytes.size() - offset);
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        value |= static_cast<std::uint64_t>(
                     std::to_integer<std::uint8_t>(bytes[offset + index]))
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

void CheckSpoolChecksums(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    CHECK(input.good());
    const std::string bytes((std::istreambuf_iterator<char>(input)),
                            std::istreambuf_iterator<char>());
    CHECK(bytes.size() >= 64U);
    const std::span<const std::byte> view(
        reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
    const std::uint64_t position_count = ReadLe64(view, 24U);
    const std::uint32_t request_count = ReadLe32(view, 32U);
    CHECK(ReadLe32(view, 36U) ==
          l2flow::checksum::Crc32c(view.first(36U)));
    const std::size_t immutable_offset =
        40U + static_cast<std::size_t>(request_count) * 2U;
    CHECK(position_count <=
          (bytes.size() - immutable_offset) / 20U);
    std::size_t offset = immutable_offset +
        static_cast<std::size_t>(position_count) * 20U;
    for (std::uint32_t index = 0U; index < request_count; ++index) {
        CHECK(offset <= bytes.size());
        CHECK(36U <= bytes.size() - offset);
        const std::uint32_t query_bytes = ReadLe32(view, offset + 12U);
        const std::uint32_t token_bytes = ReadLe32(view, offset + 16U);
        const std::uint64_t payload_bytes = ReadLe64(view, offset + 20U);
        const std::uint32_t payload_checksum =
            ReadLe32(view, offset + 28U);
        offset += 36U;
        const std::uint64_t variable_bytes =
            static_cast<std::uint64_t>(query_bytes) + token_bytes +
            payload_bytes;
        CHECK(variable_bytes <= bytes.size() - offset);
        offset += static_cast<std::size_t>(query_bytes) +
                  static_cast<std::size_t>(token_bytes);
        CHECK(payload_bytes <= bytes.size() - offset);
        CHECK(l2flow::checksum::Crc32c(view.subspan(
                  offset, static_cast<std::size_t>(payload_bytes))) ==
              payload_checksum);
        offset += static_cast<std::size_t>(payload_bytes);
    }
    CHECK(offset + 24U == bytes.size());
    CHECK(ReadLe32(view, offset + 8U) ==
          l2flow::checksum::Crc32c(
              view.subspan(immutable_offset, offset - immutable_offset)));
    CHECK(ReadLe64(view, offset + 16U) == bytes.size());
}

void TestRequestSpoolConcurrentPrepareAndRetire() {
    TemporaryDirectory directory("l2flow-request-spool-concurrent");
    RequestSpoolConfig config{};
    config.directory = directory.path() / "event";
    config.maximum_bytes = 16U * 1'024U * 1'024U;
    std::string error;
    std::unique_ptr<RequestSpool> spool = RequestSpool::Create(config, &error);
    CHECK(spool != nullptr);
    CHECK(spool->Start(&error));

    constexpr std::size_t kThreads = 32U;
    std::array<RequestGroupHandle, kThreads> handles{};
    std::array<bool, kThreads> prepared{};
    std::array<std::string, kThreads> errors{};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (std::size_t index = 0U; index < kThreads; ++index) {
        threads.emplace_back([&, index] {
            const std::array<WalPosition, 1U> positions{
                WalPosition{static_cast<std::uint64_t>(index + 1U),
                            static_cast<std::uint64_t>(index + 1U), 0U}};
            std::vector<std::byte> revision(32U * 1'024U);
            for (std::size_t byte = 0U; byte < revision.size(); ++byte) {
                revision[byte] = static_cast<std::byte>(index + byte);
            }
            const std::array<std::byte, 4U> marker{
                std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
            const std::string query = "revision-" + std::to_string(index);
            const std::string token = "token-" + std::to_string(index);
            const std::array<RequestPayload, 2U> requests{
                RequestPayload{RequestKind::kEventRevision, 64U, query,
                               token, revision},
                RequestPayload{RequestKind::kEventMarker, 1U, "marker",
                               "marker-token", marker}};
            prepared[index] = spool->PrepareGroup(
                ConsumerKind::kEvent, positions, requests, &handles[index],
                &errors[index]);
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    CHECK(std::all_of(prepared.begin(), prepared.end(),
                      [](bool value) { return value; }));
    std::array<std::uint64_t, kThreads> sequences{};
    for (std::size_t index = 0U; index < kThreads; ++index) {
        sequences[index] = handles[index].sequence;
        CHECK(handles[index].request_count == 2U);
        CheckSpoolChecksums(config.directory /
            ("group-" + std::to_string(handles[index].sequence) + ".wal"));
    }
    std::sort(sequences.begin(), sequences.end());
    CHECK(std::adjacent_find(sequences.begin(), sequences.end()) ==
          sequences.end());
    const auto prepared_stats = spool->stats();
    CHECK(prepared_stats.groups_prepared == kThreads);
    CHECK(prepared_stats.preparing_groups == 0U);
    CHECK(prepared_stats.live_groups == kThreads);
    CHECK(prepared_stats.live_bytes == prepared_stats.reserved_bytes);
    CHECK(prepared_stats.reserved_bytes <= config.maximum_bytes);

    threads.clear();
    for (std::size_t index = 0U; index < kThreads; ++index) {
        threads.emplace_back([&, index] {
            std::string local_error;
            CHECK(spool->SetState(handles[index], 0U, RequestState::kSent,
                                  &local_error));
            CHECK(spool->SetState(handles[index], 0U, RequestState::kAcked,
                                  &local_error));
            CHECK(spool->SetState(handles[index], 1U, RequestState::kSent,
                                  &local_error));
            CHECK(spool->SetState(handles[index], 1U, RequestState::kAcked,
                                  &local_error));
            CHECK(spool->Retire(handles[index], &local_error));
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    const auto retired_stats = spool->stats();
    CHECK(retired_stats.groups_retired == kThreads);
    CHECK(retired_stats.live_groups == 0U);
    CHECK(retired_stats.live_bytes == 0U);
    CHECK(retired_stats.reserved_bytes == 0U);
    CHECK(spool->healthy());
    CHECK(spool->Stop(&error));
}

void TestRequestSpoolPrepareFailureRollsBackReservation() {
    TemporaryDirectory directory("l2flow-request-spool-failure");
    RequestSpoolConfig config{};
    config.directory = directory.path() / "event";
    config.maximum_bytes = 1U * 1'024U * 1'024U;
    std::string error;
    std::unique_ptr<RequestSpool> spool = RequestSpool::Create(config, &error);
    CHECK(spool != nullptr);
    CHECK(spool->Start(&error));
    std::filesystem::create_directories(config.directory);
    {
        std::ofstream collision(config.directory / "group-1.wal");
        CHECK(collision.good());
    }
    const std::array<WalPosition, 1U> positions{WalPosition{1U, 1U, 0U}};
    const std::array<std::byte, 4U> payload{
        std::byte{1U}, std::byte{2U}, std::byte{3U}, std::byte{4U}};
    const std::array<RequestPayload, 1U> requests{
        RequestPayload{RequestKind::kEventRevision, 1U, "query", "token",
                       payload}};
    RequestGroupHandle handle{};
    CHECK(!spool->PrepareGroup(ConsumerKind::kEvent, positions, requests,
                               &handle, &error));
    CHECK(handle.sequence == 0U);
    const auto stats = spool->stats();
    CHECK(stats.preparing_groups == 0U);
    CHECK(stats.live_groups == 0U);
    CHECK(stats.reserved_bytes == 0U);
    CHECK(stats.live_bytes == 0U);
    CHECK(!spool->healthy());
    CHECK(!spool->Stop(&error));
}

void TestRequestSpoolStopWaitsForInFlightPrepare() {
    TemporaryDirectory directory("l2flow-request-spool-stop");
    RequestSpoolConfig config{};
    config.directory = directory.path() / "event";
    config.maximum_bytes = 64U * 1'024U * 1'024U;
    std::string error;
    std::unique_ptr<RequestSpool> spool = RequestSpool::Create(config, &error);
    CHECK(spool != nullptr);
    CHECK(spool->Start(&error));

    const std::array<WalPosition, 1U> positions{WalPosition{1U, 1U, 0U}};
    std::vector<std::byte> payload(32U * 1'024U * 1'024U,
                                   std::byte{0x5aU});
    const std::array<RequestPayload, 1U> requests{
        RequestPayload{RequestKind::kEventRevision, 1U, "query", "token",
                       payload}};
    RequestGroupHandle handle{};
    std::string prepare_error;
    std::atomic<bool> prepared{false};
    std::thread prepare([&] {
        prepared.store(spool->PrepareGroup(
                           ConsumerKind::kEvent, positions, requests, &handle,
                           &prepare_error),
                       std::memory_order_release);
    });
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    while (spool->stats().preparing_groups == 0U &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    CHECK(spool->stats().preparing_groups == 1U);
    CHECK(spool->Stop(&error));
    prepare.join();
    CHECK(prepared.load(std::memory_order_acquire));
    CHECK(handle.sequence != 0U);
    const auto stats = spool->stats();
    CHECK(stats.preparing_groups == 0U);
    CHECK(stats.live_groups == 1U);
    CHECK(stats.live_bytes == stats.reserved_bytes);
    CHECK(spool->healthy());
}

void TestContinuityTransitionsUseBarrierCursors() {
    TemporaryDirectory directory("l2flow-continuity");
    std::unique_ptr<DurableOutbox> outbox = StartedOutbox(directory.path());
    CHECK(outbox->Enqueue(Barrier(1U)));
    std::string error;
    CHECK(outbox->Flush(&error));
    RecordView first{};
    CHECK(outbox->TryRead(1U, &first));

    l2flow::outbox::ContinuityConfig config{};
    config.derived_stale_after_ns = 100U;
    ContinuityController controller(config, outbox.get());
    const l2flow::outbox::ConsumerHealth health{};
    const auto initial = controller.Evaluate(health, 1'000U);
    CHECK(initial.frontier_id == 1U);
    CHECK(initial.barrier == first.position);
    CHECK(initial.state == ContinuityState::kDerivedCatchup);

    CHECK(outbox->CompleteOne(ConsumerKind::kEvent, first.position));
    CHECK(outbox->CompleteOne(ConsumerKind::kKLine, first.position));
    CHECK(controller.Evaluate(health, 1'010U).state ==
          ContinuityState::kRawCatchup);
    CHECK(outbox->CompleteOne(ConsumerKind::kRaw, first.position));
    const auto normal = controller.Evaluate(health, 1'020U);
    CHECK(normal.state == ContinuityState::kNormal);
    CHECK(normal.event_current_authoritative);

    CHECK(outbox->Enqueue(Barrier(2U)));
    CHECK(outbox->Flush(&error));
    RecordView second{};
    CHECK(outbox->TryRead(2U, &second));
    const auto durable_second = controller.Evaluate(health, 1'030U);
    CHECK(durable_second.frontier_id == 2U);
    CHECK(durable_second.barrier == second.position);
    CHECK(durable_second.state == ContinuityState::kDerivedCatchup);
    CHECK(outbox->CompleteOne(ConsumerKind::kRaw, second.position));
    CHECK(outbox->CompleteOne(ConsumerKind::kKLine, second.position));
    CHECK(controller.Evaluate(health, 1'050U).state ==
          ContinuityState::kDerivedCatchup);
    const auto stale = controller.Evaluate(health, 1'200U);
    CHECK(stale.state == ContinuityState::kRawOnlyStale);
    CHECK(!stale.event_current_authoritative);

    CHECK(outbox->CompleteOne(ConsumerKind::kEvent, second.position));
    auto event_unavailable = health;
    event_unavailable.event = false;
    const auto short_outage = controller.Evaluate(event_unavailable, 1'300U);
    CHECK(short_outage.state == ContinuityState::kDerivedCatchup);
    CHECK(!short_outage.event_current_authoritative);
    CHECK(controller.Evaluate(event_unavailable, 1'400U).state ==
          ContinuityState::kRawOnlyStale);
    CHECK(outbox->Stop(&error));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 &&
        std::string_view(argv[1]) == "--request-spool-only") {
        TestSharedCrc32c();
        TestRequestSpoolPersistsBytesAndStateMachine();
        TestRequestSpoolRejectsIllegalLifecycleTransitions();
        TestRequestSpoolEnforcesGlobalCapacity();
        TestRequestSpoolConcurrentPrepareAndRetire();
        TestRequestSpoolPrepareFailureRollsBackReservation();
        TestRequestSpoolStopWaitsForInFlightPrepare();
        std::cout << "all request spool tests passed\n";
        return 0;
    }
    CHECK(argc == 1);
    TestSharedCrc32c();
    TestDurableVisibilityAndExactCursors();
    TestConcurrentProducerOrderDoesNotUseIngressMaximum();
    TestEveryCanonicalRecordKindRoundTripsFromDisk();
    TestDecodedReadCacheHasConfiguredHardBound();
    TestColdReadDoesNotBlockWalState();
    TestReclaimPreservesInFlightColdReadLease();
    TestDiskReadRejectsCorruptFrame();
    TestRequestSpoolPersistsBytesAndStateMachine();
    TestRequestSpoolRejectsIllegalLifecycleTransitions();
    TestRequestSpoolEnforcesGlobalCapacity();
    TestRequestSpoolConcurrentPrepareAndRetire();
    TestRequestSpoolPrepareFailureRollsBackReservation();
    TestRequestSpoolStopWaitsForInFlightPrepare();
    TestContinuityTransitionsUseBarrierCursors();
    std::cout << "all outbox tests passed\n";
    return 0;
}
