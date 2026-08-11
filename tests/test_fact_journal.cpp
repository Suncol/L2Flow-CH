#include "l2flow/journal/fact_journal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#if defined(L2FLOW_TEST_WRAP_PWRITE)
namespace {

std::atomic<bool> g_intercept_pwrite{false};
std::atomic<bool> g_pwrite_entered{false};
std::atomic<bool> g_release_pwrite{false};
std::atomic<bool> g_fail_pwrite{false};
std::atomic<bool> g_intercept_pread{false};
std::atomic<bool> g_pread_entered{false};
std::atomic<bool> g_release_pread{false};
std::atomic<bool> g_fail_fdatasync{false};

}  // namespace

extern "C" ssize_t __real_pwrite(int fd,
                                  const void* buffer,
                                  std::size_t count,
                                  off_t offset);

extern "C" ssize_t __wrap_pwrite(int fd,
                                  const void* buffer,
                                  std::size_t count,
                                  off_t offset) {
    if (g_intercept_pwrite.load(std::memory_order_acquire)) {
        g_pwrite_entered.store(true, std::memory_order_release);
        while (!g_release_pwrite.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        if (g_fail_pwrite.load(std::memory_order_acquire)) {
            errno = ENOSPC;
            return -1;
        }
    }
    return __real_pwrite(fd, buffer, count, offset);
}

extern "C" ssize_t __real_pread(int fd,
                                 void* buffer,
                                 std::size_t count,
                                 off_t offset);

extern "C" ssize_t __wrap_pread(int fd,
                                 void* buffer,
                                 std::size_t count,
                                 off_t offset) {
    if (g_intercept_pread.load(std::memory_order_acquire)) {
        g_pread_entered.store(true, std::memory_order_release);
        while (!g_release_pread.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    return __real_pread(fd, buffer, count, offset);
}

extern "C" int __real_fdatasync(int fd);

extern "C" int __wrap_fdatasync(int fd) {
    if (g_fail_fdatasync.load(std::memory_order_acquire)) {
        errno = EIO;
        return -1;
    }
    return __real_fdatasync(fd);
}
#endif

namespace {

using namespace l2flow::ingest;
using namespace l2flow::journal;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

template <typename Predicate>
bool WaitFor(Predicate predicate) {
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

class TemporaryFile final {
public:
    TemporaryFile() {
        std::string pattern = "/tmp/l2flow-fact-journal-test-XXXXXX";
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');
        const int fd = ::mkstemp(writable.data());
        CHECK(fd >= 0);
        CHECK(::close(fd) == 0);
        path_ = writable.data();
    }

    ~TemporaryFile() {
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove(path_, ignored));
    }

    TemporaryFile(const TemporaryFile&) = delete;
    TemporaryFile& operator=(const TemporaryFile&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

CanonicalTick Tick(std::uint64_t sequence,
                   std::uint32_t channel = 7U,
                   Market market = Market::kShenzhen) {
    CanonicalTick tick{};
    tick.common.ingress_sequence = 1'000U + sequence;
    tick.common.vendor_sequence_id = 2'000U + sequence;
    tick.common.receive_monotonic_ns = 3'000U + sequence;
    tick.common.native_sequence = sequence;
    tick.common.exchange_time_ns_from_midnight = 34'200'000'000'000U;
    tick.common.vendor_local_time_ns_from_midnight = 34'200'100'000'000U;
    tick.common.quality_flags = kQualitySequenceGapBefore;
    tick.common.gap_epoch = 5U;
    tick.common.gap_before_first = sequence - 1U;
    tick.common.gap_before_last = sequence - 1U;
    tick.common.trade_date = 20260809U;
    tick.common.instrument_id = 42U;
    tick.common.instrument_ordinal = 17U;
    tick.common.channel = channel;
    tick.common.exchange_time_raw = 93'000'123U;
    tick.common.vendor_local_time_raw = 93'000'456U;
    tick.common.exchange_time_valid = true;
    tick.common.vendor_local_time_valid = true;
    tick.common.message_key = market == Market::kShanghai
        ? MessageKey{4U, 101U, 24U}
        : MessageKey{6U, 101U, 36U};
    tick.common.kind = market == Market::kShanghai
        ? CanonicalKind::kShanghaiTick
        : CanonicalKind::kShenzhenTransaction;
    tick.common.identity.market = market;
    tick.common.identity.security_id_source_size = 2U;
    tick.common.identity.security_id_size = 6U;
    for (std::size_t index = 0U;
         index < tick.common.identity.security_id_source.size(); ++index) {
        tick.common.identity.security_id_source[index] =
            static_cast<std::byte>(0x20U + index);
        tick.common.identity.security_id[index] =
            static_cast<std::byte>(0x40U + index);
        tick.common.md_stream_id[index] =
            static_cast<std::byte>(0x60U + index);
    }
    tick.common.md_stream_id_size = 3U;
    tick.price = FixedDecimal{-123'456, -12'345'600, 4U, true, true};
    tick.amount = FixedDecimal{987'654, 987'654'000, 3U, true, true};
    tick.quantity = ScaledInteger{321, 2U, true};
    tick.primary_order_id = -1001;
    tick.buy_order_id = 1002;
    tick.sell_order_id = -1003;
    tick.sh_add_matched_quantity_raw = 44;
    tick.validity = kTickPriceValid | kTickQuantityValid |
                    kTickAmountValid | kTickExchangeTimeValid;
    tick.raw_type = -7;
    tick.raw_side = 8;
    tick.action = TickAction::kTrade;
    tick.side = Side::kBorrow;
    tick.aggressor = Aggressor::kNeutral;
    tick.order_type = OrderType::kSameSideBest;
    tick.phase = TradingPhase::kClosingCall;
    return tick;
}

bool FullTickEqual(const CanonicalTick& left, const CanonicalTick& right) {
    const CanonicalCommon& a = left.common;
    const CanonicalCommon& b = right.common;
    return a.ingress_sequence == b.ingress_sequence &&
           a.vendor_sequence_id == b.vendor_sequence_id &&
           a.receive_monotonic_ns == b.receive_monotonic_ns &&
           a.native_sequence == b.native_sequence &&
           a.exchange_time_ns_from_midnight ==
               b.exchange_time_ns_from_midnight &&
           a.vendor_local_time_ns_from_midnight ==
               b.vendor_local_time_ns_from_midnight &&
           a.quality_flags == b.quality_flags &&
           a.gap_epoch == b.gap_epoch &&
           a.gap_before_first == b.gap_before_first &&
           a.gap_before_last == b.gap_before_last &&
           a.trade_date == b.trade_date &&
           a.instrument_id == b.instrument_id &&
           a.instrument_ordinal == b.instrument_ordinal &&
           a.channel == b.channel &&
           a.exchange_time_raw == b.exchange_time_raw &&
           a.vendor_local_time_raw == b.vendor_local_time_raw &&
           a.exchange_time_valid == b.exchange_time_valid &&
           a.vendor_local_time_valid == b.vendor_local_time_valid &&
           a.message_key == b.message_key && a.kind == b.kind &&
           a.identity.market == b.identity.market &&
           a.identity.security_id_source_size ==
               b.identity.security_id_source_size &&
           a.identity.security_id_size == b.identity.security_id_size &&
           a.identity.security_id_source ==
               b.identity.security_id_source &&
           a.identity.security_id == b.identity.security_id &&
           a.md_stream_id_size == b.md_stream_id_size &&
           a.md_stream_id == b.md_stream_id &&
           left.price.raw == right.price.raw &&
           left.price.p6 == right.price.p6 &&
           left.price.source_scale == right.price.source_scale &&
           left.price.raw_valid == right.price.raw_valid &&
           left.price.p6_valid == right.price.p6_valid &&
           left.amount.raw == right.amount.raw &&
           left.amount.p6 == right.amount.p6 &&
           left.amount.source_scale == right.amount.source_scale &&
           left.amount.raw_valid == right.amount.raw_valid &&
           left.amount.p6_valid == right.amount.p6_valid &&
           left.quantity.raw == right.quantity.raw &&
           left.quantity.scale == right.quantity.scale &&
           left.quantity.valid == right.quantity.valid &&
           left.primary_order_id == right.primary_order_id &&
           left.buy_order_id == right.buy_order_id &&
           left.sell_order_id == right.sell_order_id &&
           left.sh_add_matched_quantity_raw ==
               right.sh_add_matched_quantity_raw &&
           left.validity == right.validity &&
           left.raw_type == right.raw_type &&
           left.raw_side == right.raw_side &&
           left.action == right.action && left.side == right.side &&
           left.aggressor == right.aggressor &&
           left.order_type == right.order_type && left.phase == right.phase;
}

std::unique_ptr<CanonicalFactJournal> Create(
    const TemporaryFile& file,
    std::size_t cache_entries = 8U,
    std::uint64_t maximum_records = 100U,
    std::uint64_t maximum_directory_pages = 262'144U) {
    std::string error;
    auto journal = CanonicalFactJournal::Create(
        FactJournalConfig{20260809U, file.path(), cache_entries,
                          maximum_records, maximum_directory_pages},
        &error);
    if (journal == nullptr) {
        std::cerr << error << '\n';
    }
    CHECK(journal != nullptr);
    return journal;
}

void TestDualConsumerAndStableCodec() {
    TemporaryFile file;
    auto journal = Create(file);
    const CanonicalTick tick = Tick(101U);

    const auto event_first = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{tick});
    CHECK(event_first.size() == 1U);
    CHECK(event_first[0U].code == AdmitCode::kNew);
    CHECK(event_first[0U].handle.offset == kFactJournalFileHeaderBytes);

    const auto kline_first = journal->AdmitBatch(
        FactConsumer::kKLine, std::array{tick});
    CHECK(kline_first[0U].code == AdmitCode::kNew);
    CHECK(kline_first[0U].handle == event_first[0U].handle);
    CHECK(journal->AdmitBatch(FactConsumer::kEvent, std::array{tick})[0U].code ==
          AdmitCode::kDuplicate);
    CHECK(journal->AdmitBatch(FactConsumer::kKLine, std::array{tick})[0U].code ==
          AdmitCode::kDuplicate);

    CanonicalTick decoded{};
    CHECK(journal->Read(event_first[0U].handle, &decoded));
    CHECK(FullTickEqual(decoded, tick));
    const FactJournalStats stats = journal->stats();
    CHECK(stats.records == 1U);
    CHECK(stats.record_bytes == kFactJournalRecordBytes);
    CHECK(stats.file_bytes ==
          kFactJournalFileHeaderBytes + kFactJournalRecordBytes);
    CHECK(stats.consumer_new == 2U);
    CHECK(stats.duplicates == 2U);
    CHECK(stats.hot_cache_hits >= 3U);
    CHECK(std::filesystem::file_size(file.path()) == stats.file_bytes);
    CHECK(!journal->DropFileCache());
    CHECK(journal->Flush());
    CHECK(journal->DropFileCache());
}

void TestBusinessEqualityAndConflict() {
    TemporaryFile file;
    auto journal = Create(file);
    const CanonicalTick original = Tick(201U);
    CanonicalTick provenance = original;
    provenance.common.ingress_sequence += 1U;
    provenance.common.vendor_sequence_id += 2U;
    provenance.common.receive_monotonic_ns += 3U;
    provenance.common.exchange_time_ns_from_midnight += 4U;
    provenance.common.vendor_local_time_ns_from_midnight += 5U;
    provenance.common.vendor_local_time_raw += 6U;
    provenance.common.quality_flags ^= kQualityHoleFill;
    provenance.common.gap_epoch += 7U;
    provenance.common.gap_before_first += 8U;
    provenance.common.gap_before_last += 9U;
    provenance.common.instrument_ordinal += 10U;
    provenance.common.exchange_time_valid = false;
    provenance.common.vendor_local_time_valid = false;
    provenance.common.md_stream_id_size = 1U;
    provenance.common.md_stream_id[0U] = std::byte{0xffU};
    provenance.price.p6 += 11;
    provenance.price.p6_valid = false;
    provenance.amount.p6 += 12;
    provenance.amount.p6_valid = false;
    provenance.validity ^= kTickAmountValid;

    CHECK(FactPayloadEqual(original, provenance));
    CHECK(FactPayloadFingerprint(original) ==
          FactPayloadFingerprint(provenance));
    CHECK(journal->AdmitBatch(FactConsumer::kEvent,
                              std::array{original})[0U].code ==
          AdmitCode::kNew);
    CHECK(journal->AdmitBatch(FactConsumer::kEvent,
                              std::array{provenance})[0U].code ==
          AdmitCode::kDuplicate);

    CanonicalTick conflict = provenance;
    ++conflict.price.raw;
    CHECK(!FactPayloadEqual(original, conflict));
    CHECK(FactPayloadFingerprint(original) !=
          FactPayloadFingerprint(conflict));
    CHECK(journal->AdmitBatch(FactConsumer::kEvent,
                              std::array{conflict})[0U].code ==
          AdmitCode::kConflict);
    CHECK(journal->AdmitBatch(FactConsumer::kKLine,
                              std::array{conflict})[0U].code ==
          AdmitCode::kConflict);
    journal->ClearHotCache();
    const FactJournalStats before_cold_winner = journal->stats();
    std::array<CanonicalTick, 1U> authoritative{};
    CHECK(journal->AdmitBatch(
              FactConsumer::kKLine, std::array{provenance},
              authoritative)[0U].code == AdmitCode::kNew);
    CHECK(FullTickEqual(authoritative[0U], original));
    CHECK(journal->stats().read_calls ==
          before_cold_winner.read_calls + 1U);
    CHECK(journal->healthy());
    CHECK(journal->stats().records == 1U);
    CHECK(journal->stats().conflicts == 2U);
}

void TestAuthoritativeWinnerSizeMismatchFailsClosed() {
    TemporaryFile file;
    auto journal = Create(file);
    const std::array inputs{Tick(251U), Tick(252U)};
    std::array<CanonicalTick, 1U> winners{};

    const auto results = journal->AdmitBatch(
        FactConsumer::kEvent, inputs, winners);
    CHECK(results.size() == inputs.size());
    CHECK(std::all_of(results.begin(), results.end(),
                      [](const AdmitResult& result) {
                          return result.code == AdmitCode::kFailed;
                      }));
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("winner output size mismatch") !=
          std::string::npos);
    CHECK(journal->stats().records == 0U);
}

void TestAuthoritativeWinnerOverlapFailsClosed() {
    TemporaryFile file;
    auto journal = Create(file);
    std::array storage{Tick(261U), Tick(262U), CanonicalTick{}};
    const std::span<const CanonicalTick> inputs{storage.data(), 2U};
    const std::span<CanonicalTick> overlapping_output{
        storage.data() + 1U, 2U};

    const auto results = journal->AdmitBatch(
        FactConsumer::kEvent, inputs, overlapping_output);
    CHECK(std::all_of(results.begin(), results.end(),
                      [](const AdmitResult& result) {
                          return result.code == AdmitCode::kFailed;
                      }));
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("winner output overlaps input") !=
          std::string::npos);
    CHECK(journal->stats().records == 0U);
}

void TestSameBatchFirstWins() {
    TemporaryFile file;
    auto journal = Create(file);
    const CanonicalTick first = Tick(301U);
    CanonicalTick conflict = first;
    ++conflict.buy_order_id;
    const std::array inputs{first, first, conflict};
    std::array<CanonicalTick, 3U> event_winners{};
    const auto results = journal->AdmitBatch(
        FactConsumer::kEvent, inputs, event_winners);
    CHECK(results.size() == inputs.size());
    CHECK(results[0U].code == AdmitCode::kNew);
    CHECK(results[1U].code == AdmitCode::kDuplicate);
    CHECK(results[2U].code == AdmitCode::kConflict);
    CHECK(results[0U].handle == results[1U].handle);
    CHECK(results[0U].handle == results[2U].handle);
    CHECK(FullTickEqual(event_winners[0U], first));
    CHECK(FullTickEqual(event_winners[1U], first));
    CHECK(journal->stats().records == 1U);

    std::array<CanonicalTick, 3U> kline_winners{};
    const auto kline = journal->AdmitBatch(
        FactConsumer::kKLine, inputs, kline_winners);
    CHECK(kline[0U].code == AdmitCode::kNew);
    CHECK(kline[1U].code == AdmitCode::kDuplicate);
    CHECK(kline[2U].code == AdmitCode::kConflict);
    CHECK(FullTickEqual(kline_winners[0U], first));
    CHECK(FullTickEqual(kline_winners[1U], first));

    CanonicalTick decoded{};
    CHECK(journal->Read(results[0U].handle, &decoded));
    CHECK(FullTickEqual(decoded, first));
}

void TestShenzhenChannelZeroIsAValidGlobalKey() {
    TemporaryFile file;
    auto journal = Create(file);
    const CanonicalTick tick = Tick(351U, 0U, Market::kShenzhen);
    const AdmitResult result = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{tick})[0U];
    CHECK(result.code == AdmitCode::kNew);
    CanonicalTick decoded{};
    CHECK(journal->Read(result.handle, &decoded));
    CHECK(FullTickEqual(decoded, tick));

    const CanonicalTick invalid_shanghai =
        Tick(352U, 0U, Market::kShanghai);
    CHECK(journal->AdmitBatch(FactConsumer::kEvent,
                              std::array{invalid_shanghai})[0U].code ==
          AdmitCode::kFailed);
    CHECK(!journal->healthy());
}

void TestCapacityFailsClosedAtBoundary() {
    TemporaryFile file;
    auto journal = Create(file, 4U, 2U);
    const std::array initial{Tick(401U), Tick(402U)};
    const auto accepted = journal->AdmitBatch(FactConsumer::kEvent, initial);
    CHECK(accepted[0U].code == AdmitCode::kNew);
    CHECK(accepted[1U].code == AdmitCode::kNew);
    CHECK(journal->stats().records == 2U);

    const auto overflow = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{Tick(403U)});
    CHECK(overflow[0U].code == AdmitCode::kFailed);
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("maximum_records") !=
          std::string::npos);
    CHECK(journal->stats().records == 2U);
    CHECK(journal->stats().errors == 1U);
    CHECK(journal->AdmitBatch(FactConsumer::kKLine, initial)[0U].code ==
          AdmitCode::kFailed);
    CHECK(!journal->Flush());
}

void TestDirectoryPageCapacityFailsClosed() {
    TemporaryFile file;
    auto journal = Create(file, 4U, 10U, 1U);
    const AdmitResult first = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{Tick(1U)})[0U];
    CHECK(first.code == AdmitCode::kNew);
    CHECK(journal->stats().directory_pages == 1U);

    const AdmitResult sparse = journal->AdmitBatch(
        FactConsumer::kEvent,
        std::array{Tick(kFactJournalPageEntries)})[0U];
    CHECK(sparse.code == AdmitCode::kFailed);
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("maximum_directory_pages") !=
          std::string::npos);
    CHECK(journal->stats().records == 1U);
    CHECK(journal->stats().directory_pages == 1U);
}

void TestUndecodablePayloadRejectedBeforeAppend() {
    {
        TemporaryFile file;
        auto journal = Create(file);
        CanonicalTick tick = Tick(450U);
        tick.action = static_cast<TickAction>(255U);
        CHECK(journal->AdmitBatch(
                  FactConsumer::kEvent, std::array{tick})[0U].code ==
              AdmitCode::kFailed);
        CHECK(!journal->healthy());
        CHECK(journal->stats().records == 0U);
        CHECK(std::filesystem::file_size(file.path()) ==
              kFactJournalFileHeaderBytes);
    }
    {
        TemporaryFile file;
        auto journal = Create(file);
        CanonicalTick tick = Tick(451U);
        tick.common.identity.security_id_size =
            static_cast<std::uint8_t>(kMaximumIdentityBytes + 1U);
        CHECK(journal->AdmitBatch(
                  FactConsumer::kEvent, std::array{tick})[0U].code ==
              AdmitCode::kFailed);
        CHECK(!journal->healthy());
        CHECK(journal->stats().records == 0U);
    }
}

void TestColdReadAfterEviction() {
    TemporaryFile file;
    auto journal = Create(file, 1U);
    const CanonicalTick first = Tick(501U);
    const CanonicalTick second = Tick(502U);
    const auto admitted = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{first, second});
    CHECK(admitted[0U].code == AdmitCode::kNew);
    CHECK(admitted[1U].code == AdmitCode::kNew);
    const FactJournalStats before = journal->stats();

    CanonicalTick decoded{};
    CHECK(journal->Read(admitted[0U].handle, &decoded));
    CHECK(FullTickEqual(decoded, first));
    const FactJournalStats cold = journal->stats();
    CHECK(cold.read_calls > before.read_calls);
    CHECK(cold.read_bytes - before.read_bytes == kFactJournalRecordBytes);
    CHECK(cold.hot_cache_hits == before.hot_cache_hits);

    CHECK(journal->Read(admitted[0U].handle, &decoded));
    const FactJournalStats hot = journal->stats();
    CHECK(hot.read_calls == cold.read_calls);
    CHECK(hot.hot_cache_hits == cold.hot_cache_hits + 1U);

    journal->ClearHotCache();
    CHECK(journal->Read(admitted[1U].handle, &decoded));
    CHECK(journal->stats().read_calls > hot.read_calls);
}

void TestCorruptionFailsClosed() {
    TemporaryFile file;
    auto journal = Create(file, 0U);
    const CanonicalTick tick = Tick(601U);
    const AdmitResult admitted = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{tick})[0U];
    CHECK(admitted.code == AdmitCode::kNew);
    CHECK(journal->Flush());

    const int fd = ::open(file.path().c_str(), O_RDWR | O_CLOEXEC);
    CHECK(fd >= 0);
    const off_t position = static_cast<off_t>(
        admitted.handle.offset + kFactJournalRecordHeaderBytes + 10U);
    std::byte value{};
    CHECK(::pread(fd, &value, 1U, position) == 1);
    value ^= std::byte{0x80U};
    CHECK(::pwrite(fd, &value, 1U, position) == 1);
    CHECK(::fdatasync(fd) == 0);
    CHECK(::close(fd) == 0);

    CanonicalTick decoded{};
    CHECK(!journal->Read(admitted.handle, &decoded));
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("CRC32C") != std::string::npos);
    CHECK(journal->stats().errors == 1U);
    CHECK(journal->AdmitBatch(FactConsumer::kKLine,
                              std::array{tick})[0U].code ==
          AdmitCode::kFailed);
}

void TestTruncatedReadFailsClosed() {
    TemporaryFile file;
    auto journal = Create(file, 0U);
    const CanonicalTick tick = Tick(701U);
    const AdmitResult admitted = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{tick})[0U];
    CHECK(admitted.code == AdmitCode::kNew);
    CHECK(journal->Flush());
    CHECK(::truncate(file.path().c_str(),
                     static_cast<off_t>(admitted.handle.offset + 100U)) == 0);

    CanonicalTick decoded{};
    CHECK(!journal->Read(admitted.handle, &decoded));
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("incomplete record") !=
          std::string::npos);
}

void TestConcurrentOwnerBatches() {
    TemporaryFile file;
    constexpr std::size_t kThreads = 8U;
    constexpr std::size_t kFactsPerThread = 64U;
    auto journal = Create(file, 128U, kThreads * kFactsPerThread);
    std::atomic<bool> valid{true};

    auto run = [&](FactConsumer consumer) {
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (std::size_t thread_index = 0U; thread_index < kThreads;
             ++thread_index) {
            threads.emplace_back([&, thread_index] {
                std::vector<CanonicalTick> ticks;
                ticks.reserve(kFactsPerThread);
                for (std::size_t index = 0U; index < kFactsPerThread;
                     ++index) {
                    ticks.push_back(Tick(
                        static_cast<std::uint64_t>(index + 1U),
                        static_cast<std::uint32_t>(thread_index + 1U)));
                }
                const auto results = journal->AdmitBatch(consumer, ticks);
                if (results.size() != ticks.size() ||
                    !std::all_of(results.begin(), results.end(),
                                 [](const AdmitResult& result) {
                                     return result.code == AdmitCode::kNew;
                                 })) {
                    valid.store(false, std::memory_order_relaxed);
                }
            });
        }
        for (std::thread& thread : threads) {
            thread.join();
        }
    };

    run(FactConsumer::kEvent);
    run(FactConsumer::kKLine);
    CHECK(valid.load(std::memory_order_relaxed));
    CHECK(journal->healthy());
    const FactJournalStats stats = journal->stats();
    CHECK(stats.records == kThreads * kFactsPerThread);
    CHECK(stats.consumer_new == 2U * kThreads * kFactsPerThread);
}

void TestConcurrentSameKeyWaitsForSingleWinner() {
    TemporaryFile file;
    constexpr std::size_t kThreads = 8U;
    constexpr std::size_t kUniqueFactsPerThread = 512U;
    constexpr std::uint64_t kExpectedRecords =
        1U + kThreads * kUniqueFactsPerThread;
    auto journal = Create(file, 128U, kExpectedRecords);
    const CanonicalTick shared = Tick(9'001U, 90U);
    std::atomic<std::size_t> ready{0U};
    std::atomic<bool> start{false};
    std::atomic<bool> valid{true};
    std::array<AdmitCode, kThreads> shared_codes{};
    std::array<FactHandle, kThreads> shared_handles{};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (std::size_t thread_index = 0U; thread_index < kThreads;
         ++thread_index) {
        threads.emplace_back([&, thread_index] {
            std::vector<CanonicalTick> ticks;
            ticks.reserve(kUniqueFactsPerThread + 1U);
            ticks.push_back(shared);
            for (std::size_t index = 0U; index < kUniqueFactsPerThread;
                 ++index) {
                ticks.push_back(Tick(
                    static_cast<std::uint64_t>(index + 1U),
                    static_cast<std::uint32_t>(100U + thread_index)));
            }
            ready.fetch_add(1U, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto results = journal->AdmitBatch(
                FactConsumer::kEvent, ticks);
            if (results.size() != ticks.size()) {
                valid.store(false, std::memory_order_relaxed);
                return;
            }
            shared_codes[thread_index] = results[0U].code;
            shared_handles[thread_index] = results[0U].handle;
            if (!std::all_of(
                    results.begin() + 1, results.end(),
                    [](const AdmitResult& result) {
                        return result.code == AdmitCode::kNew;
                    })) {
                valid.store(false, std::memory_order_relaxed);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) {
        thread.join();
    }

    CHECK(valid.load(std::memory_order_relaxed));
    CHECK(std::count(shared_codes.begin(), shared_codes.end(),
                     AdmitCode::kNew) == 1);
    CHECK(std::count(shared_codes.begin(), shared_codes.end(),
                     AdmitCode::kDuplicate) ==
          static_cast<std::ptrdiff_t>(kThreads - 1U));
    CHECK(std::all_of(
        shared_handles.begin(), shared_handles.end(),
        [&](FactHandle handle) { return handle == shared_handles[0U]; }));
    CHECK(journal->healthy());
    CHECK(journal->stats().records == kExpectedRecords);
    CHECK(journal->stats().partial_writes == 0U);
    CanonicalTick decoded{};
    CHECK(journal->Read(shared_handles[0U], &decoded));
    CHECK(FullTickEqual(decoded, shared));
}

#if defined(L2FLOW_TEST_WRAP_PWRITE)
void ArmBlockedPwrite(bool fail) {
    g_pwrite_entered.store(false, std::memory_order_relaxed);
    g_release_pwrite.store(false, std::memory_order_relaxed);
    g_fail_pwrite.store(fail, std::memory_order_relaxed);
    g_intercept_pwrite.store(true, std::memory_order_release);
}

void ReleaseBlockedPwrite() {
    g_release_pwrite.store(true, std::memory_order_release);
}

void DisarmPwrite() {
    g_intercept_pwrite.store(false, std::memory_order_release);
    g_fail_pwrite.store(false, std::memory_order_relaxed);
    g_release_pwrite.store(true, std::memory_order_release);
}

void ArmBlockedPread() {
    g_pread_entered.store(false, std::memory_order_relaxed);
    g_release_pread.store(false, std::memory_order_relaxed);
    g_intercept_pread.store(true, std::memory_order_release);
}

void ReleaseBlockedPread() {
    g_release_pread.store(true, std::memory_order_release);
}

void DisarmPread() {
    g_intercept_pread.store(false, std::memory_order_release);
    g_release_pread.store(true, std::memory_order_release);
}

void TestFlushWaitsForActivePwrite() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 10U);
    ArmBlockedPwrite(false);
    auto writer = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{Tick(19'001U, 190U)});
    });
    CHECK(WaitFor([&] {
        return g_pwrite_entered.load(std::memory_order_acquire) &&
               journal->stats().active_writes == 1U;
    }));
    auto flush = std::async(std::launch::async, [&] {
        return journal->Flush();
    });
    CHECK(flush.wait_for(std::chrono::milliseconds(50)) ==
          std::future_status::timeout);
    ReleaseBlockedPwrite();
    CHECK(writer.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(writer.get()[0U].code == AdmitCode::kNew);
    CHECK(flush.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(flush.get());
    DisarmPwrite();
    CHECK(journal->stats().active_writes == 0U);
    CHECK(journal->healthy());
}

void TestColdReadDoesNotBlockUnrelatedWriter() {
    TemporaryFile file;
    auto journal = Create(file, 0U, 10U);
    const CanonicalTick existing = Tick(19'101U, 191U);
    const AdmitResult handle = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{existing})[0U];
    CHECK(handle.code == AdmitCode::kNew);

    ArmBlockedPread();
    auto reader = std::async(std::launch::async, [&] {
        CanonicalTick output{};
        return journal->Read(handle.handle, &output) &&
               FullTickEqual(output, existing);
    });
    CHECK(WaitFor([&] {
        return g_pread_entered.load(std::memory_order_acquire) &&
               journal->stats().active_reads == 1U;
    }));
    auto writer = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{Tick(19'102U, 192U)});
    });
    CHECK(writer.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(writer.get()[0U].code == AdmitCode::kNew);
    CHECK(reader.wait_for(std::chrono::milliseconds(50)) ==
          std::future_status::timeout);
    ReleaseBlockedPread();
    CHECK(reader.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(reader.get());
    DisarmPread();
    CHECK(journal->stats().active_reads == 0U);
    CHECK(journal->healthy());
}

void TestColdDuplicateDoesNotBlockUnrelatedWriter() {
    TemporaryFile file;
    auto journal = Create(file, 0U, 10U);
    const CanonicalTick existing = Tick(19'201U, 193U);
    CHECK(journal->AdmitBatch(
              FactConsumer::kEvent, std::array{existing})[0U].code ==
          AdmitCode::kNew);

    ArmBlockedPread();
    auto duplicate = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kKLine, std::array{existing});
    });
    CHECK(WaitFor([&] {
        return g_pread_entered.load(std::memory_order_acquire) &&
               journal->stats().active_reads == 1U;
    }));
    auto writer = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{Tick(19'202U, 194U)});
    });
    CHECK(writer.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(writer.get()[0U].code == AdmitCode::kNew);
    ReleaseBlockedPread();
    CHECK(duplicate.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(duplicate.get()[0U].code == AdmitCode::kNew);
    DisarmPread();
    CHECK(journal->stats().active_reads == 0U);
    CHECK(journal->healthy());
}

void TestConcurrentCapacityFailureWakesWriterWaiter() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 2U);
    const CanonicalTick shared = Tick(20'001U, 200U);
    const std::array writer_ticks{shared, Tick(20'002U, 201U)};
    ArmBlockedPwrite(false);
    auto writer = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(FactConsumer::kEvent, writer_ticks);
    });
    CHECK(WaitFor([&] {
        return g_pwrite_entered.load(std::memory_order_acquire) &&
               journal->stats().active_writes == 1U;
    }));

    auto waiter = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{shared});
    });
    CHECK(WaitFor([&] {
        return journal->stats().waiting_admissions == 1U;
    }));
    const auto overflow = journal->AdmitBatch(
        FactConsumer::kEvent, std::array{Tick(20'003U, 202U)});
    CHECK(overflow[0U].code == AdmitCode::kFailed);
    CHECK(!journal->healthy());
    CHECK(waiter.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(waiter.get()[0U].code == AdmitCode::kFailed);

    ReleaseBlockedPwrite();
    CHECK(writer.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    const auto writer_results = writer.get();
    CHECK(std::all_of(
        writer_results.begin(), writer_results.end(),
        [](const AdmitResult& result) {
            return result.code == AdmitCode::kFailed;
        }));
    DisarmPwrite();
    CHECK(journal->stats().active_writes == 0U);
    CHECK(journal->stats().waiting_admissions == 0U);
    CHECK(journal->stats().records == 0U);
}

void TestConcurrentWriteFailureWakesWriterWaiter() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 10U);
    const CanonicalTick shared = Tick(21'001U, 210U);
    const std::array writer_ticks{shared, Tick(21'002U, 211U)};
    ArmBlockedPwrite(true);
    auto writer = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(FactConsumer::kEvent, writer_ticks);
    });
    CHECK(WaitFor([&] {
        return g_pwrite_entered.load(std::memory_order_acquire) &&
               journal->stats().active_writes == 1U;
    }));

    auto waiter = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{shared});
    });
    CHECK(WaitFor([&] {
        return journal->stats().waiting_admissions == 1U;
    }));
    ReleaseBlockedPwrite();
    CHECK(writer.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(waiter.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    const auto writer_results = writer.get();
    CHECK(std::all_of(
        writer_results.begin(), writer_results.end(),
        [](const AdmitResult& result) {
            return result.code == AdmitCode::kFailed;
        }));
    CHECK(waiter.get()[0U].code == AdmitCode::kFailed);
    DisarmPwrite();
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("pwrite failed") !=
          std::string::npos);
    CHECK(journal->stats().active_writes == 0U);
    CHECK(journal->stats().waiting_admissions == 0U);
    CHECK(journal->stats().errors == 1U);
}

void TestMixedBatchPublishesExistingConsumerAfterPwrite() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 10U);
    const CanonicalTick existing = Tick(21'101U, 212U);
    const CanonicalTick appended = Tick(21'102U, 213U);
    CHECK(journal->AdmitBatch(
              FactConsumer::kKLine, std::array{existing})[0U].code ==
          AdmitCode::kNew);

    ArmBlockedPwrite(false);
    const std::array mixed_ticks{existing, appended};
    std::array<CanonicalTick, 2U> mixed_winners{};
    auto mixed = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, mixed_ticks, mixed_winners);
    });
    CHECK(WaitFor([&] {
        return g_pwrite_entered.load(std::memory_order_acquire) &&
               journal->stats().active_writes == 1U;
    }));

    auto contender = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{existing});
    });
    CHECK(WaitFor([&] {
        return journal->stats().waiting_admissions == 1U;
    }));
    CHECK(contender.wait_for(std::chrono::milliseconds(50)) ==
          std::future_status::timeout);

    ReleaseBlockedPwrite();
    CHECK(mixed.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    const auto mixed_results = mixed.get();
    CHECK(mixed_results.size() == 2U);
    CHECK(mixed_results[0U].code == AdmitCode::kNew);
    CHECK(mixed_results[1U].code == AdmitCode::kNew);
    CHECK(FullTickEqual(mixed_winners[0U], existing));
    CHECK(FullTickEqual(mixed_winners[1U], appended));
    CHECK(contender.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(contender.get()[0U].code == AdmitCode::kDuplicate);
    DisarmPwrite();

    const FactJournalStats stats = journal->stats();
    CHECK(stats.records == 2U);
    CHECK(stats.consumer_new == 3U);
    CHECK(stats.duplicates == 1U);
    CHECK(stats.active_writes == 0U);
    CHECK(stats.waiting_admissions == 0U);
    CHECK(journal->healthy());
}

void TestMixedBatchWriteFailureDoesNotPublishDuplicate() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 10U);
    const CanonicalTick existing = Tick(21'201U, 214U);
    const CanonicalTick appended = Tick(21'202U, 215U);
    CHECK(journal->AdmitBatch(
              FactConsumer::kKLine, std::array{existing})[0U].code ==
          AdmitCode::kNew);

    ArmBlockedPwrite(true);
    const std::array mixed_ticks{existing, appended};
    std::array<CanonicalTick, 2U> mixed_winners{};
    auto mixed = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, mixed_ticks, mixed_winners);
    });
    CHECK(WaitFor([&] {
        return g_pwrite_entered.load(std::memory_order_acquire) &&
               journal->stats().active_writes == 1U;
    }));
    auto contender = std::async(std::launch::async, [&] {
        return journal->AdmitBatch(
            FactConsumer::kEvent, std::array{existing});
    });
    CHECK(WaitFor([&] {
        return journal->stats().waiting_admissions == 1U;
    }));

    ReleaseBlockedPwrite();
    CHECK(mixed.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    CHECK(contender.wait_for(std::chrono::seconds(1)) ==
          std::future_status::ready);
    const auto mixed_results = mixed.get();
    CHECK(std::all_of(
        mixed_results.begin(), mixed_results.end(),
        [](const AdmitResult& result) {
            return result.code == AdmitCode::kFailed;
        }));
    CHECK(contender.get()[0U].code == AdmitCode::kFailed);
    DisarmPwrite();

    const FactJournalStats stats = journal->stats();
    CHECK(stats.records == 1U);
    CHECK(stats.active_writes == 0U);
    CHECK(stats.reserved_records == 0U);
    CHECK(stats.waiting_admissions == 0U);
    CHECK(stats.errors == 1U);
    CHECK(!journal->healthy());
}

void TestFdatasyncFailureIsSticky() {
    TemporaryFile file;
    auto journal = Create(file, 8U, 10U);
    CHECK(journal->AdmitBatch(
              FactConsumer::kEvent,
              std::array{Tick(21'301U, 216U)})[0U].code ==
          AdmitCode::kNew);

    g_fail_fdatasync.store(true, std::memory_order_release);
    CHECK(!journal->Flush());
    g_fail_fdatasync.store(false, std::memory_order_release);
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("fdatasync failed") !=
          std::string::npos);
    CHECK(journal->stats().errors == 1U);
    CHECK(journal->AdmitBatch(
              FactConsumer::kEvent,
              std::array{Tick(21'302U, 217U)})[0U].code ==
          AdmitCode::kFailed);
    CHECK(!journal->Flush());
    CHECK(journal->stats().errors == 1U);
}
#endif

void TestCreationValidation() {
    TemporaryFile file;
    std::string error;
    CHECK(CanonicalFactJournal::Create(
              FactJournalConfig{0U, file.path(), 1U, 1U}, &error) == nullptr);
    CHECK(error.find("trade_date") != std::string::npos);
    CHECK(CanonicalFactJournal::Create(
              FactJournalConfig{20260809U, file.path(), 1U, 0U}, &error) ==
          nullptr);
    CHECK(error.find("maximum_records") != std::string::npos);
    CHECK(CanonicalFactJournal::Create(
              FactJournalConfig{20260809U, file.path(), 1U, 1U, 0U},
              &error) == nullptr);
    CHECK(error.find("maximum_directory_pages") != std::string::npos);
}

void TestEmptyJournalRejectsHandleWithoutReading() {
    TemporaryFile file;
    auto journal = Create(file);
    CanonicalTick output{};
    CHECK(!journal->Read(FactHandle{kFactJournalFileHeaderBytes}, &output));
    CHECK(!journal->healthy());
    CHECK(journal->fatal_error().find("invalid FactHandle") !=
          std::string::npos);
    CHECK(journal->stats().read_calls == 0U);
}

void TestSecondCreateCannotTruncateLiveJournal() {
    TemporaryFile file;
    auto first = Create(file);
    const CanonicalTick tick = Tick(30'001U, 300U);
    const AdmitResult admitted = first->AdmitBatch(
        FactConsumer::kEvent, std::array{tick})[0U];
    CHECK(admitted.code == AdmitCode::kNew);
    const std::uintmax_t bytes_before =
        std::filesystem::file_size(file.path());

    std::string error;
    auto second = CanonicalFactJournal::Create(
        FactJournalConfig{20260809U, file.path(), 8U, 100U}, &error);
    CHECK(second == nullptr);
    CHECK(error.find("already locked") != std::string::npos);
    CHECK(std::filesystem::file_size(file.path()) == bytes_before);
    CanonicalTick decoded{};
    CHECK(first->Read(admitted.handle, &decoded));
    CHECK(FullTickEqual(decoded, tick));
    CHECK(first->healthy());

    first.reset();
    auto replacement = CanonicalFactJournal::Create(
        FactJournalConfig{20260809U, file.path(), 8U, 100U}, &error);
    CHECK(replacement != nullptr);
    CHECK(replacement->stats().records == 0U);
    CHECK(std::filesystem::file_size(file.path()) ==
          kFactJournalFileHeaderBytes);
}

}  // namespace

int main() {
    TestDualConsumerAndStableCodec();
    TestBusinessEqualityAndConflict();
    TestAuthoritativeWinnerSizeMismatchFailsClosed();
    TestAuthoritativeWinnerOverlapFailsClosed();
    TestSameBatchFirstWins();
    TestShenzhenChannelZeroIsAValidGlobalKey();
    TestCapacityFailsClosedAtBoundary();
    TestDirectoryPageCapacityFailsClosed();
    TestUndecodablePayloadRejectedBeforeAppend();
    TestColdReadAfterEviction();
    TestCorruptionFailsClosed();
    TestTruncatedReadFailsClosed();
    TestConcurrentOwnerBatches();
    TestConcurrentSameKeyWaitsForSingleWinner();
#if defined(L2FLOW_TEST_WRAP_PWRITE)
    TestFlushWaitsForActivePwrite();
    TestColdReadDoesNotBlockUnrelatedWriter();
    TestColdDuplicateDoesNotBlockUnrelatedWriter();
    TestConcurrentCapacityFailureWakesWriterWaiter();
    TestConcurrentWriteFailureWakesWriterWaiter();
    TestMixedBatchPublishesExistingConsumerAfterPwrite();
    TestMixedBatchWriteFailureDoesNotPublishDuplicate();
    TestFdatasyncFailureIsSticky();
#endif
    TestCreationValidation();
    TestEmptyJournalRejectsHandleWithoutReading();
    TestSecondCreateCannotTruncateLiveJournal();
    std::cout << "fact journal tests passed\n";
    return 0;
}
