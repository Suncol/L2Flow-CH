#include "l2flow/journal/fact_journal.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace {

using l2flow::ingest::Aggressor;
using l2flow::ingest::CanonicalKind;
using l2flow::ingest::CanonicalTick;
using l2flow::ingest::Market;
using l2flow::ingest::MessageKey;
using l2flow::ingest::OrderType;
using l2flow::ingest::Side;
using l2flow::ingest::TickAction;
using l2flow::ingest::TradingPhase;
using l2flow::journal::AdmitCode;
using l2flow::journal::AdmitResult;
using l2flow::journal::CanonicalFactJournal;
using l2flow::journal::FactConsumer;
using l2flow::journal::FactHandle;
using l2flow::journal::FactJournalConfig;
using l2flow::journal::FactJournalStats;

constexpr std::uint32_t kTradeDate = 20260807U;
constexpr std::uint64_t kNanosecondsPerSecond = UINT64_C(1'000'000'000);
constexpr std::uint64_t kMixSlots = 10'000U;
constexpr std::uint64_t kShanghaiTickSlots = 3'954U;
constexpr std::uint64_t kShenzhenOrderSlots = 3'180U;
constexpr long double kDurationTolerance = 0.01L;

struct Options final {
    std::uint64_t target_rate = 800'000U;
    std::uint32_t seconds = 5U;
    std::size_t batch_rows = 256U;
    std::size_t owners = 32U;
    std::size_t hot_cache_entries = 65'536U;
    std::size_t probe_samples = 4'096U;
    std::uint64_t mixed_read_rate = 100U;
    std::size_t seed_records = 64U;
    std::filesystem::path journal_directory;
    bool help = false;
};

enum class WorkloadKind : std::uint8_t {
    kShanghaiTick,
    kShenzhenOrder,
    kShenzhenTrade,
};

struct GeneratorState final {
    std::uint64_t shanghai_sequence = 0U;
    std::uint64_t shenzhen_sequence = 0U;
};

struct ProbeRecord final {
    FactHandle handle{};
    CanonicalTick tick{};
};

struct WorkerResult final {
    std::uint64_t generated = 0U;
    std::uint64_t admitted = 0U;
    std::uint64_t event_new = 0U;
    std::uint64_t kline_new = 0U;
    std::uint64_t unexpected_duplicates = 0U;
    std::uint64_t conflicts = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t shanghai_ticks = 0U;
    std::uint64_t shenzhen_orders = 0U;
    std::uint64_t shenzhen_trades = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    std::uint64_t finish_ns = 0U;
    std::vector<std::uint64_t> event_batch_ns;
    std::vector<std::uint64_t> kline_batch_ns;
    std::vector<std::uint64_t> combined_batch_ns;
    std::vector<std::uint64_t> combined_per_fact_ns;
    std::vector<ProbeRecord> oldest_records;
    CanonicalTick last_tick{};
    bool has_last_tick = false;
    std::string error;
};

struct ProcessMemory final {
    std::uint64_t current_rss_kib = 0U;
    std::uint64_t peak_rss_kib = 0U;
    bool available = false;
};

struct MixedReadResult final {
    std::uint64_t completed = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    std::uint64_t finish_ns = 0U;
    std::vector<std::uint64_t> service_time_ns;
    std::string error;
};

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    Integer parsed{};
    const auto result = std::from_chars(
        text.data(), text.data() + text.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != text.data() + text.size()) {
        return false;
    }
    *output = parsed;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage: benchmark_fact_journal [options]\n"
        << "  --rate N          messages/s; 0 is unpaced (default 800000)\n"
        << "  --seconds N       measurement duration (default 5)\n"
        << "  --batch-rows N    rows per owner batch (default 256)\n"
        << "  --owners N        concurrent owner callers (default 32)\n"
        << "  --journal-dir DIR required; a unique journal file is created\n"
        << "  --hot-cache N     decoded hot-cache entries (default 65536)\n"
        << "  --probe-samples N post-run duplicate/read samples (default 4096)\n"
        << "  --mixed-read-rate N per-handle cache-miss reads/s; 0 disables "
           "(default 100)\n"
        << "  --seed-records N  pre-timing mixed-read records (default 64)\n";
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
    if (output == nullptr || error == nullptr) {
        return false;
    }
    Options parsed{};
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        const auto next = [&](std::string_view name) -> std::string_view {
            if (index + 1 >= argc) {
                *error = std::string(name) + " requires a value";
                return {};
            }
            ++index;
            return argv[index];
        };
        if (argument == "--help") {
            parsed.help = true;
        } else if (argument == "--rate") {
            if (!ParseInteger(next(argument), &parsed.target_rate)) {
                *error = "invalid --rate";
                return false;
            }
        } else if (argument == "--seconds") {
            if (!ParseInteger(next(argument), &parsed.seconds)) {
                *error = "invalid --seconds";
                return false;
            }
        } else if (argument == "--batch-rows") {
            if (!ParseInteger(next(argument), &parsed.batch_rows)) {
                *error = "invalid --batch-rows";
                return false;
            }
        } else if (argument == "--owners") {
            if (!ParseInteger(next(argument), &parsed.owners)) {
                *error = "invalid --owners";
                return false;
            }
        } else if (argument == "--journal-dir") {
            const std::string_view value = next(argument);
            if (value.empty()) {
                *error = "invalid --journal-dir";
                return false;
            }
            parsed.journal_directory = value;
        } else if (argument == "--hot-cache") {
            if (!ParseInteger(next(argument),
                              &parsed.hot_cache_entries)) {
                *error = "invalid --hot-cache";
                return false;
            }
        } else if (argument == "--probe-samples") {
            if (!ParseInteger(next(argument), &parsed.probe_samples)) {
                *error = "invalid --probe-samples";
                return false;
            }
        } else if (argument == "--mixed-read-rate") {
            if (!ParseInteger(next(argument),
                              &parsed.mixed_read_rate)) {
                *error = "invalid --mixed-read-rate";
                return false;
            }
        } else if (argument == "--seed-records") {
            if (!ParseInteger(next(argument), &parsed.seed_records)) {
                *error = "invalid --seed-records";
                return false;
            }
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
        if (!error->empty()) {
            return false;
        }
    }
    if (parsed.help) {
        *output = parsed;
        return true;
    }
    if (parsed.journal_directory.empty() ||
        parsed.target_rate > 10'000'000U || parsed.seconds == 0U ||
        parsed.seconds > 600U || parsed.batch_rows == 0U ||
        parsed.batch_rows > 65'536U || parsed.owners == 0U ||
        parsed.owners > 1'024U || parsed.probe_samples > 1'000'000U ||
        parsed.mixed_read_rate > 10'000U ||
        parsed.seed_records == 0U || parsed.seed_records > 65'536U ||
        parsed.mixed_read_rate >
            std::numeric_limits<std::uint64_t>::max() /
                parsed.seconds ||
        (parsed.target_rate != 0U &&
         parsed.target_rate >
             std::numeric_limits<std::uint64_t>::max() /
                 parsed.seconds)) {
        *error = "invalid rate, duration, batch, owner, seed, or probe bound";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

[[nodiscard]] std::uint64_t NowNs() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] std::uint64_t ScheduledOffsetNs(
    std::uint64_t ordinal,
    std::uint64_t rate) noexcept {
    const std::uint64_t seconds = ordinal / rate;
    const std::uint64_t remainder = ordinal % rate;
    return seconds * kNanosecondsPerSecond +
        remainder * kNanosecondsPerSecond / rate;
}

[[nodiscard]] std::uint64_t WaitUntil(std::uint64_t deadline_ns) noexcept {
    for (;;) {
        const std::uint64_t now = NowNs();
        if (now >= deadline_ns) {
            return now;
        }
        const std::uint64_t remaining = deadline_ns - now;
        if (remaining > 200'000U) {
            std::this_thread::sleep_for(
                std::chrono::nanoseconds(remaining - 100'000U));
        } else {
            std::this_thread::yield();
        }
    }
}

[[nodiscard]] std::uint64_t Percentile(
    const std::vector<std::uint64_t>& sorted,
    long double quantile) noexcept {
    if (sorted.empty()) {
        return 0U;
    }
    const long double raw = quantile *
        static_cast<long double>(sorted.size() - 1U);
    return sorted[static_cast<std::size_t>(raw)];
}

[[nodiscard]] WorkloadKind SelectKind(std::uint64_t ordinal) noexcept {
    // 3954/3180/2866 approximates the observed 2026-08-07
    // Shanghai Tick / Shenzhen Order / Shenzhen Trade split. Multiplication
    // by a number coprime to 10,000 distributes every complete mix cycle.
    const std::uint64_t slot =
        ((ordinal % kMixSlots) * UINT64_C(7'919)) % kMixSlots;
    if (slot < kShanghaiTickSlots) {
        return WorkloadKind::kShanghaiTick;
    }
    if (slot < kShanghaiTickSlots + kShenzhenOrderSlots) {
        return WorkloadKind::kShenzhenOrder;
    }
    return WorkloadKind::kShenzhenTrade;
}

void SetSixDigitIdentity(l2flow::ingest::ExactIdentity* identity,
                         Market market,
                         std::uint32_t number) noexcept {
    if (identity == nullptr) {
        return;
    }
    identity->market = market;
    identity->security_id_size = 6U;
    for (std::size_t reverse = 0U; reverse < 6U; ++reverse) {
        const std::size_t index = 5U - reverse;
        const auto digit = static_cast<unsigned char>(number % 10U);
        identity->security_id[index] = static_cast<std::byte>(
            static_cast<unsigned char>('0') + digit);
        number /= 10U;
    }
    identity->security_id_source_size = 4U;
    const std::array<char, 4U> source = market == Market::kShanghai
        ? std::array<char, 4U>{'X', 'S', 'H', 'G'}
        : std::array<char, 4U>{'X', 'S', 'H', 'E'};
    for (std::size_t index = 0U; index < source.size(); ++index) {
        identity->security_id_source[index] = static_cast<std::byte>(
            static_cast<unsigned char>(source[index]));
    }
}

[[nodiscard]] CanonicalTick MakeTemplate(std::size_t owner,
                                         WorkloadKind kind) noexcept {
    CanonicalTick tick{};
    const bool shanghai = kind == WorkloadKind::kShanghaiTick;
    const Market market = shanghai ? Market::kShanghai
                                   : Market::kShenzhen;
    tick.common.trade_date = kTradeDate;
    tick.common.instrument_id = static_cast<std::uint32_t>(owner + 1U);
    tick.common.instrument_ordinal = static_cast<std::uint32_t>(owner);
    tick.common.channel = static_cast<std::uint32_t>(owner + 1U);
    tick.common.exchange_time_raw = 93'000'000U;
    tick.common.exchange_time_valid = true;
    tick.common.kind = shanghai
        ? CanonicalKind::kShanghaiTick
        : (kind == WorkloadKind::kShenzhenOrder
               ? CanonicalKind::kShenzhenOrder
               : CanonicalKind::kShenzhenTransaction);
    tick.common.message_key = shanghai
        ? MessageKey{4U, 101U, 24U}
        : (kind == WorkloadKind::kShenzhenOrder
               ? MessageKey{6U, 101U, 33U}
               : MessageKey{6U, 101U, 36U});
    SetSixDigitIdentity(
        &tick.common.identity, market,
        (shanghai ? 600'000U : 1U) +
            static_cast<std::uint32_t>(owner));
    tick.price = {100'000, 10'000'000, 4U, true, true};
    tick.quantity = {100, 0U, true};
    tick.amount = {10'000'000, 10'000'000'000, 4U, true, true};
    tick.phase = TradingPhase::kContinuous;
    tick.validity = l2flow::ingest::kTickPriceValid |
        l2flow::ingest::kTickQuantityValid |
        l2flow::ingest::kTickAmountValid |
        l2flow::ingest::kTickExchangeTimeValid |
        l2flow::ingest::kTickPhaseValid |
        l2flow::ingest::kTickChannelHistoryValid;
    if (kind == WorkloadKind::kShenzhenOrder) {
        tick.action = TickAction::kAdd;
        tick.side = Side::kBuy;
        tick.order_type = OrderType::kLimit;
        tick.validity |= l2flow::ingest::kTickPrimaryOrderIdValid |
            l2flow::ingest::kTickSideValid |
            l2flow::ingest::kTickOrderTypeValid;
    } else {
        tick.action = TickAction::kTrade;
        tick.aggressor = Aggressor::kBuy;
        tick.validity |= l2flow::ingest::kTickBuyOrderIdValid |
            l2flow::ingest::kTickSellOrderIdValid |
            l2flow::ingest::kTickAggressorValid;
    }
    return tick;
}

[[nodiscard]] CanonicalTick MakeTick(
    const std::array<CanonicalTick, 3U>& templates,
    std::uint64_t ordinal,
    GeneratorState* generator,
    std::uint64_t* shanghai_ticks,
    std::uint64_t* shenzhen_orders,
    std::uint64_t* shenzhen_trades) noexcept {
    const WorkloadKind kind = SelectKind(ordinal);
    const std::size_t template_index = static_cast<std::size_t>(kind);
    CanonicalTick tick = templates[template_index];
    std::uint64_t sequence = 0U;
    if (kind == WorkloadKind::kShanghaiTick) {
        sequence = ++generator->shanghai_sequence;
        ++*shanghai_ticks;
    } else {
        sequence = ++generator->shenzhen_sequence;
        if (kind == WorkloadKind::kShenzhenOrder) {
            ++*shenzhen_orders;
        } else {
            ++*shenzhen_trades;
        }
    }
    tick.common.native_sequence = sequence;
    tick.common.ingress_sequence = ordinal + 1U;
    tick.common.vendor_sequence_id = ordinal + 10'001U;
    tick.common.receive_monotonic_ns = ordinal + 1U;
    tick.common.exchange_time_ns_from_midnight =
        UINT64_C(34'200'000'000'000) + ordinal * 1'000U;
    tick.primary_order_id = static_cast<std::int64_t>(sequence);
    tick.buy_order_id = static_cast<std::int64_t>(sequence * 2U);
    tick.sell_order_id = static_cast<std::int64_t>(sequence * 2U + 1U);
    return tick;
}

[[nodiscard]] std::vector<CanonicalTick> MakeSeedTicks(
    const Options& options) {
    const CanonicalTick seed_template = MakeTemplate(
        options.owners, WorkloadKind::kShanghaiTick);
    std::vector<CanonicalTick> ticks;
    ticks.reserve(options.seed_records);
    for (std::size_t index = 0U; index < options.seed_records; ++index) {
        CanonicalTick tick = seed_template;
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        tick.common.native_sequence = sequence;
        tick.common.ingress_sequence = sequence;
        tick.common.vendor_sequence_id = sequence;
        tick.common.receive_monotonic_ns = sequence;
        tick.common.exchange_time_ns_from_midnight =
            UINT64_C(34'199'000'000'000) + sequence;
        tick.primary_order_id = static_cast<std::int64_t>(sequence);
        tick.buy_order_id = static_cast<std::int64_t>(sequence * 2U);
        tick.sell_order_id = static_cast<std::int64_t>(sequence * 2U + 1U);
        ticks.push_back(std::move(tick));
    }
    return ticks;
}

[[nodiscard]] bool AdmitSeedRecords(
    const Options& options,
    CanonicalFactJournal* journal,
    std::vector<ProbeRecord>* output,
    std::string* error) {
    const std::vector<CanonicalTick> ticks = MakeSeedTicks(options);
    const std::vector<AdmitResult> event = journal->AdmitBatch(
        FactConsumer::kEvent, ticks);
    const std::vector<AdmitResult> kline = journal->AdmitBatch(
        FactConsumer::kKLine, ticks);
    if (event.size() != ticks.size() || kline.size() != ticks.size()) {
        *error = "seed admission result count mismatch";
        return false;
    }
    output->clear();
    output->reserve(ticks.size());
    for (std::size_t index = 0U; index < ticks.size(); ++index) {
        if (event[index].code != AdmitCode::kNew ||
            kline[index].code != AdmitCode::kNew ||
            event[index].handle != kline[index].handle) {
            *error = "seed Event/KLine admission invariant failed";
            return false;
        }
        output->push_back(ProbeRecord{event[index].handle, ticks[index]});
    }
    error->clear();
    return true;
}

[[nodiscard]] bool AllNew(std::span<const AdmitResult> results,
                          std::uint64_t* new_count,
                          WorkerResult* worker) noexcept {
    bool valid = true;
    for (const AdmitResult& result : results) {
        switch (result.code) {
            case AdmitCode::kNew:
                ++*new_count;
                break;
            case AdmitCode::kDuplicate:
                ++worker->unexpected_duplicates;
                valid = false;
                break;
            case AdmitCode::kConflict:
                ++worker->conflicts;
                valid = false;
                break;
            case AdmitCode::kFailed:
                ++worker->failures;
                valid = false;
                break;
        }
    }
    return valid;
}

void RunMixedReadProbe(
    std::uint64_t rate,
    std::uint64_t samples,
    CanonicalFactJournal* journal,
    std::span<const ProbeRecord> seeds,
    std::atomic<bool>* ready,
    std::atomic<bool>* start,
    std::atomic<std::uint64_t>* start_ns,
    std::atomic<bool>* abort,
    MixedReadResult* output) noexcept {
    bool ready_announced = false;
    try {
        output->service_time_ns.reserve(
            static_cast<std::size_t>(samples));
        ready->store(true, std::memory_order_release);
        ready_announced = true;
        while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        const std::uint64_t origin =
            start_ns->load(std::memory_order_acquire);
        for (std::uint64_t ordinal = 0U;
             ordinal < samples &&
             !abort->load(std::memory_order_acquire);
             ++ordinal) {
            const std::uint64_t deadline = origin +
                ScheduledOffsetNs(ordinal, rate);
            const std::uint64_t admitted_at = WaitUntil(deadline);
            output->maximum_schedule_lag_ns = std::max(
                output->maximum_schedule_lag_ns,
                admitted_at - deadline);
            const ProbeRecord& seed = seeds[static_cast<std::size_t>(
                ordinal % static_cast<std::uint64_t>(seeds.size()))];
            journal->EvictHotCache(seed.handle);
            CanonicalTick loaded{};
            const std::uint64_t begin = NowNs();
            const bool read = journal->Read(seed.handle, &loaded);
            const std::uint64_t finish = NowNs();
            output->service_time_ns.push_back(finish - begin);
            ++output->completed;
            if (!read ||
                l2flow::journal::MakeFactKey(loaded) !=
                    l2flow::journal::MakeFactKey(seed.tick) ||
                !l2flow::journal::FactPayloadEqual(loaded, seed.tick)) {
                ++output->failures;
                output->error = "mixed-read seed validation failed";
                abort->store(true, std::memory_order_release);
                break;
            }
        }
        output->finish_ns = NowNs();
    } catch (const std::exception& exception) {
        if (!ready_announced) {
            ready->store(true, std::memory_order_release);
        }
        output->error = exception.what();
        ++output->failures;
        output->finish_ns = NowNs();
        abort->store(true, std::memory_order_release);
    } catch (...) {
        if (!ready_announced) {
            ready->store(true, std::memory_order_release);
        }
        output->error = "unknown mixed-read probe exception";
        ++output->failures;
        output->finish_ns = NowNs();
        abort->store(true, std::memory_order_release);
    }
}

void RunOwner(std::size_t owner,
              const Options& options,
              std::uint64_t paced_total,
              std::size_t probe_records_per_owner,
              CanonicalFactJournal* journal,
              std::atomic<std::size_t>* ready,
              std::atomic<bool>* start,
              std::atomic<std::uint64_t>* start_ns,
              std::atomic<bool>* abort,
              WorkerResult* output) noexcept {
    bool ready_announced = false;
    try {
        std::vector<CanonicalTick> ticks;
        ticks.reserve(options.batch_rows);
        const std::array<CanonicalTick, 3U> templates{
            MakeTemplate(owner, WorkloadKind::kShanghaiTick),
            MakeTemplate(owner, WorkloadKind::kShenzhenOrder),
            MakeTemplate(owner, WorkloadKind::kShenzhenTrade)};
        const std::uint64_t owner_stride =
            static_cast<std::uint64_t>(options.owners);
        std::uint64_t local_ordinal = 0U;
        GeneratorState generator{};
        const std::uint64_t rows_for_owner =
            options.target_rate == 0U ||
                    static_cast<std::uint64_t>(owner) >= paced_total
                ? 0U
                : (paced_total - 1U - static_cast<std::uint64_t>(owner)) /
                          owner_stride +
                      1U;
        const std::size_t expected_batches = options.target_rate == 0U
            ? 4'096U
            : static_cast<std::size_t>(
                  (rows_for_owner + options.batch_rows - 1U) /
                  options.batch_rows);
        output->event_batch_ns.reserve(expected_batches);
        output->kline_batch_ns.reserve(expected_batches);
        output->combined_batch_ns.reserve(expected_batches);
        output->combined_per_fact_ns.reserve(expected_batches);
        output->oldest_records.reserve(probe_records_per_owner);

        ready->fetch_add(1U, std::memory_order_release);
        ready_announced = true;
        while (!start->load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        const std::uint64_t origin =
            start_ns->load(std::memory_order_acquire);
        const std::uint64_t unpaced_deadline = origin +
            static_cast<std::uint64_t>(options.seconds) *
                kNanosecondsPerSecond;

        while (!abort->load(std::memory_order_acquire)) {
            if (options.target_rate != 0U &&
                local_ordinal >= rows_for_owner) {
                break;
            }
            if (options.target_rate == 0U && NowNs() >= unpaced_deadline) {
                break;
            }
            const std::uint64_t remaining = options.target_rate == 0U
                ? static_cast<std::uint64_t>(options.batch_rows)
                : rows_for_owner - local_ordinal;
            const std::size_t rows = static_cast<std::size_t>(std::min(
                remaining,
                static_cast<std::uint64_t>(options.batch_rows)));
            ticks.clear();
            for (std::size_t row = 0U; row < rows; ++row) {
                const std::uint64_t ordinal =
                    static_cast<std::uint64_t>(owner) +
                    (local_ordinal + static_cast<std::uint64_t>(row)) *
                        owner_stride;
                ticks.push_back(MakeTick(
                    templates, ordinal, &generator,
                    &output->shanghai_ticks,
                    &output->shenzhen_orders,
                    &output->shenzhen_trades));
            }
            output->generated += static_cast<std::uint64_t>(rows);

            if (options.target_rate != 0U) {
                const std::uint64_t last_ordinal =
                    static_cast<std::uint64_t>(owner) +
                    (local_ordinal + static_cast<std::uint64_t>(rows) - 1U) *
                        owner_stride;
                const std::uint64_t deadline = origin +
                    ScheduledOffsetNs(last_ordinal, options.target_rate);
                const std::uint64_t admitted_at = WaitUntil(deadline);
                output->maximum_schedule_lag_ns = std::max(
                    output->maximum_schedule_lag_ns,
                    admitted_at - deadline);
            }

            const std::uint64_t event_start = NowNs();
            std::vector<AdmitResult> event_results = journal->AdmitBatch(
                FactConsumer::kEvent, ticks);
            const std::uint64_t event_finish = NowNs();
            if (event_results.size() != ticks.size() ||
                !AllNew(event_results, &output->event_new, output)) {
                output->error = "Event admission did not return all kNew";
                abort->store(true, std::memory_order_release);
                break;
            }
            const std::uint64_t kline_start = NowNs();
            std::vector<AdmitResult> kline_results = journal->AdmitBatch(
                FactConsumer::kKLine, ticks);
            const std::uint64_t kline_finish = NowNs();
            if (kline_results.size() != ticks.size() ||
                !AllNew(kline_results, &output->kline_new, output)) {
                output->error = "KLine admission did not return all kNew";
                abort->store(true, std::memory_order_release);
                break;
            }
            bool handles_match = true;
            for (std::size_t index = 0U; index < ticks.size(); ++index) {
                handles_match = handles_match &&
                    event_results[index].handle ==
                        kline_results[index].handle;
            }
            if (!handles_match) {
                output->error =
                    "Event and KLine received different FactHandles";
                output->failures += 1U;
                abort->store(true, std::memory_order_release);
                break;
            }

            const std::size_t wanted = probe_records_per_owner >
                    output->oldest_records.size()
                ? probe_records_per_owner - output->oldest_records.size()
                : 0U;
            const std::size_t to_save = std::min(wanted, ticks.size());
            for (std::size_t index = 0U; index < to_save; ++index) {
                output->oldest_records.push_back(
                    ProbeRecord{event_results[index].handle, ticks[index]});
            }
            output->last_tick = ticks.back();
            output->has_last_tick = true;
            output->admitted += static_cast<std::uint64_t>(rows);
            local_ordinal += static_cast<std::uint64_t>(rows);
            output->event_batch_ns.push_back(event_finish - event_start);
            output->kline_batch_ns.push_back(kline_finish - kline_start);
            const std::uint64_t combined_call_ns =
                (event_finish - event_start) +
                (kline_finish - kline_start);
            output->combined_batch_ns.push_back(combined_call_ns);
            output->combined_per_fact_ns.push_back(
                combined_call_ns /
                static_cast<std::uint64_t>(rows));
        }
        output->finish_ns = NowNs();
    } catch (const std::exception& exception) {
        if (!ready_announced) {
            ready->fetch_add(1U, std::memory_order_release);
        }
        output->error = exception.what();
        output->failures += 1U;
        output->finish_ns = NowNs();
        abort->store(true, std::memory_order_release);
    } catch (...) {
        if (!ready_announced) {
            ready->fetch_add(1U, std::memory_order_release);
        }
        output->error = "unknown worker exception";
        output->failures += 1U;
        output->finish_ns = NowNs();
        abort->store(true, std::memory_order_release);
    }
}

[[nodiscard]] ProcessMemory ReadProcessMemory() {
    ProcessMemory result{};
#if defined(__linux__)
    std::ifstream status("/proc/self/status");
    std::string key;
    while (status >> key) {
        if (key == "VmRSS:" || key == "VmHWM:") {
            std::uint64_t value = 0U;
            std::string unit;
            if (!(status >> value >> unit) || unit != "kB") {
                return {};
            }
            if (key == "VmRSS:") {
                result.current_rss_kib = value;
            } else {
                result.peak_rss_kib = value;
            }
        } else {
            std::string rest;
            std::getline(status, rest);
        }
    }
    result.available = result.current_rss_kib != 0U &&
        result.peak_rss_kib != 0U;
#endif
    return result;
}

[[nodiscard]] std::filesystem::path UniqueJournalPath(
    const std::filesystem::path& directory) {
    std::string process = "portable";
#if defined(__linux__)
    process = std::to_string(static_cast<std::uint64_t>(::getpid()));
#endif
    return directory /
        ("fact-journal-" + process + '-' + std::to_string(NowNs()) +
         ".bin");
}

class JournalFileCleanup final {
public:
    explicit JournalFileCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}

    ~JournalFileCleanup() {
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove(path_, ignored));
    }

    JournalFileCleanup(const JournalFileCleanup&) = delete;
    JournalFileCleanup& operator=(const JournalFileCleanup&) = delete;

private:
    std::filesystem::path path_;
};

void MergeSamples(std::vector<std::uint64_t>* destination,
                  std::vector<std::uint64_t>* source) {
    destination->insert(destination->end(),
                        std::make_move_iterator(source->begin()),
                        std::make_move_iterator(source->end()));
}

void PrintLatency(std::string_view name,
                  const std::vector<std::uint64_t>& sorted,
                  double divisor,
                  std::string_view unit) {
    const auto value = [&](long double quantile) {
        return static_cast<double>(Percentile(sorted, quantile)) / divisor;
    };
    const double maximum = sorted.empty()
        ? 0.0
        : static_cast<double>(sorted.back()) / divisor;
    std::cout << name << " samples=" << sorted.size()
              << " unit=" << unit
              << " p50=" << value(0.50L)
              << " p99=" << value(0.99L)
              << " p999=" << value(0.999L)
              << " max=" << maximum << '\n';
}

[[nodiscard]] std::uint64_t Delta(std::uint64_t after,
                                  std::uint64_t before) noexcept {
    return after >= before ? after - before : 0U;
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << "benchmark_fact_journal: " << error << '\n';
        PrintUsage();
        return 2;
    }
    if (options.help) {
        PrintUsage();
        return 0;
    }

    std::error_code filesystem_error;
    std::filesystem::create_directories(
        options.journal_directory, filesystem_error);
    if (filesystem_error ||
        !std::filesystem::is_directory(
            options.journal_directory, filesystem_error) ||
        filesystem_error) {
        std::cerr << "benchmark_fact_journal: cannot create journal "
                  << "directory: " << filesystem_error.message() << '\n';
        return 2;
    }
    const std::filesystem::path journal_path =
        UniqueJournalPath(options.journal_directory);
    JournalFileCleanup journal_cleanup(journal_path);
    const std::uint64_t paced_total = options.target_rate == 0U
        ? 0U
        : options.target_rate * static_cast<std::uint64_t>(options.seconds);
    const std::uint64_t mixed_read_samples =
        options.mixed_read_rate *
        static_cast<std::uint64_t>(options.seconds);
    if (options.target_rate != 0U &&
        paced_total > std::numeric_limits<std::uint64_t>::max() -
            options.seed_records) {
        std::cerr << "benchmark_fact_journal: seed plus target count "
                  << "overflows uint64_t\n";
        return 2;
    }
    FactJournalConfig journal_config{};
    journal_config.trade_date = kTradeDate;
    journal_config.path = journal_path;
    journal_config.hot_cache_entries = options.hot_cache_entries;
    if (options.target_rate != 0U) {
        journal_config.maximum_records = paced_total +
            static_cast<std::uint64_t>(options.seed_records);
    }
    const std::uint64_t maximum_records = journal_config.maximum_records;
    const std::uint64_t maximum_directory_pages =
        journal_config.maximum_directory_pages;
    std::unique_ptr<CanonicalFactJournal> journal =
        CanonicalFactJournal::Create(std::move(journal_config), &error);
    if (journal == nullptr) {
        std::cerr << "benchmark_fact_journal: " << error << '\n';
        return 1;
    }

    std::vector<ProbeRecord> mixed_read_seeds;
    const bool seeds_admitted = AdmitSeedRecords(
        options, journal.get(), &mixed_read_seeds, &error);
    const bool seeds_flushed = seeds_admitted && journal->Flush();
    const bool seed_file_cache_drop_advised =
        seeds_flushed && journal->DropFileCache();
    const FactJournalStats timed_start_stats = journal->stats();
    const std::uint64_t seed_count =
        static_cast<std::uint64_t>(options.seed_records);
    const std::uint64_t seed_record_bytes = seed_count *
        l2flow::journal::kFactJournalRecordBytes;
    const std::uint64_t seed_file_bytes = seed_record_bytes +
        l2flow::journal::kFactJournalFileHeaderBytes;
    const bool seeds_ready = seeds_admitted && seeds_flushed &&
        seed_file_cache_drop_advised && journal->healthy() &&
        timed_start_stats.records == seed_count &&
        timed_start_stats.record_bytes == seed_record_bytes &&
        timed_start_stats.file_bytes == seed_file_bytes &&
        timed_start_stats.write_bytes == seed_file_bytes &&
        timed_start_stats.partial_writes == 0U &&
        timed_start_stats.partial_reads == 0U &&
        timed_start_stats.consumer_new == seed_count * 2U &&
        timed_start_stats.duplicates == 0U &&
        timed_start_stats.conflicts == 0U &&
        timed_start_stats.active_writes == 0U &&
        timed_start_stats.active_reads == 0U &&
        timed_start_stats.reserved_records == 0U &&
        timed_start_stats.waiting_admissions == 0U &&
        timed_start_stats.errors == 0U &&
        timed_start_stats.flush_calls >= 1U;
    if (!seeds_ready) {
        std::cerr << "benchmark_fact_journal: mixed-read seed setup "
                  << "failed";
        if (!error.empty()) {
            std::cerr << ": " << error;
        } else if (!journal->healthy()) {
            std::cerr << ": " << journal->fatal_error();
        }
        std::cerr << '\n';
        return 1;
    }

    const std::size_t probes_per_owner = options.probe_samples == 0U
        ? 0U
        : (options.probe_samples + options.owners - 1U) / options.owners;
    std::vector<WorkerResult> worker_results(options.owners);
    std::vector<std::thread> workers;
    workers.reserve(options.owners);
    std::atomic<std::size_t> ready{0U};
    std::atomic<bool> start{false};
    std::atomic<std::uint64_t> start_ns{0U};
    std::atomic<bool> abort{false};
    std::atomic<bool> mixed_read_ready{false};
    MixedReadResult mixed_read_result{};
    std::thread mixed_read_thread;
    if (options.mixed_read_rate != 0U) {
        mixed_read_thread = std::thread(
            RunMixedReadProbe, options.mixed_read_rate,
            mixed_read_samples, journal.get(),
            std::span<const ProbeRecord>(mixed_read_seeds),
            &mixed_read_ready, &start, &start_ns, &abort,
            &mixed_read_result);
    }
    for (std::size_t owner = 0U; owner < options.owners; ++owner) {
        workers.emplace_back(
            RunOwner, owner, std::cref(options), paced_total,
            probes_per_owner, journal.get(), &ready, &start, &start_ns,
            &abort, &worker_results[owner]);
    }
    while (ready.load(std::memory_order_acquire) < options.owners) {
        std::this_thread::yield();
    }
    while (options.mixed_read_rate != 0U &&
           !mixed_read_ready.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    const std::uint64_t origin = NowNs() + UINT64_C(20'000'000);
    start_ns.store(origin, std::memory_order_release);
    start.store(true, std::memory_order_release);
    for (std::thread& worker : workers) {
        worker.join();
    }
    if (mixed_read_thread.joinable()) {
        mixed_read_thread.join();
    }

    std::uint64_t preflush_finish_ns = origin;
    for (const WorkerResult& worker : worker_results) {
        preflush_finish_ns = std::max(
            preflush_finish_ns, worker.finish_ns);
    }
    preflush_finish_ns = std::max(
        preflush_finish_ns, mixed_read_result.finish_ns);
    const std::uint64_t flush_start_ns = NowNs();
    const bool flushed = journal->Flush();
    const std::uint64_t durable_finish_ns = NowNs();
    const FactJournalStats measurement_stats = journal->stats();

    std::uint64_t generated = 0U;
    std::uint64_t admitted = 0U;
    std::uint64_t event_new = 0U;
    std::uint64_t kline_new = 0U;
    std::uint64_t unexpected_duplicates = 0U;
    std::uint64_t conflicts = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t shanghai_ticks = 0U;
    std::uint64_t shenzhen_orders = 0U;
    std::uint64_t shenzhen_trades = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    std::vector<std::uint64_t> event_batch_ns;
    std::vector<std::uint64_t> kline_batch_ns;
    std::vector<std::uint64_t> combined_batch_ns;
    std::vector<std::uint64_t> combined_per_fact_ns;
    std::vector<ProbeRecord> oldest_records;
    std::vector<CanonicalTick> newest_ticks;
    for (WorkerResult& worker : worker_results) {
        generated += worker.generated;
        admitted += worker.admitted;
        event_new += worker.event_new;
        kline_new += worker.kline_new;
        unexpected_duplicates += worker.unexpected_duplicates;
        conflicts += worker.conflicts;
        failures += worker.failures;
        shanghai_ticks += worker.shanghai_ticks;
        shenzhen_orders += worker.shenzhen_orders;
        shenzhen_trades += worker.shenzhen_trades;
        maximum_schedule_lag_ns = std::max(
            maximum_schedule_lag_ns, worker.maximum_schedule_lag_ns);
        MergeSamples(&event_batch_ns, &worker.event_batch_ns);
        MergeSamples(&kline_batch_ns, &worker.kline_batch_ns);
        MergeSamples(&combined_batch_ns, &worker.combined_batch_ns);
        MergeSamples(
            &combined_per_fact_ns, &worker.combined_per_fact_ns);
        oldest_records.insert(
            oldest_records.end(),
            std::make_move_iterator(worker.oldest_records.begin()),
            std::make_move_iterator(worker.oldest_records.end()));
        if (worker.has_last_tick) {
            newest_ticks.push_back(worker.last_tick);
        }
        if (!worker.error.empty()) {
            std::cerr << "owner worker failed: " << worker.error << '\n';
        }
    }
    if (oldest_records.size() > options.probe_samples) {
        oldest_records.resize(options.probe_samples);
    }
    std::sort(event_batch_ns.begin(), event_batch_ns.end());
    std::sort(kline_batch_ns.begin(), kline_batch_ns.end());
    std::sort(combined_batch_ns.begin(), combined_batch_ns.end());
    std::sort(combined_per_fact_ns.begin(),
              combined_per_fact_ns.end());
    std::sort(mixed_read_result.service_time_ns.begin(),
              mixed_read_result.service_time_ns.end());
    if (!mixed_read_result.error.empty()) {
        std::cerr << "mixed-read probe failed: "
                  << mixed_read_result.error << '\n';
    }

    const std::uint64_t expected = options.target_rate == 0U
        ? generated
        : paced_total;
    const std::uint64_t preflush_elapsed_ns =
        preflush_finish_ns > origin ? preflush_finish_ns - origin : 0U;
    const std::uint64_t durable_elapsed_ns =
        durable_finish_ns > origin ? durable_finish_ns - origin : 0U;
    const std::uint64_t flush_elapsed_ns =
        durable_finish_ns >= flush_start_ns
        ? durable_finish_ns - flush_start_ns
        : 0U;
    const auto Rate = [](std::uint64_t rows, std::uint64_t elapsed_ns) {
        return elapsed_ns == 0U
            ? 0.0
            : static_cast<double>(rows) * 1'000'000'000.0 /
                  static_cast<double>(elapsed_ns);
    };
    const double preflush_rate = Rate(admitted, preflush_elapsed_ns);
    const double durable_rate = Rate(admitted, durable_elapsed_ns);
    const std::uint64_t timed_write_bytes = Delta(
        measurement_stats.write_bytes, timed_start_stats.write_bytes);
    const double journal_mib_s = durable_elapsed_ns == 0U
        ? 0.0
        : static_cast<double>(timed_write_bytes) *
              1'000'000'000.0 /
              static_cast<double>(durable_elapsed_ns) /
              (1024.0 * 1024.0);
    const double journal_bytes_s = durable_elapsed_ns == 0U
        ? 0.0
        : static_cast<double>(timed_write_bytes) *
              1'000'000'000.0 /
              static_cast<double>(durable_elapsed_ns);
    const std::uint64_t nominal_ns =
        static_cast<std::uint64_t>(options.seconds) *
        kNanosecondsPerSecond;
    const std::uint64_t allowed_ns = static_cast<std::uint64_t>(
        static_cast<long double>(nominal_ns) *
        (1.0L + kDurationTolerance));
    const bool rate_gate = options.target_rate == 0U ||
        durable_elapsed_ns <= allowed_ns;
    const std::uint64_t expected_total_records = expected + seed_count;
    const std::uint64_t expected_target_record_bytes = expected *
        static_cast<std::uint64_t>(
            l2flow::journal::kFactJournalRecordBytes);
    const std::uint64_t expected_record_bytes = expected_total_records *
        static_cast<std::uint64_t>(
            l2flow::journal::kFactJournalRecordBytes);
    const std::uint64_t expected_file_bytes = expected_record_bytes +
        l2flow::journal::kFactJournalFileHeaderBytes;
    std::error_code file_size_error;
    const std::uintmax_t observed_file_bytes =
        std::filesystem::file_size(journal_path, file_size_error);

    bool valid = !abort.load(std::memory_order_acquire) && flushed &&
        journal->healthy() && generated == expected && admitted == expected &&
        event_new == expected && kline_new == expected &&
        unexpected_duplicates == 0U && conflicts == 0U && failures == 0U &&
        mixed_read_result.completed == mixed_read_samples &&
        mixed_read_result.service_time_ns.size() == mixed_read_samples &&
        mixed_read_result.failures == 0U &&
        mixed_read_result.error.empty() &&
        measurement_stats.records == expected_total_records &&
        measurement_stats.record_bytes == expected_record_bytes &&
        measurement_stats.file_bytes == expected_file_bytes &&
        measurement_stats.write_bytes == expected_file_bytes &&
        timed_write_bytes == expected_target_record_bytes &&
        !file_size_error && observed_file_bytes == expected_file_bytes &&
        measurement_stats.consumer_new == expected_total_records * 2U &&
        measurement_stats.duplicates == 0U &&
        measurement_stats.conflicts == 0U &&
        measurement_stats.partial_writes == 0U &&
        measurement_stats.active_writes == 0U &&
        measurement_stats.active_reads == 0U &&
        measurement_stats.reserved_records == 0U &&
        measurement_stats.waiting_admissions == 0U &&
        measurement_stats.errors == 0U &&
        measurement_stats.flush_calls > timed_start_stats.flush_calls &&
        rate_gate;

    std::vector<std::uint64_t> hot_duplicate_ns;
    hot_duplicate_ns.reserve(options.probe_samples);
    std::uint64_t hot_probe_failures = 0U;
    const FactJournalStats before_hot = journal->stats();
    if (!newest_ticks.empty()) {
        for (std::size_t sample = 0U; sample < options.probe_samples;
             ++sample) {
            const CanonicalTick& tick =
                newest_ticks[sample % newest_ticks.size()];
            const FactConsumer consumer = sample % 2U == 0U
                ? FactConsumer::kEvent
                : FactConsumer::kKLine;
            const std::uint64_t begin = NowNs();
            const std::vector<AdmitResult> result =
                journal->AdmitBatch(consumer, std::span(&tick, 1U));
            const std::uint64_t finish = NowNs();
            hot_duplicate_ns.push_back(finish - begin);
            if (result.size() != 1U ||
                result.front().code != AdmitCode::kDuplicate) {
                ++hot_probe_failures;
            }
        }
    }
    std::sort(hot_duplicate_ns.begin(), hot_duplicate_ns.end());
    const FactJournalStats after_hot = journal->stats();

    journal->ClearHotCache();
    const bool file_cache_drop_advised = journal->DropFileCache();
    const FactJournalStats before_reads = journal->stats();
    std::vector<std::uint64_t> cleared_cache_read_ns;
    cleared_cache_read_ns.reserve(oldest_records.size());
    std::uint64_t read_probe_failures = 0U;
    for (const ProbeRecord& probe : oldest_records) {
        CanonicalTick loaded{};
        const std::uint64_t begin = NowNs();
        const bool read = journal->Read(probe.handle, &loaded);
        const std::uint64_t finish = NowNs();
        cleared_cache_read_ns.push_back(finish - begin);
        if (!read ||
            l2flow::journal::MakeFactKey(loaded) !=
                l2flow::journal::MakeFactKey(probe.tick) ||
            !l2flow::journal::FactPayloadEqual(loaded, probe.tick)) {
            ++read_probe_failures;
        }
    }
    std::sort(cleared_cache_read_ns.begin(),
              cleared_cache_read_ns.end());
    const FactJournalStats final_stats = journal->stats();
    const ProcessMemory memory = ReadProcessMemory();

    const std::uint64_t hot_duplicates = Delta(
        after_hot.duplicates, before_hot.duplicates);
    const std::uint64_t hot_cache_hits = Delta(
        after_hot.hot_cache_hits, before_hot.hot_cache_hits);
    const std::uint64_t probe_read_calls = Delta(
        final_stats.read_calls, before_reads.read_calls);
    const std::uint64_t probe_read_bytes = Delta(
        final_stats.read_bytes, before_reads.read_bytes);
    const std::uint64_t probe_partial_reads = Delta(
        final_stats.partial_reads, before_reads.partial_reads);
    const std::uint64_t probe_read_cache_hits = Delta(
        final_stats.hot_cache_hits, before_reads.hot_cache_hits);
    const std::uint64_t mixed_window_read_calls = Delta(
        measurement_stats.read_calls, timed_start_stats.read_calls);
    const std::uint64_t mixed_window_read_bytes = Delta(
        measurement_stats.read_bytes, timed_start_stats.read_bytes);
    const std::uint64_t mixed_window_partial_reads = Delta(
        measurement_stats.partial_reads, timed_start_stats.partial_reads);
    const std::uint64_t expected_mixed_read_bytes = mixed_read_samples *
        static_cast<std::uint64_t>(
            l2flow::journal::kFactJournalRecordBytes);
    valid = valid && hot_probe_failures == 0U &&
        hot_duplicates == hot_duplicate_ns.size() &&
        hot_cache_hits == hot_duplicate_ns.size() &&
        read_probe_failures == 0U &&
        probe_read_calls == cleared_cache_read_ns.size() &&
        probe_partial_reads == 0U &&
        mixed_window_read_calls == mixed_read_samples &&
        mixed_window_read_bytes == expected_mixed_read_bytes &&
        mixed_window_partial_reads == 0U &&
        file_cache_drop_advised &&
        final_stats.records == measurement_stats.records &&
        final_stats.record_bytes == measurement_stats.record_bytes &&
        final_stats.file_bytes == measurement_stats.file_bytes &&
        final_stats.write_calls == measurement_stats.write_calls &&
        final_stats.write_bytes == measurement_stats.write_bytes &&
        final_stats.partial_writes == measurement_stats.partial_writes &&
        final_stats.consumer_new == measurement_stats.consumer_new &&
        final_stats.conflicts == 0U && final_stats.partial_reads == 0U &&
        final_stats.errors == 0U &&
        final_stats.active_writes == 0U &&
        final_stats.active_reads == 0U &&
        final_stats.reserved_records == 0U &&
        final_stats.waiting_admissions == 0U &&
        journal->healthy();

    std::cout << std::fixed << std::setprecision(3)
              << "scope journal_only=true event_worker=false "
              << "kline_worker=false clickhouse=false\n"
              << "journal_config path=" << journal_path.string()
              << " trade_date=" << kTradeDate
              << " owners=" << options.owners
              << " target_msg_s=" << options.target_rate
              << " seconds=" << options.seconds
              << " batch_rows_per_owner=" << options.batch_rows
              << " hot_cache_entries=" << options.hot_cache_entries
              << " maximum_records=" << maximum_records
              << " maximum_directory_pages=" << maximum_directory_pages
              << " seed_records=" << options.seed_records
              << " mixed_read_rate=" << options.mixed_read_rate
              << " record_bytes="
              << l2flow::journal::kFactJournalRecordBytes << '\n'
              << "workload_topology synthetic=true"
              << " independent_dense_channels_per_owner=true"
              << " target_market_channels_per_owner=2"
              << " real_channel_owner_distribution=false\n"
              << "workload target_expected=" << expected
              << " generated=" << generated
              << " admitted=" << admitted
              << " seed_records_excluded_from_target=" << seed_count
              << " shanghai_tick=" << shanghai_ticks
              << " shenzhen_order=" << shenzhen_orders
              << " shenzhen_trade=" << shenzhen_trades << '\n'
              << "consumer_results event_new=" << event_new
              << " kline_new=" << kline_new
              << " unexpected_duplicates=" << unexpected_duplicates
              << " conflicts=" << conflicts
              << " failures=" << failures << '\n'
              << "throughput preflush_msg_s=" << preflush_rate
              << " durable_msg_s=" << durable_rate
              << " journal_write_bytes_s=" << journal_bytes_s
              << " journal_write_mib_s=" << journal_mib_s
              << " target_timed_write_bytes=" << timed_write_bytes
              << " preflush_elapsed_s="
              << static_cast<double>(preflush_elapsed_ns) / 1.0e9
              << " durable_elapsed_s="
              << static_cast<double>(durable_elapsed_ns) / 1.0e9
              << " flush_ms="
              << static_cast<double>(flush_elapsed_ns) / 1.0e6
              << " maximum_schedule_lag_us="
              << static_cast<double>(maximum_schedule_lag_ns) / 1.0e3
              << " minimum_durable_msg_s="
              << (options.target_rate == 0U
                      ? 0.0
                      : static_cast<double>(options.target_rate) /
                            static_cast<double>(
                                1.0L + kDurationTolerance))
              << " duration_tolerance_pct="
              << static_cast<double>(kDurationTolerance * 100.0L)
              << " rate_gate=" << (rate_gate ? "PASS" : "FAIL") << '\n';
    std::cout
        << "latency_semantics single_consumer_batch_latency="
        << "journal_call_wall_service_time"
        << " dual_consumer_batch_latency=sum_of_event_and_kline_call_wall_times"
        << " per_fact_equivalent=batch_wall_time_divided_by_rows"
        << " per_fact_is_single_message_percentile=false\n";
    PrintLatency("event_admit_batch_latency", event_batch_ns, 1'000.0,
                 "us");
    PrintLatency("kline_admit_batch_latency", kline_batch_ns, 1'000.0,
                 "us");
    PrintLatency("dual_consumer_batch_latency", combined_batch_ns,
                 1'000.0, "us");
    PrintLatency("dual_consumer_per_fact_equivalent",
                 combined_per_fact_ns, 1.0, "ns");
    PrintLatency("per_handle_cache_evicted_mixed_read_service_time",
                 mixed_read_result.service_time_ns, 1'000.0, "us");
    PrintLatency("hot_duplicate_latency", hot_duplicate_ns, 1'000.0,
                 "us");
    PrintLatency("cleared_app_and_advised_file_cache_read_latency",
                 cleared_cache_read_ns, 1'000.0, "us");
    std::cout << "hot_probe requested=" << options.probe_samples
              << " completed=" << hot_duplicate_ns.size()
              << " duplicates=" << hot_duplicates
              << " app_cache_hits=" << hot_cache_hits
              << " failures=" << hot_probe_failures << '\n'
              << "mixed_read_probe requested_rate="
              << options.mixed_read_rate
              << " scheduled_samples=" << mixed_read_samples
              << " completed=" << mixed_read_result.completed
              << " failures=" << mixed_read_result.failures
              << " maximum_schedule_lag_us="
              << static_cast<double>(
                     mixed_read_result.maximum_schedule_lag_ns) /
                     1'000.0
              << " seed_flush=true"
              << " seed_drop_file_cache_advised="
              << (seed_file_cache_drop_advised ? "true" : "false")
              << " per_handle_cache_eviction_outside_timing=true"
              << " physical_device_miss_proven=false"
              << " journal_window_read_calls="
              << mixed_window_read_calls
              << " journal_window_read_bytes="
              << mixed_window_read_bytes
              << " journal_window_partial_reads="
              << mixed_window_partial_reads
              << " journal_window_read_gate="
              << (mixed_window_read_calls == mixed_read_samples &&
                          mixed_window_read_bytes == expected_mixed_read_bytes &&
                          mixed_window_partial_reads == 0U
                      ? "PASS"
                      : "FAIL") << '\n'
              << "read_probe completed=" << cleared_cache_read_ns.size()
              << " read_calls=" << probe_read_calls
              << " read_bytes=" << probe_read_bytes
              << " partial_reads=" << probe_partial_reads
              << " app_cache_hits=" << probe_read_cache_hits
              << " drop_file_cache_advised="
              << (file_cache_drop_advised ? "true" : "false")
              << " drop_file_cache_gate="
              << (file_cache_drop_advised ? "PASS" : "FAIL")
              << " physical_device_miss_proven=false"
              << " failures=" << read_probe_failures << '\n'
              << "journal_stats records=" << measurement_stats.records
              << " target_records=" << expected
              << " seed_records=" << seed_count
              << " record_bytes=" << measurement_stats.record_bytes
              << " file_bytes=" << measurement_stats.file_bytes
              << " observed_file_bytes=" << observed_file_bytes
              << " consumer_new=" << measurement_stats.consumer_new
              << " duplicates=" << measurement_stats.duplicates
              << " conflicts=" << measurement_stats.conflicts
              << " write_calls=" << measurement_stats.write_calls
              << " write_bytes=" << measurement_stats.write_bytes
              << " partial_writes="
              << measurement_stats.partial_writes
              << " read_calls=" << measurement_stats.read_calls
              << " read_bytes=" << measurement_stats.read_bytes
              << " partial_reads=" << measurement_stats.partial_reads
              << " hot_cache_hits=" << measurement_stats.hot_cache_hits
              << " flush_calls=" << measurement_stats.flush_calls
              << " directory_channels="
              << measurement_stats.directory_channels
              << " directory_pages=" << measurement_stats.directory_pages
              << " active_writes=" << measurement_stats.active_writes
              << " active_reads=" << measurement_stats.active_reads
              << " reserved_records="
              << measurement_stats.reserved_records
              << " waiting_admissions="
              << measurement_stats.waiting_admissions
              << " errors=" << measurement_stats.errors << '\n'
              << "memory current_rss_mib="
              << static_cast<double>(memory.current_rss_kib) / 1024.0
              << " peak_rss_mib="
              << static_cast<double>(memory.peak_rss_kib) / 1024.0
              << " available=" << (memory.available ? "true" : "false")
              << '\n'
              << "health healthy="
              << (journal->healthy() ? "true" : "false")
              << " disk_errors=" << final_stats.errors
              << " disk_partial_writes=" << final_stats.partial_writes
              << " disk_partial_reads=" << final_stats.partial_reads
              << " cleanup_on_exit=true"
              << " status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (!journal->healthy()) {
        std::cerr << "FactJournal fatal: " << journal->fatal_error()
                  << '\n';
    }
    return valid ? 0 : 1;
}
