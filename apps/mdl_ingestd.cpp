#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/sdk_runtime.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using l2flow::ingest::CanonicalSnapshot;
using l2flow::ingest::CanonicalTick;
using l2flow::ingest::ChannelGap;
using l2flow::ingest::ChannelFault;
using l2flow::ingest::EngineConfig;
using l2flow::ingest::EngineStats;
using l2flow::ingest::IngestEngine;
using l2flow::ingest::InstrumentCatalog;
using l2flow::ingest::LateRecoveryTick;
using l2flow::ingest::LoadStreamConfig;
using l2flow::ingest::MdlMessageHandler;
using l2flow::ingest::PhysicalSdkConfig;
using l2flow::ingest::PhysicalSdkSession;
using l2flow::ingest::StartMode;
using l2flow::ingest::StreamMask;

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleSignal(int signal_number) {
    static_cast<void>(signal_number);
    g_stop_requested = 1;
}

struct Options final {
    EngineConfig engine{};
    PhysicalSdkConfig sdk{};
    std::string catalog_path;
    std::string stream_config_path = "config/production.streams.conf";
    bool allow_discard_after_dispatch = false;
    bool validate_only = false;
};

void PrintUsage() {
    std::cout
        << "usage: mdl_ingestd --mode from-open|partial --trade-date YYYYMMDD "
           "--catalog FILE --sdk-library FILE "
           "--server ADDRESS --user USER "
           "--allow-discard-after-dispatch [options]\n"
        << "\nThis milestone stops at instrument dispatch. The explicit discard "
           "flag installs bounded drain workers so the physical process can "
           "be exercised without silently claiming durable output.\n"
        << "\noptions:\n"
        << "  --tick-lanes N             default 12\n"
        << "  --snapshot-lanes N         default 4\n"
        << "  --instrument-workers N     default 16\n"
        << "  --stream-config FILE       default "
           "config/production.streams.conf\n"
        << "  --first-decoder-cpu N      default -1 (OS scheduling)\n"
        << "  --partial-initial-hold-ns N\n"
        << "  --partial-gap-wait-ns N    default 500000\n"
        << "  --from-open-gap-wait-ns N  default 500000\n"
        << "  --sdk-work-threads N       default 1\n"
        << "  --sdk-console-log\n"
        << "  --validate-only            do not load or connect the SDK\n";
}

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    Integer value{};
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
    Options parsed{};
    bool mode_set = false;
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
            PrintUsage();
            std::exit(0);
        } else if (argument == "--mode") {
            const std::string_view value = next(argument);
            if (value == "from-open") {
                parsed.engine.start_mode = StartMode::kFromOpen;
            } else if (value == "partial") {
                parsed.engine.start_mode = StartMode::kPartial;
            } else {
                *error = "--mode must be from-open or partial";
                return false;
            }
            mode_set = true;
        } else if (argument == "--trade-date") {
            if (!ParseInteger(next(argument), &parsed.engine.trade_date)) {
                *error = "invalid --trade-date";
                return false;
            }
        } else if (argument == "--catalog") {
            parsed.catalog_path = next(argument);
        } else if (argument == "--stream-config") {
            parsed.stream_config_path = next(argument);
        } else if (argument == "--sdk-library") {
            parsed.sdk.shared_library = next(argument);
        } else if (argument == "--server") {
            parsed.sdk.server_address = next(argument);
        } else if (argument == "--user") {
            parsed.sdk.user_name = next(argument);
        } else if (argument == "--tick-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.tick_decoder_lanes)) {
                *error = "invalid --tick-lanes";
                return false;
            }
        } else if (argument == "--snapshot-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.snapshot_decoder_lanes)) {
                *error = "invalid --snapshot-lanes";
                return false;
            }
        } else if (argument == "--instrument-workers") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.instrument_workers)) {
                *error = "invalid --instrument-workers";
                return false;
            }
        } else if (argument == "--first-decoder-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.first_decoder_cpu)) {
                *error = "invalid --first-decoder-cpu";
                return false;
            }
        } else if (argument == "--partial-initial-hold-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.partial_initial_hold_ns)) {
                *error = "invalid --partial-initial-hold-ns";
                return false;
            }
        } else if (argument == "--partial-gap-wait-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.partial_gap_wait_ns)) {
                *error = "invalid --partial-gap-wait-ns";
                return false;
            }
        } else if (argument == "--from-open-gap-wait-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.from_open_gap_wait_ns)) {
                *error = "invalid --from-open-gap-wait-ns";
                return false;
            }
        } else if (argument == "--sdk-work-threads") {
            if (!ParseInteger(next(argument), &parsed.sdk.work_threads)) {
                *error = "invalid --sdk-work-threads";
                return false;
            }
        } else if (argument == "--sdk-console-log") {
            parsed.sdk.sdk_console_log = true;
        } else if (argument == "--allow-discard-after-dispatch") {
            parsed.allow_discard_after_dispatch = true;
        } else if (argument == "--validate-only") {
            parsed.validate_only = true;
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
        if (!error->empty()) {
            return false;
        }
    }
    if (!mode_set || parsed.engine.trade_date == 0U ||
        parsed.catalog_path.empty()) {
        *error = "--mode, --trade-date, and --catalog are required";
        return false;
    }
    if (!parsed.validate_only &&
        (parsed.sdk.shared_library.empty() ||
         parsed.sdk.server_address.empty() || parsed.sdk.user_name.empty())) {
        *error = "physical mode requires --sdk-library, --server, and --user";
        return false;
    }
    if (!parsed.validate_only && !parsed.allow_discard_after_dispatch) {
        *error =
            "this milestone requires --allow-discard-after-dispatch; no "
            "durable/Event sink has been connected yet";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

void PrintStats(const EngineStats& stats) {
    std::cout << "admitted=" << stats.admitted
              << " rejected=" << stats.rejected
              << " tick_out=" << stats.dispatched_ticks
              << " snapshot_out=" << stats.dispatched_snapshots
              << " decode_errors=" << stats.decode_errors
              << " catalog_misses=" << stats.catalog_misses
              << " gaps=" << stats.gaps_skipped
              << " frozen_channels="
              << stats.from_open_channels_frozen
              << " late_or_duplicate=" << stats.duplicates_or_late
              << " late_recovery_out="
              << stats.late_recovery_dispatched
              << " lane_full=" << stats.lane_full << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << "configuration error: " << error << '\n';
        PrintUsage();
        return 2;
    }
    InstrumentCatalog catalog;
    StreamMask stream_mask = 0U;
    if (!LoadStreamConfig(
            options.stream_config_path, &stream_mask, &error)) {
        std::cerr << "stream config error: " << error << '\n';
        return 2;
    }
    options.engine.enabled_streams = stream_mask;
    options.sdk.enabled_streams = stream_mask;
    if (!InstrumentCatalog::LoadCsv(
            options.catalog_path, &catalog, &error)) {
        std::cerr << "catalog error: " << error << '\n';
        return 2;
    }
    std::unique_ptr<IngestEngine> engine = IngestEngine::Create(
        options.engine, std::move(catalog), &error);
    if (engine == nullptr) {
        std::cerr << "engine configuration error: " << error << '\n';
        return 2;
    }
    if (options.validate_only) {
        std::cout << "configuration valid\n";
        return 0;
    }
    if (!engine->Start(&error)) {
        std::cerr << "engine start failed: " << error << '\n';
        return 1;
    }

    std::atomic<bool> drain_running{true};
    std::atomic<std::uint64_t> consumed_ticks{0U};
    std::atomic<std::uint64_t> consumed_snapshots{0U};
    std::atomic<std::uint64_t> consumed_gaps{0U};
    std::atomic<std::uint64_t> consumed_late_recovery{0U};
    std::atomic<std::uint64_t> consumed_faults{0U};
    std::vector<std::thread> drain_threads;
    drain_threads.reserve(options.engine.instrument_workers);
    for (std::size_t owner = 0U;
         owner < options.engine.instrument_workers; ++owner) {
        drain_threads.emplace_back([&, owner] {
            CanonicalTick tick{};
            CanonicalSnapshot snapshot{};
            ChannelGap gap{};
            LateRecoveryTick late_recovery{};
            ChannelFault fault{};
            while (drain_running.load(std::memory_order_acquire)) {
                bool progress = false;
                while (engine->TryPollTick(owner, &tick)) {
                    consumed_ticks.fetch_add(1U, std::memory_order_relaxed);
                    progress = true;
                }
                while (engine->TryPollSnapshot(owner, &snapshot)) {
                    consumed_snapshots.fetch_add(
                        1U, std::memory_order_relaxed);
                    progress = true;
                }
                if (owner == 0U) {
                    while (engine->TryPollGap(&gap)) {
                        consumed_gaps.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                    while (engine->TryPollLateRecovery(&late_recovery)) {
                        consumed_late_recovery.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                    while (engine->TryPollChannelFault(&fault)) {
                        consumed_faults.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                }
                if (!progress) {
                    std::this_thread::yield();
                }
            }
        });
    }

    MdlMessageHandler handler(engine.get());
    std::unique_ptr<PhysicalSdkSession> sdk = PhysicalSdkSession::Connect(
        options.sdk, &handler, &error);
    if (sdk == nullptr) {
        drain_running.store(false, std::memory_order_release);
        for (std::thread& thread : drain_threads) {
            thread.join();
        }
        engine->Stop();
        std::cerr << "SDK connect failed: " << error << '\n';
        return 1;
    }

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    while (g_stop_requested == 0 && engine->healthy() && !handler.failed()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        PrintStats(engine->stats());
    }

    // Shutdown order is contractual: quiesce SDK callbacks, drain decoder
    // lanes, then drain the instrument-dispatch queues.
    sdk->Shutdown();
    engine->Stop();
    for (;;) {
        const EngineStats final_stats = engine->stats();
        if (consumed_ticks.load(std::memory_order_acquire) >=
                final_stats.dispatched_ticks &&
            consumed_snapshots.load(std::memory_order_acquire) >=
                final_stats.dispatched_snapshots &&
            consumed_late_recovery.load(std::memory_order_acquire) >=
                final_stats.late_recovery_dispatched &&
            consumed_faults.load(std::memory_order_acquire) >=
                final_stats.channel_faults_dispatched) {
            break;
        }
        std::this_thread::yield();
    }
    drain_running.store(false, std::memory_order_release);
    for (std::thread& thread : drain_threads) {
        thread.join();
    }
    // Gap delivery is a coalescing per-Channel state mailbox, so the number
    // of snapshots consumed need not equal the number of gap ranges. Once
    // producers and the normal consumer are stopped, drain any final dirty
    // state here without relying on an event-count equality.
    ChannelGap final_gap{};
    while (engine->TryPollGap(&final_gap)) {
        consumed_gaps.fetch_add(1U, std::memory_order_relaxed);
    }
    const EngineStats final_stats = engine->stats();
    PrintStats(final_stats);
    if (!engine->healthy()) {
        std::cerr << "fatal ingest error: " << engine->fatal_error() << '\n';
        return 1;
    }
    if (handler.failed()) {
        std::cerr << "fatal SDK callback adapter error\n";
        return 1;
    }
    return 0;
}
