#include "l2flow/ingest/catalog.h"
#include "l2flow/ingest/engine.h"
#include "l2flow/ingest/sdk_runtime.h"

#if defined(L2FLOW_CH_HAS_ARROW_RING)
#include "l2flow/arrow/egress.h"
#endif

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
#include "l2flow/clickhouse/event_sink.h"
#include "l2flow/clickhouse/kline_sink.h"
#include "l2flow/clickhouse/raw_sink.h"
#include "l2flow/event/runtime.h"
#include "l2flow/journal/fact_journal.h"
#include "l2flow/kline/runtime.h"
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <charconv>
#include <chrono>
#include <climits>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <linux/mempolicy.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace {

using l2flow::ingest::CanonicalSnapshot;
using l2flow::ingest::CanonicalTick;
using l2flow::ingest::ChannelGap;
using l2flow::ingest::ChannelFault;
using l2flow::ingest::EngineConfig;
using l2flow::ingest::EngineStats;
using l2flow::ingest::IngestEngine;
using l2flow::ingest::InstrumentCatalog;
using l2flow::ingest::LoadStreamConfig;
using l2flow::ingest::MdlMessageHandler;
using l2flow::ingest::MdlConnectionBoundaryReason;
using l2flow::ingest::PhysicalSdkConfig;
using l2flow::ingest::PhysicalSdkSession;
using l2flow::ingest::StartMode;
using l2flow::ingest::StreamMask;
using l2flow::ingest::TickDispatch;
using l2flow::ingest::TickDispatchKind;
using l2flow::ingest::ContinuityInputs;
using l2flow::ingest::ContinuityModeName;
using l2flow::ingest::DispositionOutbox;
using l2flow::ingest::FreshnessFrontier;
using l2flow::ingest::MonotonicNowNs;

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleSignal(int signal_number) {
    static_cast<void>(signal_number);
    g_stop_requested = 1;
}

enum class OperationMode : std::uint8_t {
    kLive,
    kTest,
};

struct Options final {
    EngineConfig engine{};
    PhysicalSdkConfig sdk{};
    std::string catalog_path;
    std::string stream_config_path = "config/production.streams.conf";
    OperationMode operation_mode = OperationMode::kLive;
    std::uint32_t run_seconds = 0U;
    bool run_seconds_set = false;
    bool allow_discard_after_dispatch = false;
    bool validate_only = false;
    std::string process_cpu_list;
    std::optional<std::size_t> memory_node;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    l2flow::arrow_hot::ArrowHotEgressConfig arrow{};
    bool arrow_enabled = false;
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    l2flow::clickhouse::RawClickHouseConfig clickhouse{};
    l2flow::clickhouse::EventClickHouseConfig clickhouse_event{};
    l2flow::clickhouse::KLineClickHouseConfig clickhouse_kline{};
    l2flow::event::EventRuntimeConfig event{};
    l2flow::kline::KLineRuntimeConfig kline{};
    l2flow::journal::FactJournalConfig fact_journal{};
    std::string clickhouse_password_environment;
    bool clickhouse_enabled = false;
    bool event_enabled = false;
    bool kline_enabled = false;
#endif
};

inline constexpr std::uint64_t kLatencySampleEvery = 64U;
inline constexpr std::size_t kLatencyRingCapacity = 4'096U;
inline constexpr std::size_t kLatencyHistogramMaximumUs = 100'000U;
inline constexpr std::size_t kDrainBurstMessages = 256U;

struct LatencySampleSlot final {
    std::atomic<std::uint64_t> sequence{0U};
    std::atomic<std::uint64_t> latency_ns{0U};
};

// Each sampler has one drain-thread writer. Sequence tags let the reporting
// thread detect a ring overwrite without blocking the hot path.
struct alignas(64) LatencySampler final {
    std::array<LatencySampleSlot, kLatencyRingCapacity> samples{};
    std::atomic<std::uint64_t> published{0U};
    std::atomic<std::uint64_t> clock_errors{0U};
    std::uint64_t next_sequence = 0U;
};

struct LatencyWindow final {
    std::vector<std::uint64_t> samples_ns;
    std::uint64_t overwritten = 0U;
    std::uint64_t clock_errors = 0U;
};

struct LatencySummary final {
    std::size_t count = 0U;
    double average_us = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double maximum_us = 0.0;
};

class LatencyAggregate final {
public:
    LatencyAggregate()
        : histogram_(kLatencyHistogramMaximumUs + 2U, 0U) {}

    void Add(const LatencyWindow& window) {
        overwritten_ += window.overwritten;
        clock_errors_ += window.clock_errors;
        for (const std::uint64_t latency_ns : window.samples_ns) {
            ++count_;
            sum_ns_ += static_cast<long double>(latency_ns);
            maximum_ns_ = std::max(maximum_ns_, latency_ns);
            const std::uint64_t latency_us = latency_ns / 1'000U;
            const std::size_t bucket = latency_us <=
                    kLatencyHistogramMaximumUs
                ? static_cast<std::size_t>(latency_us)
                : kLatencyHistogramMaximumUs + 1U;
            ++histogram_[bucket];
        }
    }

    [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
    [[nodiscard]] std::uint64_t overwritten() const noexcept {
        return overwritten_;
    }
    [[nodiscard]] std::uint64_t clock_errors() const noexcept {
        return clock_errors_;
    }
    [[nodiscard]] double average_us() const noexcept {
        if (count_ == 0U) {
            return 0.0;
        }
        return static_cast<double>(
                   sum_ns_ / static_cast<long double>(count_)) /
               1'000.0;
    }
    [[nodiscard]] double maximum_us() const noexcept {
        return static_cast<double>(maximum_ns_) / 1'000.0;
    }
    [[nodiscard]] double PercentileUs(std::uint64_t percentile) const {
        if (count_ == 0U) {
            return 0.0;
        }
        const std::uint64_t target =
            (count_ * percentile + 99U) / 100U;
        std::uint64_t cumulative = 0U;
        for (std::size_t bucket = 0U; bucket < histogram_.size(); ++bucket) {
            cumulative += histogram_[bucket];
            if (cumulative >= target) {
                return static_cast<double>(bucket);
            }
        }
        return static_cast<double>(kLatencyHistogramMaximumUs + 1U);
    }

private:
    std::vector<std::uint64_t> histogram_;
    std::uint64_t count_ = 0U;
    std::uint64_t maximum_ns_ = 0U;
    std::uint64_t overwritten_ = 0U;
    std::uint64_t clock_errors_ = 0U;
    long double sum_ns_ = 0.0L;
};

void PrintUsage() {
    std::cout
        << "usage: mdl_ingestd [--config FILE] "
           "--mode from-open|partial --trade-date YYYYMMDD "
           "--catalog FILE --sdk-library FILE "
           "--server ADDRESS --user USER "
           "[output options] [options]\n"
        << "\nClickHouse raw output is durable only after a successful "
           "synchronous INSERT acknowledgement. Arrow output is an optional "
           "volatile hot path and is not a persistence boundary. Physical "
           "mode requires at least one output, or the explicit discard flag "
           "for testing without output.\n"
        << "\noptions:\n"
        << "  --config FILE             load one option per line; CLI scalar "
           "options override files\n"
        << "  --tick-lanes N             default 12\n"
        << "  --snapshot-lanes N         default 4\n"
        << "  --instrument-workers N     default 16\n"
        << "  --dispatch-queue-capacity N per decoder-to-owner edge; default 1024\n"
        << "  --reorder-entries-per-channel N power of two; default 4096\n"
        << "  --maximum-reorder-span N   online hole window W; default 4096\n"
        << "  --operation-mode live|test default live\n"
        << "  --stream-config FILE       default "
           "config/production.streams.conf\n"
        << "  --first-decoder-cpu N      default -1 (OS scheduling)\n"
        << "  --process-cpus LIST        Linux inherited affinity, e.g. "
           "8-59,156-187\n"
        << "  --memory-node N            Linux MPOL_BIND for future "
           "allocations\n"
        << "  --partial-initial-hold-ns N\n"
        << "  --partial-gap-wait-ns N    default 500000\n"
        << "  --from-open-gap-wait-ns N  default 500000\n"
        << "  --sdk-work-threads N       default 1\n"
        << "  --sdk-ready-timeout-seconds N default 30; range 1..3600\n"
        << "  --sdk-console-log\n"
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        << "  --arrow-ring-dir DIR       publish per-owner Arrow rings\n"
        << "  --arrow-feed-epoch N       required nonzero connection epoch\n"
        << "  --arrow-descriptors N      power of two; default 1024\n"
        << "  --arrow-segments N         default 1088\n"
        << "  --arrow-tick-segment-bytes N      default 262144\n"
        << "  --arrow-snapshot-segment-bytes N  default 262144\n"
        << "  --arrow-diagnostic-segment-bytes N default 131072\n"
        << "  --arrow-max-consumers N    default 16; maximum 64\n"
        << "  --arrow-tick-batch-rows N  default 256\n"
        << "  --arrow-snapshot-batch-rows N default 16\n"
        << "  --arrow-diagnostic-batch-rows N default 64\n"
        << "  --arrow-batch-max-delay-ns N default 1000000\n"
        << "  --arrow-heartbeat-interval-ns N default 1000000000\n"
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        << "  --clickhouse-url URL       enable durable raw Tick/Snapshot "
           "writes\n"
        << "  --clickhouse-database NAME default l2flow\n"
        << "  --clickhouse-user USER     default default\n"
        << "  --clickhouse-password-env NAME read password from environment\n"
        << "  --clickhouse-no-proxy LIST libcurl no-proxy list; default *\n"
        << "  --clickhouse-feed-epoch N  required nonzero feed-run epoch\n"
        << "  --clickhouse-source-instance-id HEX32 optional stable source ID\n"
        << "  --clickhouse-writers N     default 2\n"
        << "  --clickhouse-tick-batch-rows N default 16384\n"
        << "  --clickhouse-tick-batch-bytes N default 16777216\n"
        << "  --clickhouse-tick-batch-max-delay-ns N default 5000000\n"
        << "  --clickhouse-snapshot-batch-rows N default 256\n"
        << "  --clickhouse-snapshot-batch-bytes N default 16777216\n"
        << "  --clickhouse-snapshot-batch-max-delay-ns N default 20000000\n"
        << "  --clickhouse-queue-batches-per-lane N default 8\n"
        << "  --clickhouse-connect-timeout-ms N default 2000\n"
        << "  --clickhouse-request-timeout-ms N default 10000\n"
        << "  --clickhouse-maximum-retry-ms N default 5000\n"
        << "  --clickhouse-shutdown-timeout-ms N default 30000\n"
        << "  --clickhouse-insert-quorum N default 0 (local/no quorum)\n"
        << "  --clickhouse-no-auto-create use externally managed tables\n"
        << "  --clickhouse-no-tls-verify disable HTTPS peer verification\n"
        << "  --fact-journal-path FILE   required for Event/KLine; process-lifetime disk spill\n"
        << "  --fact-journal-hot-cache N decoded winners retained; default 65536\n"
        << "  --fact-journal-maximum-records N global first-winner cap; default 600000000\n"
        << "  --fact-journal-maximum-directory-pages N sparse 64KiB page cap; default 262144\n"
        << "  --event-enable             enable Event projection from the "
           "disposition outbox\n"
        << "  --event-revision-epoch N   required nonzero monotone writer epoch\n"
        << "  --event-calculation-run-id HEX32 required calculation identity\n"
        << "  --event-logic-version N    default 1\n"
        << "  --event-micro-batch-rows N default 512\n"
        << "  --event-micro-batch-max-delay-ns N default 50000000\n"
        << "  --event-persistence-group-max-batches N default 1024 per owner\n"
        << "  --event-persistence-group-max-rows N default 16384 per owner\n"
        << "  --event-persistence-group-max-bytes N default 16777216 per owner\n"
        << "  --event-persistence-group-max-delay-ns N default 1000000000\n"
        << "  --event-insert-request-max-rows N default 1024\n"
        << "  --event-insert-request-max-bytes N default 1048576\n"
        << "  --event-physical-group-max-batches N default 256\n"
        << "  --event-physical-group-max-delay-ns N default 1000000\n"
        << "  --event-writer-lanes N (1,2,4,8,16,32) default 1\n"
        << "  --event-queue-revision-batches N default 1024\n"
        << "  --event-queue-revision-rows N default 1048576\n"
        << "  --event-maximum-carry-orders N default 2097152 per owner\n"
        << "  --event-maximum-order-history-bytes N logical owned-byte cap; "
           "default 4294967296 per owner\n"
        << "  --event-maximum-hot-facts N default 16777216 per owner\n"
        << "  --event-maximum-hot-fact-bytes N default 4294967296 per owner\n"
        << "  --event-maximum-cached-events N default 16777216 per owner\n"
        << "  --event-maximum-pending-commits N default 1024 per owner\n"
        << "  --event-maximum-pending-revision-bytes N default 268435456 per owner\n"
        << "  --event-maximum-repair-bytes N default 536870912 per owner\n"
        << "  --event-maximum-end-candidates N default 2097152 per owner\n"
        << "  --event-maximum-end-projected-rows N default 2097153 per owner\n"
        << "  --event-maximum-end-staging-bytes N default 2147483648 per owner\n"
        << "  --event-end-slice-max-candidates N default 2048\n"
        << "  --event-end-slice-max-cpu-ns N default 500000\n"
        << "  --event-repair-slice-max-order-uses N default 4096\n"
        << "  --event-repair-slice-max-cpu-ns N default 500000\n"
        << "  --event-phase-slice-max-nodes N default 4096\n"
        << "  --event-phase-slice-max-bytes N default 4194304\n"
        << "  --event-phase-slice-max-cpu-ns N default 500000\n"
        << "  --event-maximum-pending-channel-seals N default 4096 per owner\n"
        << "  --event-eviction-slice-max-nodes N default 4096\n"
        << "  --event-eviction-slice-max-bytes N default 4194304\n"
        << "  --kline-enable             enable exchange-time KLine "
           "projection from the disposition outbox\n"
        << "  --kline-interval-seconds N repeatable; default 1; range 1..86400\n"
        << "  --kline-revision-epoch N   required nonzero monotone writer epoch\n"
        << "  --kline-calculation-run-id HEX32 required calculation identity\n"
        << "  --kline-logic-version N    default 1\n"
        << "  --kline-micro-batch-rows N default 256\n"
        << "  --kline-micro-batch-max-delay-ns N default 1000000\n"
        << "  --kline-insert-request-max-rows N default 1024\n"
        << "  --kline-insert-request-max-bytes N default 1048576\n"
        << "  --kline-physical-group-max-batches N default 256\n"
        << "  --kline-physical-group-max-delay-ns N default 1000000\n"
        << "  --kline-writer-lanes N (1,2,4,8,16,32) default 1\n"
        << "  --kline-queue-revision-batches N default 1024\n"
        << "  --kline-queue-revision-rows N default 1048576\n"
        << "  --kline-maximum-bars N     default 4194304 per owner\n"
        << "  --kline-maximum-pending-commits N default 1024 per owner\n"
#endif
        << "  --run-seconds N            test mode only; 0 means until signal, "
           "max 86400\n"
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

[[nodiscard]] std::string_view TrimWhitespace(std::string_view text) noexcept {
    const auto is_space = [](char value) noexcept {
        return std::isspace(static_cast<unsigned char>(value)) != 0;
    };
    while (!text.empty() && is_space(text.front())) {
        text.remove_prefix(1U);
    }
    while (!text.empty() && is_space(text.back())) {
        text.remove_suffix(1U);
    }
    return text;
}

[[nodiscard]] bool IsEnvironmentName(std::string_view name) noexcept {
    if (name.empty()) {
        return false;
    }
    const auto is_alpha = [](char value) noexcept {
        const unsigned char byte = static_cast<unsigned char>(value);
        return std::isalpha(byte) != 0 || value == '_';
    };
    const auto is_alnum = [](char value) noexcept {
        const unsigned char byte = static_cast<unsigned char>(value);
        return std::isalnum(byte) != 0 || value == '_';
    };
    return is_alpha(name.front()) &&
           std::all_of(name.begin() + 1, name.end(), is_alnum);
}

[[nodiscard]] std::string ConfigLocation(std::string_view path,
                                         std::size_t line_number) {
    return std::string(path) + ':' + std::to_string(line_number);
}

[[nodiscard]] bool ExpandEnvironmentReferences(
    std::string_view input,
    std::string_view path,
    std::size_t line_number,
    std::string* output,
    std::string* error) {
    output->clear();
    output->reserve(input.size());
    for (std::size_t index = 0U; index < input.size();) {
        if (input[index] != '$' || index + 1U >= input.size() ||
            input[index + 1U] != '{') {
            output->push_back(input[index]);
            ++index;
            continue;
        }
        const std::size_t closing = input.find('}', index + 2U);
        if (closing == std::string_view::npos) {
            *error = ConfigLocation(path, line_number) +
                     ": unterminated environment reference";
            return false;
        }
        const std::string_view name =
            input.substr(index + 2U, closing - index - 2U);
        if (!IsEnvironmentName(name)) {
            *error = ConfigLocation(path, line_number) +
                     ": invalid environment variable name in ${" +
                     std::string(name) + '}';
            return false;
        }
        const std::string environment_name(name);
        const char* const value = std::getenv(environment_name.c_str());
        if (value == nullptr) {
            *error = ConfigLocation(path, line_number) +
                     ": environment variable " + environment_name +
                     " is unset";
            return false;
        }
        const std::string_view expanded(value);
        if (expanded.empty()) {
            *error = ConfigLocation(path, line_number) +
                     ": environment variable " + environment_name +
                     " is empty";
            return false;
        }
        if (expanded.find_first_of("\r\n") != std::string_view::npos) {
            *error = ConfigLocation(path, line_number) +
                     ": environment variable " + environment_name +
                     " contains a line break";
            return false;
        }
        output->append(expanded);
        index = closing + 1U;
    }
    return true;
}

[[nodiscard]] bool LoadConfigurationArguments(
    std::string_view path,
    std::vector<std::string>* arguments,
    std::string* error) {
    std::ifstream input{std::string(path)};
    if (!input) {
        *error = "cannot open configuration file: " + std::string(path);
        return false;
    }
    std::string line;
    std::size_t line_number = 0U;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.size() > 65'536U) {
            *error = ConfigLocation(path, line_number) +
                     ": configuration line exceeds 65536 bytes";
            return false;
        }
        if (line.find('\0') != std::string::npos) {
            *error = ConfigLocation(path, line_number) +
                     ": configuration line contains a NUL byte";
            return false;
        }
        const std::string_view trimmed = TrimWhitespace(line);
        if (trimmed.empty() || trimmed.front() == '#') {
            continue;
        }
        if (!trimmed.starts_with("--") || trimmed.size() == 2U) {
            *error = ConfigLocation(path, line_number) +
                     ": every non-comment line must start with a long option";
            return false;
        }
        const std::size_t separator = trimmed.find_first_of(" \t\r\n\f\v");
        const std::string_view option = trimmed.substr(0U, separator);
        if (option == "--config" || option.starts_with("--config=")) {
            *error = ConfigLocation(path, line_number) +
                     ": nested --config is not allowed";
            return false;
        }
        arguments->emplace_back(option);
        if (separator == std::string_view::npos) {
            continue;
        }
        const std::string_view value =
            TrimWhitespace(trimmed.substr(separator + 1U));
        if (value.empty()) {
            continue;
        }
        std::string expanded;
        if (!ExpandEnvironmentReferences(
                value, path, line_number, &expanded, error)) {
            return false;
        }
        arguments->push_back(std::move(expanded));
    }
    if (!input.eof()) {
        *error = "failed while reading configuration file: " +
                 std::string(path);
        return false;
    }
    return true;
}

struct ExpandedArguments final {
    std::vector<std::string> storage;
    std::vector<char*> pointers;
};

[[nodiscard]] bool ExpandConfigurationArguments(
    int argc,
    char** argv,
    ExpandedArguments* output,
    std::string* error) {
    std::vector<std::string> configuration_arguments;
    std::vector<std::string> command_line_arguments;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument != "--config") {
            command_line_arguments.emplace_back(argument);
            continue;
        }
        if (index + 1 >= argc) {
            *error = "--config requires a value";
            return false;
        }
        ++index;
        const std::string_view path(argv[index]);
        if (path.empty()) {
            *error = "--config path may not be empty";
            return false;
        }
        if (!LoadConfigurationArguments(
                path, &configuration_arguments, error)) {
            return false;
        }
    }

    const std::size_t total_arguments =
        1U + configuration_arguments.size() +
        command_line_arguments.size();
    if (total_arguments >
        static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        *error = "expanded argument count exceeds INT_MAX";
        return false;
    }
    output->storage.clear();
    output->storage.reserve(total_arguments);
    output->storage.emplace_back(
        argc > 0 && argv[0] != nullptr ? argv[0] : "mdl_ingestd");
    for (std::string& argument : configuration_arguments) {
        output->storage.push_back(std::move(argument));
    }
    for (std::string& argument : command_line_arguments) {
        output->storage.push_back(std::move(argument));
    }
    output->pointers.clear();
    output->pointers.reserve(output->storage.size());
    for (std::string& argument : output->storage) {
        output->pointers.push_back(argument.data());
    }
    return true;
}

#if defined(__linux__)
struct CpuSelection final {
    cpu_set_t mask{};
    std::vector<std::size_t> cpus;
};

[[nodiscard]] bool ParseCpuSelection(std::string_view text,
                                     CpuSelection* output,
                                     std::string* error) {
    if (text.empty()) {
        *error = "--process-cpus may not be empty";
        return false;
    }
    CpuSelection parsed{};
    CPU_ZERO(&parsed.mask);
    std::size_t offset = 0U;
    while (offset < text.size()) {
        const std::size_t comma = text.find(',', offset);
        const std::size_t end = comma == std::string_view::npos
                                    ? text.size()
                                    : comma;
        const std::string_view component = text.substr(offset, end - offset);
        if (component.empty()) {
            *error = "--process-cpus contains an empty component";
            return false;
        }
        const std::size_t dash = component.find('-');
        if (dash != std::string_view::npos &&
            component.find('-', dash + 1U) != std::string_view::npos) {
            *error = "--process-cpus contains a malformed range";
            return false;
        }
        std::size_t first = 0U;
        std::size_t last = 0U;
        if (dash == std::string_view::npos) {
            if (!ParseInteger(component, &first)) {
                *error = "--process-cpus contains a nonnumeric CPU";
                return false;
            }
            last = first;
        } else if (!ParseInteger(component.substr(0U, dash), &first) ||
                   !ParseInteger(component.substr(dash + 1U), &last)) {
            *error = "--process-cpus contains a malformed range";
            return false;
        }
        if (last < first) {
            *error = "--process-cpus ranges must be ascending";
            return false;
        }
        if (last >= static_cast<std::size_t>(CPU_SETSIZE)) {
            *error = "--process-cpus contains a CPU outside CPU_SETSIZE";
            return false;
        }
        for (std::size_t cpu = first;; ++cpu) {
            const int cpu_index = static_cast<int>(cpu);
            if (CPU_ISSET(cpu_index, &parsed.mask) != 0) {
                *error = "--process-cpus contains CPU " +
                         std::to_string(cpu) + " more than once";
                return false;
            }
            CPU_SET(cpu_index, &parsed.mask);
            parsed.cpus.push_back(cpu);
            if (cpu == last) {
                break;
            }
        }
        if (comma == std::string_view::npos) {
            break;
        }
        offset = comma + 1U;
        if (offset == text.size()) {
            *error = "--process-cpus may not end with a comma";
            return false;
        }
    }
    *output = std::move(parsed);
    return true;
}
#endif

[[nodiscard]] bool ValidateProcessPlacement(const Options& options,
                                            std::string* error) {
#if defined(__linux__)
    CpuSelection process_cpus{};
    const bool has_process_cpus = !options.process_cpu_list.empty();
    if (has_process_cpus &&
        !ParseCpuSelection(options.process_cpu_list, &process_cpus, error)) {
        return false;
    }
    if (options.engine.first_decoder_cpu >= 0) {
        const std::size_t first = static_cast<std::size_t>(
            options.engine.first_decoder_cpu);
        if (options.engine.tick_decoder_lanes >
            std::numeric_limits<std::size_t>::max() -
                options.engine.snapshot_decoder_lanes) {
            *error = "configured decoder count overflows size_t";
            return false;
        }
        const std::size_t decoder_count =
            options.engine.tick_decoder_lanes +
            options.engine.snapshot_decoder_lanes;
        if (decoder_count == 0U || first >=
                static_cast<std::size_t>(CPU_SETSIZE) ||
            decoder_count > static_cast<std::size_t>(CPU_SETSIZE) - first) {
            *error = "configured decoder CPU range exceeds CPU_SETSIZE";
            return false;
        }
        if (has_process_cpus) {
            for (std::size_t offset = 0U; offset < decoder_count; ++offset) {
                const std::size_t cpu = first + offset;
                if (CPU_ISSET(static_cast<int>(cpu), &process_cpus.mask) == 0) {
                    *error = "decoder CPU " + std::to_string(cpu) +
                             " is outside --process-cpus";
                    return false;
                }
            }
        }
    }
    if (options.memory_node.has_value()) {
        const std::string node_path = "/sys/devices/system/node/node" +
                                      std::to_string(*options.memory_node);
        std::error_code status_error;
        if (!std::filesystem::is_directory(node_path, status_error)) {
            *error = "--memory-node " +
                     std::to_string(*options.memory_node) +
                     " is not present in Linux NUMA sysfs";
            return false;
        }
    }
    return true;
#else
    if (!options.process_cpu_list.empty() || options.memory_node.has_value()) {
        *error = "--process-cpus and --memory-node are supported only on Linux";
        return false;
    }
    return true;
#endif
}

[[nodiscard]] bool ApplyProcessPlacement(const Options& options,
                                         std::string* error) {
#if defined(__linux__)
    if (!options.process_cpu_list.empty()) {
        CpuSelection requested{};
        if (!ParseCpuSelection(
                options.process_cpu_list, &requested, error)) {
            return false;
        }
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
            *error = "sched_getaffinity failed: " +
                     std::string(std::strerror(errno));
            return false;
        }
        for (const std::size_t cpu : requested.cpus) {
            if (CPU_ISSET(static_cast<int>(cpu), &allowed) == 0) {
                *error = "requested CPU " + std::to_string(cpu) +
                         " is offline or excluded by the current cpuset";
                return false;
            }
        }
        if (::sched_setaffinity(0, sizeof(requested.mask),
                                &requested.mask) != 0) {
            *error = "sched_setaffinity failed: " +
                     std::string(std::strerror(errno));
            return false;
        }
        cpu_set_t effective;
        CPU_ZERO(&effective);
        if (::sched_getaffinity(0, sizeof(effective), &effective) != 0) {
            *error = "affinity readback failed: " +
                     std::string(std::strerror(errno));
            return false;
        }
        if (CPU_EQUAL(&requested.mask, &effective) == 0) {
            *error = "effective CPU affinity differs from --process-cpus";
            return false;
        }
    }
    if (options.memory_node.has_value()) {
#if defined(SYS_set_mempolicy)
        const std::size_t node = *options.memory_node;
        constexpr std::size_t kBitsPerWord =
            sizeof(unsigned long) * static_cast<std::size_t>(CHAR_BIT);
        if (node == std::numeric_limits<unsigned long>::max()) {
            *error = "--memory-node is too large for set_mempolicy";
            return false;
        }
        std::vector<unsigned long> mask(node / kBitsPerWord + 1U, 0UL);
        mask[node / kBitsPerWord] |=
            1UL << static_cast<unsigned int>(node % kBitsPerWord);
        if (mask.size() >
            std::numeric_limits<std::size_t>::max() / kBitsPerWord) {
            *error = "NUMA nodemask bit count overflows size_t";
            return false;
        }
        // set_mempolicy consumes a nodemask bit width. Pass the complete
        // allocated words so the selected bit cannot be truncated by the ABI.
        const std::size_t mask_bits = mask.size() * kBitsPerWord;
        if (mask_bits >
            static_cast<std::size_t>(
                std::numeric_limits<unsigned long>::max())) {
            *error = "NUMA nodemask is too large for set_mempolicy";
            return false;
        }
        const unsigned long maximum_node =
            static_cast<unsigned long>(mask_bits);
        if (::syscall(SYS_set_mempolicy, MPOL_BIND, mask.data(),
                      maximum_node) != 0) {
            *error = "set_mempolicy(MPOL_BIND) failed: " +
                     std::string(std::strerror(errno));
            return false;
        }
#else
        *error = "set_mempolicy is unavailable on this Linux architecture";
        return false;
#endif
    }
    return true;
#else
    if (!options.process_cpu_list.empty() || options.memory_node.has_value()) {
        *error = "process placement is supported only on Linux";
        return false;
    }
    return true;
#endif
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
    Options parsed{};
    bool mode_set = false;
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    bool arrow_feed_epoch_set = false;
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    bool clickhouse_option_seen = false;
    bool clickhouse_feed_epoch_set = false;
    bool clickhouse_source_instance_set = false;
    bool event_option_seen = false;
    bool event_revision_epoch_set = false;
    bool event_calculation_run_id_set = false;
    bool kline_option_seen = false;
    bool kline_interval_set = false;
    bool kline_revision_epoch_set = false;
    bool kline_calculation_run_id_set = false;
    bool fact_journal_option_seen = false;
#endif
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
        } else if (argument == "--operation-mode") {
            const std::string_view value = next(argument);
            if (value == "live") {
                parsed.operation_mode = OperationMode::kLive;
            } else if (value == "test") {
                parsed.operation_mode = OperationMode::kTest;
            } else {
                *error = "--operation-mode must be live or test";
                return false;
            }
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
        } else if (argument == "--dispatch-queue-capacity") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.dispatch_queue_capacity)) {
                *error = "invalid --dispatch-queue-capacity";
                return false;
            }
        } else if (argument == "--reorder-entries-per-channel") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.engine.reorder_entries_per_channel)) {
                *error = "invalid --reorder-entries-per-channel";
                return false;
            }
        } else if (argument == "--maximum-reorder-span") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.maximum_reorder_span)) {
                *error = "invalid --maximum-reorder-span";
                return false;
            }
        } else if (argument == "--first-decoder-cpu") {
            if (!ParseInteger(next(argument),
                              &parsed.engine.first_decoder_cpu)) {
                *error = "invalid --first-decoder-cpu";
                return false;
            }
        } else if (argument == "--process-cpus") {
            parsed.process_cpu_list = next(argument);
        } else if (argument == "--memory-node") {
            std::size_t node = 0U;
            if (!ParseInteger(next(argument), &node)) {
                *error = "invalid --memory-node";
                return false;
            }
            parsed.memory_node = node;
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
        } else if (argument == "--sdk-ready-timeout-seconds") {
            if (!ParseInteger(next(argument),
                              &parsed.sdk.ready_timeout_seconds) ||
                parsed.sdk.ready_timeout_seconds == 0U ||
                parsed.sdk.ready_timeout_seconds > 3'600U) {
                *error =
                    "--sdk-ready-timeout-seconds must be from 1 through 3600";
                return false;
            }
        } else if (argument == "--run-seconds") {
            if (!ParseInteger(next(argument), &parsed.run_seconds) ||
                parsed.run_seconds > 86'400U) {
                *error = "--run-seconds must be from 0 through 86400";
                return false;
            }
            parsed.run_seconds_set = true;
        } else if (argument == "--sdk-console-log") {
            parsed.sdk.sdk_console_log = true;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        } else if (argument == "--clickhouse-url") {
            parsed.clickhouse.endpoint = next(argument);
            parsed.clickhouse_enabled = true;
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-database") {
            parsed.clickhouse.database = next(argument);
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-user") {
            parsed.clickhouse.username = next(argument);
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-password-env") {
            parsed.clickhouse_password_environment = next(argument);
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-no-proxy") {
            parsed.clickhouse.no_proxy = next(argument);
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-feed-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.feed_session_epoch)) {
                *error = "invalid --clickhouse-feed-epoch";
                return false;
            }
            clickhouse_feed_epoch_set = true;
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-source-instance-id") {
            if (!l2flow::clickhouse::ParseIdentifier(
                    next(argument),
                    &parsed.clickhouse.source_instance_id)) {
                *error =
                    "--clickhouse-source-instance-id must be 32 hex digits";
                return false;
            }
            clickhouse_source_instance_set = true;
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-writers") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.writer_threads)) {
                *error = "invalid --clickhouse-writers";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-tick-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.tick_batch_rows)) {
                *error = "invalid --clickhouse-tick-batch-rows";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-tick-batch-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.tick_batch_bytes)) {
                *error = "invalid --clickhouse-tick-batch-bytes";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-tick-batch-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse.tick_batch_max_delay_ns)) {
                *error = "invalid --clickhouse-tick-batch-max-delay-ns";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-snapshot-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.snapshot_batch_rows)) {
                *error = "invalid --clickhouse-snapshot-batch-rows";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-snapshot-batch-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.snapshot_batch_bytes)) {
                *error = "invalid --clickhouse-snapshot-batch-bytes";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument ==
                   "--clickhouse-snapshot-batch-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse.snapshot_batch_max_delay_ns)) {
                *error =
                    "invalid --clickhouse-snapshot-batch-max-delay-ns";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument ==
                   "--clickhouse-queue-batches-per-lane") {
            std::size_t capacity = 0U;
            if (!ParseInteger(next(argument), &capacity)) {
                *error = "invalid --clickhouse-queue-batches-per-lane";
                return false;
            }
            parsed.clickhouse.tick_queue_batches_per_lane = capacity;
            parsed.clickhouse.snapshot_queue_batches_per_lane = capacity;
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-connect-timeout-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.connect_timeout_ms)) {
                *error = "invalid --clickhouse-connect-timeout-ms";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-request-timeout-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.request_timeout_ms)) {
                *error = "invalid --clickhouse-request-timeout-ms";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-maximum-retry-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.maximum_retry_elapsed_ms)) {
                *error = "invalid --clickhouse-maximum-retry-ms";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-shutdown-timeout-ms") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.shutdown_timeout_ms)) {
                *error = "invalid --clickhouse-shutdown-timeout-ms";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-insert-quorum") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse.insert_quorum)) {
                *error = "invalid --clickhouse-insert-quorum";
                return false;
            }
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-no-auto-create") {
            parsed.clickhouse.ensure_local_tables = false;
            clickhouse_option_seen = true;
        } else if (argument == "--clickhouse-no-tls-verify") {
            parsed.clickhouse.tls_verify_peer = false;
            clickhouse_option_seen = true;
        } else if (argument == "--fact-journal-path") {
            parsed.fact_journal.path = next(argument);
            fact_journal_option_seen = true;
        } else if (argument == "--fact-journal-hot-cache") {
            if (!ParseInteger(next(argument),
                              &parsed.fact_journal.hot_cache_entries)) {
                *error = "invalid --fact-journal-hot-cache";
                return false;
            }
            fact_journal_option_seen = true;
        } else if (argument == "--fact-journal-maximum-records") {
            if (!ParseInteger(next(argument),
                              &parsed.fact_journal.maximum_records)) {
                *error = "invalid --fact-journal-maximum-records";
                return false;
            }
            fact_journal_option_seen = true;
        } else if (argument ==
                   "--fact-journal-maximum-directory-pages") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.fact_journal.maximum_directory_pages)) {
                *error =
                    "invalid --fact-journal-maximum-directory-pages";
                return false;
            }
            fact_journal_option_seen = true;
        } else if (argument == "--event-enable") {
            parsed.event_enabled = true;
            event_option_seen = true;
        } else if (argument == "--event-revision-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.revision_epoch)) {
                *error = "invalid --event-revision-epoch";
                return false;
            }
            event_revision_epoch_set = true;
            event_option_seen = true;
        } else if (argument == "--event-calculation-run-id") {
            if (!l2flow::clickhouse::ParseIdentifier(
                    next(argument),
                    &parsed.event.worker.calculation_run_id)) {
                *error =
                    "--event-calculation-run-id must be 32 hex digits";
                return false;
            }
            event_calculation_run_id_set = true;
            event_option_seen = true;
        } else if (argument == "--event-logic-version") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.logic_version)) {
                *error = "invalid --event-logic-version";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-micro-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.event.micro_batch_rows)) {
                *error = "invalid --event-micro-batch-rows";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-micro-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.event.micro_batch_max_delay_ns)) {
                *error = "invalid --event-micro-batch-max-delay-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-persistence-group-max-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_batches)) {
                *error =
                    "invalid --event-persistence-group-max-batches";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-persistence-group-max-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_rows)) {
                *error = "invalid --event-persistence-group-max-rows";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-persistence-group-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_bytes)) {
                *error = "invalid --event-persistence-group-max-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-persistence-group-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.persistence_group_max_delay_ns)) {
                *error =
                    "invalid --event-persistence-group-max-delay-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-insert-request-max-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .insert_request_max_rows)) {
                *error = "invalid --event-insert-request-max-rows";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-insert-request-max-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .insert_request_max_bytes)) {
                *error = "invalid --event-insert-request-max-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-physical-group-max-batches") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .physical_group_max_batches)) {
                *error = "invalid --event-physical-group-max-batches";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-physical-group-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event
                                   .physical_group_max_delay_ns)) {
                *error = "invalid --event-physical-group-max-delay-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-writer-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_event.writer_lanes)) {
                *error = "invalid --event-writer-lanes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-queue-revision-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_event.queue_revision_batches)) {
                *error = "invalid --event-queue-revision-batches";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-queue-revision-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_event.queue_revision_rows)) {
                *error = "invalid --event-queue-revision-rows";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-carry-orders") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.maximum_carry_orders)) {
                *error = "invalid --event-maximum-carry-orders";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-maximum-order-history-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_order_history_bytes)) {
                *error =
                    "invalid --event-maximum-order-history-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-hot-facts") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.maximum_hot_facts)) {
                *error = "invalid --event-maximum-hot-facts";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-hot-fact-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_hot_fact_bytes)) {
                *error = "invalid --event-maximum-hot-fact-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-cached-events") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_cached_events)) {
                *error = "invalid --event-maximum-cached-events";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-pending-commits") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_pending_commits)) {
                *error = "invalid --event-maximum-pending-commits";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-maximum-pending-revision-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_pending_revision_bytes)) {
                *error =
                    "invalid --event-maximum-pending-revision-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-repair-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.maximum_repair_bytes)) {
                *error = "invalid --event-maximum-repair-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-end-candidates") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.maximum_end_candidates)) {
                *error = "invalid --event-maximum-end-candidates";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-maximum-end-projected-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_end_projected_rows)) {
                *error =
                    "invalid --event-maximum-end-projected-rows";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-maximum-end-staging-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.maximum_end_staging_bytes)) {
                *error = "invalid --event-maximum-end-staging-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-end-slice-max-candidates") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.end_slice_max_candidates)) {
                *error = "invalid --event-end-slice-max-candidates";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-end-slice-max-cpu-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.event.worker.end_slice_max_cpu_ns)) {
                *error = "invalid --event-end-slice-max-cpu-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-repair-slice-max-order-uses") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.repair_slice_max_order_uses)) {
                *error =
                    "invalid --event-repair-slice-max-order-uses";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-repair-slice-max-cpu-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.repair_slice_max_cpu_ns)) {
                *error = "invalid --event-repair-slice-max-cpu-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-phase-slice-max-nodes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.phase_slice_max_nodes)) {
                *error = "invalid --event-phase-slice-max-nodes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-phase-slice-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.phase_slice_max_bytes)) {
                *error = "invalid --event-phase-slice-max-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-phase-slice-max-cpu-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.phase_slice_max_cpu_ns)) {
                *error = "invalid --event-phase-slice-max-cpu-ns";
                return false;
            }
            event_option_seen = true;
        } else if (argument ==
                   "--event-maximum-pending-channel-seals") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event
                         .maximum_pending_channel_seals_per_owner)) {
                *error =
                    "invalid --event-maximum-pending-channel-seals";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-eviction-slice-max-nodes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.eviction_slice_max_nodes)) {
                *error = "invalid --event-eviction-slice-max-nodes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--event-eviction-slice-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.event.worker.eviction_slice_max_bytes)) {
                *error = "invalid --event-eviction-slice-max-bytes";
                return false;
            }
            event_option_seen = true;
        } else if (argument == "--kline-enable") {
            parsed.kline_enabled = true;
            kline_option_seen = true;
        } else if (argument == "--kline-interval-seconds") {
            std::uint32_t interval = 0U;
            if (!ParseInteger(next(argument), &interval)) {
                *error = "invalid --kline-interval-seconds";
                return false;
            }
            if (!kline_interval_set) {
                parsed.kline.worker.interval_seconds.clear();
                kline_interval_set = true;
            }
            parsed.kline.worker.interval_seconds.push_back(interval);
            kline_option_seen = true;
        } else if (argument == "--kline-revision-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.worker.revision_epoch)) {
                *error = "invalid --kline-revision-epoch";
                return false;
            }
            kline_revision_epoch_set = true;
            kline_option_seen = true;
        } else if (argument == "--kline-calculation-run-id") {
            if (!l2flow::clickhouse::ParseIdentifier(
                    next(argument),
                    &parsed.kline.worker.calculation_run_id)) {
                *error =
                    "--kline-calculation-run-id must be 32 hex digits";
                return false;
            }
            kline_calculation_run_id_set = true;
            kline_option_seen = true;
        } else if (argument == "--kline-logic-version") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.worker.logic_version)) {
                *error = "invalid --kline-logic-version";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-micro-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.micro_batch_rows)) {
                *error = "invalid --kline-micro-batch-rows";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-micro-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.micro_batch_max_delay_ns)) {
                *error = "invalid --kline-micro-batch-max-delay-ns";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-insert-request-max-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.insert_request_max_rows)) {
                *error = "invalid --kline-insert-request-max-rows";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-insert-request-max-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.insert_request_max_bytes)) {
                *error = "invalid --kline-insert-request-max-bytes";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-physical-group-max-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.physical_group_max_batches)) {
                *error = "invalid --kline-physical-group-max-batches";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-physical-group-max-delay-ns") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.physical_group_max_delay_ns)) {
                *error = "invalid --kline-physical-group-max-delay-ns";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-writer-lanes") {
            if (!ParseInteger(next(argument),
                              &parsed.clickhouse_kline.writer_lanes)) {
                *error = "invalid --kline-writer-lanes";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-queue-revision-batches") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.queue_revision_batches)) {
                *error = "invalid --kline-queue-revision-batches";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-queue-revision-rows") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.clickhouse_kline.queue_revision_rows)) {
                *error = "invalid --kline-queue-revision-rows";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-maximum-bars") {
            if (!ParseInteger(next(argument),
                              &parsed.kline.worker.maximum_bars)) {
                *error = "invalid --kline-maximum-bars";
                return false;
            }
            kline_option_seen = true;
        } else if (argument == "--kline-maximum-pending-commits") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.kline.worker.maximum_pending_commits)) {
                *error = "invalid --kline-maximum-pending-commits";
                return false;
            }
            kline_option_seen = true;
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        } else if (argument == "--arrow-ring-dir") {
            parsed.arrow.root_directory = next(argument);
            parsed.arrow_enabled = true;
        } else if (argument == "--arrow-feed-epoch") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.feed_session_epoch)) {
                *error = "invalid --arrow-feed-epoch";
                return false;
            }
            arrow_feed_epoch_set = true;
        } else if (argument == "--arrow-descriptors") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.descriptor_capacity)) {
                *error = "invalid --arrow-descriptors";
                return false;
            }
        } else if (argument == "--arrow-segments") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.segment_count)) {
                *error = "invalid --arrow-segments";
                return false;
            }
        } else if (argument == "--arrow-tick-segment-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.tick_segment_payload_bytes)) {
                *error = "invalid --arrow-tick-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-snapshot-segment-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.snapshot_segment_payload_bytes)) {
                *error = "invalid --arrow-snapshot-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-diagnostic-segment-bytes") {
            if (!ParseInteger(
                    next(argument),
                    &parsed.arrow.diagnostic_segment_payload_bytes)) {
                *error = "invalid --arrow-diagnostic-segment-bytes";
                return false;
            }
        } else if (argument == "--arrow-max-consumers") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.maximum_consumers)) {
                *error = "invalid --arrow-max-consumers";
                return false;
            }
        } else if (argument == "--arrow-tick-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.tick_batch_rows)) {
                *error = "invalid --arrow-tick-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-snapshot-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.snapshot_batch_rows)) {
                *error = "invalid --arrow-snapshot-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-diagnostic-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.diagnostic_batch_rows)) {
                *error = "invalid --arrow-diagnostic-batch-rows";
                return false;
            }
        } else if (argument == "--arrow-batch-max-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.maximum_batch_delay_ns)) {
                *error = "invalid --arrow-batch-max-delay-ns";
                return false;
            }
        } else if (argument == "--arrow-heartbeat-interval-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.arrow.heartbeat_interval_ns)) {
                *error = "invalid --arrow-heartbeat-interval-ns";
                return false;
            }
#endif
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
    bool output_configured = parsed.allow_discard_after_dispatch;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (event_option_seen && !parsed.event_enabled) {
        *error = "Event options require --event-enable";
        return false;
    }
    if (kline_option_seen && !parsed.kline_enabled) {
        *error = "KLine options require --kline-enable";
        return false;
    }
    if (fact_journal_option_seen &&
        !parsed.event_enabled && !parsed.kline_enabled) {
        *error = "FactJournal options require --event-enable or --kline-enable";
        return false;
    }
    if ((parsed.event_enabled || parsed.kline_enabled) &&
        (parsed.fact_journal.path.empty() ||
         parsed.fact_journal.hot_cache_entries == 0U ||
         parsed.fact_journal.maximum_records == 0U ||
         parsed.fact_journal.maximum_directory_pages == 0U)) {
        *error = "Event/KLine requires a nonempty --fact-journal-path and "
                 "nonzero FactJournal capacities";
        return false;
    }
    parsed.fact_journal.trade_date = parsed.engine.trade_date;
    if (clickhouse_option_seen && !parsed.clickhouse_enabled) {
        *error = "ClickHouse options require --clickhouse-url";
        return false;
    }
    if (parsed.clickhouse_enabled) {
        if (!clickhouse_feed_epoch_set ||
            parsed.clickhouse.feed_session_epoch == 0U) {
            *error =
                "--clickhouse-url requires a nonzero --clickhouse-feed-epoch";
            return false;
        }
        if (clickhouse_source_instance_set &&
            parsed.clickhouse.source_instance_id ==
                l2flow::clickhouse::Identifier128{}) {
            *error = "--clickhouse-source-instance-id may not be all zero";
            return false;
        }
        parsed.clickhouse.tick_decoder_lanes =
            parsed.engine.tick_decoder_lanes;
        parsed.clickhouse.snapshot_decoder_lanes =
            parsed.engine.snapshot_decoder_lanes;
        if (!parsed.clickhouse_password_environment.empty()) {
            const char* const password = std::getenv(
                parsed.clickhouse_password_environment.c_str());
            if (password == nullptr) {
                *error = "ClickHouse password environment variable is unset";
                return false;
            }
            parsed.clickhouse.password = password;
        }
        if (!l2flow::clickhouse::ValidateRawClickHouseConfig(
                parsed.clickhouse, error)) {
            return false;
        }
        parsed.engine.feed_session_epoch =
            parsed.clickhouse.feed_session_epoch;
        output_configured = true;
    }
    if (parsed.event_enabled) {
        if (!parsed.clickhouse_enabled) {
            *error = "--event-enable requires durable --clickhouse-url raw "
                     "output";
            return false;
        }
        if (!event_revision_epoch_set ||
            parsed.event.worker.revision_epoch == 0U) {
            *error = "--event-enable requires a nonzero "
                     "--event-revision-epoch";
            return false;
        }
        if (!event_calculation_run_id_set ||
            parsed.event.worker.calculation_run_id ==
                l2flow::common::Identifier128{}) {
            *error = "--event-enable requires a nonzero "
                     "--event-calculation-run-id";
            return false;
        }
        parsed.event.worker.trade_date = parsed.engine.trade_date;
        parsed.event.worker.owner = 0U;
        parsed.event.feed_session_epoch =
            parsed.clickhouse.feed_session_epoch;
        parsed.event.worker.feed_session_epoch =
            parsed.clickhouse.feed_session_epoch;
        if (parsed.engine.instrument_workers >
            std::numeric_limits<std::uint32_t>::max()) {
            *error = "--instrument-workers exceeds the Event owner range";
            return false;
        }
        parsed.event.worker.owner_count = static_cast<std::uint32_t>(
            parsed.engine.instrument_workers);

        parsed.clickhouse_event.endpoint = parsed.clickhouse.endpoint;
        parsed.clickhouse_event.database = parsed.clickhouse.database;
        parsed.clickhouse_event.username = parsed.clickhouse.username;
        parsed.clickhouse_event.password = parsed.clickhouse.password;
        parsed.clickhouse_event.no_proxy = parsed.clickhouse.no_proxy;
        parsed.clickhouse_event.connect_timeout_ms =
            parsed.clickhouse.connect_timeout_ms;
        parsed.clickhouse_event.request_timeout_ms =
            parsed.clickhouse.request_timeout_ms;
        parsed.clickhouse_event.retry_initial_backoff_ms =
            parsed.clickhouse.retry_initial_backoff_ms;
        parsed.clickhouse_event.retry_max_backoff_ms =
            parsed.clickhouse.retry_max_backoff_ms;
        parsed.clickhouse_event.maximum_retry_elapsed_ms =
            parsed.clickhouse.maximum_retry_elapsed_ms;
        parsed.clickhouse_event.shutdown_timeout_ms =
            parsed.clickhouse.shutdown_timeout_ms;
        parsed.clickhouse_event.insert_quorum =
            parsed.clickhouse.insert_quorum;
        parsed.clickhouse_event.insert_quorum_parallel =
            parsed.clickhouse.insert_quorum_parallel;
        parsed.clickhouse_event.ensure_local_tables =
            parsed.clickhouse.ensure_local_tables;
        parsed.clickhouse_event.tls_verify_peer =
            parsed.clickhouse.tls_verify_peer;
        if (!l2flow::event::ValidateEventRuntimeConfig(
                parsed.event, error) ||
            !l2flow::clickhouse::ValidateEventClickHouseConfig(
                parsed.clickhouse_event, error)) {
            return false;
        }
    }
    if (parsed.kline_enabled) {
        if (!parsed.clickhouse_enabled) {
            *error = "--kline-enable requires durable --clickhouse-url raw "
                     "output";
            return false;
        }
        if (!kline_revision_epoch_set ||
            parsed.kline.worker.revision_epoch == 0U) {
            *error = "--kline-enable requires a nonzero "
                     "--kline-revision-epoch";
            return false;
        }
        if (!kline_calculation_run_id_set ||
            parsed.kline.worker.calculation_run_id ==
                l2flow::common::Identifier128{}) {
            *error = "--kline-enable requires a nonzero "
                     "--kline-calculation-run-id";
            return false;
        }
        if (parsed.engine.instrument_workers >
            std::numeric_limits<std::uint32_t>::max()) {
            *error = "--instrument-workers exceeds the KLine owner range";
            return false;
        }
        parsed.kline.worker.trade_date = parsed.engine.trade_date;
        parsed.kline.worker.owner = 0U;
        parsed.kline.feed_session_epoch =
            parsed.clickhouse.feed_session_epoch;
        parsed.kline.worker.feed_session_epoch =
            parsed.clickhouse.feed_session_epoch;
        parsed.kline.worker.owner_count = static_cast<std::uint32_t>(
            parsed.engine.instrument_workers);
        std::vector<std::uint32_t>& intervals =
            parsed.kline.worker.interval_seconds;
        std::sort(intervals.begin(), intervals.end());
        intervals.erase(std::unique(intervals.begin(), intervals.end()),
                        intervals.end());

        parsed.clickhouse_kline.endpoint = parsed.clickhouse.endpoint;
        parsed.clickhouse_kline.database = parsed.clickhouse.database;
        parsed.clickhouse_kline.username = parsed.clickhouse.username;
        parsed.clickhouse_kline.password = parsed.clickhouse.password;
        parsed.clickhouse_kline.no_proxy = parsed.clickhouse.no_proxy;
        parsed.clickhouse_kline.connect_timeout_ms =
            parsed.clickhouse.connect_timeout_ms;
        parsed.clickhouse_kline.request_timeout_ms =
            parsed.clickhouse.request_timeout_ms;
        parsed.clickhouse_kline.retry_initial_backoff_ms =
            parsed.clickhouse.retry_initial_backoff_ms;
        parsed.clickhouse_kline.retry_max_backoff_ms =
            parsed.clickhouse.retry_max_backoff_ms;
        parsed.clickhouse_kline.maximum_retry_elapsed_ms =
            parsed.clickhouse.maximum_retry_elapsed_ms;
        parsed.clickhouse_kline.shutdown_timeout_ms =
            parsed.clickhouse.shutdown_timeout_ms;
        parsed.clickhouse_kline.insert_quorum =
            parsed.clickhouse.insert_quorum;
        parsed.clickhouse_kline.insert_quorum_parallel =
            parsed.clickhouse.insert_quorum_parallel;
        parsed.clickhouse_kline.ensure_local_tables =
            parsed.clickhouse.ensure_local_tables;
        parsed.clickhouse_kline.tls_verify_peer =
            parsed.clickhouse.tls_verify_peer;
        if (!l2flow::kline::ValidateKLineRuntimeConfig(
                parsed.kline, error) ||
            !l2flow::clickhouse::ValidateKLineClickHouseConfig(
                parsed.clickhouse_kline, error)) {
            return false;
        }
    }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    parsed.arrow.owner_count = parsed.engine.instrument_workers;
    if (parsed.arrow_enabled) {
        if (parsed.engine.feed_session_epoch != 0U &&
            parsed.engine.feed_session_epoch !=
                parsed.arrow.feed_session_epoch) {
            *error = "Arrow and ClickHouse feed epochs must match";
            return false;
        }
        if (!l2flow::arrow_hot::ValidateArrowHotEgressConfig(
                parsed.arrow, error)) {
            return false;
        }
        parsed.engine.feed_session_epoch =
            parsed.arrow.feed_session_epoch;
        output_configured = true;
    } else if (arrow_feed_epoch_set) {
        *error = "--arrow-feed-epoch requires --arrow-ring-dir";
        return false;
    }
#endif
    // Validate-only and discard-only runs have no persisted cross-process
    // identity. Keep the positive engine invariant without assigning this
    // process-local sentinel to any durable output.
    if (parsed.engine.feed_session_epoch == 0U &&
        (parsed.validate_only || parsed.allow_discard_after_dispatch)) {
        parsed.engine.feed_session_epoch = 1U;
    }
    if (!parsed.validate_only && !output_configured) {
        *error =
            "physical mode requires --clickhouse-url, --arrow-ring-dir, or "
            "--allow-discard-after-dispatch";
        return false;
    }
    if (parsed.operation_mode == OperationMode::kLive &&
        parsed.run_seconds_set) {
        *error = "--run-seconds is available only in test operation mode";
        return false;
    }
    if (!ValidateProcessPlacement(parsed, error)) {
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
              << " rejected_late_facts=" << stats.rejected_late_facts
              << " hole_fills_out=" << stats.hole_fills_dispatched
              << " source_channel_controls="
              << stats.source_channel_controls
              << " owner_control_deliveries="
              << stats.owner_control_deliveries
              << " expired_hole_sequences="
              << stats.expired_hole_sequences
              << " lane_full=" << stats.lane_full << '\n';
}

void PrintFreshness(const IngestEngine& engine,
                    bool event_enabled,
                    bool kline_enabled,
                    bool event_healthy,
                    bool kline_healthy,
                    bool event_pending,
                    bool kline_pending,
                    bool fatal) {
    const DispositionOutbox* const outbox = engine.tick_outbox();
    if (outbox == nullptr) {
        return;
    }
    ContinuityInputs inputs{};
    inputs.fatal = fatal;
    inputs.event_enabled = event_enabled;
    inputs.kline_enabled = kline_enabled;
    inputs.event_healthy = event_healthy;
    inputs.kline_healthy = kline_healthy;
    inputs.event_pending = event_pending;
    inputs.kline_pending = kline_pending;
    inputs.now_monotonic_ns = MonotonicNowNs();
    const FreshnessFrontier frontier = outbox->EvaluateFreshness(inputs);
    std::cout << "continuity_mode="
              << ContinuityModeName(frontier.mode)
              << " event_authoritative="
              << (frontier.event_authoritative ? 1 : 0)
              << " kline_authoritative="
              << (frontier.kline_authoritative ? 1 : 0)
              << " outbox_max_lag_lsn=" << frontier.max_lag_lsn
              << '\n';
}

#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
void PrintClickHouseStats(
    const l2flow::clickhouse::RawClickHouseStats& stats) {
    std::cout << "clickhouse_raw_tick_in=" << stats.tick_rows_received
              << " clickhouse_raw_snapshot_in="
              << stats.snapshot_rows_received
              << " clickhouse_batches_queued=" << stats.batches_queued
              << " clickhouse_batches_acked=" << stats.batches_acked
              << " clickhouse_batches_released=" << stats.batches_released
              << " clickhouse_rows_acked=" << stats.rows_acked
              << " clickhouse_retry_attempts=" << stats.retry_attempts
              << " clickhouse_unknown_outcomes=" << stats.unknown_outcomes
              << " clickhouse_bytes_sent=" << stats.bytes_sent
              << " clickhouse_unacked_batches=" << stats.unacked_batches
              << " clickhouse_preallocated_canonical_bytes="
              << stats.preallocated_canonical_bytes << '\n';
}

void PrintEventStats(
    const l2flow::event::EventRuntimeStats& runtime,
    const l2flow::clickhouse::EventClickHouseStats& sink,
    double sink_batches_per_second = 0.0) {
    const double facts_per_micro_batch = runtime.micro_batches_applied == 0U
        ? 0.0
        : static_cast<double>(runtime.facts_in_micro_batches) /
              static_cast<double>(runtime.micro_batches_applied);
    const double revisions_per_sink_batch =
        sink.submission_groups_queued == 0U
        ? 0.0
        : static_cast<double>(sink.revision_rows_queued) /
              static_cast<double>(sink.submission_groups_queued);
    const double revisions_per_insert =
        sink.revision_insert_requests_acked == 0U
        ? 0.0
        : static_cast<double>(sink.revision_rows_acked) /
              static_cast<double>(sink.revision_insert_requests_acked);
    std::cout << "event_ordered_dispositions="
              << runtime.ordered_dispositions_received
              << " event_hole_fill_dispositions="
              << runtime.hole_fill_dispositions_received
              << " event_rejected_dispositions="
              << runtime.rejected_dispositions_received
              << " event_gap_open_controls="
              << runtime.gap_open_controls_received
              << " event_channel_seal_controls="
              << runtime.channel_seal_controls_received
              << " event_channel_seals_applied="
              << runtime.channel_seals_applied
              << " event_channel_seals_coalesced="
              << runtime.channel_seals_coalesced
              << " event_pending_channel_seals="
              << runtime.pending_channel_seals
              << " event_pending_channel_seals_hwm="
              << runtime.pending_channel_seals_high_water
              << " event_micro_batches=" << runtime.micro_batches_applied
              << " event_facts_in_micro_batches="
              << runtime.facts_in_micro_batches
              << " event_facts_per_micro_batch=" << facts_per_micro_batch
              << " event_micro_batch_rows_max="
              << runtime.micro_batch_rows_max
              << " event_micro_batch_source_age_ns_max="
              << runtime.micro_batch_source_age_ns_max
              << " event_row_limit_flushes=" << runtime.row_limit_flushes
              << " event_timer_flushes=" << runtime.timer_flushes
              << " event_forced_active_flushes="
              << runtime.forced_active_flushes
              << " event_empty_control_flushes="
              << runtime.empty_control_flushes
              << " event_explicit_flushes=" << runtime.explicit_flushes
              << " event_facts=" << runtime.workers.facts_journaled
              << " event_repaired_uses="
              << runtime.workers.repaired_order_uses
              << " event_convergence_stops="
              << runtime.workers.repair_convergence_stops
              << " event_repair_slices="
              << runtime.workers.repair_slices
              << " event_repair_commits="
              << runtime.workers.repair_commits
              << " event_repair_restarts="
              << runtime.workers.repair_order_restarts
              << " event_ordered_batch_fast_path="
              << runtime.workers.ordered_batch_fast_path
              << " event_unordered_batch_sorts="
              << runtime.workers.unordered_batch_sorts
              << " event_barrier_index_orders_visited="
              << runtime.workers.barrier_index_orders_visited
              << " event_source_only_fast_path="
              << runtime.workers.source_only_fast_path
              << " event_order_uses_compacted="
              << runtime.workers.order_uses_compacted
              << " event_order_history_bytes="
              << runtime.workers.order_history_bytes
              << " event_order_history_bytes_hwm="
              << runtime.workers.order_history_bytes_high_watermark
              << " event_facts_evicted="
              << runtime.workers.facts_evicted
              << " event_eviction_slices="
              << runtime.workers.eviction_slices
              << " event_pending_revision_bytes="
              << runtime.workers.pending_revision_bytes
              << " event_pending_revision_bytes_hwm="
              << runtime.workers.pending_revision_bytes_high_watermark
              << " event_active_repair_orders="
              << runtime.workers.active_repair_orders
              << " event_active_repair_bytes="
              << runtime.workers.active_repair_bytes
              << " event_active_repair_bytes_hwm="
              << runtime.workers.active_repair_bytes_high_watermark
              << " event_phase_slices="
              << runtime.workers.phase_normalization_slices
              << " event_phase_facts_scanned="
              << runtime.workers.phase_facts_scanned
              << " event_phase_dirty_roles="
              << runtime.workers.phase_dirty_roles_discovered
              << " event_pending_phase_bytes="
              << runtime.workers.pending_phase_bytes
              << " event_pending_phase_bytes_hwm="
              << runtime.workers.pending_phase_bytes_high_watermark
              << " event_end_slices="
              << runtime.workers.end_expansion_slices
              << " event_end_candidates="
              << runtime.workers.end_candidates_processed
              << " event_hot_facts=" << runtime.workers.hot_facts
              << " event_hot_fact_bytes="
              << runtime.workers.hot_fact_bytes
              << " event_hot_fact_bytes_hwm="
              << runtime.workers.hot_fact_bytes_high_watermark
              << " event_revisions=" << runtime.workers.revisions_created
              << " event_pending_raw="
              << runtime.workers.pending_raw_commits
              << " event_persistence_groups_submitted="
              << runtime.workers.persistence_groups_submitted
              << " event_persistence_group_batches_max="
              << runtime.workers.persistence_group_batches_max
              << " event_persistence_group_rows_max="
              << runtime.workers.persistence_group_rows_max
              << " event_persistence_group_bytes_max="
              << runtime.workers.persistence_group_bytes_max
              << " event_sink_submission_groups_queued="
              << sink.submission_groups_queued
              << " event_sink_submission_groups_released="
              << sink.submission_groups_released
              << " event_sink_batches_per_second="
              << sink_batches_per_second
              << " event_revisions_per_sink_batch="
              << revisions_per_sink_batch
              << " event_revisions_per_insert=" << revisions_per_insert
              << " event_sink_logical_batches_queued="
              << sink.revision_batches_queued
              << " event_sink_logical_batches_acked="
              << sink.revision_batches_acked
              << " event_sink_rows_acked=" << sink.revision_rows_acked
              << " event_sink_physical_groups="
              << sink.physical_groups_committed
              << " event_sink_revision_requests="
              << sink.revision_insert_requests_acked
              << " event_sink_marker_requests="
              << sink.marker_insert_requests_acked
              << " event_sink_group_batches_max="
              << sink.physical_group_batches_max
              << " event_sink_group_rows_max="
              << sink.physical_group_rows_max
              << " event_sink_request_rows_max="
              << sink.revision_request_rows_max
              << " event_sink_request_bytes_max="
              << sink.revision_request_bytes_max
              << " event_sink_revision_latency_ns_max="
              << sink.revision_insert_latency_ns_max
              << " event_sink_marker_latency_ns_max="
              << sink.marker_insert_latency_ns_max
              << " event_recovery_runs_committed="
              << sink.recovery_runs_committed
              << " event_sink_retry_attempts=" << sink.retry_attempts
              << " event_sink_unknown_outcomes=" << sink.unknown_outcomes
              << " event_sink_queued_submission_groups="
              << sink.queued_submission_groups
              << " event_sink_queued_logical_batches="
              << sink.queued_revision_batches
              << " event_sink_queued_rows=" << sink.queued_revision_rows
              << " event_sink_queued_submission_groups_hwm="
              << sink.queued_submission_groups_high_water
              << " event_sink_queued_logical_batches_hwm="
              << sink.queued_revision_batches_high_water
              << " event_sink_queued_rows_hwm="
              << sink.queued_revision_rows_high_water
              << '\n';
}

void PrintKLineStats(
    const l2flow::kline::KLineRuntimeStats& runtime,
    const l2flow::clickhouse::KLineClickHouseStats& sink) {
    std::cout << "kline_ordered_dispositions="
              << runtime.ordered_dispositions_received
              << " kline_hole_fill_dispositions="
              << runtime.hole_fill_dispositions_received
              << " kline_rejected_dispositions="
              << runtime.rejected_dispositions_received
              << " kline_micro_batches=" << runtime.micro_batches_applied
              << " kline_facts_in_micro_batches="
              << runtime.facts_in_micro_batches
              << " kline_micro_batch_rows_max="
              << runtime.micro_batch_rows_max
              << " kline_micro_batch_source_age_ns_max="
              << runtime.micro_batch_source_age_ns_max
              << " kline_row_limit_flushes=" << runtime.row_limit_flushes
              << " kline_timer_flushes=" << runtime.timer_flushes
              << " kline_explicit_flushes=" << runtime.explicit_flushes
              << " kline_facts=" << runtime.workers.facts_journaled
              << " kline_trades=" << runtime.workers.trades_projected
              << " kline_invalid_exchange_time="
              << runtime.workers.invalid_trade_exchange_times
              << " kline_bars_created=" << runtime.workers.bars_created
              << " kline_bars_updated=" << runtime.workers.bars_updated
              << " kline_revisions=" << runtime.workers.revisions_created
              << " kline_pending_raw="
              << runtime.workers.pending_raw_commits
              << " kline_pending_revision_rows="
              << runtime.workers.pending_revision_rows
              << " kline_pending_revision_rows_owner_hwm_max="
              << runtime.workers.pending_revision_rows_high_watermark
              << " kline_pending_revision_bytes="
              << runtime.workers.pending_revision_bytes
              << " kline_pending_revision_bytes_owner_hwm_max="
              << runtime.workers.pending_revision_bytes_high_watermark
              << " kline_sink_batches_queued="
              << sink.revision_batches_queued
              << " kline_sink_batches_acked="
              << sink.revision_batches_acked
              << " kline_sink_batches_released="
              << sink.revision_batches_released
              << " kline_sink_rows_queued_total="
              << sink.revision_rows_queued
              << " kline_sink_rows_acked=" << sink.revision_rows_acked
              << " kline_sink_physical_groups="
              << sink.physical_groups_committed
              << " kline_sink_revision_insert_requests="
              << sink.revision_insert_requests_acked
              << " kline_sink_marker_insert_requests="
              << sink.marker_insert_requests_acked
              << " kline_sink_revision_insert_rows="
              << sink.revision_insert_rows_acked
              << " kline_sink_revision_insert_bytes="
              << sink.revision_insert_bytes_acked
              << " kline_sink_marker_insert_rows="
              << sink.marker_insert_rows_acked
              << " kline_sink_marker_insert_bytes="
              << sink.marker_insert_bytes_acked
              << " kline_recovery_runs_committed="
              << sink.recovery_runs_committed
              << " kline_sink_physical_group_batches_max="
              << sink.physical_group_batches_max
              << " kline_sink_physical_group_rows_max="
              << sink.physical_group_rows_max
              << " kline_sink_revision_request_rows_max="
              << sink.revision_request_rows_max
              << " kline_sink_revision_request_bytes_max="
              << sink.revision_request_bytes_max
              << " kline_sink_marker_request_rows_max="
              << sink.marker_request_rows_max
              << " kline_sink_marker_request_bytes_max="
              << sink.marker_request_bytes_max
              << " kline_sink_revision_latency_ns_total="
              << sink.revision_insert_latency_ns_total
              << " kline_sink_revision_latency_ns_max="
              << sink.revision_insert_latency_ns_max
              << " kline_sink_marker_latency_ns_total="
              << sink.marker_insert_latency_ns_total
              << " kline_sink_marker_latency_ns_max="
              << sink.marker_insert_latency_ns_max
              << " kline_sink_retry_attempts=" << sink.retry_attempts
              << " kline_sink_unknown_outcomes=" << sink.unknown_outcomes
              << " kline_sink_bytes_sent=" << sink.bytes_sent
              << " kline_sink_queued_batches="
              << sink.queued_revision_batches
              << " kline_sink_queued_rows=" << sink.queued_revision_rows
              << " kline_sink_queued_batches_hwm="
              << sink.queued_revision_batches_high_water
              << " kline_sink_queued_rows_hwm="
              << sink.queued_revision_rows_high_water
              << '\n';
    for (std::size_t lane = 0U; lane < sink.writer_lanes; ++lane) {
        const auto& lane_stats = sink.lanes[lane];
        std::cout << "kline_sink_lane=" << lane
                  << " kline_sink_lane_queued_batches="
                  << lane_stats.queued_revision_batches
                  << " kline_sink_lane_queued_rows="
                  << lane_stats.queued_revision_rows
                  << " kline_sink_lane_queued_batches_hwm="
                  << lane_stats.queued_revision_batches_high_water
                  << " kline_sink_lane_queued_rows_hwm="
                  << lane_stats.queued_revision_rows_high_water
                  << '\n';
    }
}
#endif

#if defined(L2FLOW_CH_HAS_ARROW_RING)
void PrintArrowStats(
    const l2flow::arrow_hot::ArrowHotEgressStats& stats) {
    std::cout << "arrow_tick_in=" << stats.tick_rows_received
              << " arrow_snapshot_in=" << stats.snapshot_rows_received
              << " arrow_hole_fill_in=" << stats.hole_fill_rows_received
              << " arrow_control_in=" << stats.control_rows_received
              << " arrow_batches=" << stats.published_batches
              << " arrow_rows=" << stats.published_rows
              << " arrow_no_segment_dropped_rows="
              << stats.no_segment_dropped_rows
              << " arrow_oversized_dropped_rows="
              << stats.oversized_dropped_rows
              << " arrow_control_dropped_rows="
              << stats.control_publish_dropped_rows
              << " arrow_internal_errors=" << stats.internal_errors << '\n';
}

void PublishMdlConnectionBoundary(
    l2flow::arrow_hot::ArrowHotEgress* egress,
    const MdlMessageHandler& handler) noexcept {
    if (egress == nullptr) {
        return;
    }
    const std::uint64_t observed =
        handler.connection_boundary_monotonic_ns();
    switch (handler.connection_boundary_reason()) {
        case MdlConnectionBoundaryReason::kNone:
            return;
        case MdlConnectionBoundaryReason::kConnectError:
            static_cast<void>(egress->MarkFeedConnectError(observed));
            return;
        case MdlConnectionBoundaryReason::kDisconnected:
            static_cast<void>(egress->MarkFeedDisconnected(observed));
            return;
        case MdlConnectionBoundaryReason::kServiceTimeout:
            static_cast<void>(egress->MarkFeedServiceTimeout(observed));
            return;
        case MdlConnectionBoundaryReason::kMessageDiscarded:
            static_cast<void>(egress->MarkFeedMessageDiscarded(observed));
            return;
        case MdlConnectionBoundaryReason::kSubscriptionRejected:
            static_cast<void>(
                egress->MarkFeedSubscriptionRejected(observed));
            return;
        case MdlConnectionBoundaryReason::kControlProtocolError:
            static_cast<void>(
                egress->MarkFeedControlProtocolError(observed));
            return;
        case MdlConnectionBoundaryReason::kReadyTimeout:
            static_cast<void>(egress->MarkFeedReadyTimeout(observed));
            return;
    }
}
#endif

void ObserveLatency(std::uint64_t ingress_sequence,
                    std::uint64_t receive_monotonic_ns,
                    LatencySampler* sampler) noexcept {
    if (sampler == nullptr ||
        ingress_sequence % kLatencySampleEvery != 0U) {
        return;
    }
    const std::uint64_t now = l2flow::ingest::MonotonicNowNs();
    if (now < receive_monotonic_ns) {
        sampler->clock_errors.fetch_add(1U, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t write_sequence = sampler->next_sequence;
    LatencySampleSlot& slot = sampler->samples[static_cast<std::size_t>(
        write_sequence % kLatencyRingCapacity)];
    slot.latency_ns.store(
        now - receive_monotonic_ns, std::memory_order_relaxed);
    slot.sequence.store(write_sequence + 1U, std::memory_order_release);
    sampler->next_sequence = write_sequence + 1U;
    sampler->published.store(write_sequence + 1U, std::memory_order_release);
}

LatencyWindow CollectLatency(
    LatencySampler* samplers,
    std::size_t sampler_count,
    std::vector<std::uint64_t>* cursors,
    std::vector<std::uint64_t>* clock_error_cursors) {
    LatencyWindow window;
    if (samplers == nullptr || cursors == nullptr ||
        clock_error_cursors == nullptr || cursors->size() != sampler_count ||
        clock_error_cursors->size() != sampler_count) {
        return window;
    }
    for (std::size_t index = 0U; index < sampler_count; ++index) {
        LatencySampler& sampler = samplers[index];
        const std::uint64_t end =
            sampler.published.load(std::memory_order_acquire);
        std::uint64_t begin = (*cursors)[index];
        if (end < begin) {
            begin = end;
        }
        if (end - begin > kLatencyRingCapacity) {
            window.overwritten += end - begin - kLatencyRingCapacity;
            begin = end - kLatencyRingCapacity;
        }
        for (std::uint64_t sequence = begin; sequence < end; ++sequence) {
            LatencySampleSlot& slot = sampler.samples[
                static_cast<std::size_t>(
                    sequence % kLatencyRingCapacity)];
            const std::uint64_t expected = sequence + 1U;
            const std::uint64_t first_tag =
                slot.sequence.load(std::memory_order_acquire);
            const std::uint64_t latency_ns =
                slot.latency_ns.load(std::memory_order_relaxed);
            const std::uint64_t second_tag =
                slot.sequence.load(std::memory_order_acquire);
            if (first_tag == expected && second_tag == expected) {
                window.samples_ns.push_back(latency_ns);
            } else {
                ++window.overwritten;
            }
        }
        (*cursors)[index] = end;

        const std::uint64_t clock_errors =
            sampler.clock_errors.load(std::memory_order_acquire);
        const std::uint64_t previous_clock_errors =
            (*clock_error_cursors)[index];
        if (clock_errors >= previous_clock_errors) {
            window.clock_errors += clock_errors - previous_clock_errors;
        }
        (*clock_error_cursors)[index] = clock_errors;
    }
    return window;
}

LatencySummary SummarizeLatency(
    std::vector<std::uint64_t>* samples_ns) {
    LatencySummary summary;
    if (samples_ns == nullptr || samples_ns->empty()) {
        return summary;
    }
    std::sort(samples_ns->begin(), samples_ns->end());
    summary.count = samples_ns->size();
    const long double sum = std::accumulate(
        samples_ns->begin(), samples_ns->end(), 0.0L);
    summary.average_us = static_cast<double>(
        sum / static_cast<long double>(summary.count)) / 1'000.0;
    const auto percentile = [samples_ns](std::size_t percent) {
        const std::size_t rank =
            (samples_ns->size() * percent + 99U) / 100U - 1U;
        return static_cast<double>((*samples_ns)[rank]) / 1'000.0;
    };
    summary.p50_us = percentile(50U);
    summary.p95_us = percentile(95U);
    summary.p99_us = percentile(99U);
    summary.maximum_us =
        static_cast<double>(samples_ns->back()) / 1'000.0;
    return summary;
}

[[nodiscard]] std::uint64_t CounterDelta(std::uint64_t current,
                                         std::uint64_t previous) noexcept {
    return current >= previous ? current - previous : 0U;
}

void PrintMonitor(const EngineStats& current,
                  const EngineStats& previous,
                  double interval_seconds,
                  const LatencySummary& latency,
                  const LatencyWindow& latency_window,
                  const LatencyAggregate& latency_aggregate) {
    const double safe_interval = interval_seconds > 0.0
        ? interval_seconds
        : 1.0;
    const std::uint64_t dispatch_current =
        current.dispatched_ticks + current.dispatched_snapshots;
    const std::uint64_t dispatch_previous =
        previous.dispatched_ticks + previous.dispatched_snapshots;
    std::cout << std::fixed << std::setprecision(3)
              << "monitor interval_s=" << safe_interval
              << " callback_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     current.callbacks, previous.callbacks)) /
                     safe_interval
              << " admit_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     current.admitted, previous.admitted)) /
                     safe_interval
              << " dispatch_rate_msg_s="
              << static_cast<double>(CounterDelta(
                     dispatch_current, dispatch_previous)) /
                     safe_interval
              << " delay_sample_every=" << kLatencySampleEvery
              << " delay_samples=" << latency.count
              << " delay_avg_us=" << latency.average_us
              << " delay_p50_us=" << latency.p50_us
              << " delay_p95_us=" << latency.p95_us
              << " delay_p99_us=" << latency.p99_us
              << " delay_max_us=" << latency.maximum_us
              << " delay_overwritten=" << latency_window.overwritten
              << " delay_clock_errors=" << latency_window.clock_errors
              << " total_delay_samples=" << latency_aggregate.count()
              << " total_delay_avg_us=" << latency_aggregate.average_us()
              << " total_delay_p50_us="
              << latency_aggregate.PercentileUs(50U)
              << " total_delay_p95_us="
              << latency_aggregate.PercentileUs(95U)
              << " total_delay_p99_us="
              << latency_aggregate.PercentileUs(99U)
              << " total_delay_max_us="
              << latency_aggregate.maximum_us()
              << " total_delay_overwritten="
              << latency_aggregate.overwritten()
              << " total_delay_clock_errors="
              << latency_aggregate.clock_errors()
              << " admitted=" << current.admitted
              << " rejected=" << current.rejected
              << " tick_out=" << current.dispatched_ticks
              << " snapshot_out=" << current.dispatched_snapshots
              << " decode_errors=" << current.decode_errors
              << " catalog_misses=" << current.catalog_misses
              << " gaps=" << current.gaps_skipped
              << " rejected_late_facts="
              << current.rejected_late_facts
              << " hole_fills_out="
              << current.hole_fills_dispatched
              << " source_channel_controls="
              << current.source_channel_controls
              << " owner_control_deliveries="
              << current.owner_control_deliveries
              << " lane_full=" << current.lane_full << '\n'
              << std::flush;
}

void PrintFinalLatency(const LatencyAggregate& latency) {
    std::cout << std::fixed << std::setprecision(3)
              << "final_delay basis=callback_to_dispatch"
              << " sample_every=" << kLatencySampleEvery
              << " samples=" << latency.count()
              << " avg_us=" << latency.average_us()
              << " p50_us=" << latency.PercentileUs(50U)
              << " p95_us=" << latency.PercentileUs(95U)
              << " p99_us=" << latency.PercentileUs(99U)
              << " max_us=" << latency.maximum_us()
              << " overwritten=" << latency.overwritten()
              << " clock_errors=" << latency.clock_errors() << '\n'
              << std::flush;
}

}  // namespace

int main(int argc, char** argv) {
    for (int index = 1; index < argc; ++index) {
        if (std::string_view(argv[index]) == "--help") {
            PrintUsage();
            return 0;
        }
    }
    ExpandedArguments expanded_arguments;
    Options options{};
    std::string error;
    if (!ExpandConfigurationArguments(
            argc, argv, &expanded_arguments, &error)) {
        std::cerr << "configuration error: " << error << '\n';
        return 2;
    }
    if (!ParseOptions(
            static_cast<int>(expanded_arguments.pointers.size()),
            expanded_arguments.pointers.data(), &options, &error)) {
        std::cerr << "configuration error: " << error << '\n';
        PrintUsage();
        return 2;
    }
    if (!options.validate_only &&
        !ApplyProcessPlacement(options, &error)) {
        std::cerr << "process placement error: " << error << '\n';
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
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    std::shared_ptr<l2flow::journal::CanonicalFactJournal> fact_journal;
    std::unique_ptr<l2flow::clickhouse::EventClickHouseSink> clickhouse_event;
    std::unique_ptr<l2flow::event::EventRuntime> event_runtime;
    std::unique_ptr<l2flow::clickhouse::KLineClickHouseSink> clickhouse_kline;
    std::unique_ptr<l2flow::kline::KLineRuntime> kline_runtime;
    std::unique_ptr<l2flow::clickhouse::RawClickHouseSink> clickhouse_raw;
    if ((options.event_enabled || options.kline_enabled) &&
        !options.validate_only) {
        std::unique_ptr<l2flow::journal::CanonicalFactJournal> created =
            l2flow::journal::CanonicalFactJournal::Create(
                options.fact_journal, &error);
        if (created == nullptr) {
            std::cerr << "FactJournal creation failed: " << error << '\n';
            return 1;
        }
        fact_journal = std::shared_ptr<
            l2flow::journal::CanonicalFactJournal>(std::move(created));
        options.event.worker.fact_journal = fact_journal;
        options.kline.worker.fact_journal = fact_journal;
    }
    if (options.event_enabled && !options.validate_only) {
        clickhouse_event =
            l2flow::clickhouse::EventClickHouseSink::Create(
                options.clickhouse_event, &error);
        if (clickhouse_event == nullptr) {
            std::cerr << "ClickHouse Event sink creation failed: "
                      << error << '\n';
            return 1;
        }
        event_runtime = l2flow::event::EventRuntime::Create(
            options.event, clickhouse_event.get(), &error);
        if (event_runtime == nullptr) {
            std::cerr << "Event runtime creation failed: " << error << '\n';
            return 1;
        }
    }
    if (options.kline_enabled && !options.validate_only) {
        clickhouse_kline =
            l2flow::clickhouse::KLineClickHouseSink::Create(
                options.clickhouse_kline, &error);
        if (clickhouse_kline == nullptr) {
            std::cerr << "ClickHouse KLine sink creation failed: "
                      << error << '\n';
            return 1;
        }
        kline_runtime = l2flow::kline::KLineRuntime::Create(
            options.kline, clickhouse_kline.get(), &error);
        if (kline_runtime == nullptr) {
            std::cerr << "KLine runtime creation failed: " << error << '\n';
            return 1;
        }
    }
    if (options.clickhouse_enabled && !options.validate_only) {
        clickhouse_raw = l2flow::clickhouse::RawClickHouseSink::Create(
            options.clickhouse, &error);
        if (clickhouse_raw == nullptr) {
            std::cerr << "ClickHouse raw sink creation failed: "
                      << error << '\n';
            return 1;
        }
        options.engine.raw_record_tap = clickhouse_raw.get();
    }
#endif
    {
        std::size_t tick_consumers = 0U;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (options.event_enabled) {
            ++tick_consumers;
        }
        if (options.kline_enabled) {
            ++tick_consumers;
        }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        if (options.arrow_enabled) {
            ++tick_consumers;
        }
#endif
        options.engine.tick_consumer_count =
            tick_consumers == 0U ? 1U : tick_consumers;
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
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    std::unique_ptr<l2flow::arrow_hot::ArrowHotEgress> arrow_egress;
    if (options.arrow_enabled) {
        arrow_egress = l2flow::arrow_hot::ArrowHotEgress::Create(
            options.arrow, &error);
        if (arrow_egress == nullptr) {
            std::cerr << "Arrow hot-egress start failed: " << error << '\n';
            return 1;
        }
        std::cout << "arrow_hot_run="
                  << arrow_egress->run_directory().string()
                  << " feed_epoch=" << options.arrow.feed_session_epoch
                  << " producer_instance="
                  << l2flow::arrow_hot::ProducerInstanceIdString(
                         arrow_egress->producer_instance())
                  << '\n';
    }
    const auto arrow_healthy = [&arrow_egress] {
        return arrow_egress == nullptr || arrow_egress->healthy();
    };
#else
    const auto arrow_healthy = [] { return true; };
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (clickhouse_event != nullptr) {
        if (!clickhouse_event->Start(&error)) {
            std::cerr << "ClickHouse Event sink start failed: " << error
                      << '\n';
            return 1;
        }
        std::cout << "clickhouse_event_writer_instance="
                  << l2flow::clickhouse::IdentifierString(
                         clickhouse_event->writer_instance_id())
                  << " calculation_run_id="
                  << l2flow::clickhouse::IdentifierString(
                         options.event.worker.calculation_run_id)
                  << " revision_epoch="
                  << options.event.worker.revision_epoch
                  << " logic_version="
                  << options.event.worker.logic_version
                  << " writer_lanes="
                  << options.clickhouse_event.writer_lanes << '\n';
    }
    if (clickhouse_kline != nullptr) {
        if (!clickhouse_kline->Start(&error)) {
            if (clickhouse_event != nullptr) {
                std::string stop_error;
                static_cast<void>(clickhouse_event->Stop(&stop_error));
            }
            std::cerr << "ClickHouse KLine sink start failed: " << error
                      << '\n';
            return 1;
        }
        std::cout << "clickhouse_kline_writer_instance="
                  << l2flow::clickhouse::IdentifierString(
                         clickhouse_kline->writer_instance_id())
                  << " calculation_run_id="
                  << l2flow::clickhouse::IdentifierString(
                         options.kline.worker.calculation_run_id)
                  << " revision_epoch="
                  << options.kline.worker.revision_epoch
                  << " logic_version="
                  << options.kline.worker.logic_version
                  << " writer_lanes="
                  << options.clickhouse_kline.writer_lanes
                  << " intervals_seconds=";
        for (std::size_t index = 0U;
             index < options.kline.worker.interval_seconds.size(); ++index) {
            if (index != 0U) {
                std::cout << ',';
            }
            std::cout << options.kline.worker.interval_seconds[index];
        }
        std::cout << '\n';
    }
    if (clickhouse_raw != nullptr) {
        if (!clickhouse_raw->Start(&error)) {
            if (clickhouse_kline != nullptr) {
                std::string kline_stop_error;
                static_cast<void>(
                    clickhouse_kline->Stop(&kline_stop_error));
            }
            if (clickhouse_event != nullptr) {
                std::string event_stop_error;
                static_cast<void>(
                    clickhouse_event->Stop(&event_stop_error));
            }
            std::cerr << "ClickHouse raw sink start failed: " << error
                      << '\n';
            return 1;
        }
        std::cout
            << "clickhouse_raw_writer_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_raw->writer_instance_id())
            << " source_instance="
            << l2flow::clickhouse::IdentifierString(
                   clickhouse_raw->source_instance_id())
            << " feed_epoch=" << options.clickhouse.feed_session_epoch
            << " run_started_utc_ns="
            << clickhouse_raw->run_started_utc_ns()
            << " run_started_monotonic_ns="
            << clickhouse_raw->run_started_monotonic_ns() << '\n';
        PrintClickHouseStats(clickhouse_raw->stats());
    }
    if (event_runtime != nullptr && clickhouse_event != nullptr) {
        PrintEventStats(event_runtime->stats(), clickhouse_event->stats());
    }
    if (kline_runtime != nullptr && clickhouse_kline != nullptr) {
        PrintKLineStats(kline_runtime->stats(), clickhouse_kline->stats());
    }
    const auto clickhouse_healthy = [
        &clickhouse_raw, &clickhouse_event, &event_runtime, &fact_journal,
        &clickhouse_kline, &kline_runtime] {
        return (clickhouse_raw == nullptr || clickhouse_raw->healthy()) &&
               (clickhouse_event == nullptr || clickhouse_event->healthy()) &&
               (event_runtime == nullptr || event_runtime->healthy()) &&
               (clickhouse_kline == nullptr || clickhouse_kline->healthy()) &&
               (kline_runtime == nullptr || kline_runtime->healthy()) &&
               (fact_journal == nullptr || fact_journal->healthy());
    };
#else
    const auto clickhouse_healthy = [] { return true; };
#endif
    if (!engine->Start(&error)) {
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        if (clickhouse_raw != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_raw->Stop(&stop_error));
        }
        if (event_runtime != nullptr) {
            static_cast<void>(event_runtime->DrainAll());
        }
        if (kline_runtime != nullptr) {
            static_cast<void>(kline_runtime->DrainAll());
        }
        if (clickhouse_event != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_event->Stop(&stop_error));
        }
        if (clickhouse_kline != nullptr) {
            std::string stop_error;
            static_cast<void>(clickhouse_kline->Stop(&stop_error));
        }
#endif
        std::cerr << "engine start failed: " << error << '\n';
        return 1;
    }

    std::atomic<bool> drain_after_stop{false};
    std::atomic<std::uint64_t> consumed_dispatches{0U};
    std::atomic<std::uint64_t> consumed_snapshots{0U};
    std::atomic<std::uint64_t> consumed_gaps{0U};
    std::atomic<std::uint64_t> consumed_faults{0U};
    auto owner_drained = std::make_unique<std::atomic<bool>[]>(
        options.engine.instrument_workers);
    for (std::size_t owner = 0U;
         owner < options.engine.instrument_workers; ++owner) {
        owner_drained[owner].store(false, std::memory_order_relaxed);
    }
    std::unique_ptr<LatencySampler[]> latency_samplers;
    std::vector<std::uint64_t> latency_cursors;
    std::vector<std::uint64_t> latency_clock_error_cursors;
    std::unique_ptr<LatencyAggregate> latency_aggregate;
    if (options.operation_mode == OperationMode::kTest) {
        latency_samplers = std::make_unique<LatencySampler[]>(
            options.engine.instrument_workers);
        latency_cursors.resize(options.engine.instrument_workers, 0U);
        latency_clock_error_cursors.resize(
            options.engine.instrument_workers, 0U);
        latency_aggregate = std::make_unique<LatencyAggregate>();
    }
    std::vector<std::thread> drain_threads;
    drain_threads.reserve(options.engine.instrument_workers);
    for (std::size_t owner = 0U;
         owner < options.engine.instrument_workers; ++owner) {
        drain_threads.emplace_back([&, owner] {
            TickDispatch dispatch{};
            CanonicalSnapshot snapshot{};
            ChannelGap gap{};
            ChannelFault fault{};
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
            const std::size_t event_consumer = 0U;
            const std::size_t kline_consumer =
                options.event_enabled ? 1U : 0U;
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            std::size_t arrow_consumer = 0U;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
            if (options.event_enabled) {
                ++arrow_consumer;
            }
            if (options.kline_enabled) {
                ++arrow_consumer;
            }
#endif
#endif
            const auto consume_dispatch = [&](std::size_t consumer,
                                              auto&& deliver) {
                bool consumed = false;
                for (std::size_t drained = 0U;
                     drained < kDrainBurstMessages &&
                     engine->TryPollTickDispatch(
                         consumer, owner, &dispatch);
                     ++drained) {
                    const bool projectable =
                        dispatch.kind == TickDispatchKind::kProjectOrdered ||
                        dispatch.kind == TickDispatchKind::kProjectHoleFill;
                    if (projectable && latency_samplers != nullptr) {
                        ObserveLatency(
                            dispatch.tick.common.ingress_sequence,
                            dispatch.tick.common.receive_monotonic_ns,
                            &latency_samplers[owner]);
                    }
                    deliver(dispatch);
                    consumed_dispatches.fetch_add(
                        1U, std::memory_order_relaxed);
                    consumed = true;
                }
                return consumed;
            };
            for (;;) {
                const bool stopping =
                    drain_after_stop.load(std::memory_order_acquire);
                bool progress = false;
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                const bool event_poll_blocked =
                    event_runtime != nullptr &&
                    !event_runtime->CanPollDispatch(owner);
                if (event_runtime != nullptr && !event_poll_blocked) {
                    progress = consume_dispatch(
                                   event_consumer,
                                   [&](const TickDispatch& value) {
                                       static_cast<void>(
                                           event_runtime->AppendDispatch(
                                               owner, value));
                                   }) ||
                        progress;
                }
                if (kline_runtime != nullptr) {
                    progress = consume_dispatch(
                                   kline_consumer,
                                   [&](const TickDispatch& value) {
                                       static_cast<void>(
                                           kline_runtime->AppendDispatch(
                                               owner, value));
                                   }) ||
                        progress;
                }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                if (arrow_egress != nullptr) {
                    progress = consume_dispatch(
                                   arrow_consumer,
                                   [&](const TickDispatch& value) {
                                       const bool projectable =
                                           value.kind ==
                                               TickDispatchKind::
                                                   kProjectOrdered ||
                                           value.kind ==
                                               TickDispatchKind::
                                                   kProjectHoleFill;
                                       if (projectable) {
                                           static_cast<void>(
                                               arrow_egress->
                                                   AppendTickDispatch(
                                                       owner, value));
                                       }
                                   }) ||
                        progress;
                }
#endif
#if !defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW) && \
    !defined(L2FLOW_CH_HAS_ARROW_RING)
                progress = consume_dispatch(
                               0U, [&](const TickDispatch&) {}) ||
                    progress;
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                if (event_runtime == nullptr && kline_runtime == nullptr
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                    && arrow_egress == nullptr
#endif
                ) {
                    progress = consume_dispatch(
                                   0U, [&](const TickDispatch&) {}) ||
                        progress;
                }
#elif defined(L2FLOW_CH_HAS_ARROW_RING)
                if (arrow_egress == nullptr) {
                    progress = consume_dispatch(
                                   0U, [&](const TickDispatch&) {}) ||
                        progress;
                }
#endif
                for (std::size_t drained = 0U;
                     drained < kDrainBurstMessages &&
                     engine->TryPollSnapshot(owner, &snapshot);
                     ++drained) {
                    if (latency_samplers != nullptr) {
                        ObserveLatency(
                            snapshot.common.ingress_sequence,
                            snapshot.common.receive_monotonic_ns,
                            &latency_samplers[owner]);
                    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                    if (arrow_egress != nullptr) {
                        static_cast<void>(
                            arrow_egress->AppendSnapshot(owner, snapshot));
                    }
#endif
                    consumed_snapshots.fetch_add(
                        1U, std::memory_order_relaxed);
                    progress = true;
                }
                if (owner == 0U) {
                    for (std::size_t drained = 0U;
                         drained < kDrainBurstMessages &&
                         engine->TryPollGap(&gap);
                         ++drained) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                        if (arrow_egress != nullptr) {
                            static_cast<void>(arrow_egress->AppendGap(gap));
                        }
#endif
                        consumed_gaps.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                    for (std::size_t drained = 0U;
                         drained < kDrainBurstMessages &&
                         engine->TryPollChannelFault(&fault);
                         ++drained) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                        if (arrow_egress != nullptr) {
                            static_cast<void>(
                                arrow_egress->AppendFault(fault));
                        }
#endif
                        consumed_faults.fetch_add(
                            1U, std::memory_order_relaxed);
                        progress = true;
                    }
                }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
                if (arrow_egress != nullptr) {
                    arrow_egress->FlushDue(
                        owner, l2flow::ingest::MonotonicNowNs());
                }
#endif
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                if (event_runtime != nullptr) {
                    static_cast<void>(event_runtime->FlushDue(
                        owner, l2flow::ingest::MonotonicNowNs()));
                }
                if (kline_runtime != nullptr) {
                    static_cast<void>(kline_runtime->FlushDue(
                        owner, l2flow::ingest::MonotonicNowNs()));
                }
#endif
                if (!progress) {
                    if (stopping) {
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                        if (event_runtime != nullptr &&
                            event_runtime->healthy() &&
                            !event_runtime->CanPollDispatch(owner)) {
                            // A fence cleared by this iteration's FlushDue
                            // still requires one producer-quiesced poll pass.
                            std::this_thread::yield();
                            continue;
                        }
#endif
                        owner_drained[owner].store(
                            true, std::memory_order_release);
                        break;
                    }
                    std::this_thread::yield();
                }
            }
        });
    }

    const auto stop_engine_producers = [&] { engine->Stop(); };
    const auto finish_owner_drain = [&] {
        drain_after_stop.store(true, std::memory_order_release);
        for (;;) {
            bool all_drained = true;
            for (std::size_t owner = 0U;
                 owner < options.engine.instrument_workers; ++owner) {
                all_drained =
                    owner_drained[owner].load(std::memory_order_acquire) &&
                    all_drained;
            }
            if (all_drained) {
                break;
            }
            std::this_thread::yield();
        }
        for (std::thread& thread : drain_threads) {
            thread.join();
        }
        // Gap delivery is a coalescing state mailbox, so event-count equality
        // is not meaningful. Once its producers and normal consumer stop,
        // collect the last dirty snapshots explicitly.
        ChannelGap final_gap{};
        while (engine->TryPollGap(&final_gap)) {
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            if (arrow_egress != nullptr) {
                static_cast<void>(arrow_egress->AppendGap(final_gap));
            }
#endif
            consumed_gaps.fetch_add(1U, std::memory_order_relaxed);
        }
    };
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    bool clickhouse_stop_ok = true;
    std::string clickhouse_stop_error;
    bool event_flush_ok = true;
    bool event_drain_ok = true;
    bool event_stop_ok = true;
    std::string event_stop_error;
    bool kline_flush_ok = true;
    bool kline_drain_ok = true;
    bool kline_stop_ok = true;
    std::string kline_stop_error;
    bool fact_journal_flush_ok = true;
    const auto flush_derived_runtimes = [&] {
        if (event_runtime != nullptr) {
            event_flush_ok = event_runtime->FlushAll();
        }
        if (kline_runtime != nullptr) {
            kline_flush_ok = kline_runtime->FlushAll();
        }
    };
    const auto stop_clickhouse_raw = [&] {
        if (clickhouse_raw != nullptr) {
            clickhouse_stop_ok =
                clickhouse_raw->Stop(&clickhouse_stop_error);
            PrintClickHouseStats(clickhouse_raw->stats());
        }
    };
    const auto stop_clickhouse_derived = [&] {
        if (event_runtime != nullptr) {
            event_drain_ok = event_runtime->DrainAll();
        }
        if (kline_runtime != nullptr) {
            kline_drain_ok = kline_runtime->DrainAll();
        }
        if (fact_journal != nullptr) {
            fact_journal_flush_ok = fact_journal->Flush();
        }
        if (clickhouse_event != nullptr) {
            event_stop_ok =
                clickhouse_event->Stop(&event_stop_error);
        }
        if (clickhouse_kline != nullptr) {
            kline_stop_ok =
                clickhouse_kline->Stop(&kline_stop_error);
        }
        if (event_runtime != nullptr && clickhouse_event != nullptr) {
            PrintEventStats(
                event_runtime->stats(), clickhouse_event->stats());
        }
        if (kline_runtime != nullptr && clickhouse_kline != nullptr) {
            PrintKLineStats(
                kline_runtime->stats(), clickhouse_kline->stats());
        }
    };
#endif

    MdlMessageHandler handler(engine.get(), stream_mask);
    const auto print_mdl_readiness_summary = [&handler] {
        std::cout << "mdl_feed_ready_monotonic_ns="
                  << handler.feed_ready_monotonic_ns()
                  << " pre_ready_messages_discarded="
                  << handler.pre_ready_messages_discarded() << '\n'
                  << std::flush;
    };
    std::unique_ptr<PhysicalSdkSession> sdk = PhysicalSdkSession::Connect(
        options.sdk, &handler, &error);
    if (sdk == nullptr) {
        print_mdl_readiness_summary();
        stop_engine_producers();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        // Raw Stop joins every writer and therefore closes the ACK producer
        // side. Owner threads remain alive until then so ACK-first joins cannot
        // overflow merely because shutdown has begun.
        stop_clickhouse_raw();
#endif
        finish_owner_drain();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        flush_derived_runtimes();
        stop_clickhouse_derived();
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
        if (arrow_egress != nullptr) {
            PublishMdlConnectionBoundary(arrow_egress.get(), handler);
            arrow_egress->FlushAll();
            arrow_egress->Seal(l2flow::ingest::MonotonicNowNs());
        }
#endif
        std::cerr << "SDK connect failed: " << error << '\n';
        return 1;
    }
    print_mdl_readiness_summary();
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr && handler.feed_ready()) {
        static_cast<void>(arrow_egress->MarkFeedConnected(
            handler.feed_ready_monotonic_ns()));
    }
#endif

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    l2flow::clickhouse::EventClickHouseStats previous_event_sink_stats{};
    std::chrono::steady_clock::time_point previous_event_sink_report =
        std::chrono::steady_clock::now();
    if (clickhouse_event != nullptr) {
        previous_event_sink_stats = clickhouse_event->stats();
    }
#endif
    if (options.operation_mode == OperationMode::kLive) {
        while (g_stop_requested == 0 && engine->healthy() &&
               !handler.failed() &&
               handler.connection_boundary_reason() ==
                   l2flow::ingest::MdlConnectionBoundaryReason::kNone &&
               arrow_healthy() && clickhouse_healthy()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            PrintStats(engine->stats());
            PrintFreshness(
                *engine,
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                options.event_enabled, options.kline_enabled,
                event_runtime == nullptr || event_runtime->healthy(),
                kline_runtime == nullptr || kline_runtime->healthy(),
                event_runtime != nullptr &&
                    event_runtime->stats().workers.pending_raw_commits !=
                        0U,
                kline_runtime != nullptr &&
                    kline_runtime->stats().workers.pending_raw_commits !=
                        0U,
#else
                false, false, true, true, false, false,
#endif
                !engine->healthy());
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
            if (clickhouse_raw != nullptr) {
                PrintClickHouseStats(clickhouse_raw->stats());
            }
            if (event_runtime != nullptr && clickhouse_event != nullptr) {
                const auto now = std::chrono::steady_clock::now();
                const auto current = clickhouse_event->stats();
                const double seconds = std::max(
                    std::chrono::duration<double>(
                        now - previous_event_sink_report).count(),
                    1e-9);
                const std::uint64_t groups =
                    current.submission_groups_queued >=
                            previous_event_sink_stats.submission_groups_queued
                    ? current.submission_groups_queued -
                          previous_event_sink_stats.submission_groups_queued
                    : 0U;
                PrintEventStats(event_runtime->stats(), current,
                                static_cast<double>(groups) / seconds);
                previous_event_sink_stats = current;
                previous_event_sink_report = now;
            }
            if (kline_runtime != nullptr && clickhouse_kline != nullptr) {
                PrintKLineStats(
                    kline_runtime->stats(), clickhouse_kline->stats());
            }
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
            if (arrow_egress != nullptr) {
                PrintArrowStats(arrow_egress->stats());
            }
#endif
        }
    } else {
        using SteadyClock = std::chrono::steady_clock;
        const SteadyClock::time_point monitor_started = SteadyClock::now();
        SteadyClock::time_point previous_report_time = monitor_started;
        SteadyClock::time_point next_report_time =
            monitor_started + std::chrono::seconds(1);
        const SteadyClock::time_point run_deadline = options.run_seconds == 0U
            ? SteadyClock::time_point::max()
            : monitor_started + std::chrono::seconds(options.run_seconds);
        EngineStats previous_stats = engine->stats();
        while (g_stop_requested == 0 && engine->healthy() &&
               !handler.failed() &&
               handler.connection_boundary_reason() ==
                   l2flow::ingest::MdlConnectionBoundaryReason::kNone &&
               arrow_healthy() && clickhouse_healthy()) {
            std::this_thread::sleep_until(
                std::min(next_report_time, run_deadline));
            const SteadyClock::time_point now = SteadyClock::now();
            if (now >= next_report_time) {
                const EngineStats current_stats = engine->stats();
                LatencyWindow latency_window = CollectLatency(
                    latency_samplers.get(),
                    options.engine.instrument_workers,
                    &latency_cursors, &latency_clock_error_cursors);
                latency_aggregate->Add(latency_window);
                const LatencySummary latency_summary =
                    SummarizeLatency(&latency_window.samples_ns);
                const double interval_seconds =
                    std::chrono::duration<double>(
                        now - previous_report_time).count();
                PrintMonitor(current_stats, previous_stats, interval_seconds,
                             latency_summary, latency_window,
                             *latency_aggregate);
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
                if (clickhouse_raw != nullptr) {
                    PrintClickHouseStats(clickhouse_raw->stats());
                }
                if (event_runtime != nullptr && clickhouse_event != nullptr) {
                    const auto current = clickhouse_event->stats();
                    const std::uint64_t groups =
                        current.submission_groups_queued >=
                                previous_event_sink_stats
                                    .submission_groups_queued
                        ? current.submission_groups_queued -
                              previous_event_sink_stats
                                  .submission_groups_queued
                        : 0U;
                    PrintEventStats(
                        event_runtime->stats(), current,
                        static_cast<double>(groups) /
                            std::max(interval_seconds, 1e-9));
                    previous_event_sink_stats = current;
                    previous_event_sink_report = now;
                }
                if (kline_runtime != nullptr && clickhouse_kline != nullptr) {
                    PrintKLineStats(
                        kline_runtime->stats(), clickhouse_kline->stats());
                }
#endif
                previous_stats = current_stats;
                previous_report_time = now;
                next_report_time = now + std::chrono::seconds(1);
            }
            if (now >= run_deadline) {
                break;
            }
        }
    }

    // Shutdown order is contractual: quiesce SDK callbacks and decoder
    // producers, keep owner service alive while raw writers deliver their last
    // ACKs, then close the owner consumers and derived sinks.
    sdk->Shutdown();
    stop_engine_producers();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    stop_clickhouse_raw();
#endif
    finish_owner_drain();
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    flush_derived_runtimes();
    stop_clickhouse_derived();
#endif
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr) {
        PublishMdlConnectionBoundary(arrow_egress.get(), handler);
        arrow_egress->FlushAll();
        arrow_egress->Seal(l2flow::ingest::MonotonicNowNs());
        PrintArrowStats(arrow_egress->stats());
    }
#endif
    const EngineStats final_stats = engine->stats();
    PrintStats(final_stats);
    PrintFreshness(
        *engine,
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
        options.event_enabled, options.kline_enabled,
        event_runtime == nullptr || event_runtime->healthy(),
        kline_runtime == nullptr || kline_runtime->healthy(),
        event_runtime != nullptr &&
            event_runtime->stats().workers.pending_raw_commits != 0U,
        kline_runtime != nullptr &&
            kline_runtime->stats().workers.pending_raw_commits != 0U,
#else
        false, false, true, true, false, false,
#endif
        !engine->healthy());
    if (options.operation_mode == OperationMode::kTest) {
        LatencyWindow final_latency_window = CollectLatency(
            latency_samplers.get(), options.engine.instrument_workers,
            &latency_cursors, &latency_clock_error_cursors);
        latency_aggregate->Add(final_latency_window);
        PrintFinalLatency(*latency_aggregate);
    }
#if defined(L2FLOW_CH_HAS_CLICKHOUSE_RAW)
    if (event_runtime != nullptr &&
        (!event_flush_ok || !event_drain_ok || !event_runtime->healthy())) {
        std::cerr << "fatal Event runtime error: "
                  << event_runtime->fatal_error() << '\n';
        return 1;
    }
    if (clickhouse_event != nullptr &&
        (!event_stop_ok || !clickhouse_event->healthy())) {
        std::cerr << "fatal ClickHouse Event sink error: "
                  << (event_stop_error.empty()
                          ? clickhouse_event->fatal_error()
                          : event_stop_error)
                  << '\n';
        return 1;
    }
    if (kline_runtime != nullptr &&
        (!kline_flush_ok || !kline_drain_ok || !kline_runtime->healthy())) {
        std::cerr << "fatal KLine runtime error: "
                  << kline_runtime->fatal_error() << '\n';
        return 1;
    }
    if (clickhouse_kline != nullptr &&
        (!kline_stop_ok || !clickhouse_kline->healthy())) {
        std::cerr << "fatal ClickHouse KLine sink error: "
                  << (kline_stop_error.empty()
                          ? clickhouse_kline->fatal_error()
                          : kline_stop_error)
                  << '\n';
        return 1;
    }
    if (clickhouse_raw != nullptr &&
        (!clickhouse_stop_ok || !clickhouse_raw->healthy())) {
        std::cerr << "fatal ClickHouse raw sink error: "
                  << (clickhouse_stop_error.empty()
                          ? clickhouse_raw->fatal_error()
                          : clickhouse_stop_error)
                  << '\n';
        return 1;
    }
    if (fact_journal != nullptr &&
        (!fact_journal_flush_ok || !fact_journal->healthy())) {
        std::cerr << "fatal FactJournal error: "
                  << fact_journal->fatal_error() << '\n';
        return 1;
    }
#endif
    if (!engine->healthy()) {
        std::cerr << "fatal ingest error: " << engine->fatal_error() << '\n';
        return 1;
    }
    if (handler.failed()) {
        std::cerr << "fatal SDK callback adapter error\n";
        return 1;
    }
    if (handler.connection_boundary_reason() !=
        l2flow::ingest::MdlConnectionBoundaryReason::kNone) {
        const std::string boundary_detail =
            handler.connection_boundary_detail();
        std::cerr
            << "MDL connection boundary observed; post-reconnect data was "
               "rejected. Restart mdl_ingestd with a new feed session epoch";
        if (!boundary_detail.empty()) {
            std::cerr << ": " << boundary_detail;
        }
        std::cerr << '\n';
        return 1;
    }
#if defined(L2FLOW_CH_HAS_ARROW_RING)
    if (arrow_egress != nullptr && !arrow_egress->healthy()) {
        std::cerr << "fatal Arrow hot-egress error: "
                  << arrow_egress->fatal_error() << '\n';
        return 1;
    }
#endif
    return 0;
}
