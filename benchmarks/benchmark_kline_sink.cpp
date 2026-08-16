#include "l2flow/clickhouse/kline_sink.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#if !defined(__linux__)

int main() {
    std::cout << "status=SKIP reason=loopback_transport_requires_linux\n";
    return 0;
}

#else

namespace {

using l2flow::clickhouse::KLineClickHouseConfig;
using l2flow::clickhouse::KLineClickHouseSink;
using l2flow::clickhouse::KLineClickHouseStats;
using l2flow::ingest::Market;
using l2flow::ingest::MonotonicNowNs;
using l2flow::kline::Identifier128;
using l2flow::kline::KLineRevision;
using l2flow::kline::KLineRevisionBatch;
using l2flow::kline::RevisionOperation;
using l2flow::kline::RevisionReason;

constexpr std::size_t kRevisionRowBytes = 313U;
constexpr std::size_t kMarkerRowBytes = 140U;

struct Options final {
    std::uint64_t target_rows_per_second = 1'000'000U;
    std::uint32_t seconds = 5U;
    std::size_t owners = 32U;
    std::size_t writer_lanes = 4U;
    std::size_t logical_batch_rows = 64U;
    std::size_t producer_burst_batches = 32U;
    std::size_t insert_request_rows = 2'048U;
    std::size_t insert_request_bytes = 1U * 1'024U * 1'024U;
    std::size_t physical_group_batches = 256U;
    std::uint64_t physical_group_delay_ns = 1'000'000U;
    std::uint32_t response_delay_us = 0U;
};

struct QueueSample final {
    std::uint64_t monotonic_ns = 0U;
    std::uint64_t batches = 0U;
    std::uint64_t rows = 0U;
};

class CompletionSink final : public l2flow::outbox::ConsumerCompletionSink {
public:
    [[nodiscard]] bool Complete(
        l2flow::outbox::ConsumerKind consumer,
        std::span<const l2flow::outbox::WalPosition> positions)
        noexcept override {
        if (consumer != l2flow::outbox::ConsumerKind::kKLine) {
            return false;
        }
        completed.fetch_add(positions.size(), std::memory_order_relaxed);
        return true;
    }

    std::atomic<std::uint64_t> completed{0U};
};

template <typename Integer>
[[nodiscard]] bool ParseInteger(std::string_view text,
                                Integer* output) noexcept {
    if (text.empty() || output == nullptr) {
        return false;
    }
    Integer value{};
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage: benchmark_kline_sink [options]\n"
        << "  --rate N                    revision rows/s; default 1000000\n"
        << "  --seconds N                 measured seconds; default 5\n"
        << "  --owners N                  logical KLine owners; default 32\n"
        << "  --writer-lanes N            one of 1,2,4,8,16,32; default 4\n"
        << "  --logical-batch-rows N      rows/recovery commit; default 64\n"
        << "  --producer-burst-batches N  batches prepared per pacing cut; default 32\n"
        << "  --insert-request-rows N     revision rows/HTTP bound; default 2048\n"
        << "  --insert-request-bytes N    HTTP body bound; default 1048576\n"
        << "  --physical-group-batches N  commits/physical group; default 256\n"
        << "  --physical-group-delay-ns N grouping delay; default 1000000\n"
        << "  --response-delay-us N       loopback INSERT delay; default 0\n";
}

[[nodiscard]] bool ParseOptions(int argc,
                                char** argv,
                                Options* output,
                                std::string* error) {
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
            PrintUsage();
            std::exit(0);
        } else if (argument == "--rate") {
            if (!ParseInteger(next(argument),
                              &parsed.target_rows_per_second)) {
                *error = "invalid --rate";
                return false;
            }
        } else if (argument == "--seconds") {
            if (!ParseInteger(next(argument), &parsed.seconds)) {
                *error = "invalid --seconds";
                return false;
            }
        } else if (argument == "--owners") {
            if (!ParseInteger(next(argument), &parsed.owners)) {
                *error = "invalid --owners";
                return false;
            }
        } else if (argument == "--writer-lanes") {
            if (!ParseInteger(next(argument), &parsed.writer_lanes)) {
                *error = "invalid --writer-lanes";
                return false;
            }
        } else if (argument == "--logical-batch-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.logical_batch_rows)) {
                *error = "invalid --logical-batch-rows";
                return false;
            }
        } else if (argument == "--producer-burst-batches") {
            if (!ParseInteger(next(argument),
                              &parsed.producer_burst_batches)) {
                *error = "invalid --producer-burst-batches";
                return false;
            }
        } else if (argument == "--insert-request-rows") {
            if (!ParseInteger(next(argument),
                              &parsed.insert_request_rows)) {
                *error = "invalid --insert-request-rows";
                return false;
            }
        } else if (argument == "--insert-request-bytes") {
            if (!ParseInteger(next(argument),
                              &parsed.insert_request_bytes)) {
                *error = "invalid --insert-request-bytes";
                return false;
            }
        } else if (argument == "--physical-group-batches") {
            if (!ParseInteger(next(argument),
                              &parsed.physical_group_batches)) {
                *error = "invalid --physical-group-batches";
                return false;
            }
        } else if (argument == "--physical-group-delay-ns") {
            if (!ParseInteger(next(argument),
                              &parsed.physical_group_delay_ns)) {
                *error = "invalid --physical-group-delay-ns";
                return false;
            }
        } else if (argument == "--response-delay-us") {
            if (!ParseInteger(next(argument), &parsed.response_delay_us)) {
                *error = "invalid --response-delay-us";
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

    const bool valid_lanes = parsed.writer_lanes == 1U ||
        parsed.writer_lanes == 2U || parsed.writer_lanes == 4U ||
        parsed.writer_lanes == 8U || parsed.writer_lanes == 16U ||
        parsed.writer_lanes == 32U;
    if (parsed.target_rows_per_second == 0U ||
        parsed.target_rows_per_second > 10'000'000U ||
        parsed.seconds == 0U || parsed.seconds > 600U ||
        parsed.target_rows_per_second >
            std::numeric_limits<std::uint64_t>::max() / parsed.seconds ||
        parsed.owners == 0U ||
        parsed.owners >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        !valid_lanes || parsed.logical_batch_rows == 0U ||
        parsed.producer_burst_batches == 0U ||
        parsed.insert_request_rows == 0U ||
        parsed.insert_request_bytes < kRevisionRowBytes ||
        parsed.physical_group_batches == 0U ||
        parsed.physical_group_delay_ns == 0U ||
        parsed.physical_group_delay_ns > UINT64_C(1'000'000'000) ||
        parsed.response_delay_us > 1'000'000U ||
        parsed.logical_batch_rows >
            std::numeric_limits<std::size_t>::max() /
                parsed.producer_burst_batches) {
        *error = "invalid KLine sink benchmark bounds";
        return false;
    }
    const std::size_t burst_rows =
        parsed.logical_batch_rows * parsed.producer_burst_batches;
    constexpr std::size_t kMaximumQueueRows =
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) - 1U;
    if (burst_rows == 0U ||
        burst_rows > kMaximumQueueRows / 4U) {
        *error = "KLine sink benchmark burst is too large";
        return false;
    }
    *output = parsed;
    error->clear();
    return true;
}

[[nodiscard]] std::uint64_t ScheduledOffsetNs(
    std::uint64_t ordinal,
    std::uint64_t rate) noexcept {
    const std::uint64_t seconds = ordinal / rate;
    const std::uint64_t remainder = ordinal % rate;
    return seconds * UINT64_C(1'000'000'000) +
        remainder * UINT64_C(1'000'000'000) / rate;
}

[[nodiscard]] std::uint64_t WaitUntil(std::uint64_t deadline_ns) noexcept {
    for (;;) {
        const std::uint64_t now = MonotonicNowNs();
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
    long double fraction) noexcept {
    if (sorted.empty()) {
        return 0U;
    }
    const long double position = fraction *
        static_cast<long double>(sorted.size() - 1U);
    return sorted[static_cast<std::size_t>(std::ceil(position))];
}

enum class RequestKind : std::uint8_t {
    kSchema = 0U,
    kRevision,
    kMarker,
};

struct WorkerSamples final {
    std::vector<std::uint64_t> revision_latency_ns;
    std::vector<std::uint64_t> marker_latency_ns;
    std::uint64_t revision_body_bytes = 0U;
    std::uint64_t marker_body_bytes = 0U;
    std::uint64_t revision_rows = 0U;
    std::uint64_t marker_rows = 0U;
    std::uint64_t schema_requests = 0U;
};

class LoopbackHttpServer final {
public:
    LoopbackHttpServer(std::size_t workers, std::uint32_t response_delay_us)
        : worker_samples_(workers), response_delay_us_(response_delay_us) {}

    ~LoopbackHttpServer() { Stop(); }
    LoopbackHttpServer(const LoopbackHttpServer&) = delete;
    LoopbackHttpServer& operator=(const LoopbackHttpServer&) = delete;

    [[nodiscard]] bool Start(std::string* error) {
        listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (listener_ < 0) {
            *error = "socket failed: " +
                std::error_code(errno, std::generic_category()).message();
            return false;
        }
        const int enabled = 1;
        if (::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR,
                         &enabled, sizeof(enabled)) != 0) {
            *error = "setsockopt failed";
            CloseListener();
            return false;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0U;
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::listen(listener_, 1'024) != 0) {
            *error = "bind/listen failed: " +
                std::error_code(errno, std::generic_category()).message();
            CloseListener();
            return false;
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                          &address_size) != 0) {
            *error = "getsockname failed";
            CloseListener();
            return false;
        }
        port_ = ntohs(address.sin_port);
        try {
            threads_.reserve(worker_samples_.size());
            for (std::size_t worker = 0U;
                 worker < worker_samples_.size(); ++worker) {
                threads_.emplace_back([this, worker] { Run(worker); });
            }
        } catch (...) {
            *error = "loopback HTTP worker creation failed";
            Stop();
            return false;
        }
        error->clear();
        return true;
    }

    void Stop() noexcept {
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        CloseListener();
        for (std::thread& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string fatal_error() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] std::vector<std::uint64_t> RevisionLatencies() const {
        std::vector<std::uint64_t> values;
        for (const WorkerSamples& worker : worker_samples_) {
            values.insert(values.end(), worker.revision_latency_ns.begin(),
                          worker.revision_latency_ns.end());
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    [[nodiscard]] std::vector<std::uint64_t> MarkerLatencies() const {
        std::vector<std::uint64_t> values;
        for (const WorkerSamples& worker : worker_samples_) {
            values.insert(values.end(), worker.marker_latency_ns.begin(),
                          worker.marker_latency_ns.end());
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    [[nodiscard]] std::uint64_t revision_body_bytes() const noexcept {
        return Sum(&WorkerSamples::revision_body_bytes);
    }

    [[nodiscard]] std::uint64_t marker_body_bytes() const noexcept {
        return Sum(&WorkerSamples::marker_body_bytes);
    }

    [[nodiscard]] std::uint64_t revision_rows() const noexcept {
        return Sum(&WorkerSamples::revision_rows);
    }

    [[nodiscard]] std::uint64_t marker_rows() const noexcept {
        return Sum(&WorkerSamples::marker_rows);
    }

    [[nodiscard]] std::uint64_t schema_requests() const noexcept {
        return Sum(&WorkerSamples::schema_requests);
    }

private:
    struct Request final {
        RequestKind kind = RequestKind::kSchema;
        std::size_t body_bytes = 0U;
        std::uint64_t first_byte_ns = 0U;
    };

    [[nodiscard]] std::uint64_t Sum(
        std::uint64_t WorkerSamples::*member) const noexcept {
        std::uint64_t result = 0U;
        for (const WorkerSamples& worker : worker_samples_) {
            result += worker.*member;
        }
        return result;
    }

    void CloseListener() noexcept {
        const int listener = std::exchange(listener_, -1);
        if (listener >= 0) {
            static_cast<void>(::shutdown(listener, SHUT_RDWR));
            static_cast<void>(::close(listener));
        }
    }

    void SetFatal(std::string message) noexcept {
        bool expected = true;
        if (!healthy_.compare_exchange_strong(
                expected, false, std::memory_order_acq_rel)) {
            return;
        }
        try {
            std::lock_guard<std::mutex> lock(error_mutex_);
            fatal_error_ = std::move(message);
        } catch (...) {
        }
        CloseListener();
    }

    [[nodiscard]] bool ReceiveRequest(int connection,
                                      std::string* buffered,
                                      Request* output) noexcept {
        std::array<char, 64U * 1'024U> receive_buffer{};
        std::uint64_t first_byte_ns = 0U;
        for (;;) {
            const std::size_t header_end = buffered->find("\r\n\r\n");
            if (header_end != std::string::npos) {
                const std::size_t first_space = buffered->find(' ');
                const std::size_t second_space = buffered->find(
                    ' ', first_space == std::string::npos
                             ? 0U
                             : first_space + 1U);
                constexpr std::string_view kLength =
                    "\r\nContent-Length:";
                const std::size_t length_position = buffered->find(kLength);
                if (first_space == std::string::npos ||
                    second_space == std::string::npos ||
                    length_position == std::string::npos ||
                    second_space <= first_space + 1U) {
                    return false;
                }
                std::size_t length_begin = length_position + kLength.size();
                while (length_begin < header_end &&
                       (*buffered)[length_begin] == ' ') {
                    ++length_begin;
                }
                const std::size_t length_end = buffered->find(
                    "\r\n", length_begin);
                std::size_t body_bytes = 0U;
                if (length_end == std::string::npos ||
                    length_end > header_end) {
                    return false;
                }
                const auto parsed = std::from_chars(
                    buffered->data() + length_begin,
                    buffered->data() + length_end, body_bytes);
                if (parsed.ec != std::errc{} ||
                    parsed.ptr != buffered->data() + length_end ||
                    body_bytes > 64U * 1'024U * 1'024U) {
                    return false;
                }
                const std::size_t body_begin = header_end + 4U;
                if (body_bytes >
                    std::numeric_limits<std::size_t>::max() - body_begin) {
                    return false;
                }
                const std::size_t request_end = body_begin + body_bytes;
                if (buffered->size() >= request_end) {
                    const std::string_view target(
                        buffered->data() + first_space + 1U,
                        second_space - first_space - 1U);
                    RequestKind kind = RequestKind::kSchema;
                    if (target.find("INSERT") != std::string_view::npos &&
                        target.find("kline_revision_log") !=
                            std::string_view::npos) {
                        kind = RequestKind::kRevision;
                    } else if (
                        target.find("INSERT") != std::string_view::npos &&
                        target.find("kline_recovery_run") !=
                            std::string_view::npos) {
                        kind = RequestKind::kMarker;
                    }
                    output->kind = kind;
                    output->body_bytes = body_bytes;
                    output->first_byte_ns = first_byte_ns == 0U
                        ? MonotonicNowNs()
                        : first_byte_ns;
                    buffered->erase(0U, request_end);
                    return true;
                }
            } else if (buffered->size() > 64U * 1'024U) {
                return false;
            }

            const ssize_t received = ::recv(
                connection, receive_buffer.data(), receive_buffer.size(), 0);
            if (received <= 0) {
                return false;
            }
            if (first_byte_ns == 0U) {
                first_byte_ns = MonotonicNowNs();
            }
            try {
                buffered->append(
                    receive_buffer.data(), static_cast<std::size_t>(received));
            } catch (...) {
                return false;
            }
        }
    }

    [[nodiscard]] static bool SendSuccess(int connection) noexcept {
        constexpr std::string_view response =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
            "Connection: keep-alive\r\n\r\n";
        std::size_t sent = 0U;
        while (sent < response.size()) {
            const ssize_t count = ::send(
                connection, response.data() + sent,
                response.size() - sent, MSG_NOSIGNAL);
            if (count <= 0) {
                return false;
            }
            sent += static_cast<std::size_t>(count);
        }
        return true;
    }

    void HandleConnection(int connection, WorkerSamples* samples) noexcept {
        std::string buffered;
        try {
            buffered.reserve(2U * 1'024U * 1'024U);
        } catch (...) {
            SetFatal("loopback HTTP receive allocation failed");
            return;
        }
        while (!stopping_.load(std::memory_order_acquire) && healthy()) {
            Request request{};
            if (!ReceiveRequest(connection, &buffered, &request)) {
                return;
            }
            if (request.kind == RequestKind::kRevision &&
                (request.body_bytes == 0U ||
                 request.body_bytes % kRevisionRowBytes != 0U)) {
                SetFatal("KLine revision body violates 313-byte row invariant");
                return;
            }
            if (request.kind == RequestKind::kMarker &&
                (request.body_bytes == 0U ||
                 request.body_bytes % kMarkerRowBytes != 0U)) {
                SetFatal("KLine marker body violates 120-byte row invariant");
                return;
            }
            if (request.kind != RequestKind::kSchema &&
                response_delay_us_ != 0U) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(response_delay_us_));
            }
            if (!SendSuccess(connection)) {
                return;
            }
            const std::uint64_t completed_ns = MonotonicNowNs();
            const std::uint64_t latency_ns =
                completed_ns >= request.first_byte_ns
                ? completed_ns - request.first_byte_ns
                : 0U;
            if (request.kind == RequestKind::kRevision) {
                samples->revision_latency_ns.push_back(latency_ns);
                samples->revision_body_bytes += request.body_bytes;
                samples->revision_rows +=
                    request.body_bytes / kRevisionRowBytes;
            } else if (request.kind == RequestKind::kMarker) {
                samples->marker_latency_ns.push_back(latency_ns);
                samples->marker_body_bytes += request.body_bytes;
                samples->marker_rows += request.body_bytes / kMarkerRowBytes;
            } else {
                ++samples->schema_requests;
            }
        }
    }

    void Run(std::size_t worker) noexcept {
        while (!stopping_.load(std::memory_order_acquire) && healthy()) {
            const int connection = ::accept4(
                listener_, nullptr, nullptr, SOCK_CLOEXEC);
            if (connection < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (!stopping_.load(std::memory_order_acquire) && healthy()) {
                    SetFatal("loopback HTTP accept failed");
                }
                return;
            }
            HandleConnection(connection, &worker_samples_[worker]);
            static_cast<void>(::shutdown(connection, SHUT_RDWR));
            static_cast<void>(::close(connection));
        }
    }

    std::vector<WorkerSamples> worker_samples_;
    std::uint32_t response_delay_us_ = 0U;
    std::vector<std::thread> threads_;
    int listener_ = -1;
    std::uint16_t port_ = 0U;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> healthy_{true};
    mutable std::mutex error_mutex_;
    std::string fatal_error_;
};

[[nodiscard]] Identifier128 Identifier(std::uint64_t value,
                                       std::uint8_t tag) noexcept {
    Identifier128 result{};
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        result.bytes[index] = static_cast<std::byte>(
            value >> static_cast<unsigned int>(index * 8U));
    }
    result.bytes[8U] = static_cast<std::byte>(tag);
    if (value == 0U && tag == 0U) {
        result.bytes.back() = std::byte{1U};
    }
    return result;
}

[[nodiscard]] KLineRevision Revision(
    std::uint64_t row_sequence,
    std::uint32_t owner,
    Identifier128 calculation_run,
    Identifier128 recovery_run) noexcept {
    KLineRevision revision{};
    const std::uint64_t bucket_start =
        row_sequence % UINT64_C(80'000) *
        l2flow::kline::kNanosecondsPerSecond;
    const std::int64_t price = 10'000'000 +
        static_cast<std::int64_t>(row_sequence % 1'000U);
    revision.key.trade_date = 20260811U;
    revision.key.market = Market::kShenzhen;
    revision.key.instrument_id = owner + 1U;
    revision.key.interval_seconds = 1U;
    revision.key.bucket_start_ns_from_midnight = bucket_start;
    revision.version = row_sequence;
    revision.revision_id = Identifier(row_sequence, 1U);
    revision.recovery_run_id = recovery_run;
    revision.operation = RevisionOperation::kInsert;
    revision.reason = RevisionReason::kLiveProjection;
    revision.calculation_run_id = calculation_run;
    revision.logic_version = 1U;
    revision.input_set_hash = Identifier(row_sequence, 2U);
    revision.payload_hash = Identifier(row_sequence, 3U);
    revision.payload.bucket_end_ns_from_midnight =
        bucket_start + l2flow::kline::kNanosecondsPerSecond;
    revision.payload.open_price_p6 = price;
    revision.payload.high_price_p6 = price;
    revision.payload.low_price_p6 = price;
    revision.payload.close_price_p6 = price;
    revision.payload.volume = 1;
    revision.payload.notional_p6 = price;
    revision.payload.trade_count = 1U;
    revision.payload.first_trade = l2flow::kline::TradeAnchor{
        bucket_start + 1U, owner + 1U, row_sequence, row_sequence};
    revision.payload.last_trade = revision.payload.first_trade;
    revision.payload.provisional = true;
    return revision;
}

[[nodiscard]] std::vector<std::shared_ptr<const KLineRevisionBatch>>
MakeBurst(std::uint64_t first_row,
          std::size_t rows,
          std::uint32_t owner,
          std::size_t logical_batch_rows,
          Identifier128 calculation_run,
          std::uint64_t* next_owner_batch_sequence,
          std::uint64_t* next_recovery_sequence,
          std::uint64_t* next_wal_lsn) {
    std::vector<std::shared_ptr<const KLineRevisionBatch>> batches;
    const std::size_t batch_count =
        (rows + logical_batch_rows - 1U) / logical_batch_rows;
    batches.reserve(batch_count);
    std::size_t emitted = 0U;
    while (emitted < rows) {
        const std::size_t batch_rows = std::min(
            logical_batch_rows, rows - emitted);
        const std::uint64_t batch_sequence =
            (*next_owner_batch_sequence)++;
        const Identifier128 recovery_run = Identifier(
            (*next_recovery_sequence)++, 4U);
        auto batch = std::make_shared<KLineRevisionBatch>();
        batch->calculation_run_id = calculation_run;
        batch->recovery_run_id = recovery_run;
        batch->owner = owner;
        batch->batch_sequence = batch_sequence;
        batch->reason = RevisionReason::kLiveProjection;
        const std::uint64_t wal_lsn = (*next_wal_lsn)++;
        batch->input_positions.push_back(
            l2flow::outbox::WalPosition{wal_lsn, wal_lsn, 0U});
        batch->revisions.reserve(batch_rows);
        for (std::size_t row = 0U; row < batch_rows; ++row) {
            batch->revisions.push_back(Revision(
                first_row + static_cast<std::uint64_t>(emitted + row),
                owner, calculation_run, recovery_run));
        }
        emitted += batch_rows;
        batches.push_back(std::move(batch));
    }
    return batches;
}

template <typename Value>
[[nodiscard]] double QueueSlope(
    const std::vector<QueueSample>& samples,
    Value&& value) noexcept {
    if (samples.size() < 4U) {
        return 0.0;
    }
    const std::size_t begin = samples.size() / 5U;
    const double origin = static_cast<double>(samples[begin].monotonic_ns) /
        1'000'000'000.0;
    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_xx = 0.0;
    double sum_xy = 0.0;
    const std::size_t count = samples.size() - begin;
    for (std::size_t index = begin; index < samples.size(); ++index) {
        const double x =
            static_cast<double>(samples[index].monotonic_ns) /
                1'000'000'000.0 -
            origin;
        const double y = static_cast<double>(value(samples[index]));
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_xy += x * y;
    }
    const double denominator =
        static_cast<double>(count) * sum_xx - sum_x * sum_x;
    return denominator <= 0.0
        ? 0.0
        : (static_cast<double>(count) * sum_xy - sum_x * sum_y) /
              denominator;
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << error << '\n';
        PrintUsage();
        return 2;
    }

    const std::size_t server_workers = std::max<std::size_t>(
        options.writer_lanes, 4U);
    LoopbackHttpServer server(server_workers, options.response_delay_us);
    if (!server.Start(&error)) {
        std::cout << "status=SKIP reason=loopback_unavailable detail="
                  << error << '\n';
        return 0;
    }

    const std::size_t burst_rows =
        options.logical_batch_rows * options.producer_burst_batches;
    const std::uint64_t total_rows = options.target_rows_per_second *
        static_cast<std::uint64_t>(options.seconds);
    const std::size_t queue_rows = std::max<std::size_t>(
        1'048'576U, burst_rows * 4U);
    const std::size_t queue_batches = std::max<std::size_t>(
        1'024U,
        (queue_rows + options.logical_batch_rows - 1U) /
                options.logical_batch_rows +
            options.producer_burst_batches);

    KLineClickHouseConfig config{};
    CompletionSink completion;
    config.endpoint = server.endpoint();
    config.database = "kline_sink_benchmark";
    config.insert_request_max_rows = options.insert_request_rows;
    config.insert_request_max_bytes = options.insert_request_bytes;
    config.physical_group_max_batches = options.physical_group_batches;
    config.physical_group_max_delay_ns = options.physical_group_delay_ns;
    config.writer_lanes = options.writer_lanes;
    config.queue_revision_batches = queue_batches;
    config.queue_revision_rows = queue_rows;
    config.connect_timeout_ms = 1'000U;
    config.request_timeout_ms = 10'000U;
    config.retry_initial_backoff_ms = 1U;
    config.retry_max_backoff_ms = 10U;
    config.shutdown_timeout_ms = 30'000U;
    config.ensure_local_tables = false;
    config.completion_sink = &completion;
    config.request_spool.directory =
        std::filesystem::temp_directory_path() /
        ("l2flow-kline-sink-benchmark-" +
         std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
         std::to_string(MonotonicNowNs()));

    std::unique_ptr<KLineClickHouseSink> sink =
        KLineClickHouseSink::Create(config, &error);
    if (sink == nullptr || !sink->Start(&error)) {
        std::cerr << "KLine sink start failed: " << error << '\n';
        return 1;
    }

    const Identifier128 calculation_run = Identifier(1U, 5U);
    std::vector<std::uint64_t> owner_batch_sequences(options.owners, 1U);
    std::uint64_t next_recovery_sequence = 1U;
    std::uint64_t next_wal_lsn = 1U;
    std::vector<QueueSample> queue_samples;
    queue_samples.reserve(static_cast<std::size_t>(
        total_rows / static_cast<std::uint64_t>(burst_rows) + 3U));
    const std::uint64_t start_ns =
        MonotonicNowNs() + UINT64_C(100'000'000);
    queue_samples.push_back(QueueSample{start_ns, 0U, 0U});
    std::uint64_t emitted_rows = 0U;
    std::uint64_t submitted_batches = 0U;
    std::uint64_t producer_bursts = 0U;
    std::uint64_t maximum_schedule_lag_ns = 0U;
    bool append_ok = true;
    while (emitted_rows < total_rows) {
        const std::size_t rows = static_cast<std::size_t>(std::min<
            std::uint64_t>(burst_rows, total_rows - emitted_rows));
        const std::uint32_t owner = static_cast<std::uint32_t>(
            producer_bursts % options.owners);
        std::vector<std::shared_ptr<const KLineRevisionBatch>> batches =
            MakeBurst(
                emitted_rows + 1U, rows, owner,
                options.logical_batch_rows, calculation_run,
                &owner_batch_sequences[owner], &next_recovery_sequence,
                &next_wal_lsn);
        const std::uint64_t deadline_ns = start_ns + ScheduledOffsetNs(
            emitted_rows, options.target_rows_per_second);
        const std::uint64_t now = WaitUntil(deadline_ns);
        maximum_schedule_lag_ns = std::max(
            maximum_schedule_lag_ns,
            now >= deadline_ns ? now - deadline_ns : 0U);
        submitted_batches += batches.size();
        for (auto& batch : batches) {
            if (!sink->AppendRevisionBatch(std::move(batch))) {
                append_ok = false;
                break;
            }
        }
        if (!append_ok) {
            break;
        }
        emitted_rows += rows;
        ++producer_bursts;
        const KLineClickHouseStats current = sink->stats();
        queue_samples.push_back(QueueSample{
            MonotonicNowNs(), current.queued_revision_batches,
            current.queued_revision_rows});
    }
    const std::uint64_t scheduled_end_ns = start_ns + ScheduledOffsetNs(
        total_rows, options.target_rows_per_second);
    const std::uint64_t producer_end_ns = WaitUntil(scheduled_end_ns);
    const KLineClickHouseStats before_stop = sink->stats();
    queue_samples.push_back(QueueSample{
        producer_end_ns, before_stop.queued_revision_batches,
        before_stop.queued_revision_rows});
    const bool stopped = sink->Stop(&error);
    const std::uint64_t durable_end_ns = MonotonicNowNs();
    const KLineClickHouseStats stats = sink->stats();
    const bool sink_healthy = sink->healthy();
    const std::string sink_fatal = sink->fatal_error();
    sink.reset();
    server.Stop();

    const std::vector<std::uint64_t> revision_latencies =
        server.RevisionLatencies();
    const std::vector<std::uint64_t> marker_latencies =
        server.MarkerLatencies();
    const double producer_seconds = producer_end_ns <= start_ns
        ? 0.0
        : static_cast<double>(producer_end_ns - start_ns) /
              1'000'000'000.0;
    const double durable_seconds = durable_end_ns <= start_ns
        ? 0.0
        : static_cast<double>(durable_end_ns - start_ns) /
              1'000'000'000.0;
    const double enqueue_rows_per_second = producer_seconds == 0.0
        ? 0.0
        : static_cast<double>(emitted_rows) / producer_seconds;
    const double durable_rows_per_second = durable_seconds == 0.0
        ? 0.0
        : static_cast<double>(stats.revision_rows_acked) / durable_seconds;
    const double logical_batches_per_second = producer_seconds == 0.0
        ? 0.0
        : static_cast<double>(submitted_batches) / producer_seconds;
    const double revisions_per_insert =
        stats.revision_insert_requests_acked == 0U
        ? 0.0
        : static_cast<double>(stats.revision_insert_rows_acked) /
              static_cast<double>(stats.revision_insert_requests_acked);
    const double batches_per_physical_group =
        stats.physical_groups_committed == 0U
        ? 0.0
        : static_cast<double>(stats.revision_batches_acked) /
              static_cast<double>(stats.physical_groups_committed);
    const double revision_request_average_us =
        stats.revision_insert_requests_acked == 0U
        ? 0.0
        : static_cast<double>(stats.revision_insert_latency_ns_total) /
              static_cast<double>(stats.revision_insert_requests_acked) /
              1'000.0;
    const double marker_request_average_us =
        stats.marker_insert_requests_acked == 0U
        ? 0.0
        : static_cast<double>(stats.marker_insert_latency_ns_total) /
              static_cast<double>(stats.marker_insert_requests_acked) /
              1'000.0;
    const double batch_queue_slope = QueueSlope(
        queue_samples,
        [](const QueueSample& sample) { return sample.batches; });
    const double row_queue_slope = QueueSlope(
        queue_samples, [](const QueueSample& sample) { return sample.rows; });
    const std::size_t request_row_capacity = std::min(
        options.insert_request_rows,
        options.insert_request_bytes / kRevisionRowBytes);
    const double minimum_density =
        static_cast<double>(request_row_capacity) * 0.90;
    const double maximum_row_slope =
        static_cast<double>(options.target_rows_per_second) * 0.001;
    const double maximum_batch_slope =
        static_cast<double>(options.target_rows_per_second) /
            static_cast<double>(options.logical_batch_rows) *
        0.001;
    bool lanes_drained = stats.writer_lanes == options.writer_lanes;
    for (std::size_t lane = 0U; lane < stats.writer_lanes; ++lane) {
        lanes_drained = lanes_drained &&
            stats.lanes[lane].queued_revision_batches == 0U &&
            stats.lanes[lane].queued_revision_rows == 0U;
    }

    const bool valid = append_ok && stopped && sink_healthy &&
        server.healthy() && emitted_rows == total_rows &&
        stats.revision_batches_queued == submitted_batches &&
        stats.revision_batches_acked == submitted_batches &&
        stats.revision_batches_released == submitted_batches &&
        completion.completed.load(std::memory_order_relaxed) ==
            submitted_batches &&
        stats.revision_rows_queued == total_rows &&
        stats.revision_rows_acked == total_rows &&
        stats.revision_insert_rows_acked == total_rows &&
        stats.revision_insert_bytes_acked ==
            total_rows * kRevisionRowBytes &&
        stats.marker_insert_rows_acked == submitted_batches &&
        stats.marker_insert_bytes_acked ==
            submitted_batches * kMarkerRowBytes &&
        stats.recovery_runs_committed == submitted_batches &&
        stats.marker_insert_requests_acked ==
            stats.physical_groups_committed &&
        stats.queued_revision_batches == 0U &&
        stats.queued_revision_rows == 0U && lanes_drained &&
        stats.retry_attempts == 0U && stats.unknown_outcomes == 0U &&
        revision_latencies.size() ==
            stats.revision_insert_requests_acked &&
        marker_latencies.size() == stats.marker_insert_requests_acked &&
        server.revision_rows() == total_rows &&
        server.marker_rows() == submitted_batches &&
        server.revision_body_bytes() == stats.revision_insert_bytes_acked &&
        server.marker_body_bytes() == stats.marker_insert_bytes_acked &&
        stats.bytes_sent ==
            server.revision_body_bytes() + server.marker_body_bytes() &&
        revisions_per_insert >= minimum_density &&
        revisions_per_insert <=
            static_cast<double>(request_row_capacity) &&
        stats.request_spool_live_groups == 0U &&
        stats.request_spool_bytes == 0U &&
        // The one-second smoke includes schema probes and one durable spool
        // handoff per physical group. Allow a small fixed cold-start cost
        // while still rejecting a sustained throughput collapse.
        durable_rows_per_second >=
            static_cast<double>(options.target_rows_per_second) * 0.99 &&
        row_queue_slope <= maximum_row_slope &&
        batch_queue_slope <= maximum_batch_slope;

    std::cout << std::fixed << std::setprecision(3)
              << "benchmark_scope=loopback_http_transport_no_clickhouse_storage\n"
              << "kline_sink_config target_revision_s="
              << options.target_rows_per_second
              << " seconds=" << options.seconds
              << " owners=" << options.owners
              << " writer_lanes=" << options.writer_lanes
              << " logical_batch_rows=" << options.logical_batch_rows
              << " producer_burst_batches="
              << options.producer_burst_batches
              << " burst_rows=" << burst_rows
              << " insert_request_rows=" << options.insert_request_rows
              << " insert_request_bytes=" << options.insert_request_bytes
              << " physical_group_batches="
              << options.physical_group_batches
              << " physical_group_delay_ns="
              << options.physical_group_delay_ns
              << " queue_revision_batches=" << queue_batches
              << " queue_revision_rows=" << queue_rows
              << " response_delay_us=" << options.response_delay_us << '\n'
              << "kline_sink_throughput submitted_rows=" << emitted_rows
              << " acked_rows=" << stats.revision_rows_acked
              << " producer_seconds=" << producer_seconds
              << " durable_seconds=" << durable_seconds
              << " enqueue_revision_s=" << enqueue_rows_per_second
              << " durable_revision_s=" << durable_rows_per_second
              << " logical_batches_per_s=" << logical_batches_per_second
              << " maximum_schedule_lag_us="
              << static_cast<double>(maximum_schedule_lag_ns) / 1'000.0
              << '\n'
              << "kline_sink_density producer_bursts=" << producer_bursts
              << " logical_batches=" << stats.revision_batches_queued
              << " physical_groups=" << stats.physical_groups_committed
              << " revision_requests="
              << stats.revision_insert_requests_acked
              << " marker_requests=" << stats.marker_insert_requests_acked
              << " revisions_per_insert=" << revisions_per_insert
              << " logical_batches_per_physical_group="
              << batches_per_physical_group
              << " physical_group_batches_max="
              << stats.physical_group_batches_max
              << " physical_group_rows_max="
              << stats.physical_group_rows_max
              << " revision_request_rows_max="
              << stats.revision_request_rows_max
              << " revision_request_bytes_max="
              << stats.revision_request_bytes_max
              << " marker_request_rows_max="
              << stats.marker_request_rows_max
              << " marker_request_bytes_max="
              << stats.marker_request_bytes_max << '\n'
              << "kline_sink_latency client_revision_avg_us="
              << revision_request_average_us
              << " client_revision_max_us="
              << static_cast<double>(stats.revision_insert_latency_ns_max) /
                     1'000.0
              << " client_marker_avg_us=" << marker_request_average_us
              << " client_marker_max_us="
              << static_cast<double>(stats.marker_insert_latency_ns_max) /
                     1'000.0
              << " server_revision_p50_us="
              << static_cast<double>(Percentile(
                     revision_latencies, 0.50L)) / 1'000.0
              << " server_revision_p95_us="
              << static_cast<double>(Percentile(
                     revision_latencies, 0.95L)) / 1'000.0
              << " server_revision_p99_us="
              << static_cast<double>(Percentile(
                     revision_latencies, 0.99L)) / 1'000.0
              << " server_marker_p50_us="
              << static_cast<double>(Percentile(
                     marker_latencies, 0.50L)) / 1'000.0
              << " server_marker_p95_us="
              << static_cast<double>(Percentile(
                     marker_latencies, 0.95L)) / 1'000.0
              << " server_marker_p99_us="
              << static_cast<double>(Percentile(
                     marker_latencies, 0.99L)) / 1'000.0 << '\n'
              << "kline_sink_queue samples=" << queue_samples.size()
              << " batch_slope_per_s=" << batch_queue_slope
              << " row_slope_per_s=" << row_queue_slope
              << " batch_hwm="
              << stats.queued_revision_batches_high_water
              << " row_hwm=" << stats.queued_revision_rows_high_water
              << " final_batches=" << stats.queued_revision_batches
              << " final_rows=" << stats.queued_revision_rows << '\n';
    for (std::size_t lane = 0U; lane < stats.writer_lanes; ++lane) {
        std::cout << "kline_sink_lane lane=" << lane
                  << " batch_hwm="
                  << stats.lanes[lane].queued_revision_batches_high_water
                  << " row_hwm="
                  << stats.lanes[lane].queued_revision_rows_high_water
                  << " final_batches="
                  << stats.lanes[lane].queued_revision_batches
                  << " final_rows="
                  << stats.lanes[lane].queued_revision_rows << '\n';
    }
    std::cout << "kline_sink_transport revision_body_bytes="
              << server.revision_body_bytes()
              << " marker_body_bytes=" << server.marker_body_bytes()
              << " revision_rows=" << server.revision_rows()
              << " marker_rows=" << server.marker_rows()
              << " schema_requests=" << server.schema_requests()
              << " sink_bytes_sent=" << stats.bytes_sent
              << " retry_attempts=" << stats.retry_attempts
              << " unknown_outcomes=" << stats.unknown_outcomes
              << " final_spool_groups="
              << stats.request_spool_live_groups
              << " final_spool_bytes=" << stats.request_spool_bytes
              << " sink_healthy=" << (sink_healthy ? "true" : "false")
              << " server_healthy="
              << (server.healthy() ? "true" : "false") << '\n'
              << "status=" << (valid ? "PASS" : "FAIL") << '\n';
    if (!append_ok || !stopped || !sink_healthy) {
        std::cerr << "KLine sink fatal: "
                  << (sink_fatal.empty() ? error : sink_fatal) << '\n';
    }
    if (!server.healthy()) {
        std::cerr << "Loopback server fatal: " << server.fatal_error() << '\n';
    }
    return valid ? 0 : 1;
}

#endif
