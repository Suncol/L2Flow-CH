#include "l2flow/clickhouse/event_sink.h"

#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

using l2flow::clickhouse::EventClickHouseConfig;
using l2flow::clickhouse::EventClickHouseSink;
using l2flow::clickhouse::IdentifierString;
using namespace l2flow::event;
using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

Identifier128 Identifier(std::uint8_t value) {
    Identifier128 result{};
    result.bytes[0U] = static_cast<std::byte>(value);
    return result;
}

EventRevision Revision(std::uint64_t version,
                       std::uint64_t native_sequence,
                       RevisionOperation operation,
                       std::int64_t quantity,
                       Identifier128 recovery_run) {
    EventRevision revision{};
    revision.key = EventKey{20260807U, Market::kShenzhen, 1U, 7U,
                            native_sequence, EventKind::kShenzhenTrade,
                            0, 0U};
    revision.version = version;
    revision.revision_id = Identifier(
        static_cast<std::uint8_t>(version + 16U));
    revision.recovery_run_id = recovery_run;
    revision.operation = operation;
    revision.reason = operation == RevisionOperation::kInsert
        ? RevisionReason::kLiveProjection
        : RevisionReason::kHoleFill;
    revision.calculation_run_id = Identifier(1U);
    revision.logic_version = 3U;
    revision.input_set_hash = Identifier(
        static_cast<std::uint8_t>(version + 32U));
    revision.payload_hash = Identifier(
        static_cast<std::uint8_t>(version + 48U));
    revision.is_deleted = operation == RevisionOperation::kTombstone;
    revision.payload.source_anchor.native_sequence = native_sequence;
    revision.payload.action = TickAction::kTrade;
    revision.payload.quantity = quantity;
    revision.payload.quantity_valid = true;
    return revision;
}

std::shared_ptr<const EventRevisionBatch> Batch(
    std::uint64_t batch_sequence,
    Identifier128 recovery_run,
    std::vector<EventRevision> revisions,
    std::uint32_t owner = 0U) {
    auto batch = std::make_shared<EventRevisionBatch>();
    batch->calculation_run_id = Identifier(1U);
    batch->recovery_run_id = recovery_run;
    batch->owner = owner;
    batch->batch_sequence = batch_sequence;
    batch->reason = revisions.front().reason;
    batch->revisions = std::move(revisions);
    return batch;
}

bool AppendBatch(
    EventClickHouseSink* sink,
    std::shared_ptr<const EventRevisionBatch> batch) noexcept {
    std::vector<std::shared_ptr<const EventRevisionBatch>> group;
    group.push_back(std::move(batch));
    return sink->AppendRevisionGroup(std::move(group));
}

void TestConfigValidation() {
    EventClickHouseConfig config{};
    std::string error;
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.endpoint = "http://127.0.0.1:8123/?bad=1";
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.endpoint = "http://127.0.0.1:8123";
    config.insert_request_max_rows = 0U;
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.insert_request_max_rows = 65'536U;
    config.writer_lanes = 3U;
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.writer_lanes = 8U;
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.writer_lanes = 16U;
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.writer_lanes = 32U;
    CHECK(l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
    config.writer_lanes = 64U;
    CHECK(!l2flow::clickhouse::ValidateEventClickHouseConfig(config, &error));
}

#if defined(__linux__)
struct CapturedRequest final {
    std::string target;
    std::string body;
};

enum class ResponseAction : std::uint8_t {
    kSuccess = 0U,
    kDropConnection,
    kRetryable,
    kPermanent,
};

class RetryHttpServer final {
public:
    explicit RetryHttpServer(
        std::chrono::milliseconds first_insert_delay =
            std::chrono::milliseconds{5},
        bool drop_first_insert = true,
        std::vector<ResponseAction> response_script = {})
        : first_insert_delay_(first_insert_delay),
          drop_first_insert_(drop_first_insert),
          response_script_(std::move(response_script)) {
        listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (listener_ < 0) {
            return;
        }
        const int enabled = 1;
        if (::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR,
                         &enabled, sizeof(enabled)) != 0) {
            CloseListener();
            return;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0U;
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::listen(listener_, 16) != 0) {
            CloseListener();
            return;
        }
        socklen_t address_size = sizeof(address);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                          &address_size) != 0) {
            CloseListener();
            return;
        }
        port_ = ntohs(address.sin_port);
        try {
            const int listener = listener_;
            thread_ = std::thread([this, listener] { Run(listener); });
            valid_ = true;
        } catch (...) {
            CloseListener();
            throw;
        }
    }

    ~RetryHttpServer() { Stop(); }
    RetryHttpServer(const RetryHttpServer&) = delete;
    RetryHttpServer& operator=(const RetryHttpServer&) = delete;

    [[nodiscard]] bool valid() const noexcept { return valid_; }
    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }

    void Stop() noexcept {
        if (stopped_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        if (listener_ >= 0) {
            static_cast<void>(::shutdown(listener_, SHUT_RDWR));
            static_cast<void>(::close(listener_));
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        listener_ = -1;
    }

    [[nodiscard]] std::vector<CapturedRequest> requests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_;
    }

private:
    void CloseListener() noexcept {
        if (listener_ >= 0) {
            static_cast<void>(::close(listener_));
            listener_ = -1;
        }
    }

    static bool ReceiveRequest(int connection, CapturedRequest* request) {
        std::string received;
        std::array<char, 8U * 1'024U> buffer{};
        std::size_t header_end = std::string::npos;
        while (header_end == std::string::npos) {
            const ssize_t count = ::recv(
                connection, buffer.data(), buffer.size(), 0);
            if (count <= 0) {
                return false;
            }
            received.append(buffer.data(), static_cast<std::size_t>(count));
            if (received.size() > 64U * 1'024U) {
                return false;
            }
            header_end = received.find("\r\n\r\n");
        }
        const std::size_t first_space = received.find(' ');
        const std::size_t second_space = received.find(' ', first_space + 1U);
        if (first_space == std::string::npos ||
            second_space == std::string::npos ||
            second_space <= first_space + 1U) {
            return false;
        }
        request->target = received.substr(
            first_space + 1U, second_space - first_space - 1U);

        constexpr std::string_view kLength = "\r\nContent-Length:";
        const std::size_t length_position = received.find(kLength);
        if (length_position == std::string::npos) {
            return false;
        }
        std::size_t length_begin = length_position + kLength.size();
        while (length_begin < received.size() &&
               received[length_begin] == ' ') {
            ++length_begin;
        }
        const std::size_t length_end = received.find("\r\n", length_begin);
        if (length_end == std::string::npos) {
            return false;
        }
        std::size_t content_length = 0U;
        const auto parsed = std::from_chars(
            received.data() + length_begin,
            received.data() + length_end, content_length);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != received.data() + length_end ||
            content_length > 64U * 1'024U * 1'024U) {
            return false;
        }
        const std::size_t body_begin = header_end + 4U;
        while (received.size() - body_begin < content_length) {
            const ssize_t count = ::recv(
                connection, buffer.data(), buffer.size(), 0);
            if (count <= 0) {
                return false;
            }
            received.append(buffer.data(), static_cast<std::size_t>(count));
        }
        request->body.assign(received.data() + body_begin, content_length);
        return true;
    }

    static void SendResponse(int connection,
                             ResponseAction action) noexcept {
        constexpr std::string_view success =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        constexpr std::string_view retryable =
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\n"
            "Connection: close\r\n\r\nretry";
        constexpr std::string_view permanent =
            "HTTP/1.1 400 Bad Request\r\nContent-Length: 9\r\n"
            "Connection: close\r\n\r\npermanent";
        const std::string_view response = action == ResponseAction::kRetryable
            ? retryable
            : action == ResponseAction::kPermanent ? permanent : success;
        std::size_t sent = 0U;
        while (sent < response.size()) {
            const ssize_t count = ::send(
                connection, response.data() + sent,
                response.size() - sent, MSG_NOSIGNAL);
            if (count <= 0) {
                return;
            }
            sent += static_cast<std::size_t>(count);
        }
    }

    void Run(int listener) noexcept {
        std::size_t insert_requests = 0U;
        while (!stopped_.load(std::memory_order_acquire)) {
            const int connection = ::accept(listener, nullptr, nullptr);
            if (connection < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            CapturedRequest request{};
            if (!ReceiveRequest(connection, &request)) {
                static_cast<void>(::close(connection));
                continue;
            }
            const bool insert = request.target.find("INSERT") !=
                                std::string::npos;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                requests_.push_back(std::move(request));
            }
            if (insert) {
                ++insert_requests;
            }
            if (insert && insert_requests == 1U) {
                std::this_thread::sleep_for(first_insert_delay_);
            }
            ResponseAction action = ResponseAction::kSuccess;
            if (insert && insert_requests <= response_script_.size()) {
                action = response_script_[insert_requests - 1U];
            } else if (insert && insert_requests == 1U &&
                       drop_first_insert_) {
                action = ResponseAction::kDropConnection;
            }
            if (action != ResponseAction::kDropConnection) {
                SendResponse(connection, action);
            }
            static_cast<void>(::shutdown(connection, SHUT_RDWR));
            static_cast<void>(::close(connection));
        }
    }

    std::chrono::milliseconds first_insert_delay_{};
    bool drop_first_insert_ = true;
    std::vector<ResponseAction> response_script_;
    int listener_ = -1;
    std::uint16_t port_ = 0U;
    bool valid_ = false;
    std::atomic<bool> stopped_{false};
    std::thread thread_;
    mutable std::mutex mutex_;
    std::vector<CapturedRequest> requests_;
};

EventClickHouseConfig HttpConfig(const RetryHttpServer& server) {
    EventClickHouseConfig config{};
    config.endpoint = server.endpoint();
    config.database = "event_retry_contract";
    config.insert_request_max_rows = 1U;
    config.queue_revision_batches = 2U;
    config.queue_revision_rows = 8U;
    config.connect_timeout_ms = 500U;
    config.request_timeout_ms = 1'000U;
    config.retry_initial_backoff_ms = 1U;
    config.retry_max_backoff_ms = 2U;
    config.maximum_retry_elapsed_ms = 100U;
    config.shutdown_timeout_ms = 3'000U;
    config.ensure_local_tables = false;
    return config;
}

std::vector<CapturedRequest> InsertRequests(const RetryHttpServer& server) {
    std::vector<CapturedRequest> inserts;
    for (const CapturedRequest& request : server.requests()) {
        if (request.target.find("INSERT") != std::string::npos) {
            inserts.push_back(request);
        }
    }
    return inserts;
}

std::uint32_t ReadUInt32(const std::string& body,
                         std::size_t offset) {
    CHECK(offset <= body.size());
    CHECK(sizeof(std::uint32_t) <= body.size() - offset);
    std::uint32_t value = 0U;
    std::memcpy(&value, body.data() + offset, sizeof(value));
    return value;
}

void CheckIdentifier(const std::string& body,
                     std::size_t offset,
                     Identifier128 expected) {
    CHECK(offset <= body.size());
    CHECK(expected.bytes.size() <= body.size() - offset);
    CHECK(std::memcmp(body.data() + offset, expected.bytes.data(),
                      expected.bytes.size()) == 0);
}

void TestUnknownOutcomeRetriesExactRevisionChunkBeforeCommit() {
    RetryHttpServer server;
    if (!server.valid()) {
        std::cout << "Event exact-retry fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));

    const Identifier128 recovery = Identifier(2U);
    std::vector<EventRevision> revisions;
    revisions.push_back(Revision(
        101U, 101U, RevisionOperation::kInsert, 10, recovery));
    revisions.push_back(Revision(
        102U, 102U, RevisionOperation::kInsert, 20, recovery));
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery, std::move(revisions))));
    CHECK(sink->Stop(&error));
    server.Stop();

    const std::vector<CapturedRequest> inserts = InsertRequests(server);
    CHECK(inserts.size() == 4U);
    CHECK(inserts[0U].target == inserts[1U].target);
    CHECK(inserts[0U].body == inserts[1U].body);
    CHECK(!inserts[0U].body.empty());
    CHECK(inserts[0U].target.find("event_revision_log") !=
          std::string::npos);
    CHECK(inserts[2U].target.find("event_revision_log") !=
          std::string::npos);
    CHECK(inserts[3U].target.find("event_recovery_run") !=
          std::string::npos);
    const auto stats = sink->stats();
    CHECK(stats.revision_batches_queued == 1U);
    CHECK(stats.revision_batches_acked == 1U);
    CHECK(stats.revision_batches_released == 1U);
    CHECK(stats.revision_rows_acked == 2U);
    CHECK(stats.revision_insert_requests_acked == 2U);
    CHECK(stats.recovery_runs_committed == 1U);
    CHECK(stats.retry_attempts == 1U);
    CHECK(stats.unknown_outcomes == 1U);
}

void TestOneSubmissionPreservesIndependentLogicalMarkers() {
    RetryHttpServer server(std::chrono::milliseconds{0}, false);
    if (!server.valid()) {
        std::cout << "Event grouped-marker fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    config.insert_request_max_rows = 64U;
    config.queue_revision_rows = 64U;
    config.queue_revision_batches = 8U;
    config.physical_group_max_delay_ns = UINT64_C(1'000'000'000);
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));

    const Identifier128 recovery0 = Identifier(20U);
    const Identifier128 recovery1 = Identifier(21U);
    std::vector<std::shared_ptr<const EventRevisionBatch>> group;
    group.push_back(Batch(
        1U, recovery0,
        {Revision(401U, 401U, RevisionOperation::kInsert, 1, recovery0)}));
    group.push_back(Batch(
        2U, recovery1,
        {Revision(402U, 402U, RevisionOperation::kInsert, 2, recovery1)}));
    const auto stop_started = std::chrono::steady_clock::now();
    CHECK(sink->AppendRevisionGroup(std::move(group)));
    CHECK(sink->Stop(&error));
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_started;
    server.Stop();
    CHECK(stop_elapsed < std::chrono::milliseconds{500});

    const std::vector<CapturedRequest> inserts = InsertRequests(server);
    CHECK(inserts.size() == 2U);
    CHECK(inserts[0U].target.find("event_revision_log") != std::string::npos);
    CHECK(inserts[0U].body.size() == 2U * 665U);
    CHECK(inserts[1U].target.find("event_recovery_run") != std::string::npos);
    CHECK(inserts[1U].body.size() == 2U * 120U);
    CheckIdentifier(inserts[1U].body, 18U, recovery0);
    CheckIdentifier(inserts[1U].body, 120U + 18U, recovery1);
    CHECK(ReadUInt32(inserts[1U].body, 71U) == 1U);
    CHECK(ReadUInt32(inserts[1U].body, 120U + 71U) == 1U);

    const auto stats = sink->stats();
    CHECK(stats.submission_groups_queued == 1U);
    CHECK(stats.submission_groups_released == 1U);
    CHECK(stats.revision_batches_queued == 2U);
    CHECK(stats.revision_batches_acked == 2U);
    CHECK(stats.revision_batches_released == 2U);
    CHECK(stats.physical_groups_committed == 1U);
    CHECK(stats.revision_insert_requests_acked == 1U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.recovery_runs_committed == 2U);
    CHECK(stats.queued_submission_groups == 0U);
    CHECK(stats.queued_revision_batches == 0U);
    CHECK(stats.queued_revision_rows == 0U);
}

void TestUnknownMarkerOutcomeDoesNotReplayRevisions() {
    RetryHttpServer server(
        std::chrono::milliseconds{0}, false,
        {ResponseAction::kSuccess, ResponseAction::kDropConnection,
         ResponseAction::kSuccess});
    if (!server.valid()) {
        std::cout << "Event marker-retry fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    config.insert_request_max_rows = 64U;
    config.queue_revision_rows = 64U;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    const Identifier128 recovery = Identifier(30U);
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery,
        {Revision(501U, 501U, RevisionOperation::kInsert, 1, recovery)})));
    CHECK(sink->Stop(&error));
    server.Stop();

    const std::vector<CapturedRequest> inserts = InsertRequests(server);
    CHECK(inserts.size() == 3U);
    CHECK(inserts[0U].target.find("event_revision_log") != std::string::npos);
    CHECK(inserts[1U].target.find("event_recovery_run") != std::string::npos);
    CHECK(inserts[1U].target == inserts[2U].target);
    CHECK(inserts[1U].body == inserts[2U].body);
    const auto stats = sink->stats();
    CHECK(stats.revision_insert_requests_acked == 1U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.retry_attempts == 1U);
    CHECK(stats.unknown_outcomes == 1U);
    CHECK(stats.revision_batches_acked == 1U);
    CHECK(stats.revision_batches_released == 1U);
}

void TestPermanentMarkerFailureRetainsLogicalQueue() {
    RetryHttpServer server(
        std::chrono::milliseconds{0}, false,
        {ResponseAction::kSuccess, ResponseAction::kPermanent});
    if (!server.valid()) {
        std::cout << "Event permanent-marker fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    config.insert_request_max_rows = 64U;
    config.queue_revision_rows = 64U;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    const Identifier128 recovery = Identifier(31U);
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery,
        {Revision(601U, 601U, RevisionOperation::kInsert, 1, recovery)})));
    CHECK(!sink->Stop(&error));
    server.Stop();

    const auto stats = sink->stats();
    CHECK(stats.revision_insert_requests_acked == 1U);
    CHECK(stats.marker_insert_requests_acked == 0U);
    CHECK(stats.physical_groups_committed == 0U);
    CHECK(stats.recovery_runs_committed == 0U);
    CHECK(stats.revision_batches_acked == 0U);
    CHECK(stats.revision_batches_released == 0U);
    CHECK(stats.submission_groups_released == 0U);
    CHECK(stats.queued_submission_groups == 1U);
    CHECK(stats.queued_revision_batches == 1U);
    CHECK(stats.queued_revision_rows == 1U);
    CHECK(error.find("failed permanently") != std::string::npos);
}

void TestByteBoundSplitsOneLogicalBatch() {
    RetryHttpServer server(std::chrono::milliseconds{0}, false);
    if (!server.valid()) {
        std::cout << "Event byte-bound fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    config.insert_request_max_rows = 64U;
    config.insert_request_max_bytes = 665U;
    config.queue_revision_rows = 64U;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    const Identifier128 recovery = Identifier(32U);
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery,
        {Revision(701U, 701U, RevisionOperation::kInsert, 1, recovery),
         Revision(702U, 702U, RevisionOperation::kInsert, 2, recovery)})));
    CHECK(sink->Stop(&error));
    server.Stop();

    const std::vector<CapturedRequest> inserts = InsertRequests(server);
    CHECK(inserts.size() == 3U);
    CHECK(inserts[0U].body.size() == 665U);
    CHECK(inserts[1U].body.size() == 665U);
    CHECK(inserts[2U].body.size() == 120U);
    CHECK(ReadUInt32(inserts[2U].body, 71U) == 2U);
    const auto stats = sink->stats();
    CHECK(stats.revision_insert_requests_acked == 2U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.revision_request_rows_max == 1U);
    CHECK(stats.revision_request_bytes_max == 665U);
}

void TestOrderedWriterLanesKeepOwnerAffinity() {
    RetryHttpServer server(std::chrono::milliseconds{0}, false);
    if (!server.valid()) {
        std::cout << "Event writer-lane fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    EventClickHouseConfig config = HttpConfig(server);
    config.insert_request_max_rows = 8U;
    config.writer_lanes = 2U;
    config.queue_revision_batches = 8U;
    config.queue_revision_rows = 32U;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));

    const Identifier128 recovery0 = Identifier(10U);
    const Identifier128 recovery1 = Identifier(11U);
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery0,
        {Revision(201U, 201U, RevisionOperation::kInsert, 1,
                  recovery0)},
        0U)));
    CHECK(AppendBatch(sink.get(), Batch(
        2U, recovery0,
        {Revision(202U, 202U, RevisionOperation::kInsert, 2,
                  recovery0)},
        0U)));
    CHECK(AppendBatch(sink.get(), Batch(
        1U, recovery1,
        {Revision(301U, 301U, RevisionOperation::kInsert, 3,
                  recovery1)},
        1U)));
    CHECK(sink->Stop(&error));
    server.Stop();

    const std::vector<CapturedRequest> inserts = InsertRequests(server);
    // Lane 0 combines two logical batches while lane 1 commits independently.
    CHECK(inserts.size() == 4U);
    bool saw_lane0 = false;
    bool saw_lane1 = false;
    for (const CapturedRequest& request : inserts) {
        saw_lane0 = saw_lane0 || request.target.find("%2F0%2F") !=
            std::string::npos;
        saw_lane1 = saw_lane1 || request.target.find("%2F1%2F") !=
            std::string::npos;
    }
    CHECK(saw_lane0);
    CHECK(saw_lane1);
    const auto stats = sink->stats();
    CHECK(stats.revision_batches_queued == 3U);
    CHECK(stats.revision_batches_acked == 3U);
    CHECK(stats.revision_batches_released == 3U);
    CHECK(stats.revision_rows_acked == 3U);
    CHECK(stats.recovery_runs_committed == 3U);
    CHECK(stats.queued_revision_rows == 0U);
}
#endif

std::size_t Capture(char* data,
                    std::size_t size,
                    std::size_t count,
                    void* context) noexcept {
    if (data == nullptr || context == nullptr || size == 0U) {
        return 0U;
    }
    const std::size_t bytes = size * count;
    try {
        static_cast<std::string*>(context)->append(data, bytes);
        return bytes;
    } catch (...) {
        return 0U;
    }
}

std::string Query(std::string_view endpoint,
                  std::string_view sql,
                  bool modifying = false) {
    CURL* const handle = ::curl_easy_init();
    CHECK(handle != nullptr);
    char* const escaped = ::curl_easy_escape(
        handle, sql.data(), static_cast<int>(sql.size()));
    CHECK(escaped != nullptr);
    std::string url(endpoint);
    while (url.ends_with('/')) {
        url.pop_back();
    }
    url += "/?query=";
    url += escaped;
    ::curl_free(escaped);
    std::string response;
    CHECK(::curl_easy_setopt(handle, CURLOPT_URL, url.c_str()) == CURLE_OK);
    if (modifying) {
        CHECK(::curl_easy_setopt(handle, CURLOPT_POST, 1L) == CURLE_OK);
        CHECK(::curl_easy_setopt(handle, CURLOPT_POSTFIELDS, "") == CURLE_OK);
        CHECK(::curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, 0L) ==
              CURLE_OK);
    }
    CHECK(::curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L) == CURLE_OK);
    CHECK(::curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, 5'000L) == CURLE_OK);
    CHECK(::curl_easy_setopt(handle, CURLOPT_NOPROXY, "*") == CURLE_OK);
    CHECK(::curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, &Capture) ==
          CURLE_OK);
    CHECK(::curl_easy_setopt(handle, CURLOPT_WRITEDATA, &response) ==
          CURLE_OK);
    const CURLcode code = ::curl_easy_perform(handle);
    long status = 0L;
    static_cast<void>(
        ::curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &status));
    ::curl_easy_cleanup(handle);
    CHECK(code == CURLE_OK);
    CHECK(status >= 200L && status < 300L);
    while (!response.empty() &&
           (response.back() == '\n' || response.back() == '\r')) {
        response.pop_back();
    }
    return response;
}

void TestClickHouseIntegration(std::string endpoint) {
    const auto unique = std::chrono::system_clock::now()
                            .time_since_epoch().count();
    const std::string database =
        "l2event_integration_" + std::to_string(unique);
    EventClickHouseConfig config{};
    config.endpoint = std::move(endpoint);
    config.database = database;
    config.insert_request_max_rows = 2U;
    config.queue_revision_batches = 8U;
    config.queue_revision_rows = 32U;
    config.maximum_retry_elapsed_ms = 2'000U;
    config.shutdown_timeout_ms = 10'000U;

    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));

    const Identifier128 run1 = Identifier(2U);
    const Identifier128 run2 = Identifier(3U);
    const Identifier128 run3 = Identifier(4U);
    const Identifier128 run4 = Identifier(5U);
    CHECK(AppendBatch(sink.get(), Batch(
        1U, run1,
        {Revision(101U, 101U, RevisionOperation::kInsert, 10, run1)})));
    EventRevision update = Revision(
        102U, 101U, RevisionOperation::kUpdate, 20, run2);
    update.supersedes_revision_id = Identifier(117U);
    update.supersedes_revision_id_valid = true;
    CHECK(AppendBatch(sink.get(), Batch(2U, run2, {update})));
    CHECK(AppendBatch(sink.get(), Batch(
        3U, run3,
        {Revision(103U, 102U, RevisionOperation::kInsert, 30, run3)})));
    EventRevision tombstone = Revision(
        104U, 102U, RevisionOperation::kTombstone, 30, run4);
    tombstone.supersedes_revision_id = Identifier(119U);
    tombstone.supersedes_revision_id_valid = true;
    CHECK(AppendBatch(sink.get(), Batch(4U, run4, {tombstone})));
    CHECK(sink->Stop(&error));

    CHECK(Query(config.endpoint,
                "SELECT count() FROM " + database +
                    ".event_revision_log FORMAT TSV") == "4");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM " + database +
                    ".event_recovery_run WHERE committed FORMAT TSV") ==
          "4");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM (SELECT * FROM " + database +
                    ".event FINAL) WHERE NOT is_deleted FORMAT TSV") ==
          "1");
    CHECK(Query(config.endpoint,
                "SELECT version, payload.quantity, "
                "supersedes_revision_id_valid FROM (SELECT * FROM " +
                    database +
                    ".event FINAL) WHERE NOT is_deleted FORMAT TSV") ==
          "102\t20\ttrue");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM (SELECT * FROM " + database +
                    ".event FINAL) WHERE native_sequence=102 AND "
                    "NOT is_deleted FORMAT TSV") == "0");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM system.columns WHERE database='" +
                    database +
                    "' AND table='event_revision_log' FORMAT TSV") ==
          "27");
    CHECK(Query(config.endpoint,
                "SELECT sorting_key FROM system.tables WHERE database='" +
                    database +
                    "' AND name='event_revision_log' FORMAT TSV") ==
          "calculation_run_id, recovery_run_id, version, row_index");

    config.ensure_local_tables = false;
    std::unique_ptr<EventClickHouseSink> external =
        EventClickHouseSink::Create(config, &error);
    CHECK(external != nullptr);
    CHECK(external->Start(&error));
    CHECK(external->Stop(&error));
    std::cout << "ClickHouse Event integration passed: database="
              << database << " writer="
              << IdentifierString(sink->writer_instance_id()) << '\n';
}

void TestClickHouseRejectsMismatchedSchema(const std::string& endpoint) {
    const auto unique = std::chrono::system_clock::now()
                            .time_since_epoch().count();
    const std::string database =
        "l2event_bad_schema_" + std::to_string(unique);
    EventClickHouseConfig config{};
    config.endpoint = endpoint;
    config.database = database;
    config.maximum_retry_elapsed_ms = 2'000U;
    config.shutdown_timeout_ms = 10'000U;

    std::string error;
    std::unique_ptr<EventClickHouseSink> initializer =
        EventClickHouseSink::Create(config, &error);
    CHECK(initializer != nullptr);
    CHECK(initializer->Start(&error));
    CHECK(initializer->Stop(&error));

    CHECK(Query(endpoint,
                "ALTER TABLE " + database +
                    ".event_recovery_run MODIFY COLUMN "
                    "schema_version UInt64",
                true)
              .empty());

    config.ensure_local_tables = false;
    std::unique_ptr<EventClickHouseSink> external =
        EventClickHouseSink::Create(config, &error);
    CHECK(external != nullptr);
    CHECK(!external->Start(&error));
    CHECK(error.find("Event recovery-run column contract mismatch") !=
          std::string::npos);
    CHECK(!external->healthy());

    CHECK(Query(endpoint, "DROP DATABASE " + database + " SYNC", true)
              .empty());
}

}  // namespace

int main() {
    TestConfigValidation();
#if defined(__linux__)
    TestUnknownOutcomeRetriesExactRevisionChunkBeforeCommit();
    TestOneSubmissionPreservesIndependentLogicalMarkers();
    TestUnknownMarkerOutcomeDoesNotReplayRevisions();
    TestPermanentMarkerFailureRetainsLogicalQueue();
    TestByteBoundSplitsOneLogicalBatch();
    TestOrderedWriterLanesKeepOwnerAffinity();
#endif
    const char* const endpoint = std::getenv("L2FLOW_CH_TEST_URL");
    if (endpoint == nullptr || endpoint[0] == '\0') {
        std::cout << "ClickHouse Event integration skipped; set "
                     "L2FLOW_CH_TEST_URL to enable it\n";
    } else {
        TestClickHouseIntegration(endpoint);
        TestClickHouseRejectsMismatchedSchema(endpoint);
    }
    std::cout << "all ClickHouse Event tests passed\n";
    return 0;
}
