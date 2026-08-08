#include "l2flow/clickhouse/raw_sink.h"

#include "l2flow/ingest/canonical.h"

#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
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

using l2flow::clickhouse::Blake3Hash128;
using l2flow::clickhouse::Identifier128;
using l2flow::clickhouse::IdentifierString;
using l2flow::clickhouse::ParseIdentifier;
using l2flow::clickhouse::RawBatchIdentifier;
using l2flow::clickhouse::RawClickHouseConfig;
using l2flow::clickhouse::RawClickHouseSink;
using l2flow::clickhouse::RawOccurrenceIdentifier;
using l2flow::clickhouse::RawTableId;
using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

class RecordingAckListener final : public RawTickBatchAckListener {
public:
    [[nodiscard]] bool OnRawTickBatchAcknowledged(
        std::span<const CanonicalTick> ticks) noexcept override {
        try {
            ++calls;
            acknowledged.insert(
                acknowledged.end(), ticks.begin(), ticks.end());
            return accept;
        } catch (...) {
            return false;
        }
    }

    std::vector<CanonicalTick> acknowledged;
    std::size_t calls = 0U;
    bool accept = true;
};

#if defined(__linux__)
struct CapturedRequest final {
    std::string target;
    std::string body;
};

class RetryHttpServer final {
public:
    explicit RetryHttpServer(
        std::chrono::milliseconds first_insert_delay =
            std::chrono::milliseconds{5},
        bool drop_first_insert = true)
        : first_insert_delay_(first_insert_delay),
          drop_first_insert_(drop_first_insert) {
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
            parsed.ptr != received.data() + length_end) {
            return false;
        }
        const std::size_t body_begin = header_end + 4U;
        if (content_length > 64U * 1'024U * 1'024U) {
            return false;
        }
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

    static void SendSuccess(int connection) noexcept {
        constexpr std::string_view response =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
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
            if (!insert || insert_requests != 1U || !drop_first_insert_) {
                SendSuccess(connection);
            }
            static_cast<void>(::shutdown(connection, SHUT_RDWR));
            static_cast<void>(::close(connection));
        }
    }

    std::chrono::milliseconds first_insert_delay_{};
    bool drop_first_insert_ = true;
    int listener_ = -1;
    std::uint16_t port_ = 0U;
    bool valid_ = false;
    std::atomic<bool> stopped_{false};
    std::thread thread_;
    mutable std::mutex mutex_;
    std::vector<CapturedRequest> requests_;
};
#endif

void SetIdentity(CanonicalCommon* common,
                 Market market,
                 std::string_view security) {
    CHECK(common != nullptr);
    CHECK(security.size() <= common->identity.security_id.size());
    common->identity.market = market;
    common->identity.security_id_size =
        static_cast<std::uint8_t>(security.size());
    std::copy(security.begin(), security.end(),
              reinterpret_cast<char*>(
                  common->identity.security_id.data()));
}

CanonicalTick MakeTick(std::uint64_t ingress_sequence) {
    CanonicalTick tick{};
    tick.common.ingress_sequence = ingress_sequence;
    tick.common.vendor_sequence_id = 900U;
    tick.common.receive_monotonic_ns = 10'000U + ingress_sequence;
    tick.common.native_sequence = 77U;
    tick.common.exchange_time_ns_from_midnight = 34'200'000'000'000U;
    tick.common.vendor_local_time_ns_from_midnight =
        34'200'000'000'100U;
    tick.common.trade_date = 20260807U;
    tick.common.instrument_id = 1U;
    tick.common.instrument_ordinal = 0U;
    tick.common.channel = 3U;
    tick.common.exchange_time_raw = 93'000'000U;
    tick.common.vendor_local_time_raw = 93'000'001U;
    tick.common.exchange_time_valid = true;
    tick.common.vendor_local_time_valid = true;
    tick.common.message_key = {4U, 101U, 24U};
    tick.common.kind = CanonicalKind::kShanghaiTick;
    SetIdentity(&tick.common, Market::kShanghai, "600000");
    tick.price = {12'340, 12'340'000, 3U, true, true};
    tick.quantity = {500, 0U, true};
    tick.validity |= kTickPriceValid | kTickQuantityValid |
                     kTickExchangeTimeValid;
    tick.action = TickAction::kTrade;
    tick.side = Side::kBuy;
    return tick;
}

CanonicalSnapshot MakeCatalogMissSnapshot() {
    CanonicalSnapshot snapshot{};
    snapshot.common.ingress_sequence = 3U;
    snapshot.common.vendor_sequence_id = 901U;
    snapshot.common.receive_monotonic_ns = 10'003U;
    snapshot.common.exchange_time_ns_from_midnight =
        34'200'000'000'000U;
    snapshot.common.vendor_local_time_ns_from_midnight =
        34'200'000'000'100U;
    snapshot.common.quality_flags = kQualityInstrumentNotInCatalog;
    snapshot.common.trade_date = 20260807U;
    snapshot.common.instrument_ordinal = kInvalidInstrumentOrdinal;
    snapshot.common.channel = 3U;
    snapshot.common.exchange_time_raw = 93'000'000U;
    snapshot.common.vendor_local_time_raw = 93'000'001U;
    snapshot.common.exchange_time_valid = true;
    snapshot.common.vendor_local_time_valid = true;
    snapshot.common.message_key = {4U, 101U, 4U};
    snapshot.common.kind = CanonicalKind::kShanghaiSnapshot;
    SetIdentity(&snapshot.common, Market::kShanghai, "600001");
    snapshot.last = {12'345, 12'345'000, 3U, true, true};
    snapshot.source_bid_depth = 1U;
    snapshot.retained_bid_depth = 1U;
    snapshot.bids[0U].price =
        {12'340, 12'340'000, 3U, true, true};
    snapshot.bids[0U].quantity = {500, 0U, true};
    snapshot.bids[0U].source_order_count = 2U;
    snapshot.bids[0U].order_count_valid = true;
    return snapshot;
}

void TestIdentifiers() {
    CHECK(IdentifierString(Blake3Hash128({})) ==
          "af1349b9f5f9a1a6a0404dea36dcc949");

    Identifier128 writer{};
    CHECK(ParseIdentifier("000102030405060708090a0b0c0d0e0f", &writer));
    CHECK(IdentifierString(writer) ==
          "000102030405060708090a0b0c0d0e0f");
    Identifier128 parsed{};
    CHECK(ParseIdentifier(IdentifierString(writer), &parsed));
    CHECK(parsed == writer);
    CHECK(!ParseIdentifier("0011", &parsed));
    CHECK(!ParseIdentifier("000102030405060708090a0b0c0d0e0x", &parsed));

    const Identifier128 batch = RawBatchIdentifier(
        writer, RawTableId::kRawTick, 20260807U, 42U, 1U);
    const Identifier128 occurrence = RawOccurrenceIdentifier(
        writer, 19U, 123U, CanonicalKind::kShanghaiTick);
    CHECK(IdentifierString(batch) ==
          "93f485094379558362f7544c4b6f265a");
    CHECK(IdentifierString(occurrence) ==
          "c33805e8648aac389581fe990cce4584");
    CHECK(batch != occurrence);
    CHECK(batch == RawBatchIdentifier(
                       writer, RawTableId::kRawTick,
                       20260807U, 42U, 1U));
    CHECK(occurrence == RawOccurrenceIdentifier(
                            writer, 19U, 123U,
                            CanonicalKind::kShanghaiTick));
    CHECK(batch != RawBatchIdentifier(
                       writer, RawTableId::kRawTick,
                       20260807U, 43U, 1U));
}

void TestConfigValidationAndPreallocation() {
    RawClickHouseConfig config{};
    config.feed_session_epoch = 19U;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.writer_threads = 1U;
    config.tick_batch_rows = 2U;
    config.snapshot_batch_rows = 1U;
    config.tick_queue_batches_per_lane = 2U;
    config.snapshot_queue_batches_per_lane = 2U;
    std::string error;
    CHECK(l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
    std::unique_ptr<RawClickHouseSink> sink =
        RawClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->stats().preallocated_canonical_bytes ==
          3U * 2U * sizeof(CanonicalTick) +
              3U * sizeof(CanonicalSnapshot));

    config.feed_session_epoch = 0U;
    CHECK(!l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
    config.feed_session_epoch = 19U;
    config.endpoint = "http://127.0.0.1:8123/?bad=1";
    CHECK(!l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
    config.endpoint = "http://";
    CHECK(!l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
    config.endpoint = "http://user:password@127.0.0.1:8123";
    CHECK(!l2flow::clickhouse::ValidateRawClickHouseConfig(config, &error));
}

#if defined(__linux__)
void TestUnknownOutcomeRetriesIdenticalBatch() {
    RetryHttpServer server;
    if (!server.valid()) {
        std::cout << "exact retry HTTP fixture skipped; loopback sockets are "
                     "unavailable\n";
        return;
    }
    RawClickHouseConfig config{};
    config.endpoint = server.endpoint();
    config.database = "retry_contract";
    config.feed_session_epoch = 19U;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.writer_threads = 1U;
    config.tick_batch_rows = 1U;
    config.snapshot_batch_rows = 1U;
    config.tick_queue_batches_per_lane = 2U;
    config.snapshot_queue_batches_per_lane = 2U;
    config.connect_timeout_ms = 500U;
    config.request_timeout_ms = 1'000U;
    config.retry_initial_backoff_ms = 1U;
    config.retry_max_backoff_ms = 2U;
    config.maximum_retry_elapsed_ms = 1U;
    config.shutdown_timeout_ms = 3'000U;
    RecordingAckListener listener;
    config.tick_ack_listener = &listener;

    std::string error;
    std::unique_ptr<RawClickHouseSink> sink =
        RawClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    CHECK(sink->AppendTick(0U, MakeTick(1U)));
    CHECK(sink->Stop(&error));
    server.Stop();

    std::vector<CapturedRequest> inserts;
    for (const CapturedRequest& request : server.requests()) {
        if (request.target.find("INSERT") != std::string::npos) {
            inserts.push_back(request);
        }
    }
    CHECK(inserts.size() == 2U);
    CHECK(!inserts[0U].body.empty());
    CHECK(inserts[0U].target == inserts[1U].target);
    CHECK(inserts[0U].body == inserts[1U].body);
    const auto stats = sink->stats();
    CHECK(stats.batches_queued == 1U);
    CHECK(stats.batches_acked == 1U);
    CHECK(stats.batches_released == 1U);
    CHECK(stats.retry_attempts == 1U);
    CHECK(stats.unknown_outcomes == 1U);
    CHECK(stats.bytes_sent == inserts[0U].body.size() * 2U);
    CHECK(listener.calls == 1U);
    CHECK(listener.acknowledged.size() == 1U);
    CHECK(listener.acknowledged[0U].common.ingress_sequence == 1U);
}

void TestAckListenerFailureFailsRawSinkClosed() {
    RetryHttpServer server(std::chrono::milliseconds{0}, false);
    if (!server.valid()) {
        std::cout << "ACK-listener HTTP fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    RecordingAckListener listener;
    listener.accept = false;
    RawClickHouseConfig config{};
    config.endpoint = server.endpoint();
    config.database = "ack_listener_contract";
    config.feed_session_epoch = 19U;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.writer_threads = 1U;
    config.tick_batch_rows = 1U;
    config.snapshot_batch_rows = 1U;
    config.tick_queue_batches_per_lane = 2U;
    config.snapshot_queue_batches_per_lane = 2U;
    config.tick_ack_listener = &listener;
    config.connect_timeout_ms = 500U;
    config.request_timeout_ms = 1'000U;
    config.maximum_retry_elapsed_ms = 1'000U;
    config.shutdown_timeout_ms = 3'000U;

    std::string error;
    std::unique_ptr<RawClickHouseSink> sink =
        RawClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    CHECK(sink->AppendTick(0U, MakeTick(1U)));
    CHECK(!sink->Stop(&error));
    server.Stop();

    CHECK(listener.calls == 1U);
    CHECK(!sink->healthy());
    CHECK(error.find("ACK listener rejected") != std::string::npos);
}

void TestQueueExhaustionFailsClosedAndDrains() {
    RetryHttpServer server(std::chrono::milliseconds{50}, false);
    if (!server.valid()) {
        std::cout << "queue exhaustion HTTP fixture skipped; loopback sockets "
                     "are unavailable\n";
        return;
    }
    RawClickHouseConfig config{};
    config.endpoint = server.endpoint();
    config.database = "queue_contract";
    config.feed_session_epoch = 19U;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.writer_threads = 1U;
    config.tick_batch_rows = 1U;
    config.snapshot_batch_rows = 1U;
    config.tick_queue_batches_per_lane = 1U;
    config.snapshot_queue_batches_per_lane = 1U;
    config.connect_timeout_ms = 500U;
    config.request_timeout_ms = 1'000U;
    config.maximum_retry_elapsed_ms = 1'000U;
    config.shutdown_timeout_ms = 3'000U;

    std::string error;
    std::unique_ptr<RawClickHouseSink> sink =
        RawClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    CHECK(sink->AppendTick(0U, MakeTick(1U)));
    CHECK(!sink->AppendTick(0U, MakeTick(2U)));
    CHECK(!sink->healthy());
    CHECK(!sink->Stop(&error));
    server.Stop();

    CHECK(error.find("preallocated batch pool exhausted") !=
          std::string::npos);
    const auto stats = sink->stats();
    CHECK(stats.tick_rows_received == 2U);
    CHECK(stats.batches_queued == 2U);
    CHECK(stats.batches_acked == 2U);
    CHECK(stats.batches_released == 2U);
    CHECK(stats.rows_acked == 2U);
    CHECK(stats.unacked_batches == 0U);
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

std::string Query(std::string_view endpoint, std::string_view sql) {
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
        "l2raw_integration_" + std::to_string(unique);
    Identifier128 source{};
    CHECK(ParseIdentifier("101112131415161718191a1b1c1d1e1f", &source));

    RawClickHouseConfig config{};
    config.endpoint = std::move(endpoint);
    config.database = database;
    config.feed_session_epoch = 19U;
    config.source_instance_id = source;
    config.tick_decoder_lanes = 1U;
    config.snapshot_decoder_lanes = 1U;
    config.writer_threads = 1U;
    config.tick_batch_rows = 2U;
    config.tick_batch_bytes = sizeof(CanonicalTick) * 2U;
    config.tick_batch_max_delay_ns = 1'000'000U;
    config.tick_queue_batches_per_lane = 2U;
    config.snapshot_batch_rows = 1U;
    config.snapshot_batch_bytes = sizeof(CanonicalSnapshot);
    config.snapshot_batch_max_delay_ns = 1'000'000U;
    config.snapshot_queue_batches_per_lane = 2U;
    config.maximum_retry_elapsed_ms = 2'000U;
    config.shutdown_timeout_ms = 10'000U;

    std::string error;
    std::unique_ptr<RawClickHouseSink> sink =
        RawClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    CHECK(sink->AppendTick(0U, MakeTick(1U)));
    CHECK(sink->AppendTick(0U, MakeTick(2U)));
    CHECK(sink->AppendSnapshot(0U, MakeCatalogMissSnapshot()));
    CHECK(sink->Stop(&error));

    const auto stats = sink->stats();
    CHECK(stats.tick_rows_received == 2U);
    CHECK(stats.snapshot_rows_received == 1U);
    CHECK(stats.batches_queued == 2U);
    CHECK(stats.batches_acked == 2U);
    CHECK(stats.batches_released == 2U);
    CHECK(stats.rows_acked == 3U);
    CHECK(stats.unacked_batches == 0U);
    CHECK(stats.preallocated_canonical_bytes != 0U);

    const std::string tick_table = database + ".raw_tick";
    const std::string snapshot_table = database + ".raw_snapshot";
    CHECK(Query(config.endpoint,
                "SELECT count(), uniqExact(occurrence_id), "
                "uniqExact(batch_id), min(row_index), max(row_index) FROM " +
                    tick_table + " FORMAT TSV") ==
          "2\t2\t1\t0\t1");
    CHECK(Query(config.endpoint,
                "SELECT ingress_sequence FROM " + tick_table +
                    " ORDER BY market, channel, native_sequence, "
                    "source_instance_id, feed_session_epoch, "
                    "ingress_sequence FORMAT TSV") ==
          "1\n2");
    CHECK(Query(config.endpoint,
                "SELECT any(toTypeName(trade_date)) FROM " + tick_table +
                    " FORMAT TSV") == "Date");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM " + tick_table +
                    " WHERE native_sequence = 77 FORMAT TSV") == "2");
    CHECK(Query(config.endpoint,
                "SELECT catalog_match, length(bids), "
                "bids[1].price_p6, bids[1].source_order_count FROM " +
                    snapshot_table + " FORMAT TSV") ==
          "false\t1\t12340000\t2");
    CHECK(Query(config.endpoint,
                "SELECT count() FROM system.parts WHERE active AND "
                "database = '" + database +
                    "' AND table IN ('raw_tick', 'raw_snapshot') AND "
                    "partition_id = '20260807' FORMAT TSV") == "2");

    config.ensure_local_tables = false;
    std::unique_ptr<RawClickHouseSink> externally_managed =
        RawClickHouseSink::Create(config, &error);
    CHECK(externally_managed != nullptr);
    CHECK(externally_managed->Start(&error));
    CHECK(externally_managed->Stop(&error));
    std::cout << "ClickHouse raw integration passed: database="
              << database << '\n';
}

}  // namespace

int main() {
    TestIdentifiers();
    TestConfigValidationAndPreallocation();
#if defined(__linux__)
    TestUnknownOutcomeRetriesIdenticalBatch();
    TestAckListenerFailureFailsRawSinkClosed();
    TestQueueExhaustionFailsClosedAndDrains();
#endif
    const char* const endpoint = std::getenv("L2FLOW_CH_TEST_URL");
    if (endpoint == nullptr || endpoint[0] == '\0') {
        std::cout << "ClickHouse integration skipped; set "
                     "L2FLOW_CH_TEST_URL to enable it\n";
    } else {
        TestClickHouseIntegration(endpoint);
    }
    std::cout << "all ClickHouse raw tests passed\n";
    return 0;
}
