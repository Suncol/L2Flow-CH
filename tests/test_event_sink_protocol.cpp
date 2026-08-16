#include "l2flow/clickhouse/event_sink.h"
#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
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

using l2flow::clickhouse::EventClickHouseConfig;
using l2flow::clickhouse::EventClickHouseSink;
using l2flow::event::EventKind;
using l2flow::event::EventPayload;
using l2flow::event::EventRevision;
using l2flow::event::EventRevisionBatch;
using l2flow::event::Identifier128;
using l2flow::event::Market;
using l2flow::event::OrderSnapshot;
using l2flow::event::RevisionOperation;
using l2flow::event::RevisionReason;
using l2flow::event::SourceAnchor;
using l2flow::ingest::MonotonicNowNs;
using l2flow::ingest::TickAction;

constexpr std::size_t kRevisionRowBytes = 665U;
constexpr std::size_t kMarkerRowBytes = 140U;

class TemporaryDirectory final {
public:
    explicit TemporaryDirectory(std::string_view prefix) {
        path_ = std::filesystem::temp_directory_path() /
            (std::string(prefix) + "-" +
             std::to_string(static_cast<unsigned long long>(::getpid())) +
             "-" + std::to_string(MonotonicNowNs()));
        std::filesystem::create_directories(path_);
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

[[nodiscard]] EventRevision Revision(
    std::uint64_t row_sequence,
    std::uint32_t owner,
    Identifier128 calculation_run,
    Identifier128 recovery_run) noexcept {
    EventRevision revision{};
    revision.key.trade_date = 20260811U;
    revision.key.market = Market::kShenzhen;
    revision.key.instrument_id = owner + 1U;
    revision.key.channel = owner + 1U;
    revision.key.native_sequence = row_sequence;
    revision.key.event_kind = EventKind::kShenzhenTrade;
    revision.version = row_sequence;
    revision.revision_id = Identifier(row_sequence, 1U);
    revision.recovery_run_id = recovery_run;
    revision.operation = RevisionOperation::kInsert;
    revision.reason = RevisionReason::kLiveProjection;
    revision.calculation_run_id = calculation_run;
    revision.logic_version = 1U;
    revision.input_set_hash = Identifier(row_sequence, 2U);
    revision.payload_hash = Identifier(row_sequence, 3U);
    revision.payload.source_anchor.native_sequence = row_sequence;
    revision.payload.action = TickAction::kTrade;
    revision.payload.quantity = 1;
    revision.payload.quantity_valid = true;
    return revision;
}

[[nodiscard]] std::vector<std::shared_ptr<const EventRevisionBatch>>
MakeSubmission(std::uint64_t first_row,
               std::size_t rows,
               std::size_t logical_batch_rows,
               std::uint32_t owner = 0U) {
    const Identifier128 calculation_run = Identifier(1U, 5U);
    std::vector<std::shared_ptr<const EventRevisionBatch>> submission;
    submission.reserve((rows + logical_batch_rows - 1U) /
                       logical_batch_rows);
    std::size_t emitted = 0U;
    std::uint64_t batch_sequence = 1U;
    while (emitted < rows) {
        const std::size_t batch_rows =
            std::min(logical_batch_rows, rows - emitted);
        const Identifier128 recovery_run = Identifier(batch_sequence, 4U);
        auto batch = std::make_shared<EventRevisionBatch>();
        batch->calculation_run_id = calculation_run;
        batch->recovery_run_id = recovery_run;
        batch->owner = owner;
        batch->batch_sequence = batch_sequence;
        batch->reason = RevisionReason::kLiveProjection;
        batch->input_positions.push_back(l2flow::outbox::WalPosition{
            batch_sequence, batch_sequence, 0U});
        batch->revisions.reserve(batch_rows);
        for (std::size_t row = 0U; row < batch_rows; ++row) {
            batch->revisions.push_back(Revision(
                first_row + static_cast<std::uint64_t>(emitted + row),
                owner, calculation_run, recovery_run));
        }
        submission.push_back(std::move(batch));
        emitted += batch_rows;
        ++batch_sequence;
    }
    return submission;
}

class CompletionRecorder final
    : public l2flow::outbox::ConsumerCompletionSink {
public:
    [[nodiscard]] bool Complete(
        l2flow::outbox::ConsumerKind consumer,
        std::span<const l2flow::outbox::WalPosition> positions)
        noexcept override {
        if (consumer != l2flow::outbox::ConsumerKind::kEvent ||
            marker_responses == nullptr ||
            marker_responses->load(std::memory_order_acquire) <= calls) {
            ordering_valid = false;
        }
        ++calls;
        try {
            completed.insert(completed.end(), positions.begin(),
                             positions.end());
        } catch (...) {
            return false;
        }
        return accept;
    }

    std::atomic<std::uint64_t>* marker_responses = nullptr;
    std::vector<l2flow::outbox::WalPosition> completed;
    std::uint64_t calls = 0U;
    bool ordering_valid = true;
    bool accept = true;
};

#if defined(__linux__)

struct CapturedRequest final {
    std::string target;
    std::string body;
    bool revision = false;
    bool marker = false;
};

enum class ResponseAction : std::uint8_t {
    kSuccess,
    kDropConnection,
    kRetryable,
};

class HttpServer final {
public:
    explicit HttpServer(std::vector<ResponseAction> insert_script = {})
        : insert_script_(std::move(insert_script)) {
        listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (listener_ < 0) {
            return;
        }
        const int enabled = 1;
        if (::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &enabled,
                         sizeof(enabled)) != 0) {
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
        socklen_t size = sizeof(address);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                          &size) != 0) {
            CloseListener();
            return;
        }
        port_ = ntohs(address.sin_port);
        try {
            thread_ = std::thread([this] { Run(); });
            valid_ = true;
        } catch (...) {
            CloseListener();
            throw;
        }
    }

    ~HttpServer() { Stop(); }
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    [[nodiscard]] bool valid() const noexcept { return valid_; }
    [[nodiscard]] std::string endpoint() const {
        return "http://127.0.0.1:" + std::to_string(port_);
    }
    [[nodiscard]] std::vector<CapturedRequest> inserts() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return inserts_;
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

    std::atomic<std::uint64_t> marker_responses{0U};

private:
    void CloseListener() noexcept {
        if (listener_ >= 0) {
            static_cast<void>(::close(listener_));
            listener_ = -1;
        }
    }

    [[nodiscard]] static bool Receive(int connection,
                                      CapturedRequest* request) {
        std::string received;
        std::array<char, 64U * 1'024U> buffer{};
        std::size_t header_end = std::string::npos;
        while (header_end == std::string::npos) {
            const ssize_t count = ::recv(connection, buffer.data(),
                                         buffer.size(), 0);
            if (count <= 0) {
                return false;
            }
            received.append(buffer.data(), static_cast<std::size_t>(count));
            header_end = received.find("\r\n\r\n");
            if (header_end == std::string::npos &&
                received.size() > 64U * 1'024U) {
                return false;
            }
        }
        const std::size_t first_space = received.find(' ');
        const std::size_t second_space =
            received.find(' ', first_space + 1U);
        if (first_space == std::string::npos ||
            second_space == std::string::npos) {
            return false;
        }
        request->target = received.substr(
            first_space + 1U, second_space - first_space - 1U);
        constexpr std::string_view length_header = "\r\nContent-Length:";
        const std::size_t length_position = received.find(length_header);
        if (length_position == std::string::npos) {
            return false;
        }
        std::size_t length_begin = length_position + length_header.size();
        while (length_begin < received.size() &&
               received[length_begin] == ' ') {
            ++length_begin;
        }
        const std::size_t length_end = received.find("\r\n", length_begin);
        std::size_t content_length = 0U;
        if (length_end == std::string::npos) {
            return false;
        }
        const auto parsed = std::from_chars(
            received.data() + length_begin, received.data() + length_end,
            content_length);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != received.data() + length_end ||
            content_length > 64U * 1'024U * 1'024U) {
            return false;
        }
        const std::size_t body_begin = header_end + 4U;
        while (received.size() - body_begin < content_length) {
            const ssize_t count = ::recv(connection, buffer.data(),
                                         buffer.size(), 0);
            if (count <= 0) {
                return false;
            }
            received.append(buffer.data(), static_cast<std::size_t>(count));
        }
        request->body.assign(received.data() + body_begin, content_length);
        request->revision =
            request->target.find("INSERT") != std::string::npos &&
            request->target.find("event_revision_log") != std::string::npos;
        request->marker =
            request->target.find("INSERT") != std::string::npos &&
            request->target.find("event_recovery_run") != std::string::npos;
        return true;
    }

    [[nodiscard]] static bool SendResponse(
        int connection,
        ResponseAction action) noexcept {
        constexpr std::string_view success =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        constexpr std::string_view retryable =
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\n"
            "Connection: close\r\n\r\nretry";
        const std::string_view response =
            action == ResponseAction::kRetryable ? retryable : success;
        std::size_t sent = 0U;
        while (sent < response.size()) {
            const ssize_t count = ::send(
                connection, response.data() + sent, response.size() - sent,
                MSG_NOSIGNAL);
            if (count <= 0) {
                return false;
            }
            sent += static_cast<std::size_t>(count);
        }
        return true;
    }

    void Run() noexcept {
        std::size_t insert_index = 0U;
        while (!stopped_.load(std::memory_order_acquire)) {
            const int connection = ::accept(listener_, nullptr, nullptr);
            if (connection < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            CapturedRequest request{};
            if (!Receive(connection, &request)) {
                static_cast<void>(::close(connection));
                continue;
            }
            const bool insert = request.revision || request.marker;
            ResponseAction action = ResponseAction::kSuccess;
            if (insert && insert_index < insert_script_.size()) {
                action = insert_script_[insert_index];
            }
            if (insert) {
                ++insert_index;
                std::lock_guard<std::mutex> lock(mutex_);
                inserts_.push_back(request);
            }
            if (request.marker && action == ResponseAction::kSuccess) {
                marker_responses.fetch_add(1U, std::memory_order_release);
            }
            if (action != ResponseAction::kDropConnection) {
                static_cast<void>(SendResponse(connection, action));
            }
            static_cast<void>(::shutdown(connection, SHUT_RDWR));
            static_cast<void>(::close(connection));
        }
    }

    std::vector<ResponseAction> insert_script_;
    int listener_ = -1;
    std::uint16_t port_ = 0U;
    bool valid_ = false;
    std::atomic<bool> stopped_{false};
    std::thread thread_;
    mutable std::mutex mutex_;
    std::vector<CapturedRequest> inserts_;
};

[[nodiscard]] EventClickHouseConfig Config(
    const HttpServer& server,
    const std::filesystem::path& spool_directory,
    CompletionRecorder* completion) {
    EventClickHouseConfig config{};
    config.endpoint = server.endpoint();
    config.database = "event_protocol_test";
    config.insert_request_max_rows = 4'096U;
    config.insert_request_max_bytes = 4U * 1'024U * 1'024U;
    config.physical_group_max_batches = 256U;
    config.physical_group_max_rows = 16'384U;
    config.physical_group_max_revision_bytes = 16U * 1'024U * 1'024U;
    config.physical_group_max_delay_ns = UINT64_C(1'000'000'000);
    config.writer_lanes = 1U;
    config.queue_revision_batches = 64U;
    config.queue_revision_rows = 32'768U;
    config.connect_timeout_ms = 500U;
    config.request_timeout_ms = 3'000U;
    config.retry_initial_backoff_ms = 1U;
    config.retry_max_backoff_ms = 2U;
    config.shutdown_timeout_ms = 5'000U;
    config.ensure_local_tables = false;
    config.request_spool.directory = spool_directory;
    config.request_spool.maximum_bytes = 64U * 1'024U * 1'024U;
    config.completion_sink = completion;
    return config;
}

class ReferenceWriter final {
public:
    template <typename Value>
    void Append(Value value) {
        static_assert(std::is_integral_v<Value> &&
                      !std::is_same_v<Value, bool>);
        using Unsigned = std::make_unsigned_t<Value>;
        const Unsigned encoded = std::bit_cast<Unsigned>(value);
        for (std::size_t index = 0U; index < sizeof(Value); ++index) {
            bytes_.push_back(static_cast<std::byte>(
                static_cast<std::uint64_t>(encoded) >>
                static_cast<unsigned int>(index * 8U)));
        }
    }

    template <typename Enum>
    void AppendEnum(Enum value) {
        Append(static_cast<std::underlying_type_t<Enum>>(value));
    }

    void AppendBool(bool value) {
        Append<std::uint8_t>(value ? 1U : 0U);
    }
    void AppendIdentifier(Identifier128 value) {
        bytes_.insert(bytes_.end(), value.bytes.begin(), value.bytes.end());
    }
    [[nodiscard]] std::size_t size() const noexcept { return bytes_.size(); }
    [[nodiscard]] const std::vector<std::byte>& bytes() const noexcept {
        return bytes_;
    }

private:
    std::vector<std::byte> bytes_;
};

void AppendAnchor(ReferenceWriter* writer, const SourceAnchor& value) {
    writer->Append(value.native_sequence);
    writer->Append(value.ingress_sequence);
    writer->Append(value.vendor_sequence_id);
    writer->Append(value.receive_monotonic_ns);
    writer->Append(value.exchange_time_ns_from_midnight);
    writer->Append(value.vendor_local_time_ns_from_midnight);
    writer->Append(value.exchange_time_raw);
    writer->Append(value.vendor_local_time_raw);
    writer->AppendBool(value.exchange_time_valid);
    writer->AppendBool(value.vendor_local_time_valid);
}

void AppendOrder(ReferenceWriter* writer, const OrderSnapshot& value) {
    writer->Append(value.key.trade_date);
    writer->AppendEnum(value.key.market);
    writer->Append(value.key.instrument_id);
    writer->Append(value.key.channel);
    writer->Append(value.key.order_id);
    writer->AppendEnum(value.side);
    writer->AppendEnum(value.order_type);
    writer->AppendEnum(value.sh_side_source);
    writer->AppendEnum(value.sh_order_source);
    writer->Append(value.price_p6);
    writer->AppendBool(value.price_valid);
    writer->AppendEnum(value.sh_price_source);
    writer->Append(value.execution_boundary_price_p6);
    writer->AppendBool(value.execution_boundary_price_valid);
    writer->Append(value.published_quantity);
    writer->AppendBool(value.published_quantity_valid);
    writer->Append(value.original_quantity);
    writer->AppendBool(value.original_quantity_valid);
    writer->AppendEnum(value.sh_original_quantity_status);
    writer->Append(value.remaining_quantity);
    writer->AppendBool(value.remaining_quantity_valid);
    writer->Append(value.source_matched_quantity);
    writer->AppendBool(value.source_matched_quantity_valid);
    writer->Append(value.observed_pre_add_trade_quantity);
    writer->Append(value.post_add_trade_quantity);
    writer->Append(value.total_trade_quantity);
    writer->Append(value.total_cancel_quantity);
    writer->Append(value.trade_count);
    writer->Append(value.cancel_count);
    writer->AppendEnum(value.phase_at_first);
    writer->AppendEnum(value.phase_at_add);
    writer->AppendEnum(value.phase_at_last);
    writer->AppendBool(value.add_seen);
    writer->AppendBool(value.apply_to_book);
    AppendAnchor(writer, value.first_anchor);
    AppendAnchor(writer, value.last_anchor);
    AppendAnchor(writer, value.add_anchor);
    writer->AppendEnum(value.finality);
    writer->Append(value.quality_flags);
    writer->Append(value.source_quality_flags);
}

void AppendPayload(ReferenceWriter* writer, const EventPayload& value) {
    AppendAnchor(writer, value.source_anchor);
    writer->AppendEnum(value.action);
    writer->AppendEnum(value.side);
    writer->AppendEnum(value.aggressor);
    writer->AppendEnum(value.order_type);
    writer->AppendEnum(value.phase);
    writer->Append(value.price_p6);
    writer->AppendBool(value.price_valid);
    writer->Append(value.amount_p6);
    writer->AppendBool(value.amount_valid);
    writer->Append(value.quantity);
    writer->AppendBool(value.quantity_valid);
    writer->Append(value.matched_quantity);
    writer->AppendBool(value.matched_quantity_valid);
    writer->Append(value.primary_order_id);
    writer->Append(value.buy_order_id);
    writer->Append(value.sell_order_id);
    writer->Append(value.source_quality_flags);
    writer->Append(value.event_quality_flags);
    writer->AppendBool(value.referenced_order_found);
    writer->AppendBool(value.referenced_order_found_valid);
    writer->AppendBool(value.side_from_order);
    writer->AppendEnum(value.order_delta_operation);
    writer->AppendBool(value.order_snapshot_valid);
    AppendOrder(writer, value.order);
}

[[nodiscard]] std::uint16_t TradeDateDays(std::uint32_t value) {
    using namespace std::chrono;
    const year_month_day date{
        std::chrono::year{static_cast<int>(value / 10'000U)},
        std::chrono::month{(value / 100U) % 100U},
        std::chrono::day{value % 100U}};
    CHECK(date.ok());
    return static_cast<std::uint16_t>(
        sys_days{date}.time_since_epoch().count());
}

[[nodiscard]] Identifier128 ChunkIdentifier(Identifier128 writer,
                                            Identifier128 recovery_run,
                                            std::uint32_t chunk,
                                            std::uint8_t table_tag) noexcept {
    std::array<std::byte, 41U> input{};
    std::copy(writer.bytes.begin(), writer.bytes.end(), input.begin());
    std::copy(recovery_run.bytes.begin(), recovery_run.bytes.end(),
              input.begin() + 16U);
    std::size_t offset = 32U;
    for (std::size_t index = 0U; index < sizeof(chunk); ++index) {
        input[offset++] = static_cast<std::byte>(chunk >> (index * 8U));
    }
    for (std::size_t index = 0U;
         index < sizeof(l2flow::event::kEventSchemaVersion); ++index) {
        input[offset++] = static_cast<std::byte>(
            l2flow::event::kEventSchemaVersion >> (index * 8U));
    }
    input[offset] = static_cast<std::byte>(table_tag);
    return l2flow::clickhouse::Blake3Hash128(input);
}

void AppendRevisionReference(ReferenceWriter* writer,
                             const EventRevision& revision,
                             Identifier128 writer_instance_id,
                             Identifier128 batch_id,
                             std::uint64_t sink_batch_sequence,
                             std::uint32_t chunk_index,
                             std::uint32_t row_index) {
    writer->Append(TradeDateDays(revision.key.trade_date));
    writer->AppendEnum(revision.key.market);
    writer->Append(revision.key.instrument_id);
    writer->Append(revision.key.channel);
    writer->Append(revision.key.native_sequence);
    writer->AppendEnum(revision.key.event_kind);
    writer->Append(revision.key.affected_order_id);
    writer->Append(revision.key.occurrence);
    writer->Append(revision.version);
    writer->AppendIdentifier(revision.revision_id);
    writer->AppendIdentifier(revision.supersedes_revision_id);
    writer->AppendBool(revision.supersedes_revision_id_valid);
    writer->AppendIdentifier(revision.recovery_run_id);
    writer->AppendEnum(revision.operation);
    writer->AppendEnum(revision.reason);
    writer->AppendIdentifier(revision.calculation_run_id);
    writer->Append(revision.logic_version);
    writer->AppendIdentifier(revision.input_set_hash);
    writer->AppendIdentifier(revision.payload_hash);
    writer->AppendBool(revision.is_deleted);
    AppendPayload(writer, revision.payload);
    writer->AppendIdentifier(writer_instance_id);
    writer->AppendIdentifier(batch_id);
    writer->Append(sink_batch_sequence);
    writer->Append(chunk_index);
    writer->Append(row_index);
    writer->Append(l2flow::event::kEventSchemaVersion);
}

void AppendMarkerReference(ReferenceWriter* writer,
                           const EventRevisionBatch& batch,
                           Identifier128 writer_instance_id,
                           std::uint32_t chunk_count,
                           std::uint64_t committed_utc_ns) {
    writer->Append(TradeDateDays(batch.revisions.front().key.trade_date));
    writer->AppendIdentifier(batch.calculation_run_id);
    writer->AppendIdentifier(batch.recovery_run_id);
    writer->Append(batch.owner);
    writer->Append(batch.batch_sequence);
    const l2flow::outbox::WalPosition input_max =
        batch.input_positions.back();
    writer->Append(input_max.lsn);
    writer->Append(input_max.batch_sequence);
    writer->Append(input_max.row_index);
    writer->AppendEnum(batch.reason);
    writer->Append(batch.revisions.front().version);
    writer->Append(batch.revisions.back().version);
    writer->Append(static_cast<std::uint64_t>(batch.revisions.size()));
    writer->Append(chunk_count);
    writer->AppendBool(true);
    writer->Append(committed_utc_ns);
    writer->AppendIdentifier(writer_instance_id);
    writer->AppendIdentifier(ChunkIdentifier(
        writer_instance_id, batch.recovery_run_id, 0U, 2U));
    writer->Append(l2flow::event::kEventSchemaVersion);
}

[[nodiscard]] std::uint32_t ReadLe32(std::string_view bytes,
                                     std::size_t offset) {
    CHECK(offset <= bytes.size());
    CHECK(sizeof(std::uint32_t) <= bytes.size() - offset);
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        value |= static_cast<std::uint32_t>(
                     static_cast<unsigned char>(bytes[offset + index]))
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

[[nodiscard]] std::uint64_t ReadLe64(std::string_view bytes,
                                     std::size_t offset) {
    CHECK(offset <= bytes.size());
    CHECK(sizeof(std::uint64_t) <= bytes.size() - offset);
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        value |= static_cast<std::uint64_t>(
                     static_cast<unsigned char>(bytes[offset + index]))
                 << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

void CheckBytes(std::string_view actual,
                const std::vector<std::byte>& expected) {
    CHECK(actual.size() == expected.size());
    CHECK(std::memcmp(actual.data(), expected.data(), expected.size()) == 0);
}

void TestMultiRequestGroupAndGoldenPayload() {
    HttpServer server;
    if (!server.valid()) {
        std::cout << "Event protocol fixture skipped: loopback unavailable\n";
        return;
    }
    TemporaryDirectory directory("l2flow-event-protocol-group");
    CompletionRecorder completion;
    completion.marker_responses = &server.marker_responses;
    EventClickHouseConfig config = Config(
        server, directory.path() / "spool", &completion);
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    const Identifier128 writer_instance_id = sink->writer_instance_id();
    CHECK(sink->Start(&error));
    auto submission = MakeSubmission(1U, 16'384U, 512U);
    const auto fixture = submission;
    CHECK(sink->AppendRevisionGroup(std::move(submission)));
    CHECK(sink->Stop(&error));
    server.Stop();

    const std::vector<CapturedRequest> inserts = server.inserts();
    CHECK(inserts.size() == 5U);
    for (std::size_t index = 0U; index < 4U; ++index) {
        CHECK(inserts[index].revision);
        CHECK(!inserts[index].marker);
        CHECK(inserts[index].body.size() == 4'096U * kRevisionRowBytes);
    }
    CHECK(inserts[4U].marker);
    CHECK(inserts[4U].body.size() == 32U * kMarkerRowBytes);

    std::array<ReferenceWriter, 4U> expected_revisions{};
    std::size_t global_row = 0U;
    for (std::size_t batch_index = 0U;
         batch_index < fixture.size(); ++batch_index) {
        const EventRevisionBatch& batch = *fixture[batch_index];
        const Identifier128 batch_id = ChunkIdentifier(
            writer_instance_id, batch.recovery_run_id, 0U, 1U);
        for (std::size_t row = 0U; row < batch.revisions.size(); ++row) {
            ReferenceWriter& expected =
                expected_revisions[global_row / 4'096U];
            const std::size_t checkpoint = expected.size();
            AppendRevisionReference(
                &expected, batch.revisions[row], writer_instance_id,
                batch_id, static_cast<std::uint64_t>(batch_index + 1U), 0U,
                static_cast<std::uint32_t>(row));
            CHECK(expected.size() - checkpoint == kRevisionRowBytes);
            ++global_row;
        }
    }
    CHECK(global_row == 16'384U);
    for (std::size_t index = 0U; index < expected_revisions.size(); ++index) {
        CheckBytes(inserts[index].body, expected_revisions[index].bytes());
    }

    const std::uint64_t committed_utc_ns =
        ReadLe64(inserts.back().body, 96U);
    ReferenceWriter expected_markers;
    for (const auto& batch : fixture) {
        const std::size_t checkpoint = expected_markers.size();
        AppendMarkerReference(&expected_markers, *batch, writer_instance_id,
                              1U, committed_utc_ns);
        CHECK(expected_markers.size() - checkpoint == kMarkerRowBytes);
    }
    CheckBytes(inserts.back().body, expected_markers.bytes());

    CHECK(completion.ordering_valid);
    CHECK(completion.calls == 1U);
    CHECK(completion.completed.size() == fixture.size());
    for (std::size_t index = 0U; index < fixture.size(); ++index) {
        CHECK(completion.completed[index] ==
              fixture[index]->input_positions.front());
    }
    const auto stats = sink->stats();
    CHECK(stats.physical_groups_committed == 1U);
    CHECK(stats.revision_insert_requests_acked == 4U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.revision_batches_acked == 32U);
    CHECK(stats.revision_rows_acked == 16'384U);
    CHECK(stats.physical_group_batches_max == 32U);
    CHECK(stats.physical_group_rows_max == 16'384U);
    CHECK(stats.request_spool_live_groups == 0U);
    CHECK(stats.request_spool_preparing_groups == 0U);
    CHECK(stats.request_spool_bytes == 0U);
    CHECK(stats.request_spool_reserved_bytes == 0U);
}

void TestRetryReusesExactRequest() {
    HttpServer server({ResponseAction::kDropConnection,
                       ResponseAction::kSuccess,
                       ResponseAction::kSuccess,
                       ResponseAction::kSuccess});
    if (!server.valid()) {
        return;
    }
    TemporaryDirectory directory("l2flow-event-protocol-retry");
    CompletionRecorder completion;
    completion.marker_responses = &server.marker_responses;
    EventClickHouseConfig config = Config(
        server, directory.path() / "spool", &completion);
    config.insert_request_max_rows = 1U;
    config.physical_group_max_rows = 8U;
    config.physical_group_max_revision_bytes = 8U * kRevisionRowBytes;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    auto submission = MakeSubmission(1U, 2U, 2U);
    CHECK(sink->AppendRevisionGroup(std::move(submission)));
    const auto stop_started = std::chrono::steady_clock::now();
    CHECK(sink->Stop(&error));
    const auto stop_elapsed = std::chrono::steady_clock::now() - stop_started;
    server.Stop();
    CHECK(stop_elapsed < std::chrono::milliseconds(750));
    const std::vector<CapturedRequest> inserts = server.inserts();
    CHECK(inserts.size() == 4U);
    CHECK(inserts[0U].revision && inserts[1U].revision);
    CHECK(inserts[0U].target == inserts[1U].target);
    CHECK(inserts[0U].body == inserts[1U].body);
    CHECK(inserts[2U].revision);
    CHECK(inserts[3U].marker);
    const auto stats = sink->stats();
    CHECK(stats.retry_attempts == 1U);
    CHECK(stats.unknown_outcomes == 1U);
    CHECK(stats.revision_rows_acked == 2U);
    CHECK(stats.request_spool_live_groups == 0U);
    CHECK(completion.ordering_valid);
    CHECK(completion.calls == 1U);
}

void TestOversizedLogicalBatchUsesOneMarker() {
    HttpServer server;
    if (!server.valid()) {
        return;
    }
    TemporaryDirectory directory("l2flow-event-protocol-oversized");
    CompletionRecorder completion;
    completion.marker_responses = &server.marker_responses;
    EventClickHouseConfig config = Config(
        server, directory.path() / "spool", &completion);
    config.physical_group_max_rows = 1'024U;
    config.physical_group_max_revision_bytes = 1'024U * kRevisionRowBytes;
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    auto submission = MakeSubmission(1U, 5'000U, 5'000U);
    CHECK(sink->AppendRevisionGroup(std::move(submission)));
    CHECK(sink->Stop(&error));
    server.Stop();
    const std::vector<CapturedRequest> inserts = server.inserts();
    CHECK(inserts.size() == 3U);
    CHECK(inserts[0U].revision &&
          inserts[0U].body.size() == 4'096U * kRevisionRowBytes);
    CHECK(inserts[1U].revision &&
          inserts[1U].body.size() == 904U * kRevisionRowBytes);
    CHECK(inserts[2U].marker &&
          inserts[2U].body.size() == kMarkerRowBytes);
    CHECK(ReadLe32(inserts[2U].body, 91U) == 2U);
    const auto stats = sink->stats();
    CHECK(stats.physical_groups_committed == 1U);
    CHECK(stats.revision_insert_requests_acked == 2U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.revision_rows_acked == 5'000U);
    CHECK(completion.calls == 1U);
}

void TestCompletionFailureRetainsGroup() {
    HttpServer server;
    if (!server.valid()) {
        return;
    }
    TemporaryDirectory directory("l2flow-event-protocol-completion");
    CompletionRecorder completion;
    completion.marker_responses = &server.marker_responses;
    completion.accept = false;
    EventClickHouseConfig config = Config(
        server, directory.path() / "spool", &completion);
    std::string error;
    std::unique_ptr<EventClickHouseSink> sink =
        EventClickHouseSink::Create(config, &error);
    CHECK(sink != nullptr);
    CHECK(sink->Start(&error));
    auto submission = MakeSubmission(1U, 1U, 1U);
    CHECK(sink->AppendRevisionGroup(std::move(submission)));
    CHECK(!sink->Stop(&error));
    server.Stop();
    CHECK(!sink->healthy());
    CHECK(error.find("cursor") != std::string::npos);
    CHECK(completion.ordering_valid);
    CHECK(completion.calls == 1U);
    const auto stats = sink->stats();
    CHECK(stats.revision_insert_requests_acked == 1U);
    CHECK(stats.marker_insert_requests_acked == 1U);
    CHECK(stats.revision_batches_released == 0U);
    CHECK(stats.queued_revision_batches == 1U);
    CHECK(stats.request_spool_live_groups == 1U);
    CHECK(stats.request_spool_bytes != 0U);
    CHECK(stats.request_spool_reserved_bytes == stats.request_spool_bytes);
}

#endif

}  // namespace

int main() {
#if defined(__linux__)
    TestMultiRequestGroupAndGoldenPayload();
    TestRetryReusesExactRequest();
    TestOversizedLogicalBatchUsesOneMarker();
    TestCompletionFailureRetainsGroup();
#else
    std::cout << "Event protocol fixtures skipped: Linux sockets required\n";
#endif
    std::cout << "all Event sink protocol tests passed\n";
    return 0;
}
