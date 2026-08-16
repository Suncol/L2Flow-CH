#include "l2flow/clickhouse/freshness.h"

#include "l2flow/clickhouse/raw_consumer.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <curl/curl.h>
#include <limits>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::clickhouse {
namespace {

constexpr std::size_t kMaximumHttpResponseBytes = 64U * 1'024U;
constexpr std::uint32_t kFreshnessSchemaVersion = 1U;
constexpr std::size_t kFreshnessInputRowBinaryBytes = 179U;
constexpr std::string_view kFreshnessInputColumns =
    "source_instance_id,feed_session_epoch,calculation_run_id,domain,"
    "publication_sequence,frontier_id,barrier_lsn,barrier_batch_sequence,"
    "barrier_row_index,canonical_lsn,canonical_batch_sequence,"
    "canonical_row_index,raw_lsn,raw_batch_sequence,raw_row_index,"
    "event_lsn,event_batch_sequence,event_row_index,kline_lsn,"
    "kline_batch_sequence,kline_row_index,"
    "continuity_state,authoritative,publisher_instance_id,schema_version";
constexpr std::string_view kFreshnessInputStructure =
    "source_instance_id FixedString(16),feed_session_epoch UInt64,"
    "calculation_run_id FixedString(16),domain UInt8,"
    "publication_sequence UInt64,frontier_id UInt64,barrier_lsn UInt64,"
    "barrier_batch_sequence UInt64,barrier_row_index UInt32,"
    "canonical_lsn UInt64,canonical_batch_sequence UInt64,"
    "canonical_row_index UInt32,raw_lsn UInt64,raw_batch_sequence UInt64,"
    "raw_row_index UInt32,event_lsn UInt64,event_batch_sequence UInt64,"
    "event_row_index UInt32,kline_lsn UInt64,kline_batch_sequence UInt64,"
    "kline_row_index UInt32,continuity_state UInt8,authoritative Bool,"
    "publisher_instance_id FixedString(16),schema_version UInt32";

[[nodiscard]] bool IsZero(common::Identifier128 identifier) noexcept {
    return std::all_of(identifier.bytes.begin(), identifier.bytes.end(),
                       [](std::byte value) {
                           return value == std::byte{0U};
                       });
}

[[nodiscard]] bool IsIdentifierName(std::string_view value) noexcept {
    if (value.empty() ||
        !((value.front() >= 'A' && value.front() <= 'Z') ||
          (value.front() >= 'a' && value.front() <= 'z') ||
          value.front() == '_')) {
        return false;
    }
    return std::all_of(value.begin() + 1, value.end(), [](char character) {
        return (character >= 'A' && character <= 'Z') ||
               (character >= 'a' && character <= 'z') ||
               (character >= '0' && character <= '9') || character == '_';
    });
}

template <typename T>
void AppendScalar(std::vector<std::byte>* output, T value) {
    static_assert(std::is_integral_v<T> || std::is_enum_v<T>);
    if constexpr (std::is_enum_v<T>) {
        using Stored = std::underlying_type_t<T>;
        const Stored stored = static_cast<Stored>(value);
        const std::size_t offset = output->size();
        output->resize(offset + sizeof(stored));
        std::memcpy(output->data() + offset, &stored, sizeof(stored));
    } else {
        const std::size_t offset = output->size();
        output->resize(offset + sizeof(value));
        std::memcpy(output->data() + offset, &value, sizeof(value));
    }
}

void AppendIdentifier(std::vector<std::byte>* output,
                      common::Identifier128 identifier) {
    output->insert(output->end(), identifier.bytes.begin(),
                   identifier.bytes.end());
}

void AppendFreshnessRow(std::vector<std::byte>* output,
                        const FreshnessClickHouseConfig& config,
                        const outbox::FreshnessSnapshot& snapshot,
                        common::Identifier128 calculation_run_id,
                        std::uint8_t domain,
                        bool authoritative,
                        std::uint64_t publication_sequence) {
    AppendIdentifier(output, config.source_instance_id);
    AppendScalar(output, config.feed_session_epoch);
    AppendIdentifier(output, calculation_run_id);
    AppendScalar(output, domain);
    AppendScalar(output, publication_sequence);
    AppendScalar(output, snapshot.frontier_id);
    AppendScalar(output, snapshot.barrier.lsn);
    AppendScalar(output, snapshot.barrier.batch_sequence);
    AppendScalar(output, snapshot.barrier.row_index);
    AppendScalar(output, snapshot.canonical_tail.lsn);
    AppendScalar(output, snapshot.canonical_tail.batch_sequence);
    AppendScalar(output, snapshot.canonical_tail.row_index);
    AppendScalar(output, snapshot.raw_cursor.lsn);
    AppendScalar(output, snapshot.raw_cursor.batch_sequence);
    AppendScalar(output, snapshot.raw_cursor.row_index);
    AppendScalar(output, snapshot.event_cursor.lsn);
    AppendScalar(output, snapshot.event_cursor.batch_sequence);
    AppendScalar(output, snapshot.event_cursor.row_index);
    AppendScalar(output, snapshot.kline_cursor.lsn);
    AppendScalar(output, snapshot.kline_cursor.batch_sequence);
    AppendScalar(output, snapshot.kline_cursor.row_index);
    AppendScalar(output, snapshot.state);
    AppendScalar(output, static_cast<std::uint8_t>(authoritative ? 1U : 0U));
    AppendIdentifier(output, config.publisher_instance_id);
    AppendScalar(output, kFreshnessSchemaVersion);
}

struct HttpResult final {
    CURLcode curl_code = CURLE_OK;
    long status_code = 0L;
    std::string response;
    std::string transport_error;
};

size_t CaptureResponse(char* data,
                       std::size_t size,
                       std::size_t count,
                       void* context) noexcept {
    if (data == nullptr || context == nullptr || size == 0U ||
        count > std::numeric_limits<std::size_t>::max() / size) {
        return 0U;
    }
    const std::size_t bytes = size * count;
    std::string* response = static_cast<std::string*>(context);
    if (response->size() > kMaximumHttpResponseBytes ||
        bytes > kMaximumHttpResponseBytes - response->size()) {
        return 0U;
    }
    try {
        response->append(data, bytes);
        return bytes;
    } catch (...) {
        return 0U;
    }
}

class CurlHeaders final {
public:
    ~CurlHeaders() { ::curl_slist_free_all(head_); }

    [[nodiscard]] bool Append(const char* value) noexcept {
        curl_slist* const next = ::curl_slist_append(head_, value);
        if (next == nullptr) {
            return false;
        }
        head_ = next;
        return true;
    }
    [[nodiscard]] curl_slist* get() const noexcept { return head_; }

private:
    curl_slist* head_ = nullptr;
};

class HttpClient final {
public:
    explicit HttpClient(const FreshnessClickHouseConfig& config)
        : config_(config), handle_(::curl_easy_init()) {
        if (handle_ == nullptr) {
            throw std::runtime_error("curl_easy_init failed");
        }
    }
    ~HttpClient() { ::curl_easy_cleanup(handle_); }

    [[nodiscard]] HttpResult ExecuteSql(std::string_view sql) {
        const std::string url = config_.endpoint + "/?query=" + Escape(sql) +
                                "&wait_end_of_query=1";
        return Perform(url, {});
    }

    [[nodiscard]] HttpResult Insert(std::string_view query_id,
                                    std::string_view dedup_token,
                                    std::span<const std::byte> payload) {
        const std::string query = DerivedFreshnessInsertSql(
            config_.database, config_.lease_ns);
        const std::string url = config_.endpoint + "/?query=" + Escape(query) +
            "&query_id=" + Escape(query_id) +
            "&async_insert=0&wait_end_of_query=1&insert_deduplicate=1" +
            "&insert_deduplication_token=" + Escape(dedup_token) +
            "&insert_quorum=" + std::to_string(config_.insert_quorum) +
            "&insert_quorum_parallel=" +
            (config_.insert_quorum_parallel ? "1" : "0");
        return Perform(url, payload);
    }

private:
    [[nodiscard]] std::string Escape(std::string_view value) {
        if (value.size() > static_cast<std::size_t>(
                               std::numeric_limits<int>::max())) {
            throw std::length_error("ClickHouse URL value is too large");
        }
        char* const escaped = ::curl_easy_escape(
            handle_, value.data(), static_cast<int>(value.size()));
        if (escaped == nullptr) {
            throw std::bad_alloc();
        }
        std::string result(escaped);
        ::curl_free(escaped);
        return result;
    }

    [[nodiscard]] HttpResult Perform(std::string_view url,
                                     std::span<const std::byte> payload) {
        HttpResult result{};
        if (payload.size() > static_cast<std::size_t>(
                                 std::numeric_limits<curl_off_t>::max())) {
            result.curl_code = CURLE_FILESIZE_EXCEEDED;
            result.transport_error = "freshness payload is too large";
            return result;
        }
        std::array<char, CURL_ERROR_SIZE> error_buffer{};
        CurlHeaders headers;
        if (!headers.Append("Content-Type: application/octet-stream") ||
            !headers.Append("Expect:")) {
            result.curl_code = CURLE_OUT_OF_MEMORY;
            return result;
        }
        ::curl_easy_reset(handle_);
        const auto set = [this](CURLoption option, auto value) {
            return ::curl_easy_setopt(handle_, option, value) == CURLE_OK;
        };
        const bool configured =
            set(CURLOPT_URL, url.data()) && set(CURLOPT_POST, 1L) &&
            set(CURLOPT_POSTFIELDS,
                reinterpret_cast<const char*>(payload.data())) &&
            set(CURLOPT_POSTFIELDSIZE_LARGE,
                static_cast<curl_off_t>(payload.size())) &&
            set(CURLOPT_HTTPHEADER, headers.get()) &&
            set(CURLOPT_USERNAME, config_.username.c_str()) &&
            set(CURLOPT_PASSWORD, config_.password.c_str()) &&
            set(CURLOPT_NOPROXY, config_.no_proxy.c_str()) &&
            set(CURLOPT_CONNECTTIMEOUT_MS,
                static_cast<long>(config_.connect_timeout_ms)) &&
            set(CURLOPT_TIMEOUT_MS,
                static_cast<long>(config_.request_timeout_ms)) &&
            set(CURLOPT_NOSIGNAL, 1L) && set(CURLOPT_TCP_KEEPALIVE, 1L) &&
            set(CURLOPT_SSL_VERIFYPEER,
                config_.tls_verify_peer ? 1L : 0L) &&
            set(CURLOPT_SSL_VERIFYHOST,
                config_.tls_verify_peer ? 2L : 0L) &&
            set(CURLOPT_WRITEFUNCTION, &CaptureResponse) &&
            set(CURLOPT_WRITEDATA, &result.response) &&
            set(CURLOPT_ERRORBUFFER, error_buffer.data()) &&
            set(CURLOPT_USERAGENT, "l2flow-clickhouse-freshness/1");
        if (!configured) {
            result.curl_code = CURLE_FAILED_INIT;
            return result;
        }
        result.curl_code = ::curl_easy_perform(handle_);
        if (result.curl_code != CURLE_OK) {
            result.transport_error = error_buffer.front() != '\0'
                ? error_buffer.data()
                : ::curl_easy_strerror(result.curl_code);
            return result;
        }
        static_cast<void>(::curl_easy_getinfo(
            handle_, CURLINFO_RESPONSE_CODE, &result.status_code));
        return result;
    }

    const FreshnessClickHouseConfig& config_;
    CURL* handle_ = nullptr;
};

[[nodiscard]] bool HttpSucceeded(const HttpResult& result) noexcept {
    return result.curl_code == CURLE_OK && result.status_code >= 200L &&
           result.status_code < 300L;
}

[[nodiscard]] std::string HttpErrorText(const HttpResult& result) {
    if (result.curl_code != CURLE_OK) {
        return "transport error: " + result.transport_error;
    }
    return "HTTP " + std::to_string(result.status_code) + ": " +
           result.response;
}

void InitializeCurl() {
    static std::once_flag once;
    static CURLcode initialization = CURLE_FAILED_INIT;
    std::call_once(once, [] {
        initialization = ::curl_global_init(CURL_GLOBAL_DEFAULT);
    });
    if (initialization != CURLE_OK) {
        throw std::runtime_error("curl_global_init failed");
    }
}

}  // namespace

std::string DerivedFreshnessInsertSql(std::string_view database,
                                      std::uint64_t lease_ns) {
    constexpr std::string_view kMaximumUInt64 = "18446744073709551615";
    const std::string lease = std::to_string(lease_ns);
    // Keep the saturating addition in UInt128. ClickHouse may infer a signed
    // result for UInt64 subtraction near UINT64_MAX.
    return "INSERT INTO " + std::string(database) +
           ".derived_freshness_log (" +
           std::string(kFreshnessInputColumns) +
           ",observed_utc_ns,valid_until_utc_ns) "
           "WITH toUInt64(toUnixTimestamp64Nano(now64(9))) AS "
           "_l2flow_server_utc_ns "
           "SELECT *,_l2flow_server_utc_ns,"
           "toUInt64(least(toUInt128(_l2flow_server_utc_ns)+toUInt128('" +
           lease + "'),toUInt128('" + std::string(kMaximumUInt64) +
           "'))) FROM input('" + std::string(kFreshnessInputStructure) +
           "') FORMAT RowBinary";
}

std::string DerivedFreshnessLogDdl(std::string_view database) {
    return "CREATE TABLE IF NOT EXISTS " + std::string(database) +
           ".derived_freshness_log ("
           "source_instance_id FixedString(16), feed_session_epoch UInt64, "
           "calculation_run_id FixedString(16), domain UInt8, "
           "publication_sequence UInt64, frontier_id UInt64, "
           "barrier_lsn UInt64, barrier_batch_sequence UInt64, "
           "barrier_row_index UInt32, canonical_lsn UInt64, "
           "canonical_batch_sequence UInt64, canonical_row_index UInt32, "
           "raw_lsn UInt64, raw_batch_sequence UInt64, raw_row_index UInt32, "
           "event_lsn UInt64, event_batch_sequence UInt64, "
           "event_row_index UInt32, kline_lsn UInt64, "
           "kline_batch_sequence UInt64, kline_row_index UInt32, "
           "continuity_state UInt8, "
           "authoritative Bool, observed_utc_ns UInt64, "
           "valid_until_utc_ns UInt64, "
           "publisher_instance_id FixedString(16), schema_version UInt32) "
           "ENGINE = MergeTree ORDER BY "
           "(calculation_run_id,domain,publication_sequence) SETTINGS "
           "non_replicated_deduplication_window=10000";
}

std::string DerivedFreshnessTableProbe(std::string_view database) {
    return "SELECT throwIf(count() != 1 OR "
           "countIf(engine NOT IN ('MergeTree','ReplicatedMergeTree')) != 0 "
           "OR countIf(sorting_key!='calculation_run_id, domain, "
           "publication_sequence') != 0, "
           "'derived freshness table contract mismatch') FROM system.tables "
           "WHERE database='" + std::string(database) +
           "' AND name='derived_freshness_log' FORMAT Null";
}

std::string DerivedFreshnessColumnProbe(std::string_view database) {
    return "SELECT throwIf(count() != 27 OR "
           "countIf(name='source_instance_id' AND type='FixedString(16)') != 1 "
           "OR countIf(name='feed_session_epoch' AND type='UInt64') != 1 "
           "OR countIf(name='calculation_run_id' AND type='FixedString(16)') != 1 "
           "OR countIf(name='domain' AND type='UInt8') != 1 "
           "OR countIf(name='publication_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='frontier_id' AND type='UInt64') != 1 "
           "OR countIf(name='barrier_lsn' AND type='UInt64') != 1 "
           "OR countIf(name='barrier_batch_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='barrier_row_index' AND type='UInt32') != 1 "
           "OR countIf(name='canonical_lsn' AND type='UInt64') != 1 "
           "OR countIf(name='canonical_batch_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='canonical_row_index' AND type='UInt32') != 1 "
           "OR countIf(name='raw_lsn' AND type='UInt64') != 1 "
           "OR countIf(name='raw_batch_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='raw_row_index' AND type='UInt32') != 1 "
           "OR countIf(name='event_lsn' AND type='UInt64') != 1 "
           "OR countIf(name='event_batch_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='event_row_index' AND type='UInt32') != 1 "
           "OR countIf(name='kline_lsn' AND type='UInt64') != 1 "
           "OR countIf(name='kline_batch_sequence' AND type='UInt64') != 1 "
           "OR countIf(name='kline_row_index' AND type='UInt32') != 1 "
           "OR countIf(name='continuity_state' AND type='UInt8') != 1 "
           "OR countIf(name='authoritative' AND type='Bool') != 1 "
           "OR countIf(name='observed_utc_ns' AND type='UInt64') != 1 "
           "OR countIf(name='valid_until_utc_ns' AND type='UInt64') != 1 "
           "OR countIf(name='publisher_instance_id' AND type='FixedString(16)') != 1 "
           "OR countIf(name='schema_version' AND type='UInt32') != 1, "
           "'derived freshness column contract mismatch') FROM system.columns "
           "WHERE database='" + std::string(database) +
           "' AND table='derived_freshness_log' FORMAT Null";
}

bool ValidateFreshnessClickHouseConfig(
    const FreshnessClickHouseConfig& config,
    std::string* error) noexcept {
    const auto fail = [error](const char* message) noexcept {
        if (error != nullptr) {
            try {
                *error = message;
            } catch (...) {
            }
        }
        return false;
    };
    const std::string_view endpoint = config.endpoint;
    const bool http = endpoint.starts_with("http://");
    const bool https = endpoint.starts_with("https://");
    if ((!http && !https) || endpoint.find('?') != std::string::npos ||
        endpoint.find('#') != std::string::npos) {
        return fail("freshness endpoint must be an HTTP(S) base URL");
    }
    const std::size_t authority_begin = endpoint.find("://") + 3U;
    const std::size_t authority_end = endpoint.find('/', authority_begin);
    const std::size_t authority_size =
        (authority_end == std::string::npos ? endpoint.size()
                                            : authority_end) -
        authority_begin;
    if (authority_size == 0U ||
        endpoint.substr(authority_begin, authority_size).find('@') !=
            std::string::npos ||
        !IsIdentifierName(config.database) || config.username.empty() ||
        IsZero(config.source_instance_id) ||
        config.feed_session_epoch == 0U ||
        IsZero(config.publisher_instance_id) ||
        (!config.event_enabled && !config.kline_enabled) ||
        (config.event_enabled && IsZero(config.event_calculation_run_id)) ||
        (config.kline_enabled && IsZero(config.kline_calculation_run_id)) ||
        config.lease_ns == 0U || config.connect_timeout_ms == 0U ||
        config.request_timeout_ms == 0U) {
        return fail("invalid ClickHouse freshness configuration");
    }
    if (error != nullptr) {
        try {
            error->clear();
        } catch (...) {
        }
    }
    return true;
}

class FreshnessClickHousePublisher::Impl final {
public:
    explicit Impl(FreshnessClickHouseConfig config)
        : config_(std::move(config)) {}

    [[nodiscard]] bool Publish(const outbox::FreshnessSnapshot& snapshot,
                               std::string* error) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        try {
            InitializeCurl();
            HttpClient client(config_);
            if (!schema_ready_) {
                const auto execute = [&client, error](const std::string& sql) {
                    const HttpResult result = client.ExecuteSql(sql);
                    if (!HttpSucceeded(result)) {
                        if (error != nullptr) {
                            *error = HttpErrorText(result);
                        }
                        return false;
                    }
                    return true;
                };
                if (config_.ensure_local_tables &&
                    (!execute("CREATE DATABASE IF NOT EXISTS " +
                              config_.database) ||
                     !execute(DerivedFreshnessLogDdl(config_.database)))) {
                    return false;
                }
                if (!execute(DerivedFreshnessTableProbe(config_.database)) ||
                    !execute(DerivedFreshnessColumnProbe(config_.database))) {
                    return false;
                }
                schema_ready_ = true;
            }

            if (next_publication_sequence_ == 0U) {
                if (error != nullptr) {
                    *error = "freshness publication sequence exhausted";
                }
                return false;
            }
            const std::uint64_t sequence = next_publication_sequence_++;
            std::vector<std::byte> payload;
            payload.reserve(
                (config_.event_enabled ? kFreshnessInputRowBinaryBytes : 0U) +
                (config_.kline_enabled ? kFreshnessInputRowBinaryBytes : 0U));
            if (config_.event_enabled) {
                AppendFreshnessRow(
                    &payload, config_, snapshot,
                    config_.event_calculation_run_id, 1U,
                    snapshot.event_current_authoritative, sequence);
            }
            if (config_.kline_enabled) {
                AppendFreshnessRow(
                    &payload, config_, snapshot,
                    config_.kline_calculation_run_id, 2U,
                    snapshot.kline_current_authoritative, sequence);
            }
            const std::string suffix =
                IdentifierString(config_.publisher_instance_id) + "/" +
                std::to_string(sequence);
            const HttpResult result = client.Insert(
                "l2flow/derived_freshness/" + suffix,
                "l2flow/derived_freshness/" + suffix, payload);
            if (!HttpSucceeded(result)) {
                if (error != nullptr) {
                    *error = HttpErrorText(result);
                }
                return false;
            }
            if (error != nullptr) {
                error->clear();
            }
            return true;
        } catch (const std::exception& exception) {
            if (error != nullptr) {
                try {
                    *error = exception.what();
                } catch (...) {
                }
            }
            return false;
        }
    }

    [[nodiscard]] const FreshnessClickHouseConfig& config() const noexcept {
        return config_;
    }

private:
    FreshnessClickHouseConfig config_;
    std::mutex mutex_;
    std::uint64_t next_publication_sequence_ = 1U;
    bool schema_ready_ = false;
};

FreshnessClickHousePublisher::FreshnessClickHousePublisher(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

FreshnessClickHousePublisher::~FreshnessClickHousePublisher() = default;

std::unique_ptr<FreshnessClickHousePublisher>
FreshnessClickHousePublisher::Create(FreshnessClickHouseConfig config,
                                     std::string* error) {
    if (!ValidateFreshnessClickHouseConfig(config, error)) {
        return nullptr;
    }
    try {
        return std::unique_ptr<FreshnessClickHousePublisher>(
            new FreshnessClickHousePublisher(
                std::make_unique<Impl>(std::move(config))));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = exception.what();
        }
        return nullptr;
    }
}

bool FreshnessClickHousePublisher::Publish(
    const outbox::FreshnessSnapshot& snapshot,
    std::string* error) noexcept {
    return impl_->Publish(snapshot, error);
}

const FreshnessClickHouseConfig&
FreshnessClickHousePublisher::config() const noexcept {
    return impl_->config();
}

}  // namespace l2flow::clickhouse
