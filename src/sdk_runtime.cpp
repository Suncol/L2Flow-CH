#include "l2flow/ingest/sdk_runtime.h"

#include "mdl_api_msg.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <exception>
#include <limits>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

namespace l2flow::ingest {
namespace {

static_assert(datayes::mdl::MDLSID_MDL_API == 1);
static_assert(datayes::mdl::mdl_api_msg::MDLVID_MDL_API == 101U);
static_assert(
    datayes::mdl::mdl_api_msg::MDLMID_MDL_API_ConnectErrorEvent == 2);
static_assert(
    datayes::mdl::mdl_api_msg::MDLMID_MDL_API_DisconnectedEvent == 3);
static_assert(
    datayes::mdl::mdl_api_msg::MDLMID_MDL_API_MessageServiceTimeOutEvent ==
    5);
static_assert(
    datayes::mdl::mdl_api_msg::MDLMID_MDL_API_MessageDiscardedEvent == 6);
static_assert(datayes::mdl::MDLSID_MDL_SYS == 2);
static_assert(datayes::mdl::mdl_sys_msg::MDLVID_MDL_SYS == 101U);
static_assert(
    datayes::mdl::mdl_sys_msg::MDLMID_MDL_SYS_LogonResponse == 2);
static_assert(
    datayes::mdl::mdl_sys_msg::MDLMID_MDL_SYS_SubscribeResponse == 23);

using LogonResponse = datayes::mdl::mdl_sys_msg::LogonResponse;
using SubscribeResponse = datayes::mdl::mdl_sys_msg::SubscribeResponse;
static_assert(sizeof(datayes::mdl::MDLAnsiString) == 6U);
static_assert(sizeof(LogonResponse) == 24U);
static_assert(offsetof(LogonResponse, UserName) == 0U);
static_assert(offsetof(LogonResponse, Password) == 6U);
static_assert(offsetof(LogonResponse, Services) == 12U);
static_assert(offsetof(LogonResponse, ReturnCode) == 20U);
static_assert(sizeof(LogonResponse::ServicesItem) == 16U);
static_assert(offsetof(LogonResponse::ServicesItem, Messages) == 8U);
static_assert(sizeof(LogonResponse::ServicesItem::MessagesItem) == 8U);
static_assert(sizeof(SubscribeResponse) == 8U);
static_assert(offsetof(SubscribeResponse, Services) == 0U);
static_assert(sizeof(SubscribeResponse::ServicesItem) == 16U);
static_assert(offsetof(SubscribeResponse::ServicesItem, Messages) == 8U);
static_assert(sizeof(SubscribeResponse::ServicesItem::MessagesItem) == 8U);

template <typename Unsigned>
[[nodiscard]] bool LoadLittleEndian(std::span<const std::byte> bytes,
                                    std::size_t offset,
                                    Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    static_assert(sizeof(Unsigned) <= sizeof(std::uint64_t));
    if (output == nullptr || offset > bytes.size() ||
        sizeof(Unsigned) > bytes.size() - offset) {
        return false;
    }
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
        value |= static_cast<std::uint64_t>(
                     std::to_integer<unsigned int>(bytes[offset + index]))
                 << static_cast<unsigned int>(index * 8U);
    }
    *output = static_cast<Unsigned>(value);
    return true;
}

struct WireList final {
    std::size_t start = 0U;
    std::uint32_t count = 0U;
};

struct WireRange final {
    std::size_t start = 0U;
    std::size_t end = 0U;
};

[[nodiscard]] bool ValidateWireString(
    std::span<const std::byte> body,
    std::size_t descriptor_offset) noexcept {
    std::uint16_t length = 0U;
    std::uint32_t relative = 0U;
    if (!LoadLittleEndian(body, descriptor_offset, &length) ||
        !LoadLittleEndian(body, descriptor_offset + sizeof(length),
                          &relative)) {
        return false;
    }
    if (length == 0U) {
        return true;
    }
    if (relative == 0U ||
        descriptor_offset >
            std::numeric_limits<std::size_t>::max() - relative) {
        return false;
    }
    const std::size_t start = descriptor_offset + relative;
    return start <= body.size() && length <= body.size() - start;
}

[[nodiscard]] bool RangesOverlap(const WireRange& left,
                                 const WireRange& right) noexcept {
    return left.start < right.end && right.start < left.end;
}

[[nodiscard]] bool ParseWireList(std::span<const std::byte> body,
                                 std::size_t descriptor_offset,
                                 std::size_t item_bytes,
                                 std::uint32_t maximum_items,
                                 WireList* output) noexcept {
    std::uint32_t count = 0U;
    std::uint32_t relative = 0U;
    if (output == nullptr || item_bytes == 0U ||
        !LoadLittleEndian(body, descriptor_offset, &count) ||
        !LoadLittleEndian(body, descriptor_offset + sizeof(count),
                          &relative) ||
        count > maximum_items) {
        return false;
    }
    if (count == 0U) {
        *output = {};
        return true;
    }
    if (relative == 0U ||
        descriptor_offset >
            std::numeric_limits<std::size_t>::max() - relative) {
        return false;
    }
    const std::size_t start = descriptor_offset + relative;
    if (count > std::numeric_limits<std::size_t>::max() / item_bytes) {
        return false;
    }
    const std::size_t bytes = static_cast<std::size_t>(count) * item_bytes;
    if (start > body.size() || bytes > body.size() - start) {
        return false;
    }
    *output = {start, count};
    return true;
}

enum class SystemResponseCode : std::uint8_t {
    kIgnored,
    kProgress,
    kRejected,
    kMalformed,
};

struct SystemResponseResult final {
    SystemResponseCode code = SystemResponseCode::kIgnored;
    StreamMask confirmed_streams = 0U;
    bool successful_logon = false;
    std::string detail;
};

[[nodiscard]] SystemResponseResult ParseSystemResponse(
    const ParsedHeader& header,
    std::span<const std::byte> body,
    StreamMask expected_streams) {
    SystemResponseResult result{};
    const bool logon =
        header.key == MessageKey{2U, 101U, 2U};
    const bool subscribe =
        header.key == MessageKey{2U, 101U, 23U};
    if (!logon && !subscribe) {
        return result;
    }
    result.code = SystemResponseCode::kMalformed;
    if (header.encoding != kBinaryEncoding) {
        result.detail = "MDL system response is not binary encoded";
        return result;
    }
    const std::size_t minimum_bytes =
        logon ? sizeof(LogonResponse) : sizeof(SubscribeResponse);
    const std::size_t services_descriptor =
        logon ? offsetof(LogonResponse, Services)
              : offsetof(SubscribeResponse, Services);
    if (body.size() < minimum_bytes) {
        result.detail = "MDL system response is truncated";
        return result;
    }
    if (logon) {
        if (!ValidateWireString(body, offsetof(LogonResponse, UserName)) ||
            !ValidateWireString(body, offsetof(LogonResponse, Password))) {
            result.detail =
                "MDL LogonResponse has an invalid string field";
            return result;
        }
        std::uint32_t return_code = 0U;
        if (!LoadLittleEndian(
                body, offsetof(LogonResponse, ReturnCode), &return_code)) {
            result.detail = "MDL LogonResponse return code is truncated";
            return result;
        }
        if (return_code != static_cast<std::uint32_t>(datayes::mdl::MDLEC_OK)) {
            result.code = SystemResponseCode::kRejected;
            result.detail = "MDL LogonResponse returned code " +
                            std::to_string(return_code);
            return result;
        }
    }

    constexpr std::uint32_t kMaximumServices = 256U;
    constexpr std::uint32_t kMaximumMessages = 4'096U;
    WireList services{};
    if (!ParseWireList(body, services_descriptor, 16U,
                       kMaximumServices, &services)) {
        result.detail = "MDL system response has an invalid service list";
        return result;
    }
    const std::size_t services_end =
        services.start + static_cast<std::size_t>(services.count) * 16U;
    if (services.count != 0U && services.start < minimum_bytes) {
        result.detail =
            "MDL system response service list overlaps its fixed fields";
        return result;
    }
    StreamMask seen = 0U;
    std::uint32_t total_messages = 0U;
    std::array<std::pair<std::uint32_t, std::uint32_t>,
               kMaximumServices> seen_services{};
    std::size_t seen_service_count = 0U;
    std::vector<WireRange> message_ranges;
    message_ranges.reserve(services.count);
    for (std::uint32_t service_index = 0U;
         service_index < services.count; ++service_index) {
        const std::size_t service_offset =
            services.start + static_cast<std::size_t>(service_index) * 16U;
        std::uint32_t service_id = 0U;
        std::uint32_t service_version = 0U;
        WireList messages{};
        if (!LoadLittleEndian(body, service_offset, &service_id) ||
            !LoadLittleEndian(body, service_offset + 4U, &service_version) ||
            !ParseWireList(body, service_offset + 8U, 8U,
                           kMaximumMessages, &messages) ||
            messages.count > kMaximumMessages - total_messages) {
            result.detail =
                "MDL system response has an invalid message list";
            return result;
        }
        const auto service_key = std::pair{service_id, service_version};
        if (std::find(seen_services.begin(),
                      seen_services.begin() +
                          static_cast<std::ptrdiff_t>(seen_service_count),
                      service_key) !=
            seen_services.begin() +
                static_cast<std::ptrdiff_t>(seen_service_count)) {
            result.detail =
                "MDL system response repeats a service status list";
            return result;
        }
        seen_services[seen_service_count++] = service_key;
        if (messages.count != 0U) {
            const WireRange range{
                messages.start,
                messages.start +
                    static_cast<std::size_t>(messages.count) * 8U};
            if (range.start < services_end ||
                std::any_of(message_ranges.begin(), message_ranges.end(),
                            [&range](const WireRange& existing) {
                                return RangesOverlap(range, existing);
                            })) {
                result.detail =
                    "MDL system response message lists overlap";
                return result;
            }
            message_ranges.push_back(range);
        }
        total_messages += messages.count;
        std::vector<std::uint32_t> seen_message_ids;
        seen_message_ids.reserve(messages.count);
        for (std::uint32_t message_index = 0U;
             message_index < messages.count; ++message_index) {
            const std::size_t message_offset =
                messages.start +
                static_cast<std::size_t>(message_index) * 8U;
            std::uint32_t message_id = 0U;
            std::uint32_t message_status = 0U;
            if (!LoadLittleEndian(body, message_offset, &message_id) ||
                !LoadLittleEndian(body, message_offset + 4U,
                                  &message_status)) {
                result.detail = "MDL subscription status is truncated";
                return result;
            }
            if (std::find(seen_message_ids.begin(), seen_message_ids.end(),
                          message_id) != seen_message_ids.end()) {
                result.detail =
                    "MDL system response repeats a message status";
                return result;
            }
            seen_message_ids.push_back(message_id);
            StreamMask bit = 0U;
            if (service_id <=
                    std::numeric_limits<std::uint8_t>::max() &&
                service_version <=
                    std::numeric_limits<std::uint16_t>::max() &&
                message_id <=
                    std::numeric_limits<std::uint16_t>::max()) {
                bit = StreamBit({
                    static_cast<std::uint8_t>(service_id),
                    static_cast<std::uint16_t>(service_version),
                    static_cast<std::uint16_t>(message_id)});
            }
            if ((bit & expected_streams) == 0U) {
                continue;
            }
            if ((seen & bit) != 0U) {
                result.detail =
                    "MDL system response repeats a subscription status";
                return result;
            }
            seen |= bit;
            if (message_status !=
                static_cast<std::uint32_t>(datayes::mdl::MDLEC_OK)) {
                std::ostringstream detail;
                detail << "MDL subscription " << service_id << '.'
                       << service_version << '.' << message_id
                       << " returned status " << message_status;
                result.code = SystemResponseCode::kRejected;
                result.detail = detail.str();
                return result;
            }
            result.confirmed_streams |= bit;
        }
    }
    result.code = SystemResponseCode::kProgress;
    result.successful_logon = logon;
    return result;
}

class ActiveCallbackGuard final {
public:
    explicit ActiveCallbackGuard(
        std::atomic<std::uint64_t>* counter) noexcept
        : counter_(counter) {
        counter_->fetch_add(1U, std::memory_order_acq_rel);
    }
    ~ActiveCallbackGuard() {
        counter_->fetch_sub(1U, std::memory_order_acq_rel);
    }

private:
    std::atomic<std::uint64_t>* counter_;
};

void SetError(std::string* error, std::string value) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(value);
    } catch (...) {
    }
}

[[nodiscard]] bool ValidCStringInput(const std::string& value,
                                     bool require_nonempty) noexcept {
    return (!require_nonempty || !value.empty()) &&
           value.find('\0') == std::string::npos;
}

class LocalDlHandle final {
public:
    explicit LocalDlHandle(void* handle) noexcept : handle_(handle) {}
    ~LocalDlHandle() {
        if (handle_ != nullptr) {
            static_cast<void>(::dlclose(handle_));
        }
    }
    [[nodiscard]] void* get() const noexcept { return handle_; }
    [[nodiscard]] void* release() noexcept {
        return std::exchange(handle_, nullptr);
    }

private:
    void* handle_ = nullptr;
};

}  // namespace

MdlMessageHandler::MdlMessageHandler(
    IngestEngine* engine,
    StreamMask expected_streams) noexcept
    : engine_(engine),
      expected_streams_(expected_streams & kSupportedStreamMask) {}

void MdlMessageHandler::OnMessage(
    datayes::mdl::Subscriber* sender,
    const datayes::mdl::MDLMessage* message) {
    const std::uint64_t callback_entry_ns = MonotonicNowNs();
    static_cast<void>(sender);
    ActiveCallbackGuard active(&active_callbacks_);
    bool accepting = true;
    // A read-modify-write gate gives every callback and the first connection
    // boundary one total order. Callbacks linearized before closure may drain;
    // callbacks linearized after it cannot enter the old feed epoch.
    if (!accepting_.compare_exchange_strong(
            accepting, true, std::memory_order_acq_rel,
            std::memory_order_acquire) ||
        engine_ == nullptr) {
        last_result_.store(
            AdmissionResult::kNotRunning, std::memory_order_release);
        return;
    }
    if (message == nullptr) {
        last_result_.store(
            AdmissionResult::kNullMessage, std::memory_order_release);
        return;
    }
    try {
        const datayes::mdl::MDLMessageHead* const vendor_head =
            message->GetHead();
        if (vendor_head == nullptr) {
            last_result_.store(
                AdmissionResult::kHeaderTruncated,
                std::memory_order_release);
            return;
        }
        std::array<std::byte, kMdlHeaderBytes> header{};
        static_assert(sizeof(datayes::mdl::MDLMessageHead) ==
                      kMdlHeaderBytes);
        static_assert(alignof(datayes::mdl::MDLMessageHead) == 1U);
        std::memcpy(header.data(), vendor_head, header.size());
        ParsedHeader parsed{};
        if (!ParseMdlHeader(header, &parsed) ||
            parsed.head_size != kMdlHeaderBytes ||
            parsed.message_size < kMdlHeaderBytes) {
            last_result_.store(
                AdmissionResult::kHeaderInvalid,
                std::memory_order_release);
            return;
        }
        const std::size_t body_size = static_cast<std::size_t>(
            parsed.message_size - kMdlHeaderBytes);
        const char* const body_pointer = message->GetBody();
        if (body_size != 0U && body_pointer == nullptr) {
            last_result_.store(
                AdmissionResult::kBodyTruncated,
                std::memory_order_release);
            return;
        }
        const auto body = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(body_pointer), body_size);
        if (parsed.key.service_id == 1U &&
            parsed.key.service_version == 101U) {
            const MdlConnectionBoundaryReason reason =
                ClassifyMdlConnectionBoundary(parsed.key);
            if (reason != MdlConnectionBoundaryReason::kNone) {
                ClaimConnectionBoundary(
                    reason, callback_entry_ns,
                    reason == MdlConnectionBoundaryReason::kConnectError
                        ? "MDL ConnectErrorEvent"
                    : reason == MdlConnectionBoundaryReason::kDisconnected
                        ? "MDL DisconnectedEvent"
                    : reason == MdlConnectionBoundaryReason::kServiceTimeout
                        ? "MDL MessageServiceTimeOutEvent"
                        : "MDL MessageDiscardedEvent");
            }
            last_result_.store(
                AdmissionResult::kAccepted, std::memory_order_release);
            return;
        }
        if (parsed.key.service_id == 2U &&
            parsed.key.service_version == 101U) {
            const SystemResponseResult response = ParseSystemResponse(
                parsed, body, expected_streams_);
            if (response.code == SystemResponseCode::kProgress) {
                ConfirmStreams(response.confirmed_streams,
                               response.successful_logon,
                               callback_entry_ns);
            } else if (response.code == SystemResponseCode::kRejected) {
                ClaimConnectionBoundary(
                    MdlConnectionBoundaryReason::kSubscriptionRejected,
                    callback_entry_ns, response.detail);
            } else if (response.code == SystemResponseCode::kMalformed) {
                ClaimConnectionBoundary(
                    MdlConnectionBoundaryReason::kControlProtocolError,
                    callback_entry_ns, response.detail);
            }
            last_result_.store(
                AdmissionResult::kAccepted, std::memory_order_release);
            return;
        }
        const MarketReadinessDecision readiness =
            CheckMarketReadiness(callback_entry_ns);
        if (readiness != MarketReadinessDecision::kAdmit) {
            last_result_.store(
                readiness == MarketReadinessDecision::kDiscard
                    ? AdmissionResult::kFeedNotReady
                    : AdmissionResult::kNotRunning,
                std::memory_order_release);
            return;
        }
        const AdmissionResult result = engine_->AdmitMdlMessage(
            header, body, callback_entry_ns);
        last_result_.store(result, std::memory_order_release);
    } catch (...) {
        failed_.store(true, std::memory_order_release);
        ClaimConnectionBoundary(
            MdlConnectionBoundaryReason::kControlProtocolError,
            callback_entry_ns, "SDK callback adapter exception");
        last_result_.store(
            AdmissionResult::kInternalFailure,
            std::memory_order_release);
    }
}

void MdlMessageHandler::StopAccepting() noexcept {
    static_cast<void>(
        accepting_.exchange(false, std::memory_order_acq_rel));
    connection_condition_.notify_all();
}

std::uint64_t MdlMessageHandler::active_callbacks() const noexcept {
    return active_callbacks_.load(std::memory_order_acquire);
}

AdmissionResult MdlMessageHandler::last_result() const noexcept {
    return last_result_.load(std::memory_order_acquire);
}

bool MdlMessageHandler::failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
}

bool MdlMessageHandler::feed_ready() const noexcept {
    return feed_ready_.load(std::memory_order_acquire);
}

std::uint64_t MdlMessageHandler::feed_ready_monotonic_ns() const noexcept {
    if (!feed_ready()) {
        return 0U;
    }
    return feed_ready_monotonic_ns_.load(std::memory_order_relaxed);
}

std::uint64_t MdlMessageHandler::pre_ready_messages_discarded()
    const noexcept {
    return pre_ready_messages_discarded_.load(std::memory_order_acquire);
}

StreamMask MdlMessageHandler::expected_streams() const noexcept {
    return expected_streams_;
}

MdlConnectionBoundaryReason
MdlMessageHandler::connection_boundary_reason() const noexcept {
    return connection_boundary_reason_.load(std::memory_order_acquire);
}

std::uint64_t MdlMessageHandler::connection_boundary_monotonic_ns()
    const noexcept {
    if (connection_boundary_reason() ==
        MdlConnectionBoundaryReason::kNone) {
        return 0U;
    }
    return connection_boundary_monotonic_ns_.load(
        std::memory_order_relaxed);
}

std::string MdlMessageHandler::connection_boundary_detail() const {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    return connection_boundary_detail_;
}

MdlMessageHandler::MarketReadinessDecision
MdlMessageHandler::CheckMarketReadiness(
    std::uint64_t observed_monotonic_ns) {
    if (feed_ready()) {
        return MarketReadinessDecision::kAdmit;
    }

    bool notify = false;
    MarketReadinessDecision decision = MarketReadinessDecision::kClosed;
    {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        const bool boundary_free =
            connection_boundary_reason_.load(std::memory_order_relaxed) ==
            MdlConnectionBoundaryReason::kNone;
        const bool accepting =
            accepting_.load(std::memory_order_acquire);
        if (readiness_state_ == ReadinessState::kReady &&
            boundary_free && accepting) {
            decision = MarketReadinessDecision::kAdmit;
        } else if (readiness_state_ == ReadinessState::kAwaiting &&
                   boundary_free && accepting) {
            if (engine_->config().start_mode == StartMode::kPartial) {
                pre_ready_messages_discarded_.fetch_add(
                    1U, std::memory_order_relaxed);
            } else {
                accepting_.store(false, std::memory_order_release);
                ClaimConnectionBoundaryLocked(
                    MdlConnectionBoundaryReason::kControlProtocolError,
                    observed_monotonic_ns,
                    "market data arrived before MDL feed readiness in "
                    "from-open mode");
                notify = true;
            }
            decision = MarketReadinessDecision::kDiscard;
        }
    }
    if (notify) {
        connection_condition_.notify_all();
    }
    return decision;
}

bool MdlMessageHandler::WaitUntilReady(
    std::chrono::milliseconds timeout) noexcept {
    bool notify = false;
    bool ready = false;
    try {
        std::unique_lock<std::mutex> lock(connection_mutex_);
        static_cast<void>(connection_condition_.wait_for(
            lock, timeout, [this] {
                return readiness_state_ != ReadinessState::kAwaiting ||
                       connection_boundary_reason_.load(
                           std::memory_order_acquire) !=
                           MdlConnectionBoundaryReason::kNone ||
                       failed_.load(std::memory_order_acquire) ||
                       !accepting_.load(std::memory_order_acquire);
            }));
        if (readiness_state_ == ReadinessState::kAwaiting &&
            connection_boundary_reason_.load(std::memory_order_relaxed) ==
                MdlConnectionBoundaryReason::kNone &&
            !failed_.load(std::memory_order_acquire) &&
            accepting_.load(std::memory_order_acquire)) {
            accepting_.store(false, std::memory_order_release);
            ClaimConnectionBoundaryLocked(
                MdlConnectionBoundaryReason::kReadyTimeout,
                MonotonicNowNs(),
                "timed out waiting for successful MDL subscription status");
            notify = true;
        }
        ready = readiness_state_ == ReadinessState::kReady &&
                connection_boundary_reason_.load(std::memory_order_relaxed) ==
                    MdlConnectionBoundaryReason::kNone &&
                !failed_.load(std::memory_order_acquire) &&
                accepting_.load(std::memory_order_acquire);
    } catch (...) {
        failed_.store(true, std::memory_order_release);
        notify = true;
    }
    if (notify) {
        connection_condition_.notify_all();
    }
    return ready;
}

void MdlMessageHandler::ConfirmStreams(
    StreamMask streams,
    bool successful_logon,
    std::uint64_t observed_monotonic_ns) noexcept {
    bool became_ready = false;
    try {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        if (readiness_state_ != ReadinessState::kAwaiting ||
            !accepting_.load(std::memory_order_acquire) ||
            connection_boundary_reason_.load(std::memory_order_relaxed) !=
                MdlConnectionBoundaryReason::kNone) {
            return;
        }
        confirmed_streams_ |= streams;
        logon_accepted_ = logon_accepted_ || successful_logon;
        if (!feed_ready_.load(std::memory_order_relaxed) &&
            logon_accepted_ && expected_streams_ != 0U &&
            (confirmed_streams_ & expected_streams_) == expected_streams_) {
            feed_ready_monotonic_ns_.store(
                observed_monotonic_ns, std::memory_order_relaxed);
            readiness_state_ = ReadinessState::kReady;
            feed_ready_.store(true, std::memory_order_release);
            became_ready = true;
        }
    } catch (...) {
        failed_.store(true, std::memory_order_release);
    }
    if (became_ready || failed()) {
        connection_condition_.notify_all();
    }
}

void MdlMessageHandler::ClaimConnectionBoundary(
    MdlConnectionBoundaryReason reason,
    std::uint64_t observed_monotonic_ns,
    std::string_view detail) noexcept {
    if (reason == MdlConnectionBoundaryReason::kNone) {
        return;
    }
    bool claimed = false;
    try {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        if (connection_boundary_reason_.load(std::memory_order_relaxed) !=
            MdlConnectionBoundaryReason::kNone) {
            return;
        }
        accepting_.store(false, std::memory_order_release);
        ClaimConnectionBoundaryLocked(
            reason, observed_monotonic_ns, detail);
        claimed = true;
    } catch (...) {
        failed_.store(true, std::memory_order_release);
    }
    if (claimed || failed()) {
        connection_condition_.notify_all();
    }
}

void MdlMessageHandler::ClaimConnectionBoundaryLocked(
    MdlConnectionBoundaryReason reason,
    std::uint64_t observed_monotonic_ns,
    std::string_view detail) noexcept {
    readiness_state_ = ReadinessState::kClosed;
    try {
        connection_boundary_detail_.assign(detail);
    } catch (...) {
        connection_boundary_detail_.clear();
    }
    connection_boundary_monotonic_ns_.store(
        observed_monotonic_ns, std::memory_order_relaxed);
    connection_boundary_reason_.store(reason, std::memory_order_release);
}

namespace {

void QuiesceSdkCallbacks(datayes::mdl::IOManager* manager,
                         MdlMessageHandler* handler) noexcept {
    if (handler != nullptr) {
        handler->StopAccepting();
    }
    if (manager != nullptr) {
        try {
            manager->Shutdown();
        } catch (...) {
            // Continuing destruction would leave the application handler as
            // a possible dangling callback target.
            std::terminate();
        }
    }
    if (handler != nullptr) {
        while (handler->active_callbacks() != 0U) {
            std::this_thread::yield();
        }
    }
}

}  // namespace

class PhysicalSdkSession::Impl final {
public:
    using CreateFunction =
        datayes::mdl::IOManager* (*)(std::uint32_t, int, int);

    Impl(void* library,
         datayes::mdl::IOManager* manager,
         datayes::mdl::Subscriber* subscriber,
         MdlMessageHandler* handler) noexcept
        : library_(library),
          manager_(manager),
          subscriber_(subscriber),
          handler_(handler) {}

    ~Impl() { Shutdown(); }

    void Shutdown() noexcept {
        if (shutdown_) {
            return;
        }
        shutdown_ = true;
        QuiesceSdkCallbacks(manager_, handler_);
        if (subscriber_ != nullptr) {
            try {
                static_cast<void>(subscriber_->ReleaseRef());
            } catch (...) {
            }
            subscriber_ = nullptr;
        }
        if (manager_ != nullptr) {
            try {
                static_cast<void>(manager_->ReleaseRef());
            } catch (...) {
            }
            manager_ = nullptr;
        }
        // The vendor library is intentionally retained for process lifetime;
        // the SDK owns process-global state whose unload finalizers are not a
        // supported session boundary.
        static_cast<void>(library_);
    }

private:
    void* library_ = nullptr;
    datayes::mdl::IOManager* manager_ = nullptr;
    datayes::mdl::Subscriber* subscriber_ = nullptr;
    MdlMessageHandler* handler_ = nullptr;
    bool shutdown_ = false;
};

PhysicalSdkSession::PhysicalSdkSession(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

PhysicalSdkSession::~PhysicalSdkSession() = default;

std::unique_ptr<PhysicalSdkSession> PhysicalSdkSession::Connect(
    const PhysicalSdkConfig& config,
    MdlMessageHandler* handler,
    std::string* error) {
    if (handler == nullptr) {
        SetError(error, "MDL message handler is null");
        return nullptr;
    }
    const std::string library_path = config.shared_library.string();
    if (!ValidCStringInput(library_path, true) ||
        !ValidCStringInput(config.server_address, true) ||
        !ValidCStringInput(config.user_name, true) ||
        config.work_threads <= 0 || config.io_threads != 1 ||
        config.heartbeat_interval_seconds == 0U ||
        config.heartbeat_timeout_seconds == 0U ||
        config.ready_timeout_seconds == 0U ||
        config.enabled_streams == 0U ||
        (config.enabled_streams & ~kSupportedStreamMask) != 0U) {
        SetError(error,
                 "invalid SDK path, address, credential, heartbeat, or "
                 "thread configuration (io_threads must equal one)");
        return nullptr;
    }
    if (handler->expected_streams() != config.enabled_streams) {
        SetError(error,
                 "MDL handler expected streams do not match SDK enabled "
                 "streams");
        return nullptr;
    }
    try {
        ::dlerror();
#if defined(RTLD_DEEPBIND)
        // The vendor SDK statically embeds protobuf 2.5, while Arrow may load
        // a newer system protobuf into the process. Prefer the SDK's own
        // definitions so ELF symbol interposition cannot mix the two ABIs.
        constexpr int kSdkDlopenFlags =
            RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND;
#else
        constexpr int kSdkDlopenFlags = RTLD_NOW | RTLD_LOCAL;
#endif
        void* const raw_library =
            ::dlopen(library_path.c_str(), kSdkDlopenFlags);
        if (raw_library == nullptr) {
            const char* const detail = ::dlerror();
            SetError(error, std::string("dlopen SDK failed: ") +
                                (detail == nullptr ? "<no detail>" : detail));
            return nullptr;
        }
        LocalDlHandle library(raw_library);
        ::dlerror();
        void* const raw_symbol =
            ::dlsym(library.get(), "DllCreateIOManager");
        const char* const symbol_error = ::dlerror();
        if (symbol_error != nullptr || raw_symbol == nullptr) {
            SetError(error, std::string("dlsym DllCreateIOManager failed: ") +
                                (symbol_error == nullptr ? "<null symbol>"
                                                         : symbol_error));
            return nullptr;
        }
        Impl::CreateFunction create = nullptr;
        static_assert(sizeof(create) == sizeof(raw_symbol));
        std::memcpy(&create, &raw_symbol, sizeof(create));
        if (create == nullptr) {
            SetError(error, "DllCreateIOManager resolved to null");
            return nullptr;
        }
        datayes::mdl::IOManager* const manager = create(
            datayes::mdl::MDL_VERSION, config.work_threads,
            config.io_threads);
        if (manager == nullptr) {
            SetError(error, "DllCreateIOManager returned null");
            return nullptr;
        }
        // From this point the DSO is retained for process lifetime, including
        // failed composition paths where vendor object ownership could be
        // ambiguous after an exception.
        void* const lifetime_library = library.release();

        datayes::mdl::Subscriber* subscriber = nullptr;
        try {
            manager->EnableLog("mdl_ingestd", config.sdk_console_log);
            {
                datayes::mdl::SubscriberPtr temporary =
                    manager->CreateSubscriber(handler, false);
                if (temporary.IsNull()) {
                    QuiesceSdkCallbacks(manager, handler);
                    try {
                        static_cast<void>(manager->ReleaseRef());
                    } catch (...) {
                    }
                    SetError(error, "SDK CreateSubscriber returned null");
                    return nullptr;
                }
                subscriber = temporary.Duplicate();
            }
            subscriber->SetServerAddress(config.server_address.c_str());
            subscriber->SetUserName(config.user_name.c_str());
            subscriber->SetHeartbeatInterval(
                config.heartbeat_interval_seconds);
            subscriber->SetHeartbeatTimeout(
                config.heartbeat_timeout_seconds);
            subscriber->SetMessageEncoding(datayes::mdl::MDLEID_BINARY);
            subscriber->EnableMergeMessage(false);
            subscriber->SetSendMacAuth(false);
            subscriber->EnableServerSelect(false);
            for (const MessageKey& key : kSupportedMessageKeys) {
                if (StreamEnabled(config.enabled_streams, key)) {
                    subscriber->AddSubscription(
                        key.service_id, key.service_version,
                        key.message_id);
                }
            }
            const char* const connect_result = subscriber->Connect();
            if (connect_result != nullptr && connect_result[0] != '\0') {
                constexpr std::size_t kMaximumErrorBytes = 4'096U;
                const std::size_t length =
                    ::strnlen(connect_result, kMaximumErrorBytes + 1U);
                const std::string detail =
                    length <= kMaximumErrorBytes
                        ? std::string(connect_result, length)
                        : std::string("SDK error text exceeds 4096 bytes");
                handler->ClaimConnectionBoundary(
                    MdlConnectionBoundaryReason::kConnectError,
                    MonotonicNowNs(), detail);
                QuiesceSdkCallbacks(manager, handler);
                try {
                    static_cast<void>(subscriber->ReleaseRef());
                } catch (...) {
                }
                try {
                    static_cast<void>(manager->ReleaseRef());
                } catch (...) {
                }
                SetError(error, "SDK Connect failed: " + detail);
                return nullptr;
            }
            const auto ready_timeout =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::seconds(config.ready_timeout_seconds));
            if (!handler->WaitUntilReady(ready_timeout)) {
                const std::string detail =
                    handler->connection_boundary_detail();
                QuiesceSdkCallbacks(manager, handler);
                try {
                    static_cast<void>(subscriber->ReleaseRef());
                } catch (...) {
                }
                try {
                    static_cast<void>(manager->ReleaseRef());
                } catch (...) {
                }
                SetError(error,
                         detail.empty()
                             ? "SDK did not reach a ready subscription state"
                             : "SDK did not reach a ready subscription state: " +
                                   detail);
                return nullptr;
            }
        } catch (...) {
            QuiesceSdkCallbacks(manager, handler);
            if (subscriber != nullptr) {
                try {
                    static_cast<void>(subscriber->ReleaseRef());
                } catch (...) {
                }
            }
            try {
                static_cast<void>(manager->ReleaseRef());
            } catch (...) {
            }
            throw;
        }

        std::unique_ptr<Impl> impl;
        try {
            impl = std::make_unique<Impl>(
                lifetime_library, manager, subscriber, handler);
        } catch (...) {
            QuiesceSdkCallbacks(manager, handler);
            try {
                static_cast<void>(subscriber->ReleaseRef());
            } catch (...) {
            }
            try {
                static_cast<void>(manager->ReleaseRef());
            } catch (...) {
            }
            throw;
        }
        auto session = std::unique_ptr<PhysicalSdkSession>(
            new PhysicalSdkSession(std::move(impl)));
        SetError(error, {});
        return session;
    } catch (const std::exception& exception) {
        SetError(error, std::string("SDK composition failed: ") +
                            exception.what());
        return nullptr;
    } catch (...) {
        SetError(error, "SDK composition failed unexpectedly");
        return nullptr;
    }
}

void PhysicalSdkSession::Shutdown() noexcept {
    if (impl_ != nullptr) {
        impl_->Shutdown();
    }
}

}  // namespace l2flow::ingest
