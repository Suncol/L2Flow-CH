#pragma once

#include "l2flow/ingest/engine.h"

#include "mdl_api.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace l2flow::ingest {

enum class MdlConnectionBoundaryReason : std::uint8_t {
    kNone = 0U,
    kConnectError = 1U,
    kDisconnected = 2U,
    kServiceTimeout = 3U,
    kMessageDiscarded = 4U,
    kSubscriptionRejected = 5U,
    kControlProtocolError = 6U,
    kReadyTimeout = 7U,
};

[[nodiscard]] constexpr MdlConnectionBoundaryReason
ClassifyMdlConnectionBoundary(const MessageKey& key) noexcept {
    if (key == MessageKey{1U, 101U, 2U}) {
        return MdlConnectionBoundaryReason::kConnectError;
    }
    if (key == MessageKey{1U, 101U, 3U}) {
        return MdlConnectionBoundaryReason::kDisconnected;
    }
    if (key == MessageKey{1U, 101U, 5U}) {
        return MdlConnectionBoundaryReason::kServiceTimeout;
    }
    if (key == MessageKey{1U, 101U, 6U}) {
        return MdlConnectionBoundaryReason::kMessageDiscarded;
    }
    return MdlConnectionBoundaryReason::kNone;
}

// ABI adapter only. The IOManager must be shut down (which quiesces vendor
// callbacks) before this handler and the engine are destroyed.
class MdlMessageHandler final : public datayes::mdl::MessageHandlerBase {
public:
    explicit MdlMessageHandler(
        IngestEngine* engine,
        StreamMask expected_streams = kDefaultAShareL2StreamMask) noexcept;
    void OnMessage(datayes::mdl::Subscriber* sender,
                   const datayes::mdl::MDLMessage* message) override;

    void StopAccepting() noexcept;
    [[nodiscard]] std::uint64_t active_callbacks() const noexcept;
    [[nodiscard]] AdmissionResult last_result() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] bool feed_ready() const noexcept;
    [[nodiscard]] std::uint64_t feed_ready_monotonic_ns() const noexcept;
    [[nodiscard]] std::uint64_t pre_ready_messages_discarded() const noexcept;
    [[nodiscard]] StreamMask expected_streams() const noexcept;
    [[nodiscard]] MdlConnectionBoundaryReason connection_boundary_reason()
        const noexcept;
    [[nodiscard]] std::uint64_t connection_boundary_monotonic_ns()
        const noexcept;
    [[nodiscard]] std::string connection_boundary_detail() const;

private:
    [[nodiscard]] bool WaitUntilReady(
        std::chrono::milliseconds timeout) noexcept;
    void ConfirmStreams(StreamMask streams,
                        bool successful_logon,
                        std::uint64_t observed_monotonic_ns) noexcept;
    void ClaimConnectionBoundary(
        MdlConnectionBoundaryReason reason,
        std::uint64_t observed_monotonic_ns,
        std::string_view detail) noexcept;
    void ClaimConnectionBoundaryLocked(
        MdlConnectionBoundaryReason reason,
        std::uint64_t observed_monotonic_ns,
        std::string_view detail) noexcept;

    enum class ReadinessState : std::uint8_t {
        kAwaiting,
        kReady,
        kClosed,
    };
    enum class MarketReadinessDecision : std::uint8_t {
        kAdmit,
        kDiscard,
        kClosed,
    };

    [[nodiscard]] MarketReadinessDecision CheckMarketReadiness(
        std::uint64_t observed_monotonic_ns);

    IngestEngine* engine_ = nullptr;
    StreamMask expected_streams_ = 0U;
    std::atomic<bool> accepting_{true};
    std::atomic<std::uint64_t> active_callbacks_{0U};
    std::atomic<AdmissionResult> last_result_{AdmissionResult::kAccepted};
    std::atomic<bool> failed_{false};
    std::atomic<bool> feed_ready_{false};
    std::atomic<std::uint64_t> feed_ready_monotonic_ns_{0U};
    std::atomic<std::uint64_t> pre_ready_messages_discarded_{0U};
    std::atomic<MdlConnectionBoundaryReason> connection_boundary_reason_{
        MdlConnectionBoundaryReason::kNone};
    std::atomic<std::uint64_t> connection_boundary_monotonic_ns_{0U};
    mutable std::mutex connection_mutex_;
    std::condition_variable connection_condition_;
    ReadinessState readiness_state_ = ReadinessState::kAwaiting;
    StreamMask confirmed_streams_ = 0U;
    bool logon_accepted_ = false;
    std::string connection_boundary_detail_;

    friend class PhysicalSdkSession;
    friend struct MdlMessageHandlerTestAccess;
};

struct PhysicalSdkConfig final {
    std::filesystem::path shared_library;
    std::string server_address;
    std::string user_name;
    int work_threads = 1;
    int io_threads = 1;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
    std::uint32_t ready_timeout_seconds = 30U;
    bool sdk_console_log = false;
    StreamMask enabled_streams = kDefaultAShareL2StreamMask;
};

class PhysicalSdkSession final {
public:
    ~PhysicalSdkSession();
    PhysicalSdkSession(const PhysicalSdkSession&) = delete;
    PhysicalSdkSession& operator=(const PhysicalSdkSession&) = delete;

    [[nodiscard]] static std::unique_ptr<PhysicalSdkSession> Connect(
        const PhysicalSdkConfig& config,
        MdlMessageHandler* handler,
        std::string* error);

    // Idempotent. IOManager::Shutdown is the callback-quiescence boundary.
    void Shutdown() noexcept;

private:
    class Impl;
    explicit PhysicalSdkSession(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ingest
