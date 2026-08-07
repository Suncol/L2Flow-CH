#pragma once

#include "l2flow/ingest/engine.h"

#include "mdl_api.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace l2flow::ingest {

// ABI adapter only. The IOManager must be shut down (which quiesces vendor
// callbacks) before this handler and the engine are destroyed.
class MdlMessageHandler final : public datayes::mdl::MessageHandlerBase {
public:
    explicit MdlMessageHandler(IngestEngine* engine) noexcept;
    void OnMessage(datayes::mdl::Subscriber* sender,
                   const datayes::mdl::MDLMessage* message) override;

    void StopAccepting() noexcept;
    [[nodiscard]] std::uint64_t active_callbacks() const noexcept;
    [[nodiscard]] AdmissionResult last_result() const noexcept;
    [[nodiscard]] bool failed() const noexcept;

private:
    IngestEngine* engine_ = nullptr;
    std::atomic<bool> accepting_{true};
    std::atomic<std::uint64_t> active_callbacks_{0U};
    std::atomic<AdmissionResult> last_result_{AdmissionResult::kAccepted};
    std::atomic<bool> failed_{false};
};

struct PhysicalSdkConfig final {
    std::filesystem::path shared_library;
    std::string server_address;
    std::string user_name;
    int work_threads = 1;
    int io_threads = 1;
    std::uint32_t heartbeat_interval_seconds = 10U;
    std::uint32_t heartbeat_timeout_seconds = 30U;
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
