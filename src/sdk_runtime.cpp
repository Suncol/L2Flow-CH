#include "l2flow/ingest/sdk_runtime.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace l2flow::ingest {
namespace {

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

MdlMessageHandler::MdlMessageHandler(IngestEngine* engine) noexcept
    : engine_(engine) {}

void MdlMessageHandler::OnMessage(
    datayes::mdl::Subscriber* sender,
    const datayes::mdl::MDLMessage* message) {
    const std::uint64_t callback_entry_ns = MonotonicNowNs();
    static_cast<void>(sender);
    ActiveCallbackGuard active(&active_callbacks_);
    if (!accepting_.load(std::memory_order_acquire) || engine_ == nullptr) {
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
        const AdmissionResult result = engine_->AdmitMdlMessage(
            header, body, callback_entry_ns);
        last_result_.store(result, std::memory_order_release);
    } catch (...) {
        failed_.store(true, std::memory_order_release);
        last_result_.store(
            AdmissionResult::kInternalFailure,
            std::memory_order_release);
    }
}

void MdlMessageHandler::StopAccepting() noexcept {
    accepting_.store(false, std::memory_order_release);
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
        config.enabled_streams == 0U ||
        (config.enabled_streams & ~kSupportedStreamMask) != 0U) {
        SetError(error,
                 "invalid SDK path, address, credential, heartbeat, or "
                 "thread configuration (io_threads must equal one)");
        return nullptr;
    }
    try {
        ::dlerror();
        void* const raw_library =
            ::dlopen(library_path.c_str(), RTLD_NOW | RTLD_LOCAL);
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
