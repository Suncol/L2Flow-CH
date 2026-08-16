#include "l2flow/outbox/consumers.h"

#include "l2flow/ingest/engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace l2flow::outbox {
namespace {

[[nodiscard]] bool IsProject(
    ingest::TickDispatchKind kind) noexcept {
    return kind == ingest::TickDispatchKind::kProjectOrdered ||
           kind == ingest::TickDispatchKind::kProjectHoleFill;
}

[[nodiscard]] std::uint64_t DeadlineAfter(
    std::uint64_t timeout_ns) noexcept {
    const std::uint64_t now = ingest::MonotonicNowNs();
    return timeout_ns > std::numeric_limits<std::uint64_t>::max() - now
        ? std::numeric_limits<std::uint64_t>::max()
        : now + timeout_ns;
}

}  // namespace

class RawOutboxConsumer::Impl final {
public:
    explicit Impl(RawOutboxConsumerConfig config) : config_(config) {}

    ~Impl() { Stop(); }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "raw outbox consumer can be started exactly once";
            }
            return false;
        }
        try {
            next_lsn_ = config_.outbox->consumer_cursor(
                            ConsumerKind::kRaw).lsn + 1U;
            thread_ = std::thread([this] { Run(); });
        } catch (const std::exception& exception) {
            Fail(std::string("raw outbox consumer start failed: ") +
                 exception.what());
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool DrainThrough(std::uint64_t lsn,
                                    std::uint64_t timeout_ns) noexcept {
        if (lsn == 0U) {
            return true;
        }
        const std::uint64_t deadline = DeadlineAfter(timeout_ns);
        while (healthy() && config_.outbox->healthy() &&
               config_.outbox->consumer_cursor(
                   ConsumerKind::kRaw).lsn < lsn) {
            if (ingest::MonotonicNowNs() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        return healthy() && config_.outbox->consumer_cursor(
                                ConsumerKind::kRaw).lsn >= lsn;
    }

    void Stop() noexcept {
        stopping_.store(true, std::memory_order_release);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire) &&
               config_.sink->healthy();
    }

    [[nodiscard]] std::string fatal_error() const {
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            if (!fatal_error_.empty()) {
                return fatal_error_;
            }
        }
        return config_.sink->fatal_error();
    }

    [[nodiscard]] std::uint64_t reader_lsn() const noexcept {
        return reader_lsn_.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] bool PollDue(std::uint64_t now) noexcept {
        for (std::size_t lane = 0U; lane < config_.tick_lanes; ++lane) {
            if (config_.sink->CanFlushTick(lane) &&
                !config_.sink->PollTick(lane, now)) {
                return false;
            }
        }
        for (std::size_t lane = 0U; lane < config_.snapshot_lanes; ++lane) {
            if (config_.sink->CanFlushSnapshot(lane) &&
                !config_.sink->PollSnapshot(lane, now)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool FlushAll() noexcept {
        for (std::size_t lane = 0U; lane < config_.tick_lanes; ++lane) {
            while (!stopping_.load(std::memory_order_acquire) && healthy() &&
                   !config_.sink->CanFlushTick(lane)) {
                if (!PollDue(ingest::MonotonicNowNs())) {
                    return false;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            if (stopping_.load(std::memory_order_acquire) || !healthy() ||
                !config_.sink->FlushTick(lane)) {
                return false;
            }
        }
        for (std::size_t lane = 0U; lane < config_.snapshot_lanes; ++lane) {
            while (!stopping_.load(std::memory_order_acquire) && healthy() &&
                   !config_.sink->CanFlushSnapshot(lane)) {
                if (!PollDue(ingest::MonotonicNowNs())) {
                    return false;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            if (stopping_.load(std::memory_order_acquire) || !healthy() ||
                !config_.sink->FlushSnapshot(lane)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool AppendTick(const RecordView& view) noexcept {
        const std::size_t lane = view.record->producer_lane;
        if (lane >= config_.tick_lanes) {
            return false;
        }
        while (!stopping_.load(std::memory_order_acquire) && healthy() &&
               !config_.sink->CanAppendTick(lane)) {
            if (!PollDue(ingest::MonotonicNowNs())) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        return !stopping_.load(std::memory_order_acquire) && healthy() &&
               config_.sink->AppendTick(
                                lane, view.position,
                                view.record->raw_tick);
    }

    [[nodiscard]] bool AppendSnapshot(const RecordView& view) noexcept {
        const std::size_t lane = view.record->producer_lane;
        if (lane >= config_.snapshot_lanes) {
            return false;
        }
        while (!stopping_.load(std::memory_order_acquire) && healthy() &&
               !config_.sink->CanAppendSnapshot(lane)) {
            if (!PollDue(ingest::MonotonicNowNs())) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        return !stopping_.load(std::memory_order_acquire) && healthy() &&
               config_.sink->AppendSnapshot(
                                lane, view.position,
                                view.record->raw_snapshot);
    }

    [[nodiscard]] bool Consume(const RecordView& view) noexcept {
        switch (view.record->kind) {
            case RecordKind::kTickOccurrence:
                return AppendTick(view);
            case RecordKind::kSnapshot:
                return AppendSnapshot(view);
            case RecordKind::kTickControl:
            case RecordKind::kGapDiagnostic:
            case RecordKind::kChannelFault:
                return config_.outbox->CompleteOne(
                    ConsumerKind::kRaw, view.position);
            case RecordKind::kFreshnessBarrier:
            case RecordKind::kFinalBarrier:
                return FlushAll() && config_.outbox->CompleteOne(
                    ConsumerKind::kRaw, view.position);
        }
        return false;
    }

    void Run() noexcept {
        std::uint64_t last_poll_ns = 0U;
        while (!stopping_.load(std::memory_order_acquire) && healthy()) {
            RecordView view{};
            if (config_.outbox->TryRead(next_lsn_, &view)) {
                if (!Consume(view)) {
                    if (stopping_.load(std::memory_order_acquire)) {
                        return;
                    }
                    Fail("raw outbox consumer could not consume WAL record");
                    return;
                }
                reader_lsn_.store(next_lsn_, std::memory_order_release);
                if (next_lsn_ == std::numeric_limits<std::uint64_t>::max()) {
                    Fail("raw outbox reader LSN exhausted");
                    return;
                }
                ++next_lsn_;
                continue;
            }
            const std::uint64_t now = ingest::MonotonicNowNs();
            if (last_poll_ns == 0U || now - last_poll_ns >= 50'000U) {
                if (!PollDue(now)) {
                    Fail("raw ClickHouse consumer poll failed");
                    return;
                }
                last_poll_ns = now;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }

    void Fail(std::string message) noexcept {
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
    }

    RawOutboxConsumerConfig config_{};
    std::thread thread_;
    std::uint64_t next_lsn_ = 1U;
    std::atomic<std::uint64_t> reader_lsn_{0U};
    std::atomic<bool> started_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> healthy_{true};
    mutable std::mutex error_mutex_;
    std::string fatal_error_;
};

class DerivedOutboxConsumer::Impl final {
public:
    struct BarrierLatch final {
        explicit BarrierLatch(std::size_t owners,
                              WalPosition value) noexcept
            : remaining(owners), position(value) {}

        std::atomic<std::size_t> remaining;
        std::atomic<bool> failed{false};
        WalPosition position{};
    };

    struct Task final {
        enum class Kind : std::uint8_t {
            kDispatch,
            kBarrier,
        };

        Kind kind = Kind::kDispatch;
        ingest::TickDispatch dispatch{};
        WalPosition position{};
        std::shared_ptr<BarrierLatch> barrier;
    };

    struct OwnerQueue final {
        std::mutex mutex;
        std::condition_variable ready;
        std::condition_variable available;
        std::deque<Task> tasks;
        std::thread thread;
    };

    explicit Impl(DerivedOutboxConsumerConfig config)
        : config_(config) {
        owners_.reserve(config_.owner_count);
        for (std::size_t owner = 0U; owner < config_.owner_count; ++owner) {
            owners_.push_back(std::make_unique<OwnerQueue>());
        }
    }

    ~Impl() { Stop(); }

    [[nodiscard]] bool Start(std::string* error) {
        bool expected = false;
        if (!started_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            if (error != nullptr) {
                *error = "derived outbox consumer can be started exactly once";
            }
            return false;
        }
        try {
            next_lsn_ = config_.outbox->consumer_cursor(Consumer()).lsn + 1U;
            for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
                owners_[owner]->thread = std::thread(
                    [this, owner] { OwnerLoop(owner); });
            }
            router_ = std::thread([this] { RouterLoop(); });
        } catch (const std::exception& exception) {
            Fail(std::string("derived outbox consumer start failed: ") +
                 exception.what());
            Stop();
            if (error != nullptr) {
                *error = fatal_error();
            }
            return false;
        }
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    [[nodiscard]] bool DrainThrough(std::uint64_t lsn,
                                    std::uint64_t timeout_ns) noexcept {
        if (lsn == 0U) {
            return true;
        }
        const std::uint64_t deadline = DeadlineAfter(timeout_ns);
        while (healthy() && config_.outbox->healthy() &&
               config_.outbox->consumer_cursor(Consumer()).lsn < lsn) {
            if (ingest::MonotonicNowNs() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        return healthy() &&
               config_.outbox->consumer_cursor(Consumer()).lsn >= lsn;
    }

    void Stop() noexcept {
        stopping_.store(true, std::memory_order_release);
        for (const auto& owner : owners_) {
            owner->ready.notify_all();
            owner->available.notify_all();
        }
        if (router_.joinable()) {
            router_.join();
        }
        for (const auto& owner : owners_) {
            if (owner->thread.joinable()) {
                owner->thread.join();
            }
        }
    }

    [[nodiscard]] bool healthy() const noexcept {
        if (!healthy_.load(std::memory_order_acquire)) {
            return false;
        }
        return config_.domain == DerivedDomain::kEvent
            ? config_.event_runtime->healthy()
            : config_.kline_runtime->healthy();
    }

    [[nodiscard]] std::string fatal_error() const {
        {
            std::lock_guard<std::mutex> lock(error_mutex_);
            if (!fatal_error_.empty()) {
                return fatal_error_;
            }
        }
        return config_.domain == DerivedDomain::kEvent
            ? config_.event_runtime->fatal_error()
            : config_.kline_runtime->fatal_error();
    }

    [[nodiscard]] std::uint64_t reader_lsn() const noexcept {
        return reader_lsn_.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] ConsumerKind Consumer() const noexcept {
        return config_.domain == DerivedDomain::kEvent
            ? ConsumerKind::kEvent
            : ConsumerKind::kKLine;
    }

    [[nodiscard]] bool RuntimeCanPoll(std::size_t owner) const noexcept {
        return config_.domain != DerivedDomain::kEvent ||
               config_.event_runtime->CanPollDispatch(owner);
    }

    [[nodiscard]] bool RuntimeAppend(
        std::size_t owner,
        const ingest::TickDispatch& dispatch,
        WalPosition position) noexcept {
        return config_.domain == DerivedDomain::kEvent
            ? config_.event_runtime->AppendDispatch(owner, dispatch, position)
            : config_.kline_runtime->AppendDispatch(owner, dispatch, position);
    }

    [[nodiscard]] bool RuntimeFlushDue(std::size_t owner) noexcept {
        const std::uint64_t now = ingest::MonotonicNowNs();
        return config_.domain == DerivedDomain::kEvent
            ? config_.event_runtime->FlushDue(owner, now)
            : config_.kline_runtime->FlushDue(owner, now);
    }

    [[nodiscard]] bool RuntimeFlush(std::size_t owner) noexcept {
        return config_.domain == DerivedDomain::kEvent
            ? config_.event_runtime->Flush(owner)
            : config_.kline_runtime->Flush(owner);
    }

    [[nodiscard]] bool Push(std::size_t owner, Task task) noexcept {
        if (owner >= owners_.size()) {
            return false;
        }
        OwnerQueue& queue = *owners_[owner];
        std::unique_lock<std::mutex> lock(queue.mutex);
        queue.available.wait(lock, [this, &queue] {
            return stopping_.load(std::memory_order_acquire) || !healthy() ||
                   queue.tasks.size() < config_.queue_records_per_owner;
        });
        if (stopping_.load(std::memory_order_acquire) || !healthy()) {
            return false;
        }
        try {
            queue.tasks.push_back(std::move(task));
        } catch (...) {
            return false;
        }
        lock.unlock();
        queue.ready.notify_one();
        return true;
    }

    [[nodiscard]] bool RouteBarrier(const RecordView& view) noexcept {
        std::shared_ptr<BarrierLatch> latch;
        try {
            latch = std::make_shared<BarrierLatch>(
                owners_.size(), view.position);
        } catch (...) {
            return false;
        }
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            Task task{};
            task.kind = Task::Kind::kBarrier;
            task.position = view.position;
            task.barrier = latch;
            if (!Push(owner, std::move(task))) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool Route(const RecordView& view) noexcept {
        switch (view.record->kind) {
            case RecordKind::kTickOccurrence:
                if (!view.record->catalog_match) {
                    return config_.outbox->CompleteOne(
                        Consumer(), view.position);
                }
                break;
            case RecordKind::kTickControl:
                break;
            case RecordKind::kFreshnessBarrier:
            case RecordKind::kFinalBarrier:
                return RouteBarrier(view);
            case RecordKind::kSnapshot:
            case RecordKind::kGapDiagnostic:
            case RecordKind::kChannelFault:
                return config_.outbox->CompleteOne(
                    Consumer(), view.position);
        }
        if (view.record->owner >= owners_.size()) {
            return false;
        }
        Task task{};
        task.kind = Task::Kind::kDispatch;
        task.dispatch = view.record->disposition;
        task.position = view.position;
        return Push(view.record->owner, std::move(task));
    }

    void RouterLoop() noexcept {
        while (!stopping_.load(std::memory_order_acquire) && healthy()) {
            RecordView view{};
            if (!config_.outbox->TryRead(next_lsn_, &view)) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }
            if (!Route(view)) {
                Fail("derived outbox router could not route WAL record");
                return;
            }
            reader_lsn_.store(next_lsn_, std::memory_order_release);
            if (next_lsn_ == std::numeric_limits<std::uint64_t>::max()) {
                Fail("derived outbox reader LSN exhausted");
                return;
            }
            ++next_lsn_;
        }
    }

    [[nodiscard]] bool Pop(std::size_t owner, Task* task) noexcept {
        OwnerQueue& queue = *owners_[owner];
        std::unique_lock<std::mutex> lock(queue.mutex);
        queue.ready.wait_for(lock, std::chrono::microseconds(100),
                             [this, &queue] {
            return stopping_.load(std::memory_order_acquire) ||
                   !healthy() || !queue.tasks.empty();
        });
        if (queue.tasks.empty()) {
            return false;
        }
        *task = std::move(queue.tasks.front());
        queue.tasks.pop_front();
        lock.unlock();
        queue.available.notify_one();
        return true;
    }

    [[nodiscard]] bool ApplyDispatch(std::size_t owner,
                                     const Task& task) noexcept {
        while (healthy() && !RuntimeCanPoll(owner)) {
            if (!RuntimeFlushDue(owner)) {
                return false;
            }
            std::this_thread::yield();
        }
        if (!healthy() ||
            !RuntimeAppend(owner, task.dispatch, task.position) ||
            !RuntimeFlushDue(owner)) {
            return false;
        }
        if (!IsProject(task.dispatch.kind)) {
            return config_.outbox->CompleteOne(
                Consumer(), task.position);
        }
        return true;
    }

    [[nodiscard]] bool ApplyBarrier(std::size_t owner,
                                    const Task& task) noexcept {
        const bool flushed = RuntimeFlush(owner);
        if (!flushed) {
            task.barrier->failed.store(true, std::memory_order_release);
        }
        if (task.barrier->remaining.fetch_sub(
                1U, std::memory_order_acq_rel) == 1U) {
            if (task.barrier->failed.load(std::memory_order_acquire) ||
                !config_.outbox->CompleteOne(
                    Consumer(), task.barrier->position)) {
                return false;
            }
        }
        return flushed;
    }

    void OwnerLoop(std::size_t owner) noexcept {
        while (!stopping_.load(std::memory_order_acquire) && healthy()) {
            Task task{};
            if (!Pop(owner, &task)) {
                if (!stopping_.load(std::memory_order_acquire) && healthy() &&
                    !RuntimeFlushDue(owner)) {
                    Fail("derived runtime service failed");
                    return;
                }
                continue;
            }
            const bool applied = task.kind == Task::Kind::kDispatch
                ? ApplyDispatch(owner, task)
                : ApplyBarrier(owner, task);
            if (!applied) {
                Fail("derived runtime could not apply durable WAL task");
                return;
            }
        }
    }

    void Fail(std::string message) noexcept {
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
        for (const auto& owner : owners_) {
            owner->ready.notify_all();
            owner->available.notify_all();
        }
    }

    DerivedOutboxConsumerConfig config_{};
    std::vector<std::unique_ptr<OwnerQueue>> owners_;
    std::thread router_;
    std::uint64_t next_lsn_ = 1U;
    std::atomic<std::uint64_t> reader_lsn_{0U};
    std::atomic<bool> started_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> healthy_{true};
    mutable std::mutex error_mutex_;
    std::string fatal_error_;
};

std::unique_ptr<RawOutboxConsumer> RawOutboxConsumer::Create(
    RawOutboxConsumerConfig config,
    std::string* error) {
    if (config.outbox == nullptr || config.sink == nullptr ||
        config.tick_lanes == 0U ||
        config.snapshot_lanes == 0U ||
        !config.outbox->consumer_enabled(ConsumerKind::kRaw)) {
        if (error != nullptr) {
            *error = "invalid raw outbox consumer configuration";
        }
        return nullptr;
    }
    try {
        return std::unique_ptr<RawOutboxConsumer>(
            new RawOutboxConsumer(std::make_unique<Impl>(config)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("raw outbox consumer creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

RawOutboxConsumer::RawOutboxConsumer(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
RawOutboxConsumer::~RawOutboxConsumer() = default;
bool RawOutboxConsumer::Start(std::string* error) {
    return impl_->Start(error);
}
bool RawOutboxConsumer::DrainThrough(
    std::uint64_t lsn, std::uint64_t timeout_ns) noexcept {
    return impl_->DrainThrough(lsn, timeout_ns);
}
void RawOutboxConsumer::Stop() noexcept { impl_->Stop(); }
bool RawOutboxConsumer::healthy() const noexcept { return impl_->healthy(); }
std::string RawOutboxConsumer::fatal_error() const {
    return impl_->fatal_error();
}
std::uint64_t RawOutboxConsumer::reader_lsn() const noexcept {
    return impl_->reader_lsn();
}

std::unique_ptr<DerivedOutboxConsumer> DerivedOutboxConsumer::Create(
    DerivedOutboxConsumerConfig config,
    std::string* error) {
    const bool runtime_valid = config.domain == DerivedDomain::kEvent
        ? config.event_runtime != nullptr && config.kline_runtime == nullptr
        : config.kline_runtime != nullptr && config.event_runtime == nullptr;
    const ConsumerKind consumer = config.domain == DerivedDomain::kEvent
        ? ConsumerKind::kEvent
        : ConsumerKind::kKLine;
    if (config.outbox == nullptr || !runtime_valid ||
        config.owner_count == 0U ||
        config.queue_records_per_owner == 0U ||
        !config.outbox->consumer_enabled(consumer)) {
        if (error != nullptr) {
            *error = "invalid derived outbox consumer configuration";
        }
        return nullptr;
    }
    try {
        return std::unique_ptr<DerivedOutboxConsumer>(
            new DerivedOutboxConsumer(std::make_unique<Impl>(config)));
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("derived outbox consumer creation failed: ") +
                     exception.what();
        }
        return nullptr;
    }
}

DerivedOutboxConsumer::DerivedOutboxConsumer(
    std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
DerivedOutboxConsumer::~DerivedOutboxConsumer() = default;
bool DerivedOutboxConsumer::Start(std::string* error) {
    return impl_->Start(error);
}
bool DerivedOutboxConsumer::DrainThrough(
    std::uint64_t lsn, std::uint64_t timeout_ns) noexcept {
    return impl_->DrainThrough(lsn, timeout_ns);
}
void DerivedOutboxConsumer::Stop() noexcept { impl_->Stop(); }
bool DerivedOutboxConsumer::healthy() const noexcept {
    return impl_->healthy();
}
std::string DerivedOutboxConsumer::fatal_error() const {
    return impl_->fatal_error();
}
std::uint64_t DerivedOutboxConsumer::reader_lsn() const noexcept {
    return impl_->reader_lsn();
}

}  // namespace l2flow::outbox
