#include "l2flow/arrow/egress.h"

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api_vector.h>
#include <arrow/datum.h>
#include <arrow/record_batch.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/file.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace l2flow::arrow_hot {
namespace {

void SetError(std::string* error, std::string value) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(value);
    } catch (...) {
    }
}

[[nodiscard]] std::uint64_t AddSaturated(std::uint64_t left,
                                         std::uint64_t right) noexcept {
    return left > std::numeric_limits<std::uint64_t>::max() - right
        ? std::numeric_limits<std::uint64_t>::max()
        : left + right;
}

[[nodiscard]] std::uint64_t MonotonicNowNs() noexcept {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(value.tv_sec) *
               UINT64_C(1'000'000'000) +
           static_cast<std::uint64_t>(value.tv_nsec);
}

[[nodiscard]] std::string OwnerName(std::string_view prefix,
                                    std::size_t owner) {
    std::ostringstream output;
    output << prefix << "-owner-" << owner;
    return output.str();
}

class RunDirectoryCleanup final {
public:
    explicit RunDirectoryCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}

    ~RunDirectoryCleanup() {
        if (!active_) {
            return;
        }
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove_all(path_, ignored));
    }

    RunDirectoryCleanup(const RunDirectoryCleanup&) = delete;
    RunDirectoryCleanup& operator=(const RunDirectoryCleanup&) = delete;

    void Release() noexcept { active_ = false; }

private:
    std::filesystem::path path_;
    bool active_ = true;
};

class TemporaryFileCleanup final {
public:
    explicit TemporaryFileCleanup(std::filesystem::path path)
        : path_(std::move(path)) {}

    ~TemporaryFileCleanup() {
        if (active_) {
            static_cast<void>(::unlink(path_.c_str()));
        }
    }

    TemporaryFileCleanup(const TemporaryFileCleanup&) = delete;
    TemporaryFileCleanup& operator=(const TemporaryFileCleanup&) = delete;

    void Release() noexcept { active_ = false; }

private:
    std::filesystem::path path_;
    bool active_ = true;
};

class UniqueFd final {
public:
    explicit UniqueFd(int value = -1) noexcept : value_(value) {}
    ~UniqueFd() {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    [[nodiscard]] int get() const noexcept { return value_; }

    [[nodiscard]] int release() noexcept {
        return std::exchange(value_, -1);
    }

private:
    int value_ = -1;
};

[[nodiscard]] bool ParsePositiveU64(std::string_view text,
                                    std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() || value == 0U) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ReadPreviousEpoch(
    const std::filesystem::path& root,
    bool* found,
    std::uint64_t* epoch,
    std::string* error) {
    *found = false;
    *epoch = 0U;
    const std::filesystem::path current_path = root / "CURRENT";
    if (!std::filesystem::exists(current_path)) {
        return true;
    }
    std::ifstream current(current_path, std::ios::binary);
    std::string run_name;
    std::string trailing_line;
    if (!current || !std::getline(current, run_name) || run_name.empty() ||
        std::getline(current, trailing_line) ||
        std::filesystem::path(run_name).filename() != run_name ||
        run_name == "." || run_name == "..") {
        SetError(error, "existing Arrow CURRENT file is invalid");
        return false;
    }
    std::ifstream manifest(root / run_name / "manifest.txt",
                           std::ios::binary);
    if (!manifest) {
        SetError(error, "existing Arrow CURRENT manifest is missing");
        return false;
    }
    std::string line;
    const std::string expected_protocol =
        "protocol_version=" + std::to_string(kArrowRingProtocolVersion);
    bool format_valid = false;
    bool protocol_valid = false;
    bool epoch_found = false;
    while (std::getline(manifest, line)) {
        if (line == "format=l2flow-arrow-hot-v1") {
            format_valid = true;
        } else if (line == expected_protocol) {
            protocol_valid = true;
        } else if (line.starts_with("feed_session_epoch=")) {
            if (epoch_found ||
                !ParsePositiveU64(
                    std::string_view(line).substr(
                        std::string_view("feed_session_epoch=").size()),
                    epoch)) {
                SetError(error,
                         "existing Arrow manifest has an invalid epoch");
                return false;
            }
            epoch_found = true;
        }
    }
    if (!format_valid || !protocol_valid || !epoch_found) {
        SetError(error, "existing Arrow manifest is incomplete");
        return false;
    }
    *found = true;
    return true;
}

[[nodiscard]] bool ValidateConfig(const ArrowHotEgressConfig& config,
                                  std::string* error) noexcept {
    const bool descriptor_power_of_two =
        config.descriptor_capacity != 0U &&
        (config.descriptor_capacity &
         (config.descriptor_capacity - 1U)) == 0U;
    const std::size_t maximum_u32 = static_cast<std::size_t>(
        std::numeric_limits<std::uint32_t>::max());
    const std::size_t maximum_batch_rows = static_cast<std::size_t>(
        std::numeric_limits<std::uint32_t>::max());
    if (config.root_directory.empty() || config.owner_count == 0U ||
        config.owner_count >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) ||
        config.feed_session_epoch == 0U ||
        !descriptor_power_of_two ||
        config.descriptor_capacity < 2U ||
        config.descriptor_capacity > maximum_u32 ||
        config.segment_count < config.descriptor_capacity ||
        config.segment_count > maximum_u32 ||
        config.tick_segment_payload_bytes < 1'024U ||
        config.tick_segment_payload_bytes > maximum_u32 ||
        config.snapshot_segment_payload_bytes < 1'024U ||
        config.snapshot_segment_payload_bytes > maximum_u32 ||
        config.diagnostic_segment_payload_bytes < 1'024U ||
        config.diagnostic_segment_payload_bytes > maximum_u32 ||
        config.maximum_consumers == 0U ||
        config.maximum_consumers > kMaximumArrowRingConsumers ||
        config.tick_batch_rows == 0U ||
        config.tick_batch_rows > maximum_batch_rows ||
        config.snapshot_batch_rows == 0U ||
        config.snapshot_batch_rows > maximum_batch_rows ||
        config.diagnostic_batch_rows == 0U ||
        config.diagnostic_batch_rows > maximum_batch_rows ||
        config.maximum_batch_delay_ns == 0U ||
        config.heartbeat_interval_ns == 0U) {
        SetError(error, "invalid Arrow hot-egress configuration");
        return false;
    }
    return true;
}

[[nodiscard]] RingLocation RingPaths(
    const std::filesystem::path& run_directory,
    std::string_view name) {
    return {
        run_directory / (std::string(name) + ".arrow"),
        run_directory / (std::string(name) + ".ctl"),
    };
}

[[nodiscard]] RingWriterConfig RingConfig(
    const ArrowHotEgressConfig& config,
    const std::filesystem::path& run_directory,
    ProducerInstanceId producer_instance,
    RingStreamKind kind,
    std::uint32_t shard,
    std::string_view name,
    std::size_t segment_payload_bytes) {
    RingWriterConfig ring{};
    ring.location = RingPaths(run_directory, name);
    ring.stream_kind = kind;
    ring.shard_id = shard;
    ring.descriptor_capacity = config.descriptor_capacity;
    ring.segment_count = config.segment_count;
    ring.segment_payload_bytes = segment_payload_bytes;
    ring.maximum_consumers = config.maximum_consumers;
    ring.feed_session_epoch = config.feed_session_epoch;
    ring.producer_instance = producer_instance;
    return ring;
}

[[nodiscard]] bool WriteTextFile(const std::filesystem::path& path,
                                 std::string_view content,
                                 std::string* error) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
        SetError(error, "cannot create " + path.string());
        return false;
    }
    output.write(content.data(), static_cast<std::streamsize>(content.size()));
    output.close();
    if (!output) {
        SetError(error, "cannot finish writing " + path.string());
        return false;
    }
    return true;
}

}  // namespace

bool ValidateArrowHotEgressConfig(const ArrowHotEgressConfig& config,
                                  std::string* error) noexcept {
    if (!ValidateConfig(config, error)) {
        return false;
    }
    SetError(error, {});
    return true;
}

class ArrowHotEgress::Impl final {
public:
    struct OwnerState final {
        std::unique_ptr<SharedArrowRingWriter> tick_writer;
        std::unique_ptr<TickRecordBatchBuilder> tick_builder;
        std::uint64_t tick_deadline_ns = 0U;
        std::unique_ptr<SharedArrowRingWriter> snapshot_writer;
        std::unique_ptr<SnapshotRecordBatchBuilder> snapshot_builder;
        std::uint64_t snapshot_deadline_ns = 0U;
        std::uint64_t heartbeat_deadline_ns = 0U;
    };

    Impl(ArrowHotEgressConfig config,
         ProducerInstanceId producer_instance,
         std::filesystem::path run_directory,
         int root_lock_fd) noexcept
        : config_(std::move(config)),
          producer_instance_(producer_instance),
          run_directory_(std::move(run_directory)),
          manifest_path_(run_directory_ / "manifest.txt"),
          root_lock_fd_(root_lock_fd) {}

    ~Impl() {
        if (initialized_) {
            Seal(MonotonicNowNs());
        }
        if (root_lock_fd_ >= 0) {
            static_cast<void>(::close(root_lock_fd_));
        }
    }

    [[nodiscard]] bool AppendTickDispatch(
        std::size_t owner,
        const ingest::TickDispatch& dispatch) noexcept {
        if (!CanAppend(owner)) {
            return false;
        }
        if (dispatch.owner != owner ||
            dispatch.feed_session_epoch != config_.feed_session_epoch ||
            (dispatch.kind != ingest::TickDispatchKind::kProjectOrdered &&
             dispatch.kind != ingest::TickDispatchKind::kProjectHoleFill)) {
            Fail("Tick Arrow dispatch owner, epoch, or kind is invalid");
            return false;
        }
        OwnerState& state = *owners_[owner];
        std::string error;
        const bool hole_fill =
            dispatch.kind == ingest::TickDispatchKind::kProjectHoleFill;
        const bool appended =
            dispatch.kind == ingest::TickDispatchKind::kProjectOrdered
                ? state.tick_builder->AppendOrdered(dispatch.tick, &error)
                : hole_fill
                      ? state.tick_builder->AppendHoleFill(dispatch, &error)
                      : false;
        if (!appended) {
            if (!hole_fill && dispatch.kind !=
                    ingest::TickDispatchKind::kProjectOrdered) {
                error = "dispatch is not projectable";
            }
            Fail("Tick Arrow builder failed: ", error);
            return false;
        }
        tick_rows_received_.fetch_add(1U, std::memory_order_relaxed);
        if (hole_fill) {
            hole_fill_rows_received_.fetch_add(
                1U, std::memory_order_relaxed);
        }
        const std::uint64_t receive_ns =
            dispatch.tick.common.receive_monotonic_ns;
        MaybeTouchOwner(owner, receive_ns);
        if (state.tick_builder->rows() == 1U) {
            state.tick_deadline_ns = AddSaturated(
                receive_ns, config_.maximum_batch_delay_ns);
        }
        if (state.tick_builder->rows() >= config_.tick_batch_rows) {
            FlushTick(owner);
        }
        return healthy();
    }

    [[nodiscard]] bool AppendSnapshot(
        std::size_t owner,
        const ingest::CanonicalSnapshot& snapshot) noexcept {
        if (!CanAppend(owner)) {
            return false;
        }
        OwnerState& state = *owners_[owner];
        std::string error;
        if (!state.snapshot_builder->Append(snapshot, &error)) {
            Fail("Snapshot Arrow builder failed: ", error);
            return false;
        }
        snapshot_rows_received_.fetch_add(1U, std::memory_order_relaxed);
        MaybeTouchOwner(owner, snapshot.common.receive_monotonic_ns);
        if (state.snapshot_builder->rows() == 1U) {
            state.snapshot_deadline_ns = AddSaturated(
                snapshot.common.receive_monotonic_ns,
                config_.maximum_batch_delay_ns);
        }
        if (state.snapshot_builder->rows() >= config_.snapshot_batch_rows) {
            FlushSnapshot(owner);
        }
        return healthy();
    }

    [[nodiscard]] bool AppendGap(const ingest::ChannelGap& record) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (!CanAppendControl()) {
            return false;
        }
        std::string error;
        if (!control_builder_->AppendGap(record, &error)) {
            Fail("Control gap Arrow builder failed: ", error);
            return false;
        }
        OnControlRow(record.detected_monotonic_ns);
        return healthy();
    }

    [[nodiscard]] bool AppendFault(
        const ingest::ChannelFault& record) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (!CanAppendControl()) {
            return false;
        }
        std::string error;
        if (!control_builder_->AppendFault(record, &error)) {
            Fail("Control fault Arrow builder failed: ", error);
            return false;
        }
        OnControlRow(record.detected_monotonic_ns);
        return healthy();
    }

    [[nodiscard]] bool MarkFeedConnected(
        std::uint64_t observed_monotonic_ns) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (!CanAppendControl()) {
            return false;
        }
        std::string error;
        if (!control_builder_->AppendLifecycle(
                {ControlKind::kFeedConnected, ContinuityReason::kMdlConnect,
                 observed_monotonic_ns},
                &error)) {
            Fail("feed-connected control builder failed: ", error);
            return false;
        }
        control_rows_received_.fetch_add(1U, std::memory_order_relaxed);
        FlushControlLocked();
        return healthy();
    }

    [[nodiscard]] bool MarkFeedBoundary(
        ControlKind kind,
        ContinuityReason reason,
        std::uint64_t observed_monotonic_ns) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (!CanAppendControl()) {
            return false;
        }
        std::string error;
        if (!control_builder_->AppendLifecycle(
                {kind, reason, observed_monotonic_ns}, &error)) {
            Fail("feed-boundary control builder failed: ", error);
            return false;
        }
        control_rows_received_.fetch_add(1U, std::memory_order_relaxed);
        FlushControlLocked();
        return healthy();
    }

    void FlushDue(std::size_t owner,
                  std::uint64_t now_monotonic_ns) noexcept {
        if (owner >= owners_.size() || !healthy()) {
            return;
        }
        OwnerState& state = *owners_[owner];
        MaybeTouchOwner(owner, now_monotonic_ns);
        if (state.tick_builder->rows() != 0U &&
            now_monotonic_ns >= state.tick_deadline_ns) {
            FlushTick(owner);
        }
        if (state.snapshot_builder->rows() != 0U &&
            now_monotonic_ns >= state.snapshot_deadline_ns) {
            FlushSnapshot(owner);
        }
        if (owner == 0U) {
            MaybeTouchControl(now_monotonic_ns);
            std::lock_guard<std::mutex> lock(control_mutex_);
            if (control_builder_->rows() != 0U &&
                now_monotonic_ns >= control_deadline_ns_) {
                FlushControlLocked();
            }
        }
    }

    void FlushAll() noexcept {
        if (!healthy()) {
            return;
        }
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            FlushTick(owner);
            FlushSnapshot(owner);
        }
        std::lock_guard<std::mutex> lock(control_mutex_);
        FlushControlLocked();
    }

    void Seal(std::uint64_t observed_monotonic_ns) noexcept {
        bool expected = false;
        if (!sealed_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }
        FlushAllBeforeSeal();
        {
            std::lock_guard<std::mutex> lock(control_mutex_);
            if (healthy()) {
                std::string error;
                if (control_builder_->AppendLifecycle(
                        {ControlKind::kProducerSealed,
                         ContinuityReason::kProducerShutdown,
                         observed_monotonic_ns},
                        &error)) {
                    control_rows_received_.fetch_add(
                        1U, std::memory_order_relaxed);
                    FlushControlLocked();
                } else {
                    Fail("producer-sealed control builder failed: ", error);
                }
            }
        }
        for (const std::unique_ptr<OwnerState>& owner : owners_) {
            owner->tick_writer->Seal(observed_monotonic_ns);
            owner->snapshot_writer->Seal(observed_monotonic_ns);
        }
        control_writer_->Seal(observed_monotonic_ns);
    }

    [[nodiscard]] bool healthy() const noexcept {
        return healthy_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::string FatalError() const {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return fatal_error_;
    }

    [[nodiscard]] ArrowHotEgressStats Stats() const noexcept {
        ArrowHotEgressStats output{};
        output.tick_rows_received = tick_rows_received_.load(
            std::memory_order_relaxed);
        output.snapshot_rows_received = snapshot_rows_received_.load(
            std::memory_order_relaxed);
        output.hole_fill_rows_received =
            hole_fill_rows_received_.load(std::memory_order_relaxed);
        output.control_rows_received = control_rows_received_.load(
            std::memory_order_relaxed);
        output.published_batches = published_batches_.load(
            std::memory_order_relaxed);
        output.published_rows = published_rows_.load(
            std::memory_order_relaxed);
        output.no_segment_dropped_rows = no_segment_dropped_rows_.load(
            std::memory_order_relaxed);
        output.oversized_dropped_rows = oversized_dropped_rows_.load(
            std::memory_order_relaxed);
        output.control_publish_dropped_rows =
            control_publish_dropped_rows_.load(std::memory_order_relaxed);
        output.internal_errors = internal_errors_.load(
            std::memory_order_relaxed);
        return output;
    }

    [[nodiscard]] bool Publish(SharedArrowRingWriter* writer,
                               const BuiltRecordBatch& built,
                               RingStreamKind kind,
                               std::uint32_t shard,
                               bool report_overrun) noexcept {
        try {
            if (writer == nullptr || built.batch == nullptr) {
                Fail("Arrow egress attempted to publish an empty batch");
                return false;
            }
            const PublishResult result = writer->TryPublish(
                *built.batch, built.metadata);
            const std::uint64_t rows = static_cast<std::uint64_t>(
                built.batch->num_rows());
            if (result.code == PublishCode::kPublished) {
                published_batches_.fetch_add(1U, std::memory_order_relaxed);
                published_rows_.fetch_add(rows, std::memory_order_relaxed);
                return true;
            }
            if (result.code == PublishCode::kPayloadTooLarge && rows > 1U) {
                const std::int64_t left_rows = built.batch->num_rows() / 2;
                BuiltRecordBatch left{};
                BuiltRecordBatch right{};
                std::string compact_error;
                left.batch = CompactSlice(
                    *built.batch, 0, left_rows, &compact_error);
                right.batch = CompactSlice(
                    *built.batch, left_rows,
                    built.batch->num_rows() - left_rows, &compact_error);
                if (left.batch == nullptr || right.batch == nullptr) {
                    Fail("cannot compact an oversized Arrow batch: ",
                         compact_error);
                    return false;
                }
                if (!MetadataForSlice(
                        *left.batch, built.metadata, &left.metadata) ||
                    !MetadataForSlice(
                        *right.batch, built.metadata, &right.metadata)) {
                    Fail("cannot derive metadata for an oversized Arrow slice");
                    return false;
                }
                const bool left_ok = Publish(
                    writer, left, kind, shard, report_overrun);
                const bool right_ok = Publish(
                    writer, right, kind, shard, report_overrun);
                return left_ok && right_ok;
            }
            if (result.code == PublishCode::kNoReusableSegment ||
                result.code == PublishCode::kPayloadTooLarge) {
                if (result.code == PublishCode::kNoReusableSegment) {
                    no_segment_dropped_rows_.fetch_add(
                        rows, std::memory_order_relaxed);
                } else {
                    oversized_dropped_rows_.fetch_add(
                        rows, std::memory_order_relaxed);
                }
                if (report_overrun) {
                    ReportOverrun(kind, shard, rows);
                } else {
                    control_publish_dropped_rows_.fetch_add(
                        rows, std::memory_order_relaxed);
                }
                return true;
            }
            Fail("Arrow ring publish failed: ", result.error);
            return false;
        } catch (const std::exception& exception) {
            Fail("Arrow egress publish exception: ", exception.what());
            return false;
        } catch (...) {
            Fail("Arrow egress publish failed unexpectedly");
            return false;
        }
    }

    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> CompactSlice(
        const arrow::RecordBatch& batch,
        std::int64_t offset,
        std::int64_t length,
        std::string* error) {
        if (offset < 0 || length <= 0 || offset > batch.num_rows() ||
            length > batch.num_rows() - offset) {
            SetError(error, "invalid compact-slice bounds");
            return nullptr;
        }
        arrow::Int64Builder indices;
        const arrow::Status reserve = indices.Reserve(length);
        if (!reserve.ok()) {
            SetError(error, reserve.ToString());
            return nullptr;
        }
        for (std::int64_t row = 0; row < length; ++row) {
            const arrow::Status appended = indices.Append(offset + row);
            if (!appended.ok()) {
                SetError(error, appended.ToString());
                return nullptr;
            }
        }
        auto finished = indices.Finish();
        if (!finished.ok()) {
            SetError(error, finished.status().ToString());
            return nullptr;
        }
        auto taken = arrow::compute::Take(
            arrow::Datum(batch), arrow::Datum(*finished));
        if (!taken.ok() || taken->kind() != arrow::Datum::RECORD_BATCH) {
            SetError(error, taken.ok()
                ? "Arrow Take did not return a RecordBatch"
                : taken.status().ToString());
            return nullptr;
        }
        SetError(error, {});
        return taken->record_batch();
    }

    [[nodiscard]] bool MetadataForSlice(
        const arrow::RecordBatch& batch,
        const BatchMetadata& fallback,
        BatchMetadata* output) noexcept {
        if (output == nullptr || batch.num_rows() <= 0) {
            return false;
        }
        try {
            BatchMetadata metadata{};
            metadata.feed_session_epoch = fallback.feed_session_epoch;
            metadata.flags = fallback.flags;
            const auto ingress = std::dynamic_pointer_cast<arrow::UInt64Array>(
                batch.GetColumnByName("ingress_sequence"));
            if (ingress == nullptr) {
                *output = metadata;
                return true;
            }
            const auto feed_epoch =
                std::dynamic_pointer_cast<arrow::UInt64Array>(
                    batch.GetColumnByName("feed_session_epoch"));
            const auto exchange =
                std::dynamic_pointer_cast<arrow::UInt64Array>(
                    batch.GetColumnByName(
                        "exchange_time_ns_from_midnight"));
            if (feed_epoch == nullptr || exchange == nullptr ||
                ingress->length() != batch.num_rows() ||
                feed_epoch->length() != batch.num_rows() ||
                exchange->length() != batch.num_rows() ||
                ingress->IsNull(0) || feed_epoch->IsNull(0)) {
                return false;
            }
            metadata.feed_session_epoch = feed_epoch->Value(0);
            metadata.first_ingress_sequence = ingress->Value(0);
            metadata.last_ingress_sequence = ingress->Value(0);
            bool has_exchange_time = false;
            for (std::int64_t row = 0; row < batch.num_rows(); ++row) {
                if (ingress->IsNull(row) || feed_epoch->IsNull(row) ||
                    feed_epoch->Value(row) !=
                        metadata.feed_session_epoch) {
                    return false;
                }
                metadata.first_ingress_sequence = std::min(
                    metadata.first_ingress_sequence,
                    ingress->Value(row));
                metadata.last_ingress_sequence = std::max(
                    metadata.last_ingress_sequence,
                    ingress->Value(row));
                if (!exchange->IsNull(row)) {
                    const std::uint64_t value = exchange->Value(row);
                    if (!has_exchange_time) {
                        metadata.minimum_exchange_time_ns = value;
                        metadata.maximum_exchange_time_ns = value;
                        has_exchange_time = true;
                    } else {
                        metadata.minimum_exchange_time_ns = std::min(
                            metadata.minimum_exchange_time_ns, value);
                        metadata.maximum_exchange_time_ns = std::max(
                            metadata.maximum_exchange_time_ns, value);
                    }
                }
            }
            *output = metadata;
            return true;
        } catch (...) {
            return false;
        }
    }

    void ReportOverrun(RingStreamKind kind,
                       std::uint32_t shard,
                       std::uint64_t rows) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (!CanAppendControl()) {
            return;
        }
        std::string error;
        const std::uint64_t now = MonotonicNowNs();
        if (!control_builder_->AppendOverrun(
                {kind, shard, rows, now}, &error)) {
            Fail("hot-overrun control builder failed: ", error);
            return;
        }
        OnControlRow(now);
    }

    void FlushTick(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.tick_builder->rows() == 0U || !healthy()) {
            return;
        }
        std::string error;
        BuiltRecordBatch built = state.tick_builder->Finish(&error);
        state.tick_deadline_ns = 0U;
        if (built.batch == nullptr) {
            Fail("Tick Arrow batch finish failed: ", error);
            return;
        }
        static_cast<void>(Publish(
            state.tick_writer.get(), built, RingStreamKind::kOrderedTick,
            static_cast<std::uint32_t>(owner), true));
    }

    void FlushSnapshot(std::size_t owner) noexcept {
        OwnerState& state = *owners_[owner];
        if (state.snapshot_builder->rows() == 0U || !healthy()) {
            return;
        }
        std::string error;
        BuiltRecordBatch built = state.snapshot_builder->Finish(&error);
        state.snapshot_deadline_ns = 0U;
        if (built.batch == nullptr) {
            Fail("Snapshot Arrow batch finish failed: ", error);
            return;
        }
        static_cast<void>(Publish(
            state.snapshot_writer.get(), built, RingStreamKind::kSnapshot,
            static_cast<std::uint32_t>(owner), true));
    }

    void FlushControlLocked() noexcept {
        if (control_builder_->rows() == 0U || !healthy()) {
            return;
        }
        std::string error;
        BuiltRecordBatch built = control_builder_->Finish(&error);
        control_deadline_ns_ = 0U;
        if (built.batch == nullptr) {
            Fail("Control Arrow batch finish failed: ", error);
            return;
        }
        static_cast<void>(Publish(
            control_writer_.get(), built, RingStreamKind::kControl, 0U,
            false));
    }

    void FlushAllBeforeSeal() noexcept {
        for (std::size_t owner = 0U; owner < owners_.size(); ++owner) {
            FlushTick(owner);
            FlushSnapshot(owner);
        }
        std::lock_guard<std::mutex> lock(control_mutex_);
        FlushControlLocked();
    }

    void OnControlRow(std::uint64_t observed_monotonic_ns) noexcept {
        control_rows_received_.fetch_add(1U, std::memory_order_relaxed);
        if (control_builder_->rows() == 1U) {
            control_deadline_ns_ = AddSaturated(
                observed_monotonic_ns, config_.maximum_batch_delay_ns);
        }
        if (control_builder_->rows() >= config_.diagnostic_batch_rows) {
            FlushControlLocked();
        }
    }

    void MaybeTouchOwner(std::size_t owner,
                         std::uint64_t now_monotonic_ns) noexcept {
        OwnerState& state = *owners_[owner];
        if (now_monotonic_ns < state.heartbeat_deadline_ns) {
            return;
        }
        state.tick_writer->TouchHeartbeat(now_monotonic_ns);
        state.snapshot_writer->TouchHeartbeat(now_monotonic_ns);
        state.heartbeat_deadline_ns = AddSaturated(
            now_monotonic_ns, config_.heartbeat_interval_ns);
    }

    void MaybeTouchControl(std::uint64_t now_monotonic_ns) noexcept {
        if (now_monotonic_ns < diagnostic_heartbeat_deadline_ns_) {
            return;
        }
        control_writer_->TouchHeartbeat(now_monotonic_ns);
        diagnostic_heartbeat_deadline_ns_ = AddSaturated(
            now_monotonic_ns, config_.heartbeat_interval_ns);
    }

    [[nodiscard]] bool CanAppend(std::size_t owner) const noexcept {
        return owner < owners_.size() && healthy() &&
               !sealed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool CanAppendControl() const noexcept {
        return healthy() && !sealed_.load(std::memory_order_acquire);
    }

    void Fail(std::string_view message,
              std::string_view detail = {}) noexcept {
        bool expected = true;
        if (healthy_.compare_exchange_strong(
                expected, false, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            try {
                std::lock_guard<std::mutex> lock(error_mutex_);
                fatal_error_.assign(message);
                fatal_error_.append(detail);
            } catch (...) {
            }
        }
        internal_errors_.fetch_add(1U, std::memory_order_relaxed);
    }

    ArrowHotEgressConfig config_;
    ProducerInstanceId producer_instance_{};
    std::filesystem::path run_directory_;
    std::filesystem::path manifest_path_;
    int root_lock_fd_ = -1;
    std::vector<std::unique_ptr<OwnerState>> owners_;
    std::unique_ptr<SharedArrowRingWriter> control_writer_;
    std::unique_ptr<ControlRecordBatchBuilder> control_builder_;
    std::uint64_t control_deadline_ns_ = 0U;
    std::uint64_t diagnostic_heartbeat_deadline_ns_ = 0U;
    mutable std::mutex control_mutex_;
    mutable std::mutex error_mutex_;
    std::string fatal_error_;
    std::atomic<bool> healthy_{true};
    std::atomic<bool> sealed_{false};
    bool initialized_ = false;
    std::atomic<std::uint64_t> tick_rows_received_{0U};
    std::atomic<std::uint64_t> snapshot_rows_received_{0U};
    std::atomic<std::uint64_t> hole_fill_rows_received_{0U};
    std::atomic<std::uint64_t> control_rows_received_{0U};
    std::atomic<std::uint64_t> published_batches_{0U};
    std::atomic<std::uint64_t> published_rows_{0U};
    std::atomic<std::uint64_t> no_segment_dropped_rows_{0U};
    std::atomic<std::uint64_t> oversized_dropped_rows_{0U};
    std::atomic<std::uint64_t> control_publish_dropped_rows_{0U};
    std::atomic<std::uint64_t> internal_errors_{0U};
};

std::unique_ptr<ArrowHotEgress> ArrowHotEgress::Create(
    ArrowHotEgressConfig config,
    std::string* error) {
    if (!ValidateArrowHotEgressConfig(config, error)) {
        return nullptr;
    }
    try {
        std::filesystem::create_directories(config.root_directory);
        const std::filesystem::path lock_path =
            config.root_directory / ".producer.lock";
        UniqueFd root_lock(::open(
            lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC,
            S_IRUSR | S_IWUSR | S_IRGRP));
        if (root_lock.get() < 0) {
            SetError(error, "cannot open Arrow producer lock: " +
                                std::string(std::strerror(errno)));
            return nullptr;
        }
        if (::flock(root_lock.get(), LOCK_EX | LOCK_NB) != 0) {
            SetError(error,
                     errno == EWOULDBLOCK
                         ? "another Arrow producer owns this root directory"
                         : "cannot lock Arrow producer root: " +
                               std::string(std::strerror(errno)));
            return nullptr;
        }
        bool previous_epoch_found = false;
        std::uint64_t previous_epoch = 0U;
        if (!ReadPreviousEpoch(config.root_directory,
                               &previous_epoch_found,
                               &previous_epoch, error)) {
            return nullptr;
        }
        if (previous_epoch_found &&
            config.feed_session_epoch <= previous_epoch) {
            SetError(error,
                     "Arrow feed epoch must be greater than the current epoch " +
                         std::to_string(previous_epoch));
            return nullptr;
        }
        const ProducerInstanceId producer_instance =
            GenerateProducerInstanceId(error);
        if (producer_instance == ProducerInstanceId{}) {
            return nullptr;
        }
        const std::string run_name =
            "producer-" + ProducerInstanceIdString(producer_instance) +
            "-epoch-" + std::to_string(config.feed_session_epoch);
        const std::filesystem::path run_directory =
            config.root_directory / run_name;
        if (!std::filesystem::create_directory(run_directory)) {
            SetError(error, "Arrow producer run directory already exists");
            return nullptr;
        }
        RunDirectoryCleanup cleanup(run_directory);
        auto impl = std::make_unique<Impl>(
            std::move(config), producer_instance, run_directory,
            root_lock.release());
        impl->owners_.reserve(impl->config_.owner_count);
        for (std::size_t owner = 0U;
             owner < impl->config_.owner_count; ++owner) {
            auto state = std::make_unique<Impl::OwnerState>();
            const std::string tick_name = OwnerName("tick", owner);
            state->tick_writer = SharedArrowRingWriter::Create(
                RingConfig(impl->config_, run_directory, producer_instance,
                           RingStreamKind::kOrderedTick,
                           static_cast<std::uint32_t>(owner), tick_name,
                           impl->config_.tick_segment_payload_bytes),
                TickArrowSchema(), error);
            state->tick_builder = TickRecordBatchBuilder::Create(
                impl->config_.tick_batch_rows,
                impl->config_.feed_session_epoch, error);
            const std::string snapshot_name = OwnerName("snapshot", owner);
            state->snapshot_writer = SharedArrowRingWriter::Create(
                RingConfig(impl->config_, run_directory, producer_instance,
                           RingStreamKind::kSnapshot,
                           static_cast<std::uint32_t>(owner), snapshot_name,
                           impl->config_.snapshot_segment_payload_bytes),
                SnapshotArrowSchema(), error);
            state->snapshot_builder = SnapshotRecordBatchBuilder::Create(
                impl->config_.snapshot_batch_rows,
                impl->config_.feed_session_epoch, error);
            if (state->tick_writer == nullptr ||
                state->tick_builder == nullptr ||
                state->snapshot_writer == nullptr ||
                state->snapshot_builder == nullptr) {
                return nullptr;
            }
            impl->owners_.push_back(std::move(state));
        }

        impl->control_writer_ = SharedArrowRingWriter::Create(
            RingConfig(impl->config_, run_directory, producer_instance,
                       RingStreamKind::kControl, 0U, "control",
                       impl->config_.diagnostic_segment_payload_bytes),
            ControlArrowSchema(), error);
        impl->control_builder_ = ControlRecordBatchBuilder::Create(
            impl->config_.diagnostic_batch_rows,
            impl->config_.feed_session_epoch, producer_instance, error);
        if (impl->control_writer_ == nullptr ||
            impl->control_builder_ == nullptr) {
            return nullptr;
        }
        impl->initialized_ = true;

        const std::uint64_t now = MonotonicNowNs();
        for (const std::unique_ptr<Impl::OwnerState>& owner : impl->owners_) {
            owner->heartbeat_deadline_ns = AddSaturated(
                now, impl->config_.heartbeat_interval_ns);
        }
        impl->diagnostic_heartbeat_deadline_ns_ = AddSaturated(
            now, impl->config_.heartbeat_interval_ns);
        if (!impl->control_builder_->AppendLifecycle(
                {ControlKind::kProducerStarted,
                 ContinuityReason::kProcessStart, now},
                error)) {
            return nullptr;
        }
        impl->control_rows_received_.fetch_add(1U,
                                               std::memory_order_relaxed);
        impl->FlushControlLocked();
        if (!impl->healthy()) {
            SetError(error, impl->FatalError());
            return nullptr;
        }

        std::ostringstream manifest;
        manifest << "format=l2flow-arrow-hot-v1\n"
                 << "protocol_version=" << kArrowRingProtocolVersion << '\n'
                 << "producer_instance="
                 << ProducerInstanceIdString(producer_instance) << '\n'
                 << "feed_session_epoch="
                 << impl->config_.feed_session_epoch << '\n'
                 << "owner_count=" << impl->config_.owner_count << '\n';
        for (std::size_t owner = 0U;
             owner < impl->config_.owner_count; ++owner) {
            manifest << "tick." << owner << '='
                     << OwnerName("tick", owner) << ".arrow\n"
                     << "tick_control." << owner << '='
                     << OwnerName("tick", owner) << ".ctl\n"
                     << "snapshot." << owner << '='
                     << OwnerName("snapshot", owner) << ".arrow\n"
                     << "snapshot_control." << owner << '='
                     << OwnerName("snapshot", owner) << ".ctl\n";
        }
        manifest << "control=control.arrow\n"
                 << "control_control=control.ctl\n";
        if (!WriteTextFile(impl->manifest_path_, manifest.str(), error)) {
            return nullptr;
        }

        auto output = std::unique_ptr<ArrowHotEgress>(
            new ArrowHotEgress(std::move(impl)));
        const std::filesystem::path current_temporary =
            output->impl_->config_.root_directory /
            ("CURRENT." + ProducerInstanceIdString(producer_instance) +
             ".tmp");
        TemporaryFileCleanup current_cleanup(current_temporary);
        if (!WriteTextFile(current_temporary, run_name + "\n", error)) {
            return nullptr;
        }
        std::filesystem::rename(
            current_temporary,
            output->impl_->config_.root_directory / "CURRENT");
        current_cleanup.Release();
        cleanup.Release();
        SetError(error, {});
        return output;
    } catch (const std::exception& exception) {
        SetError(error, std::string("Arrow hot-egress creation failed: ") +
                            exception.what());
        return nullptr;
    }
}

ArrowHotEgress::ArrowHotEgress(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ArrowHotEgress::~ArrowHotEgress() = default;

bool ArrowHotEgress::AppendTickDispatch(
    std::size_t owner,
    const ingest::TickDispatch& dispatch) noexcept {
    return impl_->AppendTickDispatch(owner, dispatch);
}

bool ArrowHotEgress::AppendSnapshot(
    std::size_t owner,
    const ingest::CanonicalSnapshot& snapshot) noexcept {
    return impl_->AppendSnapshot(owner, snapshot);
}

bool ArrowHotEgress::AppendGap(const ingest::ChannelGap& record) noexcept {
    return impl_->AppendGap(record);
}

bool ArrowHotEgress::AppendFault(
    const ingest::ChannelFault& record) noexcept {
    return impl_->AppendFault(record);
}

bool ArrowHotEgress::MarkFeedConnected(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedConnected(observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedDisconnected(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedDisconnected, ContinuityReason::kMdlDisconnect,
        observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedConnectError(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedConnectError, ContinuityReason::kMdlConnectError,
        observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedServiceTimeout(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedServiceTimeout,
        ContinuityReason::kMdlServiceTimeout, observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedMessageDiscarded(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedMessageDiscarded,
        ContinuityReason::kMdlMessageDiscarded, observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedSubscriptionRejected(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedSubscriptionRejected,
        ContinuityReason::kMdlSubscriptionRejected,
        observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedControlProtocolError(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedControlProtocolError,
        ContinuityReason::kMdlControlProtocolError,
        observed_monotonic_ns);
}

bool ArrowHotEgress::MarkFeedReadyTimeout(
    std::uint64_t observed_monotonic_ns) noexcept {
    return impl_->MarkFeedBoundary(
        ControlKind::kFeedReadyTimeout,
        ContinuityReason::kMdlReadyTimeout, observed_monotonic_ns);
}

void ArrowHotEgress::FlushDue(
    std::size_t owner,
    std::uint64_t now_monotonic_ns) noexcept {
    impl_->FlushDue(owner, now_monotonic_ns);
}

void ArrowHotEgress::FlushAll() noexcept { impl_->FlushAll(); }

void ArrowHotEgress::Seal(std::uint64_t observed_monotonic_ns) noexcept {
    impl_->Seal(observed_monotonic_ns);
}

bool ArrowHotEgress::healthy() const noexcept { return impl_->healthy(); }

std::string ArrowHotEgress::fatal_error() const {
    return impl_->FatalError();
}

ArrowHotEgressStats ArrowHotEgress::stats() const noexcept {
    return impl_->Stats();
}

const ArrowHotEgressConfig& ArrowHotEgress::config() const noexcept {
    return impl_->config_;
}

ProducerInstanceId ArrowHotEgress::producer_instance() const noexcept {
    return impl_->producer_instance_;
}

const std::filesystem::path& ArrowHotEgress::run_directory() const noexcept {
    return impl_->run_directory_;
}

const std::filesystem::path& ArrowHotEgress::manifest_path() const noexcept {
    return impl_->manifest_path_;
}

}  // namespace l2flow::arrow_hot
