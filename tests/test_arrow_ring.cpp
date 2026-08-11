#include "l2flow/arrow/ring.h"
#include "l2flow/arrow/schemas.h"
#include "l2flow/arrow/egress.h"
#include "l2flow/arrow/c_api.h"

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using namespace l2flow::arrow_hot;
using namespace l2flow::ingest;

#define CHECK(condition)                                                     \
    do {                                                                     \
        if (!(condition)) {                                                  \
            std::cerr << "CHECK failed at " << __FILE__ << ':' << __LINE__  \
                      << ": " #condition << '\n';                           \
            std::exit(1);                                                    \
        }                                                                    \
    } while (false)

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view prefix =
            "/tmp/l2flow-arrow-ring-test-XXXXXX";
        CHECK(prefix.size() + 1U <= pattern.size());
        std::copy(prefix.begin(), prefix.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        CHECK(created != nullptr);
        path_ = created;
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        static_cast<void>(std::filesystem::remove_all(path_, ignored));
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

[[nodiscard]] RingWriterConfig MakeConfig(
    const TemporaryDirectory& directory,
    std::string_view name,
    std::size_t descriptor_capacity = 4U,
    std::size_t segment_count = 5U,
    std::size_t maximum_consumers = 4U) {
    RingWriterConfig config{};
    config.location.data_path = directory.path() /
        (std::string(name) + ".data");
    config.location.control_path = directory.path() /
        (std::string(name) + ".control");
    config.stream_kind = RingStreamKind::kOrderedTick;
    config.shard_id = 7U;
    config.descriptor_capacity = descriptor_capacity;
    config.segment_count = segment_count;
    config.segment_payload_bytes = 1024U * 1024U;
    config.maximum_consumers = maximum_consumers;
    config.feed_session_epoch = 23U;
    config.producer_instance = {11U, 29U};
    return config;
}

[[nodiscard]] std::shared_ptr<arrow::Schema> ScalarSchema() {
    return arrow::schema({arrow::field("value", arrow::uint64(), false)});
}

[[nodiscard]] std::shared_ptr<arrow::RecordBatch> ScalarBatch(
    std::uint64_t value) {
    arrow::UInt64Builder builder;
    CHECK(builder.Append(value).ok());
    auto array = builder.Finish();
    CHECK(array.ok());
    return arrow::RecordBatch::Make(ScalarSchema(), 1, {*array});
}

[[nodiscard]] BatchMetadata Metadata(std::uint64_t ingress) {
    BatchMetadata metadata{};
    metadata.feed_session_epoch = 23U;
    metadata.first_ingress_sequence = ingress;
    metadata.last_ingress_sequence = ingress;
    metadata.publish_monotonic_ns = ingress * 10U;
    return metadata;
}

void OverwriteProtocolUint64(const std::filesystem::path& path,
                             std::streamoff offset,
                             std::uint64_t value) {
    std::fstream stream(path,
                        std::ios::binary | std::ios::in | std::ios::out);
    CHECK(stream.good());
    stream.seekp(offset);
    CHECK(stream.good());
    stream.write(reinterpret_cast<const char*>(&value), sizeof(value));
    stream.flush();
    CHECK(stream.good());
}

[[nodiscard]] std::unique_ptr<SharedArrowRingWriter> CreateWriter(
    RingWriterConfig config,
    std::shared_ptr<arrow::Schema> schema) {
    std::string error;
    auto writer = SharedArrowRingWriter::Create(
        std::move(config), std::move(schema), &error);
    CHECK(writer != nullptr);
    CHECK(error.empty());
    return writer;
}

[[nodiscard]] std::unique_ptr<SharedArrowRingReader> OpenReader(
    const RingLocation& location,
    ReaderStart start) {
    std::string error;
    auto reader = SharedArrowRingReader::Open(location, start, &error);
    CHECK(reader != nullptr);
    CHECK(error.empty());
    return reader;
}

void TestRoundTripAndStarts() {
    TemporaryDirectory directory;
    const RingWriterConfig config = MakeConfig(directory, "round-trip");
    auto writer = CreateWriter(config, ScalarSchema());
    auto latest = OpenReader(config.location, ReaderStart::kLatest);

    const std::uint64_t initial_heartbeat =
        latest->producer_heartbeat_monotonic_ns();
    CHECK(initial_heartbeat <
          std::numeric_limits<std::uint64_t>::max() - 2U);
    writer->TouchHeartbeat(initial_heartbeat + 2U);
    writer->TouchHeartbeat(initial_heartbeat + 1U);
    CHECK(latest->producer_heartbeat_monotonic_ns() ==
          initial_heartbeat + 2U);

    CHECK(latest->TryRead().code == ReadCode::kEmpty);
    const PublishResult published = writer->TryPublish(
        *ScalarBatch(42U), Metadata(101U));
    CHECK(published.code == PublishCode::kPublished);
    CHECK(published.batch_sequence == 1U);

    ReadResult result = latest->TryRead();
    CHECK(result.code == ReadCode::kBatch);
    CHECK(result.metadata.feed_session_epoch == 23U);
    CHECK(result.metadata.first_ingress_sequence == 101U);
    CHECK((result.metadata.producer_instance ==
           ProducerInstanceId{11U, 29U}));
    std::string error;
    const std::shared_ptr<arrow::RecordBatch> decoded =
        latest->Decode(result.lease, &error);
    CHECK(decoded != nullptr);
    CHECK(error.empty());
    CHECK(decoded->ValidateFull().ok());
    const auto values = std::static_pointer_cast<arrow::UInt64Array>(
        decoded->column(0));
    CHECK(values->Value(0) == 42U);

    auto earliest = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    CHECK(earliest->TryRead().code == ReadCode::kBatch);
    writer->Seal(999U);
    CHECK(latest->TryRead().code == ReadCode::kClosed);
    const PublishResult after_seal = writer->TryPublish(
        *ScalarBatch(43U), Metadata(102U));
    CHECK(after_seal.code == PublishCode::kInternalError);
}

void TestOverrunAndSeek() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "overrun", 2U, 2U);
    auto writer = CreateWriter(config, ScalarSchema());
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    for (std::uint64_t value = 1U; value <= 3U; ++value) {
        CHECK(writer->TryPublish(*ScalarBatch(value), Metadata(value)).code ==
              PublishCode::kPublished);
    }
    const ReadResult overrun = reader->TryRead();
    CHECK(overrun.code == ReadCode::kOverrun);
    CHECK(overrun.metadata.oldest_available_sequence == 2U);
    CHECK(overrun.metadata.newest_available_sequence == 3U);
    CHECK(reader->SeekToEarliestAvailable());
    const ReadResult recovered = reader->TryRead();
    CHECK(recovered.code == ReadCode::kBatch);
    CHECK(recovered.metadata.batch_sequence == 2U);
}

void TestDecodedBatchRetainsLease() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "lease", 2U, 2U);
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer->TryPublish(*ScalarBatch(1U), Metadata(1U)).code ==
          PublishCode::kPublished);
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    ReadResult result = reader->TryRead();
    CHECK(result.code == ReadCode::kBatch);
    std::string error;
    std::shared_ptr<arrow::RecordBatch> decoded =
        reader->Decode(result.lease, &error);
    CHECK(decoded != nullptr);
    result.lease.reset();
    reader.reset();

    CHECK(writer->TryPublish(*ScalarBatch(2U), Metadata(2U)).code ==
          PublishCode::kPublished);
    CHECK(writer->TryPublish(*ScalarBatch(3U), Metadata(3U)).code ==
          PublishCode::kNoReusableSegment);
    auto values = std::static_pointer_cast<arrow::UInt64Array>(
        decoded->column(0));
    CHECK(values->Value(0) == 1U);
    values.reset();
    decoded.reset();
    CHECK(writer->TryPublish(*ScalarBatch(3U), Metadata(3U)).code ==
          PublishCode::kPublished);
}

void TestMultipleLeasesOnOneSegment() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "multi-lease", 2U, 2U);
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer->TryPublish(*ScalarBatch(1U), Metadata(1U)).code ==
          PublishCode::kPublished);
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    ReadResult first = reader->TryRead();
    CHECK(first.code == ReadCode::kBatch);
    CHECK(reader->SeekToEarliestAvailable());
    ReadResult second = reader->TryRead();
    CHECK(second.code == ReadCode::kBatch);
    first.lease.reset();
    CHECK(writer->TryPublish(*ScalarBatch(2U), Metadata(2U)).code ==
          PublishCode::kPublished);
    CHECK(writer->TryPublish(*ScalarBatch(3U), Metadata(3U)).code ==
          PublishCode::kNoReusableSegment);
    second.lease.reset();
    CHECK(writer->TryPublish(*ScalarBatch(3U), Metadata(3U)).code ==
          PublishCode::kPublished);
}

void TestCrossThreadLeaseTransitions() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "cross-thread-lease", 2U, 2U);
    auto writer = CreateWriter(config, ScalarSchema());
    const std::shared_ptr<arrow::RecordBatch> batch = ScalarBatch(9U);
    CHECK(writer->TryPublish(*batch, Metadata(1U)).code ==
          PublishCode::kPublished);
    CHECK(writer->TryPublish(*batch, Metadata(2U)).code ==
          PublishCode::kPublished);
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    ReadResult held = reader->TryRead();
    CHECK(held.code == ReadCode::kBatch);

    constexpr std::uint64_t kIterations = 2'000U;
    for (std::uint64_t next_sequence = 3U;
         next_sequence < 3U + kIterations; ++next_sequence) {
        CHECK(reader->SeekToEarliestAvailable());
        std::shared_ptr<ArrowBatchLease> releasing =
            std::move(held.lease);
        std::atomic<bool> release_now{false};
        std::thread releaser([&] {
            while (!release_now.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            releasing.reset();
        });
        release_now.store(true, std::memory_order_release);
        ReadResult reacquired = reader->TryRead();
        releaser.join();
        CHECK(reacquired.code == ReadCode::kBatch);
        CHECK(reacquired.lease != nullptr);

        CHECK(writer->TryPublish(
                  *batch, Metadata(next_sequence)).code ==
              PublishCode::kNoReusableSegment);
        reacquired.lease.reset();
        CHECK(writer->TryPublish(
                  *batch, Metadata(next_sequence)).code ==
              PublishCode::kPublished);
        CHECK(reader->SeekToEarliestAvailable());
        held = reader->TryRead();
        CHECK(held.code == ReadCode::kBatch);
    }
}

void TestPayloadCorruptionDetection() {
    TemporaryDirectory directory;
    const RingWriterConfig config = MakeConfig(directory, "corrupt");
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer->TryPublish(*ScalarBatch(777U), Metadata(1U)).code ==
          PublishCode::kPublished);
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    ReadResult original = reader->TryRead();
    CHECK(original.code == ReadCode::kBatch);
    std::vector<std::uint8_t> payload(
        original.lease->data(),
        original.lease->data() + original.lease->size());
    original.lease.reset();
    reader.reset();

    std::ifstream input(config.location.data_path, std::ios::binary);
    CHECK(input.good());
    const std::istreambuf_iterator<char> file_end;
    const std::vector<char> file{
        std::istreambuf_iterator<char>(input), file_end};
    const auto found = std::search(
        file.begin(), file.end(), payload.begin(), payload.end(),
        [](char left, std::uint8_t right) {
            return static_cast<std::uint8_t>(left) == right;
        });
    CHECK(found != file.end());
    const auto offset_difference = std::distance(file.begin(), found);
    CHECK(offset_difference >= 0);
    const std::size_t payload_offset =
        static_cast<std::size_t>(offset_difference);
    CHECK(payload_offset <= file.size());
    CHECK(payload.size() <= file.size() - payload_offset);
    const std::size_t corrupt_offset =
        payload_offset + payload.size() / 2U;
    std::fstream mutate(config.location.data_path,
                        std::ios::binary | std::ios::in | std::ios::out);
    CHECK(mutate.good());
    mutate.seekp(static_cast<std::streamoff>(corrupt_offset));
    const char changed = static_cast<char>(
        static_cast<unsigned char>(file[corrupt_offset]) ^ 0x01U);
    mutate.write(&changed, 1);
    mutate.close();
    CHECK(static_cast<bool>(mutate));

    reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    const ReadResult corrupt = reader->TryRead();
    CHECK(corrupt.code == ReadCode::kCorrupt);
    CHECK(corrupt.error.find("CRC32") != std::string::npos);
}

void TestMismatchedControlFile() {
    TemporaryDirectory directory;
    const RingWriterConfig first_config = MakeConfig(directory, "match-a");
    const RingWriterConfig second_config = MakeConfig(directory, "match-b");
    auto first = CreateWriter(first_config, ScalarSchema());
    auto second = CreateWriter(second_config, ScalarSchema());
    CHECK(first != nullptr);
    CHECK(second != nullptr);
    CHECK(first_config.producer_instance == second_config.producer_instance);
    std::string error;
    auto reader = SharedArrowRingReader::Open(
        {first_config.location.data_path,
         second_config.location.control_path},
        ReaderStart::kLatest, &error);
    CHECK(reader == nullptr);
    CHECK(error.find("does not match") != std::string::npos);
}

void TestConcurrentDescriptorAndSegmentReuse() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "concurrent-reuse", 64U, 72U);
    auto writer = CreateWriter(config, ScalarSchema());
    auto reader = OpenReader(
        config.location, ReaderStart::kEarliestAvailable);
    const std::shared_ptr<arrow::RecordBatch> batch = ScalarBatch(777U);
    constexpr std::uint64_t kPublishCount = 20'000U;
    std::atomic<bool> start{false};
    std::atomic<bool> writer_failed{false};

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (std::uint64_t sequence = 1U;
             sequence <= kPublishCount; ++sequence) {
            const PublishResult result = writer->TryPublish(
                *batch, Metadata(sequence));
            if (result.code != PublishCode::kPublished ||
                result.batch_sequence != sequence) {
                writer_failed.store(true, std::memory_order_release);
                break;
            }
            if ((sequence & 15U) == 0U) {
                std::this_thread::yield();
            }
        }
        writer->Seal(kPublishCount * 10U + 1U);
    });

    start.store(true, std::memory_order_release);
    std::uint64_t batches_read = 0U;
    std::uint64_t previous_sequence = 0U;
    for (;;) {
        ReadResult result = reader->TryRead();
        if (result.code == ReadCode::kEmpty ||
            result.code == ReadCode::kRetry) {
            std::this_thread::yield();
            continue;
        }
        if (result.code == ReadCode::kOverrun) {
            CHECK(reader->SeekToEarliestAvailable());
            continue;
        }
        if (result.code == ReadCode::kClosed) {
            break;
        }
        CHECK(result.code == ReadCode::kBatch);
        CHECK(result.lease != nullptr);
        CHECK(result.metadata.batch_sequence > previous_sequence);
        CHECK(result.metadata.first_ingress_sequence ==
              result.metadata.batch_sequence);
        CHECK(result.metadata.last_ingress_sequence ==
              result.metadata.batch_sequence);
        std::string error;
        const std::shared_ptr<arrow::RecordBatch> decoded =
            reader->Decode(result.lease, &error);
        CHECK(decoded != nullptr);
        CHECK(error.empty());
        const auto values = std::static_pointer_cast<arrow::UInt64Array>(
            decoded->column(0));
        CHECK(values->Value(0) == 777U);
        previous_sequence = result.metadata.batch_sequence;
        ++batches_read;
    }
    producer.join();
    CHECK(!writer_failed.load(std::memory_order_acquire));
    CHECK(batches_read != 0U);
}

int CheckReadOnlyMappings(const char* data_path, const char* control_path) {
    std::string error;
    auto reader = SharedArrowRingReader::Open(
        {data_path, control_path}, ReaderStart::kLatest, &error);
    if (reader == nullptr) {
        return 20;
    }
    std::ifstream maps("/proc/self/maps");
    std::string line;
    bool data_read_only = false;
    bool control_read_write = false;
    while (std::getline(maps, line)) {
        const bool is_data = line.find(data_path) != std::string::npos;
        const bool is_control = line.find(control_path) != std::string::npos;
        if (!is_data && !is_control) {
            continue;
        }
        const std::size_t first_space = line.find(' ');
        if (first_space == std::string::npos || first_space + 4U >= line.size()) {
            return 21;
        }
        const std::string permissions = line.substr(first_space + 1U, 4U);
        if (is_data && permissions == "r--s") {
            data_read_only = true;
        }
        if (is_control && permissions == "rw-s") {
            control_read_write = true;
        }
    }
    return data_read_only && control_read_write ? 0 : 22;
}

void TestReaderMappingPermissions() {
    TemporaryDirectory directory;
    const RingWriterConfig config = MakeConfig(directory, "permissions");
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer != nullptr);
    const pid_t child = ::fork();
    CHECK(child >= 0);
    if (child == 0) {
        ::execl("/proc/self/exe", "test_arrow_ring",
                "--check-read-only", config.location.data_path.c_str(),
                config.location.control_path.c_str(),
                static_cast<char*>(nullptr));
        ::_exit(23);
    }
    int status = 0;
    CHECK(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}

void TestValidationAndConsumerCapacity() {
    TemporaryDirectory directory;
    RingWriterConfig config = MakeConfig(
        directory, "validation", 4U, 5U, 1U);
    auto writer = CreateWriter(config, ScalarSchema());
    std::string error;

    BatchMetadata wrong_epoch = Metadata(1U);
    wrong_epoch.feed_session_epoch = 24U;
    CHECK(writer->TryPublish(*ScalarBatch(1U), wrong_epoch).code ==
          PublishCode::kInvalidBatch);

    auto wrong_schema = arrow::schema(
        {arrow::field("other", arrow::uint64(), false)});
    arrow::UInt64Builder builder;
    CHECK(builder.Append(1U).ok());
    auto array = builder.Finish();
    CHECK(array.ok());
    const auto wrong_batch = arrow::RecordBatch::Make(
        wrong_schema, 1, {*array});
    CHECK(writer->TryPublish(*wrong_batch, Metadata(1U)).code ==
          PublishCode::kInvalidBatch);

    RingWriterConfig invalid_kind = MakeConfig(directory, "invalid-kind");
    invalid_kind.stream_kind = static_cast<RingStreamKind>(999U);
    std::string invalid_kind_error;
    auto invalid_kind_writer = SharedArrowRingWriter::Create(
        invalid_kind, ScalarSchema(), &invalid_kind_error);
    CHECK(invalid_kind_writer == nullptr);
    CHECK(invalid_kind_error.find("invalid Arrow ring") != std::string::npos);
    CHECK(!std::filesystem::exists(invalid_kind.location.data_path));

    invalid_kind = MakeConfig(directory, "removed-late-kind");
    invalid_kind.stream_kind = static_cast<RingStreamKind>(3U);
    invalid_kind_error.clear();
    invalid_kind_writer = SharedArrowRingWriter::Create(
        invalid_kind, ScalarSchema(), &invalid_kind_error);
    CHECK(invalid_kind_writer == nullptr);
    CHECK(invalid_kind_error.find("invalid Arrow ring") != std::string::npos);
    CHECK(!std::filesystem::exists(invalid_kind.location.data_path));

    auto invalid_start = SharedArrowRingReader::Open(
        config.location, static_cast<ReaderStart>(99U), &error);
    CHECK(invalid_start == nullptr);
    CHECK(error.find("start mode") != std::string::npos);

    auto first = OpenReader(config.location, ReaderStart::kLatest);
    auto second = SharedArrowRingReader::Open(
        config.location, ReaderStart::kLatest, &error);
    CHECK(second == nullptr);
    CHECK(error.find("registry is full") != std::string::npos);
    first.reset();
    second = SharedArrowRingReader::Open(
        config.location, ReaderStart::kLatest, &error);
    CHECK(second != nullptr);
}

void TestHeaderAndSchemaBounds() {
    TemporaryDirectory directory;

    const RingWriterConfig schema_header =
        MakeConfig(directory, "schema-header-bound");
    auto schema_header_writer = CreateWriter(schema_header, ScalarSchema());
    CHECK(schema_header_writer != nullptr);
    // protocol v2 DataHeader::schema_bytes is fixed at byte offset 48.
    OverwriteProtocolUint64(
        schema_header.location.data_path, 48,
        static_cast<std::uint64_t>(kMaximumArrowRingSchemaBytes) + 1U);
    std::string error;
    auto reader = SharedArrowRingReader::Open(
        schema_header.location, ReaderStart::kLatest, &error);
    CHECK(reader == nullptr);
    CHECK(error.find("invalid bounds") != std::string::npos);

    const RingWriterConfig initializing =
        MakeConfig(directory, "initializing-state");
    auto initializing_writer = CreateWriter(initializing, ScalarSchema());
    CHECK(initializing_writer != nullptr);
    // protocol v2 DataHeader::producer_state is fixed at byte offset 208.
    OverwriteProtocolUint64(initializing.location.data_path, 208, 0U);
    reader = SharedArrowRingReader::Open(
        initializing.location, ReaderStart::kLatest, &error);
    CHECK(reader == nullptr);
    CHECK(error.find("not fully initialized") != std::string::npos);

    const RingWriterConfig huge_schema_config =
        MakeConfig(directory, "huge-schema");
    const auto huge_metadata = arrow::key_value_metadata(
        std::vector<std::string>{"oversized"},
        std::vector<std::string>{
            std::string(kMaximumArrowRingSchemaBytes, 'x')});
    const auto huge_schema = arrow::schema(
        {arrow::field("value", arrow::uint64(), false)}, huge_metadata);
    auto huge_writer = SharedArrowRingWriter::Create(
        huge_schema_config, huge_schema, &error);
    CHECK(huge_writer == nullptr);
    CHECK(error.find("schema exceeds") != std::string::npos);
    CHECK(!std::filesystem::exists(
        huge_schema_config.location.data_path));
}

void TestEgressConfigValidation() {
    TemporaryDirectory directory;
    ArrowHotEgressConfig config{};
    config.root_directory = directory.path() / "validate-egress";
    config.owner_count = 1U;
    config.feed_session_epoch = 1U;
    std::string error = "stale";
    CHECK(ValidateArrowHotEgressConfig(config, &error));
    CHECK(error.empty());

    config.descriptor_capacity = 3U;
    CHECK(!ValidateArrowHotEgressConfig(config, &error));
    config.descriptor_capacity = 1'024U;
    config.maximum_consumers = kMaximumArrowRingConsumers + 1U;
    CHECK(!ValidateArrowHotEgressConfig(config, &error));
    config.maximum_consumers = 16U;
    if constexpr (sizeof(std::size_t) > sizeof(std::uint32_t)) {
        config.tick_segment_payload_bytes =
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) + 1U;
        CHECK(!ValidateArrowHotEgressConfig(config, &error));
        config.tick_segment_payload_bytes = 256U * 1'024U;
        config.tick_batch_rows =
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) + 1U;
        CHECK(!ValidateArrowHotEgressConfig(config, &error));
    }
}

void TestCreationRollback() {
    TemporaryDirectory directory;
    const RingWriterConfig config = MakeConfig(directory, "rollback");
    {
        std::ofstream existing(config.location.control_path);
        CHECK(existing.good());
    }
    std::string error;
    auto writer = SharedArrowRingWriter::Create(
        config, ScalarSchema(), &error);
    CHECK(writer == nullptr);
    CHECK(!std::filesystem::exists(config.location.data_path));
    CHECK(std::filesystem::exists(config.location.control_path));
}

void TestDeadConsumerReaping() {
    TemporaryDirectory directory;
    const RingWriterConfig config =
        MakeConfig(directory, "dead-consumer", 2U, 2U);
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer->TryPublish(*ScalarBatch(1U), Metadata(1U)).code ==
          PublishCode::kPublished);

    std::array<int, 2U> ready_pipe{};
    std::array<int, 2U> exit_pipe{};
    CHECK(::pipe(ready_pipe.data()) == 0);
    CHECK(::pipe(exit_pipe.data()) == 0);
    const pid_t child = ::fork();
    CHECK(child >= 0);
    if (child == 0) {
        static_cast<void>(::close(ready_pipe[0]));
        static_cast<void>(::close(exit_pipe[1]));
        std::string child_error;
        auto reader = SharedArrowRingReader::Open(
            config.location, ReaderStart::kEarliestAvailable, &child_error);
        if (reader == nullptr) {
            ::_exit(10);
        }
        ReadResult result = reader->TryRead();
        if (result.code != ReadCode::kBatch || result.lease == nullptr) {
            ::_exit(11);
        }
        const char ready = 'R';
        if (::write(ready_pipe[1], &ready, 1U) != 1) {
            ::_exit(12);
        }
        char finish = 0;
        if (::read(exit_pipe[0], &finish, 1U) != 1 || finish != 'X') {
            ::_exit(13);
        }
        ::_exit(0);
    }

    static_cast<void>(::close(ready_pipe[1]));
    static_cast<void>(::close(exit_pipe[0]));
    char ready = 0;
    CHECK(::read(ready_pipe[0], &ready, 1U) == 1);
    CHECK(ready == 'R');
    CHECK(writer->TryPublish(*ScalarBatch(2U), Metadata(2U)).code ==
          PublishCode::kPublished);
    const char finish = 'X';
    CHECK(::write(exit_pipe[1], &finish, 1U) == 1);
    int child_status = 0;
    CHECK(::waitpid(child, &child_status, 0) == child);
    CHECK(WIFEXITED(child_status));
    CHECK(WEXITSTATUS(child_status) == 0);
    static_cast<void>(::close(ready_pipe[0]));
    static_cast<void>(::close(exit_pipe[1]));

    CHECK(writer->TryPublish(*ScalarBatch(3U), Metadata(3U)).code ==
          PublishCode::kPublished);
    CHECK(writer->stats().reaped_consumers == 1U);
}

void FillCommon(CanonicalCommon* common,
                std::uint64_t ingress,
                std::uint64_t exchange_time) {
    CHECK(common != nullptr);
    common->ingress_sequence = ingress;
    common->vendor_sequence_id = ingress + 100U;
    common->receive_monotonic_ns = ingress + 1'000U;
    common->native_sequence = ingress + 200U;
    common->exchange_time_valid = true;
    common->exchange_time_ns_from_midnight = exchange_time;
    common->trade_date = 20'260'807U;
    common->instrument_id = 600'000U;
    common->instrument_ordinal = 3U;
    common->channel = 1U;
    common->identity.market = Market::kShanghai;
}

void TestCanonicalSchemas() {
    std::string error;
    auto ticks = TickRecordBatchBuilder::Create(4U, 23U, &error);
    CHECK(ticks != nullptr);
    CanonicalTick tick{};
    FillCommon(&tick.common, 9U, 0U);
    tick.price = {12'345, 12'345'000, 3U, true, true};
    tick.quantity = {100, 0U, true};
    tick.action = TickAction::kTrade;
    CHECK(ticks->AppendOrdered(tick, &error));
    TickDispatch fill{};
    fill.tick = tick;
    FillCommon(&fill.tick.common, 10U, 100U);
    fill.feed_session_epoch = 23U;
    fill.expected_sequence = 215U;
    fill.admission_floor = 200U;
    fill.generation = 2U;
    fill.evict_before = 211U;
    fill.kind = TickDispatchKind::kProjectHoleFill;
    fill.catalog_match = true;
    CHECK(ticks->AppendHoleFill(fill, &error));
    BuiltRecordBatch tick_batch = ticks->Finish(&error);
    CHECK(tick_batch.batch != nullptr);
    CHECK(tick_batch.batch->num_rows() == 2);
    CHECK(tick_batch.batch->ValidateFull().ok());
    CHECK(tick_batch.metadata.first_ingress_sequence == 9U);
    CHECK(tick_batch.metadata.last_ingress_sequence == 10U);
    CHECK(tick_batch.metadata.minimum_exchange_time_ns == 0U);
    CHECK(tick_batch.metadata.maximum_exchange_time_ns == 100U);
    const auto stream_roles = std::static_pointer_cast<arrow::UInt8Array>(
        tick_batch.batch->GetColumnByName("stream_role"));
    const auto expected_sequences =
        std::static_pointer_cast<arrow::UInt64Array>(
            tick_batch.batch->GetColumnByName("expected_sequence"));
    CHECK(stream_roles->Value(0) ==
          static_cast<std::uint8_t>(TickStreamRole::kRealtimeOrdered));
    CHECK(stream_roles->Value(1) ==
          static_cast<std::uint8_t>(TickStreamRole::kHoleFill));
    CHECK(expected_sequences->IsNull(0));
    CHECK(expected_sequences->Value(1) == 215U);

    auto snapshots = SnapshotRecordBatchBuilder::Create(2U, 23U, &error);
    CHECK(snapshots != nullptr);
    CanonicalSnapshot snapshot{};
    FillCommon(&snapshot.common, 11U, 200U);
    snapshot.source_bid_depth = 20U;
    snapshot.source_ask_depth = 20U;
    snapshot.retained_bid_depth = 2U;
    snapshot.retained_ask_depth = 1U;
    snapshot.bids[0].price = {100, 100'000'000, 0U, true, true};
    snapshot.bids[0].quantity = {5, 0U, true};
    snapshot.bids[0].source_order_count = 2U;
    snapshot.bids[0].order_count_valid = true;
    snapshot.bids[1].price = {99, 99'000'000, 0U, true, true};
    snapshot.bids[1].quantity = {6, 0U, true};
    snapshot.asks[0].price = {101, 101'000'000, 0U, true, true};
    snapshot.asks[0].quantity = {7, 0U, true};
    CHECK(snapshots->Append(snapshot, &error));
    BuiltRecordBatch snapshot_batch = snapshots->Finish(&error);
    CHECK(snapshot_batch.batch != nullptr);
    CHECK(snapshot_batch.batch->ValidateFull().ok());
    const auto bids = std::static_pointer_cast<arrow::ListArray>(
        snapshot_batch.batch->GetColumnByName("bids"));
    const auto asks = std::static_pointer_cast<arrow::ListArray>(
        snapshot_batch.batch->GetColumnByName("asks"));
    CHECK(bids != nullptr);
    CHECK(asks != nullptr);
    CHECK(bids->value_length(0) == 2);
    CHECK(asks->value_length(0) == 1);

    auto controls = ControlRecordBatchBuilder::Create(
        4U, 23U, {11U, 29U}, &error);
    CHECK(controls != nullptr);
    CHECK(controls->AppendLifecycle(
        {ControlKind::kProducerStarted, ContinuityReason::kProcessStart, 1U},
        &error));
    CHECK(!controls->AppendLifecycle(
        {ControlKind::kFeedReadyTimeout,
         ContinuityReason::kMdlConnectError, 1U},
        &error));
    CHECK(controls->AppendLifecycle(
        {ControlKind::kFeedReadyTimeout,
         ContinuityReason::kMdlReadyTimeout, 1U},
        &error));
    ChannelGap gap{};
    gap.market = Market::kShanghai;
    gap.channel = 2U;
    gap.first_missing = 10U;
    gap.last_missing = 11U;
    gap.first_present_after_gap = 12U;
    gap.detected_monotonic_ns = 2U;
    gap.gap_epoch = 1U;
    gap.cumulative_missing_sequences = 2U;
    CHECK(controls->AppendGap(gap, &error));
    ChannelGap invalid_gap = gap;
    invalid_gap.market = Market::kUnknown;
    CHECK(!controls->AppendGap(invalid_gap, &error));
    ChannelFault fault{};
    fault.market = Market::kShanghai;
    fault.channel = 2U;
    fault.expected_sequence = 13U;
    fault.observed_sequence = 14U;
    fault.detected_monotonic_ns = 3U;
    ChannelFault invalid_fault = fault;
    invalid_fault.reason = static_cast<ChannelFaultReason>(255U);
    CHECK(!controls->AppendFault(invalid_fault, &error));
    invalid_fault = fault;
    invalid_fault.market = Market::kUnknown;
    CHECK(!controls->AppendFault(invalid_fault, &error));
    CHECK(controls->AppendFault(fault, &error));
    CHECK(controls->AppendOverrun(
        {RingStreamKind::kSnapshot, 3U, 99U, 4U}, &error));
    BuiltRecordBatch control_batch = controls->Finish(&error);
    CHECK(control_batch.batch != nullptr);
    CHECK(control_batch.batch->num_rows() == 5);
    CHECK(control_batch.batch->ValidateFull().ok());
}

void TestHotEgressFanoutSink() {
    TemporaryDirectory directory;
    ArrowHotEgressConfig config{};
    config.root_directory = directory.path() / "hot";
    config.owner_count = 2U;
    config.feed_session_epoch = 77U;
    config.descriptor_capacity = 8U;
    config.segment_count = 10U;
    config.tick_segment_payload_bytes = 1024U * 1024U;
    config.snapshot_segment_payload_bytes = 1024U * 1024U;
    config.diagnostic_segment_payload_bytes = 1024U * 1024U;
    config.maximum_consumers = 2U;
    config.tick_batch_rows = 2U;
    config.snapshot_batch_rows = 2U;
    config.diagnostic_batch_rows = 2U;
    config.maximum_batch_delay_ns = 100U;

    std::string error;
    auto egress = ArrowHotEgress::Create(config, &error);
    CHECK(egress != nullptr);
    CHECK(error.empty());
    CHECK(std::filesystem::exists(egress->manifest_path()));
    std::ifstream current(config.root_directory / "CURRENT");
    std::string current_run;
    CHECK(static_cast<bool>(std::getline(current, current_run)));
    CHECK(current_run == egress->run_directory().filename().string());

    CanonicalTick tick{};
    FillCommon(&tick.common, 100U, 1U);
    TickDispatch ordered{};
    ordered.tick = tick;
    ordered.feed_session_epoch = 77U;
    ordered.expected_sequence = tick.common.native_sequence;
    ordered.admission_floor = tick.common.native_sequence;
    ordered.evict_before = tick.common.native_sequence + 1U;
    ordered.channel = tick.common.channel;
    ordered.market = tick.common.identity.market;
    ordered.owner = 1U;
    ordered.kind = TickDispatchKind::kProjectOrdered;
    ordered.catalog_match = true;
    CHECK(egress->AppendTickDispatch(1U, ordered));
    egress->FlushDue(1U, std::numeric_limits<std::uint64_t>::max());

    CanonicalSnapshot snapshot{};
    FillCommon(&snapshot.common, 101U, 2U);
    CHECK(egress->AppendSnapshot(0U, snapshot));

    TickDispatch fill = ordered;
    FillCommon(&fill.tick.common, 102U, 3U);
    fill.expected_sequence = fill.tick.common.native_sequence + 5U;
    fill.admission_floor = fill.tick.common.native_sequence - 2U;
    fill.evict_before = fill.tick.common.native_sequence + 1U;
    fill.generation = 1U;
    fill.kind = TickDispatchKind::kProjectHoleFill;
    CHECK(egress->AppendTickDispatch(1U, fill));

    ChannelGap gap{};
    gap.market = Market::kShanghai;
    gap.channel = 3U;
    gap.first_missing = 20U;
    gap.last_missing = 21U;
    gap.first_present_after_gap = 22U;
    gap.detected_monotonic_ns = 4U;
    gap.gap_epoch = 1U;
    gap.cumulative_missing_sequences = 2U;
    CHECK(egress->AppendGap(gap));
    CHECK(egress->MarkFeedConnected(5U));
    egress->FlushAll();
    egress->Seal(6U);
    CHECK(egress->healthy());

    const RingLocation tick_location = {
        egress->run_directory() / "tick-owner-1.arrow",
        egress->run_directory() / "tick-owner-1.ctl"};
    auto tick_reader = OpenReader(
        tick_location, ReaderStart::kEarliestAvailable);
    ReadResult tick_result = tick_reader->TryRead();
    CHECK(tick_result.code == ReadCode::kBatch);
    CHECK(tick_result.metadata.row_count == 1U);
    const std::shared_ptr<arrow::RecordBatch> decoded_tick =
        tick_reader->Decode(tick_result.lease, &error);
    CHECK(decoded_tick != nullptr);
    CHECK(decoded_tick->schema()->Equals(*TickArrowSchema(), true));
    ReadResult fill_result = tick_reader->TryRead();
    CHECK(fill_result.code == ReadCode::kBatch);
    CHECK(fill_result.metadata.row_count == 1U);

    const RingLocation snapshot_location = {
        egress->run_directory() / "snapshot-owner-0.arrow",
        egress->run_directory() / "snapshot-owner-0.ctl"};
    auto snapshot_reader = OpenReader(
        snapshot_location, ReaderStart::kEarliestAvailable);
    CHECK(snapshot_reader->TryRead().code == ReadCode::kBatch);

    const RingLocation control_location = {
        egress->run_directory() / "control.arrow",
        egress->run_directory() / "control.ctl"};
    auto control_reader = OpenReader(
        control_location, ReaderStart::kEarliestAvailable);
    std::uint64_t control_rows = 0U;
    for (;;) {
        ReadResult result = control_reader->TryRead();
        if (result.code == ReadCode::kClosed) {
            break;
        }
        CHECK(result.code == ReadCode::kBatch);
        control_rows += result.metadata.row_count;
    }
    CHECK(control_rows == 4U);
    const ArrowHotEgressStats stats = egress->stats();
    CHECK(stats.tick_rows_received == 2U);
    CHECK(stats.snapshot_rows_received == 1U);
    CHECK(stats.hole_fill_rows_received == 1U);
    CHECK(stats.control_rows_received == 4U);
    CHECK(stats.internal_errors == 0U);
}

void TestHotEgressProducerLockAndMonotonicEpoch() {
    TemporaryDirectory directory;
    ArrowHotEgressConfig config{};
    config.root_directory = directory.path() / "epoch-root";
    config.owner_count = 1U;
    config.feed_session_epoch = 40U;
    config.descriptor_capacity = 4U;
    config.segment_count = 5U;
    config.tick_segment_payload_bytes = 1024U * 1024U;
    config.snapshot_segment_payload_bytes = 1024U * 1024U;
    config.diagnostic_segment_payload_bytes = 1024U * 1024U;
    config.maximum_consumers = 2U;
    config.tick_batch_rows = 2U;
    config.snapshot_batch_rows = 2U;
    config.diagnostic_batch_rows = 2U;
    config.maximum_batch_delay_ns = 100U;

    std::string error;
    auto first = ArrowHotEgress::Create(config, &error);
    CHECK(first != nullptr);
    CHECK(error.empty());
    const std::filesystem::path first_run = first->run_directory();

    ArrowHotEgressConfig concurrent_config = config;
    concurrent_config.feed_session_epoch = 41U;
    auto concurrent = ArrowHotEgress::Create(concurrent_config, &error);
    CHECK(concurrent == nullptr);
    CHECK(error.find("another Arrow producer") != std::string::npos);

    first.reset();
    CHECK(std::filesystem::exists(first_run / "manifest.txt"));

    {
        std::ofstream malformed_current(
            config.root_directory / "CURRENT", std::ios::binary | std::ios::trunc);
        CHECK(malformed_current.good());
        malformed_current << first_run.filename().string() << "\nextra\n";
    }
    auto malformed = ArrowHotEgress::Create(concurrent_config, &error);
    CHECK(malformed == nullptr);
    CHECK(error.find("CURRENT file is invalid") != std::string::npos);
    {
        std::ofstream restored_current(
            config.root_directory / "CURRENT", std::ios::binary | std::ios::trunc);
        CHECK(restored_current.good());
        restored_current << first_run.filename().string() << '\n';
    }

    auto repeated = ArrowHotEgress::Create(config, &error);
    CHECK(repeated == nullptr);
    CHECK(error.find("must be greater than the current epoch 40") !=
          std::string::npos);

    auto successor = ArrowHotEgress::Create(concurrent_config, &error);
    CHECK(successor != nullptr);
    CHECK(error.empty());
    CHECK(successor->config().feed_session_epoch == 41U);
    CHECK(successor->run_directory() != first_run);

    std::ifstream current(config.root_directory / "CURRENT");
    std::string current_run;
    CHECK(static_cast<bool>(std::getline(current, current_run)));
    CHECK(current_run == successor->run_directory().filename().string());
}

void TestOversizedBatchSplitting() {
    std::string error;
    auto one_builder = TickRecordBatchBuilder::Create(1U, 88U, &error);
    CHECK(one_builder != nullptr);
    CanonicalTick sample{};
    FillCommon(&sample.common, 1U, 0U);
    CHECK(one_builder->AppendOrdered(sample, &error));
    BuiltRecordBatch one = one_builder->Finish(&error);
    CHECK(one.batch != nullptr);
    arrow::ipc::IpcWriteOptions options =
        arrow::ipc::IpcWriteOptions::Defaults();
    options.alignment = 64;
    options.use_threads = false;
    auto serialized = arrow::ipc::SerializeRecordBatch(*one.batch, options);
    CHECK(serialized.ok());
    CHECK((*serialized)->size() > 0);
    const std::size_t one_row_bytes = static_cast<std::size_t>(
        (*serialized)->size());
    CHECK(one_row_bytes >= 1'024U);
    auto eight_builder = TickRecordBatchBuilder::Create(8U, 88U, &error);
    CHECK(eight_builder != nullptr);
    for (std::uint64_t row = 1U; row <= 8U; ++row) {
        CanonicalTick tick = sample;
        FillCommon(&tick.common, row, row - 1U);
        CHECK(eight_builder->AppendOrdered(tick, &error));
    }
    BuiltRecordBatch eight = eight_builder->Finish(&error);
    CHECK(eight.batch != nullptr);
    auto serialized_eight =
        arrow::ipc::SerializeRecordBatch(*eight.batch, options);
    CHECK(serialized_eight.ok());
    CHECK((*serialized_eight)->size() > (*serialized)->size());
    const std::size_t eight_row_bytes = static_cast<std::size_t>(
        (*serialized_eight)->size());

    TemporaryDirectory directory;
    ArrowHotEgressConfig config{};
    config.root_directory = directory.path() / "split";
    config.owner_count = 1U;
    config.feed_session_epoch = 88U;
    config.descriptor_capacity = 16U;
    config.segment_count = 20U;
    config.tick_segment_payload_bytes =
        one_row_bytes + (eight_row_bytes - one_row_bytes) / 2U;
    config.snapshot_segment_payload_bytes = 1024U * 1024U;
    config.diagnostic_segment_payload_bytes = 1024U * 1024U;
    config.maximum_consumers = 2U;
    config.tick_batch_rows = 8U;
    config.snapshot_batch_rows = 2U;
    config.diagnostic_batch_rows = 4U;
    config.maximum_batch_delay_ns = 100U;
    auto egress = ArrowHotEgress::Create(config, &error);
    CHECK(egress != nullptr);
    for (std::uint64_t row = 1U; row <= 8U; ++row) {
        CanonicalTick tick = sample;
        FillCommon(&tick.common, row, row - 1U);
        TickDispatch dispatch{};
        dispatch.tick = tick;
        dispatch.feed_session_epoch = 88U;
        dispatch.expected_sequence = tick.common.native_sequence;
        dispatch.admission_floor = tick.common.native_sequence;
        dispatch.evict_before = tick.common.native_sequence + 1U;
        dispatch.channel = tick.common.channel;
        dispatch.market = tick.common.identity.market;
        dispatch.kind = TickDispatchKind::kProjectOrdered;
        dispatch.catalog_match = true;
        CHECK(egress->AppendTickDispatch(0U, dispatch));
    }
    egress->Seal(100U);
    CHECK(egress->healthy());
    const ArrowHotEgressStats stats = egress->stats();
    CHECK(stats.tick_rows_received == 8U);
    CHECK(stats.oversized_dropped_rows == 0U);
    CHECK(stats.no_segment_dropped_rows == 0U);
    CHECK(stats.published_batches > 2U);

    auto reader = OpenReader(
        {egress->run_directory() / "tick-owner-0.arrow",
         egress->run_directory() / "tick-owner-0.ctl"},
        ReaderStart::kEarliestAvailable);
    std::uint64_t rows = 0U;
    for (;;) {
        const ReadResult result = reader->TryRead();
        if (result.code == ReadCode::kClosed) {
            break;
        }
        CHECK(result.code == ReadCode::kBatch);
        CHECK(result.metadata.first_ingress_sequence <=
              result.metadata.last_ingress_sequence);
        rows += result.metadata.row_count;
    }
    CHECK(rows == 8U);
}

void TestCAbiLease() {
    TemporaryDirectory directory;
    const RingWriterConfig config = MakeConfig(directory, "c-api");
    auto writer = CreateWriter(config, ScalarSchema());
    CHECK(writer->TryPublish(*ScalarBatch(55U), Metadata(8U)).code ==
          PublishCode::kPublished);
    writer->Seal(10U);

    std::array<char, 512U> error{};
    l2flow_arrow_reader* reader = nullptr;
    CHECK(l2flow_arrow_reader_open(
              config.location.data_path.c_str(),
              config.location.control_path.c_str(),
              L2FLOW_ARROW_START_EARLIEST_AVAILABLE, &reader,
              error.data(), error.size()) == 0);
    CHECK(reader != nullptr);
    CHECK(l2flow_arrow_reader_stream_kind(reader) ==
          static_cast<std::uint32_t>(RingStreamKind::kOrderedTick));
    CHECK(l2flow_arrow_reader_shard_id(reader) == 7U);
    CHECK(l2flow_arrow_reader_feed_session_epoch(reader) == 23U);
    CHECK(l2flow_arrow_reader_producer_instance_high(reader) == 11U);
    CHECK(l2flow_arrow_reader_producer_instance_low(reader) == 29U);
    const std::uint8_t* schema_data = nullptr;
    std::size_t schema_size = 0U;
    CHECK(l2flow_arrow_reader_schema(
              reader, &schema_data, &schema_size,
              error.data(), error.size()) == 0);
    CHECK(schema_data != nullptr);
    CHECK(schema_size != 0U);

    l2flow_arrow_batch batch{};
    CHECK(l2flow_arrow_reader_try_read(
              reader, &batch, error.data(), error.size()) ==
          L2FLOW_ARROW_READ_BATCH);
    CHECK(batch.data != nullptr);
    CHECK(batch.size != 0U);
    CHECK(batch.lease != nullptr);
    CHECK(batch.row_count == 1U);
    const std::uint8_t first_byte = batch.data[0];
    l2flow_arrow_reader_close(reader);
    CHECK(batch.data[0] == first_byte);
    l2flow_arrow_batch_release(&batch);
    CHECK(batch.lease == nullptr);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string_view(argv[1]) == "--check-read-only") {
        return CheckReadOnlyMappings(argv[2], argv[3]);
    }
    TestRoundTripAndStarts();
    TestOverrunAndSeek();
    TestDecodedBatchRetainsLease();
    TestMultipleLeasesOnOneSegment();
    TestCrossThreadLeaseTransitions();
    TestPayloadCorruptionDetection();
    TestMismatchedControlFile();
    TestConcurrentDescriptorAndSegmentReuse();
    TestReaderMappingPermissions();
    TestValidationAndConsumerCapacity();
    TestHeaderAndSchemaBounds();
    TestEgressConfigValidation();
    TestCreationRollback();
    TestDeadConsumerReaping();
    TestCanonicalSchemas();
    TestHotEgressFanoutSink();
    TestHotEgressProducerLockAndMonotonicEpoch();
    TestOversizedBatchSplitting();
    TestCAbiLease();
    std::cout << "all Arrow ring tests passed\n";
    return 0;
}
