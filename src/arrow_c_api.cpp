#include "l2flow/arrow/c_api.h"

#include "l2flow/arrow/ring.h"

#include <arrow/buffer.h>
#include <arrow/ipc/writer.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

struct l2flow_arrow_reader final {
    std::unique_ptr<l2flow::arrow_hot::SharedArrowRingReader> reader;
    std::shared_ptr<arrow::Buffer> serialized_schema;
};

static_assert(sizeof(void*) == 8U,
              "the l2flow-arrow-hot/2 C ABI requires a 64-bit process");
static_assert(std::is_standard_layout_v<l2flow_arrow_batch>);
static_assert(sizeof(l2flow_arrow_batch) == 120U);
static_assert(offsetof(l2flow_arrow_batch, lease) == 16U);
static_assert(offsetof(l2flow_arrow_batch, batch_sequence) == 24U);
static_assert(offsetof(l2flow_arrow_batch, row_count) == 112U);
static_assert(offsetof(l2flow_arrow_batch, flags) == 116U);

namespace {

using l2flow::arrow_hot::ArrowBatchLease;
using l2flow::arrow_hot::ReadCode;
using l2flow::arrow_hot::ReadMetadata;
using l2flow::arrow_hot::ReadResult;
using l2flow::arrow_hot::ReaderStart;
using l2flow::arrow_hot::RingLocation;
using l2flow::arrow_hot::SharedArrowRingReader;

void CopyError(std::string_view value,
               char* output,
               std::size_t capacity) noexcept {
    if (output == nullptr || capacity == 0U) {
        return;
    }
    const std::size_t count = std::min(value.size(), capacity - 1U);
    if (count != 0U) {
        std::memcpy(output, value.data(), count);
    }
    output[count] = '\0';
}

void ClearError(char* output, std::size_t capacity) noexcept {
    if (output != nullptr && capacity != 0U) {
        output[0] = '\0';
    }
}

void FillMetadata(const ReadMetadata& source,
                  l2flow_arrow_batch* output) noexcept {
    output->batch_sequence = source.batch_sequence;
    output->oldest_available_sequence = source.oldest_available_sequence;
    output->newest_available_sequence = source.newest_available_sequence;
    output->feed_session_epoch = source.feed_session_epoch;
    output->first_ingress_sequence = source.first_ingress_sequence;
    output->last_ingress_sequence = source.last_ingress_sequence;
    output->minimum_exchange_time_ns = source.minimum_exchange_time_ns;
    output->maximum_exchange_time_ns = source.maximum_exchange_time_ns;
    output->publish_monotonic_ns = source.publish_monotonic_ns;
    output->producer_instance_high = source.producer_instance.high;
    output->producer_instance_low = source.producer_instance.low;
    output->row_count = source.row_count;
    output->flags = source.flags;
}

[[nodiscard]] int ConvertReadCode(ReadCode code) noexcept {
    switch (code) {
        case ReadCode::kBatch:
            return L2FLOW_ARROW_READ_BATCH;
        case ReadCode::kEmpty:
            return L2FLOW_ARROW_READ_EMPTY;
        case ReadCode::kOverrun:
            return L2FLOW_ARROW_READ_OVERRUN;
        case ReadCode::kRetry:
            return L2FLOW_ARROW_READ_RETRY;
        case ReadCode::kCorrupt:
            return L2FLOW_ARROW_READ_CORRUPT;
        case ReadCode::kClosed:
            return L2FLOW_ARROW_READ_CLOSED;
    }
    return L2FLOW_ARROW_READ_API_ERROR;
}

}  // namespace

extern "C" int l2flow_arrow_reader_open(
    const char* data_path,
    const char* control_path,
    int start,
    l2flow_arrow_reader** output,
    char* error,
    size_t error_capacity) {
    if (output != nullptr) {
        *output = nullptr;
    }
    if (data_path == nullptr || control_path == nullptr || output == nullptr ||
        (start != L2FLOW_ARROW_START_LATEST &&
         start != L2FLOW_ARROW_START_EARLIEST_AVAILABLE)) {
        CopyError("invalid Arrow reader open arguments", error,
                  error_capacity);
        return 1;
    }
    try {
        std::string detail;
        auto reader = SharedArrowRingReader::Open(
            RingLocation{std::filesystem::path(data_path),
                         std::filesystem::path(control_path)},
            start == L2FLOW_ARROW_START_LATEST
                ? ReaderStart::kLatest
                : ReaderStart::kEarliestAvailable,
            &detail);
        if (reader == nullptr) {
            CopyError(detail, error, error_capacity);
            return 1;
        }
        auto serialized = arrow::ipc::SerializeSchema(*reader->schema());
        if (!serialized.ok()) {
            CopyError(serialized.status().ToString(), error, error_capacity);
            return 1;
        }
        auto wrapper = std::make_unique<l2flow_arrow_reader>();
        wrapper->reader = std::move(reader);
        wrapper->serialized_schema = std::move(*serialized);
        *output = wrapper.release();
        ClearError(error, error_capacity);
        return 0;
    } catch (const std::exception& exception) {
        CopyError(exception.what(), error, error_capacity);
        return 1;
    } catch (...) {
        CopyError("Arrow reader open failed unexpectedly", error,
                  error_capacity);
        return 1;
    }
}

extern "C" void l2flow_arrow_reader_close(l2flow_arrow_reader* reader) {
    delete reader;
}

extern "C" int l2flow_arrow_reader_schema(
    const l2flow_arrow_reader* reader,
    const uint8_t** data,
    size_t* size,
    char* error,
    size_t error_capacity) {
    if (data != nullptr) {
        *data = nullptr;
    }
    if (size != nullptr) {
        *size = 0U;
    }
    if (reader == nullptr || reader->serialized_schema == nullptr ||
        data == nullptr || size == nullptr ||
        reader->serialized_schema->size() < 0 ||
        static_cast<std::uint64_t>(reader->serialized_schema->size()) >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        CopyError("invalid Arrow reader schema request", error,
                  error_capacity);
        return 1;
    }
    *data = reader->serialized_schema->data();
    *size = static_cast<std::size_t>(reader->serialized_schema->size());
    ClearError(error, error_capacity);
    return 0;
}

extern "C" int l2flow_arrow_reader_try_read(
    l2flow_arrow_reader* reader,
    l2flow_arrow_batch* output,
    char* error,
    size_t error_capacity) {
    if (output != nullptr) {
        *output = {};
    }
    if (reader == nullptr || reader->reader == nullptr || output == nullptr) {
        CopyError("invalid Arrow reader read request", error, error_capacity);
        return L2FLOW_ARROW_READ_API_ERROR;
    }
    try {
        ReadResult result = reader->reader->TryRead();
        FillMetadata(result.metadata, output);
        if (result.code != ReadCode::kBatch) {
            CopyError(result.error, error, error_capacity);
            return ConvertReadCode(result.code);
        }
        if (result.lease == nullptr) {
            CopyError("Arrow reader returned a batch without a lease", error,
                      error_capacity);
            return L2FLOW_ARROW_READ_API_ERROR;
        }
        auto lease = std::make_unique<std::shared_ptr<ArrowBatchLease>>(
            std::move(result.lease));
        output->data = (*lease)->data();
        output->size = (*lease)->size();
        output->lease = lease.release();
        ClearError(error, error_capacity);
        return L2FLOW_ARROW_READ_BATCH;
    } catch (const std::exception& exception) {
        CopyError(exception.what(), error, error_capacity);
        return L2FLOW_ARROW_READ_API_ERROR;
    } catch (...) {
        CopyError("Arrow reader read failed unexpectedly", error,
                  error_capacity);
        return L2FLOW_ARROW_READ_API_ERROR;
    }
}

extern "C" void l2flow_arrow_batch_release(l2flow_arrow_batch* batch) {
    if (batch == nullptr) {
        return;
    }
    delete static_cast<std::shared_ptr<ArrowBatchLease>*>(batch->lease);
    *batch = {};
}

extern "C" int l2flow_arrow_reader_seek_earliest(
    l2flow_arrow_reader* reader,
    char* error,
    size_t error_capacity) {
    if (reader == nullptr || reader->reader == nullptr ||
        !reader->reader->SeekToEarliestAvailable()) {
        CopyError("cannot seek Arrow reader to earliest batch", error,
                  error_capacity);
        return 1;
    }
    ClearError(error, error_capacity);
    return 0;
}

extern "C" int l2flow_arrow_reader_seek_latest(
    l2flow_arrow_reader* reader,
    char* error,
    size_t error_capacity) {
    if (reader == nullptr || reader->reader == nullptr ||
        !reader->reader->SeekToLatest()) {
        CopyError("cannot seek Arrow reader to latest batch", error,
                  error_capacity);
        return 1;
    }
    ClearError(error, error_capacity);
    return 0;
}

extern "C" uint32_t l2flow_arrow_reader_stream_kind(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : static_cast<std::uint32_t>(reader->reader->stream_kind());
}

extern "C" uint32_t l2flow_arrow_reader_shard_id(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : reader->reader->shard_id();
}

extern "C" uint64_t l2flow_arrow_reader_feed_session_epoch(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : reader->reader->feed_session_epoch();
}

extern "C" uint64_t l2flow_arrow_reader_producer_instance_high(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : reader->reader->producer_instance().high;
}

extern "C" uint64_t l2flow_arrow_reader_producer_instance_low(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : reader->reader->producer_instance().low;
}

extern "C" uint32_t l2flow_arrow_reader_producer_state(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : static_cast<std::uint32_t>(reader->reader->producer_state());
}

extern "C" uint64_t
l2flow_arrow_reader_producer_heartbeat_monotonic_ns(
    const l2flow_arrow_reader* reader) {
    return reader == nullptr || reader->reader == nullptr
        ? 0U
        : reader->reader->producer_heartbeat_monotonic_ns();
}

extern "C" const char* l2flow_arrow_library_version(void) {
    return "l2flow-arrow-hot/2 arrow-ipc-v5";
}
