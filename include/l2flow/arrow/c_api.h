#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_arrow_reader l2flow_arrow_reader;

enum l2flow_arrow_reader_start {
    L2FLOW_ARROW_START_LATEST = 0,
    L2FLOW_ARROW_START_EARLIEST_AVAILABLE = 1,
};

enum l2flow_arrow_read_code {
    L2FLOW_ARROW_READ_BATCH = 0,
    L2FLOW_ARROW_READ_EMPTY = 1,
    L2FLOW_ARROW_READ_OVERRUN = 2,
    L2FLOW_ARROW_READ_RETRY = 3,
    L2FLOW_ARROW_READ_CORRUPT = 4,
    L2FLOW_ARROW_READ_CLOSED = 5,
    L2FLOW_ARROW_READ_API_ERROR = 6,
};

typedef struct l2flow_arrow_batch {
    const uint8_t* data;
    size_t size;
    void* lease;
    uint64_t batch_sequence;
    uint64_t oldest_available_sequence;
    uint64_t newest_available_sequence;
    uint64_t feed_session_epoch;
    uint64_t first_ingress_sequence;
    uint64_t last_ingress_sequence;
    uint64_t minimum_exchange_time_ns;
    uint64_t maximum_exchange_time_ns;
    uint64_t publish_monotonic_ns;
    uint64_t producer_instance_high;
    uint64_t producer_instance_low;
    uint32_t row_count;
    uint32_t flags;
} l2flow_arrow_batch;

// All functions return zero on API success unless documented otherwise.
// Errors are copied as a nul-terminated string when error_capacity is nonzero.
// A reader handle is a single-threaded mutable cursor: read, seek, schema, and
// metadata calls on the same handle must not overlap with each other or close.
int l2flow_arrow_reader_open(const char* data_path,
                             const char* control_path,
                             int start,
                             l2flow_arrow_reader** output,
                             char* error,
                             size_t error_capacity);

void l2flow_arrow_reader_close(l2flow_arrow_reader* reader);

int l2flow_arrow_reader_schema(const l2flow_arrow_reader* reader,
                               const uint8_t** data,
                               size_t* size,
                               char* error,
                               size_t error_capacity);

// Returns one of l2flow_arrow_read_code. A BATCH result owns one lease in
// output; release it exactly once with l2flow_arrow_batch_release. The lease
// may be released on another thread and may outlive its reader handle. Output
// must be fresh or previously released; overwriting a live lease is invalid.
int l2flow_arrow_reader_try_read(l2flow_arrow_reader* reader,
                                 l2flow_arrow_batch* output,
                                 char* error,
                                 size_t error_capacity);

void l2flow_arrow_batch_release(l2flow_arrow_batch* batch);

int l2flow_arrow_reader_seek_earliest(l2flow_arrow_reader* reader,
                                      char* error,
                                      size_t error_capacity);
int l2flow_arrow_reader_seek_latest(l2flow_arrow_reader* reader,
                                    char* error,
                                    size_t error_capacity);

uint32_t l2flow_arrow_reader_stream_kind(
    const l2flow_arrow_reader* reader);
uint32_t l2flow_arrow_reader_shard_id(
    const l2flow_arrow_reader* reader);
uint64_t l2flow_arrow_reader_feed_session_epoch(
    const l2flow_arrow_reader* reader);
uint64_t l2flow_arrow_reader_producer_instance_high(
    const l2flow_arrow_reader* reader);
uint64_t l2flow_arrow_reader_producer_instance_low(
    const l2flow_arrow_reader* reader);
uint32_t l2flow_arrow_reader_producer_state(
    const l2flow_arrow_reader* reader);
uint64_t l2flow_arrow_reader_producer_heartbeat_monotonic_ns(
    const l2flow_arrow_reader* reader);

const char* l2flow_arrow_library_version(void);

#ifdef __cplusplus
}
#endif
