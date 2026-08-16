# Arrow shared-memory hot path

## 1. Contract and non-goals

The Arrow path is a bounded, volatile, same-host transport for the most recent
batches. It is not a WAL, a ClickHouse acknowledgement boundary, or a source
for recovering ClickHouse-unacknowledged records.

This distinction is required by the MDL source contract. After a disconnect or
process crash, `mdl_ingestd` cannot ask MDL to resume at the last native
sequence acknowledged by ClickHouse. A record that existed only in this Arrow
ring is irrecoverable from the ring once it is overwritten or the host loses
the tmpfs contents. The raw ClickHouse sink therefore receives decoded facts
through an independent pre-recovery branch with its own queueing, retry,
acknowledgement, and fail-closed policy.

The implemented boundary is:

```text
MDL callback -> admission/decode -> RawCanonicalBatch -> ClickHouse writers
                                |
                                +-> SequenceRecovery -> owner TickDispatch FIFO
                                                        |
                                    ordered/hole fill -> ArrowHotEgress -> mmap
```

`ArrowHotEgress` does not depend on `IngestEngine`; the daemon currently calls
it from the owner drain threads. ClickHouse writing does not run in those
threads: decoder lanes publish preallocated raw batch indices and dedicated
writer threads perform Arrow columnization, HTTP INSERT, and retry. A pinned
Arrow consumer therefore cannot control ClickHouse acknowledgement, and a
ClickHouse retry does not block Arrow owner-drain publication.

## 2. Ring set and ordering

One producer run creates:

- one Tick ring per instrument owner containing both ordered and accepted
  hole-fill rows;
- one Snapshot ring per instrument owner;
- one global Control ring.

Every individual ring is single-producer and multi-consumer. Tick and Snapshot
rings preserve the FIFO order delivered to that owner; Control preserves its
own FIFO order. There is no exchange-provided or locally claimed global order
across owners, rings, markets, channels, Tick, Snapshot, and Control.

`SequenceRecovery` emits five `TickDispatch` kinds on the owner FIFO. Arrow
accepts only `kProjectOrdered` and `kProjectHoleFill`; rejected occurrence
dispositions and owner correctness controls are consumed by Event/KLine but are
not Arrow Tick rows. The Tick schema's `stream_role` distinguishes ordered from
hole fill. For a hole fill, nullable `expected_sequence`, `admission_floor`,
`generation`, `evict_before`, and `catalog_match` preserve the arrival-time
classification token; those columns are null on an ordered row. Diagnostic
`ChannelGap` and `ChannelFault` records still enter the global Control ring.

`ingress_sequence` remains available for downstream correlation within one
feed epoch. Descriptor `first_ingress_sequence` and
`last_ingress_sequence` are minima and maxima, not proof that all intermediate
ingress values are present in that batch or ring.

Canonical C++ structs are projected field by field into typed Arrow columns;
their ABI bytes are never persisted. Schemas are dictionary-free because each
payload is a standalone Arrow IPC RecordBatch message. Writers use Arrow IPC
metadata V5, 64-byte alignment, no compression, and a CRC32 over the serialized
payload.

## 3. Memory protocol

Each ring has two files:

- `*.arrow`: producer read/write mapping, consumer read-only mapping;
- `*.ctl`: producer and consumer read/write mapping for registrations and
  segment lease bits.

The data mapping contains a fixed header, one serialized Arrow schema, a
power-of-two descriptor journal, and a separate segment pool. Descriptor and
payload publication uses release/acquire ordering. The two-variable handshake
between segment generation and reader lease bits uses sequentially consistent
operations so writer and reader cannot both miss the other's reservation:

1. reserve an unpinned segment by changing its generation to a writing state;
2. serialize and copy the RecordBatch payload into that segment;
3. publish the completed even segment generation;
4. fill a descriptor and publish its sequence last;
5. advance the header's newest sequence last.

Each data/control pair carries the same random 128-bit ring instance. A reader
rejects a control file from another ring even when both rings belong to the
same producer and have identical capacities. Shared-memory structure sizes and
the publication-field offset are compile-time protocol invariants.

The serialized schema is bounded to 16 MiB by protocol v2. Both creation and
reader validation enforce that limit before Arrow parses the schema, so a
damaged header cannot turn the rest of a large mapping into unbounded schema
input.

A reader atomically snapshots descriptor fields, pins the referenced segment,
then checks the segment generation and descriptor publication sequence again.
It never uses mutable descriptor fields after that validation. CRC and the
segment header are checked before a lease is returned. A local RAII guard
releases the pin if validation or lease allocation fails.

Reader leases set one bit per process registration and segment. Multiple local
Arrow objects for the same segment are reference-counted before that bit is
cleared. The local zero-to-one and one-to-zero transitions are serialized so a
lease released on one thread cannot race a new lease on another thread and
prematurely clear the shared bit. A decoded C++ RecordBatch, a PyArrow
RecordBatch, or a Polars object
may outlive the reader object because its backing buffer retains the lease.
Keeping many old batches alive can pin the segment pool and cause hot-path
loss, so factor code must release batches promptly rather than use the ring as
an object cache.

Consumer slots are identified by PID plus Linux `/proc/<pid>/stat` process
start ticks. The producer reclaims a dead consumer only after positive evidence
that the process instance is gone; a transient `/proc` read failure is not
treated as death. A live but stalled consumer is deliberately not reaped,
because clearing its lease could make an existing Arrow view reference memory
being overwritten. Producer and consumers must share the same Linux PID
namespace; otherwise a consumer PID does not identify the same process from the
producer's `/proc` view and lease recovery is unsafe. Do not fork a process with
an open reader; open a separate reader after the fork.

`SharedArrowRingWriter::TryPublish` is a single-producer-thread API. Publication
must be quiesced before `Seal` or destruction. Each reader handle is one
single-threaded mutable cursor: `TryRead`, seek, heartbeat, and decode calls on
that handle must not overlap. Returned leases and decoded Arrow objects may be
released on other threads and may outlive the reader handle.

## 4. Overrun and loss semantics

There are two different loss cases.

Reader overrun occurs when the requested descriptor sequence has already been
replaced. `TryRead` returns `kOverrun` with the current oldest/newest interval;
the Python API raises `ArrowHotOverrun`. The reader does not silently jump.
The caller must choose `seek_earliest()` to accept an incomplete suffix or
`seek_latest()` to discard the retained backlog. That policy decision belongs
to the factor process.

Producer hot loss occurs when all reusable segments are pinned, or when one
row remains larger than a segment after recursive compact splitting. This is a
soft Arrow-only loss: it increments daemon counters and attempts to publish a
`kHotPublishOverrun` Control row naming the affected stream, shard, and dropped
row count. If the Control ring is itself unable to publish, the
`arrow_control_dropped_rows` daemon counter is the remaining signal. Production
monitoring must alert on all three counters:

```text
arrow_no_segment_dropped_rows
arrow_oversized_dropped_rows
arrow_control_dropped_rows
```

These counters do not authorize the durable pipeline to acknowledge anything.
An internal Arrow/schema/protocol error is sticky, marks the egress unhealthy,
and causes the daemon to shut down instead of continuing with an unknown hot
path state.

## 5. Feed epochs and MDL disconnects

Every process attempt uses an explicit positive `feed_session_epoch`. It is a
local continuity epoch, not an MDL native sequence, transport offset,
timestamp, or ClickHouse revision.

The root directory contains an advisory `.producer.lock`, so only one
`ArrowHotEgress` can publish there. Startup reads the manifest selected by
`CURRENT` and rejects an epoch less than or equal to the current epoch. The
caller must still allocate epochs from durable, monotonically increasing
supervisor state. The check under the Arrow root is only a local guard: a tmpfs
is cleared by reboot, and a wall clock can move backward.

All ring files and `manifest.txt` are created in a unique producer directory.
Only after they are ready does the producer atomically rename a temporary file
to `CURRENT`. Readers first resolve `CURRENT`, then validate the manifest,
producer instance, stream kind, shard, and epoch.

The MDL 2.13.234 headers define these API and system messages:

```text
API  ServiceID=1, ServiceVersion=101, MessageID=2   ConnectErrorEvent
API  ServiceID=1, ServiceVersion=101, MessageID=3   DisconnectedEvent
API  ServiceID=1, ServiceVersion=101, MessageID=5   MessageServiceTimeOutEvent
API  ServiceID=1, ServiceVersion=101, MessageID=6   MessageDiscardedEvent
SYS  ServiceID=2, ServiceVersion=101, MessageID=2   LogonResponse
SYS  ServiceID=2, ServiceVersion=101, MessageID=23  SubscribeResponse
```

An empty immediate error from `Subscriber::Connect()` only means that the
connection attempt was started; it is not the feed-ready boundary. The daemon
publishes `kFeedConnected` only after a bounds-checked binary `LogonResponse`
has `ReturnCode == MDLEC_OK` and every tuple selected by the stream config has a
successful `MessageStatus`. Successful statuses may be accumulated from
subsequent `SubscribeResponse` messages, but a `SubscribeResponse` does not
replace the successful Logon response. The recorded event time is the callback
entry time of the response that completes readiness. A market callback before
this state is counted and discarded in PARTIAL mode; that callback is not
published to raw, derived, or Arrow outputs. FROM_OPEN treats the same callback
as a fatal control-order failure because its complete-prefix contract does not
permit dropping initial records.

The event names and IDs above are SDK facts. The following shutdown decisions
are local continuity policy. `ConnectErrorEvent`, `DisconnectedEvent`, a
synchronous `Connect()` error, `MessageServiceTimeOutEvent`,
`MessageDiscardedEvent`, a rejected configured subscription, a malformed
system response, and expiration of `--sdk-ready-timeout-seconds` all terminate
the current process/feed epoch. Treating API timeout/discard events as fatal is
deliberately conservative; the header defines the events but does not itself
state this process policy.

On the first boundary, the adapter stops admitting subsequent market-data
callbacks. The daemon then quiesces the SDK, drains records already admitted to
the engine and owner queues, publishes the typed boundary to the Control ring,
publishes `kProducerSealed`, seals every ring, and exits nonzero. An SDK
auto-reconnect therefore cannot feed data into the old epoch. The supervisor
must restart the process with a strictly larger epoch; neither Arrow nor
ClickHouse can ask MDL to resume at a ClickHouse-acknowledged native sequence.

A process crash cannot publish a disconnect or seal event. Its rings remain in
`ACTIVE` state with a heartbeat that stops changing. A reader may use the raw
`CLOCK_MONOTONIC` heartbeat, the unchanged `CURRENT` producer instance, and
external process supervision as evidence, but heartbeat age alone cannot
distinguish a crash from a long scheduler or host stall. After restart, a new
producer instance and larger epoch are the explicit discontinuity boundary.

## 6. Discovery layout

`CURRENT` contains one producer directory basename. Its strict `key=value`
manifest has this shape:

```text
format=l2flow-arrow-hot-v1
protocol_version=2
producer_instance=<128-bit hex id>
feed_session_epoch=<positive integer>
owner_count=<N>
tick.0=tick-owner-0.arrow
tick_control.0=tick-owner-0.ctl
snapshot.0=snapshot-owner-0.arrow
snapshot_control.0=snapshot-owner-0.ctl
control=control.arrow
control_control=control.ctl
```

Old run directories are not a history database and are not deleted
automatically. Deploy the root on a dedicated tmpfs, normally under `/dev/shm`,
and remove obsolete run directories only after no new reader needs to open
them. Existing Linux mappings remain valid after unlink, but an opener racing
with cleanup must retry discovery. A reboot or tmpfs cleanup is expected to
remove the data.

The files are created for a same-service-account deployment. A consumer needs
read access to `*.arrow` and write access to `*.ctl`; run producer and consumers
under the same Unix account unless permissions are deliberately managed.

## 7. Capacity and latency sizing

For descriptor capacity `D`, segment count `S`, segment payload bytes `P`, and
maximum consumers `C`, approximate logical bytes for one ring are:

```text
data = aligned_header_schema + 128*D + S*align64(64 + P)
ctl  = align4096(4096 + 64*C) + 64*S
```

The complete run has `2*owner_count + 1` rings. Tick/Snapshot and diagnostic
rings can use different `P`. Creation uses `ftruncate` followed by
`posix_fallocate`, so insufficient backing space fails at startup instead of
surfacing as a first-write `SIGBUS` in the hot path. Mapped pages can still
fault on first touch, and tmpfs capacity plus virtual address space must be
planned for the complete configured maximum.

`D` determines descriptor history. Measure the actual published batch rate
for each ring; its approximate retained time is `D / batches_per_second`.
When batches are consistently row-triggered with `B` rows at row rate `R`, the
estimate is `D*B/R`, but time-triggered partial batches make that shortcut
optimistic. `S` must be at least `D`; `S-D` is lease headroom, not additional
descriptor history.

Choose `P` from measured serialized batch sizes. Oversized multi-row batches
are compacted and split recursively so a zero-copy slice cannot retain the
original large buffers. A single row larger than `P` is dropped from the hot
path and reported as above.

`--arrow-batch-max-delay-ns` is a batching/scheduling target, not a hard
real-time guarantee. Row limits, Arrow allocation/serialization, CPU
saturation, and OS scheduling all contribute to latency. Owner drain loops use
bounded bursts so a continuously full Tick queue cannot indefinitely starve
Snapshot/Control deadline checks. Benchmark with representative nested book
depth and consumer lease duration before selecting production values.

The production default is 1,000,000 ns. At the 1M-message/s target this keeps
the fixed Arrow IPC cost bounded while retaining approximately millisecond
reader visibility. A lower value such as 200,000 ns is supported, but it can
multiply the aggregate batch rate across all owners and must be requalified;
it is a latency/throughput choice, not a universally safer setting.

The producer heartbeat interval defaults to one second. Consumers should use
multiple missed intervals plus external process state, rather than one late
sample, when declaring a producer unavailable.

## 8. Throughput and latency qualification

The benchmark target is an aggregate ordered Tick rate of 1M messages/s, with
required coverage at 500k, 750k, and 1M messages/s. An Arrow-enabled benchmark
uses the same synthetic MDL callback, decode/recovery, owner dispatch, and
`ArrowHotEgress` code as the daemon. It also starts one C++ ring reader per
owner; every reader verifies CRC/protocol state, decodes every IPC batch,
checks every native sequence, and releases the lease before advancing.

Configure a Release build with Arrow enabled:

```bash
cmake -S . -B build-benchmark-arrow \
  -DCMAKE_BUILD_TYPE=Release \
  -DL2FLOW_CH_BUILD_TESTS=OFF \
  -DL2FLOW_CH_BUILD_BENCHMARKS=ON \
  -DL2FLOW_CH_ENABLE_ARROW_RING=ON
cmake --build build-benchmark-arrow --target benchmark_mdl_ingest -j
```

Use a new dedicated tmpfs root and a larger epoch for every run. The following
command shape is repeated with `RATE` equal to `500000`, `750000`, and
`1000000`, and `EPOCH` equal to three strictly increasing positive values:

```bash
numactl --physcpubind=64-127 --membind=1 \
./build-benchmark-arrow/benchmark_mdl_ingest \
  --rate RATE --seconds 300 --warmup-seconds 5 \
  --pattern ordered --sample-every 67 \
  --channels 16 --tick-lanes 12 --owners 16 \
  --producer-cpu 127 --first-consumer-cpu 64 \
  --first-decoder-cpu 80 --first-arrow-reader-cpu 93 \
  --arrow-ring-dir /dev/shm/l2flow/arrow-benchmark \
  --feed-epoch EPOCH \
  --arrow-descriptors 256 --arrow-segments 320 \
  --arrow-tick-bytes 262144 --arrow-batch-rows 256 \
  --arrow-max-delay-ns 1000000
```

Those CPU numbers describe one 64-physical-core NUMA node on the validation
host. They are not portable defaults. On another machine, select one NUMA node
with `lscpu -e`/`numactl --hardware`, reserve distinct physical cores for the
producer, owner consumers, decoders, and Arrow readers, and verify the reported
`observed_bindings_valid=true` plus `/proc/self/numa_maps` counters. A benchmark
on contended CPU 0-class cores can measure host scheduling noise instead of the
pipeline.

The pass threshold for scheduled producer, owner/Arrow append, and Arrow
reader throughput is at least 99% of the requested rate. A pass also requires
exact admitted, dispatched, published, and read row counts; correct affinity;
and zero lane-full, dispatch-overflow, gap, hole-fill/rejection, channel-fault,
ordering, clock, protocol, oversized, pinned-segment, and Control-drop counts.
The output contains two distinct latency distributions:

- `callback_entry_to_arrow_append_us` ends after the owner calls the egress;
  a partial batch may not yet be published at that point.
- `callback_entry_to_arrow_reader_us` ends after a reader acquires the segment
  lease, validates the payload, and completes Arrow IPC decode. This is the
  relevant in-host transport latency boundary for the C++ reader.

A five-second window is useful for development regression checks but is not a
production capacity claim. Qualification requires the 300-second sweep on the
actual CPU/NUMA/tmpfs placement, with representative Tick/Snapshot mix, book
depth, batch sizes, and reader lease duration. This C++ reader benchmark does
not establish Python factor throughput; the cross-language tests establish
ABI and lifetime correctness, and the production Python/Polars workload must
be measured separately. Neither benchmark includes the network/MDL work before
callback entry or a ClickHouse acknowledgement.

The 2026-08-07 development run used Arrow 25, a Release build, one NUMA-node
CPU layout, `/dev/shm`, 16 owners/readers, 256-row limits, and the 1 ms delay.
It produced the following short-run evidence; all rows were exact and every
loss/error counter was zero:

| Target | Window | Producer msg/s | Arrow reader msg/s | Reader p50 | Reader p99 | Status |
|---:|---:|---:|---:|---:|---:|:---:|
| 500k | 5 s | 500,000 | 499,781 | 611 us | 1,130 us | PASS |
| 750k | 5 s | 750,000 | 749,649 | 617 us | 1,124 us | PASS |
| 1M | 5 s | 1,000,011 | 999,227 | 667 us | 1,193 us | PASS |
| 1M | 30 s | 1,000,000 | 999,924 | 646 us | 1,175 us | PASS |

For comparison, the same 30-second 1M target with a 200 us delay completed all
rows without corruption or loss but sustained only about 645k messages/s,
because it generated roughly 2.75 million small IPC batches. This measured
fixed-cost tradeoff is why 1 ms is the default. It does not remove the need for
the 300-second deployment qualification described above.

## 9. Build, launch, and Python reader

Arrow 25 requires the project C++20 build. Create an isolated Python
environment and configure the cross-language test with it:

```bash
uv venv --python 3.13 python/.venv
UV_CACHE_DIR=/tmp/l2flow-uv-cache uv pip install \
  --python python/.venv/bin/python pyarrow==25.0.0 polars==1.43.2
cmake -S . -B build-arrow \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_CH_BUILD_BENCHMARKS=OFF \
  -DL2FLOW_CH_PYTHON_EXECUTABLE="$PWD/python/.venv/bin/python"
cmake --build build-arrow -j
ctest --test-dir build-arrow --output-on-failure
```

Example launch arguments for the Arrow portion are:

```text
--arrow-ring-dir /dev/shm/l2flow/arrow
--feed-epoch <durably allocated increasing process epoch>
--sdk-ready-timeout-seconds 30
--arrow-descriptors 1024
--arrow-segments 1088
--arrow-tick-batch-rows 256
--arrow-snapshot-batch-rows 16
  --arrow-batch-max-delay-ns 1000000
```

Python reads a current ring without copying its IPC payload:

```python
from l2flow_arrow_hot import ArrowHotOverrun, RingReader

reader = RingReader.open_current(
    "/dev/shm/l2flow/arrow", "tick", owner=0, start="latest"
)
try:
    hot = reader.try_read()       # None means empty or a transient retry
    if hot is not None:
        frame = hot.to_polars()   # rechunk=False
except ArrowHotOverrun:
    reader.seek_latest()          # explicit application policy
except StopIteration:
    pass                          # sealed and fully consumed
```

The Python binding verifies the exact `l2flow-arrow-hot/2 arrow-ipc-v5` shared
library ABI before opening a reader; it will not call an older ctypes layout.
An unclosed `RingReader` also releases its C++ registration during Python object
finalization, though explicit context-manager or `close()` use remains the
deterministic production pattern.

The producer path is not end-to-end zero-copy: canonical values are appended
to Arrow builders, Arrow serializes an IPC message, and that message is copied
once into the reserved mmap segment. The consumer payload is lease-backed and
low-copy; PyArrow or Polars may still copy for operations or types that require
new buffers.
