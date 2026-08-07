# Arrow + ClickHouse Release Benchmark

Date: 2026-08-07 (Asia/Shanghai)

## Outcome

The combined synthetic MDL callback, ingest, shared-memory Arrow reader, and
real ClickHouse `raw_tick` path sustained both requested rates in a Release
build after correcting benchmark thread-affinity inheritance and selecting a
batching configuration that the local ClickHouse server could sustain.

| Target | Producer | Dispatch | Arrow reader | Final ClickHouse ACK | Result |
| ---: | ---: | ---: | ---: | ---: | :---: |
| 800,000 msg/s | 800,000.014 | 799,999.966 | 796,304.174 | 796,865.708 | PASS |
| 1,000,000 msg/s | 1,000,000.006 | 999,999.928 | 992,591.917 | 993,689.106 | PASS |

`PASS` requires each measured rate to be at least 99% of target, exact row
counts throughout the ingest and Arrow paths, a healthy engine and sink, no
ordering or protocol errors, all ClickHouse batches acknowledged and released,
no unacknowledged batch, and no retry or unknown outcome.

The unmodified ClickHouse runtime defaults do not sustain this workload. That
failure and the final runtime settings are documented below so the tuned result
is not confused with a default-parameter result.

## Scope

The benchmark enters the production synthetic callback adapter and exercises:

```text
synthetic MDL callback
  -> admission and decode/normalize
  -> preallocated raw canonical batches
  -> ArrowStream ClickHouse INSERT
  -> raw_tick MergeTree
  -> instrument-owner dispatch
  -> shared-memory Arrow egress
  -> Arrow reader and RecordBatch decode
```

Only Tick messages are generated. Snapshot throughput is not covered by this
run. The benchmark uses synchronous HTTP `ArrowStream` INSERTs into a real local
ClickHouse 26.8 server.

## Build

Source revision at test time:

```text
3bd6fc88a268d1d5df50a2d346a5fcc33edd9eed
```

The worktree also contained the uncommitted ClickHouse integration under test.

```bash
cmake -S . -B build-benchmark-clickhouse-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DL2FLOW_CH_ENABLE_ARROW_RING=ON \
  -DL2FLOW_CH_ENABLE_CLICKHOUSE_RAW=ON \
  -DL2FLOW_CH_BUILD_BENCHMARKS=ON \
  -DL2FLOW_CH_BUILD_TESTS=ON \
  -DL2FLOW_CH_PYTHON_EXECUTABLE="$PWD/python/.venv/bin/python"
cmake --build build-benchmark-clickhouse-release -j 16
```

Toolchain:

| Component | Version |
| --- | --- |
| Compiler | GCC 13.3.0 |
| Apache Arrow | 25.0.0 |
| ClickHouse | 26.8.1.923 |
| Kernel | Linux 6.17.0-35-generic |

## Host And Isolation

| Resource | Configuration |
| --- | --- |
| CPU | 2 x AMD EPYC 9534, 64 cores/socket, SMT2, 256 logical CPUs |
| RAM | 1.0 TiB |
| NUMA node 0 | CPUs `0-63,128-191`, ClickHouse server |
| NUMA node 1 | CPUs `64-127,192-255`, benchmark process and memory |
| Owner consumers | CPUs `64-79` |
| Tick and snapshot decoders | CPUs `80-92` |
| Arrow readers | CPUs `93-108` |
| Callback producer | CPU `127` |
| ClickHouse writers | Inherited the broad node-1 benchmark mask |
| Transport | Loopback HTTP, synchronous ArrowStream |
| Storage | Repository-local ClickHouse data on the local filesystem |

The ClickHouse server was moved to the node-0 CPU mask with
`taskset -a -pc 0-63,128-191 <pid>`. The benchmark was run with:

```bash
numactl --physcpubind=64-127,192-255 --membind=1 ...
```

The benchmark originally pinned its main thread before creating ClickHouse
writers. POSIX threads inherit creator affinity, so every ClickHouse writer was
accidentally restricted to producer CPU 127. The benchmark now starts the sink
while the main thread still has the broad node-1 mask and pins the producer only
after the writer threads exist. Without that correction, measured producer
throughput was only approximately 577k to 683k msg/s and represented benchmark
CPU contention rather than the intended topology.

## Runtime Configuration

Common ingest configuration:

| Setting | Value |
| --- | ---: |
| Measurement | 30 s |
| Warm-up | 5 s |
| Channels / instrument owners | 16 / 16 |
| Tick decoder lanes | 12 |
| Latency sampling stride | 64 |
| Pattern | ordered |

Arrow used its benchmark defaults:

| Setting | Value |
| --- | ---: |
| Tick batch rows | 256 |
| Maximum batch delay | 1 ms |
| Segment payload | 262,144 bytes |
| Segments per owner | 320 |
| Descriptor capacity | 256 |

ClickHouse used:

| Setting | Compiled default | Tested value |
| --- | ---: | ---: |
| Writer threads | 2 | 6 |
| Tick batch rows | 16,384 | 16,384 |
| Tick batch bytes | 16 MiB | 16 MiB |
| Tick maximum batch delay | 5 ms | 500 ms |
| Queue batches per lane | 8 | 16 |
| Async insert | off | off |
| Insert quorum | 0 | 0 |

At these rates the 16,384-row cap normally closes a batch before the 500 ms
timer. Mean persisted batches contained 16,355 rows at 800k and 16,325 rows at
1M.

## Default-Parameter Failure

An 800k msg/s smoke run with the ClickHouse runtime defaults failed closed after
approximately 45 ms:

| Metric | Value |
| --- | ---: |
| Admitted callbacks | 36,103 |
| Rows ultimately ACKed | 36,101 |
| Batches queued / ACKed / released | 108 / 108 / 108 |
| Fatal reason | `raw_tick lane 5: preallocated batch pool exhausted` |
| Server INSERT duration p50 / p95 / p99 | 53 / 66 / 77 ms |
| Mean rows per INSERT | approximately 334 |

With 12 lanes and a 5 ms flush timer, the sink produces roughly 2,400 small
INSERTs per second. Two synchronous HTTP writers cannot acknowledge them before
the eight-batch-per-lane pools fill. The fail-closed behavior worked as designed:
all 36,101 rows that reached the sink were ACKed, while the two admitted but not
yet decoded callbacks were terminated behind an explicit continuity boundary.
This runtime configuration is not viable at the requested rates.

## Throughput And Durability

| Metric | 800k run | 1M run |
| --- | ---: | ---: |
| Measured callbacks | 24,000,000 | 30,000,000 |
| Warm-up callbacks | 4,000,000 | 5,000,000 |
| Producer msg/s | 800,000.014 | 1,000,000.006 |
| Dispatch msg/s | 799,999.966 | 999,999.928 |
| Arrow reader msg/s | 796,304.174 | 992,591.917 |
| ClickHouse ACK msg/s at producer end | 795,794.298 | 993,338.518 |
| ClickHouse final effective ACK msg/s | 796,865.708 | 993,689.106 |
| Producer end to final ACK | 137.665 ms | 222.284 ms |
| `RawClickHouseSink::Stop` drain | 137.128 ms | 221.753 ms |
| Rows behind ACK at producer end | 147,200 | 233,152 |
| Final rows ACKed | 28,000,000 | 35,000,000 |
| Batches queued / ACKed / released | 1,712 / 1,712 / 1,712 | 2,144 / 2,144 / 2,144 |
| Retry attempts | 0 | 0 |
| Unknown outcomes | 0 | 0 |
| Unacknowledged batches after stop | 0 | 0 |
| Arrow or ingest dropped rows | 0 | 0 |
| Ordering / protocol / clock errors | 0 / 0 / 0 | 0 / 0 / 0 |

The databases used for the reported runs were:

```text
l2flow_bench_release_800k_20260807_02
l2flow_bench_release_1m_20260807_01
```

After background merges, both tables had eight active parts. Their compressed
disk sizes were approximately 1000.46 MiB and 1.22 GiB, respectively.

## Latency

Callback-entry to Arrow append latency includes admission, decode/normalize,
sequence recovery, owner dispatch, and the owner-side Arrow append operation.

| Target | Samples | p50 | p90 | p99 | p99.9 | Maximum |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 800k | 375,008 | 2.880 us | 24.000 us | 126.931 us | 163.662 us | 2,577.985 us |
| 1M | 468,752 | 3.020 us | 18.350 us | 126.142 us | 166.512 us | 3,415.914 us |

Callback-entry to Arrow reader latency ends after the shared-memory Arrow batch
has been decoded by the benchmark reader.

| Target | Samples | p50 | p90 | p99 | p99.9 | Maximum |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 800k | 375,008 | 624.835 us | 1,025.130 us | 1,130.371 us | 1,166.512 us | 140,193.315 us |
| 1M | 468,752 | 626.286 us | 1,028.540 us | 1,140.941 us | 1,184.472 us | 224,656.265 us |

ClickHouse server INSERT duration was read from `system.query_log`, restricted
to the unique writer `query_id` prefix for each run.

| Target | INSERTs | Mean rows/INSERT | Mean | p50 | p95 | p99 | Maximum |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 800k | 1,712 | 16,355.1 | 69.282 ms | 67 ms | 83 ms | 89 ms | 279 ms |
| 1M | 2,144 | 16,324.6 | 66.578 ms | 65 ms | 80 ms | 86 ms | 277 ms |

The server duration is not per-row callback-to-durability latency. It excludes
time spent filling a batch and waiting in the client queue. The sink currently
does not expose queue-to-ACK percentiles. Because row count, rather than the
500 ms timer, closed most batches, mean batch-fill intervals were approximately
246 ms at 800k and 196 ms at 1M. A precise callback-to-ACK distribution requires
batch-level ACK timestamp instrumentation in the writer, not per-callback clock
reads in the decoder.

## Persistent Data Validation

A full scan after both runs verified occurrence identity, batch identity, and
ingress boundaries:

| Run | Rows | Unique occurrence IDs | Unique batch IDs | Ingress range |
| --- | ---: | ---: | ---: | ---: |
| 800k | 28,000,000 | 28,000,000 | 1,712 | 1..28,000,000 |
| 1M | 35,000,000 | 35,000,000 | 2,144 | 1..35,000,000 |

## Verification

The combined Release build completed the full configured regression suite:

```text
100% tests passed, 0 tests failed out of 12
```

This includes core ingest, Arrow ring, Python Arrow reader, ClickHouse raw path,
and daemon configuration tests.

## Limits Of The Result

- The source is a synthetic in-process MDL callback with a small Tick body, not
  a live vendor network session.
- The run covers `raw_tick`; it does not characterize large snapshots.
- ClickHouse ran on the same host over loopback and used local `MergeTree`
  tables with `insert_quorum=0`. This is a local durable ACK result, not a
  replicated quorum result.
- The server and client were isolated by socket/NUMA CPU masks, but shared the
  host filesystem and memory subsystem.
- The successful runtime settings trade durability latency for sustainable
  INSERT size. Meeting a 1-5 ms ClickHouse flush target at 800k to 1M msg/s
  requires cross-lane coalescing, a lower-overhead insert transport, or a server
  capable of sustaining thousands of small synchronous INSERTs per second.
- The 30-second windows demonstrate steady operation at both requested rates;
  a production qualification should repeat them for at least 300 seconds and
  include replicated ClickHouse, network faults, restart/retry, and snapshot
  traffic.
