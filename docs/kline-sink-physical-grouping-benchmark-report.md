# KLine sink physical-grouping benchmark report

Test date: 2026-08-11 (Asia/Shanghai)

> Historical pre-outbox evidence. This report describes the former KLine sink
> protocol (including its 120-byte marker) before request-spool admission,
> exact WAL input positions, and causal freshness gating were introduced. The
> benchmark has since been replaced; these numbers are retained only as the
> record of that older implementation. See
> [`kline-worker-clickhouse.md`](kline-worker-clickhouse.md).

## 1. Scope and result

The KLine sink physical-grouping path passed paced 800k and 1M revision-row/s
tests for both P1 and P2 on this host. All four runs drained every logical
batch, row, and lane queue, preserved marker counts, and reported no retry or
unknown outcome.

This is a component transport result, not a ClickHouse storage result. The
benchmark executes:

```text
synthetic immutable KLineRevisionBatch producer
    -> sink validation and bounded lane queue
    -> 313-byte KLine RowBinary serialization
    -> lane-local physical grouping
    -> libcurl HTTP
    -> multithreaded keep-alive loopback HTTP server
    -> 120-byte grouped marker request
    -> logical ACK and queue release
```

The server validates body widths and returns HTTP success, but it does not
parse rows as ClickHouse, create MergeTree parts, run the materialized view,
replace versions, merge parts, replicate data, wait for quorum, or write to
disk. Consequently, the result supports only a five-second loopback transport
capacity claim for generated KLine revision rows.

It also does not establish 800k-1M inbound market messages/s end to end. One
input trade can generate zero, one, or several revisions depending on interval
configuration and bar changes. Production capacity must compare the measured
input workload's generated revision rate with sink and ClickHouse ACK capacity.

## 2. Candidates

| Candidate | Revision request bound | Physical group bound | Linger |
| --- | ---: | ---: | ---: |
| P1 | 2,048 rows / 1 MiB | 256 logical batches | 1 ms |
| P2 | 4,096 rows / 2 MiB | 256 logical batches | 1 ms in this test |

At the checked 313-byte revision width, a full P1 payload is 641,024 bytes and
a full P2 payload is 1,282,048 bytes. The byte bounds therefore do not bind
before the row bounds in these two candidates. Each marker row is exactly 120
bytes.

The synthetic producer used 32 owners, four writer lanes, and 32 logical
batches per paced burst so a request could reach its requested row capacity:

- P1 used 64 rows per logical batch: `32 * 64 = 2,048` rows per full burst;
- P2 used 128 rows per logical batch: `32 * 128 = 4,096` rows per full burst.

This request-aligned shape is intentional for a capacity boundary. It is not a
claim that live KLine logical batches will have the same density. Live density
must be computed from cumulative row/request counter deltas.

## 3. Host and method

| Item | Value |
| --- | --- |
| Build | Release |
| Kernel | Linux 6.17.0-35-generic, x86-64 |
| CPU | 2 x AMD EPYC 9534 |
| Topology | 128 physical cores / 256 logical CPUs / 2 NUMA nodes |
| Writer lanes / owners | 4 / 32 |
| Measured duration | 5 seconds per run |
| CPU placement | no benchmark thread pinning or isolation |
| Loopback response delay | 0 us configured |

The producer is paced against monotonic time. Durable rate is acknowledged
logical revision rows divided by elapsed time from the scheduled start through
final sink drain. Queue slope is an ordinary least-squares regression over the
last 80% of queue samples. The gate requires:

```text
durable revision rate >= 99% of offered rate
average rows/revision INSERT >= 90% of effective request row capacity
row and logical-batch queue slopes <= 0.1% of offered rate
submitted == queued == request-ACKed == logical-ACKed rows
queued == logical-ACKed == released batches
marker rows == logical batches
marker requests == committed physical groups
all final global and per-lane queues == 0
retry attempts == 0 and unknown outcomes == 0
```

## 4. Measurements

| Candidate | Offered revisions/s | Durable revisions/s | Average rows/INSERT | Maximum revision body | Server revision p99 | Row queue slope |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| P1 | 800,000 | 799,929.194 | 2,047.083 | 641,024 B | 274.188 us | -2.421 rows/s |
| P1 | 1,000,000 | 999,813.040 | 2,047.502 | 641,024 B | 248.645 us | -1.848 rows/s |
| P2 | 800,000 | 799,972.478 | 4,094.166 | 1,282,048 B | 697.659 us | +6.449 rows/s |
| P2 | 1,000,000 | 999,919.858 | 4,095.004 | 1,282,048 B | 541.534 us | -13.901 rows/s |

At 1M offered revisions/s, P1 submitted and durably acknowledged 5,000,000
rows in 78,125 logical batches. It used 2,442 revision requests and 2,442
marker requests. Every lane reached 32 queued batches and 2,048 queued rows as
its lifetime HWM, then drained to zero.

At the same offered rate, P2 submitted and durably acknowledged 5,000,000 rows
in 39,063 logical batches. It used 1,221 revision requests and 1,221 marker
requests. Every lane reached 32 queued batches and 4,096 queued rows as its
lifetime HWM, then drained to zero.

P2 approximately halves the HTTP request count relative to P1 for this aligned
workload, while its loopback server revision p99 is approximately twice P1's.
That is a transport tradeoff, not evidence that P2 will improve ClickHouse
storage throughput or tail latency.

P2's 800k five-second row regression was slightly positive at 6.449 rows/s,
or about 0.00081% of offered rate, while the final queue was zero. It passed the
explicit 0.1% short-run gate, but this small-window result is not evidence of a
zero long-run slope; production qualification still requires at least a
30-minute window.

## 5. Commands

P1 was run at both `--rate 800000` and `--rate 1000000`:

```bash
./build-release/benchmark_kline_sink --rate 1000000 --seconds 5 \
  --owners 32 --writer-lanes 4 --logical-batch-rows 64 \
  --producer-burst-batches 32 --insert-request-rows 2048 \
  --insert-request-bytes 1048576 --physical-group-batches 256 \
  --physical-group-delay-ns 1000000
```

P2 changed the logical and physical row sizes:

```bash
./build-release/benchmark_kline_sink --rate 1000000 --seconds 5 \
  --owners 32 --writer-lanes 4 --logical-batch-rows 128 \
  --producer-burst-batches 32 --insert-request-rows 4096 \
  --insert-request-bytes 2097152 --physical-group-batches 256 \
  --physical-group-delay-ns 1000000
```

## 6. Production decision boundary

The production profile remains at 1,024 rows / 1 MiB / 256 batches / 1 ms and
four lanes. Neither P1 nor P2 should become the default from this benchmark
alone.

A production candidate must next be tested against the target ClickHouse DDL,
materialized view, storage, replication, and quorum settings. At minimum, run
30-minute opening-burst and longer soak windows while differencing the exported
KLine counters and grouping `system.query_log` by lane-bearing `query_id`.
Reject a candidate if queue slope remains positive, request p95/p99 violates
the latency objective, or `system.parts` active-part count and merge backlog
grow together.

P1 is the lower transport-latency candidate. P2 is the lower request-count
candidate. The real ClickHouse test, not this loopback test, must decide whether
the request-count reduction outweighs its larger body and higher observed
transport latency.
