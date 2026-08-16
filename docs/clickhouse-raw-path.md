# ClickHouse raw persistence path

## Scope

This milestone persists every successfully decoded and normalized Tick or
Snapshot occurrence in ClickHouse:

```text
MDL callback
  -> existing receive_monotonic_ns
  -> bounded admission and body copy
  -> decoder lane
       -> CanonicalTick / CanonicalSnapshot
       -> preallocated RawCanonicalBatch (AoS)
       -> SPSC ready queue
  -> ClickHouse writer thread
       -> Arrow columnization
       -> provenance IDs
       -> ArrowStream serialization
       -> synchronous INSERT / exact retry
  -> raw_tick / raw_snapshot
```

The tap is after complete decode/normalization and before SequenceRecovery,
duplicate/conflict/late classification, or catalog-miss suppression. It
therefore records normal arrivals, retransmissions, conflicting canonical
payloads, late gap members, and catalog misses. A packet that cannot be
decoded into a canonical record is counted as a decode error and is not in
these raw tables.

Event/KLine revisions and historical reconciliation are separate milestones.
The shared-memory Arrow path is optional and does not participate in the raw
INSERT acknowledgement.

## Hot-path boundary

The callback does not read a UTC wall clock for each message. The decoder does
not use an Arrow builder, hash a row, serialize IPC, call libcurl, allocate
memory, or wait for ClickHouse.

Each decoder lane owns one active fixed-capacity canonical batch. Its steady
state work is a fixed-memory record copy plus updates to batch-local metadata:

- row and byte count;
- trade date;
- first creation monotonic time;
- minimum and maximum ingress sequence.

When the row, byte, or delay threshold is reached, the lane publishes one
batch index to its SPSC queue and switches to another preallocated slot. Raw
statistics are updated once per published batch, not with a contended global
atomic operation for every row. Snapshot idle-loop delay checks are throttled;
message-path checks reuse the callback's existing monotonic timestamp.

The writer alone constructs Arrow columns and nested Snapshot levels. The
serialized payload stays alive until ClickHouse acknowledges the INSERT or the
retry budget is exhausted.

## Durability and failure semantics

The only durable boundary is a successful synchronous ClickHouse response. In
a replicated deployment, configure `insert_quorum` so that this response is a
quorum acknowledgement.

| State | Durable |
| --- | --- |
| Active canonical batch | No |
| Published in-memory SPSC batch | No |
| HTTP INSERT in flight | Unknown |
| Successful ClickHouse response | Yes |
| Timeout or lost response | Unknown; retry the exact batch |

There is no Kafka/Redpanda dependency and no local WAL in this path. A process
or host failure before ClickHouse acknowledges a batch can lose that active or
queued data. If host-power-loss recovery before ACK is required, add a local
WAL as a separate durability layer rather than treating the in-memory queue as
one.

Queue exhaustion is fatal. `RawRecordTap::Append*` returns false, the ingest
engine closes admission, and `mdl_ingestd` exits nonzero after attempting to
drain already-published batches. Rows are never silently discarded to keep the
process alive. A permanent ClickHouse error or an exhausted retry interval has
the same fail-closed behavior.

## Batch and retry identity

The writer creates one random `writer_instance_id` per process. A deployment
may pass a stable `source_instance_id`; otherwise the writer ID is also used as
the source ID. A supervisor must allocate a nonzero `feed_session_epoch` for
every physical feed run and must not reuse the same source/epoch pair.

`batch_sequence` is process-local and strictly increasing. The 16-byte batch
ID is the first 128 bits of BLAKE3 over fixed-width little-endian input:

```text
writer_instance_id
|| table_id
|| trade_date
|| batch_sequence
|| schema_version
```

Each raw occurrence ID is the first 128 bits of BLAKE3 over:

```text
source_instance_id
|| feed_session_epoch
|| ingress_sequence
|| canonical_kind
```

A retry reuses the same in-memory canonical rows, ArrowStream bytes, row order,
batch ID, sequence, query ID, and ClickHouse deduplication token:

```text
query_id = l2flow/<table>/<writer_instance_id>/<batch_sequence>

insert_deduplication_token =
    l2flow/<table>/<trade_date>/<batch_id>/<schema_version>
```

It never rebuilds or regroups a timed-out batch. This handles the case where
ClickHouse committed an INSERT but its response did not reach the writer.
The retry elapsed-time budget begins when the first unknown outcome returns, so
an initial request timeout cannot consume the budget before the first exact
retry is attempted.
Local `MergeTree` tables set `non_replicated_deduplication_window=10000` and
replicated tables set `replicated_deduplication_window=10000`. Startup rejects
either table type when its table-local window is smaller. Deployments with more
than 10000 blocks inside the maximum retry interval must increase the setting.

Every request uses ArrowStream with synchronous settings equivalent to:

```text
async_insert=0
wait_end_of_query=1
insert_deduplicate=1
insert_quorum=<configured value>
insert_quorum_parallel=1
```

## Schema and ordering

Both raw tables use `PARTITION BY trade_date`. Local development uses
`MergeTree`; production uses `ReplicatedMergeTree` with the same columns,
partition expression, and business sorting key.

- Local DDL: [`../clickhouse/schema/raw_tables.sql`](../clickhouse/schema/raw_tables.sql)
- Replicated DDL: [`../clickhouse/schema/raw_tables_replicated.sql`](../clickhouse/schema/raw_tables_replicated.sql)

The replicated file uses ClickHouse macros for the Keeper root, shard, and
replica. Set those macros in deployment configuration and apply schema changes
through the cluster's normal orchestration. The daemon's auto-create mode is
intended only for local `MergeTree` tables.

Transport provenance columns are:

```text
source_instance_id FixedString(16)
writer_instance_id FixedString(16)
batch_id FixedString(16)
batch_sequence UInt64
row_index UInt32
occurrence_id FixedString(16)
schema_version UInt32
```

`catalog_match` is also persisted. A source retransmission has a different
ingress sequence and occurrence ID and remains a separate fact. A transport
retry has the same batch token and does not create another ClickHouse block.

MergeTree sorting keys organize parts; they do not guarantee implicit SELECT
order. Channel replay must use an explicit order:

```sql
SELECT *
FROM l2flow.raw_tick
WHERE trade_date = {trade_date:Date}
  AND market = {market:UInt8}
  AND channel = {channel:UInt32}
ORDER BY
    native_sequence,
    source_instance_id,
    feed_session_epoch,
    ingress_sequence;
```

For a source-run occurrence timeline, including Snapshots that have no native
sequence, use:

```sql
ORDER BY source_instance_id, feed_session_epoch, ingress_sequence
```

Do not use implicit part order, wall-clock time, or batch sequence as replay
correctness order.

## Time model

Rows preserve these source and receive clocks:

- exchange time and raw exchange time;
- vendor `LocalTime` and its normalized nanoseconds from midnight;
- callback `receive_monotonic_ns`.

The sink captures `run_started_utc_ns` and `run_started_monotonic_ns` once at
startup and prints both. They provide an operational estimate:

```text
estimated_receive_utc_ns =
    run_started_utc_ns
    + receive_monotonic_ns
    - run_started_monotonic_ns
```

The estimate is not used for correctness ordering and does not model later NTP
steps. If accurate UTC correlation across long runs is required, add periodic
non-hot-path `(utc, monotonic)` calibration control rows. Do not restore a
wall-clock read to every callback.

## Capacity

The queue is sized in batches per decoder lane. A starting estimate is:

```text
required_batches =
    ceil(peak_rows_per_second
         * tolerated_clickhouse_stall_seconds
         / rows_per_batch)
    * safety_factor
```

Preallocated canonical payload memory is approximately:

```text
decoder_lanes
* (rounded_queue_capacity + 1 active slot)
* rows_per_batch
* sizeof(CanonicalRecord)
```

`clickhouse_preallocated_canonical_bytes` prints the actual configured Tick
plus Snapshot payload allocation. It excludes Arrow builder memory, serialized
in-flight payloads, libcurl buffers, and container metadata. Queue capacity
should cover seconds of measured ClickHouse jitter, not minutes of outage.

## Daemon configuration

`--clickhouse-url` enables this sink. Required and commonly used options are:

```text
--clickhouse-url <http-or-https-base-url>
--feed-epoch <nonzero process epoch>
--clickhouse-source-instance-id <optional stable 32-hex ID>
--clickhouse-database <database>
--clickhouse-user <user>
--clickhouse-password-env <environment-variable-name>
--clickhouse-writers <count>
--clickhouse-queue-batches-per-lane <count>
--clickhouse-insert-quorum <count>
--clickhouse-no-auto-create
```

The password value is never accepted as a command-line value or printed.
Direct connections are the default (`no_proxy=*`) so machine-wide proxy
variables cannot intercept persistence traffic; `--clickhouse-no-proxy` can
provide a narrower libcurl exclusion list. `--clickhouse-no-tls-verify` is an
explicit development escape hatch and should not be used in production.

## Shutdown

Normal shutdown follows this order:

1. Stop SDK callbacks.
2. Stop admission and wait for an active callback.
3. Drain decoder lanes and flush every partial raw batch.
4. Join decoder threads.
5. Drain ClickHouse ready queues and wait for every INSERT ACK.
6. Stop writer threads and report final ACK/release counts.
7. Seal the independent Arrow hot path, if enabled.

If the ClickHouse shutdown deadline expires, the daemon prints the sink error,
the remaining batch count, and exits nonzero. A successful exit requires no
active or unreleased raw batch.

## Verification

The deterministic ingest test proves that the raw tap sees retransmissions and
catalog misses before recovery/suppression and that a tap failure closes
admission. The ClickHouse test covers BLAKE3 vectors, preallocation, Arrow
mapping, nested Snapshot levels, occurrence uniqueness, row indices, Date
partitioning, and explicit replay order. Its lost-ACK fixture accepts the full
first ArrowStream request and drops the response, then verifies that the retry
uses a byte-identical request target and payload before returning success.
Another fixture holds the first INSERT open until a two-slot canonical pool is
exhausted; it verifies fail-closed admission and that shutdown still drains and
acknowledges both already-copied rows before returning the fatal status.

```bash
ctest --test-dir build --output-on-failure

./clickhouse-test/start.sh
L2FLOW_CH_TEST_URL=http://127.0.0.1:8123 \
  ./build/test_clickhouse_raw
```
