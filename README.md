# L2Flow-CH `mdl_ingestd`

`mdl_ingestd` decodes MDL market data into one run-scoped local canonical WAL.
Raw ClickHouse, Event, and KLine are independent consumers of that durable
record order:

```text
MDL callback -> bounded decoder lanes -> SequenceRecovery
                                      -> canonical/disposition WAL
                                           |-> raw cursor -> raw ClickHouse
                                           |-> Event cursor -> Event revisions
                                           `-> KLine cursor -> KLine revisions
```

The WAL committer assigns a global LSN and makes a batch visible only after
`fdatasync`. It stores the source identity, feed epoch, batch identity, row
count, checksums, original decoded fact, final recovery disposition/control,
and exact `(lsn, batch_sequence, row_index)` positions. Consumer completion may
arrive out of order, but a cursor advances only across the largest gap-free WAL
prefix. Durable payloads are decoded from their segment on cache miss; only a
configured number of decoded batches remains cached, so consumer lag does not
retain every canonical record in RAM. `ingress_sequence` is never used as a
global checkpoint.

Raw ClickHouse ACK no longer invokes or waits for Event/KLine. A slow or
unavailable derived sink pins only its own cursor. The shared WAL is bounded;
if its storage or admission capacity is exhausted, the process enters
`FATAL_CONTINUITY`.

## Continuity and current-query contract

The process first fences every decoder lane, then writes a barrier into the WAL
and publishes exact raw/Event/KLine cursors to `derived_freshness_log`. The
fence waits for all inputs admitted before its cut to reach a final recovery or
decode outcome; later inputs may continue concurrently. The WAL committer
records the latest barrier only after its batch sync succeeds, so authority
cannot skip older decoder work and closes even when every downstream consumer
is blocked before reading that barrier.
ClickHouse generates both lease timestamps from its own `now64(9)` value in
the freshness `INSERT SELECT`; application wall-clock skew cannot lengthen or
prematurely expire current-query authority.
The states are:

- `NORMAL`: enabled consumers have crossed the latest barrier.
- `DERIVED_CATCHUP`: Event or KLine is behind, or has only recently become
  unavailable.
- `RAW_ONLY_STALE`: a derived cursor has stopped progressing, or its sink has
  remained unavailable past `--derived-stale-after-ns`.
- `RAW_CATCHUP`: the raw ClickHouse cursor is behind while the canonical WAL
  remains healthy.
- `FATAL_CONTINUITY`: the canonical WAL can no longer provide its durability
  boundary.

`event` and `kline` are fail-closed views, not `ReplacingMergeTree` current
tables. A view returns rows only when exactly one calculation run has an
authoritative, unexpired latest freshness lease; overlapping active runs close
the view instead of mixing revisions. Each recovery marker stores
the maximum causal WAL position of its batch; the view excludes markers beyond
the published domain cursor before selecting the latest revision and filtering
tombstones. Thus out-of-order owners cannot expose a partially completed prefix
as authoritative current.

An empty current view can mean either no business rows or a closed freshness
gate. Query services must inspect `derived_freshness_log` and surface stale or
unavailable status rather than silently interpreting both cases as the same
answer.

The schema files are:

- [raw_tables.sql](clickhouse/schema/raw_tables.sql)
- [event_tables.sql](clickhouse/schema/event_tables.sql)
- [kline_tables.sql](clickhouse/schema/kline_tables.sql)
- Keeper-backed variants under the same directory with `_replicated` suffixes.

This is a replacement schema. An installation containing the old
`event_current_mv`/`kline_current_mv` or current `ReplacingMergeTree` tables
must receive an explicit offline migration; startup probes reject that layout
instead of running a compatibility path.

## Run-scoped durability boundary

The canonical WAL creates a new `O_EXCL` run directory and does not reopen an
old run. This matches the deployment assumption that another source can replay
the feed after a process crash. The WAL is nevertheless the durable handoff
inside a live run: no consumer can observe a record before its batch is synced,
and lagging consumers retain WAL segments.

Event and KLine also create a run-scoped request spool beneath the WAL run
directory. Before the first HTTP send, a physical group is serialized and
synced with its exact input positions, revision/marker RowBinary bytes, row
counts, payload checksums, query IDs, deduplication tokens, and initial state.
Live request states are updated in place. They are not individually synced,
because this implementation deliberately does not resume request state after a
process crash; the canonical WAL and external source recovery are the recovery
boundaries.

This adds one batched WAL sync before all consumers and one request-spool sync
before each derived physical group. It can increase Event/KLine persistence
latency, but it is not on the raw ClickHouse ACK path. Tune WAL commit size and
delay, derived physical-group size, and storage placement together; do not
remove the pre-send spool sync while relying on it as the live-run handoff.

## Build and test

The default SDK include path points to the sibling MDL SDK checkout. Override
it with `-DL2FLOW_CH_SDK_INCLUDE_DIR=/path/to/include` when needed.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Apache Arrow 25 and libcurl are required by the default build. A core-only
build is useful when those development packages are unavailable:

```bash
cmake -S . -B build-core \
  -DL2FLOW_CH_ENABLE_ARROW_RING=OFF \
  -DL2FLOW_CH_ENABLE_CLICKHOUSE_RAW=OFF \
  -DL2FLOW_CH_BUILD_BENCHMARKS=OFF
cmake --build build-core -j
ctest --test-dir build-core --output-on-failure
```

The new architecture is covered by `canonical_outbox` and
`consumer_contracts`. The KLine loopback smoke additionally verifies exact
marker/cursor completion through the durable request spool.

## Configuration

Every run requires one nonzero shared feed epoch and a WAL root:

```text
--mode partial
--trade-date 20260814
--catalog config/catalog.example.csv
--feed-epoch <new nonzero epoch>
--outbox-dir /local-nvme/l2flow/canonical-outbox
```

Important WAL bounds are:

```text
--outbox-producer-queue-records 65536
--outbox-commit-batch-records 256
--outbox-commit-batch-bytes 4194304
--outbox-commit-max-delay-ns 500000
--outbox-segment-max-bytes 268435456
--outbox-maximum-reservoir-bytes 34359738368
--outbox-read-cache-batches 8
--derived-stale-after-ns 30000000000
```

Enable raw ClickHouse with `--clickhouse-url`. Event/KLine require that raw
output plus their shared process-lifetime FactJournal, independent revision
epochs, and unique calculation run IDs:

```text
--clickhouse-url http://127.0.0.1:8123
--clickhouse-source-instance-id <stable 32-hex source ID>
--fact-journal-path /local-nvme/l2flow/facts-current.fjn

--event-enable
--event-revision-epoch <new nonzero UInt32>
--event-calculation-run-id <new 32-hex ID>

--kline-enable
--kline-interval-seconds 1
--kline-revision-epoch <new nonzero UInt32>
--kline-calculation-run-id <new 32-hex ID>
```

Use a new feed epoch and new calculation run IDs for external recovery. A
calculation run ID must not be reused across sources or feed epochs because it
is the query-gate identity.

The checked-in host profile is
[current-server.production.conf](config/current-server.production.conf); its
environment template is
[current-server.production.env.example](config/current-server.production.env.example).
Validate it before loading the SDK:

```bash
./build/mdl_ingestd \
  --config config/current-server.production.conf \
  --validate-only
```

Response files accept one complete long option per nonempty line. `${NAME}`
expands one nonempty environment variable; there is no shell, quote, inline
comment, recursive expansion, or nested response-file evaluation.

## Sequence recovery and outputs

`SequenceRecovery` remains first-wins per native channel position. A decoded
tick is durably recorded with both the original fact and exactly one final
disposition: ordered projection, accepted hole fill, or rejected late fact.
`GapOpen` and `ChannelSeal` are WAL control records fanned out in owner order.
Catalog misses remain in raw storage but do not enter derived projection.

The optional Arrow branch follows the canonical WAL as a volatile observer. It
does not own a durable cursor and therefore does not delay WAL reclamation; if
it falls behind the oldest resident LSN, it resumes at that LSN. It must not be
used as a durability or continuity authority.

Detailed current contracts are in:

- [durable ingest design](docs/mdl-ingestd-design.md)
- [raw ClickHouse consumer](docs/clickhouse-raw-path.md)
- [Event projection and ClickHouse](docs/event-worker-clickhouse.md)
- [KLine projection and ClickHouse](docs/kline-worker-clickhouse.md)
- [FactJournal](docs/fact-journal.md)
- [Arrow hot path](docs/arrow-hot-path.md)
