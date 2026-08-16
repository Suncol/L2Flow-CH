# Raw ClickHouse WAL consumer

## Boundary

Raw ClickHouse is an independent consumer of the canonical WAL. It is no
longer the component that synchronously dispatches Event/KLine callbacks.

```text
durable canonical WAL -> RawOutboxConsumer -> RawClickHouseConsumer
                                      ACK -> raw cursor completion
```

Only an ACK for the exact ClickHouse batch advances the raw cursor positions
carried by that batch. Event and KLine have separate cursors and cannot make a
raw batch succeed or fail.

## Input semantics

The WAL Tick record contains the original decoded tick and a separate recovery
disposition. Raw storage always uses the original tick. Therefore duplicates,
late rejects, accepted hole fills, and catalog misses remain observable in
`raw_tick`; SequenceRecovery never rewrites their raw fields. Snapshots use the
original decoded snapshot.

`GapOpen`, `ChannelSeal`, diagnostics, faults, and freshness barriers do not
create raw table rows. The follower completes ordinary control/diagnostic
positions directly. A barrier flushes every partial Tick/Snapshot lane first,
so crossing it proves all earlier raw rows have ACKed.

## Batching and exact positions

Each decoder lane has preallocated active and queued batches. A row carries its
full `(lsn, batch_sequence, row_index)` position beside the canonical payload.
The consumer performs a capacity preflight before append or flush, avoiding an
ambiguous result after a row has entered an active batch.

Writer threads may ACK batches out of order. The WAL validates every supplied
position and advances the raw cursor only over the contiguous completed prefix.

Raw ClickHouse provenance remains distinct:

- source instance ID and feed epoch identify the logical source run;
- writer instance ID and raw batch sequence identify one physical writer run;
- batch ID/query ID/deduplication token identify a retryable INSERT; and
- occurrence ID identifies the raw fact independently of batching.

## Retry and continuity

Transport errors and explicitly retryable ClickHouse responses retry with the
same query ID and deduplication token. Retry continues during the live run;
there is no finite elapsed-time policy that converts a temporary ClickHouse
outage directly into process death.

A nonretryable schema/authentication/request error stops this consumer and
pins the raw cursor. Continuity becomes `RAW_CATCHUP` while the canonical WAL
is still writable. Only when that durable reservoir itself can no longer admit
or sync new records does the process enter `FATAL_CONTINUITY`.

## Schema and deployment

Single-node and replicated definitions are in:

- [raw_tables.sql](../clickhouse/schema/raw_tables.sql)
- [raw_tables_replicated.sql](../clickhouse/schema/raw_tables_replicated.sql)

With `--clickhouse-no-auto-create`, startup still probes table engine,
partitioning, and column contracts. A mismatch fails this consumer without
silently changing external production schema.

The primary tuning controls are batch rows/bytes/delay, queue batches per
decoder lane, writer count, HTTP timeouts, and insert quorum. Larger batches
improve transport density but extend the interval between WAL visibility and
raw cursor ACK.

## Recovery scope

The local canonical WAL is run-scoped and is not reopened after a process
crash. Raw INSERT retry and cursor tracking protect a live process from
transient ClickHouse failures. Process-crash recovery must replay from the
external source with a new feed epoch; it must not infer progress from a
maximum ingress sequence.
