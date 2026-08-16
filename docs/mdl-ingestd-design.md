# `mdl_ingestd` durable-outbox design

## Scope

The process has one continuity boundary: a run-scoped local canonical WAL.
Raw ClickHouse, Event, and KLine are independent consumers of its total order.
No raw ClickHouse ACK calls a derived listener, and no derived failure directly
terminates raw ingestion.

Crash-time reopening of a prior WAL/request-spool run is deliberately out of
scope. Deployment recovery replays from an external source under a new feed
epoch and new derived calculation run IDs.

## Data flow

```text
SDK callback
  -> bounded body copy into decoder lane
  -> decode + catalog lookup + SequenceRecovery
  -> DurableOutbox::Enqueue(CanonicalRecord)
  -> single WAL committer assigns LSN and fdatasyncs a batch
       |-> RawOutboxConsumer -> raw_tick/raw_snapshot -> raw cursor
       |-> DerivedOutboxConsumer(Event) -> EventRuntime -> Event sink -> Event cursor
       `-> DerivedOutboxConsumer(KLine) -> KLineRuntime -> KLine sink -> KLine cursor
```

The SDK callback remains bounded and does not wait for WAL I/O. The durable
handoff occurs between decoder/recovery and all output consumers: a consumer
cannot read a record until the containing batch has completed `fdatasync`.
External source recovery covers a process crash between callback admission and
that handoff.

## Canonical WAL record

Every record has a global WAL position:

```text
(lsn, batch_sequence, row_index)
```

`lsn` is assigned only by the single committer and is the total-order key.
`batch_sequence` and `row_index` bind the cursor to the exact durable frame.
`ingress_sequence` remains source metadata; it is neither globally ordered
across decoder lanes nor a checkpoint key.

Tick occurrences retain both:

- the original, unmodified decoded tick used by raw storage; and
- the final `TickDispatch` produced by SequenceRecovery, including admission
  frontier, recovery generation, owner fence, and ordered/hole-fill/reject
  classification.

Other WAL kinds are Snapshot, `GapOpen`/`ChannelSeal` control, gap diagnostic,
channel fault, freshness barrier, and final barrier. Catalog misses are
retained in raw records and marked non-projectable.

Each WAL batch persists source instance ID, feed epoch, run ID, batch sequence,
batch ID, first LSN, row count, payload byte count, per-row payload length and
CRC32C, batch CRC32C, and frame CRC32C. The in-memory index is published only
after the frame sync succeeds. Consumers read and validate the exact frame
from its still-open segment on a cache miss. Decoded records live only in the
hard-bounded `outbox-read-cache-batches` cache; a lagging consumer therefore
retains disk segments and compact batch indexes, not every canonical payload
object.

## Admission, batching, and fatal boundary

Decoder lanes are WAL producers. Admission is bounded by
`outbox-producer-queue-records`; the committer groups by record count, byte
count, and maximum delay. Queue exhaustion, serialization failure, write/sync
failure, or reservoir exhaustion makes the outbox unhealthy and stops further
admission as `FATAL_CONTINUITY`.

WAL segments are reclaimed only when their last LSN is no greater than the
minimum contiguous cursor of every enabled durable consumer. A disabled
consumer is advanced with the durable tail and does not pin storage. A missing
or failed enabled consumer stays behind and retains the reservoir.

## Exact independent cursors

Each consumer has its own cursor file. Completion may arrive out of order, so
the outbox stores completed LSNs above the current cursor and advances only
while `cursor + 1` is present. Before accepting completion, it verifies the
entire supplied `WalPosition` against the resident durable record.

Consequences:

- a fast owner cannot skip a slow owner;
- a maximum `ingress_sequence` cannot falsely acknowledge a hole;
- raw ACK has no effect on Event or KLine cursor state; and
- Event and KLine may advance at different rates.

Cursor checkpoints are serialized so concurrent forced and periodic writes
cannot regress the on-disk value. They are audit artifacts for the live run;
startup never reopens them.

## Raw consumer

The raw follower reads the WAL in LSN order. Tick/Snapshot records are copied
into bounded ClickHouse batches with their exact WAL positions. Diagnostic and
control records require no raw row and complete immediately. A barrier forces
all partial raw batches to publish; the barrier completes only after prior raw
rows have ACKed.

ClickHouse transport failures judged retryable are retried indefinitely during
the live run with stable query IDs and deduplication tokens. A nonretryable
failure stops the raw consumer and pins its cursor. That is `RAW_CATCHUP`, not
immediate fatal continuity. If the pinned raw cursor ultimately exhausts the
canonical reservoir, the resulting WAL write failure is `FATAL_CONTINUITY`.

## Event and KLine consumers

Each derived follower has one router and a bounded FIFO per instrument owner.
The router reads global LSN order, preserving each owner's recovery fence
order. A full owner FIFO backpressures that derived follower only.

For projectable ticks, the runtime carries the exact WAL position into the
worker. If a calculation produces no revision, the worker completes that
position directly. If it produces revisions, the immutable revision batch
contains the sorted unique set of causal WAL positions; only the ClickHouse
sink may complete them.

Controls and non-projectable records complete once their required owner-local
effect is applied. A barrier is inserted into every owner FIFO. Its cursor
position completes only after every owner has flushed its calculation cut and
pending derived commits, so it is a real domain fence rather than a router
observation timestamp.

## Derived request spool and ClickHouse commit

One physical group contains one or more immutable logical revision batches and
their recovery markers. Before HTTP transmission the request spool writes and
syncs:

- consumer domain and exact sorted causal WAL positions;
- request kind and row count;
- exact revision and marker RowBinary payloads;
- payload CRC32C;
- query ID and insert deduplication token; and
- a state byte/complement pair per request.

The live state machine is `PREPARED -> SENT -> ACKED`, with `UNKNOWN` for an
ambiguous retryable result and `BLOCKED` for a terminal result. States are
updated in the file for diagnosis but are not individually synced because a
crashed run is never resumed.

Revision requests ACK first; the committed recovery-marker request ACKs last.
Only after the marker ACK does the sink complete all causal WAL positions.
The spool file is retired only after cursor completion succeeds. Stable query
IDs and deduplication tokens make retry of an unknown HTTP result idempotent
within ClickHouse's configured deduplication contract.

## Freshness frontier and fail-closed views

The main loop first takes a stable cut of every decoder lane. A lane completes
the cut only after every earlier admitted message has reached its final
recovery or terminal decode outcome; SequenceRecovery keeps its configured gap
wait and is not force-flushed early. Later admissions continue concurrently.
Only then does the main loop append a freshness barrier. The single WAL
committer makes that barrier the active frontier only after the containing
batch has completed `fdatasync`. If the lane fence cannot complete, no lease is
renewed and the query gate expires closed. The continuity controller publishes:

- barrier and canonical tail positions;
- exact raw, Event, and KLine cursor positions;
- source ID, feed epoch, calculation run and publisher identities;
- continuity state and per-domain authoritative flags.

The heartbeat RowBinary body contains no application-generated lease
timestamp. Its `INSERT SELECT` derives both `observed_utc_ns` and
`valid_until_utc_ns` from one ClickHouse `now64(9)` value. Lease creation and
lease validation therefore share the ClickHouse clock domain. This is
explicitly a ClickHouse query-time lease: neither the application wall clock
nor pre-server transport time is an operand in its expiration calculation.
Failure to publish does not make old data authoritative forever: the last
server-generated lease expires and both current views close.
Replicated deployments must still keep ClickHouse ingest and query replicas
clock-synchronized within the accepted lease error budget; this contract
removes application-to-ClickHouse skew, not skew between ClickHouse nodes.

Every Event/KLine recovery marker also stores the maximum causal WAL position
for its logical batch. The current view performs these operations in order:

1. choose the latest freshness row for every calculation run in the domain;
2. require exactly one run to have `authoritative = true` and an unexpired
   lease, otherwise fail closed;
3. join only committed recovery runs whose causal maximum is no greater than
   the published domain cursor;
4. rank eligible revisions by logical key and descending version/provenance;
5. retain rank 1 and then filter tombstones.

Step 3 is required for multi-owner correctness. A writer can commit LSN 102
before another writer commits LSN 101; the contiguous cursor remains 100, so
the LSN-102 marker is excluded even though it is already visible in
ClickHouse.

Within a calculation run, `publication_sequence` is the primary latest-row
key. Thus a timed-out older publication that finishes late cannot regress a
newer cursor or undo a later revocation; ClickHouse timestamps remain the
lease clock and the deterministic secondary ordering key.

## State transitions

At a barrier:

- `NORMAL`: all enabled consumers are healthy and at or beyond the barrier.
- `DERIVED_CATCHUP`: either derived cursor is behind, or a derived component
  has not yet exceeded the stale timeout.
- `RAW_ONLY_STALE`: a derived cursor has made no progress for the stale timeout
  while behind, or the component has remained unavailable for that duration.
- `RAW_CATCHUP`: derived domains are current but raw is unhealthy or behind.
- `FATAL_CONTINUITY`: the canonical WAL is unhealthy.

Event and KLine authoritative flags are independent. The global state may be
`RAW_ONLY_STALE` because KLine is stale while the Event view remains valid at
its own cursor.

## Startup and shutdown

The process starts the new outbox run before sinks and engine admission.
Unavailable derived components leave their enabled cursor at zero; raw can
continue until storage pressure reaches the configured reservoir bound.
Transport outages after a sink starts are retried in place. A component that
cannot complete startup requires process supervision/restart in the current
implementation.

Shutdown stops SDK admission, drains decoder lanes, syncs the WAL, appends a
final barrier, and asks each available consumer to drain through its LSN. The
freshness publisher first writes an explicit non-authoritative row; if that
fails, the lease still expires fail-closed. A cursor that misses the shutdown
deadline is reported as requiring external source recovery.

## Non-goals

- reopening or replaying a prior local WAL/request spool after process crash;
- treating Arrow shared memory as a durability boundary;
- deriving a global checkpoint from source ingress/native sequence maxima;
- preserving the old synchronous listener or ACK-join APIs; or
- serving current Event/KLine from an ungated replacement table.
