# Event projection and ClickHouse contract

## Input ownership

Event is an independent canonical-WAL consumer. The outbox router preserves
each instrument owner's FIFO and carries every projectable Tick's exact WAL
position into `EventRuntime`. There is no raw-ACK inbox, disposition join, or
raw listener.

Only catalog-resolved `PROJECT_ORDERED` and `PROJECT_HOLE_FILL` dispositions
enter projection. `REJECT_LATE_FACT` is accounted and completes without a
derived row. `GapOpen` and `ChannelSeal` are ordered owner controls; a control
cannot overtake the active projection cut associated with an earlier owner
fence.

The owner mapping is derived from catalog-local `instrument_ordinal`. It is a
runtime sharding choice only; stable Event and ClickHouse identities use
business keys and never the ordinal.

## Calculation and FactJournal

Each owner is single-writer. `EventRuntime` forms bounded micro-batches, while
`EventWorker` maintains private order state, gap ranges, repair transactions,
phase normalization, Shanghai END expansion, and eviction state. These paths
retain their existing hard count/byte/slice bounds and fail the Event consumer
if a bound is violated.

Event and KLine share the process-lifetime canonical FactJournal. It records
accepted first-winner facts and supplies exact historical facts needed by
derived repair. It is a bounded local spill/cache, not the canonical WAL and
not a crash-restart checkpoint.

For every projection cut, the worker collects a sorted unique set of causal
WAL positions. If the cut produces no revision, it completes those positions
directly. If it produces revisions, the positions move with the immutable
`EventRevisionBatch`; only the ClickHouse sink can complete them.

## Revision identity

The Event logical key is:

```text
(trade_date, market, instrument_id, channel, native_sequence,
 event_kind, affected_order_id, occurrence)
```

`version` is not part of that key. Its high epoch is externally allocated and
must increase when a new calculation generation can supersede an older one.
`revision_id`, `supersedes_revision_id`, `input_set_hash`, and `payload_hash`
make the revision lineage auditable. Tombstones are revisions and participate
in replacement before current rows are filtered.

Each logical batch has a unique `recovery_run_id`, owner-local calculation
batch sequence, reason, exact causal positions, and one committed recovery
marker. Revisions without an acknowledged marker are never current.

## Request spool and ACK order

The Event sink may combine consecutive owner batches into a physical group,
but their logical recovery markers remain independent. Before any HTTP send it
syncs a run-scoped request-spool file containing:

- every causal WAL position;
- exact revision and marker RowBinary payloads;
- request kinds, row counts, and CRC32C values;
- stable query IDs and deduplication tokens; and
- PREPARED state bytes for every request.

Revision requests are sent first and the marker request last. Retryable or
ambiguous results reuse the same identities. After the marker ACK, the sink
completes the Event positions in the canonical outbox and only then retires the
spool group.

The spool is shared across writer lanes without serializing file preparation:
`Start`/`Stop` use a lifecycle lock, the live-entry registry uses a short lock,
and each group owns its state-transition lock. Exact file bytes are reserved
atomically before encoding, so concurrent prepares cannot exceed the global
spool bound. CRC32C uses the shared portable slicing-by-8 implementation.

Sink queue saturation applies bounded backpressure to this derived consumer.
It does not kill or block the raw cursor. A retryable ClickHouse outage is
retried indefinitely during the live process; cursor stall drives
`DERIVED_CATCHUP`, then `RAW_ONLY_STALE` after the configured timeout.

The request spool adds one pre-send sync per physical group. That affects Event
publication latency but is not on the raw ACK path. Physical-group size is the
main amortization control; overly small groups turn storage sync latency into a
throughput ceiling.

HTTP request bounds and physical-group bounds are independent. The production
candidate keeps revision requests at 4,096 rows / 4 MiB while allowing one
physical group to contain 16,384 revision rows / 16 MiB. A normal full group
therefore persists four revision requests and one marker request with one
`fdatasync`; an oversized logical recovery batch is still kept in one group and
split only at immutable revision-request boundaries.

## ClickHouse tables

The replacement schema contains:

- `event_revision_log`: immutable revision rows;
- `event_recovery_run`: committed markers, including
  `input_max_lsn/input_max_batch_sequence/input_max_row_index`;
- `derived_freshness_log`: leased cursor/frontier publications; and
- `event`: a fail-closed ordinary view.

The old `event` `ReplacingMergeTree` and `event_current_mv` are not supported.
Startup verifies the new engines, sort keys, marker columns, freshness columns,
and view references. Existing old layouts require an explicit offline
migration.

## Authoritative current query

For one calculation run, the `event` view:

1. selects the latest Event-domain freshness publication;
2. requires an authoritative, unexpired lease;
3. joins only committed recovery markers whose maximum causal WAL position is
   at or below the published Event cursor;
4. ranks the remaining revisions by logical key, version, and deterministic
   writer provenance; and
5. retains rank 1 before filtering `is_deleted`.

The causal marker condition prevents a fast owner whose output is already in
ClickHouse from leaking beyond a cursor pinned by a slower owner. Filtering a
tombstone before ranking would resurrect an older row, so tombstones are
filtered only after rank selection.

When Event is behind, unhealthy, or its lease expires, the view returns no
authoritative rows. Query services must inspect the latest
`derived_freshness_log` row to distinguish that state from an empty business
result.

## Barriers and shutdown

A freshness/final barrier is sent to every Event owner after all earlier WAL
records have been routed. Each owner flushes its active cut, finishes bounded
repair/eviction work, and submits pending commits. The Event barrier position
can advance only after all causal revision markers ACK.

At shutdown the process publishes a non-authoritative freshness row, drains
through the final barrier when possible, then stops the Event sink and follower.
A missed deadline is not treated as successful current state; external source
recovery is required.

## Capacity boundary

The important independent bounds are owner micro-batch rows/delay, hot facts,
order-history bytes, repair bytes, END staging, pending revision commits/bytes,
owner persistence groups, sink queue batches/rows, writer lanes, request spool
bytes, and the shared canonical WAL reservoir. Increasing one does not remove
pressure at another. Production qualification must measure both derived cursor
lag and WAL reservoir growth, not just ClickHouse INSERT rate.
