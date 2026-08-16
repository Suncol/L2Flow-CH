# KLine projection and ClickHouse contract

## Input and time domain

KLine is an independent canonical-WAL consumer. Projectable ordered and
hole-fill Tick dispositions enter the owner-local runtime with their exact WAL
positions. Rejected occurrences and recovery controls do not require KLine
rows; controls still flush the preceding owner cut before their positions can
complete.

Only eligible trade facts update bars. Window boundaries and OHLC order use
valid normalized exchange time in nanoseconds from exchange midnight. Host
wall time, receive monotonic time, and vendor local time are not substitutes.
Configured intervals are integer seconds in `[1, 86400]`, with half-open
windows `[bucket_start, bucket_end)` anchored at exchange midnight.

## Calculation and completion

Each owner is single-writer and holds its own bar map. `KLineRuntime` builds
bounded micro-batches. The worker first records accepted facts in the shared
FactJournal, applies first-winner/duplicate/conflict rules, and produces higher
versions for changed logical bars. Accepted hole fills can revise an earlier
bar.

The logical key is:

```text
(trade_date, market, instrument_id, interval_seconds,
 bucket_start_ns_from_midnight)
```

`version` is an attribute, not a key member. The externally allocated revision
epoch and unique calculation run ID separate calculation generations.

A micro-batch collects sorted unique causal WAL positions. If no KLine
revision is produced, those positions complete directly. Otherwise they are
owned by an immutable `KLineRevisionBatch` until its ClickHouse recovery marker
ACKs.

## Request spool and retries

Before sending a physical group, the KLine sink syncs a request-spool file with
the exact causal positions, revision/marker RowBinary bytes, payload CRCs, row
counts, query IDs, deduplication tokens, and request states. Revision INSERTs
precede the marker INSERT. The KLine cursor advances only after marker ACK and
exact-position validation by the canonical outbox.

Retryable ClickHouse failures retry with stable request identity for the live
run. Queue saturation blocks only this derived follower. A stalled cursor is
reported as `DERIVED_CATCHUP`, then `RAW_ONLY_STALE` after
`--derived-stale-after-ns`; it does not synchronously fail raw ClickHouse.

One spool `fdatasync` per physical group increases KLine persistence latency.
Larger groups amortize that cost, subject to request byte/row bounds and the
acceptable freshness delay.

## ClickHouse schema

The replacement schema contains:

- `kline_revision_log`: immutable KLine revisions;
- `kline_recovery_run`: committed logical-batch markers with the maximum
  causal WAL position;
- `derived_freshness_log`: shared leased cursor/frontier publications; and
- `kline`: a fail-closed ordinary view.

The old `ReplacingMergeTree(version)` current table and
`kline_current_mv` are not retained. Runtime probes reject an old or partially
migrated schema.

## Current-view ordering

The KLine view first selects an authoritative unexpired KLine freshness lease.
It then excludes recovery runs whose
`(input_max_lsn,input_max_batch_sequence,input_max_row_index)` exceeds the
published KLine cursor. Only committed, cursor-bounded revisions participate
in `row_number()` replacement. The latest eligible revision wins, after which
tombstones are filtered.

This order is required for correctness:

- filtering before replacement could resurrect a deleted bar; and
- ranking before cursor filtering could expose a fast owner's revision beyond
  the gap-free KLine frontier.

When KLine is not caught up or its lease expires, the view returns no
authoritative current rows. Consumers must read `derived_freshness_log` to
surface stale/unavailable state explicitly.

## Provisional bars

Realtime bars remain provisional when the process cannot prove that an older
accepted hole-fill trade will never arrive. A later accepted fact emits a new
revision of the same logical key. The current view resolves that history only
within the published authoritative cursor.

## Barriers, capacity, and recovery

A WAL barrier is routed behind all prior records to every owner. Each owner
flushes its active cut and pending KLine commits; the domain cursor crosses the
barrier only after all earlier recovery markers ACK.

The main independent limits are bars per owner, pending commits, micro-batch
rows/delay, sink queue rows/batches, writer lanes, physical request/group
bounds, request-spool bytes, and canonical WAL reservoir bytes. A long derived
outage grows the WAL until either KLine catches up or the durable reservoir
hits its fatal bound.

The local WAL, FactJournal, and request spool are not reopened after a process
crash. External replay under a new feed epoch and calculation run reconstructs
the state. No maximum ingress sequence is a valid KLine checkpoint.
