# KLine worker and ClickHouse revision contract

## 1. Implemented path

The optional KLine plane is a sibling of the Event plane. It consumes the same
unified owner-local `TickDispatch` FIFO:

```text
GapOpen / ChannelSeal -> validate owner FIFO control
REJECT_LATE_FACT ------+
                       +--> bounded ACK/disposition join -> reject + ACK: settle
                       |
PROJECT_* -------------+--> project + ACK: durable dependency
          |
          +-> micro-batch -> journal winner -> update intervals
                                            -> immutable pending batch
                                                          |
                                     durable dependency --+-> KLine sink
```

The worker may calculate an in-memory revision before its raw occurrence is
durable. It cannot submit that revision to the KLine sink until all raw Tick
dependencies at the head of its pending FIFO have received a successful raw
ClickHouse ACK. The raw, runtime, and revision queues are volatile memory, not
a WAL. Event and KLine must share one local disk-backed canonical FactJournal;
that file is a process-lifetime spill/cache and is not reopened after a crash.
See [fact-journal.md](fact-journal.md) for its capacity, I/O, and recovery
contract.

The runtime joins either arrival order by `(feed_session_epoch,
ingress_sequence, canonical_kind)`. `REJECT_LATE_FACT + raw ACK` erases the join
entry directly and never reaches KLine or the FactJournal. The join, ACK inbox,
worker ACK index, pending FIFO, bar map, and sink queues are independently
bounded and fail closed on exhaustion. `GapOpen` and `ChannelSeal` remain in the
same producer-to-owner FIFO and are validated by KLine, but this worker does not
use Event's retention frontier to reclaim bar state.

## 2. Exchange-time-only windows

The SDK 2.13.234 body fields used by the implemented Tick decoders are:

- Shanghai `NGTSTick.TickTime`;
- Shenzhen `Order300192_v2.TransactTime`;
- Shenzhen `Transaction300191_v2.TransactTime`.

The decoder validates the MDL `hhmmssmmm` value and converts it exactly to
nanoseconds from exchange midnight. A KLine trade is eligible only when all
of the following are true:

```text
common.exchange_time_valid == true
(validity & kTickExchangeTimeValid) != 0
exchange_time_ns_from_midnight < 86_400 * 1e9
```

There is deliberately no fallback to `MDLMessageHead.LocalTime`,
`vendor_local_time_ns_from_midnight`, `receive_monotonic_ns`, system UTC, or
the host wall clock. The MDL header describes `LocalTime` as message creation
time; it is not used as exchange event time.

For an integer interval `n` in seconds:

```text
duration_ns = n * 1_000_000_000
bucket_start_ns =
    floor(exchange_time_ns_from_midnight / duration_ns) * duration_ns
bucket = [bucket_start_ns, bucket_start_ns + duration_ns)
```

Windows are anchored at exchange midnight, not process startup or the local
clock. `trade_date` remains part of the logical key. If `n` does not divide
86,400, the mathematical final bucket can end after midnight; supported
Shanghai/Shenzhen A-share trading times are far from that boundary, and the
trade date still isolates the row.

`receive_monotonic_ns` is used only to decide when to flush a partially filled
in-memory micro-batch. The sink also uses local monotonic time for retry and
shutdown deadlines. Its best-effort `committed_utc_ns` is operational marker
metadata only. None of these values changes a KLine key or payload.

## 3. Trade eligibility and values

Only canonical `TickAction::kTrade` records are projected:

- Shanghai: `CanonicalKind::kShanghaiTick` decoded from `NGTSTick`;
- Shenzhen: `CanonicalKind::kShenzhenTransaction` decoded from tuple 6.36.

The canonical p6 price must be valid and positive. Quantity must be a valid,
positive, scale-zero integer. Order references, aggressor, source amount, and
channel-history completeness are not prerequisites for OHLCV.

The stored values are:

```text
open/close = first/last accepted trade in the deterministic order below
high/low  = maximum/minimum valid p6 trade price
volume    = checked sum of integer trade quantity
notional_p6 = checked sum(price_p6 * quantity)
trade_count = number of accepted unique trade facts
```

`notional_p6` is computed independently of the optional Shanghai source
amount. Signed 64-bit multiplication and accumulation are checked; overflow
fails the worker closed instead of wrapping. No empty or carry-forward bars
are synthesized.

## 4. Ordering and out-of-order repair

The OHLC anchor order is:

```text
(exchange_time_ns_from_midnight, channel, native_sequence)
```

Exchange time is always the first member. Channel/native sequence provides a
stable tie-break for the SDK's millisecond time resolution. It is not a claim
that the exchange publishes a total order across channels. The ingress
sequence is retained in the first/last anchors for provenance but does not
control OHLC.

For channel next-expected `E`, origin, and `maximum_reorder_span=W`, recovery
captures `A=max(origin,E-W)` at first arrival. A record at or beyond `E` follows
the ordered/reorder path. A record in `[A,E)` is projected only when it
atomically claims a still-open exact hole; a record below `A` or a
behind-frontier position absent from the hole ledger is rejected. The boundary
is exact (`sequence == A` can fill an open hole), and owner queueing never
recomputes the captured token.

The KLine runtime routes an accepted hole fill to the same instrument owner. A
hole-filled trade updates the historical bar and emits a higher-version
`UPDATE` for the same KLine key. A fill that precedes the current first anchor
changes open; one after the last anchor changes close; an interior fill can
still change high, low, volume, notional, and count.

The fact key is:

```text
(trade_date, market, channel, native_sequence)
```

This matches the sequence domain already used by `SequenceRecovery` and has
the same documented Shenzhen joint 6.33/6.36 caveat. Every accepted
structurally valid Tick action is journaled, not only trades.
`SequenceRecovery` applies strict first-wins to `(trade_date, market, channel,
feed_session_epoch, native_sequence)`: every later occurrence at the same
position is rejected without payload comparison and cannot reach KLine. The
FactJournal's duplicate/conflict result remains a defensive invariant check,
not a second winner-selection policy.

The raw table retains independently acknowledged occurrences. Choosing a
different winner for a source conflict requires explicit reconciliation; the
live KLine worker does not guess.

## 5. Keys, revisions, and provisional state

The logical KLine key is:

```text
(trade_date, market, instrument_id, interval_seconds,
 bucket_start_ns_from_midnight)
```

`instrument_id` is the stable catalog identity. The dense
`instrument_ordinal` is used only for owner routing and is not persisted as a
KLine identity. Multiple configured intervals and all Event/KLine owners share
one canonical journal but produce independent KLine keys. KLine retains bar
state, not a full trading-day `CanonicalTick` map.

Each changed bar produces an immutable revision. Its version is:

```text
version = (revision_epoch << 32) | owner_local_revision_counter
```

Every logical key has one stable owner for a frozen daily catalog. A
supervisor must durably allocate a strictly increasing, nonzero
`revision_epoch` for every run that can revise the same key space. It must also
provide a unique nonzero `calculation_run_id`; this executable validates but
does not allocate or persist either value.

Realtime rows remain `provisional=true`. Observing a later exchange timestamp,
a higher native sequence, an empty current hole ledger, a Shanghai `Ended`
status, or a local timer does not prove that durable historical reconciliation
is complete. This implementation does not expose an end-of-day
reconciliation/finalize API and therefore does not write `provisional=false`
on its own.

## 6. ClickHouse contract

The sink writes revision chunks to `kline_revision_log` using immutable
RowBinary bodies, stable query IDs, and stable insert-deduplication tokens. An
unknown HTTP outcome retries the exact same chunk. Only after every revision
chunk is acknowledged does it write the corresponding
`kline_recovery_run` commit marker.

`kline` is a `ReplacingMergeTree(version)` current table populated by a
materialized view. Its sort key excludes `version` and exactly matches the
logical KLine key inside the `trade_date` partition. Correct direct reads must
perform replacement before tombstone filtering:

```sql
SELECT *
FROM l2flow.kline FINAL
WHERE is_deleted = false;
```

The sink supports one, two, four, or eight writer lanes. A batch is routed by
`owner % writer_lanes`, preserving FIFO for one owner while permitting
unrelated owners to write concurrently. Queue batch and row capacities are
global bounds across the lanes. Exhaustion, permanent INSERT failure, retry
budget expiry, schema mismatch, or marker failure makes the sink unhealthy.

The single-node and Keeper-backed schemas are in
`clickhouse/schema/kline_tables.sql` and
`clickhouse/schema/kline_tables_replicated.sql`.

## 7. Configuration and operational boundary

Enable the path on top of durable raw ClickHouse output:

```text
--clickhouse-url http://127.0.0.1:8123
--clickhouse-feed-epoch <nonzero raw feed epoch>
--kline-enable
--kline-interval-seconds 1
--kline-interval-seconds 5
--kline-revision-epoch <durably allocated nonzero epoch>
--kline-calculation-run-id <unique 32-hex run ID>
```

Intervals are repeatable, sorted, deduplicated at startup, and restricted to
`[1, 86400]`. They are immutable for a runtime. Capacity, micro-batch, sink
chunk, queue, and writer-lane flags are listed by `mdl_ingestd --help`.

KLine does not retain a full in-memory Tick history after each batch, but its
bar/head maps are bounded independently by `maximum_bars` and are not reclaimed
from the `ChannelSeal` controls it receives. The shared FactJournal's physical
file and directory also remain process-lifetime state. Event fact compaction
therefore does not imply KLine or whole-process RSS compaction.

The current implementation is intraday and process-local. It has no cold
bootstrap from `raw_tick`, checkpoint restore, local WAL, crash replay, or
cross-table atomic transaction spanning raw and KLine tables. A successful raw
ACK gates live revision submission, but a process crash can still lose
in-memory derived state. Restart reconciliation remains a separate required
workflow.
