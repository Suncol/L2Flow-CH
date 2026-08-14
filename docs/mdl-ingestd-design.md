# `mdl_ingestd` design and correctness contract

## 1. Scope

The core ingest library ends at instrument dispatch:

```text
MDL callback
  -> header/tuple/size/route admission
  -> one preallocated SPSC lane
  -> full wire decode and canonical normalization
  -> per-channel native sequence recovery for ticks
  -> lane x instrument-owner SPSC dispatch matrix
```

The core has no Redis dependency and no disk path. `mdl_ingestd` can attach the
bounded shared-memory Arrow sink described in
[`arrow-hot-path.md`](arrow-hot-path.md); that sink is not durable and does not
change the recovery contract below. It can also attach the raw ClickHouse sink
described in [`clickhouse-raw-path.md`](clickhouse-raw-path.md). That sink taps
decode-complete canonical facts before recovery and persists them in
`raw_tick`/`raw_snapshot`; its in-memory queues are not a WAL. The repository
also contains the optional owner-local Event projection and ClickHouse
revision sink described in
[`event-worker-clickhouse.md`](event-worker-clickhouse.md). Event calculation
consumes projectable facts and channel controls from the unified `TickDispatch`
FIFO. The optional KLine plane in
[`kline-worker-clickhouse.md`](kline-worker-clickhouse.md) consumes the same
stream and uses SDK body exchange time for integer-second windows. Both
runtimes settle rejected occurrences in a bounded raw-ACK/disposition join;
only projectable first-wins facts enter their workers, and publication remains
gated by corresponding raw ACKs. Event and KLine share the local disk-backed
process-lifetime FactJournal described in
[`fact-journal.md`](fact-journal.md). That file is not a durable restart/replay
boundary. The repository does not contain CSV/WAL replay, checkpoint restore,
reconnect epoch inference, cold derived-state bootstrap, or intraday restart
reconciliation. Startup is exactly one of:

- `from-open`: the process claims coverage beginning at native sequence 1;
- `partial`: the process claims only a bounded process-start suffix and never
  claims an opening prefix.

Snapshots do not carry the tick native-sequence contract and therefore bypass
channel recovery after full decode.

## 2. Factual ABI/schema basis versus implementation policy

The distinction matters: timeout, lane count, capacity, and CPU placement are
engineering choices, not exchange or MDL facts.

### Facts checked against MDL SDK 2.13.234 headers

- `MDLMessageHead` is a packed 23-byte ABI object. The adapter copies its
  bytes and decodes fields without dereferencing an unaligned packed object.
- `MDLAnsiString` stores a 16-bit length and a 32-bit offset relative to the
  descriptor. `MDLList` stores a 32-bit count and relative 32-bit offset.
- MDL API `1.101` defines `ConnectErrorEvent` as message 2,
  `DisconnectedEvent` as 3, `MessageServiceTimeOutEvent` as 5, and
  `MessageDiscardedEvent` as 6. MDL SYS `2.101` defines `LogonResponse` as
  message 2 and `SubscribeResponse` as 23. `LogonResponse` carries a return
  code plus per-subscription message statuses.
- The implemented decoders are exactly:

  | Tuple | Vendor type | Fixed bytes |
  |---|---|---:|
  | `4.101.4` | `SHL2MarketData` | 248 |
  | `4.101.24` | `NGTSTick` | 70 |
  | `6.101.28` | `Snapshot300111_v2` | 224 |
  | `6.101.33` | `Order300192_v2` | 58 |
  | `6.101.36` | `Transaction300191_v2` | 70 |

- The SDK layouts expose signed `BizIndex`/`ApplSeqNum` fields and the stated
  channel fields. For this recovery contract the decoder accepts only
  positive native sequence values, accepts only positive Shanghai `Channel`,
  and does not reject Shenzhen `ChannelNo == 0`. These acceptance rules are
  implementation validation policy, not inferred wire-layout facts.
- `MDLTime` is `hhmmssmmm`; `1,000,000,000` is its null value. The canonical
  projection stores the original integer and, only when valid, exact
  nanoseconds from midnight. It does not invent a UTC timestamp.
- Signed fixed-point null values are their vendor integer minimum sentinels.
  Canonical values retain exact raw integer plus source scale. p6 is emitted
  only after a checked integer multiplication.

All vendor layout offsets used by the decoder have compile-time `sizeof` and
`offsetof` assertions against the SDK headers. Dynamic strings, level lists,
and nested queues are checked for arithmetic overflow, body bounds, count
bounds, printable identity text, and mutual range overlap before use.

### Native-domain caveat that must remain explicit

The local reference documentation records that the vendor tables describe
order and transaction `ApplSeqNum` as unique and continuous under one
`ChannelNo`. This implementation consequently merges 6.33 and 6.36 into one
Shenzhen sequence domain. The same reference also notes that the vendor
documents do not separately state the sentence “the counter is shared across
the two message families.” Production acceptance must therefore verify the
joint monotonic sequence using a real replay containing both tuples, or obtain
vendor confirmation. A replay containing only one tuple cannot validate that
assumption.

There is no documented exchange total order across channels or markets. The
implementation guarantees native order within each channel. The owner poller
preserves each producer FIFO but its merge across lane queues is a local fair
poll order, not an exchange-provided global order. Snapshot/tick relative order
is likewise not claimed.

### Explicit implementation policies

- Subscriber creation uses `multithread_callback=false`; physical SDK config
  requires one I/O thread. The admission API detects concurrent producers and
  fails the engine rather than corrupting SPSC ownership.
- Default PARTIAL initial hold is 200 microseconds. FROM_OPEN and PARTIAL gap
  waits both default to 20 milliseconds but remain separately configurable.
  These values are tuning defaults, not protocol constants or statements
  about upstream backfill latency.
- TickDispatch/fault overload, admission-lane exhaustion, raw ClickHouse
  batch-pool exhaustion, and any derived ACK-join exhaustion are fatal
  continuity boundaries.
  The process never silently drops a record to remain live. A raw row already
  acknowledged by ClickHouse can support later reconciliation, but it does not
  make an overloaded live process safe to continue. Gap notification is the
  exception: it uses fixed per-Channel aggregate state plus a dirty bitmap,
  not a bounded event queue.
- An exception during a vendor callback operation is caught at the ABI
  adapter, recorded as a sticky failure, and causes process shutdown; no C++
  exception is allowed to escape into the SDK.
- The process does not equate an immediate error-free `Connect()` return with
  feed readiness. It requires a successful Logon response and successful
  status for every configured tuple. Bounds-invalid control lists, rejected
  statuses, and the local readiness deadline terminate the process/feed epoch.
  In PARTIAL mode, business callbacks received before readiness are counted
  and discarded; the first callback after readiness establishes the admitted
  live prefix. FROM_OPEN keeps pre-ready business data fatal because dropping
  it would violate the declared from-open continuity.
- API service-timeout and message-discard events also terminate the current
  process/feed epoch. Their message names and IDs are SDK facts; choosing a
  fatal boundary is conservative implementation policy, not a claim that the
  SDK protocol requires this exact response.
- For `mdl_ingestd`, the configured stream file is the source of physical
  subscriptions. There is no special-case protection for any unselected
  tuple.
- Event projection is optional and requires the durable raw ClickHouse path.
  Revision batches are computed in memory but cannot enter the Event sink
  before all raw occurrences in their pending FIFO entry are ACKed. A raw ACK
  listener failure, Event repair/index capacity failure, or Event sink queue
  failure is a sticky process boundary.
- The default stream file selects the five implemented Shanghai/Shenzhen
  L2 message families used for A-share processing. Tuple subscription and
  instrument-universe filtering are separate: the immutable daily catalog is
  authoritative for which exact identities are dispatched.

## 3. Minimal admission

The SDK callback performs only bounded work:

1. copy the 23-byte header;
2. check header size, declared message size, binary encoding, supported tuple,
   and whether the tuple is selected by stream config;
3. read the fixed routing fields (channel for ticks, exact identity bytes for
   snapshots);
4. acquire one lane-owned preallocated slot;
5. copy the body once, publish the slot index, and return.

There is no callback allocation, mutex, reference-count handoff, full decode,
instrument-state mutation, or downstream fan-out. One callback producer and
one lane consumer exchange slot indices through SPSC rings; a second SPSC ring
returns consumed slots to the callback producer.

The SDK `SequenceID` is retained as vendor metadata but is never used as the
exchange-native tick sequence.

## 4. Decoder and canonical representation

Every decoder lane owns its messages and performs full schema validation away
from the callback. Canonical records are fixed-size, trivially copyable
objects. They use:

- exact `security_id_source + security_id` identity bytes;
- explicit market, tuple, channel, native sequence, trade date, ingress and
  vendor sequence metadata;
- original exchange/vendor local time and validity;
- integer fixed-point raw/scale plus checked p6 where meaningful;
- validity bitmaps and quality flags instead of numeric missing sentinels;
- up to 10 retained book levels while preserving source depth counts and a
  truncation flag.

After successful normalization, the decoder copies the canonical record into
its preallocated raw batch before SequenceRecovery or catalog suppression.
The decoder does not construct Arrow arrays, hash IDs, serialize IPC, call a
wall clock, or perform network I/O. Dedicated ClickHouse writer threads do all
columnization, provenance hashing, synchronous INSERT, and exact-batch retry.
The only per-row receive clock is the callback's existing monotonic timestamp;
one UTC/monotonic anchor pair is captured at process startup for operational
correlation and is not replay order.

Shanghai `BidNum`/`SellNum` are retained neither as array bounds nor used to
walk memory; the two MDL list descriptors are the authoritative dynamic list
bounds. Nested queue records are validated even though this stage does not
publish the queue contents.

The decoder keeps action-dependent field meaning. Examples:

- SH `A`: source `TradeMoney` is interpreted as matched quantity under the
  documented/reference rule and is not published as trade amount;
- SH `D` and status messages do not publish meaningless price/amount fields;
- SZ order price is valid only for limit orders;
- SZ transaction execution type `70` is trade and `52` is cancellation;
- an ambiguous SZ cancellation keeps both raw order references and leaves
  primary order/side invalid.

Unknown enum values remain unknown and flagged; they are never guessed.

The immutable daily catalog maps the exact opaque identity to a dense ordinal
and stable instrument ID. A tick absent from the catalog is still processed as
an output-free continuity token, so filtering cannot manufacture a native
gap. An unknown snapshot is counted and not dispatched. Both are copied to the
raw ClickHouse path with `kQualityInstrumentNotInCatalog` and
`catalog_match=false` before owner suppression.

## 5. Channel recovery

### FROM_OPEN

Each newly observed channel begins with `expected = 1`.

- `sequence == expected`: publish, advance, and drain contiguous pending
  records;
- future sequence: retain it during `from_open_gap_wait_ns`, allowing an
  out-of-order network/upstream backfill to close the gap normally;
- any later occurrence at an already pending native position: reject it without
  payload comparison; the first canonical body remains authoritative;
- gap-wait expiry, reorder-span breach, or a full-table slot collision: record
  an exact open-hole interval, emit `GapOpen`, advance to the smallest retained
  position, drain in order, and retry the current record without discarding the
  current or retained body;
- a later record below the advanced frontier: accept it only when it is still
  inside the online window and atomically claims a position in the exact hole
  ledger; otherwise emit a rejected-occurrence disposition;
- a full-decode failure with a valid native descriptor: freeze only that
  channel and emit a decode fault.

Other channels and snapshot lanes continue. FROM_OPEN history is complete
until the first committed gap; after that gap, all subsequent realtime records
for that channel carry incomplete-history quality. Closing or expiring every
currently repairable hole does not restore the historical completeness claim.

### PARTIAL

PARTIAL uses the same bounded gap advance, exact hole admission, and
first-wins behavior, but its starting claim is different: it never claims the
unobserved opening prefix.

1. During the configurable initial hold it retains arrivals and selects the
   smallest observed native sequence as the process-start origin.
2. Later future records wait in the bounded reorder table.
3. When `partial_gap_wait_ns` expires, a record exceeds the bounded span, or
   a modulo slot collides because the reorder table is full, the lane records
   an exact missing interval, emits `GapOpen`, advances to the smallest present
   sequence, drains retained records in native order, and iteratively retries
   the current record.
4. A later record below the advanced frontier is not inserted backward into
   the ordered stream. It is either an accepted exact hole fill or a rejected
   occurrence, decided once at SequenceRecovery arrival.
5. A later record at a pending or already claimed native position is rejected;
   payload equality/conflict does not alter first-wins.

The capacity rule below is identical after either mode has an active frontier.
For example, with `expected=4` and retained `5,6,7,8`, arrival of `9` at a full
four-slot table performs this sequence without dropping any received body:

```text
record GapRange[4,4]
advance expected -> 5
emit 5,6,7,8 and release their reorder slots
retry 9 and emit it
```

All realtime records from a PARTIAL channel have
`kTickChannelHistoryValid` cleared; after a committed gap they also retain
the channel's incomplete-history quality. Missing native positions do not
allocate one payload placeholder apiece. They are stored as a preallocated,
sorted set of disjoint inclusive intervals bounded by
`reorder_entries_per_channel`; a split that would exhaust interval capacity
fails the engine closed.

No fake event is synthesized for a missing native position because its
instrument and message kind are unknowable. Instead, the first subsequently
dispatched target record carries `gap_before_first/last`, `gap_epoch`, and
`kQualitySequenceGapBefore`.

Diagnostic gap telemetry cannot overflow an event queue. Every observed Channel
owns one fixed mailbox, and decoder lanes set its dirty bit after publishing the
latest exact `GapRange`. The mailbox also carries cumulative `gap_epoch` and
`cumulative_missing_sequences`. If a consumer is slow, multiple notifications
for the same Channel coalesce: the latest range remains exact and an epoch jump
reports that older notifications were coalesced. Recovering every older range
then requires the canonical record metadata and/or the separate raw durable
path; the mailbox never fabricates an enclosing range that could label present
sequences as missing.

That coalescing mailbox is diagnostic. Correctness controls use `GapOpen` and
`ChannelSeal` records broadcast into every affected decoder-lane-to-owner SPSC
FIFO. `GapOpen` is published before the first drained fact beyond the committed
gap. A `ChannelSeal` is published when the retention frontier advances after a
hole fill or expiration. Both carry a generation and a per-edge
`dispatch_fence`; downstream owners consume them in FIFO order with projectable
and rejected occurrences.

PARTIAL clears `kTickChannelHistoryValid` from the first emitted record, not
only after the first observed gap, because the pre-start channel prefix is
unknown. `kQualityChannelHistoryIncomplete` instructs Event/book derivation to
publish null for any field that requires unavailable history. Source-carried
fields such as native sequence, time, raw price, quantity, and order references
retain their independent validity. A resource failure (allocation,
loss-sensitive output queue exhaustion, or invalid channel identity) is not
reclassified as a historical gap and can still fail the process.

### Exact admission and first-wins

For an active channel define:

```text
E = expected, the next native sequence SequenceRecovery wants
W = maximum_reorder_span
A = max(process_origin, E - W)
R = first position in the earliest open-hole interval, or E when none exists
```

`mdl_ingestd` exposes `W` as `--maximum-reorder-span`. The independent
`--reorder-entries-per-channel` value is the preallocated pending-record and
hole-interval capacity; it must be a power of two and at least `W`. Neither
capacity grows when repair or eviction falls behind.

Classification uses `E` and `A` at the occurrence's first arrival:

```text
sequence >= E     -> ordered/reorder path
A <= sequence < E -> PROJECT_HOLE_FILL only if the exact position is OPEN
sequence < A      -> REJECT_LATE_FACT
```

`sequence == A` is eligible only for an open hole. A sequence in `[A,E)` that
is not open is also rejected. A successful hole claim removes exactly that
position before dispatch, so a second arrival cannot claim it. Every occurrence
carries its captured `E/A` and generation. Projectable occurrences additionally
carry the current `R` as `evict_before`, and `ChannelSeal` carries a newly
advanced `R`; none is recomputed after owner-queue delay.

Strict first-wins applies to `(trade_date, market, channel,
feed_session_epoch, native_sequence)`. The first body wins whether later bodies
are byte-identical or conflicting; every later body is rejected without
replacement. The raw tap still captured each decoded occurrence before this
decision. For catalog-resolved ticks, `REJECT_LATE_FACT` is therefore required
to settle the raw ACK without admitting the body to Event, KLine, or the
FactJournal.

Open holes older than `A` expire and can never reopen. `E` may jump across a
gap, so `E-1` is not necessarily a complete prefix. `A` is the admission close
frontier, while `R` is the repair retention frontier. With no hole `R=E`; with
holes it pins only the suffix beginning at the earliest still-open position.

## 6. Instrument dispatch

For `P` decoder lanes and `W` instrument owners, the dispatcher builds a
`P x W` SPSC matrix. Each edge has exactly one lane producer and one owner
consumer, avoiding an MPSC compare-and-swap hotspot. Owner assignment is
`instrument_ordinal % W` and is stable for the frozen daily catalog.

Ticks and snapshots use separate matrices so the much larger snapshot record
does not inflate every tick slot. Every Tick edge carries one fixed-width
`TickDispatch` representation:

```text
kProjectOrdered
kProjectHoleFill
kRejectLateFact
kGapOpen
kChannelSeal
```

Catalog-resolved projectable and rejected occurrences go only to their
instrument owner.
`GapOpen` and `ChannelSeal` are broadcast to all owners because a missing
position's instrument is unknowable. Before broadcasting a control, the lane
checks capacity on every destination edge; partial broadcast is not permitted.
Each published record receives the next producer-edge `dispatch_fence`, so one
owner can validate FIFO progress without treating an owner-local sparse channel
subsequence as a completeness proof. Channel faults retain the separate
diagnostic endpoint. Gap telemetry uses the fixed per-lane/per-channel mailbox
and dirty bitmap described above.

The executable gives each owner endpoint to exactly one drain thread. That
thread also owns the corresponding Event and KLine workers when those outputs
are enabled. It polls `TryPollTickDispatch(owner, ...)` and forwards each record
to both enabled runtimes; only projectable kinds enter Arrow. It never assigns a
second consumer to the owner endpoint. Calling one owner endpoint from multiple
consumers violates the SPSC contract.

### Raw ACK/disposition join

The raw tap captures a decoded occurrence before SequenceRecovery, so raw ACK
and final disposition can arrive in either order. Event and KLine each maintain
an owner-local bounded join keyed by:

```text
(feed_session_epoch, ingress_sequence, canonical_kind)
```

Projectable disposition plus ACK forwards the durability dependency to that
worker's pending revision FIFO. Rejected disposition plus ACK deletes the join
slot immediately and never enters the FactJournal, repair, bar projection, or
derived sink. A duplicate ACK/disposition side is a sticky failure while the
key still has an unmatched live join slot; inbox overflow and join capacity
exhaustion are also fail-closed. Resolved slots are reclaimed immediately, so
the join does not keep an unbounded post-resolution duplicate history. The raw
callback-once and SequenceRecovery occurrence-once rules are producer
contracts.

## 7. 64-core / 1-TiB deployment starting point

The defaults are a starting point, not a measured production guarantee:

- 12 tick decoder/recovery lanes;
- 4 snapshot decoder lanes;
- 16 instrument owners;
- one SDK I/O thread and serialized callback admission;
- bounded per-edge dispatch queues and lazily allocated per-observed-channel
  reorder arrays.

On a 64-core host, reserve cores for the OS/IRQs, the vendor SDK, ClickHouse
writer threads, and Event/KLine owner work. Pin decoder lanes only after
measuring the actual NUMA topology; a flat logical CPU number does not identify
socket locality.
Keep the SDK callback and its hottest decoder lanes in the same NUMA domain
when possible, or use an explicit interleave/binding policy outside this
portable core. Do not claim low latency from core count alone.

Large admission arenas and SPSC record storage are allocated without eager
value-initialization. Their real producer performs the first write, avoiding a
startup pass that would pre-fault roughly 500 MiB on the main thread under the
default capacities. This improves first-touch placement but does not replace
an explicit production NUMA policy.

Hot decoder counters are cache-line-isolated per lane and aggregated only when
`stats()` is sampled. Raw sink row counters are updated once per batch
publication, not once per record. Decoder lanes therefore do not serialize
each message on a shared global statistics cache line.

When `first_decoder_cpu` is configured, every decoder thread is explicitly
pinned and its idle path remains in pause-spin. This removes scheduler-yield
wake-up latency at the cost of one continuously occupied logical CPU per
decoder lane. When decoder affinity is disabled, the portable idle policy
retains the bounded spin-then-yield behavior.

Capacity must be derived from measured peak burst duration, not average rate.
The principal memory terms are:

```text
tick admission      = tick_lanes * tick_slots * max_tick_body
snapshot admission  = snapshot_lanes * snapshot_slots * max_snapshot_body
tick dispatch       = tick_lanes * owners * edge_capacity * sizeof(TickDispatch)
snapshot dispatch   = snapshot_lanes * owners * edge_capacity * sizeof(snapshot)
reorder             = observed_channels * reorder_entries * sizeof(pending tick)
hole ledger         = observed_channels * reorder_entries * sizeof(hole interval)
gap state           = tick_lanes * max_channels * sizeof(gap mailbox slot)
Event hot suffix    = sum_channels(E-R) + accepted inflight + eviction lag
Event carry orders  = full baselines + retained order-use suffixes
Event staging       = repair + END + pending revisions + ACK joins
KLine               = bars + heads + pending revisions + ACK joins
FactJournal         = process-lifetime file + directory + hot cache
```

Event enforces independent hard caps for hot fact count/estimated bytes, carry
order count, conservative order-history bytes, repair bytes, Shanghai END
candidates/rows/staging bytes, pending revision bytes, and ACK join entries. Its
eviction is node/estimated-byte sliced and compacts order histories to a full
private-state baseline before erasing closed fact/Bundle/head/phase/barrier
state. In no-gap traffic `R=E`, so the logical Event fact suffix normally
approaches zero instead of retaining a fixed full window.

This is logical compaction, not a process RSS guarantee. The carry-order table
uses a fixed directory with lazily allocated bucket-head pages, and its two
range indexes use exactly accounted arrays. Fact/Bundle/head indexes remain
standard node containers; erasing entries need not shrink their hash buckets
or make the allocator return pages to the OS. No per-channel hot-fact
`ChannelPage`/slab allocator or `madvise`/`unmap` reclaim path is implemented.
KLine bar state and the shared FactJournal file/directory have independent
lifetimes and are not reclaimed by Event seals. A 1-TiB deployment can
accommodate large configured caps, but oversized queues and maps increase
cold-page, TLB, allocator, and cache cost. Increase them only from burst,
repair, and latency measurements.

## 8. Lifecycle and failure boundaries

Startup order is catalog/stream config -> FactJournal and Event/KLine/raw
sink/runtime object preallocation -> engine preallocation -> optional Arrow
egress creation -> Event/KLine ClickHouse schema validation and writer startup
-> raw ClickHouse schema validation and writer startup -> decoder threads ->
owner drain threads -> SDK manager/subscriber -> connect. `Connect()` may invoke
callbacks synchronously, so the sinks, Event/KLine runtimes, engine, and owner
drains are running before it is called. The physical session is returned only
after successful Logon plus confirmation of every configured subscription.
Market data is admitted only after that readiness state. The readiness timeout
defaults to 30 seconds and is configurable with
`--sdk-ready-timeout-seconds`.

The first connection/discard/readiness boundary is sticky. The handler stops
admission immediately, so SDK auto-reconnect callbacks cannot enter the old
feed epoch. Shutdown drains records admitted before the boundary, then the
Arrow control stream records the boundary and seals. A subsequent process must
use a strictly larger externally allocated epoch; no MDL resume from a
ClickHouse-acknowledged native sequence is assumed.

Shutdown order is fixed:

1. stop handler admission;
2. call `IOManager::Shutdown()` as the callback-quiescence boundary;
3. release Subscriber, then IOManager;
4. drain and join decoder lanes, flushing partial raw batches;
5. keep every owner thread servicing its `TickDispatch` FIFO, Event/KLine
   background work, and raw-ACK inbox while the ClickHouse raw writers flush;
6. wait for every ClickHouse raw batch ACK and stop/join the raw writer
   threads, which closes the only remaining ACK producer side;
7. only then signal the owner consumers to observe empty engine FIFOs, drain
   the final ACK inbox entries, collect fault/final dirty gap diagnostics, and
   exit; flush every final partial Event and KLine micro-batch after they join;
8. finish private Event repair/eviction, drain both ACK/disposition joins and
   worker raw-ACK gates, flush the shared FactJournal, submit all now-durable
   revision batches, and stop the Event/KLine writers after their independent
   recovery markers are ACKed;
9. seal the optional Arrow hot path and destroy the engine/handler.

Stopping owner consumers before step 6 is invalid: raw `Stop()` can still
drain more queued rows than one owner ACK inbox can hold. Keeping the consumers
alive makes shutdown use the same bounded join path as steady state instead of
turning a legal slow-sink backlog into an artificial capacity failure.

After SDK shutdown returns, the session waits for its in-flight callback count
to reach zero before releasing Subscriber and IOManager. If vendor shutdown
throws, the process terminates instead of continuing destruction with a
possibly live callback target. The successfully loaded vendor DSO is
intentionally retained for process lifetime.

## 9. Verification status and remaining gates

Automated tests cover all five fixed layouts, SH/SZ normalization, joint
6.33/6.36 ordering, equal default gap waits, FROM_OPEN in-window backfill,
timeout/capacity gap advance, strict pending first-wins, exact admission-floor
accept/reject boundaries, PARTIAL gap advance, reorder-capacity no-loss
progression, exact hole claim/expiration, owner FIFO `GapOpen`/`ChannelSeal`
fences, coalescing diagnostic gap mailbox behavior, catalog-miss continuity
tokens, nested snapshot lists, overlapping dynamic range rejection, configured
stream exclusion, and runtime tuple handling. Wire-level control tests
additionally cover Logon/Subscribe readiness accumulation, nonzero
return/status rejection, malformed and overlapping list rejection, API
timeout/discard classification, sticky first boundary behavior, PARTIAL
pre-ready discard, FROM_OPEN pre-ready rejection, and readiness timeout.
Raw-path tests additionally prove pre-recovery capture of retransmissions and
catalog misses, fail-closed tap behavior, fixed BLAKE3 provenance vectors,
preallocation accounting, ArrowStream-to-ClickHouse mapping, nested Snapshot
levels, Date partitioning, occurrence uniqueness, and explicit replay order.
Event tests cover journal-first same-batch ordering, cross-batch hole-filled Add
repair, unknown-reference removal, order-baseline/phase-anchor
compaction, no-gap fact eviction, raw ACK-before/after project and reject joins,
Shanghai END additions/tombstones and sliced capacity limits, private budgeted
repair, per-order generation restart, disjoint live publication during repair,
multi-owner routing, final partial-batch drain, immutable retry bodies,
current-table replacement/tombstones, recovery markers, and startup rejection
of a mismatched external schema. KLine tests cover SDK exchange-time windowing
despite opposing local receive order, invalid-time no-fallback behavior,
deterministic OHLC, multiple intervals, defensive duplicate/conflict checks,
ACK-before/after disposition joins, hole-fill historical revision, runtime
routing, immutable sink retries, owner-affine writer lanes, current-table
replacement, and schema validation.

Sanitizer, NUMA, and end-to-end throughput qualification must be rerun against
the replacement dispatch/retention implementation. The 2026-08-06 numbers in
[numa-stress-report.md](numa-stress-report.md) preserve evidence for the
superseded independent-recovery-queue architecture only; they are not evidence
for this implementation. Production acceptance still requires:

- real MDL SDK connection and shutdown tests;
- captured five-tuple replay, including jointly interleaved SZ 6.33/6.36;
- queue/capacity testing at measured burst rate;
- the same pinned NUMA test under production CPU/IRQ isolation with the
  unchanged 500-microsecond default gap policy;
- fault injection for malformed bodies, lane saturation, slow owners, SDK
  disconnects, and process termination;
- allocator-aware Event pages/slabs if an OS-visible RSS plateau is required;
- coordinated Event/KLine close frontiers and FactJournal directory/file
  reclamation;
- cold Event/KLine bootstrap and reconciliation, explicit derived-state session
  Finalize, and durable external allocation of revision epochs and calculation
  run IDs.
