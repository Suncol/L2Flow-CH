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
change the recovery contract below. The repository does not contain CSV/WAL
replay, checkpoint restore, reconnect epoch inference, or intraday restart
recovery. Startup is exactly one of:

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
  waits both default to 500 microseconds but remain separately configurable.
  These values are tuning defaults, not protocol constants or statements
  about upstream backfill latency.
- Instrument/fault/LateRecovery dispatch overload and admission-lane
  exhaustion are fatal because this milestone has no durable catch-up source.
  No existing record is overwritten. Gap notification is the exception: it
  uses fixed per-Channel aggregate state plus a dirty bitmap, not a bounded
  event queue.
- An exception during a vendor callback operation is caught at the ABI
  adapter, recorded as a sticky failure, and causes process shutdown; no C++
  exception is allowed to escape into the SDK.
- The process does not equate an immediate error-free `Connect()` return with
  feed readiness. It requires a successful Logon response and successful
  status for every configured tuple. Bounds-invalid control lists, rejected
  statuses, pre-ready market data, and the local readiness deadline terminate
  the process/feed epoch.
- API service-timeout and message-discard events also terminate the current
  process/feed epoch. Their message names and IDs are SDK facts; choosing a
  fatal boundary is conservative implementation policy, not a claim that the
  SDK protocol requires this exact response.
- For `mdl_ingestd`, the configured stream file is the source of physical
  subscriptions. There is no special-case protection for any unselected
  tuple.
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
and stable instrument ID. A tick absent from the catalog is still inserted as
an output-free continuity token, so filtering cannot manufacture a native
gap. An unknown snapshot is counted and not dispatched.

## 5. Channel recovery

### FROM_OPEN

Each newly observed channel begins with `expected = 1`.

- `sequence == expected`: publish, advance, and drain contiguous pending
  records;
- future sequence: retain it during `from_open_gap_wait_ns`, allowing an
  out-of-order network/upstream backfill to close the gap normally;
- canonical duplicate while pending: keep the first and count the duplicate;
- conflicting canonical projection at the same pending native position:
  freeze only that channel and emit `ChannelFault`;
- gap-wait expiry, reorder-span breach, or a full-table slot collision: record
  `ChannelGap`, advance to the smallest retained position, drain in order, and
  retry the current record without freezing or discarding retained bodies;
- a later record below the advanced frontier: publish its canonical body to
  `LateRecovery`, never insert it backward into the realtime stream;
- a full-decode failure with a valid native descriptor: freeze only that
  channel and emit a decode fault.

Other channels and snapshot lanes continue. FROM_OPEN history is complete
until the first committed gap; after that gap, all subsequent realtime records
for that Channel carry incomplete-history quality. Because this stage does not
retain unbounded committed payload history, `LateRecovery` identifies a body
as behind the frontier but cannot by itself prove whether it is a recovered
gap member or a retransmission.

### PARTIAL

PARTIAL uses the same bounded gap advance and LateRecovery behavior, but its
starting claim is different: it never claims the unobserved opening prefix.

1. During the configurable initial hold it retains arrivals and selects the
   smallest observed native sequence as the process-start origin.
2. Later future records wait in the bounded reorder table.
3. When `partial_gap_wait_ns` expires, a record exceeds the bounded span, or
   a modulo slot collides because the reorder table is full, the lane records
   `ChannelGap`, advances to the smallest present sequence, drains retained
   records in native order, and iteratively retries the current record.
4. A later record below the advanced frontier is not inserted backward into
   the realtime stream. Its full canonical tick is published to the bounded
   `LateRecovery` branch with the committed frontier and observed gap epoch.
   Such a body can be a recovered gap member or a retransmission; downstream
   durable reconciliation must decide which.
5. A conflicting pending record deterministically keeps the first projection,
   diverts the conflicting canonical body to `LateRecovery`, and continues.

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
allocate one placeholder apiece.

No fake event is synthesized for a missing native position because its
instrument and message kind are unknowable. Instead, the first subsequently
dispatched target record carries `gap_before_first/last`, `gap_epoch`, and
`kQualitySequenceGapBefore`.

Gap control delivery cannot overflow a queue. Every observed Channel owns one
fixed mailbox, and decoder lanes set its dirty bit after publishing the latest
exact `GapRange`. The mailbox also carries cumulative `gap_epoch` and
`cumulative_missing_sequences`. If a consumer is slow, multiple notifications
for the same Channel coalesce: the latest range remains exact and an epoch jump
reports that older notifications were coalesced. Recovering every older range
then requires the canonical record metadata and/or the separate raw durable
path; the mailbox never fabricates an enclosing range that could label present
sequences as missing.

PARTIAL clears `kTickChannelHistoryValid` from the first emitted record, not
only after the first observed gap, because the pre-start channel prefix is
unknown. `kQualityChannelHistoryIncomplete` instructs Event/book derivation to
publish null for any field that requires unavailable history. Source-carried
fields such as native sequence, time, raw price, quantity, and order references
retain their independent validity. A resource failure (allocation,
loss-sensitive output queue exhaustion, or invalid channel identity) is not
reclassified as a historical gap and can still fail the process.

`LateRecovery` is deliberately loss-sensitive and bounded. Queue exhaustion
fails the engine rather than silently discarding the canonical body. If that
body is nevertheless lost during a crash and no SDK replay or raw durable copy
exists, later historical repair is impossible; the current repository does
not yet implement the raw durable publisher.

## 6. Instrument dispatch

For `P` decoder lanes and `W` instrument owners, the dispatcher builds a
`P x W` SPSC matrix. Each edge has exactly one lane producer and one owner
consumer, avoiding an MPSC compare-and-swap hotspot. Owner assignment is
`instrument_ordinal % W` and is stable for the frozen daily catalog.

Ticks and snapshots use separate matrices so the much larger snapshot record
does not inflate every tick slot. Channel faults and canonical LateRecovery
bodies each use one SPSC queue per tick lane and one fair single-consumer
endpoint. Gap state uses the fixed per-lane/per-Channel mailbox and dirty
bitmap described above.

The next Event/KLine stage must give each owner index to exactly one consumer.
Calling one owner endpoint from multiple consumers violates the SPSC contract.

## 7. 64-core / 1-TiB deployment starting point

The defaults are a starting point, not a measured production guarantee:

- 12 tick decoder/recovery lanes;
- 4 snapshot decoder lanes;
- 16 instrument owners;
- one SDK I/O thread and serialized callback admission;
- bounded per-edge dispatch queues and lazily allocated per-observed-channel
  reorder slabs.

On a 64-core host, reserve cores for the OS/IRQs, the vendor SDK, and the future
Event/KLine/Kafka stages. Pin decoder lanes only after measuring the actual
NUMA topology; a flat logical CPU number does not identify socket locality.
Keep the SDK callback and its hottest decoder lanes in the same NUMA domain
when possible, or use an explicit interleave/binding policy outside this
portable core. Do not claim low latency from core count alone.

Large admission arenas and SPSC record storage are allocated without eager
value-initialization. Their real producer performs the first write, avoiding a
startup pass that would pre-fault roughly 500 MiB on the main thread under the
default capacities. This improves first-touch placement but does not replace
an explicit production NUMA policy.

Hot decoder counters are cache-line-isolated per lane and aggregated only when
`stats()` is sampled. Decoder lanes therefore do not serialize each message on
a shared global statistics cache line.

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
tick dispatch       = tick_lanes * owners * edge_capacity * sizeof(tick)
snapshot dispatch   = snapshot_lanes * owners * edge_capacity * sizeof(snapshot)
reorder             = observed_channels * reorder_entries * sizeof(pending tick)
late recovery       = tick_lanes * late_capacity * sizeof(late canonical tick)
gap state           = tick_lanes * max_channels * sizeof(gap mailbox slot)
```

A 1-TiB deployment has ample capacity, but oversized queues increase cold-page,
TLB, and cache cost. Increase them only from burst and latency measurements.

## 8. Lifecycle and failure boundaries

Startup order is catalog/stream config -> engine preallocation -> decoder
threads -> SDK manager/subscriber -> connect. `Connect()` may invoke callbacks
synchronously, so the engine is running before it is called. The physical
session is returned only after successful Logon plus confirmation of every
configured subscription. Market data is admitted only after that readiness
state. The readiness timeout defaults to 30 seconds and is configurable with
`--sdk-ready-timeout-seconds`.

The first connection/discard/readiness boundary is sticky. The handler stops
admission immediately, so SDK auto-reconnect callbacks cannot enter the old
feed epoch. Shutdown drains records admitted before the boundary, then the
Arrow control stream records the boundary and seals. A subsequent process must
use a strictly larger externally allocated epoch; no MDL resume from a
Kafka-acknowledged native sequence is assumed.

Shutdown order is fixed:

1. stop handler admission;
2. call `IOManager::Shutdown()` as the callback-quiescence boundary;
3. release Subscriber, then IOManager;
4. drain and join decoder lanes;
5. drain instrument/fault/LateRecovery queues and final dirty gap state;
6. destroy the engine and handler.

After SDK shutdown returns, the session waits for its in-flight callback count
to reach zero before releasing Subscriber and IOManager. If vendor shutdown
throws, the process terminates instead of continuing destruction with a
possibly live callback target. The successfully loaded vendor DSO is
intentionally retained for process lifetime.

## 9. Verification status and remaining gates

Automated tests cover all five fixed layouts, SH/SZ normalization, joint
6.33/6.36 ordering, equal default gap waits, FROM_OPEN in-window backfill,
timeout/capacity gap advance and conflict isolation, PARTIAL gap advance,
reorder-capacity no-loss progression, LateRecovery diversion, coalescing gap
mailbox behavior, catalog-miss continuity tokens, nested snapshot lists,
overlapping dynamic range rejection, configured stream exclusion, and runtime
tuple handling. Wire-level control tests additionally cover Logon/Subscribe
readiness accumulation, nonzero return/status rejection, malformed and
overlapping list rejection, API timeout/discard classification, sticky first
boundary behavior, pre-ready market rejection, and readiness timeout.

The implementation passes the strict warning build and ASan/UBSan tests in the
available environment. TSan builds successfully, but execution in the current
ptrace/container environment terminates before the test with an unsupported
memory-mapping error; this is an environment limitation, not a passing TSan
result.

The NUMA-pinned synthetic callback benchmark sustained 800k, 1.0M, and 1.2M
messages/s for five minutes in both ordered and locally reversed test cases,
subject to the exact test policy and limitations in
[numa-stress-report.md](numa-stress-report.md). This is evidence for the
implemented in-process pipeline, not for the physical vendor SDK, network,
every configured tuple, or a 1-TiB production host. Production acceptance
still requires:

- real MDL SDK connection and shutdown tests;
- captured five-tuple replay, including jointly interleaved SZ 6.33/6.36;
- queue/capacity testing at measured burst rate;
- the same pinned NUMA test under production CPU/IRQ isolation with the
  unchanged 500-microsecond default gap policy;
- fault injection for malformed bodies, lane saturation, slow owners, SDK
  disconnects, and process termination.
