# Event worker and ClickHouse revision contract

## 1. Scope and implemented boundary

The optional Event plane consumes the unified owner-local `TickDispatch` stream
that follows `SequenceRecovery`:

```text
decoder lane
  -> per-lane disposition outbox (LSN)
       -> independent Event / KLine / Arrow cursors
            -> GapOpen / ChannelSeal -> owner fence / retention request
            -> REJECT_LATE_FACT -> discard; do not journal
            -> PROJECT_* -> computation micro-batch -> journal first-wins fact
                                            -> append/minimal-closure repair
                                            -> logical recovery commit
                                            -> owner persistence group
                                            -> lane FIFO -> physical revision INSERTs
                                                         -> independent marker rows
```

Every disposition and channel control for one decoder-lane-to-owner edge uses
the same FIFO and carries a strictly increasing `dispatch_fence`. A `GapOpen`
therefore reaches an owner before that producer's first fact beyond the gap,
and an accepted fill reaches it before a `ChannelSeal` that releases the filled
position.

`GapOpen` is always applied individually. The current FactJournal persists
facts, not generation-specific gap ranges, so there is no durable source from
which a coalesced-away `GapOpen` could be reconstructed. `ChannelSeal` is only
an eviction watermark and may occupy an owner-local per-channel mailbox. A
later seal can replace the entry only when feed epoch and generation match and
its dispatch fence is newer; the entry retains the maximum `evict_before` and
the newest fence. A projected fact/hole fill or `GapOpen` for that channel
drains the pending seal first. Thus coalescing never crosses a state-changing
same-channel dispatch, and a generation/fence mismatch remains fatal.

While an owner has a `PendingProjectionCut`, `CanPollDispatch(owner)` is false
and the daemon checks it before every producer-FIFO poll. A `GapOpen` or
`ChannelSeal` may already have been popped when flushing the preceding
micro-batch creates that cut; the runtime retains exactly that one control in a
fixed `optional<TickDispatch>` slot and applies it only after the cut clears.
All later dispatches remain in the producer FIFO, so the deferred control keeps
its original `dispatch_fence` position. After application, a seal may move to
the bounded mailbox described above. A second deferred control, a mailbox
capacity breach, or a direct `AppendDispatch` while projection-fenced is a
fail-closed protocol error.

A control still closes a nonempty active computation cut when ordering requires
it. When active is empty, `GapOpen` is applied and `ChannelSeal` is staged
directly without running a control-only worker service cycle. Neither path
closes owner persistence. Immutable logical recovery commits remain in the
owner FIFO and are submitted only when the independent
persistence rows, bytes, logical-batch count, pending-commit capacity, or age
bound closes a durable prefix. This is the boundary that prevents control
broadcasts from becoming one HTTP transaction per empty or tiny computation
cut.

The worker computes against the shared local disk-backed canonical journal
and owner-local projection indexes and submits revision batches without
waiting for a raw ClickHouse ACK. SequenceRecovery output is appended to a
process-lifetime per-lane disposition outbox; Event, KLine, and Arrow each
have independent cursors. A rejected occurrence never enters the worker or
FactJournal. The outbox, runtime, and Event queues are volatile; none is a
crash-restart boundary. The local FactJournal is also not a restart
boundary; its exact capacity and recovery contract are in
[fact-journal.md](fact-journal.md).

This implementation is intraday and process-local. It does not bootstrap
Event state from `raw_tick` after a restart, expose a general end-of-day
`Finalize` API, or implement a cold Channel-suffix replay fallback. Shanghai
status `phase=Ended` is implemented as an intraday broadcast barrier. These
distinctions are part of the correctness boundary, not future behavior that
callers may assume today.

## 2. Admission, holes, and retention frontiers

The business position is the upstream native sequence within one market and
Channel:

```text
Shanghai: (trade_date, market, channel, BizIndex)
Shenzhen: (trade_date, market, channel, ApplSeqNum)
```

`SequenceRecovery` maintains one native sequence domain per `(market,
channel, feed_session_epoch)`. There is no claimed exchange total order across
Channels. Shenzhen 6.33 orders and 6.36 transactions share the configured
Channel sequence domain; the vendor-domain caveat and required production
replay check remain documented in
[mdl-ingestd-design.md](mdl-ingestd-design.md).

For one channel, define:

```text
E = next sequence expected by SequenceRecovery
W = maximum_reorder_span
A = max(process_origin, E - W)
R = first sequence in the earliest open-hole interval, or E when none exists
```

`SequenceRecovery` captures `E` and `A` when the occurrence first arrives. If a
future record waits in the reorder table, those captured values remain attached
when it is eventually emitted; neither runtime nor worker reclassifies it using
a later frontier.

```text
sequence >= E     -> ordered/reorder path
A <= sequence < E -> accepted only by atomically claiming an exact open hole
sequence < A      -> rejected permanently
```

The lower boundary is exact: `sequence == A` may fill an open hole, while
`sequence < A` cannot enter Event. A behind-frontier sequence absent from the
hole ledger is also rejected, regardless of whether its payload equals the
winner. Claiming removes exactly that position from the interval ledger before
publication. The fill dispatch remains ahead of any resulting seal on the same
owner FIFO, so owner queueing cannot turn an accepted fill into an expired one.

`E`, `A`, and `R` express different facts. `E` can jump over a committed gap and
is not a completeness proof. `A` closes admission for old positions. `R` is the
upstream retention request: with no open hole it follows `E`; with holes it pins
the suffix at the earliest still-open position. Expired holes are removed from
the ledger and cannot be reopened. A channel that ever skipped a gap retains
incomplete-history quality even after all repairable holes close.

Each owner-local worker reports related, but deliberately distinct, state:

- `journal_tail`: maximum native position accepted by this Event owner for the
  channel;
- `projected_frontier`: maximum inserted position committed to live Event
  caches;
- `expected_sequence` and `admission_floor`: monotone observations carried by
  classified occurrences;
- `sealed_before`: prefix whose Event fact state this owner has actually
  compacted and erased;
- `eviction_target`: latest requested retention frontier;
- `applied_dispatch_fence`: last control/occurrence fence applied for the
  channel.

These values are maxima or local progress, not interchangeable completeness
proofs. A micro-batch is only a throughput and journal-first calculation cut.
`projected_frontier` may advance for a disjoint live component while older
repair remains private, and `sealed_before` advances only after sliced eviction
finishes.

## 3. Stable identities

The code uses the following logical keys:

```text
FactKey =
    (trade_date, market, channel, native_sequence)

OrderKey =
    (trade_date, market, instrument_id, channel, order_id)

EventKey =
    (trade_date, market, instrument_id, channel, native_sequence,
     event_kind, affected_order_id, occurrence)
```

Here `instrument` means the exact catalog-resolved security identity
`(market, security_id_source, security_id)`.  The catalog supplies a stable
`instrument_id` for Event/ClickHouse identity and a dense
`instrument_ordinal` for runtime owner routing.  The ordinal is a daily
catalog-local routing value; it is not persisted as the Event identity and it
must not be used as a restart/WAL key.

`FactKey` intentionally omits the instrument and feed epoch. One worker instance
is bound to exactly one positive `feed_session_epoch`, and the upstream
first-wins key is:

```text
(trade_date, market, channel, feed_session_epoch, native_sequence)
```

The first canonical occurrence at that key is authoritative. Every later
occurrence at the same native position is classified `kRejectLateFact` without
payload comparison and cannot replace the winner. Its raw occurrence remains
independently available in `raw_tick` if that write succeeded. The Event
FactJournal retains its own equal/conflict checks as fail-closed invariant
validation, but those checks are not a second winner-selection policy and a
rejected occurrence never reaches them.

Source events use `affected_order_id=0`; order revision events use the stable
order ID. `occurrence` is a deterministic collision field and is not an
arrival row number, batch ordinal, ClickHouse version, or calculation-run
identifier. Under the currently accepted Add/Trade/Cancel shapes, one fact
produces at most one revision fragment for a given order and occurrence is
normally zero.

## 4. Owner-local indexes and state

One owner thread mutates a worker. The state remains owner-local and is never
mutated by a background calculation thread. Hot point lookups use bounded hash
tables; indexes that require native-sequence range traversal retain ordered
array/deque/tree representations:

| Logical index | Current representation and purpose |
|---|---|
| Canonical FactJournal | one process-wide disk file retains the canonical winner; its paged process-wide directory owns independent Event/KLine first-seen state and is not reclaimed by Event seals |
| Event fact metadata | owner-local `facts_`, keyed by `FactKey`, retains the journal handle, semantic hash, instrument/phase overlay, deque of roles, and hole-fill/barrier flags, but not a full source Tick |
| Channel/instrument fact indexes | fixed-directory lazy-paged outer hashes avoid rehash; ordered inner native-sequence containers locate the fact prefix to evict and support Shanghai phase range scans; directory/page/node and inner growth share the hot-fact byte cap |
| Phase/barrier index | fixed-directory lazy-paged outer hashes avoid rehash; phase transitions retain an ordered suffix plus the last compacted phase anchor, while retained `Ended` barriers use an ordered inner map; all outer and inner growth shares the hot-fact byte cap |
| Order history | fixed-directory `LazyPagedHashMap<OrderKey, OrderHistory>`; bucket-head pages and stable nodes allocate lazily without whole-table rehash, and each value contains one full `OrderBaseline` plus a native-sequence-keyed `map<OrderUseNode>` suffix; count and conservative owned bytes have separate hard caps |
| Order range indexes | `map<InstrumentChannelKey, ExactList<OrderKey>>` covers all carry orders and the active-END subset; exact arrays charge the complete new allocation before growth and release their old allocation only after commit |
| Active END index | the active-order ExactLists exclude orders whose finalization has already emitted |
| BundleCache | complete current EventKey-to-payload set for each retained FactKey |
| EventHead | latest revision ID, payload hash, and tombstone state for each retained EventKey |
| Pending commit FIFO | immutable revision batches and conservative owned-byte accounting |

The RoleEval cache key does not store a separate role byte because validated
input forbids one order ID from occupying two roles in the same fact. Ambiguous
Shenzhen trades whose positive buy and sell references are equal mutate
neither order role and carry an explicit ambiguous-reference quality flag.

For an ordered batch containing only projectable Shanghai status facts while
the owner has no order histories, the worker skips order-use and phase-repair
walks.  It still journals every fact, builds the same source bundle, allocates
the same monotonically increasing Event versions.  This is a
semantics-preserving source-only fast path, not a shortcut
for Add/Trade/Cancel or hole-fill repair batches. `ordered_batch_fast_path`,
`unordered_batch_sorts`, `source_only_fast_path`, and
`barrier_index_orders_visited` are exported so a benchmark can verify which
path was actually measured.

Shanghai semantic state includes the published snapshot plus hidden
`pre_add_active_trade_quantity`, minimum and maximum execution prices,
execution-price presence, terminal state, finalize-emitted state, and
revision-emitted state. Shenzhen state includes terminal,
finalize-emitted, and revision-emitted state. Convergence compares the full
state object. It never compares ClickHouse version, revision ID, writer ID,
batch provenance, or calculation-run identity.

### Gap-driven retention and order baselines

Every projectable dispatch carries the current upstream `retention_floor`; a
`ChannelSeal` carries the same information when closing or expiring a hole
makes the frontier advance without another ordinary fact. Because controls and
facts share the producer-to-owner FIFO, the worker can queue eviction without
inferring channel completeness from its sparse instrument-owned subsequence.

For each retained fact below `evict_before`, sliced eviction first compacts all
of that fact's evaluated order uses. The leading use of an order suffix becomes
the new baseline:

```text
OrderBaseline =
    compacted_before
    last_sequence
    optional<full private OrderState>
    input_set_hash
```

The baseline is not a public `OrderSnapshot`. It preserves hidden Shanghai
pre-add quantity, execution bounds, terminal/finalization/revision flags, and
the corresponding Shenzhen hidden flags, because those fields affect future
transitions. Once all roles are compacted, eviction removes the fact metadata,
instrument/channel index entry, Bundle, Event heads/tombstones, and any old
barrier. An evicted phase status becomes the cutoff anchor, so the last phase
before the retained suffix remains available.

No-gap traffic requests a frontier immediately after the projected fact, so
fact/bundle/head history normally collapses after projection rather than
retaining a fixed `W`-sized payload window. With an open hole at `R`, Event
retains the conservative `[R,E)` suffix until the fill is repaired or the hole
expires. An active private repair pauses eviction; completed work resumes in
configured node/byte slices. A carry order whose baseline contains state is not
retired merely because its old uses were compacted: a future higher-sequence
Trade/Cancel can still refer to it. `maximum_carry_orders` is therefore a
separate hard bound, while `maximum_order_history_bytes` bounds conservative
owned bytes for baselines and retained suffix nodes.

An immutable pending revision batch owns the complete output rows needed after
projection, so process-lifetime operation does not pin old FactRecords until a
ClickHouse ACK. This is not a durable restart guarantee: the implementation has
no checkpoint/bootstrap boundary and a crash can lose both pending output and
compacted in-memory state.

The public `CopyBundle`, `CopyOrder`, and `CopyChannelState` methods are
owner-local inspection helpers. They must not be called concurrently with
that owner's mutation methods. Runtime health, fatal error, and statistics
sampling are separately made concurrency-safe for the supervisor thread.

## 5. Projection functions

The projection is split along the state dependency boundary:

```text
ProjectSource(fact)
    -> state-independent source event fragment

ApplyOrderRole(previous_state, fact, primary|buy|sell|barrier)
    -> next full state
    -> optional order revision fragment
    -> source quality/reference contribution

AssembleBundle(fact, role evaluations)
    -> deterministic EventKey-to-payload bundle
```

A trade can reference two orders, but each state transition is evaluated
independently. Recomputing a buy role therefore does not replay the sell
state chain. Bundle assembly reuses the cached sell fragment and combines both
roles only at the source fact.

The operation embedded in an order payload is derived from hidden semantic
state: the first emitted order fragment is `Insert`, a first terminal fragment
is `Finalize`, and another fragment is `Update`. There is no continuously
incrementing business revision number in the payload. ClickHouse replacement
version and Event identity are deliberately separate.

Shanghai phase for non-status facts is normalized from the latest status at
or before that fact in the same Instrument/Channel. A newly inserted earlier
status can consequently make every affected fact up to the next status dirty.
This is a real business dependency, not a Channel-suffix fallback.

### Sliced Shanghai phase normalization

After journal admission and owner-index insertion, a non-empty journal cut is
owned by one `PendingProjectionCut`. It retains the inserted FactKeys, their raw
dependencies, requested retention frontiers, exact phase scan spans, dirty
sets, and discovery cursors until the cut is finalized. For an inserted valid
status, its span starts after that status and stops before the next status in
the same Instrument/Channel. An inserted non-status Shanghai fact contributes
its own position. Overlapping spans are sorted and coalesced before scanning.

The cut advances on the owner thread through these stages:

```text
kNormalize
  -> kRegisterInserted
  -> kSeedOrders
  -> kSeedEndOrders
  -> kSeedBundles
  -> kExpandComponents
  -> kFinalize
```

`kNormalize` visits retained facts by span cursor and recomputes the projected
phase. Before changing a retained fact's phase-validity overlay or semantic
hash, it reserves the conservative dirty-Bundle, dirty-order, and dirty-use
growth; exhaustion therefore fails closed before that mutation.
`kRegisterInserted` records direct order uses and END work. The seed stages
identify late/overlapping order, END, and Bundle roots. `kExpandComponents`
then walks the bidirectional Fact-to-Order dependency graph with explicit
fact/order frontiers, one role or dirty sequence at a time, rather than
rescanning the accumulated sets to a fixed point. `kFinalize` partitions the
resulting closure into disjoint live work and repair work, publishes/merges it,
transfers the retained raw dependencies, and only then queues the retention
requests. There is no synchronous phase-closure compatibility path.

Each advance is budgeted by `phase_slice_max_nodes`,
`phase_slice_max_bytes`, and `phase_slice_max_cpu_ns`; zero is invalid for each
setting. The node, conservative work-byte, and steady-clock elapsed checks run
between processed units, so a unit that has started is completed before the
owner yields. `phase_normalization_slices`, `phase_facts_scanned`, and
`phase_dirty_roles_discovered` expose progress.

The pending cut's conservative owned bytes and the existing
`RepairTransaction` bytes share the single per-owner `maximum_repair_bytes`
cap. `pending_phase_bytes` and its high-water mark report the cut portion; they
are not an additional allowance. While the cut exists, later owner dispatch is
fenced, the older active repair does not advance, and eviction does not run.
Later outbox records for this owner stay unread until the cut clears.

## 6. Journal-first micro-batches and the live path

For each owner cut, `ApplyBatch` performs these operations:

1. Validate epoch, owner, arrival-time admission token, generation, and strictly
   increasing dispatch fence.
2. Capture the current owner-local projected maxima for touched channels.
3. Journal every accepted `FactKey` before registering or applying any order
   role. FactJournal duplicate/conflict results retain the existing winner and
   are treated as invariant/continuity outcomes; rejected occurrences never
   enter this batch.
4. Build exact Shanghai phase spans, transfer the journal cut's raw
   dependencies and retention requests into a `PendingProjectionCut`, charge
   its initial workspace, and install the owner input fence.
5. Advance normalization, inserted-use registration, END/order/Bundle seeding,
   and bounded Fact-to-Order component discovery under the phase slice budgets.
   `ApplyBatch` may return with this cut still pending.
6. At cut finalization, split the discovered graph into repair-connected and
   disjoint live components. A trade connects its two roles for Bundle
   publication even though their state transitions remain independent.
7. Apply disjoint live components and merge dirty components into the existing
   owner repair transaction; raw dependencies follow the component that owns
   their inserted fact.
8. After the cut clears, advance configured order-repair slices, queue the
   carried retention frontiers, and run bounded eviction only when neither the
   pending cut nor repair pins state.

Within a batch this makes `Trade(102), Add(101)` project from the journaled
order `101,102`, without publishing an intermediate unknown-reference Event.
If the two facts arrive in different batches, Trade 102 can first publish its
currently correct unknown-reference payload; Add 101 then enters repair and
revises only the affected order chain and bundles.

Normal projection touches zero, one, or two direct order roles, plus any real
Shanghai barrier roles. Already ordered cuts avoid sorting; unordered/hole-fill
cuts sort by stable key. The current implementation mixes a lazy-paged hash,
ordinary unordered maps, `std::map`, ExactLists, vectors, and deques, so it does
not claim container-level O(1) execution.

## 7. Minimal-closure repair

An owner has at most one active `RepairTransaction`, containing one task per
dirty `OrderKey`. New hole fills merge into that transaction. A newly inserted
earlier use moves the task's effective start to the earliest dirty sequence; a
new later use extends its worklist. Neither operation restarts unrelated orders
or the whole Instrument/Channel.

Each task captures the live `OrderHistory::generation`, reads the retained
suffix in place, and starts from `LatestStateBefore(earliest_dirty_sequence)`.
It never clones or mutates that suffix while sliced repair is running. For each
later use it computes a role evaluation into a private, attempt-generation
tagged patch, compares the observable fragment, and compares the full
post-state:

- a changed fragment marks only that FactKey bundle dirty;
- a changed full state advances to the next use of the same order;
- an equal full state stops that propagation chain after the current use,
  unless another explicitly dirty use remains later in the task;
- input/provenance hash changes update the cache patch but do not by themselves
  extend the semantic state closure.

New facts and new order uses are registered in the owner journal/index so
subsequent input can discover them. Role evaluations, bundles, Event heads, and
versions remain private until the transaction commits. If a relevant order
gains another use while repair runs, only that order task is reset. Reinitializing
the task advances its attempt generation in constant work; old patch nodes stay
accounted but are ignored by repair calculation, Bundle assembly, and commit.
After every task is complete and its captured live generation is stable, commit
applies only the current-attempt evaluations to the live nodes and synchronizes
the Shanghai active-END index. Fact business payloads themselves are immutable
after first-key admission. Shanghai phase dirty roles arrive from the finalized
`PendingProjectionCut`; its bounded graph discovery determines the affected
closure before that work is merged into this transaction.

One slice processes at least one order-use node and then returns when either
`repair_slice_max_order_uses` or `repair_slice_max_cpu_ns` is reached. The owner
loop services normal traffic between ordinary repair slices. A pending phase
cut has higher priority: it fences that traffic and pauses active-repair
advancement until cut finalization. A sliced Shanghai END also fences input only
until its fixed candidate prefix has been attached; ordinary order repair then
resumes without the fence. If the pending raw-commit FIFO is full after
calculation, the finished repair waits without leaking its private state.

### Shanghai END

An accepted Shanghai status fact with `phase=Ended` is a broadcast barrier for
orders in the same Instrument/Channel whose finalization has not already
emitted. A hole-filled ordinary fact propagates only its affected order to that
barrier. Bundle assembly reuses all other cached finalize fragments.

- A newly recovered order that existed before END adds its finalize Event.
- An order made terminal before END removes the old finalize Event with a
  tombstone.
- An earlier recovered END necessarily attaches a barrier use to every order
  in its Instrument/Channel and is therefore a real broadcast repair.

An order first discovered later but with a use before an existing END is
attached to that stored barrier when its OrderHistory is created. This keeps
hole-filled order creation consistent without replaying unrelated Channel
facts.

END expansion is preflighted against independent candidate-count,
projected-row, and conservative staging-byte caps. Candidate attachment then
runs on the owner thread in slices bounded by count and CPU time. The fact's
role list is a deque, so appending a large END fanout does not trigger a single
whole-vector relocation. Exceeding any END cap fails the worker closed before
unbounded staging is admitted.

## 8. Disposition outbox and derived persistence

The raw sink still captures canonical facts before `SequenceRecovery`. Its
ClickHouse ACK is no longer a derived persist gate and no longer calls Event
or KLine.

After classification, each decoder lane appends one sequenced `TickDispatch`
to a process-lifetime outbox and stamps `dispatch_fence = outbox_lsn`.
Controls occupy one LSN and are broadcast to every owner cursor. Event, KLine,
and Arrow consume with independent cursors, so one plane can lag without
stopping the others. Outbox exhaustion is `FATAL_CONTINUITY`. Derived
ClickHouse or runtime failure does not stop raw ingest; the process publishes
`DERIVED_CATCHUP` or `RAW_ONLY_STALE` and `event_authoritative=0`.

`REJECT_LATE_FACT` is counted and discarded. It never journals, repairs, or
enqueues a revision.

An Event projection commit updates live in-memory caches and then waits only
on its own persist-group bounds before `AppendRevisionGroup`. FIFO submission
still prevents a later logical batch from passing an earlier one. A sink
rejection is sticky and fails the Event runtime closed. Acceptance means only
that the bounded volatile sink queue retained the immutable objects. Derived
durability still requires successful ClickHouse responses for all physical
revision requests followed by the marker request that contains one marker row
per logical batch. Pending commit count and pending immutable-revision bytes
remain bounded; exhausting either bound stops the Event plane, not raw.

## 9. Revision and version semantics

Bundle diff is performed by stable `EventKey`:

| Old row | New row | Revision |
|---|---|---|
| absent | present | `INSERT` |
| present, payload differs | present | `UPDATE` |
| present | absent | `TOMBSTONE`, `is_deleted=true` |
| payload equal | payload equal | no row |

A tombstone keeps the prior payload for audit but uses the same logical
EventKey. Every noninitial revision records the preceding revision ID in
`supersedes_revision_id`.

The worker version layout is:

```text
version = (revision_epoch << 32) | owner_local_revision_counter
```

The counter starts at one and is monotone within one worker process. Versions
need not be globally contiguous across owners; each EventKey has exactly one
owner. A supervisor must durably allocate a strictly increasing nonzero
`revision_epoch` for every process/state rebuild that can revise the same key
space. The executable validates but does not persist or allocate this epoch.
It must also receive a unique nonzero `calculation_run_id`; reusing a run ID
after restart can collide with deterministic recovery-run identities.

Each nonempty calculation batch receives a deterministic `recovery_run_id`
from `(calculation_run_id, owner, owner_batch_sequence)`. Revision IDs are
content-derived from the calculation run, recovery run, version, payload hash,
and operation. These IDs and the batch object are fixed before sink admission.

## 10. ClickHouse sink and query semantics

The Event sink writes three data objects:

- `event_revision_log`: append-only revision history, ordered by EventKey plus
  version;
- `event`: a `ReplacingMergeTree(version)` current table populated by a
  materialized view from the revision log;
- `event_recovery_run`: one independent committed marker per logical recovery
  commit, written only after all revision rows in its physical group have
  received successful ClickHouse responses.

The sink consumes one owner submission group at a time but may split it across
several physical groups. Within a lane, each normal physical group is a
consecutive logical-batch prefix bounded by revision-request rows/bytes,
marker-request rows/bytes, logical-batch count, and maximum wait. A single
oversized logical head batch is isolated and split across multiple revision
requests rather than rejected merely for exceeding one request.

Before the first send, the physical group freezes its ordered logical-batch
list, all RowBinary revision bodies, the grouped marker body, query IDs, and
deduplication tokens. Every unknown-outcome retry reuses those exact bytes and
identities. Only after every revision request succeeds does the writer send the
marker body containing one row and commit ID for each logical recovery run.
Only after that marker request succeeds are all logical batches acknowledged
and released. A permanent intermediate failure releases none of them. Local
nonreplicated tables use ClickHouse's configured deduplication window;
replicated DDL relies on the replicated engine deduplication mechanism.

At startup, local mode can create the four table/view objects. Both local and
externally managed modes then validate table engines, Date partition keys,
sorting keys, current-table replacement version, materialized-view
source/target, all 27 revision/current column types, and all 15 recovery-marker
column types before accepting a batch.

`ReplacingMergeTree` replacement is asynchronous unless the query uses
`FINAL`. A correctness-oriented current query must replace before filtering
tombstones:

```sql
SELECT *
FROM l2flow.event FINAL
WHERE NOT is_deleted;
```

This current table is row-wise eventually consistent. A repair split into
several ClickHouse chunks can expose a temporary mix of old and new EventKeys;
`ReplacingMergeTree` does not make the whole recovery run atomic.

For an as-of view that excludes uncommitted partial runs, select revision-log
rows whose calculation/recovery pair has a committed marker, then choose the
highest version per EventKey before removing tombstones. One direct form is:

```sql
SELECT *
FROM
(
    SELECT r.*
    FROM l2flow.event_revision_log AS r
    INNER JOIN
    (
        SELECT calculation_run_id, recovery_run_id
        FROM l2flow.event_recovery_run
        WHERE committed
        GROUP BY calculation_run_id, recovery_run_id
    ) AS c USING (calculation_run_id, recovery_run_id)
    ORDER BY version DESC
    LIMIT 1 BY
        trade_date, market, instrument_id, channel, native_sequence,
        event_kind, affected_order_id, occurrence
)
WHERE NOT is_deleted;
```

Production queries should also constrain `trade_date` and any desired
calculation/logic-version policy. The marker certifies that this sink received
success for every chunk in that run; it does not turn unrelated recovery runs
into one transaction.

### Ordered writer lanes

`EventClickHouseConfig::writer_lanes` accepts exactly `1`, `2`, `4`, or `8`.
Each lane has one HTTP client, one writer thread, and one bounded volatile
queue. An owner persistence group is routed by `owner % writer_lanes`;
therefore all logical commits from one Event owner remain FIFO even when a
submission is consumed by multiple physical groups. Different owners may
execute ClickHouse INSERTs concurrently. There is deliberately no cross-lane
total order because the Event contract has no cross-Channel exchange order.

`queue_revision_batches` and `queue_revision_rows` are global budgets shared by
all lanes. They are not multiplied by the lane count. Queue exhaustion,
permanent INSERT failure, or a retry-budget expiry changes the whole sink to
fail-closed; another lane cannot silently continue with an incomplete Event
view. Query IDs include the lane, while RowBinary bodies and deduplication
tokens remain immutable across retries. The queues contain submission vectors
of `shared_ptr<const EventRevisionBatch>` objects in process memory; this
feature does not add a WAL, checkpoint, disk queue, or local staging file.
Statistics separately expose queued submission groups, logical batches, rows,
their high-water marks, physical revision/marker request counts and latency,
`facts_per_micro_batch`, `revisions_per_sink_batch`, and the calculated
`sink_batches_per_second`. Engine statistics separately expose source channel
controls and their owner-delivery fanout; runtime statistics expose forced and
empty control flushes plus applied/coalesced/pending seals.

## 11. Capacity and failure policy

The shared journal has one global physical-record bound for the trading date.
Event carry-order count, conservative order-history owned bytes, hot fact count,
conservative hot-fact owned bytes, cached Event rows, combined active-repair and
pending-phase-cut bytes, pending commit count, pending immutable revision
bytes, Shanghai END candidates/rows/staging bytes, and Event sink batch/row
queues have configured hard
bounds. The pending phase cut and active repair specifically share
`maximum_repair_bytes`; they do not each receive that allowance. Exhaustion
fails the relevant journal/worker/runtime/sink closed rather than dropping a
revision and continuing with an unprovable current view.

A source-key conflict reported by FactJournal is a defensive invariant outcome,
not an alternate live admission path. The worker retains the existing winner,
increments the source-conflict statistic, and never replaces it. Under the
normal runtime contract, SequenceRecovery has already classified every later
body at that native position as rejected, so such a conflict indicates direct
worker misuse or state divergence and must be investigated against `raw_tick`.

## 12. Memory bound, complexity, and remaining gates

At the SequenceRecovery boundary, the logical repairable native-sequence span
obeys:

```text
hot sequence span <= sum_channels(E - R) + accepted in-flight + eviction lag
                  <= sum_channels(W) + accepted in-flight + eviction lag
```

The Event worker enforces count and conservative byte caps over retained fact,
Bundle, and head nodes and the channel/instrument/phase/barrier indexes. It uses
a separate byte cap for carry-order baselines and order-use suffixes. Its
complete logical capacity also includes private repair, END staging, pending
revisions, ACK join, micro-batches, and sink queues. A slow repair or eviction
backlog never expands the admission window; it eventually reaches a configured
cap and fails closed. `order_history_bytes` and `hot_fact_bytes` export the two
live estimates and their high-water marks.

In configured-cap terms, the owner-local portion is bounded by:

```text
M_event_owner_logical <= maximum_hot_fact_bytes
                       + maximum_order_history_bytes
                       + maximum_repair_bytes
                       + maximum_end_staging_bytes
                       + maximum_pending_revision_bytes
                       + bounded ACK join/inbox/index storage
                       + bounded micro-batch and eviction-task storage
```

Sink queues and the process-wide FactJournal are additional independently
bounded components. Count caps remain necessary alongside byte caps to bound
hash/tree metadata and adversarial collections of small logical objects.

This is not an allocator-level RSS guarantee. `facts_`, `bundle_cache_`, and
`event_heads_` remain standard node containers; erasing their nodes does not
shrink unordered-map bucket arrays. The four auxiliary outer indexes and the
carry-order table can delete an empty lazy bucket-head page, and an empty
ExactList can release its array, but the general allocator may retain those
allocations. The implementation has no per-channel hot-fact `ChannelPage`/slab
allocator and does not call `madvise`/`unmap` to return detached Event pages.
Consequently `hot_facts`/`hot_fact_bytes` describe logical owned state, not an
exact `RssAnon` bound.

The process-wide FactJournal file/directory is also not reclaimed by Event
`ChannelSeal`, and KLine has its own bar/head lifetime. A process-wide RSS
plateau would require separate Event and KLine close frontiers plus journal
reclamation at their minimum; that integration is not implemented.

Let `U` be the number of actually visited uses on dirty order chains and `E`
the number of changed Event rows. With the current lazy-paged hash,
unordered-map, map, ExactList, vector, and deque indexes:

```text
normal projection:
    O(optional batch sorting + expected-constant hash lookups
      + involved order roles)

hole-fill lookup and repair:
    O(log N + U + dirty bundle assembly/diff)

ClickHouse output:
    O(E), excluding engine merge work
```

The ordinary hole-fill path does not replay an unrelated Channel suffix. A
Channel-suffix fallback for damaged/missing indexes or logic-version migration
is not implemented.

The phase slice is intentionally not a universal per-call latency bound. Three
remaining implementation limits are explicit:

- a cold `FactJournal::Read` performs one synchronous `pread`; a slice cannot
  preempt that read after the current work unit starts;
- late middle insertion into the `InstrumentFactIndex` and `PhaseIndex`
  `std::deque` containers remains `O(W)` in the retained suffix length, plus
  admitted in-flight/eviction lag;
- `CommitProjection` remains a one-shot transaction for both a live cut commit
  and a later repair commit, so final Bundle diff/revision assembly and commit
  are not sliced by the phase node/byte/CPU budgets.

Additional production gates remain:

- cold bootstrap/reconciliation from durable raw facts after process restart;
- a public explicit session/day `Finalize` barrier and tests for it;
- durable supervisor allocation of revision epochs and calculation-run IDs;
- ClickHouse writer-lane (1/2/4/8) measurements against the target node or
  cluster, using the observed revision amplification rather than assuming one
  revision row per callback;
- a decision on ClickHouse sharding only after those measurements.  This
  branch intentionally does not add WAL, disk queues, checkpoints, or restart
  bootstrap from local files.  A ClickHouse outage that exceeds the bounded
  in-memory queues therefore remains a fail-closed continuity boundary.
- captured-market equivalence tests against the reference Shanghai and
  Shenzhen projectors;
- measured peak-rate sizing and recovery-slice latency on the deployment NUMA
  topology;
- allocator/page reclamation if an OS-visible RSS plateau is a requirement;
- coordinated Event/KLine journal close frontiers and FactJournal directory/file
  reclamation;
- query-layer policy for selecting committed calculation/logic versions.
