# Event worker and ClickHouse revision contract

## 1. Scope and implemented boundary

The optional Event plane consumes the two canonical Tick outputs that follow
`SequenceRecovery`:

```text
ordered owner Tick -----------------------+
                                            v
LateRecovery Tick -> owner inbox -> micro-batch cut
                                            |
                                            v
                              journal all accepted facts
                                            |
                         +------------------+------------------+
                         |                                     |
                  append projection                  minimal-closure repair
                         |                                     |
                         +------------------+------------------+
                                            v
                              immutable EventRevisionBatch
                                            |
raw_tick ClickHouse ACK --------------------+ -> Event sink queue
                                                   |
                                                   v
                                      event_revision_log + current MV
                                                   |
                                                   v
                                      event_recovery_run commit marker
```

The worker computes against in-memory journaled facts before the raw ACK, but
it does not submit a revision batch to the Event sink until every raw
occurrence on which that projection cut depends has been acknowledged by the
raw ClickHouse sink. The raw and Event queues are volatile; neither is a WAL.

This implementation is intraday and process-local. It does not bootstrap
Event state from `raw_tick` after a restart, expose a general end-of-day
`Finalize` API, or implement a cold Channel-suffix replay fallback. Shanghai
status `phase=Ended` is implemented as an intraday broadcast barrier. These
distinctions are part of the correctness boundary, not future behavior that
callers may assume today.

## 2. Ordering and watermarks

The business position is the upstream native sequence within one market and
Channel:

```text
Shanghai: (trade_date, instrument_id, channel, BizIndex)
Shenzhen: (trade_date, instrument_id, channel, ApplSeqNum)
```

`SequenceRecovery` maintains one native sequence domain per `(market,
channel)`. There is no claimed exchange total order across Channels. Shenzhen
6.33 orders and 6.36 transactions share the configured Channel sequence
domain; the vendor-domain caveat and required production replay check remain
documented in [mdl-ingestd-design.md](mdl-ingestd-design.md).

The Event runtime observes two different upstream boundaries:

- An ordered Tick has already passed `SequenceRecovery`. The runtime records
  `native_sequence + 1` as a conservative committed lower bound for that
  particular observation. It is not a fresh read of a global Channel state.
- A `LateRecoveryTick` carries the explicit
  `committed_next_sequence` and `observed_gap_epoch` captured by
  `SequenceRecovery` when it diverted the body behind the committed branch.

Each owner-local worker reports three related values:

- `journal_tail`: maximum native position accepted into its FactJournal for
  that Channel;
- `projected_frontier`: maximum newly inserted position whose connected
  component has committed to the live Event caches;
- `committed_next_sequence`: maximum upstream lower bound observed by that
  owner.

These are maxima, not equivalent completeness proofs. In particular,
`projected_frontier` can advance for a disjoint live component while an older
dirty order component remains in a private repair transaction. The upstream
watermark proves only that the realtime ordered branch will not move backward;
historical repair can still arrive through `LateRecovery`.

A micro-batch is a throughput and journal-first calculation cut. It cannot
prove that a smaller native sequence will not arrive in a later batch. The
cross-batch contract is therefore ordered-stream watermark plus revision, not
sequence continuity inferred inside the Event worker.

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

`FactKey` intentionally omits the instrument. This relies on the upstream
contract that one positive native sequence identifies at most one canonical
business position in a `(trade_date, market, channel)` domain. The Event
worker retains the first business payload for a `FactKey`: an equal later
payload is a duplicate, while a different payload is reported as a source
conflict and does not replace the first projection. Both raw occurrences are
still independently available in `raw_tick` if their raw writes succeeded.

`LateRecoveryReason::kPendingCanonicalConflict` is stronger than an ordinary
behind-frontier observation: SequenceRecovery has already retained a
different pending canonical body at that position. The runtime therefore
queues only the conflicting occurrence's raw durability dependency and never
admits that body to FactJournal. This prevents the conflict branch from racing
the retained ordered fact and accidentally becoming authoritative.

Source events use `affected_order_id=0`; order revision events use the stable
order ID. `occurrence` is a deterministic collision field and is not an
arrival row number, batch ordinal, ClickHouse version, or calculation-run
identifier. Under the currently accepted Add/Trade/Cancel shapes, one fact
produces at most one revision fragment for a given order and occurrence is
normally zero.

## 4. Owner-local indexes and state

One owner thread mutates a worker.  The state remains owner-local and is never
mutated by a background calculation thread.  Hot point lookups use bounded
hash tables; indexes that require native-sequence range traversal retain an
ordered vector/tree representation:

| Logical index | Current representation and purpose |
|---|---|
| FactJournal | `facts_`, keyed by `FactKey` in a bounded hash table, retains the source Tick, normalized projection Tick, source fragment, hash, roles, and late/barrier flags |
| Instrument fact journal | hash lookup to an append-ordered `FactKey` vector; late inserts stay sorted for Shanghai phase range scans |
| Phase/barrier index | phase transitions use an ordered vector; `Ended` barriers retain an ordered map and a direct Instrument/Channel→orders index |
| OrderUseIndex | `OrderHistory::uses`, keyed by native sequence, records Add/Trade/Cancel/barrier references even when no order state existed |
| OrderVersionChain | `OrderHistory::versions`, stores the full post-state and semantic input hash after each use |
| RoleEvalCache | caches the order-role post-state, optional order fragment, source quality contribution, reference resolution, and hashes |
| BundleCache | stores the complete current EventKey-to-payload set for one FactKey |
| EventHead | stores the latest revision ID, payload hash, and tombstone state for each EventKey |
| Pending raw commit FIFO | holds immutable revision batches and their exact raw occurrence dependencies |
| Acknowledged raw index | bounded hash set for ACK-before-fact dependencies, with the same per-owner bound as the runtime ACK inbox |

The RoleEval cache key does not store a separate role byte because validated
input forbids one order ID from occupying two roles in the same fact. Ambiguous
Shenzhen trades whose positive buy and sell references are equal mutate
neither order role and carry an explicit ambiguous-reference quality flag.

For an ordered batch containing only projectable Shanghai status facts while
the owner has no order histories, the worker skips order-use and phase-repair
walks.  It still journals every fact, builds the same source bundle, allocates
the same monotonically increasing Event versions, and applies the same raw-ACK
gate.  This is a semantics-preserving source-only fast path, not a shortcut
for Add/Trade/Cancel or late-repair batches.  `ordered_batch_fast_path`,
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

## 6. Journal-first micro-batches and the live path

For each owner cut, `ApplyBatch` performs these operations:

1. Capture the current owner-local projected maxima for touched Channels.
2. Validate structural ownership and journal every new `FactKey` before
   registering or applying any order role.
3. Classify equal keys as duplicate or source conflict, retaining the first
   payload; an upstream pending-canonical conflict contributes only a no-op raw
   dependency and is not journaled.
4. Normalize Shanghai phases and register all direct order uses. Unknown order
   references are registered; they are not discarded.
5. Mark a new fact late when it came from `LateRecovery` or its position is
   behind the captured projected maximum.
6. Split the batch's Fact-to-Order graph into repair-connected and disjoint
   live components. A trade connects its two roles for bundle publication even
   though their state transitions remain independent.
7. Apply disjoint live components and assemble their bundles immediately.
8. Merge dirty components into the owner repair transaction and advance one
   configured repair slice.

Within a batch this makes `Trade(102), Add(101)` project from the journaled
order `101,102`, without publishing an intermediate unknown-reference Event.
If the two facts arrive in different batches, Trade 102 can first publish its
currently correct unknown-reference payload; Add 101 then enters repair and
revises only the affected order chain and bundles.

Normal projection touches zero, one, or two direct order roles, plus any real
Shanghai barrier roles. The current containers are `std::map`, and the worker
also sorts each inserted micro-batch by stable key, so this implementation
does not claim container-level O(1) execution.

## 7. Minimal-closure repair

An owner has at most one active `RepairTransaction`, containing one task per
dirty `OrderKey`. New late facts merge into that transaction. A newly inserted
earlier use moves the task's effective start to the earliest dirty sequence;
a new later use extends its worklist. Neither operation restarts unrelated
orders or the whole Instrument/Channel.

Each task clones its live OrderHistory and starts from
`LatestStateBefore(earliest_dirty_sequence)`. For each later use it computes a
new role evaluation, compares the observable fragment, and compares the full
post-state:

- a changed fragment marks only that FactKey bundle dirty;
- a changed full state advances to the next use of the same order;
- an equal full state stops that propagation chain after the current use,
  unless another explicitly dirty use remains later in the task;
- input/provenance hash changes update the cache patch but do not by themselves
  extend the semantic state closure.

New facts and new order uses are registered in the owner journal/index so
subsequent input can discover them. The recomputed histories, role evaluations,
bundles, Event heads, and versions remain private until the transaction
commits. A generation counter on each live OrderUseIndex is captured by its
task. If a relevant order gains another use while repair runs, only that order
task is reset against the new generation. Fact business payloads themselves
are immutable after first-key admission; a Shanghai phase renormalization is
synchronously merged as dirty work for every affected role.

One slice processes at least one order-use node and then returns when either
`repair_slice_max_order_uses` or `repair_slice_max_cpu_ns` is reached. The owner
loop services normal traffic between slices. If the pending raw-commit FIFO is
full after calculation, the finished repair waits without leaking its private
state.

### Shanghai END

An accepted Shanghai status fact with `phase=Ended` is a broadcast barrier for
all orders already indexed in the same Instrument/Channel. A late ordinary
fact propagates only its affected order to that barrier. Bundle assembly reuses
all other cached finalize fragments.

- A newly recovered order that existed before END adds its finalize Event.
- An order made terminal before END removes the old finalize Event with a
  tombstone.
- An earlier recovered END necessarily attaches a barrier use to every order
  in its Instrument/Channel and is therefore a real broadcast repair.

An order first discovered later but with a use before an existing END is
attached to that stored barrier when its OrderHistory is created. This keeps
late order creation consistent without replaying unrelated Channel facts.

## 8. Raw durability gate

The raw sink captures canonical facts before `SequenceRecovery`. After one
exact `raw_tick` RowBinary INSERT is acknowledged, it calls the configured
`RawTickBatchAckListener` with the immutable batch rows. An unknown HTTP
outcome is retried with the same raw batch before this callback; the callback
is made once after success. Listener rejection is a fatal raw-sink continuity
failure.

`EventRuntime` groups ACK occurrences by instrument owner and places them in a
bounded, mutex-protected inbox. The owner converts them to
`(ingress_sequence, canonical_kind)` dependencies. An Event projection commit
can update live in-memory caches before these dependencies are durable, but
its immutable EventRevisionBatch remains in the pending FIFO. FIFO submission
has two consequences:

- ACKs may arrive before facts or out of order;
- a later durable batch cannot pass an earlier batch with an unacknowledged raw
  dependency.

After all dependencies of the FIFO head are acknowledged, the worker submits
the same versions, revision IDs, payloads, and recovery-run ID to the shared
Event sink. A sink rejection is sticky and fails the Event runtime closed.
The owner-local ACK-before-fact index is bounded as well as the concurrent
inbox. This matters because a duplicate suppressed inside SequenceRecovery can
have a durable raw occurrence without ever producing an Event input. Such
unmatched ACKs cannot grow without limit: exhausting the configured bound is a
continuity failure and stops the runtime instead of silently forgetting a
possibly still-in-flight dependency.

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
- `event_recovery_run`: one committed marker written only after every revision
  chunk in a calculation batch has received a successful ClickHouse response.

The sink serializes nested payload tuples directly in RowBinary. A large
revision batch is split into immutable chunks. Every retry of one chunk reuses
the same body, query ID, and deduplication token. Local nonreplicated tables use
ClickHouse's configured deduplication window; replicated DDL relies on the
replicated engine deduplication mechanism. The recovery marker has its own
stable token and is inserted after all chunks.

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
queue. A revision batch is routed by `batch.owner % writer_lanes`; therefore
all batches from one logical Event owner remain FIFO and their revision chunks
and recovery marker are serialized on the same lane. Different owners may
execute ClickHouse INSERTs concurrently. There is deliberately no cross-lane
total order because the Event contract has no cross-Channel exchange order.

`queue_revision_batches` and `queue_revision_rows` are global budgets shared by
all lanes. They are not multiplied by the lane count. Queue exhaustion,
permanent INSERT failure, or a retry-budget expiry changes the whole sink to
fail-closed; another lane cannot silently continue with an incomplete Event
view. Query IDs include the lane, while RowBinary bodies and deduplication
tokens remain immutable across retries. The queues contain only
`shared_ptr<const EventRevisionBatch>` objects in process memory; this feature
does not add a WAL, checkpoint, disk queue, or local staging file.

## 11. Capacity and failure policy

The FactJournal, order count, current BundleCache row count, pending raw commit
count, micro-batch, LateRecovery inbox, raw-ACK inbox, acknowledged-raw index,
Event sink batch queue, and Event sink row queue all have configured bounds.
Exhaustion fails the relevant worker/runtime/sink closed rather than dropping
a revision and continuing with an unprovable current view.

Source-key conflict is deliberately different: the worker retains the first
fact, increments the source-conflict statistic, and continues. It never guesses
which conflicting body is authoritative. Resolving such conflicts requires a
separate reconciliation policy over `raw_tick`.

## 12. Complexity and remaining gates

Let `U` be the number of actually visited uses on dirty order chains and `E`
the number of changed Event rows. With the current hash/vector hot indexes
and ordered range indexes:

```text
normal projection:
    O(batch sorting + expected-constant hash lookups + involved order roles)

late lookup and repair:
    O(log N + U + dirty bundle assembly/diff)

ClickHouse output:
    O(E), excluding engine merge work
```

The ordinary late path does not replay the unrelated Channel suffix. A Channel
suffix fallback for damaged/missing indexes or logic-version migration is not
implemented yet.

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
- query-layer policy for selecting committed calculation/logic versions.
