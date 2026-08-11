# Event batching optimization benchmark report

Test date: 2026-08-11 (Asia/Shanghai)

This report covers the current Event batching implementation after correctness
fences, logical recovery commits, owner persistence submissions, and physical
ClickHouse requests were separated. It replaces the former report for the old
single-logical-batch sink path.

## 1. Result

The selected configuration passed the 800k/s and 1M/s endpoints in three
independent five-second gates:

1. synthetic callback, decode, SequenceRecovery, and owner dispatch;
2. EventRuntime/EventWorker source-only projection with the file-backed shared
   FactJournal and an in-memory immutable-group sink;
3. production Event RowBinary serialization, lane grouping, retry-capable HTTP
   client, and bounded queues against a keep-alive loopback HTTP server.

The result supports a five-second component-capacity claim for 800k-1M
messages/s on this host. It is not a full production qualification: the sink
test does not execute ClickHouse MergeTree inserts, the Event state workload is
one source-event revision per fact without hole-fill repair or control bursts,
and no combined raw/Event/KLine/Arrow 180-minute or full-day run was performed.

## 2. Implemented boundaries

The current write path has three independent batching levels:

```text
computation micro-batch
    -> immutable logical EventRevisionBatch / recovery commit
    -> owner persistence submission (durable FIFO prefix)
    -> lane-local physical revision request(s) + grouped marker request
```

A `GapOpen` or `ChannelSeal` may end a computation cut for ordering, but it does
not directly enqueue a sink batch. Durable logical commits remain owner-local
until rows, owned bytes, logical-batch count, pending-commit capacity, or age
closes a persistence submission.

The sink retains each logical recovery identity and writes one independent
marker row and commit ID per logical commit. It freezes the physical revision
bodies, grouped marker body, query IDs, and deduplication tokens before sending.
All revision requests must succeed before the marker request is sent; no
logical batch is acknowledged or released until that marker request succeeds.
Unknown-outcome retries reuse the exact same bytes and identities.

The control mailbox coalesces only same-epoch, same-generation `ChannelSeal`
entries with a strictly newer dispatch fence. It keeps the newest fence and
maximum eviction watermark. A same-channel projected fact, hole fill, or
`GapOpen` applies the pending seal first. `GapOpen` is never coalesced because
the current FactJournal does not persist generation-specific gap controls.

## 3. Selected configuration

| Boundary | Selected value |
| --- | ---: |
| Event computation micro-batch | 512 facts / 50 ms |
| owner persistence submission | 1,024 logical commits / 16,384 revisions / 16 MiB / 1 s |
| physical revision request | 1,024 rows / 1 MiB |
| physical group | 256 logical commits / 1 ms |
| Event writer lanes in production profile | 8 |
| sink logical-batch queue | 1,024 global |
| sink revision-row queue | 1,048,576 global |
| pending ChannelSeal mailbox | 4,096 per owner |
| raw ACK service slice | 1,024 entries / 150 us |

The 512-row computation cut is intentional. A measured 16,384-row cut reduced
the same Event state workload to about 713k facts/s and raised projection-cut
p99 to about 2.37 seconds. Larger logical cuts are therefore not assumed to be
faster merely because they reduce batch count.

## 4. Test host and placement

| Item | Value |
| --- | --- |
| CPU | 2 x AMD EPYC 9534, 128 physical cores / 256 logical CPUs |
| NUMA | node0 `0-63,128-191`; node1 `64-127,192-255` |
| ingest producer | CPU 74 |
| ingest owner consumers | CPUs 32-63 |
| ingest decoder allocation | starts at CPU 77; observed CPUs 77-89 |
| ingest channels / owners / tick lanes | 32 / 32 / 12 |
| ingest latency sampling | every 100th measured message |
| Event state actors | 32 |
| Event state latency sampling | every 100th fact |
| sink owners / lanes | 32 / 8 |

CPU placement matters on this shared host. An attempted rerun with owners on
CPUs 0-31 was rejected after host-level utilization showed that CPUs 8-24 were
already saturated by unrelated work. The reported pair uses nonoverlapping
producer, decoder, and owner CPU sets with materially lower observed contention;
some owner cores still carried unrelated host work. The producer/decoders are
on node1 and owners are on node0, so these figures do not depend on a favorable
same-NUMA placement. Maximum producer schedule lag stayed below one millisecond
at both rates.

## 5. Callback and dispatch gate

Command shape:

```bash
build-release/benchmark_mdl_ingest \
  --rate RATE --seconds 5 --warmup-seconds 1 \
  --channels 32 --owners 32 --tick-lanes 12 \
  --sample-every 100 \
  --producer-cpu 74 --first-consumer-cpu 32 --first-decoder-cpu 77
```

| Metric | 800k/s | 1M/s |
| --- | ---: | ---: |
| measured callbacks | 4,000,000 | 5,000,000 |
| producer rate | 800,000.143 msg/s | 1,000,000.081 msg/s |
| owner dispatch rate | 799,996.876 msg/s | 999,999.709 msg/s |
| maximum schedule lag | 860.992 us | 954.153 us |
| latency samples | 40,000 | 50,016 |
| callback-to-dispatch p50 | 3.130 us | 2.680 us |
| callback-to-dispatch p90 | 3.880 us | 3.360 us |
| callback-to-dispatch p99 | 4.720 us | 4.200 us |
| callback-to-dispatch maximum | 1,701.364 us | 1,683.164 us |
| lane full / dispatch overflow | 0 / 0 | 0 / 0 |
| ordering / decode / gap errors | 0 / 0 / 0 | 0 / 0 / 0 |
| status | PASS | PASS |

Warm-up messages are included in exact admission/consumption accounting but
excluded from the measured-rate numerator. Both runs admitted and consumed all
warm-up plus measured messages.

## 6. Event state gate

Command shape:

```bash
build-release/benchmark_event_state \
  --rate RATE --seconds 5 --actors 32 \
  --micro-batch-rows 512 --sample-every 100 --pacing-burst 64
```

This workload uses production EventRuntime/EventWorker and the file-backed
CanonicalFactJournal. The sink checks owner FIFO and immutable group accounting
in memory so this gate measures projection state rather than HTTP or ClickHouse.

| Metric | 800k/s | 1M/s |
| --- | ---: | ---: |
| facts / revisions | 4,000,000 / 4,000,000 | 5,000,000 / 5,000,000 |
| steady owner-active rate | 798,427.895 fact/s | 998,143.451 fact/s |
| rate gate (99% target) | 792,000 fact/s | 990,000 fact/s |
| facts per computation micro-batch | 510.204 | 510.621 |
| required density (80% of 512) | 409.600 | 409.600 |
| row-limit / timer / forced-control flushes | 7,808 / 0 / 0 | 9,760 / 0 / 0 |
| logical recovery batches | 7,840 | 9,792 |
| owner persistence submissions | 256 | 320 |
| revisions per persistence submission | 15,625 | 15,625 |
| dispatch latency p99 | 40,359.211 us | 38,508.635 us |
| completed projection-cut latency p99 | 66,837.723 us | 65,908.453 us |
| `DrainAll` tail | 83.090 ms | 78.858 ms |
| journal `fdatasync` tail | 133.756 ms | 21.111 ms |
| current/peak RSS | 966.508 MiB | 970.879 MiB |
| pending raw commits / ACK index | 0 / 0 | 0 / 0 |
| journal records / conflicts / errors | 4,000,000 / 0 / 0 | 5,000,000 / 0 / 0 |
| status | PASS | PASS |

The separately printed durable rate includes final `DrainAll` and journal
`fdatasync`; it is not used as the continuous throughput gate. The gate uses
the owner-active window, while shutdown costs are reported independently.

## 7. Physical sink gate

Command shape:

```bash
build-release/benchmark_event_sink --rate RATE --seconds 5
```

The loopback server reads the real HTTP requests and RowBinary bodies produced
by `EventClickHouseSink`. It returns successful keep-alive responses but does
not parse rows into a ClickHouse table or execute MergeTree writes.

| Metric | 800k revisions/s | 1M revisions/s |
| --- | ---: | ---: |
| submitted / acknowledged rows | 4,000,000 / 4,000,000 | 5,000,000 / 5,000,000 |
| durable transport rate | 797,928.275 row/s | 996,356.650 row/s |
| owner sink submissions per second | 49.000 | 61.200 |
| logical recovery batches | 7,813 | 9,766 |
| physical groups | 3,907 | 4,883 |
| revision / marker requests | 3,907 / 3,907 | 4,883 / 4,883 |
| revisions per revision INSERT | 1,023.803 | 1,023.961 |
| logical batches per physical group | 2.000 | 2.000 |
| revision client average / maximum | 325.396 / 1,074.925 us | 327.650 / 2,241.552 us |
| loopback server revision p99 | 529.338 us | 495.007 us |
| submission-group queue slope | 0.000 groups/s | -0.073 groups/s |
| logical-batch queue slope | -0.445 batches/s | -0.731 batches/s |
| row queue regression slope | -231.787 rows/s | -376.299 rows/s |
| group / logical-batch / row HWM | 3 / 50 / 25,600 | 4 / 62 / 31,744 |
| final group / batch / row queue | 0 / 0 / 0 | 0 / 0 / 0 |
| retry / unknown outcome | 0 / 0 | 0 / 0 |
| status | PASS | PASS |

The sink gate requires:

```text
durable rate >= 99% of target
average revisions/request >= 90% of min(row limit, byte limit / 665)
group, logical-batch, and row queue slopes <= 0.1% of offered rate
all submitted logical batches ACKed and released
all three final queue values == 0
```

The queue capacities remain the production defaults during this test; they are
not scaled to the total five-second workload. High-water marks stayed at or
below about 3.1% of the row cap and 6.1% of the logical-batch cap.

One immediately preceding 1M/s attempt reached 994,043.005 durable rows/s and
drained every queue, but the five-second submission-group regression was
`0.063 groups/s`, narrowly above the strict `0.061 groups/s` gate, so that run
was correctly rejected. The independent rerun reported above passed with a
negative group slope. This sensitivity is another reason that production
qualification must use longer queue-slope windows rather than treating one
five-second sample as a soak result.

## 8. Correctness regression

The release build and all 40 registered tests passed. Focused Event tests cover:

- raw ACK first, disposition first, rejection settlement, and bounded ACK drain;
- computation cuts that do not close owner persistence groups;
- persistence closure by rows, bytes, batch count, age, and pending capacity;
- same-generation seal coalescing with latest fence and maximum watermark;
- same-channel fact/GapOpen ordering and generation/fence fail-closed behavior;
- multi-logical-batch revision/marker grouping and lane FIFO;
- revision success followed by marker retry or permanent failure;
- unknown-outcome exact-body/query-ID/dedup-token retries;
- no immutable logical-batch release before all revision and marker requests
  succeed.

## 9. Remaining qualification

Before production rollout, run the same counters against the target ClickHouse
node or replicated cluster. The required request density must be derived from
measured peak revisions per second divided by sustainable physical requests per
second. A successful loopback result cannot establish MergeTree merge, disk,
replication, quorum, network, or server query-latency capacity.

Qualification should progress through opening-burst, 30-minute, 180-minute,
and full-trading-day runs. Every stage must retain exact raw/Event/KLine
accounting, zero lane-full/admission failures, nonpositive long-window sink
batch/row slopes, bounded occurrence joins and pending commits, acceptable
callback and projection-cut latency, and an explainable RSS/PSS slope. Gap-open
and repair-heavy captured replays are required in addition to the source-only
state workload used here.
