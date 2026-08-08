# Event state-compute capacity (volatile-memory benchmark)

This report measures the Event state path separately from ClickHouse.  The
benchmark drives one owner thread per Event actor and runs the complete

```text
journal -> projection -> raw-ACK gate -> immutable revision batch
```

path.  The revision sink is a bounded, in-process counter that validates
per-owner `batch_sequence` monotonicity; it does not perform I/O.  The Event
FactJournal, caches, pending raw-commit deque, and ACK index are all process
memory.  There is no WAL, checkpoint, local file queue, or disk staging in
this benchmark.

The workload is deliberately narrow: one Shanghai `Status(Continuous)` fact
per callback, with one distinct `(security, channel)` per actor and strictly
increasing native sequence.  It is useful for a state-compute ceiling and for
comparing code changes, but it is not a production Event amplification model.
It does not include Add/Trade/Cancel order chains, Shenzhen 6.33/6.36 mixed
domains, late recovery, repair closure, or security skew.

## Reproduction

```bash
cmake --build build-benchmark-clickhouse-release -j 16
./build-benchmark-clickhouse-release/benchmark_event_state \
  --rate 800000 --seconds 5 --actors 16 --micro-batch-rows 768
./build-benchmark-clickhouse-release/benchmark_event_state \
  --rate 1000000 --seconds 5 --actors 16 --micro-batch-rows 768
# Compute-ceiling run (same callback count, no pacing-loop overhead):
./build-benchmark-clickhouse-release/benchmark_event_state \
  --rate 1000000 --seconds 5 --actors 16 --micro-batch-rows 768 --unpaced
```

`effective_fact_s` is calculated from the first actual callback timestamp to
the latest actor finish timestamp.  The benchmark requires at least 99% of
the requested rate and checks that every fact, raw ACK, revision, and
in-memory sink row is accounted for.

## Results observed in this workspace

The observed runs used the available AMD EPYC 9534 host (256 logical CPUs,
two NUMA nodes) and a Release build.  CPU affinity and NUMA placement were not
forced, so these numbers are host observations rather than a portable limit.

| mode | requested callback rate | duration | actors | micro-batch rows | facts journaled | revisions | in-memory rows ACKed | effective state rate | result |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| scheduled | 800,000 msg/s | 5 s | 16 | 768 | 4,000,000 | 4,000,000 | 4,000,000 | 799,037.658 fact/s | PASS |
| scheduled | 1,000,000 msg/s | 5 s | 16 | 768 | 5,000,000 | 5,000,000 | 5,000,000 | 993,802.130 fact/s | PASS |
| unpaced (capacity ceiling) | 1,000,000 msg/s target | 5 s / 5,000,000 facts | 16 | 768 | 5,000,000 | 5,000,000 | 5,000,000 | 1,546,986.564 fact/s | PASS |

The scheduled runs reported `ordered_batch_fast_path == source_only_fast_path`
and `unordered_batch_sorts == 0`.  These counters verify that the measured
stream used the intended ordered append path; they are not a claim about
unordered production traffic.  Repeated scheduled 1M runs on the same shared
host in the recent repeats varied between 987,954.296 and 994,783.999 fact/s (one run therefore fell
below the benchmark's 99% qualification line even though all 5,000,000 facts,
revisions, and ACKs were accounted for).  The unpaced run removes the
per-callback pacing loop and is the better estimate of the Event state path's
compute ceiling; it is not a proof that a real feed has zero scheduling or
dispatch overhead.

For a small actor-count sweep (the same Shanghai Status-only workload,
unpaced, 3,000,000 facts per run), the observed ceiling was:

| Event owner actors | effective state rate | qualification against 1M target | interpretation |
|---:|---:|:---:|:---|
| 4 | 598,655.908 fact/s | FAIL | too few single-writer actors for this host/workload |
| 8 | 1,082,592.564 fact/s | PASS | clears 1M for this synthetic path |
| 16 | 1,525,591.435 fact/s | PASS | additional parallel state capacity |
| 32 | 1,566,258.143 fact/s | PASS | near the observed saturation point; do not oversubscribe blindly |

This sweep is an implementation signal, not a universal scaling curve.  A
single hot security remains serialized on its owner, and mixed Add/Trade/Cancel
or late-repair work can have a much larger per-callback cost.

## What this does and does not prove

For this source-only workload on this host, the volatile Event state path has
enough unpaced compute capacity for the 800k–1M target and met the scheduled
qualification line in the reported PASS runs.  The repeatability note above
means that a production acceptance test still needs CPU pinning/NUMA policy
and a longer window before treating 1M as a stable SLO.  This benchmark does
**not** prove that ClickHouse can ingest 1M revision rows/s, nor that a mixed
order-chain workload has the same capacity.  ClickHouse lane tests must be run
against the target ClickHouse topology with its actual `revision_rows / callback`
amplification, merge settings, replication, and network path.

The benchmark's `InMemoryRevisionSink` is intentionally not a ClickHouse
replacement: it only checks owner-local batch ordering and counts rows.  The
ClickHouse server may use its own configured MergeTree storage; this branch
adds no process-side WAL, checkpoint, disk queue, or local staging file.
Consequently, a process restart does not reconstruct the in-memory Event state
automatically, and a ClickHouse outage that fills the bounded RAM queues is a
fail-closed condition.
