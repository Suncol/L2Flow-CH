# Event source-only capacity benchmark with a file-backed FactJournal

Test date: 2026-08-09 (Asia/Shanghai)

## Outcome

The current file-backed Event path did not qualify at the lower 800,000 fact/s
endpoint:

| Target | Scheduled window | Facts completed | Pre-flush rate | Durable rate | Required rate | Result |
|---:|---:|---:|---:|---:|---:|:---:|
| 800,000 fact/s | 300 s | 240,000,000 | 414,691.414 fact/s | 414,322.757 fact/s | 792,000 fact/s | FAIL |

All 240,000,000 input facts, synthetic raw ACKs, journal records, revisions,
and in-process sink rows were accounted for exactly. The failure is a throughput
qualification failure, not a missing-row result. The durable path took 579.259
seconds and achieved 51.790% of the requested rate; it was far below the
`0.99 * target` gate.

The 1,000,000 fact/s Event endpoint was not run. Since the same implementation
already missed the lower endpoint by a wide margin, a 1M run would not close
the failed 800k qualification and would consume another large journal file and
hundreds of GiB of process memory.

The shared FactJournal has separately passed 300-second journal-only tests at
800k and 1M. See
[`fact-journal-benchmark-report.md`](fact-journal-benchmark-report.md). That
result does not override this Event failure: the journal benchmark does not run
Event projection or retain Event's intraday metadata, while this benchmark
does.

## 2026-08-10 latency optimization check

The normal Event and KLine admission path now receives each complete
authoritative winner directly from `AdmitBatch`. It no longer performs one
additional `FactJournal::Read` per consumer-new fact. That removes a second
shared-mutex acquisition, decoded-LRU lookup/mutation, and full tick copy from
the Worker path while preserving first-winner provenance. Event historical
repair and genuinely cold access still use `Read`.

A one-second, unpaced, 32-owner A/B used the same source snapshot, CPU set,
workload, and Release configuration on both binaries:

```bash
taskset -c 136-167 <benchmark_event_state> \
  --rate 1000000 --seconds 1 --actors 32 \
  --micro-batch-rows 32 --journal-dir /tmp --unpaced
```

| Binary | Actor elapsed | Pre-flush rate | Durable rate | Hot-cache hits | Result |
|---|---:|---:|---:|---:|:---:|
| Before optimization | 1.730 s | 577,985.798 fact/s | 551,130.341 fact/s | 1,000,000 | FAIL |
| After optimization | 0.759 s | 1,316,706.340 fact/s | 1,079,082.408 fact/s | 0 | PASS |

The optimized sample improved pre-flush throughput by approximately 127.8%
and reduced pre-flush elapsed time by 56.1%. The exact transition from one
hot-cache hit per projected fact to zero verifies that the redundant Worker
reads were removed. This short unpaced A/B isolates the changed hot path; it
does not replace the historical 300-second capacity result below, establish a
stable p99 latency distribution under the host's concurrent load, or qualify
full-day memory growth and durability.

After adding the final non-overlapping-span API guard, the rebuilt optimized
binary was checked again with the same command. One confirmation sample
reported 1,282,389.397 fact/s pre-flush and 1,165,963.240 fact/s durable, zero
hot-cache hits, and PASS. Other final-binary samples during changing host load
ranged from 623,785.218 to 678,612.658 fact/s pre-flush. This spread is why the
short run is used as a directional hot-path check rather than a stable absolute
latency or capacity result.

## Superseded results

This report no longer includes the earlier five-second throughput table or
actor-count sweep. Those measurements used the former memory-only FactJournal
path, timed only to the last actor completion, and did not include the current
`DrainAll()` plus final journal `fdatasync` boundary. Their old
`effective_fact_s` values are not comparable with the current
`durable_fact_s` result and must not be used as current capacity evidence.

## Measured path

The benchmark drives one producer thread per Event actor and executes:

```text
synthetic Shanghai Status(Continuous) fact
  -> EventRuntime append
  -> shared CanonicalFactJournal file append
  -> Event source-only projection
  -> synthetic raw-ACK gate
  -> immutable revision batch
  -> bounded in-process ordering/counting sink
  -> Event DrainAll
  -> FactJournal Flush / fdatasync
```

ClickHouse is disabled. Raw acknowledgements are supplied directly by the
benchmark after each input batch; no raw ClickHouse durability is exercised.
`InMemoryRevisionSink` validates owner-local `batch_sequence` monotonicity and
counts revision rows and batches. It performs no I/O and does not make revision
rows durable.

This is not a memory-only benchmark. All Event actors share one explicitly
injected `CanonicalFactJournal`. Each distinct accepted fact is encoded as one
344-byte record in a unique local file, plus one 32-byte file header. The
in-memory sparse identity directory, bounded decoded-record LRU, Event
projection and repair metadata, pending raw-commit state, ACK indexes,
benchmark input buffers, and revision sink remain in process memory.
Reclaimable filesystem page cache is kernel memory and is not process RSS.

## Formal configuration

| Setting | Value |
|---|---:|
| Mode | scheduled |
| Requested rate | 800,000 fact/s |
| Requested duration | 300 s |
| Event actors | 32 |
| Event micro-batch rows | 256 |
| Journal decoded hot cache | 65,536 entries |
| Journal maximum records | 240,000,000 |
| Maximum cached events per actor | 7,500,257 |
| Journal directory | `/tmp` |
| Journal record/header | 344 / 32 bytes |

The Event executable does not expose a hot-cache option and therefore used the
library default of 65,536 entries. The production configuration and the
journal-only formal tests use 262,144 entries. This Event result is not fully
production-configuration-aligned, although increasing the cache alone cannot
turn a 414k fact/s result into an asserted 800k result without a new measured
run.

The journal's test-local `maximum_records` equals the scheduled fact count,
rather than the 600,000,000-record production guard. The file was created below
`/tmp`, which is ext4 on `/dev/sda3`; the host-visible parent block device is
the `ROTA=0` `PERC H755N Front` volume. This describes the observed host path,
not the controller cache policy or underlying drive topology.

The exact executed command was:

```bash
build-release/benchmark_event_state \
  --rate 800000 --seconds 300 --actors 32 --micro-batch-rows 256 \
  --journal-dir /tmp
```

The Release build can be refreshed separately with:

```bash
cmake --build build-release --target benchmark_event_state -j 16
```

## Throughput boundaries

| Metric | Observed value |
|---|---:|
| Last-actor elapsed | 578.743 s |
| Pre-flush elapsed, including `DrainAll` | 578.744 s |
| Durable elapsed, including journal `fdatasync` | 579.259 s |
| Pre-flush throughput | 414,691.414 fact/s |
| Durable throughput | 414,322.757 fact/s |
| Required throughput | 792,000 fact/s |
| `DrainAll` time | 0.023 ms |
| Final journal `fdatasync` | 514.956 ms |
| Maximum producer schedule lag | 279,809,981.724 us (279.810 s) |
| Status | FAIL |

The producer actors directly execute Event work. Once they fall behind their
scheduled deadlines, they process as fast as the path allows instead of
buffering an external 800k/s arrival stream. The approximately 279.810-second
maximum lag and 578.743-second actor interval show that the target 300-second
schedule was not maintained.

`DrainAll` was effectively empty, and the final `fdatasync` added about 0.515
seconds. The run was already limited to 414,691.414 fact/s before the final
durability call, so the throughput failure cannot be attributed to the final
sync alone. The journal-only result demonstrates that the storage-admission
layer can pass the endpoint on this host under its own workload, but this
combined benchmark does not isolate which Event data structures or operations
dominate the additional cost.

## Exact accounting

| Metric | Observed value |
|---|---:|
| Expected facts | 240,000,000 |
| Normal facts received | 240,000,000 |
| Synthetic raw ACKs received | 240,000,000 |
| Facts journaled / physical records | 240,000,000 / 240,000,000 |
| Event consumer-new count | 240,000,000 |
| Revisions created | 240,000,000 |
| In-process sink rows | 240,000,000 |
| Journal record bytes | 82,560,000,000 |
| Journal-accounted file bytes | 82,560,000,032 |
| Filesystem-reported apparent length | 82,560,000,032 bytes |
| Journal write calls | 937,505 |
| Partial writes | 0 |
| Journal record read calls | 11,532 |
| Journal decoded hot-cache hits | 239,988,468 |
| Journal flush calls | 1 |
| Active reads / writes | 0 / 0 |
| Reserved records / waiting admissions | 0 / 0 |
| Source conflicts / journal errors | 0 / 0 |
| Journal cleanup | removed |

For the historical binary measured in this section, the reported read-call and
hot-hit counters arithmetically sum to 240,000,000 and show that almost all
Event handle loads used the decoded LRU. That identity no longer applies to the
optimized binary because normal projection consumes the winner returned by
`AdmitBatch` and does not call `Read`. `read_calls` counts `pread` system calls,
however; because this prior binary did not print `partial_reads` or
`read_bytes`, the report does not convert its 11,532 calls into an asserted
number of distinct complete cold-record reads. The benchmark also does not
record a read service-time distribution. Consumer-new is 240,000,000 rather
than twice that value because KLine is not part of this Event benchmark.

The journal-accounted length and the apparent length returned by
`std::filesystem::file_size()` both exactly match `32 + 344 * records`. The
filesystem observation is now emitted as `observed_file_bytes`; it is not a
measurement of allocated filesystem blocks or bytes observed at the device.
The 937,505 write calls are the file header plus the owner-local batches
produced by the 32 actors. No partial write, conflict, journal error, or
in-flight operation remained at the boundary. Runtime, journal, and sink were
healthy, and the unique journal file was removed after shutdown.

## Memory result

| Metric | Observed value |
|---|---:|
| Current process RSS | 291,824.078 MiB |
| Peak process RSS | 291,824.078 MiB |
| Current/peak process RSS | 284.984 GiB |

RSS was sampled after Event drain and journal flush while the runtime and
journal were still alive. It is whole-process memory, not an isolated Event
metadata measurement. It includes the journal directory/LRU, Event projection
and repair state, benchmark buffers, revision sink, allocator/container
overhead, and other process state; it excludes reclaimable kernel filesystem
page cache.

For context, the separate 240,000,000-target journal-only run reported a
3,845.434 MiB peak RSS, but it used a different workload and process. The two
values must not be subtracted as if they were component-level memory profiles.
They do show why a journal-only memory result cannot be used as the Event
process budget: Event's retained intraday state changes the scale materially.

The 240,000,000 facts cover only 50.581% of the 474,485,838 catalog-filtered
facts observed for 2026-08-07. Memory growth must not be linearly extrapolated
from this synthetic source-only run, but 284.984 GiB at half the observed fact
count is already a blocking full-day-capacity concern. The retained formal
output did not include the journal directory-page count. The current source
prints `directory_pages` for subsequent runs, but no value is reconstructed
for this prior result; its full-day page dimension remains unmeasured here.

## Timing and PASS semantics

The executable reports three distinct boundaries:

| Metric | End boundary |
|---|---|
| `actors_elapsed_s` | Last producer actor returns after its owner flush |
| `preflush_elapsed_s` / `preflush_fact_s` | `EventRuntime::DrainAll()` completes |
| `durable_elapsed_s` / `durable_fact_s` | Final `CanonicalFactJournal::Flush()` and `fdatasync` complete |

The origin is the first actual fact-processing timestamp. PASS uses
`durable_fact_s >= 0.99 * requested_rate`; actor-end throughput is diagnostic,
not the qualification metric.

PASS also requires exact accounting of normal facts, synthetic raw ACKs,
journaled facts, revisions, in-process sink rows, journal records,
journal-accounted file bytes, and filesystem-reported apparent length. It
requires zero late facts, source conflicts, invalid inputs, unexpected
duplicates, pending raw commits, pending raw ACK
dependencies, active journal reads/writes, reserved records, waiting
admissions, and journal errors. Runtime, journal, and sink health and successful
journal-file cleanup are mandatory.

The current source also requires `partial_writes == 0` and
`partial_reads == 0` for PASS. The formal 800k run used the immediately prior
benchmark binary: its retained output explicitly reported
`partial_writes=0`, but did not print `partial_reads`, so the latter value is
unknown and is not inferred here. This cannot change the recorded outcome,
because the run already failed its durable-throughput gate independently of
the partial-I/O checks. Future runs print and gate both counters.

## Workload boundary

The synthetic stream is deliberately narrow:

- one Shanghai `4.101.24` `Status(Continuous)` fact per callback;
- one distinct synthetic security and channel per actor;
- strictly increasing native sequence in each channel;
- ordered source-only projection;
- one source-event revision per accepted fact for this workload.

It does not include Shanghai Add/Trade/Cancel chains, the Shenzhen 6.33/6.36
shared sequence domain, late recovery, historical repair closure, real channel
gaps, burstiness, security skew, or hot-order dependency chains. A single hot
security remains serialized on its owner, and mixed order-chain or repair work
can cost substantially more than this source-only path.

The benchmark does not exercise KLine or ClickHouse. It cannot establish
ClickHouse revision throughput, merge/replication behavior, network latency, or
a real `revision_rows / callback` amplification distribution.

## Durability and recovery boundary

`FactJournal::Flush()` waits for active writes and calls `fdatasync`, so
`durable_fact_s` includes an explicit local-file durability API boundary. This
is not a power-loss test of an unknown storage-controller cache policy, and the
file is not a crash-recovery WAL or checkpoint:

- creation starts a new empty journal for one trade date;
- the implementation cannot reopen, scan, or replay an earlier journal;
- the benchmark implements no derived Event-state checkpoint or restart
  reconstruction;
- the in-memory revision sink is not a disk-backed ClickHouse outage queue;
- durable raw storage remains the intended reconstruction source, but this
  benchmark neither writes it nor tests its replay procedure.

A process restart therefore does not reconstruct Event state automatically.
FactJournal stores accepted source winners; it does not absorb an unbounded
revision backlog or replace the separately required raw replay/checkpoint
design.

## Qualification conclusion

The optimized Event implementation is not yet qualified for sustained
800k-1M fact/s because the historical 300-second test predates the optimization
and failed. The short A/B shows that the redundant journal read was a material
part of the former approximately 414k fact/s ceiling, so the next formal run
should repeat the 800k endpoint with the optimized binary. A 1M long run is not
useful as capacity evidence until that lower endpoint passes. The 284.984 GiB
retained process footprint remains a separate full-day-capacity concern and is
not improved or requalified by the short latency test.

After that, production acceptance still needs the real source mix and skew,
full-day metadata and directory-page growth, the target journal device and
cache configuration, raw replay/restart tests, and a separate Event-to-
ClickHouse test with the intended topology.

The registered CTest smoke uses 1,000 facts/s for one second, one actor, and a
32-row micro-batch. It verifies construction, accounting, durable flush, and
cleanup, but it is not a throughput qualification.

ThreadSanitizer runtime validation is unavailable in the current environment:
the runtime exits before the test body with
`FATAL: ThreadSanitizer: unexpected memory mapping`. This is an environment
limitation and must not be recorded as a TSan pass.
