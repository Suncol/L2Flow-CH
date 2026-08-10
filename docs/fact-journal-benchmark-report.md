# FactJournal 800k-1M throughput and read-latency report

Test date: 2026-08-10 (Asia/Shanghai)

## Outcome

The file-backed `CanonicalFactJournal` passed both 300-second endpoint tests:

| Target | Target facts | Pre-flush rate | Durable rate | Durable threshold | Result |
|---:|---:|---:|---:|---:|:---:|
| 800,000 msg/s | 240,000,000 | 799,985.583 msg/s | 799,100.436 msg/s | 792,079.208 msg/s | PASS |
| 1,000,000 msg/s | 300,000,000 | 999,974.743 msg/s | 994,275.469 msg/s | 990,099.010 msg/s | PASS |

The durable interval includes the final successful `fdatasync`. The benchmark
allows one percent elapsed-time tolerance: a paced run must finish its target
count and durability boundary within `300 s * 1.01`. PASS also requires exact
record, consumer, and file-byte accounting; successful mixed and post-run read
probes; zero conflict/failure/error/partial-I/O counters; no active or reserved
operation at the boundary; and a healthy journal.

Both endpoints were rerun after the final mixed-batch publication, sticky
failure, and failure-injection fixes. The results below therefore come from the
same final Release implementation covered by the unit and sanitizer runs, not
from the earlier pre-fix benchmark binary.

These are the two requested endpoints. The test did not sweep every rate
between 800k and 1M, so it does not establish a complete throughput curve.
PASS has no latency-SLO gate: the latency distributions below are observations,
not a claim that a predefined p99 or p99.9 objective was satisfied.

## Scope

This is a journal-only test. Its timed write path is:

```text
32 synthetic owner callers
  -> Event AdmitBatch
  -> KLine AdmitBatch for the same facts
  -> one shared CanonicalFactJournal
  -> positional file writes
  -> final Flush / fdatasync
```

Event admission writes each distinct canonical winner once. KLine admission
sets its in-memory directory consumer-seen bit referencing the same physical
record; the bit is not persisted in the 344-byte record, and KLine does not
append a second record. The test does not execute `EventWorker`, `KLineWorker`,
Event/KLine projection, a ClickHouse sink, network I/O, or ClickHouse merges
and replication. Passing this report is therefore a storage-admission
prerequisite, not an Event/KLine/ClickHouse end-to-end qualification.

The target workload uses a deterministic approximation of the observed
2026-08-07 source-family mix:

| Source family | Share per 10,000 target facts |
|---|---:|
| Shanghai Tick | 3,954 |
| Shenzhen Order | 3,180 |
| Shenzhen Trade | 2,866 |

Each owner uses synthetic, independent, densely increasing market/channel
sequences. The 32 owners create 64 target `(market, channel)` domains, and a
separate Shanghai channel holds the read-probe seeds. This is deliberately not
the real production channel-to-owner distribution, channel sparsity, burst
profile, or hot-owner skew.

Each owner always calls Event first and KLine second for a batch. The test does
not cover a different consumer order or independently competing Event/KLine
callers.

## Configuration

| Setting | Value |
|---|---:|
| Build | Release |
| Duration | 300 s per endpoint |
| Concurrent owner callers | 32 |
| Rows per owner batch | 256 |
| Decoded hot-cache entries | 262,144 |
| Mixed reads | 100/s, 30,000 samples |
| Pre-timing seed records | 64 |
| Post-run hot-duplicate samples | 4,096 |
| Post-run cleared-cache read samples | 4,096 |
| Physical record size | 344 bytes |
| File header | 32 bytes |
| Maximum directory pages | 262,144 |

For a paced test, `maximum_records` is set to `target facts + 64 seeds`, rather
than to the 600,000,000-record production default. The 64 seeds are admitted by
both consumers, flushed, and passed through the initial file-cache advice
before the timed writers start. Seeds count in final journal/file accounting,
but not in the target count, timed write bytes, or throughput numerator.

The tested 262,144-entry hot cache matches
`current-server.production.conf`. Other aspects of the benchmark, especially
its dense synthetic channels and journal-only execution path, are not a claim
that the complete workload is production-aligned.

The exact executed commands were:

```bash
build-release/benchmark_fact_journal \
  --rate 800000 --seconds 300 --batch-rows 256 --owners 32 \
  --journal-dir /tmp --hot-cache 262144 --probe-samples 4096 \
  --mixed-read-rate 100 --seed-records 64

build-release/benchmark_fact_journal \
  --rate 1000000 --seconds 300 --batch-rows 256 --owners 32 \
  --journal-dir /tmp --hot-cache 262144 --probe-samples 4096 \
  --mixed-read-rate 100 --seed-records 64
```

The Release build can be refreshed separately with:

```bash
cmake --build build-release --target benchmark_fact_journal -j 16
```

The benchmark creates a unique file below `--journal-dir` and removes it on
normal exit. The directory must nevertheless have enough free space for the
entire live file while the test runs.

## Host and storage

| Resource | Observed configuration |
|---|---|
| CPU | 2 x AMD EPYC 9534, 64 cores/socket, SMT2, 256 logical CPUs |
| RAM | approximately 1.0 TiB |
| Kernel | Linux 6.17.0-35-generic, x86-64 |
| Compiler | GCC 13.3.0 |
| Journal mount | `/tmp` on `/dev/sda3`, ext4 |
| Parent block device | `/dev/sda`, model `PERC H755N Front`, `ROTA=0` |

The device name describes the host-visible RAID-controller volume; it does not
document the controller cache policy or the underlying drive topology. No CPU
or NUMA isolation result is claimed here. These numbers are observations from
this host and filesystem, not portable limits for another journal device.

## Throughput and accounting

| Metric | 800k run | 1M run |
|---|---:|---:|
| Timed target facts | 240,000,000 | 300,000,000 |
| Seed records excluded from target | 64 | 64 |
| Final physical records | 240,000,064 | 300,000,064 |
| Final consumer-new count | 480,000,128 | 600,000,128 |
| Timed target write bytes | 82,560,000,000 | 103,200,000,000 |
| Final journal-accounted file bytes | 82,560,022,048 | 103,200,022,048 |
| Filesystem-reported apparent bytes | 82,560,022,048 | 103,200,022,048 |
| Filesystem-reported apparent length | 76.890 GiB | 96.113 GiB |
| Pre-flush throughput | 799,985.583 msg/s | 999,974.743 msg/s |
| Durable throughput | 799,100.436 msg/s | 994,275.469 msg/s |
| Durable elapsed time | 300.338 s | 301.727 s |
| Logical record-write rate at durable boundary | 262.156 MiB/s | 326.186 MiB/s |
| Final `fdatasync` | 331.862 ms | 1,719.325 ms |
| Writer maximum schedule lag | 16,740.562 us | 30,401.948 us |
| Directory pages | 58,625 | 73,281 |
| Nominal directory slot payload | 3.578 GiB | 4.473 GiB |
| Current process RSS | 3,845.492 MiB | 4,776.996 MiB |
| Peak process RSS | 3,845.492 MiB | 4,776.996 MiB |
| Result | PASS | PASS |

The difference between the two final-flush times is not a device-latency
comparison. Kernel writeback progress, device/controller state, and concurrent
host activity before the single final `fdatasync` can change how much dirty
data remains at that call.

The MiB/s row is `344 * target facts / durable elapsed time`: it is logical
successful record-write throughput, not measured physical-device bandwidth.
The journal-accounted file bytes and `std::filesystem::file_size()` result are
logical/apparent lengths. The latter is now emitted as `observed_file_bytes`;
it is not allocated filesystem blocks or bytes observed at the device. These
lengths exclude filesystem metadata, reserve, and RAID/controller overhead.

The timed target path had zero unexpected duplicate classifications. The
post-run hot probe then intentionally produced and validated 4,096 equal
duplicates. Both runs reported zero for all of the following failure or
in-flight conditions:

```text
source conflicts
admission/read failures
journal errors
partial writes
partial reads
active writes
active reads
reserved records
waiting admissions
```

The filesystem-reported apparent length matched `32 + 344 * records`. The
target-only timed write delta matched `344 * target facts`, and the final
consumer count matched `2 * records`. Post-run probes did not change record or
file/write-byte counts. No allocated-block or device-byte measurement was made.

RSS is the whole benchmark process, not a direct measurement of directory pages
alone. `current_rss_mib` is sampled near the end, while `peak_rss_mib` is the
process high-water mark from `/proc/self/status`. Both final runs reported the
same current and peak value at their measurement boundary. RSS includes the
sparse directory, decoded LRU, benchmark sample vectors, allocator/container
overhead, and other process state. Conversely, reclaimable Linux filesystem
page cache is kernel memory and is not charged as process RSS. Neither RSS
value includes Event's intraday projection/repair metadata because Event is not
run here.

## Latency

| Probe | Run | Samples | p50 | p99 | p99.9 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| Mixed read under paced writes | 800k | 30,000 | 8.360 us | 2,636.904 us | 3,872.434 us | 9,635.104 us |
| Mixed read under paced writes | 1M | 30,000 | 8.490 us | 2,773.023 us | 4,055.925 us | 7,740.501 us |
| Hot duplicate after the run | 800k | 4,096 | 0.550 us | 0.760 us | 4.930 us | 13.940 us |
| Hot duplicate after the run | 1M | 4,096 | 0.549 us | 0.651 us | 5.340 us | 15.980 us |
| Cleared-app-cache, file-cache-advised read | 800k | 4,096 | 1.490 us | 3.750 us | 87.230 us | 433.124 us |
| Cleared-app-cache, file-cache-advised read | 1M | 4,096 | 1.490 us | 3.800 us | 114.318 us | 166.267 us |

For each mixed sample, the benchmark selects one of the separately flushed
64 seed handles, calls `EvictHotCache(handle)` outside the timed interval, and
then times `Read(handle)`. This forces an application decoded-cache miss for
that handle without clearing the writers' entire hot cache. All 30,000 reads in
each run completed; validation checked `MakeFactKey` and authoritative
business-payload equality through `FactPayloadEqual`. That predicate
intentionally does not equate every arrival provenance, recovery annotation,
or derived canonical field. Journal read-call and byte counts reconciled
exactly, with one complete 344-byte `pread` per sample and no partial read. The
repeatedly read seed file region is only 22,048 bytes including its header, so
this is a deliberately small, controlled read working set.

The post-run read probe calls `ClearHotCache()` and then requests
`POSIX_FADV_DONTNEED` through `DropFileCache()` before timing old-handle reads.
That advice is a successful, gated request to the kernel, but it is advisory.
Neither the mixed-read nor post-run result proves that a particular `pread`
reached the physical device rather than filesystem, controller, or device
cache. The report therefore does not label these values as physical-device
miss latency.

Percentiles use the benchmark's non-interpolated order statistic,
`sorted[floor(q * (n - 1))]`. Mixed-reader maximum schedule lag was 5,954.852 us
at 800k and 5,966.115 us at 1M. The benchmark gates completed sample count, but
it has no separate schedule-lag threshold.

Admission latency emitted by the benchmark has a separate definition:

- Event and KLine batch latency is the wall service time of the corresponding
  `AdmitBatch` journal call.
- Dual-consumer batch latency is the sum of the Event and KLine call wall
  times; validation work between the calls is excluded.
- The printed per-fact equivalent is batch wall time divided by rows. It is an
  amortized equivalent, not a percentile of independently timed messages.

| Admission measurement | Run | Samples | p50 | p99 | p99.9 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| Event batch service time | 800k | 937,504 | 2,731.944 us | 6,132.554 us | 9,710.998 us | 21,490.846 us |
| Event batch service time | 1M | 1,171,904 | 2,699.717 us | 6,082.226 us | 10,911.632 us | 25,643.245 us |
| KLine batch service time | 800k | 937,504 | 2,045.048 us | 3,667.202 us | 5,135.682 us | 16,886.616 us |
| KLine batch service time | 1M | 1,171,904 | 1,909.717 us | 3,590.475 us | 5,132.054 us | 16,834.971 us |
| Dual-consumer batch service time | 800k | 937,504 | 4,926.072 us | 7,697.928 us | 12,758.802 us | 25,628.444 us |
| Dual-consumer batch service time | 1M | 1,171,904 | 4,724.000 us | 7,793.392 us | 13,687.024 us | 28,113.164 us |
| Dual-consumer per-fact equivalent | 800k | 937,504 | 19,242 ns | 30,070 ns | 49,839 ns | 100,111 ns |
| Dual-consumer per-fact equivalent | 1M | 1,171,904 | 18,453 ns | 30,454 ns | 53,675 ns | 260,269 ns |

The Event and KLine calls are made sequentially for each owner batch in this
synthetic benchmark. The dual-consumer values are sums of those two call times,
not elapsed latency through concurrently running Event and KLine workers.

## Capacity and lifecycle interpretation

The 300-second tests prove completion of the two paced endpoint workloads
within the configured one-percent durable-duration gate for this dense
synthetic topology and local storage. They do not prove full-day capacity. The
240,000,000 and 300,000,000 target facts cover only 50.581% and 63.226% of the
474,485,838 catalog-filtered facts observed for 2026-08-07.

The file representation avoids retaining every 344-byte authoritative payload
in process RAM; the bounded decoded LRU still caches its hot subset. The sparse
identity directory remains resident for the process/trading-day lifetime. Real
page usage depends on the cardinality of
`(market, channel, native_sequence / 4096)`, not only on record count. The
dense 58,625/73,281-page results must not be extrapolated to a sparse or skewed
production feed without measuring its actual page occupancy.

Event metadata and repair indexes also continue to grow intraday. The
journal-only RSS figures cannot be used as the total Event/KLine process memory
budget. Full-day sizing, including the 600,000,000-record production guard and
the observed-day 152.013 GiB journal file, is documented in
[`fact-journal.md`](fact-journal.md).

## Durability and recovery limits

The final `fdatasync` successfully completes the explicit local durability API
boundary for this newly created file. There is no per-record or per-batch sync
in the measured write phase; the reported boundary is the single run-end
`Flush`. The result is not a separate power-loss test of the unknown RAID cache
policy, and it does not make the journal a WAL or checkpoint:

- `Create()` starts an empty file for one trade date; it cannot reopen, scan,
  replay, or repair an earlier file.
- There is no implemented FactJournal replay boundary, derived-state
  checkpoint, or intraday Event-state recovery procedure in this benchmark.
- Records are not recycled during the trading-day lifetime.
- Durable `raw_tick` storage remains the intended reconstruction source, but
  this journal-only test does not execute or validate that replay path.

The benchmark also does not test a storage outage, ENOSPC, process crash during
write, restart, or contention with ClickHouse merge I/O. Production acceptance
must repeat the long run on the target isolated journal device and must include
free-space, error injection, restart/replay policy, and full-day page/RSS
measurements.

## Verification limitation

The Release target builds with the repository's strict warning policy, and the
benchmark's functional smoke test passes. ThreadSanitizer runtime validation is
not available from this environment: the runtime exits before the test body
with `FATAL: ThreadSanitizer: unexpected memory mapping`. This is an environment
limitation, not a TSan pass, and the Release throughput PASS must not be treated
as independent proof that all concurrency defects are absent.
