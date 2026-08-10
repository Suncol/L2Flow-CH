# Canonical FactJournal capacity and storage contract

## 1. What the capacity counts

`FactJournal` capacity is the maximum number of distinct canonical fact
winners retained for one configured trading date. A fact is identified by:

```text
(trade_date, market, channel, native_sequence)
```

This is a channel-global identity, not an instrument-owner-local identity.
Event and KLine share one `CanonicalFactJournal`; the first business payload
for a key is appended once, while independent Event and KLine consumer bits
record whether each projection has observed it. The second consumer therefore
does not consume another physical record.

The capacity is not a queue depth, micro-batch size, rate limit, or amount of
time. It is a trading-day lifecycle bound. The journal retains the winner so
that a later occurrence can be classified as an equal duplicate or a source
conflict and so Event historical repair can load an earlier source fact.
Evicting an authoritative winner without a replay/checkpoint boundary would
make those decisions unprovable. The implementation consequently fails closed
when its global `maximum_records` bound is reached.

The former per-owner `maximum_facts` fields and command-line options have been
removed. There is no compatibility path back to the 4,194,304-fact owner-local
tables, and workers must be given the shared journal explicitly.

## 2. Why the old bound cannot cover a day

The removed production setting provided:

```text
4,194,304 facts/owner * 32 owners = 134,217,728 facts
```

The catalog-filtered 2026-08-07 data observed in this workspace contains:

| Source family | Facts |
|---|---:|
| Shanghai Tick | 187,592,385 |
| Shenzhen Order | 150,870,093 |
| Shenzhen Trade | 136,023,360 |
| Total | 474,485,838 |

The old total covers only `28.286983%` of that day. Even a perfectly uniform
32-way distribution averages `14,827,682.4375` facts per owner, or
`3.535195` times the old bound. Owner skew can only make the first exhaustion
earlier. At 800,000 and 1,000,000 messages/s, 134,217,728 positions represent
only about 167.8 and 134.2 seconds respectively; higher short-window
throughput cannot repair a lifecycle capacity deficit.

## 3. Disk and memory representation

Each physical journal record is 344 bytes:

```text
24-byte record header + 313-byte explicit little-endian CanonicalTick
payload + 7 bytes zero padding
```

The record header includes format metadata and a CRC32C over the payload. The
codec does not dump C++ ABI padding. Records are appended with positional
writes. Classification and offset reservation are serialized, but unrelated
reserved ranges are written concurrently. A pending directory flag prevents
another batch from reading or comparing an incomplete record. `Flush()` waits
for all active writes and then calls `fdatasync`.

The canonical record file is only one part of the resource budget:

| Component | Placement | Lifecycle |
|---|---|---|
| 344-byte canonical winner | local journal file | process/trading day |
| 16-byte `{offset+flags, fingerprint}` slot | process RAM | process/trading day |
| decoded winner LRU | process RAM, bounded | hot working set |
| Event fact metadata and repair indexes | process RAM | process/trading day |
| KLine bars and revision state | process RAM | process/trading day |
| operating-system file cache | reclaimable kernel RAM | kernel policy |

The directory allocates 4,096-entry pages per `(market, channel)` and native
sequence page. A page is 64 KiB whether it contains one winner or 4,096.
Six hundred million densely packed occupied slots require at least 146,485
pages, whose slot storage is 8.9407 GiB. That value is a dense lower bound,
not an upper bound: catalog filtering and channel sequence gaps can leave
pages partially occupied. The independent production guard of 262,144 pages
caps page slot storage at 16 GiB. Hash tables, page objects, alignment, and
allocator metadata add overhead, and the process fails closed if either the
record or page guard is reached. The page guard must be recalculated from the
real `(market, channel, native_sequence / 4096)` cardinality when feed or
catalog coverage changes.

KLine no longer retains a full `CanonicalTick` per fact after its micro-batch.
Event retains a compact handle and projection/repair metadata, but its fact
metadata, order histories, role cache, bundle cache, event heads, and indexes
still grow with the state needed for intraday repair. Moving the canonical
payload to disk therefore removes the largest duplicated payload, but it is
not a claim that all Event trading-day state has become bounded or external.

## 4. Trading-day sizing

For `N` distinct winners, the exact journal file size after all appends is:

```text
file_bytes = 32 + 344 * N
```

| Record count | File bytes | GiB |
|---:|---:|---:|
| 474,485,838 | 163,223,128,304 | 152.0134 |
| 600,000,000 | 206,400,000,032 | 192.2250 |

The production cap of 600,000,000 records gives `26.4527%` count headroom over
the observed day. It is a logical maximum, not a physical preallocation. The
journal grows as facts arrive; an I/O error or exhausted filesystem makes it
sticky unhealthy and stops projection. Production storage must therefore
budget at least the full 206.4 GB file plus filesystem free-space policy,
operational reserve, and any concurrent workload. Capacity planning must be
repeated from the complete owner/channel distribution when the catalog or feed
mix changes.

The sustained record-write bandwidth, before filesystem metadata and device
effects, is:

| Message rate | Journal bytes/s | MiB/s |
|---:|---:|---:|
| 800,000/s | 275,200,000 | 262.4512 |
| 1,000,000/s | 344,000,000 | 328.0640 |

Because Event and KLine share the record, enabling both does not double these
file bytes. It does add classification, exact comparison, projection, cache,
and consumer-state work.

## 5. Duplicate and read latency

The directory fingerprint is only a fast negative test. A matching
fingerprint always loads the authoritative winner and performs the complete
business-payload comparison, so a 64-bit collision cannot change duplicate or
conflict semantics.

`AdmitBatch` can fill a caller-owned output span with the complete authoritative
winner for every `kNew` or `kDuplicate` result. Event and KLine use this output
for normal first-winner projection, so they do not reacquire the journal mutex,
look up the decoded LRU, and copy the same tick through a separate `Read` call
for each accepted fact. The output span is optional; when supplied, it must be
non-overlapping and have exactly the same number of entries as the input span.
A size mismatch makes the journal unhealthy and fails closed. Conflict and
failed results do not promise an output value, so callers must inspect the
corresponding `AdmitResult` before consuming a winner.

An evicted historical Event repair or cold duplicate still performs a
synchronous `pread`, validates the record header, CRC32C, padding, and decoded
enum/length bounds, and then caches the decoded tick. Pending-range and handle
checks, directory publication, cache mutation, and counter merges remain under
the shared mutex, but the immutable record `pread` and decode run outside it.
Admission similarly prefetches cold authoritative winners outside the mutex
and re-scans before its final locked classification. Concurrent readers of the
same cold offset may still issue duplicate reads; this implementation does not
provide per-offset read single-flight or asynchronous prefetch.

`DropFileCache()` is a benchmark aid that performs a successful `Flush()`
precondition check and issues `POSIX_FADV_DONTNEED`. The advice is not a proof
that a subsequent read reached physical media. Benchmark output therefore
labels such samples as application-cache-cleared and file-cache-drop-advised,
not as guaranteed device misses.

## 6. Recovery and operational boundary

Creation starts a new empty file for exactly one trade date. It does not scan,
reopen, replay, or repair an earlier journal. This journal is a local
process-lifetime spill/cache, not a crash-recovery log or checkpoint. Durable
`raw_tick` data in ClickHouse remains the reconstruction source of truth.

On orderly shutdown, `mdl_ingestd` first quiesces and flushes Event/KLine,
waits for journal writes, executes `fdatasync`, and then stops the raw sink.
A crash before that boundary can leave an unusable local file; the next run
starts a new file and must rebuild according to the separately defined raw
replay procedure. This repository still does not implement that replay or an
intraday derived-state checkpoint.

The production path should be on storage isolated from ClickHouse merge I/O.
Operators must monitor journal health, records, bytes, partial I/O, errors,
free space, device latency, and the remaining record budget. A supervisor may
remove or rotate a completed process-lifetime file only after the process has
closed it and the durable raw retention/rebuild policy permits removal.
