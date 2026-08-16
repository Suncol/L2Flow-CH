# Arrow shared-memory hot path

Arrow is an optional volatile observer of the canonical WAL. It is not a
durability boundary and is not one of the raw/Event/KLine cursor owners.

```text
durable canonical WAL -> ArrowOutboxFollower -> per-owner Arrow rings
```

The follower publishes catalog-resolved ordered/hole-fill Tick dispositions,
catalog-resolved Snapshots, gap diagnostics, channel faults, and lifecycle
flushes. Recovery controls themselves are consumed by the Event/KLine actors
and are not Arrow data rows.

Because Arrow owns no durable WAL cursor, it cannot hold the reservoir or
force raw continuity failure. If it falls behind the oldest resident LSN, it
resumes from that resident frontier. A downstream user requiring replay or
authoritative completeness must use raw storage and freshness metadata, not
the shared-memory rings.

## Ring layout

The protocol retains a fixed set of per-owner Tick and Snapshot rings plus
diagnostic/control rings. Each ring uses bounded descriptors and fixed-size
shared-memory segments. Producers never publish a partially encoded Arrow IPC
batch; lifecycle metadata and CRC checks let readers reject malformed or
overwritten frames.

The manifest and ring ABI are versioned. Python readers must use the Arrow
major pinned by `python/pyproject.toml`/`python/uv.lock` and validate manifest,
schema, owner, epoch, segment, sequence, and CRC fields before consuming data.

## Configuration

Arrow shares the mandatory process `--feed-epoch`; there is no independent
Arrow feed-epoch option. Enable it with:

```text
--arrow-ring-dir /dev/shm/l2flow/arrow
--arrow-descriptors 1024
--arrow-segments 1088
--arrow-tick-segment-bytes 262144
--arrow-snapshot-segment-bytes 262144
--arrow-diagnostic-segment-bytes 131072
--arrow-max-consumers 16
--arrow-tick-batch-rows 256
--arrow-snapshot-batch-rows 16
--arrow-diagnostic-batch-rows 64
--arrow-batch-max-delay-ns 1000000
--arrow-heartbeat-interval-ns 1000000000
```

Segment and descriptor counts are hard capacity bounds. The checked-in
production profile preallocates all owner rings even for message families not
subscribed in that run, because the ABI uses a fixed ring set.

## Reader behavior

Readers should drain all assigned rings concurrently, renew their leases, and
treat a sequence discontinuity, generation change, CRC failure, overwritten
descriptor, or lifecycle boundary as loss of the volatile stream. They must
not substitute the Arrow follower's progress for a canonical WAL or
ClickHouse freshness cursor.

The local Python reader and probe tools are documented in
[python/README.md](../python/README.md).

## Shutdown

After the final WAL barrier, the process asks the follower to drain to that LSN
within the common shutdown deadline, flushes all Arrow batches, publishes the
MDL connection boundary, and seals the rings. A failed volatile drain does not
advance or alter any durable consumer cursor.
