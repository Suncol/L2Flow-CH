# Validated Morning Live Parameters

Saved on 2026-08-12 from the live full-pipeline run started at 14:54 CST.
Use [`live-morning-validated.args.sh`](live-morning-validated.args.sh) for the
next morning run.

## Fixed parameters

- Runtime: 10,800 seconds (180 minutes).
- SDK work threads: 1. Keep this value to preserve SDK callback seriality.
- Dispatch queue capacity: 32,768.
- Raw ClickHouse: 8 writers, 64 queued batches per lane, 500 ms Tick batch
  maximum delay. Expected canonical raw preallocation is 5,827,461,120 bytes.
- Event: 8 writer lanes, 262,144 pending commits, 4,194,304 revision batches,
  33,554,432 revision rows.
- Event carry orders: 16,777,216 per owner; order-history logical cap: 16 GiB
  per owner. The 2026-08-14 from-open run exhausted the previous 4,194,304
  order bound at 184,110,241 Event facts. Scaling that observation to the
  recorded 474,485,838-fact day projects about 10.81M orders for the busiest
  owner, leaving about 55% count headroom and 2.3x projected byte headroom.
- Shanghai END expansion: 8,388,608 candidates and projected rows per owner,
  with 8 GiB repair and END-staging caps.
- FactJournal: 800,000,000 records and 524,288 sparse directory pages. These
  provide 68.60% record headroom over the recorded 474,485,838-fact day and
  bound record plus page payload to about 288 GiB.
- KLine: 32 writer lanes, 65,536 pending commits, 65,536 revision batches,
  16,777,216 revision rows, 1,024-row micro-batches, 200 ms maximum delay.
- Base profile: `config/current-server.production.conf`.
- Validated binary SHA-256:
  `6a26efef951516bf687503f146cbe837b2a5def1918e1e5d974850d3d01b8c74`.

## Per-run values

Never copy a prior run's identity or storage paths. Generate all of these for
the new trade date/run:

- `L2FLOW_TRADE_DATE`, `L2FLOW_START_MODE`, and catalog path.
- Unique `L2FLOW_MDL_USER`.
- Unique source, Event, and KLine 16-byte hexadecimal IDs.
- Unique raw feed epoch and Event/KLine revision epochs.
- Fresh ClickHouse database and complete raw/Event/KLine schema.
- Fresh nonexistent Arrow root and fact-journal path.
- The Arrow consumers must use the exact raw feed epoch from the producer and
  cover owner ranges `0:4`, `4:8`, `8:12`, `12:16`, `16:20`, `20:24`,
  `24:28`, and `28:32`.

## Startup invariants

Before starting, run `pgrep -a -x mdl_ingestd` and proceed only when it prints
nothing. Validate the exact final command with `--validate-only`, then launch
one systemd transient unit with `Restart=no`. Never stop a live instance to
change parameters; allow its `--run-seconds` timer or a process failure to end
it.

Immediately after startup, verify `mdl_feed_ready_monotonic_ns` and
`clickhouse_preallocated_canonical_bytes=5827461120` in the pipeline log before
starting the eight Arrow consumers. Monitor every 30 seconds for throughput,
latency, ACK/unacked counts, Event/KLine pending and queues, retry/unknown,
Arrow overrun/error, process/cgroup RSS, journal size, and free disk.

## Shell usage

The per-run launcher sets `ARROW_ROOT`, `DATABASE`, and all required
`L2FLOW_*` environment values, then loads the fixed argument array:

```bash
ARROW_ROOT=/dev/shm/l2flow/arrow-live-NEW
DATABASE=l2flow_live_NEW
source config/live-morning-validated.args.sh
build-release/mdl_ingestd "${L2FLOW_VALIDATED_LIVE_ARGS[@]}" --validate-only
```

The saved source run remains at
`artifacts/live-20260812-full-pipeline-180m-1452/run-ingest.sh` for exact audit.
