# L2Flow-CH `mdl_ingestd`

This repository implements the first realtime market-data process boundary:

```text
MDL SDK callback
  -> bounded minimal admission
  -> channel/instrument-sharded decoder lanes
  -> exchange-native channel sequence recovery
  -> fixed-width canonical records
  -> instrument-owner dispatch queues + gap/LateRecovery controls
```

It deliberately does **not** implement Event/KLine workers, shared-memory
Arrow rings, Kafka/Redpanda, ClickHouse, or intraday restart recovery yet.
The supported startup modes are only `from-open` and `partial`.

## Build and test

The default SDK include path points at the sibling reference checkout. It can
be overridden with `-DL2FLOW_CH_SDK_INCLUDE_DIR=/path/to/include`.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Optional checks:

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DL2FLOW_CH_ENABLE_ASAN_UBSAN=ON \
  -DL2FLOW_CH_BUILD_BENCHMARKS=OFF
cmake --build build-asan -j
ASAN_OPTIONS=detect_leaks=0 ./build-asan/test_mdl_ingest
```

Leak detection is disabled in the example only because LeakSanitizer cannot
run under the ptrace-based Codex environment; it should be enabled in normal
CI.

## Stream selection

Stream selection is configuration-driven. By default `mdl_ingestd` reads
`config/production.streams.conf`, which selects the five implemented L2
message families used for Shanghai/Shenzhen A-share processing.
`--stream-config FILE` overrides that path. The file contains one exact tuple
per line; blank lines and full-line comments are accepted:

```text
4.101.4
4.101.24
6.101.28
6.101.33
6.101.36
```

Only tuples with a decoder in this build are accepted. The example is
[config/production.streams.conf](config/production.streams.conf). There is no
tuple-specific hard-fail branch: an unconfigured tuple is not subscribed, a
configured-but-unimplemented tuple is rejected during configuration, and an
unexpected runtime tuple is reported as `unsupported_message`.

This tuple selection controls message families, not the security universe
inside a vendor stream. The exact A-share universe is the supplied daily
instrument catalog: messages for identities absent from that catalog are not
dispatched. Therefore the implementation does not claim that a tuple-level
SDK subscription suppresses every non-A-share packet at the network edge.
Production launch units should pass an absolute `--stream-config` path rather
than depend on their working directory.

## Validate a configuration

```bash
./build/mdl_ingestd \
  --mode partial \
  --trade-date 20260806 \
  --catalog config/catalog.example.csv \
  --validate-only
```

Physical runs default to `--operation-mode live`. Live mode is unbounded,
does not accept `--run-seconds`, and does not allocate or publish the test
latency sampler. Use test mode only for a bounded operational measurement:

```bash
./build/mdl_ingestd \
  --mode partial \
  --operation-mode test \
  --run-seconds 300 \
  --trade-date 20260806 \
  --catalog config/catalog.example.csv \
  --sdk-library /path/to/libmdl_api.so \
  --server 127.0.0.1:9112 \
  --user l2flow-measurement \
  --allow-discard-after-dispatch
```

Test mode reports callback, admission, and dispatch throughput plus a 1-in-64
sample of callback-entry-to-dispatch-drain latency. Production launch units
must use live mode and external process supervision for lifecycle control.

The catalog uses exact, untrimmed identities:

```text
instrument_id,market,security_id_source,security_id
1,SH,,600000
2,SZ,102,000001
```

The current executable has no production sink after instrument dispatch. A
physical SDK run therefore requires `--allow-discard-after-dispatch`, making
the temporary drain behavior explicit rather than silently discarding data.
The core library exposes `TryPollTick`, `TryPollSnapshot`, `TryPollGap`,
`TryPollLateRecovery`, and `TryPollChannelFault` for the next-stage workers.
In either startup mode, a native record arriving behind the already-published
frontier is never inserted backward into the realtime ordered stream; its
canonical body is sent to `LateRecovery` for later reconciliation. This is
not a raw durable copy: the Kafka/Redpanda raw path remains outside this
milestone.

`from-open` and `partial` both default to a 500,000 ns gap wait. The two
settings remain independent (`--from-open-gap-wait-ns` and
`--partial-gap-wait-ns`) for measured production tuning; `from-open` does not
receive a longer default merely because upstream/network backfill is possible.

The full contract and deployment guidance are in
[docs/mdl-ingestd-design.md](docs/mdl-ingestd-design.md).

## NUMA-pinned ingest benchmark

The synthetic benchmark enters through `MdlMessageHandler::OnMessage` and
measures from the first timestamp inside that callback until an instrument
owner polls the canonical tick from its dispatch endpoint. It covers SDK
message access, header admission, body copy, full decode/normalization,
Channel recovery, and the lane-by-owner dispatch matrix. It does not include
the physical MDL network/SDK path before callback entry.

Example for the tested NUMA node 1 layout on a 64-physical-core host:

```bash
numactl --physcpubind=32-63 --membind=1 \
  ./build/benchmark_mdl_ingest \
  --rate 1200000 --seconds 300 --warmup-seconds 5 \
  --pattern ordered --sample-every 67 \
  --channels 16 --tick-lanes 12 --owners 16 \
  --producer-cpu 63 --first-consumer-cpu 32 \
  --first-decoder-cpu 48
```

Use `--pattern local-reverse --reorder-window 8` to inject bounded local
out-of-order delivery independently within every Channel. `--gap-wait-ns`
is an explicit benchmark override; omitting it preserves the production
FROM_OPEN default of 500,000 ns. When decoder CPU affinity is configured,
decoder idle loops use pause-spin instead of yielding, so each configured
decoder, including an idle snapshot decoder in this benchmark, consumes a
dedicated logical CPU by design.

The complete five-minute 800k/1.0M/1.2M msg/s results, measurement contract,
quality counters, NUMA placement, default-gap boundary test, and production
caveats are in
[docs/numa-stress-report.md](docs/numa-stress-report.md).
