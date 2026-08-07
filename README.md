# L2Flow-CH `mdl_ingestd`

This repository implements the first realtime market-data process boundary:

```text
MDL SDK callback
  -> bounded minimal admission
  -> channel/instrument-sharded decoder lanes
  -> decode-complete raw tap -> preallocated canonical batches
       -> ClickHouse writer threads -> raw_tick/raw_snapshot MergeTree
  -> exchange-native channel sequence recovery
  -> fixed-width canonical records
  -> instrument-owner dispatch queues + gap/LateRecovery controls
  -> per-owner shared-memory Arrow rings + Python/Polars reader
```

`raw_tick` and `raw_snapshot` are the first durable ClickHouse boundary in this
repository. The raw tap runs after complete decode/normalization and before
SequenceRecovery, duplicate/conflict handling, and catalog-miss suppression.
The shared-memory Arrow branch remains an independent bounded volatile hot
path. Event/KLine workers and historical reconciliation remain outside this
milestone. The supported startup modes are `from-open` and `partial`.

## Build and test

The default SDK include path points at the sibling reference checkout. It can
be overridden with `-DL2FLOW_CH_SDK_INCLUDE_DIR=/path/to/include`.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Python environment

The shared-memory reader and local ClickHouse/Redis clients use one uv-managed
Python 3.13 environment under `python/.venv`. Its Arrow major is pinned to the
C++ build:

```bash
uv sync --project python --locked
uv run --project python --locked python -c \
  'import pyarrow, clickhouse_connect, redis, polars; print(pyarrow.__version__)'
```

Use `python/uv.lock` as the dependency source of truth; do not maintain a
second `requirements.txt` or install packages into the environment with `pip`.

The read-only MDL feeder Redis latest-value API, its Polars parsing and strict
optional numeric view, and runnable query examples are documented in
[python/README.md](python/README.md).

## Local ClickHouse

The repository-local single-binary ClickHouse test instance is documented in
[clickhouse-test/README.md](clickhouse-test/README.md). It binds only to
localhost, keeps all runtime state under `clickhouse-test/`, and includes
lifecycle, MergeTree smoke-test, restart-persistence, and Python client tools.
The raw table DDL is in [clickhouse/schema/raw_tables.sql](clickhouse/schema/raw_tables.sql)
for a local node and
[clickhouse/schema/raw_tables_replicated.sql](clickhouse/schema/raw_tables_replicated.sql)
for a Keeper-backed deployment.

Run the C++ raw writer integration test against that local instance with:

```bash
./clickhouse-test/start.sh
L2FLOW_CH_TEST_URL=http://127.0.0.1:8123 \
  ./build/test_clickhouse_raw
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

For live Arrow-ring tests that must exclude snapshots, use
[`config/live-ticks.streams.conf`](config/live-ticks.streams.conf). It subscribes
only to Shanghai Tick plus Shenzhen Order and Transaction. The Arrow manifest
still contains empty Snapshot rings as part of protocol v2's fixed ring set;
`python/live_ring_probe.py` drains every Tick owner concurrently and fails if
any Snapshot row is published.

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

The executable enables durable raw writes with `--clickhouse-url` and a
nonzero `--clickhouse-feed-epoch`. A stable 128-bit source identity may be
supplied with `--clickhouse-source-instance-id`; otherwise the process creates
one and prints it. Passwords are read only through
`--clickhouse-password-env`. The optional Arrow hot path uses
`--arrow-ring-dir` and `--arrow-feed-epoch`. A physical SDK run with neither
output requires `--allow-discard-after-dispatch`, making temporary drain
behavior explicit rather than silently discarding data.

Example local raw launch arguments are:

```text
--clickhouse-url http://127.0.0.1:8123
--clickhouse-database l2flow
--clickhouse-feed-epoch <durably allocated nonzero run epoch>
--clickhouse-source-instance-id <stable 32-hex source ID>
```

For replicated production tables, provision the external DDL, add
`--clickhouse-no-auto-create`, and set the required quorum, normally
`--clickhouse-insert-quorum 2`.

An error-free return from the SDK `Connect()` call is not considered ready.
`mdl_ingestd` waits up to `--sdk-ready-timeout-seconds` (default 30) for a
successful Logon response and successful status for every configured stream;
failure creates a continuity boundary and requires a new feed session epoch.
The core library exposes `TryPollTick`, `TryPollSnapshot`, `TryPollGap`,
`TryPollLateRecovery`, and `TryPollChannelFault` for the next-stage workers.
In either startup mode, a native record arriving behind the already-published
frontier is never inserted backward into the realtime ordered stream; its
canonical body is sent to `LateRecovery` for later reconciliation. This is
separate from the raw durable copy, which already captured the decoded
occurrence before SequenceRecovery changed or diverted it.

The Arrow memory protocol, restart/disconnect contract, sizing formulas,
deployment rules, and Python API are documented in
[docs/arrow-hot-path.md](docs/arrow-hot-path.md).
The ClickHouse ACK, batching, retry, replay-order, schema, and overload
contracts are documented in
[docs/clickhouse-raw-path.md](docs/clickhouse-raw-path.md).

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

The current acceptance envelope is 500k, 750k, and 1M messages/s; the default
target is 1M messages/s. Example for the tested NUMA node 1 layout on a
64-physical-core host:

```bash
numactl --physcpubind=32-63 --membind=1 \
  ./build/benchmark_mdl_ingest \
  --rate 1000000 --seconds 300 --warmup-seconds 5 \
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

When Arrow is enabled at build time, `--arrow-ring-dir` extends the same run
through the production `ArrowHotEgress` and one concurrently pinned C++
reader/decode worker per owner. It verifies exact row counts, native sequence
continuity, CRC/protocol validity, lifecycle Control publication, and zero hot
loss. It reports both callback-to-Arrow-append and callback-to-reader-decode
latency. The required sweep is:

```text
500000 messages/s
750000 messages/s
1000000 messages/s
```

Use five-second measurement windows only as development checks. Production
qualification uses 300-second windows on the deployment NUMA/tmpfs layout and
must pass every rate without lane overflow, reader overrun, segment drop,
decode/protocol error, or ordering error. Reproducible commands and the exact
measurement boundary are in [docs/arrow-hot-path.md](docs/arrow-hot-path.md).

The earlier ingestion-only five-minute 800k/1.0M/1.2M msg/s results,
measurement contract, NUMA placement, default-gap boundary test, and
production caveats remain in
[docs/numa-stress-report.md](docs/numa-stress-report.md).
