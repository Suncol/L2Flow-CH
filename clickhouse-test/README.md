# Repository-local ClickHouse

This directory contains a localhost-only, single-binary ClickHouse instance
for the C++ SDK callback -> normalize -> ClickHouse -> Python/PyArrow prototype.
It does not install system packages, create a system service, or write to
`/etc`, `/var/lib`, or `/var/log`.

## Installed binary

The local ignored file `clickhouse` is the official x86_64 build downloaded
from `https://clickhouse.com/` with `CLICKHOUSE_ONLY=1`. Its installed version
and post-decompression SHA-256 are recorded in the ignored files
`CLICKHOUSE_VERSION.txt` and `clickhouse.sha256.local`.

Verify them from this directory:

```bash
./clickhouse server --version
sha256sum -c clickhouse.sha256.local
```

The first execution of the official download may expand its packed executable
on disk. The hash in this directory is for the expanded executable that is
actually run.

## Lifecycle

Run every command from any working directory; the scripts resolve this
directory themselves:

```bash
./clickhouse-test/start.sh
./clickhouse-test/status.sh
./clickhouse-test/stop.sh
./clickhouse-test/restart.sh
```

The configured interfaces are:

```text
HTTP:      127.0.0.1:8123
Native TCP: 127.0.0.1:19000
user:      default
password:  empty
```

Native port 19000 is intentional: another local service on this host already
owns the standard port 9000. HTTP remains on the standard port 8123.

The empty-password user is restricted to IPv4/IPv6 loopback. The MySQL,
PostgreSQL, and interserver HTTP listeners from the upstream template are
disabled for this test instance. Do not change `listen_host` to a wildcard
without also configuring authentication, TLS, and host firewall rules.

Runtime state is isolated under this directory:

```text
data/            persistent ClickHouse metadata and parts
tmp/             temporary query data
logs/            ClickHouse and console logs
run/             PID file
user_files/      files exposed to the file() table function
format_schemas/  external format schemas
```

ClickHouse 26 records its authoritative process metadata in `data/status`.
The lifecycle scripts validate that PID against the exact binary and config,
then mirror it into `run/clickhouse.pid`; a stale status file is never trusted
without that process-identity check.

These runtime paths and the binary are intentionally ignored by Git.

## SQL acceptance and persistence

Start the server and run the idempotent MergeTree acceptance test:

```bash
./clickhouse-test/start.sh
./clickhouse-test/smoke-test.sh
```

The test recreates `l2test.raw_tick`, inserts sequences `3, 1, 2`, and checks
that an explicit event-time query returns `1, 2, 3`. It is destructive only to
the dedicated `l2test.raw_tick` smoke-test table.

Verify restart persistence:

```bash
./clickhouse-test/restart.sh
./clickhouse-test/check-persistence.sh
```

Direct HTTP and Native queries:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:8123/ping
./clickhouse-test/clickhouse client \
  --host 127.0.0.1 --port 19000 \
  --query 'SELECT version(), now()'
```

Always include a query `ORDER BY` when result order is part of the contract;
the MergeTree sorting key does not guarantee implicit query result order.

## Python and Arrow environment

The `python/pyproject.toml` project requires `clickhouse-connect` with its
Arrow extra and pins `pyarrow` to the same Arrow 25 major version used by the
shared-memory reader. From the repository root, create or exactly synchronize
the uv-managed environment:

```bash
uv sync --project python --locked
```

This creates `python/.venv` using the minor version in
`python/.python-version`; `python/uv.lock` is the exact dependency record. Do
not install packages into this environment with `pip`.

After starting ClickHouse and running `smoke-test.sh`, execute the Python Arrow
integration check:

```bash
uv run --project python --locked \
  python clickhouse-test/test_clickhouse.py
```

The example adds loopback hosts to `NO_PROXY` inside its own process so a
machine-wide HTTP proxy cannot intercept the local ClickHouse connection. It
uses ClickHouse's Arrow response format and verifies the returned
`pyarrow.Table`, Arrow integer types, row count, and explicit sequence order.

`clickhouse-connect` uses HTTP port 8123. A C++ Native-protocol driver should
use port 19000 for this host. For HTTP INSERTs, use batches and a ClickHouse
bulk format; do not issue one INSERT request per market-data record.

The uv dependency covers Python's PyArrow runtime and ClickHouse Arrow response
decoding. The C++ build still resolves an Arrow 25 development installation
through `find_package(Arrow 25 CONFIG REQUIRED)`; the PyArrow wheel does not
provide `ArrowConfig.cmake`. Keep both layers on Arrow major version 25 and
validate both test suites when upgrading.

## Logs and disk usage

```bash
tail -f clickhouse-test/logs/clickhouse-server.log
du -sh clickhouse-test/data
./clickhouse-test/clickhouse client \
  --host 127.0.0.1 --port 19000 --query \
  "SELECT database, table, sum(rows), formatReadableSize(sum(bytes_on_disk))
   FROM system.parts WHERE active GROUP BY database, table"
```
