# Local Redis

This directory contains an unprivileged Redis 8.10.0 deployment. It listens
only on `127.0.0.1:6379` and has RDB and AOF persistence disabled.

## Lifecycle

```bash
./redis-test/start.sh
./redis-test/status.sh
./redis-test/stop.sh
```

## CLI

```bash
./redis-test/bin/redis-cli -h 127.0.0.1 -p 6379 ping
./redis-test/bin/redis-cli -h 127.0.0.1 -p 6379
```

## Python client

The `python/` uv project includes the official `redis` client with the
`hiredis` parser. From the repository root, synchronize the locked environment
and run the checked client example:

```bash
uv sync --project python --locked
uv run --project python --locked python redis-test/test_redis.py
```

The example checks `PING`, server metadata, and one namespaced round trip. Its
temporary key has a 30-second expiry and is deleted before the client exits.

## Benchmark

```bash
./redis-test/bin/redis-benchmark \
  -h 127.0.0.1 \
  -p 6379 \
  -c 50 \
  -n 1000000 \
  -P 16 \
  -t set,get \
  -q
```

Runtime files stay under `data/`, `log/`, and `run/`. Relative runtime paths
are resolved by `start.sh`, so use that script to start the background server.

This configuration has no password because it is bound exclusively to the
loopback interface. Do not change the bind address without adding ACL or
password authentication and reviewing firewall exposure.
