#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
HOST="127.0.0.1"
PORT="6379"

cd "$ROOT_DIR"

if "$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" ping >/dev/null 2>&1; then
    echo "Redis is already running at $HOST:$PORT"
    exit 0
fi

"$ROOT_DIR/bin/redis-server" "$ROOT_DIR/conf/redis.conf"

for _ in {1..50}; do
    if "$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" ping >/dev/null 2>&1; then
        echo "Redis started at $HOST:$PORT (pid $(<run/redis.pid))"
        exit 0
    fi
    sleep 0.1
done

echo "Redis did not become ready; recent log output:" >&2
tail -n 30 "$ROOT_DIR/log/redis.log" >&2 || true
exit 1
