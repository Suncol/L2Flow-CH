#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
HOST="127.0.0.1"
PORT="6379"

if ! "$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" ping >/dev/null 2>&1; then
    echo "Redis is not running at $HOST:$PORT"
    exit 0
fi

"$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" shutdown nosave

for _ in {1..50}; do
    if ! "$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" ping >/dev/null 2>&1; then
        echo "Redis stopped"
        exit 0
    fi
    sleep 0.1
done

echo "Redis did not stop within 5 seconds" >&2
exit 1
