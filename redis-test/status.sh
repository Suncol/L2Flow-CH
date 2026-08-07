#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
HOST="127.0.0.1"
PORT="6379"

if ! "$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" ping >/dev/null 2>&1; then
    echo "Redis is not running at $HOST:$PORT"
    exit 1
fi

echo "Redis is running at $HOST:$PORT"
"$ROOT_DIR/bin/redis-cli" -h "$HOST" -p "$PORT" info server \
    | tr -d '\r' \
    | sed -n -e '/^redis_version:/p' -e '/^process_id:/p' -e '/^uptime_in_seconds:/p'
