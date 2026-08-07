#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

process_ok=0
http_ok=0

if [[ -s "$CLICKHOUSE_PID_FILE" ]]; then
    pid="$(<"$CLICKHOUSE_PID_FILE")"

    if clickhouse_pid_is_this_instance "$pid"; then
        echo "Process: running, PID=$pid"
        process_ok=1
    elif clickhouse_pid_is_live "$pid"; then
        echo "Process: PID file belongs to another process, PID=$pid"
    else
        echo "Process: stale PID file, PID=$pid"
    fi
else
    echo "Process: no PID file"
fi

if clickhouse_http_ready; then
    version="$(clickhouse_native_query 'SELECT version()')"
    echo "HTTP health: OK"
    echo "Native query: OK, version=$version"
    http_ok=1
else
    echo "HTTP health: unavailable"
fi

if (( process_ok == 1 && http_ok == 1 )); then
    exit 0
fi

exit 1
