#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

clickhouse_prepare_directories
cd "$CLICKHOUSE_BASE"

if [[ ! -x "$CLICKHOUSE_BINARY" ]]; then
    echo "ClickHouse binary is missing: $CLICKHOUSE_BINARY" >&2
    exit 1
fi

if [[ ! -s "$CLICKHOUSE_PID_FILE" ]]; then
    clickhouse_recover_pid_file || true
fi

if [[ -s "$CLICKHOUSE_PID_FILE" ]]; then
    existing_pid="$(<"$CLICKHOUSE_PID_FILE")"

    if clickhouse_pid_is_this_instance "$existing_pid"; then
        echo "ClickHouse is already running: PID=$existing_pid"
        exit 0
    fi

    if clickhouse_pid_is_live "$existing_pid"; then
        echo "PID file points to another live process: PID=$existing_pid" >&2
        echo "Refusing to overwrite $CLICKHOUSE_PID_FILE" >&2
        exit 1
    fi

    rm -f -- "$CLICKHOUSE_PID_FILE"
fi

# A stale mirror can coexist with a valid ClickHouse 26 data/status file.
# Re-check the authoritative status after removing the stale mirror so a
# second server is never launched for this data directory.
if [[ ! -s "$CLICKHOUSE_PID_FILE" ]] && clickhouse_recover_pid_file; then
    recovered_pid="$(<"$CLICKHOUSE_PID_FILE")"
    echo "ClickHouse is already running: PID=$recovered_pid"
    exit 0
fi

if ! env CLICKHOUSE_WATCHDOG_ENABLE=0 \
    "$CLICKHOUSE_BINARY" server \
    "--config-file=$CLICKHOUSE_CONFIG" \
    --daemon \
    >> "$CLICKHOUSE_CONSOLE_LOG" 2>&1; then

    echo "ClickHouse daemon launch failed." >&2
    tail -n 100 "$CLICKHOUSE_CONSOLE_LOG" >&2 || true
    exit 1
fi

for _ in $(seq 1 100); do
    pid=""
    if [[ -s "$CLICKHOUSE_PID_FILE" ]]; then
        pid="$(<"$CLICKHOUSE_PID_FILE")"
    elif clickhouse_recover_pid_file; then
        pid="$(<"$CLICKHOUSE_PID_FILE")"
    fi

    if clickhouse_pid_is_this_instance "$pid" && clickhouse_http_ready; then
        version="$(clickhouse_native_query 'SELECT version()')"
        echo "ClickHouse started: PID=$pid, version=$version"
        echo "HTTP:   http://127.0.0.1:8123"
        echo "Native: 127.0.0.1:$CLICKHOUSE_NATIVE_PORT"
        exit 0
    fi

    if [[ -n "$pid" ]] && ! clickhouse_pid_is_live "$pid"; then
        rm -f -- "$CLICKHOUSE_PID_FILE"
        echo "ClickHouse exited during startup." >&2
        tail -n 100 "$CLICKHOUSE_CONSOLE_LOG" >&2 || true
        exit 1
    fi

    sleep 0.1
done

echo "ClickHouse startup health check timed out: PID=${pid:-unknown}" >&2
tail -n 100 "$CLICKHOUSE_CONSOLE_LOG" >&2 || true
exit 1
