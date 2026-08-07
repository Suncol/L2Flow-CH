#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [[ ! -s "$CLICKHOUSE_PID_FILE" ]]; then
    clickhouse_recover_pid_file || true
fi

if [[ ! -s "$CLICKHOUSE_PID_FILE" ]]; then
    if clickhouse_http_ready; then
        echo "HTTP is available, but this instance has no PID file." >&2
        echo "Refusing to stop an unmanaged process." >&2
        exit 1
    fi

    echo "ClickHouse is not running."
    exit 0
fi

pid="$(<"$CLICKHOUSE_PID_FILE")"

if ! clickhouse_pid_is_live "$pid"; then
    rm -f -- "$CLICKHOUSE_PID_FILE"
    echo "Removed stale PID file; ClickHouse is not running."
    exit 0
fi

if ! clickhouse_pid_is_this_instance "$pid"; then
    echo "PID $pid does not belong to this ClickHouse instance." >&2
    echo "Refusing to send a signal." >&2
    exit 1
fi

kill -TERM "$pid"

for _ in $(seq 1 300); do
    if ! clickhouse_pid_is_live "$pid"; then
        rm -f -- "$CLICKHOUSE_PID_FILE"
        echo "ClickHouse stopped."
        exit 0
    fi

    if [[ -r "/proc/$pid/stat" ]] && [[ "$(cut -d ' ' -f 3 "/proc/$pid/stat")" == "Z" ]]; then
        rm -f -- "$CLICKHOUSE_PID_FILE"
        echo "ClickHouse stopped."
        exit 0
    fi

    sleep 0.1
done

echo "ClickHouse is still stopping: PID=$pid" >&2
echo "Inspect $CLICKHOUSE_CONSOLE_LOG before taking further action." >&2
exit 1
