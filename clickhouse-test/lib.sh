#!/usr/bin/env bash

CLICKHOUSE_BASE="$({
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
    pwd
})"

CLICKHOUSE_BINARY="$CLICKHOUSE_BASE/clickhouse"
CLICKHOUSE_CONFIG="$CLICKHOUSE_BASE/config/config.xml"
CLICKHOUSE_PID_FILE="$CLICKHOUSE_BASE/run/clickhouse.pid"
CLICKHOUSE_STATUS_FILE="$CLICKHOUSE_BASE/data/status"
CLICKHOUSE_CONSOLE_LOG="$CLICKHOUSE_BASE/logs/server-console.log"
CLICKHOUSE_HTTP_URL="http://127.0.0.1:8123"
CLICKHOUSE_NATIVE_PORT="19000"

clickhouse_prepare_directories() {
    mkdir -p \
        "$CLICKHOUSE_BASE/data" \
        "$CLICKHOUSE_BASE/format_schemas" \
        "$CLICKHOUSE_BASE/logs" \
        "$CLICKHOUSE_BASE/run" \
        "$CLICKHOUSE_BASE/tmp" \
        "$CLICKHOUSE_BASE/user_files"
}

clickhouse_http_ready() {
    curl --noproxy '*' --fail --silent --show-error \
        --max-time 2 "$CLICKHOUSE_HTTP_URL/ping" \
        >/dev/null 2>&1
}

clickhouse_pid_is_live() {
    local pid="${1:-}"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1

    kill -0 "$pid" 2>/dev/null || [[ -d "/proc/$pid" ]]
}

clickhouse_pid_is_this_instance() {
    local pid="${1:-}"
    local expected_exe actual_exe

    clickhouse_pid_is_live "$pid" || return 1
    [[ -r "/proc/$pid/exe" && -r "/proc/$pid/cmdline" ]] || return 1

    expected_exe="$(readlink -f -- "$CLICKHOUSE_BINARY")"
    actual_exe="$(readlink -f -- "/proc/$pid/exe")"
    [[ "$actual_exe" == "$expected_exe" ]] || return 1

    tr '\0' '\n' < "/proc/$pid/cmdline" \
        | grep -Fqx -- "--config-file=$CLICKHOUSE_CONFIG"
}

clickhouse_status_pid() {
    local label value

    [[ -r "$CLICKHOUSE_STATUS_FILE" ]] || return 1
    while IFS=': ' read -r label value; do
        if [[ "$label" == "PID" && "$value" =~ ^[0-9]+$ ]]; then
            printf '%s\n' "$value"
            return 0
        fi
    done < "$CLICKHOUSE_STATUS_FILE"
    return 1
}

clickhouse_recover_pid_file() {
    local pid

    pid="$(clickhouse_status_pid)" || return 1
    clickhouse_pid_is_this_instance "$pid" || return 1
    printf '%s\n' "$pid" > "$CLICKHOUSE_PID_FILE"
}

clickhouse_native_query() {
    "$CLICKHOUSE_BINARY" client \
        --host 127.0.0.1 \
        --port "$CLICKHOUSE_NATIVE_PORT" \
        --query "$1"
}
