#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if ! clickhouse_http_ready; then
    echo "ClickHouse is unavailable; run $CLICKHOUSE_BASE/start.sh first." >&2
    exit 1
fi

"$CLICKHOUSE_BINARY" client \
    --host 127.0.0.1 \
    --port "$CLICKHOUSE_NATIVE_PORT" \
    --multiquery \
    < "$CLICKHOUSE_BASE/smoke-test.sql"

actual_sequences="$(clickhouse_native_query \
    'SELECT sequence FROM l2test.raw_tick ORDER BY event_time, sequence FORMAT TSVRaw')"
expected_sequences=$'1\n2\n3'

if [[ "$actual_sequences" != "$expected_sequences" ]]; then
    echo "Unexpected event-time ordering:" >&2
    printf '%s\n' "$actual_sequences" >&2
    exit 1
fi

row_count="$(clickhouse_native_query 'SELECT count() FROM l2test.raw_tick')"
if [[ "$row_count" != "3" ]]; then
    echo "Unexpected row count: $row_count" >&2
    exit 1
fi

echo "Smoke test passed: MergeTree row_count=3, sequence_order=1,2,3"
