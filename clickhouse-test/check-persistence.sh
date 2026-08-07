#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if ! clickhouse_http_ready; then
    echo "ClickHouse is unavailable; run $CLICKHOUSE_BASE/start.sh first." >&2
    exit 1
fi

row_count="$(clickhouse_native_query 'SELECT count() FROM l2test.raw_tick')"
sequences="$(clickhouse_native_query \
    'SELECT sequence FROM l2test.raw_tick ORDER BY event_time, sequence FORMAT CSV')"

if [[ "$row_count" != "3" || "$sequences" != $'1\n2\n3' ]]; then
    echo "Persistence check failed: row_count=$row_count" >&2
    printf 'Sequences:\n%s\n' "$sequences" >&2
    exit 1
fi

echo "Persistence check passed: row_count=3, sequence_order=1,2,3"
