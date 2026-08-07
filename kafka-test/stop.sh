#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [[ ! -s "$KAFKA_PID_FILE" ]]; then
    if kafka_broker_ready; then
        echo "A Kafka broker is reachable, but this instance has no PID file." >&2
        echo "Refusing to stop an unmanaged process." >&2
        exit 1
    fi

    echo "Kafka is not running."
    exit 0
fi

pid="$(<"$KAFKA_PID_FILE")"

if ! kafka_pid_is_live "$pid"; then
    rm -f -- "$KAFKA_PID_FILE"
    echo "Removed stale PID file; Kafka is not running."
    exit 0
fi

if ! kafka_pid_is_this_instance "$pid"; then
    echo "PID $pid does not belong to this Kafka instance." >&2
    echo "Refusing to send a signal." >&2
    exit 1
fi

kill -TERM "$pid"

for _ in $(seq 1 300); do
    if ! kafka_pid_is_live "$pid"; then
        rm -f -- "$KAFKA_PID_FILE"
        echo "Kafka stopped."
        exit 0
    fi

    if [[ -r "/proc/$pid/stat" ]] && [[ "$(cut -d ' ' -f 3 "/proc/$pid/stat")" == "Z" ]]; then
        rm -f -- "$KAFKA_PID_FILE"
        echo "Kafka stopped."
        exit 0
    fi

    sleep 0.1
done

echo "Kafka is still stopping: PID=$pid" >&2
echo "Inspect $KAFKA_CONSOLE_LOG before taking further action." >&2
exit 1
