#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

kafka_require_installation
kafka_require_java
kafka_prepare_directories
cd "$KAFKA_BASE"

if [[ -s "$KAFKA_PID_FILE" ]]; then
    existing_pid="$(<"$KAFKA_PID_FILE")"

    if kafka_pid_is_this_instance "$existing_pid"; then
        if kafka_broker_ready; then
            echo "Kafka is already running: PID=$existing_pid, broker=$KAFKA_BOOTSTRAP_SERVER"
            exit 0
        fi

        echo "Kafka process exists but the broker is not ready: PID=$existing_pid" >&2
        echo "Inspect $KAFKA_CONSOLE_LOG" >&2
        exit 1
    fi

    if kafka_pid_is_live "$existing_pid"; then
        echo "PID file points to another live process: PID=$existing_pid" >&2
        echo "Refusing to overwrite $KAFKA_PID_FILE" >&2
        exit 1
    fi

    rm -f -- "$KAFKA_PID_FILE"
fi

if kafka_port_is_open; then
    echo "Port 127.0.0.1:9092 is already in use by an unmanaged process." >&2
    exit 1
fi

"$KAFKA_BASE/initialize.sh"

nohup env LOG_DIR="$KAFKA_LOG_DIR" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" \
    > "$KAFKA_CONSOLE_LOG" 2>&1 &

kafka_pid=$!
printf '%s\n' "$kafka_pid" > "$KAFKA_PID_FILE"

for _ in $(seq 1 120); do
    if ! kafka_pid_is_live "$kafka_pid"; then
        rm -f -- "$KAFKA_PID_FILE"
        echo "Kafka exited during startup; recent log output:" >&2
        tail -n 100 "$KAFKA_CONSOLE_LOG" >&2 || true
        exit 1
    fi

    if kafka_pid_is_this_instance "$kafka_pid" && kafka_broker_ready; then
        echo "Kafka started: PID=$kafka_pid, broker=$KAFKA_BOOTSTRAP_SERVER"
        echo "Data: $KAFKA_DATA_DIR"
        echo "Log:  $KAFKA_CONSOLE_LOG"
        exit 0
    fi

    sleep 0.25
done

echo "Kafka startup health check timed out: PID=$kafka_pid" >&2
echo "Inspect $KAFKA_CONSOLE_LOG" >&2
exit 1
