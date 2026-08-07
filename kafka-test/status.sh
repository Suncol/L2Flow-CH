#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

kafka_require_installation

process_ok=0
broker_ok=0

if [[ -s "$KAFKA_PID_FILE" ]]; then
    pid="$(<"$KAFKA_PID_FILE")"

    if kafka_pid_is_this_instance "$pid"; then
        echo "Process: running, PID=$pid"
        process_ok=1
    elif kafka_pid_is_live "$pid"; then
        echo "Process: PID file belongs to another process, PID=$pid"
    else
        echo "Process: stale PID file, PID=$pid"
    fi
else
    echo "Process: no PID file"
fi

if kafka_broker_ready; then
    version="$($KAFKA_HOME/bin/kafka-topics.sh --version | awk '{print $1}')"
    cluster_id="$(sed -n 's/^cluster.id=//p' "$KAFKA_DATA_DIR/meta.properties" 2>/dev/null || true)"
    echo "Broker:  ready at $KAFKA_BOOTSTRAP_SERVER"
    echo "Version: $version"
    echo "Cluster: ${cluster_id:-unknown}"
    broker_ok=1
else
    echo "Broker:  unavailable at $KAFKA_BOOTSTRAP_SERVER"
fi

if (( process_ok == 1 && broker_ok == 1 )); then
    exit 0
fi

exit 1
