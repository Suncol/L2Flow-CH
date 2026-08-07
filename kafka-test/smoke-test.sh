#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

kafka_require_installation

if ! kafka_broker_ready; then
    echo "Kafka is not ready at $KAFKA_BOOTSTRAP_SERVER" >&2
    echo "Run $KAFKA_BASE/start.sh first." >&2
    exit 1
fi

topic="kafka-smoke-$(date +%s)-$$"
payload="kafka-smoke-payload-$RANDOM-$$"
consumer_stderr="$KAFKA_RUN_DIR/smoke-consumer.stderr"

cleanup() {
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --delete --topic "$topic" >/dev/null 2>&1 || true
    rm -f -- "$consumer_stderr"
}
trap cleanup EXIT

"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --create --topic "$topic" \
    --partitions 1 --replication-factor 1 >/dev/null

printf '%s\n' "$payload" | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --topic "$topic" >/dev/null

received="$(timeout 20 "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --topic "$topic" --from-beginning --max-messages 1 \
    2>"$consumer_stderr")"

if [[ "$received" != "$payload" ]]; then
    echo "Kafka round-trip mismatch." >&2
    echo "Expected: $payload" >&2
    echo "Actual:   $received" >&2
    cat "$consumer_stderr" >&2
    exit 1
fi

echo "Kafka smoke test passed: produced and consumed one record via $topic"
