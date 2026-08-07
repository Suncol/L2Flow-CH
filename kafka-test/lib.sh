#!/usr/bin/env bash

KAFKA_BASE="$({
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
    pwd
})"

KAFKA_VERSION="4.3.1"
KAFKA_SCALA_VERSION="2.13"
KAFKA_HOME="$KAFKA_BASE/kafka"
KAFKA_CONFIG="$KAFKA_BASE/config/server.properties"
KAFKA_DATA_DIR="$KAFKA_BASE/data"
KAFKA_LOG_DIR="$KAFKA_BASE/logs"
KAFKA_RUN_DIR="$KAFKA_BASE/run"
KAFKA_PID_FILE="$KAFKA_RUN_DIR/kafka.pid"
KAFKA_CONSOLE_LOG="$KAFKA_LOG_DIR/server-console.log"
KAFKA_BOOTSTRAP_SERVER="127.0.0.1:9092"

# kafka-run-class.sh otherwise enables the JVM's local JMX connector, which
# opens an extra random port. Preserve explicit caller-supplied JMX options.
: "${KAFKA_JMX_OPTS:=-Dkafka.local.jmx.disabled=true}"
export KAFKA_JMX_OPTS

kafka_prepare_directories() {
    mkdir -p \
        "$KAFKA_DATA_DIR" \
        "$KAFKA_LOG_DIR" \
        "$KAFKA_RUN_DIR"
}

kafka_require_installation() {
    if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
        echo "Kafka is not installed under $KAFKA_HOME" >&2
        echo "Run $KAFKA_BASE/install.sh first." >&2
        return 1
    fi
}

kafka_require_java() {
    local java_version java_major

    if ! command -v java >/dev/null 2>&1; then
        echo "Java is not available on PATH; Kafka $KAFKA_VERSION requires Java 17+." >&2
        return 1
    fi

    java_version="$(java -version 2>&1 | sed -n '1s/.*version "\([^"]*\)".*/\1/p')"
    java_major="${java_version%%.*}"

    if [[ "$java_major" == "1" ]]; then
        java_major="$(printf '%s' "$java_version" | cut -d. -f2)"
    fi

    if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 17 )); then
        echo "Kafka $KAFKA_VERSION requires Java 17+; found: ${java_version:-unknown}." >&2
        return 1
    fi
}

kafka_pid_is_live() {
    local pid="${1:-}"

    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || [[ -d "/proc/$pid" ]]
}

kafka_pid_is_this_instance() {
    local pid="${1:-}"

    kafka_pid_is_live "$pid" || return 1
    [[ -r "/proc/$pid/cmdline" ]] || return 1

    tr '\0' '\n' < "/proc/$pid/cmdline" | grep -Fqx -- "kafka.Kafka" \
        && tr '\0' '\n' < "/proc/$pid/cmdline" | grep -Fqx -- "$KAFKA_CONFIG"
}

kafka_broker_ready() {
    kafka_require_installation >/dev/null 2>&1 || return 1

    timeout 5 "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --list >/dev/null 2>&1
}

kafka_port_is_open() {
    timeout 1 bash -c '</dev/tcp/127.0.0.1/9092' >/dev/null 2>&1
}
