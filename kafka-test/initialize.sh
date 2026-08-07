#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

kafka_require_installation
kafka_require_java
kafka_prepare_directories
cd "$KAFKA_BASE"

if [[ -s "$KAFKA_DATA_DIR/meta.properties" ]]; then
    cluster_id="$(sed -n 's/^cluster.id=//p' "$KAFKA_DATA_DIR/meta.properties")"
    echo "KRaft storage is already initialized: cluster.id=$cluster_id"
    exit 0
fi

if find "$KAFKA_DATA_DIR" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    echo "Data directory is non-empty but has no meta.properties:" >&2
    echo "  $KAFKA_DATA_DIR" >&2
    echo "Refusing to format it automatically." >&2
    exit 1
fi

cluster_id="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"

"$KAFKA_HOME/bin/kafka-storage.sh" format \
    --standalone \
    --cluster-id "$cluster_id" \
    --config "$KAFKA_CONFIG"

echo "KRaft storage initialized: cluster.id=$cluster_id"
