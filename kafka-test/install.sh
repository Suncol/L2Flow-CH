#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

ARCHIVE_NAME="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz"
DOWNLOAD_DIR="$KAFKA_BASE/downloads"
DIST_DIR="$KAFKA_BASE/dist"
INSTALL_DIR="$DIST_DIR/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}"
ARCHIVE_PATH="$DOWNLOAD_DIR/$ARCHIVE_NAME"
DOWNLOAD_URLS=(
    "https://dlcdn.apache.org/kafka/$KAFKA_VERSION/$ARCHIVE_NAME"
    "https://downloads.apache.org/kafka/$KAFKA_VERSION/$ARCHIVE_NAME"
    "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$ARCHIVE_NAME"
)
EXPECTED_SHA512="c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c"

kafka_require_java

for command_name in curl sha512sum tar; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Required command is missing: $command_name" >&2
        exit 1
    fi
done

mkdir -p "$DOWNLOAD_DIR" "$DIST_DIR"

if [[ ! -f "$ARCHIVE_PATH" ]]; then
    partial_path="$ARCHIVE_PATH.part"
    download_ok=0

    for download_url in "${DOWNLOAD_URLS[@]}"; do
        echo "Downloading Kafka $KAFKA_VERSION from $download_url"

        if curl -fL --retry 3 --retry-delay 2 --continue-at - \
            "$download_url" -o "$partial_path"; then
            download_ok=1
            break
        fi

        echo "Download endpoint unavailable; trying the next Apache endpoint." >&2
    done

    if (( download_ok == 0 )); then
        echo "All Apache Kafka download endpoints failed." >&2
        exit 1
    fi

    mv -- "$partial_path" "$ARCHIVE_PATH"
fi

actual_sha512="$(sha512sum "$ARCHIVE_PATH" | awk '{print $1}')"
if [[ "$actual_sha512" != "$EXPECTED_SHA512" ]]; then
    echo "SHA-512 verification failed for $ARCHIVE_PATH" >&2
    echo "Expected: $EXPECTED_SHA512" >&2
    echo "Actual:   $actual_sha512" >&2
    exit 1
fi

echo "SHA-512 verified: $ARCHIVE_NAME"

if [[ ! -x "$INSTALL_DIR/bin/kafka-server-start.sh" ]]; then
    if [[ -e "$INSTALL_DIR" ]]; then
        echo "Incomplete install directory already exists: $INSTALL_DIR" >&2
        echo "Move it aside, then run this installer again." >&2
        exit 1
    fi

    tar -xzf "$ARCHIVE_PATH" -C "$DIST_DIR"
fi

ln -sfn "dist/$(basename -- "$INSTALL_DIR")" "$KAFKA_HOME"

installed_version="$($KAFKA_HOME/bin/kafka-topics.sh --version | awk '{print $1}')"
if [[ "$installed_version" != "$KAFKA_VERSION" ]]; then
    echo "Unexpected installed Kafka version: $installed_version" >&2
    exit 1
fi

echo "Kafka $installed_version installed at $INSTALL_DIR"
echo "Java: $(java -version 2>&1 | head -n 1)"
echo "Next: $KAFKA_BASE/start.sh"
