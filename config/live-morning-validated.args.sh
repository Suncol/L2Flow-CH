#!/usr/bin/env bash

# Validated 2026-08-12 against the live feeder with raw ClickHouse, Event,
# KLine, and all 32 Arrow owners. This file only defines arguments; sourcing
# it does not start mdl_ingestd.
#
# Required variables supplied by the per-run launcher:
#   ARROW_ROOT       unique, nonexistent Arrow ring directory
#   DATABASE         freshly created ClickHouse database
#
# Required L2FLOW_* environment variables remain defined by the launcher.
if [[ -z ${ARROW_ROOT:-} || -z ${DATABASE:-} ]]; then
    echo "ARROW_ROOT and DATABASE must be set before sourcing this file" >&2
    return 2 2>/dev/null || exit 2
fi

L2FLOW_VALIDATED_LIVE_ARGS=(
    --config config/current-server.production.conf
    --sdk-work-threads 1
    --operation-mode test
    --run-seconds 10800
    --dispatch-queue-capacity 32768
    --arrow-ring-dir "$ARROW_ROOT"
    --clickhouse-database "$DATABASE"
    --clickhouse-writers 8
    --clickhouse-queue-batches-per-lane 64
    --clickhouse-tick-batch-max-delay-ns 500000000
    --event-writer-lanes 8
    --event-maximum-carry-orders 16777216
    --event-maximum-order-history-bytes 17179869184
    --event-maximum-pending-commits 262144
    --event-maximum-repair-bytes 8589934592
    --event-maximum-end-candidates 8388608
    --event-maximum-end-projected-rows 8388609
    --event-maximum-end-staging-bytes 8589934592
    --event-queue-revision-batches 4194304
    --event-queue-revision-rows 33554432
    --kline-writer-lanes 32
    --kline-maximum-pending-commits 65536
    --kline-queue-revision-batches 65536
    --kline-queue-revision-rows 16777216
    --kline-micro-batch-rows 1024
    --kline-micro-batch-max-delay-ns 200000000
)
