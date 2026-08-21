-- Keeper-backed KLine schema. Configure l2flow_keeper_root, shard, and replica
-- macros on every replica. Apply through the deployment's cluster orchestrator,
-- optionally adding its ON CLUSTER clause.
CREATE DATABASE IF NOT EXISTS l2flow;

CREATE TABLE IF NOT EXISTS l2flow.kline_revision_log
(
    trade_date Date,
    market UInt8,
    instrument_id UInt32,
    interval_seconds UInt32,
    bucket_start_ns_from_midnight UInt64,
    version UInt64,
    revision_id FixedString(16),
    supersedes_revision_id FixedString(16),
    supersedes_revision_id_valid Bool,
    recovery_run_id FixedString(16),
    revision_operation UInt8,
    revision_reason UInt8,
    calculation_run_id FixedString(16),
    logic_version UInt32,
    input_set_hash FixedString(16),
    payload_hash FixedString(16),
    is_deleted Bool,
    payload Tuple(
        bucket_end_ns_from_midnight UInt64,
        open_price_p6 Int64,
        high_price_p6 Int64,
        low_price_p6 Int64,
        close_price_p6 Int64,
        volume Int64,
        notional_p6 Int64,
        trade_count UInt64,
        first_trade Tuple(
            exchange_time_ns_from_midnight UInt64,
            channel UInt32,
            native_sequence UInt64,
            ingress_sequence UInt64),
        last_trade Tuple(
            exchange_time_ns_from_midnight UInt64,
            channel UInt32,
            native_sequence UInt64,
            ingress_sequence UInt64),
        source_quality_flags UInt64,
        has_hole_fill Bool,
        provisional Bool),
    writer_instance_id FixedString(16),
    batch_id FixedString(16),
    batch_sequence UInt64,
    chunk_index UInt32,
    row_index UInt32,
    schema_version UInt32
)
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/kline_revision_log',
    '{replica}')
PARTITION BY trade_date
ORDER BY
(
    calculation_run_id,
    recovery_run_id,
    version,
    row_index
);

CREATE TABLE IF NOT EXISTS l2flow.kline
AS l2flow.kline_revision_log
ENGINE = ReplicatedReplacingMergeTree(
    '{l2flow_keeper_root}/{shard}/kline',
    '{replica}',
    version)
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    interval_seconds,
    bucket_start_ns_from_midnight
);

CREATE MATERIALIZED VIEW IF NOT EXISTS l2flow.kline_current_mv
TO l2flow.kline
AS SELECT * FROM l2flow.kline_revision_log;

CREATE TABLE IF NOT EXISTS l2flow.kline_recovery_run
(
    trade_date Date,
    calculation_run_id FixedString(16),
    recovery_run_id FixedString(16),
    owner UInt32,
    calculation_batch_sequence UInt64,
    revision_reason UInt8,
    minimum_version UInt64,
    maximum_version UInt64,
    revision_count UInt64,
    chunk_count UInt32,
    committed Bool,
    committed_utc_ns UInt64,
    writer_instance_id FixedString(16),
    commit_id FixedString(16),
    schema_version UInt32
)
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/kline_recovery_run',
    '{replica}')
PARTITION BY trade_date
ORDER BY (calculation_run_id, recovery_run_id);

-- SELECT * FROM l2flow.kline FINAL WHERE is_deleted = false;
