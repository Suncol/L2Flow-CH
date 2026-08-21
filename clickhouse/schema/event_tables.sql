-- Local/single-node Event revision schema. Event identity is the first eight
-- columns. Version and writer provenance are attributes, never key members.
CREATE DATABASE IF NOT EXISTS l2flow;

CREATE TABLE IF NOT EXISTS l2flow.event_revision_log
(
    trade_date Date,
    market UInt8,
    instrument_id UInt32,
    channel UInt32,
    native_sequence UInt64,
    event_kind UInt8,
    affected_order_id Int64,
    occurrence UInt32,
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
        source_anchor Tuple(
            native_sequence UInt64,
            ingress_sequence UInt64,
            vendor_sequence_id UInt64,
            receive_monotonic_ns UInt64,
            exchange_time_ns_from_midnight UInt64,
            vendor_local_time_ns_from_midnight UInt64,
            exchange_time_raw UInt32,
            vendor_local_time_raw UInt32,
            exchange_time_valid Bool,
            vendor_local_time_valid Bool),
        action UInt8,
        side UInt8,
        aggressor UInt8,
        order_type UInt8,
        phase UInt8,
        price_p6 Int64,
        price_valid Bool,
        amount_p6 Int64,
        amount_valid Bool,
        quantity Int64,
        quantity_valid Bool,
        matched_quantity Int64,
        matched_quantity_valid Bool,
        primary_order_id Int64,
        buy_order_id Int64,
        sell_order_id Int64,
        source_quality_flags UInt64,
        event_quality_flags UInt64,
        referenced_order_found Bool,
        referenced_order_found_valid Bool,
        side_from_order Bool,
        order_delta_operation UInt8,
        order_snapshot_valid Bool,
        order Tuple(
            trade_date UInt32,
            market UInt8,
            instrument_id UInt32,
            channel UInt32,
            order_id Int64,
            side UInt8,
            order_type UInt8,
            sh_side_source UInt8,
            sh_order_source UInt8,
            price_p6 Int64,
            price_valid Bool,
            sh_price_source UInt8,
            execution_boundary_price_p6 Int64,
            execution_boundary_price_valid Bool,
            published_quantity Int64,
            published_quantity_valid Bool,
            original_quantity Int64,
            original_quantity_valid Bool,
            sh_original_quantity_status UInt8,
            remaining_quantity Int64,
            remaining_quantity_valid Bool,
            source_matched_quantity Int64,
            source_matched_quantity_valid Bool,
            observed_pre_add_trade_quantity Int64,
            post_add_trade_quantity Int64,
            total_trade_quantity Int64,
            total_cancel_quantity Int64,
            trade_count UInt64,
            cancel_count UInt64,
            phase_at_first UInt8,
            phase_at_add UInt8,
            phase_at_last UInt8,
            add_seen Bool,
            apply_to_book Bool,
            first_anchor Tuple(
                native_sequence UInt64,
                ingress_sequence UInt64,
                vendor_sequence_id UInt64,
                receive_monotonic_ns UInt64,
                exchange_time_ns_from_midnight UInt64,
                vendor_local_time_ns_from_midnight UInt64,
                exchange_time_raw UInt32,
                vendor_local_time_raw UInt32,
                exchange_time_valid Bool,
                vendor_local_time_valid Bool),
            last_anchor Tuple(
                native_sequence UInt64,
                ingress_sequence UInt64,
                vendor_sequence_id UInt64,
                receive_monotonic_ns UInt64,
                exchange_time_ns_from_midnight UInt64,
                vendor_local_time_ns_from_midnight UInt64,
                exchange_time_raw UInt32,
                vendor_local_time_raw UInt32,
                exchange_time_valid Bool,
                vendor_local_time_valid Bool),
            add_anchor Tuple(
                native_sequence UInt64,
                ingress_sequence UInt64,
                vendor_sequence_id UInt64,
                receive_monotonic_ns UInt64,
                exchange_time_ns_from_midnight UInt64,
                vendor_local_time_ns_from_midnight UInt64,
                exchange_time_raw UInt32,
                vendor_local_time_raw UInt32,
                exchange_time_valid Bool,
                vendor_local_time_valid Bool),
            finality UInt8,
            quality_flags UInt64,
            source_quality_flags UInt64)),
    writer_instance_id FixedString(16),
    batch_id FixedString(16),
    batch_sequence UInt64,
    chunk_index UInt32,
    row_index UInt32,
    schema_version UInt32
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY
(
    calculation_run_id,
    recovery_run_id,
    version,
    row_index
)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS l2flow.event
AS l2flow.event_revision_log
ENGINE = ReplacingMergeTree(version)
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    channel,
    native_sequence,
    event_kind,
    affected_order_id,
    occurrence
);

CREATE MATERIALIZED VIEW IF NOT EXISTS l2flow.event_current_mv
TO l2flow.event
AS SELECT * FROM l2flow.event_revision_log;

CREATE TABLE IF NOT EXISTS l2flow.event_recovery_run
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
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (calculation_run_id, recovery_run_id)
SETTINGS non_replicated_deduplication_window = 10000;

-- Current is row-wise eventually consistent. Replacement must happen before
-- tombstones are filtered; use FINAL for direct correctness-oriented reads.
--
-- SELECT *
-- FROM l2flow.event FINAL
-- WHERE is_deleted = false;
