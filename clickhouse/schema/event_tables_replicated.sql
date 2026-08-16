-- Keeper-backed Event schema. Configure l2flow_keeper_root, shard, and replica
-- macros on every replica. Apply this file through the deployment's cluster
-- orchestrator, optionally adding its ON CLUSTER clause.
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
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/event_revision_log',
    '{replica}')
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    channel,
    native_sequence,
    event_kind,
    affected_order_id,
    occurrence,
    version
);

CREATE TABLE IF NOT EXISTS l2flow.event_recovery_run
(
    trade_date Date,
    calculation_run_id FixedString(16),
    recovery_run_id FixedString(16),
    owner UInt32,
    calculation_batch_sequence UInt64,
    input_max_lsn UInt64,
    input_max_batch_sequence UInt64,
    input_max_row_index UInt32,
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
    '{l2flow_keeper_root}/{shard}/event_recovery_run',
    '{replica}')
PARTITION BY trade_date
ORDER BY (calculation_run_id, recovery_run_id);

CREATE TABLE IF NOT EXISTS l2flow.derived_freshness_log
(
    source_instance_id FixedString(16),
    feed_session_epoch UInt64,
    calculation_run_id FixedString(16),
    domain UInt8,
    publication_sequence UInt64,
    frontier_id UInt64,
    barrier_lsn UInt64,
    barrier_batch_sequence UInt64,
    barrier_row_index UInt32,
    canonical_lsn UInt64,
    canonical_batch_sequence UInt64,
    canonical_row_index UInt32,
    raw_lsn UInt64,
    raw_batch_sequence UInt64,
    raw_row_index UInt32,
    event_lsn UInt64,
    event_batch_sequence UInt64,
    event_row_index UInt32,
    kline_lsn UInt64,
    kline_batch_sequence UInt64,
    kline_row_index UInt32,
    continuity_state UInt8,
    authoritative Bool,
    observed_utc_ns UInt64,
    valid_until_utc_ns UInt64,
    publisher_instance_id FixedString(16),
    schema_version UInt32
)
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/derived_freshness_log',
    '{replica}')
ORDER BY (calculation_run_id, domain, publication_sequence);

CREATE VIEW IF NOT EXISTS l2flow.event AS
SELECT * EXCEPT (_l2flow_rank)
FROM
(
    SELECT
        r.*,
        row_number() OVER
        (
            PARTITION BY r.trade_date, r.market, r.instrument_id, r.channel,
                         r.native_sequence, r.event_kind,
                         r.affected_order_id, r.occurrence
            ORDER BY r.version DESC, r.batch_sequence DESC,
                     r.chunk_index DESC, r.row_index DESC,
                     r.revision_id DESC
        ) AS _l2flow_rank
    FROM l2flow.event_revision_log AS r
    INNER JOIN
    (
        SELECT trade_date AS committed_trade_date,
               calculation_run_id AS committed_calculation_run_id,
               recovery_run_id AS committed_recovery_run_id,
               input_max_lsn, input_max_batch_sequence, input_max_row_index
        FROM l2flow.event_recovery_run
        WHERE committed
        GROUP BY trade_date, calculation_run_id, recovery_run_id,
                 input_max_lsn, input_max_batch_sequence, input_max_row_index
    ) AS c ON r.trade_date = c.committed_trade_date
          AND r.calculation_run_id = c.committed_calculation_run_id
          AND r.recovery_run_id = c.committed_recovery_run_id
    INNER JOIN
    (
        SELECT calculation_run_id AS freshness_calculation_run_id,
               tupleElement(latest, 3) AS cursor_lsn,
               tupleElement(latest, 4) AS cursor_batch_sequence,
               tupleElement(latest, 5) AS cursor_row_index
        FROM
        (
            SELECT
                calculation_run_id,
                latest,
                count() OVER () AS active_run_count
            FROM
            (
                SELECT
                    calculation_run_id,
                    argMax(
                        tuple(authoritative, valid_until_utc_ns, event_lsn,
                              event_batch_sequence, event_row_index),
                        tuple(publication_sequence, observed_utc_ns,
                              publisher_instance_id)) AS latest
                FROM l2flow.derived_freshness_log
                WHERE domain = 1
                GROUP BY calculation_run_id
            )
            WHERE tupleElement(latest, 1) = 1
              AND tupleElement(latest, 2) >=
                  toUInt64(toUnixTimestamp64Nano(now64(9)))
        )
        WHERE active_run_count = 1
    ) AS f ON r.calculation_run_id = f.freshness_calculation_run_id
          AND tuple(c.input_max_lsn, c.input_max_batch_sequence,
                    c.input_max_row_index) <=
              tuple(f.cursor_lsn, f.cursor_batch_sequence,
                    f.cursor_row_index)
)
WHERE _l2flow_rank = 1 AND is_deleted = 0;
