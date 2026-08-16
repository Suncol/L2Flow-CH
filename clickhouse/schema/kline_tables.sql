-- Local/single-node KLine revision schema. Windows are anchored at exchange
-- midnight and use [bucket_start, bucket_end) over the SDK exchange time.
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
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    interval_seconds,
    bucket_start_ns_from_midnight,
    version
)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS l2flow.kline_recovery_run
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
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (calculation_run_id, recovery_run_id)
SETTINGS non_replicated_deduplication_window = 10000;

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
ENGINE = MergeTree
ORDER BY (calculation_run_id, domain, publication_sequence)
SETTINGS non_replicated_deduplication_window = 10000;

-- This view is deliberately fail-closed. A revision becomes current only
-- after its recovery marker exists and exactly one KLine (domain=2)
-- calculation run has an authoritative, unexpired latest lease. Overlapping
-- active runs close the view instead of mixing their revisions. Replacement
-- is selected before tombstones are filtered.
CREATE VIEW IF NOT EXISTS l2flow.kline AS
SELECT * EXCEPT (_l2flow_rank)
FROM
(
    SELECT
        r.*,
        row_number() OVER
        (
            PARTITION BY r.trade_date, r.market, r.instrument_id,
                         r.interval_seconds,
                         r.bucket_start_ns_from_midnight
            ORDER BY r.version DESC, r.batch_sequence DESC,
                     r.chunk_index DESC, r.row_index DESC,
                     r.revision_id DESC
        ) AS _l2flow_rank
    FROM l2flow.kline_revision_log AS r
    INNER JOIN
    (
        SELECT trade_date AS committed_trade_date,
               calculation_run_id AS committed_calculation_run_id,
               recovery_run_id AS committed_recovery_run_id,
               input_max_lsn, input_max_batch_sequence, input_max_row_index
        FROM l2flow.kline_recovery_run
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
                        tuple(authoritative, valid_until_utc_ns, kline_lsn,
                              kline_batch_sequence, kline_row_index),
                        tuple(publication_sequence, observed_utc_ns,
                              publisher_instance_id)) AS latest
                FROM l2flow.derived_freshness_log
                WHERE domain = 2
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
