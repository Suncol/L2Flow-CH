-- Production schema. Configure these ClickHouse macros on every replica:
--   l2flow_keeper_root: deployment-owned Keeper root, for example
--                       /clickhouse/tables/l2flow
--   shard:              stable shard identifier
--   replica:            unique replica identifier within the shard
-- Apply this file through the deployment orchestrator on each replica (or add
-- the deployment's ON CLUSTER clause). Increase the table-local deduplication
-- window if 10000 blocks do not cover the configured retry interval.
CREATE DATABASE IF NOT EXISTS l2flow;

CREATE TABLE IF NOT EXISTS l2flow.raw_tick
(
    feed_session_epoch UInt64,
    ingress_sequence UInt64,
    vendor_sequence_id UInt64,
    receive_monotonic_ns UInt64,
    native_sequence Nullable(UInt64),
    exchange_time_ns_from_midnight Nullable(UInt64),
    vendor_local_time_ns_from_midnight Nullable(UInt64),
    quality_flags UInt64,
    trade_date Date,
    instrument_id Nullable(UInt32),
    instrument_ordinal Nullable(UInt32),
    channel UInt32,
    exchange_time_raw UInt32,
    vendor_local_time_raw UInt32,
    service_id UInt8,
    service_version UInt16,
    message_id UInt16,
    canonical_kind UInt8,
    market UInt8,
    security_id_source String,
    security_id String,
    md_stream_id String,
    price_raw Nullable(Int64),
    price_p6 Nullable(Int64),
    price_source_scale UInt8,
    amount_raw Nullable(Int64),
    amount_p6 Nullable(Int64),
    amount_source_scale UInt8,
    quantity_raw Nullable(Int64),
    quantity_scale UInt8,
    primary_order_id Nullable(Int64),
    buy_order_id Nullable(Int64),
    sell_order_id Nullable(Int64),
    sh_add_matched_quantity_raw Nullable(Int64),
    validity UInt64,
    raw_type Int32,
    raw_side Int32,
    action UInt8,
    side UInt8,
    aggressor UInt8,
    order_type UInt8,
    phase UInt8,
    catalog_match Bool,
    source_instance_id FixedString(16),
    writer_instance_id FixedString(16),
    batch_id FixedString(16),
    batch_sequence UInt64,
    row_index UInt32,
    occurrence_id FixedString(16),
    schema_version UInt32
)
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/raw_tick',
    '{replica}')
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    exchange_time_ns_from_midnight,
    channel,
    native_sequence,
    source_instance_id,
    feed_session_epoch,
    ingress_sequence
)
SETTINGS
    allow_nullable_key = 1,
    replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS l2flow.raw_snapshot
(
    feed_session_epoch UInt64,
    ingress_sequence UInt64,
    vendor_sequence_id UInt64,
    receive_monotonic_ns UInt64,
    native_sequence Nullable(UInt64),
    exchange_time_ns_from_midnight Nullable(UInt64),
    vendor_local_time_ns_from_midnight Nullable(UInt64),
    quality_flags UInt64,
    trade_date Date,
    instrument_id Nullable(UInt32),
    instrument_ordinal Nullable(UInt32),
    channel UInt32,
    exchange_time_raw UInt32,
    vendor_local_time_raw UInt32,
    service_id UInt8,
    service_version UInt16,
    message_id UInt16,
    canonical_kind UInt8,
    market UInt8,
    security_id_source String,
    security_id String,
    md_stream_id String,
    previous_close_raw Nullable(Int64),
    previous_close_p6 Nullable(Int64),
    previous_close_source_scale UInt8,
    open_raw Nullable(Int64),
    open_p6 Nullable(Int64),
    open_source_scale UInt8,
    high_raw Nullable(Int64),
    high_p6 Nullable(Int64),
    high_source_scale UInt8,
    low_raw Nullable(Int64),
    low_p6 Nullable(Int64),
    low_source_scale UInt8,
    last_raw Nullable(Int64),
    last_p6 Nullable(Int64),
    last_source_scale UInt8,
    close_raw Nullable(Int64),
    close_p6 Nullable(Int64),
    close_source_scale UInt8,
    turnover_raw Nullable(Int64),
    turnover_p6 Nullable(Int64),
    turnover_source_scale UInt8,
    volume_raw Nullable(Int64),
    volume_scale UInt8,
    total_bid_quantity_raw Nullable(Int64),
    total_bid_quantity_scale UInt8,
    total_ask_quantity_raw Nullable(Int64),
    total_ask_quantity_scale UInt8,
    weighted_average_bid_raw Nullable(Int64),
    weighted_average_bid_p6 Nullable(Int64),
    weighted_average_bid_source_scale UInt8,
    weighted_average_ask_raw Nullable(Int64),
    weighted_average_ask_p6 Nullable(Int64),
    weighted_average_ask_source_scale UInt8,
    trade_count Nullable(UInt64),
    image_status Nullable(Int32),
    instrument_status_code Nullable(String),
    trading_phase_code Nullable(String),
    source_bid_depth UInt32,
    source_ask_depth UInt32,
    retained_bid_depth UInt8,
    retained_ask_depth UInt8,
    bids Array(Tuple(
        price_raw Nullable(Int64),
        price_p6 Nullable(Int64),
        price_source_scale UInt8,
        quantity_raw Nullable(Int64),
        quantity_scale UInt8,
        source_order_count Nullable(UInt32))),
    asks Array(Tuple(
        price_raw Nullable(Int64),
        price_p6 Nullable(Int64),
        price_source_scale UInt8,
        quantity_raw Nullable(Int64),
        quantity_scale UInt8,
        source_order_count Nullable(UInt32))),
    catalog_match Bool,
    source_instance_id FixedString(16),
    writer_instance_id FixedString(16),
    batch_id FixedString(16),
    batch_sequence UInt64,
    row_index UInt32,
    occurrence_id FixedString(16),
    schema_version UInt32
)
ENGINE = ReplicatedMergeTree(
    '{l2flow_keeper_root}/{shard}/raw_snapshot',
    '{replica}')
PARTITION BY trade_date
ORDER BY
(
    market,
    instrument_id,
    exchange_time_ns_from_midnight,
    source_instance_id,
    feed_session_epoch,
    ingress_sequence
)
SETTINGS
    allow_nullable_key = 1,
    replicated_deduplication_window = 10000;
