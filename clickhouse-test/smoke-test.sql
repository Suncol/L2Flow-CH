CREATE DATABASE IF NOT EXISTS l2test;

DROP TABLE IF EXISTS l2test.raw_tick;

CREATE TABLE l2test.raw_tick
(
    trading_day Date,
    event_time DateTime64(3, 'Asia/Shanghai'),
    receive_time DateTime64(6, 'Asia/Shanghai'),
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    channel UInt16,
    sequence UInt64,
    price_int Int64,
    quantity UInt64
)
ENGINE = MergeTree
PARTITION BY trading_day
ORDER BY
(
    exchange,
    symbol,
    event_time,
    channel,
    sequence
);

INSERT INTO l2test.raw_tick VALUES
(
    '2026-08-06',
    '2026-08-06 09:30:00.003',
    '2026-08-06 09:30:00.003100',
    'SSE',
    '600000',
    1,
    3,
    100300,
    100
),
(
    '2026-08-06',
    '2026-08-06 09:30:00.001',
    '2026-08-06 09:30:00.004100',
    'SSE',
    '600000',
    1,
    1,
    100100,
    200
),
(
    '2026-08-06',
    '2026-08-06 09:30:00.002',
    '2026-08-06 09:30:00.004500',
    'SSE',
    '600000',
    1,
    2,
    100200,
    150
);

SELECT
    event_time,
    receive_time,
    sequence,
    price_int,
    quantity
FROM l2test.raw_tick
ORDER BY
    event_time,
    sequence;

SELECT
    database,
    table,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size
FROM system.parts
WHERE active
    AND database = 'l2test'
GROUP BY
    database,
    table;
