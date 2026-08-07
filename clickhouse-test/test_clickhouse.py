from __future__ import annotations

import os
import sys

import clickhouse_connect
import pyarrow as pa


ARROW_QUERY = """
    SELECT
        event_time,
        sequence,
        price_int,
        quantity
    FROM raw_tick
    ORDER BY
        event_time,
        sequence
"""


def configure_loopback_no_proxy() -> None:
    existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy", "")
    entries = [entry.strip() for entry in existing.split(",") if entry.strip()]

    for host in ("127.0.0.1", "localhost", "::1"):
        if host not in entries:
            entries.append(host)

    value = ",".join(entries)
    os.environ["NO_PROXY"] = value
    os.environ["no_proxy"] = value


def main() -> int:
    configure_loopback_no_proxy()

    client = clickhouse_connect.get_client(
        host="127.0.0.1",
        port=8123,
        username="default",
        password="",
        database="l2test",
        connect_timeout=5,
        send_receive_timeout=10,
    )

    try:
        version = client.command("SELECT version()")
        print(f"ClickHouse version: {version}")

        table = client.query_arrow(ARROW_QUERY)
        print(table)

        if not isinstance(table, pa.Table):
            raise RuntimeError(
                f"expected pyarrow.Table, received {type(table).__name__}"
            )

        expected_types = {
            "sequence": pa.uint64(),
            "price_int": pa.int64(),
            "quantity": pa.uint64(),
        }
        for column_name, expected_type in expected_types.items():
            actual_type = table.schema.field(column_name).type
            if actual_type != expected_type:
                raise RuntimeError(
                    f"expected {column_name} to have Arrow type {expected_type}, "
                    f"received {actual_type}"
                )

        sequences = table.column("sequence").to_pylist()
        if sequences != [1, 2, 3]:
            raise RuntimeError(
                f"expected ordered Arrow sequences [1, 2, 3], received {sequences}"
            )

        count = client.command("SELECT count() FROM raw_tick")
        print(f"row count: {count}")

        if table.num_rows != 3:
            raise RuntimeError(
                f"expected 3 Arrow rows, received {table.num_rows}"
            )
        if count != 3:
            raise RuntimeError(f"expected 3 rows, received {count}")
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ClickHouse test failed: {exc}", file=sys.stderr)
        raise
