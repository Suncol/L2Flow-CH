from __future__ import annotations

import csv
import io
import json
import pathlib
import tempfile
import unittest
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

import polars as pl

from l2flow_feeder_latest import (
    DATASETS,
    FeederConfigError,
    FeederLatestClient,
    LatestNotFoundError,
    NUMERIC_SCHEMA,
    PayloadDecodeError,
    PayloadIdentityError,
    PayloadParseError,
    SchemaMismatchError,
    TypedFrameError,
    UnknownDatasetError,
)


Payload = bytes | bytearray | memoryview | str


def _schema(dataset: str, variant: str = "base") -> tuple[str, ...]:
    return next(
        fields for name, fields in DATASETS[dataset].schemas if name == variant
    )


def _encode_row(values: Sequence[str], *, trailer: bool = True) -> bytes:
    output = io.StringIO()
    csv.writer(output, lineterminator="\n").writerow(values)
    text = output.getvalue().removesuffix("\n")
    if trailer:
        text += ","
    return text.encode("gbk")


def _payload(
    dataset: str,
    security_id: str,
    *,
    variant: str = "base",
    values: Mapping[str, str] | None = None,
    trailer: bool = True,
) -> bytes:
    fields = _schema(dataset, variant)
    row = ["" for _ in fields]
    defaults = {
        "SecurityID": security_id,
        DATASETS[dataset].exchange_time_field: "09:30:00.123",
        "LocalTime": "09:30:00.456",
        "SeqNo": "42",
    }
    if values:
        defaults.update(values)
    for field, value in defaults.items():
        row[fields.index(field)] = value
    return _encode_row(row, trailer=trailer)


class FakeRedis:
    def __init__(
        self,
        values: Mapping[str, Payload | None] | None = None,
        server_info: Mapping[str, Any] | None = None,
    ) -> None:
        self.values = dict(values or {})
        self.server_info = dict(
            server_info
            or {
                "redis_version": "feeder",
                "compiler": "213234",
                "connected_clients": 2,
                "db0": {"keys": 123, "expires": 0, "avg_ttl": 0},
            }
        )
        self.get_calls: list[str] = []
        self.mget_calls: list[list[str]] = []
        self.info_calls = 0
        self.close_calls = 0

    def get(self, name: str) -> Payload | None:
        self.get_calls.append(name)
        return self.values.get(name)

    def mget(self, keys: Sequence[str]) -> list[Payload | None]:
        copied = list(keys)
        self.mget_calls.append(copied)
        return [self.values.get(key) for key in copied]

    def info(self) -> Mapping[str, Any]:
        self.info_calls += 1
        return self.server_info

    def close(self) -> None:
        self.close_calls += 1


class LatestParsingTest(unittest.TestCase):
    def test_sh_snapshot_with_quoted_order_queues_uses_polars_strings(self) -> None:
        key = "mdl.4.4.600000"
        fake = FakeRedis(
            {
                key: _payload(
                    "sh_snapshot",
                    "600000",
                    variant="with_l2_order_queues",
                    values={
                        "LastPrice": "12.345",
                        "Turnover": "123456.78901",
                        "TradNumber": "7",
                        "BidOrderQueue": "[100,200,300]",
                        "AskOrderQueue": "[400,500]",
                    },
                )
            }
        )
        update = FeederLatestClient(redis_client=fake).latest(
            "sh_snapshot", "600000"
        )

        self.assertEqual(update.schema_variant, "with_l2_order_queues")
        self.assertEqual(update.frame.shape, (1, 106))
        self.assertTrue(all(dtype == pl.String for dtype in update.frame.dtypes))
        self.assertEqual(update.data["BidOrderQueue"], "[100,200,300]")
        self.assertEqual(update.data["AskOrderQueue"], "[400,500]")
        self.assertEqual(update.exchange_update_time, "09:30:00.123")
        self.assertEqual(update.feeder_local_time, "09:30:00.456")
        self.assertEqual(update.sequence_no, "42")
        self.assertEqual(update.queried_at.tzinfo.key, "Asia/Shanghai")
        self.assertTrue(update.raw_csv.endswith(","))

        typed = update.typed_frame(
            fields=("LastPrice", "Turnover", "TradNumber", "SeqNo")
        )
        self.assertEqual(typed.schema["SecurityID"], pl.String)
        self.assertEqual(
            typed.schema["LastPrice"], pl.Decimal(precision=38, scale=3)
        )
        self.assertEqual(
            typed.schema["Turnover"], pl.Decimal(precision=38, scale=5)
        )
        self.assertEqual(typed.schema["TradNumber"], pl.UInt64)
        self.assertEqual(typed.schema["SeqNo"], pl.UInt64)
        self.assertEqual(typed.item(0, "LastPrice"), Decimal("12.345"))
        self.assertEqual(update.frame.schema["LastPrice"], pl.String)
        self.assertEqual(update.data["LastPrice"], "12.345")

    def test_sz_snapshot_queue_order_and_confirmed_scales(self) -> None:
        key = "mdl.6.28.000001"
        fake = FakeRedis(
            {
                key: _payload(
                    "sz_snapshot",
                    "000001",
                    variant="with_l2_order_queues",
                    values={
                        "SecurityIDSource": "102 ",
                        "PreCloPrice": "34.7000",
                        "LastPrice": "34.700000",
                        "Volume": "1000",
                        "AskOrderQueue": "[11,22]",
                        "BidOrderQueue": "[33,44]",
                    },
                )
            }
        )
        update = FeederLatestClient(redis_client=fake).latest_snapshot("sz", "000001")

        self.assertEqual(update.frame.shape, (1, 91))
        self.assertEqual(update.data["AskOrderQueue"], "[11,22]")
        self.assertEqual(update.data["BidOrderQueue"], "[33,44]")
        typed = update.typed_frame(
            fields=("PreCloPrice", "LastPrice", "Volume", "SeqNo")
        )
        self.assertEqual(
            typed.schema["PreCloPrice"], pl.Decimal(precision=38, scale=4)
        )
        self.assertEqual(
            typed.schema["LastPrice"], pl.Decimal(precision=38, scale=6)
        )
        self.assertEqual(typed.schema["Volume"], pl.Int64)
        self.assertEqual(typed.schema["SeqNo"], pl.UInt64)
        self.assertEqual(typed.schema["AskOrderQueue"], pl.String)

    def test_base_snapshot_without_order_queues_is_accepted(self) -> None:
        key = "mdl.4.4.600001"
        fake = FakeRedis({key: _payload("sh_snapshot", "600001")})
        update = FeederLatestClient(redis_client=fake).latest_snapshot("SH", "600001")
        self.assertEqual(update.schema_variant, "base")
        self.assertEqual(update.frame.width, 104)
        self.assertNotIn("BidOrderQueue", update.data)

    def test_record_without_publisher_trailer_is_accepted(self) -> None:
        key = "mdl.4.24.600000"
        fake = FakeRedis(
            {
                key: _payload(
                    "sh_l2_tick",
                    "600000",
                    values={"BizIndex": "1", "Price": "10.000", "Qty": "100"},
                    trailer=False,
                )
            }
        )
        update = FeederLatestClient(redis_client=fake).latest(
            "sh_l2_tick", "600000"
        )
        self.assertFalse(update.raw_csv.endswith(","))
        self.assertEqual(update.frame.width, 13)

    def test_wrong_width_and_multiple_trailers_are_rejected(self) -> None:
        fields = _schema("sz_l2_order")
        short_values = ["" for _ in fields[:-1]]
        short_values[fields[:-1].index("SecurityID")] = "000001"
        short = FakeRedis({"mdl.6.33.000001": _encode_row(short_values)})
        with self.assertRaises(SchemaMismatchError):
            FeederLatestClient(redis_client=short).latest("sz_l2_order", "000001")

        extra_trailer = _payload("sz_l2_order", "000001") + b","
        doubled = FakeRedis({"mdl.6.33.000001": extra_trailer})
        with self.assertRaisesRegex(SchemaMismatchError, "more than one trailing"):
            FeederLatestClient(redis_client=doubled).latest(
                "sz_l2_order", "000001"
            )

    def test_multiple_rows_are_rejected(self) -> None:
        first = _payload("sh_l2_tick", "600000", trailer=False)
        payload = first + b"\n" + first
        fake = FakeRedis({"mdl.4.24.600000": payload})
        with self.assertRaisesRegex(PayloadParseError, "exactly one CSV row"):
            FeederLatestClient(redis_client=fake).latest("sh_l2_tick", "600000")

    def test_invalid_gbk_and_payload_identity_are_rejected(self) -> None:
        invalid = FakeRedis({"mdl.4.24.600000": b"\x81"})
        with self.assertRaises(PayloadDecodeError):
            FeederLatestClient(redis_client=invalid).latest(
                "sh_l2_tick", "600000"
            )

        wrong_id = FakeRedis(
            {"mdl.4.24.600000": _payload("sh_l2_tick", "600001")}
        )
        with self.assertRaises(PayloadIdentityError):
            FeederLatestClient(redis_client=wrong_id).latest(
                "sh_l2_tick", "600000"
            )

    def test_required_update_metadata_must_be_present(self) -> None:
        fake = FakeRedis(
            {
                "mdl.6.36.000001": _payload(
                    "sz_l2_trade", "000001", values={"TransactTime": ""}
                )
            }
        )
        with self.assertRaisesRegex(PayloadParseError, "required TransactTime"):
            FeederLatestClient(redis_client=fake).latest("sz_l2_trade", "000001")

    def test_missing_and_invalid_requests_fail_before_ambiguous_results(self) -> None:
        client = FeederLatestClient(redis_client=FakeRedis())
        with self.assertRaises(LatestNotFoundError):
            client.latest("sh_snapshot", "600000")
        with self.assertRaises(UnknownDatasetError):
            client.latest("unknown", "600000")
        with self.assertRaises(ValueError):
            client.latest("sh_snapshot", "60000*")
        with self.assertRaises(TypeError):
            client.latest("sz_snapshot", 1)  # type: ignore[arg-type]


class LatestBatchAndInfoTest(unittest.TestCase):
    def test_sz_l2_uses_one_mget_and_preserves_signed_native_types(self) -> None:
        order_key = "mdl.6.33.000001"
        trade_key = "mdl.6.36.000001"
        fake = FakeRedis(
            {
                order_key: _payload(
                    "sz_l2_order",
                    "000001",
                    values={
                        "ChannelNo": "2011",
                        "ApplSeqNum": "7",
                        "Price": "12.3400",
                        "OrderQty": "1000",
                    },
                ),
                trade_key: _payload(
                    "sz_l2_trade",
                    "000001",
                    values={
                        "ChannelNo": "2021",
                        "ApplSeqNum": "8",
                        "BidApplSeqNum": "7",
                        "OfferApplSeqNum": "0",
                        "LastPx": "12.3400",
                        "LastQty": "500",
                    },
                ),
            }
        )
        updates = FeederLatestClient(redis_client=fake).latest_l2("SZ", "000001")

        self.assertEqual(fake.get_calls, [])
        self.assertEqual(fake.mget_calls, [[order_key, trade_key]])
        self.assertEqual(set(updates), {"order", "trade"})
        self.assertEqual(updates["order"].queried_at, updates["trade"].queried_at)
        order = updates["order"].typed_frame()
        trade = updates["trade"].typed_frame()
        self.assertEqual(order.schema["ChannelNo"], pl.UInt64)
        self.assertEqual(order.schema["ApplSeqNum"], pl.Int64)
        self.assertEqual(
            order.schema["Price"], pl.Decimal(precision=38, scale=4)
        )
        self.assertEqual(trade.schema["BidApplSeqNum"], pl.Int64)
        self.assertEqual(
            trade.schema["LastPx"], pl.Decimal(precision=38, scale=4)
        )

    def test_strict_typed_error_has_context_and_keeps_source(self) -> None:
        key = "mdl.6.33.000001"
        fake = FakeRedis(
            {
                key: _payload(
                    "sz_l2_order",
                    "000001",
                    values={"Price": "invalid-price"},
                )
            }
        )
        update = FeederLatestClient(redis_client=fake).latest(
            "sz_l2_order", "000001"
        )
        with self.assertRaises(TypedFrameError) as raised:
            update.typed_frame(fields=("Price",))
        message = str(raised.exception)
        self.assertIn("dataset='sz_l2_order'", message)
        self.assertIn(f"redis_key='{key}'", message)
        self.assertIn("field='Price'", message)
        self.assertIn("raw_value='invalid-price'", message)
        self.assertEqual(update.frame.schema["Price"], pl.String)
        self.assertEqual(update.data["Price"], "invalid-price")

        with self.assertRaisesRegex(ValueError, "no confirmed numeric type"):
            update.typed_frame(fields=("TransactTime",))

    def test_info_lists_only_supported_latest_types_without_scan(self) -> None:
        fake = FakeRedis()
        client = FeederLatestClient(
            redis_client=fake,
            enable_l2_orders=True,
        )
        info = client.info()

        self.assertEqual(fake.info_calls, 1)
        self.assertEqual(set(info["latest_query_types"]), set(DATASETS))
        self.assertEqual(info["server"]["compiler"], "213234")
        self.assertEqual(info["server"]["keyspace"]["total_keys"], 123)
        self.assertTrue(info["enable_l2_orders"])
        self.assertFalse(info["discovery"]["scan_supported"])
        self.assertFalse(info["time_semantics"]["trading_date_available"])
        self.assertEqual(
            info["latest_query_types"]["sz_l2_trade"]["typed_numeric_fields"][
                "LastPx"
            ],
            "Decimal(precision=38, scale=4)",
        )

        static_info = client.info(probe_server=False)
        self.assertIsNone(static_info["server"])
        self.assertEqual(fake.info_calls, 1)

    def test_config_selects_redis_publisher_and_maps_wildcard_host(self) -> None:
        config = {
            "feeder_client": {
                "Publishers": [
                    {"Type": "TCP_SERVER", "Address": "0.0.0.0:9112"},
                    {
                        "Type": "REDIS_SERVER",
                        "Address": "0.0.0.0:9379",
                        "Encoding": 5,
                        "EnableL2Orders": True,
                    },
                ]
            }
        }
        fake = FakeRedis()
        with tempfile.TemporaryDirectory(prefix="l2flow-feeder-test-") as temporary:
            path = pathlib.Path(temporary) / "feeder_client.cfg"
            path.write_text(json.dumps(config), encoding="utf-8")
            client = FeederLatestClient.from_feeder_config(
                path, redis_client=fake
            )
            self.assertEqual(client.host, "127.0.0.1")
            self.assertEqual(client.port, 9379)
            self.assertTrue(client.enable_l2_orders)
            self.assertEqual(client.config_path, path.resolve())
            client.close()
        self.assertEqual(fake.close_calls, 1)

    def test_config_rejects_missing_file_and_non_gbk_redis_encoding(self) -> None:
        with tempfile.TemporaryDirectory(prefix="l2flow-feeder-test-") as temporary:
            root = pathlib.Path(temporary)
            with self.assertRaises(FeederConfigError):
                FeederLatestClient.from_feeder_config(root / "missing.cfg")

            path = root / "feeder_client.cfg"
            path.write_text(
                json.dumps(
                    {
                        "Publishers": [
                            {
                                "Type": "REDIS_SERVER",
                                "Address": "127.0.0.1:9379",
                                "Encoding": 7,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(FeederConfigError, "Encoding must be 5"):
                FeederLatestClient.from_feeder_config(path)

    def test_numeric_schema_covers_only_fields_in_each_source_schema(self) -> None:
        for dataset, numeric_fields in NUMERIC_SCHEMA.items():
            available = set().union(
                *(set(fields) for _, fields in DATASETS[dataset].schemas)
            )
            self.assertLessEqual(set(numeric_fields), available)
            for field in (
                DATASETS[dataset].exchange_time_field,
                "LocalTime",
                "SecurityID",
            ):
                self.assertNotIn(field, numeric_fields)


if __name__ == "__main__":
    unittest.main()
