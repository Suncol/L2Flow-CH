"""Read the latest MDL snapshot and L2 records from feeder_client Redis.

The feeder exposes only the latest value for a known Redis key.  Values are
GBK-encoded CSV records and may contain quoted order-queue fields, so payloads
are parsed by Polars rather than by splitting on commas.
"""

from __future__ import annotations

import io
import json
import pathlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Final, Literal, Protocol
from zoneinfo import ZoneInfo

import polars as pl
import redis
from redis.exceptions import RedisError


Market = Literal["SH", "SZ"]
PayloadValue = bytes | bytearray | memoryview | str

PAYLOAD_ENCODING: Final = "gbk"
DEFAULT_TIMEZONE: Final = "Asia/Shanghai"
_SECURITY_ID_PATTERN: Final = re.compile(r"^[0-9]{6}$")


class FeederLatestError(RuntimeError):
    """Base error raised by the feeder latest-value client."""


class FeederConfigError(FeederLatestError):
    """The feeder configuration is missing or invalid."""


class FeederConnectionError(FeederLatestError):
    """A Redis command could not be completed."""


class UnknownDatasetError(FeederLatestError):
    """The requested latest-value dataset is not supported."""


class LatestNotFoundError(FeederLatestError):
    """Redis has no latest value for one or more requested keys."""


class PayloadDecodeError(FeederLatestError):
    """A Redis payload is not valid GBK text."""


class PayloadParseError(FeederLatestError):
    """A decoded Redis payload is not one valid CSV record."""


class SchemaMismatchError(PayloadParseError):
    """A parsed record does not match a documented feeder schema width."""


class PayloadIdentityError(PayloadParseError):
    """The SecurityID in a payload does not match its Redis key."""


class TypedFrameError(FeederLatestError):
    """A confirmed numeric field could not be converted strictly."""


class _RedisReader(Protocol):
    def get(self, name: str) -> PayloadValue | None: ...

    def mget(self, keys: Sequence[str]) -> list[PayloadValue | None]: ...

    def info(self) -> Mapping[str, Any]: ...

    def close(self) -> None: ...


def _price_levels(side: str) -> tuple[str, ...]:
    return tuple(
        field
        for level in range(1, 11)
        for field in (f"{side}Price{level}", f"{side}Volume{level}")
    )


def _order_counts(side: str) -> tuple[str, ...]:
    return tuple(f"NumOrders{side}{level}" for level in range(1, 11))


_SH_SNAPSHOT_BASE: Final = (
    "UpdateTime",
    "SecurityID",
    "ImageStatus",
    "PreCloPrice",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "LastPrice",
    "ClosePrice",
    "InstruStatus",
    "TradNumber",
    "TradVolume",
    "Turnover",
    "TotalBidVol",
    "WAvgBidPri",
    "AltWAvgBidPri",
    "TotalAskVol",
    "WAvgAskPri",
    "AltWAvgAskPri",
    "EtfBuyNumber",
    "EtfBuyVolume",
    "EtfBuyMoney",
    "EtfSellNumber",
    "EtfSellVolume",
    "ETFSellMoney",
    "YieldToMatu",
    "TotWarExNum",
    "WarLowerPri",
    "WarUpperPri",
    "WiDBuyNum",
    "WiDBuyVol",
    "WiDBuyMon",
    "WiDSellNum",
    "WiDSellVol",
    "WiDSellMon",
    "TotBidNum",
    "TotSellNum",
    "MaxBidDur",
    "MaxSellDur",
    "BidNum",
    "SellNum",
    "IOPV",
    *_price_levels("Ask"),
    *_price_levels("Bid"),
    *_order_counts("B"),
    *_order_counts("S"),
    "LocalTime",
    "SeqNo",
)

_SH_SNAPSHOT_WITH_QUEUES: Final = (
    *_SH_SNAPSHOT_BASE[:-2],
    "BidOrderQueue",
    "AskOrderQueue",
    *_SH_SNAPSHOT_BASE[-2:],
)

_SZ_SNAPSHOT_BASE: Final = (
    "UpdateTime",
    "MDStreamID",
    "SecurityID",
    "SecurityIDSource",
    "TradingPhaseCode",
    "PreCloPrice",
    "TurnNum",
    "Volume",
    "Turnover",
    "LastPrice",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "DifPrice1",
    "DifPrice2",
    "PE1",
    "PE2",
    "PreCloseIOPV",
    "IOPV",
    "TotalBidQty",
    "WeightedAvgBidPx",
    "TotalOfferQty",
    "WeightedAvgOfferPx",
    "HighLimitPrice",
    "LowLimitPrice",
    "OpenInt",
    "OptPremiumRatio",
    *_price_levels("Ask"),
    *_price_levels("Bid"),
    *_order_counts("B"),
    *_order_counts("S"),
    "LocalTime",
    "SeqNo",
)

_SZ_SNAPSHOT_WITH_QUEUES: Final = (
    *_SZ_SNAPSHOT_BASE[:-2],
    "AskOrderQueue",
    "BidOrderQueue",
    *_SZ_SNAPSHOT_BASE[-2:],
)

_SH_L2_TICK: Final = (
    "BizIndex",
    "Channel",
    "SecurityID",
    "TickTime",
    "Type",
    "BuyOrderNO",
    "SellOrderNO",
    "Price",
    "Qty",
    "TradeMoney",
    "TickBSFlag",
    "LocalTime",
    "SeqNo",
)

_SZ_L2_ORDER: Final = (
    "ChannelNo",
    "ApplSeqNum",
    "MDStreamID",
    "SecurityID",
    "SecurityIDSource",
    "Price",
    "OrderQty",
    "Side",
    "TransactTime",
    "OrdType",
    "LocalTime",
    "SeqNo",
)

_SZ_L2_TRADE: Final = (
    "ChannelNo",
    "ApplSeqNum",
    "MDStreamID",
    "BidApplSeqNum",
    "OfferApplSeqNum",
    "SecurityID",
    "SecurityIDSource",
    "LastPx",
    "LastQty",
    "ExecType",
    "TransactTime",
    "LocalTime",
    "SeqNo",
)


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Static Redis key and CSV schema metadata for one message family."""

    name: str
    market: Market
    kind: str
    service_id: int
    message_id: int
    exchange_time_field: str
    description: str
    schemas: tuple[tuple[str, tuple[str, ...]], ...]

    @property
    def key_template(self) -> str:
        return f"mdl.{self.service_id}.{self.message_id}.{{security_id}}"

    def key(self, security_id: str) -> str:
        return self.key_template.format(security_id=security_id)

    def schema_for_width(self, width: int) -> tuple[str, tuple[str, ...]] | None:
        return next(
            (
                (variant, fields)
                for variant, fields in self.schemas
                if len(fields) == width
            ),
            None,
        )

    @property
    def expected_widths(self) -> tuple[int, ...]:
        return tuple(len(fields) for _, fields in self.schemas)


DATASETS: Final[Mapping[str, DatasetSpec]] = MappingProxyType(
    {
        "sh_snapshot": DatasetSpec(
            name="sh_snapshot",
            market="SH",
            kind="snapshot",
            service_id=4,
            message_id=4,
            exchange_time_field="UpdateTime",
            description="Shanghai Level-2 ten-level snapshot",
            schemas=(
                ("base", _SH_SNAPSHOT_BASE),
                ("with_l2_order_queues", _SH_SNAPSHOT_WITH_QUEUES),
            ),
        ),
        "sz_snapshot": DatasetSpec(
            name="sz_snapshot",
            market="SZ",
            kind="snapshot",
            service_id=6,
            message_id=28,
            exchange_time_field="UpdateTime",
            description="Shenzhen Level-2 ten-level snapshot",
            schemas=(
                ("base", _SZ_SNAPSHOT_BASE),
                ("with_l2_order_queues", _SZ_SNAPSHOT_WITH_QUEUES),
            ),
        ),
        "sh_l2_tick": DatasetSpec(
            name="sh_l2_tick",
            market="SH",
            kind="merged_tick",
            service_id=4,
            message_id=24,
            exchange_time_field="TickTime",
            description="Shanghai Level-2 merged order/trade tick",
            schemas=(("base", _SH_L2_TICK),),
        ),
        "sz_l2_order": DatasetSpec(
            name="sz_l2_order",
            market="SZ",
            kind="order",
            service_id=6,
            message_id=33,
            exchange_time_field="TransactTime",
            description="Shenzhen Level-2 order tick",
            schemas=(("base", _SZ_L2_ORDER),),
        ),
        "sz_l2_trade": DatasetSpec(
            name="sz_l2_trade",
            market="SZ",
            kind="trade",
            service_id=6,
            message_id=36,
            exchange_time_field="TransactTime",
            description="Shenzhen Level-2 trade tick",
            schemas=(("base", _SZ_L2_TRADE),),
        ),
    }
)


_SH_DECIMAL_3_FIELDS: Final = (
    "PreCloPrice",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "LastPrice",
    "ClosePrice",
    "TradVolume",
    "TotalBidVol",
    "WAvgBidPri",
    "AltWAvgBidPri",
    "TotalAskVol",
    "WAvgAskPri",
    "AltWAvgAskPri",
    "EtfBuyVolume",
    "EtfSellVolume",
    "TotWarExNum",
    "WarLowerPri",
    "WiDBuyVol",
    "WiDSellVol",
    "IOPV",
    *tuple(f"AskPrice{level}" for level in range(1, 11)),
    *tuple(f"AskVolume{level}" for level in range(1, 11)),
    *tuple(f"BidPrice{level}" for level in range(1, 11)),
    *tuple(f"BidVolume{level}" for level in range(1, 11)),
)

_SH_DECIMAL_5_FIELDS: Final = (
    "Turnover",
    "EtfBuyMoney",
    "ETFSellMoney",
    "WarUpperPri",
    "WiDBuyMon",
    "WiDSellMon",
)

_SH_UNSIGNED_COUNT_FIELDS: Final = (
    "TradNumber",
    "EtfBuyNumber",
    "EtfSellNumber",
    "WiDBuyNum",
    "WiDSellNum",
    "TotBidNum",
    "TotSellNum",
    "MaxBidDur",
    "MaxSellDur",
    "BidNum",
    "SellNum",
    *_order_counts("B"),
    *_order_counts("S"),
    "SeqNo",
)

_SZ_DECIMAL_6_FIELDS: Final = (
    "LastPrice",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "DifPrice1",
    "DifPrice2",
    "PE1",
    "PE2",
    "PreCloseIOPV",
    "IOPV",
    "WeightedAvgBidPx",
    "WeightedAvgOfferPx",
    "HighLimitPrice",
    "LowLimitPrice",
    "OptPremiumRatio",
    *tuple(f"AskPrice{level}" for level in range(1, 11)),
    *tuple(f"BidPrice{level}" for level in range(1, 11)),
)

_SZ_SIGNED_COUNT_FIELDS: Final = (
    "TurnNum",
    "Volume",
    "TotalBidQty",
    "TotalOfferQty",
    "OpenInt",
    *tuple(f"AskVolume{level}" for level in range(1, 11)),
    *tuple(f"BidVolume{level}" for level in range(1, 11)),
)

_NUMERIC_SCHEMA_MUTABLE: dict[str, dict[str, Any]] = {
    "sh_snapshot": {
        **{field: pl.Decimal(precision=38, scale=3) for field in _SH_DECIMAL_3_FIELDS},
        **{field: pl.Decimal(precision=38, scale=5) for field in _SH_DECIMAL_5_FIELDS},
        "YieldToMatu": pl.Decimal(precision=38, scale=4),
        **{field: pl.UInt64 for field in _SH_UNSIGNED_COUNT_FIELDS},
    },
    "sz_snapshot": {
        "PreCloPrice": pl.Decimal(precision=38, scale=4),
        "Turnover": pl.Decimal(precision=38, scale=4),
        **{field: pl.Decimal(precision=38, scale=6) for field in _SZ_DECIMAL_6_FIELDS},
        **{field: pl.Int64 for field in _SZ_SIGNED_COUNT_FIELDS},
        **{field: pl.UInt64 for field in _order_counts("B")},
        **{field: pl.UInt64 for field in _order_counts("S")},
        "SeqNo": pl.UInt64,
    },
    "sh_l2_tick": {
        "BizIndex": pl.Int64,
        "Channel": pl.Int64,
        "BuyOrderNO": pl.Int64,
        "SellOrderNO": pl.Int64,
        "Price": pl.Decimal(precision=38, scale=3),
        "Qty": pl.Int64,
        "TradeMoney": pl.Decimal(precision=38, scale=3),
        "SeqNo": pl.UInt64,
    },
    "sz_l2_order": {
        "ChannelNo": pl.UInt64,
        "ApplSeqNum": pl.Int64,
        "Price": pl.Decimal(precision=38, scale=4),
        "OrderQty": pl.Int64,
        "SeqNo": pl.UInt64,
    },
    "sz_l2_trade": {
        "ChannelNo": pl.UInt64,
        "ApplSeqNum": pl.Int64,
        "BidApplSeqNum": pl.Int64,
        "OfferApplSeqNum": pl.Int64,
        "LastPx": pl.Decimal(precision=38, scale=4),
        "LastQty": pl.Int64,
        "SeqNo": pl.UInt64,
    },
}

# The conversion table comes from the exact SDK message types corresponding to
# service/message IDs 4/4, 4/24, 6/28, 6/33, and 6/36.  It is intentionally
# field-specific: values are never inferred from the spelling of one record.
NUMERIC_SCHEMA: Final[Mapping[str, Mapping[str, Any]]] = MappingProxyType(
    {
        dataset: MappingProxyType(fields)
        for dataset, fields in _NUMERIC_SCHEMA_MUTABLE.items()
    }
)


@dataclass(frozen=True, slots=True)
class LatestUpdate:
    """One latest feeder record plus its source and timing metadata."""

    dataset: str
    market: Market
    kind: str
    security_id: str
    redis_key: str
    frame: pl.DataFrame
    data: Mapping[str, str | None]
    raw_csv: str
    exchange_update_time: str | None
    feeder_local_time: str | None
    sequence_no: str | None
    queried_at: datetime
    schema_variant: str

    def typed_frame(self, fields: Iterable[str] | None = None) -> pl.DataFrame:
        """Return a derived frame with confirmed numeric fields strictly typed.

        The original ``frame``, ``data``, and ``raw_csv`` remain unchanged.  If
        ``fields`` is omitted, every field registered for this dataset is cast;
        otherwise only the requested registered fields are cast.  Unregistered
        fields require an explicit caller-side Polars expression.
        """

        schema = NUMERIC_SCHEMA[self.dataset]
        if fields is None:
            selected_fields = tuple(schema)
        else:
            selected_fields = tuple(fields)
            invalid = [
                field
                for field in selected_fields
                if not isinstance(field, str) or field not in schema
            ]
            if invalid:
                raise ValueError(
                    f"dataset {self.dataset!r} has no confirmed numeric type for: "
                    + ", ".join(repr(field) for field in invalid)
                )

        typed = self.frame.clone()
        for field in selected_fields:
            dtype = schema[field]
            try:
                typed = typed.with_columns(
                    pl.col(field).cast(dtype, strict=True).alias(field)
                )
            except (
                pl.exceptions.PolarsError,
                TypeError,
                ValueError,
                OverflowError,
            ) as exc:
                original_value = self.data.get(field)
                raise TypedFrameError(
                    f"strict typed_frame conversion failed: dataset={self.dataset!r}, "
                    f"redis_key={self.redis_key!r}, field={field!r}, "
                    f"dtype={dtype}, raw_value={original_value!r}"
                ) from exc
        return typed

    def to_dict(self) -> dict[str, Any]:
        """Return all result data in a JSON-serializable representation."""

        return {
            "dataset": self.dataset,
            "market": self.market,
            "kind": self.kind,
            "security_id": self.security_id,
            "redis_key": self.redis_key,
            "schema_variant": self.schema_variant,
            "field_count": self.frame.width,
            "data": dict(self.data),
            "raw_csv": self.raw_csv,
            "exchange_update_time": self.exchange_update_time,
            "feeder_local_time": self.feeder_local_time,
            "sequence_no": self.sequence_no,
            "queried_at": self.queried_at.isoformat(),
        }


def _dataset_spec(dataset: str) -> DatasetSpec:
    if not isinstance(dataset, str):
        raise TypeError("dataset must be a string")
    try:
        return DATASETS[dataset]
    except KeyError as exc:
        supported = ", ".join(DATASETS)
        raise UnknownDatasetError(
            f"unsupported dataset {dataset!r}; choose one of: {supported}"
        ) from exc


def _security_id(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(
            "security_id must be a six-digit string so leading zeros are preserved"
        )
    if _SECURITY_ID_PATTERN.fullmatch(value) is None:
        raise ValueError("security_id must contain exactly six ASCII digits")
    return value


def _market(value: str) -> Market:
    if not isinstance(value, str):
        raise TypeError("market must be 'SH' or 'SZ'")
    normalized = value.upper()
    if normalized == "SH":
        return "SH"
    if normalized == "SZ":
        return "SZ"
    raise ValueError("market must be 'SH' or 'SZ'")


def _decode_payload(payload: PayloadValue, redis_key: str) -> str:
    if isinstance(payload, str):
        return payload
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise PayloadDecodeError(
            f"Redis key {redis_key!r} returned unsupported payload type "
            f"{type(payload).__name__}"
        )
    try:
        return bytes(payload).decode(PAYLOAD_ENCODING, errors="strict")
    except UnicodeDecodeError as exc:
        raise PayloadDecodeError(
            f"Redis key {redis_key!r} is not valid {PAYLOAD_ENCODING.upper()}"
        ) from exc


def _parse_payload(
    raw_csv: str, spec: DatasetSpec, redis_key: str
) -> tuple[pl.DataFrame, str]:
    # Redis values occasionally retain line endings from the CSV publisher.
    csv_record = raw_csv.rstrip("\r\n")
    if not csv_record:
        raise PayloadParseError(f"Redis key {redis_key!r} contains an empty CSV record")
    if csv_record.endswith(",,"):
        raise SchemaMismatchError(
            f"Redis key {redis_key!r} has more than one trailing empty CSV field"
        )
    # feeder_client writes one delimiter after SeqNo.  Normalize exactly that
    # publisher trailer before parsing; a missing/extra schema field must not be
    # able to masquerade as the trailer during width validation.
    parse_record = csv_record[:-1] if csv_record.endswith(",") else csv_record

    try:
        frame = pl.read_csv(
            io.StringIO(parse_record),
            has_header=False,
            separator=",",
            quote_char='"',
            infer_schema=False,
            try_parse_dates=False,
            truncate_ragged_lines=False,
            raise_if_empty=True,
        )
    except pl.exceptions.PolarsError as exc:
        raise PayloadParseError(
            f"Redis key {redis_key!r} is not valid feeder CSV: {exc}"
        ) from exc

    if frame.height != 1:
        raise PayloadParseError(
            f"Redis key {redis_key!r} must contain exactly one CSV row; "
            f"parsed {frame.height} rows"
        )

    schema_match = spec.schema_for_width(frame.width)
    if schema_match is None:
        expected = ", ".join(str(width) for width in spec.expected_widths)
        raise SchemaMismatchError(
            f"Redis key {redis_key!r} has {frame.width} CSV fields; "
            f"expected exactly one of [{expected}] after at most one empty trailer"
        )

    schema_variant, fields = schema_match
    frame.columns = list(fields)
    if any(dtype != pl.String for dtype in frame.dtypes):
        raise PayloadParseError(
            f"Redis key {redis_key!r} was not parsed entirely as Polars String columns"
        )
    return frame, schema_variant


def _parse_address(address: str) -> tuple[str, int]:
    if not isinstance(address, str) or not address:
        raise FeederConfigError("REDIS_SERVER Address must be a non-empty string")
    if address.startswith("["):
        bracket = address.find("]")
        if bracket < 0 or address[bracket + 1 : bracket + 2] != ":":
            raise FeederConfigError(f"invalid REDIS_SERVER Address: {address!r}")
        host = address[1:bracket]
        port_text = address[bracket + 2 :]
    else:
        host, separator, port_text = address.rpartition(":")
        if not separator:
            raise FeederConfigError(f"invalid REDIS_SERVER Address: {address!r}")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise FeederConfigError(f"invalid REDIS_SERVER port in {address!r}") from exc
    if not host or not 1 <= port <= 65535:
        raise FeederConfigError(f"invalid REDIS_SERVER Address: {address!r}")
    return host, port


def _keyspace_summary(server_info: Mapping[str, Any]) -> dict[str, Any]:
    databases: dict[str, dict[str, int]] = {}
    total_keys = 0
    for name, value in server_info.items():
        if re.fullmatch(r"db[0-9]+", str(name)) is None or not isinstance(
            value, Mapping
        ):
            continue
        parsed: dict[str, int] = {}
        for field in ("keys", "expires", "avg_ttl"):
            item = value.get(field)
            if isinstance(item, int):
                parsed[field] = item
        if parsed:
            databases[str(name)] = parsed
            total_keys += parsed.get("keys", 0)
    return {"total_keys": total_keys, "databases": databases}


class FeederLatestClient:
    """Read-only client for feeder_client's latest-value Redis keys."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9379,
        *,
        socket_timeout: float = 2.0,
        timezone: str = DEFAULT_TIMEZONE,
        redis_client: _RedisReader | None = None,
        enable_l2_orders: bool | None = None,
        config_path: pathlib.Path | None = None,
    ) -> None:
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("port must be an integer in [1, 65535]")
        if socket_timeout <= 0:
            raise ValueError("socket_timeout must be positive")
        if enable_l2_orders is not None and not isinstance(enable_l2_orders, bool):
            raise TypeError("enable_l2_orders must be bool or None")

        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.timezone = ZoneInfo(timezone)
        self.enable_l2_orders = enable_l2_orders
        self.config_path = config_path
        if redis_client is None:
            self._redis = redis.Redis(
                host=host,
                port=port,
                db=0,
                decode_responses=False,
                protocol=2,
                driver_info=None,
                socket_connect_timeout=socket_timeout,
                socket_timeout=socket_timeout,
            )
        else:
            self._redis = redis_client

    @classmethod
    def from_feeder_config(
        cls,
        config_path: str | pathlib.Path,
        *,
        host: str | None = None,
        **kwargs: Any,
    ) -> FeederLatestClient:
        """Build a client from feeder_client.cfg without reading credentials."""

        path = pathlib.Path(config_path).expanduser()
        try:
            path = path.resolve(strict=True)
            document = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, RuntimeError, UnicodeError, json.JSONDecodeError) as exc:
            raise FeederConfigError(f"cannot read feeder configuration {path}") from exc
        if not isinstance(document, Mapping):
            raise FeederConfigError("feeder configuration root must be a JSON object")

        feeder_section = document.get("feeder_client", document)
        if not isinstance(feeder_section, Mapping):
            raise FeederConfigError("feeder_client configuration must be a JSON object")
        publishers = feeder_section.get("Publishers")
        if not isinstance(publishers, list):
            raise FeederConfigError("feeder_client.Publishers must be a JSON array")
        redis_publishers = [
            publisher
            for publisher in publishers
            if isinstance(publisher, Mapping)
            and str(publisher.get("Type", "")).upper() == "REDIS_SERVER"
        ]
        if len(redis_publishers) != 1:
            raise FeederConfigError(
                "feeder configuration must contain exactly one REDIS_SERVER publisher"
            )

        publisher = redis_publishers[0]
        bind_host, port = _parse_address(publisher.get("Address"))
        encoding = publisher.get("Encoding")
        if encoding not in (None, 5):
            raise FeederConfigError(
                "REDIS_SERVER Encoding must be 5 (GBK) for this client"
            )
        connect_host = host
        if connect_host is None:
            connect_host = (
                "127.0.0.1" if bind_host in {"0.0.0.0", "::", "*"} else bind_host
            )
        enable_l2_orders = publisher.get("EnableL2Orders", False)
        if not isinstance(enable_l2_orders, bool):
            raise FeederConfigError("EnableL2Orders must be a JSON boolean")
        return cls(
            host=connect_host,
            port=port,
            enable_l2_orders=enable_l2_orders,
            config_path=path,
            **kwargs,
        )

    def __enter__(self) -> FeederLatestClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._redis.close()

    def _now(self) -> datetime:
        return datetime.now(self.timezone)

    def _result(
        self,
        spec: DatasetSpec,
        security_id: str,
        redis_key: str,
        payload: PayloadValue,
        queried_at: datetime,
    ) -> LatestUpdate:
        raw_csv = _decode_payload(payload, redis_key)
        frame, schema_variant = _parse_payload(raw_csv, spec, redis_key)
        row = frame.row(0, named=True)
        payload_security_id = row.get("SecurityID")
        if payload_security_id != security_id:
            raise PayloadIdentityError(
                f"Redis key {redis_key!r} contains SecurityID "
                f"{payload_security_id!r}, expected {security_id!r}"
            )
        for required_field in (spec.exchange_time_field, "LocalTime", "SeqNo"):
            if row.get(required_field) in (None, ""):
                raise PayloadParseError(
                    f"Redis key {redis_key!r} has no required {required_field} value"
                )
        data = MappingProxyType(dict(row))
        return LatestUpdate(
            dataset=spec.name,
            market=spec.market,
            kind=spec.kind,
            security_id=security_id,
            redis_key=redis_key,
            frame=frame,
            data=data,
            raw_csv=raw_csv,
            exchange_update_time=row.get(spec.exchange_time_field),
            feeder_local_time=row.get("LocalTime"),
            sequence_no=row.get("SeqNo"),
            queried_at=queried_at,
            schema_variant=schema_variant,
        )

    def latest(self, dataset: str, security_id: str) -> LatestUpdate:
        """GET one latest snapshot/order/trade record for a known security."""

        spec = _dataset_spec(dataset)
        normalized_security_id = _security_id(security_id)
        redis_key = spec.key(normalized_security_id)
        try:
            payload = self._redis.get(redis_key)
        except RedisError as exc:
            raise FeederConnectionError(f"Redis GET failed for {redis_key!r}") from exc
        queried_at = self._now()
        if payload is None:
            raise LatestNotFoundError(f"Redis has no latest value for {redis_key!r}")
        return self._result(
            spec, normalized_security_id, redis_key, payload, queried_at
        )

    def latest_many(
        self, requests: Iterable[tuple[str, str]]
    ) -> list[LatestUpdate]:
        """MGET multiple latest records and give them one response timestamp."""

        prepared: list[tuple[DatasetSpec, str, str]] = []
        for dataset, security_id in requests:
            spec = _dataset_spec(dataset)
            normalized_security_id = _security_id(security_id)
            prepared.append(
                (spec, normalized_security_id, spec.key(normalized_security_id))
            )
        if not prepared:
            return []

        keys = [redis_key for _, _, redis_key in prepared]
        try:
            payloads = self._redis.mget(keys)
        except RedisError as exc:
            raise FeederConnectionError("Redis MGET failed") from exc
        queried_at = self._now()
        if len(payloads) != len(prepared):
            raise FeederConnectionError(
                f"Redis MGET returned {len(payloads)} values for {len(prepared)} keys"
            )
        missing = [key for key, payload in zip(keys, payloads) if payload is None]
        if missing:
            raise LatestNotFoundError(
                "Redis has no latest value for: "
                + ", ".join(repr(key) for key in missing)
            )

        return [
            self._result(spec, security_id, redis_key, payload, queried_at)
            for (spec, security_id, redis_key), payload in zip(prepared, payloads)
            if payload is not None
        ]

    def latest_snapshot(self, market: str, security_id: str) -> LatestUpdate:
        """Return the latest Shanghai or Shenzhen Level-2 snapshot."""

        normalized_market = _market(market)
        dataset = "sh_snapshot" if normalized_market == "SH" else "sz_snapshot"
        return self.latest(dataset, security_id)

    def latest_l2(
        self, market: str, security_id: str
    ) -> dict[str, LatestUpdate]:
        """Return SH merged tick, or SZ order and trade in one MGET."""

        normalized_market = _market(market)
        if normalized_market == "SH":
            return {"tick": self.latest("sh_l2_tick", security_id)}
        updates = self.latest_many(
            (("sz_l2_order", security_id), ("sz_l2_trade", security_id))
        )
        return {"order": updates[0], "trade": updates[1]}

    def info(self, *, probe_server: bool = True) -> dict[str, Any]:
        """Describe queryable latest types and optionally inspect Redis INFO."""

        server: dict[str, Any] | None = None
        if probe_server:
            try:
                raw_server_info = self._redis.info()
            except RedisError as exc:
                raise FeederConnectionError("Redis INFO failed") from exc
            if not isinstance(raw_server_info, Mapping):
                raise FeederConnectionError("Redis INFO returned an invalid response")
            server = {"reachable": True}
            for field in (
                "redis_version",
                "compiler",
                "uptime_in_seconds",
                "connected_clients",
                "used_memory",
                "used_memory_human",
            ):
                if field in raw_server_info:
                    server[field] = raw_server_info[field]
            server["keyspace"] = _keyspace_summary(raw_server_info)

        query_types: dict[str, Any] = {}
        for name, spec in DATASETS.items():
            query_types[name] = {
                "market": spec.market,
                "kind": spec.kind,
                "description": spec.description,
                "redis_key_template": spec.key_template,
                "exchange_update_time_field": spec.exchange_time_field,
                "feeder_update_time_field": "LocalTime",
                "sequence_field": "SeqNo",
                "schema_variants": {
                    variant: len(fields) for variant, fields in spec.schemas
                },
                "typed_numeric_fields": {
                    field: str(dtype) for field, dtype in NUMERIC_SCHEMA[name].items()
                },
            }

        return {
            "endpoint": {
                "host": self.host,
                "port": self.port,
                "database": 0,
                "protocol": "RESP2",
                "payload_encoding": PAYLOAD_ENCODING.upper(),
                "timezone": self.timezone.key,
            },
            "server": server,
            "enable_l2_orders": self.enable_l2_orders,
            "latest_query_types": query_types,
            "discovery": {
                "scan_supported": False,
                "security_ids_enumerable": False,
                "note": (
                    "The current feeder Redis does not implement SCAN; query a known "
                    "six-digit SecurityID and treat a nil GET as unavailable."
                ),
            },
            "time_semantics": {
                "exchange_update_time": (
                    "Raw intraday UpdateTime, TickTime, or TransactTime from the "
                    "payload."
                ),
                "feeder_local_time": "Raw feeder_client LocalTime from the payload.",
                "queried_at": (
                    "Timezone-aware client timestamp taken after the Redis response."
                ),
                "trading_date_available": False,
                "note": (
                    "Redis payloads do not contain a trading date; this client does "
                    "not combine an intraday time with the current date."
                ),
            },
        }


__all__ = [
    "DATASETS",
    "NUMERIC_SCHEMA",
    "DatasetSpec",
    "FeederConfigError",
    "FeederConnectionError",
    "FeederLatestClient",
    "FeederLatestError",
    "LatestNotFoundError",
    "LatestUpdate",
    "PayloadDecodeError",
    "PayloadIdentityError",
    "PayloadParseError",
    "SchemaMismatchError",
    "TypedFrameError",
    "UnknownDatasetError",
]
