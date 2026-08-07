"""Lease-safe Python reader for L2Flow's volatile Arrow hot path."""

from __future__ import annotations

import ctypes
import ctypes.util
import dataclasses
import os
import pathlib
from collections.abc import Mapping
from typing import Final, Literal

import pyarrow as pa


START_LATEST: Final = 0
START_EARLIEST_AVAILABLE: Final = 1

READ_BATCH: Final = 0
READ_EMPTY: Final = 1
READ_OVERRUN: Final = 2
READ_RETRY: Final = 3
READ_CORRUPT: Final = 4
READ_CLOSED: Final = 5
READ_API_ERROR: Final = 6

STREAM_ORDERED_TICK: Final = 1
STREAM_SNAPSHOT: Final = 2
STREAM_LATE_RECOVERY: Final = 3
STREAM_CONTROL: Final = 4

PRODUCER_INITIALIZING: Final = 0
PRODUCER_ACTIVE: Final = 1
PRODUCER_SEALED: Final = 2
RING_PROTOCOL_VERSION: Final = 2
LIBRARY_ABI_VERSION: Final = b"l2flow-arrow-hot/2 arrow-ipc-v5"

_ERROR_BYTES: Final = 2048
_StreamName = Literal["tick", "snapshot", "late_recovery", "control"]
_StartName = Literal["latest", "earliest"]


class ArrowHotError(RuntimeError):
    """Base exception for the shared-memory Arrow reader."""


class ArrowHotOverrun(ArrowHotError):
    """The requested descriptor was overwritten before it was acquired."""

    def __init__(self, oldest_available: int, newest_available: int) -> None:
        self.oldest_available = oldest_available
        self.newest_available = newest_available
        super().__init__(
            "Arrow hot ring overrun: requested sequence is no longer retained; "
            f"available=[{oldest_available}, {newest_available}]"
        )


class _CBatch(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("size", ctypes.c_size_t),
        ("lease", ctypes.c_void_p),
        ("batch_sequence", ctypes.c_uint64),
        ("oldest_available_sequence", ctypes.c_uint64),
        ("newest_available_sequence", ctypes.c_uint64),
        ("feed_session_epoch", ctypes.c_uint64),
        ("first_ingress_sequence", ctypes.c_uint64),
        ("last_ingress_sequence", ctypes.c_uint64),
        ("minimum_exchange_time_ns", ctypes.c_uint64),
        ("maximum_exchange_time_ns", ctypes.c_uint64),
        ("publish_monotonic_ns", ctypes.c_uint64),
        ("producer_instance_high", ctypes.c_uint64),
        ("producer_instance_low", ctypes.c_uint64),
        ("row_count", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
    ]


if ctypes.sizeof(_CBatch) != 120:
    raise RuntimeError("l2flow-arrow-hot/2 requires the 64-bit C batch ABI")


@dataclasses.dataclass(frozen=True, slots=True)
class BatchMetadata:
    batch_sequence: int
    oldest_available_sequence: int
    newest_available_sequence: int
    feed_session_epoch: int
    first_ingress_sequence: int
    last_ingress_sequence: int
    minimum_exchange_time_ns: int
    maximum_exchange_time_ns: int
    publish_monotonic_ns: int
    producer_instance_high: int
    producer_instance_low: int
    row_count: int
    flags: int


@dataclasses.dataclass(frozen=True, slots=True)
class HotRecordBatch:
    batch: pa.RecordBatch
    metadata: BatchMetadata

    def to_polars(self):
        """Convert through Polars' Arrow bridge without forced rechunking."""
        import polars as pl

        return pl.from_arrow(self.batch, rechunk=False)


@dataclasses.dataclass(frozen=True, slots=True)
class RingManifest:
    root: pathlib.Path
    run_directory: pathlib.Path
    values: Mapping[str, str]

    @property
    def feed_session_epoch(self) -> int:
        try:
            epoch = int(self.values["feed_session_epoch"])
        except (KeyError, ValueError) as exc:
            raise ArrowHotError("manifest has an invalid feed_session_epoch") from exc
        if epoch <= 0:
            raise ArrowHotError("manifest feed_session_epoch must be positive")
        return epoch

    @property
    def producer_instance(self) -> tuple[int, int]:
        try:
            encoded = self.values["producer_instance"]
        except KeyError as exc:
            raise ArrowHotError("manifest has no producer_instance") from exc
        if len(encoded) != 32 or any(
            character not in "0123456789abcdef" for character in encoded
        ):
            raise ArrowHotError("manifest has an invalid producer_instance")
        value = int(encoded, 16)
        return value >> 64, value & ((1 << 64) - 1)

    @classmethod
    def open_current(cls, root: os.PathLike[str] | str) -> RingManifest:
        root_path = pathlib.Path(root).resolve(strict=True)
        current_lines = (root_path / "CURRENT").read_text(
            encoding="ascii"
        ).splitlines()
        if len(current_lines) != 1 or current_lines[0] != current_lines[0].strip():
            raise ArrowHotError("CURRENT must contain exactly one directory name")
        current = current_lines[0]
        if not current or pathlib.Path(current).name != current or current in {".", ".."}:
            raise ArrowHotError("CURRENT contains an unsafe producer directory")
        run_directory = root_path / current
        if not run_directory.is_dir():
            raise ArrowHotError(f"current producer directory is missing: {run_directory}")
        values: dict[str, str] = {}
        manifest_path = run_directory / "manifest.txt"
        for line_number, raw_line in enumerate(
            manifest_path.read_text(encoding="ascii").splitlines(), start=1
        ):
            if not raw_line or raw_line.startswith("#"):
                continue
            key, separator, value = raw_line.partition("=")
            if not separator or not key or not value or key in values:
                raise ArrowHotError(
                    f"invalid manifest entry at {manifest_path}:{line_number}"
                )
            values[key] = value
        if values.get("format") != "l2flow-arrow-hot-v1":
            raise ArrowHotError("unsupported Arrow hot manifest format")
        if values.get("protocol_version") != str(RING_PROTOCOL_VERSION):
            raise ArrowHotError("unsupported Arrow hot protocol version")
        return cls(root_path, run_directory, values)

    def ring_paths(self, stream: _StreamName, owner: int = 0) -> tuple[pathlib.Path, pathlib.Path]:
        if owner < 0:
            raise ArrowHotError("owner must be nonnegative")
        if stream in {"tick", "snapshot"}:
            key = f"{stream}.{owner}"
            control_key = f"{stream}_control.{owner}"
        else:
            key = stream
            control_key = f"{stream}_control" if stream == "late_recovery" else "control_control"
        try:
            data_name = self.values[key]
            control_name = self.values[control_key]
        except KeyError as exc:
            raise ArrowHotError(
                f"stream {stream!r} owner {owner} is absent from the manifest"
            ) from exc
        for value in (data_name, control_name):
            if pathlib.Path(value).name != value or value in {".", ".."}:
                raise ArrowHotError("manifest contains an unsafe ring filename")
        return self.run_directory / data_name, self.run_directory / control_name


class _LeaseOwner:
    __slots__ = ("_library", "_batch", "_released")

    def __init__(self, library: ctypes.CDLL, batch: _CBatch) -> None:
        self._library = library
        self._batch = batch
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._library.l2flow_arrow_batch_release(ctypes.byref(self._batch))
            self._released = True

    def __del__(self) -> None:
        self.release()


def _error_text(buffer: ctypes.Array[ctypes.c_char]) -> str:
    return bytes(buffer.value).decode("utf-8", errors="replace")


def _candidate_libraries() -> list[pathlib.Path | str]:
    candidates: list[pathlib.Path | str] = []
    configured = os.environ.get("L2FLOW_ARROW_LIBRARY")
    if configured:
        candidates.append(pathlib.Path(configured))
    repository = pathlib.Path(__file__).resolve().parent.parent
    candidates.extend(
        [
            repository / "build-arrow" / "libl2flow_ch_arrow.so",
            repository / "build" / "libl2flow_ch_arrow.so",
        ]
    )
    system = ctypes.util.find_library("l2flow_ch_arrow")
    if system:
        candidates.append(system)
    return candidates


def _load_library(path: os.PathLike[str] | str | None) -> ctypes.CDLL:
    candidates = [pathlib.Path(path)] if path is not None else _candidate_libraries()
    failures: list[str] = []
    for candidate in candidates:
        try:
            library = ctypes.CDLL(str(candidate))
            _bind_library(library)
            return library
        except (OSError, AttributeError, ArrowHotError) as exc:
            failures.append(f"{candidate}: {exc}")
    raise ArrowHotError("cannot load libl2flow_ch_arrow.so; " + "; ".join(failures))


def _bind_library(library: ctypes.CDLL) -> None:
    library.l2flow_arrow_library_version.argtypes = []
    library.l2flow_arrow_library_version.restype = ctypes.c_char_p
    version = library.l2flow_arrow_library_version()
    if version != LIBRARY_ABI_VERSION:
        actual = "<null>" if version is None else version.decode(
            "ascii", errors="replace"
        )
        raise ArrowHotError(
            "incompatible libl2flow_ch_arrow ABI: "
            f"expected {LIBRARY_ABI_VERSION.decode('ascii')!r}, got {actual!r}"
        )

    reader_pointer = ctypes.c_void_p
    error_pointer = ctypes.POINTER(ctypes.c_char)
    library.l2flow_arrow_reader_open.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.POINTER(reader_pointer),
        error_pointer,
        ctypes.c_size_t,
    ]
    library.l2flow_arrow_reader_open.restype = ctypes.c_int
    library.l2flow_arrow_reader_close.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_close.restype = None
    library.l2flow_arrow_reader_schema.argtypes = [
        reader_pointer,
        ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)),
        ctypes.POINTER(ctypes.c_size_t),
        error_pointer,
        ctypes.c_size_t,
    ]
    library.l2flow_arrow_reader_schema.restype = ctypes.c_int
    library.l2flow_arrow_reader_try_read.argtypes = [
        reader_pointer,
        ctypes.POINTER(_CBatch),
        error_pointer,
        ctypes.c_size_t,
    ]
    library.l2flow_arrow_reader_try_read.restype = ctypes.c_int
    library.l2flow_arrow_batch_release.argtypes = [ctypes.POINTER(_CBatch)]
    library.l2flow_arrow_batch_release.restype = None
    library.l2flow_arrow_reader_seek_earliest.argtypes = [
        reader_pointer,
        error_pointer,
        ctypes.c_size_t,
    ]
    library.l2flow_arrow_reader_seek_earliest.restype = ctypes.c_int
    library.l2flow_arrow_reader_seek_latest.argtypes = [
        reader_pointer,
        error_pointer,
        ctypes.c_size_t,
    ]
    library.l2flow_arrow_reader_seek_latest.restype = ctypes.c_int
    library.l2flow_arrow_reader_stream_kind.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_stream_kind.restype = ctypes.c_uint32
    library.l2flow_arrow_reader_shard_id.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_shard_id.restype = ctypes.c_uint32
    library.l2flow_arrow_reader_feed_session_epoch.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_feed_session_epoch.restype = ctypes.c_uint64
    library.l2flow_arrow_reader_producer_instance_high.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_producer_instance_high.restype = ctypes.c_uint64
    library.l2flow_arrow_reader_producer_instance_low.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_producer_instance_low.restype = ctypes.c_uint64
    library.l2flow_arrow_reader_producer_state.argtypes = [reader_pointer]
    library.l2flow_arrow_reader_producer_state.restype = ctypes.c_uint32
    library.l2flow_arrow_reader_producer_heartbeat_monotonic_ns.argtypes = [
        reader_pointer
    ]
    library.l2flow_arrow_reader_producer_heartbeat_monotonic_ns.restype = (
        ctypes.c_uint64
    )


class RingReader:
    """Single-threaded reader; returned Arrow objects may outlive this object."""

    def __init__(
        self,
        data_path: os.PathLike[str] | str,
        control_path: os.PathLike[str] | str,
        *,
        start: _StartName = "latest",
        library_path: os.PathLike[str] | str | None = None,
    ) -> None:
        if start not in {"latest", "earliest"}:
            raise ValueError("start must be 'latest' or 'earliest'")
        self._library = _load_library(library_path)
        self._reader = ctypes.c_void_p()
        error = ctypes.create_string_buffer(_ERROR_BYTES)
        result = self._library.l2flow_arrow_reader_open(
            os.fsencode(data_path),
            os.fsencode(control_path),
            START_LATEST if start == "latest" else START_EARLIEST_AVAILABLE,
            ctypes.byref(self._reader),
            error,
            len(error),
        )
        if result != 0:
            raise ArrowHotError(_error_text(error) or "cannot open Arrow ring")
        try:
            self.schema = self._read_schema()
        except BaseException:
            self.close()
            raise

    @classmethod
    def open_current(
        cls,
        root: os.PathLike[str] | str,
        stream: _StreamName,
        owner: int = 0,
        *,
        start: _StartName = "latest",
        library_path: os.PathLike[str] | str | None = None,
    ) -> RingReader:
        if stream not in {"tick", "snapshot", "late_recovery", "control"}:
            raise ValueError("stream must be tick, snapshot, late_recovery, or control")
        manifest = RingManifest.open_current(root)
        data_path, control_path = manifest.ring_paths(stream, owner)
        reader = cls(
            data_path,
            control_path,
            start=start,
            library_path=library_path,
        )
        expected_kind = {
            "tick": STREAM_ORDERED_TICK,
            "snapshot": STREAM_SNAPSHOT,
            "late_recovery": STREAM_LATE_RECOVERY,
            "control": STREAM_CONTROL,
        }[stream]
        expected_shard = owner if stream in {"tick", "snapshot"} else 0
        if reader.stream_kind != expected_kind or reader.shard_id != expected_shard:
            reader.close()
            raise ArrowHotError("manifest ring kind/shard does not match its key")
        if reader.feed_session_epoch != manifest.feed_session_epoch:
            reader.close()
            raise ArrowHotError("manifest and ring feed_session_epoch differ")
        if reader.producer_instance != manifest.producer_instance:
            reader.close()
            raise ArrowHotError("manifest and ring producer_instance differ")
        return reader

    def _read_schema(self) -> pa.Schema:
        data = ctypes.POINTER(ctypes.c_uint8)()
        size = ctypes.c_size_t()
        error = ctypes.create_string_buffer(_ERROR_BYTES)
        result = self._library.l2flow_arrow_reader_schema(
            self._reader,
            ctypes.byref(data),
            ctypes.byref(size),
            error,
            len(error),
        )
        if result != 0 or not data or size.value == 0:
            raise ArrowHotError(_error_text(error) or "cannot read Arrow schema")
        schema_bytes = ctypes.string_at(data, size.value)
        return pa.ipc.read_schema(pa.BufferReader(schema_bytes))

    @property
    def stream_kind(self) -> int:
        self._ensure_open()
        return int(self._library.l2flow_arrow_reader_stream_kind(self._reader))

    @property
    def shard_id(self) -> int:
        self._ensure_open()
        return int(self._library.l2flow_arrow_reader_shard_id(self._reader))

    @property
    def feed_session_epoch(self) -> int:
        self._ensure_open()
        return int(
            self._library.l2flow_arrow_reader_feed_session_epoch(self._reader)
        )

    @property
    def producer_instance(self) -> tuple[int, int]:
        self._ensure_open()
        return (
            int(
                self._library.l2flow_arrow_reader_producer_instance_high(
                    self._reader
                )
            ),
            int(
                self._library.l2flow_arrow_reader_producer_instance_low(
                    self._reader
                )
            ),
        )

    @property
    def producer_state(self) -> int:
        self._ensure_open()
        return int(self._library.l2flow_arrow_reader_producer_state(self._reader))

    @property
    def producer_heartbeat_monotonic_ns(self) -> int:
        self._ensure_open()
        return int(
            self._library.l2flow_arrow_reader_producer_heartbeat_monotonic_ns(
                self._reader
            )
        )

    def try_read(self) -> HotRecordBatch | None:
        self._ensure_open()
        raw = _CBatch()
        error = ctypes.create_string_buffer(_ERROR_BYTES)
        code = self._library.l2flow_arrow_reader_try_read(
            self._reader, ctypes.byref(raw), error, len(error)
        )
        if code in {READ_EMPTY, READ_RETRY}:
            return None
        if code == READ_CLOSED:
            raise StopIteration
        if code == READ_OVERRUN:
            raise ArrowHotOverrun(
                int(raw.oldest_available_sequence),
                int(raw.newest_available_sequence),
            )
        if code != READ_BATCH:
            raise ArrowHotError(_error_text(error) or f"Arrow read failed ({code})")
        if not raw.data or raw.size == 0 or not raw.lease:
            if raw.lease:
                self._library.l2flow_arrow_batch_release(ctypes.byref(raw))
            raise ArrowHotError("C reader returned an incomplete Arrow lease")
        metadata = BatchMetadata(
            batch_sequence=int(raw.batch_sequence),
            oldest_available_sequence=int(raw.oldest_available_sequence),
            newest_available_sequence=int(raw.newest_available_sequence),
            feed_session_epoch=int(raw.feed_session_epoch),
            first_ingress_sequence=int(raw.first_ingress_sequence),
            last_ingress_sequence=int(raw.last_ingress_sequence),
            minimum_exchange_time_ns=int(raw.minimum_exchange_time_ns),
            maximum_exchange_time_ns=int(raw.maximum_exchange_time_ns),
            publish_monotonic_ns=int(raw.publish_monotonic_ns),
            producer_instance_high=int(raw.producer_instance_high),
            producer_instance_low=int(raw.producer_instance_low),
            row_count=int(raw.row_count),
            flags=int(raw.flags),
        )
        owner = _LeaseOwner(self._library, raw)
        try:
            address = ctypes.cast(raw.data, ctypes.c_void_p).value
            if address is None:
                raise ArrowHotError("Arrow lease has a null address")
            payload = pa.foreign_buffer(address, int(raw.size), base=owner)
            batch = pa.ipc.read_record_batch(pa.BufferReader(payload), self.schema)
        except BaseException:
            owner.release()
            raise
        if batch.num_rows != metadata.row_count:
            del batch
            owner.release()
            raise ArrowHotError("decoded row count does not match ring metadata")
        return HotRecordBatch(batch, metadata)

    def seek_earliest(self) -> None:
        self._seek("l2flow_arrow_reader_seek_earliest")

    def seek_latest(self) -> None:
        self._seek("l2flow_arrow_reader_seek_latest")

    def _seek(self, function_name: str) -> None:
        self._ensure_open()
        error = ctypes.create_string_buffer(_ERROR_BYTES)
        function = getattr(self._library, function_name)
        if function(self._reader, error, len(error)) != 0:
            raise ArrowHotError(_error_text(error) or "Arrow seek failed")

    def close(self) -> None:
        reader = getattr(self, "_reader", None)
        if reader is not None and reader.value:
            self._library.l2flow_arrow_reader_close(reader)
            reader.value = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _ensure_open(self) -> None:
        if not self._reader.value:
            raise ArrowHotError("Arrow reader is closed")

    def __enter__(self) -> RingReader:
        self._ensure_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
