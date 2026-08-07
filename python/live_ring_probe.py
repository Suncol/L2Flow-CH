"""Concurrent read-side probe for a live tick-only Arrow-ring run."""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import pathlib
import queue
import threading
import time

from l2flow_arrow_hot import (
    ArrowHotOverrun,
    RingManifest,
    RingReader,
)


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerResult:
    owner: int
    tick_batches: int
    tick_rows: int
    snapshot_batches: int
    snapshot_rows: int
    first_batch_sequence: int | None
    last_batch_sequence: int | None


def _positive_float(text: str) -> float:
    value = float(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def _nonnegative_int(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("value must be nonnegative")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Drain every live tick owner while asserting that all snapshot "
            "rings remain empty."
        )
    )
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("--library", type=pathlib.Path, required=True)
    parser.add_argument(
        "--duration-seconds", type=_positive_float, default=30.0
    )
    parser.add_argument(
        "--startup-timeout-seconds", type=_positive_float, default=30.0
    )
    parser.add_argument(
        "--expected-feed-epoch", type=_nonnegative_int, default=0
    )
    parser.add_argument(
        "--minimum-tick-rows", type=_nonnegative_int, default=1
    )
    return parser.parse_args()


def _wait_for_manifest(root: pathlib.Path, timeout: float) -> RingManifest:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return RingManifest.open_current(root)
        except (FileNotFoundError, OSError, RuntimeError) as exc:
            last_error = exc
            time.sleep(0.05)
    detail = "" if last_error is None else f": {last_error}"
    raise TimeoutError(f"Arrow CURRENT was not ready within {timeout}s{detail}")


def _read_owner(
    root: pathlib.Path,
    library: pathlib.Path,
    owner: int,
    expected_epoch: int,
    expected_instance: tuple[int, int],
    ready: queue.Queue[int],
    start: threading.Event,
    stop_at: list[float],
) -> OwnerResult:
    tick_batches = 0
    tick_rows = 0
    snapshot_batches = 0
    snapshot_rows = 0
    first_batch_sequence: int | None = None
    last_batch_sequence: int | None = None

    with (
        RingReader.open_current(
            root, "tick", owner, start="latest", library_path=library
        ) as ticks,
        RingReader.open_current(
            root, "snapshot", owner, start="latest", library_path=library
        ) as snapshots,
    ):
        for reader in (ticks, snapshots):
            if reader.feed_session_epoch != expected_epoch:
                raise RuntimeError(
                    f"owner {owner}: feed epoch changed while readers opened"
                )
            if reader.producer_instance != expected_instance:
                raise RuntimeError(
                    f"owner {owner}: producer instance changed while readers opened"
                )

        ready.put(owner)
        if not start.wait(timeout=60.0):
            raise TimeoutError("probe start barrier timed out")

        idle_reads = 0
        while time.monotonic() < stop_at[0]:
            try:
                hot = ticks.try_read()
            except ArrowHotOverrun as exc:
                raise RuntimeError(
                    f"owner {owner}: tick reader overrun; "
                    f"available=[{exc.oldest_available}, {exc.newest_available}]"
                ) from exc
            except StopIteration:
                break

            if hot is None:
                idle_reads += 1
            else:
                metadata = hot.metadata
                if metadata.feed_session_epoch != expected_epoch:
                    raise RuntimeError(
                        f"owner {owner}: batch feed epoch changed"
                    )
                if (
                    metadata.producer_instance_high,
                    metadata.producer_instance_low,
                ) != expected_instance:
                    raise RuntimeError(
                        f"owner {owner}: batch producer instance changed"
                    )
                if metadata.row_count <= 0:
                    raise RuntimeError(f"owner {owner}: empty tick batch published")
                if metadata.first_ingress_sequence > metadata.last_ingress_sequence:
                    raise RuntimeError(
                        f"owner {owner}: invalid ingress sequence interval"
                    )
                if (
                    last_batch_sequence is not None
                    and metadata.batch_sequence != last_batch_sequence + 1
                ):
                    raise RuntimeError(
                        f"owner {owner}: non-contiguous batch sequence "
                        f"{last_batch_sequence} -> {metadata.batch_sequence}"
                    )
                if first_batch_sequence is None:
                    first_batch_sequence = metadata.batch_sequence
                last_batch_sequence = metadata.batch_sequence
                tick_batches += 1
                tick_rows += hot.batch.num_rows
                idle_reads = 0

            try:
                snapshot = snapshots.try_read()
            except ArrowHotOverrun as exc:
                raise RuntimeError(
                    f"owner {owner}: snapshot reader overrun, proving the "
                    "snapshot stream was published"
                ) from exc
            except StopIteration:
                snapshot = None
            if snapshot is not None:
                snapshot_batches += 1
                snapshot_rows += snapshot.batch.num_rows
                raise RuntimeError(
                    f"owner {owner}: received {snapshot.batch.num_rows} "
                    "snapshot rows in a tick-only run"
                )

            if idle_reads >= 16:
                time.sleep(0.0005)
                idle_reads = 0

    return OwnerResult(
        owner=owner,
        tick_batches=tick_batches,
        tick_rows=tick_rows,
        snapshot_batches=snapshot_batches,
        snapshot_rows=snapshot_rows,
        first_batch_sequence=first_batch_sequence,
        last_batch_sequence=last_batch_sequence,
    )


def main() -> int:
    args = _parse_args()
    manifest = _wait_for_manifest(args.root, args.startup_timeout_seconds)
    owner_count = int(manifest.values["owner_count"])
    if owner_count <= 0:
        raise RuntimeError("manifest owner_count must be positive")
    if (
        args.expected_feed_epoch != 0
        and manifest.feed_session_epoch != args.expected_feed_epoch
    ):
        raise RuntimeError(
            f"expected feed epoch {args.expected_feed_epoch}, "
            f"found {manifest.feed_session_epoch}"
        )

    ready: queue.Queue[int] = queue.Queue()
    start = threading.Event()
    stop_at = [0.0]
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=owner_count,
        thread_name_prefix="arrow-owner",
    ) as executor:
        futures = [
            executor.submit(
                _read_owner,
                manifest.root,
                args.library,
                owner,
                manifest.feed_session_epoch,
                manifest.producer_instance,
                ready,
                start,
                stop_at,
            )
            for owner in range(owner_count)
        ]
        startup_deadline = time.monotonic() + args.startup_timeout_seconds
        opened: set[int] = set()
        try:
            while len(opened) != owner_count:
                remaining = startup_deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"only {len(opened)}/{owner_count} owner readers opened"
                    )
                try:
                    opened.add(ready.get(timeout=min(remaining, 0.1)))
                except queue.Empty:
                    for future in futures:
                        if future.done() and future.exception() is not None:
                            raise future.exception()  # type: ignore[misc]
        except BaseException:
            start.set()
            raise

        stop_at[0] = time.monotonic() + args.duration_seconds
        start.set()
        results = [future.result() for future in futures]

    total_tick_batches = sum(result.tick_batches for result in results)
    total_tick_rows = sum(result.tick_rows for result in results)
    total_snapshot_batches = sum(result.snapshot_batches for result in results)
    total_snapshot_rows = sum(result.snapshot_rows for result in results)
    if total_tick_rows < args.minimum_tick_rows:
        raise RuntimeError(
            f"received {total_tick_rows} tick rows, fewer than required "
            f"{args.minimum_tick_rows}"
        )
    if total_snapshot_rows != 0:
        raise RuntimeError(f"received {total_snapshot_rows} snapshot rows")

    print(
        json.dumps(
            {
                "status": "PASS",
                "feed_session_epoch": manifest.feed_session_epoch,
                "producer_instance": f"{manifest.producer_instance[0]:016x}"
                f"{manifest.producer_instance[1]:016x}",
                "owner_count": owner_count,
                "duration_seconds": args.duration_seconds,
                "tick_batches": total_tick_batches,
                "tick_rows": total_tick_rows,
                "snapshot_batches": total_snapshot_batches,
                "snapshot_rows": total_snapshot_rows,
                "owners": [dataclasses.asdict(result) for result in results],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
