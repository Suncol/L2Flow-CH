from __future__ import annotations

import gc
import os
import pathlib
import subprocess
import sys
import tempfile

import pyarrow as pa
import polars as pl

from l2flow_arrow_hot import PRODUCER_SEALED, RingManifest, RingReader


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: test_reader.py FIXTURE LIBRARY")
    fixture = pathlib.Path(sys.argv[1]).resolve(strict=True)
    library = pathlib.Path(sys.argv[2]).resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="l2flow-python-reader-") as temporary:
        root = pathlib.Path(temporary) / "hot"
        subprocess.run([fixture, root], check=True, text=True)
        manifest = RingManifest.open_current(root)
        assert manifest.feed_session_epoch == 9001
        assert manifest.producer_instance != (0, 0)

        try:
            RingReader.open_current(
                root, "invalid", library_path=library  # type: ignore[arg-type]
            )
        except ValueError:
            pass
        else:
            raise AssertionError("invalid stream name was accepted")

        reader = RingReader.open_current(
            root, "tick", 0, start="earliest", library_path=library
        )
        assert reader.producer_state == PRODUCER_SEALED
        assert reader.producer_instance == manifest.producer_instance
        assert reader.producer_heartbeat_monotonic_ns > 0
        hot = reader.try_read()
        assert hot is not None
        assert isinstance(hot.batch, pa.RecordBatch)
        assert hot.batch.num_rows == 2
        assert hot.metadata.first_ingress_sequence == 1
        assert hot.metadata.last_ingress_sequence == 2
        frame = hot.to_polars()
        assert isinstance(frame, pl.DataFrame)
        assert frame["price_raw"].to_list() == [12345, 12346]

        # Closing the registry reader must not invalidate Arrow/Polars views;
        # the foreign-buffer base retains the opaque C++ segment lease.
        reader.close()
        gc.collect()
        assert hot.batch.column("price_raw")[1].as_py() == 12346
        assert frame["ingress_sequence"].to_list() == [1, 2]

        with RingReader.open_current(
            root, "snapshot", 0, start="earliest", library_path=library
        ) as snapshots:
            snapshot = snapshots.try_read()
            assert snapshot is not None
            assert snapshot.batch.column("bids")[0].as_py()[0]["price_raw"] == 100

        with RingReader.open_current(
            root, "control", start="earliest", library_path=library
        ) as controls:
            rows = 0
            kinds: list[int] = []
            while True:
                try:
                    control = controls.try_read()
                except StopIteration:
                    break
                if control is None:
                    continue
                rows += control.batch.num_rows
                kinds.extend(control.batch.column("control_kind").to_pylist())
            assert rows == 4
            assert kinds == [1, 5, 2, 4]

    print("Python PyArrow/Polars ring test passed")
    return 0


if __name__ == "__main__":
    os.environ.setdefault("PYTHONMALLOC", "malloc")
    raise SystemExit(main())
