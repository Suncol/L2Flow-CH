"""Command-line examples for the feeder latest-value Redis client."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from l2flow_feeder_latest import DATASETS, FeederLatestClient, LatestUpdate


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query latest MDL snapshot and L2 records from feeder Redis"
    )
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        help="feeder_client.cfg; its REDIS_SERVER address and queue setting are used",
    )
    parser.add_argument(
        "--host",
        help="Redis host override (default: 127.0.0.1 for a wildcard config address)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9379,
        help="Redis port when --config is not supplied (default: 9379)",
    )
    parser.add_argument("--timeout", type=float, default=2.0)
    subparsers = parser.add_subparsers(dest="command", required=True)

    info = subparsers.add_parser("info", help="show supported latest query types")
    info.add_argument(
        "--no-probe",
        action="store_true",
        help="show static interface information without issuing Redis INFO",
    )

    latest = subparsers.add_parser("latest", help="query one exact dataset")
    latest.add_argument("dataset", choices=tuple(DATASETS))
    latest.add_argument("security_id")
    latest.add_argument(
        "--typed",
        action="store_true",
        help="also include a view using the confirmed per-field numeric schema",
    )

    snapshot = subparsers.add_parser("snapshot", help="query a market snapshot")
    snapshot.add_argument("market", choices=("SH", "SZ"))
    snapshot.add_argument("security_id")
    snapshot.add_argument("--typed", action="store_true")

    l2 = subparsers.add_parser("l2", help="query SH tick or SZ order and trade")
    l2.add_argument("market", choices=("SH", "SZ"))
    l2.add_argument("security_id")
    l2.add_argument("--typed", action="store_true")
    return parser


def _client(arguments: argparse.Namespace) -> FeederLatestClient:
    if arguments.config is not None:
        return FeederLatestClient.from_feeder_config(
            arguments.config,
            host=arguments.host,
            socket_timeout=arguments.timeout,
        )
    return FeederLatestClient(
        host=arguments.host or "127.0.0.1",
        port=arguments.port,
        socket_timeout=arguments.timeout,
    )


def _update_document(update: LatestUpdate, typed: bool) -> dict[str, Any]:
    document = update.to_dict()
    if typed:
        typed_frame = update.typed_frame()
        document["typed_view"] = {
            "dtypes": {
                name: str(dtype)
                for name, dtype in zip(typed_frame.columns, typed_frame.dtypes)
            },
            "data": typed_frame.row(0, named=True),
        }
    return document


def main() -> int:
    arguments = _parser().parse_args()
    with _client(arguments) as client:
        if arguments.command == "info":
            result: Any = client.info(probe_server=not arguments.no_probe)
        elif arguments.command == "latest":
            result = _update_document(
                client.latest(arguments.dataset, arguments.security_id),
                arguments.typed,
            )
        elif arguments.command == "snapshot":
            result = _update_document(
                client.latest_snapshot(arguments.market, arguments.security_id),
                arguments.typed,
            )
        else:
            updates = client.latest_l2(arguments.market, arguments.security_id)
            result = {
                name: _update_document(update, arguments.typed)
                for name, update in updates.items()
            }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
