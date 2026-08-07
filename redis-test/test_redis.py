from __future__ import annotations

import os
import sys

import redis
from redis.utils import HIREDIS_AVAILABLE


def main() -> int:
    client = redis.Redis(
        host="127.0.0.1",
        port=6379,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )
    test_key = f"l2flow:test:python-client:{os.getpid()}"

    try:
        if not HIREDIS_AVAILABLE:
            raise RuntimeError("redis-py is not using the required hiredis parser")

        if client.ping() is not True:
            raise RuntimeError("Redis PING did not return true")

        server_info = client.info(section="server")
        print(f"Redis version: {server_info['redis_version']}")
        print(f"redis-py version: {redis.__version__}")
        print("hiredis parser: enabled")

        if client.set(test_key, "arrow-clickhouse-redis", ex=30) is not True:
            raise RuntimeError("Redis SET did not return true")
        try:
            value = client.get(test_key)
            if value != "arrow-clickhouse-redis":
                raise RuntimeError(f"unexpected Redis value: {value!r}")
        finally:
            client.delete(test_key)
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Redis test failed: {exc}", file=sys.stderr)
        raise
