# Python feeder latest reader

`l2flow_feeder_latest.py` is a read-only client for the limited Redis protocol
implemented by MDL `feeder_client`. It reads only the current value stored at a
known security key; it does not subscribe, scan keys, or retain history.

## Environment

The project uses the repository's uv environment. Polars and the Redis client
with hiredis are locked in `uv.lock`:

```bash
uv sync --project python --locked
```

## Supported latest values

| Dataset | Redis key | Exchange time |
| --- | --- | --- |
| `sh_snapshot` | `mdl.4.4.{security_id}` | `UpdateTime` |
| `sz_snapshot` | `mdl.6.28.{security_id}` | `UpdateTime` |
| `sh_l2_tick` | `mdl.4.24.{security_id}` | `TickTime` |
| `sz_l2_order` | `mdl.6.33.{security_id}` | `TransactTime` |
| `sz_l2_trade` | `mdl.6.36.{security_id}` | `TransactTime` |

The current feeder server does not implement `SCAN`, so the client requires a
known six-digit security ID. A missing key raises `LatestNotFoundError`.

## Python API

```python
from l2flow_feeder_latest import FeederLatestClient

with FeederLatestClient.from_feeder_config(
    "/home/sunc/L2Flow/MDL/feeder_client.cfg"
) as client:
    capabilities = client.info()

    snapshot = client.latest_snapshot("SH", "600000")
    print(snapshot.frame)                 # every source field is pl.String
    print(snapshot.data)                  # complete field-name mapping
    print(snapshot.raw_csv)               # decoded original GBK CSV
    print(snapshot.exchange_update_time)  # payload UpdateTime
    print(snapshot.feeder_local_time)      # payload LocalTime
    print(snapshot.sequence_no)            # payload SeqNo, still a string
    print(snapshot.queried_at)             # timezone-aware Asia/Shanghai time

    tick = client.latest("sh_l2_tick", "600000")
    typed = tick.typed_frame(fields=("Price", "Qty", "SeqNo"))
```

`latest_l2("SH", security_id)` returns a `{"tick": update}` mapping.
`latest_l2("SZ", security_id)` issues one `MGET` and returns
`{"order": update, "trade": update}`.

The source `frame`, `data`, and `raw_csv` are never changed by
`typed_frame()`. The optional typed view uses the exported per-dataset
`NUMERIC_SCHEMA` and `strict=True`. Confirmed prices and monetary fields become
fixed-scale `pl.Decimal`; confirmed counts and native sequence fields become
signed or unsigned integers according to the matching SDK structure. Security
IDs, market/source IDs, status/side fields, intraday times, and opaque order
queues remain `pl.String`. A failed conversion raises `TypedFrameError` with
the dataset, Redis key, field, target type, and original value.

The Redis payload does not carry a trading date. The API therefore returns the
raw intraday exchange/feeder times and a timezone-aware query timestamp, but
does not synthesize a dated exchange timestamp.

## Command-line examples

```bash
uv run --project python --locked python python/example_feeder_latest.py \
  --config /home/sunc/L2Flow/MDL/feeder_client.cfg info

uv run --project python --locked python python/example_feeder_latest.py \
  --config /home/sunc/L2Flow/MDL/feeder_client.cfg \
  latest sh_snapshot 600000

uv run --project python --locked python python/example_feeder_latest.py \
  --config /home/sunc/L2Flow/MDL/feeder_client.cfg \
  snapshot SZ 000001 --typed

uv run --project python --locked python python/example_feeder_latest.py \
  --config /home/sunc/L2Flow/MDL/feeder_client.cfg \
  l2 SZ 000001
```
