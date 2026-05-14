"""Feature storage: partitioned Parquet writer and reader.

Partition layout: symbol=X/date=YYYYMMDD/hour=HH/part-NNNN.parquet

Both write_partitioned and read_partitioned are compatible with the Rust
implementation in cfb-storage — cross-language reads are covered in tests.
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from cfb.models import FeatureRow

_NS_PER_DAY = 86_400_000_000_000
_NS_PER_HOUR = 3_600_000_000_000

_FEATURE_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("vwap_1m", pa.float64(), nullable=True),
    pa.field("vwap_5m", pa.float64(), nullable=True),
    pa.field("vwap_15m", pa.float64(), nullable=True),
    pa.field("rv_1m", pa.float64(), nullable=False),
    pa.field("rv_5m", pa.float64(), nullable=False),
    pa.field("ofi_1m", pa.float64(), nullable=False),
    pa.field("microprice", pa.float64(), nullable=True),
    pa.field("trade_count_1m", pa.int64(), nullable=False),
])

_PARQUET_WRITE_OPTS: dict[str, object] = {
    "compression": "snappy",
    "use_dictionary": True,
}


def _derive_partitions(ts_col: pa.Array) -> tuple[pa.Array, pa.Array]:
    """Compute date and hour partition columns from a nanosecond timestamp array."""
    ts = ts_col.cast(pa.int64())
    date_arr = pc.divide(ts, pa.scalar(_NS_PER_DAY, pa.int64()))  # type: ignore[attr-defined]
    hour_arr = pc.divide(  # type: ignore[attr-defined]
        pc.subtract(ts, pc.multiply(date_arr, pa.scalar(_NS_PER_DAY, pa.int64()))),  # type: ignore[attr-defined]
        pa.scalar(_NS_PER_HOUR, pa.int64()),
    )
    return date_arr.cast(pa.int32()), hour_arr.cast(pa.int32())


def write_partitioned(
    rows: list[FeatureRow] | pl.DataFrame | pa.Table,
    base_path: Path | str,
    *,
    part_index: int = 0,
) -> None:
    """Write feature rows to hive-partitioned Parquet.

    Handles list[FeatureRow], Polars DataFrame, or PyArrow Table as input.
    Partitions by symbol / date / hour — one file per unique (symbol, date, hour).
    """
    base = Path(base_path)

    # Normalise input to PyArrow Table
    if isinstance(rows, list):
        table = pa.table(
            {
                "ts": [r.ts for r in rows],
                "symbol": [r.symbol for r in rows],
                "vwap_1m": [r.vwap_1m for r in rows],
                "vwap_5m": [r.vwap_5m for r in rows],
                "vwap_15m": [r.vwap_15m for r in rows],
                "rv_1m": [r.rv_1m for r in rows],
                "rv_5m": [r.rv_5m for r in rows],
                "ofi_1m": [r.ofi_1m for r in rows],
                "microprice": [r.microprice for r in rows],
                "trade_count_1m": [r.trade_count_1m for r in rows],
            },
            schema=_FEATURE_SCHEMA,
        )
    elif isinstance(rows, pl.DataFrame):
        table = rows.to_arrow()
    else:
        table = rows

    date_arr, hour_arr = _derive_partitions(table.column("ts"))
    table = table.append_column("date", date_arr)
    table = table.append_column("hour", hour_arr)

    pq.write_to_dataset(  # type: ignore[no-untyped-call]
        table,
        root_path=str(base),
        partition_cols=["symbol", "date", "hour"],
        basename_template=f"part-{part_index:04d}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        compression="snappy",
        use_dictionary=True,
    )


def read_partitioned(
    base_path: Path | str,
    *,
    symbol: str | None = None,
    date_from: int | None = None,
    date_to: int | None = None,
) -> pl.DataFrame:
    """Read feature rows from partitioned Parquet with optional predicate pushdown.

    date_from / date_to are integer day epochs (ts // _NS_PER_DAY).
    """
    base = Path(base_path)
    dataset = ds.dataset(str(base), format="parquet", partitioning="hive")  # type: ignore[no-untyped-call]

    filters: list[ds.Expression] = []  # type: ignore[name-defined]
    if symbol is not None:
        filters.append(ds.field("symbol") == symbol)  # type: ignore[attr-defined, no-untyped-call]
    if date_from is not None:
        filters.append(ds.field("date") >= date_from)  # type: ignore[attr-defined, no-untyped-call]
    if date_to is not None:
        filters.append(ds.field("date") <= date_to)  # type: ignore[attr-defined, no-untyped-call]

    filt = None
    for f in filters:
        filt = f if filt is None else filt & f

    table = dataset.to_table(filter=filt)
    # Drop partition columns added by write_to_dataset
    drop = [c for c in ["date", "hour"] if c in table.schema.names]
    return pl.from_arrow(table.drop_columns(drop) if drop else table)  # type: ignore[return-value]


def scan_partitioned(
    base_path: Path | str,
    *,
    symbol: str | None = None,
) -> pl.LazyFrame:
    """Return a Polars LazyFrame over the partitioned dataset (for query layer)."""
    base = Path(base_path)
    lf = pl.scan_parquet(f"{base}/**/*.parquet", hive_partitioning=True)
    if symbol is not None:
        lf = lf.filter(pl.col("symbol") == symbol)
    return lf


def measure_write(
    rows: list[FeatureRow],
    base_path: Path | str,
) -> dict[str, float]:
    """Write rows and return timing dict with wall_s and bytes_written."""
    t0 = time.perf_counter()
    write_partitioned(rows, base_path)
    wall_s = time.perf_counter() - t0

    total_bytes = sum(
        p.stat().st_size
        for p in Path(base_path).rglob("*.parquet")
    )
    return {
        "wall_s": wall_s,
        "bytes_written": float(total_bytes),
        "rows_written": float(len(rows)),
        "rows_per_sec": len(rows) / wall_s,
        "mb_per_sec": (total_bytes / 1e6) / wall_s,
    }
