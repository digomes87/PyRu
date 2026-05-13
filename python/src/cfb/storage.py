"""Feature storage: partitioned Parquet writer and reader."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def write_partitioned(df: pl.DataFrame, base_path: Path | str) -> None:
    """Write a feature DataFrame to partitioned Parquet (symbol / date / hour).

    Partition layout: symbol=BTCUSDT/date=2024-01-15/hour=14/
    """
    base = Path(base_path)
    arrow_table = df.to_arrow()

    ts_col = df["ts"]
    dates = (ts_col // (86400 * 1_000_000_000)).cast(pl.Int64)
    hours = ((ts_col % (86400 * 1_000_000_000)) // (3600 * 1_000_000_000)).cast(pl.Int64)

    arrow_table = arrow_table.append_column(
        "_date_int", pa.array(dates.to_list(), type=pa.int32())
    )
    arrow_table = arrow_table.append_column(
        "_hour_int", pa.array(hours.to_list(), type=pa.int32())
    )

    pq.write_to_dataset(
        arrow_table,
        root_path=str(base),
        partition_cols=["symbol", "_date_int", "_hour_int"],
        use_legacy_dataset=False,
    )


def read_partitioned(base_path: Path | str, symbol: str | None = None) -> pl.DataFrame:
    """Read feature rows from partitioned Parquet, optionally filtered by symbol."""
    import pyarrow.dataset as ds

    base = Path(base_path)
    dataset = ds.dataset(str(base), format="parquet", partitioning="hive")

    if symbol is not None:
        filt = ds.field("symbol") == symbol
        table = dataset.to_table(filter=filt)
    else:
        table = dataset.to_table()

    return pl.from_arrow(table)
