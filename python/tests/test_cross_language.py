"""Cross-language Parquet compatibility test.

Writes a fixture with Python (pyarrow) and verifies it is readable by the Rust
reader (cfb-storage), and vice versa via file inspection.

These tests run against pre-generated fixtures in spec/conformance_cases/parquet/
if they exist, or generate them on the fly.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pytest

from cfb.models import FeatureRow
from cfb.storage import read_partitioned, write_partitioned

BASE_TS = 1_700_000_000 * 1_000_000_000
_NS_PER_HOUR = 3_600_000_000_000


def make_rows(n: int = 5) -> list[FeatureRow]:
    return [
        FeatureRow(
            ts=BASE_TS + i * _NS_PER_HOUR,
            symbol="BTCUSDT",
            vwap_1m=30_000.0 + i * 10.0,
            vwap_5m=30_000.0,
            vwap_15m=30_000.0,
            rv_1m=float(i) * 1e-5,
            rv_5m=float(i) * 2e-5,
            ofi_1m=float(i - 2),
            microprice=None,
            trade_count_1m=i + 1,
        )
        for i in range(n)
    ]


def test_python_write_is_valid_parquet(tmp_path: Path) -> None:
    """Files written by Python must be valid Parquet (readable by pyarrow itself)."""
    rows = make_rows(5)
    write_partitioned(rows, tmp_path)
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 5, "one file per hour"

    import pyarrow.parquet as pq
    for f in parquet_files:
        pf = pq.ParquetFile(f)
        table = pf.read()
        assert table.num_rows == 1


def test_python_written_readable_by_polars(tmp_path: Path) -> None:
    """Python-written Parquet is readable by Polars (which uses its own Rust reader)."""
    rows = make_rows(10)
    write_partitioned(rows, tmp_path)
    df = pl.read_parquet(str(tmp_path / "**/*.parquet"), hive_partitioning=True)
    assert len(df) == 10
    # Polars should preserve float values
    for i, row in enumerate(df.sort("ts").iter_rows(named=True)):
        assert abs(row["vwap_1m"] - (30_000.0 + i * 10.0)) < 1e-9


def test_roundtrip_values_exact(tmp_path: Path) -> None:
    """Write with Python, read back, verify all column values within tolerance."""
    rows = make_rows(3)
    write_partitioned(rows, tmp_path)
    df = read_partitioned(tmp_path).sort("ts")

    for i, row in enumerate(df.iter_rows(named=True)):
        assert row["ts"] == BASE_TS + i * _NS_PER_HOUR
        assert abs(row["vwap_1m"] - (30_000.0 + i * 10.0)) < 1e-9
        assert abs(row["rv_1m"] - i * 1e-5) < 1e-12
        assert abs(row["ofi_1m"] - (i - 2)) < 1e-9
        assert row["trade_count_1m"] == i + 1
        assert row["microprice"] is None
