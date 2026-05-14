"""Storage layer tests — write/read roundtrip, partition correctness, predicate pushdown."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from cfb.models import FeatureRow
from cfb.storage import read_partitioned, scan_partitioned, write_partitioned

_NS_PER_DAY = 86_400_000_000_000
_NS_PER_HOUR = 3_600_000_000_000

BASE_TS = 1_700_000_000 * 1_000_000_000  # 2023-11-14 ~22:13 UTC


def make_row(ts: int, symbol: str = "BTCUSDT", price: float = 30_000.0) -> FeatureRow:
    return FeatureRow(
        ts=ts,
        symbol=symbol,
        vwap_1m=price,
        vwap_5m=price,
        vwap_15m=price,
        rv_1m=0.0,
        rv_5m=0.0,
        ofi_1m=1.0,
        microprice=None,
        trade_count_1m=1,
    )


# ---------------------------------------------------------------------------
# Basic roundtrip
# ---------------------------------------------------------------------------

def test_write_read_roundtrip(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS + i * _NS_PER_HOUR) for i in range(5)]
    write_partitioned(rows, tmp_path)
    df = read_partitioned(tmp_path)
    assert len(df) == 5
    assert set(df["symbol"].to_list()) == {"BTCUSDT"}


def test_schema_preserved(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS, price=31_234.56)]
    write_partitioned(rows, tmp_path)
    df = read_partitioned(tmp_path)
    assert df.schema["ts"] == pl.Int64
    assert df.schema["vwap_1m"] == pl.Float64
    assert df.schema["trade_count_1m"] == pl.Int64
    assert abs(df["vwap_1m"][0] - 31_234.56) < 1e-9


def test_null_microprice_preserved(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS)]
    write_partitioned(rows, tmp_path)
    df = read_partitioned(tmp_path)
    assert df["microprice"][0] is None


# ---------------------------------------------------------------------------
# Partition correctness
# ---------------------------------------------------------------------------

def test_partition_directories_created(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS)]
    write_partitioned(rows, tmp_path)
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) >= 1
    # Must be under symbol= / date= / hour= hierarchy
    for f in parquet_files:
        parts = [p.name for p in f.parents]
        assert any(p.startswith("symbol=") for p in parts)
        assert any(p.startswith("date=") for p in parts)
        assert any(p.startswith("hour=") for p in parts)


def test_rows_span_multiple_hours(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS + i * _NS_PER_HOUR) for i in range(3)]
    write_partitioned(rows, tmp_path)
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 3, "Each hour should produce one file"


def test_multi_symbol_partitioned_separately(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS, symbol="BTCUSDT"), make_row(BASE_TS, symbol="ETHUSDT")]
    write_partitioned(rows, tmp_path)
    df_btc = read_partitioned(tmp_path, symbol="BTCUSDT")
    df_eth = read_partitioned(tmp_path, symbol="ETHUSDT")
    assert len(df_btc) == 1 and len(df_eth) == 1


# ---------------------------------------------------------------------------
# Predicate pushdown
# ---------------------------------------------------------------------------

def test_symbol_filter_reads_only_matching(tmp_path: Path) -> None:
    rows = [
        make_row(BASE_TS, symbol="BTCUSDT"),
        make_row(BASE_TS + _NS_PER_HOUR, symbol="ETHUSDT"),
    ]
    write_partitioned(rows, tmp_path)
    df = read_partitioned(tmp_path, symbol="BTCUSDT")
    assert len(df) == 1
    assert df["symbol"][0] == "BTCUSDT"


def test_polars_lazy_scan(tmp_path: Path) -> None:
    rows = [make_row(BASE_TS + i * _NS_PER_HOUR) for i in range(5)]
    write_partitioned(rows, tmp_path)
    lf = scan_partitioned(tmp_path, symbol="BTCUSDT")
    result = lf.select("ts", "vwap_1m").collect()
    assert len(result) == 5


# ---------------------------------------------------------------------------
# Values integrity
# ---------------------------------------------------------------------------

def test_all_feature_columns_readable(tmp_path: Path) -> None:
    row = FeatureRow(
        ts=BASE_TS,
        symbol="BTCUSDT",
        vwap_1m=30_100.5,
        vwap_5m=30_050.0,
        vwap_15m=30_025.0,
        rv_1m=0.000123,
        rv_5m=0.000456,
        ofi_1m=-1.5,
        microprice=None,
        trade_count_1m=42,
    )
    write_partitioned([row], tmp_path)
    df = read_partitioned(tmp_path)
    r = df.row(0, named=True)
    assert r["ts"] == BASE_TS
    assert abs(r["vwap_1m"] - 30_100.5) < 1e-9
    assert abs(r["rv_1m"] - 0.000123) < 1e-12
    assert abs(r["ofi_1m"] - (-1.5)) < 1e-9
    assert r["trade_count_1m"] == 42
    assert r["microprice"] is None
