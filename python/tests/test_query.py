"""Query engine tests — both DuckDB and Polars must return identical results."""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

import polars as pl
import pytest

from cfb.models import FeatureRow
from cfb.query import DuckDBEngine, PolarsEngine
from cfb.storage import write_partitioned

_NS_PER_MIN = 60_000_000_000
_NS_PER_HOUR = 3_600_000_000_000
BASE_TS = 1_700_000_000 * 1_000_000_000
TOL = 1e-9


def make_rows(n: int, symbols: list[str] | None = None) -> list[FeatureRow]:
    syms = symbols or ["BTCUSDT"]
    rows = []
    for i in range(n):
        sym = syms[i % len(syms)]
        rows.append(FeatureRow(
            ts=BASE_TS + i * _NS_PER_MIN,
            symbol=sym,
            vwap_1m=30_000.0 + i * 0.5,
            vwap_5m=30_000.0,
            vwap_15m=30_000.0,
            rv_1m=float(i % 10) * 1e-5,
            rv_5m=float(i % 10) * 2e-5,
            ofi_1m=float((i % 10) - 5),
            microprice=None,
            trade_count_1m=(i % 50) + 1,
        ))
    return rows


@pytest.fixture(scope="module")
def dataset_path() -> Path:
    with tempfile.TemporaryDirectory() as d:
        rows = make_rows(120, symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        write_partitioned(rows, d)
        yield Path(d)


@pytest.fixture(scope="module")
def duckdb_engine(dataset_path: Path) -> DuckDBEngine:
    return DuckDBEngine(dataset_path)


@pytest.fixture(scope="module")
def polars_engine(dataset_path: Path) -> PolarsEngine:
    return PolarsEngine(dataset_path)


def _float_cols_close(df1: pl.DataFrame, df2: pl.DataFrame, col: str) -> bool:
    if col not in df1.columns or col not in df2.columns:
        return True
    a = df1.sort("minute_ts" if "minute_ts" in df1.columns else "symbol")[col].to_list()
    b = df2.sort("minute_ts" if "minute_ts" in df2.columns else "symbol")[col].to_list()
    return all(
        math.isclose(x, y, abs_tol=TOL) if (x is not None and y is not None) else x == y
        for x, y in zip(a, b)
    )


# ---------------------------------------------------------------------------
# Q1 — VWAP per minute
# ---------------------------------------------------------------------------

def test_q1_duckdb_returns_rows(duckdb_engine: DuckDBEngine) -> None:
    df = duckdb_engine.q1_vwap_per_minute("BTCUSDT", BASE_TS)
    assert len(df) > 0
    assert "minute_ts" in df.columns
    assert "vwap" in df.columns


def test_q1_polars_returns_rows(polars_engine: PolarsEngine) -> None:
    df = polars_engine.q1_vwap_per_minute("BTCUSDT", BASE_TS)
    assert len(df) > 0


def test_q1_results_match(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    duck = duckdb_engine.q1_vwap_per_minute("BTCUSDT", BASE_TS)
    pols = polars_engine.q1_vwap_per_minute("BTCUSDT", BASE_TS)
    assert len(duck) == len(pols), f"row count mismatch: {len(duck)} vs {len(pols)}"
    assert _float_cols_close(duck, pols, "vwap")


# ---------------------------------------------------------------------------
# Q2 — Top symbols by volume
# ---------------------------------------------------------------------------

def test_q2_both_return_symbols(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    duck = duckdb_engine.q2_top_symbols_24h(BASE_TS)
    pols = polars_engine.q2_top_symbols_24h(BASE_TS)
    assert len(duck) > 0 and len(pols) > 0
    assert set(duck["symbol"].to_list()) == set(pols["symbol"].to_list())


# ---------------------------------------------------------------------------
# Q3 — RV distribution
# ---------------------------------------------------------------------------

def test_q3_rv_distribution(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    duck = duckdb_engine.q3_rv_distribution(BASE_TS)
    pols = polars_engine.q3_rv_distribution(BASE_TS)
    assert len(duck) == len(pols)


# ---------------------------------------------------------------------------
# Q4 — Point lookup
# ---------------------------------------------------------------------------

def test_q4_point_lookup_finds_row(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    ts = BASE_TS  # first trade
    duck = duckdb_engine.q4_point_lookup("BTCUSDT", ts)
    pols = polars_engine.q4_point_lookup("BTCUSDT", ts)
    assert len(duck) == 1 and len(pols) == 1
    assert duck["ts"][0] == ts
    assert pols["ts"][0] == ts


def test_q4_point_lookup_missing(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    duck = duckdb_engine.q4_point_lookup("BTCUSDT", 999)
    pols = polars_engine.q4_point_lookup("BTCUSDT", 999)
    assert len(duck) == 0 and len(pols) == 0


# ---------------------------------------------------------------------------
# Q5 — OFI momentum
# ---------------------------------------------------------------------------

def test_q5_ofi_momentum(duckdb_engine: DuckDBEngine, polars_engine: PolarsEngine) -> None:
    duck = duckdb_engine.q5_ofi_momentum("BTCUSDT", BASE_TS)
    pols = polars_engine.q5_ofi_momentum("BTCUSDT", BASE_TS)
    assert len(duck) > 0 and len(pols) > 0
