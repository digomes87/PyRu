"""Query engine benchmark — DuckDB vs Polars on five canonical queries.

Run with:
    uv run pytest benches/bench_query.py \
        --benchmark-json=../bench/results/query_python_$(git rev-parse --short HEAD).json
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from cfb.models import FeatureRow
from cfb.query import DuckDBEngine, PolarsEngine
from cfb.storage import write_partitioned

_NS_PER_MIN = 60_000_000_000
BASE_TS = 1_700_000_000 * 1_000_000_000


def make_rows(n: int) -> list[FeatureRow]:
    syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    return [
        FeatureRow(
            ts=BASE_TS + i * 1_000_000_000,  # 1-second spacing → manageable partition count
            symbol=syms[i % len(syms)],
            vwap_1m=30_000.0 + (i % 500) * 0.1,
            vwap_5m=30_000.0,
            vwap_15m=30_000.0,
            rv_1m=float(i % 20) * 1e-5,
            rv_5m=float(i % 20) * 2e-5,
            ofi_1m=float((i % 20) - 10),
            microprice=None,
            trade_count_1m=(i % 50) + 1,
        )
        for i in range(n)
    ]


@pytest.fixture(scope="session")
def dataset_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    p = tmp_path_factory.mktemp("query_bench")
    write_partitioned(make_rows(100_000), p)
    return p


@pytest.fixture(scope="session")
def duck(dataset_path: Path) -> DuckDBEngine:
    return DuckDBEngine(dataset_path)


@pytest.fixture(scope="session")
def pols(dataset_path: Path) -> PolarsEngine:
    return PolarsEngine(dataset_path)


# ---------------------------------------------------------------------------
# Q1 — VWAP per minute
# ---------------------------------------------------------------------------

def test_duck_q1(benchmark: pytest.FixtureRequest, duck: DuckDBEngine) -> None:
    result = benchmark(lambda: duck.q1_vwap_per_minute("BTCUSDT", BASE_TS))
    assert len(result) > 0


def test_pols_q1(benchmark: pytest.FixtureRequest, pols: PolarsEngine) -> None:
    result = benchmark(lambda: pols.q1_vwap_per_minute("BTCUSDT", BASE_TS))
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Q2 — Top symbols
# ---------------------------------------------------------------------------

def test_duck_q2(benchmark: pytest.FixtureRequest, duck: DuckDBEngine) -> None:
    result = benchmark(lambda: duck.q2_top_symbols_24h(BASE_TS))
    assert len(result) > 0


def test_pols_q2(benchmark: pytest.FixtureRequest, pols: PolarsEngine) -> None:
    result = benchmark(lambda: pols.q2_top_symbols_24h(BASE_TS))
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Q3 — RV distribution
# ---------------------------------------------------------------------------

def test_duck_q3(benchmark: pytest.FixtureRequest, duck: DuckDBEngine) -> None:
    result = benchmark(lambda: duck.q3_rv_distribution(BASE_TS))
    assert len(result) > 0


def test_pols_q3(benchmark: pytest.FixtureRequest, pols: PolarsEngine) -> None:
    result = benchmark(lambda: pols.q3_rv_distribution(BASE_TS))
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Q4 — Point lookup
# ---------------------------------------------------------------------------

def test_duck_q4(benchmark: pytest.FixtureRequest, duck: DuckDBEngine) -> None:
    result = benchmark(lambda: duck.q4_point_lookup("BTCUSDT", BASE_TS))
    assert len(result) == 1


def test_pols_q4(benchmark: pytest.FixtureRequest, pols: PolarsEngine) -> None:
    result = benchmark(lambda: pols.q4_point_lookup("BTCUSDT", BASE_TS))
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Q5 — OFI momentum
# ---------------------------------------------------------------------------

def test_duck_q5(benchmark: pytest.FixtureRequest, duck: DuckDBEngine) -> None:
    cutoff = BASE_TS + 5 * 60 * _NS_PER_MIN
    result = benchmark(lambda: duck.q5_ofi_momentum("BTCUSDT", BASE_TS))
    assert len(result) > 0


def test_pols_q5(benchmark: pytest.FixtureRequest, pols: PolarsEngine) -> None:
    result = benchmark(lambda: pols.q5_ofi_momentum("BTCUSDT", BASE_TS))
    assert len(result) > 0
