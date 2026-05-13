"""Feature computation benchmark — compares all Python implementations.

Benchmarks:
  - streaming reference (pure Python)
  - Polars lazy rolling
  - pandas naive (on small datasets only — O(n²))
  - numpy + numba JIT

Run with:
    uv run pytest benches/bench_features.py \
        --benchmark-json=../bench/results/features_python_$(git rev-parse --short HEAD).json
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pytest

from cfb.features import compute_numba, compute_pandas, compute_polars, compute_stream
from cfb.ingest import to_arrow
from cfb.models import Trade

NS = 1_000_000_000
BASE_TS = 1_700_000_000 * NS


def make_trades(n: int) -> list[Trade]:
    return [
        Trade(
            ts=BASE_TS + i * 100_000_000,
            symbol="BTCUSDT",
            price=30_000.0 + (i % 500) * 0.1,
            qty=0.1 + (i % 10) * 0.05,
            side="buy" if i % 2 == 0 else "sell",  # type: ignore[arg-type]
        )
        for i in range(n)
    ]


def trades_to_polars_df(trades: list[Trade]) -> pl.DataFrame:
    return pl.DataFrame({
        "ts": [t.ts for t in trades],
        "symbol": [t.symbol for t in trades],
        "price": [t.price for t in trades],
        "qty": [t.qty for t in trades],
        "side": [t.side for t in trades],
    })


def trades_to_pandas_df(trades: list[Trade]) -> pd.DataFrame:
    return pd.DataFrame({
        "ts": [t.ts for t in trades],
        "symbol": [t.symbol for t in trades],
        "price": [t.price for t in trades],
        "qty": [t.qty for t in trades],
        "side": [t.side for t in trades],
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def trades_1k() -> list[Trade]:
    return make_trades(1_000)


@pytest.fixture(scope="session")
def trades_10k() -> list[Trade]:
    return make_trades(10_000)


@pytest.fixture(scope="session")
def trades_100k() -> list[Trade]:
    return make_trades(100_000)


@pytest.fixture(scope="session")
def polars_df_1k(trades_1k: list[Trade]) -> pl.DataFrame:
    return trades_to_polars_df(trades_1k)


@pytest.fixture(scope="session")
def polars_df_10k(trades_10k: list[Trade]) -> pl.DataFrame:
    return trades_to_polars_df(trades_10k)


@pytest.fixture(scope="session")
def polars_df_100k(trades_100k: list[Trade]) -> pl.DataFrame:
    return trades_to_polars_df(trades_100k)


# ---------------------------------------------------------------------------
# Streaming reference
# ---------------------------------------------------------------------------

def test_stream_1k(benchmark: pytest.FixtureRequest, trades_1k: list[Trade]) -> None:
    result = benchmark(lambda: compute_stream(trades_1k))
    assert len(result) == 1_000


def test_stream_10k(benchmark: pytest.FixtureRequest, trades_10k: list[Trade]) -> None:
    result = benchmark(lambda: compute_stream(trades_10k))
    assert len(result) == 10_000


# ---------------------------------------------------------------------------
# Polars
# ---------------------------------------------------------------------------

def test_polars_1k(benchmark: pytest.FixtureRequest, polars_df_1k: pl.DataFrame) -> None:
    result = benchmark(lambda: compute_polars(polars_df_1k))
    assert len(result) == 1_000


def test_polars_10k(benchmark: pytest.FixtureRequest, polars_df_10k: pl.DataFrame) -> None:
    result = benchmark(lambda: compute_polars(polars_df_10k))
    assert len(result) == 10_000


def test_polars_100k(benchmark: pytest.FixtureRequest, polars_df_100k: pl.DataFrame) -> None:
    result = benchmark(lambda: compute_polars(polars_df_100k))
    assert len(result) == 100_000


# ---------------------------------------------------------------------------
# Pandas (only small — O(n²) inner loop)
# ---------------------------------------------------------------------------

def test_pandas_1k(benchmark: pytest.FixtureRequest, trades_1k: list[Trade]) -> None:
    df = trades_to_pandas_df(trades_1k)
    result = benchmark(lambda: compute_pandas(df))
    assert len(result) == 1_000


# ---------------------------------------------------------------------------
# Numba (warm JIT on session fixture)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def numba_inputs_1k(trades_1k: list[Trade]) -> tuple[np.ndarray, ...]:
    ts = np.array([t.ts for t in trades_1k], dtype=np.int64)
    price = np.array([t.price for t in trades_1k], dtype=np.float64)
    qty = np.array([t.qty for t in trades_1k], dtype=np.float64)
    signed_qty = np.array([t.qty if t.side == "buy" else -t.qty for t in trades_1k])
    return ts, price, qty, signed_qty


@pytest.fixture(scope="session")
def numba_inputs_100k(trades_100k: list[Trade]) -> tuple[np.ndarray, ...]:
    ts = np.array([t.ts for t in trades_100k], dtype=np.int64)
    price = np.array([t.price for t in trades_100k], dtype=np.float64)
    qty = np.array([t.qty for t in trades_100k], dtype=np.float64)
    signed_qty = np.array([t.qty if t.side == "buy" else -t.qty for t in trades_100k])
    # warm the JIT
    compute_numba(None, ts[:100], price[:100], qty[:100], signed_qty[:100])
    return ts, price, qty, signed_qty


def test_numba_1k(
    benchmark: pytest.FixtureRequest,
    numba_inputs_1k: tuple[np.ndarray, ...],
) -> None:
    ts, price, qty, sq = numba_inputs_1k
    compute_numba(None, ts, price, qty, sq)  # warm JIT
    result = benchmark(lambda: compute_numba(None, ts, price, qty, sq))
    assert len(result["vwap_1m"]) == 1_000


def test_numba_100k(
    benchmark: pytest.FixtureRequest,
    numba_inputs_100k: tuple[np.ndarray, ...],
) -> None:
    ts, price, qty, sq = numba_inputs_100k
    result = benchmark(lambda: compute_numba(None, ts, price, qty, sq))
    assert len(result["vwap_1m"]) == 100_000
