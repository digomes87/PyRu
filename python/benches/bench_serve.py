"""Serving layer latency benchmark — in-process measurements.

Since we cannot easily start/stop uvicorn in a benchmark loop, we measure
the hot-path lookup logic directly (the part that matters most for latency).

These measurements correspond to:
  - Cache hit: Polars DataFrame filter
  - Cold path: DuckDB point query on Parquet

For end-to-end HTTP latency, use the k6/vegeta load tests in docker/.

Run with:
    uv run pytest benches/bench_serve.py \
        --benchmark-json=../bench/results/serve_python_$(git rev-parse --short HEAD).json
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

from cfb.models import FeatureRow
from cfb.storage import write_partitioned

BASE_TS = 1_700_000_000 * 1_000_000_000
_NS_PER_S = 1_000_000_000


def make_feature_rows(n: int) -> list[FeatureRow]:
    return [
        FeatureRow(
            ts=BASE_TS + i * _NS_PER_S,
            symbol="BTCUSDT",
            vwap_1m=30_000.0 + i * 0.01,
            vwap_5m=30_000.0,
            vwap_15m=30_000.0,
            rv_1m=float(i % 100) * 1e-6,
            rv_5m=float(i % 100) * 2e-6,
            ofi_1m=float((i % 20) - 10),
            microprice=None,
            trade_count_1m=(i % 50) + 1,
        )
        for i in range(n)
    ]


@pytest.fixture(scope="session")
def cache_df_10k() -> pl.DataFrame:
    rows = make_feature_rows(10_000)
    return pl.DataFrame({
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
    })


@pytest.fixture(scope="session")
def dataset_path_for_serve(tmp_path_factory: pytest.TempPathFactory) -> Path:
    p = tmp_path_factory.mktemp("serve")
    write_partitioned(make_feature_rows(10_000), p)
    return p


# ---------------------------------------------------------------------------
# Hot path — Polars DataFrame cache lookup
# ---------------------------------------------------------------------------

def test_cache_hit_first_row(benchmark: pytest.FixtureRequest, cache_df_10k: pl.DataFrame) -> None:
    """Lookup the first row (best case)."""
    ts = BASE_TS
    result = benchmark(
        lambda: cache_df_10k.filter(
            (pl.col("symbol") == "BTCUSDT") & (pl.col("ts") == ts)
        )
    )
    assert len(result) == 1


def test_cache_hit_middle_row(benchmark: pytest.FixtureRequest, cache_df_10k: pl.DataFrame) -> None:
    """Lookup a row in the middle (typical case)."""
    ts = BASE_TS + 5_000 * _NS_PER_S
    result = benchmark(
        lambda: cache_df_10k.filter(
            (pl.col("symbol") == "BTCUSDT") & (pl.col("ts") == ts)
        )
    )
    assert len(result) == 1


def test_cache_miss(benchmark: pytest.FixtureRequest, cache_df_10k: pl.DataFrame) -> None:
    """Lookup a ts that does not exist."""
    result = benchmark(
        lambda: cache_df_10k.filter(
            (pl.col("symbol") == "BTCUSDT") & (pl.col("ts") == 999)
        )
    )
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Cold path — DuckDB point query
# ---------------------------------------------------------------------------

def test_cold_duckdb_lookup(
    benchmark: pytest.FixtureRequest,
    dataset_path_for_serve: Path,
) -> None:
    """Cold-path DuckDB point lookup — representative of cache-miss latency."""
    import duckdb
    con = duckdb.connect()
    ts = BASE_TS + 100 * _NS_PER_S

    result = benchmark(
        lambda: con.execute(
            f"SELECT * FROM read_parquet('{dataset_path_for_serve}/**/*.parquet', "
            f"hive_partitioning=true) WHERE symbol = 'BTCUSDT' AND ts = {ts} LIMIT 1"
        ).fetchone()
    )
    assert result is not None
