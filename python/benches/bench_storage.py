"""Storage layer benchmarks — write throughput, read throughput, predicate pushdown.

Run with:
    uv run pytest benches/bench_storage.py \
        --benchmark-json=../bench/results/storage_python_$(git rev-parse --short HEAD).json
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from cfb.models import FeatureRow
from cfb.storage import read_partitioned, scan_partitioned, write_partitioned

_NS_PER_HOUR = 3_600_000_000_000
BASE_TS = 1_700_000_000 * 1_000_000_000


def make_feature_rows(n: int, symbols: list[str] | None = None) -> list[FeatureRow]:
    syms = symbols or ["BTCUSDT"]
    return [
        FeatureRow(
            ts=BASE_TS + i * 1_000_000_000,  # 1s apart → ~2.8h per 10k rows
            symbol=syms[i % len(syms)],
            vwap_1m=30_000.0 + (i % 500) * 0.1,
            vwap_5m=30_000.0,
            vwap_15m=30_000.0,
            rv_1m=0.00001 * (i % 100),
            rv_5m=0.00002 * (i % 100),
            ofi_1m=float(i % 10 - 5),
            microprice=None,
            trade_count_1m=i % 50 + 1,
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rows_10k() -> list[FeatureRow]:
    return make_feature_rows(10_000)


@pytest.fixture(scope="session")
def rows_100k() -> list[FeatureRow]:
    return make_feature_rows(100_000)


@pytest.fixture(scope="session")
def multi_symbol_rows_50k() -> list[FeatureRow]:
    return make_feature_rows(50_000, symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"])


@pytest.fixture(scope="session")
def written_100k_path(tmp_path_factory: pytest.TempPathFactory, rows_100k: list[FeatureRow]) -> Path:
    p = tmp_path_factory.mktemp("storage_100k")
    write_partitioned(rows_100k, p)
    return p


@pytest.fixture(scope="session")
def written_multi_path(
    tmp_path_factory: pytest.TempPathFactory,
    multi_symbol_rows_50k: list[FeatureRow],
) -> Path:
    p = tmp_path_factory.mktemp("storage_multi")
    write_partitioned(multi_symbol_rows_50k, p)
    return p


# ---------------------------------------------------------------------------
# Write benchmarks
# ---------------------------------------------------------------------------

def test_write_10k(benchmark: pytest.FixtureRequest, rows_10k: list[FeatureRow]) -> None:
    def _write() -> None:
        with tempfile.TemporaryDirectory() as d:
            write_partitioned(rows_10k, d)
    benchmark(_write)


def test_write_100k(benchmark: pytest.FixtureRequest, rows_100k: list[FeatureRow]) -> None:
    def _write() -> None:
        with tempfile.TemporaryDirectory() as d:
            write_partitioned(rows_100k, d)
    benchmark(_write)


# ---------------------------------------------------------------------------
# Read benchmarks — full scan
# ---------------------------------------------------------------------------

def test_read_scan_100k(benchmark: pytest.FixtureRequest, written_100k_path: Path) -> None:
    result = benchmark(lambda: read_partitioned(written_100k_path))
    assert len(result) == 100_000


def test_read_scan_polars_lazy_100k(benchmark: pytest.FixtureRequest, written_100k_path: Path) -> None:
    result = benchmark(lambda: scan_partitioned(written_100k_path).collect())
    assert len(result) == 100_000


# ---------------------------------------------------------------------------
# Read benchmarks — predicate pushdown (symbol filter)
# ---------------------------------------------------------------------------

def test_read_predicate_btc(
    benchmark: pytest.FixtureRequest,
    written_multi_path: Path,
) -> None:
    """Filter to one symbol — should skip 2/3 of files."""
    result = benchmark(lambda: read_partitioned(written_multi_path, symbol="BTCUSDT"))
    assert len(result) > 0


def test_read_predicate_polars_btc(
    benchmark: pytest.FixtureRequest,
    written_multi_path: Path,
) -> None:
    result = benchmark(
        lambda: scan_partitioned(written_multi_path, symbol="BTCUSDT").collect()
    )
    assert len(result) > 0
