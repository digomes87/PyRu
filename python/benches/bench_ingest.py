"""Ingestion benchmark suite.

Measures:
- from_file throughput (events/sec) at different dataset sizes
- batched throughput and latency overhead
- to_arrow conversion throughput

Run with:
    uv run pytest benches/bench_ingest.py --benchmark-json=../bench/results/ingest_python_$(git rev-parse --short HEAD).json
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from cfb.ingest import BatchConfig, batched, from_async_iter, from_file, to_arrow
from cfb.models import Trade

# ---------------------------------------------------------------------------
# Synthetic data generators
# ---------------------------------------------------------------------------

NS = 1_000_000_000
BASE_TS = 1_700_000_000 * NS


def make_trades(n: int) -> list[Trade]:
    """Generate n synthetic BTCUSDT trades, 100ms apart."""
    trades = []
    for i in range(n):
        trades.append(Trade(
            ts=BASE_TS + i * 100_000_000,
            symbol="BTCUSDT",
            price=30_000.0 + (i % 500) * 0.1,
            qty=0.1 + (i % 10) * 0.05,
            side="buy" if i % 2 == 0 else "sell",  # type: ignore[arg-type]
        ))
    return trades


def write_fixture(trades: list[Trade], path: Path) -> None:
    with open(path, "w") as fh:
        for t in trades:
            fh.write(t.model_dump_json() + "\n")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def fixture_10k(tmp_path_factory: pytest.TempPathFactory) -> Path:
    p = tmp_path_factory.mktemp("bench") / "10k.jsonl"
    write_fixture(make_trades(10_000), p)
    return p


@pytest.fixture(scope="session")
def fixture_100k(tmp_path_factory: pytest.TempPathFactory) -> Path:
    p = tmp_path_factory.mktemp("bench") / "100k.jsonl"
    write_fixture(make_trades(100_000), p)
    return p


@pytest.fixture(scope="session")
def trades_1k() -> list[Trade]:
    return make_trades(1_000)


@pytest.fixture(scope="session")
def trades_10k() -> list[Trade]:
    return make_trades(10_000)


@pytest.fixture(scope="session")
def trades_100k() -> list[Trade]:
    return make_trades(100_000)


# ---------------------------------------------------------------------------
# from_file benchmarks
# ---------------------------------------------------------------------------

def test_from_file_10k(benchmark: pytest.FixtureRequest, fixture_10k: Path) -> None:
    """from_file throughput: 10k trades."""
    result = benchmark(lambda: list(from_file(fixture_10k)))
    assert len(result) == 10_000


def test_from_file_100k(benchmark: pytest.FixtureRequest, fixture_100k: Path) -> None:
    """from_file throughput: 100k trades."""
    result = benchmark(lambda: list(from_file(fixture_100k)))
    assert len(result) == 100_000


# ---------------------------------------------------------------------------
# to_arrow benchmarks
# ---------------------------------------------------------------------------

def test_to_arrow_1k(benchmark: pytest.FixtureRequest, trades_1k: list[Trade]) -> None:
    """Arrow conversion: 1k trades."""
    result = benchmark(lambda: to_arrow(trades_1k))
    assert result.num_rows == 1_000


def test_to_arrow_10k(benchmark: pytest.FixtureRequest, trades_10k: list[Trade]) -> None:
    """Arrow conversion: 10k trades."""
    result = benchmark(lambda: to_arrow(trades_10k))
    assert result.num_rows == 10_000


# ---------------------------------------------------------------------------
# batched throughput (async, measured via asyncio.run)
# ---------------------------------------------------------------------------

def _run_batched(trades: list[Trade], cfg: BatchConfig) -> tuple[int, int]:
    """Returns (total_trades, num_batches)."""
    async def _inner() -> tuple[int, int]:
        total, num_batches = 0, 0
        async for b in batched(from_async_iter(trades), cfg):
            total += len(b)
            num_batches += 1
        return total, num_batches
    return asyncio.run(_inner())


def test_batched_throughput_size1k(
    benchmark: pytest.FixtureRequest,
    trades_10k: list[Trade],
) -> None:
    """Batching throughput: 10k trades, batch_size=1000."""
    cfg = BatchConfig(max_size=1_000, max_latency_ms=10_000.0)
    total, batches = benchmark(lambda: _run_batched(trades_10k, cfg))
    assert total == 10_000
    assert batches == 10


def test_batched_throughput_size100(
    benchmark: pytest.FixtureRequest,
    trades_10k: list[Trade],
) -> None:
    """Batching throughput: 10k trades, batch_size=100."""
    cfg = BatchConfig(max_size=100, max_latency_ms=10_000.0)
    total, batches = benchmark(lambda: _run_batched(trades_10k, cfg))
    assert total == 10_000
    assert batches == 100
