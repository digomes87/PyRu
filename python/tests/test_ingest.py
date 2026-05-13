"""Unit tests for the ingestion layer — from_file, batched, to_arrow."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from cfb.ingest import BatchConfig, batched, from_async_iter, from_file, to_arrow
from cfb.models import Trade


def make_trade(ts: int = 1_000_000_000, price: float = 30_000.0, side: str = "buy") -> Trade:
    return Trade(ts=ts, symbol="BTCUSDT", price=price, qty=1.0, side=side)  # type: ignore[arg-type]


def write_jsonl(path: Path, trades: list[Trade]) -> None:
    with open(path, "w") as fh:
        for t in trades:
            fh.write(t.model_dump_json() + "\n")


# ---------------------------------------------------------------------------
# from_file
# ---------------------------------------------------------------------------

def test_from_file_yields_trades(tmp_path: Path) -> None:
    trades = [make_trade(ts=i * 1_000_000_000) for i in range(5)]
    p = tmp_path / "trades.jsonl"
    write_jsonl(p, trades)

    result = list(from_file(p))
    assert len(result) == 5
    assert result[0].ts == 0
    assert result[4].ts == 4_000_000_000


def test_from_file_skips_blank_lines(tmp_path: Path) -> None:
    p = tmp_path / "trades.jsonl"
    t = make_trade()
    p.write_text(f"\n{t.model_dump_json()}\n\n{t.model_dump_json()}\n")
    assert len(list(from_file(p))) == 2


def test_from_file_empty(tmp_path: Path) -> None:
    p = tmp_path / "empty.jsonl"
    p.write_text("")
    assert list(from_file(p)) == []


# ---------------------------------------------------------------------------
# batched — size boundary
# ---------------------------------------------------------------------------

async def collect_batches(trades: list[Trade], cfg: BatchConfig) -> list[list[Trade]]:
    batches = []
    async for b in batched(from_async_iter(trades), cfg):
        batches.append(b)
    return batches


@pytest.mark.asyncio
async def test_batched_exact_size() -> None:
    """Batch emits exactly when max_size is reached."""
    trades = [make_trade(ts=i) for i in range(10)]
    cfg = BatchConfig(max_size=5, max_latency_ms=10_000.0)
    batches = await collect_batches(trades, cfg)
    assert len(batches) == 2
    assert all(len(b) == 5 for b in batches)


@pytest.mark.asyncio
async def test_batched_partial_flush_at_end() -> None:
    """Remaining trades are flushed when the source is exhausted."""
    trades = [make_trade(ts=i) for i in range(7)]
    cfg = BatchConfig(max_size=5, max_latency_ms=10_000.0)
    batches = await collect_batches(trades, cfg)
    assert len(batches) == 2
    assert len(batches[0]) == 5
    assert len(batches[1]) == 2


@pytest.mark.asyncio
async def test_batched_empty_source() -> None:
    """Empty source produces no batches."""
    batches = await collect_batches([], BatchConfig())
    assert batches == []


@pytest.mark.asyncio
async def test_batched_single_trade() -> None:
    batches = await collect_batches([make_trade()], BatchConfig(max_size=100))
    assert len(batches) == 1
    assert len(batches[0]) == 1


@pytest.mark.asyncio
async def test_batched_preserves_order() -> None:
    trades = [make_trade(ts=i * 1_000_000_000) for i in range(20)]
    cfg = BatchConfig(max_size=7, max_latency_ms=10_000.0)
    batches = await collect_batches(trades, cfg)
    flat = [t for b in batches for t in b]
    assert [t.ts for t in flat] == [t.ts for t in trades]


@pytest.mark.asyncio
async def test_batched_latency_flush() -> None:
    """Batch flushes on timeout even when max_size not reached."""
    async def slow_source() -> None:  # type: ignore[return]
        for i in range(3):
            yield make_trade(ts=i)
            await asyncio.sleep(0.005)  # 5ms between trades

    cfg = BatchConfig(max_size=100, max_latency_ms=8.0)  # flush at 8ms
    batches: list[list[Trade]] = []
    async for b in batched(slow_source(), cfg):  # type: ignore[arg-type]
        batches.append(b)

    assert len(batches) >= 1
    assert sum(len(b) for b in batches) == 3


# ---------------------------------------------------------------------------
# to_arrow
# ---------------------------------------------------------------------------

def test_to_arrow_schema() -> None:
    trades = [make_trade(ts=1_700_000_000_000_000_000, price=30_100.5, side="sell")]
    batch = to_arrow(trades)
    assert isinstance(batch, pa.RecordBatch)
    assert batch.schema.field("ts").type == pa.int64()
    assert batch.schema.field("price").type == pa.float64()
    assert batch.num_rows == 1
    assert batch.column("side")[0].as_py() == "sell"


def test_to_arrow_empty() -> None:
    batch = to_arrow([])
    assert batch.num_rows == 0
    assert set(batch.schema.names) == {"ts", "symbol", "price", "qty", "side"}


def test_to_arrow_values() -> None:
    trades = [make_trade(ts=i * 1_000_000_000, price=float(30_000 + i)) for i in range(5)]
    batch = to_arrow(trades)
    assert batch.column("ts").to_pylist() == [i * 1_000_000_000 for i in range(5)]
    assert batch.column("price").to_pylist() == [float(30_000 + i) for i in range(5)]
