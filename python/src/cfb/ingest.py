"""Trade ingestion: websocket and file-based sources.

Public API:
    from_file(path)           -> Iterator[Trade]
    from_websocket(url)       -> AsyncIterator[Trade]
    batched(source, config)   -> AsyncIterator[list[Trade]]
    to_arrow(trades)          -> pa.RecordBatch
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from cfb.models import Trade

_TRADE_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
])


def from_file(path: Path | str) -> Iterator[Trade]:
    """Yield Trade objects from a newline-delimited JSON file."""
    with open(path) as fh:
        for line in fh:
            stripped = line.strip()
            if stripped:
                yield Trade.model_validate_json(stripped)


async def from_websocket(url: str) -> AsyncIterator[Trade]:
    """Yield Trade objects from a WebSocket stream of JSON messages."""
    try:
        from websockets.asyncio.client import connect
    except ImportError as e:
        raise RuntimeError("websockets package is required") from e

    async with connect(url) as ws:
        async for message in ws:
            if isinstance(message, (str, bytes)):
                yield Trade.model_validate_json(message)


async def from_async_iter(trades: list[Trade]) -> AsyncIterator[Trade]:
    """Wrap a list of trades as an async iterator (for testing)."""
    for t in trades:
        yield t


@dataclass
class BatchConfig:
    max_size: int = 1_000
    max_latency_ms: float = 100.0
    queue_maxsize: int = 64
    late_drop_counter: list[int] = field(default_factory=lambda: [0])


async def batched(
    source: AsyncIterator[Trade],
    config: BatchConfig | None = None,
) -> AsyncIterator[list[Trade]]:
    """Wrap an async trade source into bounded batches.

    Emits a batch when either max_size trades accumulate or max_latency_ms
    elapses, whichever comes first. Backpressure via bounded asyncio.Queue.
    """
    cfg = config or BatchConfig()
    loop = asyncio.get_running_loop()
    output_q: asyncio.Queue[list[Trade]] = asyncio.Queue(maxsize=cfg.queue_maxsize)

    async def producer() -> None:
        buf: list[Trade] = []
        deadline: float | None = None

        async def flush() -> None:
            nonlocal buf, deadline
            if buf:
                await output_q.put(buf)
                buf = []
            deadline = None

        try:
            async for trade in source:
                if deadline is None:
                    deadline = loop.time() + cfg.max_latency_ms / 1000.0
                buf.append(trade)
                if len(buf) >= cfg.max_size:
                    await flush()
                elif loop.time() >= deadline:
                    await flush()
            await flush()
        finally:
            await output_q.put([])  # sentinel: always sent even on exception

    task = asyncio.create_task(producer())
    try:
        while True:
            batch = await output_q.get()
            if not batch:
                break
            yield batch
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def to_arrow(trades: list[Trade]) -> pa.RecordBatch:
    """Convert a list of Trade objects to an Arrow RecordBatch."""
    if not trades:
        return pa.record_batch(
            {col: [] for col in _TRADE_SCHEMA.names},
            schema=_TRADE_SCHEMA,
        )
    return pa.record_batch(
        {
            "ts": [t.ts for t in trades],
            "symbol": [t.symbol for t in trades],
            "price": [t.price for t in trades],
            "qty": [t.qty for t in trades],
            "side": [t.side for t in trades],
        },
        schema=_TRADE_SCHEMA,
    )
