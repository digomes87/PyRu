"""Trade ingestion: websocket and file-based sources."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncIterator, Iterator

from cfb.models import Trade

try:
    import websockets
    from websockets.asyncio.client import connect as ws_connect
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False


def from_file(path: Path | str) -> Iterator[Trade]:
    """Yield Trade objects from a newline-delimited JSON file."""
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield Trade.model_validate_json(line)


async def from_websocket(url: str) -> AsyncIterator[Trade]:
    """Yield Trade objects from a WebSocket stream of JSON messages."""
    if not _HAS_WEBSOCKETS:
        raise RuntimeError("websockets package is required for WebSocket ingestion")
    async with ws_connect(url) as ws:
        async for message in ws:
            yield Trade.model_validate_json(message)


class BatchConfig:
    def __init__(self, max_size: int = 1000, max_latency_ms: float = 100.0) -> None:
        self.max_size = max_size
        self.max_latency_ms = max_latency_ms


async def batched(
    source: AsyncIterator[Trade],
    config: BatchConfig | None = None,
    queue_maxsize: int = 64,
) -> AsyncIterator[list[Trade]]:
    """Wrap an async trade source into bounded batches.

    Emits a batch when either max_size trades accumulate or max_latency_ms elapses.
    Provides backpressure via a bounded asyncio.Queue.
    """
    cfg = config or BatchConfig()
    batch: list[Trade] = []
    deadline: float | None = None
    output_q: asyncio.Queue[list[Trade]] = asyncio.Queue(maxsize=queue_maxsize)

    async def producer() -> None:
        nonlocal batch, deadline
        async for trade in source:
            if deadline is None:
                deadline = asyncio.get_event_loop().time() + cfg.max_latency_ms / 1000.0
            batch.append(trade)
            now = asyncio.get_event_loop().time()
            if len(batch) >= cfg.max_size or now >= deadline:
                await output_q.put(batch)
                batch = []
                deadline = None
        if batch:
            await output_q.put(batch)
        await output_q.put([])  # sentinel

    asyncio.create_task(producer())
    while True:
        b = await output_q.get()
        if not b:
            break
        yield b
