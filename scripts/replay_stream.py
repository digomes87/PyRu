"""Replay recorded trade data as a WebSocket stream.

Reads trade events from a JSONL file and re-emits them over a local WebSocket
server, optionally at a faster-than-realtime replay speed. Used as the load
generator for ingestion benchmarks.

Usage:
    python scripts/replay_stream.py --file data/raw/btcusdt_replay.jsonl --speed 10 --port 9000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import websockets
from websockets.asyncio.server import serve


async def replay_handler(
    websocket: websockets.ServerConnection,
    events: list[dict],
    speed: float,
) -> None:
    if not events:
        return

    first_ts = events[0]["ts"]
    wall_start = time.monotonic_ns()

    for event in events:
        # compute how far along in the stream we should be
        elapsed_sim_ns = event["ts"] - first_ts
        elapsed_wall_ns = int(elapsed_sim_ns / speed)
        target_wall_ns = wall_start + elapsed_wall_ns

        now = time.monotonic_ns()
        if target_wall_ns > now:
            await asyncio.sleep((target_wall_ns - now) / 1e9)

        await websocket.send(json.dumps(event))

    print(f"Replay complete: {len(events)} events sent")


def load_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


async def main(args: argparse.Namespace) -> None:
    events = load_events(Path(args.file))
    print(f"Loaded {len(events)} events from {args.file}")
    print(f"Replaying at {args.speed}x speed on ws://localhost:{args.port}")

    async def handler(ws: websockets.ServerConnection) -> None:
        await replay_handler(ws, events, args.speed)

    async with serve(handler, "localhost", args.port):
        await asyncio.get_event_loop().create_future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay trade stream over WebSocket")
    parser.add_argument("--file", default="data/raw/btcusdt_replay.jsonl", help="Input JSONL file")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier")
    parser.add_argument("--port", type=int, default=9000, help="WebSocket port")
    asyncio.run(main(parser.parse_args()))
