"""Generate conformance fixture expected-output JSON from input JSONL files.

Run from the repo root:
    python spec/generate_fixtures.py

Outputs case_*_expected.json alongside each case_*_input.jsonl.
These values are the canonical reference; implementations must match within 1e-9.
"""

from __future__ import annotations

import json
import math
from collections import deque
from pathlib import Path
from typing import Iterator

NS_PER_S = 1_000_000_000
WINDOW_1M = 60 * NS_PER_S
WINDOW_5M = 5 * 60 * NS_PER_S
WINDOW_15M = 15 * 60 * NS_PER_S
LATE_THRESHOLD = 2 * NS_PER_S


def vwap(trades: list[dict]) -> float | None:
    pv = sum(t["price"] * t["qty"] for t in trades)
    v = sum(t["qty"] for t in trades)
    if v == 0.0:
        return None
    return pv / v


def rv(trades: list[dict]) -> float:
    if len(trades) < 2:
        return 0.0
    result = 0.0
    for i in range(1, len(trades)):
        p_prev = trades[i - 1]["price"]
        p_curr = trades[i]["price"]
        if p_prev > 0.0 and p_curr > 0.0:
            r = math.log(p_curr / p_prev)
            result += r * r
    return result


def ofi(trades: list[dict]) -> float:
    total = 0.0
    for t in trades:
        sign = 1.0 if t["side"] == "buy" else -1.0
        total += sign * t["qty"]
    return total


def process_stream(events: list[dict]) -> list[dict]:
    watermark = 0
    buffer: deque[dict] = deque()
    rows = []

    for e in events:
        ts = e["ts"]
        if ts < watermark - LATE_THRESHOLD:
            continue
        watermark = max(watermark, ts)

        buffer.append(e)

        def window(w_ns: int) -> list[dict]:
            return [t for t in buffer if t["ts"] >= ts - w_ns]

        w1m = window(WINDOW_1M)
        w5m = window(WINDOW_5M)
        w15m = window(WINDOW_15M)

        rows.append({
            "ts": ts,
            "symbol": e["symbol"],
            "vwap_1m": vwap(w1m),
            "vwap_5m": vwap(w5m),
            "vwap_15m": vwap(w15m),
            "rv_1m": rv(w1m),
            "rv_5m": rv(w5m),
            "ofi_1m": ofi(w1m),
            "microprice": None,
            "trade_count_1m": len(w1m),
        })

        # prune buffer beyond the largest window
        cutoff = watermark - WINDOW_15M
        while buffer and buffer[0]["ts"] < cutoff:
            buffer.popleft()

    return rows


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> None:
    cases_dir = Path(__file__).parent / "conformance_cases"
    for input_file in sorted(cases_dir.glob("case_*_*.jsonl")):
        stem = input_file.stem.replace(".jsonl", "")
        case_id = "_".join(stem.split("_")[:2])
        expected_file = cases_dir / f"{case_id}_expected.json"

        events = load_jsonl(input_file)
        rows = process_stream(events)
        expected_file.write_text(json.dumps(rows, indent=2, allow_nan=False) + "\n")
        print(f"wrote {expected_file.name}")


if __name__ == "__main__":
    main()
