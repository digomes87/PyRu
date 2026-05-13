"""Feature computation — Polars lazy API implementation.

Each feature is a separate function. compute_all composes them into
a single pass over the input dataframe.
"""

from __future__ import annotations

import math
from collections import deque

import polars as pl

from cfb.models import FeatureRow, Trade

NS_PER_S = 1_000_000_000
WINDOW_1M = 60 * NS_PER_S
WINDOW_5M = 5 * 60 * NS_PER_S
WINDOW_15M = 15 * 60 * NS_PER_S
LATE_THRESHOLD = 2 * NS_PER_S


# ---------------------------------------------------------------------------
# Streaming (event-by-event) reference implementation
# Used for conformance testing and as a baseline for Polars comparison.
# ---------------------------------------------------------------------------

def _vwap(trades: list[Trade]) -> float | None:
    pv = sum(t.price * t.qty for t in trades)
    v = sum(t.qty for t in trades)
    return pv / v if v > 0 else None


def _rv(trades: list[Trade]) -> float:
    if len(trades) < 2:
        return 0.0
    result = 0.0
    for i in range(1, len(trades)):
        p0, p1 = trades[i - 1].price, trades[i].price
        if p0 > 0 and p1 > 0:
            r = math.log(p1 / p0)
            result += r * r
    return result


def _ofi(trades: list[Trade]) -> float:
    total = 0.0
    for t in trades:
        total += t.qty if t.side == "buy" else -t.qty
    return total


def compute_stream(events: list[Trade]) -> list[FeatureRow]:
    """Reference streaming implementation. O(n) per event via deque."""
    watermark = 0
    buf: deque[Trade] = deque()
    rows: list[FeatureRow] = []

    for e in events:
        if e.ts < watermark - LATE_THRESHOLD:
            continue
        watermark = max(watermark, e.ts)
        buf.append(e)

        def window(w: int) -> list[Trade]:
            return [t for t in buf if t.ts >= e.ts - w]

        w1m = window(WINDOW_1M)
        w5m = window(WINDOW_5M)
        w15m = window(WINDOW_15M)

        rows.append(FeatureRow(
            ts=e.ts,
            symbol=e.symbol,
            vwap_1m=_vwap(w1m),
            vwap_5m=_vwap(w5m),
            vwap_15m=_vwap(w15m),
            rv_1m=_rv(w1m),
            rv_5m=_rv(w5m),
            ofi_1m=_ofi(w1m),
            microprice=None,
            trade_count_1m=len(w1m),
        ))

        cutoff = watermark - WINDOW_15M
        while buf and buf[0].ts < cutoff:
            buf.popleft()

    return rows


# ---------------------------------------------------------------------------
# Polars batch implementation (the primary benchmarked path)
# ---------------------------------------------------------------------------

def compute_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Compute all features over a batch DataFrame using Polars rolling ops.

    Input schema must match the Trade model (ts i64, symbol str, price f64,
    qty f64, side str). Output has all feature columns appended.

    Note: Polars rolling_* by default operates over row counts unless
    index_column + period are specified. We use group_by_dynamic to align
    on nanosecond timestamps.
    """
    lf = df.lazy().sort("ts")

    signed_qty = (
        pl.when(pl.col("side") == "buy")
        .then(pl.col("qty"))
        .otherwise(-pl.col("qty"))
    ).alias("signed_qty")

    pv = (pl.col("price") * pl.col("qty")).alias("pv")

    lf = lf.with_columns([signed_qty, pv])

    def rolling_vwap(window_ns: int, name: str) -> pl.Expr:
        return (
            pl.col("pv").rolling_sum_by("ts", window_size=f"{window_ns}ns")
            / pl.col("qty").rolling_sum_by("ts", window_size=f"{window_ns}ns")
        ).alias(name)

    def rolling_ofi(window_ns: int, name: str) -> pl.Expr:
        return pl.col("signed_qty").rolling_sum_by("ts", window_size=f"{window_ns}ns").alias(name)

    def rolling_count(window_ns: int, name: str) -> pl.Expr:
        return pl.col("ts").rolling_count_by("ts", window_size=f"{window_ns}ns").alias(name)

    lf = lf.with_columns([
        rolling_vwap(WINDOW_1M, "vwap_1m"),
        rolling_vwap(WINDOW_5M, "vwap_5m"),
        rolling_vwap(WINDOW_15M, "vwap_15m"),
        rolling_ofi(WINDOW_1M, "ofi_1m"),
        rolling_count(WINDOW_1M, "trade_count_1m"),
    ])

    log_ret = (pl.col("price") / pl.col("price").shift(1)).log(base=math.e).alias("log_ret")
    lf = lf.with_columns(log_ret)

    sq_ret = (pl.col("log_ret") ** 2).alias("sq_ret")
    lf = lf.with_columns(sq_ret)

    def rolling_rv(window_ns: int, name: str) -> pl.Expr:
        return pl.col("sq_ret").rolling_sum_by("ts", window_size=f"{window_ns}ns").alias(name)

    lf = lf.with_columns([
        rolling_rv(WINDOW_1M, "rv_1m"),
        rolling_rv(WINDOW_5M, "rv_5m"),
    ])

    lf = lf.with_columns(pl.lit(None).cast(pl.Float64).alias("microprice"))

    return lf.select([
        "ts", "symbol",
        "vwap_1m", "vwap_5m", "vwap_15m",
        "rv_1m", "rv_5m",
        "ofi_1m",
        "microprice",
        "trade_count_1m",
    ]).collect()
