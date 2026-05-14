"""Feature computation — three Python implementations for comparison.

compute_stream  : streaming reference (event-by-event, pure Python)
compute_polars  : Polars lazy rolling API — the primary benchmarked path
compute_pandas  : pandas groupby + rolling — naive baseline
compute_numba   : numpy arrays + numba JIT — hand-tuned ceiling
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any

import polars as pl

from cfb.models import FeatureRow, Trade

NS_PER_S = 1_000_000_000
WINDOW_1M = 60 * NS_PER_S
WINDOW_5M = 5 * 60 * NS_PER_S
WINDOW_15M = 15 * 60 * NS_PER_S
LATE_THRESHOLD = 2 * NS_PER_S


# ---------------------------------------------------------------------------
# Streaming reference implementation
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
    return sum(t.qty if t.side == "buy" else -t.qty for t in trades)


def compute_stream(events: list[Trade]) -> list[FeatureRow]:
    """Reference streaming implementation — O(n) amortized via deque."""
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
# Polars rolling implementation
# ---------------------------------------------------------------------------

def compute_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Feature computation using Polars rolling_*_by expressions.

    Single-symbol assumption for the rolling window (ts must be monotone).
    For multi-symbol inputs, group by symbol first.
    """
    lf = df.lazy().sort("ts")

    signed_qty = (
        pl.when(pl.col("side") == "buy")
        .then(pl.col("qty"))
        .otherwise(-pl.col("qty"))
    ).alias("signed_qty")

    pv = (pl.col("price") * pl.col("qty")).alias("pv")
    one = pl.lit(1, dtype=pl.Int64).alias("_one")

    lf = lf.with_columns([signed_qty, pv, one])

    def rsum(expr: pl.Expr, window_ns: int) -> pl.Expr:
        return expr.rolling_sum_by("ts", window_size=f"{window_ns}i")

    lf = lf.with_columns([
        (rsum(pl.col("pv"), WINDOW_1M) / rsum(pl.col("qty"), WINDOW_1M)).alias("vwap_1m"),
        (rsum(pl.col("pv"), WINDOW_5M) / rsum(pl.col("qty"), WINDOW_5M)).alias("vwap_5m"),
        (rsum(pl.col("pv"), WINDOW_15M) / rsum(pl.col("qty"), WINDOW_15M)).alias("vwap_15m"),
        rsum(pl.col("signed_qty"), WINDOW_1M).alias("ofi_1m"),
        rsum(pl.col("_one"), WINDOW_1M).alias("trade_count_1m"),
    ])

    log_ret = (
        (pl.col("price") / pl.col("price").shift(1))
        .map_elements(lambda x: math.log(x) if x and x > 0 else 0.0, return_dtype=pl.Float64)
        .alias("log_ret")
    )
    lf = lf.with_columns(log_ret)
    lf = lf.with_columns((pl.col("log_ret") ** 2).alias("sq_ret"))
    lf = lf.with_columns([
        rsum(pl.col("sq_ret"), WINDOW_1M).alias("rv_1m"),
        rsum(pl.col("sq_ret"), WINDOW_5M).alias("rv_5m"),
    ])
    lf = lf.with_columns(pl.lit(None, dtype=pl.Float64).alias("microprice"))

    return lf.select([
        "ts", "symbol",
        "vwap_1m", "vwap_5m", "vwap_15m",
        "rv_1m", "rv_5m",
        "ofi_1m", "microprice", "trade_count_1m",
    ]).collect()


# ---------------------------------------------------------------------------
# Pandas naive baseline
# ---------------------------------------------------------------------------

def compute_pandas(df: Any) -> Any:
    """Feature computation using pandas rolling. Requires pandas installed."""
    import numpy as np
    import pandas as pd

    df = df.sort_values("ts").reset_index(drop=True)
    ts = df["ts"].values
    price = df["price"].values
    qty = df["qty"].values
    side = df["side"].values

    n = len(df)
    pv = price * qty
    signed_qty = np.where(side == "buy", qty, -qty)

    def _roll(arr: Any, window_ns: int) -> Any:
        out = np.empty(n)
        for i in range(n):
            mask = ts >= ts[i] - window_ns
            mask[i + 1:] = False
            out[i] = arr[mask].sum()
        return out

    pv_1m = _roll(pv, WINDOW_1M)
    q_1m = _roll(qty, WINDOW_1M)
    pv_5m = _roll(pv, WINDOW_5M)
    q_5m = _roll(qty, WINDOW_5M)
    pv_15m = _roll(pv, WINDOW_15M)
    q_15m = _roll(qty, WINDOW_15M)
    ofi_1m = _roll(signed_qty, WINDOW_1M)
    count_1m = _roll(np.ones(n), WINDOW_1M)

    log_ret = np.concatenate([[0.0], np.log(price[1:] / price[:-1])])
    sq_ret = log_ret ** 2
    rv_1m = _roll(sq_ret, WINDOW_1M)
    rv_5m = _roll(sq_ret, WINDOW_5M)

    return pd.DataFrame({
        "ts": ts,
        "symbol": df["symbol"].values,
        "vwap_1m": np.where(q_1m > 0, pv_1m / q_1m, np.nan),
        "vwap_5m": np.where(q_5m > 0, pv_5m / q_5m, np.nan),
        "vwap_15m": np.where(q_15m > 0, pv_15m / q_15m, np.nan),
        "rv_1m": rv_1m,
        "rv_5m": rv_5m,
        "ofi_1m": ofi_1m,
        "microprice": np.nan,
        "trade_count_1m": count_1m.astype(int),
    })


# ---------------------------------------------------------------------------
# numpy + numba hand-tuned implementation
# ---------------------------------------------------------------------------

def compute_numba(
    trades_arr: Any, ts_arr: Any, price_arr: Any, qty_arr: Any, side_arr: Any
) -> dict[str, Any]:
    """Feature computation using numba JIT for maximum Python-side throughput.

    Inputs are numpy arrays (ts: int64, price/qty: float64, side: int8 where 1=buy,-1=sell).
    Returns a dict of numpy arrays matching the output schema.
    """
    try:
        import numba as nb
        import numpy as np
    except ImportError as e:
        raise RuntimeError("numba not installed") from e

    @nb.njit(cache=True)  # type: ignore[untyped-decorator]
    def _compute_nb(ts: Any, price: Any, qty: Any, signed_qty: Any) -> tuple[Any, ...]:
        n = len(ts)
        vwap_1m = np.empty(n)
        vwap_5m = np.empty(n)
        vwap_15m = np.empty(n)
        rv_1m = np.empty(n)
        rv_5m = np.empty(n)
        ofi_1m = np.empty(n)
        count_1m = np.empty(n, dtype=np.int64)

        W1 = 60_000_000_000
        W5 = 300_000_000_000
        W15 = 900_000_000_000

        # sliding window pointers
        lo1, lo5, lo15 = 0, 0, 0
        pv1 = pv5 = pv15 = 0.0
        q1 = q5 = q15 = 0.0
        ofi1 = 0.0
        cnt1 = 0

        # rv: need (price[i] / price[i-1]) across the window
        # keep sq_ret buffer per window — simplified: recompute on eviction
        sq_ret = np.empty(n)
        sq_ret[0] = 0.0
        for i in range(1, n):
            if price[i - 1] > 0:
                r = math.log(price[i] / price[i - 1])
                sq_ret[i] = r * r
            else:
                sq_ret[i] = 0.0

        rv1_sum = rv5_sum = 0.0
        lo_rv1 = lo_rv5 = 0

        for i in range(n):
            t = ts[i]
            pv_i = price[i] * qty[i]
            sq_i = sq_ret[i]

            # add to windows
            pv1 += pv_i
            pv5 += pv_i
            pv15 += pv_i
            q1 += qty[i]
            q5 += qty[i]
            q15 += qty[i]
            ofi1 += signed_qty[i]
            cnt1 += 1
            rv1_sum += sq_i
            rv5_sum += sq_i

            # evict from 1m
            while lo1 <= i and ts[lo1] < t - W1:
                pv1 -= price[lo1] * qty[lo1]
                q1 -= qty[lo1]
                ofi1 -= signed_qty[lo1]
                cnt1 -= 1
                lo1 += 1
            while lo_rv1 <= i and ts[lo_rv1] < t - W1:
                rv1_sum -= sq_ret[lo_rv1]
                lo_rv1 += 1

            # evict from 5m
            while lo5 <= i and ts[lo5] < t - W5:
                pv5 -= price[lo5] * qty[lo5]
                q5 -= qty[lo5]
                lo5 += 1
            while lo_rv5 <= i and ts[lo_rv5] < t - W5:
                rv5_sum -= sq_ret[lo_rv5]
                lo_rv5 += 1

            # evict from 15m
            while lo15 <= i and ts[lo15] < t - W15:
                pv15 -= price[lo15] * qty[lo15]
                q15 -= qty[lo15]
                lo15 += 1

            vwap_1m[i] = pv1 / q1 if q1 > 0 else math.nan
            vwap_5m[i] = pv5 / q5 if q5 > 0 else math.nan
            vwap_15m[i] = pv15 / q15 if q15 > 0 else math.nan
            ofi_1m[i] = ofi1
            count_1m[i] = cnt1
            rv_1m[i] = max(0.0, rv1_sum)
            rv_5m[i] = max(0.0, rv5_sum)

        return vwap_1m, vwap_5m, vwap_15m, rv_1m, rv_5m, ofi_1m, count_1m

    # Inline math.log in numba requires importing math at module level inside njit
    import numpy as np
    vwap_1m, vwap_5m, vwap_15m, rv_1m, rv_5m, ofi_1m, count_1m = _compute_nb(
        ts_arr, price_arr, qty_arr, side_arr
    )
    return {
        "vwap_1m": vwap_1m,
        "vwap_5m": vwap_5m,
        "vwap_15m": vwap_15m,
        "rv_1m": rv_1m,
        "rv_5m": rv_5m,
        "ofi_1m": ofi_1m,
        "trade_count_1m": count_1m,
    }
