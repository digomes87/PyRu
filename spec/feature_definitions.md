# Feature Definitions

Mathematical specification for each computed feature. Implementations must follow these definitions exactly to produce conformance-passing outputs.

## Notation

- Let the current event be `e` with timestamp `ts` and window `W = [ts - w, ts)`.
- Let `T(W)` be the set of all trade events in window `W` (including `e` itself, since the window is right-open at `ts` but `e` lands exactly at `ts` — include `e`).
- `p_i`, `q_i`, `s_i` denote price, quantity, and side of trade `i`.

## VWAP (Volume-Weighted Average Price)

For window `W`:

```
VWAP(W) = Σ(p_i * q_i) / Σ(q_i)   for i in T(W)
```

Edge cases:
- `T(W)` is empty → `null`
- `Σ(q_i) == 0.0` → `null` (should not occur with real data, but guard against it)

Computed for `w` = 60s (`vwap_1m`), 300s (`vwap_5m`), 900s (`vwap_15m`).

## Realized Volatility (RV)

Defined as the sum of squared log returns over consecutive trades in window `W`, ordered by `ts`:

```
r_i = ln(p_i / p_{i-1})           for consecutive pairs in T(W), ordered by ts
RV(W) = Σ(r_i²)
```

Edge cases:
- `|T(W)| < 2` → `0.0` (no returns computable)
- `p_{i-1} == 0.0` → skip that return (log of zero is undefined; should not occur with real prices)
- If two events share the same `ts`, use their order of arrival; `r_i = 0.0` for the duplicate-ts case.

Computed for `w` = 60s (`rv_1m`), 300s (`rv_5m`).

## Order Flow Imbalance (OFI)

Signed net volume over window `W`:

```
OFI(W) = Σ(q_i * sign_i)   for i in T(W)

where sign_i = +1.0 if s_i == "buy"
               -1.0 if s_i == "sell"
```

Edge cases:
- `T(W)` is empty → `0.0`

Computed for `w` = 60s (`ofi_1m`).

## Microprice

When order book data is present alongside a trade event, the microprice is:

```
microprice = (ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty)
```

Where `bid_price`, `bid_qty`, `ask_price`, `ask_qty` refer to the best bid and ask at the moment of the trade.

When no book snapshot is available: emit `null`.

## Trade Count

```
trade_count_1m = |T(W)|   for w = 60s
```

Includes the current event `e`. Minimum value is `1`.

## Implementation notes

1. **Efficient windowing:** a naive O(n²) scan per event will not achieve the throughput targets. Use a monotonic deque or circular buffer approach to maintain running sums as events enter and expire from each window.
2. **Floating-point order:** compute sums in arrival order to maintain determinism across implementations. Do not sort by price or quantity before summing.
3. **Nanosecond arithmetic:** all window boundaries are computed in nanoseconds. Do not round to milliseconds or seconds during computation — only at display time.
