# Functional Contract

Both the Python and Rust implementations must satisfy this contract exactly. Implementations that pass the conformance suite may be benchmarked; implementations that do not must not have their performance numbers recorded.

## Input schema

Each trade event is a JSON object (or equivalent Arrow struct) with the following fields:

| Field    | Type             | Notes                                          |
|----------|------------------|------------------------------------------------|
| `ts`     | `i64`            | Nanoseconds since Unix epoch (UTC)             |
| `symbol` | `str`            | Ticker, e.g. `"BTCUSDT"`                       |
| `price`  | `f64`            | Trade price                                    |
| `qty`    | `f64`            | Trade quantity (base currency)                 |
| `side`   | `"buy"\|"sell"` | Aggressor side                                 |

Events arrive roughly in order. **Late events** — those whose `ts` is behind the current watermark by up to 2 seconds — must be accepted and folded into the correct windows retroactively. Events more than 2 seconds late may be dropped; a counter named `late_drop_total` must be incremented for each dropped event.

## Output schema

For every input event (including late ones that are accepted), emit one feature row:

| Field            | Type      | Notes                                                                 |
|------------------|-----------|-----------------------------------------------------------------------|
| `ts`             | `i64`     | Same as input `ts`                                                    |
| `symbol`         | `str`     | Same as input `symbol`                                                |
| `vwap_1m`        | `f64`     | Volume-weighted avg price, rolling 1-minute window                    |
| `vwap_5m`        | `f64`     | Volume-weighted avg price, rolling 5-minute window                    |
| `vwap_15m`       | `f64`     | Volume-weighted avg price, rolling 15-minute window                   |
| `rv_1m`          | `f64`     | Realized volatility, rolling 1-minute window                          |
| `rv_5m`          | `f64`     | Realized volatility, rolling 5-minute window                          |
| `ofi_1m`         | `f64`     | Order flow imbalance, rolling 1-minute window                         |
| `microprice`     | `f64?`    | Book microprice if book data present, else `null`                     |
| `trade_count_1m` | `i64`     | Number of trades in last 60 seconds (inclusive of current event)      |

## Windowing semantics

- Windows are **time-based**, closed on the left, open on the right: `[ts - window_ns, ts)`.
- A window containing zero trades must produce `null` for VWAP, `0.0` for RV, `0.0` for OFI, and `0` for trade count. In practice this only occurs for the very first event.
- Single-trade windows: VWAP = that trade's price; RV = `0.0` (no log returns).

## Conformance tolerance

Floating-point outputs must match the reference to within `1e-9` absolute error. Integer fields must be exactly equal. Null/non-null must match exactly.

## Conformance cases

See `spec/conformance_cases/`. Each case is:
- `input.jsonl` — one trade per line, newline-delimited JSON
- `expected.parquet` — the expected output rows in the same order as the input
