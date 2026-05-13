-- Canonical analytical query set
-- Both DuckDB and DataFusion must produce identical results for these queries.
-- Results are compared row-by-row within float tolerance 1e-9.

-- Q1: VWAP per minute for the last 7 days, single symbol
SELECT
    (ts / 60000000000) * 60000000000 AS minute_ts,
    SUM(vwap_1m * trade_count_1m) / SUM(trade_count_1m) AS vwap
FROM features
WHERE symbol = 'BTCUSDT'
  AND ts >= :cutoff_ts
GROUP BY minute_ts
ORDER BY minute_ts;

-- Q2: Top 10 symbols by total volume (trade_count) in last 24h
SELECT
    symbol,
    SUM(trade_count_1m) AS total_trades,
    AVG(vwap_1m)        AS avg_vwap
FROM features
WHERE ts >= :cutoff_24h
GROUP BY symbol
ORDER BY total_trades DESC
LIMIT 10;

-- Q3: Realized volatility distribution, last hour, all symbols
SELECT
    symbol,
    MIN(rv_1m)    AS rv_min,
    AVG(rv_1m)    AS rv_avg,
    MAX(rv_1m)    AS rv_max,
    COUNT(*)      AS n
FROM features
WHERE ts >= :cutoff_1h
GROUP BY symbol
ORDER BY rv_avg DESC;

-- Q4: Point lookup — feature vector at timestamp T for symbol S
SELECT *
FROM features
WHERE symbol = :symbol
  AND ts = :ts
LIMIT 1;

-- Q5: OFI momentum — rolling signed imbalance over the last 5 minutes,
--     grouped into 1-minute buckets, for one symbol
SELECT
    (ts / 60000000000) * 60000000000 AS minute_ts,
    SUM(ofi_1m)  AS cumulative_ofi,
    COUNT(*)     AS n_trades
FROM features
WHERE symbol = :symbol
  AND ts >= :cutoff_5m
GROUP BY minute_ts
ORDER BY minute_ts;
