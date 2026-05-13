"""Query layer: DuckDB and Polars LazyFrame engines.

Both engines implement the five canonical queries from spec/queries.sql.
Results must be identical within float tolerance 1e-9.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

_NS_PER_MIN = 60_000_000_000
_NS_PER_HOUR = 3_600_000_000_000
_NS_PER_DAY = 86_400_000_000_000


class DuckDBEngine:
    """Query engine backed by DuckDB (C++, columnar, vectorised)."""

    def __init__(self, base_path: Path | str) -> None:
        self._path = str(base_path)
        self._con = duckdb.connect()
        # Register the dataset as a view so queries can reference it by name
        self._con.execute(
            f"CREATE OR REPLACE VIEW features AS "
            f"SELECT * FROM read_parquet('{self._path}/**/*.parquet', hive_partitioning=true)"
        )

    def execute(self, sql: str) -> pl.DataFrame:
        result = self._con.execute(sql)
        # Use fetchdf() for reliability across duckdb versions
        df = result.df()
        return pl.from_pandas(df)

    # --- Q1 ---------------------------------------------------------------
    def q1_vwap_per_minute(self, symbol: str, cutoff_ts: int) -> pl.DataFrame:
        return self.execute(f"""
            SELECT
                (ts / {_NS_PER_MIN}) * {_NS_PER_MIN} AS minute_ts,
                SUM(vwap_1m * trade_count_1m) / SUM(trade_count_1m) AS vwap
            FROM features
            WHERE symbol = '{symbol}' AND ts >= {cutoff_ts}
            GROUP BY minute_ts
            ORDER BY minute_ts
        """)

    # --- Q2 ---------------------------------------------------------------
    def q2_top_symbols_24h(self, cutoff_ts: int) -> pl.DataFrame:
        return self.execute(f"""
            SELECT symbol,
                   SUM(trade_count_1m) AS total_trades,
                   AVG(vwap_1m)        AS avg_vwap
            FROM features
            WHERE ts >= {cutoff_ts}
            GROUP BY symbol
            ORDER BY total_trades DESC
            LIMIT 10
        """)

    # --- Q3 ---------------------------------------------------------------
    def q3_rv_distribution(self, cutoff_ts: int) -> pl.DataFrame:
        return self.execute(f"""
            SELECT symbol,
                   MIN(rv_1m) AS rv_min,
                   AVG(rv_1m) AS rv_avg,
                   MAX(rv_1m) AS rv_max,
                   COUNT(*)   AS n
            FROM features
            WHERE ts >= {cutoff_ts}
            GROUP BY symbol
            ORDER BY rv_avg DESC
        """)

    # --- Q4 ---------------------------------------------------------------
    def q4_point_lookup(self, symbol: str, ts: int) -> pl.DataFrame:
        return self.execute(f"""
            SELECT * FROM features
            WHERE symbol = '{symbol}' AND ts = {ts}
            LIMIT 1
        """)

    # --- Q5 ---------------------------------------------------------------
    def q5_ofi_momentum(self, symbol: str, cutoff_ts: int) -> pl.DataFrame:
        return self.execute(f"""
            SELECT
                (ts / {_NS_PER_MIN}) * {_NS_PER_MIN} AS minute_ts,
                SUM(ofi_1m) AS cumulative_ofi,
                COUNT(*)    AS n_trades
            FROM features
            WHERE symbol = '{symbol}' AND ts >= {cutoff_ts}
            GROUP BY minute_ts
            ORDER BY minute_ts
        """)


class PolarsEngine:
    """Query engine backed by Polars LazyFrame (Rust core, SIMD-optimised)."""

    def __init__(self, base_path: Path | str) -> None:
        self._path = str(base_path)

    def _scan(self, symbol: str | None = None) -> pl.LazyFrame:
        lf = pl.scan_parquet(f"{self._path}/**/*.parquet", hive_partitioning=True)
        if symbol is not None:
            lf = lf.filter(pl.col("symbol") == symbol)
        return lf

    # --- Q1 ---------------------------------------------------------------
    def q1_vwap_per_minute(self, symbol: str, cutoff_ts: int) -> pl.DataFrame:
        return (
            self._scan(symbol)
            .filter(pl.col("ts") >= cutoff_ts)
            .with_columns(
                (pl.col("ts") // _NS_PER_MIN * _NS_PER_MIN).alias("minute_ts")
            )
            .group_by("minute_ts")
            .agg(
                (pl.col("vwap_1m") * pl.col("trade_count_1m")).sum()
                / pl.col("trade_count_1m").sum()
            )
            .sort("minute_ts")
            .collect()
        )

    # --- Q2 ---------------------------------------------------------------
    def q2_top_symbols_24h(self, cutoff_ts: int) -> pl.DataFrame:
        return (
            self._scan()
            .filter(pl.col("ts") >= cutoff_ts)
            .group_by("symbol")
            .agg([
                pl.col("trade_count_1m").sum().alias("total_trades"),
                pl.col("vwap_1m").mean().alias("avg_vwap"),
            ])
            .sort("total_trades", descending=True)
            .head(10)
            .collect()
        )

    # --- Q3 ---------------------------------------------------------------
    def q3_rv_distribution(self, cutoff_ts: int) -> pl.DataFrame:
        return (
            self._scan()
            .filter(pl.col("ts") >= cutoff_ts)
            .group_by("symbol")
            .agg([
                pl.col("rv_1m").min().alias("rv_min"),
                pl.col("rv_1m").mean().alias("rv_avg"),
                pl.col("rv_1m").max().alias("rv_max"),
                pl.len().alias("n"),
            ])
            .sort("rv_avg", descending=True)
            .collect()
        )

    # --- Q4 ---------------------------------------------------------------
    def q4_point_lookup(self, symbol: str, ts: int) -> pl.DataFrame:
        return (
            self._scan(symbol)
            .filter(pl.col("ts") == ts)
            .head(1)
            .collect()
        )

    # --- Q5 ---------------------------------------------------------------
    def q5_ofi_momentum(self, symbol: str, cutoff_ts: int) -> pl.DataFrame:
        return (
            self._scan(symbol)
            .filter(pl.col("ts") >= cutoff_ts)
            .with_columns(
                (pl.col("ts") // _NS_PER_MIN * _NS_PER_MIN).alias("minute_ts")
            )
            .group_by("minute_ts")
            .agg([
                pl.col("ofi_1m").sum().alias("cumulative_ofi"),
                pl.len().alias("n_trades"),
            ])
            .sort("minute_ts")
            .collect()
        )
