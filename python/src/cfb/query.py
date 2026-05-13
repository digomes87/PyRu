"""Query layer: DuckDB and Polars LazyFrame engines."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


class DuckDBEngine:
    def __init__(self, base_path: Path | str) -> None:
        self._path = str(base_path)
        self._con = duckdb.connect()

    def execute(self, sql: str) -> pl.DataFrame:
        result = self._con.execute(sql.replace("{path}", self._path))
        return pl.from_arrow(result.arrow())

    def q1_vwap_7d(self, symbol: str) -> pl.DataFrame:
        sql = """
        SELECT
            date_trunc('minute', to_timestamp(ts / 1e9)) AS minute,
            sum(vwap_1m * trade_count_1m) / sum(trade_count_1m) AS vwap
        FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        WHERE symbol = '{symbol}'
          AND ts >= epoch_ns(now()) - interval '7 days'
        GROUP BY 1
        ORDER BY 1
        """.replace("{symbol}", symbol)
        return self.execute(sql)


class PolarsEngine:
    def __init__(self, base_path: Path | str) -> None:
        self._path = str(base_path)

    def _scan(self) -> pl.LazyFrame:
        return pl.scan_parquet(f"{self._path}/**/*.parquet", hive_partitioning=True)

    def q1_vwap_7d(self, symbol: str) -> pl.DataFrame:
        cutoff_ns = (
            pl.Series([0]).cast(pl.Int64)
        )
        return (
            self._scan()
            .filter(pl.col("symbol") == symbol)
            .with_columns(
                (pl.col("ts") // 60_000_000_000 * 60_000_000_000).alias("minute")
            )
            .group_by("minute")
            .agg(
                (pl.col("vwap_1m") * pl.col("trade_count_1m")).sum()
                / pl.col("trade_count_1m").sum()
            )
            .sort("minute")
            .collect()
        )
