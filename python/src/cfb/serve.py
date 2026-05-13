"""Feature serving — FastAPI application.

Hot path: in-process Polars DataFrame cache (last N minutes).
Cold path: Parquet scan via DuckDB.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cfb.models import FeatureRow

app = FastAPI(title="cfb-serve", version="0.1.0")

_cache: pl.DataFrame | None = None
_base_path: Path = Path(os.getenv("CFB_DATA_PATH", "data/features"))


class HealthResponse(BaseModel):
    status: str
    cache_rows: int


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    rows = len(_cache) if _cache is not None else 0
    return HealthResponse(status="ok", cache_rows=rows)


@app.get("/features/{symbol}/{ts}", response_model=FeatureRow)
async def get_features(symbol: str, ts: int) -> FeatureRow:
    global _cache

    if _cache is not None:
        row = _cache.filter(
            (pl.col("symbol") == symbol) & (pl.col("ts") == ts)
        )
        if len(row) > 0:
            return FeatureRow(**row.row(0, named=True))

    import duckdb
    con = duckdb.connect()
    result = con.execute(
        f"""
        SELECT * FROM read_parquet('{_base_path}/**/*.parquet', hive_partitioning=true)
        WHERE symbol = ? AND ts = ?
        LIMIT 1
        """,
        [symbol, ts],
    ).fetchone()

    if result is None:
        raise HTTPException(status_code=404, detail="Feature row not found")

    cols = ["ts", "symbol", "vwap_1m", "vwap_5m", "vwap_15m",
            "rv_1m", "rv_5m", "ofi_1m", "microprice", "trade_count_1m"]
    return FeatureRow(**dict(zip(cols, result)))


def main() -> None:
    uvicorn.run("cfb.serve:app", host="0.0.0.0", port=8000, loop="uvloop")


if __name__ == "__main__":
    main()
