"""Feature serving — FastAPI application.

Architecture:
  - Hot path: in-process Polars DataFrame cache (configurable window).
  - Cold path: DuckDB scan over partitioned Parquet.
  - Health endpoint: /health
  - Feature endpoint: GET /features/{symbol}/{ts}

Performance notes:
  - uvloop dramatically lowers tail latency vs default asyncio loop.
  - The hot cache is a simple DataFrame kept in process memory.
  - Production use: wrap in a uvicorn process manager; pin workers to CPUs.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cfb.models import FeatureRow

app = FastAPI(title="cfb-serve", version="0.1.0")

_base_path = Path(os.getenv("CFB_DATA_PATH", "data/features"))
_cache: pl.DataFrame | None = None
_cache_loaded_at: float = 0.0
_cache_ttl_s: float = float(os.getenv("CFB_CACHE_TTL_S", "60"))


class HealthResponse(BaseModel):
    status: str
    cache_rows: int
    cache_age_s: float


class FeatureResponse(BaseModel):
    ts: int
    symbol: str
    vwap_1m: float | None
    vwap_5m: float | None
    vwap_15m: float | None
    rv_1m: float
    rv_5m: float
    ofi_1m: float
    microprice: float | None
    trade_count_1m: int
    source: str  # "cache" or "parquet"


def _load_cache() -> pl.DataFrame:
    import pyarrow.dataset as ds
    dataset = ds.dataset(str(_base_path), format="parquet", partitioning="hive")  # type: ignore[no-untyped-call]
    return pl.from_arrow(dataset.to_table())  # type: ignore[return-value]


def _get_cache() -> pl.DataFrame | None:
    global _cache, _cache_loaded_at
    now = time.monotonic()
    if _cache is None or (now - _cache_loaded_at) > _cache_ttl_s:
        try:
            _cache = _load_cache()
            _cache_loaded_at = now
        except Exception:
            pass
    return _cache


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    cache = _get_cache()
    rows = len(cache) if cache is not None else 0
    age = time.monotonic() - _cache_loaded_at
    return HealthResponse(status="ok", cache_rows=rows, cache_age_s=round(age, 2))


@app.get("/features/{symbol}/{ts}", response_model=FeatureResponse)
async def get_features(symbol: str, ts: int) -> FeatureResponse:
    # Hot path: Polars in-process cache
    cache = _get_cache()
    if cache is not None and len(cache) > 0:
        row = cache.filter(
            (pl.col("symbol") == symbol) & (pl.col("ts") == ts)
        )
        if len(row) > 0:
            r = row.row(0, named=True)
            fields = {k: r.get(k) for k in FeatureResponse.model_fields}
            return FeatureResponse(**fields, source="cache")  # type: ignore[arg-type]

    # Cold path: DuckDB scan
    import duckdb
    con = duckdb.connect()
    result = con.execute(
        f"SELECT * FROM read_parquet('{_base_path}/**/*.parquet', hive_partitioning=true) "
        f"WHERE symbol = ? AND ts = ? LIMIT 1",
        [symbol, ts],
    ).fetchone()

    if result is None:
        raise HTTPException(status_code=404, detail=f"Feature not found: {symbol}@{ts}")

    cols = list(FeatureRow.model_fields.keys())
    d = dict(zip(cols, result))
    return FeatureResponse(**d, source="parquet")


def main() -> None:
    uvicorn.run(
        "cfb.serve:app",
        host="0.0.0.0",
        port=8000,
        loop="uvloop",
        workers=1,
    )


if __name__ == "__main__":
    main()
