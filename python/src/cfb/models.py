"""Shared data models for the feature pipeline."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class Trade(BaseModel):
    ts: int
    symbol: str
    price: float
    qty: float
    side: Literal["buy", "sell"]


class FeatureRow(BaseModel):
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
