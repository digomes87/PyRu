"""Conformance test suite.

Each case in spec/conformance_cases/ is an input.jsonl + expected.json pair.
The streaming compute_stream implementation must match expected within 1e-9.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from cfb.features import compute_stream
from cfb.models import FeatureRow, Trade

CASES_DIR = Path(__file__).parents[2] / "spec" / "conformance_cases"
TOLERANCE = 1e-9


def _load_case(jsonl_path: Path) -> tuple[list[Trade], list[dict]]:
    trades = [
        Trade.model_validate_json(line)
        for line in jsonl_path.read_text().splitlines()
        if line.strip()
    ]
    stem = jsonl_path.stem
    case_id = "_".join(stem.split("_")[:2])
    expected_path = jsonl_path.parent / f"{case_id}_expected.json"
    expected = json.loads(expected_path.read_text())
    return trades, expected


def _float_eq(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return math.isclose(a, b, abs_tol=TOLERANCE, rel_tol=0)


def _assert_row(got: FeatureRow, exp: dict, idx: int) -> None:
    assert got.ts == exp["ts"], f"row {idx}: ts mismatch"
    assert got.symbol == exp["symbol"], f"row {idx}: symbol mismatch"
    assert _float_eq(got.vwap_1m, exp["vwap_1m"]), (
        f"row {idx}: vwap_1m {got.vwap_1m} != {exp['vwap_1m']}"
    )
    assert _float_eq(got.vwap_5m, exp["vwap_5m"]), (
        f"row {idx}: vwap_5m {got.vwap_5m} != {exp['vwap_5m']}"
    )
    assert _float_eq(got.vwap_15m, exp["vwap_15m"]), (
        f"row {idx}: vwap_15m {got.vwap_15m} != {exp['vwap_15m']}"
    )
    assert _float_eq(got.rv_1m, exp["rv_1m"]), (
        f"row {idx}: rv_1m {got.rv_1m} != {exp['rv_1m']}"
    )
    assert _float_eq(got.rv_5m, exp["rv_5m"]), (
        f"row {idx}: rv_5m {got.rv_5m} != {exp['rv_5m']}"
    )
    assert _float_eq(got.ofi_1m, exp["ofi_1m"]), (
        f"row {idx}: ofi_1m {got.ofi_1m} != {exp['ofi_1m']}"
    )
    assert _float_eq(got.microprice, exp["microprice"]), f"row {idx}: microprice mismatch"
    assert got.trade_count_1m == exp["trade_count_1m"], f"row {idx}: trade_count_1m mismatch"


def collect_cases() -> list[tuple[str, Path]]:
    return [
        (p.stem, p)
        for p in sorted(CASES_DIR.glob("case_*.jsonl"))
    ]


@pytest.mark.parametrize("case_name,jsonl_path", collect_cases())
def test_conformance(case_name: str, jsonl_path: Path) -> None:
    trades, expected = _load_case(jsonl_path)
    rows = compute_stream(trades)

    assert len(rows) == len(expected), (
        f"{case_name}: got {len(rows)} rows, expected {len(expected)}"
    )
    for i, (got, exp) in enumerate(zip(rows, expected)):
        _assert_row(got, exp, i)
