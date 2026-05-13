"""Analysis script: turns bench/results/ JSON files into plots in bench/plots/.

Run from the repo root:
    python bench/analyze.py

Requires: polars, matplotlib (install via uv or pip).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parents[1]
RESULTS_DIR = REPO_ROOT / "bench" / "results"
PLOTS_DIR = REPO_ROOT / "bench" / "plots"


def main() -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import polars as pl
    except ImportError as e:
        print(f"Missing dependency: {e}", file=sys.stderr)
        print("Install with: pip install matplotlib polars", file=sys.stderr)
        sys.exit(1)

    PLOTS_DIR.mkdir(exist_ok=True)

    result_files = sorted(RESULTS_DIR.glob("*.json"))
    if not result_files:
        print("No result files found in bench/results/. Run scripts/run_all_benches.sh first.")
        return

    print(f"Found {len(result_files)} result files:")
    for f in result_files:
        print(f"  {f.name}")

    # placeholder: actual plot generation implemented per-phase
    print("\nPlot generation: available after Phase 1 benchmarks are committed.")
    print(f"Plots will be written to: {PLOTS_DIR}")


if __name__ == "__main__":
    main()
