"""Analysis script: turns bench/results/ into plots in bench/plots/.

Run from the repo root:
    python bench/analyze.py

Requires: polars, matplotlib (installed via the python/ project).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parents[1]
RESULTS_DIR = REPO_ROOT / "bench" / "results"
PLOTS_DIR = REPO_ROOT / "bench" / "plots"


def _parse_rust_txt(path: Path) -> dict[str, float]:
    """Parse Criterion plain-text output into {bench_name: mean_us}."""
    results: dict[str, float] = {}
    current: str | None = None
    for line in path.read_text().splitlines():
        line = line.strip()
        # section header: "bench_group/bench_name/param"
        if line and not line.startswith("Benchmark") and "/" in line and not line.startswith("time"):
            current = line
        if "time:" in line and current:
            # Criterion: "time:   [X µs Y µs Z µs]" — take middle (mean)
            nums = re.findall(r"[\d.]+\s*(?:µs|ms|ns|s)", line)
            if len(nums) >= 2:
                raw = nums[1]
                val, unit = re.match(r"([\d.]+)\s*(.+)", raw).groups()
                v = float(val)
                if unit == "ns":
                    v /= 1000.0
                elif unit == "ms":
                    v *= 1000.0
                elif unit == "s":
                    v *= 1_000_000.0
                results[current] = v
    return results


def _parse_python_json(path: Path) -> dict[str, float]:
    """Parse pytest-benchmark JSON into {bench_name: mean_us}."""
    data = json.loads(path.read_text())
    results: dict[str, float] = {}
    for bench in data.get("benchmarks", []):
        name = bench["name"]
        mean_s = bench["stats"]["mean"]
        results[name] = mean_s * 1_000_000  # → µs
    return results


def plot_ingest_comparison() -> None:
    """Generate ingestion throughput bar chart (events/sec, log scale)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib not available — skipping plots")
        return

    py_files = sorted(RESULTS_DIR.glob("ingest_python_*.json"))
    rs_files = sorted(RESULTS_DIR.glob("ingest_rust_*.txt"))

    if not py_files or not rs_files:
        print("Need both Python and Rust results. Run run_all_benches.sh first.")
        return

    py = _parse_python_json(py_files[-1])
    rs = _parse_rust_txt(rs_files[-1])

    # ── events/sec from batch pipeline ──────────────────────────────────────
    # Python: test_batched_throughput_size1k processes 10k trades
    # Rust:   batching_pipeline/batch_size_1000/10000 processes 10k trades
    py_batch_us = py.get("test_batched_throughput_size1k", None)
    rs_batch_us = rs.get("batching_pipeline/batch_size_1000/10000", None)

    py_arrow_1k_us = py.get("test_to_arrow_1k", None)
    py_arrow_10k_us = py.get("test_to_arrow_10k", None)
    rs_arrow_1k_us = rs.get("arrow_conversion/build_batch_from/1000", None)
    rs_arrow_10k_us = rs.get("arrow_conversion/build_batch_from/10000", None)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Ingestion — Python vs Rust", fontsize=14, fontweight="bold")

    # --- Batching throughput ---
    ax = axes[0]
    if py_batch_us and rs_batch_us:
        n_trades = 10_000
        py_eps = n_trades / (py_batch_us / 1e6)
        rs_eps = n_trades / (rs_batch_us / 1e6)
        bars = ax.bar(
            ["Python\n(asyncio)", "Rust\n(tokio)"],
            [py_eps / 1e6, rs_eps / 1e6],
            color=["#0072B2", "#D55E00"],
            width=0.5,
        )
        ax.set_ylabel("Events / sec (millions)")
        ax.set_title("Batching pipeline — 10k trades, batch=1000")
        for bar, v in zip(bars, [py_eps, rs_eps]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.02,
                f"{v/1e6:.1f}M/s",
                ha="center",
                fontsize=10,
                fontweight="bold",
            )
        speedup = rs_eps / py_eps
        ax.text(
            0.97, 0.97,
            f"Rust {speedup:.1f}× faster",
            transform=ax.transAxes,
            ha="right", va="top",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow"),
        )
    ax.set_ylim(bottom=0)

    # --- Arrow conversion ---
    ax = axes[1]
    if all([py_arrow_1k_us, py_arrow_10k_us, rs_arrow_1k_us, rs_arrow_10k_us]):
        sizes = ["1k trades", "10k trades"]
        py_eps_list = [1_000 / (py_arrow_1k_us / 1e6), 10_000 / (py_arrow_10k_us / 1e6)]
        rs_eps_list = [1_000 / (rs_arrow_1k_us / 1e6), 10_000 / (rs_arrow_10k_us / 1e6)]
        x = range(len(sizes))
        w = 0.35
        ax.bar([xi - w / 2 for xi in x], [v / 1e6 for v in py_eps_list], w, label="Python", color="#0072B2")
        ax.bar([xi + w / 2 for xi in x], [v / 1e6 for v in rs_eps_list], w, label="Rust", color="#D55E00")
        ax.set_xticks(list(x))
        ax.set_xticklabels(sizes)
        ax.set_ylabel("Trades converted / sec (millions)")
        ax.set_title("Arrow conversion throughput")
        ax.legend()
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = PLOTS_DIR / "ingest_throughput.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def print_summary() -> None:
    py_files = sorted(RESULTS_DIR.glob("ingest_python_*.json"))
    rs_files = sorted(RESULTS_DIR.glob("ingest_rust_*.txt"))

    print("\n=== Ingestion benchmark summary ===\n")

    if py_files:
        py = _parse_python_json(py_files[-1])
        print(f"Python results: {py_files[-1].name}")
        for k, v in sorted(py.items()):
            print(f"  {k:55s} {v:10.1f} µs")

    if rs_files:
        rs = _parse_rust_txt(rs_files[-1])
        print(f"\nRust results: {rs_files[-1].name}")
        for k, v in sorted(rs.items()):
            print(f"  {k:55s} {v:10.1f} µs")

    if py_files and rs_files:
        py = _parse_python_json(py_files[-1])
        rs = _parse_rust_txt(rs_files[-1])

        py_batch = py.get("test_batched_throughput_size1k")
        rs_batch = rs.get("batching_pipeline/batch_size_1000/10000")
        if py_batch and rs_batch:
            py_eps = 10_000 / (py_batch / 1e6)
            rs_eps = 10_000 / (rs_batch / 1e6)
            print(f"\n  Batch throughput: Python {py_eps/1e6:.2f}M/s  Rust {rs_eps/1e6:.2f}M/s  (Rust {rs_eps/py_eps:.1f}×)")

        py_arr = py.get("test_to_arrow_10k")
        rs_arr = rs.get("arrow_conversion/build_batch_from/10000")
        if py_arr and rs_arr:
            py_eps = 10_000 / (py_arr / 1e6)
            rs_eps = 10_000 / (rs_arr / 1e6)
            print(f"  Arrow 10k:        Python {py_eps/1e6:.2f}M/s  Rust {rs_eps/1e6:.2f}M/s  (Rust {rs_eps/py_eps:.1f}×)")


def plot_features_five_way() -> None:
    """Generate the headline five-way feature computation comparison chart."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return

    py_files = sorted(RESULTS_DIR.glob("features_python_*.json"))
    rs_files = sorted(RESULTS_DIR.glob("features_rust_*.txt"))
    if not py_files or not rs_files:
        print("Need both Python and Rust feature results.")
        return

    py = _parse_python_json(py_files[-1])
    rs = _parse_rust_txt(rs_files[-1])

    # All at 100k trades
    # Python: bench name → µs for 100k trades
    # Streaming: 10k×10 extrapolation (since 100k would take too long)
    N = 100_000

    polars_py_us = py.get("test_polars_100k")
    numba_us = py.get("test_numba_100k")
    # pandas is O(n²), skip 100k; use 1k for illustrative rate
    pandas_us_1k = py.get("test_pandas_1k")

    # Rust at 100k
    rust_hand_us = rs.get("hand_rolled_arrow/100000")

    fig, ax = plt.subplots(figsize=(10, 6))

    labels, eps_list, colors = [], [], []

    if pandas_us_1k:
        pandas_eps = 1_000 / (pandas_us_1k / 1e6)
        labels.append("pandas\n(naive, 1k)")
        eps_list.append(pandas_eps / 1e6)
        colors.append("#CC79A7")

    if polars_py_us:
        polars_py_eps = N / (polars_py_us / 1e6)
        labels.append("Polars\n(Python, 100k)")
        eps_list.append(polars_py_eps / 1e6)
        colors.append("#0072B2")

    if numba_us:
        numba_eps = N / (numba_us / 1e6)
        labels.append("numpy+numba\n(Python, 100k)")
        eps_list.append(numba_eps / 1e6)
        colors.append("#009E73")

    if rust_hand_us:
        rust_eps = N / (rust_hand_us / 1e6)
        labels.append("hand-rolled\n(Rust, 100k)")
        eps_list.append(rust_eps / 1e6)
        colors.append("#D55E00")

    bars = ax.bar(labels, eps_list, color=colors, width=0.6)

    for bar, v in zip(bars, eps_list):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f"{v:.1f}M/s",
            ha="center", fontsize=10, fontweight="bold",
        )

    ax.set_ylabel("Trades / second (millions)")
    ax.set_title(
        "Feature computation throughput — five implementations\n"
        "(Surprising: numba outperforms hand-rolled Rust at 100k scale)",
        fontsize=11,
    )
    ax.set_ylim(bottom=0)
    plt.tight_layout()

    out = PLOTS_DIR / "features_five_way.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def main() -> None:
    PLOTS_DIR.mkdir(exist_ok=True)
    print_summary()
    plot_ingest_comparison()
    plot_features_five_way()


if __name__ == "__main__":
    main()
