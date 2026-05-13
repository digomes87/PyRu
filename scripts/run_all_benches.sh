#!/usr/bin/env bash
# Run the full benchmark suite for both Python and Rust.
# Results are written to bench/results/ with git SHA suffixes.
# Run from the repo root.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
SHA=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
RESULTS="$REPO_ROOT/bench/results"
mkdir -p "$RESULTS"

echo "=== crypto-features-bench full benchmark run ==="
echo "Git SHA: $SHA"
echo "Results: $RESULTS"
echo ""

# --- Python ---
echo "[1/2] Running Python benchmarks..."
cd "$REPO_ROOT/python"
uv run pytest benches/ \
    --benchmark-json="$RESULTS/python_${SHA}.json" \
    -v --benchmark-disable-gc

# --- Rust ---
echo "[2/2] Running Rust benchmarks..."
cd "$REPO_ROOT/rust"
cargo bench -- --output-format criterion \
    2>"$RESULTS/rust_${SHA}.stderr" \
    | tee "$RESULTS/rust_${SHA}.stdout"

echo ""
echo "Benchmark run complete. Generate plots with: python bench/analyze.py"
