.PHONY: all bench bench-python bench-rust test test-python test-rust lint plots clean

SHA := $(shell git rev-parse --short HEAD)
RESULTS := bench/results
PYTHON  := cd python && uv run

# ── Default ──────────────────────────────────────────────────────────────────
all: test bench plots

# ── Tests ────────────────────────────────────────────────────────────────────
test: test-python test-rust

test-python:
	$(PYTHON) pytest tests/ -v

test-rust:
	cd rust && cargo test --all

# ── Benchmarks ───────────────────────────────────────────────────────────────
bench: bench-python bench-rust plots

bench-python:
	$(PYTHON) pytest benches/bench_ingest.py \
	    --benchmark-json=../$(RESULTS)/ingest_python_$(SHA).json \
	    --benchmark-min-rounds=5 -q
	$(PYTHON) pytest benches/bench_features.py \
	    --benchmark-json=../$(RESULTS)/features_python_$(SHA).json \
	    --benchmark-min-rounds=3 -q
	$(PYTHON) pytest benches/bench_storage.py \
	    --benchmark-json=../$(RESULTS)/storage_python_$(SHA).json \
	    --benchmark-min-rounds=3 -q
	$(PYTHON) pytest benches/bench_query.py \
	    --benchmark-json=../$(RESULTS)/query_python_$(SHA).json \
	    --benchmark-min-rounds=5 -q
	$(PYTHON) pytest benches/bench_serve.py \
	    --benchmark-json=../$(RESULTS)/serve_python_$(SHA).json \
	    --benchmark-min-rounds=5 -q

bench-rust:
	cd rust && cargo bench --package cfb-ingest  2>&1 | tee ../$(RESULTS)/ingest_rust_$(SHA).txt
	cd rust && cargo bench --package cfb-features 2>&1 | tee ../$(RESULTS)/features_rust_$(SHA).txt
	cd rust && cargo bench --package cfb-storage  2>&1 | tee ../$(RESULTS)/storage_rust_$(SHA).txt
	cd rust && cargo bench --package cfb-serve    2>&1 | tee ../$(RESULTS)/serve_rust_$(SHA).txt

# ── Analysis ─────────────────────────────────────────────────────────────────
plots:
	$(PYTHON) python ../bench/analyze.py

# ── Lint ─────────────────────────────────────────────────────────────────────
lint:
	$(PYTHON) ruff check src/ tests/ benches/
	$(PYTHON) mypy src/cfb/
	cd rust && cargo fmt --all -- --check
	cd rust && cargo clippy --all-targets -- -D warnings

# ── Cleanup ──────────────────────────────────────────────────────────────────
clean:
	cd rust && cargo clean
	find python -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	find python -name '.pytest_cache' -exec rm -rf {} + 2>/dev/null || true

help:
	@echo "make test          — run all tests"
	@echo "make bench         — run all benchmarks + generate plots"
	@echo "make bench-python  — Python benchmarks only"
	@echo "make bench-rust    — Rust benchmarks only"
	@echo "make plots         — regenerate plots from existing results"
	@echo "make lint          — lint + typecheck both stacks"
	@echo "make clean         — remove build artefacts"
