# crypto-features-bench

A reproducible, honest comparison of Python and Rust across a five-stage crypto feature engineering pipeline.

**Status: in progress** — Phase 0 (Bootstrap) complete.

---

## What this is

This project implements a high-throughput crypto trade feature pipeline in both Python and Rust, then measures each at every stage: ingestion, feature computation, storage, query, and online serving. The goal is not to "prove Rust wins" — it is to produce honest, reproducible evidence of where each language earns its keep in a modern data-engineering workload.

Results, code, and methodology are all public. Surprising negatives are called out explicitly. This is an engineering trade-off study, not a language advocacy piece.

---

## TL;DR results (updated per phase)

| Stage        | Python                        | Rust                           | Verdict                                  |
|--------------|-------------------------------|--------------------------------|------------------------------------------|
| 1. Ingestion | batch 5.2M ev/s, Arrow 10.6M/s | batch 15.8M ev/s, Arrow 53.8M/s | Rust 3–5× faster; Python sufficient <1M ev/s |
| 2. Features  | Polars 7.3M/s, Numba 34.3M/s  | hand-rolled 16.9M/s             | Numba > Rust > Polars-Py (all within 5×)  |
| 3. Storage   | write 3.0M/s; Polars read 47.6M/s | write 4.3M/s; read 19.2M/s  | Rust faster writes; Polars-Py fastest reads |
| 4. Query     | Polars 3–7ms; DuckDB 6–11ms   | TBD (DataFusion)               | Polars-Py wins all 5 queries vs DuckDB    |
| 5. Serving   | TBD                           | TBD                            | TBD                                      |

## Stage 1 — Ingestion

![Ingestion throughput](bench/plots/ingest_throughput.png)

**Batching pipeline (10k trades, batch=1000):**
- Python asyncio: **5.2M events/sec**
- Rust tokio: **15.8M events/sec** — 3.1× faster

**Arrow batch conversion (10k trades):**
- Python pyarrow: **10.6M trades/sec**
- Rust arrow-rs: **53.8M trades/sec** — 5.1× faster

**Honest takeaway:** For most workloads under ~500k events/sec, the Python asyncio pipeline is entirely adequate. The Rust advantage shows up at very high throughputs or when you need predictable tail latency — asyncio's event loop overhead is invisible at human scale but not at 10M+ events/sec. JSON parsing dominates `from_file` on both sides; switching to a binary format (Arrow IPC or msgpack) would equalize them.

---

## Stage 2 — Feature Computation

![Five-way feature comparison](bench/plots/features_five_way.png)

**At 100k trades:**
| Implementation       | Throughput   | Notes                              |
|----------------------|--------------|------------------------------------|
| pandas naive         | ~0.05M/s     | O(n²) — only useful as a baseline  |
| Polars (Python)      | 7.3M/s       |                                    |
| numpy + numba (JIT)  | **34.3M/s**  | Outperforms everything             |
| hand-rolled Rust     | 16.9M/s      | 2.3× Polars-Py                     |

**Honest takeaway:** Python+Numba beats hand-rolled Rust by 2× on this workload. The LLVM JIT generates more aggressively optimized inner loops for this tight sliding-window pattern than rustc does at default settings. The lesson: "Rust" is not automatically faster than "Python" — the algorithm, data layout, and compiler backend all matter.

## Stage 4 — Query Engine

![Query matrix](bench/plots/query_matrix.png)

| Query | Polars (Python) | DuckDB (Python) | Winner |
|-------|-----------------|-----------------|--------|
| Q1 VWAP/min | 4.9ms | 9.1ms | Polars 1.9× |
| Q2 Top symbols | 7.0ms | 10.5ms | Polars 1.5× |
| Q3 RV distribution | 7.0ms | 9.9ms | Polars 1.4× |
| Q4 Point lookup | **3.4ms** | 6.5ms | Polars 1.9× |
| Q5 OFI momentum | 4.8ms | 9.1ms | Polars 1.9× |

**Honest takeaway:** Polars beats DuckDB on all five queries at this dataset size. This is somewhat surprising because DuckDB's optimizer is excellent for complex SQL. The explanation: these queries are simple aggregations over Parquet that Polars' lazy engine handles via tight SIMD inner loops with minimal overhead. DuckDB's advantage emerges at larger datasets (billions of rows), complex multi-table joins, and window functions where its query planner's sophistication matters more. At 100k–10M rows with simple groupby patterns, Polars lazy is hard to beat from Python.

---

## Stage 3 — Storage

![Storage throughput](bench/plots/storage_write_throughput.png)

**Write (100k rows, Snappy Parquet, hive-partitioned):**
- Python (pyarrow): 3.0M rows/s
- Rust (arrow-rs): **4.3M rows/s** — 1.4× faster

**Read (100k rows, full scan):**
- PyArrow dataset API: 10.8M rows/s
- Rust (arrow-rs): 19.2M rows/s
- Polars lazy (Python): **47.6M rows/s** — fastest of all three

**Honest takeaway:** Rust wins on writes, but Polars lazy scan from Python outperforms both Rust arrow-rs and PyArrow dataset by 2–4×. The reason: Polars' parquet reader aggressively applies column projection and page-level skipping that the lower-level APIs don't activate by default. Cross-language compatibility confirmed — Python-written files are readable by Rust and vice versa.

---

## Reproducing the results

```bash
# Download data
bash scripts/download_data.sh

# Run all benchmarks
bash scripts/run_all_benches.sh

# Generate plots
python bench/analyze.py
```

Hardware spec, software versions, and step-by-step instructions will be documented here as each phase completes.

---

## License

MIT — see [LICENSE](LICENSE).
