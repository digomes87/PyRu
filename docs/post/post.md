# Python vs Rust Across a Five-Stage Crypto Feature Pipeline: An Honest Benchmark

> **TL;DR:** I built the same crypto trade feature pipeline twice — once in Python, once in Rust — and measured both at every stage. Rust wins on ingestion, storage writes, and serving latency. Python wins on feature computation (via Numba) and read throughput (via Polars). DuckDB loses to Polars on simple queries. The results are reproducible and sometimes counterintuitive.

---

## Why I built this

Every "Python vs Rust" benchmark I've read is either:
1. Toy examples that prove the author's preferred conclusion, or
2. A single stage (usually a tight CPU loop) that doesn't reflect real workloads.

I wanted something different: a realistic data pipeline with five distinct stages, measured with the same rigor I'd apply to production work, with the surprising findings documented honestly. If Rust loses somewhere, I say so. If Python wins with a different library, that counts.

The domain is crypto feature engineering — a workload that combines I/O-heavy ingestion, CPU-heavy rolling computations, analytical reads, and latency-sensitive serving. It exercises the full stack of a data engineer's concerns.

---

## What I built

A feature pipeline that consumes a stream of trade events:

```json
{"ts": 1700000000000000000, "symbol": "BTCUSDT", "price": 30000.0, "qty": 1.0, "side": "buy"}
```

And emits, for every event, a feature row:

```
vwap_1m, vwap_5m, vwap_15m   — rolling VWAP over 1/5/15 minute windows
rv_1m, rv_5m                  — realized volatility (sum of squared log returns)
ofi_1m                        — order flow imbalance (signed net volume)
trade_count_1m                — trades in last minute
```

Both implementations must pass a conformance test suite before any benchmarking starts. Numbers from a non-conforming implementation don't count.

**Five stages:** Ingestion → Feature computation → Storage → Query → Serving.

**Hardware:** Apple M-series (arm64), 16 GB RAM. Single-core benchmarks unless noted.

**Stack:**
- Python 3.12, Polars 1.40, DuckDB 1.5, pyarrow 24, numba 0.65, FastAPI 0.136
- Rust 1.75 (edition 2021), tokio 1, arrow-rs 53, polars 0.43, axum 0.7

---

## Stage 1: Ingestion

**What I measured:** throughput of parsing trade events into Arrow RecordBatches, and the batching pipeline latency.

| Metric | Python (asyncio) | Rust (tokio) |
|--------|-----------------|--------------|
| Batch pipeline (10k trades) | 5.2M events/s | 15.8M events/s |
| Arrow conversion (10k trades) | 10.6M rows/s | 53.8M rows/s |

**Rust wins by 3–5×.** The gap is driven by two things: Python's asyncio event loop overhead between yield points, and pyarrow's Python-layer type coercion when constructing arrays from Python lists. Arrow-rs constructs arrays directly from typed Rust iterators with zero copies.

**Honest nuance:** For most real ingestion workloads — Kafka, WebSocket, file replay — the bottleneck is I/O, not CPU. At 5.2M events/second, Python's asyncio pipeline can absorb any exchange feed I've worked with. The Rust advantage matters when you're aggregating across hundreds of symbols simultaneously or operating at colocation-level latencies.

---

## Stage 2: Feature Computation

This is the most interesting stage, with the most surprising result.

I implemented the rolling VWAP, RV, and OFI computations five ways:

1. **Streaming reference (Python)** — simple deque, O(n²) per 10k events
2. **Pandas naive** — vectorized but O(n²) due to rolling window scan
3. **Polars lazy (Python)** — `rolling_sum_by` expressions
4. **numpy + Numba JIT** — hand-tuned sliding window sums with LLVM JIT
5. **Hand-rolled Rust (Arrow arrays)** — same algorithm as Numba, native code

**Results at 100k trades:**

| Implementation | Throughput |
|---------------|-----------|
| Streaming Python | ~1k/s |
| Pandas | ~50k/s |
| Polars Python | 7.3M/s |
| **numpy + Numba** | **34.3M/s** |
| Hand-rolled Rust | 16.9M/s |

**The finding I didn't expect:** Numba outperforms hand-rolled Rust by 2×.

This result stood up to repeated measurement. The explanation: the LLVM compiler that Numba uses at JIT time applies more aggressive auto-vectorization to the inner sliding-window loop than rustc does at standard optimization levels. The Numba kernel compiles to tight SIMD code that processes multiple array elements per clock cycle. Rust with `RUSTFLAGS="-C target-cpu=native"` or explicit SIMD intrinsics would close the gap — but that's not what you get out of the box.

The takeaway: "Rust is always faster than Python" is not true. The JIT compiler, the algorithm, and the data layout all matter more than the language.

**What Polars shows:** Polars Python at 7.3M/s is roughly 2.3× behind hand-rolled Rust. For most production feature pipelines — which operate on seconds-to-minutes batch cadences, not microsecond ticks — Polars Python is entirely adequate. You'd need to be computing features for thousands of symbols in real-time at tick frequency before Polars becomes a bottleneck.

---

## Stage 3: Storage

**What I measured:** write throughput to hive-partitioned Parquet, and read throughput for full scans and predicate-pushdown reads.

Partition layout: `symbol=X/date=D/hour=H/part-NNNN.parquet`

| Metric | Python (pyarrow) | Python (Polars lazy) | Rust (arrow-rs) |
|--------|-----------------|---------------------|-----------------|
| Write 100k rows | 3.0M rows/s | — | **4.3M rows/s** |
| Read scan 100k | 10.8M rows/s | **47.6M rows/s** | 19.2M rows/s |

**Rust wins on writes (1.4×).** The gap is modest because both are ultimately bottlenecked by the same Parquet encoding and Snappy compression pipeline.

**Python Polars wins on reads (47.6M rows/s, 2.5× faster than Rust arrow-rs).** This surprised me. Polars' parquet reader aggressively applies column projection pushdown — it skips pages for columns you don't read and uses SIMD-accelerated decoding for the ones you do. The raw arrow-rs `ParquetRecordBatchReaderBuilder` doesn't apply these optimizations automatically.

**Cross-language compatibility is real.** Files written by Python pyarrow are read correctly by Rust arrow-rs and vice versa. The Parquet spec is well-implemented enough that this just works.

---

## Stage 4: Query Engine

I compared DuckDB (embedded C++) versus Polars lazy frames on five analytical queries over 100k stored feature rows.

| Query | DuckDB | Polars | Winner |
|-------|--------|--------|--------|
| Q1: VWAP per minute | 9.1ms | 4.9ms | Polars 1.9× |
| Q2: Top symbols | 10.5ms | 7.0ms | Polars 1.5× |
| Q3: RV distribution | 9.9ms | 7.0ms | Polars 1.4× |
| Q4: Point lookup | 6.5ms | 3.4ms | Polars 1.9× |
| Q5: OFI momentum | 9.1ms | 4.8ms | Polars 1.9× |

**Polars wins all five.** This is counterintuitive — DuckDB is a serious analytical database with a sophisticated query optimizer, and it's written in C++. How does a Python library beat it?

At this dataset size (100k rows, simple aggregations), Polars' lazy frame executes the query plan through a single tight SIMD pass over the Parquet data. DuckDB must initialize its query engine, plan the query, and execute it through a more general pipeline. For complex multi-table joins, window functions, or billions of rows, DuckDB's optimizer would win. For these simple groupby patterns, Polars' directness beats DuckDB's sophistication.

**The important caveat:** this benchmark used 100k rows. At 100M+ rows, or with queries involving multiple joins and subqueries, DuckDB would likely dominate. Choose the tool for your actual data volume and query complexity.

---

## Stage 5: Serving

This is where Rust's advantage is largest and most practical.

**Hot path (cache hit):**
- Python: Polars DataFrame filter on 10k cached rows — **154 µs**
- Rust: HashMap lookup — **15 nanoseconds**
- Speedup: **10,000×**

**Cold path (cache miss → Parquet scan):**
- Python: DuckDB point query — **522 µs**
- Rust: arrow-rs Parquet scan — **~5ms** (Python wins here)

The hot-path gap is the most dramatic result in the entire project. It's not about Rust being "fast" — it's about choosing the right data structure. A HashMap lookup is O(1) with essentially zero overhead. A Polars DataFrame filter on 10k rows requires deserializing a column, applying a predicate mask, and materializing a result — correct, but orders of magnitude more work.

In practice, for HTTP serving, you'd add uvicorn/asyncio overhead (~100–200 µs) on the Python side and zero-copy axum deserialization on the Rust side. At P50, users won't notice. At P99/P999 under heavy load — when GC pauses and event-loop saturation kick in on Python — the Rust tail latency advantage becomes the difference between meeting your SLO and breaching it.

**The cold path inversion:** For cache misses, Python's DuckDB at 522 µs outperforms Rust's raw Parquet scan at ~5ms. DuckDB's query planner is better than a naïve arrow-rs scan. The production recommendation: use Rust for the hot path, DuckDB for cache-miss fallback.

---

## The five surprising findings

1. **Numba beats hand-rolled Rust** on the rolling window computation. The JIT compiler has access to runtime CPU information and generates tighter SIMD code than rustc at default settings.

2. **Polars lazy beats DuckDB** at 100k rows on simple aggregations. DuckDB's optimizer is a liability when the query fits in a single pass.

3. **Polars lazy (Python) reads Parquet faster than Rust arrow-rs** (47.6M vs 19.2M rows/s). Polars' parquet reader has more aggressive pushdown logic.

4. **Rust's hot-path serving advantage is 10,000×**, not 2–5× like most benchmarks claim. When you compare the right data structure (HashMap) to the right Python idiom (DataFrame filter), the gap is enormous.

5. **The cold path inverts** — Python DuckDB is 10× faster than Rust arrow-rs for point queries on Parquet. The right tool for cold lookups is still C++.

---

## When to choose what

| Scenario | Recommendation |
|----------|---------------|
| Ingestion < 1M events/sec | Python asyncio — adequate, simpler |
| Ingestion > 10M events/sec | Rust tokio — clear win |
| Feature computation, batch cadence > 1s | Polars Python — fast enough, ergonomic |
| Feature computation, tick-by-tick realtime | Numba or Rust — both work |
| Parquet writes | Rust — small but consistent edge |
| Parquet reads (analytical) | Polars Python — best pushdown |
| Simple SQL queries, < 100M rows | Polars LazyFrame — surprisingly fast |
| Complex SQL, > 100M rows, joins | DuckDB — optimizer matters at scale |
| Serving, hot path | Rust — HashMap vs DataFrame is no contest |
| Serving, cold fallback | DuckDB — its query planner earns its keep |

---

## Methodology and reproducibility

All benchmarks were run on the same machine with the same dataset. Each benchmark ran for at least 5 rounds (Python pytest-benchmark) or 100 Criterion samples (Rust). Results include warmup phases.

**To reproduce:**

```bash
git clone <repo>
cd crypto-features-bench
bash scripts/download_data.sh   # requires Kaggle CLI
bash scripts/run_all_benches.sh
python bench/analyze.py         # generates plots
```

All result JSON files are committed to `bench/results/`. Plots are in `bench/plots/`.

---

## Lines of code and hours

| Stage | Python LOC | Rust LOC | Py hours | Rust hours |
|-------|-----------|---------|---------|-----------|
| Ingestion | 120 | 195 | 3 | 5 |
| Features | 253 | 210 | 4 | 6 |
| Storage | 180 | 220 | 3 | 5 |
| Query | 195 | 110 | 3 | 4 |
| Serving | 100 | 140 | 2 | 4 |
| **Total** | **848** | **875** | **15** | **24** |

**The honest conclusion on developer cost:** Rust takes about 1.6× longer to write in this domain. For a production team, that cost needs to be justified by the performance benefit at your specific workload. At <1M events/sec, it isn't. At 10M+ events/sec or sub-millisecond serving SLOs, it clearly is.

---

## What I'd do differently

1. **Profile before optimizing.** I assumed Rust arrow-rs would outperform Polars on reads. It didn't. A profiler would have told me this before I wrote the code.

2. **Try PGO (Profile-Guided Optimization) on Rust.** The Numba result suggests there's headroom in the Rust hot loops that PGO could reclaim.

3. **Test at 1B+ rows.** Every claim in this post is bounded by dataset size. At billion-row scale, DuckDB and DataFusion would show their strength.

4. **Add Go** for the serving stage. Go's goroutines have much lower overhead than asyncio and would likely land between Python and Rust on tail latency — the interesting middle ground.

---

## License and contact

MIT. Code at the repository linked above. Issues and PRs welcome.
