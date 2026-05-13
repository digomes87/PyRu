# crypto-features-bench

A reproducible, honest comparison of Python and Rust across a five-stage crypto feature engineering pipeline.

**Status: in progress** — Phase 0 (Bootstrap) complete.

---

## What this is

This project implements a high-throughput crypto trade feature pipeline in both Python and Rust, then measures each at every stage: ingestion, feature computation, storage, query, and online serving. The goal is not to "prove Rust wins" — it is to produce honest, reproducible evidence of where each language earns its keep in a modern data-engineering workload.

Results, code, and methodology are all public. Surprising negatives are called out explicitly. This is an engineering trade-off study, not a language advocacy piece.

---

## Stages (results coming as each phase lands)

| Stage            | Python result | Rust result | Verdict |
|------------------|--------------|-------------|---------|
| 1. Ingestion     | TBD          | TBD         | TBD     |
| 2. Features      | TBD          | TBD         | TBD     |
| 3. Storage       | TBD          | TBD         | TBD     |
| 4. Query         | TBD          | TBD         | TBD     |
| 5. Serving       | TBD          | TBD         | TBD     |

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
