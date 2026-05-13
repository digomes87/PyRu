# Development Journal

## 2026-05-13 — Phase 0, Bootstrap

- Hours: 2.0
- Done:
  - Initialized repository with MIT license
  - Created directory scaffold
  - Set up Python project with uv and all core dependencies
  - Set up Rust workspace with five crates
  - Added CI workflows for both stacks
  - Wrote functional contract and feature math specs
  - Created 5 hand-computed conformance fixtures
- Surprises: none yet — this is setup work
- Decisions:
  - Python managed via `uv` for reproducible envs
  - Rust edition 2021, MSRV 1.75
  - Polars chosen as the primary feature engine on both sides (apples-to-apples)
  - DuckDB + DataFusion added for query-layer comparison
- TODO next: Phase 1 — implement ingestion on both sides, benchmark throughput and latency
