# Contributing

Contributions are welcome. This project is a benchmark, so the bar for a contribution is: does it improve the **honesty**, **reproducibility**, or **coverage** of the results?

## What we're looking for

- **Additional language implementations** — a Go or Java serving layer would make the comparison richer.
- **Bug fixes** — if a benchmark is measuring the wrong thing, that's a bug.
- **New conformance fixtures** — edge cases the current five cases don't cover (late events beyond 2s, zero-qty trades, etc.).
- **Larger dataset runs** — if you have access to a bigger machine or the actual Kaggle data, committed result files are valuable.
- **Methodology improvements** — cold vs warm cache separation, thread-pinning, turbo-boost documentation.

## What we're not looking for

- Micro-optimisations to one side that make the comparison unfair.
- New features that aren't benchmarked and don't appear in the functional spec.
- Benchmark results without the corresponding code change.

## Workflow

1. Fork and clone the repository.
2. Create a branch: `feat/your-thing` or `fix/your-thing`.
3. Follow the commit style in `docs/commit-style.md` (short, imperative, conventional commits).
4. Add or update conformance tests if the change touches the pipeline contract.
5. Commit benchmark results alongside the code change.
6. Open a PR; the description should explain what changed and why the numbers moved.

## Development setup

```bash
# Python
cd python
uv sync --all-groups
uv run pytest tests/ -v

# Rust
cd rust
cargo test --all
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## Benchmark discipline

- Run benchmarks on a quiet machine (no background load).
- Use the same hardware for Python and Rust comparisons.
- Always commit both the before and after result files when a change affects performance.
- If a result is anomalous, investigate before committing it.

## Code style

- Python: `ruff` for linting, `mypy` (strict) for types.
- Rust: `rustfmt` + `clippy -D warnings`.
- Commit messages: Conventional Commits in English.

## Questions

Open a GitHub issue. Tag it `question`.
