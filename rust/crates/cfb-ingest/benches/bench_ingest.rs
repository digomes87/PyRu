use criterion::{criterion_group, criterion_main, Criterion};

fn bench_placeholder(_c: &mut Criterion) {
    // Phase 1 will implement real benchmarks here.
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
