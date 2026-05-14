use std::hint::black_box;

use cfb_features::{arrow_impl::compute_arrow, compute_stream};
use cfb_ingest::synth::make_trades;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn bench_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_reference");

    for n in [1_000usize, 10_000] {
        let trades = make_trades(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &trades, |b, t| {
            b.iter(|| compute_stream(black_box(t)));
        });
    }

    group.finish();
}

fn bench_arrow_handrolled(c: &mut Criterion) {
    let mut group = c.benchmark_group("hand_rolled_arrow");

    for n in [1_000usize, 10_000, 100_000] {
        let trades = make_trades(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &trades, |b, t| {
            b.iter(|| compute_arrow(black_box(t)));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_streaming, bench_arrow_handrolled);
criterion_main!(benches);
