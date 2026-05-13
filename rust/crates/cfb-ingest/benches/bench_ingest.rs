use std::hint::black_box;

use cfb_ingest::{
    batch::{build_batch_from, BatchConfig},
    source::from_iter,
    synth::make_trades,
    Batcher,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

fn bench_arrow_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_conversion");

    for n in [1_000usize, 10_000, 100_000] {
        let trades = make_trades(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("build_batch_from", n), &trades, |b, t| {
            b.iter(|| build_batch_from(black_box(t)));
        });
    }

    group.finish();
}

fn bench_batching_pipeline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("batching_pipeline");

    for n in [10_000usize, 100_000] {
        let trades = make_trades(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("batch_size_1000", n),
            &trades,
            |b, t| {
                b.iter(|| {
                    let cfg = BatchConfig {
                        max_size: 1_000,
                        max_latency: std::time::Duration::from_secs(60),
                        channel_capacity: 64,
                    };
                    rt.block_on(async {
                        let source = from_iter(black_box(t.clone()));
                        let (_, mut rx) = Batcher::spawn(source, cfg);
                        let mut total = 0usize;
                        while let Some(batch) = rx.recv().await {
                            total += batch.num_rows();
                            if batch.num_rows() == 0 {
                                break;
                            }
                        }
                        total
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_arrow_conversion, bench_batching_pipeline);
criterion_main!(benches);
