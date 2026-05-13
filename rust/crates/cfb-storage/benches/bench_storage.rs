use std::hint::black_box;
use std::time::Instant;

use cfb_storage::{build_feature_batch, read_partitioned, write_partitioned};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;

const NS_PER_S: i64 = 1_000_000_000;
const BASE_TS: i64 = 1_700_000_000 * NS_PER_S;

fn make_batch(n: usize) -> arrow_array::RecordBatch {
    build_feature_batch(
        (0..n).map(|i| BASE_TS + i as i64 * NS_PER_S).collect(),
        (0..n).map(|_| "BTCUSDT").collect(),
        (0..n).map(|i| Some(30_000.0 + i as f64 * 0.1)).collect(),
        (0..n).map(|_| Some(30_000.0)).collect(),
        (0..n).map(|_| Some(30_000.0)).collect(),
        vec![0.0; n],
        vec![0.0; n],
        vec![1.0; n],
        vec![None; n],
        vec![1i64; n],
    )
}

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_write");

    for n in [10_000usize, 100_000] {
        let batch = make_batch(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::from_parameter(n), &batch, |b, batch| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let tmp = TempDir::new().unwrap();
                    let t0 = Instant::now();
                    write_partitioned(black_box(batch), tmp.path(), 0).unwrap();
                    total += t0.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_read");
    let n = 100_000usize;
    let batch = make_batch(n);
    let tmp = TempDir::new().unwrap();
    write_partitioned(&batch, tmp.path(), 0).unwrap();

    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("full_scan_100k", |b| {
        b.iter(|| {
            let batches = read_partitioned(black_box(tmp.path()), Some("BTCUSDT")).unwrap();
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_write, bench_read);
criterion_main!(benches);
