use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const BASE_TS: i64 = 1_700_000_000_000_000_000i64;
const NS_PER_S: i64 = 1_000_000_000;

#[derive(Clone)]
#[allow(dead_code)]
struct FeatureRow {
    ts: i64,
    symbol: String,
    vwap_1m: Option<f64>,
    rv_1m: f64,
    ofi_1m: f64,
    trade_count_1m: i64,
}

fn make_cache(n: usize) -> HashMap<(String, i64), FeatureRow> {
    (0..n)
        .map(|i| {
            let ts = BASE_TS + i as i64 * NS_PER_S;
            let key = ("BTCUSDT".to_owned(), ts);
            let row = FeatureRow {
                ts,
                symbol: "BTCUSDT".to_owned(),
                vwap_1m: Some(30_000.0 + i as f64 * 0.01),
                rv_1m: (i % 100) as f64 * 1e-6,
                ofi_1m: (i % 20) as f64 - 10.0,
                trade_count_1m: (i % 50 + 1) as i64,
            };
            (key, row)
        })
        .collect()
}

fn bench_cache_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_lookup");

    for n in [1_000usize, 10_000, 100_000] {
        let cache = make_cache(n);
        // look up a row in the middle
        let ts_mid = BASE_TS + (n / 2) as i64 * NS_PER_S;
        let key = ("BTCUSDT".to_owned(), ts_mid);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("hashmap_hit", n), &key, |b, k| {
            b.iter(|| black_box(cache.get(black_box(k))).is_some());
        });

        let miss_key = ("BTCUSDT".to_owned(), 0i64);
        group.bench_with_input(BenchmarkId::new("hashmap_miss", n), &miss_key, |b, k| {
            b.iter(|| black_box(cache.get(black_box(k))).is_none());
        });
    }

    group.finish();
}

criterion_group!(benches, bench_cache_lookup);
criterion_main!(benches);
