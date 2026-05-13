/// Synthetic trade generator for benchmarks and integration tests.
use crate::{models::Side, Trade};

const BASE_TS: i64 = 1_700_000_000 * 1_000_000_000;

pub fn make_trades(n: usize) -> Vec<Trade> {
    (0..n)
        .map(|i| Trade {
            ts: BASE_TS + i as i64 * 100_000_000,
            symbol: "BTCUSDT".into(),
            price: 30_000.0 + (i % 500) as f64 * 0.1,
            qty: 0.1 + (i % 10) as f64 * 0.05,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
        })
        .collect()
}
