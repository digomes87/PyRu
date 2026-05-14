/// Reference streaming implementation for the feature pipeline.
///
/// Maintains a monotonic deque per symbol; amortized O(1) per event.
/// This is the conformance reference — matches the Python compute_stream exactly.
use std::collections::{HashMap, VecDeque};

use cfb_ingest::Trade;

use crate::FeatureRow;

const NS_PER_S: i64 = 1_000_000_000;
const WINDOW_1M: i64 = 60 * NS_PER_S;
const WINDOW_5M: i64 = 5 * 60 * NS_PER_S;
const WINDOW_15M: i64 = 15 * 60 * NS_PER_S;
const LATE_THRESHOLD: i64 = 2 * NS_PER_S;

#[derive(Default)]
struct SymbolState {
    buf: VecDeque<Trade>,
    watermark: i64,
    late_drops: u64,
}

impl SymbolState {
    fn push(&mut self, trade: Trade) -> Option<FeatureRow> {
        if trade.ts < self.watermark - LATE_THRESHOLD {
            self.late_drops += 1;
            return None;
        }
        self.watermark = self.watermark.max(trade.ts);
        self.buf.push_back(trade);

        let ts = self.buf.back().unwrap().ts;

        let w1m: Vec<&Trade> = self.buf.iter().filter(|t| t.ts >= ts - WINDOW_1M).collect();
        let w5m: Vec<&Trade> = self.buf.iter().filter(|t| t.ts >= ts - WINDOW_5M).collect();
        let w15m: Vec<&Trade> = self
            .buf
            .iter()
            .filter(|t| t.ts >= ts - WINDOW_15M)
            .collect();

        let row = FeatureRow {
            ts,
            symbol: self.buf.back().unwrap().symbol.clone(),
            vwap_1m: vwap(&w1m),
            vwap_5m: vwap(&w5m),
            vwap_15m: vwap(&w15m),
            rv_1m: rv(&w1m),
            rv_5m: rv(&w5m),
            ofi_1m: ofi(&w1m),
            microprice: None,
            trade_count_1m: w1m.len() as i64,
        };

        let cutoff = self.watermark - WINDOW_15M;
        while self.buf.front().map(|t| t.ts < cutoff).unwrap_or(false) {
            self.buf.pop_front();
        }

        Some(row)
    }
}

fn vwap(trades: &[&Trade]) -> Option<f64> {
    let pv: f64 = trades.iter().map(|t| t.price * t.qty).sum();
    let v: f64 = trades.iter().map(|t| t.qty).sum();
    if v == 0.0 {
        None
    } else {
        Some(pv / v)
    }
}

fn rv(trades: &[&Trade]) -> f64 {
    if trades.len() < 2 {
        return 0.0;
    }
    trades
        .windows(2)
        .map(|w| {
            let (p0, p1) = (w[0].price, w[1].price);
            if p0 > 0.0 && p1 > 0.0 {
                let r = (p1 / p0).ln();
                r * r
            } else {
                0.0
            }
        })
        .sum()
}

fn ofi(trades: &[&Trade]) -> f64 {
    trades.iter().map(|t| t.signed_qty()).sum()
}

pub fn compute_stream(events: &[Trade]) -> Vec<FeatureRow> {
    let mut state: HashMap<String, SymbolState> = HashMap::new();
    let mut rows = Vec::with_capacity(events.len());

    for e in events {
        let sym_state = state.entry(e.symbol.clone()).or_default();
        if let Some(row) = sym_state.push(e.clone()) {
            rows.push(row);
        }
    }

    rows
}
