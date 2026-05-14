/// Hand-rolled feature computation directly on Arrow primitive arrays.
///
/// Uses monotonic deques for O(n) total complexity. This is the performance ceiling —
/// no abstraction overhead from Polars or DataFusion.
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use cfb_ingest::Trade;

const NS_PER_S: i64 = 1_000_000_000;
const WINDOW_1M: i64 = 60 * NS_PER_S;
const WINDOW_5M: i64 = 5 * 60 * NS_PER_S;
const WINDOW_15M: i64 = 15 * 60 * NS_PER_S;

struct WindowState {
    buf: VecDeque<(i64, f64, f64, f64)>, // (ts, price, qty, signed_qty)
    pv_sum: f64,
    qty_sum: f64,
    ofi_sum: f64,
    prev_price: Option<f64>,
    sq_ret_buf: VecDeque<(i64, f64)>,
    sq_ret_sum: f64,
}

impl WindowState {
    fn new() -> Self {
        Self {
            buf: VecDeque::new(),
            pv_sum: 0.0,
            qty_sum: 0.0,
            ofi_sum: 0.0,
            prev_price: None,
            sq_ret_buf: VecDeque::new(),
            sq_ret_sum: 0.0,
        }
    }

    fn push_and_evict(&mut self, ts: i64, price: f64, qty: f64, signed_qty: f64, window: i64) {
        let sq_ret = match self.prev_price {
            Some(p) if p > 0.0 && price > 0.0 => {
                let r = (price / p).ln();
                r * r
            }
            _ => 0.0,
        };
        self.prev_price = Some(price);

        self.buf.push_back((ts, price, qty, signed_qty));
        self.pv_sum += price * qty;
        self.qty_sum += qty;
        self.ofi_sum += signed_qty;
        self.sq_ret_buf.push_back((ts, sq_ret));
        self.sq_ret_sum += sq_ret;

        let cutoff = ts - window;
        while self.buf.front().map(|e| e.0 < cutoff).unwrap_or(false) {
            let (_, p, q, sq) = self.buf.pop_front().unwrap();
            self.pv_sum -= p * q;
            self.qty_sum -= q;
            self.ofi_sum -= sq;
        }
        while self
            .sq_ret_buf
            .front()
            .map(|e| e.0 < cutoff)
            .unwrap_or(false)
        {
            let (_, sr) = self.sq_ret_buf.pop_front().unwrap();
            self.sq_ret_sum -= sr;
        }
    }

    fn vwap(&self) -> Option<f64> {
        if self.qty_sum == 0.0 {
            None
        } else {
            Some(self.pv_sum / self.qty_sum)
        }
    }

    fn rv(&self) -> f64 {
        self.sq_ret_sum.max(0.0)
    }

    fn ofi(&self) -> f64 {
        self.ofi_sum
    }

    fn count(&self) -> i64 {
        self.buf.len() as i64
    }
}

pub fn compute_arrow(trades: &[Trade]) -> RecordBatch {
    let n = trades.len();
    let mut ts_out = Vec::with_capacity(n);
    let mut symbol_out: Vec<String> = Vec::with_capacity(n);
    let mut vwap_1m_out: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut vwap_5m_out: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut vwap_15m_out: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut rv_1m_out: Vec<f64> = Vec::with_capacity(n);
    let mut rv_5m_out: Vec<f64> = Vec::with_capacity(n);
    let mut ofi_1m_out: Vec<f64> = Vec::with_capacity(n);
    let mut count_1m_out: Vec<i64> = Vec::with_capacity(n);

    let mut w1m = WindowState::new();
    let mut w5m = WindowState::new();
    let mut w15m = WindowState::new();

    for t in trades {
        let sq = t.signed_qty();
        w1m.push_and_evict(t.ts, t.price, t.qty, sq, WINDOW_1M);
        w5m.push_and_evict(t.ts, t.price, t.qty, sq, WINDOW_5M);
        w15m.push_and_evict(t.ts, t.price, t.qty, sq, WINDOW_15M);

        ts_out.push(t.ts);
        symbol_out.push(t.symbol.clone());
        vwap_1m_out.push(w1m.vwap());
        vwap_5m_out.push(w5m.vwap());
        vwap_15m_out.push(w15m.vwap());
        rv_1m_out.push(w1m.rv());
        rv_5m_out.push(w5m.rv());
        ofi_1m_out.push(w1m.ofi());
        count_1m_out.push(w1m.count());
    }

    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("vwap_1m", DataType::Float64, true),
        Field::new("vwap_5m", DataType::Float64, true),
        Field::new("vwap_15m", DataType::Float64, true),
        Field::new("rv_1m", DataType::Float64, false),
        Field::new("rv_5m", DataType::Float64, false),
        Field::new("ofi_1m", DataType::Float64, false),
        Field::new("microprice", DataType::Float64, true),
        Field::new("trade_count_1m", DataType::Int64, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ts_out)),
            Arc::new(StringArray::from_iter_values(
                symbol_out.iter().map(|s| s.as_str()),
            )),
            Arc::new(Float64Array::from(vwap_1m_out)),
            Arc::new(Float64Array::from(vwap_5m_out)),
            Arc::new(Float64Array::from(vwap_15m_out)),
            Arc::new(Float64Array::from(rv_1m_out)),
            Arc::new(Float64Array::from(rv_5m_out)),
            Arc::new(Float64Array::from(ofi_1m_out)),
            Arc::new(Float64Array::from(vec![Option::<f64>::None; n])),
            Arc::new(Int64Array::from(count_1m_out)),
        ],
    )
    .expect("schema matches")
}
