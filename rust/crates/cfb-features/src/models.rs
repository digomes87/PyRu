use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureRow {
    pub ts: i64,
    pub symbol: String,
    pub vwap_1m: Option<f64>,
    pub vwap_5m: Option<f64>,
    pub vwap_15m: Option<f64>,
    pub rv_1m: f64,
    pub rv_5m: f64,
    pub ofi_1m: f64,
    pub microprice: Option<f64>,
    pub trade_count_1m: i64,
}
