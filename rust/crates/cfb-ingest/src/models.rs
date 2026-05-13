use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub ts: i64,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
}

impl Trade {
    pub fn signed_qty(&self) -> f64 {
        match self.side {
            Side::Buy => self.qty,
            Side::Sell => -self.qty,
        }
    }
}
