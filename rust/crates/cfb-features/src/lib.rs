pub mod models;
pub mod streaming;
pub mod polars_impl;
pub mod arrow_impl;

pub use models::FeatureRow;
pub use streaming::compute_stream;

#[cfg(test)]
mod tests {
    use super::*;
    use cfb_ingest::{Trade, models::Side};

    fn trade(ts: i64, price: f64, qty: f64, side: &str) -> Trade {
        Trade {
            ts,
            symbol: "BTCUSDT".into(),
            price,
            qty,
            side: if side == "buy" { Side::Buy } else { Side::Sell },
        }
    }

    const TOL: f64 = 1e-9;

    fn approx_eq(a: Option<f64>, b: Option<f64>) -> bool {
        match (a, b) {
            (None, None) => true,
            (Some(x), Some(y)) => (x - y).abs() < TOL,
            _ => false,
        }
    }

    #[test]
    fn conformance_case_01_single_trade() {
        let events = vec![trade(1700000000000000000, 30000.0, 1.0, "buy")];
        let rows = compute_stream(&events);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert!(approx_eq(r.vwap_1m, Some(30000.0)));
        assert!((r.rv_1m - 0.0).abs() < TOL);
        assert!((r.ofi_1m - 1.0).abs() < TOL);
        assert_eq!(r.trade_count_1m, 1);
    }

    #[test]
    fn conformance_case_02_same_price() {
        let events = vec![
            trade(1700000000000000000, 30000.0, 1.0, "buy"),
            trade(1700000010000000000, 30000.0, 3.0, "sell"),
        ];
        let rows = compute_stream(&events);
        assert_eq!(rows.len(), 2);
        assert!(approx_eq(rows[1].vwap_1m, Some(30000.0)));
        assert!((rows[1].rv_1m - 0.0).abs() < TOL);
        assert!((rows[1].ofi_1m - (-2.0)).abs() < TOL);
        assert_eq!(rows[1].trade_count_1m, 2);
    }

    #[test]
    fn conformance_case_03_rv_nonzero() {
        let events = vec![
            trade(1700000000000000000, 30000.0, 1.0, "buy"),
            trade(1700000010000000000, 30300.0, 2.0, "buy"),
            trade(1700000020000000000, 29900.0, 1.5, "sell"),
        ];
        let rows = compute_stream(&events);
        assert_eq!(rows.len(), 3);

        let expected_rv1 = (30300_f64 / 30000.0).ln().powi(2);
        assert!((rows[1].rv_1m - expected_rv1).abs() < TOL, "rv at row 1");

        let expected_vwap2 = (30000.0 * 1.0 + 30300.0 * 2.0 + 29900.0 * 1.5) / (1.0 + 2.0 + 1.5);
        assert!((rows[2].vwap_1m.unwrap() - expected_vwap2).abs() < TOL, "vwap at row 2");
    }

    #[test]
    fn conformance_case_04_window_expiry() {
        let ns = 1_000_000_000i64;
        let events = vec![
            trade(1700000000 * ns, 30000.0, 5.0, "buy"),
            trade(1700000061 * ns, 31000.0, 2.0, "sell"),
        ];
        let rows = compute_stream(&events);
        assert_eq!(rows.len(), 2);
        // trade 1 expired from 1m window
        assert!(approx_eq(rows[1].vwap_1m, Some(31000.0)), "1m window should only have trade 2");
        // trade 1 still in 5m window
        let expected_vwap5 = (30000.0 * 5.0 + 31000.0 * 2.0) / 7.0;
        assert!((rows[1].vwap_5m.unwrap() - expected_vwap5).abs() < TOL, "5m window includes both");
    }
}
