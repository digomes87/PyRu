pub mod models;
pub mod source;
pub mod batch;
pub mod synth;

pub use models::{Side, Trade};
pub use source::{from_file, from_iter, from_websocket};
pub use batch::{BatchConfig, Batcher};

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow_array::StringArray;
    use tempfile::NamedTempFile;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::batch::build_batch_from;

    fn trade(ts: i64, price: f64, qty: f64, side: &str) -> Trade {
        Trade {
            ts,
            symbol: "BTCUSDT".into(),
            price,
            qty,
            side: if side == "buy" { Side::Buy } else { Side::Sell },
        }
    }

    // ----- from_file ---------------------------------------------------------

    #[tokio::test]
    async fn test_from_file_reads_all() {
        let mut f = NamedTempFile::new().unwrap();
        for i in 0..5 {
            writeln!(
                f,
                r#"{{"ts":{i},"symbol":"BTCUSDT","price":30000.0,"qty":1.0,"side":"buy"}}"#
            )
            .unwrap();
        }
        let stream = from_file(f.path());
        tokio::pin!(stream);
        let trades: Vec<Trade> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(trades.len(), 5);
        assert_eq!(trades[0].ts, 0);
        assert_eq!(trades[4].ts, 4);
    }

    #[tokio::test]
    async fn test_from_file_skips_blank_lines() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            r#"{{"ts":1,"symbol":"BTCUSDT","price":30000.0,"qty":1.0,"side":"buy"}}"#
        )
        .unwrap();
        writeln!(f).unwrap();
        let stream = from_file(f.path());
        tokio::pin!(stream);
        let trades: Vec<Trade> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(trades.len(), 1);
    }

    // ----- batching ----------------------------------------------------------

    async fn run_batcher(trades: Vec<Trade>, cfg: BatchConfig) -> Vec<Vec<Trade>> {
        let source = from_iter(trades);
        let (_, mut rx) = Batcher::spawn(source, cfg);
        let mut batches = Vec::new();
        while let Some(batch) = rx.recv().await {
            if batch.num_rows() == 0 {
                break;
            }
            // decode back to Vec<Trade> for assertions
            let ts_arr = batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            batches.push(ts_arr.values().to_vec());
        }
        batches.iter().map(|v| {
            v.iter().map(|&ts| trade(ts, 30000.0, 1.0, "buy")).collect()
        }).collect()
    }

    #[tokio::test]
    async fn test_batcher_exact_size() {
        let trades: Vec<Trade> = (0..10).map(|i| trade(i, 30000.0, 1.0, "buy")).collect();
        let cfg = BatchConfig { max_size: 5, max_latency: std::time::Duration::from_secs(10), ..Default::default() };
        let batches = run_batcher(trades, cfg).await;
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|b| b.len() == 5));
    }

    #[tokio::test]
    async fn test_batcher_partial_flush_at_end() {
        let trades: Vec<Trade> = (0..7).map(|i| trade(i, 30000.0, 1.0, "buy")).collect();
        let cfg = BatchConfig { max_size: 5, max_latency: std::time::Duration::from_secs(10), ..Default::default() };
        let batches = run_batcher(trades, cfg).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 5);
        assert_eq!(batches[1].len(), 2);
    }

    #[tokio::test]
    async fn test_batcher_empty_source() {
        let cfg = BatchConfig::default();
        let batches = run_batcher(vec![], cfg).await;
        assert_eq!(batches.len(), 0);
    }

    // ----- build_batch_from --------------------------------------------------

    #[test]
    fn test_arrow_batch_schema() {
        let trades = vec![trade(1_000_000_000, 30_100.5, 0.5, "sell")];
        let batch = build_batch_from(&trades);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field_with_name("ts").unwrap().data_type(), &arrow_schema::DataType::Int64);
        let side = batch.column_by_name("side").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(side.value(0), "sell");
    }
}
