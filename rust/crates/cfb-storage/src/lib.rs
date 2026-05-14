pub mod reader;
pub mod writer;

pub use reader::read_partitioned;
pub use writer::{build_feature_batch, feature_schema, write_partitioned};

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const BASE_TS: i64 = 1_700_000_000 * 1_000_000_000;

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        build_feature_batch(
            (0..n).map(|i| BASE_TS + i as i64 * NS_PER_HOUR).collect(),
            (0..n).map(|_| "BTCUSDT").collect(),
            (0..n).map(|i| Some(30_000.0 + i as f64)).collect(),
            (0..n).map(|_| Some(30_000.0)).collect(),
            (0..n).map(|_| Some(30_000.0)).collect(),
            vec![0.0; n],
            vec![0.0; n],
            vec![1.0; n],
            vec![None; n],
            (0..n).map(|_| 1i64).collect(),
        )
    }

    #[test]
    fn test_write_creates_parquet_files() {
        let tmp = TempDir::new().unwrap();
        let batch = make_batch(5);
        write_partitioned(&batch, tmp.path(), 0).unwrap();

        let parquet_files: Vec<_> = walkdir::WalkDir::new(tmp.path())
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "parquet"))
            .collect();
        assert_eq!(parquet_files.len(), 5, "one file per hour");
    }

    #[test]
    fn test_partition_directory_structure() {
        let tmp = TempDir::new().unwrap();
        let batch = make_batch(3);
        write_partitioned(&batch, tmp.path(), 0).unwrap();

        for entry in walkdir::WalkDir::new(tmp.path())
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "parquet"))
        {
            let parts: Vec<String> = entry
                .path()
                .ancestors()
                .map(|a| a.file_name().map_or("", |n| n.to_str().unwrap()).to_owned())
                .collect();
            assert!(parts.iter().any(|p| p.starts_with("symbol=")));
            assert!(parts.iter().any(|p| p.starts_with("date=")));
            assert!(parts.iter().any(|p| p.starts_with("hour=")));
        }
    }

    #[test]
    fn test_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let batch = make_batch(10);
        let n_rows = batch.num_rows();
        write_partitioned(&batch, tmp.path(), 0).unwrap();

        let batches = read_partitioned(tmp.path(), Some("BTCUSDT")).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n_rows);
    }

    #[test]
    fn test_symbol_filter() {
        let tmp = TempDir::new().unwrap();
        // Two symbols at the same ts
        let batch_btc = build_feature_batch(
            vec![BASE_TS],
            vec!["BTCUSDT"],
            vec![Some(30_000.0)],
            vec![None],
            vec![None],
            vec![0.0],
            vec![0.0],
            vec![1.0],
            vec![None],
            vec![1],
        );
        let batch_eth = build_feature_batch(
            vec![BASE_TS],
            vec!["ETHUSDT"],
            vec![Some(1_800.0)],
            vec![None],
            vec![None],
            vec![0.0],
            vec![0.0],
            vec![1.0],
            vec![None],
            vec![1],
        );
        write_partitioned(&batch_btc, tmp.path(), 0).unwrap();
        write_partitioned(&batch_eth, tmp.path(), 1).unwrap();

        let btc_batches = read_partitioned(tmp.path(), Some("BTCUSDT")).unwrap();
        let eth_batches = read_partitioned(tmp.path(), Some("ETHUSDT")).unwrap();

        let btc_total: usize = btc_batches.iter().map(|b| b.num_rows()).sum();
        let eth_total: usize = eth_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(btc_total, 1);
        assert_eq!(eth_total, 1);
    }
}
