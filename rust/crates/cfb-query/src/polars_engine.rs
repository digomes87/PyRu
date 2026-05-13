use std::path::Path;

use anyhow::Result;
use polars::prelude::*;

pub struct PolarsEngine {
    base_path: String,
}

impl PolarsEngine {
    pub fn new(base_path: &Path) -> Self {
        Self {
            base_path: base_path.to_string_lossy().to_string(),
        }
    }

    fn scan(&self) -> LazyFrame {
        LazyFrame::scan_parquet(
            format!("{}/**/*.parquet", self.base_path),
            Default::default(),
        )
        .expect("parquet scan")
    }

    pub fn q1_vwap_7d(&self, symbol: &str) -> Result<DataFrame> {
        let df = self
            .scan()
            .filter(col("symbol").eq(lit(symbol)))
            .with_column((col("ts") / lit(60_000_000_000i64) * lit(60_000_000_000i64)).alias("minute"))
            .group_by([col("minute")])
            .agg([(col("vwap_1m") * col("trade_count_1m")).sum()
                / col("trade_count_1m").sum()])
            .sort(["minute"], Default::default())
            .collect()?;
        Ok(df)
    }
}
