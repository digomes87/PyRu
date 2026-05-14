use std::path::Path;

use anyhow::Result;
use datafusion::prelude::*;

pub struct DataFusionEngine {
    ctx: SessionContext,
    #[allow(dead_code)]
    base_path: String,
}

impl DataFusionEngine {
    pub async fn new(base_path: &Path) -> Result<Self> {
        let ctx = SessionContext::new();
        let base = base_path.to_string_lossy().to_string();

        ctx.register_parquet(
            "features",
            &format!("{base}/**/*.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

        Ok(Self {
            ctx,
            base_path: base,
        })
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<arrow_array::RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        Ok(df.collect().await?)
    }

    pub async fn q1_vwap_7d(&self, symbol: &str) -> Result<Vec<arrow_array::RecordBatch>> {
        let sql = format!(
            "SELECT ts / 60000000000 * 60000000000 AS minute,
                    SUM(vwap_1m * trade_count_1m) / SUM(trade_count_1m) AS vwap
             FROM features
             WHERE symbol = '{symbol}'
             GROUP BY 1
             ORDER BY 1"
        );
        self.execute(&sql).await
    }
}
