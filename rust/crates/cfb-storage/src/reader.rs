use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn read_partitioned(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: Result<Vec<_>, _> = reader.collect();
    Ok(batches?)
}
