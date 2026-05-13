use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Read all Parquet files from a directory (non-recursive) into RecordBatches.
pub fn read_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let batches: std::result::Result<Vec<_>, _> = reader.collect();
    Ok(batches?)
}

/// Recursively read all .parquet files under base_path, optionally filtered by symbol.
pub fn read_partitioned(base_path: &Path, symbol_filter: Option<&str>) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::new();

    for entry in walkdir::WalkDir::new(base_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
    {
        let path = entry.path();

        // Apply symbol filter based on directory name
        if let Some(sym) = symbol_filter {
            let in_sym_dir = path.ancestors().any(|a| {
                a.file_name()
                    .map_or(false, |n| n.to_string_lossy() == format!("symbol={sym}"))
            });
            if !in_sym_dir {
                continue;
            }
        }

        let batches = read_file(path)?;
        result.extend(batches);
    }

    Ok(result)
}
