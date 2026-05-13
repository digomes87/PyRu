use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub fn write_partitioned(batch: &RecordBatch, base_path: &Path) -> Result<()> {
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let output_path = base_path.join("data.parquet");
    let file = std::fs::File::create(&output_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(())
}
