use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion};

const NS_PER_DAY: i64 = 86_400_000_000_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// One in-memory partition bucket.
struct Partition {
    rows: Vec<usize>, // indices into the source batch
}

/// Write a feature RecordBatch to hive-partitioned Parquet.
///
/// Layout: `{base}/{symbol=X}/{date=D}/{hour=H}/part-{idx:04}-0.parquet`
/// Compatible with the Python pyarrow writer (same schema, same compression).
pub fn write_partitioned(batch: &RecordBatch, base: &Path, part_idx: u32) -> Result<()> {
    let ts_arr = batch
        .column_by_name("ts")
        .context("missing ts column")?
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("ts is not Int64")?;
    let sym_arr = batch
        .column_by_name("symbol")
        .context("missing symbol column")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("symbol is not Utf8")?;

    // Group row indices by (symbol, date, hour)
    let mut buckets: HashMap<(String, i32, i32), Vec<usize>> = HashMap::new();
    for i in 0..batch.num_rows() {
        let ts = ts_arr.value(i);
        let date = (ts / NS_PER_DAY) as i32;
        let hour = ((ts % NS_PER_DAY) / NS_PER_HOUR) as i32;
        let sym = sym_arr.value(i).to_owned();
        buckets.entry((sym, date, hour)).or_default().push(i);
    }

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(true)
        .build();

    for ((symbol, date, hour), indices) in &buckets {
        let dir = base
            .join(format!("symbol={symbol}"))
            .join(format!("date={date}"))
            .join(format!("hour={hour}"));
        fs::create_dir_all(&dir)?;

        let file_path = dir.join(format!("part-{part_idx:04}-0.parquet"));
        let sub_batch = take_rows(batch, indices)?;

        let file = fs::File::create(&file_path)
            .with_context(|| format!("creating {}", file_path.display()))?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&sub_batch.schema()), Some(props.clone()))
                .context("creating ArrowWriter")?;
        writer.write(&sub_batch).context("writing batch")?;
        writer.close().context("closing writer")?;
    }

    Ok(())
}

/// Slice a RecordBatch to the given row indices.
fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    use arrow_array::{Array, ArrayRef};

    fn take_col(col: &dyn Array, idxs: &[usize]) -> ArrayRef {
        let idx_arr =
            arrow_array::Int64Array::from(idxs.iter().map(|&i| i as i64).collect::<Vec<_>>());
        arrow::compute::take(col, &idx_arr, None).expect("take")
    }

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take_col(c.as_ref(), indices))
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(&batch.schema()), columns)?)
}

/// Build a feature RecordBatch from parallel vecs — used in tests and benches.
pub fn build_feature_batch(
    ts: Vec<i64>,
    symbol: Vec<&str>,
    vwap_1m: Vec<Option<f64>>,
    vwap_5m: Vec<Option<f64>>,
    vwap_15m: Vec<Option<f64>>,
    rv_1m: Vec<f64>,
    rv_5m: Vec<f64>,
    ofi_1m: Vec<f64>,
    microprice: Vec<Option<f64>>,
    trade_count_1m: Vec<i64>,
) -> RecordBatch {
    let schema = feature_schema();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(symbol)),
            Arc::new(Float64Array::from(vwap_1m)),
            Arc::new(Float64Array::from(vwap_5m)),
            Arc::new(Float64Array::from(vwap_15m)),
            Arc::new(Float64Array::from(rv_1m)),
            Arc::new(Float64Array::from(rv_5m)),
            Arc::new(Float64Array::from(ofi_1m)),
            Arc::new(Float64Array::from(microprice)),
            Arc::new(Int64Array::from(trade_count_1m)),
        ],
    )
    .expect("schema matches arrays")
}

pub fn feature_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("vwap_1m", DataType::Float64, true),
        Field::new("vwap_5m", DataType::Float64, true),
        Field::new("vwap_15m", DataType::Float64, true),
        Field::new("rv_1m", DataType::Float64, false),
        Field::new("rv_5m", DataType::Float64, false),
        Field::new("ofi_1m", DataType::Float64, false),
        Field::new("microprice", DataType::Float64, true),
        Field::new("trade_count_1m", DataType::Int64, false),
    ])
}
