use std::time::Duration;

use anyhow::Result;
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use crate::Trade;

pub struct BatchConfig {
    pub max_size: usize,
    pub max_latency: Duration,
    pub channel_capacity: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 1_000,
            max_latency: Duration::from_millis(100),
            channel_capacity: 64,
        }
    }
}

pub struct Batcher {
    #[allow(dead_code)]
    tx: mpsc::Sender<RecordBatch>,
}

impl Batcher {
    pub fn spawn<S>(source: S, config: BatchConfig) -> (Self, mpsc::Receiver<RecordBatch>)
    where
        S: Stream<Item = Result<Trade>> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            let mut buf: Vec<Trade> = Vec::with_capacity(config.max_size);
            let mut deadline = Instant::now() + config.max_latency;
            tokio::pin!(source);

            loop {
                tokio::select! {
                    biased;
                    maybe_trade = source.next() => {
                        match maybe_trade {
                            Some(Ok(trade)) => {
                                buf.push(trade);
                                if buf.len() >= config.max_size {
                                    let batch = build_batch(std::mem::take(&mut buf));
                                    if tx_clone.send(batch).await.is_err() { break; }
                                    deadline = Instant::now() + config.max_latency;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("ingest error: {e}");
                            }
                            None => {
                                if !buf.is_empty() {
                                    let batch = build_batch(std::mem::take(&mut buf));
                                    let _ = tx_clone.send(batch).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if !buf.is_empty() {
                            let batch = build_batch(std::mem::take(&mut buf));
                            if tx_clone.send(batch).await.is_err() { break; }
                        }
                        deadline = Instant::now() + config.max_latency;
                    }
                }
            }
        });

        (Self { tx }, rx)
    }
}

fn build_batch(trades: Vec<Trade>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("side", DataType::Utf8, false),
    ]);

    let ts: Int64Array = trades.iter().map(|t| t.ts).collect();
    let symbol: StringArray = trades.iter().map(|t| Some(t.symbol.as_str())).collect();
    let price: Float64Array = trades.iter().map(|t| t.price).collect();
    let qty: Float64Array = trades.iter().map(|t| t.qty).collect();
    let side: StringArray = trades
        .iter()
        .map(|t| Some(match t.side {
            crate::models::Side::Buy => "buy",
            crate::models::Side::Sell => "sell",
        }))
        .collect();

    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(ts),
            std::sync::Arc::new(symbol),
            std::sync::Arc::new(price),
            std::sync::Arc::new(qty),
            std::sync::Arc::new(side),
        ],
    )
    .expect("schema matches arrays")
}
