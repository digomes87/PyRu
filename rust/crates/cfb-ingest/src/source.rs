use std::path::Path;

use anyhow::Result;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use crate::Trade;

pub fn from_file(path: impl Into<std::path::PathBuf> + Send) -> impl Stream<Item = Result<Trade>> + Send {
    let path = path.into();
    async_stream::stream! {
        let file = tokio::fs::File::open(&path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            let trimmed = line.trim().to_owned();
            if !trimmed.is_empty() {
                let trade: Trade = serde_json::from_str(&trimmed)?;
                yield Ok(trade);
            }
        }
    }
}

pub fn from_iter(trades: Vec<Trade>) -> impl Stream<Item = Result<Trade>> + Send {
    async_stream::stream! {
        for trade in trades {
            yield Ok(trade);
        }
    }
}

pub fn from_websocket(url: String) -> impl Stream<Item = Result<Trade>> + Send {
    async_stream::stream! {
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::Message;

        let (mut ws, _) = connect_async(&url).await?;
        while let Some(msg) = ws.next().await {
            match msg? {
                Message::Text(text) => {
                    let trade: Trade = serde_json::from_str(&text)?;
                    yield Ok(trade);
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    }
}
