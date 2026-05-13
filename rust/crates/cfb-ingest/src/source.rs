use std::path::Path;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::{Stream, StreamExt};

use crate::Trade;

pub fn from_file(path: impl AsRef<Path>) -> impl Stream<Item = Result<Trade>> + Send {
    let path = path.as_ref().to_owned();
    async_stream::stream! {
        let file = tokio::fs::File::open(&path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            let line = line.trim().to_owned();
            if !line.is_empty() {
                let trade: Trade = serde_json::from_str(&line)?;
                yield Ok(trade);
            }
        }
    }
}

pub fn from_websocket(url: &str) -> impl Stream<Item = Result<Trade>> + Send + '_ {
    async_stream::stream! {
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::Message;

        let (mut ws, _) = connect_async(url).await?;
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
