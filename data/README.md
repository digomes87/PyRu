# Data

Raw data is not committed to this repository. Run the download script to fetch it:

```bash
bash scripts/download_data.sh
```

## Sources

- **Historical OHLCV (1-minute BTC):** Kaggle `mczielinski/bitcoin-historical-data`
- **High-frequency order book:** Kaggle `martinsn/high-frequency-crypto-limit-order-book-data`
- **Live stream (load generator only):** Binance public WebSocket — `wss://stream.binance.com:9443/ws/btcusdt@trade`
  - A recorded replay file is provided at `data/raw/btcusdt_replay.jsonl` after running the download script.

## Layout

```
data/
├── raw/          # gitignored — populated by download_data.sh
│   ├── btcusd_1min.csv
│   ├── hf_orderbook/
│   └── btcusdt_replay.jsonl
└── README.md
```

## Checksums

SHA-256 checksums for raw files are verified by `scripts/download_data.sh`. If a file fails verification, delete it and re-run the script.
