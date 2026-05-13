#!/usr/bin/env bash
# Download and verify benchmark datasets from Kaggle.
# Requires: kaggle CLI configured with API credentials in ~/.kaggle/kaggle.json
# or KAGGLE_USERNAME / KAGGLE_KEY environment variables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_RAW="$SCRIPT_DIR/../data/raw"
mkdir -p "$DATA_RAW"

check_kaggle() {
    if ! command -v kaggle &>/dev/null; then
        echo "kaggle CLI not found. Install with: pip install kaggle"
        exit 1
    fi
}

download_btc_1min() {
    echo "Downloading BTC 1-minute OHLCV data..."
    kaggle datasets download \
        -d mczielinski/bitcoin-historical-data \
        -p "$DATA_RAW/btc_1min" \
        --unzip
    echo "SHA-256:"
    find "$DATA_RAW/btc_1min" -name "*.csv" -exec shasum -a 256 {} \;
}

download_hf_orderbook() {
    echo "Downloading high-frequency order book data..."
    kaggle datasets download \
        -d martinsn/high-frequency-crypto-limit-order-book-data \
        -p "$DATA_RAW/hf_orderbook" \
        --unzip
    echo "SHA-256:"
    find "$DATA_RAW/hf_orderbook" -name "*.csv" -exec shasum -a 256 {} \;
}

main() {
    check_kaggle
    download_btc_1min
    download_hf_orderbook
    echo ""
    echo "Data download complete. Files are in $DATA_RAW"
    echo "Run scripts/replay_stream.py to generate the WebSocket replay fixture."
}

main "$@"
