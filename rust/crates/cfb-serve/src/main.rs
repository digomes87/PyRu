use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    data_path: Arc<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize, Deserialize)]
struct FeatureRow {
    ts: i64,
    symbol: String,
    vwap_1m: Option<f64>,
    vwap_5m: Option<f64>,
    vwap_15m: Option<f64>,
    rv_1m: f64,
    rv_5m: f64,
    ofi_1m: f64,
    microprice: Option<f64>,
    trade_count_1m: i64,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn get_features(
    State(_state): State<AppState>,
    Path((symbol, ts)): Path<(String, i64)>,
) -> impl IntoResponse {
    // Stub — full implementation in Phase 5
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "not implemented",
            "symbol": symbol,
            "ts": ts
        })),
    )
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let data_path = std::env::var("CFB_DATA_PATH").unwrap_or_else(|_| "data/features".into());
    let state = AppState {
        data_path: Arc::new(data_path),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/features/:symbol/:ts", get(get_features))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:8001").await.unwrap();
    tracing::info!("cfb-serve listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
