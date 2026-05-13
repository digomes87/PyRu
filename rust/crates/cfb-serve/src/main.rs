use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    cache: Arc<RwLock<FeatureCache>>,
    data_path: Arc<String>,
}

struct FeatureCache {
    /// (symbol, ts) → FeatureRow
    rows: HashMap<(String, i64), FeatureRow>,
    loaded_at: Option<Instant>,
}

impl FeatureCache {
    fn new() -> Self {
        Self { rows: HashMap::new(), loaded_at: None }
    }

    fn get(&self, symbol: &str, ts: i64) -> Option<&FeatureRow> {
        self.rows.get(&(symbol.to_owned(), ts))
    }

    fn insert(&mut self, row: FeatureRow) {
        self.rows.insert((row.symbol.clone(), row.ts), row);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Serialize)]
struct FeatureResponse {
    #[serde(flatten)]
    row: FeatureRow,
    source: &'static str,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    cache_rows: usize,
    cache_age_s: f64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let cache = state.cache.read().await;
    let age = cache
        .loaded_at
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(f64::INFINITY);
    Json(HealthResponse {
        status: "ok",
        cache_rows: cache.len(),
        cache_age_s: age,
    })
}

async fn get_features(
    State(state): State<AppState>,
    Path((symbol, ts)): Path<(String, i64)>,
) -> impl IntoResponse {
    // Hot path: check in-process cache
    {
        let cache = state.cache.read().await;
        if let Some(row) = cache.get(&symbol, ts) {
            return (
                StatusCode::OK,
                Json(serde_json::json!(FeatureResponse { row: row.clone(), source: "cache" })),
            );
        }
    }

    // Cold path: read from Parquet (stub for Phase 5 — full implementation
    // would use cfb-query DataFusion engine)
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": "not found",
            "symbol": symbol,
            "ts": ts
        })),
    )
}

async fn load_cache(State(state): State<AppState>) -> impl IntoResponse {
    let path = state.data_path.as_str();
    let mut cache = state.cache.write().await;

    // Stub: in a full implementation this would scan the Parquet files
    // using cfb-storage::read_partitioned and populate the cache.
    cache.loaded_at = Some(Instant::now());

    (StatusCode::OK, Json(serde_json::json!({ "loaded": cache.len() })))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let data_path = std::env::var("CFB_DATA_PATH").unwrap_or_else(|_| "data/features".into());

    let state = AppState {
        cache: Arc::new(RwLock::new(FeatureCache::new())),
        data_path: Arc::new(data_path),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/features/{symbol}/{ts}", get(get_features))
        .route("/cache/load", get(load_cache))
        .with_state(state);

    let addr = std::env::var("CFB_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let listener = TcpListener::bind(&addr).await.unwrap();
    tracing::info!("cfb-serve listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
