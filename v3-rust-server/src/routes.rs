use axum::{
    Json, Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Serialize;

use crate::{
    AppState,
    domains::{infrastructure, intelligence, market, seismology, unrest},
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: String,
    service: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct NotFoundResponse {
    error: String,
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        service: "worldmonitor-rust-server".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

async fn not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(NotFoundResponse {
            error: "Not found".to_string(),
        }),
    )
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        // Phase A routes needed for Rust TUI + core parity harness.
        .route(
            "/api/intelligence/v1/get-country-intel-brief",
            post(intelligence::get_country_intel_brief),
        )
        .route(
            "/api/intelligence/v1/get-risk-scores",
            post(intelligence::get_risk_scores),
        )
        .route(
            "/api/market/v1/get-country-stock-index",
            post(market::get_country_stock_index),
        )
        .route(
            "/api/seismology/v1/list-earthquakes",
            post(seismology::list_earthquakes),
        )
        .route(
            "/api/unrest/v1/list-unrest-events",
            post(unrest::list_unrest_events),
        )
        .route(
            "/api/infrastructure/v1/list-service-statuses",
            post(infrastructure::list_service_statuses),
        )
        .route(
            "/api/market/v1/list-crypto-quotes",
            post(market::list_crypto_quotes),
        )
        .fallback(not_found)
        .with_state(state)
}
