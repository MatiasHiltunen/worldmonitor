use axum::{
    Json, Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Serialize;

use crate::{
    AppState,
    domains::{
        aviation, climate, conflict, cyber, displacement, economic, infrastructure,
        infrastructure_ops, intelligence, maritime, market, military, news, prediction, research,
        seismology, unrest, wildfire,
    },
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
            "/api/aviation/v1/list-airport-delays",
            post(aviation::list_airport_delays),
        )
        .route(
            "/api/climate/v1/list-climate-anomalies",
            post(climate::list_climate_anomalies),
        )
        .route(
            "/api/cyber/v1/list-cyber-threats",
            post(cyber::list_cyber_threats),
        )
        .route(
            "/api/displacement/v1/get-displacement-summary",
            post(displacement::get_displacement_summary),
        )
        .route(
            "/api/displacement/v1/get-population-exposure",
            post(displacement::get_population_exposure),
        )
        .route(
            "/api/intelligence/v1/get-country-intel-brief",
            post(intelligence::get_country_intel_brief),
        )
        .route(
            "/api/intelligence/v1/get-pizzint-status",
            post(intelligence::get_pizzint_status),
        )
        .route(
            "/api/intelligence/v1/classify-event",
            post(intelligence::classify_event),
        )
        .route(
            "/api/intelligence/v1/search-gdelt-documents",
            post(intelligence::search_gdelt_documents),
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
            "/api/market/v1/list-market-quotes",
            post(market::list_market_quotes),
        )
        .route(
            "/api/market/v1/list-commodity-quotes",
            post(market::list_commodity_quotes),
        )
        .route(
            "/api/market/v1/get-sector-summary",
            post(market::get_sector_summary),
        )
        .route(
            "/api/market/v1/list-stablecoin-markets",
            post(market::list_stablecoin_markets),
        )
        .route(
            "/api/market/v1/list-etf-flows",
            post(market::list_etf_flows),
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
            "/api/infrastructure/v1/list-internet-outages",
            post(infrastructure_ops::list_internet_outages),
        )
        .route(
            "/api/infrastructure/v1/get-temporal-baseline",
            post(infrastructure_ops::get_temporal_baseline),
        )
        .route(
            "/api/infrastructure/v1/record-baseline-snapshot",
            post(infrastructure_ops::record_baseline_snapshot),
        )
        .route(
            "/api/infrastructure/v1/get-cable-health",
            post(infrastructure_ops::get_cable_health),
        )
        .route(
            "/api/market/v1/list-crypto-quotes",
            post(market::list_crypto_quotes),
        )
        .route(
            "/api/economic/v1/get-fred-series",
            post(economic::get_fred_series),
        )
        .route(
            "/api/economic/v1/list-world-bank-indicators",
            post(economic::list_world_bank_indicators),
        )
        .route(
            "/api/economic/v1/get-energy-prices",
            post(economic::get_energy_prices),
        )
        .route(
            "/api/economic/v1/get-macro-signals",
            post(economic::get_macro_signals),
        )
        .route(
            "/api/conflict/v1/list-acled-events",
            post(conflict::list_acled_events),
        )
        .route(
            "/api/conflict/v1/list-ucdp-events",
            post(conflict::list_ucdp_events),
        )
        .route(
            "/api/conflict/v1/get-humanitarian-summary",
            post(conflict::get_humanitarian_summary),
        )
        .route(
            "/api/prediction/v1/list-prediction-markets",
            post(prediction::list_prediction_markets),
        )
        .route(
            "/api/research/v1/list-arxiv-papers",
            post(research::list_arxiv_papers),
        )
        .route(
            "/api/research/v1/list-trending-repos",
            post(research::list_trending_repos),
        )
        .route(
            "/api/research/v1/list-hackernews-items",
            post(research::list_hackernews_items),
        )
        .route(
            "/api/research/v1/list-tech-events",
            post(research::list_tech_events),
        )
        .route(
            "/api/maritime/v1/get-vessel-snapshot",
            post(maritime::get_vessel_snapshot),
        )
        .route(
            "/api/maritime/v1/list-navigational-warnings",
            post(maritime::list_navigational_warnings),
        )
        .route(
            "/api/military/v1/list-military-flights",
            post(military::list_military_flights),
        )
        .route(
            "/api/military/v1/get-theater-posture",
            post(military::get_theater_posture),
        )
        .route(
            "/api/military/v1/get-aircraft-details",
            post(military::get_aircraft_details),
        )
        .route(
            "/api/military/v1/get-aircraft-details-batch",
            post(military::get_aircraft_details_batch),
        )
        .route(
            "/api/military/v1/get-wingbits-status",
            post(military::get_wingbits_status),
        )
        .route(
            "/api/military/v1/get-usni-fleet-report",
            post(military::get_usni_fleet_report),
        )
        .route(
            "/api/news/v1/summarize-article",
            post(news::summarize_article),
        )
        .route(
            "/api/wildfire/v1/list-fire-detections",
            post(wildfire::list_fire_detections),
        )
        .fallback(not_found)
        .with_state(state)
}
