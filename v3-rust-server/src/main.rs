use std::sync::Arc;

use anyhow::Result;
use axum::middleware;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

mod cache;
mod config;
mod domains;
mod error;
mod http;
mod routes;

use crate::config::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub http_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let config = AppConfig::from_env()?;
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(config.request_timeout_ms))
        .build()?;
    let state = AppState {
        config: Arc::new(config.clone()),
        http_client,
    };

    let app = routes::build_router(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            http::auth::enforce_api_key,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            http::cors::enforce_cors,
        ))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!("worldmonitor rust server listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
