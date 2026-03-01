use std::sync::Arc;

use anyhow::Result;
use axum::{Router, middleware};
use tower_http::trace::TraceLayer;

pub mod cache;
pub mod config;
pub mod domains;
pub mod error;
pub mod http;
pub mod in_process;
pub mod routes;

use crate::config::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub http_client: reqwest::Client,
}

impl AppState {
    pub fn from_config(config: AppConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.request_timeout_ms))
            .build()?;
        Ok(Self {
            config: Arc::new(config),
            http_client,
        })
    }
}

pub fn build_app(state: AppState) -> Router {
    routes::build_router(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            http::auth::enforce_api_key,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            http::cors::enforce_cors,
        ))
        .layer(TraceLayer::new_for_http())
}
