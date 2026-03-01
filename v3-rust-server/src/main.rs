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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
        middleware,
        response::Response,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    fn test_state() -> AppState {
        let config = AppConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
            valid_keys: vec!["test-key".to_string()],
            runtime_env: "development".to_string(),
            groq_api_key: None,
            acled_access_token: None,
            finnhub_api_key: None,
            request_timeout_ms: 500,
        };
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .expect("build test http client");
        AppState {
            config: Arc::new(config),
            http_client,
        }
    }

    fn test_app() -> axum::Router {
        let state = test_state();
        routes::build_router(state.clone())
            .layer(middleware::from_fn_with_state(
                state.clone(),
                http::auth::enforce_api_key,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                http::cors::enforce_cors,
            ))
    }

    async fn response_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body bytes");
        serde_json::from_slice(&body).expect("decode response json")
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["service"], "worldmonitor-rust-server");
    }

    #[tokio::test]
    async fn unknown_route_returns_not_found() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/does-not-exist")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let payload = response_json(response).await;
        assert_eq!(payload["error"], "Not found");
    }

    #[tokio::test]
    async fn invalid_country_code_returns_bad_request() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/intelligence/v1/get-country-intel-brief")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"countryCode":"USA"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_stock_country_returns_unavailable_payload() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/market/v1/get-country-stock-index")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"countryCode":"ZZ"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["available"], false);
        assert_eq!(payload["code"], "ZZ");
    }

    #[tokio::test]
    async fn disallowed_origin_is_blocked() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn desktop_origin_requires_api_key() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/intelligence/v1/get-risk-scores")
                    .header(header::ORIGIN, "tauri://localhost")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn desktop_origin_with_valid_key_succeeds() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/intelligence/v1/get-risk-scores")
                    .header(header::ORIGIN, "tauri://localhost")
                    .header("X-WorldMonitor-Key", "test-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
