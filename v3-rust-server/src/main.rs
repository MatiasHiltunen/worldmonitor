use anyhow::Result;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};
use v3_rust_server::{AppState, build_app, config::AppConfig};

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let config = AppConfig::from_env()?;
    let state = AppState::from_config(config.clone())?;
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!("worldmonitor rust server listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
        response::Response,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use v3_rust_server::{AppState, build_app, config::AppConfig};

    fn test_state() -> AppState {
        let config = AppConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
            valid_keys: vec!["test-key".to_string()],
            runtime_env: "development".to_string(),
            groq_api_key: None,
            acled_access_token: None,
            finnhub_api_key: None,
            fred_api_key: None,
            eia_api_key: None,
            request_timeout_ms: 500,
        };
        AppState::from_config(config).expect("build test app state")
    }

    fn test_app() -> axum::Router {
        build_app(test_state())
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

    #[tokio::test]
    async fn gdelt_search_requires_query_length() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/intelligence/v1/search-gdelt-documents")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"query":"a"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["query"], "a");
        assert_eq!(
            payload["error"],
            "Query parameter required (min 2 characters)"
        );
    }

    #[tokio::test]
    async fn classify_event_without_groq_key_returns_empty_payload() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/intelligence/v1/classify-event")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"title":"Signal report"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("classification").is_none());
    }

    #[tokio::test]
    async fn baseline_endpoint_rejects_invalid_payload() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/infrastructure/v1/get-temporal-baseline")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"type":"","count":0}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(
            payload["error"],
            "Missing or invalid params: type and count required"
        );
    }

    #[tokio::test]
    async fn baseline_snapshot_then_read_returns_learning_state() {
        let app = test_app();
        let record_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/infrastructure/v1/record-baseline-snapshot")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"updates":[{"type":"news","region":"global","count":10}]}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(record_response.status(), StatusCode::OK);
        let record_payload = response_json(record_response).await;
        assert_eq!(record_payload["updated"], 1);

        let get_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/infrastructure/v1/get-temporal-baseline")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"type":"news","region":"global","count":12}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(get_response.status(), StatusCode::OK);
        let get_payload = response_json(get_response).await;
        assert_eq!(get_payload["learning"], true);
    }

    #[tokio::test]
    async fn fred_endpoint_requires_series_id() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/economic/v1/get-fred-series")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"seriesId":""}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn world_bank_endpoint_requires_indicator_code() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/economic/v1/list-world-bank-indicators")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"indicatorCode":""}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn energy_endpoint_without_api_key_returns_empty_prices() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/economic/v1/get-energy-prices")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"commodities":["wti"]}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["prices"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn macro_signals_endpoint_returns_contract_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/economic/v1/get-macro-signals")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("timestamp").is_some());
        assert!(payload.get("verdict").is_some());
        assert!(payload.get("signals").is_some());
        assert!(payload.get("meta").is_some());
    }

    #[tokio::test]
    async fn humanitarian_summary_requires_iso_country_code() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/conflict/v1/get-humanitarian-summary")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"countryCode":"usa"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn acled_endpoint_without_token_returns_empty_events() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/conflict/v1/list-acled-events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["events"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn prediction_endpoint_returns_markets_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/prediction/v1/list-prediction-markets")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("markets").is_some());
    }

    #[tokio::test]
    async fn tech_events_endpoint_rejects_negative_days() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/research/v1/list-tech-events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"days":-1}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn aviation_endpoint_returns_alerts_contract() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/aviation/v1/list-airport-delays")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"region":"AIRPORT_REGION_UNSPECIFIED","minSeverity":"FLIGHT_DELAY_SEVERITY_UNSPECIFIED"}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("alerts").is_some());
    }

    #[tokio::test]
    async fn flight_radar_endpoint_returns_contract_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/aviation/v1/get-flight-radar")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"includeGround":false}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("source").is_some());
        assert!(payload.get("flights").is_some());
    }

    #[tokio::test]
    async fn radiation_endpoint_returns_contract_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/climate/v1/get-global-radiation-situation")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("source").is_some());
        assert!(payload.get("entries").is_some());
    }

    #[tokio::test]
    async fn displacement_population_exposure_contract_is_available() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/displacement/v1/get-population-exposure")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"mode":"countries"}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["success"], true);
        assert!(payload.get("countries").is_some());
    }

    #[tokio::test]
    async fn maritime_snapshot_endpoint_returns_optional_snapshot_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/maritime/v1/get-vessel-snapshot")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.is_object());
    }

    #[tokio::test]
    async fn marine_traffic_endpoint_returns_contract_shape() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/maritime/v1/get-marine-traffic")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"area":""}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("source").is_some());
        assert!(payload.get("warnings").is_some());
    }

    #[tokio::test]
    async fn military_wingbits_status_endpoint_returns_configured_flag() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/military/v1/get-wingbits-status")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert!(payload.get("configured").is_some());
    }

    #[tokio::test]
    async fn news_summarize_endpoint_skips_unknown_provider() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/news/v1/summarize-article")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"provider":"unknown","headlines":["Signal update"],"mode":"brief","geoContext":"","variant":"full","lang":"en"}"#,
                    ))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["skipped"], true);
    }

    #[tokio::test]
    async fn wildfire_endpoint_without_api_key_returns_empty_payload() {
        let app = test_app();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wildfire/v1/list-fire-detections")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{}"#))
                    .expect("build request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response_json(response).await;
        assert_eq!(payload["fireDetections"], serde_json::json!([]));
    }
}
