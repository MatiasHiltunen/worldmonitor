use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::Value;
use thiserror::Error;
use tower::ServiceExt;

use crate::{AppState, build_app, config::AppConfig};

#[derive(Debug, Error)]
pub enum InProcessClientError {
    #[error("failed to serialize request JSON: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("failed to build request: {0}")]
    RequestBuild(#[source] axum::http::Error),
    #[error("in-process dispatch failed: {0}")]
    Dispatch(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("failed to read response body: {0}")]
    BodyRead(#[source] axum::Error),
    #[error("HTTP {status} from {path}: {body}")]
    HttpStatus {
        status: StatusCode,
        path: String,
        body: String,
    },
    #[error("response was not valid JSON: {0}")]
    Decode(#[source] serde_json::Error),
}

#[derive(Clone)]
pub struct InProcessClient {
    app: axum::Router,
    default_origin: Option<String>,
    default_api_key: Option<String>,
}

impl InProcessClient {
    pub fn from_state(state: AppState) -> Self {
        Self {
            app: build_app(state),
            default_origin: None,
            default_api_key: None,
        }
    }

    pub fn from_config(config: AppConfig) -> anyhow::Result<Self> {
        let state = AppState::from_config(config)?;
        Ok(Self::from_state(state))
    }

    pub fn with_default_origin(mut self, origin: Option<String>) -> Self {
        self.default_origin = origin;
        self
    }

    pub fn with_default_api_key(mut self, api_key: Option<String>) -> Self {
        self.default_api_key = api_key;
        self
    }

    pub async fn post_json_path(
        &self,
        path: &str,
        request_body: &Value,
    ) -> Result<Value, InProcessClientError> {
        self.post_json_path_with_overrides(path, request_body, None, None)
            .await
    }

    pub async fn post_json_path_with_overrides(
        &self,
        path: &str,
        request_body: &Value,
        origin: Option<&str>,
        api_key: Option<&str>,
    ) -> Result<Value, InProcessClientError> {
        let request_json =
            serde_json::to_vec(request_body).map_err(InProcessClientError::Encode)?;

        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "application/json");

        if let Some(origin) = origin.or(self.default_origin.as_deref()) {
            builder = builder.header(header::ORIGIN, origin);
        }

        if let Some(api_key) = api_key.or(self.default_api_key.as_deref()) {
            builder = builder.header("X-WorldMonitor-Key", api_key);
        }

        let request = builder
            .body(Body::from(request_json))
            .map_err(InProcessClientError::RequestBuild)?;

        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| InProcessClientError::Dispatch(Box::new(error)))?;

        let status = response.status();
        let body_bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(InProcessClientError::BodyRead)?;
        let body_text = String::from_utf8_lossy(&body_bytes).to_string();

        if !status.is_success() {
            return Err(InProcessClientError::HttpStatus {
                status,
                path: path.to_string(),
                body: truncate_for_error(&body_text, 240),
            });
        }

        serde_json::from_slice::<Value>(&body_bytes).map_err(InProcessClientError::Decode)
    }
}

fn truncate_for_error(input: &str, max_chars: usize) -> String {
    let normalized = input.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut shortened = normalized.chars().take(max_chars).collect::<String>();
    shortened.push_str("...");
    shortened
}

#[cfg(test)]
mod tests {
    use crate::config::AppConfig;

    use super::*;

    fn test_config() -> AppConfig {
        AppConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
            valid_keys: vec!["test-key".to_string()],
            runtime_env: "development".to_string(),
            groq_api_key: None,
            acled_access_token: None,
            finnhub_api_key: None,
            fred_api_key: None,
            eia_api_key: None,
            request_timeout_ms: 500,
        }
    }

    #[tokio::test]
    async fn in_process_client_calls_route_without_http_socket() {
        let client = InProcessClient::from_config(test_config()).expect("build in-process client");
        let payload = client
            .post_json_path(
                "/api/market/v1/get-country-stock-index",
                &serde_json::json!({"countryCode":"ZZ"}),
            )
            .await
            .expect("route response");

        assert_eq!(payload["available"], false);
        assert_eq!(payload["code"], "ZZ");
    }
}
