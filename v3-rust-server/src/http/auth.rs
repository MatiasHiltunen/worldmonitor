use axum::{
    extract::State,
    http::{Request, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::{AppState, error::AppError};

static DESKTOP_ORIGIN_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"^https?://tauri\.localhost(:\d+)?$").expect("valid regex"),
        Regex::new(r"^https?://[a-z0-9-]+\.tauri\.localhost(:\d+)?$").expect("valid regex"),
        Regex::new(r"^tauri://localhost$").expect("valid regex"),
        Regex::new(r"^asset://localhost$").expect("valid regex"),
    ]
});

fn is_desktop_origin(origin: &str) -> bool {
    !origin.is_empty()
        && DESKTOP_ORIGIN_PATTERNS
            .iter()
            .any(|pattern| pattern.is_match(origin))
}

fn key_is_valid(key: &str, valid_keys: &[String]) -> bool {
    valid_keys.iter().any(|candidate| candidate == key)
}

pub async fn enforce_api_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    let key = request
        .headers()
        .get("X-WorldMonitor-Key")
        .and_then(|value| value.to_str().ok());

    if is_desktop_origin(origin) {
        if key.is_none() {
            return AppError::Unauthorized("API key required for desktop access".to_string())
                .into_response();
        }
        if let Some(key) = key
            && !key_is_valid(key, &state.config.valid_keys)
        {
            return AppError::Unauthorized("Invalid API key".to_string()).into_response();
        }
        return next.run(request).await;
    }

    if let Some(key) = key
        && !key_is_valid(key, &state.config.valid_keys)
    {
        return AppError::Unauthorized("Invalid API key".to_string()).into_response();
    }

    next.run(request).await
}
