use axum::{
    extract::State,
    http::{HeaderValue, Method, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::json;

use crate::AppState;

static PROD_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"^https://(.*\.)?worldmonitor\.app$").expect("valid regex"),
        Regex::new(r"^https://worldmonitor-[a-z0-9-]+-elie-[a-z0-9]+\.vercel\.app$")
            .expect("valid regex"),
        Regex::new(r"^https?://tauri\.localhost(:\d+)?$").expect("valid regex"),
        Regex::new(r"^https?://[a-z0-9-]+\.tauri\.localhost(:\d+)?$").expect("valid regex"),
        Regex::new(r"^tauri://localhost$").expect("valid regex"),
        Regex::new(r"^asset://localhost$").expect("valid regex"),
    ]
});

static DEV_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"^https?://localhost(:\d+)?$").expect("valid regex"),
        Regex::new(r"^https?://127\.0\.0\.1(:\d+)?$").expect("valid regex"),
    ]
});

fn is_allowed_origin(origin: &str, runtime_env: &str) -> bool {
    if origin.is_empty() {
        return false;
    }
    let mut patterns = PROD_PATTERNS.iter().collect::<Vec<_>>();
    if runtime_env != "production" {
        patterns.extend(DEV_PATTERNS.iter());
    }
    patterns.iter().any(|pattern| pattern.is_match(origin))
}

fn cors_allow_origin(origin: &str, runtime_env: &str) -> String {
    if is_allowed_origin(origin, runtime_env) {
        origin.to_string()
    } else {
        "https://worldmonitor.app".to_string()
    }
}

fn set_cors_headers(response: &mut Response, allow_origin: &str) {
    if let Ok(value) = HeaderValue::from_str(allow_origin) {
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, Authorization, X-WorldMonitor-Key"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("86400"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Origin"));
}

pub async fn enforce_cors(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !origin.is_empty() && !is_allowed_origin(origin.as_str(), state.config.runtime_env.as_str())
    {
        let response = (
            StatusCode::FORBIDDEN,
            axum::Json(json!({ "error": "Origin not allowed" })),
        );
        return response.into_response();
    }

    let allow_origin = cors_allow_origin(origin.as_str(), state.config.runtime_env.as_str());

    if request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        set_cors_headers(&mut response, allow_origin.as_str());
        return response;
    }

    let mut response = next.run(request).await;
    set_cors_headers(&mut response, allow_origin.as_str());
    response
}
