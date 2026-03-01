use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListPredictionMarketsRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub query: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationRequest {
    #[serde(default)]
    pub page_size: usize,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListPredictionMarketsResponse {
    pub markets: Vec<PredictionMarket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PredictionMarket {
    pub id: String,
    pub title: String,
    pub yes_price: f64,
    pub volume: f64,
    pub url: String,
    pub closes_at: i64,
    pub category: String,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Clone)]
struct CacheEntry {
    value: ListPredictionMarketsResponse,
    expires_at: Instant,
}

static PREDICTION_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

fn string_value(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|raw| raw.trim().to_string())
        .unwrap_or_default()
}

fn parse_yes_price(market: &Value) -> f64 {
    let Some(raw_prices) = market.get("outcomePrices").and_then(Value::as_str) else {
        return 0.5;
    };

    let Ok(parsed) = serde_json::from_str::<Vec<Value>>(raw_prices) else {
        return 0.5;
    };

    parse_f64(parsed.first()).unwrap_or(0.5)
}

fn map_event(event: &Value, category: &str) -> PredictionMarket {
    let top_market = event
        .get("markets")
        .and_then(Value::as_array)
        .and_then(|markets| markets.first());

    let title = top_market
        .and_then(|market| market.get("question"))
        .and_then(Value::as_str)
        .map(|value| value.to_string())
        .unwrap_or_else(|| string_value(event.get("title")));

    PredictionMarket {
        id: string_value(event.get("id")),
        title,
        yes_price: top_market.map(parse_yes_price).unwrap_or(0.5),
        volume: parse_f64(event.get("volume")).unwrap_or(0.0),
        url: format!(
            "https://polymarket.com/event/{}",
            string_value(event.get("slug"))
        ),
        closes_at: 0,
        category: category.to_string(),
    }
}

fn map_market(market: &Value) -> PredictionMarket {
    let slug = string_value(market.get("slug"));
    PredictionMarket {
        id: slug.clone(),
        title: string_value(market.get("question")),
        yes_price: parse_yes_price(market),
        volume: parse_f64(market.get("volumeNum"))
            .or_else(|| parse_f64(market.get("volume")))
            .unwrap_or(0.0),
        url: format!("https://polymarket.com/market/{slug}"),
        closes_at: 0,
        category: String::new(),
    }
}

fn get_cache(key: &str) -> Result<Option<(ListPredictionMarketsResponse, bool)>, AppError> {
    let cache = PREDICTION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("prediction cache lock poisoned".to_string()))?;
    Ok(cache
        .get(key)
        .map(|entry| (entry.value.clone(), Instant::now() <= entry.expires_at)))
}

fn set_cache(key: String, value: &ListPredictionMarketsResponse) -> Result<(), AppError> {
    let mut cache = PREDICTION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("prediction cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        },
    );
    Ok(())
}

async fn fetch_markets(
    state: &AppState,
    request: &ListPredictionMarketsRequest,
) -> Vec<PredictionMarket> {
    let use_events = !request.category.trim().is_empty();
    let endpoint = if use_events { "events" } else { "markets" };

    let limit = request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(50)
        .min(100);

    let mut url = match reqwest::Url::parse(&format!("{}/{}", GAMMA_BASE, endpoint)) {
        Ok(url) => url,
        Err(_) => return Vec::new(),
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("closed", "false");
        query.append_pair("order", "volume");
        query.append_pair("ascending", "false");
        query.append_pair("limit", &limit.to_string());
        if use_events {
            query.append_pair("tag_slug", request.category.trim());
        }
    }

    let response = match state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return Vec::new(),
    };

    let payload = match response.json::<Value>().await {
        Ok(payload) => payload,
        Err(_) => return Vec::new(),
    };

    let Some(rows) = payload.as_array() else {
        return Vec::new();
    };

    let mut markets = if use_events {
        rows.iter()
            .map(|event| map_event(event, request.category.trim()))
            .collect::<Vec<_>>()
    } else {
        rows.iter().map(map_market).collect::<Vec<_>>()
    };

    if !request.query.trim().is_empty() {
        let needle = request.query.trim().to_ascii_lowercase();
        markets.retain(|market| market.title.to_ascii_lowercase().contains(&needle));
    }

    markets
}

pub async fn list_prediction_markets(
    State(state): State<AppState>,
    Json(request): Json<ListPredictionMarketsRequest>,
) -> Result<Json<ListPredictionMarketsResponse>, AppError> {
    let cache_key = format!(
        "{}:{}:{}",
        request.category.trim().to_ascii_lowercase(),
        request.query.trim().to_ascii_lowercase(),
        request
            .pagination
            .as_ref()
            .map(|pagination| pagination.page_size)
            .unwrap_or(50)
    );

    let stale_cached = get_cache(&cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let markets = fetch_markets(&state, &request).await;
    let response = ListPredictionMarketsResponse {
        markets,
        pagination: None,
    };

    if !response.markets.is_empty() {
        set_cache(cache_key, &response)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListPredictionMarketsResponse {
            markets: Vec::new(),
            pagination: None,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yes_price_from_gamma_string_array() {
        let payload = serde_json::json!({"outcomePrices":"[\"0.73\",\"0.27\"]"});
        assert_eq!(parse_yes_price(&payload), 0.73);
    }

    #[test]
    fn defaults_yes_price_when_missing() {
        let payload = serde_json::json!({});
        assert_eq!(parse_yes_price(&payload), 0.5);
    }
}
