use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use futures::future::join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const FRED_API_BASE: &str = "https://api.stlouisfed.org/fred";
const YAHOO_CHART_BASE: &str = "https://query1.finance.yahoo.com/v8/finance/chart";

const FRED_CACHE_TTL: Duration = Duration::from_secs(3_600);
const WORLD_BANK_CACHE_TTL: Duration = Duration::from_secs(86_400);
const ENERGY_CACHE_TTL: Duration = Duration::from_secs(3_600);
const MACRO_CACHE_TTL: Duration = Duration::from_secs(300);

const DEFAULT_FRED_LIMIT: i32 = 120;
const MAX_FRED_LIMIT: i32 = 1_000;

const TECH_COUNTRIES: [&str; 47] = [
    "USA", "CHN", "JPN", "DEU", "KOR", "GBR", "IND", "ISR", "SGP", "TWN", "FRA", "CAN", "SWE",
    "NLD", "CHE", "FIN", "IRL", "AUS", "BRA", "IDN", "ARE", "SAU", "QAT", "BHR", "EGY", "TUR",
    "MYS", "THA", "VNM", "PHL", "ESP", "ITA", "POL", "CZE", "DNK", "NOR", "AUT", "BEL", "PRT",
    "EST", "MEX", "ARG", "CHL", "COL", "ZAF", "NGA", "KEN",
];

#[derive(Clone)]
struct EiaSeriesConfig {
    commodity: &'static str,
    name: &'static str,
    unit: &'static str,
    api_path: &'static str,
    series_facet: &'static str,
}

const EIA_SERIES: [EiaSeriesConfig; 2] = [
    EiaSeriesConfig {
        commodity: "wti",
        name: "WTI Crude Oil",
        unit: "$/barrel",
        api_path: "/v2/petroleum/pri/spt/data/",
        series_facet: "RWTC",
    },
    EiaSeriesConfig {
        commodity: "brent",
        name: "Brent Crude Oil",
        unit: "$/barrel",
        api_path: "/v2/petroleum/pri/spt/data/",
        series_facet: "RBRTE",
    },
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFredSeriesRequest {
    pub series_id: String,
    #[serde(default)]
    pub limit: i32,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetFredSeriesResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<FredSeries>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FredSeries {
    pub series_id: String,
    pub title: String,
    pub units: String,
    pub frequency: String,
    pub observations: Vec<FredObservation>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FredObservation {
    pub date: String,
    pub value: f64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListWorldBankIndicatorsRequest {
    #[serde(default)]
    pub indicator_code: String,
    #[serde(default)]
    pub country_code: String,
    #[serde(default)]
    pub year: i32,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationRequest {
    #[serde(default)]
    pub page_size: usize,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListWorldBankIndicatorsResponse {
    pub data: Vec<WorldBankCountryData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WorldBankCountryData {
    pub country_code: String,
    pub country_name: String,
    pub indicator_code: String,
    pub indicator_name: String,
    pub year: i32,
    pub value: f64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetEnergyPricesRequest {
    #[serde(default)]
    pub commodities: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetEnergyPricesResponse {
    pub prices: Vec<EnergyPrice>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnergyPrice {
    pub commodity: String,
    pub name: String,
    pub price: f64,
    pub unit: String,
    pub change: f64,
    pub price_at: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetMacroSignalsRequest {}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetMacroSignalsResponse {
    pub timestamp: String,
    pub verdict: String,
    pub bullish_count: i32,
    pub total_count: i32,
    pub signals: MacroSignals,
    pub meta: MacroMeta,
    pub unavailable: bool,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MacroSignals {
    pub liquidity: LiquiditySignal,
    pub flow_structure: FlowStructureSignal,
    pub macro_regime: MacroRegimeSignal,
    pub technical_trend: TechnicalTrendSignal,
    pub hash_rate: HashRateSignal,
    pub mining_cost: MiningCostSignal,
    pub fear_greed: FearGreedSignal,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LiquiditySignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    pub sparkline: Vec<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FlowStructureSignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub btc_return_5: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qqq_return_5: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MacroRegimeSignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qqq_roc_20: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xlp_roc_20: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TechnicalTrendSignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub btc_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sma_50: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sma_200: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vwap_30d: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mayer_multiple: Option<f64>,
    pub sparkline: Vec<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HashRateSignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_30d: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MiningCostSignal {
    pub status: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FearGreedSignal {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<i32>,
    pub history: Vec<FearGreedHistoryEntry>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FearGreedHistoryEntry {
    pub value: i32,
    pub date: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MacroMeta {
    pub qqq_sparkline: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct FredObservationsResponse {
    #[serde(default)]
    observations: Vec<FredObservationRow>,
}

#[derive(Debug, Deserialize, Default)]
struct FredObservationRow {
    #[serde(default)]
    date: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize, Default)]
struct FredSeriesMetadataResponse {
    #[serde(default)]
    seriess: Vec<FredSeriesMetadata>,
}

#[derive(Debug, Deserialize, Default)]
struct FredSeriesMetadata {
    #[serde(default)]
    title: String,
    #[serde(default)]
    units: String,
    #[serde(default)]
    frequency: String,
}

#[derive(Debug, Deserialize, Default)]
struct EiaResponse {
    #[serde(default)]
    response: Option<EiaInnerResponse>,
}

#[derive(Debug, Deserialize, Default)]
struct EiaInnerResponse {
    #[serde(default)]
    data: Vec<EiaRow>,
}

#[derive(Debug, Deserialize, Default)]
struct EiaRow {
    #[serde(default)]
    period: String,
    #[serde(default)]
    value: Option<f64>,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

static FRED_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetFredSeriesResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static WORLD_BANK_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListWorldBankIndicatorsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ENERGY_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetEnergyPricesResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static MACRO_CACHE: Lazy<Mutex<Option<CacheEntry<GetMacroSignalsResponse>>>> =
    Lazy::new(|| Mutex::new(None));

fn round_to(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn cache_key(items: &[String]) -> String {
    let mut normalized = items
        .iter()
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.join(",")
}

fn trim_or_empty(value: &str) -> String {
    value.trim().to_string()
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

fn parse_i32(value: Option<&Value>) -> Option<i32> {
    if let Some(number) = value.and_then(Value::as_i64) {
        return Some(number as i32);
    }
    value
        .and_then(Value::as_str)
        .and_then(|raw| raw.trim().parse::<i32>().ok())
}

fn parse_i64(value: Option<&Value>) -> Option<i64> {
    if let Some(number) = value.and_then(Value::as_i64) {
        return Some(number);
    }
    value
        .and_then(Value::as_str)
        .and_then(|raw| raw.trim().parse::<i64>().ok())
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn rate_of_change(values: &[f64], days: usize) -> Option<f64> {
    if values.len() < days + 1 {
        return None;
    }
    let recent = *values.last()?;
    let past = values[values.len().saturating_sub(1 + days)];
    if past == 0.0 {
        return None;
    }
    Some(((recent - past) / past) * 100.0)
}

fn sma_calc(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period || period == 0 {
        return None;
    }
    let start = values.len() - period;
    let sum = values[start..].iter().sum::<f64>();
    Some(sum / period as f64)
}

fn extract_close_prices(chart: &Value) -> Vec<f64> {
    chart
        .pointer("/chart/result/0/indicators/quote/0/close")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_f64())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn extract_aligned_price_volume(chart: &Value) -> Vec<(f64, f64)> {
    let closes = chart
        .pointer("/chart/result/0/indicators/quote/0/close")
        .and_then(Value::as_array);
    let volumes = chart
        .pointer("/chart/result/0/indicators/quote/0/volume")
        .and_then(Value::as_array);
    let (Some(closes), Some(volumes)) = (closes, volumes) else {
        return Vec::new();
    };

    let mut pairs = Vec::with_capacity(closes.len().min(volumes.len()));
    for index in 0..closes.len().min(volumes.len()) {
        if let (Some(price), Some(volume)) = (closes[index].as_f64(), volumes[index].as_f64()) {
            pairs.push((price, volume));
        }
    }
    pairs
}

fn extract_hashrates(payload: &Value) -> Vec<f64> {
    let list = payload
        .get("hashrates")
        .and_then(Value::as_array)
        .or_else(|| payload.as_array());
    let Some(items) = list else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|entry| {
            entry
                .get("avgHashrate")
                .and_then(Value::as_f64)
                .or_else(|| entry.as_f64())
                .or_else(|| parse_f64(Some(entry)))
        })
        .collect::<Vec<_>>()
}

fn parse_period_to_epoch_ms(period: &str) -> Option<i64> {
    if period.trim().is_empty() {
        return None;
    }

    if let Ok(date) = NaiveDate::parse_from_str(period, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .map(|dt| dt.and_utc().timestamp_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(&format!("{}-01", period), "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .map(|dt| dt.and_utc().timestamp_millis());
    }
    if let Ok(year) = period.parse::<i32>() {
        return NaiveDate::from_ymd_opt(year, 1, 1)
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().timestamp_millis());
    }
    None
}

fn map_slice<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: &str,
) -> Result<Option<(T, bool)>, AppError> {
    let cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    Ok(cache
        .get(key)
        .map(|entry| (entry.value.clone(), Instant::now() <= entry.expires_at)))
}

fn set_map_cache<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: String,
    value: T,
    ttl: Duration,
) -> Result<(), AppError> {
    let mut cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value,
            expires_at: Instant::now() + ttl,
        },
    );
    Ok(())
}

fn macro_cache() -> Result<Option<(GetMacroSignalsResponse, bool)>, AppError> {
    let cache = MACRO_CACHE
        .lock()
        .map_err(|_| AppError::Internal("macro cache lock poisoned".to_string()))?;
    Ok(cache
        .as_ref()
        .map(|entry| (entry.value.clone(), Instant::now() <= entry.expires_at)))
}

fn set_macro_cache(value: &GetMacroSignalsResponse) -> Result<(), AppError> {
    let mut cache = MACRO_CACHE
        .lock()
        .map_err(|_| AppError::Internal("macro cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: value.clone(),
        expires_at: Instant::now() + MACRO_CACHE_TTL,
    });
    Ok(())
}

async fn fetch_json_value(state: &AppState, url: &str) -> Option<Value> {
    let response = state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    response.json::<Value>().await.ok()
}

async fn fetch_fred_series(
    state: &AppState,
    series_id: &str,
    limit: i32,
    api_key: &str,
) -> Option<FredSeries> {
    let observations_url = format!(
        "{}/series/observations?series_id={}&api_key={}&file_type=json&sort_order=desc&limit={}",
        FRED_API_BASE,
        urlencoding::encode(series_id),
        urlencoding::encode(api_key),
        limit
    );
    let metadata_url = format!(
        "{}/series?series_id={}&api_key={}&file_type=json",
        FRED_API_BASE,
        urlencoding::encode(series_id),
        urlencoding::encode(api_key)
    );

    let obs_future = state
        .http_client
        .get(observations_url)
        .header("Accept", "application/json")
        .send();
    let meta_future = state
        .http_client
        .get(metadata_url)
        .header("Accept", "application/json")
        .send();

    let (obs_response, meta_response) = tokio::join!(obs_future, meta_future);

    let obs_response = obs_response.ok()?;
    if !obs_response.status().is_success() {
        return None;
    }

    let obs_payload = obs_response.json::<FredObservationsResponse>().await.ok()?;
    let mut observations = obs_payload
        .observations
        .into_iter()
        .filter_map(|observation| {
            if observation.value.trim() == "." {
                return None;
            }
            let value = observation.value.trim().parse::<f64>().ok()?;
            Some(FredObservation {
                date: observation.date,
                value,
            })
        })
        .collect::<Vec<_>>();
    observations.reverse();

    let mut title = series_id.to_string();
    let mut units = String::new();
    let mut frequency = String::new();

    if let Ok(meta_response) = meta_response
        && meta_response.status().is_success()
        && let Ok(meta) = meta_response.json::<FredSeriesMetadataResponse>().await
        && let Some(series_meta) = meta.seriess.first()
    {
        if !series_meta.title.trim().is_empty() {
            title = series_meta.title.clone();
        }
        units = trim_or_empty(&series_meta.units);
        frequency = trim_or_empty(&series_meta.frequency);
    }

    Some(FredSeries {
        series_id: series_id.to_string(),
        title,
        units,
        frequency,
        observations,
    })
}

async fn fetch_world_bank_data(
    state: &AppState,
    request: &ListWorldBankIndicatorsRequest,
) -> Vec<WorldBankCountryData> {
    let indicator = request.indicator_code.trim();
    if indicator.is_empty() {
        return Vec::new();
    }

    let country_list = if request.country_code.trim().is_empty() {
        TECH_COUNTRIES.join(";")
    } else {
        request.country_code.trim().to_string()
    };

    let current_year = Utc::now().year();
    let years = if request.year > 0 { request.year } else { 5 };
    let start_year = current_year - years;

    let url = format!(
        "https://api.worldbank.org/v2/country/{}/indicator/{}?format=json&date={}:{}&per_page=1000",
        country_list, indicator, start_year, current_year
    );

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

    let rows = payload
        .as_array()
        .and_then(|array| array.get(1))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let indicator_name = rows
        .first()
        .and_then(|row| row.get("indicator"))
        .and_then(|value| value.get("value"))
        .and_then(Value::as_str)
        .map(trim_or_empty)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| indicator.to_string());

    rows.into_iter()
        .filter_map(|row| {
            let country_iso = row
                .get("countryiso3code")
                .and_then(Value::as_str)
                .map(trim_or_empty)
                .unwrap_or_default();
            if country_iso.is_empty() {
                return None;
            }

            let value = parse_f64(row.get("value"))?;
            let year = row
                .get("date")
                .and_then(Value::as_str)
                .and_then(|raw| raw.parse::<i32>().ok())
                .unwrap_or_default();

            let country_name = row
                .get("country")
                .and_then(|value| value.get("value"))
                .and_then(Value::as_str)
                .map(trim_or_empty)
                .unwrap_or_default();

            Some(WorldBankCountryData {
                country_code: country_iso,
                country_name,
                indicator_code: indicator.to_string(),
                indicator_name: indicator_name.clone(),
                year,
                value,
            })
        })
        .collect::<Vec<_>>()
}

async fn fetch_eia_series(
    state: &AppState,
    config: &EiaSeriesConfig,
    api_key: &str,
) -> Option<EnergyPrice> {
    let mut url = reqwest::Url::parse(&format!("https://api.eia.gov{}", config.api_path)).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("api_key", api_key);
        query.append_pair("data[]", "value");
        query.append_pair("frequency", "weekly");
        query.append_pair("facets[series][]", config.series_facet);
        query.append_pair("sort[0][column]", "period");
        query.append_pair("sort[0][direction]", "desc");
        query.append_pair("length", "2");
    }

    let response = state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let payload = response.json::<EiaResponse>().await.ok()?;
    let rows = payload.response?.data;
    if rows.is_empty() {
        return None;
    }

    let current = rows.first()?;
    let previous = rows.get(1);

    let price = current.value.unwrap_or(0.0);
    let previous_price = previous.and_then(|row| row.value).unwrap_or(price);
    let change = if previous_price != 0.0 {
        ((price - previous_price) / previous_price) * 100.0
    } else {
        0.0
    };

    Some(EnergyPrice {
        commodity: config.commodity.to_string(),
        name: config.name.to_string(),
        price,
        unit: config.unit.to_string(),
        change: round_to(change, 1),
        price_at: parse_period_to_epoch_ms(&current.period)
            .unwrap_or_else(|| Utc::now().timestamp_millis()),
    })
}

async fn fetch_energy_prices(
    state: &AppState,
    api_key: &str,
    commodities: &[String],
) -> Vec<EnergyPrice> {
    let requested = commodities
        .iter()
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();

    let selected = if requested.is_empty() {
        EIA_SERIES.to_vec()
    } else {
        EIA_SERIES
            .iter()
            .filter(|series| requested.contains(&series.commodity.to_string()))
            .cloned()
            .collect::<Vec<_>>()
    };

    let jobs = selected
        .iter()
        .map(|series| fetch_eia_series(state, series, api_key));

    join_all(jobs)
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
}

#[cfg(test)]
fn build_fallback_macro_result() -> GetMacroSignalsResponse {
    GetMacroSignalsResponse {
        timestamp: now_iso(),
        verdict: "UNKNOWN".to_string(),
        bullish_count: 0,
        total_count: 0,
        signals: MacroSignals {
            liquidity: LiquiditySignal {
                status: "UNKNOWN".to_string(),
                value: None,
                sparkline: Vec::new(),
            },
            flow_structure: FlowStructureSignal {
                status: "UNKNOWN".to_string(),
                btc_return_5: None,
                qqq_return_5: None,
            },
            macro_regime: MacroRegimeSignal {
                status: "UNKNOWN".to_string(),
                qqq_roc_20: None,
                xlp_roc_20: None,
            },
            technical_trend: TechnicalTrendSignal {
                status: "UNKNOWN".to_string(),
                btc_price: None,
                sma_50: None,
                sma_200: None,
                vwap_30d: None,
                mayer_multiple: None,
                sparkline: Vec::new(),
            },
            hash_rate: HashRateSignal {
                status: "UNKNOWN".to_string(),
                change_30d: None,
            },
            mining_cost: MiningCostSignal {
                status: "UNKNOWN".to_string(),
            },
            fear_greed: FearGreedSignal {
                status: "UNKNOWN".to_string(),
                value: None,
                history: Vec::new(),
            },
        },
        meta: MacroMeta {
            qqq_sparkline: Vec::new(),
        },
        unavailable: true,
    }
}

fn last_n(values: &[f64], n: usize) -> Vec<f64> {
    let len = values.len();
    if len <= n {
        return values.to_vec();
    }
    values[len - n..].to_vec()
}

async fn compute_macro_signals(
    state: &AppState,
    stale_result: Option<GetMacroSignalsResponse>,
) -> GetMacroSignalsResponse {
    let jpy_url = format!("{}/JPY=X?range=1y&interval=1d", YAHOO_CHART_BASE);
    let btc_url = format!("{}/BTC-USD?range=1y&interval=1d", YAHOO_CHART_BASE);
    let qqq_url = format!("{}/QQQ?range=1y&interval=1d", YAHOO_CHART_BASE);
    let xlp_url = format!("{}/XLP?range=1y&interval=1d", YAHOO_CHART_BASE);

    let (jpy_chart, btc_chart, qqq_chart, xlp_chart, fear_greed, mempool_hash) = tokio::join!(
        fetch_json_value(state, &jpy_url),
        fetch_json_value(state, &btc_url),
        fetch_json_value(state, &qqq_url),
        fetch_json_value(state, &xlp_url),
        fetch_json_value(
            state,
            "https://api.alternative.me/fng/?limit=30&format=json"
        ),
        fetch_json_value(state, "https://mempool.space/api/v1/mining/hashrate/1m")
    );

    let jpy_prices = jpy_chart
        .as_ref()
        .map(extract_close_prices)
        .unwrap_or_default();
    let btc_prices = btc_chart
        .as_ref()
        .map(extract_close_prices)
        .unwrap_or_default();
    let btc_aligned = btc_chart
        .as_ref()
        .map(extract_aligned_price_volume)
        .unwrap_or_default();
    let qqq_prices = qqq_chart
        .as_ref()
        .map(extract_close_prices)
        .unwrap_or_default();
    let xlp_prices = xlp_chart
        .as_ref()
        .map(extract_close_prices)
        .unwrap_or_default();

    let jpy_roc_30 = rate_of_change(&jpy_prices, 30);
    let liquidity_status = match jpy_roc_30 {
        Some(value) if value < -2.0 => "SQUEEZE",
        Some(_) => "NORMAL",
        None => "UNKNOWN",
    }
    .to_string();

    let btc_return_5 = rate_of_change(&btc_prices, 5);
    let qqq_return_5 = rate_of_change(&qqq_prices, 5);
    let flow_status = match (btc_return_5, qqq_return_5) {
        (Some(btc), Some(qqq)) => {
            if (btc - qqq).abs() > 5.0 {
                "PASSIVE GAP"
            } else {
                "ALIGNED"
            }
        }
        _ => "UNKNOWN",
    }
    .to_string();

    let qqq_roc_20 = rate_of_change(&qqq_prices, 20);
    let xlp_roc_20 = rate_of_change(&xlp_prices, 20);
    let regime_status = match (qqq_roc_20, xlp_roc_20) {
        (Some(qqq), Some(xlp)) => {
            if qqq > xlp {
                "RISK-ON"
            } else {
                "DEFENSIVE"
            }
        }
        _ => "UNKNOWN",
    }
    .to_string();

    let btc_sma_50 = sma_calc(&btc_prices, 50);
    let btc_sma_200 = sma_calc(&btc_prices, 200);
    let btc_current = btc_prices.last().copied();

    let btc_vwap_30 = if btc_aligned.len() >= 30 {
        let window = &btc_aligned[btc_aligned.len() - 30..];
        let sum_pv = window
            .iter()
            .map(|(price, volume)| price * volume)
            .sum::<f64>();
        let sum_v = window.iter().map(|(_, volume)| *volume).sum::<f64>();
        (sum_v > 0.0).then_some((sum_pv / sum_v).round())
    } else {
        None
    };

    let trend_status = match (btc_current, btc_sma_50) {
        (Some(current), Some(sma50)) => {
            let above_sma = current > sma50 * 1.02;
            let below_sma = current < sma50 * 0.98;
            let above_vwap = btc_vwap_30.map(|vwap| current > vwap);

            if above_sma && above_vwap != Some(false) {
                "BULLISH"
            } else if below_sma && above_vwap != Some(true) {
                "BEARISH"
            } else {
                "NEUTRAL"
            }
        }
        _ => "UNKNOWN",
    }
    .to_string();

    let mayer_multiple = match (btc_current, btc_sma_200) {
        (Some(current), Some(sma200)) if sma200 > 0.0 => Some(round_to(current / sma200, 2)),
        _ => None,
    };

    let hashrates = mempool_hash
        .as_ref()
        .map(extract_hashrates)
        .unwrap_or_default();
    let hash_change = if hashrates.len() >= 2 {
        let recent = *hashrates.last().unwrap_or(&0.0);
        let older = *hashrates.first().unwrap_or(&0.0);
        if older > 0.0 {
            Some(round_to(((recent - older) / older) * 100.0, 1))
        } else {
            None
        }
    } else {
        None
    };

    let hash_status = match hash_change {
        Some(change) if change > 3.0 => "GROWING",
        Some(change) if change < -3.0 => "DECLINING",
        Some(_) => "STABLE",
        None => "UNKNOWN",
    }
    .to_string();

    let mining_status = match (btc_current, hash_change) {
        (Some(price), Some(_)) if price > 60_000.0 => "PROFITABLE",
        (Some(price), Some(_)) if price > 40_000.0 => "TIGHT",
        (Some(_), Some(_)) => "SQUEEZE",
        _ => "UNKNOWN",
    }
    .to_string();

    let mut fear_greed_status = "UNKNOWN".to_string();
    let mut fear_greed_value = None;
    let mut fear_greed_history = Vec::new();
    if let Some(items) = fear_greed
        .as_ref()
        .and_then(|payload| payload.get("data"))
        .and_then(Value::as_array)
    {
        if let Some(first) = items.first() {
            fear_greed_value = parse_i32(first.get("value"));
            fear_greed_status = first
                .get("value_classification")
                .and_then(Value::as_str)
                .map(trim_or_empty)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "UNKNOWN".to_string());
        }

        fear_greed_history = items
            .iter()
            .take(30)
            .filter_map(|item| {
                let value = parse_i32(item.get("value"))?;
                let timestamp = parse_i64(item.get("timestamp"))?;
                let date = Utc
                    .timestamp_opt(timestamp, 0)
                    .single()
                    .map(|dt| dt.format("%Y-%m-%d").to_string())?;
                Some(FearGreedHistoryEntry { value, date })
            })
            .collect::<Vec<_>>();
        fear_greed_history.reverse();
    }

    let btc_sparkline = last_n(&btc_prices, 30);
    let qqq_sparkline = last_n(&qqq_prices, 30);
    let jpy_sparkline = last_n(&jpy_prices, 30);

    let signal_checks = [
        (
            liquidity_status.as_str(),
            liquidity_status.as_str() == "NORMAL",
        ),
        (flow_status.as_str(), flow_status.as_str() == "ALIGNED"),
        (regime_status.as_str(), regime_status.as_str() == "RISK-ON"),
        (trend_status.as_str(), trend_status.as_str() == "BULLISH"),
        (hash_status.as_str(), hash_status.as_str() == "GROWING"),
        (
            mining_status.as_str(),
            mining_status.as_str() == "PROFITABLE",
        ),
        (
            fear_greed_status.as_str(),
            fear_greed_value.is_some_and(|value| value > 50),
        ),
    ];

    let mut bullish_count = 0;
    let mut total_count = 0;
    for (status, bullish) in signal_checks {
        if status != "UNKNOWN" {
            total_count += 1;
            if bullish {
                bullish_count += 1;
            }
        }
    }

    if total_count == 0
        && let Some(stale) = stale_result
        && !stale.unavailable
    {
        return stale;
    }

    let verdict = if total_count == 0 {
        "UNKNOWN"
    } else if (bullish_count as f64 / total_count as f64) >= 0.57 {
        "BUY"
    } else {
        "CASH"
    }
    .to_string();

    GetMacroSignalsResponse {
        timestamp: now_iso(),
        verdict,
        bullish_count,
        total_count,
        signals: MacroSignals {
            liquidity: LiquiditySignal {
                status: liquidity_status,
                value: jpy_roc_30.map(|value| round_to(value, 2)),
                sparkline: jpy_sparkline,
            },
            flow_structure: FlowStructureSignal {
                status: flow_status,
                btc_return_5: btc_return_5.map(|value| round_to(value, 2)),
                qqq_return_5: qqq_return_5.map(|value| round_to(value, 2)),
            },
            macro_regime: MacroRegimeSignal {
                status: regime_status,
                qqq_roc_20: qqq_roc_20.map(|value| round_to(value, 2)),
                xlp_roc_20: xlp_roc_20.map(|value| round_to(value, 2)),
            },
            technical_trend: TechnicalTrendSignal {
                status: trend_status,
                btc_price: btc_current,
                sma_50: btc_sma_50.map(|value| value.round()),
                sma_200: btc_sma_200.map(|value| value.round()),
                vwap_30d: btc_vwap_30,
                mayer_multiple,
                sparkline: btc_sparkline,
            },
            hash_rate: HashRateSignal {
                status: hash_status,
                change_30d: hash_change,
            },
            mining_cost: MiningCostSignal {
                status: mining_status,
            },
            fear_greed: FearGreedSignal {
                status: fear_greed_status,
                value: fear_greed_value,
                history: fear_greed_history,
            },
        },
        meta: MacroMeta { qqq_sparkline },
        unavailable: false,
    }
}

pub async fn get_fred_series(
    State(state): State<AppState>,
    Json(request): Json<GetFredSeriesRequest>,
) -> Result<Json<GetFredSeriesResponse>, AppError> {
    let series_id = request.series_id.trim();
    if series_id.is_empty() {
        return Err(AppError::BadRequest("seriesId is required".to_string()));
    }

    let limit = if request.limit > 0 {
        request.limit.min(MAX_FRED_LIMIT)
    } else {
        DEFAULT_FRED_LIMIT
    };

    let cache_key = format!("{}:{}", series_id, limit);
    let stale_cached = map_slice(&FRED_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let Some(api_key) = state.config.fred_api_key.as_deref() else {
        let fallback = stale_cached
            .map(|(cached, _)| cached)
            .unwrap_or(GetFredSeriesResponse { series: None });
        return Ok(Json(fallback));
    };

    let series = fetch_fred_series(&state, series_id, limit, api_key).await;
    let response = GetFredSeriesResponse { series };

    if response.series.is_some() {
        set_map_cache(&FRED_CACHE, cache_key, response.clone(), FRED_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(
        stale_cached
            .map(|(cached, _)| cached)
            .unwrap_or(GetFredSeriesResponse { series: None }),
    ))
}

pub async fn list_world_bank_indicators(
    State(state): State<AppState>,
    Json(request): Json<ListWorldBankIndicatorsRequest>,
) -> Result<Json<ListWorldBankIndicatorsResponse>, AppError> {
    let indicator = request.indicator_code.trim();
    if indicator.is_empty() {
        return Err(AppError::BadRequest(
            "indicatorCode is required".to_string(),
        ));
    }

    let cache_key = format!(
        "{}:{}:{}",
        indicator,
        if request.country_code.trim().is_empty() {
            "all"
        } else {
            request.country_code.trim()
        },
        if request.year > 0 { request.year } else { 0 },
    );

    let stale_cached = map_slice(&WORLD_BANK_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let data = fetch_world_bank_data(&state, &request).await;
    if !data.is_empty() {
        let response = ListWorldBankIndicatorsResponse {
            data,
            pagination: None,
        };
        set_map_cache(
            &WORLD_BANK_CACHE,
            cache_key,
            response.clone(),
            WORLD_BANK_CACHE_TTL,
        )?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListWorldBankIndicatorsResponse {
            data: Vec::new(),
            pagination: None,
        },
    )))
}

pub async fn get_energy_prices(
    State(state): State<AppState>,
    Json(request): Json<GetEnergyPricesRequest>,
) -> Result<Json<GetEnergyPricesResponse>, AppError> {
    let key = cache_key(&request.commodities);
    let cache_key = if key.is_empty() {
        "all".to_string()
    } else {
        key
    };

    let stale_cached = map_slice(&ENERGY_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let Some(api_key) = state.config.eia_api_key.as_deref() else {
        let fallback = stale_cached
            .map(|(cached, _)| cached)
            .unwrap_or(GetEnergyPricesResponse { prices: Vec::new() });
        return Ok(Json(fallback));
    };

    let prices = fetch_energy_prices(&state, api_key, &request.commodities).await;
    if !prices.is_empty() {
        let response = GetEnergyPricesResponse { prices };
        set_map_cache(&ENERGY_CACHE, cache_key, response.clone(), ENERGY_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(
        stale_cached
            .map(|(cached, _)| cached)
            .unwrap_or(GetEnergyPricesResponse { prices: Vec::new() }),
    ))
}

pub async fn get_macro_signals(
    State(state): State<AppState>,
    Json(_request): Json<GetMacroSignalsRequest>,
) -> Result<Json<GetMacroSignalsResponse>, AppError> {
    let stale_cached = macro_cache()?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let stale_payload = stale_cached.map(|(cached, _)| cached);
    let response = compute_macro_signals(&state, stale_payload.clone()).await;

    let response = if response.unavailable {
        stale_payload
            .filter(|cached| !cached.unavailable)
            .unwrap_or(response)
    } else {
        response
    };

    set_macro_cache(&response)?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_of_change_requires_sufficient_history() {
        assert_eq!(rate_of_change(&[100.0], 1), None);
        assert_eq!(rate_of_change(&[100.0, 110.0], 1), Some(10.0));
    }

    #[test]
    fn sma_calc_requires_period() {
        assert_eq!(sma_calc(&[1.0, 2.0], 3), None);
        assert_eq!(sma_calc(&[1.0, 2.0, 3.0], 2), Some(2.5));
    }

    #[test]
    fn extract_close_prices_handles_missing_shape() {
        let payload = serde_json::json!({"chart": {"result": []}});
        assert!(extract_close_prices(&payload).is_empty());
    }

    #[test]
    fn parse_period_to_epoch_ms_accepts_multiple_formats() {
        assert!(parse_period_to_epoch_ms("2025-01-31").is_some());
        assert!(parse_period_to_epoch_ms("2025-01").is_some());
        assert!(parse_period_to_epoch_ms("2025").is_some());
        assert!(parse_period_to_epoch_ms("not-a-date").is_none());
    }

    #[test]
    fn fallback_macro_result_is_marked_unavailable() {
        let payload = build_fallback_macro_result();
        assert_eq!(payload.verdict, "UNKNOWN");
        assert!(payload.unavailable);
        assert_eq!(payload.total_count, 0);
    }
}
