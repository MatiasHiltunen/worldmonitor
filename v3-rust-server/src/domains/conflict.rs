use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const ACLED_API_URL: &str = "https://acleddata.com/api/acled/read";
const HAPI_URL: &str = "https://hapi.humdata.org/api/v2/coordination-context/conflict-events";
const HAPI_APP_ID_B64: &str = "d29ybGRtb25pdG9yOm1vbml0b3JAd29ybGRtb25pdG9yLmFwcA==";

const UCDP_BASE_URL: &str = "https://ucdpapi.pcr.uu.se/api/gedevents";
const UCDP_PAGE_SIZE: i64 = 1_000;
const UCDP_MAX_PAGES: i64 = 12;
const UCDP_TRAILING_WINDOW_MS: i64 = 365 * 24 * 60 * 60 * 1000;

const ACLED_CACHE_TTL: Duration = Duration::from_secs(900);
const UCDP_CACHE_TTL_FULL: Duration = Duration::from_secs(6 * 60 * 60);
const UCDP_CACHE_TTL_PARTIAL: Duration = Duration::from_secs(10 * 60);
const UCDP_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(60);
const UCDP_VERSION_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const HAPI_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

const ISO2_TO_ISO3: [(&str, &str); 32] = [
    ("US", "USA"),
    ("RU", "RUS"),
    ("CN", "CHN"),
    ("UA", "UKR"),
    ("IR", "IRN"),
    ("IL", "ISR"),
    ("TW", "TWN"),
    ("KP", "PRK"),
    ("SA", "SAU"),
    ("TR", "TUR"),
    ("PL", "POL"),
    ("DE", "DEU"),
    ("FR", "FRA"),
    ("GB", "GBR"),
    ("IN", "IND"),
    ("PK", "PAK"),
    ("SY", "SYR"),
    ("YE", "YEM"),
    ("MM", "MMR"),
    ("VE", "VEN"),
    ("AF", "AFG"),
    ("SD", "SDN"),
    ("SS", "SSD"),
    ("SO", "SOM"),
    ("CD", "COD"),
    ("ET", "ETH"),
    ("IQ", "IRQ"),
    ("CO", "COL"),
    ("NG", "NGA"),
    ("PS", "PSE"),
    ("BR", "BRA"),
    ("AE", "ARE"),
];

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListAcledEventsRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub country: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListUcdpEventsRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub country: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimeRange {
    #[serde(default)]
    pub start: i64,
    #[serde(default)]
    pub end: i64,
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
pub struct ListAcledEventsResponse {
    pub events: Vec<AcledConflictEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListUcdpEventsResponse {
    pub events: Vec<UcdpViolenceEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AcledConflictEvent {
    pub id: String,
    pub event_type: String,
    pub country: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub occurred_at: i64,
    pub fatalities: i32,
    pub actors: Vec<String>,
    pub source: String,
    pub admin1: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UcdpViolenceEvent {
    pub id: String,
    pub date_start: i64,
    pub date_end: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub country: String,
    pub side_a: String,
    pub side_b: String,
    pub deaths_best: i32,
    pub deaths_low: i32,
    pub deaths_high: i32,
    pub violence_type: String,
    pub source_original: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetHumanitarianSummaryRequest {
    #[serde(default)]
    pub country_code: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetHumanitarianSummaryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<HumanitarianCountrySummary>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HumanitarianCountrySummary {
    pub country_code: String,
    pub country_name: String,
    pub conflict_events_total: i32,
    pub conflict_political_violence_events: i32,
    pub conflict_fatalities: i32,
    pub reference_period: String,
    pub conflict_demonstrations: i32,
    pub updated_at: i64,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

#[derive(Clone, Default)]
struct HapiCountryAgg {
    location_name: String,
    month: String,
    events_total: i32,
    events_political_violence: i32,
    events_civilian_targeting: i32,
    events_demonstrations: i32,
    fatalities_political_violence: i32,
    fatalities_civilian_targeting: i32,
}

static ACLED_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListAcledEventsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static UCDP_CACHE: Lazy<Mutex<Option<CacheEntry<Vec<UcdpViolenceEvent>>>>> =
    Lazy::new(|| Mutex::new(None));
static UCDP_FALLBACK_CACHE: Lazy<Mutex<Option<CacheEntry<Vec<UcdpViolenceEvent>>>>> =
    Lazy::new(|| Mutex::new(None));
static UCDP_NEGATIVE_CACHE: Lazy<Mutex<Option<Instant>>> = Lazy::new(|| Mutex::new(None));
static UCDP_VERSION_CACHE: Lazy<Mutex<Option<CacheEntry<String>>>> = Lazy::new(|| Mutex::new(None));
static HAPI_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetHumanitarianSummaryResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

fn parse_i32(value: Option<&Value>) -> i32 {
    if let Some(number) = value.and_then(Value::as_i64) {
        return number as i32;
    }
    value
        .and_then(Value::as_str)
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

fn value_string(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|raw| raw.trim().to_string())
        .unwrap_or_default()
}

fn truncate_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect::<String>()
}

fn pagination_size(pagination: Option<&PaginationRequest>, default: usize, max: usize) -> usize {
    pagination
        .map(|p| p.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(default)
        .min(max)
}

fn in_time_range(epoch_ms: i64, range: Option<&TimeRange>) -> bool {
    let Some(range) = range else {
        return true;
    };
    if range.start > 0 && epoch_ms < range.start {
        return false;
    }
    if range.end > 0 && epoch_ms > range.end {
        return false;
    }
    true
}

fn country_code_valid(value: &str) -> bool {
    value.len() == 2 && value.chars().all(|c| c.is_ascii_uppercase())
}

fn iso2_to_iso3(code: &str) -> Option<&'static str> {
    ISO2_TO_ISO3
        .iter()
        .find_map(|(iso2, iso3)| (*iso2 == code).then_some(*iso3))
}

fn map_violence_type(value: i32) -> String {
    match value {
        1 => "UCDP_VIOLENCE_TYPE_STATE_BASED",
        2 => "UCDP_VIOLENCE_TYPE_NON_STATE",
        3 => "UCDP_VIOLENCE_TYPE_ONE_SIDED",
        _ => "UCDP_VIOLENCE_TYPE_UNSPECIFIED",
    }
    .to_string()
}

fn parse_date_ms(value: Option<&Value>) -> i64 {
    let Some(raw) = value.and_then(Value::as_str) else {
        return 0;
    };
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|date| date.and_utc().timestamp_millis())
        .unwrap_or(0)
}

fn date_to_iso(ms: i64) -> String {
    if ms <= 0 {
        return Utc::now().format("%Y-%m-%d").to_string();
    }
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string())
}

fn get_map_cache<T: Clone>(
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
    value: &T,
    ttl: Duration,
) -> Result<(), AppError> {
    let mut cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + ttl,
        },
    );
    Ok(())
}

fn get_ucdp_cache(
    cache: &Mutex<Option<CacheEntry<Vec<UcdpViolenceEvent>>>>,
) -> Result<Option<(Vec<UcdpViolenceEvent>, bool)>, AppError> {
    let cache = cache
        .lock()
        .map_err(|_| AppError::Internal("ucdp cache lock poisoned".to_string()))?;
    Ok(cache
        .as_ref()
        .map(|entry| (entry.value.clone(), Instant::now() <= entry.expires_at)))
}

fn set_ucdp_cache(
    cache: &Mutex<Option<CacheEntry<Vec<UcdpViolenceEvent>>>>,
    value: &[UcdpViolenceEvent],
    ttl: Duration,
) -> Result<(), AppError> {
    let mut cache = cache
        .lock()
        .map_err(|_| AppError::Internal("ucdp cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: value.to_vec(),
        expires_at: Instant::now() + ttl,
    });
    Ok(())
}

fn set_ucdp_negative_backoff() -> Result<(), AppError> {
    let mut cache = UCDP_NEGATIVE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("ucdp cache lock poisoned".to_string()))?;
    *cache = Some(Instant::now() + UCDP_NEGATIVE_CACHE_TTL);
    Ok(())
}

fn should_skip_ucdp_fetch() -> Result<bool, AppError> {
    let cache = UCDP_NEGATIVE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("ucdp cache lock poisoned".to_string()))?;
    Ok(cache.is_some_and(|deadline| Instant::now() < deadline))
}

fn clear_ucdp_negative_backoff() -> Result<(), AppError> {
    let mut cache = UCDP_NEGATIVE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("ucdp cache lock poisoned".to_string()))?;
    *cache = None;
    Ok(())
}

fn version_candidates() -> Vec<String> {
    let year = Utc::now().year() - 2000;
    let mut candidates = vec![
        format!("{}.1", year),
        format!("{}.1", year - 1),
        "25.1".to_string(),
        "24.1".to_string(),
    ];
    candidates.sort();
    candidates.dedup();
    candidates
}

fn get_cached_ucdp_version() -> Result<Option<String>, AppError> {
    let cache = UCDP_VERSION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("ucdp version cache lock poisoned".to_string()))?;
    Ok(cache
        .as_ref()
        .and_then(|entry| (Instant::now() <= entry.expires_at).then_some(entry.value.clone())))
}

fn set_cached_ucdp_version(version: &str) -> Result<(), AppError> {
    let mut cache = UCDP_VERSION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("ucdp version cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: version.to_string(),
        expires_at: Instant::now() + UCDP_VERSION_CACHE_TTL,
    });
    Ok(())
}

async fn fetch_ucdp_page(state: &AppState, version: &str, page: i64) -> Option<Value> {
    let url = format!(
        "{}/{version}?pagesize={}&page={page}",
        UCDP_BASE_URL, UCDP_PAGE_SIZE
    );

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

    let payload = response.json::<Value>().await.ok()?;
    payload
        .get("Result")
        .and_then(Value::as_array)
        .is_some()
        .then_some(payload)
}

async fn discover_ucdp_version(state: &AppState) -> Option<(String, Value)> {
    if let Ok(Some(version)) = get_cached_ucdp_version()
        && let Some(page0) = fetch_ucdp_page(state, &version, 0).await
    {
        return Some((version, page0));
    }

    for version in version_candidates() {
        if let Some(page0) = fetch_ucdp_page(state, &version, 0).await {
            let _ = set_cached_ucdp_version(&version);
            return Some((version, page0));
        }
    }

    None
}

fn get_latest_date_ms(events: &[Value]) -> i64 {
    events
        .iter()
        .filter_map(|event| {
            event
                .get("date_start")
                .and_then(Value::as_str)
                .and_then(|raw| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|date| date.and_utc().timestamp_millis())
        })
        .max()
        .unwrap_or(0)
}

async fn fetch_acled_events(
    state: &AppState,
    request: &ListAcledEventsRequest,
) -> Vec<AcledConflictEvent> {
    let Some(token) = state.config.acled_access_token.as_deref() else {
        return Vec::new();
    };

    let now = now_ms();
    let start_ms = request
        .time_range
        .as_ref()
        .map(|range| range.start)
        .filter(|start| *start > 0)
        .unwrap_or(now - 30 * 24 * 60 * 60 * 1000);
    let end_ms = request
        .time_range
        .as_ref()
        .map(|range| range.end)
        .filter(|end| *end > 0)
        .unwrap_or(now);

    let start_date = date_to_iso(start_ms);
    let end_date = date_to_iso(end_ms);

    let mut url = match reqwest::Url::parse(ACLED_API_URL) {
        Ok(url) => url,
        Err(_) => return Vec::new(),
    };

    {
        let mut query = url.query_pairs_mut();
        query.append_pair(
            "event_type",
            "Battles|Explosions/Remote violence|Violence against civilians",
        );
        query.append_pair("event_date", &format!("{}|{}", start_date, end_date));
        query.append_pair("event_date_where", "BETWEEN");
        query.append_pair("limit", "500");
        query.append_pair("_format", "json");
        if !request.country.trim().is_empty() {
            query.append_pair("country", request.country.trim());
        }
    }

    let response = match state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("Authorization", format!("Bearer {token}"))
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
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    rows.into_iter()
        .filter_map(|row| {
            let lat = parse_f64(row.get("latitude"))?;
            let lon = parse_f64(row.get("longitude"))?;
            if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                return None;
            }

            let occurred_at = parse_date_ms(row.get("event_date"));
            if !in_time_range(occurred_at, request.time_range.as_ref()) {
                return None;
            }

            let actor1 = value_string(row.get("actor1"));
            let actor2 = value_string(row.get("actor2"));
            let actors = [actor1, actor2]
                .into_iter()
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();

            Some(AcledConflictEvent {
                id: format!("acled-{}", value_string(row.get("event_id_cnty"))),
                event_type: value_string(row.get("event_type")),
                country: value_string(row.get("country")),
                location: Some(GeoCoordinates {
                    latitude: lat,
                    longitude: lon,
                }),
                occurred_at,
                fatalities: parse_i32(row.get("fatalities")),
                actors,
                source: value_string(row.get("source")),
                admin1: value_string(row.get("admin1")),
            })
        })
        .collect::<Vec<_>>()
}

async fn fetch_ucdp_events(
    state: &AppState,
    request: &ListUcdpEventsRequest,
) -> Vec<UcdpViolenceEvent> {
    if should_skip_ucdp_fetch().unwrap_or(false) {
        if let Ok(Some((cached, true))) = get_ucdp_cache(&UCDP_FALLBACK_CACHE) {
            return cached;
        }
        return Vec::new();
    }

    let Some((version, page0)) = discover_ucdp_version(state).await else {
        let _ = set_ucdp_negative_backoff();
        if let Ok(Some((cached, true))) = get_ucdp_cache(&UCDP_FALLBACK_CACHE) {
            return cached;
        }
        return Vec::new();
    };

    let total_pages = page0
        .get("TotalPages")
        .and_then(Value::as_i64)
        .unwrap_or(1)
        .max(1);
    let newest_page = total_pages - 1;

    let mut all_rows = Vec::new();
    let mut latest_dataset_ms = 0;
    let mut failed_pages = 0;

    for offset in 0..UCDP_MAX_PAGES {
        if newest_page < offset {
            break;
        }

        let page_number = newest_page - offset;
        let payload = if page_number == 0 {
            Some(page0.clone())
        } else {
            fetch_ucdp_page(state, &version, page_number).await
        };

        let Some(payload) = payload else {
            failed_pages += 1;
            continue;
        };

        let rows = payload
            .get("Result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if latest_dataset_ms == 0 {
            latest_dataset_ms = get_latest_date_ms(&rows);
        }

        all_rows.extend(rows);
    }

    let is_partial = failed_pages > 0;
    let cutoff = if latest_dataset_ms > 0 {
        latest_dataset_ms - UCDP_TRAILING_WINDOW_MS
    } else {
        0
    };

    let mut events = all_rows
        .into_iter()
        .filter_map(|row| {
            let date_start = parse_date_ms(row.get("date_start"));
            if cutoff > 0 && date_start < cutoff {
                return None;
            }

            let country = value_string(row.get("country"));
            if !request.country.trim().is_empty() && country != request.country.trim() {
                return None;
            }

            let latitude = parse_f64(row.get("latitude"));
            let longitude = parse_f64(row.get("longitude"));
            let location = match (latitude, longitude) {
                (Some(lat), Some(lon))
                    if (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon) =>
                {
                    Some(GeoCoordinates {
                        latitude: lat,
                        longitude: lon,
                    })
                }
                _ => None,
            };

            Some(UcdpViolenceEvent {
                id: value_string(row.get("id")),
                date_start,
                date_end: parse_date_ms(row.get("date_end")),
                location,
                country,
                side_a: truncate_chars(&value_string(row.get("side_a")), 200),
                side_b: truncate_chars(&value_string(row.get("side_b")), 200),
                deaths_best: parse_i32(row.get("best")),
                deaths_low: parse_i32(row.get("low")),
                deaths_high: parse_i32(row.get("high")),
                violence_type: map_violence_type(
                    row.get("type_of_violence")
                        .and_then(Value::as_i64)
                        .unwrap_or_default() as i32,
                ),
                source_original: truncate_chars(&value_string(row.get("source_original")), 300),
            })
        })
        .collect::<Vec<_>>();

    events.sort_by(|a, b| b.date_start.cmp(&a.date_start));

    if events.is_empty() {
        let _ = set_ucdp_negative_backoff();
        if let Ok(Some((cached, true))) = get_ucdp_cache(&UCDP_FALLBACK_CACHE) {
            return cached;
        }
        return Vec::new();
    }

    let ttl = if is_partial {
        UCDP_CACHE_TTL_PARTIAL
    } else {
        UCDP_CACHE_TTL_FULL
    };

    let _ = set_ucdp_cache(&UCDP_CACHE, &events, ttl);
    let _ = set_ucdp_cache(&UCDP_FALLBACK_CACHE, &events, ttl);
    let _ = clear_ucdp_negative_backoff();

    events
}

async fn fetch_hapi_summary(
    state: &AppState,
    country_code: &str,
) -> Option<HumanitarianCountrySummary> {
    let iso3 = iso2_to_iso3(country_code)?;

    let mut url = reqwest::Url::parse(HAPI_URL).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("output_format", "json");
        query.append_pair("limit", "1000");
        query.append_pair("offset", "0");
        query.append_pair("app_identifier", HAPI_APP_ID_B64);
        query.append_pair("location_code", iso3);
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

    let payload = response.json::<Value>().await.ok()?;
    let rows = payload
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut by_country: HashMap<String, HapiCountryAgg> = HashMap::new();

    for row in rows {
        let iso3_value = value_string(row.get("location_code"));
        if iso3_value.is_empty() {
            continue;
        }

        let month = value_string(row.get("reference_period_start"));
        let event_type = value_string(row.get("event_type")).to_ascii_lowercase();
        let events = parse_i32(row.get("events"));
        let fatalities = parse_i32(row.get("fatalities"));

        let entry = by_country
            .entry(iso3_value)
            .or_insert_with(HapiCountryAgg::default);

        if entry.location_name.is_empty() {
            entry.location_name = value_string(row.get("location_name"));
        }

        if month > entry.month {
            entry.month = month.clone();
            entry.events_total = 0;
            entry.events_political_violence = 0;
            entry.events_civilian_targeting = 0;
            entry.events_demonstrations = 0;
            entry.fatalities_political_violence = 0;
            entry.fatalities_civilian_targeting = 0;
        }

        if month == entry.month {
            entry.events_total += events;
            if event_type.contains("political_violence") {
                entry.events_political_violence += events;
                entry.fatalities_political_violence += fatalities;
            }
            if event_type.contains("civilian_targeting") {
                entry.events_civilian_targeting += events;
                entry.fatalities_civilian_targeting += fatalities;
            }
            if event_type.contains("demonstration") {
                entry.events_demonstrations += events;
            }
        }
    }

    let entry = by_country.get(iso3)?;
    Some(HumanitarianCountrySummary {
        country_code: country_code.to_string(),
        country_name: entry.location_name.clone(),
        conflict_events_total: entry.events_total,
        conflict_political_violence_events: entry.events_political_violence
            + entry.events_civilian_targeting,
        conflict_fatalities: entry.fatalities_political_violence
            + entry.fatalities_civilian_targeting,
        reference_period: entry.month.clone(),
        conflict_demonstrations: entry.events_demonstrations,
        updated_at: now_ms(),
    })
}

pub async fn list_acled_events(
    State(state): State<AppState>,
    Json(request): Json<ListAcledEventsRequest>,
) -> Result<Json<ListAcledEventsResponse>, AppError> {
    let cache_key = format!(
        "{}:{}:{}",
        request.country.trim().to_ascii_uppercase(),
        request
            .time_range
            .as_ref()
            .map(|range| range.start)
            .unwrap_or(0),
        request
            .time_range
            .as_ref()
            .map(|range| range.end)
            .unwrap_or(0)
    );

    let stale_cached = get_map_cache(&ACLED_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let events = fetch_acled_events(&state, &request).await;
    let page_size = pagination_size(request.pagination.as_ref(), 500, 1_000);
    let total_count = events.len();
    let response = ListAcledEventsResponse {
        events: events.into_iter().take(page_size).collect::<Vec<_>>(),
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };

    if !response.events.is_empty() {
        set_map_cache(&ACLED_CACHE, cache_key, &response, ACLED_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListAcledEventsResponse {
            events: Vec::new(),
            pagination: None,
        },
    )))
}

pub async fn list_ucdp_events(
    State(state): State<AppState>,
    Json(request): Json<ListUcdpEventsRequest>,
) -> Result<Json<ListUcdpEventsResponse>, AppError> {
    if let Some((cached, true)) = get_ucdp_cache(&UCDP_CACHE)? {
        let events = if request.country.trim().is_empty() {
            cached
        } else {
            cached
                .into_iter()
                .filter(|event| event.country == request.country.trim())
                .collect::<Vec<_>>()
        };
        return Ok(Json(ListUcdpEventsResponse {
            events,
            pagination: None,
        }));
    }

    if let Some((cached, true)) = get_ucdp_cache(&UCDP_FALLBACK_CACHE)? {
        let events = if request.country.trim().is_empty() {
            cached
        } else {
            cached
                .into_iter()
                .filter(|event| event.country == request.country.trim())
                .collect::<Vec<_>>()
        };
        return Ok(Json(ListUcdpEventsResponse {
            events,
            pagination: None,
        }));
    }

    let events = fetch_ucdp_events(&state, &request).await;
    let page_size = pagination_size(request.pagination.as_ref(), 500, 1_000);
    let total_count = events.len();

    Ok(Json(ListUcdpEventsResponse {
        events: events.into_iter().take(page_size).collect::<Vec<_>>(),
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    }))
}

pub async fn get_humanitarian_summary(
    State(state): State<AppState>,
    Json(request): Json<GetHumanitarianSummaryRequest>,
) -> Result<Json<GetHumanitarianSummaryResponse>, AppError> {
    let country_code = request.country_code.trim().to_ascii_uppercase();
    if !country_code_valid(&country_code) {
        return Err(AppError::BadRequest(
            "countryCode must be a 2-letter uppercase ISO code".to_string(),
        ));
    }

    let stale_cached = get_map_cache(&HAPI_CACHE, &country_code)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let summary = fetch_hapi_summary(&state, &country_code).await;
    let response = GetHumanitarianSummaryResponse { summary };

    if response.summary.is_some() {
        set_map_cache(&HAPI_CACHE, country_code, &response, HAPI_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        GetHumanitarianSummaryResponse { summary: None },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_iso2_country_codes() {
        assert!(country_code_valid("US"));
        assert!(country_code_valid("YE"));
        assert!(!country_code_valid("usa"));
        assert!(!country_code_valid("U"));
    }

    #[test]
    fn maps_ucdp_violence_types() {
        assert_eq!(map_violence_type(1), "UCDP_VIOLENCE_TYPE_STATE_BASED");
        assert_eq!(map_violence_type(2), "UCDP_VIOLENCE_TYPE_NON_STATE");
        assert_eq!(map_violence_type(3), "UCDP_VIOLENCE_TYPE_ONE_SIDED");
        assert_eq!(map_violence_type(9), "UCDP_VIOLENCE_TYPE_UNSPECIFIED");
    }

    #[test]
    fn parses_date_to_epoch_ms() {
        let value = Value::String("2025-02-20".to_string());
        assert!(parse_date_ms(Some(&value)) > 0);
    }
}
