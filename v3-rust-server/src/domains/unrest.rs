use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{DateTime, NaiveDate, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const ACLED_API_URL: &str = "https://acleddata.com/api/acled/read";
const GDELT_GEO_URL: &str = "https://api.gdeltproject.org/api/v2/geo/geo";
const UNREST_CACHE_TTL: Duration = Duration::from_secs(900);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListUnrestEventsRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub min_severity: String,
    #[serde(default)]
    pub bounding_box: Option<BoundingBox>,
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

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BoundingBox {
    #[serde(default)]
    pub north_east: GeoCoordinates,
    #[serde(default)]
    pub south_west: GeoCoordinates,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    #[serde(default)]
    pub latitude: f64,
    #[serde(default)]
    pub longitude: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListUnrestEventsResponse {
    pub events: Vec<UnrestEvent>,
    pub clusters: Vec<UnrestCluster>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UnrestEvent {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub event_type: String,
    pub city: String,
    pub country: String,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub occurred_at: i64,
    pub severity: String,
    pub fatalities: i32,
    pub sources: Vec<String>,
    pub source_type: String,
    pub tags: Vec<String>,
    pub actors: Vec<String>,
    pub confidence: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UnrestCluster {
    pub id: String,
    pub country: String,
    pub region: String,
    pub event_count: i32,
    pub events: Vec<UnrestEvent>,
    pub severity: String,
    pub start_at: i64,
    pub end_at: i64,
    pub primary_cause: String,
}

#[derive(Clone)]
struct CacheEntry {
    response: ListUnrestEventsResponse,
    expires_at: Instant,
}

static UNREST_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn truncate_chars(input: &str, limit: usize) -> String {
    input.chars().take(limit).collect::<String>()
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
        .map(|text| text.trim().to_string())
        .unwrap_or_default()
}

fn parse_date_to_epoch_ms(date: &str) -> i64 {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .ok()
        .and_then(|day| day.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp_millis())
        .unwrap_or_else(now_epoch_ms)
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "SEVERITY_LEVEL_HIGH" => 0,
        "SEVERITY_LEVEL_MEDIUM" => 1,
        "SEVERITY_LEVEL_LOW" => 2,
        _ => 3,
    }
}

fn severity_threshold(value: &str) -> u8 {
    match value {
        "SEVERITY_LEVEL_HIGH" => 3,
        "SEVERITY_LEVEL_MEDIUM" => 2,
        "SEVERITY_LEVEL_LOW" => 1,
        _ => 0,
    }
}

fn matches_min_severity(event: &UnrestEvent, minimum: &str) -> bool {
    let required = severity_threshold(minimum);
    if required == 0 {
        return true;
    }
    severity_threshold(event.severity.as_str()) >= required
}

fn map_acled_event_type(event_type: &str, sub_event_type: &str) -> String {
    let lower = format!("{} {}", event_type, sub_event_type).to_ascii_lowercase();
    if lower.contains("riot") || lower.contains("mob violence") {
        "UNREST_EVENT_TYPE_RIOT".to_string()
    } else if lower.contains("strike") {
        "UNREST_EVENT_TYPE_STRIKE".to_string()
    } else if lower.contains("demonstration") {
        "UNREST_EVENT_TYPE_DEMONSTRATION".to_string()
    } else if lower.contains("protest") {
        "UNREST_EVENT_TYPE_PROTEST".to_string()
    } else {
        "UNREST_EVENT_TYPE_CIVIL_UNREST".to_string()
    }
}

fn classify_severity(fatalities: i32, event_type: &str) -> String {
    let lower = event_type.to_ascii_lowercase();
    if fatalities > 0 || lower.contains("riot") {
        "SEVERITY_LEVEL_HIGH".to_string()
    } else if lower.contains("protest") {
        "SEVERITY_LEVEL_MEDIUM".to_string()
    } else {
        "SEVERITY_LEVEL_LOW".to_string()
    }
}

fn classify_gdelt_severity(count: i64, name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if count > 100 || lower.contains("riot") || lower.contains("clash") {
        "SEVERITY_LEVEL_HIGH".to_string()
    } else if count < 25 {
        "SEVERITY_LEVEL_LOW".to_string()
    } else {
        "SEVERITY_LEVEL_MEDIUM".to_string()
    }
}

fn classify_gdelt_event_type(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.contains("riot") {
        "UNREST_EVENT_TYPE_RIOT".to_string()
    } else if lower.contains("strike") {
        "UNREST_EVENT_TYPE_STRIKE".to_string()
    } else if lower.contains("demonstration") {
        "UNREST_EVENT_TYPE_DEMONSTRATION".to_string()
    } else {
        "UNREST_EVENT_TYPE_PROTEST".to_string()
    }
}

fn merge_sources(base: &[String], incoming: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    base.iter()
        .chain(incoming.iter())
        .filter_map(|source| {
            let normalized = source.trim();
            (!normalized.is_empty() && seen.insert(normalized.to_string()))
                .then_some(normalized.to_string())
        })
        .collect::<Vec<_>>()
}

fn deduplication_key(event: &UnrestEvent) -> String {
    let (lat, lon) = event
        .location
        .as_ref()
        .map(|location| (location.latitude, location.longitude))
        .unwrap_or((0.0, 0.0));
    let lat_key = (lat * 10.0).round() / 10.0;
    let lon_key = (lon * 10.0).round() / 10.0;
    let date_key = DateTime::from_timestamp_millis(event.occurred_at)
        .map(|ts| ts.date_naive().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{lat_key:.1}:{lon_key:.1}:{date_key}")
}

fn deduplicate_events(events: Vec<UnrestEvent>) -> Vec<UnrestEvent> {
    let mut unique: HashMap<String, UnrestEvent> = HashMap::new();

    for event in events {
        let key = deduplication_key(&event);
        if let Some(existing) = unique.get_mut(&key) {
            if event.source_type == "UNREST_SOURCE_TYPE_ACLED"
                && existing.source_type != "UNREST_SOURCE_TYPE_ACLED"
            {
                let mut replacement = event.clone();
                replacement.sources = merge_sources(&event.sources, &existing.sources);
                unique.insert(key, replacement);
                continue;
            }

            if existing.source_type == "UNREST_SOURCE_TYPE_ACLED" {
                existing.sources = merge_sources(&existing.sources, &event.sources);
            } else {
                existing.sources = merge_sources(&existing.sources, &event.sources);
                if existing.sources.len() >= 2 {
                    existing.confidence = "CONFIDENCE_LEVEL_HIGH".to_string();
                }
            }
            continue;
        }

        unique.insert(key, event);
    }

    unique.into_values().collect::<Vec<_>>()
}

fn sort_by_severity_and_recency(events: &mut [UnrestEvent]) {
    events.sort_by(|left, right| {
        let severity_diff =
            severity_rank(left.severity.as_str()).cmp(&severity_rank(right.severity.as_str()));
        if severity_diff != std::cmp::Ordering::Equal {
            return severity_diff;
        }
        right.occurred_at.cmp(&left.occurred_at)
    });
}

fn in_bounding_box(location: &Option<GeoCoordinates>, bounding_box: Option<&BoundingBox>) -> bool {
    let Some(bounding_box) = bounding_box else {
        return true;
    };
    let Some(location) = location else {
        return false;
    };

    location.latitude <= bounding_box.north_east.latitude
        && location.latitude >= bounding_box.south_west.latitude
        && location.longitude <= bounding_box.north_east.longitude
        && location.longitude >= bounding_box.south_west.longitude
}

fn page_size(request: &ListUnrestEventsRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn get_cached(cache_key: &str) -> Result<Option<ListUnrestEventsResponse>, AppError> {
    let cache = UNREST_CACHE
        .lock()
        .map_err(|_| AppError::Internal("unrest cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(cache_key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.response.clone()));
    }
    Ok(None)
}

fn set_cached(cache_key: &str, response: &ListUnrestEventsResponse) -> Result<(), AppError> {
    let mut cache = UNREST_CACHE
        .lock()
        .map_err(|_| AppError::Internal("unrest cache lock poisoned".to_string()))?;
    cache.insert(
        cache_key.to_string(),
        CacheEntry {
            response: response.clone(),
            expires_at: Instant::now() + UNREST_CACHE_TTL,
        },
    );
    Ok(())
}

async fn fetch_acled_protests(
    state: &AppState,
    request: &ListUnrestEventsRequest,
) -> Vec<UnrestEvent> {
    let Some(token) = state.config.acled_access_token.as_deref() else {
        return Vec::new();
    };

    let now = now_epoch_ms();
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

    let start_date = DateTime::from_timestamp_millis(start_ms)
        .map(|date| date.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| Utc::now().date_naive().format("%Y-%m-%d").to_string());
    let end_date = DateTime::from_timestamp_millis(end_ms)
        .map(|date| date.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| Utc::now().date_naive().format("%Y-%m-%d").to_string());

    let mut params = vec![
        ("event_type", "Protests".to_string()),
        ("event_date", format!("{}|{}", start_date, end_date)),
        ("event_date_where", "BETWEEN".to_string()),
        ("limit", "500".to_string()),
        ("_format", "json".to_string()),
    ];

    if !request.country.trim().is_empty() {
        params.push(("country", request.country.trim().to_string()));
    }

    let query = params
        .into_iter()
        .map(|(key, value)| format!("{}={}", key, urlencoding::encode(&value)))
        .collect::<Vec<_>>()
        .join("&");

    let response = match state
        .http_client
        .get(format!("{}?{}", ACLED_API_URL, query))
        .header("Accept", "application/json")
        .header("Authorization", format!("Bearer {}", token))
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

    let events = payload
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    events
        .iter()
        .filter_map(|event| {
            let latitude = parse_f64(event.get("latitude"))?;
            let longitude = parse_f64(event.get("longitude"))?;
            if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
                return None;
            }

            let notes = value_string(event.get("notes"));
            let sub_event_type = value_string(event.get("sub_event_type"));
            let event_type = value_string(event.get("event_type"));
            let location = value_string(event.get("location"));
            let fatalities = parse_i32(event.get("fatalities"));

            let title = if !notes.is_empty() {
                truncate_chars(&notes, 200)
            } else {
                format!("{} in {}", sub_event_type, location)
            };

            Some(UnrestEvent {
                id: format!("acled-{}", value_string(event.get("event_id_cnty"))),
                title,
                summary: truncate_chars(&notes, 500),
                event_type: map_acled_event_type(&event_type, &sub_event_type),
                city: location,
                country: value_string(event.get("country")),
                region: value_string(event.get("admin1")),
                location: Some(GeoCoordinates {
                    latitude,
                    longitude,
                }),
                occurred_at: parse_date_to_epoch_ms(&value_string(event.get("event_date"))),
                severity: classify_severity(fatalities, &event_type),
                fatalities,
                sources: value_string(event.get("source"))
                    .split(';')
                    .map(str::trim)
                    .filter(|source| !source.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                source_type: "UNREST_SOURCE_TYPE_ACLED".to_string(),
                tags: value_string(event.get("tags"))
                    .split(';')
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                actors: [
                    value_string(event.get("actor1")),
                    value_string(event.get("actor2")),
                ]
                .into_iter()
                .filter(|actor| !actor.is_empty())
                .collect::<Vec<_>>(),
                confidence: "CONFIDENCE_LEVEL_HIGH".to_string(),
            })
        })
        .collect::<Vec<_>>()
}

async fn fetch_gdelt_events(state: &AppState) -> Vec<UnrestEvent> {
    let query = "query=protest&format=geojson&maxrecords=250&timespan=7d";
    let response = match state
        .http_client
        .get(format!("{}?{}", GDELT_GEO_URL, query))
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

    let features = payload
        .get("features")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut seen_locations = HashSet::new();
    let mut events = Vec::new();

    for feature in features {
        let name = value_string(
            feature
                .get("properties")
                .and_then(|props| props.get("name")),
        );
        if name.is_empty() || !seen_locations.insert(name.clone()) {
            continue;
        }

        let count = feature
            .get("properties")
            .and_then(|props| props.get("count"))
            .and_then(Value::as_i64)
            .or_else(|| {
                feature
                    .get("properties")
                    .and_then(|props| props.get("count"))
                    .and_then(Value::as_str)
                    .and_then(|raw| raw.parse::<i64>().ok())
            })
            .unwrap_or(1);

        if count < 5 {
            continue;
        }

        let coordinates = feature
            .get("geometry")
            .and_then(|geometry| geometry.get("coordinates"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if coordinates.len() < 2 {
            continue;
        }

        let longitude = coordinates.first().and_then(Value::as_f64).unwrap_or(0.0);
        let latitude = coordinates.get(1).and_then(Value::as_f64).unwrap_or(0.0);
        if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
            continue;
        }

        let city = name
            .split(',')
            .next()
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        let country = name
            .split(',')
            .next_back()
            .map(str::trim)
            .unwrap_or(&name)
            .to_string();

        events.push(UnrestEvent {
            id: format!("gdelt-{:.2}-{:.2}-{}", latitude, longitude, now_epoch_ms()),
            title: format!("{} ({} reports)", name, count),
            summary: String::new(),
            event_type: classify_gdelt_event_type(&name),
            city,
            country,
            region: String::new(),
            location: Some(GeoCoordinates {
                latitude,
                longitude,
            }),
            occurred_at: now_epoch_ms(),
            severity: classify_gdelt_severity(count, &name),
            fatalities: 0,
            sources: vec!["GDELT".to_string()],
            source_type: "UNREST_SOURCE_TYPE_GDELT".to_string(),
            tags: Vec::new(),
            actors: Vec::new(),
            confidence: if count > 20 {
                "CONFIDENCE_LEVEL_HIGH".to_string()
            } else {
                "CONFIDENCE_LEVEL_MEDIUM".to_string()
            },
        });
    }

    events
}

pub async fn list_unrest_events(
    State(state): State<AppState>,
    Json(request): Json<ListUnrestEventsRequest>,
) -> Result<Json<ListUnrestEventsResponse>, AppError> {
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

    if let Some(cached) = get_cached(&cache_key)? {
        return Ok(Json(cached));
    }

    let (acled_events, gdelt_events) = tokio::join!(
        fetch_acled_protests(&state, &request),
        fetch_gdelt_events(&state)
    );

    let mut events = deduplicate_events(
        acled_events
            .into_iter()
            .chain(gdelt_events.into_iter())
            .collect::<Vec<_>>(),
    );

    if !request.min_severity.trim().is_empty() {
        events.retain(|event| matches_min_severity(event, request.min_severity.as_str()));
    }

    if request.bounding_box.is_some() {
        events.retain(|event| in_bounding_box(&event.location, request.bounding_box.as_ref()));
    }

    sort_by_severity_and_recency(&mut events);

    let total_count = events.len();
    events.truncate(page_size(&request));

    let response = ListUnrestEventsResponse {
        events,
        clusters: Vec::new(),
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };

    if !response.events.is_empty() {
        set_cached(&cache_key, &response)?;
    }

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(
        id: &str,
        source_type: &str,
        severity: &str,
        occurred_at: i64,
        sources: Vec<&str>,
    ) -> UnrestEvent {
        UnrestEvent {
            id: id.to_string(),
            title: "Event".to_string(),
            summary: String::new(),
            event_type: "UNREST_EVENT_TYPE_PROTEST".to_string(),
            city: "City".to_string(),
            country: "Country".to_string(),
            region: "Region".to_string(),
            location: Some(GeoCoordinates {
                latitude: 10.1,
                longitude: 20.1,
            }),
            occurred_at,
            severity: severity.to_string(),
            fatalities: 0,
            sources: sources.into_iter().map(ToString::to_string).collect(),
            source_type: source_type.to_string(),
            tags: Vec::new(),
            actors: Vec::new(),
            confidence: "CONFIDENCE_LEVEL_MEDIUM".to_string(),
        }
    }

    #[test]
    fn maps_event_types() {
        assert_eq!(
            map_acled_event_type("Riots", ""),
            "UNREST_EVENT_TYPE_RIOT".to_string()
        );
        assert_eq!(
            map_acled_event_type("Protests", ""),
            "UNREST_EVENT_TYPE_PROTEST".to_string()
        );
    }

    #[test]
    fn deduplicate_prefers_acled_source() {
        let gdelt = sample_event(
            "gdelt-1",
            "UNREST_SOURCE_TYPE_GDELT",
            "SEVERITY_LEVEL_LOW",
            1_000,
            vec!["GDELT"],
        );
        let acled = sample_event(
            "acled-1",
            "UNREST_SOURCE_TYPE_ACLED",
            "SEVERITY_LEVEL_HIGH",
            1_200,
            vec!["ACLED"],
        );
        let deduped = deduplicate_events(vec![gdelt, acled.clone()]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].source_type, "UNREST_SOURCE_TYPE_ACLED");
        assert!(deduped[0].sources.iter().any(|source| source == "GDELT"));
        assert!(deduped[0].sources.iter().any(|source| source == "ACLED"));
    }

    #[test]
    fn sorts_by_severity_then_recency() {
        let mut events = vec![
            sample_event(
                "a",
                "UNREST_SOURCE_TYPE_GDELT",
                "SEVERITY_LEVEL_LOW",
                100,
                vec!["one"],
            ),
            sample_event(
                "b",
                "UNREST_SOURCE_TYPE_GDELT",
                "SEVERITY_LEVEL_HIGH",
                50,
                vec!["two"],
            ),
            sample_event(
                "c",
                "UNREST_SOURCE_TYPE_GDELT",
                "SEVERITY_LEVEL_HIGH",
                200,
                vec!["three"],
            ),
        ];
        sort_by_severity_and_recency(&mut events);
        assert_eq!(events[0].id, "c");
        assert_eq!(events[1].id, "b");
        assert_eq!(events[2].id, "a");
    }

    #[test]
    fn bounding_box_filter_checks_coordinates() {
        let location = Some(GeoCoordinates {
            latitude: 40.0,
            longitude: -70.0,
        });
        let bounding = BoundingBox {
            north_east: GeoCoordinates {
                latitude: 45.0,
                longitude: -60.0,
            },
            south_west: GeoCoordinates {
                latitude: 35.0,
                longitude: -80.0,
            },
        };
        assert!(in_bounding_box(&location, Some(&bounding)));
        assert!(in_bounding_box(&location, None::<&BoundingBox>));
    }
}
