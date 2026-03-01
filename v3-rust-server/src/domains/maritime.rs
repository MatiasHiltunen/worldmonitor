use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{DateTime, NaiveDate, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(10);
const WARNINGS_CACHE_TTL: Duration = Duration::from_secs(3_600);
const NGA_WARNINGS_URL: &str =
    "https://msi.nga.mil/api/publications/broadcast-warn?output=json&status=A";

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetVesselSnapshotRequest {
    #[serde(default)]
    pub bounding_box: Option<BoundingBox>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BoundingBox {
    #[serde(default)]
    pub north_east: Option<GeoCoordinates>,
    #[serde(default)]
    pub south_west: Option<GeoCoordinates>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetVesselSnapshotResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<VesselSnapshot>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct VesselSnapshot {
    pub snapshot_at: i64,
    pub density_zones: Vec<AisDensityZone>,
    pub disruptions: Vec<AisDisruption>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AisDensityZone {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub intensity: f64,
    pub delta_pct: f64,
    pub ships_per_day: f64,
    pub note: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AisDisruption {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub disruption_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub severity: String,
    pub change_pct: f64,
    pub window_hours: i64,
    pub dark_ships: i64,
    pub vessel_count: i64,
    pub region: String,
    pub description: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListNavigationalWarningsRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub area: String,
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
pub struct ListNavigationalWarningsResponse {
    pub warnings: Vec<NavigationalWarning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NavigationalWarning {
    pub id: String,
    pub title: String,
    pub text: String,
    pub area: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub issued_at: i64,
    pub expires_at: i64,
    pub authority: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Clone)]
struct SnapshotCacheEntry {
    value: Option<VesselSnapshot>,
    expires_at: Instant,
}

#[derive(Clone)]
struct WarningsCacheEntry {
    value: ListNavigationalWarningsResponse,
    expires_at: Instant,
}

static SNAPSHOT_CACHE: Lazy<Mutex<Option<SnapshotCacheEntry>>> = Lazy::new(|| Mutex::new(None));
static WARNINGS_CACHE: Lazy<Mutex<HashMap<String, WarningsCacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn parse_f64(value: Option<&Value>) -> f64 {
    let Some(value) = value else {
        return 0.0;
    };
    if let Some(number) = value.as_f64() {
        return number;
    }
    value
        .as_str()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn parse_i64(value: Option<&Value>) -> i64 {
    let Some(value) = value else {
        return 0;
    };
    if let Some(number) = value.as_i64() {
        return number;
    }
    if let Some(number) = value.as_u64() {
        return number as i64;
    }
    value
        .as_str()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

fn parse_string(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|raw| raw.trim().to_string())
        .unwrap_or_default()
}

fn relay_base_url() -> Option<String> {
    let raw = std::env::var("WS_RELAY_URL").ok()?;
    if raw.trim().is_empty() {
        return None;
    }
    Some(
        raw.trim()
            .replace("wss://", "https://")
            .replace("ws://", "http://")
            .trim_end_matches('/')
            .to_string(),
    )
}

fn map_disruption_type(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "gap_spike" => "AIS_DISRUPTION_TYPE_GAP_SPIKE",
        "chokepoint_congestion" => "AIS_DISRUPTION_TYPE_CHOKEPOINT_CONGESTION",
        _ => "AIS_DISRUPTION_TYPE_UNSPECIFIED",
    }
}

fn map_disruption_severity(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "low" => "AIS_DISRUPTION_SEVERITY_LOW",
        "elevated" => "AIS_DISRUPTION_SEVERITY_ELEVATED",
        "high" => "AIS_DISRUPTION_SEVERITY_HIGH",
        _ => "AIS_DISRUPTION_SEVERITY_UNSPECIFIED",
    }
}

fn get_snapshot_cache() -> Result<Option<Option<VesselSnapshot>>, AppError> {
    let cache = SNAPSHOT_CACHE
        .lock()
        .map_err(|_| AppError::Internal("maritime snapshot cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_snapshot_cache(snapshot: &Option<VesselSnapshot>) -> Result<(), AppError> {
    let mut cache = SNAPSHOT_CACHE
        .lock()
        .map_err(|_| AppError::Internal("maritime snapshot cache lock poisoned".to_string()))?;
    *cache = Some(SnapshotCacheEntry {
        value: snapshot.clone(),
        expires_at: Instant::now() + SNAPSHOT_CACHE_TTL,
    });
    Ok(())
}

async fn fetch_snapshot_from_relay(state: &AppState) -> Option<VesselSnapshot> {
    let base_url = relay_base_url()?;
    let endpoint = format!("{base_url}/ais/snapshot?candidates=false");
    let response = state
        .http_client
        .get(endpoint)
        .header("Accept", "application/json")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload = response.json::<Value>().await.ok()?;
    let density_rows = payload
        .get("density")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let disruptions_rows = payload
        .get("disruptions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let density_zones = density_rows
        .iter()
        .map(|row| AisDensityZone {
            id: parse_string(row.get("id")),
            name: parse_string(row.get("name")),
            location: Some(GeoCoordinates {
                latitude: parse_f64(row.get("lat")),
                longitude: parse_f64(row.get("lon")),
            }),
            intensity: parse_f64(row.get("intensity")),
            delta_pct: parse_f64(row.get("deltaPct")),
            ships_per_day: parse_f64(row.get("shipsPerDay")),
            note: parse_string(row.get("note")),
        })
        .collect::<Vec<_>>();

    let disruptions = disruptions_rows
        .iter()
        .map(|row| AisDisruption {
            id: parse_string(row.get("id")),
            name: parse_string(row.get("name")),
            disruption_type: map_disruption_type(&parse_string(row.get("type"))).to_string(),
            location: Some(GeoCoordinates {
                latitude: parse_f64(row.get("lat")),
                longitude: parse_f64(row.get("lon")),
            }),
            severity: map_disruption_severity(&parse_string(row.get("severity"))).to_string(),
            change_pct: parse_f64(row.get("changePct")),
            window_hours: parse_i64(row.get("windowHours")),
            dark_ships: parse_i64(row.get("darkShips")),
            vessel_count: parse_i64(row.get("vesselCount")),
            region: parse_string(row.get("region")),
            description: parse_string(row.get("description")),
        })
        .collect::<Vec<_>>();

    Some(VesselSnapshot {
        snapshot_at: now_epoch_ms(),
        density_zones,
        disruptions,
    })
}

fn parse_nga_date(raw: &str) -> i64 {
    static NGA_DATE_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(\d{2})(\d{2})(\d{2})Z\s+([A-Z]{3})\s+(\d{4})")
            .expect("valid nga date regex")
    });
    let value = raw.trim();
    if value.is_empty() {
        return 0;
    }

    if let Some(captures) = NGA_DATE_RE.captures(value) {
        let day = captures
            .get(1)
            .and_then(|v| v.as_str().parse::<u32>().ok())
            .unwrap_or(1);
        let hour = captures
            .get(2)
            .and_then(|v| v.as_str().parse::<u32>().ok())
            .unwrap_or(0);
        let minute = captures
            .get(3)
            .and_then(|v| v.as_str().parse::<u32>().ok())
            .unwrap_or(0);
        let month = match captures
            .get(4)
            .map(|v| v.as_str().to_ascii_uppercase())
            .as_deref()
        {
            Some("JAN") => 1,
            Some("FEB") => 2,
            Some("MAR") => 3,
            Some("APR") => 4,
            Some("MAY") => 5,
            Some("JUN") => 6,
            Some("JUL") => 7,
            Some("AUG") => 8,
            Some("SEP") => 9,
            Some("OCT") => 10,
            Some("NOV") => 11,
            Some("DEC") => 12,
            _ => 1,
        };
        let year = captures
            .get(5)
            .and_then(|v| v.as_str().parse::<i32>().ok())
            .unwrap_or(1970);

        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day)
            && let Some(datetime) = date.and_hms_opt(hour, minute, 0)
        {
            return DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).timestamp_millis();
        }
    }

    DateTime::parse_from_rfc3339(value)
        .map(|datetime| datetime.timestamp_millis())
        .or_else(|_| {
            DateTime::parse_from_rfc2822(value).map(|datetime| datetime.timestamp_millis())
        })
        .unwrap_or(0)
}

fn warning_page_size(request: &ListNavigationalWarningsRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn get_warnings_cache(key: &str) -> Result<Option<ListNavigationalWarningsResponse>, AppError> {
    let cache = WARNINGS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("maritime warnings cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_warnings_cache(
    key: String,
    value: &ListNavigationalWarningsResponse,
) -> Result<(), AppError> {
    let mut cache = WARNINGS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("maritime warnings cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        WarningsCacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + WARNINGS_CACHE_TTL,
        },
    );
    Ok(())
}

async fn fetch_nga_warnings(state: &AppState, area: &str) -> Vec<NavigationalWarning> {
    let response = match state
        .http_client
        .get(NGA_WARNINGS_URL)
        .header("Accept", "application/json")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
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

    let rows = if let Some(array) = payload.as_array() {
        array.clone()
    } else {
        payload
            .get("broadcast_warn")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };

    let area_filter = area.trim().to_ascii_lowercase();
    let mut warnings = rows
        .into_iter()
        .map(|row| {
            let nav_area = parse_string(row.get("navArea"));
            let msg_year = parse_string(row.get("msgYear"));
            let msg_number = parse_string(row.get("msgNumber"));
            let text = parse_string(row.get("text"));
            let subregion = parse_string(row.get("subregion"));
            let area_name = if subregion.is_empty() {
                nav_area.clone()
            } else {
                format!("{nav_area} {subregion}")
            };
            NavigationalWarning {
                id: format!("{nav_area}-{msg_year}-{msg_number}"),
                title: format!("NAVAREA {nav_area} {msg_number}/{msg_year}"),
                text,
                area: area_name,
                location: None,
                issued_at: parse_nga_date(&parse_string(row.get("issueDate"))),
                expires_at: 0,
                authority: parse_string(row.get("authority")),
            }
        })
        .collect::<Vec<_>>();

    if !area_filter.is_empty() {
        warnings.retain(|warning| {
            warning.area.to_ascii_lowercase().contains(&area_filter)
                || warning.text.to_ascii_lowercase().contains(&area_filter)
        });
    }
    warnings.sort_by(|a, b| b.issued_at.cmp(&a.issued_at));
    warnings
}

pub async fn get_vessel_snapshot(
    State(state): State<AppState>,
    Json(_request): Json<GetVesselSnapshotRequest>,
) -> Result<Json<GetVesselSnapshotResponse>, AppError> {
    if let Some(cached) = get_snapshot_cache()? {
        return Ok(Json(GetVesselSnapshotResponse { snapshot: cached }));
    }

    let snapshot = fetch_snapshot_from_relay(&state).await;
    set_snapshot_cache(&snapshot)?;
    Ok(Json(GetVesselSnapshotResponse { snapshot }))
}

pub async fn list_navigational_warnings(
    State(state): State<AppState>,
    Json(request): Json<ListNavigationalWarningsRequest>,
) -> Result<Json<ListNavigationalWarningsResponse>, AppError> {
    let area = request.area.trim().to_string();
    let cache_key = if area.is_empty() {
        "all".to_string()
    } else {
        area.to_ascii_lowercase()
    };

    if let Some(cached) = get_warnings_cache(&cache_key)? {
        return Ok(Json(cached));
    }

    let mut warnings = fetch_nga_warnings(&state, &area).await;
    let total_count = warnings.len();
    warnings.truncate(warning_page_size(&request));

    let response = ListNavigationalWarningsResponse {
        warnings,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };

    if !response.warnings.is_empty() {
        set_warnings_cache(cache_key, &response)?;
    }
    Ok(Json(response))
}
