use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(1_800);
const FIRMS_SOURCE: &str = "VIIRS_SNPP_NRT";

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListFireDetectionsRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
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
pub struct ListFireDetectionsResponse {
    pub fire_detections: Vec<FireDetection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FireDetection {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub brightness: f64,
    pub frp: f64,
    pub confidence: String,
    pub satellite: String,
    pub detected_at: i64,
    pub region: String,
    pub day_night: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Clone)]
struct CacheEntry {
    detections: Vec<FireDetection>,
    expires_at: Instant,
}

static FIRE_CACHE: Lazy<Mutex<Option<CacheEntry>>> = Lazy::new(|| Mutex::new(None));

const MONITORED_REGIONS: &[(&str, &str)] = &[
    ("Ukraine", "22,44,40,53"),
    ("Russia", "20,50,180,82"),
    ("Iran", "44,25,63,40"),
    ("Israel/Gaza", "34,29,36,34"),
    ("Syria", "35,32,42,37"),
    ("Taiwan", "119,21,123,26"),
    ("North Korea", "124,37,131,43"),
    ("Saudi Arabia", "34,16,56,32"),
    ("Turkey", "26,36,45,42"),
];

fn parse_page_size(request: &ListFireDetectionsRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn parse_detected_at(acq_date: &str, acq_time: &str) -> i64 {
    let padded = format!("{:0>4}", acq_time.trim());
    let datetime = format!("{} {}:{}:00", acq_date.trim(), &padded[..2], &padded[2..]);
    NaiveDateTime::parse_from_str(&datetime, "%Y-%m-%d %H:%M:%S")
        .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).timestamp_millis())
        .unwrap_or(0)
}

fn map_confidence(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "h" => "FIRE_CONFIDENCE_HIGH",
        "n" => "FIRE_CONFIDENCE_NOMINAL",
        "l" => "FIRE_CONFIDENCE_LOW",
        _ => "FIRE_CONFIDENCE_UNSPECIFIED",
    }
}

fn parse_csv_rows(csv: &str) -> Vec<std::collections::HashMap<String, String>> {
    let lines = csv
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() < 2 {
        return Vec::new();
    }
    let headers = lines[0]
        .split(',')
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    lines
        .iter()
        .skip(1)
        .filter_map(|line| {
            let columns = line.split(',').map(str::trim).collect::<Vec<_>>();
            if columns.len() < headers.len() {
                return None;
            }
            let mut row = std::collections::HashMap::new();
            for (index, header) in headers.iter().enumerate() {
                row.insert(
                    header.clone(),
                    columns.get(index).copied().unwrap_or_default().to_string(),
                );
            }
            Some(row)
        })
        .collect()
}

fn parse_f64(value: Option<&String>) -> f64 {
    value
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn within_time_range(timestamp: i64, range: Option<&TimeRange>) -> bool {
    let Some(range) = range else {
        return true;
    };
    if range.start > 0 && timestamp < range.start {
        return false;
    }
    if range.end > 0 && timestamp > range.end {
        return false;
    }
    true
}

fn within_bounding_box(latitude: f64, longitude: f64, bounding_box: Option<&BoundingBox>) -> bool {
    let Some(bounding_box) = bounding_box else {
        return true;
    };
    let (Some(south_west), Some(north_east)) = (&bounding_box.south_west, &bounding_box.north_east)
    else {
        return true;
    };
    latitude >= south_west.latitude
        && latitude <= north_east.latitude
        && longitude >= south_west.longitude
        && longitude <= north_east.longitude
}

fn get_cache() -> Result<Option<Vec<FireDetection>>, AppError> {
    let cache = FIRE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("wildfire cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.detections.clone()));
    }
    Ok(None)
}

fn set_cache(detections: &[FireDetection]) -> Result<(), AppError> {
    let mut cache = FIRE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("wildfire cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        detections: detections.to_vec(),
        expires_at: Instant::now() + CACHE_TTL,
    });
    Ok(())
}

async fn fetch_region_detections(
    state: &AppState,
    api_key: &str,
    region_name: &str,
    bbox: &str,
) -> Vec<FireDetection> {
    let endpoint = format!(
        "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{}/{}/{}/1",
        api_key, FIRMS_SOURCE, bbox
    );
    let response = match state
        .http_client
        .get(endpoint)
        .header("Accept", "text/csv")
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

    let body = match response.text().await {
        Ok(body) => body,
        Err(_) => return Vec::new(),
    };
    let rows = parse_csv_rows(&body);
    rows.into_iter()
        .filter_map(|row| {
            let latitude = parse_f64(row.get("latitude"));
            let longitude = parse_f64(row.get("longitude"));
            if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
                return None;
            }
            let acq_date = row.get("acq_date").cloned().unwrap_or_default();
            let acq_time = row.get("acq_time").cloned().unwrap_or_default();
            let detected_at = parse_detected_at(&acq_date, &acq_time);
            Some(FireDetection {
                id: format!(
                    "{}-{}-{}-{}",
                    row.get("latitude").cloned().unwrap_or_default(),
                    row.get("longitude").cloned().unwrap_or_default(),
                    acq_date,
                    acq_time
                ),
                location: Some(GeoCoordinates {
                    latitude,
                    longitude,
                }),
                brightness: parse_f64(row.get("bright_ti4")),
                frp: parse_f64(row.get("frp")),
                confidence: map_confidence(
                    row.get("confidence")
                        .map(String::as_str)
                        .unwrap_or_default(),
                )
                .to_string(),
                satellite: row.get("satellite").cloned().unwrap_or_default(),
                detected_at,
                region: region_name.to_string(),
                day_night: row.get("daynight").cloned().unwrap_or_default(),
            })
        })
        .collect()
}

pub async fn list_fire_detections(
    State(state): State<AppState>,
    Json(request): Json<ListFireDetectionsRequest>,
) -> Result<Json<ListFireDetectionsResponse>, AppError> {
    let api_key = std::env::var("NASA_FIRMS_API_KEY")
        .or_else(|_| std::env::var("FIRMS_API_KEY"))
        .unwrap_or_default();
    if api_key.trim().is_empty() {
        return Ok(Json(ListFireDetectionsResponse {
            fire_detections: Vec::new(),
            pagination: Some(PaginationResponse {
                next_cursor: String::new(),
                total_count: 0,
            }),
        }));
    }

    let all_detections = match get_cache()? {
        Some(cached) => cached,
        None => {
            let tasks = MONITORED_REGIONS.iter().map(|(region_name, bbox)| {
                fetch_region_detections(&state, api_key.trim(), region_name, bbox)
            });
            let results = join_all(tasks).await;
            let detections = results.into_iter().flatten().collect::<Vec<_>>();
            if !detections.is_empty() {
                set_cache(&detections)?;
            }
            detections
        }
    };

    let mut filtered = all_detections
        .into_iter()
        .filter(|detection| within_time_range(detection.detected_at, request.time_range.as_ref()))
        .filter(|detection| {
            let Some(location) = detection.location.as_ref() else {
                return false;
            };
            within_bounding_box(
                location.latitude,
                location.longitude,
                request.bounding_box.as_ref(),
            )
        })
        .collect::<Vec<_>>();

    filtered.sort_by(|a, b| b.detected_at.cmp(&a.detected_at));
    let total_count = filtered.len();
    filtered.truncate(parse_page_size(&request));

    Ok(Json(ListFireDetectionsResponse {
        fire_detections: filtered,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    }))
}
