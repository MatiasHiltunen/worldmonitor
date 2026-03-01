use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{AppState, error::AppError};

const USGS_FEED_URL: &str =
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson";
const CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListEarthquakesRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub min_magnitude: f64,
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

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListEarthquakesResponse {
    pub earthquakes: Vec<Earthquake>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Earthquake {
    pub id: String,
    pub place: String,
    pub magnitude: f64,
    pub depth_km: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub occurred_at: i64,
    pub source_url: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Debug, Deserialize)]
struct UsgsFeed {
    #[serde(default)]
    features: Vec<UsgsFeature>,
}

#[derive(Debug, Deserialize)]
struct UsgsFeature {
    #[serde(default)]
    id: String,
    #[serde(default)]
    properties: UsgsProperties,
    #[serde(default)]
    geometry: UsgsGeometry,
}

#[derive(Debug, Deserialize, Default)]
struct UsgsProperties {
    #[serde(default)]
    place: String,
    #[serde(default)]
    mag: Option<f64>,
    #[serde(default)]
    time: Option<i64>,
    #[serde(default)]
    url: String,
}

#[derive(Debug, Deserialize, Default)]
struct UsgsGeometry {
    #[serde(default)]
    coordinates: Vec<f64>,
}

#[derive(Clone)]
struct CacheEntry {
    earthquakes: Vec<Earthquake>,
    expires_at: Instant,
}

static QUAKE_CACHE: Lazy<Mutex<Option<CacheEntry>>> = Lazy::new(|| Mutex::new(None));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn within_time_range(occurred_at: i64, range: Option<&TimeRange>) -> bool {
    let Some(range) = range else {
        return true;
    };
    if range.start > 0 && occurred_at < range.start {
        return false;
    }
    if range.end > 0 && occurred_at > range.end {
        return false;
    }
    true
}

fn page_size(request: &ListEarthquakesRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

async fn fetch_usgs_quakes(state: &AppState) -> Result<Vec<Earthquake>, AppError> {
    let response = state
        .http_client
        .get(USGS_FEED_URL)
        .header("Accept", "application/json")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("USGS request failed: {}", error)))?;

    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "USGS API error: HTTP {}",
            response.status().as_u16()
        )));
    }

    let feed = response
        .json::<UsgsFeed>()
        .await
        .map_err(|error| AppError::Internal(format!("USGS decode failed: {}", error)))?;

    let now = now_epoch_ms();
    let quakes = feed
        .features
        .into_iter()
        .filter_map(|feature| {
            let lon = feature.geometry.coordinates.first().copied()?;
            let lat = feature.geometry.coordinates.get(1).copied()?;
            let depth = feature.geometry.coordinates.get(2).copied().unwrap_or(0.0);
            if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                return None;
            }
            Some(Earthquake {
                id: feature.id,
                place: feature.properties.place,
                magnitude: feature.properties.mag.unwrap_or(0.0),
                depth_km: depth,
                location: Some(GeoCoordinates {
                    latitude: lat,
                    longitude: lon,
                }),
                occurred_at: feature.properties.time.unwrap_or(now),
                source_url: feature.properties.url,
            })
        })
        .collect::<Vec<_>>();

    Ok(quakes)
}

fn get_cached_quakes() -> Result<Option<Vec<Earthquake>>, AppError> {
    let cache = QUAKE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("seismology cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.earthquakes.clone()));
    }
    Ok(None)
}

fn update_cache(earthquakes: &[Earthquake]) -> Result<(), AppError> {
    let mut cache = QUAKE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("seismology cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        earthquakes: earthquakes.to_vec(),
        expires_at: Instant::now() + CACHE_TTL,
    });
    Ok(())
}

pub async fn list_earthquakes(
    State(state): State<AppState>,
    Json(request): Json<ListEarthquakesRequest>,
) -> Result<Json<ListEarthquakesResponse>, AppError> {
    let all_quakes = match get_cached_quakes()? {
        Some(cached) => cached,
        None => {
            let fetched = fetch_usgs_quakes(&state).await?;
            update_cache(&fetched)?;
            fetched
        }
    };

    let threshold = if request.min_magnitude > 0.0 {
        request.min_magnitude
    } else {
        0.0
    };
    let filtered = all_quakes
        .into_iter()
        .filter(|quake| quake.magnitude >= threshold)
        .filter(|quake| within_time_range(quake.occurred_at, request.time_range.as_ref()))
        .collect::<Vec<_>>();

    let size = page_size(&request);
    let total = filtered.len();
    let earthquakes = filtered.into_iter().take(size).collect::<Vec<_>>();

    Ok(Json(ListEarthquakesResponse {
        earthquakes,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count: total,
        }),
    }))
}
