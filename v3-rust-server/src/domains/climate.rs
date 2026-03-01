use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use futures::future::join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(1_800);
const RADIATION_CACHE_TTL: Duration = Duration::from_secs(900);
const OPEN_METEO_BASE: &str = "https://archive-api.open-meteo.com/v1/archive";
const OPEN_METEO_FORECAST_BASE: &str = "https://api.open-meteo.com/v1/forecast";

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListClimateAnomaliesRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub min_severity: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalRadiationSituationRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub min_severity: String,
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
pub struct ListClimateAnomaliesResponse {
    pub anomalies: Vec<ClimateAnomaly>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetGlobalRadiationSituationResponse {
    pub snapshot_at: i64,
    pub source: String,
    pub entries: Vec<GlobalRadiationEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GlobalRadiationEntry {
    pub id: String,
    pub zone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub shortwave_radiation_wm2: f64,
    pub uv_index: f64,
    pub severity: String,
    pub trend: String,
    pub observed_at: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClimateAnomaly {
    pub zone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub temp_delta: f64,
    pub precip_delta: f64,
    pub severity: String,
    #[serde(rename = "type")]
    pub anomaly_type: String,
    pub period: String,
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

#[derive(Clone)]
struct CacheEntry {
    anomalies: Vec<ClimateAnomaly>,
    expires_at: Instant,
}

#[derive(Clone)]
struct RadiationCacheEntry {
    snapshot_at: i64,
    entries: Vec<GlobalRadiationEntry>,
    expires_at: Instant,
}

static CLIMATE_CACHE: Lazy<Mutex<Option<CacheEntry>>> = Lazy::new(|| Mutex::new(None));
static RADIATION_CACHE: Lazy<Mutex<Option<RadiationCacheEntry>>> = Lazy::new(|| Mutex::new(None));

#[derive(Clone, Copy)]
struct Zone {
    name: &'static str,
    latitude: f64,
    longitude: f64,
}

const ZONES: &[Zone] = &[
    Zone {
        name: "Ukraine",
        latitude: 48.4,
        longitude: 31.2,
    },
    Zone {
        name: "Middle East",
        latitude: 33.0,
        longitude: 44.0,
    },
    Zone {
        name: "Sahel",
        latitude: 14.0,
        longitude: 0.0,
    },
    Zone {
        name: "Horn of Africa",
        latitude: 8.0,
        longitude: 42.0,
    },
    Zone {
        name: "South Asia",
        latitude: 25.0,
        longitude: 78.0,
    },
    Zone {
        name: "California",
        latitude: 36.8,
        longitude: -119.4,
    },
    Zone {
        name: "Amazon",
        latitude: -3.4,
        longitude: -60.0,
    },
    Zone {
        name: "Australia",
        latitude: -25.0,
        longitude: 134.0,
    },
    Zone {
        name: "Mediterranean",
        latitude: 38.0,
        longitude: 20.0,
    },
    Zone {
        name: "Taiwan Strait",
        latitude: 24.0,
        longitude: 120.0,
    },
    Zone {
        name: "Myanmar",
        latitude: 19.8,
        longitude: 96.7,
    },
    Zone {
        name: "Central Africa",
        latitude: 4.0,
        longitude: 22.0,
    },
    Zone {
        name: "Southern Africa",
        latitude: -25.0,
        longitude: 28.0,
    },
    Zone {
        name: "Central Asia",
        latitude: 42.0,
        longitude: 65.0,
    },
    Zone {
        name: "Caribbean",
        latitude: 19.0,
        longitude: -72.0,
    },
];

#[derive(Clone, Copy)]
struct RadiationZone {
    id: &'static str,
    name: &'static str,
    latitude: f64,
    longitude: f64,
}

const RADIATION_ZONES: &[RadiationZone] = &[
    RadiationZone {
        id: "north-atlantic",
        name: "North Atlantic",
        latitude: 30.0,
        longitude: -40.0,
    },
    RadiationZone {
        id: "equatorial-pacific",
        name: "Equatorial Pacific",
        latitude: 0.0,
        longitude: -140.0,
    },
    RadiationZone {
        id: "europe",
        name: "Europe",
        latitude: 50.0,
        longitude: 10.0,
    },
    RadiationZone {
        id: "north-africa",
        name: "North Africa",
        latitude: 24.0,
        longitude: 15.0,
    },
    RadiationZone {
        id: "middle-east",
        name: "Middle East",
        latitude: 28.0,
        longitude: 47.0,
    },
    RadiationZone {
        id: "south-asia",
        name: "South Asia",
        latitude: 23.0,
        longitude: 80.0,
    },
    RadiationZone {
        id: "east-asia",
        name: "East Asia",
        latitude: 35.0,
        longitude: 120.0,
    },
    RadiationZone {
        id: "southeast-asia",
        name: "Southeast Asia",
        latitude: 8.0,
        longitude: 106.0,
    },
    RadiationZone {
        id: "australia",
        name: "Australia",
        latitude: -25.0,
        longitude: 134.0,
    },
    RadiationZone {
        id: "southern-africa",
        name: "Southern Africa",
        latitude: -26.0,
        longitude: 28.0,
    },
    RadiationZone {
        id: "west-africa",
        name: "West Africa",
        latitude: 12.0,
        longitude: -1.0,
    },
    RadiationZone {
        id: "south-america",
        name: "South America",
        latitude: -15.0,
        longitude: -60.0,
    },
    RadiationZone {
        id: "north-america",
        name: "North America",
        latitude: 40.0,
        longitude: -100.0,
    },
    RadiationZone {
        id: "arctic",
        name: "Arctic",
        latitude: 74.0,
        longitude: 20.0,
    },
    RadiationZone {
        id: "antarctic",
        name: "Antarctic Periphery",
        latitude: -66.0,
        longitude: 20.0,
    },
];

fn page_size(request: &ListClimateAnomaliesRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn radiation_page_size(request: &GetGlobalRadiationSituationRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(200)
        .min(1_000)
}

fn severity_rank(severity: &str) -> usize {
    match severity {
        "ANOMALY_SEVERITY_NORMAL" => 1,
        "ANOMALY_SEVERITY_MODERATE" => 2,
        "ANOMALY_SEVERITY_EXTREME" => 3,
        _ => 0,
    }
}

fn radiation_severity_rank(severity: &str) -> usize {
    match severity {
        "RADIATION_SEVERITY_LOW" => 1,
        "RADIATION_SEVERITY_MODERATE" => 2,
        "RADIATION_SEVERITY_HIGH" => 3,
        "RADIATION_SEVERITY_EXTREME" => 4,
        _ => 0,
    }
}

fn parse_min_severity(requested: &str) -> usize {
    if requested.trim().is_empty() || requested.eq_ignore_ascii_case("ANOMALY_SEVERITY_UNSPECIFIED")
    {
        return 0;
    }
    severity_rank(requested.trim())
}

fn parse_radiation_min_severity(requested: &str) -> usize {
    if requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("RADIATION_SEVERITY_UNSPECIFIED")
    {
        return 0;
    }
    radiation_severity_rank(requested.trim())
}

fn classify_severity(temp_delta: f64, precip_delta: f64) -> &'static str {
    let abs_temp = temp_delta.abs();
    let abs_precip = precip_delta.abs();
    if abs_temp >= 5.0 || abs_precip >= 80.0 {
        "ANOMALY_SEVERITY_EXTREME"
    } else if abs_temp >= 3.0 || abs_precip >= 40.0 {
        "ANOMALY_SEVERITY_MODERATE"
    } else {
        "ANOMALY_SEVERITY_NORMAL"
    }
}

fn classify_radiation_severity(shortwave_radiation_wm2: f64, uv_index: f64) -> &'static str {
    if uv_index >= 10.0 || shortwave_radiation_wm2 >= 900.0 {
        "RADIATION_SEVERITY_EXTREME"
    } else if uv_index >= 7.0 || shortwave_radiation_wm2 >= 700.0 {
        "RADIATION_SEVERITY_HIGH"
    } else if uv_index >= 4.0 || shortwave_radiation_wm2 >= 350.0 {
        "RADIATION_SEVERITY_MODERATE"
    } else {
        "RADIATION_SEVERITY_LOW"
    }
}

fn classify_radiation_trend(current: f64, prior: Option<f64>) -> &'static str {
    let Some(prior) = prior else {
        return "RADIATION_TREND_STABLE";
    };
    let delta = current - prior;
    if delta > 80.0 {
        "RADIATION_TREND_RISING"
    } else if delta < -80.0 {
        "RADIATION_TREND_FALLING"
    } else {
        "RADIATION_TREND_STABLE"
    }
}

fn classify_type(temp_delta: f64, precip_delta: f64) -> &'static str {
    let abs_temp = temp_delta.abs();
    let abs_precip = precip_delta.abs();
    if abs_temp >= abs_precip / 20.0 {
        if temp_delta > 0.0 && precip_delta < -20.0 {
            return "ANOMALY_TYPE_MIXED";
        }
        if temp_delta > 3.0 {
            return "ANOMALY_TYPE_WARM";
        }
        if temp_delta < -3.0 {
            return "ANOMALY_TYPE_COLD";
        }
    }
    if precip_delta > 40.0 {
        "ANOMALY_TYPE_WET"
    } else if precip_delta < -40.0 {
        "ANOMALY_TYPE_DRY"
    } else if temp_delta >= 0.0 {
        "ANOMALY_TYPE_WARM"
    } else {
        "ANOMALY_TYPE_COLD"
    }
}

fn avg(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn round_one(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn as_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

fn now_epoch_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn parse_open_meteo_timestamp_ms(raw: &str) -> i64 {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return now_epoch_ms();
    }
    if let Ok(date) = DateTime::parse_from_rfc3339(trimmed) {
        return date.timestamp_millis();
    }
    if let Ok(datetime) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M") {
        return DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).timestamp_millis();
    }
    now_epoch_ms()
}

async fn fetch_global_radiation_entry(
    state: &AppState,
    zone: RadiationZone,
) -> Option<GlobalRadiationEntry> {
    let mut url = reqwest::Url::parse(OPEN_METEO_FORECAST_BASE).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("latitude", &zone.latitude.to_string());
        query.append_pair("longitude", &zone.longitude.to_string());
        query.append_pair("current", "shortwave_radiation,uv_index");
        query.append_pair("hourly", "shortwave_radiation");
        query.append_pair("past_hours", "2");
        query.append_pair("forecast_hours", "1");
        query.append_pair("timezone", "UTC");
    }

    let response = state
        .http_client
        .get(url)
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
    let shortwave_radiation_wm2 = as_f64(payload.pointer("/current/shortwave_radiation"))?;
    let uv_index = as_f64(payload.pointer("/current/uv_index")).unwrap_or(0.0);
    let observed_at = payload
        .pointer("/current/time")
        .and_then(Value::as_str)
        .map(parse_open_meteo_timestamp_ms)
        .unwrap_or_else(now_epoch_ms);

    let prior_shortwave = payload
        .pointer("/hourly/shortwave_radiation")
        .and_then(Value::as_array)
        .and_then(|values| {
            if values.len() >= 2 {
                as_f64(values.get(values.len().saturating_sub(2)))
            } else {
                None
            }
        });

    Some(GlobalRadiationEntry {
        id: zone.id.to_string(),
        zone: zone.name.to_string(),
        location: Some(GeoCoordinates {
            latitude: zone.latitude,
            longitude: zone.longitude,
        }),
        shortwave_radiation_wm2: round_one(shortwave_radiation_wm2),
        uv_index: round_one(uv_index),
        severity: classify_radiation_severity(shortwave_radiation_wm2, uv_index).to_string(),
        trend: classify_radiation_trend(shortwave_radiation_wm2, prior_shortwave).to_string(),
        observed_at,
    })
}

async fn fetch_zone_anomaly(
    state: &AppState,
    zone: Zone,
    start_date: &str,
    end_date: &str,
) -> Option<ClimateAnomaly> {
    let mut url = reqwest::Url::parse(OPEN_METEO_BASE).ok()?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("latitude", &zone.latitude.to_string());
        query.append_pair("longitude", &zone.longitude.to_string());
        query.append_pair("start_date", start_date);
        query.append_pair("end_date", end_date);
        query.append_pair("daily", "temperature_2m_mean,precipitation_sum");
        query.append_pair("timezone", "UTC");
    }

    let response = state
        .http_client
        .get(url)
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
    let temps = payload
        .pointer("/daily/temperature_2m_mean")
        .and_then(Value::as_array)?;
    let precips = payload
        .pointer("/daily/precipitation_sum")
        .and_then(Value::as_array)?;

    let mut paired_temps = Vec::new();
    let mut paired_precips = Vec::new();
    let length = temps.len().min(precips.len());
    for index in 0..length {
        let Some(temp) = as_f64(temps.get(index)) else {
            continue;
        };
        let Some(precip) = as_f64(precips.get(index)) else {
            continue;
        };
        paired_temps.push(temp);
        paired_precips.push(precip);
    }

    if paired_temps.len() < 14 {
        return None;
    }

    let split = paired_temps.len().saturating_sub(7);
    if split == 0 {
        return None;
    }
    let recent_temps = &paired_temps[split..];
    let baseline_temps = &paired_temps[..split];
    let recent_precips = &paired_precips[split..];
    let baseline_precips = &paired_precips[..split];

    let temp_delta = round_one(avg(recent_temps) - avg(baseline_temps));
    let precip_delta = round_one(avg(recent_precips) - avg(baseline_precips));

    Some(ClimateAnomaly {
        zone: zone.name.to_string(),
        location: Some(GeoCoordinates {
            latitude: zone.latitude,
            longitude: zone.longitude,
        }),
        temp_delta,
        precip_delta,
        severity: classify_severity(temp_delta, precip_delta).to_string(),
        anomaly_type: classify_type(temp_delta, precip_delta).to_string(),
        period: format!("{start_date} to {end_date}"),
    })
}

fn get_cached_anomalies() -> Result<Option<Vec<ClimateAnomaly>>, AppError> {
    let cache = CLIMATE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("climate cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.anomalies.clone()));
    }
    Ok(None)
}

fn set_cached_anomalies(anomalies: &[ClimateAnomaly]) -> Result<(), AppError> {
    let mut cache = CLIMATE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("climate cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        anomalies: anomalies.to_vec(),
        expires_at: Instant::now() + CACHE_TTL,
    });
    Ok(())
}

fn get_cached_radiation() -> Result<Option<(i64, Vec<GlobalRadiationEntry>)>, AppError> {
    let cache = RADIATION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("radiation cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some((entry.snapshot_at, entry.entries.clone())));
    }
    Ok(None)
}

fn set_cached_radiation(
    snapshot_at: i64,
    entries: &[GlobalRadiationEntry],
) -> Result<(), AppError> {
    let mut cache = RADIATION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("radiation cache lock poisoned".to_string()))?;
    *cache = Some(RadiationCacheEntry {
        snapshot_at,
        entries: entries.to_vec(),
        expires_at: Instant::now() + RADIATION_CACHE_TTL,
    });
    Ok(())
}

pub async fn list_climate_anomalies(
    State(state): State<AppState>,
    Json(request): Json<ListClimateAnomaliesRequest>,
) -> Result<Json<ListClimateAnomaliesResponse>, AppError> {
    let all_anomalies = match get_cached_anomalies()? {
        Some(cached) => cached,
        None => {
            let end_date = Utc::now().date_naive();
            let start_date = end_date - ChronoDuration::days(30);
            let start_date_str = start_date.format("%Y-%m-%d").to_string();
            let end_date_str = end_date.format("%Y-%m-%d").to_string();

            let tasks = ZONES
                .iter()
                .map(|zone| fetch_zone_anomaly(&state, *zone, &start_date_str, &end_date_str));
            let results = join_all(tasks).await;
            let mut anomalies = results.into_iter().flatten().collect::<Vec<_>>();
            anomalies.sort_by(|a, b| {
                severity_rank(&b.severity)
                    .cmp(&severity_rank(&a.severity))
                    .then_with(|| b.temp_delta.abs().total_cmp(&a.temp_delta.abs()))
            });
            if !anomalies.is_empty() {
                set_cached_anomalies(&anomalies)?;
            }
            anomalies
        }
    };

    let min_rank = parse_min_severity(&request.min_severity);
    let mut filtered = all_anomalies
        .into_iter()
        .filter(|anomaly| severity_rank(&anomaly.severity) >= min_rank)
        .collect::<Vec<_>>();

    let total_count = filtered.len();
    filtered.truncate(page_size(&request));

    Ok(Json(ListClimateAnomaliesResponse {
        anomalies: filtered,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    }))
}

pub async fn get_global_radiation_situation(
    State(state): State<AppState>,
    Json(request): Json<GetGlobalRadiationSituationRequest>,
) -> Result<Json<GetGlobalRadiationSituationResponse>, AppError> {
    let (snapshot_at, entries) = match get_cached_radiation()? {
        Some(cached) => cached,
        None => {
            let tasks = RADIATION_ZONES
                .iter()
                .map(|zone| fetch_global_radiation_entry(&state, *zone));
            let mut entries = join_all(tasks)
                .await
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| {
                radiation_severity_rank(&b.severity)
                    .cmp(&radiation_severity_rank(&a.severity))
                    .then_with(|| {
                        b.shortwave_radiation_wm2
                            .total_cmp(&a.shortwave_radiation_wm2)
                    })
            });
            let snapshot_at = entries
                .iter()
                .map(|entry| entry.observed_at)
                .max()
                .unwrap_or_else(now_epoch_ms);
            if !entries.is_empty() {
                set_cached_radiation(snapshot_at, &entries)?;
            }
            (snapshot_at, entries)
        }
    };

    let has_observations = !entries.is_empty();
    let min_rank = parse_radiation_min_severity(&request.min_severity);
    let mut filtered = entries
        .into_iter()
        .filter(|entry| radiation_severity_rank(&entry.severity) >= min_rank)
        .collect::<Vec<_>>();
    let total_count = filtered.len();
    filtered.truncate(radiation_page_size(&request));

    Ok(Json(GetGlobalRadiationSituationResponse {
        snapshot_at,
        source: if has_observations {
            "RADIATION_SOURCE_OPEN_METEO".to_string()
        } else {
            "RADIATION_SOURCE_OPEN_METEO_UNAVAILABLE".to_string()
        },
        entries: filtered,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    }))
}
