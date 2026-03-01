use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{Duration as ChronoDuration, Utc};
use futures::future::join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(1_800);
const OPEN_METEO_BASE: &str = "https://archive-api.open-meteo.com/v1/archive";

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

static CLIMATE_CACHE: Lazy<Mutex<Option<CacheEntry>>> = Lazy::new(|| Mutex::new(None));

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

fn page_size(request: &ListClimateAnomaliesRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
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

fn parse_min_severity(requested: &str) -> usize {
    if requested.trim().is_empty() || requested.eq_ignore_ascii_case("ANOMALY_SEVERITY_UNSPECIFIED")
    {
        return 0;
    }
    severity_rank(requested.trim())
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
