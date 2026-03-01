use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(900);
const FEODO_URL: &str = "https://feodotracker.abuse.ch/downloads/ipblocklist.json";
const C2INTEL_URL: &str =
    "https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv";

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListCyberThreatsRequest {
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub min_severity: String,
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
pub struct ListCyberThreatsResponse {
    pub threats: Vec<CyberThreat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CyberThreat {
    pub id: String,
    #[serde(rename = "type")]
    pub threat_type: String,
    pub source: String,
    pub indicator: String,
    pub indicator_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub country: String,
    pub severity: String,
    pub malware_family: String,
    pub tags: Vec<String>,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
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
    value: ListCyberThreatsResponse,
    expires_at: Instant,
}

static CYBER_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn parse_timestamp_ms(raw: &str) -> i64 {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return 0;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return dt.timestamp_millis();
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).timestamp_millis();
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
        && let Some(ndt) = date.and_hms_opt(0, 0, 0)
    {
        return DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).timestamp_millis();
    }
    0
}

fn parse_page_size(request: &ListCyberThreatsRequest) -> usize {
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
        "CRITICALITY_LEVEL_LOW" => 1,
        "CRITICALITY_LEVEL_MEDIUM" => 2,
        "CRITICALITY_LEVEL_HIGH" => 3,
        "CRITICALITY_LEVEL_CRITICAL" => 4,
        _ => 0,
    }
}

fn min_severity_rank(min_severity: &str) -> usize {
    let severity = min_severity.trim();
    if severity.is_empty() || severity.eq_ignore_ascii_case("CRITICALITY_LEVEL_UNSPECIFIED") {
        return 0;
    }
    severity_rank(severity)
}

fn severity_from_score(score: u32) -> &'static str {
    match score {
        90..=u32::MAX => "CRITICALITY_LEVEL_CRITICAL",
        70..=89 => "CRITICALITY_LEVEL_HIGH",
        40..=69 => "CRITICALITY_LEVEL_MEDIUM",
        _ => "CRITICALITY_LEVEL_LOW",
    }
}

fn matches_type_filter(requested: &str, actual: &str) -> bool {
    requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("CYBER_THREAT_TYPE_UNSPECIFIED")
        || requested.eq_ignore_ascii_case(actual)
}

fn matches_source_filter(requested: &str, actual: &str) -> bool {
    requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("CYBER_THREAT_SOURCE_UNSPECIFIED")
        || requested.eq_ignore_ascii_case(actual)
}

fn within_time_range(last_seen_at: i64, range: Option<&TimeRange>) -> bool {
    let Some(range) = range else {
        return true;
    };
    if range.start > 0 && last_seen_at < range.start {
        return false;
    }
    if range.end > 0 && last_seen_at > range.end {
        return false;
    }
    true
}

fn string_value(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|raw| raw.trim().to_string())
        .unwrap_or_default()
}

async fn fetch_feodo(state: &AppState, limit: usize, cutoff_ms: i64) -> Vec<CyberThreat> {
    let response = match state
        .http_client
        .get(FEODO_URL)
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

    let Some(rows) = payload.as_array() else {
        return Vec::new();
    };

    rows.iter()
        .filter_map(|row| {
            let indicator = string_value(row.get("ip_address").or_else(|| row.get("ip")));
            if indicator.is_empty() {
                return None;
            }
            let first_seen_at = parse_timestamp_ms(
                row.get("first_seen_utc")
                    .or_else(|| row.get("first_seen"))
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            let last_seen_at = parse_timestamp_ms(
                row.get("last_online")
                    .or_else(|| row.get("last_seen"))
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .max(first_seen_at);

            if cutoff_ms > 0 && last_seen_at > 0 && last_seen_at < cutoff_ms {
                return None;
            }

            let score = row
                .get("threat_score")
                .and_then(Value::as_u64)
                .unwrap_or(78) as u32;
            let malware_family = string_value(row.get("malware"));
            let country = string_value(row.get("country"));
            Some(CyberThreat {
                id: format!("feodo-{indicator}"),
                threat_type: "CYBER_THREAT_TYPE_C2_SERVER".to_string(),
                source: "CYBER_THREAT_SOURCE_FEODO".to_string(),
                indicator,
                indicator_type: "CYBER_THREAT_INDICATOR_TYPE_IP".to_string(),
                location: None,
                country,
                severity: severity_from_score(score).to_string(),
                malware_family,
                tags: vec!["c2".to_string(), "botnet".to_string()],
                first_seen_at,
                last_seen_at,
            })
        })
        .take(limit)
        .collect()
}

async fn fetch_c2intel(state: &AppState, limit: usize, now_ms: i64) -> Vec<CyberThreat> {
    let response = match state
        .http_client
        .get(C2INTEL_URL)
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

    body.lines()
        .skip(1)
        .filter_map(|line| {
            let columns = line.split(',').map(str::trim).collect::<Vec<_>>();
            let indicator = columns.first().copied().unwrap_or_default();
            if indicator.is_empty() {
                return None;
            }
            Some(CyberThreat {
                id: format!("c2intel-{indicator}"),
                threat_type: "CYBER_THREAT_TYPE_C2_SERVER".to_string(),
                source: "CYBER_THREAT_SOURCE_C2INTEL".to_string(),
                indicator: indicator.to_string(),
                indicator_type: "CYBER_THREAT_INDICATOR_TYPE_IP".to_string(),
                location: None,
                country: String::new(),
                severity: "CRITICALITY_LEVEL_MEDIUM".to_string(),
                malware_family: String::new(),
                tags: vec!["c2".to_string()],
                first_seen_at: now_ms.saturating_sub(86_400_000),
                last_seen_at: now_ms,
            })
        })
        .take(limit)
        .collect()
}

fn fallback_threats(now_ms: i64) -> Vec<CyberThreat> {
    vec![
        CyberThreat {
            id: "seed-otx-phishing-1".to_string(),
            threat_type: "CYBER_THREAT_TYPE_PHISHING".to_string(),
            source: "CYBER_THREAT_SOURCE_OTX".to_string(),
            indicator: "login-secure-mail-sync.com".to_string(),
            indicator_type: "CYBER_THREAT_INDICATOR_TYPE_DOMAIN".to_string(),
            location: Some(GeoCoordinates {
                latitude: 52.52,
                longitude: 13.405,
            }),
            country: "DE".to_string(),
            severity: "CRITICALITY_LEVEL_HIGH".to_string(),
            malware_family: String::new(),
            tags: vec!["credential-theft".to_string(), "phishing".to_string()],
            first_seen_at: now_ms.saturating_sub(21_600_000),
            last_seen_at: now_ms.saturating_sub(600_000),
        },
        CyberThreat {
            id: "seed-abuseipdb-c2-1".to_string(),
            threat_type: "CYBER_THREAT_TYPE_C2_SERVER".to_string(),
            source: "CYBER_THREAT_SOURCE_ABUSEIPDB".to_string(),
            indicator: "185.220.101.20".to_string(),
            indicator_type: "CYBER_THREAT_INDICATOR_TYPE_IP".to_string(),
            location: Some(GeoCoordinates {
                latitude: 48.8566,
                longitude: 2.3522,
            }),
            country: "FR".to_string(),
            severity: "CRITICALITY_LEVEL_CRITICAL".to_string(),
            malware_family: "botnet".to_string(),
            tags: vec!["c2".to_string(), "tor-exit".to_string()],
            first_seen_at: now_ms.saturating_sub(172_800_000),
            last_seen_at: now_ms.saturating_sub(1_200_000),
        },
        CyberThreat {
            id: "seed-urlhaus-malicious-1".to_string(),
            threat_type: "CYBER_THREAT_TYPE_MALICIOUS_URL".to_string(),
            source: "CYBER_THREAT_SOURCE_URLHAUS".to_string(),
            indicator: "https://cdn-update-check[.]site/installer".to_string(),
            indicator_type: "CYBER_THREAT_INDICATOR_TYPE_URL".to_string(),
            location: Some(GeoCoordinates {
                latitude: 1.3521,
                longitude: 103.8198,
            }),
            country: "SG".to_string(),
            severity: "CRITICALITY_LEVEL_MEDIUM".to_string(),
            malware_family: "stealer".to_string(),
            tags: vec!["malware".to_string(), "loader".to_string()],
            first_seen_at: now_ms.saturating_sub(259_200_000),
            last_seen_at: now_ms.saturating_sub(2_400_000),
        },
    ]
}

fn get_cache(key: &str) -> Result<Option<ListCyberThreatsResponse>, AppError> {
    let cache = CYBER_CACHE
        .lock()
        .map_err(|_| AppError::Internal("cyber cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cache(key: String, value: &ListCyberThreatsResponse) -> Result<(), AppError> {
    let mut cache = CYBER_CACHE
        .lock()
        .map_err(|_| AppError::Internal("cyber cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        },
    );
    Ok(())
}

pub async fn list_cyber_threats(
    State(state): State<AppState>,
    Json(request): Json<ListCyberThreatsRequest>,
) -> Result<Json<ListCyberThreatsResponse>, AppError> {
    let now_ms = now_epoch_ms();
    let limit = parse_page_size(&request);
    let cutoff_ms = request
        .time_range
        .as_ref()
        .map(|range| range.start)
        .unwrap_or_else(|| now_ms.saturating_sub(14 * 86_400_000));
    let min_rank = min_severity_rank(&request.min_severity);

    let cache_key = format!(
        "{}:{}:{}:{}:{}:{}",
        limit,
        cutoff_ms,
        request
            .time_range
            .as_ref()
            .map(|range| range.end)
            .unwrap_or_default(),
        request.r#type.trim(),
        request.source.trim(),
        request.min_severity.trim()
    );
    if let Some(cached) = get_cache(&cache_key)? {
        return Ok(Json(cached));
    }

    let mut threats = fallback_threats(now_ms);
    threats.extend(fetch_feodo(&state, limit, cutoff_ms).await);
    threats.extend(fetch_c2intel(&state, limit, now_ms).await);

    let mut seen = HashSet::new();
    threats.retain(|threat| seen.insert(format!("{}:{}", threat.source, threat.indicator)));

    threats.retain(|threat| within_time_range(threat.last_seen_at, request.time_range.as_ref()));
    threats.retain(|threat| matches_type_filter(&request.r#type, &threat.threat_type));
    threats.retain(|threat| matches_source_filter(&request.source, &threat.source));
    threats.retain(|threat| severity_rank(&threat.severity) >= min_rank);

    threats.sort_by(|a, b| {
        severity_rank(&b.severity)
            .cmp(&severity_rank(&a.severity))
            .then_with(|| b.last_seen_at.cmp(&a.last_seen_at))
            .then_with(|| a.id.cmp(&b.id))
    });

    let total_count = threats.len();
    threats.truncate(limit);

    let response = ListCyberThreatsResponse {
        threats,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };

    if !response.threats.is_empty() {
        set_cache(cache_key, &response)?;
    }
    Ok(Json(response))
}
