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
const URLHAUS_RECENT_BASE: &str = "https://urlhaus-api.abuse.ch/v1/urls/recent/limit";
const OTX_INDICATORS_URL: &str =
    "https://otx.alienvault.com/api/v1/indicators/export?type=IPv4&modified_since=";
const ABUSEIPDB_BLACKLIST_URL: &str = "https://api.abuseipdb.com/api/v2/blacklist";

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

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

fn parse_u32(value: Option<&Value>) -> Option<u32> {
    let value = value?;
    if let Some(number) = value.as_u64() {
        return Some(number as u32);
    }
    value.as_str()?.trim().parse::<u32>().ok()
}

fn normalize_country(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() == 2 {
        return trimmed.to_ascii_uppercase();
    }
    trimmed.to_string()
}

fn country_centroid(country: &str) -> Option<GeoCoordinates> {
    match country.trim().to_ascii_uppercase().as_str() {
        "US" => Some(GeoCoordinates {
            latitude: 39.8283,
            longitude: -98.5795,
        }),
        "DE" => Some(GeoCoordinates {
            latitude: 51.1657,
            longitude: 10.4515,
        }),
        "FR" => Some(GeoCoordinates {
            latitude: 46.2276,
            longitude: 2.2137,
        }),
        "GB" | "UK" => Some(GeoCoordinates {
            latitude: 55.3781,
            longitude: -3.436,
        }),
        "NL" => Some(GeoCoordinates {
            latitude: 52.1326,
            longitude: 5.2913,
        }),
        "RU" => Some(GeoCoordinates {
            latitude: 61.524,
            longitude: 105.3188,
        }),
        "CN" => Some(GeoCoordinates {
            latitude: 35.8617,
            longitude: 104.1954,
        }),
        "SG" => Some(GeoCoordinates {
            latitude: 1.3521,
            longitude: 103.8198,
        }),
        "BR" => Some(GeoCoordinates {
            latitude: -14.235,
            longitude: -51.9253,
        }),
        "IN" => Some(GeoCoordinates {
            latitude: 20.5937,
            longitude: 78.9629,
        }),
        "UA" => Some(GeoCoordinates {
            latitude: 48.3794,
            longitude: 31.1656,
        }),
        _ => None,
    }
}

fn is_ipv4(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() != 4 {
        return false;
    }
    parts.iter().all(|part| {
        if part.is_empty() || part.len() > 3 {
            return false;
        }
        part.parse::<u8>().is_ok()
    })
}

fn infer_urlhaus_type(raw: &Value, tags: &[String]) -> &'static str {
    let threat =
        string_value(raw.get("threat").or_else(|| raw.get("threat_type"))).to_ascii_lowercase();
    let joined_tags = tags.join(" ");
    if threat.contains("phish") || joined_tags.contains("phish") {
        return "CYBER_THREAT_TYPE_PHISHING";
    }
    if threat.contains("malware") || threat.contains("payload") || joined_tags.contains("malware") {
        return "CYBER_THREAT_TYPE_MALWARE_HOST";
    }
    "CYBER_THREAT_TYPE_MALICIOUS_URL"
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

async fn fetch_urlhaus(state: &AppState, limit: usize, cutoff_ms: i64) -> Vec<CyberThreat> {
    let auth_key = std::env::var("URLHAUS_AUTH_KEY").unwrap_or_default();
    if auth_key.trim().is_empty() {
        return Vec::new();
    }

    let endpoint = format!("{}/{}/", URLHAUS_RECENT_BASE, limit.min(1_000));
    let response = match state
        .http_client
        .get(endpoint)
        .header("Accept", "application/json")
        .header("Auth-Key", auth_key.trim())
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
    let rows = payload
        .get("urls")
        .and_then(Value::as_array)
        .or_else(|| payload.get("data").and_then(Value::as_array))
        .cloned()
        .unwrap_or_default();

    rows.into_iter()
        .filter_map(|row| {
            let status = string_value(row.get("url_status").or_else(|| row.get("status")));
            if !status.is_empty() && !status.eq_ignore_ascii_case("online") {
                return None;
            }

            let raw_url = string_value(row.get("url").or_else(|| row.get("ioc")));
            if raw_url.is_empty() {
                return None;
            }

            let hostname = reqwest::Url::parse(&raw_url)
                .ok()
                .and_then(|url| url.host_str().map(|host| host.to_string()))
                .unwrap_or_default();
            let ip_candidate = string_value(row.get("host").or_else(|| row.get("ip_address")));
            let (indicator, indicator_type) = if is_ipv4(&ip_candidate) {
                (ip_candidate, "CYBER_THREAT_INDICATOR_TYPE_IP")
            } else if is_ipv4(&hostname) {
                (hostname, "CYBER_THREAT_INDICATOR_TYPE_IP")
            } else if !hostname.is_empty() {
                (hostname, "CYBER_THREAT_INDICATOR_TYPE_DOMAIN")
            } else {
                (raw_url, "CYBER_THREAT_INDICATOR_TYPE_URL")
            };

            let tags = row
                .get("tags")
                .and_then(Value::as_array)
                .map(|tags| {
                    tags.iter()
                        .filter_map(Value::as_str)
                        .map(|tag| tag.trim().to_ascii_lowercase())
                        .filter(|tag| !tag.is_empty())
                        .take(8)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let threat_type = infer_urlhaus_type(&row, &tags);
            let severity = if tags
                .iter()
                .any(|tag| tag.contains("ransomware") || tag.contains("botnet"))
            {
                "CRITICALITY_LEVEL_CRITICAL"
            } else if threat_type == "CYBER_THREAT_TYPE_MALWARE_HOST" {
                "CRITICALITY_LEVEL_HIGH"
            } else {
                "CRITICALITY_LEVEL_MEDIUM"
            };

            let first_seen_at = parse_timestamp_ms(
                row.get("dateadded")
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

            Some(CyberThreat {
                id: format!("urlhaus:{indicator_type}:{indicator}"),
                threat_type: threat_type.to_string(),
                source: "CYBER_THREAT_SOURCE_URLHAUS".to_string(),
                indicator,
                indicator_type: indicator_type.to_string(),
                location: None,
                country: normalize_country(&string_value(
                    row.get("country").or_else(|| row.get("country_code")),
                )),
                severity: severity.to_string(),
                malware_family: string_value(row.get("threat")),
                tags,
                first_seen_at,
                last_seen_at,
            })
        })
        .take(limit)
        .collect()
}

async fn fetch_otx(state: &AppState, limit: usize, since_days: i64) -> Vec<CyberThreat> {
    let api_key = std::env::var("OTX_API_KEY").unwrap_or_default();
    if api_key.trim().is_empty() {
        return Vec::new();
    }

    let since = (Utc::now() - chrono::Duration::days(since_days.max(1)))
        .format("%Y-%m-%d")
        .to_string();
    let endpoint = format!("{}{}", OTX_INDICATORS_URL, urlencoding::encode(&since));
    let response = match state
        .http_client
        .get(endpoint)
        .header("Accept", "application/json")
        .header("X-OTX-API-KEY", api_key.trim())
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
    let rows = payload
        .get("results")
        .and_then(Value::as_array)
        .or_else(|| payload.as_array())
        .cloned()
        .unwrap_or_default();

    rows.into_iter()
        .filter_map(|row| {
            let indicator = string_value(row.get("indicator").or_else(|| row.get("ip")));
            if !is_ipv4(&indicator) {
                return None;
            }
            let tags = row
                .get("tags")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter_map(Value::as_str)
                        .map(|tag| tag.trim().to_ascii_lowercase())
                        .filter(|tag| !tag.is_empty())
                        .take(8)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let is_c2 = tags
                .iter()
                .any(|tag| tag.contains("c2") || tag.contains("botnet"));
            let severity = if tags.iter().any(|tag| {
                tag.contains("ransomware")
                    || tag.contains("apt")
                    || tag.contains("c2")
                    || tag.contains("botnet")
            }) {
                "CRITICALITY_LEVEL_HIGH"
            } else {
                "CRITICALITY_LEVEL_MEDIUM"
            };

            let first_seen_at = parse_timestamp_ms(
                row.get("created")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            let last_seen_at = parse_timestamp_ms(
                row.get("modified")
                    .or_else(|| row.get("created"))
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .max(first_seen_at);

            Some(CyberThreat {
                id: format!("otx:{indicator}"),
                threat_type: if is_c2 {
                    "CYBER_THREAT_TYPE_C2_SERVER".to_string()
                } else {
                    "CYBER_THREAT_TYPE_MALWARE_HOST".to_string()
                },
                source: "CYBER_THREAT_SOURCE_OTX".to_string(),
                indicator,
                indicator_type: "CYBER_THREAT_INDICATOR_TYPE_IP".to_string(),
                location: None,
                country: String::new(),
                severity: severity.to_string(),
                malware_family: string_value(row.get("title").or_else(|| row.get("description"))),
                tags,
                first_seen_at,
                last_seen_at,
            })
        })
        .take(limit)
        .collect()
}

async fn fetch_abuseipdb(state: &AppState, limit: usize) -> Vec<CyberThreat> {
    let api_key = std::env::var("ABUSEIPDB_API_KEY").unwrap_or_default();
    if api_key.trim().is_empty() {
        return Vec::new();
    }

    let mut url = match reqwest::Url::parse(ABUSEIPDB_BLACKLIST_URL) {
        Ok(url) => url,
        Err(_) => return Vec::new(),
    };
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("confidenceMinimum", "90");
        query.append_pair("limit", &limit.min(500).to_string());
    }
    let response = match state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("Key", api_key.trim())
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
    let rows = payload
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    rows.into_iter()
        .filter_map(|row| {
            let indicator = string_value(row.get("ipAddress").or_else(|| row.get("ip")));
            if !is_ipv4(&indicator) {
                return None;
            }
            let score = parse_u32(row.get("abuseConfidenceScore")).unwrap_or(0);
            let severity = if score >= 95 {
                "CRITICALITY_LEVEL_CRITICAL"
            } else if score >= 80 {
                "CRITICALITY_LEVEL_HIGH"
            } else {
                "CRITICALITY_LEVEL_MEDIUM"
            };

            let country = normalize_country(&string_value(
                row.get("countryCode").or_else(|| row.get("country")),
            ));
            Some(CyberThreat {
                id: format!("abuseipdb:{indicator}"),
                threat_type: "CYBER_THREAT_TYPE_MALWARE_HOST".to_string(),
                source: "CYBER_THREAT_SOURCE_ABUSEIPDB".to_string(),
                indicator,
                indicator_type: "CYBER_THREAT_INDICATOR_TYPE_IP".to_string(),
                location: match (
                    parse_f64(row.get("latitude")),
                    parse_f64(row.get("longitude")),
                ) {
                    (Some(latitude), Some(longitude))
                        if (-90.0..=90.0).contains(&latitude)
                            && (-180.0..=180.0).contains(&longitude) =>
                    {
                        Some(GeoCoordinates {
                            latitude,
                            longitude,
                        })
                    }
                    _ => None,
                },
                country,
                severity: severity.to_string(),
                malware_family: String::new(),
                tags: vec![format!("score:{score}")],
                first_seen_at: 0,
                last_seen_at: parse_timestamp_ms(
                    row.get("lastReportedAt")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                ),
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
    let since_days = ((now_ms.saturating_sub(cutoff_ms)) / 86_400_000).clamp(1, 90);
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
    threats.extend(fetch_urlhaus(&state, limit, cutoff_ms).await);
    threats.extend(fetch_c2intel(&state, limit, now_ms).await);
    threats.extend(fetch_otx(&state, limit, since_days).await);
    threats.extend(fetch_abuseipdb(&state, limit).await);

    let mut seen = HashSet::new();
    threats.retain(|threat| seen.insert(format!("{}:{}", threat.source, threat.indicator)));

    for threat in threats.iter_mut() {
        if threat.location.is_none() {
            threat.location = country_centroid(&threat.country);
        }
    }

    threats.retain(|threat| within_time_range(threat.last_seen_at, request.time_range.as_ref()));
    threats.retain(|threat| matches_type_filter(&request.r#type, &threat.threat_type));
    threats.retain(|threat| matches_source_filter(&request.source, &threat.source));
    threats.retain(|threat| severity_rank(&threat.severity) >= min_rank);
    threats.retain(|threat| threat.location.is_some());

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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{country_centroid, infer_urlhaus_type, severity_rank};

    #[test]
    fn urlhaus_type_prefers_phishing_signal() {
        let record = json!({"threat":"phishing-kit"});
        let tags = vec!["credential-theft".to_string()];
        let ty = infer_urlhaus_type(&record, &tags);
        assert_eq!(ty, "CYBER_THREAT_TYPE_PHISHING");
    }

    #[test]
    fn country_centroid_resolves_known_code() {
        let centroid = country_centroid("DE").expect("known country centroid");
        assert!(centroid.latitude > 40.0);
        assert!(centroid.longitude > 0.0);
    }

    #[test]
    fn severity_rank_orders_critical_above_medium() {
        assert!(
            severity_rank("CRITICALITY_LEVEL_CRITICAL") > severity_rank("CRITICALITY_LEVEL_MEDIUM")
        );
    }
}
