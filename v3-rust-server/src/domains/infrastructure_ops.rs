use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{Datelike, TimeZone, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const CLOUDFLARE_RADAR_URL: &str = "https://api.cloudflare.com/client/v4/radar/annotations/outages";
const NGA_WARNINGS_URL: &str =
    "https://msi.nga.mil/api/publications/broadcast-warn?output=json&status=A";

const OUTAGE_CACHE_TTL: Duration = Duration::from_secs(300);
const CABLE_CACHE_TTL: Duration = Duration::from_secs(180);

const BASELINE_TTL_SECONDS: i64 = 90 * 24 * 60 * 60;
const MIN_SAMPLES: i32 = 10;
const Z_THRESHOLD_LOW: f64 = 1.5;
const Z_THRESHOLD_MEDIUM: f64 = 2.0;
const Z_THRESHOLD_HIGH: f64 = 3.0;

const VALID_BASELINE_TYPES: [&str; 6] = [
    "military_flights",
    "vessels",
    "protests",
    "news",
    "ais_gaps",
    "satellite_fires",
];

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListInternetOutagesRequest {
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

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
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
pub struct ListInternetOutagesResponse {
    pub outages: Vec<InternetOutage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InternetOutage {
    pub id: String,
    pub title: String,
    pub link: String,
    pub description: String,
    pub detected_at: i64,
    pub country: String,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub severity: String,
    pub categories: Vec<String>,
    pub cause: String,
    pub outage_type: String,
    pub ended_at: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTemporalBaselineRequest {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub count: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetTemporalBaselineResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anomaly: Option<BaselineAnomaly>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineStats>,
    pub learning: bool,
    pub sample_count: i32,
    pub samples_needed: i32,
    pub error: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BaselineAnomaly {
    pub z_score: f64,
    pub severity: String,
    pub multiplier: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BaselineStats {
    pub mean: f64,
    pub std_dev: f64,
    pub sample_count: i32,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RecordBaselineSnapshotRequest {
    #[serde(default)]
    pub updates: Vec<BaselineUpdate>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BaselineUpdate {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub count: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RecordBaselineSnapshotResponse {
    pub updated: i32,
    pub error: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetCableHealthRequest {}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetCableHealthResponse {
    pub generated_at: i64,
    pub cables: HashMap<String, CableHealthRecord>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CableHealthRecord {
    pub status: String,
    pub score: f64,
    pub confidence: f64,
    pub last_updated: i64,
    pub evidence: Vec<CableHealthEvidence>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CableHealthEvidence {
    pub source: String,
    pub summary: String,
    pub ts: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CloudflareResponse {
    #[serde(default)]
    configured: Option<bool>,
    #[serde(default)]
    success: bool,
    #[serde(default)]
    errors: Vec<CloudflareError>,
    #[serde(default)]
    result: CloudflareResult,
}

#[derive(Debug, Deserialize, Default)]
struct CloudflareError {
    #[serde(default)]
    code: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CloudflareResult {
    #[serde(default)]
    annotations: Vec<CloudflareOutage>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CloudflareOutage {
    #[serde(default)]
    id: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    start_date: String,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    locations: Vec<String>,
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    linked_url: String,
    #[serde(default)]
    locations_details: Vec<CloudflareLocationDetail>,
    #[serde(default)]
    asns_details: Vec<CloudflareAsnDetail>,
    #[serde(default)]
    outage: CloudflareOutageMeta,
}

#[derive(Debug, Deserialize, Default)]
struct CloudflareLocationDetail {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize, Default)]
struct CloudflareAsnDetail {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CloudflareOutageMeta {
    #[serde(default)]
    outage_cause: String,
    #[serde(default)]
    outage_type: String,
}

#[derive(Debug, Clone)]
struct BaselineEntry {
    mean: f64,
    m2: f64,
    sample_count: i32,
    last_updated: i64,
}

#[derive(Debug, Deserialize, Default)]
struct NgaWarning {
    #[serde(default)]
    text: String,
    #[serde(default, alias = "issueDate")]
    issue_date: String,
}

#[derive(Debug, Clone)]
struct CableSignal {
    status: String,
    score: f64,
    confidence: f64,
    ts: i64,
    summary: String,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

static OUTAGE_CACHE: Lazy<Mutex<Option<CacheEntry<Vec<InternetOutage>>>>> =
    Lazy::new(|| Mutex::new(None));
static BASELINE_CACHE: Lazy<Mutex<HashMap<String, BaselineEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static CABLE_CACHE: Lazy<Mutex<Option<CacheEntry<GetCableHealthResponse>>>> =
    Lazy::new(|| Mutex::new(None));

static DATE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(\d{2})(\d{4})Z\s+([A-Z]{3})\s+(\d{4})").expect("valid date regex")
});

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn page_size(request: &ListInternetOutagesRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn outage_severity(outage_type: &str) -> String {
    if outage_type.eq_ignore_ascii_case("NATIONWIDE") {
        "OUTAGE_SEVERITY_TOTAL".to_string()
    } else if outage_type.eq_ignore_ascii_case("REGIONAL") {
        "OUTAGE_SEVERITY_MAJOR".to_string()
    } else {
        "OUTAGE_SEVERITY_PARTIAL".to_string()
    }
}

fn to_epoch_ms(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.timestamp_millis())
        .unwrap_or(0)
}

fn country_coords(code: &str) -> Option<(f64, f64)> {
    match code {
        "US" => Some((37.09, -95.71)),
        "GB" => Some((55.37, -3.44)),
        "DE" => Some((51.17, 10.45)),
        "FR" => Some((46.23, 2.21)),
        "ES" => Some((40.46, -3.75)),
        "IT" => Some((41.87, 12.57)),
        "RU" => Some((61.52, 105.32)),
        "CN" => Some((35.86, 104.20)),
        "IN" => Some((20.59, 78.96)),
        "JP" => Some((36.20, 138.25)),
        "BR" => Some((-14.24, -51.93)),
        "AU" => Some((-25.27, 133.78)),
        "CA" => Some((56.13, -106.35)),
        "MX" => Some((23.63, -102.55)),
        "ZA" => Some((-30.56, 22.94)),
        _ => None,
    }
}

fn get_cached_outages() -> Result<Option<Vec<InternetOutage>>, AppError> {
    let cache = OUTAGE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("outage cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cached_outages(outages: &[InternetOutage]) -> Result<(), AppError> {
    let mut cache = OUTAGE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("outage cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: outages.to_vec(),
        expires_at: Instant::now() + OUTAGE_CACHE_TTL,
    });
    Ok(())
}

fn get_cached_cables() -> Result<Option<GetCableHealthResponse>, AppError> {
    let cache = CABLE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("cable cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cached_cables(response: &GetCableHealthResponse) -> Result<(), AppError> {
    let mut cache = CABLE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("cable cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: response.clone(),
        expires_at: Instant::now() + CABLE_CACHE_TTL,
    });
    Ok(())
}

async fn fetch_cloudflare_outages(state: &AppState) -> Vec<InternetOutage> {
    let Ok(token) = std::env::var("CLOUDFLARE_API_TOKEN") else {
        return Vec::new();
    };
    if token.trim().is_empty() {
        return Vec::new();
    }

    let response = match state
        .http_client
        .get(format!("{}?dateRange=7d&limit=50", CLOUDFLARE_RADAR_URL))
        .header("Authorization", format!("Bearer {}", token))
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return Vec::new(),
    };

    let payload = match response.json::<CloudflareResponse>().await {
        Ok(payload) => payload,
        Err(_) => return Vec::new(),
    };

    if payload.configured == Some(false) || !payload.success || !payload.errors.is_empty() {
        return Vec::new();
    }

    payload
        .result
        .annotations
        .into_iter()
        .filter_map(|raw| {
            let country_code = raw.locations.first()?.to_uppercase();
            let coords = country_coords(country_code.as_str());
            let country = raw
                .locations_details
                .first()
                .map(|detail| detail.name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or(country_code.clone());

            let mut categories = vec!["Cloudflare Radar".to_string()];
            if !raw.outage.outage_cause.is_empty() {
                categories.push(raw.outage.outage_cause.replace('_', " "));
            }
            if !raw.outage.outage_type.is_empty() {
                categories.push(raw.outage.outage_type.clone());
            }
            for asn in raw.asns_details.into_iter().take(2) {
                if !asn.name.is_empty() {
                    categories.push(asn.name);
                }
            }

            let title = raw
                .scope
                .clone()
                .map(|scope| format!("{} outage in {}", scope, country))
                .unwrap_or_else(|| format!("Internet disruption in {}", country));

            Some(InternetOutage {
                id: format!("cf-{}", raw.id),
                title,
                link: if raw.linked_url.is_empty() {
                    "https://radar.cloudflare.com/outage-center".to_string()
                } else {
                    raw.linked_url
                },
                description: raw.description,
                detected_at: to_epoch_ms(raw.start_date.as_str()),
                country,
                region: String::new(),
                location: coords.map(|(latitude, longitude)| GeoCoordinates {
                    latitude,
                    longitude,
                }),
                severity: outage_severity(raw.outage.outage_type.as_str()),
                categories,
                cause: raw.outage.outage_cause,
                outage_type: raw.outage.outage_type,
                ended_at: raw
                    .end_date
                    .as_ref()
                    .map(|date| to_epoch_ms(date))
                    .unwrap_or(0),
            })
        })
        .collect::<Vec<_>>()
}

fn make_baseline_key(kind: &str, region: &str, weekday: u32, month: u32) -> String {
    format!("baseline:{}:{}:{}:{}", kind, region, weekday, month)
}

fn baseline_severity(z_score: f64) -> String {
    if z_score >= Z_THRESHOLD_HIGH {
        "critical".to_string()
    } else if z_score >= Z_THRESHOLD_MEDIUM {
        "high".to_string()
    } else if z_score >= Z_THRESHOLD_LOW {
        "medium".to_string()
    } else {
        "normal".to_string()
    }
}

fn cable_related(text: &str) -> bool {
    let upper = text.to_ascii_uppercase();
    [
        "CABLE",
        "CABLESHIP",
        "CABLE SHIP",
        "SUBMARINE CABLE",
        "UNDERSEA CABLE",
        "FIBER OPTIC",
    ]
    .iter()
    .any(|keyword| upper.contains(keyword))
}

fn cable_id_from_text(text: &str) -> String {
    const MAP: [(&str, &str); 18] = [
        ("MAREA", "marea"),
        ("GRACE HOPPER", "grace_hopper"),
        ("HAVFRUE", "havfrue"),
        ("FASTER", "faster"),
        ("SOUTHERN CROSS", "southern_cross"),
        ("CURIE", "curie"),
        ("SEA-ME-WE", "seamewe6"),
        ("SEAMEWE", "seamewe6"),
        ("SMW6", "seamewe6"),
        ("FLAG", "flag"),
        ("2AFRICA", "2africa"),
        ("WACS", "wacs"),
        ("EASSY", "eassy"),
        ("SAM-1", "sam1"),
        ("ELLALINK", "ellalink"),
        ("APG", "apg"),
        ("INDIGO", "indigo"),
        ("SJC", "sjc"),
    ];

    let upper = text.to_ascii_uppercase();
    if let Some((_, id)) = MAP.iter().find(|(name, _)| upper.contains(*name)) {
        return (*id).to_string();
    }

    let mut hasher = DefaultHasher::new();
    upper.chars().take(80).collect::<String>().hash(&mut hasher);
    format!("cable_{:x}", hasher.finish())
}

fn parse_issue_date(date_str: &str) -> i64 {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(date_str) {
        return parsed.timestamp_millis();
    }

    if let Some(captures) = DATE_RE.captures(date_str) {
        let day = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<u32>().ok())
            .unwrap_or(1);
        let hour = captures
            .get(2)
            .map(|value| value.as_str())
            .and_then(|value| value.get(0..2))
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let minute = captures
            .get(2)
            .map(|value| value.as_str())
            .and_then(|value| value.get(2..4))
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let month = captures
            .get(3)
            .map(|value| value.as_str().to_ascii_uppercase())
            .map(|month| match month.as_str() {
                "JAN" => 1,
                "FEB" => 2,
                "MAR" => 3,
                "APR" => 4,
                "MAY" => 5,
                "JUN" => 6,
                "JUL" => 7,
                "AUG" => 8,
                "SEP" => 9,
                "OCT" => 10,
                "NOV" => 11,
                "DEC" => 12,
                _ => 1,
            })
            .unwrap_or(1);
        let year = captures
            .get(4)
            .and_then(|value| value.as_str().parse::<i32>().ok())
            .unwrap_or(Utc::now().year());

        if let Some(parsed) = Utc
            .with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
        {
            return parsed.timestamp_millis();
        }
    }

    now_epoch_ms()
}

async fn fetch_nga_warnings(state: &AppState) -> Vec<NgaWarning> {
    let response = match state
        .http_client
        .get(NGA_WARNINGS_URL)
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

    let array = if let Some(items) = payload.as_array() {
        items.clone()
    } else {
        payload
            .get("warnings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };

    array
        .into_iter()
        .filter_map(|warning| serde_json::from_value::<NgaWarning>(warning).ok())
        .collect::<Vec<_>>()
}

fn warning_signal(text: &str) -> (String, f64, f64) {
    let upper = text.to_ascii_uppercase();
    let fault = [
        "FAULT", "BREAK", "CUT", "DAMAGE", "SEVERED", "OUTAGE", "FAILURE",
    ]
    .iter()
    .any(|keyword| upper.contains(keyword));
    if fault {
        return ("CABLE_HEALTH_STATUS_FAULT".to_string(), 1.0, 0.9);
    }

    let repair = ["REPAIR", "ON STATION", "OPERATIONS IN PROGRESS", "LAYING"]
        .iter()
        .any(|keyword| upper.contains(keyword));
    if repair {
        return ("CABLE_HEALTH_STATUS_DEGRADED".to_string(), 0.75, 0.85);
    }

    ("CABLE_HEALTH_STATUS_DEGRADED".to_string(), 0.55, 0.65)
}

fn process_cable_signals(warnings: &[NgaWarning]) -> HashMap<String, CableHealthRecord> {
    let mut signals: HashMap<String, Vec<CableSignal>> = HashMap::new();

    for warning in warnings {
        if !cable_related(&warning.text) {
            continue;
        }

        let cable_id = cable_id_from_text(&warning.text);
        let ts = parse_issue_date(&warning.issue_date);
        let (status, score, confidence) = warning_signal(&warning.text);
        let summary = warning.text.chars().take(180).collect::<String>();

        signals.entry(cable_id).or_default().push(CableSignal {
            status,
            score,
            confidence,
            ts,
            summary,
        });
    }

    let mut output = HashMap::new();
    for (cable_id, mut cable_signals) in signals {
        cable_signals.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let top = match cable_signals.first() {
            Some(top) => top,
            None => continue,
        };

        let has_fault = cable_signals
            .iter()
            .any(|signal| signal.status == "CABLE_HEALTH_STATUS_FAULT" && signal.score >= 0.8);

        let status = if has_fault {
            "CABLE_HEALTH_STATUS_FAULT".to_string()
        } else if top.score >= 0.5 {
            "CABLE_HEALTH_STATUS_DEGRADED".to_string()
        } else {
            "CABLE_HEALTH_STATUS_OK".to_string()
        };

        let last_updated = cable_signals
            .iter()
            .map(|signal| signal.ts)
            .max()
            .unwrap_or(0);
        let evidence = cable_signals
            .iter()
            .take(3)
            .map(|signal| CableHealthEvidence {
                source: "NGA".to_string(),
                summary: signal.summary.clone(),
                ts: signal.ts,
            })
            .collect::<Vec<_>>();

        output.insert(
            cable_id,
            CableHealthRecord {
                status,
                score: round_two(top.score),
                confidence: round_two(top.confidence),
                last_updated,
                evidence,
            },
        );
    }

    output
}

pub async fn list_internet_outages(
    State(state): State<AppState>,
    Json(request): Json<ListInternetOutagesRequest>,
) -> Result<Json<ListInternetOutagesResponse>, AppError> {
    let all_outages = if let Some(cached) = get_cached_outages()? {
        cached
    } else {
        let fresh = fetch_cloudflare_outages(&state).await;
        if !fresh.is_empty() {
            set_cached_outages(&fresh)?;
        }
        fresh
    };

    let mut filtered = all_outages;

    if !request.country.trim().is_empty() {
        let target = request.country.to_ascii_lowercase();
        filtered.retain(|outage| {
            outage
                .country
                .to_ascii_lowercase()
                .contains(target.as_str())
        });
    }

    if let Some(range) = request.time_range.as_ref() {
        if range.start > 0 {
            filtered.retain(|outage| outage.detected_at >= range.start);
        }
        if range.end > 0 {
            filtered.retain(|outage| outage.detected_at <= range.end);
        }
    }

    filtered.sort_by(|left, right| right.detected_at.cmp(&left.detected_at));

    let total_count = filtered.len();
    filtered.truncate(page_size(&request));

    Ok(Json(ListInternetOutagesResponse {
        outages: filtered,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    }))
}

pub async fn get_temporal_baseline(
    Json(request): Json<GetTemporalBaselineRequest>,
) -> Result<Json<GetTemporalBaselineResponse>, AppError> {
    if request.r#type.trim().is_empty()
        || !VALID_BASELINE_TYPES
            .iter()
            .any(|kind| *kind == request.r#type.trim())
        || !request.count.is_finite()
    {
        return Ok(Json(GetTemporalBaselineResponse {
            anomaly: None,
            baseline: None,
            learning: false,
            sample_count: 0,
            samples_needed: 0,
            error: "Missing or invalid params: type and count required".to_string(),
        }));
    }

    let region = if request.region.trim().is_empty() {
        "global"
    } else {
        request.region.trim()
    };

    let now = Utc::now();
    let key = make_baseline_key(
        request.r#type.trim(),
        region,
        now.weekday().num_days_from_sunday(),
        now.month(),
    );

    let baseline = {
        let mut map = BASELINE_CACHE
            .lock()
            .map_err(|_| AppError::Internal("baseline cache lock poisoned".to_string()))?;

        map.retain(|_, value| now_epoch_ms() - value.last_updated <= BASELINE_TTL_SECONDS * 1000);
        map.get(key.as_str()).cloned()
    };

    let Some(baseline) = baseline else {
        return Ok(Json(GetTemporalBaselineResponse {
            anomaly: None,
            baseline: None,
            learning: true,
            sample_count: 0,
            samples_needed: MIN_SAMPLES,
            error: String::new(),
        }));
    };

    if baseline.sample_count < MIN_SAMPLES {
        return Ok(Json(GetTemporalBaselineResponse {
            anomaly: None,
            baseline: None,
            learning: true,
            sample_count: baseline.sample_count,
            samples_needed: MIN_SAMPLES,
            error: String::new(),
        }));
    }

    let variance = if baseline.sample_count > 1 {
        (baseline.m2 / (baseline.sample_count - 1) as f64).max(0.0)
    } else {
        0.0
    };
    let std_dev = variance.sqrt();
    let z_score = if std_dev > 0.0 {
        ((request.count - baseline.mean) / std_dev).abs()
    } else {
        0.0
    };

    let multiplier = if baseline.mean > 0.0 {
        round_two(request.count / baseline.mean)
    } else if request.count > 0.0 {
        999.0
    } else {
        1.0
    };

    Ok(Json(GetTemporalBaselineResponse {
        anomaly: (z_score >= Z_THRESHOLD_LOW).then_some(BaselineAnomaly {
            z_score: round_two(z_score),
            severity: baseline_severity(z_score),
            multiplier,
        }),
        baseline: Some(BaselineStats {
            mean: round_two(baseline.mean),
            std_dev: round_two(std_dev),
            sample_count: baseline.sample_count,
        }),
        learning: false,
        sample_count: baseline.sample_count,
        samples_needed: MIN_SAMPLES,
        error: String::new(),
    }))
}

pub async fn record_baseline_snapshot(
    Json(request): Json<RecordBaselineSnapshotRequest>,
) -> Result<Json<RecordBaselineSnapshotResponse>, AppError> {
    if request.updates.is_empty() {
        return Ok(Json(RecordBaselineSnapshotResponse {
            updated: 0,
            error: "Body must have updates array".to_string(),
        }));
    }

    let now = Utc::now();
    let weekday = now.weekday().num_days_from_sunday();
    let month = now.month();

    let mut updated = 0;
    let mut map = BASELINE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("baseline cache lock poisoned".to_string()))?;

    for item in request.updates.iter().take(20) {
        if item.r#type.trim().is_empty()
            || !VALID_BASELINE_TYPES
                .iter()
                .any(|kind| *kind == item.r#type.trim())
            || !item.count.is_finite()
        {
            continue;
        }

        let region = if item.region.trim().is_empty() {
            "global"
        } else {
            item.region.trim()
        };

        let key = make_baseline_key(item.r#type.trim(), region, weekday, month);
        let previous = map.get(key.as_str()).cloned().unwrap_or(BaselineEntry {
            mean: 0.0,
            m2: 0.0,
            sample_count: 0,
            last_updated: 0,
        });

        let n = previous.sample_count + 1;
        let delta = item.count - previous.mean;
        let new_mean = previous.mean + delta / n as f64;
        let delta2 = item.count - new_mean;
        let new_m2 = previous.m2 + delta * delta2;

        map.insert(
            key,
            BaselineEntry {
                mean: new_mean,
                m2: new_m2,
                sample_count: n,
                last_updated: now_epoch_ms(),
            },
        );
        updated += 1;
    }

    Ok(Json(RecordBaselineSnapshotResponse {
        updated,
        error: String::new(),
    }))
}

pub async fn get_cable_health(
    State(state): State<AppState>,
    Json(_request): Json<GetCableHealthRequest>,
) -> Result<Json<GetCableHealthResponse>, AppError> {
    if let Some(cached) = get_cached_cables()? {
        return Ok(Json(cached));
    }

    let warnings = fetch_nga_warnings(&state).await;
    let cables = process_cable_signals(&warnings);

    let response = GetCableHealthResponse {
        generated_at: now_epoch_ms(),
        cables,
    };

    if !response.cables.is_empty() {
        set_cached_cables(&response)?;
    }

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_outage_severity() {
        assert_eq!(outage_severity("NATIONWIDE"), "OUTAGE_SEVERITY_TOTAL");
        assert_eq!(outage_severity("REGIONAL"), "OUTAGE_SEVERITY_MAJOR");
        assert_eq!(outage_severity("LOCAL"), "OUTAGE_SEVERITY_PARTIAL");
    }

    #[test]
    fn computes_baseline_severity_levels() {
        assert_eq!(baseline_severity(0.5), "normal");
        assert_eq!(baseline_severity(1.6), "medium");
        assert_eq!(baseline_severity(2.5), "high");
        assert_eq!(baseline_severity(3.5), "critical");
    }

    #[test]
    fn identifies_cable_related_warnings() {
        assert!(cable_related("SUBMARINE CABLE maintenance"));
        assert!(!cable_related("weather bulletin"));
    }

    #[test]
    fn maps_warning_signal_types() {
        let (status, score, _) = warning_signal("Cable FAULT reported near coast");
        assert_eq!(status, "CABLE_HEALTH_STATUS_FAULT");
        assert_eq!(score, 1.0);

        let (status, score, _) = warning_signal("Cable repair vessel on station");
        assert_eq!(status, "CABLE_HEALTH_STATUS_DEGRADED");
        assert!(score >= 0.7);
    }
}
