use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{Duration as ChronoDuration, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{AppState, error::AppError};

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_MODEL: &str = "llama-3.1-8b-instant";
const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const GDELT_DOC_API: &str = "https://api.gdeltproject.org/api/v2/doc/doc";
const PIZZINT_API: &str = "https://www.pizzint.watch/api/dashboard-data";
const GDELT_BATCH_API: &str = "https://www.pizzint.watch/api/gdelt/batch";
const DEFAULT_GDELT_PAIRS: &str =
    "usa_russia,russia_ukraine,usa_china,china_taiwan,usa_iran,usa_venezuela";

const INTEL_CACHE_TTL: Duration = Duration::from_secs(7_200);
const RISK_CACHE_TTL: Duration = Duration::from_secs(600);
const RISK_STALE_TTL: Duration = Duration::from_secs(3_600);
const CLASSIFY_CACHE_TTL: Duration = Duration::from_secs(86_400);
const GDELT_DOC_CACHE_TTL: Duration = Duration::from_secs(600);
const PIZZINT_CACHE_TTL: Duration = Duration::from_secs(600);

const GDELT_DEFAULT_RECORDS: i32 = 10;
const GDELT_MAX_RECORDS: i32 = 20;
const TITLE_MAX_LEN: usize = 500;

const VALID_LEVELS: &[&str] = &["critical", "high", "medium", "low", "info"];
const VALID_CATEGORIES: &[&str] = &[
    "conflict",
    "protest",
    "disaster",
    "diplomatic",
    "economic",
    "terrorism",
    "cyber",
    "health",
    "environmental",
    "military",
    "crime",
    "infrastructure",
    "tech",
    "general",
];

const TIER1_COUNTRIES: [(&str, &str); 20] = [
    ("US", "United States"),
    ("RU", "Russia"),
    ("CN", "China"),
    ("UA", "Ukraine"),
    ("IR", "Iran"),
    ("IL", "Israel"),
    ("TW", "Taiwan"),
    ("KP", "North Korea"),
    ("SA", "Saudi Arabia"),
    ("TR", "Turkey"),
    ("PL", "Poland"),
    ("DE", "Germany"),
    ("FR", "France"),
    ("GB", "United Kingdom"),
    ("IN", "India"),
    ("PK", "Pakistan"),
    ("SY", "Syria"),
    ("YE", "Yemen"),
    ("MM", "Myanmar"),
    ("VE", "Venezuela"),
];

const COUNTRY_KEYWORDS: [(&str, &[&str]); 20] = [
    (
        "US",
        &[
            "united states",
            "usa",
            "america",
            "washington",
            "biden",
            "trump",
            "pentagon",
        ],
    ),
    ("RU", &["russia", "moscow", "kremlin", "putin"]),
    ("CN", &["china", "beijing", "xi jinping", "prc"]),
    ("UA", &["ukraine", "kyiv", "zelensky", "donbas"]),
    ("IR", &["iran", "tehran", "khamenei", "irgc"]),
    ("IL", &["israel", "tel aviv", "netanyahu", "idf", "gaza"]),
    ("TW", &["taiwan", "taipei"]),
    ("KP", &["north korea", "pyongyang", "kim jong"]),
    ("SA", &["saudi arabia", "riyadh"]),
    ("TR", &["turkey", "ankara", "erdogan"]),
    ("PL", &["poland", "warsaw"]),
    ("DE", &["germany", "berlin"]),
    ("FR", &["france", "paris", "macron"]),
    ("GB", &["britain", "uk", "london"]),
    ("IN", &["india", "delhi", "modi"]),
    ("PK", &["pakistan", "islamabad"]),
    ("SY", &["syria", "damascus"]),
    ("YE", &["yemen", "sanaa", "houthi"]),
    ("MM", &["myanmar", "burma"]),
    ("VE", &["venezuela", "caracas", "maduro"]),
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCountryIntelBriefRequest {
    pub country_code: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetCountryIntelBriefResponse {
    pub country_code: String,
    pub country_name: String,
    pub brief: String,
    pub model: String,
    pub generated_at: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetRiskScoresRequest {
    #[serde(default)]
    pub region: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetRiskScoresResponse {
    pub cii_scores: Vec<CiiScore>,
    pub strategic_risks: Vec<StrategicRisk>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetPizzintStatusRequest {
    #[serde(default)]
    pub include_gdelt: bool,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetPizzintStatusResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pizzint: Option<PizzintStatus>,
    pub tension_pairs: Vec<GdeltTensionPair>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PizzintStatus {
    pub defcon_level: i32,
    pub defcon_label: String,
    pub aggregate_activity: f64,
    pub active_spikes: i32,
    pub locations_monitored: i32,
    pub locations_open: i32,
    pub updated_at: i64,
    pub data_freshness: String,
    pub locations: Vec<PizzintLocation>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PizzintLocation {
    pub place_id: String,
    pub name: String,
    pub address: String,
    pub current_popularity: i32,
    pub percentage_of_usual: i32,
    pub is_spike: bool,
    pub spike_magnitude: f64,
    pub data_source: String,
    pub recorded_at: String,
    pub data_freshness: String,
    pub is_closed_now: bool,
    pub lat: f64,
    pub lng: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdeltTensionPair {
    pub id: String,
    pub countries: Vec<String>,
    pub label: String,
    pub score: f64,
    pub trend: String,
    pub change_percent: f64,
    pub region: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifyEventRequest {
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub country: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClassifyEventResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classification: Option<EventClassification>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventClassification {
    pub category: String,
    pub subcategory: String,
    pub severity: String,
    pub confidence: f64,
    pub analysis: String,
    pub entities: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SearchGdeltDocumentsRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub max_records: i32,
    #[serde(default)]
    pub timespan: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SearchGdeltDocumentsResponse {
    pub articles: Vec<GdeltArticle>,
    pub query: String,
    pub error: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdeltArticle {
    pub title: String,
    pub url: String,
    pub source: String,
    pub date: String,
    pub image: String,
    pub language: String,
    pub tone: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CiiScore {
    pub region: String,
    pub static_baseline: f64,
    pub dynamic_score: f64,
    pub combined_score: f64,
    pub trend: String,
    pub components: CiiComponents,
    pub computed_at: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CiiComponents {
    pub news_activity: f64,
    pub cii_contribution: f64,
    pub geo_convergence: f64,
    pub military_activity: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StrategicRisk {
    pub region: String,
    pub level: String,
    pub score: f64,
    pub factors: Vec<String>,
    pub trend: String,
}

#[derive(Debug, Deserialize, Default)]
struct AcledEvent {
    #[serde(default)]
    country: String,
    #[serde(default)]
    event_type: String,
}

#[derive(Debug, Deserialize, Default)]
struct AcledResponse {
    #[serde(default)]
    data: Vec<AcledEvent>,
    #[serde(default)]
    message: String,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Deserialize)]
struct GroqParsedClassification {
    level: String,
    category: String,
}

#[derive(Debug, Deserialize, Default)]
struct GdeltDocsApiResponse {
    #[serde(default)]
    articles: Vec<GdeltDocsApiArticle>,
}

#[derive(Debug, Deserialize, Default)]
struct GdeltDocsApiArticle {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    source: Option<GdeltArticleSource>,
    #[serde(default)]
    seendate: String,
    #[serde(default)]
    socialimage: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    tone: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct GdeltArticleSource {
    #[serde(default)]
    domain: String,
}

#[derive(Debug, Deserialize, Default)]
struct PizzintApiResponse {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    data: Vec<PizzintApiLocation>,
}

#[derive(Debug, Deserialize, Default)]
struct PizzintApiLocation {
    #[serde(default)]
    place_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    address: String,
    #[serde(default)]
    current_popularity: i32,
    #[serde(default)]
    percentage_of_usual: Option<f64>,
    #[serde(default)]
    is_spike: bool,
    #[serde(default)]
    spike_magnitude: Option<f64>,
    #[serde(default)]
    data_source: String,
    #[serde(default)]
    recorded_at: String,
    #[serde(default)]
    data_freshness: String,
    #[serde(default)]
    is_closed_now: Option<bool>,
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    lng: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct GdeltPoint {
    #[serde(default)]
    v: f64,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

static INTEL_BRIEF_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetCountryIntelBriefResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static RISK_CACHE: Lazy<Mutex<Option<CacheEntry<GetRiskScoresResponse>>>> =
    Lazy::new(|| Mutex::new(None));
static RISK_STALE_CACHE: Lazy<Mutex<Option<CacheEntry<GetRiskScoresResponse>>>> =
    Lazy::new(|| Mutex::new(None));
static CLASSIFY_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ClassifyEventResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static GDELT_DOC_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<SearchGdeltDocumentsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static PIZZINT_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetPizzintStatusResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn hash_string(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn baseline_risk(code: &str) -> f64 {
    match code {
        "US" => 5.0,
        "RU" => 35.0,
        "CN" => 25.0,
        "UA" => 50.0,
        "IR" => 40.0,
        "IL" => 45.0,
        "TW" => 30.0,
        "KP" => 45.0,
        "SA" => 20.0,
        "TR" => 25.0,
        "PL" => 10.0,
        "DE" => 5.0,
        "FR" => 10.0,
        "GB" => 5.0,
        "IN" => 20.0,
        "PK" => 35.0,
        "SY" => 50.0,
        "YE" => 50.0,
        "MM" => 45.0,
        "VE" => 40.0,
        _ => 20.0,
    }
}

fn event_multiplier(code: &str) -> f64 {
    match code {
        "US" => 0.3,
        "RU" => 2.0,
        "CN" => 2.5,
        "UA" => 0.8,
        "IR" => 2.0,
        "IL" => 0.7,
        "TW" => 1.5,
        "KP" => 3.0,
        "SA" => 2.0,
        "TR" => 1.2,
        "PL" => 0.8,
        "DE" => 0.5,
        "FR" => 0.6,
        "GB" => 0.5,
        "IN" => 0.8,
        "PK" => 1.5,
        "SY" => 0.7,
        "YE" => 0.7,
        "MM" => 1.8,
        "VE" => 1.8,
        _ => 1.0,
    }
}

fn country_name(code: &str) -> String {
    TIER1_COUNTRIES
        .iter()
        .find_map(|(known, name)| (*known == code).then_some((*name).to_string()))
        .unwrap_or_else(|| code.to_string())
}

fn is_valid_country_code(value: &str) -> bool {
    value.len() == 2 && value.chars().all(|ch| ch.is_ascii_alphabetic())
}

fn normalize_country_name(text: &str) -> Option<&'static str> {
    let lower = text.to_ascii_lowercase();
    COUNTRY_KEYWORDS.iter().find_map(|(code, keywords)| {
        keywords
            .iter()
            .any(|keyword| lower.contains(keyword))
            .then_some(*code)
    })
}

fn severity_level(score: f64) -> String {
    if score >= 70.0 {
        "SEVERITY_LEVEL_HIGH".to_string()
    } else if score >= 40.0 {
        "SEVERITY_LEVEL_MEDIUM".to_string()
    } else {
        "SEVERITY_LEVEL_LOW".to_string()
    }
}

fn map_level_to_severity(level: &str) -> String {
    match level {
        "critical" | "high" => "SEVERITY_LEVEL_HIGH".to_string(),
        "medium" => "SEVERITY_LEVEL_MEDIUM".to_string(),
        _ => "SEVERITY_LEVEL_LOW".to_string(),
    }
}

fn clamp_gdelt_records(value: i32) -> i32 {
    let requested = if value > 0 {
        value
    } else {
        GDELT_DEFAULT_RECORDS
    };
    requested.min(GDELT_MAX_RECORDS)
}

fn sanitize_title(value: &str) -> String {
    value.trim().chars().take(TITLE_MAX_LEN).collect::<String>()
}

fn extract_json_object(raw: &str) -> Option<String> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    (end >= start).then(|| raw[start..=end].to_string())
}

fn parse_groq_classification(raw: &str) -> Option<GroqParsedClassification> {
    let raw_json = extract_json_object(raw)?;
    serde_json::from_str::<GroqParsedClassification>(raw_json.as_str()).ok()
}

fn trend_from_change(change: f64) -> String {
    if change > 5.0 {
        "TREND_DIRECTION_RISING".to_string()
    } else if change < -5.0 {
        "TREND_DIRECTION_FALLING".to_string()
    } else {
        "TREND_DIRECTION_STABLE".to_string()
    }
}

fn get_map_cached<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: &str,
) -> Result<Option<T>, AppError> {
    let cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_map_cached<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: &str,
    value: &T,
    ttl: Duration,
) -> Result<(), AppError> {
    let mut cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    cache.insert(
        key.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + ttl,
        },
    );
    Ok(())
}

fn get_cached_intel(country_code: &str) -> Result<Option<GetCountryIntelBriefResponse>, AppError> {
    get_map_cached(&INTEL_BRIEF_CACHE, country_code)
}

fn set_cached_intel(
    country_code: &str,
    response: &GetCountryIntelBriefResponse,
) -> Result<(), AppError> {
    set_map_cached(&INTEL_BRIEF_CACHE, country_code, response, INTEL_CACHE_TTL)
}

fn get_cached_risk() -> Result<Option<GetRiskScoresResponse>, AppError> {
    let cache = RISK_CACHE
        .lock()
        .map_err(|_| AppError::Internal("risk cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cached_risk(response: &GetRiskScoresResponse) -> Result<(), AppError> {
    let mut cache = RISK_CACHE
        .lock()
        .map_err(|_| AppError::Internal("risk cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: response.clone(),
        expires_at: Instant::now() + RISK_CACHE_TTL,
    });
    Ok(())
}

fn get_stale_risk() -> Result<Option<GetRiskScoresResponse>, AppError> {
    let cache = RISK_STALE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("risk stale cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_stale_risk(response: &GetRiskScoresResponse) -> Result<(), AppError> {
    let mut cache = RISK_STALE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("risk stale cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: response.clone(),
        expires_at: Instant::now() + RISK_STALE_TTL,
    });
    Ok(())
}

async fn fetch_groq_brief(
    state: &AppState,
    api_key: &str,
    country_code: &str,
    country_name: &str,
) -> Result<String, AppError> {
    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    let system_prompt = format!(
        "You are a senior intelligence analyst providing comprehensive country situation briefs. Current date: {}. Provide geopolitical context appropriate for the current date.\n\nWrite a concise intelligence brief for the requested country covering:\n1. Current Situation - what is happening right now\n2. Military & Security Posture\n3. Key Risk Factors\n4. Regional Context\n5. Outlook & Watch Items\n\nRules:\n- Be specific and analytical\n- 4-5 paragraphs, 250-350 words\n- No speculation beyond what data supports\n- Use plain language, not jargon",
        date_str
    );

    let response = state
        .http_client
        .post(GROQ_API_URL)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .header("User-Agent", CHROME_UA)
        .json(&json!({
            "model": GROQ_MODEL,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": format!("Country: {} ({})", country_name, country_code) },
            ],
            "temperature": 0.4,
            "max_tokens": 900,
        }))
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("Groq request failed: {}", error)))?;

    if !response.status().is_success() {
        return Ok(String::new());
    }

    let payload = response
        .json::<Value>()
        .await
        .map_err(|error| AppError::Internal(format!("Groq decode failed: {}", error)))?;

    let brief = payload
        .get("choices")
        .and_then(|choices| choices.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();

    Ok(brief)
}

async fn fetch_groq_classification(
    state: &AppState,
    api_key: &str,
    title: &str,
) -> Option<GroqParsedClassification> {
    let system_prompt = "You classify news headlines into threat level and category. Return ONLY valid JSON, no other text.\n\nLevels: critical, high, medium, low, info\nCategories: conflict, protest, disaster, diplomatic, economic, terrorism, cyber, health, environmental, military, crime, infrastructure, tech, general\n\nFocus: geopolitical events, conflicts, disasters, diplomacy. Classify by real-world severity and impact.\n\nReturn: {\"level\":\"...\",\"category\":\"...\"}";

    let response = state
        .http_client
        .post(GROQ_API_URL)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .header("User-Agent", CHROME_UA)
        .json(&json!({
            "model": GROQ_MODEL,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": title },
            ],
            "temperature": 0,
            "max_tokens": 50,
        }))
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let payload = response.json::<Value>().await.ok()?;
    let raw = payload
        .get("choices")
        .and_then(|choices| choices.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();

    let parsed = parse_groq_classification(&raw)?;
    let level_ok = VALID_LEVELS.iter().any(|level| *level == parsed.level);
    let category_ok = VALID_CATEGORIES
        .iter()
        .any(|category| *category == parsed.category);

    (level_ok && category_ok).then_some(parsed)
}

async fn fetch_acled_protests(state: &AppState) -> Result<Vec<AcledEvent>, AppError> {
    let Some(token) = state.config.acled_access_token.as_deref() else {
        return Ok(Vec::new());
    };

    let end_date = Utc::now().date_naive();
    let start_date = end_date - ChronoDuration::days(7);
    let url = format!(
        "https://acleddata.com/api/acled/read?_format=json&event_type=Protests&event_type=Riots&event_date={}|{}&event_date_where=BETWEEN&limit=500",
        start_date.format("%Y-%m-%d"),
        end_date.format("%Y-%m-%d")
    );

    let response = state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("ACLED request failed: {}", error)))?;

    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "ACLED API error: HTTP {}",
            response.status().as_u16()
        )));
    }

    let payload = response
        .json::<AcledResponse>()
        .await
        .map_err(|error| AppError::Internal(format!("ACLED decode failed: {}", error)))?;

    if !payload.message.is_empty() || !payload.error.is_empty() {
        return Err(AppError::Internal(format!(
            "ACLED API error: {}{}{}",
            payload.message,
            if !payload.message.is_empty() && !payload.error.is_empty() {
                " | "
            } else {
                ""
            },
            payload.error
        )));
    }

    Ok(payload.data)
}

async fn fetch_gdelt_documents(
    state: &AppState,
    query: &str,
    timespan: &str,
    max_records: i32,
) -> Result<Vec<GdeltArticle>, AppError> {
    let url = format!(
        "{}?query={}&mode=artlist&maxrecords={}&format=json&sort=date&timespan={}",
        GDELT_DOC_API,
        urlencoding::encode(query),
        max_records,
        urlencoding::encode(timespan)
    );

    let response = state
        .http_client
        .get(url)
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("GDELT request failed: {}", error)))?;

    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "GDELT returned {}",
            response.status().as_u16()
        )));
    }

    let payload = response
        .json::<GdeltDocsApiResponse>()
        .await
        .map_err(|error| AppError::Internal(format!("GDELT decode failed: {}", error)))?;

    Ok(payload
        .articles
        .into_iter()
        .map(|article| GdeltArticle {
            title: article.title,
            url: article.url,
            source: if !article.domain.is_empty() {
                article.domain
            } else {
                article
                    .source
                    .map(|source| source.domain)
                    .unwrap_or_default()
            },
            date: article.seendate,
            image: article.socialimage,
            language: article.language,
            tone: article.tone.unwrap_or(0.0),
        })
        .collect::<Vec<_>>())
}

async fn fetch_pizzint_status(state: &AppState) -> Option<PizzintStatus> {
    let response = state
        .http_client
        .get(PIZZINT_API)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let payload = response.json::<PizzintApiResponse>().await.ok()?;
    if !payload.success || payload.data.is_empty() {
        return None;
    }

    let locations = payload
        .data
        .into_iter()
        .map(|entry| PizzintLocation {
            place_id: entry.place_id,
            name: entry.name,
            address: entry.address,
            current_popularity: entry.current_popularity,
            percentage_of_usual: entry.percentage_of_usual.unwrap_or(0.0).round() as i32,
            is_spike: entry.is_spike,
            spike_magnitude: entry.spike_magnitude.unwrap_or(0.0),
            data_source: entry.data_source,
            recorded_at: entry.recorded_at,
            data_freshness: if entry.data_freshness.eq_ignore_ascii_case("fresh") {
                "DATA_FRESHNESS_FRESH".to_string()
            } else {
                "DATA_FRESHNESS_STALE".to_string()
            },
            is_closed_now: entry.is_closed_now.unwrap_or(false),
            lat: entry.lat.unwrap_or(0.0),
            lng: entry.lng.unwrap_or(0.0),
        })
        .collect::<Vec<_>>();

    let open_locations = locations
        .iter()
        .filter(|location| !location.is_closed_now)
        .count() as i32;
    let active_spikes = locations
        .iter()
        .filter(|location| location.is_spike)
        .count() as i32;
    let average_popularity = if open_locations > 0 {
        locations
            .iter()
            .filter(|location| !location.is_closed_now)
            .map(|location| location.current_popularity as f64)
            .sum::<f64>()
            / open_locations as f64
    } else {
        0.0
    };

    let mut adjusted = average_popularity;
    if active_spikes > 0 {
        adjusted += active_spikes as f64 * 10.0;
    }
    adjusted = adjusted.min(100.0);

    let (defcon_level, defcon_label) = if adjusted >= 85.0 {
        (1, "Maximum Activity")
    } else if adjusted >= 70.0 {
        (2, "High Activity")
    } else if adjusted >= 50.0 {
        (3, "Elevated Activity")
    } else if adjusted >= 25.0 {
        (4, "Above Normal")
    } else {
        (5, "Normal Activity")
    };

    let has_fresh = locations
        .iter()
        .any(|location| location.data_freshness == "DATA_FRESHNESS_FRESH");

    Some(PizzintStatus {
        defcon_level,
        defcon_label: defcon_label.to_string(),
        aggregate_activity: average_popularity.round(),
        active_spikes,
        locations_monitored: locations.len() as i32,
        locations_open: open_locations,
        updated_at: now_epoch_ms(),
        data_freshness: if has_fresh {
            "DATA_FRESHNESS_FRESH".to_string()
        } else {
            "DATA_FRESHNESS_STALE".to_string()
        },
        locations,
    })
}

async fn fetch_gdelt_tension_pairs(state: &AppState) -> Vec<GdeltTensionPair> {
    let url = format!(
        "{}?pairs={}&method=gpr",
        GDELT_BATCH_API,
        urlencoding::encode(DEFAULT_GDELT_PAIRS)
    );

    let response = match state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return Vec::new(),
    };

    let payload = match response.json::<HashMap<String, Vec<GdeltPoint>>>().await {
        Ok(payload) => payload,
        Err(_) => return Vec::new(),
    };

    payload
        .into_iter()
        .filter_map(|(pair_key, points)| {
            let latest = points.last()?;
            let previous = if points.len() > 1 {
                &points[points.len() - 2]
            } else {
                latest
            };
            let change = if previous.v > 0.0 {
                ((latest.v - previous.v) / previous.v) * 100.0
            } else {
                0.0
            };
            let countries = pair_key
                .split('_')
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            Some(GdeltTensionPair {
                id: pair_key.clone(),
                label: countries
                    .iter()
                    .map(|country| country.to_uppercase())
                    .collect::<Vec<_>>()
                    .join(" - "),
                countries,
                score: latest.v,
                trend: trend_from_change(change),
                change_percent: ((change * 10.0).round()) / 10.0,
                region: "global".to_string(),
            })
        })
        .collect::<Vec<_>>()
}

fn compute_cii_scores(protests: &[AcledEvent]) -> Vec<CiiScore> {
    let mut country_events: HashMap<String, (u32, u32)> = HashMap::new();
    for event in protests {
        let Some(code) = normalize_country_name(&event.country) else {
            continue;
        };
        let entry = country_events.entry(code.to_string()).or_insert((0, 0));
        if event.event_type.eq_ignore_ascii_case("Riots") {
            entry.1 += 1;
        } else {
            entry.0 += 1;
        }
    }

    let computed_at = now_epoch_ms();
    let mut scores = TIER1_COUNTRIES
        .iter()
        .map(|(code, _)| {
            let (protests_count, riots_count) =
                country_events.get(*code).copied().unwrap_or((0, 0));
            let baseline = baseline_risk(code);
            let multiplier = event_multiplier(code);
            let unrest = ((protests_count as f64 + riots_count as f64 * 2.0) * multiplier * 2.0)
                .round()
                .min(100.0);
            let security = (baseline + riots_count as f64 * multiplier * 5.0).min(100.0);
            let information =
                ((protests_count as f64 + riots_count as f64) * multiplier * 3.0).min(100.0);
            let composite = (baseline
                + (unrest * 0.4 + security * 0.35 + information * 0.25) * 0.5)
                .round()
                .min(100.0);

            CiiScore {
                region: (*code).to_string(),
                static_baseline: baseline,
                dynamic_score: (composite - baseline).max(0.0),
                combined_score: composite,
                trend: "TREND_DIRECTION_STABLE".to_string(),
                components: CiiComponents {
                    news_activity: information,
                    cii_contribution: unrest,
                    geo_convergence: 0.0,
                    military_activity: 0.0,
                },
                computed_at,
            }
        })
        .collect::<Vec<_>>();

    scores.sort_by(|a, b| {
        b.combined_score
            .partial_cmp(&a.combined_score)
            .unwrap_or(Ordering::Equal)
    });
    scores
}

fn compute_strategic_risks(cii_scores: &[CiiScore]) -> Vec<StrategicRisk> {
    let top5 = cii_scores.iter().take(5).collect::<Vec<_>>();

    let (weighted_sum, total_weight) = top5
        .iter()
        .enumerate()
        .map(|(idx, score)| {
            let weight = 1.0 - (idx as f64 * 0.15);
            (score.combined_score * weight, weight)
        })
        .fold((0.0, 0.0), |(sum, wsum), (value, weight)| {
            (sum + value, wsum + weight)
        });

    let overall_score = if total_weight > 0.0 {
        ((weighted_sum / total_weight) * 0.7 + 15.0)
            .round()
            .min(100.0)
    } else {
        0.0
    };

    vec![StrategicRisk {
        region: "global".to_string(),
        level: severity_level(overall_score),
        score: overall_score,
        factors: top5.iter().map(|score| score.region.clone()).collect(),
        trend: "TREND_DIRECTION_STABLE".to_string(),
    }]
}

fn baseline_risk_response() -> GetRiskScoresResponse {
    let cii_scores = compute_cii_scores(&[]);
    GetRiskScoresResponse {
        strategic_risks: compute_strategic_risks(&cii_scores),
        cii_scores,
    }
}

pub async fn get_country_intel_brief(
    State(state): State<AppState>,
    Json(request): Json<GetCountryIntelBriefRequest>,
) -> Result<Json<GetCountryIntelBriefResponse>, AppError> {
    let code = request.country_code.trim().to_uppercase();
    if !is_valid_country_code(code.as_str()) {
        return Err(AppError::BadRequest(
            "countryCode must be a two-letter ISO code".to_string(),
        ));
    }

    let country = country_name(&code);
    let empty = GetCountryIntelBriefResponse {
        country_code: code.clone(),
        country_name: country.clone(),
        brief: String::new(),
        model: GROQ_MODEL.to_string(),
        generated_at: now_epoch_ms(),
    };

    let Some(api_key) = state.config.groq_api_key.as_deref() else {
        return Ok(Json(empty));
    };

    if let Some(cached) = get_cached_intel(&code)? {
        return Ok(Json(cached));
    }

    let brief = fetch_groq_brief(&state, api_key, &code, &country)
        .await
        .unwrap_or_default();

    let response = GetCountryIntelBriefResponse {
        country_code: code.clone(),
        country_name: country,
        brief,
        model: GROQ_MODEL.to_string(),
        generated_at: now_epoch_ms(),
    };

    if !response.brief.is_empty() {
        set_cached_intel(&code, &response)?;
    }

    Ok(Json(response))
}

pub async fn get_risk_scores(
    State(state): State<AppState>,
    Json(_request): Json<GetRiskScoresRequest>,
) -> Result<Json<GetRiskScoresResponse>, AppError> {
    if let Some(cached) = get_cached_risk()? {
        return Ok(Json(cached));
    }

    let fresh = if state.config.acled_access_token.is_some() {
        match fetch_acled_protests(&state).await {
            Ok(protests) => {
                let cii_scores = compute_cii_scores(&protests);
                Some(GetRiskScoresResponse {
                    strategic_risks: compute_strategic_risks(&cii_scores),
                    cii_scores,
                })
            }
            Err(_) => None,
        }
    } else {
        Some(baseline_risk_response())
    };

    let response = if let Some(result) = fresh {
        set_cached_risk(&result)?;
        set_stale_risk(&result)?;
        result
    } else if let Some(stale) = get_stale_risk()? {
        stale
    } else {
        baseline_risk_response()
    };

    Ok(Json(response))
}

pub async fn search_gdelt_documents(
    State(state): State<AppState>,
    Json(request): Json<SearchGdeltDocumentsRequest>,
) -> Result<Json<SearchGdeltDocumentsResponse>, AppError> {
    let query = request.query.trim().to_string();
    if query.len() < 2 {
        return Ok(Json(SearchGdeltDocumentsResponse {
            articles: Vec::new(),
            query,
            error: "Query parameter required (min 2 characters)".to_string(),
        }));
    }

    let max_records = clamp_gdelt_records(request.max_records);
    let timespan = if request.timespan.trim().is_empty() {
        "72h".to_string()
    } else {
        request.timespan.trim().to_string()
    };

    let cache_key = format!("{}:{}:{}", query, timespan, max_records);
    if let Some(cached) = get_map_cached(&GDELT_DOC_CACHE, &cache_key)? {
        return Ok(Json(cached));
    }

    match fetch_gdelt_documents(&state, &query, &timespan, max_records).await {
        Ok(articles) => {
            let response = SearchGdeltDocumentsResponse {
                articles,
                query,
                error: String::new(),
            };
            if !response.articles.is_empty() {
                set_map_cached(&GDELT_DOC_CACHE, &cache_key, &response, GDELT_DOC_CACHE_TTL)?;
            }
            Ok(Json(response))
        }
        Err(error) => Ok(Json(SearchGdeltDocumentsResponse {
            articles: Vec::new(),
            query,
            error: error.to_string(),
        })),
    }
}

pub async fn classify_event(
    State(state): State<AppState>,
    Json(request): Json<ClassifyEventRequest>,
) -> Result<Json<ClassifyEventResponse>, AppError> {
    let Some(api_key) = state.config.groq_api_key.as_deref() else {
        return Ok(Json(ClassifyEventResponse {
            classification: None,
        }));
    };

    let title = sanitize_title(&request.title);
    if title.is_empty() {
        return Ok(Json(ClassifyEventResponse {
            classification: None,
        }));
    }

    let cache_key = format!("classify:{}", hash_string(&title.to_lowercase()));
    if let Some(cached) = get_map_cached(&CLASSIFY_CACHE, &cache_key)? {
        return Ok(Json(cached));
    }

    let response = if let Some(parsed) = fetch_groq_classification(&state, api_key, &title).await {
        ClassifyEventResponse {
            classification: Some(EventClassification {
                category: parsed.category.clone(),
                subcategory: parsed.level.clone(),
                severity: map_level_to_severity(parsed.level.as_str()),
                confidence: 0.9,
                analysis: String::new(),
                entities: Vec::new(),
            }),
        }
    } else {
        ClassifyEventResponse {
            classification: None,
        }
    };

    if response.classification.is_some() {
        set_map_cached(&CLASSIFY_CACHE, &cache_key, &response, CLASSIFY_CACHE_TTL)?;
    }

    Ok(Json(response))
}

pub async fn get_pizzint_status(
    State(state): State<AppState>,
    Json(request): Json<GetPizzintStatusRequest>,
) -> Result<Json<GetPizzintStatusResponse>, AppError> {
    let cache_key = if request.include_gdelt {
        "gdelt"
    } else {
        "base"
    };

    if let Some(cached) = get_map_cached(&PIZZINT_CACHE, cache_key)? {
        return Ok(Json(cached));
    }

    let pizzint = fetch_pizzint_status(&state).await;
    let tension_pairs = if request.include_gdelt {
        fetch_gdelt_tension_pairs(&state).await
    } else {
        Vec::new()
    };

    let response = GetPizzintStatusResponse {
        pizzint: pizzint.clone(),
        tension_pairs,
    };

    if pizzint.is_some() {
        set_map_cached(&PIZZINT_CACHE, cache_key, &response, PIZZINT_CACHE_TTL)?;
    }

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_iso_country_code() {
        assert!(is_valid_country_code("US"));
        assert!(is_valid_country_code("de"));
        assert!(!is_valid_country_code("USA"));
        assert!(!is_valid_country_code("U1"));
    }

    #[test]
    fn normalizes_country_keywords() {
        assert_eq!(normalize_country_name("Riot in Washington"), Some("US"));
        assert_eq!(normalize_country_name("Moscow update"), Some("RU"));
        assert_eq!(normalize_country_name("Unknown place"), None);
    }

    #[test]
    fn computes_scores_for_tier1_set() {
        let scores = compute_cii_scores(&[]);
        assert_eq!(scores.len(), TIER1_COUNTRIES.len());
        assert!(
            scores
                .windows(2)
                .all(|pair| { pair[0].combined_score >= pair[1].combined_score })
        );
    }

    #[test]
    fn strategic_risk_uses_global_region() {
        let scores = compute_cii_scores(&[]);
        let risks = compute_strategic_risks(&scores);
        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].region, "global");
        assert!(risks[0].score >= 0.0 && risks[0].score <= 100.0);
    }

    #[test]
    fn maps_levels_to_severity() {
        assert_eq!(map_level_to_severity("critical"), "SEVERITY_LEVEL_HIGH");
        assert_eq!(map_level_to_severity("high"), "SEVERITY_LEVEL_HIGH");
        assert_eq!(map_level_to_severity("medium"), "SEVERITY_LEVEL_MEDIUM");
        assert_eq!(map_level_to_severity("low"), "SEVERITY_LEVEL_LOW");
    }

    #[test]
    fn clamps_gdelt_max_records() {
        assert_eq!(clamp_gdelt_records(0), GDELT_DEFAULT_RECORDS);
        assert_eq!(clamp_gdelt_records(5), 5);
        assert_eq!(clamp_gdelt_records(999), GDELT_MAX_RECORDS);
    }

    #[test]
    fn sanitizes_title_length() {
        let long = "a".repeat(2_000);
        let sanitized = sanitize_title(&long);
        assert_eq!(sanitized.len(), TITLE_MAX_LEN);
    }

    #[test]
    fn extracts_json_from_llm_output() {
        let raw = "```json\n{\"level\":\"high\",\"category\":\"conflict\"}\n```";
        let parsed = parse_groq_classification(raw).expect("parse json payload");
        assert_eq!(parsed.level, "high");
        assert_eq!(parsed.category, "conflict");
    }
}
