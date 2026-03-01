use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use chrono::{Duration as ChronoDuration, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{AppState, error::AppError};

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_MODEL: &str = "llama-3.1-8b-instant";
const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const INTEL_CACHE_TTL: Duration = Duration::from_secs(7_200);
const RISK_CACHE_TTL: Duration = Duration::from_secs(600);
const RISK_STALE_TTL: Duration = Duration::from_secs(3_600);

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

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
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

fn get_cached_intel(country_code: &str) -> Result<Option<GetCountryIntelBriefResponse>, AppError> {
    let cache = INTEL_BRIEF_CACHE
        .lock()
        .map_err(|_| AppError::Internal("intel cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(country_code)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cached_intel(
    country_code: &str,
    response: &GetCountryIntelBriefResponse,
) -> Result<(), AppError> {
    let mut cache = INTEL_BRIEF_CACHE
        .lock()
        .map_err(|_| AppError::Internal("intel cache lock poisoned".to_string()))?;
    cache.insert(
        country_code.to_string(),
        CacheEntry {
            value: response.clone(),
            expires_at: Instant::now() + INTEL_CACHE_TTL,
        },
    );
    Ok(())
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
        .json::<serde_json::Value>()
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
}
