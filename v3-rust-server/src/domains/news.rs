use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(86_400);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SummarizeArticleRequest {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub headlines: Vec<String>,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub geo_context: String,
    #[serde(default)]
    pub variant: String,
    #[serde(default)]
    pub lang: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SummarizeArticleResponse {
    pub summary: String,
    pub model: String,
    pub provider: String,
    pub cached: bool,
    pub tokens: i64,
    pub fallback: bool,
    pub skipped: bool,
    pub reason: String,
    pub error: String,
    pub error_type: String,
}

#[derive(Clone)]
struct CacheEntry {
    summary: String,
    model: String,
    expires_at: Instant,
}

#[derive(Clone)]
struct ProviderCredentials {
    api_url: String,
    model: String,
    headers: Vec<(String, String)>,
    extra_body: Option<Value>,
}

static SUMMARY_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn get_cache(key: &str) -> Result<Option<(String, String)>, AppError> {
    let cache = SUMMARY_CACHE
        .lock()
        .map_err(|_| AppError::Internal("news summary cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some((entry.summary.clone(), entry.model.clone())));
    }
    Ok(None)
}

fn set_cache(key: String, summary: &str, model: &str) -> Result<(), AppError> {
    let mut cache = SUMMARY_CACHE
        .lock()
        .map_err(|_| AppError::Internal("news summary cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            summary: summary.to_string(),
            model: model.to_string(),
            expires_at: Instant::now() + CACHE_TTL,
        },
    );
    Ok(())
}

fn sanitize_headlines(input: &[String]) -> Vec<String> {
    input
        .iter()
        .take(10)
        .map(|headline| headline.chars().take(500).collect::<String>())
        .filter(|headline| !headline.trim().is_empty())
        .collect()
}

fn deduplicate_headlines(input: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for headline in input {
        let normalized = headline.trim().to_ascii_lowercase();
        if normalized.is_empty() || !seen.insert(normalized) {
            continue;
        }
        output.push(headline.trim().to_string());
    }
    output
}

fn summary_cache_key(
    headlines: &[String],
    mode: &str,
    geo_context: &str,
    variant: &str,
    lang: &str,
) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    mode.to_ascii_lowercase().hash(&mut hasher);
    variant.to_ascii_lowercase().hash(&mut hasher);
    lang.to_ascii_lowercase().hash(&mut hasher);
    geo_context.hash(&mut hasher);
    for headline in headlines {
        headline.hash(&mut hasher);
    }
    format!("summary:v3:{:x}", hasher.finish())
}

fn provider_credentials(provider: &str) -> Option<ProviderCredentials> {
    match provider.trim().to_ascii_lowercase().as_str() {
        "ollama" => {
            let base_url = std::env::var("OLLAMA_API_URL").ok()?;
            let api_url = format!(
                "{}/v1/chat/completions",
                base_url.trim_end_matches('/').trim()
            );
            let mut headers = vec![("Content-Type".to_string(), "application/json".to_string())];
            if let Ok(api_key) = std::env::var("OLLAMA_API_KEY")
                && !api_key.trim().is_empty()
            {
                headers.push(("Authorization".to_string(), format!("Bearer {api_key}")));
            }
            Some(ProviderCredentials {
                api_url,
                model: std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3.1:8b".to_string()),
                headers,
                extra_body: Some(json!({"think": false})),
            })
        }
        "groq" => {
            let api_key = std::env::var("GROQ_API_KEY").ok()?;
            if api_key.trim().is_empty() {
                return None;
            }
            Some(ProviderCredentials {
                api_url: "https://api.groq.com/openai/v1/chat/completions".to_string(),
                model: "llama-3.1-8b-instant".to_string(),
                headers: vec![
                    (
                        "Authorization".to_string(),
                        format!("Bearer {}", api_key.trim()),
                    ),
                    ("Content-Type".to_string(), "application/json".to_string()),
                ],
                extra_body: None,
            })
        }
        "openrouter" => {
            let api_key = std::env::var("OPENROUTER_API_KEY").ok()?;
            if api_key.trim().is_empty() {
                return None;
            }
            Some(ProviderCredentials {
                api_url: "https://openrouter.ai/api/v1/chat/completions".to_string(),
                model: "openrouter/free".to_string(),
                headers: vec![
                    (
                        "Authorization".to_string(),
                        format!("Bearer {}", api_key.trim()),
                    ),
                    ("Content-Type".to_string(), "application/json".to_string()),
                    (
                        "HTTP-Referer".to_string(),
                        "https://worldmonitor.app".to_string(),
                    ),
                    ("X-Title".to_string(), "WorldMonitor".to_string()),
                ],
                extra_body: None,
            })
        }
        _ => None,
    }
}

fn provider_skip_reason(provider: &str) -> String {
    match provider.trim().to_ascii_lowercase().as_str() {
        "ollama" => "OLLAMA_API_URL not configured".to_string(),
        "groq" => "GROQ_API_KEY not configured".to_string(),
        "openrouter" => "OPENROUTER_API_KEY not configured".to_string(),
        unknown => format!("Unknown provider: {unknown}"),
    }
}

fn build_prompts(
    headlines: &[String],
    mode: &str,
    geo_context: &str,
    variant: &str,
    lang: &str,
) -> (String, String) {
    let is_tech = variant.eq_ignore_ascii_case("tech");
    let language_suffix = if lang.trim().is_empty() || lang.eq_ignore_ascii_case("en") {
        String::new()
    } else {
        format!("\nOutput language: {}.", lang.to_ascii_uppercase())
    };

    let numbered = headlines
        .iter()
        .enumerate()
        .map(|(index, headline)| format!("{}. {}", index + 1, headline))
        .collect::<Vec<_>>()
        .join("\n");
    let geo = if geo_context.trim().is_empty() {
        String::new()
    } else {
        format!("\n\n{}", geo_context.trim())
    };

    match mode.trim().to_ascii_lowercase().as_str() {
        "brief" => (
            if is_tech {
                "Summarize the key technology development in 2-3 sentences. Focus on products, AI, startups, and funding.".to_string()
            } else {
                "Summarize the key development in 2-3 sentences with direct geopolitical context."
                    .to_string()
            } + &language_suffix,
            format!("Summarize the top story:\n{numbered}{geo}"),
        ),
        "analysis" => (
            if is_tech {
                "Analyze the main tech trend in 2-3 sentences, focusing on implications."
                    .to_string()
            } else {
                "Analyze the key risk pattern in 2-3 sentences with concrete implications."
                    .to_string()
            } + &language_suffix,
            format!("Provide analysis for:\n{numbered}{geo}"),
        ),
        "translate" => {
            let target = if variant.trim().is_empty() {
                lang
            } else {
                variant
            };
            (
                format!(
                    "Translate the following text to {}. Output only translated text.",
                    target
                ),
                format!(
                    "Translate:\n{}",
                    headlines.first().cloned().unwrap_or_default()
                ),
            )
        }
        _ => (
            "Synthesize the key signal in at most 2 sentences.".to_string() + &language_suffix,
            format!("Key takeaway:\n{numbered}{geo}"),
        ),
    }
}

fn strip_think_blocks(content: &str) -> String {
    static THINK_BLOCK_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?is)<think>.*?</think>").expect("valid think block regex"));
    static DANGLING_THINK_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?is)<think>.*$").expect("valid dangling think regex"));
    let cleaned = THINK_BLOCK_RE.replace_all(content, "");
    DANGLING_THINK_RE
        .replace_all(cleaned.as_ref(), "")
        .trim()
        .to_string()
}

pub async fn summarize_article(
    State(state): State<AppState>,
    Json(request): Json<SummarizeArticleRequest>,
) -> Result<Json<SummarizeArticleResponse>, AppError> {
    let provider = if request.provider.trim().is_empty() {
        "groq".to_string()
    } else {
        request.provider.trim().to_string()
    };
    let mode = if request.mode.trim().is_empty() {
        "brief".to_string()
    } else {
        request.mode.trim().to_string()
    };
    let variant = if request.variant.trim().is_empty() {
        "full".to_string()
    } else {
        request.variant.trim().to_string()
    };
    let lang = if request.lang.trim().is_empty() {
        "en".to_string()
    } else {
        request.lang.trim().to_string()
    };
    let geo_context = request.geo_context.chars().take(2_000).collect::<String>();
    let headlines = sanitize_headlines(&request.headlines);

    if headlines.is_empty() {
        return Ok(Json(SummarizeArticleResponse {
            summary: String::new(),
            model: String::new(),
            provider,
            cached: false,
            tokens: 0,
            fallback: false,
            skipped: false,
            reason: String::new(),
            error: "Headlines array required".to_string(),
            error_type: "ValidationError".to_string(),
        }));
    }

    let Some(credentials) = provider_credentials(&provider) else {
        return Ok(Json(SummarizeArticleResponse {
            summary: String::new(),
            model: String::new(),
            provider: provider.clone(),
            cached: false,
            tokens: 0,
            fallback: true,
            skipped: true,
            reason: provider_skip_reason(&provider),
            error: String::new(),
            error_type: String::new(),
        }));
    };

    let deduped = deduplicate_headlines(&headlines);
    let cache_key = summary_cache_key(&deduped, &mode, &geo_context, &variant, &lang);
    if let Some((summary, model)) = get_cache(&cache_key)? {
        return Ok(Json(SummarizeArticleResponse {
            summary,
            model,
            provider: "cache".to_string(),
            cached: true,
            tokens: 0,
            fallback: false,
            skipped: false,
            reason: String::new(),
            error: String::new(),
            error_type: String::new(),
        }));
    }

    let (system_prompt, user_prompt) =
        build_prompts(&deduped, &mode, &geo_context, &variant, &lang);
    let mut body = json!({
        "model": credentials.model,
        "messages": [
            {"role":"system","content":system_prompt},
            {"role":"user","content":user_prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 150,
        "top_p": 0.9
    });
    if let Some(extra_body) = credentials.extra_body {
        if let Some(object) = body.as_object_mut()
            && let Some(extra) = extra_body.as_object()
        {
            for (key, value) in extra {
                object.insert(key.to_string(), value.clone());
            }
        }
    }

    let mut request_builder = state.http_client.post(credentials.api_url).json(&body);
    for (key, value) in credentials.headers {
        request_builder = request_builder.header(key, value);
    }
    request_builder = request_builder.header(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    );

    let response = match request_builder.send().await {
        Ok(response) => response,
        Err(error) => {
            return Ok(Json(SummarizeArticleResponse {
                summary: String::new(),
                model: String::new(),
                provider,
                cached: false,
                tokens: 0,
                fallback: true,
                skipped: false,
                reason: String::new(),
                error: error.to_string(),
                error_type: "RequestError".to_string(),
            }));
        }
    };

    if !response.status().is_success() {
        let status = response.status().as_u16();
        return Ok(Json(SummarizeArticleResponse {
            summary: String::new(),
            model: String::new(),
            provider,
            cached: false,
            tokens: 0,
            fallback: true,
            skipped: false,
            reason: String::new(),
            error: if status == 429 {
                "Rate limited".to_string()
            } else {
                "Provider API error".to_string()
            },
            error_type: String::new(),
        }));
    }

    let payload = match response.json::<Value>().await {
        Ok(payload) => payload,
        Err(error) => {
            return Ok(Json(SummarizeArticleResponse {
                summary: String::new(),
                model: String::new(),
                provider,
                cached: false,
                tokens: 0,
                fallback: true,
                skipped: false,
                reason: String::new(),
                error: error.to_string(),
                error_type: "DecodeError".to_string(),
            }));
        }
    };

    let raw_summary = payload
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .pointer("/choices/0/message/reasoning")
                .and_then(Value::as_str)
        })
        .unwrap_or_default();
    let summary = strip_think_blocks(raw_summary);
    if summary.is_empty() {
        return Ok(Json(SummarizeArticleResponse {
            summary: String::new(),
            model: String::new(),
            provider,
            cached: false,
            tokens: 0,
            fallback: true,
            skipped: false,
            reason: String::new(),
            error: "Empty response".to_string(),
            error_type: String::new(),
        }));
    }

    let model = payload
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let tokens = payload
        .pointer("/usage/total_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);

    set_cache(cache_key, &summary, &model)?;
    Ok(Json(SummarizeArticleResponse {
        summary,
        model,
        provider,
        cached: false,
        tokens,
        fallback: false,
        skipped: false,
        reason: String::new(),
        error: String::new(),
        error_type: String::new(),
    }))
}
