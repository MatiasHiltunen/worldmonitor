use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const ARXIV_CACHE_TTL: Duration = Duration::from_secs(3_600);
const TRENDING_CACHE_TTL: Duration = Duration::from_secs(3_600);
const HN_CACHE_TTL: Duration = Duration::from_secs(600);
const TECH_EVENTS_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

const HN_MAX_CONCURRENCY: usize = 10;
const DEFAULT_PAGE_SIZE: usize = 50;
const DEFAULT_HN_PAGE_SIZE: usize = 30;

static ENTRY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<entry>(.*?)</entry>").expect("valid arxiv entry regex"));
static AUTHOR_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?s)<author>\s*<name>(.*?)</name>\s*</author>").expect("valid author regex")
});
static CATEGORY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<category[^>]*term=\"([^\"]+)\""#).expect("valid category regex"));
static ALT_LINK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"<link[^>]*rel=\"alternate\"[^>]*href=\"([^\"]+)\""#)
        .expect("valid alt link regex")
});

static RSS_ITEM_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<item>(.*?)</item>").expect("valid rss item regex"));
static RSS_GUID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<guid[^>]*>(.*?)</guid>").expect("valid guid regex"));
static RSS_DATE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)on\s+(\w+\s+\d{1,2},?\s+\d{4})").expect("valid date regex"));
static RSS_LOCATION_RE_1: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:in|at)\s+([A-Za-z\s]+,\s*[A-Za-z\s]+)(?:\.|$)")
        .expect("valid location regex")
});
static RSS_LOCATION_RE_2: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)Location:\s*([^<\n]+)").expect("valid location regex"));

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationRequest {
    #[serde(default)]
    pub page_size: usize,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListArxivPapersRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub query: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListArxivPapersResponse {
    pub papers: Vec<ArxivPaper>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ArxivPaper {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub authors: Vec<String>,
    pub categories: Vec<String>,
    pub published_at: i64,
    pub url: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListTrendingReposRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub language: String,
    #[serde(default)]
    pub period: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListTrendingReposResponse {
    pub repos: Vec<GithubRepo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GithubRepo {
    pub full_name: String,
    pub description: String,
    pub language: String,
    pub stars: i32,
    pub stars_today: i32,
    pub forks: i32,
    pub url: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListHackernewsItemsRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub feed_type: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListHackernewsItemsResponse {
    pub items: Vec<HackernewsItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HackernewsItem {
    pub id: i64,
    pub title: String,
    pub url: String,
    pub score: i32,
    pub comment_count: i32,
    pub by: String,
    pub submitted_at: i64,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListTechEventsRequest {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub mappable: bool,
    #[serde(default)]
    pub limit: i32,
    #[serde(default)]
    pub days: i32,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListTechEventsResponse {
    pub success: bool,
    pub count: i32,
    pub conference_count: i32,
    pub mappable_count: i32,
    pub last_updated: String,
    pub events: Vec<TechEvent>,
    pub error: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TechEvent {
    pub id: String,
    pub title: String,
    pub r#type: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coords: Option<TechEventCoords>,
    pub start_date: String,
    pub end_date: String,
    pub url: String,
    pub source: String,
    pub description: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TechEventCoords {
    pub lat: f64,
    pub lng: f64,
    pub country: String,
    pub original: String,
    pub r#virtual: bool,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

static ARXIV_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListArxivPapersResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static TRENDING_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListTrendingReposResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static HN_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListHackernewsItemsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static TECH_EVENTS_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListTechEventsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn string_value(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|raw| raw.trim().to_string())
        .unwrap_or_default()
}

fn parse_i32(value: Option<&Value>) -> i32 {
    if let Some(number) = value.and_then(Value::as_i64) {
        return number as i32;
    }
    value
        .and_then(Value::as_str)
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

fn page_size(request: Option<&PaginationRequest>, default: usize, max: usize) -> usize {
    request
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(default)
        .min(max)
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn normalize_whitespace(value: &str) -> String {
    value
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn between(value: &str, start: &str, end: &str) -> Option<String> {
    let from = value.find(start)? + start.len();
    let to = value[from..].find(end)? + from;
    Some(value[from..to].to_string())
}

fn extract_rss_tag(item: &str, tag: &str) -> Option<String> {
    let cdata_start = format!("<{tag}><![CDATA[");
    let cdata_end = format!("]]></{tag}>");
    if let Some(value) = between(item, &cdata_start, &cdata_end) {
        return Some(value);
    }

    between(item, &format!("<{tag}>"), &format!("</{tag}>"))
}

fn parse_arxiv_papers(xml: &str) -> Vec<ArxivPaper> {
    ENTRY_RE
        .captures_iter(xml)
        .filter_map(|capture| {
            let block = capture.get(1)?.as_str();
            let raw_id = between(block, "<id>", "</id>")?;
            let id = raw_id
                .rsplit('/')
                .next()
                .map(|value| value.to_string())
                .unwrap_or_else(|| raw_id.clone());

            let title = normalize_whitespace(&xml_unescape(
                &between(block, "<title>", "</title>").unwrap_or_default(),
            ));
            let summary = normalize_whitespace(&xml_unescape(
                &between(block, "<summary>", "</summary>").unwrap_or_default(),
            ));

            let authors = AUTHOR_RE
                .captures_iter(block)
                .filter_map(|author| {
                    author
                        .get(1)
                        .map(|name| normalize_whitespace(&xml_unescape(name.as_str())))
                })
                .collect::<Vec<_>>();

            let categories = CATEGORY_RE
                .captures_iter(block)
                .filter_map(|category| {
                    category
                        .get(1)
                        .map(|value| xml_unescape(value.as_str()).trim().to_string())
                })
                .collect::<Vec<_>>();

            let published_at = between(block, "<published>", "</published>")
                .and_then(|value| DateTime::parse_from_rfc3339(value.trim()).ok())
                .map(|date| date.timestamp_millis())
                .unwrap_or(0);

            let url = ALT_LINK_RE
                .captures(block)
                .and_then(|link| link.get(1))
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_else(|| raw_id.clone());

            Some(ArxivPaper {
                id,
                title,
                summary,
                authors,
                categories,
                published_at,
                url,
            })
        })
        .collect::<Vec<_>>()
}

fn allowed_hn_feed(feed: &str) -> &'static str {
    match feed {
        "top" | "new" | "best" | "ask" | "show" | "job" => {
            if feed == "new" {
                "new"
            } else if feed == "best" {
                "best"
            } else if feed == "ask" {
                "ask"
            } else if feed == "show" {
                "show"
            } else if feed == "job" {
                "job"
            } else {
                "top"
            }
        }
        _ => "top",
    }
}

fn parse_ics_event_type(summary: &str, location: &str) -> String {
    if summary.starts_with("Earnings:") {
        "earnings".to_string()
    } else if summary.starts_with("IPO") {
        "ipo".to_string()
    } else if !location.is_empty() {
        "conference".to_string()
    } else {
        "other".to_string()
    }
}

fn parse_ics_date(raw: &str) -> Option<String> {
    if raw.len() != 8 || !raw.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!("{}-{}-{}", &raw[0..4], &raw[4..6], &raw[6..8]))
}

fn parse_ics_field(block: &str, key: &str) -> Option<String> {
    for line in block.lines() {
        if let Some(value) = line.trim().strip_prefix(key) {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn parse_ics_events(content: &str) -> Vec<TechEvent> {
    content
        .split("BEGIN:VEVENT")
        .skip(1)
        .filter_map(|block| {
            let summary = parse_ics_field(block, "SUMMARY:")?;
            let start_raw = parse_ics_field(block, "DTSTART;VALUE=DATE:")?;
            let end_raw =
                parse_ics_field(block, "DTEND;VALUE=DATE:").unwrap_or_else(|| start_raw.clone());
            let start_date = parse_ics_date(&start_raw)?;
            let end_date = parse_ics_date(&end_raw).unwrap_or_else(|| start_date.clone());

            let location = parse_ics_field(block, "LOCATION:").unwrap_or_default();
            let url = parse_ics_field(block, "URL:").unwrap_or_default();
            let id = parse_ics_field(block, "UID:")
                .unwrap_or_else(|| format!("ics-{}-{}", summary.to_ascii_lowercase(), start_date));

            let is_virtual = location.to_ascii_lowercase().contains("online");
            let coords = is_virtual.then_some(TechEventCoords {
                lat: 0.0,
                lng: 0.0,
                country: "Virtual".to_string(),
                original: location.clone(),
                r#virtual: true,
            });

            Some(TechEvent {
                id,
                title: summary.clone(),
                r#type: parse_ics_event_type(&summary, &location),
                location,
                coords,
                start_date,
                end_date,
                url,
                source: "techmeme".to_string(),
                description: String::new(),
            })
        })
        .collect::<Vec<_>>()
}

fn parse_dev_events_rss(content: &str) -> Vec<TechEvent> {
    let today = Utc::now().date_naive();

    RSS_ITEM_RE
        .captures_iter(content)
        .filter_map(|capture| {
            let block = capture.get(1)?.as_str();
            let title = extract_rss_tag(block, "title")?;
            let title = normalize_whitespace(&xml_unescape(&title));
            if title.is_empty() {
                return None;
            }

            let link = extract_rss_tag(block, "link").unwrap_or_default();
            let description = extract_rss_tag(block, "description").unwrap_or_default();
            let guid = RSS_GUID_RE
                .captures(block)
                .and_then(|guid| guid.get(1))
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            let start_date = RSS_DATE_RE
                .captures(&description)
                .and_then(|date| date.get(1))
                .and_then(|date| {
                    DateTime::parse_from_rfc2822(&format!("{} 00:00:00 +0000", date.as_str())).ok()
                })
                .map(|date| date.date_naive())
                .or_else(|| {
                    RSS_DATE_RE
                        .captures(&description)
                        .and_then(|date| date.get(1))
                        .and_then(|date| NaiveDate::parse_from_str(date.as_str(), "%B %d, %Y").ok())
                        .or_else(|| {
                            RSS_DATE_RE
                                .captures(&description)
                                .and_then(|date| date.get(1))
                                .and_then(|date| {
                                    NaiveDate::parse_from_str(date.as_str(), "%B %d %Y").ok()
                                })
                        })
                })?;

            if start_date < today {
                return None;
            }

            let mut location = RSS_LOCATION_RE_1
                .captures(&description)
                .and_then(|loc| loc.get(1))
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            if location.is_empty() {
                location = RSS_LOCATION_RE_2
                    .captures(&description)
                    .and_then(|loc| loc.get(1))
                    .map(|value| value.as_str().trim().to_string())
                    .unwrap_or_default();
            }
            if description.to_ascii_lowercase().contains("online") {
                location = "Online".to_string();
            }

            let coords = if location.eq_ignore_ascii_case("online") {
                Some(TechEventCoords {
                    lat: 0.0,
                    lng: 0.0,
                    country: "Virtual".to_string(),
                    original: "Online".to_string(),
                    r#virtual: true,
                })
            } else {
                None
            };

            let fallback_id = format!(
                "dev-events-{}",
                title
                    .to_ascii_lowercase()
                    .replace(|c: char| !c.is_ascii_alphanumeric(), "")
                    .chars()
                    .take(20)
                    .collect::<String>()
            );

            Some(TechEvent {
                id: if guid.is_empty() { fallback_id } else { guid },
                title,
                r#type: "conference".to_string(),
                location,
                coords,
                start_date: start_date.format("%Y-%m-%d").to_string(),
                end_date: start_date.format("%Y-%m-%d").to_string(),
                url: link,
                source: "dev.events".to_string(),
                description: String::new(),
            })
        })
        .collect::<Vec<_>>()
}

fn curated_events() -> Vec<TechEvent> {
    vec![
        TechEvent {
            id: "step-dubai-2026".to_string(),
            title: "STEP Dubai 2026".to_string(),
            r#type: "conference".to_string(),
            location: "Dubai Internet City, Dubai".to_string(),
            coords: Some(TechEventCoords {
                lat: 25.0956,
                lng: 55.1548,
                country: "UAE".to_string(),
                original: "Dubai Internet City, Dubai".to_string(),
                r#virtual: false,
            }),
            start_date: "2026-02-11".to_string(),
            end_date: "2026-02-12".to_string(),
            url: "https://dubai.stepconference.com".to_string(),
            source: "curated".to_string(),
            description: "Intelligence Everywhere: The AI Economy".to_string(),
        },
        TechEvent {
            id: "token2049-dubai-2026".to_string(),
            title: "TOKEN2049 Dubai 2026".to_string(),
            r#type: "conference".to_string(),
            location: "Dubai, UAE".to_string(),
            coords: Some(TechEventCoords {
                lat: 25.2048,
                lng: 55.2708,
                country: "UAE".to_string(),
                original: "Dubai, UAE".to_string(),
                r#virtual: false,
            }),
            start_date: "2026-04-29".to_string(),
            end_date: "2026-04-30".to_string(),
            url: "https://www.token2049.com".to_string(),
            source: "curated".to_string(),
            description: "Premier crypto event in Dubai".to_string(),
        },
        TechEvent {
            id: "collision-2026".to_string(),
            title: "Collision 2026".to_string(),
            r#type: "conference".to_string(),
            location: "Toronto, Canada".to_string(),
            coords: Some(TechEventCoords {
                lat: 43.6532,
                lng: -79.3832,
                country: "Canada".to_string(),
                original: "Toronto, Canada".to_string(),
                r#virtual: false,
            }),
            start_date: "2026-06-22".to_string(),
            end_date: "2026-06-25".to_string(),
            url: "https://collisionconf.com".to_string(),
            source: "curated".to_string(),
            description: "North America's fastest growing tech conference".to_string(),
        },
        TechEvent {
            id: "web-summit-2026".to_string(),
            title: "Web Summit 2026".to_string(),
            r#type: "conference".to_string(),
            location: "Lisbon, Portugal".to_string(),
            coords: Some(TechEventCoords {
                lat: 38.7223,
                lng: -9.1393,
                country: "Portugal".to_string(),
                original: "Lisbon, Portugal".to_string(),
                r#virtual: false,
            }),
            start_date: "2026-11-02".to_string(),
            end_date: "2026-11-05".to_string(),
            url: "https://websummit.com".to_string(),
            source: "curated".to_string(),
            description: "The world's premier tech conference".to_string(),
        },
    ]
}

fn dedupe_events(events: &[TechEvent]) -> Vec<TechEvent> {
    let mut seen = HashSet::new();
    events
        .iter()
        .filter_map(|event| {
            let year = event.start_date.chars().take(4).collect::<String>();
            let normalized_title = event
                .title
                .to_ascii_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "")
                .chars()
                .take(30)
                .collect::<String>();
            let key = format!("{}{}", normalized_title, year);
            seen.insert(key).then_some(event.clone())
        })
        .collect::<Vec<_>>()
}

fn limit_events(mut response: ListTechEventsResponse, limit: i32) -> ListTechEventsResponse {
    let events = response
        .events
        .into_iter()
        .take(limit as usize)
        .collect::<Vec<_>>();
    let conference_count = events
        .iter()
        .filter(|event| event.r#type == "conference")
        .count() as i32;
    let mappable_count = events
        .iter()
        .filter(|event| {
            event.r#type == "conference"
                && event
                    .coords
                    .as_ref()
                    .is_some_and(|coords| !coords.r#virtual)
        })
        .count() as i32;

    response.count = events.len() as i32;
    response.conference_count = conference_count;
    response.mappable_count = mappable_count;
    response.events = events;
    response
}

fn get_cache<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: &str,
) -> Result<Option<(T, bool)>, AppError> {
    let cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    Ok(cache
        .get(key)
        .map(|entry| (entry.value.clone(), Instant::now() <= entry.expires_at)))
}

fn set_cache<T: Clone>(
    cache: &Mutex<HashMap<String, CacheEntry<T>>>,
    key: String,
    value: &T,
    ttl: Duration,
) -> Result<(), AppError> {
    let mut cache = cache
        .lock()
        .map_err(|_| AppError::Internal("cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + ttl,
        },
    );
    Ok(())
}

async fn fetch_arxiv_papers(state: &AppState, request: &ListArxivPapersRequest) -> Vec<ArxivPaper> {
    let category = if request.category.trim().is_empty() {
        "cs.AI"
    } else {
        request.category.trim()
    };
    let size = page_size(request.pagination.as_ref(), DEFAULT_PAGE_SIZE, 200);

    let search_query = if request.query.trim().is_empty() {
        format!("cat:{}", category)
    } else {
        format!("all:{}+AND+cat:{}", request.query.trim(), category)
    };

    let url = format!(
        "https://export.arxiv.org/api/query?search_query={}&start=0&max_results={}",
        urlencoding::encode(&search_query),
        size
    );

    let response = match state
        .http_client
        .get(url)
        .header("Accept", "application/xml")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return Vec::new(),
    };

    let xml = match response.text().await {
        Ok(xml) => xml,
        Err(_) => return Vec::new(),
    };

    parse_arxiv_papers(&xml)
}

async fn fetch_trending_repos(
    state: &AppState,
    request: &ListTrendingReposRequest,
) -> Vec<GithubRepo> {
    let language = if request.language.trim().is_empty() {
        "python"
    } else {
        request.language.trim()
    };
    let period = if request.period.trim().is_empty() {
        "daily"
    } else {
        request.period.trim()
    };
    let size = page_size(request.pagination.as_ref(), DEFAULT_PAGE_SIZE, 100);

    let primary_url = format!(
        "https://api.gitterapp.com/repositories?language={}&since={}",
        urlencoding::encode(language),
        urlencoding::encode(period)
    );

    let payload = if let Ok(response) = state
        .http_client
        .get(primary_url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        if response.status().is_success() {
            match response.json::<Value>().await {
                Ok(payload) => payload,
                Err(_) => Value::Null,
            }
        } else {
            Value::Null
        }
    } else {
        Value::Null
    };

    let payload = if payload.is_array() {
        payload
    } else {
        let fallback_url = format!(
            "https://gh-trending-api.herokuapp.com/repositories/{}?since={}",
            urlencoding::encode(language),
            urlencoding::encode(period)
        );
        let response = match state
            .http_client
            .get(fallback_url)
            .header("Accept", "application/json")
            .header("User-Agent", CHROME_UA)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            _ => return Vec::new(),
        };

        match response.json::<Value>().await {
            Ok(payload) => payload,
            Err(_) => return Vec::new(),
        }
    };

    let Some(rows) = payload.as_array() else {
        return Vec::new();
    };

    rows.iter()
        .take(size)
        .map(|row| {
            let author = string_value(row.get("author"));
            let name = string_value(row.get("name"));
            let full_name = if !author.is_empty() && !name.is_empty() {
                format!("{author}/{name}")
            } else {
                string_value(row.get("fullName"))
            };
            GithubRepo {
                full_name,
                description: string_value(row.get("description")),
                language: string_value(row.get("language")),
                stars: parse_i32(row.get("stars")),
                stars_today: parse_i32(row.get("currentPeriodStars")),
                forks: parse_i32(row.get("forks")),
                url: {
                    let direct = string_value(row.get("url"));
                    if !direct.is_empty() {
                        direct
                    } else {
                        format!("https://github.com/{author}/{name}")
                    }
                },
            }
        })
        .collect::<Vec<_>>()
}

async fn fetch_hackernews_items(
    state: &AppState,
    request: &ListHackernewsItemsRequest,
) -> Vec<HackernewsItem> {
    let feed = allowed_hn_feed(request.feed_type.trim());
    let size = page_size(request.pagination.as_ref(), DEFAULT_HN_PAGE_SIZE, 100);

    let ids_url = format!("https://hacker-news.firebaseio.com/v0/{}stories.json", feed);
    let ids_response = match state
        .http_client
        .get(ids_url)
        .header("User-Agent", CHROME_UA)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => response,
        _ => return Vec::new(),
    };

    let ids_payload = match ids_response.json::<Value>().await {
        Ok(payload) => payload,
        Err(_) => return Vec::new(),
    };

    let Some(ids) = ids_payload.as_array() else {
        return Vec::new();
    };

    let ids = ids
        .iter()
        .take(size)
        .filter_map(|id| id.as_i64())
        .collect::<Vec<_>>();

    let mut items = Vec::new();
    for chunk in ids.chunks(HN_MAX_CONCURRENCY) {
        let jobs = chunk.iter().map(|id| async move {
            let url = format!("https://hacker-news.firebaseio.com/v0/item/{}.json", id);
            let response = state
                .http_client
                .get(url)
                .header("User-Agent", CHROME_UA)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            let payload = response.json::<Value>().await.ok()?;
            if payload.get("type").and_then(Value::as_str) != Some("story") {
                return None;
            }
            Some(HackernewsItem {
                id: payload
                    .get("id")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                title: string_value(payload.get("title")),
                url: string_value(payload.get("url")),
                score: parse_i32(payload.get("score")),
                comment_count: parse_i32(payload.get("descendants")),
                by: string_value(payload.get("by")),
                submitted_at: payload
                    .get("time")
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
                    * 1000,
            })
        });

        for item in futures::future::join_all(jobs).await.into_iter().flatten() {
            items.push(item);
        }
    }

    items
}

async fn fetch_tech_events(
    state: &AppState,
    request: &ListTechEventsRequest,
) -> ListTechEventsResponse {
    let ics = state
        .http_client
        .get("https://www.techmeme.com/newsy_events.ics")
        .header("User-Agent", CHROME_UA)
        .send();
    let rss = state
        .http_client
        .get("https://dev.events/rss.xml")
        .header("User-Agent", CHROME_UA)
        .send();

    let (ics_response, rss_response) = tokio::join!(ics, rss);

    let mut events = Vec::new();

    if let Ok(response) = ics_response
        && response.status().is_success()
        && let Ok(content) = response.text().await
    {
        events.extend(parse_ics_events(&content));
    }

    if let Ok(response) = rss_response
        && response.status().is_success()
        && let Ok(content) = response.text().await
    {
        events.extend(parse_dev_events_rss(&content));
    }

    let today = Utc::now().date_naive();
    for event in curated_events() {
        if NaiveDate::parse_from_str(&event.start_date, "%Y-%m-%d")
            .ok()
            .is_some_and(|date| date >= today)
        {
            events.push(event);
        }
    }

    let mut events = dedupe_events(&events);
    events.sort_by(|a, b| a.start_date.cmp(&b.start_date));

    let type_filter = request.r#type.trim().to_ascii_lowercase();
    if !type_filter.is_empty() && type_filter != "all" {
        events.retain(|event| event.r#type.to_ascii_lowercase() == type_filter);
    }

    if request.mappable {
        events.retain(|event| {
            event
                .coords
                .as_ref()
                .is_some_and(|coords| !coords.r#virtual)
        });
    }

    if request.days > 0 {
        let cutoff = today + ChronoDuration::days(request.days as i64);
        events.retain(|event| {
            NaiveDate::parse_from_str(&event.start_date, "%Y-%m-%d")
                .ok()
                .is_some_and(|date| date <= cutoff)
        });
    }

    if request.limit > 0 {
        events = events
            .into_iter()
            .take(request.limit as usize)
            .collect::<Vec<_>>();
    }

    let conference_count = events
        .iter()
        .filter(|event| event.r#type == "conference")
        .count() as i32;
    let mappable_count = events
        .iter()
        .filter(|event| {
            event.r#type == "conference"
                && event
                    .coords
                    .as_ref()
                    .is_some_and(|coords| !coords.r#virtual)
        })
        .count() as i32;

    ListTechEventsResponse {
        success: true,
        count: events.len() as i32,
        conference_count,
        mappable_count,
        last_updated: now_iso(),
        events,
        error: String::new(),
    }
}

pub async fn list_arxiv_papers(
    State(state): State<AppState>,
    Json(request): Json<ListArxivPapersRequest>,
) -> Result<Json<ListArxivPapersResponse>, AppError> {
    let cache_key = format!(
        "{}:{}:{}",
        if request.category.trim().is_empty() {
            "cs.AI"
        } else {
            request.category.trim()
        },
        request.query.trim(),
        page_size(request.pagination.as_ref(), DEFAULT_PAGE_SIZE, 200)
    );

    let stale_cached = get_cache(&ARXIV_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let papers = fetch_arxiv_papers(&state, &request).await;
    let response = ListArxivPapersResponse {
        papers,
        pagination: None,
    };

    if !response.papers.is_empty() {
        set_cache(&ARXIV_CACHE, cache_key, &response, ARXIV_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListArxivPapersResponse {
            papers: Vec::new(),
            pagination: None,
        },
    )))
}

pub async fn list_trending_repos(
    State(state): State<AppState>,
    Json(request): Json<ListTrendingReposRequest>,
) -> Result<Json<ListTrendingReposResponse>, AppError> {
    let cache_key = format!(
        "{}:{}:{}",
        if request.language.trim().is_empty() {
            "python"
        } else {
            request.language.trim()
        },
        if request.period.trim().is_empty() {
            "daily"
        } else {
            request.period.trim()
        },
        page_size(request.pagination.as_ref(), DEFAULT_PAGE_SIZE, 100)
    );

    let stale_cached = get_cache(&TRENDING_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let repos = fetch_trending_repos(&state, &request).await;
    let response = ListTrendingReposResponse {
        repos,
        pagination: None,
    };

    if !response.repos.is_empty() {
        set_cache(&TRENDING_CACHE, cache_key, &response, TRENDING_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListTrendingReposResponse {
            repos: Vec::new(),
            pagination: None,
        },
    )))
}

pub async fn list_hackernews_items(
    State(state): State<AppState>,
    Json(request): Json<ListHackernewsItemsRequest>,
) -> Result<Json<ListHackernewsItemsResponse>, AppError> {
    let feed = allowed_hn_feed(request.feed_type.trim());
    let cache_key = format!(
        "{}:{}",
        feed,
        page_size(request.pagination.as_ref(), DEFAULT_HN_PAGE_SIZE, 100)
    );

    let stale_cached = get_cache(&HN_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let items = fetch_hackernews_items(&state, &request).await;
    let response = ListHackernewsItemsResponse {
        items,
        pagination: None,
    };

    if !response.items.is_empty() {
        set_cache(&HN_CACHE, cache_key, &response, HN_CACHE_TTL)?;
        return Ok(Json(response));
    }

    Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
        ListHackernewsItemsResponse {
            items: Vec::new(),
            pagination: None,
        },
    )))
}

pub async fn list_tech_events(
    State(state): State<AppState>,
    Json(request): Json<ListTechEventsRequest>,
) -> Result<Json<ListTechEventsResponse>, AppError> {
    if request.limit < 0 || request.limit > 500 {
        return Err(AppError::BadRequest(
            "limit must be between 0 and 500".to_string(),
        ));
    }
    if request.days < 0 {
        return Err(AppError::BadRequest(
            "days must be greater than or equal to 0".to_string(),
        ));
    }

    let cache_key = format!(
        "{}:{}:{}",
        if request.r#type.trim().is_empty() {
            "all"
        } else {
            request.r#type.trim()
        },
        if request.mappable { 1 } else { 0 },
        request.days
    );

    let stale_cached = get_cache(&TECH_EVENTS_CACHE, &cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        if request.limit > 0 && cached.events.len() > request.limit as usize {
            return Ok(Json(limit_events(cached, request.limit)));
        }
        return Ok(Json(cached));
    }

    let mut cache_request = request.clone();
    cache_request.limit = 0;

    let response = fetch_tech_events(&state, &cache_request).await;
    if !response.events.is_empty() {
        set_cache(
            &TECH_EVENTS_CACHE,
            cache_key,
            &response,
            TECH_EVENTS_CACHE_TTL,
        )?;
    }

    if request.limit > 0 && response.events.len() > request.limit as usize {
        return Ok(Json(limit_events(response, request.limit)));
    }

    if response.events.is_empty() {
        return Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
            ListTechEventsResponse {
                success: false,
                count: 0,
                conference_count: 0,
                mappable_count: 0,
                last_updated: now_iso(),
                events: Vec::new(),
                error: "No events available".to_string(),
            },
        )));
    }

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arxiv_entry_from_xml() {
        let xml = r#"<feed><entry><id>http://arxiv.org/abs/2501.00001v1</id><title>Test Title</title><summary>Desc</summary><author><name>Alice</name></author><category term="cs.AI"/><published>2025-01-01T00:00:00Z</published><link rel="alternate" href="http://arxiv.org/abs/2501.00001"/></entry></feed>"#;
        let papers = parse_arxiv_papers(xml);
        assert_eq!(papers.len(), 1);
        assert_eq!(papers[0].id, "2501.00001v1");
        assert_eq!(papers[0].authors, vec!["Alice"]);
    }

    #[test]
    fn falls_back_to_top_hn_feed() {
        assert_eq!(allowed_hn_feed("unknown"), "top");
        assert_eq!(allowed_hn_feed("best"), "best");
    }

    #[test]
    fn parses_ics_dates() {
        assert_eq!(parse_ics_date("20260211"), Some("2026-02-11".to_string()));
        assert_eq!(parse_ics_date("bad"), None);
    }
}
