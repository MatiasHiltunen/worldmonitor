use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use feed_rs::parser;
use flow::VersionPackLoader;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use ratatui_textarea::{Input as TextInput, TextArea};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use strum::{Display, EnumIter, IntoEnumIterator};
use v3_rust_server::{
    config::AppConfig as ServerAppConfig,
    in_process::{InProcessClient, InProcessClientError},
};

const DEFAULT_CHATJIMMY_PACK_PATH: &str =
    "/data/data/com.termux/files/home/lega/tui_webflow/packs/chatjimmy_news_single.eon";

#[derive(Debug, Parser)]
#[command(
    name = "worldmonitor-v2",
    version,
    about = "WorldMonitor v2 (pure Rust TUI client)"
)]
struct Cli {
    #[arg(long, env = "WM_BASE_URL", default_value = "http://127.0.0.1:3000")]
    base_url: String,
    #[arg(long, env = "WORLDMONITOR_API_KEY")]
    api_key: Option<String>,
    #[arg(long, env = "WM_API_MODE", value_enum, default_value_t = ApiMode::Library)]
    api_mode: ApiMode,
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 0)]
    auto_refresh_secs: u64,
    #[arg(long, env = "WM_BRIEF_PROVIDER", value_enum, default_value_t = BriefIntelProvider::Auto)]
    brief_provider: BriefIntelProvider,
    #[arg(long, env = "WM_CHATJIMMY_PACK", default_value = DEFAULT_CHATJIMMY_PACK_PATH)]
    chatjimmy_pack: String,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum ApiMode {
    Library,
    Http,
    Auto,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum BriefIntelProvider {
    Auto,
    Server,
    Chatjimmy,
}

#[derive(Clone, Copy, Debug, Display, EnumIter, PartialEq, Eq, Hash)]
enum Endpoint {
    #[strum(to_string = "Seismology: List Earthquakes")]
    SeismologyEarthquakes,
    #[strum(to_string = "Unrest: List Unrest Events")]
    UnrestEvents,
    #[strum(to_string = "Infrastructure: Service Status")]
    InfrastructureStatuses,
    #[strum(to_string = "Market: List Crypto Quotes")]
    MarketCryptoQuotes,
}

impl Endpoint {
    fn path(self) -> &'static str {
        match self {
            Endpoint::SeismologyEarthquakes => "/api/seismology/v1/list-earthquakes",
            Endpoint::UnrestEvents => "/api/unrest/v1/list-unrest-events",
            Endpoint::InfrastructureStatuses => "/api/infrastructure/v1/list-service-statuses",
            Endpoint::MarketCryptoQuotes => "/api/market/v1/list-crypto-quotes",
        }
    }

    fn default_request_body(self) -> Value {
        match self {
            Endpoint::SeismologyEarthquakes => {
                json!({
                    "minMagnitude": 4.5,
                    "pagination": { "pageSize": 20, "cursor": "" }
                })
            }
            Endpoint::UnrestEvents => {
                json!({
                    "country": "",
                    "minSeverity": "SEVERITY_LEVEL_UNSPECIFIED",
                    "pagination": { "pageSize": 20, "cursor": "" }
                })
            }
            Endpoint::InfrastructureStatuses => {
                json!({
                    "status": "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED"
                })
            }
            Endpoint::MarketCryptoQuotes => {
                json!({
                    "ids": ["bitcoin", "ethereum", "solana", "xrp", "dogecoin"]
                })
            }
        }
    }

    fn openapi_doc_filename(self) -> &'static str {
        match self {
            Endpoint::SeismologyEarthquakes => "SeismologyService.openapi.json",
            Endpoint::UnrestEvents => "UnrestService.openapi.json",
            Endpoint::InfrastructureStatuses => "InfrastructureService.openapi.json",
            Endpoint::MarketCryptoQuotes => "MarketService.openapi.json",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppView {
    Api,
    Rss,
    Brief,
    Settings,
}

impl AppView {
    fn next(self) -> Self {
        match self {
            AppView::Api => AppView::Rss,
            AppView::Rss => AppView::Brief,
            AppView::Brief => AppView::Settings,
            AppView::Settings => AppView::Api,
        }
    }
}

#[derive(Clone, Copy, Debug, Display, EnumIter, PartialEq, Eq, Hash)]
enum FeedVariant {
    #[strum(to_string = "WORLD")]
    World,
    #[strum(to_string = "TECH")]
    Tech,
    #[strum(to_string = "FINANCE")]
    Finance,
}

impl FeedVariant {
    fn next(self) -> Self {
        match self {
            FeedVariant::World => FeedVariant::Tech,
            FeedVariant::Tech => FeedVariant::Finance,
            FeedVariant::Finance => FeedVariant::World,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FeedSource {
    id: &'static str,
    name: &'static str,
    url: &'static str,
    category: &'static str,
}

const WORLD_FEEDS: &[FeedSource] = &[
    FeedSource {
        id: "bbc-world",
        name: "BBC World",
        url: "https://feeds.bbci.co.uk/news/world/rss.xml",
        category: "Geopolitics",
    },
    FeedSource {
        id: "guardian-world",
        name: "Guardian World",
        url: "https://www.theguardian.com/world/rss",
        category: "Geopolitics",
    },
    FeedSource {
        id: "aljazeera-all",
        name: "Al Jazeera",
        url: "https://www.aljazeera.com/xml/rss/all.xml",
        category: "Conflict",
    },
    FeedSource {
        id: "un-news",
        name: "UN News",
        url: "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
        category: "Humanitarian",
    },
    FeedSource {
        id: "npr-world",
        name: "NPR World",
        url: "https://feeds.npr.org/1004/rss.xml",
        category: "Geopolitics",
    },
];

const TECH_FEEDS: &[FeedSource] = &[
    FeedSource {
        id: "hn-frontpage",
        name: "Hacker News",
        url: "https://hnrss.org/frontpage",
        category: "Startups",
    },
    FeedSource {
        id: "techcrunch",
        name: "TechCrunch",
        url: "https://techcrunch.com/feed/",
        category: "Startups",
    },
    FeedSource {
        id: "the-verge",
        name: "The Verge",
        url: "https://www.theverge.com/rss/index.xml",
        category: "AI/Tech",
    },
    FeedSource {
        id: "ars",
        name: "Ars Technica",
        url: "https://feeds.arstechnica.com/arstechnica/index",
        category: "AI/Tech",
    },
    FeedSource {
        id: "github-blog",
        name: "GitHub Blog",
        url: "https://github.blog/feed/",
        category: "Developer",
    },
];

const FINANCE_FEEDS: &[FeedSource] = &[
    FeedSource {
        id: "marketwatch",
        name: "MarketWatch",
        url: "https://feeds.marketwatch.com/marketwatch/topstories/",
        category: "Markets",
    },
    FeedSource {
        id: "cnbc-markets",
        name: "CNBC Markets",
        url: "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        category: "Markets",
    },
    FeedSource {
        id: "coindesk",
        name: "CoinDesk",
        url: "https://www.coindesk.com/arc/outboundfeeds/rss/",
        category: "Crypto",
    },
    FeedSource {
        id: "investing",
        name: "Investing.com",
        url: "https://www.investing.com/rss/news_25.rss",
        category: "Macro",
    },
    FeedSource {
        id: "ft-world-economy",
        name: "FT Economy",
        url: "https://www.ft.com/world?format=rss",
        category: "Macro",
    },
];

const BRIEF_COUNTRIES: &[(&str, &str)] = &[
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

fn feed_sources_for_variant(variant: FeedVariant) -> &'static [FeedSource] {
    match variant {
        FeedVariant::World => WORLD_FEEDS,
        FeedVariant::Tech => TECH_FEEDS,
        FeedVariant::Finance => FINANCE_FEEDS,
    }
}

fn country_name_from_code(code: &str) -> String {
    BRIEF_COUNTRIES
        .iter()
        .find(|(country_code, _)| country_code.eq_ignore_ascii_case(code))
        .map(|(_, name)| (*name).to_string())
        .unwrap_or_else(|| code.to_string())
}

fn brief_country_aliases(code: &str) -> &'static [&'static str] {
    match code {
        "US" => &["united states", "usa", "washington", "america"],
        "RU" => &["russia", "moscow", "kremlin"],
        "CN" => &["china", "beijing"],
        "UA" => &["ukraine", "kyiv"],
        "IR" => &["iran", "tehran"],
        "IL" => &["israel", "jerusalem", "tel aviv"],
        "TW" => &["taiwan", "taipei"],
        "KP" => &["north korea", "pyongyang"],
        "SA" => &["saudi arabia", "riyadh"],
        "TR" => &["turkey", "ankara"],
        "PL" => &["poland", "warsaw"],
        "DE" => &["germany", "berlin"],
        "FR" => &["france", "paris"],
        "GB" => &["united kingdom", "britain", "london"],
        "IN" => &["india", "new delhi"],
        "PK" => &["pakistan", "islamabad"],
        "SY" => &["syria", "damascus"],
        "YE" => &["yemen", "sanaa"],
        "MM" => &["myanmar", "burma"],
        "VE" => &["venezuela", "caracas"],
        _ => &[],
    }
}

fn brief_search_terms(country_code: &str, country_name: &str) -> Vec<String> {
    let mut terms = brief_country_aliases(country_code)
        .iter()
        .map(|term| term.to_lowercase())
        .collect::<Vec<_>>();
    terms.extend(
        country_name
            .split(|ch: char| !ch.is_ascii_alphabetic())
            .map(str::trim)
            .filter(|term| term.len() >= 4)
            .map(|term| term.to_lowercase()),
    );
    terms.sort();
    terms.dedup();
    terms
}

fn is_valid_country_code(input: &str) -> bool {
    input.len() == 2 && input.chars().all(|ch| ch.is_ascii_alphabetic())
}

#[derive(Clone, Debug)]
struct FeedHealth {
    consecutive_failures: u8,
    cooldown_until: Option<Instant>,
    last_error: Option<String>,
    last_success: Option<Instant>,
}

impl Default for FeedHealth {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            cooldown_until: None,
            last_error: None,
            last_success: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RssItem {
    id: String,
    title: String,
    summary: String,
    link: String,
    source_name: String,
    category: String,
    published_ts_ms: i64,
    keyword_hits: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct RssFetchResult {
    items: Vec<RssItem>,
    updated_health: HashMap<String, FeedHealth>,
    fetched_feeds: usize,
    skipped_cooldown: usize,
    failed_feeds: usize,
    duration_ms: u128,
    keyword_counts: HashMap<String, usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RssInputMode {
    None,
    Search,
    Keywords,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BriefInputMode {
    None,
    CountryCode,
}

#[derive(Clone, Debug, Default)]
struct BriefSnapshot {
    country_code: String,
    country_name: String,
    intel_brief: String,
    intel_model: String,
    intel_generated_at: i64,
    cii_score: Option<f64>,
    cii_trend: String,
    strategic_level: String,
    stock_available: bool,
    stock_index_name: String,
    stock_price: f64,
    stock_week_change: f64,
    stock_currency: String,
    errors: Vec<String>,
}

#[derive(Clone, Debug)]
struct SettingsCheck {
    capability: String,
    key_names: String,
    required: bool,
    configured: bool,
    note: String,
}

#[derive(Clone, Debug)]
struct BriefSourceEvidence {
    id: String,
    text: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CountryIntelBriefResponse {
    #[serde(default)]
    country_code: String,
    #[serde(default)]
    country_name: String,
    #[serde(default)]
    brief: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    generated_at: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RiskScoresResponse {
    #[serde(default)]
    cii_scores: Vec<CiiScore>,
    #[serde(default)]
    strategic_risks: Vec<StrategicRisk>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CiiScore {
    #[serde(default)]
    region: String,
    #[serde(default)]
    combined_score: f64,
    #[serde(default)]
    trend: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StrategicRisk {
    #[serde(default)]
    region: String,
    #[serde(default)]
    level: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CountryStockIndexResponse {
    #[serde(default)]
    available: bool,
    #[serde(default)]
    code: String,
    #[serde(default)]
    index_name: String,
    #[serde(default)]
    price: f64,
    #[serde(default)]
    week_change_percent: f64,
    #[serde(default)]
    currency: String,
}

#[derive(Clone, Debug)]
struct ChatJimmyProvider {
    pack_path: PathBuf,
    base_url: String,
    headers: HashMap<String, String>,
    default_model: String,
    default_top_k: i64,
    system_prompt: String,
}

impl ChatJimmyProvider {
    fn from_pack(pack_path: impl AsRef<Path>) -> Result<Self> {
        let pack_path = pack_path.as_ref();
        let pack = VersionPackLoader::load_dir(pack_path).map_err(|error| {
            anyhow!(
                "failed to load ChatJimmy pack {}: {}",
                pack_path.display(),
                error
            )
        })?;
        let features = &pack.version.features;

        let default_model = features
            .get("default_model")
            .and_then(Value::as_str)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "llama3.1-8B".to_string());
        let default_top_k = features
            .get("default_top_k")
            .and_then(Value::as_i64)
            .unwrap_or(8)
            .clamp(1, 64);
        let system_prompt = features
            .get("system_prompt")
            .and_then(Value::as_str)
            .map(|value| value.to_string())
            .unwrap_or_else(|| {
                "You are a strict JSON API. Always return exactly one valid JSON object."
                    .to_string()
            });

        Ok(Self {
            pack_path: pack_path.to_path_buf(),
            base_url: pack.version.base_url.trim_end_matches('/').to_string(),
            headers: pack.version.headers.clone(),
            default_model,
            default_top_k,
            system_prompt,
        })
    }

    fn brief_prompt(
        &self,
        country_code: &str,
        country_name: &str,
        source_text_block: &str,
    ) -> String {
        format!(
            "Return ONLY valid JSON (no markdown, no backticks) with schema: {{\"brief\":\"string\",\"confidence\":0.0,\"used_source_ids\":[\"S1\"],\"insufficient_context\":false}}.\n\
Task: produce a grounded brief for {} ({}).\n\
STRICT RULES:\n\
- Use ONLY the SOURCE TEXT below. Do not use outside knowledge.\n\
- If a fact is not in SOURCE TEXT, do not mention it.\n\
- Write 4-8 bullet lines, each ending with one or more citations like [S1] or [S2][S3].\n\
- If source text is weak, set insufficient_context=true and state missing coverage.\n\
- Never mention events, dates, actors, or claims not present in sources.\n\
\n\
SOURCE TEXT:\n\
{}",
            country_name, country_code, source_text_block
        )
    }
}

#[derive(Clone)]
struct ApiClient {
    base_url: String,
    api_key: Option<String>,
    client: Client,
    transport: ApiTransport,
    brief_provider: BriefIntelProvider,
    chatjimmy_pack_path: String,
    chatjimmy: Option<ChatJimmyProvider>,
    chatjimmy_init_error: Option<String>,
}

#[derive(Clone)]
enum ApiTransport {
    Http,
    Library(InProcessClient),
}

impl ApiClient {
    fn new(
        base_url: String,
        api_key: Option<String>,
        timeout_secs: u64,
        api_mode: ApiMode,
        brief_provider: BriefIntelProvider,
        chatjimmy_pack: String,
    ) -> Result<Self> {
        let normalized_base_url = base_url.trim_end_matches('/').to_string();
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;

        let transport = match api_mode {
            ApiMode::Http => ApiTransport::Http,
            ApiMode::Library => {
                ApiTransport::Library(Self::build_library_client(api_key.clone(), timeout_secs)?)
            }
            ApiMode::Auto => {
                if looks_like_local_base_url(&normalized_base_url) {
                    ApiTransport::Library(Self::build_library_client(
                        api_key.clone(),
                        timeout_secs,
                    )?)
                } else {
                    ApiTransport::Http
                }
            }
        };

        let (chatjimmy, chatjimmy_init_error) = match ChatJimmyProvider::from_pack(&chatjimmy_pack)
        {
            Ok(provider) => (Some(provider), None),
            Err(error) => (None, Some(error.to_string())),
        };

        Ok(Self {
            base_url: normalized_base_url,
            api_key,
            client,
            transport,
            brief_provider,
            chatjimmy_pack_path: chatjimmy_pack,
            chatjimmy,
            chatjimmy_init_error,
        })
    }

    fn build_library_client(api_key: Option<String>, timeout_secs: u64) -> Result<InProcessClient> {
        let fallback_config = ServerAppConfig {
            bind_addr: "127.0.0.1:3000"
                .parse()
                .expect("valid default bind address"),
            valid_keys: Vec::new(),
            runtime_env: "development".to_string(),
            groq_api_key: None,
            acled_access_token: None,
            finnhub_api_key: None,
            fred_api_key: None,
            eia_api_key: None,
            request_timeout_ms: timeout_secs.saturating_mul(1000),
        };

        let mut server_config = ServerAppConfig::from_env().unwrap_or(fallback_config);
        server_config.request_timeout_ms = timeout_secs.saturating_mul(1000);
        if server_config.valid_keys.is_empty()
            && let Some(key) = api_key.as_ref()
        {
            server_config.valid_keys.push(key.clone());
        }

        let mut local = InProcessClient::from_config(server_config)
            .context("failed to initialize in-process rust-server client")?;
        if let Some(key) = api_key {
            local = local.with_default_api_key(Some(key));
        }
        Ok(local)
    }

    fn post_json_path_http(&self, path: &str, request_body: &Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self
            .client
            .post(&url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json");
        if let Some(key) = &self.api_key {
            request = request.header("X-WorldMonitor-Key", key);
        }

        let response = request
            .json(request_body)
            .send()
            .with_context(|| format!("request failed: {url}"))?;

        let status = response.status();
        let body_text = response
            .text()
            .context("failed to read upstream response body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "HTTP {} from {}: {}",
                status.as_u16(),
                path,
                truncate_for_error(&body_text, 180)
            ));
        }

        let payload: Value =
            serde_json::from_str(&body_text).context("response was not valid JSON")?;
        Ok(payload)
    }

    fn post_json_path_library(&self, path: &str, request_body: &Value) -> Result<Value> {
        let ApiTransport::Library(local) = &self.transport else {
            return Err(anyhow!("library transport unavailable"));
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to build tokio runtime for in-process call")?;

        runtime
            .block_on(local.post_json_path(path, request_body))
            .map_err(|error| match error {
                InProcessClientError::HttpStatus { status, path, body } => anyhow!(
                    "HTTP {} from {}: {}",
                    status.as_u16(),
                    path,
                    truncate_for_error(&body, 180)
                ),
                other => anyhow!("in-process call failed for {}: {}", path, other),
            })
    }

    fn post_json_path(&self, path: &str, request_body: &Value) -> Result<Value> {
        match &self.transport {
            ApiTransport::Http => self.post_json_path_http(path, request_body),
            ApiTransport::Library(_) => self.post_json_path_library(path, request_body),
        }
    }

    fn fetch_json(&self, endpoint: Endpoint, request_body: &Value) -> Result<Value> {
        self.post_json_path(endpoint.path(), request_body)
    }

    fn get_country_intel_brief(&self, country_code: &str) -> Result<CountryIntelBriefResponse> {
        let payload = self.post_json_path(
            "/api/intelligence/v1/get-country-intel-brief",
            &json!({ "countryCode": country_code }),
        )?;
        serde_json::from_value(payload).context("failed to decode intelligence brief response")
    }

    fn get_country_intel_brief_chatjimmy(
        &self,
        country_code: &str,
        evidence: &[BriefSourceEvidence],
    ) -> Result<CountryIntelBriefResponse> {
        let provider = self.chatjimmy.as_ref().ok_or_else(|| {
            anyhow!(
                "{}",
                self.chatjimmy_init_error
                    .clone()
                    .unwrap_or_else(|| "ChatJimmy provider is not configured".to_string())
            )
        })?;

        let model = self.resolve_chatjimmy_model(provider)?;
        let country_name = country_name_from_code(country_code);
        let source_text_block = render_source_text_block(evidence);
        if source_text_block.is_empty() {
            return Err(anyhow!(
                "ChatJimmy grounded brief requires source text, but none was provided"
            ));
        }
        let prompt = provider.brief_prompt(country_code, country_name.as_str(), &source_text_block);
        let url = format!("{}/api/chat", provider.base_url);

        let mut request = self
            .client
            .post(url)
            .header("Accept", "text/event-stream")
            .header("Content-Type", "application/json")
            .header("Origin", provider.base_url.as_str())
            .header("Referer", format!("{}/", provider.base_url));

        for (key, value) in &provider.headers {
            request = request.header(key, value);
        }

        let response = request
            .json(&json!({
                "messages": [{ "role": "user", "content": prompt }],
                "chatOptions": {
                    "selectedModel": model,
                    "systemPrompt": provider.system_prompt,
                    "topK": provider.default_top_k
                },
                "attachment": Value::Null
            }))
            .send()
            .with_context(|| format!("ChatJimmy request failed: {}", provider.base_url))?;

        let status = response.status();
        let body = response
            .text()
            .context("failed to read ChatJimmy response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "ChatJimmy HTTP {}: {}",
                status.as_u16(),
                truncate_for_error(&body, 220)
            ));
        }

        let payload = parse_chatjimmy_payload(body.as_str())?;
        let raw_brief = payload
            .get("brief")
            .and_then(Value::as_str)
            .or_else(|| payload.get("summary").and_then(Value::as_str))
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        let used_source_ids = payload
            .get("used_source_ids")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let (brief, model_label) = if brief_is_grounded(&raw_brief, &used_source_ids, evidence) {
            (raw_brief, format!("chatjimmy/{}", model))
        } else {
            (
                render_grounded_fallback_brief(country_code, country_name.as_str(), evidence),
                format!("chatjimmy/{}/grounded-fallback", model),
            )
        };

        Ok(CountryIntelBriefResponse {
            country_code: country_code.to_uppercase(),
            country_name,
            brief,
            model: model_label,
            generated_at: now_epoch_ms(),
        })
    }

    fn resolve_chatjimmy_model(&self, provider: &ChatJimmyProvider) -> Result<String> {
        let url = format!("{}/api/models", provider.base_url);
        let mut request = self
            .client
            .get(url)
            .header("Accept", "application/json")
            .header("Referer", format!("{}/", provider.base_url));
        for (key, value) in &provider.headers {
            request = request.header(key, value);
        }

        let response = request.send().with_context(|| {
            format!("failed to load ChatJimmy models from {}", provider.base_url)
        })?;
        let status = response.status();
        let body = response
            .text()
            .context("failed to read ChatJimmy models payload")?;
        if !status.is_success() {
            return Err(anyhow!(
                "ChatJimmy models probe HTTP {}: {}",
                status.as_u16(),
                truncate_for_error(&body, 200)
            ));
        }

        let payload: Value = serde_json::from_str(body.as_str())
            .context("ChatJimmy models response was not valid JSON")?;
        Ok(payload
            .get("data")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|model| model.get("id"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.to_string())
            .unwrap_or_else(|| provider.default_model.clone()))
    }

    fn get_risk_scores(&self, region: &str) -> Result<RiskScoresResponse> {
        let payload = self.post_json_path(
            "/api/intelligence/v1/get-risk-scores",
            &json!({ "region": region }),
        )?;
        serde_json::from_value(payload).context("failed to decode risk scores response")
    }

    fn get_country_stock_index(&self, country_code: &str) -> Result<CountryStockIndexResponse> {
        let payload = self.post_json_path(
            "/api/market/v1/get-country-stock-index",
            &json!({ "countryCode": country_code }),
        )?;
        serde_json::from_value(payload).context("failed to decode country stock index response")
    }

    fn transport_label(&self) -> &'static str {
        match &self.transport {
            ApiTransport::Http => "http",
            ApiTransport::Library(_) => "library",
        }
    }

    fn cli_api_key_present(&self) -> bool {
        self.api_key
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    }

    fn brief_provider_label(&self) -> &'static str {
        match self.brief_provider {
            BriefIntelProvider::Auto => "auto(server->chatjimmy)",
            BriefIntelProvider::Server => "server(groq)",
            BriefIntelProvider::Chatjimmy => "chatjimmy",
        }
    }

    fn chatjimmy_ready(&self) -> bool {
        self.chatjimmy.is_some()
    }

    fn chatjimmy_pack_display(&self) -> String {
        self.chatjimmy_pack_path.clone()
    }

    fn chatjimmy_status_note(&self) -> String {
        match (&self.chatjimmy, &self.chatjimmy_init_error) {
            (Some(provider), _) => format!("Loaded pack: {}", provider.pack_path.display()),
            (None, Some(error)) => format!("Pack unavailable: {}", truncate_for_error(error, 160)),
            (None, None) => "Pack unavailable".to_string(),
        }
    }
}

fn looks_like_local_base_url(base_url: &str) -> bool {
    let lower = base_url.to_ascii_lowercase();
    lower.starts_with("http://127.0.0.1:")
        || lower.starts_with("http://localhost:")
        || lower == "http://127.0.0.1"
        || lower == "http://localhost"
}

fn env_key_present(name: &str) -> bool {
    std::env::var(name)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn any_env_key_present(names: &[&str]) -> bool {
    names.iter().any(|name| env_key_present(name))
}

fn build_settings_checks(api: &ApiClient) -> Vec<SettingsCheck> {
    let remote_http =
        matches!(api.transport, ApiTransport::Http) && !looks_like_local_base_url(&api.base_url);
    let ollama_ready = env_key_present("OLLAMA_API_URL");
    let groq_ready = env_key_present("GROQ_API_KEY");
    let chatjimmy_ready = api.chatjimmy_ready();
    let brief_ready = match api.brief_provider {
        BriefIntelProvider::Server => groq_ready,
        BriefIntelProvider::Chatjimmy => chatjimmy_ready,
        BriefIntelProvider::Auto => groq_ready || chatjimmy_ready,
    };
    let brief_key_names = match api.brief_provider {
        BriefIntelProvider::Server => "GROQ_API_KEY",
        BriefIntelProvider::Chatjimmy => "WM_CHATJIMMY_PACK (or --chatjimmy-pack)",
        BriefIntelProvider::Auto => "GROQ_API_KEY OR WM_CHATJIMMY_PACK",
    };
    let brief_note = match api.brief_provider {
        BriefIntelProvider::Server => {
            "Server BRIEF endpoint requires GROQ_API_KEY in rust-server environment.".to_string()
        }
        BriefIntelProvider::Chatjimmy => format!(
            "Grounded-source mode: output limited to provided RSS/risk/stock evidence. {}",
            api.chatjimmy_status_note()
        ),
        BriefIntelProvider::Auto => format!(
            "Auto mode uses server first, then grounded ChatJimmy fallback. {}",
            api.chatjimmy_status_note()
        ),
    };

    vec![
        SettingsCheck {
            capability: "Protected HTTP API access".to_string(),
            key_names: "WORLDMONITOR_API_KEY / --api-key".to_string(),
            required: remote_http,
            configured: api.cli_api_key_present() || env_key_present("WORLDMONITOR_API_KEY"),
            note: if remote_http {
                "Required for many remote hosts when using HTTP mode.".to_string()
            } else {
                "Optional in local library mode.".to_string()
            },
        },
        SettingsCheck {
            capability: format!("Intel BRIEF provider ({})", api.brief_provider_label()),
            key_names: brief_key_names.to_string(),
            required: true,
            configured: brief_ready,
            note: brief_note,
        },
        SettingsCheck {
            capability: "ChatJimmy pack availability".to_string(),
            key_names: "WM_CHATJIMMY_PACK / --chatjimmy-pack".to_string(),
            required: matches!(api.brief_provider, BriefIntelProvider::Chatjimmy),
            configured: chatjimmy_ready,
            note: format!("Pack path: {}", api.chatjimmy_pack_display()),
        },
        SettingsCheck {
            capability: "Conflict ACLED feeds".to_string(),
            key_names: "ACLED_ACCESS_TOKEN".to_string(),
            required: false,
            configured: env_key_present("ACLED_ACCESS_TOKEN"),
            note: "Optional but improves unrest/conflict completeness.".to_string(),
        },
        SettingsCheck {
            capability: "Market quote enrichment".to_string(),
            key_names: "FINNHUB_API_KEY".to_string(),
            required: false,
            configured: env_key_present("FINNHUB_API_KEY"),
            note: "Optional for richer market endpoints.".to_string(),
        },
        SettingsCheck {
            capability: "Economic indicators".to_string(),
            key_names: "FRED_API_KEY + EIA_API_KEY".to_string(),
            required: false,
            configured: env_key_present("FRED_API_KEY") && env_key_present("EIA_API_KEY"),
            note: "Both keys recommended for full economic coverage.".to_string(),
        },
        SettingsCheck {
            capability: "Military aircraft enrichment".to_string(),
            key_names: "WINGBITS_API_KEY".to_string(),
            required: false,
            configured: env_key_present("WINGBITS_API_KEY"),
            note: "Needed for Wingbits details/batch endpoints.".to_string(),
        },
        SettingsCheck {
            capability: "Wildfire detections (FIRMS)".to_string(),
            key_names: "NASA_FIRMS_API_KEY or FIRMS_API_KEY".to_string(),
            required: false,
            configured: any_env_key_present(&["NASA_FIRMS_API_KEY", "FIRMS_API_KEY"]),
            note: "Without this, wildfire endpoint returns empty detections.".to_string(),
        },
        SettingsCheck {
            capability: "Cyber threat optional feeds".to_string(),
            key_names: "URLHAUS_AUTH_KEY / OTX_API_KEY / ABUSEIPDB_API_KEY".to_string(),
            required: false,
            configured: any_env_key_present(&[
                "URLHAUS_AUTH_KEY",
                "OTX_API_KEY",
                "ABUSEIPDB_API_KEY",
            ]),
            note: "At least one key enables additional cyber feed sources.".to_string(),
        },
        SettingsCheck {
            capability: "News summarization providers".to_string(),
            key_names: "OLLAMA_API_URL or OPENROUTER_API_KEY or GROQ_API_KEY or WM_CHATJIMMY_PACK"
                .to_string(),
            required: false,
            configured: ollama_ready
                || env_key_present("OPENROUTER_API_KEY")
                || groq_ready
                || chatjimmy_ready,
            note: "Configure one provider for summarize-article endpoint or BRIEF fallback."
                .to_string(),
        },
    ]
}

struct App {
    view: AppView,
    endpoints: Vec<Endpoint>,
    selected: usize,
    output_lines: Vec<String>,
    status_line: String,
    in_flight: bool,
    scroll: u16,
    template_bodies: HashMap<Endpoint, String>,
    request_bodies: HashMap<Endpoint, String>,
    request_editor: TextArea<'static>,
    editor_endpoint: Endpoint,
    editing_request: bool,
    auto_refresh_enabled: bool,
    refresh_interval: Option<Duration>,
    last_fetch_finished_at: Option<Instant>,
    rss_variant: FeedVariant,
    rss_items: Vec<RssItem>,
    rss_view_items: Vec<RssItem>,
    rss_selected: usize,
    rss_detail_scroll: u16,
    rss_query: String,
    rss_keywords: Vec<String>,
    rss_categories: Vec<String>,
    rss_category_index: usize,
    rss_search_editor: TextArea<'static>,
    rss_keywords_editor: TextArea<'static>,
    rss_input_mode: RssInputMode,
    rss_in_flight: bool,
    rss_last_fetch_finished_at: Option<Instant>,
    rss_feed_health: HashMap<String, FeedHealth>,
    rss_keyword_baseline: HashMap<String, f64>,
    rss_spikes: Vec<String>,
    rss_last_fetch_summary: String,
    brief_country_code: String,
    brief_country_index: usize,
    brief_country_editor: TextArea<'static>,
    brief_input_mode: BriefInputMode,
    brief_in_flight: bool,
    brief_last_fetch_finished_at: Option<Instant>,
    brief_last_fetch_summary: String,
    brief_snapshot: Option<BriefSnapshot>,
    brief_related_rss: Vec<RssItem>,
    brief_related_selected: usize,
    brief_brief_scroll: u16,
    brief_news_scroll: u16,
    brief_provider_label: String,
    transport_label: String,
    base_url_display: String,
    settings_checks: Vec<SettingsCheck>,
    settings_selected: usize,
    settings_detail_scroll: u16,
}

impl App {
    fn new(refresh_interval: Option<Duration>, api: &ApiClient) -> Self {
        let endpoints: Vec<Endpoint> = Endpoint::iter().collect();
        let selected = 0;
        let selected_endpoint = endpoints[selected];

        let (template_bodies, request_bodies, template_source, loaded_from_docs) =
            build_initial_request_bodies(&endpoints);

        let rss_variant = FeedVariant::World;
        let rss_categories = categories_for_variant(rss_variant);
        let rss_search_editor = build_single_line_editor("Search RSS (Enter apply, Esc cancel)");
        let rss_keywords_editor =
            build_single_line_editor("Keywords (comma-separated, Enter apply, Esc cancel)");
        let brief_country_code = BRIEF_COUNTRIES
            .first()
            .map(|(code, _)| (*code).to_string())
            .unwrap_or_else(|| "US".to_string());
        let mut brief_country_editor =
            build_single_line_editor("Country code (ISO-2, Enter apply, Esc cancel)");
        brief_country_editor.insert_str(brief_country_code.clone());
        let settings_checks = build_settings_checks(api);

        let request_editor = build_request_editor(
            request_bodies
                .get(&selected_endpoint)
                .map(String::as_str)
                .unwrap_or("{}"),
            false,
            selected_endpoint,
        );

        Self {
            view: AppView::Api,
            endpoints,
            selected,
            output_lines: vec![
                "WorldMonitor v2 (pure Rust)".to_string(),
                String::new(),
                template_source,
                String::new(),
                "Use Up/Down to choose endpoint, Enter/r to fetch.".to_string(),
                "Press Tab to switch API/RSS/BRIEF/SETTINGS workspaces.".to_string(),
                "Press e to edit request JSON, a to toggle auto-refresh.".to_string(),
                "Press t to reset current request body to template.".to_string(),
                "Use j/k to scroll response, q or Esc to quit.".to_string(),
            ],
            status_line: if loaded_from_docs > 0 {
                "Ready (OpenAPI templates loaded)".to_string()
            } else {
                "Ready (built-in request seeds)".to_string()
            },
            in_flight: false,
            scroll: 0,
            template_bodies,
            request_bodies,
            request_editor,
            editor_endpoint: selected_endpoint,
            editing_request: false,
            auto_refresh_enabled: refresh_interval.is_some(),
            refresh_interval,
            last_fetch_finished_at: None,
            rss_variant,
            rss_items: Vec::new(),
            rss_view_items: Vec::new(),
            rss_selected: 0,
            rss_detail_scroll: 0,
            rss_query: String::new(),
            rss_keywords: vec![
                "conflict".to_string(),
                "cyber".to_string(),
                "earthquake".to_string(),
                "sanction".to_string(),
            ],
            rss_categories,
            rss_category_index: 0,
            rss_search_editor,
            rss_keywords_editor,
            rss_input_mode: RssInputMode::None,
            rss_in_flight: false,
            rss_last_fetch_finished_at: None,
            rss_feed_health: HashMap::new(),
            rss_keyword_baseline: HashMap::new(),
            rss_spikes: Vec::new(),
            rss_last_fetch_summary: "RSS not fetched yet".to_string(),
            brief_country_code,
            brief_country_index: 0,
            brief_country_editor,
            brief_input_mode: BriefInputMode::None,
            brief_in_flight: false,
            brief_last_fetch_finished_at: None,
            brief_last_fetch_summary: "Country brief not fetched yet".to_string(),
            brief_snapshot: None,
            brief_related_rss: Vec::new(),
            brief_related_selected: 0,
            brief_brief_scroll: 0,
            brief_news_scroll: 0,
            brief_provider_label: api.brief_provider_label().to_string(),
            transport_label: api.transport_label().to_string(),
            base_url_display: api.base_url.clone(),
            settings_checks,
            settings_selected: 0,
            settings_detail_scroll: 0,
        }
    }

    fn selected_endpoint(&self) -> Endpoint {
        self.endpoints[self.selected]
    }

    fn select_next(&mut self) {
        self.selected = (self.selected + 1) % self.endpoints.len();
    }

    fn select_prev(&mut self) {
        if self.selected == 0 {
            self.selected = self.endpoints.len() - 1;
        } else {
            self.selected -= 1;
        }
    }

    fn scroll_down(&mut self) {
        self.scroll = self.scroll.saturating_add(1);
    }

    fn scroll_up(&mut self) {
        self.scroll = self.scroll.saturating_sub(1);
    }

    fn current_saved_request_body(&self) -> &str {
        self.request_bodies
            .get(&self.selected_endpoint())
            .map(String::as_str)
            .unwrap_or("{}")
    }

    fn ensure_editor_synced(&mut self) {
        if self.editing_request {
            return;
        }
        let selected = self.selected_endpoint();
        if self.editor_endpoint != selected {
            let body = self.current_saved_request_body().to_string();
            self.editor_endpoint = selected;
            self.request_editor = build_request_editor(&body, false, selected);
        }
    }

    fn enter_request_editor(&mut self) {
        let selected = self.selected_endpoint();
        let body = self.current_saved_request_body().to_string();
        self.editor_endpoint = selected;
        self.request_editor = build_request_editor(&body, true, selected);
        self.editing_request = true;
        self.status_line = format!("Editing request JSON for {}", selected);
    }

    fn discard_request_editor(&mut self) {
        self.editing_request = false;
        self.ensure_editor_synced();
        self.status_line = "Discarded unsaved request edits".to_string();
    }

    fn editor_text(&self) -> String {
        self.request_editor.lines().join("\n")
    }

    fn save_request_editor(&mut self) -> Result<()> {
        let selected = self.selected_endpoint();
        let raw = self.editor_text();
        let parsed: Value =
            serde_json::from_str(&raw).context("edited request body is not valid JSON")?;
        let pretty = pretty_json(&parsed);
        self.request_bodies.insert(selected, pretty.clone());
        self.editor_endpoint = selected;
        self.request_editor = build_request_editor(&pretty, true, selected);
        self.status_line = format!("Saved request body for {}", selected);
        Ok(())
    }

    fn template_body_for(&self, endpoint: Endpoint) -> &str {
        self.template_bodies
            .get(&endpoint)
            .map(String::as_str)
            .unwrap_or("{}")
    }

    fn reset_selected_request_to_template(&mut self) {
        let selected = self.selected_endpoint();
        let template = self.template_body_for(selected).to_string();
        self.request_bodies.insert(selected, template);
        self.ensure_editor_synced();
        self.status_line = format!("Reset request body for {} to template", selected);
    }

    fn reset_editor_to_template(&mut self) {
        let selected = self.selected_endpoint();
        let template = self.template_body_for(selected).to_string();
        self.request_editor = build_request_editor(&template, true, selected);
        self.status_line = format!("Reset unsaved editor content for {} to template", selected);
    }

    fn request_body_json(&self, endpoint: Endpoint) -> Result<Value> {
        let raw = self
            .request_bodies
            .get(&endpoint)
            .map(String::as_str)
            .unwrap_or("{}");
        serde_json::from_str(raw)
            .with_context(|| format!("saved request body for {} is invalid JSON", endpoint))
    }

    fn toggle_view(&mut self) {
        self.view = self.view.next();
        match self.view {
            AppView::Api => {
                self.status_line = "Switched to API workspace".to_string();
                self.ensure_editor_synced();
            }
            AppView::Rss => {
                self.status_line = "Switched to RSS workspace".to_string();
            }
            AppView::Brief => {
                self.status_line = "Switched to BRIEF workspace".to_string();
            }
            AppView::Settings => {
                self.status_line =
                    "Switched to SETTINGS workspace (API key/provider audit)".to_string();
            }
        }
    }

    fn toggle_auto_refresh(&mut self) {
        if self.refresh_interval.is_none() {
            self.status_line = "Auto-refresh unavailable; set --auto-refresh-secs > 0".to_string();
            return;
        }
        self.auto_refresh_enabled = !self.auto_refresh_enabled;
        self.status_line = if self.auto_refresh_enabled {
            "Auto-refresh enabled".to_string()
        } else {
            "Auto-refresh disabled".to_string()
        };
    }

    fn current_rss_category(&self) -> &str {
        self.rss_categories
            .get(self.rss_category_index)
            .map(String::as_str)
            .unwrap_or("All")
    }

    fn apply_rss_filters(&mut self) {
        let selected_category = self.current_rss_category().to_string();
        let query = self.rss_query.to_lowercase();
        self.rss_view_items = self
            .rss_items
            .iter()
            .filter(|item| selected_category == "All" || item.category == selected_category)
            .filter(|item| {
                if query.is_empty() {
                    true
                } else {
                    item.title.to_lowercase().contains(&query)
                        || item.summary.to_lowercase().contains(&query)
                        || item.source_name.to_lowercase().contains(&query)
                }
            })
            .cloned()
            .collect();

        if self.rss_view_items.is_empty() {
            self.rss_selected = 0;
        } else if self.rss_selected >= self.rss_view_items.len() {
            self.rss_selected = self.rss_view_items.len().saturating_sub(1);
        }
        self.rss_detail_scroll = 0;
    }

    fn cycle_rss_category_next(&mut self) {
        if self.rss_categories.is_empty() {
            return;
        }
        self.rss_category_index = (self.rss_category_index + 1) % self.rss_categories.len();
        self.apply_rss_filters();
    }

    fn cycle_rss_category_prev(&mut self) {
        if self.rss_categories.is_empty() {
            return;
        }
        if self.rss_category_index == 0 {
            self.rss_category_index = self.rss_categories.len() - 1;
        } else {
            self.rss_category_index -= 1;
        }
        self.apply_rss_filters();
    }

    fn cycle_rss_variant(&mut self) {
        self.rss_variant = self.rss_variant.next();
        self.rss_categories = categories_for_variant(self.rss_variant);
        self.rss_category_index = 0;
        self.rss_items.clear();
        self.rss_view_items.clear();
        self.rss_selected = 0;
        self.rss_detail_scroll = 0;
        self.rss_last_fetch_summary =
            format!("Switched to {} variant; refreshing feeds", self.rss_variant);
    }

    fn rss_select_next(&mut self) {
        if self.rss_view_items.is_empty() {
            self.rss_selected = 0;
            return;
        }
        self.rss_selected = (self.rss_selected + 1) % self.rss_view_items.len();
        self.rss_detail_scroll = 0;
    }

    fn rss_select_prev(&mut self) {
        if self.rss_view_items.is_empty() {
            self.rss_selected = 0;
            return;
        }
        if self.rss_selected == 0 {
            self.rss_selected = self.rss_view_items.len() - 1;
        } else {
            self.rss_selected -= 1;
        }
        self.rss_detail_scroll = 0;
    }

    fn rss_detail_scroll_down(&mut self) {
        self.rss_detail_scroll = self.rss_detail_scroll.saturating_add(1);
    }

    fn rss_detail_scroll_up(&mut self) {
        self.rss_detail_scroll = self.rss_detail_scroll.saturating_sub(1);
    }

    fn enter_rss_search_editor(&mut self) {
        self.rss_input_mode = RssInputMode::Search;
        self.rss_search_editor = build_single_line_editor("Search RSS (Enter apply, Esc cancel)");
        if !self.rss_query.is_empty() {
            self.rss_search_editor.insert_str(self.rss_query.clone());
        }
        self.status_line = "Editing RSS search query".to_string();
    }

    fn enter_rss_keywords_editor(&mut self) {
        self.rss_input_mode = RssInputMode::Keywords;
        self.rss_keywords_editor =
            build_single_line_editor("Keywords (comma-separated, Enter apply, Esc cancel)");
        if !self.rss_keywords.is_empty() {
            self.rss_keywords_editor
                .insert_str(self.rss_keywords.join(", "));
        }
        self.status_line = "Editing keyword monitors".to_string();
    }

    fn reset_rss_filters(&mut self) {
        self.rss_query.clear();
        self.rss_category_index = 0;
        self.apply_rss_filters();
        self.status_line = "Cleared RSS filters".to_string();
    }

    fn rss_editor_active(&self) -> bool {
        self.rss_input_mode != RssInputMode::None
    }

    fn brief_editor_active(&self) -> bool {
        self.brief_input_mode != BriefInputMode::None
    }

    fn set_brief_country_code(&mut self, country_code: String) {
        self.brief_country_code = country_code.to_uppercase();
        if let Some((index, _)) = BRIEF_COUNTRIES
            .iter()
            .enumerate()
            .find(|(_, (code, _))| *code == self.brief_country_code)
        {
            self.brief_country_index = index;
        }
        self.brief_brief_scroll = 0;
        self.brief_news_scroll = 0;
    }

    fn enter_brief_country_editor(&mut self) {
        self.brief_input_mode = BriefInputMode::CountryCode;
        self.brief_country_editor =
            build_single_line_editor("Country code (ISO-2, Enter apply, Esc cancel)");
        self.brief_country_editor
            .insert_str(self.brief_country_code.clone());
        self.status_line = "Editing BRIEF country code".to_string();
    }

    fn cycle_brief_country_next(&mut self) {
        if BRIEF_COUNTRIES.is_empty() {
            return;
        }
        self.brief_country_index = (self.brief_country_index + 1) % BRIEF_COUNTRIES.len();
        self.set_brief_country_code(BRIEF_COUNTRIES[self.brief_country_index].0.to_string());
        self.status_line = format!(
            "Selected {} ({})",
            BRIEF_COUNTRIES[self.brief_country_index].1, self.brief_country_code
        );
    }

    fn cycle_brief_country_prev(&mut self) {
        if BRIEF_COUNTRIES.is_empty() {
            return;
        }
        if self.brief_country_index == 0 {
            self.brief_country_index = BRIEF_COUNTRIES.len() - 1;
        } else {
            self.brief_country_index -= 1;
        }
        self.set_brief_country_code(BRIEF_COUNTRIES[self.brief_country_index].0.to_string());
        self.status_line = format!(
            "Selected {} ({})",
            BRIEF_COUNTRIES[self.brief_country_index].1, self.brief_country_code
        );
    }

    fn refresh_brief_related_rss(&mut self) {
        let Some(snapshot) = self.brief_snapshot.as_ref() else {
            self.brief_related_rss.clear();
            self.brief_related_selected = 0;
            self.brief_news_scroll = 0;
            return;
        };

        let terms = brief_search_terms(
            snapshot.country_code.as_str(),
            snapshot.country_name.as_str(),
        );

        if terms.is_empty() {
            self.brief_related_rss.clear();
            self.brief_related_selected = 0;
            self.brief_news_scroll = 0;
            return;
        }

        self.brief_related_rss = self
            .rss_items
            .iter()
            .filter(|item| {
                let haystack = format!("{} {} {}", item.title, item.summary, item.source_name);
                terms
                    .iter()
                    .any(|term| contains_keyword_word(haystack.as_str(), term))
            })
            .take(40)
            .cloned()
            .collect();

        if self.brief_related_rss.is_empty() {
            self.brief_related_selected = 0;
            self.brief_news_scroll = 0;
            return;
        }
        if self.brief_related_selected >= self.brief_related_rss.len() {
            self.brief_related_selected = self.brief_related_rss.len() - 1;
        }
        self.brief_news_scroll = 0;
    }

    fn collect_brief_rss_source_snippets(&self, country_code: &str, limit: usize) -> Vec<String> {
        let country_name = country_name_from_code(country_code);
        let terms = brief_search_terms(country_code, country_name.as_str());
        if terms.is_empty() {
            return Vec::new();
        }

        self.rss_items
            .iter()
            .filter(|item| {
                let haystack = format!("{} {} {}", item.title, item.summary, item.source_name);
                terms
                    .iter()
                    .any(|term| contains_keyword_word(haystack.as_str(), term))
            })
            .take(limit)
            .map(|item| {
                format!(
                    "RSS {} | {} | {}",
                    item.source_name,
                    item.title,
                    truncate_for_error(item.summary.as_str(), 220)
                )
            })
            .collect()
    }

    fn brief_select_next(&mut self) {
        if self.brief_related_rss.is_empty() {
            self.brief_related_selected = 0;
            return;
        }
        self.brief_related_selected =
            (self.brief_related_selected + 1) % self.brief_related_rss.len();
        self.brief_news_scroll = 0;
    }

    fn brief_select_prev(&mut self) {
        if self.brief_related_rss.is_empty() {
            self.brief_related_selected = 0;
            return;
        }
        if self.brief_related_selected == 0 {
            self.brief_related_selected = self.brief_related_rss.len() - 1;
        } else {
            self.brief_related_selected -= 1;
        }
        self.brief_news_scroll = 0;
    }

    fn brief_scroll_down(&mut self) {
        self.brief_brief_scroll = self.brief_brief_scroll.saturating_add(1);
    }

    fn brief_scroll_up(&mut self) {
        self.brief_brief_scroll = self.brief_brief_scroll.saturating_sub(1);
    }

    fn brief_news_scroll_down(&mut self) {
        self.brief_news_scroll = self.brief_news_scroll.saturating_add(1);
    }

    fn brief_news_scroll_up(&mut self) {
        self.brief_news_scroll = self.brief_news_scroll.saturating_sub(1);
    }

    fn refresh_settings(&mut self, api: &ApiClient) {
        self.settings_checks = build_settings_checks(api);
        if self.settings_checks.is_empty() {
            self.settings_selected = 0;
        } else if self.settings_selected >= self.settings_checks.len() {
            self.settings_selected = self.settings_checks.len().saturating_sub(1);
        }
        self.settings_detail_scroll = 0;
        self.status_line = "Refreshed API key and provider configuration audit".to_string();
    }

    fn settings_select_next(&mut self) {
        if self.settings_checks.is_empty() {
            self.settings_selected = 0;
            return;
        }
        self.settings_selected = (self.settings_selected + 1) % self.settings_checks.len();
        self.settings_detail_scroll = 0;
    }

    fn settings_select_prev(&mut self) {
        if self.settings_checks.is_empty() {
            self.settings_selected = 0;
            return;
        }
        if self.settings_selected == 0 {
            self.settings_selected = self.settings_checks.len() - 1;
        } else {
            self.settings_selected -= 1;
        }
        self.settings_detail_scroll = 0;
    }

    fn settings_detail_scroll_down(&mut self) {
        self.settings_detail_scroll = self.settings_detail_scroll.saturating_add(1);
    }

    fn settings_detail_scroll_up(&mut self) {
        self.settings_detail_scroll = self.settings_detail_scroll.saturating_sub(1);
    }

    fn should_auto_refresh_api(&self) -> bool {
        let Some(interval) = self.refresh_interval else {
            return false;
        };
        if !self.auto_refresh_enabled
            || self.in_flight
            || self.editing_request
            || self.view != AppView::Api
        {
            return false;
        }

        match self.last_fetch_finished_at {
            Some(last) => last.elapsed() >= interval,
            None => true,
        }
    }

    fn should_auto_refresh_rss(&self) -> bool {
        let Some(interval) = self.refresh_interval else {
            return false;
        };
        if !self.auto_refresh_enabled
            || self.rss_in_flight
            || self.rss_editor_active()
            || self.view != AppView::Rss
        {
            return false;
        }

        match self.rss_last_fetch_finished_at {
            Some(last) => last.elapsed() >= interval,
            None => true,
        }
    }

    fn should_auto_refresh_brief(&self) -> bool {
        let Some(interval) = self.refresh_interval else {
            return false;
        };
        if !self.auto_refresh_enabled
            || self.brief_in_flight
            || self.brief_editor_active()
            || self.view != AppView::Brief
        {
            return false;
        }

        match self.brief_last_fetch_finished_at {
            Some(last) => last.elapsed() >= interval,
            None => true,
        }
    }

    fn auto_refresh_summary(&self) -> String {
        let Some(interval) = self.refresh_interval else {
            return "auto-refresh: off".to_string();
        };
        if !self.auto_refresh_enabled {
            return format!("auto-refresh: paused ({}s)", interval.as_secs());
        }

        let base = match self.view {
            AppView::Api => self.last_fetch_finished_at,
            AppView::Rss => self.rss_last_fetch_finished_at,
            AppView::Brief => self.brief_last_fetch_finished_at,
            AppView::Settings => None,
        };

        let remaining = match base {
            Some(last) => interval.saturating_sub(last.elapsed()).as_secs(),
            None => 0,
        };
        format!(
            "auto-refresh: every {}s (next {}s)",
            interval.as_secs(),
            remaining
        )
    }
}

enum WorkerEvent {
    ApiSuccess {
        endpoint: Endpoint,
        lines: Vec<String>,
    },
    ApiFailure {
        endpoint: Endpoint,
        error: String,
    },
    RssSuccess(RssFetchResult),
    BriefSuccess {
        snapshot: BriefSnapshot,
        duration_ms: u128,
    },
}

fn truncate_for_error(input: &str, max_chars: usize) -> String {
    let normalized = input.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut shortened = normalized.chars().take(max_chars).collect::<String>();
    shortened.push_str("...");
    shortened
}

fn format_error_chain(error: &anyhow::Error, max_chars: usize) -> String {
    let joined = error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ");
    truncate_for_error(joined.as_str(), max_chars)
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn build_initial_request_bodies(
    endpoints: &[Endpoint],
) -> (
    HashMap<Endpoint, String>,
    HashMap<Endpoint, String>,
    String,
    usize,
) {
    let mut template_bodies = HashMap::new();
    let mut request_bodies = HashMap::new();
    let openapi_dir = find_openapi_dir();
    let mut loaded_from_docs = 0usize;

    for &endpoint in endpoints {
        let template = match openapi_dir
            .as_ref()
            .and_then(|dir| load_openapi_template(endpoint, dir).ok().flatten())
        {
            Some(template) => {
                loaded_from_docs += 1;
                merge_template_with_seed(template, endpoint.default_request_body())
            }
            None => endpoint.default_request_body(),
        };
        let template_pretty = pretty_json(&template);
        template_bodies.insert(endpoint, template_pretty.clone());
        request_bodies.insert(endpoint, template_pretty);
    }

    let summary = match openapi_dir {
        Some(dir) => format!(
            "OpenAPI request templates: {}/{} endpoints ({})",
            loaded_from_docs,
            endpoints.len(),
            dir.display()
        ),
        None => "OpenAPI request templates: not found, using built-in endpoint seeds".to_string(),
    };

    (template_bodies, request_bodies, summary, loaded_from_docs)
}

fn find_openapi_dir() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let current_dir = std::env::current_dir().ok();

    let mut candidates = vec![
        manifest_dir.join("../docs/api"),
        manifest_dir.join("docs/api"),
        PathBuf::from("docs/api"),
        PathBuf::from("../docs/api"),
    ];

    if let Some(dir) = current_dir {
        candidates.push(dir.join("docs/api"));
        candidates.push(dir.join("../docs/api"));
    }

    candidates
        .into_iter()
        .find(|path| path.is_dir())
        .and_then(|path| path.canonicalize().ok())
}

fn load_openapi_template(endpoint: Endpoint, openapi_dir: &Path) -> Result<Option<Value>> {
    let file_path = openapi_dir.join(endpoint.openapi_doc_filename());
    if !file_path.is_file() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&file_path)
        .with_context(|| format!("failed to read {}", file_path.display()))?;
    let doc: Value = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", file_path.display()))?;

    let schema = extract_request_schema(&doc, endpoint.path());
    let Some(schema) = schema else {
        return Ok(None);
    };

    let mut seen_refs = HashSet::new();
    let template = schema_to_template(schema, &doc, &mut seen_refs);
    if template.is_null() {
        Ok(None)
    } else {
        Ok(Some(template))
    }
}

fn extract_request_schema<'a>(doc: &'a Value, endpoint_path: &str) -> Option<&'a Value> {
    let escaped_path = escape_json_pointer_token(endpoint_path);
    let pointer = format!(
        "/paths/{}/post/requestBody/content/application~1json/schema",
        escaped_path
    );
    doc.pointer(&pointer)
}

fn escape_json_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn ref_to_json_pointer(reference: &str) -> Option<String> {
    reference
        .strip_prefix('#')
        .map(ToString::to_string)
        .filter(|pointer| pointer.starts_with('/'))
}

fn schema_to_template(schema: &Value, doc: &Value, seen_refs: &mut HashSet<String>) -> Value {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if !seen_refs.insert(reference.to_string()) {
            return Value::Null;
        }

        let resolved = ref_to_json_pointer(reference)
            .and_then(|pointer| doc.pointer(&pointer))
            .map(|resolved_schema| schema_to_template(resolved_schema, doc, seen_refs))
            .unwrap_or(Value::Null);

        seen_refs.remove(reference);
        return resolved;
    }

    if let Some(example) = schema.get("example") {
        return example.clone();
    }
    if let Some(default) = schema.get("default") {
        return default.clone();
    }
    if let Some(const_value) = schema.get("const") {
        return const_value.clone();
    }
    if let Some(first_enum) = schema
        .get("enum")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
    {
        return first_enum.clone();
    }

    if let Some(one_of) = schema.get("oneOf").and_then(Value::as_array)
        && let Some(first) = one_of.first()
    {
        return schema_to_template(first, doc, seen_refs);
    }
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array)
        && let Some(first) = any_of.first()
    {
        return schema_to_template(first, doc, seen_refs);
    }
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array)
        && let Some(first) = all_of.first()
    {
        return schema_to_template(first, doc, seen_refs);
    }

    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        return match kind {
            "object" => {
                let mut object = Map::new();
                if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                    for (key, value_schema) in properties {
                        let value = schema_to_template(value_schema, doc, seen_refs);
                        if !value.is_null() {
                            object.insert(key.clone(), value);
                        }
                    }
                }
                Value::Object(object)
            }
            "array" => {
                let item = schema
                    .get("items")
                    .map(|items| schema_to_template(items, doc, seen_refs))
                    .filter(|value| !value.is_null());
                match item {
                    Some(value) => Value::Array(vec![value]),
                    None => Value::Array(Vec::new()),
                }
            }
            "string" => Value::String(String::new()),
            "integer" => Value::from(0),
            "number" => Value::from(0),
            "boolean" => Value::Bool(false),
            _ => Value::Null,
        };
    }

    if schema.get("properties").is_some() {
        let mut object = Map::new();
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (key, value_schema) in properties {
                let value = schema_to_template(value_schema, doc, seen_refs);
                if !value.is_null() {
                    object.insert(key.clone(), value);
                }
            }
        }
        return Value::Object(object);
    }

    if schema.get("items").is_some() {
        let item = schema
            .get("items")
            .map(|items| schema_to_template(items, doc, seen_refs))
            .filter(|value| !value.is_null());
        return match item {
            Some(value) => Value::Array(vec![value]),
            None => Value::Array(Vec::new()),
        };
    }

    Value::Null
}

fn merge_template_with_seed(template: Value, seed: Value) -> Value {
    match (template, seed) {
        (Value::Object(mut template_map), Value::Object(seed_map)) => {
            for (key, seed_value) in seed_map {
                let merged = match template_map.remove(&key) {
                    Some(existing) => merge_template_with_seed(existing, seed_value),
                    None => seed_value,
                };
                template_map.insert(key, merged);
            }
            Value::Object(template_map)
        }
        (_, seed_value) => seed_value,
    }
}

fn build_request_editor(content: &str, editing: bool, endpoint: Endpoint) -> TextArea<'static> {
    let mut lines: Vec<String> = content.lines().map(ToString::to_string).collect();
    if lines.is_empty() {
        lines.push(String::new());
    }

    let mut textarea = TextArea::new(lines);
    let title = if editing {
        format!(
            "Request JSON [{}] (Ctrl+S save, Ctrl+R fetch, Esc discard)",
            endpoint.path()
        )
    } else {
        format!("Request JSON [{}] (press e to edit)", endpoint.path())
    };

    textarea.set_block(Block::default().borders(Borders::ALL).title(title));
    textarea.set_line_number_style(Style::default().fg(Color::DarkGray));
    textarea
}

fn build_single_line_editor(title: &str) -> TextArea<'static> {
    let mut textarea = TextArea::new(vec![String::new()]);
    textarea.set_block(
        Block::default()
            .borders(Borders::ALL)
            .title(title.to_string()),
    );
    textarea.set_line_number_style(Style::default().fg(Color::DarkGray));
    textarea
}

fn categories_for_variant(variant: FeedVariant) -> Vec<String> {
    let mut set = HashSet::new();
    let mut categories = vec!["All".to_string()];
    for source in feed_sources_for_variant(variant) {
        if set.insert(source.category) {
            categories.push(source.category.to_string());
        }
    }
    categories
}

fn format_payload(endpoint: Endpoint, payload: Value) -> Result<Vec<String>> {
    match endpoint {
        Endpoint::SeismologyEarthquakes => format_earthquakes(payload),
        Endpoint::UnrestEvents => format_unrest(payload),
        Endpoint::InfrastructureStatuses => format_service_status(payload),
        Endpoint::MarketCryptoQuotes => format_crypto_quotes(payload),
    }
}

fn compact_enum_label(value: &str) -> &str {
    value.rsplit('_').next().unwrap_or(value)
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "SEVERITY_LEVEL_HIGH" => 0,
        "SEVERITY_LEVEL_MEDIUM" => 1,
        "SEVERITY_LEVEL_LOW" => 2,
        _ => 3,
    }
}

fn service_status_rank(value: &str) -> u8 {
    match value {
        "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE" => 0,
        "SERVICE_OPERATIONAL_STATUS_PARTIAL_OUTAGE" => 1,
        "SERVICE_OPERATIONAL_STATUS_DEGRADED" => 2,
        "SERVICE_OPERATIONAL_STATUS_MAINTENANCE" => 3,
        "SERVICE_OPERATIONAL_STATUS_OPERATIONAL" => 4,
        _ => 5,
    }
}

#[derive(Debug, Deserialize)]
struct EarthquakesResponse {
    #[serde(default)]
    earthquakes: Vec<Earthquake>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Earthquake {
    #[serde(default)]
    place: String,
    #[serde(default)]
    magnitude: f64,
    #[serde(default)]
    depth_km: f64,
}

fn format_earthquakes(payload: Value) -> Result<Vec<String>> {
    let response: EarthquakesResponse =
        serde_json::from_value(payload).context("failed to decode seismology response")?;
    if response.earthquakes.is_empty() {
        return Ok(vec!["No earthquake records returned.".to_string()]);
    }

    let mut lines = vec![
        format!("Total earthquakes: {}", response.earthquakes.len()),
        String::new(),
    ];

    for quake in response.earthquakes.iter().take(30) {
        lines.push(format!(
            "M{:>4.1} | {:>6.1} km | {}",
            quake.magnitude, quake.depth_km, quake.place
        ));
    }
    Ok(lines)
}

#[derive(Debug, Deserialize)]
struct UnrestResponse {
    #[serde(default)]
    events: Vec<UnrestEvent>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct UnrestEvent {
    #[serde(default)]
    title: String,
    #[serde(default)]
    country: String,
    #[serde(default)]
    severity: String,
}

fn format_unrest(payload: Value) -> Result<Vec<String>> {
    let mut response: UnrestResponse =
        serde_json::from_value(payload).context("failed to decode unrest response")?;
    if response.events.is_empty() {
        return Ok(vec!["No unrest events returned.".to_string()]);
    }

    response
        .events
        .sort_by_key(|event| severity_rank(event.severity.as_str()));

    let mut lines = vec![
        format!("Total unrest events: {}", response.events.len()),
        String::new(),
    ];
    for event in response.events.iter().take(30) {
        lines.push(format!(
            "{:<6} | {:<20} | {}",
            compact_enum_label(&event.severity),
            event.country,
            event.title
        ));
    }
    Ok(lines)
}

#[derive(Debug, Deserialize)]
struct ServiceStatusResponse {
    #[serde(default)]
    statuses: Vec<ServiceStatus>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ServiceStatus {
    #[serde(default)]
    name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    latency_ms: i64,
}

fn format_service_status(payload: Value) -> Result<Vec<String>> {
    let mut response: ServiceStatusResponse =
        serde_json::from_value(payload).context("failed to decode infrastructure response")?;
    if response.statuses.is_empty() {
        return Ok(vec!["No service statuses returned.".to_string()]);
    }

    response
        .statuses
        .sort_by_key(|item| service_status_rank(item.status.as_str()));

    let mut lines = vec![
        format!("Total services checked: {}", response.statuses.len()),
        String::new(),
    ];
    for status in response.statuses.iter().take(40) {
        lines.push(format!(
            "{:<12} | {:<15} | {} ms",
            compact_enum_label(&status.status),
            status.name,
            status.latency_ms
        ));
    }
    Ok(lines)
}

#[derive(Debug, Deserialize)]
struct CryptoQuotesResponse {
    #[serde(default)]
    quotes: Vec<CryptoQuote>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CryptoQuote {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    price: f64,
    #[serde(default)]
    change: f64,
}

fn format_crypto_quotes(payload: Value) -> Result<Vec<String>> {
    let response: CryptoQuotesResponse =
        serde_json::from_value(payload).context("failed to decode market response")?;
    if response.quotes.is_empty() {
        return Ok(vec!["No crypto quotes returned.".to_string()]);
    }

    let mut lines = vec![
        format!("Total crypto quotes: {}", response.quotes.len()),
        String::new(),
    ];
    for quote in response.quotes.iter().take(25) {
        lines.push(format!(
            "{:<6} | ${:<12.4} | {:+6.2}%",
            quote.symbol.to_uppercase(),
            quote.price,
            quote.change
        ));
    }
    Ok(lines)
}

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn parse_chatjimmy_payload(body: &str) -> Result<Value> {
    let primary = body.split("<|stats|>").next().unwrap_or(body).trim();
    if !primary.is_empty() {
        if let Ok(value) = serde_json::from_str::<Value>(primary) {
            return Ok(value);
        }
    }

    if let Some(object) = extract_first_json_object(body) {
        return serde_json::from_str::<Value>(object)
            .context("failed to parse extracted ChatJimmy JSON object");
    }

    Err(anyhow!(
        "ChatJimmy response did not contain a valid JSON object: {}",
        truncate_for_error(body, 240)
    ))
}

fn extract_first_json_object(input: &str) -> Option<&str> {
    let bytes = input.as_bytes();
    let mut start_index: Option<usize> = None;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;

    for (index, byte) in bytes.iter().enumerate() {
        if start_index.is_none() {
            if *byte == b'{' {
                start_index = Some(index);
                depth = 1;
            }
            continue;
        }

        if in_string {
            if escape {
                escape = false;
                continue;
            }
            if *byte == b'\\' {
                escape = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match *byte {
            b'"' => in_string = true,
            b'{' => depth = depth.saturating_add(1),
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0
                    && let Some(start) = start_index
                {
                    return input.get(start..=index);
                }
            }
            _ => {}
        }
    }

    None
}

fn render_source_text_block(evidence: &[BriefSourceEvidence]) -> String {
    evidence
        .iter()
        .map(|item| format!("[{}] {}", item.id, item.text))
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_source_ids_from_text(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut ids = Vec::new();
    let mut index = 0usize;
    while index + 3 < bytes.len() {
        if bytes[index] == b'[' && bytes[index + 1] == b'S' {
            let mut cursor = index + 2;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor > index + 2 && cursor < bytes.len() && bytes[cursor] == b']' {
                if let Some(value) = text.get(index + 1..cursor) {
                    ids.push(value.to_string());
                }
                index = cursor + 1;
                continue;
            }
        }
        index += 1;
    }
    ids
}

fn brief_is_grounded(
    brief: &str,
    used_source_ids: &[String],
    evidence: &[BriefSourceEvidence],
) -> bool {
    if brief.trim().is_empty() {
        return false;
    }

    let allowed_ids = evidence
        .iter()
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    if allowed_ids.is_empty() {
        return false;
    }

    let cited_ids = extract_source_ids_from_text(brief);
    if cited_ids.is_empty() {
        return false;
    }
    if cited_ids
        .iter()
        .any(|id| !allowed_ids.contains(id.as_str()))
    {
        return false;
    }
    if used_source_ids
        .iter()
        .any(|id| !allowed_ids.contains(id.as_str()))
    {
        return false;
    }

    let source_corpus = evidence
        .iter()
        .map(|item| item.text.to_lowercase())
        .collect::<Vec<_>>()
        .join(" ");

    // Require every non-empty line to carry at least one citation marker.
    let mut non_empty_lines = 0usize;
    for line in brief.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        non_empty_lines += 1;
        if !trimmed.contains("[S") {
            return false;
        }

        // Strict overlap check: line content must mostly use tokens present in source text.
        let content = strip_source_citations(trimmed).to_lowercase();
        let tokens = content
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter(|token| token.len() >= 4)
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            return false;
        }
        let matched = tokens
            .iter()
            .filter(|token| source_corpus.contains(**token))
            .count();
        if matched * 2 < tokens.len() {
            return false;
        }
    }

    non_empty_lines > 0
}

fn strip_source_citations(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut index = 0usize;
    let bytes = line.as_bytes();
    while index < bytes.len() {
        if bytes[index] == b'[' && index + 2 < bytes.len() && bytes[index + 1] == b'S' {
            let mut cursor = index + 2;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b']' {
                index = cursor + 1;
                continue;
            }
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

fn render_grounded_fallback_brief(
    country_code: &str,
    country_name: &str,
    evidence: &[BriefSourceEvidence],
) -> String {
    if evidence.is_empty() {
        return format!(
            "- Insufficient source evidence for {} ({}). [S0]",
            country_name, country_code
        );
    }

    let mut lines = Vec::new();
    lines.push(format!(
        "- Grounded summary for {} ({}), limited to provided sources. [{}]",
        country_name, country_code, evidence[0].id
    ));
    for item in evidence.iter().take(7) {
        lines.push(format!(
            "- {} [{}]",
            truncate_for_error(item.text.as_str(), 180),
            item.id
        ));
    }
    lines.join("\n")
}

fn strip_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    for c in input.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    decode_basic_entities(out.trim())
}

fn decode_basic_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn normalize_title_for_dedup(title: &str) -> String {
    title
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn contains_keyword_word(haystack: &str, keyword: &str) -> bool {
    let hay = haystack.to_lowercase();
    let needle = keyword.to_lowercase();
    if needle.is_empty() {
        return false;
    }

    let mut search_from = 0usize;
    while let Some(rel_pos) = hay[search_from..].find(&needle) {
        let start = search_from + rel_pos;
        let end = start + needle.len();
        let prev_ok = if start == 0 {
            true
        } else {
            !hay[..start]
                .chars()
                .next_back()
                .unwrap_or(' ')
                .is_ascii_alphanumeric()
        };
        let next_ok = if end >= hay.len() {
            true
        } else {
            !hay[end..]
                .chars()
                .next()
                .unwrap_or(' ')
                .is_ascii_alphanumeric()
        };
        if prev_ok && next_ok {
            return true;
        }
        search_from = end;
    }

    false
}

fn format_age(ts_ms: i64) -> String {
    if ts_ms <= 0 {
        return "unknown".to_string();
    }
    let delta_sec = (now_epoch_ms().saturating_sub(ts_ms) / 1000).max(0);
    if delta_sec < 60 {
        return format!("{}s", delta_sec);
    }
    if delta_sec < 3600 {
        return format!("{}m", delta_sec / 60);
    }
    if delta_sec < 86_400 {
        return format!("{}h", delta_sec / 3600);
    }
    format!("{}d", delta_sec / 86_400)
}

fn spinner_symbol() -> &'static str {
    match (now_epoch_ms().div_euclid(220)).rem_euclid(4) {
        0 => "⠋",
        1 => "⠙",
        2 => "⠹",
        _ => "⠸",
    }
}

fn sanitize_filename_part(input: &str) -> String {
    let clean = input
        .to_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    clean
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn export_brief_snapshot(
    snapshot: &BriefSnapshot,
    related_rss: &[RssItem],
) -> Result<(PathBuf, PathBuf)> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = manifest_dir.join("exports").join("briefs");
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let stamp = now_epoch_ms();
    let country_code = if snapshot.country_code.is_empty() {
        "xx".to_string()
    } else {
        snapshot.country_code.to_lowercase()
    };
    let slug = sanitize_filename_part(snapshot.country_name.as_str());
    let base = if slug.is_empty() {
        format!("brief-{}-{}", country_code, stamp)
    } else {
        format!("brief-{}-{}-{}", country_code, slug, stamp)
    };

    let json_path = output_dir.join(format!("{}.json", base));
    let txt_path = output_dir.join(format!("{}.txt", base));

    let related_json = related_rss
        .iter()
        .map(|item| {
            json!({
                "title": item.title,
                "source": item.source_name,
                "category": item.category,
                "publishedTsMs": item.published_ts_ms,
                "summary": item.summary,
                "link": item.link,
                "keywordHits": item.keyword_hits,
            })
        })
        .collect::<Vec<_>>();

    let payload = json!({
        "exportedAtMs": stamp,
        "countryCode": snapshot.country_code,
        "countryName": snapshot.country_name,
        "intelBrief": snapshot.intel_brief,
        "intelModel": snapshot.intel_model,
        "intelGeneratedAtMs": snapshot.intel_generated_at,
        "ciiScore": snapshot.cii_score,
        "ciiTrend": snapshot.cii_trend,
        "strategicLevel": snapshot.strategic_level,
        "stock": {
            "available": snapshot.stock_available,
            "indexName": snapshot.stock_index_name,
            "price": snapshot.stock_price,
            "weekChangePercent": snapshot.stock_week_change,
            "currency": snapshot.stock_currency,
        },
        "warnings": snapshot.errors,
        "relatedRss": related_json
    });
    fs::write(
        &json_path,
        serde_json::to_string_pretty(&payload).context("failed to serialize brief export JSON")?,
    )
    .with_context(|| format!("failed to write {}", json_path.display()))?;

    let mut text_lines = vec![
        format!(
            "WorldMonitor Brief Export | {} ({})",
            snapshot.country_name, snapshot.country_code
        ),
        format!("Exported at (ms): {}", stamp),
        String::new(),
        format!(
            "CII: {} | Trend: {} | Strategic: {}",
            snapshot
                .cii_score
                .map(|score| format!("{:.1}/100", score))
                .unwrap_or_else(|| "n/a".to_string()),
            compact_enum_label(snapshot.cii_trend.as_str()),
            compact_enum_label(snapshot.strategic_level.as_str())
        ),
        format!(
            "Stock: {}",
            if snapshot.stock_available {
                format!(
                    "{} {:.2} {} ({:+.2}% 1W)",
                    snapshot.stock_index_name,
                    snapshot.stock_price,
                    snapshot.stock_currency,
                    snapshot.stock_week_change
                )
            } else {
                "Unavailable".to_string()
            }
        ),
        String::new(),
        "Intel Brief:".to_string(),
        snapshot.intel_brief.clone(),
        String::new(),
    ];

    if snapshot.errors.is_empty() {
        text_lines.push("Warnings: none".to_string());
    } else {
        text_lines.push("Warnings:".to_string());
        text_lines.extend(snapshot.errors.iter().map(|err| format!("- {}", err)));
    }
    text_lines.push(String::new());
    text_lines.push(format!("Related RSS headlines: {}", related_rss.len()));
    for (index, item) in related_rss.iter().take(25).enumerate() {
        text_lines.push(format!(
            "{}. [{}] {} | {}",
            index + 1,
            item.source_name,
            item.title,
            item.link
        ));
    }

    fs::write(&txt_path, text_lines.join("\n"))
        .with_context(|| format!("failed to write {}", txt_path.display()))?;

    Ok((json_path, txt_path))
}

fn build_item_from_entry(
    source: FeedSource,
    entry: &feed_rs::model::Entry,
    keywords: &[String],
) -> Option<RssItem> {
    let title = strip_html(entry.title.as_ref()?.content.trim());
    if title.is_empty() {
        return None;
    }

    let link = entry
        .links
        .first()
        .map(|link| link.href.clone())
        .unwrap_or_default();

    let summary = entry
        .summary
        .as_ref()
        .map(|summary| strip_html(summary.content.trim()))
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|content| content.body.as_ref())
                .map(|body| strip_html(body))
        })
        .unwrap_or_default();

    let published_ts_ms = entry
        .published
        .or(entry.updated)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(now_epoch_ms);

    let body = format!("{title} {summary}");
    let keyword_hits = keywords
        .iter()
        .filter(|keyword| contains_keyword_word(&body, keyword))
        .cloned()
        .collect::<Vec<_>>();

    Some(RssItem {
        id: format!("{}:{}", source.id, entry.id),
        title,
        summary,
        link,
        source_name: source.name.to_string(),
        category: source.category.to_string(),
        published_ts_ms,
        keyword_hits,
    })
}

fn fetch_rss_snapshot(
    client: Client,
    variant: FeedVariant,
    previous_health: HashMap<String, FeedHealth>,
    keywords: Vec<String>,
) -> RssFetchResult {
    let started = Instant::now();
    let now = Instant::now();
    let mut health = previous_health;
    let mut items = Vec::new();
    let mut fetched_feeds = 0usize;
    let mut skipped_cooldown = 0usize;
    let mut failed_feeds = 0usize;

    for source in feed_sources_for_variant(variant) {
        let entry = health.entry(source.id.to_string()).or_default();
        if let Some(until) = entry.cooldown_until
            && until > now
        {
            skipped_cooldown += 1;
            continue;
        }

        let response = client
            .get(source.url)
            .header(
                "Accept",
                "application/rss+xml, application/atom+xml, text/xml, application/xml, */*",
            )
            .header("User-Agent", "worldmonitor-v2-rust/0.1")
            .send();

        let bytes = match response {
            Ok(resp) if resp.status().is_success() => match resp.bytes() {
                Ok(bytes) => bytes,
                Err(err) => {
                    entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                    entry.last_error = Some(format!("Body read failed: {err}"));
                    failed_feeds += 1;
                    continue;
                }
            },
            Ok(resp) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_error = Some(format!("HTTP {}", resp.status().as_u16()));
                failed_feeds += 1;
                continue;
            }
            Err(err) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_error = Some(err.to_string());
                failed_feeds += 1;
                continue;
            }
        };

        let feed = match parser::parse(&bytes[..]) {
            Ok(feed) => feed,
            Err(err) => {
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_error = Some(format!("Parse error: {err}"));
                failed_feeds += 1;
                continue;
            }
        };

        entry.consecutive_failures = 0;
        entry.cooldown_until = None;
        entry.last_error = None;
        entry.last_success = Some(Instant::now());
        fetched_feeds += 1;

        for entry in &feed.entries {
            if let Some(item) = build_item_from_entry(*source, entry, &keywords) {
                items.push(item);
            }
        }
    }

    for state in health.values_mut() {
        if state.consecutive_failures >= 3 {
            state.cooldown_until = Some(Instant::now() + Duration::from_secs(300));
        }
    }

    let mut seen = HashSet::new();
    items.retain(|item| seen.insert(normalize_title_for_dedup(&item.title)));
    items.sort_by(|a, b| b.published_ts_ms.cmp(&a.published_ts_ms));

    let mut keyword_counts = HashMap::new();
    for keyword in &keywords {
        let count = items
            .iter()
            .filter(|item| item.keyword_hits.iter().any(|hit| hit == keyword))
            .count();
        keyword_counts.insert(keyword.to_lowercase(), count);
    }

    RssFetchResult {
        items,
        updated_health: health,
        fetched_feeds,
        skipped_cooldown,
        failed_feeds,
        duration_ms: started.elapsed().as_millis(),
        keyword_counts,
    }
}

fn apply_intel_brief(snapshot: &mut BriefSnapshot, intel: CountryIntelBriefResponse) {
    if !intel.country_code.is_empty() {
        snapshot.country_code = intel.country_code.to_uppercase();
    }
    if !intel.country_name.is_empty() {
        snapshot.country_name = intel.country_name;
    }
    snapshot.intel_brief = intel.brief;
    snapshot.intel_model = intel.model;
    snapshot.intel_generated_at = intel.generated_at;
}

fn fetch_country_brief_snapshot(
    api: &ApiClient,
    country_code: &str,
    rss_source_snippets: Vec<String>,
) -> BriefSnapshot {
    let normalized_code = country_code.to_uppercase();
    let mut snapshot = BriefSnapshot {
        country_code: normalized_code.clone(),
        country_name: country_name_from_code(&normalized_code),
        cii_trend: "TREND_DIRECTION_UNSPECIFIED".to_string(),
        strategic_level: "SEVERITY_LEVEL_UNSPECIFIED".to_string(),
        ..BriefSnapshot::default()
    };
    let mut evidence = Vec::<BriefSourceEvidence>::new();
    let mut next_source_id = 1usize;

    for snippet in rss_source_snippets {
        let trimmed = snippet.trim();
        if trimmed.is_empty() {
            continue;
        }
        evidence.push(BriefSourceEvidence {
            id: format!("S{}", next_source_id),
            text: truncate_for_error(trimmed, 300),
        });
        next_source_id += 1;
    }

    match api.get_risk_scores(snapshot.country_code.as_str()) {
        Ok(risk) => {
            if let Some(cii) = risk.cii_scores.iter().find(|score| {
                score
                    .region
                    .eq_ignore_ascii_case(snapshot.country_code.as_str())
            }) {
                snapshot.cii_score = Some(cii.combined_score);
                if !cii.trend.is_empty() {
                    snapshot.cii_trend = cii.trend.clone();
                }
                evidence.push(BriefSourceEvidence {
                    id: format!("S{}", next_source_id),
                    text: format!(
                        "Risk score source: region={} combined_score={:.1} trend={}.",
                        cii.region,
                        cii.combined_score,
                        compact_enum_label(cii.trend.as_str())
                    ),
                });
                next_source_id += 1;
            } else {
                snapshot
                    .errors
                    .push(format!("No CII score found for {}", snapshot.country_code));
            }

            if let Some(strategic) = risk
                .strategic_risks
                .iter()
                .find(|risk| {
                    risk.region
                        .eq_ignore_ascii_case(snapshot.country_code.as_str())
                })
                .or_else(|| {
                    risk.strategic_risks
                        .iter()
                        .find(|risk| risk.region.eq_ignore_ascii_case("global"))
                })
            {
                if !strategic.level.is_empty() {
                    snapshot.strategic_level = strategic.level.clone();
                }
                evidence.push(BriefSourceEvidence {
                    id: format!("S{}", next_source_id),
                    text: format!(
                        "Strategic risk source: region={} level={}.",
                        strategic.region,
                        compact_enum_label(strategic.level.as_str())
                    ),
                });
                next_source_id += 1;
            }
        }
        Err(error) => snapshot.errors.push(format!(
            "Risk score request failed: {}",
            format_error_chain(&error, 280)
        )),
    }

    match api.get_country_stock_index(snapshot.country_code.as_str()) {
        Ok(stock) => {
            snapshot.stock_available = stock.available;
            snapshot.stock_index_name = stock.index_name.clone();
            snapshot.stock_price = stock.price;
            snapshot.stock_week_change = stock.week_change_percent;
            snapshot.stock_currency = stock.currency.clone();
            if !stock.code.is_empty() && stock.code != snapshot.country_code {
                snapshot.errors.push(format!(
                    "Stock endpoint returned mismatched code {}",
                    stock.code
                ));
            }
            if snapshot.stock_available {
                evidence.push(BriefSourceEvidence {
                    id: format!("S{}", next_source_id),
                    text: format!(
                        "Stock source: {} {} {:.2} {} ({:+.2}% 1W).",
                        stock.code,
                        stock.index_name,
                        stock.price,
                        stock.currency,
                        stock.week_change_percent
                    ),
                });
            } else {
                snapshot.errors.push(format!(
                    "No mapped stock index for {}",
                    snapshot.country_code
                ));
            }
        }
        Err(error) => snapshot.errors.push(format!(
            "Stock index request failed: {}",
            format_error_chain(&error, 280)
        )),
    }

    match api.brief_provider {
        BriefIntelProvider::Server => match api.get_country_intel_brief(&normalized_code) {
            Ok(intel) => {
                apply_intel_brief(&mut snapshot, intel);
                if snapshot.intel_brief.trim().is_empty() {
                    snapshot.errors.push(
                        "Intel brief unavailable from server provider (empty result).".to_string(),
                    );
                }
            }
            Err(error) => snapshot.errors.push(format!(
                "Intel brief request failed (server): {}",
                format_error_chain(&error, 280)
            )),
        },
        BriefIntelProvider::Chatjimmy => {
            match api.get_country_intel_brief_chatjimmy(&normalized_code, &evidence) {
                Ok(intel) => apply_intel_brief(&mut snapshot, intel),
                Err(error) => snapshot.errors.push(format!(
                    "Intel brief request failed (chatjimmy): {}",
                    format_error_chain(&error, 280)
                )),
            }
        }
        BriefIntelProvider::Auto => {
            let mut should_try_chatjimmy = false;
            match api.get_country_intel_brief(&normalized_code) {
                Ok(intel) => {
                    apply_intel_brief(&mut snapshot, intel);
                    if snapshot.intel_brief.trim().is_empty() {
                        should_try_chatjimmy = true;
                        snapshot.errors.push(
                            "Server intel provider returned empty brief; trying ChatJimmy fallback."
                                .to_string(),
                        );
                    }
                }
                Err(error) => {
                    should_try_chatjimmy = true;
                    snapshot.errors.push(format!(
                        "Server intel provider failed: {}",
                        format_error_chain(&error, 220)
                    ));
                }
            }

            if should_try_chatjimmy {
                match api.get_country_intel_brief_chatjimmy(&normalized_code, &evidence) {
                    Ok(intel) => apply_intel_brief(&mut snapshot, intel),
                    Err(error) => snapshot.errors.push(format!(
                        "ChatJimmy fallback failed: {}",
                        format_error_chain(&error, 280)
                    )),
                }
            }
        }
    }

    if snapshot.intel_brief.trim().is_empty() {
        snapshot
            .errors
            .push("No intel brief provider returned narrative text.".to_string());
    }

    snapshot
}

fn start_api_fetch(app: &mut App, api: &ApiClient, sender: &Sender<WorkerEvent>) {
    if app.in_flight {
        return;
    }

    let endpoint = app.selected_endpoint();
    let request_body = match app.request_body_json(endpoint) {
        Ok(body) => body,
        Err(err) => {
            app.status_line = "Invalid request body JSON".to_string();
            app.output_lines = vec![
                format!("Request body for {} could not be parsed.", endpoint),
                String::new(),
                err.to_string(),
                String::new(),
                "Press e to edit and Ctrl+S to save valid JSON.".to_string(),
            ];
            return;
        }
    };
    app.in_flight = true;
    app.status_line = format!("Loading {}", endpoint);

    let api = api.clone();
    let sender = sender.clone();
    thread::spawn(move || {
        let result = api
            .fetch_json(endpoint, &request_body)
            .and_then(|payload| format_payload(endpoint, payload));
        let event = match result {
            Ok(lines) => WorkerEvent::ApiSuccess { endpoint, lines },
            Err(error) => WorkerEvent::ApiFailure {
                endpoint,
                error: error.to_string(),
            },
        };
        let _ = sender.send(event);
    });
}

fn start_rss_fetch(app: &mut App, api: &ApiClient, sender: &Sender<WorkerEvent>) {
    if app.rss_in_flight {
        return;
    }

    app.rss_in_flight = true;
    app.status_line = format!("Refreshing RSS feeds for {} variant", app.rss_variant);

    let client = api.client.clone();
    let variant = app.rss_variant;
    let previous_health = app.rss_feed_health.clone();
    let keywords = app.rss_keywords.clone();
    let sender = sender.clone();

    thread::spawn(move || {
        let result = fetch_rss_snapshot(client, variant, previous_health, keywords);
        let _ = sender.send(WorkerEvent::RssSuccess(result));
    });
}

fn start_brief_fetch(app: &mut App, api: &ApiClient, sender: &Sender<WorkerEvent>) {
    if app.brief_in_flight {
        return;
    }

    let country_code = app.brief_country_code.trim().to_uppercase();
    if !is_valid_country_code(&country_code) {
        app.status_line = "Invalid country code (use ISO-2 like US, GB, JP)".to_string();
        return;
    }

    app.set_brief_country_code(country_code.clone());
    app.brief_in_flight = true;
    app.status_line = format!(
        "Generating country brief for {} via {}",
        country_code,
        api.brief_provider_label()
    );

    let rss_source_snippets = app.collect_brief_rss_source_snippets(country_code.as_str(), 10);
    let api = api.clone();
    let sender = sender.clone();
    thread::spawn(move || {
        let started = Instant::now();
        let snapshot =
            fetch_country_brief_snapshot(&api, country_code.as_str(), rss_source_snippets);
        let _ = sender.send(WorkerEvent::BriefSuccess {
            snapshot,
            duration_ms: started.elapsed().as_millis(),
        });
    });
}

fn apply_worker_events(app: &mut App, receiver: &Receiver<WorkerEvent>) {
    while let Ok(event) = receiver.try_recv() {
        match event {
            WorkerEvent::ApiSuccess { endpoint, lines } => {
                app.output_lines = lines;
                app.status_line = format!("Loaded {}", endpoint);
                app.in_flight = false;
                app.scroll = 0;
                app.last_fetch_finished_at = Some(Instant::now());
            }
            WorkerEvent::ApiFailure { endpoint, error } => {
                app.output_lines = vec![
                    format!("Request failed for {}", endpoint),
                    String::new(),
                    error,
                    String::new(),
                    "Tip: run against local API (`--base-url http://127.0.0.1:3000`)".to_string(),
                    "or provide `--api-key` for protected remote hosts.".to_string(),
                ];
                app.status_line = "Request failed".to_string();
                app.in_flight = false;
                app.scroll = 0;
                app.last_fetch_finished_at = Some(Instant::now());
            }
            WorkerEvent::RssSuccess(result) => {
                app.rss_items = result.items;
                app.rss_feed_health = result.updated_health;
                app.rss_last_fetch_finished_at = Some(Instant::now());
                app.rss_in_flight = false;
                app.apply_rss_filters();

                let mut spikes = Vec::new();
                for (keyword, count) in &result.keyword_counts {
                    let baseline = app
                        .rss_keyword_baseline
                        .entry(keyword.clone())
                        .or_insert(*count as f64);
                    if *count >= 3 && (*count as f64) >= (*baseline * 2.0) && *baseline > 0.0 {
                        spikes.push(format!("{keyword}: {} -> {}", *baseline as usize, count));
                    }
                    *baseline = (*baseline * 0.7) + ((*count as f64) * 0.3);
                }
                app.rss_spikes = spikes;
                app.rss_last_fetch_summary = format!(
                    "{} feeds ok | {} failed | {} cooldown | {} items | {}ms",
                    result.fetched_feeds,
                    result.failed_feeds,
                    result.skipped_cooldown,
                    app.rss_items.len(),
                    result.duration_ms
                );
                app.status_line = format!(
                    "RSS updated: {} headlines for {}",
                    app.rss_items.len(),
                    app.rss_variant
                );
                app.refresh_brief_related_rss();
            }
            WorkerEvent::BriefSuccess {
                mut snapshot,
                duration_ms,
            } => {
                if snapshot.country_name.is_empty() {
                    snapshot.country_name = country_name_from_code(snapshot.country_code.as_str());
                }
                app.brief_in_flight = false;
                app.brief_last_fetch_finished_at = Some(Instant::now());
                app.set_brief_country_code(snapshot.country_code.clone());
                app.brief_snapshot = Some(snapshot.clone());
                app.refresh_brief_related_rss();
                let all_core_sections_missing = snapshot.intel_brief.trim().is_empty()
                    && snapshot.cii_score.is_none()
                    && !snapshot.stock_available;
                let intel_source = if snapshot.intel_model.trim().is_empty() {
                    "unknown".to_string()
                } else {
                    snapshot.intel_model.clone()
                };
                app.brief_last_fetch_summary = format!(
                    "{} | {} | {} warnings | {}ms",
                    snapshot.country_code,
                    intel_source,
                    snapshot.errors.len(),
                    duration_ms
                );
                app.status_line = if snapshot.errors.is_empty() {
                    format!("BRIEF updated for {}", snapshot.country_code)
                } else if all_core_sections_missing {
                    format!(
                        "BRIEF unavailable at current base URL for {} (check --base-url / --api-key)",
                        snapshot.country_code
                    )
                } else {
                    format!(
                        "BRIEF updated for {} with {} warnings",
                        snapshot.country_code,
                        snapshot.errors.len()
                    )
                };
            }
        }
    }
}

fn handle_editor_key(app: &mut App, key: KeyEvent, api: &ApiClient, sender: &Sender<WorkerEvent>) {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('s') => {
                if let Err(err) = app.save_request_editor() {
                    app.status_line = format!("Save failed: {}", err);
                }
            }
            KeyCode::Char('r') => {
                if let Err(err) = app.save_request_editor() {
                    app.status_line = format!("Save failed: {}", err);
                    return;
                }
                app.editing_request = false;
                app.ensure_editor_synced();
                start_api_fetch(app, api, sender);
            }
            KeyCode::Char('t') => app.reset_editor_to_template(),
            _ => {
                let _ = app.request_editor.input(TextInput::from(key));
            }
        }
        return;
    }

    match key.code {
        KeyCode::Esc => app.discard_request_editor(),
        _ => {
            let _ = app.request_editor.input(TextInput::from(key));
            app.status_line = "Editing request JSON".to_string();
        }
    }
}

fn handle_rss_input_key(
    app: &mut App,
    key: KeyEvent,
    api: &ApiClient,
    sender: &Sender<WorkerEvent>,
) {
    match app.rss_input_mode {
        RssInputMode::Search => match key.code {
            KeyCode::Esc => {
                app.rss_input_mode = RssInputMode::None;
                app.status_line = "Cancelled RSS search edit".to_string();
            }
            KeyCode::Enter => {
                app.rss_query = app.rss_search_editor.lines().join("").trim().to_string();
                app.rss_input_mode = RssInputMode::None;
                app.apply_rss_filters();
                app.status_line = if app.rss_query.is_empty() {
                    "Cleared RSS query".to_string()
                } else {
                    format!("Applied RSS query: {}", app.rss_query)
                };
            }
            _ => {
                let _ = app.rss_search_editor.input(TextInput::from(key));
            }
        },
        RssInputMode::Keywords => match key.code {
            KeyCode::Esc => {
                app.rss_input_mode = RssInputMode::None;
                app.status_line = "Cancelled keyword edit".to_string();
            }
            KeyCode::Enter => {
                let raw = app.rss_keywords_editor.lines().join("");
                app.rss_keywords = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|keyword| !keyword.is_empty())
                    .map(|keyword| keyword.to_lowercase())
                    .collect::<Vec<_>>();
                app.rss_input_mode = RssInputMode::None;
                app.status_line = format!("Updated {} keyword monitors", app.rss_keywords.len());
                start_rss_fetch(app, api, sender);
            }
            _ => {
                let _ = app.rss_keywords_editor.input(TextInput::from(key));
            }
        },
        RssInputMode::None => {}
    }
}

fn handle_brief_input_key(
    app: &mut App,
    key: KeyEvent,
    api: &ApiClient,
    sender: &Sender<WorkerEvent>,
) {
    match app.brief_input_mode {
        BriefInputMode::CountryCode => match key.code {
            KeyCode::Esc => {
                app.brief_input_mode = BriefInputMode::None;
                app.status_line = "Cancelled BRIEF country edit".to_string();
            }
            KeyCode::Enter => {
                let code = app
                    .brief_country_editor
                    .lines()
                    .join("")
                    .trim()
                    .to_uppercase();
                if !is_valid_country_code(code.as_str()) {
                    app.status_line = "Invalid country code (must be two letters)".to_string();
                    return;
                }
                app.set_brief_country_code(code);
                app.brief_input_mode = BriefInputMode::None;
                start_brief_fetch(app, api, sender);
            }
            _ => {
                let _ = app.brief_country_editor.input(TextInput::from(key));
            }
        },
        BriefInputMode::None => {}
    }
}

fn workspace_tab(active: bool, label: &str, hotkey: &str) -> Span<'static> {
    let content = if active {
        format!(" [{}:{}] ", hotkey, label)
    } else {
        format!("  {}:{}  ", hotkey, label)
    };
    if active {
        Span::styled(
            content,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(content, Style::default().fg(Color::DarkGray))
    }
}

fn draw_ui(frame: &mut ratatui::Frame<'_>, app: &App) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(area);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "Workspaces ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            workspace_tab(app.view == AppView::Api, "API", "1"),
            workspace_tab(app.view == AppView::Rss, "RSS", "2"),
            workspace_tab(app.view == AppView::Brief, "BRIEF", "3"),
            workspace_tab(app.view == AppView::Settings, "SETTINGS", "4"),
            Span::styled("  Tab cycle", Style::default().fg(Color::DarkGray)),
        ])),
        vertical[0],
    );

    match app.view {
        AppView::Api => draw_api_workspace(frame, vertical[1], app),
        AppView::Rss => draw_rss_workspace(frame, vertical[1], app),
        AppView::Brief => draw_brief_workspace(frame, vertical[1], app),
        AppView::Settings => draw_settings_workspace(frame, vertical[1], app),
    }

    let footer_style = if app.in_flight || app.rss_in_flight || app.brief_in_flight {
        Style::default().fg(Color::Yellow)
    } else if app.editing_request || app.rss_editor_active() || app.brief_editor_active() {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::Green)
    };

    let controls = match app.view {
        AppView::Api => {
            if app.editing_request {
                "API EDIT | Ctrl+S save | Ctrl+R save+fetch | Ctrl+T reset | Esc discard"
            } else {
                "API | Up/Down endpoint | Enter/r fetch | e edit | t reset | a auto | Tab cycle | q quit"
            }
        }
        AppView::Rss => {
            if app.rss_editor_active() {
                "RSS INPUT | Enter apply | Esc cancel"
            } else {
                "RSS | v variant | ←/→ category | ↑/↓ headlines | u/d detail | / search | m keywords | f refresh | a auto | Tab cycle | q quit"
            }
        }
        AppView::Brief => {
            if app.brief_editor_active() {
                "BRIEF INPUT | Enter apply | Esc cancel"
            } else {
                "BRIEF | n/p country | c edit code | Enter/r/f refresh | x export | j/k brief scroll | ↑/↓ RSS | u/d RSS detail | a auto | Tab cycle | q quit"
            }
        }
        AppView::Settings => {
            "SETTINGS | ↑/↓ select check | u/d detail scroll | g rescan keys | a auto | Tab cycle | q quit"
        }
    };

    let footer = Paragraph::new(format!(
        "{} | {} | {}",
        app.status_line,
        app.auto_refresh_summary(),
        controls
    ))
    .style(footer_style)
    .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, vertical[2]);
}

fn age_color(ts_ms: i64) -> Color {
    if ts_ms <= 0 {
        return Color::DarkGray;
    }
    let delta_sec = (now_epoch_ms().saturating_sub(ts_ms) / 1000).max(0);
    if delta_sec <= 20 * 60 {
        Color::Green
    } else if delta_sec <= 2 * 3600 {
        Color::Yellow
    } else {
        Color::DarkGray
    }
}

fn sentiment_color(value: &str) -> Color {
    let lower = value.to_ascii_lowercase();
    if lower.contains("critical") || lower.contains("high") || lower.contains("severe") {
        Color::Red
    } else if lower.contains("medium")
        || lower.contains("moderate")
        || lower.contains("elevated")
        || lower.contains("watch")
    {
        Color::Yellow
    } else if lower.contains("low")
        || lower.contains("normal")
        || lower.contains("stable")
        || lower.contains("operational")
    {
        Color::Green
    } else {
        Color::Cyan
    }
}

fn signed_value_color(value: f64) -> Color {
    if value > 0.0 {
        Color::Green
    } else if value < 0.0 {
        Color::Red
    } else {
        Color::DarkGray
    }
}

fn settings_status_style(required: bool, configured: bool) -> Style {
    match (required, configured) {
        (true, true) => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        (true, false) => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        (false, true) => Style::default().fg(Color::LightGreen),
        (false, false) => Style::default().fg(Color::Yellow),
    }
}

fn settings_status_label(required: bool, configured: bool) -> &'static str {
    match (required, configured) {
        (true, true) => "READY",
        (true, false) => "MISSING",
        (false, true) => "OPTIONAL-READY",
        (false, false) => "OPTIONAL-MISSING",
    }
}

fn endpoint_style(endpoint: Endpoint) -> Style {
    match endpoint {
        Endpoint::SeismologyEarthquakes => Style::default().fg(Color::LightMagenta),
        Endpoint::UnrestEvents => Style::default().fg(Color::LightRed),
        Endpoint::InfrastructureStatuses => Style::default().fg(Color::LightCyan),
        Endpoint::MarketCryptoQuotes => Style::default().fg(Color::LightGreen),
    }
}

fn draw_api_workspace(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &App) {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(38), Constraint::Min(1)])
        .split(area);
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(8),
            Constraint::Length(12),
        ])
        .split(horizontal[1]);

    let endpoint_items: Vec<ListItem<'_>> = app
        .endpoints
        .iter()
        .map(|endpoint| {
            ListItem::new(Line::from(vec![Span::styled(
                endpoint.to_string(),
                endpoint_style(*endpoint),
            )]))
        })
        .collect();

    let endpoint_list = List::new(endpoint_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("API Endpoints"),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");

    let mut list_state = ListState::default();
    list_state.select(Some(app.selected));
    frame.render_stateful_widget(endpoint_list, horizontal[0], &mut list_state);

    let api_dashboard = vec![
        Line::from(vec![
            Span::styled("Transport ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.transport_label.as_str(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled("Mode ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if app.in_flight { "FETCHING" } else { "IDLE" },
                Style::default().fg(if app.in_flight {
                    Color::Yellow
                } else {
                    Color::Green
                }),
            ),
        ]),
        Line::from(vec![
            Span::styled("Endpoint ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.selected_endpoint().path(),
                Style::default().fg(Color::LightBlue),
            ),
        ]),
        Line::from(vec![
            Span::styled("Base ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.base_url_display.as_str(),
                Style::default().fg(Color::Gray),
            ),
        ]),
    ];
    let api_dashboard_panel = Paragraph::new(api_dashboard)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("API Dashboard"),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(api_dashboard_panel, right_chunks[0]);

    let result_text = Text::from(
        app.output_lines
            .iter()
            .map(|line| Line::raw(line.as_str()))
            .collect::<Vec<_>>(),
    );
    let result_panel = Paragraph::new(result_text)
        .block(Block::default().borders(Borders::ALL).title("Response"))
        .wrap(Wrap { trim: false })
        .scroll((app.scroll, 0));
    frame.render_widget(result_panel, right_chunks[1]);

    if app.editing_request {
        frame.render_widget(&app.request_editor, right_chunks[2]);
    } else {
        let request_preview = Paragraph::new(app.current_saved_request_body().to_string())
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Request JSON [{}] (press e to edit)",
                app.selected_endpoint().path()
            )))
            .wrap(Wrap { trim: false });
        frame.render_widget(request_preview, right_chunks[2]);
    }
}

fn draw_rss_workspace(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &App) {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(40), Constraint::Min(1)])
        .split(area);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(horizontal[1]);

    let spikes = if app.rss_spikes.is_empty() {
        "none".to_string()
    } else {
        app.rss_spikes.join(" | ")
    };
    let now = Instant::now();
    let cooldown_count = app
        .rss_feed_health
        .values()
        .filter(|health| {
            health
                .cooldown_until
                .as_ref()
                .map(|until| *until > now)
                .unwrap_or(false)
        })
        .count();
    let failing_count = app
        .rss_feed_health
        .values()
        .filter(|health| health.consecutive_failures > 0)
        .count();
    let healthy_count = app
        .rss_feed_health
        .values()
        .filter(|health| health.consecutive_failures == 0 && health.last_success.is_some())
        .count();

    let search_label = if app.rss_query.is_empty() {
        "<none>".to_string()
    } else {
        app.rss_query.clone()
    };
    let keywords_label = if app.rss_keywords.is_empty() {
        "<none>".to_string()
    } else {
        app.rss_keywords.join(", ")
    };

    let control_lines = vec![
        Line::from(vec![
            Span::styled("Variant ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.rss_variant.to_string(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled("Category ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.current_rss_category(),
                Style::default().fg(Color::LightBlue),
            ),
        ]),
        Line::from(vec![
            Span::styled("Search ", Style::default().fg(Color::DarkGray)),
            Span::styled(search_label, Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("Keywords ", Style::default().fg(Color::DarkGray)),
            Span::styled(keywords_label, Style::default().fg(Color::Magenta)),
        ]),
        Line::raw(""),
        Line::from(vec![
            Span::styled("Feeds ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                feed_sources_for_variant(app.rss_variant).len().to_string(),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw("  "),
            Span::styled("Healthy ", Style::default().fg(Color::DarkGray)),
            Span::styled(healthy_count.to_string(), Style::default().fg(Color::Green)),
            Span::raw("  "),
            Span::styled("Failing ", Style::default().fg(Color::DarkGray)),
            Span::styled(failing_count.to_string(), Style::default().fg(Color::Red)),
            Span::raw("  "),
            Span::styled("Cooldown ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                cooldown_count.to_string(),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![Span::styled(
            app.rss_last_fetch_summary.clone(),
            Style::default().fg(Color::Gray),
        )]),
        Line::from(vec![
            Span::styled("Spikes ", Style::default().fg(Color::DarkGray)),
            Span::styled(spikes, Style::default().fg(Color::LightYellow)),
        ]),
    ];

    let left_panel = Paragraph::new(control_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("RSS Dashboard"),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(left_panel, horizontal[0]);

    let headlines: Vec<ListItem<'_>> = app
        .rss_view_items
        .iter()
        .map(|item| {
            let age = format_age(item.published_ts_ms);
            let marker = if item.keyword_hits.is_empty() {
                Span::raw("")
            } else {
                Span::styled(
                    format!("  [{}]", item.keyword_hits.join(",")),
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                )
            };
            ListItem::new(Line::from(vec![
                Span::styled(age, Style::default().fg(age_color(item.published_ts_ms))),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                Span::styled(item.source_name.clone(), Style::default().fg(Color::Cyan)),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                Span::styled(item.title.clone(), Style::default().fg(Color::White)),
                marker,
            ]))
        })
        .collect();

    let headline_block_title = format!("Headlines ({})", app.rss_view_items.len());
    let headline_list = List::new(headlines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(headline_block_title),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");
    let mut rss_state = ListState::default();
    if !app.rss_view_items.is_empty() {
        rss_state.select(Some(app.rss_selected));
    }
    frame.render_stateful_widget(headline_list, right[0], &mut rss_state);

    if app.rss_editor_active() {
        match app.rss_input_mode {
            RssInputMode::Search => frame.render_widget(&app.rss_search_editor, right[1]),
            RssInputMode::Keywords => frame.render_widget(&app.rss_keywords_editor, right[1]),
            RssInputMode::None => {}
        }
    } else {
        let detail = app
            .rss_view_items
            .get(app.rss_selected)
            .map(|item| {
                let link = if item.link.is_empty() {
                    "<no link>".to_string()
                } else {
                    item.link.clone()
                };
                format!(
                    "Title: {}\nSource: {} | Category: {} | Age: {}\nID: {}\n\n{}\n\nLink: {}",
                    item.title,
                    item.source_name,
                    item.category,
                    format_age(item.published_ts_ms),
                    item.id,
                    item.summary,
                    link
                )
            })
            .unwrap_or_else(|| "No RSS items loaded yet. Press f to fetch.".to_string());

        let details_panel = Paragraph::new(detail)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Headline Detail"),
            )
            .wrap(Wrap { trim: false })
            .scroll((app.rss_detail_scroll, 0));
        frame.render_widget(details_panel, right[1]);
    }
}

fn draw_brief_workspace(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &App) {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(40), Constraint::Min(1)])
        .split(area);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(62), Constraint::Min(10)])
        .split(horizontal[1]);

    let country_code = app.brief_country_code.as_str();
    let snapshot = app
        .brief_snapshot
        .as_ref()
        .filter(|snap| snap.country_code.eq_ignore_ascii_case(country_code));
    let country_name = snapshot
        .map(|snap| snap.country_name.clone())
        .unwrap_or_else(|| country_name_from_code(country_code));
    let cii_score = snapshot
        .and_then(|snap| snap.cii_score)
        .map(|score| format!("{:.1}/100", score))
        .unwrap_or_else(|| "n/a".to_string());
    let cii_trend = snapshot
        .map(|snap| compact_enum_label(snap.cii_trend.as_str()).to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let strategic = snapshot
        .map(|snap| compact_enum_label(snap.strategic_level.as_str()).to_string())
        .unwrap_or_else(|| "n/a".to_string());
    let cii_trend_color = sentiment_color(&cii_trend);
    let strategic_color = sentiment_color(&strategic);
    let stock_line = snapshot
        .map(|snap| {
            if snap.stock_available {
                format!(
                    "{} {:.2} {} ({:+.2}% 1W)",
                    snap.stock_index_name,
                    snap.stock_price,
                    snap.stock_currency,
                    snap.stock_week_change
                )
            } else {
                "Unavailable".to_string()
            }
        })
        .unwrap_or_else(|| "n/a".to_string());
    let intel_age = snapshot
        .map(|snap| format_age(snap.intel_generated_at))
        .unwrap_or_else(|| "unknown".to_string());
    let provider_label = snapshot
        .map(|snap| {
            if snap.intel_model.trim().is_empty() {
                app.brief_provider_label.clone()
            } else {
                snap.intel_model.clone()
            }
        })
        .unwrap_or_else(|| app.brief_provider_label.clone());

    let warning_lines = snapshot
        .map(|snap| {
            if snap.errors.is_empty() {
                vec!["Warnings: none".to_string()]
            } else {
                let mut lines = vec![format!("Warnings: {}", snap.errors.len())];
                lines.extend(
                    snap.errors
                        .iter()
                        .take(3)
                        .map(|error| format!("- {}", error)),
                );
                lines
            }
        })
        .unwrap_or_else(|| vec!["Warnings: none".to_string()]);

    let cii_value = snapshot.and_then(|snap| snap.cii_score).unwrap_or_default();
    let cii_color = if snapshot.is_none() {
        Color::DarkGray
    } else if cii_value >= 70.0 {
        Color::Red
    } else if cii_value >= 40.0 {
        Color::Yellow
    } else {
        Color::Green
    };

    let mut overview_lines = vec![
        Line::from(vec![
            Span::styled("Country ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{} ({})", country_name, country_code),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("CII ", Style::default().fg(Color::DarkGray)),
            Span::styled(cii_score, Style::default().fg(cii_color)),
            Span::styled("  Trend ", Style::default().fg(Color::DarkGray)),
            Span::styled(cii_trend.clone(), Style::default().fg(cii_trend_color)),
        ]),
        Line::from(vec![
            Span::styled("Strategic ", Style::default().fg(Color::DarkGray)),
            Span::styled(strategic.clone(), Style::default().fg(strategic_color)),
        ]),
        Line::from(vec![
            Span::styled("Stock ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                stock_line,
                Style::default().fg(signed_value_color(
                    snapshot.map(|snap| snap.stock_week_change).unwrap_or(0.0),
                )),
            ),
        ]),
        Line::from(vec![
            Span::styled("Intel age ", Style::default().fg(Color::DarkGray)),
            Span::styled(intel_age, Style::default().fg(Color::LightBlue)),
        ]),
        Line::from(vec![
            Span::styled("Provider ", Style::default().fg(Color::DarkGray)),
            Span::styled(provider_label, Style::default().fg(Color::LightMagenta)),
        ]),
        Line::from(vec![
            Span::styled("Related RSS ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.brief_related_rss.len().to_string(),
                Style::default().fg(Color::Magenta),
            ),
        ]),
        Line::from(vec![
            Span::styled("Status ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if app.brief_in_flight {
                    format!("{} refreshing brief...", spinner_symbol())
                } else {
                    "idle".to_string()
                },
                Style::default().fg(if app.brief_in_flight {
                    Color::Yellow
                } else {
                    Color::Green
                }),
            ),
        ]),
        Line::from(vec![Span::styled(
            app.brief_last_fetch_summary.clone(),
            Style::default().fg(Color::Gray),
        )]),
        Line::raw(""),
    ];
    overview_lines.extend(warning_lines.into_iter().map(|line| {
        let color = if line.starts_with("Warnings:") {
            Color::Yellow
        } else {
            Color::Red
        };
        Line::from(vec![Span::styled(line, Style::default().fg(color))])
    }));
    let overview = Paragraph::new(overview_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("BRIEF Dashboard"),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(overview, horizontal[0]);

    let brief_text = if app.brief_in_flight && snapshot.is_none() {
        format!(
            "{} Building country brief...\n\nAnalyzing geopolitical context\nAssessing risk baseline and trend\nFetching market index snapshot\nCorrelating with RSS signals",
            spinner_symbol()
        )
    } else {
        snapshot
            .map(|snap| {
                if snap.intel_brief.trim().is_empty() {
                    if app.brief_in_flight {
                        format!(
                            "{} Refreshing intelligence narrative...\n\nPrevious cycle returned empty text.",
                            spinner_symbol()
                        )
                    } else {
                        "No intelligence brief returned yet. Press Enter/r/f to refresh."
                            .to_string()
                    }
                } else {
                    let mut text = String::new();
                    if app.brief_in_flight {
                        text.push_str(format!("{} Refreshing in background...\n\n", spinner_symbol()).as_str());
                    }
                    text.push_str(snap.intel_brief.as_str());
                    text
                }
            })
            .unwrap_or_else(|| "No intelligence brief loaded. Press Enter/r/f to fetch.".to_string())
    };

    let brief_title = snapshot
        .map(|snap| {
            if snap.intel_model.is_empty() {
                format!("Intel Brief ({})", snap.country_code)
            } else {
                format!("Intel Brief ({}, {})", snap.country_code, snap.intel_model)
            }
        })
        .unwrap_or_else(|| format!("Intel Brief ({})", app.brief_country_code));
    let brief_title = if app.brief_in_flight {
        format!("{} {}", spinner_symbol(), brief_title)
    } else {
        brief_title
    };

    let brief_panel = Paragraph::new(brief_text)
        .block(Block::default().borders(Borders::ALL).title(brief_title))
        .wrap(Wrap { trim: false })
        .scroll((app.brief_brief_scroll, 0));
    frame.render_widget(brief_panel, right[0]);

    if app.brief_editor_active() {
        frame.render_widget(&app.brief_country_editor, right[1]);
        return;
    }

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
        .split(right[1]);

    let related_items: Vec<ListItem<'_>> = app
        .brief_related_rss
        .iter()
        .map(|item| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format_age(item.published_ts_ms),
                    Style::default().fg(age_color(item.published_ts_ms)),
                ),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                Span::styled(item.source_name.clone(), Style::default().fg(Color::Cyan)),
                Span::styled(" | ", Style::default().fg(Color::DarkGray)),
                Span::styled(item.title.clone(), Style::default().fg(Color::White)),
            ]))
        })
        .collect();
    let related_list = List::new(related_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Related RSS ({})", app.brief_related_rss.len())),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");
    let mut related_state = ListState::default();
    if !app.brief_related_rss.is_empty() {
        related_state.select(Some(app.brief_related_selected));
    }
    frame.render_stateful_widget(related_list, bottom[0], &mut related_state);

    let related_detail = app
        .brief_related_rss
        .get(app.brief_related_selected)
        .map(|item| {
            let link = if item.link.is_empty() {
                "<no link>".to_string()
            } else {
                item.link.clone()
            };
            format!(
                "Title: {}\nSource: {} | Category: {} | Age: {}\n\n{}\n\nLink: {}",
                item.title,
                item.source_name,
                item.category,
                format_age(item.published_ts_ms),
                item.summary,
                link
            )
        })
        .unwrap_or_else(|| {
            "No related RSS headlines yet. Refresh RSS and BRIEF to correlate signals.".to_string()
        });
    let related_detail_panel = Paragraph::new(related_detail)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Related Headline Detail"),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.brief_news_scroll, 0));
    frame.render_widget(related_detail_panel, bottom[1]);
}

fn draw_settings_workspace(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &App) {
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(62), Constraint::Min(1)])
        .split(area);
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(7), Constraint::Min(1)])
        .split(horizontal[0]);

    let required_total = app
        .settings_checks
        .iter()
        .filter(|item| item.required)
        .count();
    let required_missing = app
        .settings_checks
        .iter()
        .filter(|item| item.required && !item.configured)
        .count();
    let optional_total = app.settings_checks.len().saturating_sub(required_total);
    let optional_missing = app
        .settings_checks
        .iter()
        .filter(|item| !item.required && !item.configured)
        .count();

    let dashboard_lines = vec![
        Line::from(vec![
            Span::styled("Transport ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.transport_label.as_str(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled("Base ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.base_url_display.as_str(),
                Style::default().fg(Color::Gray),
            ),
        ]),
        Line::from(vec![
            Span::styled("Required keys ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "{} ready / {} missing",
                    required_total.saturating_sub(required_missing),
                    required_missing
                ),
                Style::default().fg(if required_missing > 0 {
                    Color::Red
                } else {
                    Color::Green
                }),
            ),
        ]),
        Line::from(vec![
            Span::styled("Optional keys ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "{} ready / {} missing",
                    optional_total.saturating_sub(optional_missing),
                    optional_missing
                ),
                Style::default().fg(if optional_missing > 0 {
                    Color::Yellow
                } else {
                    Color::Green
                }),
            ),
        ]),
    ];
    let dashboard = Paragraph::new(dashboard_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Configuration Dashboard"),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(dashboard, left[0]);

    let key_rows: Vec<ListItem<'_>> = app
        .settings_checks
        .iter()
        .map(|check| {
            let style = settings_status_style(check.required, check.configured);
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!(
                        "{:<16}",
                        settings_status_label(check.required, check.configured)
                    ),
                    style,
                ),
                Span::styled(" ", Style::default().fg(Color::DarkGray)),
                Span::styled(check.capability.clone(), Style::default().fg(Color::White)),
            ]))
        })
        .collect();
    let key_list = List::new(key_rows)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("API Key Audit"),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");
    let mut key_state = ListState::default();
    if !app.settings_checks.is_empty() {
        key_state.select(Some(app.settings_selected));
    }
    frame.render_stateful_widget(key_list, left[1], &mut key_state);

    let detail_text = app
        .settings_checks
        .get(app.settings_selected)
        .map(|check| {
            let status = settings_status_label(check.required, check.configured);
            format!(
                "Capability: {}\n\nStatus: {}\nRequirement: {}\nKeys: {}\n\n{}\n\nAction:\n{}",
                check.capability,
                status,
                if check.required {
                    "Required"
                } else {
                    "Optional"
                },
                check.key_names,
                check.note,
                if check.configured {
                    "Configuration detected. No action needed."
                } else if check.required {
                    "Set the key/env and restart the TUI for full functionality."
                } else {
                    "Optional; add key to unlock enhanced data quality."
                }
            )
        })
        .unwrap_or_else(|| "No key checks available.".to_string());
    let detail_panel = Paragraph::new(detail_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Selected Capability"),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.settings_detail_scroll, 0));
    frame.render_widget(detail_panel, horizontal[1]);
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    api: ApiClient,
    refresh_interval: Option<Duration>,
) -> Result<()> {
    let mut app = App::new(refresh_interval, &api);
    let (sender, receiver) = mpsc::channel::<WorkerEvent>();

    start_api_fetch(&mut app, &api, &sender);

    loop {
        apply_worker_events(&mut app, &receiver);
        if app.should_auto_refresh_api() {
            start_api_fetch(&mut app, &api, &sender);
        }
        if app.should_auto_refresh_rss() {
            start_rss_fetch(&mut app, &api, &sender);
        }
        if app.should_auto_refresh_brief() {
            start_brief_fetch(&mut app, &api, &sender);
        }
        terminal
            .draw(|frame| draw_ui(frame, &app))
            .context("failed to draw TUI frame")?;

        if event::poll(Duration::from_millis(100)).context("failed to poll terminal events")? {
            let Event::Key(key) = event::read().context("failed to read terminal event")? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }

            if app.editing_request {
                handle_editor_key(&mut app, key, &api, &sender);
                continue;
            }

            if app.view == AppView::Rss && app.rss_editor_active() {
                handle_rss_input_key(&mut app, key, &api, &sender);
                continue;
            }

            if app.view == AppView::Brief && app.brief_editor_active() {
                handle_brief_input_key(&mut app, key, &api, &sender);
                continue;
            }

            if key.code == KeyCode::Tab {
                app.toggle_view();
                if app.view == AppView::Rss && app.rss_items.is_empty() && !app.rss_in_flight {
                    start_rss_fetch(&mut app, &api, &sender);
                }
                if app.view == AppView::Brief
                    && app.brief_snapshot.is_none()
                    && !app.brief_in_flight
                {
                    start_brief_fetch(&mut app, &api, &sender);
                }
                if app.view == AppView::Api {
                    app.ensure_editor_synced();
                }
                continue;
            }

            if key.code == KeyCode::Char('1') {
                app.view = AppView::Api;
                app.status_line = "Switched to API workspace".to_string();
                app.ensure_editor_synced();
                continue;
            }
            if key.code == KeyCode::Char('2') {
                app.view = AppView::Rss;
                app.status_line = "Switched to RSS workspace".to_string();
                if app.rss_items.is_empty() && !app.rss_in_flight {
                    start_rss_fetch(&mut app, &api, &sender);
                }
                continue;
            }
            if key.code == KeyCode::Char('3') {
                app.view = AppView::Brief;
                app.status_line = "Switched to BRIEF workspace".to_string();
                if app.brief_snapshot.is_none() && !app.brief_in_flight {
                    start_brief_fetch(&mut app, &api, &sender);
                }
                continue;
            }
            if key.code == KeyCode::Char('4') {
                app.view = AppView::Settings;
                app.status_line = "Switched to SETTINGS workspace".to_string();
                app.refresh_settings(&api);
                continue;
            }

            match app.view {
                AppView::Api => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Up => {
                        app.select_prev();
                        app.ensure_editor_synced();
                    }
                    KeyCode::Down => {
                        app.select_next();
                        app.ensure_editor_synced();
                    }
                    KeyCode::Char('j') => app.scroll_down(),
                    KeyCode::Char('k') => app.scroll_up(),
                    KeyCode::Enter | KeyCode::Char('r') => start_api_fetch(&mut app, &api, &sender),
                    KeyCode::Char('e') => app.enter_request_editor(),
                    KeyCode::Char('t') => app.reset_selected_request_to_template(),
                    KeyCode::Char('a') => app.toggle_auto_refresh(),
                    _ => {}
                },
                AppView::Rss => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Up | KeyCode::Char('k') => app.rss_select_prev(),
                    KeyCode::Down | KeyCode::Char('j') => app.rss_select_next(),
                    KeyCode::Left => app.cycle_rss_category_prev(),
                    KeyCode::Right => app.cycle_rss_category_next(),
                    KeyCode::Char('u') => app.rss_detail_scroll_up(),
                    KeyCode::Char('d') => app.rss_detail_scroll_down(),
                    KeyCode::Char('a') => app.toggle_auto_refresh(),
                    KeyCode::Char('v') => {
                        app.cycle_rss_variant();
                        start_rss_fetch(&mut app, &api, &sender);
                    }
                    KeyCode::Char('f') | KeyCode::Char('r') => {
                        start_rss_fetch(&mut app, &api, &sender)
                    }
                    KeyCode::Char('/') => app.enter_rss_search_editor(),
                    KeyCode::Char('m') => app.enter_rss_keywords_editor(),
                    KeyCode::Char('t') => app.reset_rss_filters(),
                    _ => {}
                },
                AppView::Brief => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Enter | KeyCode::Char('r') | KeyCode::Char('f') => {
                        start_brief_fetch(&mut app, &api, &sender)
                    }
                    KeyCode::Char('x') => {
                        if let Some(snapshot) = app.brief_snapshot.as_ref() {
                            match export_brief_snapshot(snapshot, &app.brief_related_rss) {
                                Ok((json_path, txt_path)) => {
                                    app.status_line = format!(
                                        "Exported BRIEF to {} and {}",
                                        json_path.display(),
                                        txt_path.display()
                                    );
                                }
                                Err(err) => {
                                    app.status_line = format!("Brief export failed: {}", err);
                                }
                            }
                        } else {
                            app.status_line = "No BRIEF snapshot to export yet".to_string();
                        }
                    }
                    KeyCode::Char('a') => app.toggle_auto_refresh(),
                    KeyCode::Char('c') => app.enter_brief_country_editor(),
                    KeyCode::Char('n') | KeyCode::Right => {
                        app.cycle_brief_country_next();
                        start_brief_fetch(&mut app, &api, &sender);
                    }
                    KeyCode::Char('p') | KeyCode::Left => {
                        app.cycle_brief_country_prev();
                        start_brief_fetch(&mut app, &api, &sender);
                    }
                    KeyCode::Up => app.brief_select_prev(),
                    KeyCode::Down => app.brief_select_next(),
                    KeyCode::Char('j') => app.brief_scroll_down(),
                    KeyCode::Char('k') => app.brief_scroll_up(),
                    KeyCode::Char('u') => app.brief_news_scroll_up(),
                    KeyCode::Char('d') => app.brief_news_scroll_down(),
                    _ => {}
                },
                AppView::Settings => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Up | KeyCode::Char('k') => app.settings_select_prev(),
                    KeyCode::Down | KeyCode::Char('j') => app.settings_select_next(),
                    KeyCode::Char('u') => app.settings_detail_scroll_up(),
                    KeyCode::Char('d') => app.settings_detail_scroll_down(),
                    KeyCode::Char('g') | KeyCode::Char('r') => app.refresh_settings(&api),
                    KeyCode::Char('a') => app.toggle_auto_refresh(),
                    _ => {}
                },
            }
        }
    }

    Ok(())
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode().context("failed to enable terminal raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).context("failed to initialize terminal backend")
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode().context("failed to disable terminal raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
        .context("failed to leave alternate screen")?;
    terminal.show_cursor().context("failed to show cursor")
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let api = ApiClient::new(
        cli.base_url,
        cli.api_key,
        cli.timeout_secs,
        cli.api_mode,
        cli.brief_provider,
        cli.chatjimmy_pack,
    )?;
    let refresh_interval = if cli.auto_refresh_secs > 0 {
        Some(Duration::from_secs(cli.auto_refresh_secs))
    } else {
        None
    };

    let mut terminal = init_terminal()?;
    let run_result = run_app(&mut terminal, api, refresh_interval);
    let restore_result = restore_terminal(&mut terminal);

    run_result?;
    restore_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BriefSourceEvidence, brief_is_grounded, extract_first_json_object, parse_chatjimmy_payload,
        strip_source_citations,
    };

    #[test]
    fn parse_chatjimmy_payload_accepts_stats_suffix() {
        let body = r#"{"brief":"Signal stable","confidence":0.77}
<|stats|>{"decode_tokens":12}<|/stats|>"#;
        let payload = parse_chatjimmy_payload(body).expect("payload should parse");
        assert_eq!(payload["brief"], "Signal stable");
    }

    #[test]
    fn extract_first_json_object_finds_embedded_object() {
        let body = "prefix\n{\"brief\":\"Nested\",\"watch_items\":[\"A\",\"B\"]}\ntrailer";
        let object = extract_first_json_object(body).expect("object expected");
        assert!(object.contains("\"watch_items\""));
    }

    #[test]
    fn grounded_brief_requires_valid_source_citations() {
        let evidence = vec![
            BriefSourceEvidence {
                id: "S1".to_string(),
                text: "Risk trend is rising in the latest report.".to_string(),
            },
            BriefSourceEvidence {
                id: "S2".to_string(),
                text: "Signal remains stable in the latest feed.".to_string(),
            },
        ];
        let grounded =
            "- Signal remains stable in the latest feed. [S2]\n- Risk trend is rising. [S1]";
        assert!(brief_is_grounded(
            grounded,
            &["S1".to_string(), "S2".to_string()],
            &evidence
        ));
        let ungrounded = "- Signal remains stable in the latest feed.";
        assert!(!brief_is_grounded(ungrounded, &[], &evidence));
        let invalid_source = "- New claim. [S9]";
        assert!(!brief_is_grounded(
            invalid_source,
            &["S9".to_string()],
            &evidence
        ));
    }

    #[test]
    fn strip_source_citations_removes_tags() {
        let line = "- Risk moved higher from latest report. [S1][S2]";
        assert_eq!(
            strip_source_citations(line),
            "- Risk moved higher from latest report. "
        );
    }
}
