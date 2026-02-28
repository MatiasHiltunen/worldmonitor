use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use ratatui_textarea::{Input as TextInput, TextArea};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use strum::{Display, EnumIter, IntoEnumIterator};

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
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,
    #[arg(long, default_value_t = 0)]
    auto_refresh_secs: u64,
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

#[derive(Clone)]
struct ApiClient {
    base_url: String,
    api_key: Option<String>,
    client: Client,
}

impl ApiClient {
    fn new(base_url: String, api_key: Option<String>, timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            client,
        })
    }

    fn fetch_json(&self, endpoint: Endpoint, request_body: &Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, endpoint.path());
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
            .with_context(|| format!("request failed: {}", endpoint.path()))?;

        let status = response.status();
        let body_text = response
            .text()
            .context("failed to read upstream response body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "HTTP {} from {}: {}",
                status.as_u16(),
                endpoint.path(),
                truncate_for_error(&body_text, 180)
            ));
        }

        let payload: Value =
            serde_json::from_str(&body_text).context("response was not valid JSON")?;
        Ok(payload)
    }
}

struct App {
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
}

impl App {
    fn new(refresh_interval: Option<Duration>) -> Self {
        let endpoints: Vec<Endpoint> = Endpoint::iter().collect();
        let selected = 0;
        let selected_endpoint = endpoints[selected];

        let (template_bodies, request_bodies, template_source, loaded_from_docs) =
            build_initial_request_bodies(&endpoints);

        let request_editor = build_request_editor(
            request_bodies
                .get(&selected_endpoint)
                .map(String::as_str)
                .unwrap_or("{}"),
            false,
            selected_endpoint,
        );

        Self {
            endpoints,
            selected,
            output_lines: vec![
                "WorldMonitor v2 (pure Rust)".to_string(),
                String::new(),
                template_source,
                String::new(),
                "Use Up/Down to choose endpoint, Enter/r to fetch.".to_string(),
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

    fn should_auto_refresh(&self) -> bool {
        let Some(interval) = self.refresh_interval else {
            return false;
        };
        if !self.auto_refresh_enabled || self.in_flight || self.editing_request {
            return false;
        }

        match self.last_fetch_finished_at {
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

        let remaining = match self.last_fetch_finished_at {
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
    Success {
        endpoint: Endpoint,
        lines: Vec<String>,
    },
    Failure {
        endpoint: Endpoint,
        error: String,
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

fn start_fetch(app: &mut App, api: &ApiClient, sender: &Sender<WorkerEvent>) {
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
            Ok(lines) => WorkerEvent::Success { endpoint, lines },
            Err(error) => WorkerEvent::Failure {
                endpoint,
                error: error.to_string(),
            },
        };
        let _ = sender.send(event);
    });
}

fn apply_worker_events(app: &mut App, receiver: &Receiver<WorkerEvent>) {
    while let Ok(event) = receiver.try_recv() {
        match event {
            WorkerEvent::Success { endpoint, lines } => {
                app.output_lines = lines;
                app.status_line = format!("Loaded {}", endpoint);
                app.in_flight = false;
                app.scroll = 0;
                app.last_fetch_finished_at = Some(Instant::now());
            }
            WorkerEvent::Failure { endpoint, error } => {
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
                start_fetch(app, api, sender);
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

fn draw_ui(frame: &mut ratatui::Frame<'_>, app: &App) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(2)])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(38), Constraint::Min(1)])
        .split(vertical[0]);
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(12)])
        .split(horizontal[1]);

    let endpoint_items: Vec<ListItem<'_>> = app
        .endpoints
        .iter()
        .map(|endpoint| ListItem::new(endpoint.to_string()))
        .collect();

    let endpoint_list = List::new(endpoint_items)
        .block(Block::default().borders(Borders::ALL).title("Endpoints"))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");

    let mut list_state = ListState::default();
    list_state.select(Some(app.selected));
    frame.render_stateful_widget(endpoint_list, horizontal[0], &mut list_state);

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
    frame.render_widget(result_panel, right_chunks[0]);

    if app.editing_request {
        frame.render_widget(&app.request_editor, right_chunks[1]);
    } else {
        let request_preview = Paragraph::new(app.current_saved_request_body().to_string())
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Request JSON [{}] (press e to edit)",
                app.selected_endpoint().path()
            )))
            .wrap(Wrap { trim: false });
        frame.render_widget(request_preview, right_chunks[1]);
    }

    let footer_style = if app.in_flight {
        Style::default().fg(Color::Yellow)
    } else if app.editing_request {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::Green)
    };
    let controls = if app.editing_request {
        "EDIT MODE | type JSON | Ctrl+S save | Ctrl+R save+fetch | Ctrl+T reset | Esc discard"
    } else {
        "Up/Down endpoint | Enter/r fetch | e edit request | t reset template | a auto-refresh | j/k scroll | q quit"
    };
    let footer = Paragraph::new(format!(
        "{} | {} | {}",
        app.status_line,
        app.auto_refresh_summary(),
        controls
    ))
    .style(footer_style)
    .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, vertical[1]);
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    api: ApiClient,
    refresh_interval: Option<Duration>,
) -> Result<()> {
    let mut app = App::new(refresh_interval);
    let (sender, receiver) = mpsc::channel::<WorkerEvent>();

    start_fetch(&mut app, &api, &sender);

    loop {
        apply_worker_events(&mut app, &receiver);
        if app.should_auto_refresh() {
            start_fetch(&mut app, &api, &sender);
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

            match key.code {
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
                KeyCode::Enter | KeyCode::Char('r') => start_fetch(&mut app, &api, &sender),
                KeyCode::Char('e') => app.enter_request_editor(),
                KeyCode::Char('t') => app.reset_selected_request_to_template(),
                KeyCode::Char('a') => app.toggle_auto_refresh(),
                _ => {}
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
    let api = ApiClient::new(cli.base_url, cli.api_key, cli.timeout_secs)?;
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
