use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use futures::future::join_all;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AppState, error::AppError};

const FLIGHTS_CACHE_TTL: Duration = Duration::from_secs(120);
const THEATER_CACHE_TTL: Duration = Duration::from_secs(300);
const WINGBITS_CACHE_TTL: Duration = Duration::from_secs(300);
const USNI_CACHE_TTL: Duration = Duration::from_secs(21_600);
const USNI_STALE_TTL: Duration = Duration::from_secs(604_800);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListMilitaryFlightsRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub bounding_box: Option<BoundingBox>,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub aircraft_type: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PaginationRequest {
    #[serde(default)]
    pub page_size: usize,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BoundingBox {
    #[serde(default)]
    pub north_east: Option<GeoCoordinates>,
    #[serde(default)]
    pub south_west: Option<GeoCoordinates>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListMilitaryFlightsResponse {
    pub flights: Vec<MilitaryFlight>,
    pub clusters: Vec<MilitaryFlightCluster>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MilitaryFlight {
    pub id: String,
    pub callsign: String,
    pub hex_code: String,
    pub registration: String,
    pub aircraft_type: String,
    pub aircraft_model: String,
    pub operator: String,
    pub operator_country: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub altitude: f64,
    pub heading: f64,
    pub speed: f64,
    pub vertical_rate: f64,
    pub on_ground: bool,
    pub squawk: String,
    pub origin: String,
    pub destination: String,
    pub last_seen_at: i64,
    pub first_seen_at: i64,
    pub confidence: String,
    pub is_interesting: bool,
    pub note: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment: Option<FlightEnrichment>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FlightEnrichment {
    pub manufacturer: String,
    pub owner: String,
    pub operator_name: String,
    pub type_code: String,
    pub built_year: String,
    pub confirmed_military: bool,
    pub military_branch: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MilitaryFlightCluster {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub flight_count: usize,
    pub flights: Vec<MilitaryFlight>,
    pub dominant_operator: String,
    pub activity_type: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaginationResponse {
    pub next_cursor: String,
    pub total_count: usize,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetTheaterPostureRequest {
    #[serde(default)]
    pub theater: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetTheaterPostureResponse {
    pub theaters: Vec<TheaterPosture>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TheaterPosture {
    pub theater: String,
    pub posture_level: String,
    pub active_flights: usize,
    pub tracked_vessels: usize,
    pub active_operations: Vec<String>,
    pub assessed_at: i64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetAircraftDetailsRequest {
    #[serde(default)]
    pub icao24: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetAircraftDetailsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<AircraftDetails>,
    pub configured: bool,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AircraftDetails {
    pub icao24: String,
    pub registration: String,
    pub manufacturer_icao: String,
    pub manufacturer_name: String,
    pub model: String,
    pub typecode: String,
    pub serial_number: String,
    pub icao_aircraft_type: String,
    pub operator: String,
    pub operator_callsign: String,
    pub operator_icao: String,
    pub owner: String,
    pub built: String,
    pub engines: String,
    pub category_description: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetAircraftDetailsBatchRequest {
    #[serde(default)]
    pub icao24s: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetAircraftDetailsBatchResponse {
    pub results: HashMap<String, AircraftDetails>,
    pub fetched: usize,
    pub requested: usize,
    pub configured: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetWingbitsStatusRequest {}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetWingbitsStatusResponse {
    pub configured: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetUSNIFleetReportRequest {
    #[serde(default)]
    pub force_refresh: bool,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetUSNIFleetReportResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<USNIFleetReport>,
    pub cached: bool,
    pub stale: bool,
    pub error: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct USNIFleetReport {
    pub article_url: String,
    pub article_date: String,
    pub article_title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub battle_force_summary: Option<BattleForceSummary>,
    pub vessels: Vec<USNIVessel>,
    pub strike_groups: Vec<USNIStrikeGroup>,
    pub regions: Vec<String>,
    pub parsing_warnings: Vec<String>,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BattleForceSummary {
    pub total_ships: i64,
    pub deployed: i64,
    pub underway: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct USNIVessel {
    pub name: String,
    pub hull_number: String,
    pub vessel_type: String,
    pub region: String,
    pub region_lat: f64,
    pub region_lon: f64,
    pub deployment_status: String,
    pub home_port: String,
    pub strike_group: String,
    pub activity_description: String,
    pub article_url: String,
    pub article_date: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct USNIStrikeGroup {
    pub name: String,
    pub carrier: String,
    pub air_wing: String,
    pub destroyer_squadron: String,
    pub escorts: Vec<String>,
}

#[derive(Clone)]
struct FlightsCacheEntry {
    value: ListMilitaryFlightsResponse,
    expires_at: Instant,
}

#[derive(Clone)]
struct TheaterCacheEntry {
    value: GetTheaterPostureResponse,
    expires_at: Instant,
}

#[derive(Clone)]
struct WingbitsCacheEntry {
    value: AircraftDetails,
    expires_at: Instant,
}

#[derive(Clone)]
struct UsniCacheEntry {
    value: USNIFleetReport,
    expires_at: Instant,
}

static FLIGHTS_CACHE: Lazy<Mutex<HashMap<String, FlightsCacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static THEATER_CACHE: Lazy<Mutex<Option<TheaterCacheEntry>>> = Lazy::new(|| Mutex::new(None));
static WINGBITS_CACHE: Lazy<Mutex<HashMap<String, WingbitsCacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static USNI_CACHE: Lazy<Mutex<Option<UsniCacheEntry>>> = Lazy::new(|| Mutex::new(None));
static USNI_STALE_CACHE: Lazy<Mutex<Option<UsniCacheEntry>>> = Lazy::new(|| Mutex::new(None));

#[derive(Clone)]
struct RawFlight {
    id: String,
    callsign: String,
    lat: f64,
    lon: f64,
    altitude: f64,
    heading: f64,
    speed: f64,
    vertical_rate: f64,
    first_seen_at: i64,
    last_seen_at: i64,
}

#[derive(Clone, Copy)]
struct TheaterDef {
    id: &'static str,
    north: f64,
    south: f64,
    east: f64,
    west: f64,
    elevated: usize,
    critical: usize,
}

const THEATERS: &[TheaterDef] = &[
    TheaterDef {
        id: "iran-theater",
        north: 42.0,
        south: 20.0,
        east: 65.0,
        west: 30.0,
        elevated: 8,
        critical: 20,
    },
    TheaterDef {
        id: "taiwan-theater",
        north: 30.0,
        south: 18.0,
        east: 130.0,
        west: 115.0,
        elevated: 6,
        critical: 15,
    },
    TheaterDef {
        id: "baltic-theater",
        north: 65.0,
        south: 52.0,
        east: 32.0,
        west: 10.0,
        elevated: 5,
        critical: 12,
    },
    TheaterDef {
        id: "blacksea-theater",
        north: 48.0,
        south: 40.0,
        east: 42.0,
        west: 26.0,
        elevated: 4,
        critical: 10,
    },
    TheaterDef {
        id: "korea-theater",
        north: 43.0,
        south: 33.0,
        east: 132.0,
        west: 124.0,
        elevated: 5,
        critical: 12,
    },
    TheaterDef {
        id: "south-china-sea",
        north: 25.0,
        south: 5.0,
        east: 121.0,
        west: 105.0,
        elevated: 6,
        critical: 15,
    },
    TheaterDef {
        id: "east-med-theater",
        north: 37.0,
        south: 33.0,
        east: 37.0,
        west: 25.0,
        elevated: 4,
        critical: 10,
    },
    TheaterDef {
        id: "israel-gaza-theater",
        north: 33.0,
        south: 29.0,
        east: 36.0,
        west: 33.0,
        elevated: 3,
        critical: 8,
    },
    TheaterDef {
        id: "yemen-redsea-theater",
        north: 22.0,
        south: 11.0,
        east: 54.0,
        west: 32.0,
        elevated: 4,
        critical: 10,
    },
];

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn parse_page_size(request: &ListMilitaryFlightsRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
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

fn parse_bool(value: Option<&Value>) -> bool {
    let Some(value) = value else {
        return false;
    };
    if let Some(boolean) = value.as_bool() {
        return boolean;
    }
    matches!(value.as_str().map(|v| v.trim()), Some("true"))
}

fn is_military_callsign(callsign: &str) -> bool {
    if callsign.trim().is_empty() {
        return false;
    }
    let uppercase = callsign.trim().to_ascii_uppercase();
    const PREFIXES: &[&str] = &[
        "RCH", "REACH", "SHELL", "TEXACO", "ARCO", "ESSO", "PETRO", "AWACS", "SENTRY", "NATO",
        "USAF", "USN", "USMC", "RAF", "IAF", "VKS", "PLAAF", "DUKE", "HAVOC", "VIPER", "RAGE",
        "FURY", "COBRA", "PYTHON", "REAPER", "HUNTER", "DUSTOFF",
    ];
    PREFIXES.iter().any(|prefix| uppercase.starts_with(prefix))
        || Regex::new(r"^[A-Z]{3,6}\d{1,3}$")
            .expect("valid military callsign regex")
            .is_match(&uppercase)
}

fn detect_aircraft_type(callsign: &str) -> &'static str {
    let uppercase = callsign.trim().to_ascii_uppercase();
    if Regex::new(r"^(SHELL|TEXACO|ARCO|ESSO|PETRO|KC)")
        .expect("valid tanker regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_TANKER";
    }
    if Regex::new(r"^(AWACS|SENTRY|E3|E8)")
        .expect("valid awacs regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_AWACS";
    }
    if Regex::new(r"^(RCH|REACH|C17|C5|C130|DUSTOFF)")
        .expect("valid transport regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_TRANSPORT";
    }
    if Regex::new(r"^(VIPER|RAGE|FURY|EAGLE|RAPTOR)")
        .expect("valid fighter regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_FIGHTER";
    }
    if Regex::new(r"^(RQ|MQ|REAPER|PREDATOR)")
        .expect("valid drone regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_DRONE";
    }
    if Regex::new(r"^(B52|B1|B2|BONE|DEATH)")
        .expect("valid bomber regex")
        .is_match(&uppercase)
    {
        return "MILITARY_AIRCRAFT_TYPE_BOMBER";
    }
    "MILITARY_AIRCRAFT_TYPE_UNKNOWN"
}

fn detect_operator(callsign: &str) -> &'static str {
    let uppercase = callsign.trim().to_ascii_uppercase();
    if uppercase.starts_with("RCH")
        || uppercase.starts_with("REACH")
        || uppercase.starts_with("SHELL")
        || uppercase.starts_with("TEXACO")
    {
        return "MILITARY_OPERATOR_USAF";
    }
    if uppercase.starts_with("NAVY") {
        return "MILITARY_OPERATOR_USN";
    }
    if uppercase.starts_with("RAF") {
        return "MILITARY_OPERATOR_RAF";
    }
    if uppercase.starts_with("NATO") {
        return "MILITARY_OPERATOR_NATO";
    }
    "MILITARY_OPERATOR_OTHER"
}

fn opensky_base_url() -> Option<String> {
    if std::env::var("LOCAL_API_MODE")
        .unwrap_or_default()
        .contains("sidecar")
    {
        return Some("https://opensky-network.org/api/states/all".to_string());
    }
    let relay = std::env::var("WS_RELAY_URL").ok()?;
    if relay.trim().is_empty() {
        return None;
    }
    Some(format!("{}/opensky", relay.trim_end_matches('/')))
}

async fn fetch_opensky_flights(state: &AppState, bbox: Option<&BoundingBox>) -> Vec<RawFlight> {
    let Some(base_url) = opensky_base_url() else {
        return Vec::new();
    };
    let mut url = match reqwest::Url::parse(&base_url) {
        Ok(url) => url,
        Err(_) => return Vec::new(),
    };
    if let Some(bounding_box) = bbox
        && let (Some(south_west), Some(north_east)) =
            (&bounding_box.south_west, &bounding_box.north_east)
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("lamin", &south_west.latitude.to_string());
        query.append_pair("lamax", &north_east.latitude.to_string());
        query.append_pair("lomin", &south_west.longitude.to_string());
        query.append_pair("lomax", &north_east.longitude.to_string());
    }

    let response = match state
        .http_client
        .get(url)
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
    let Some(states) = payload.get("states").and_then(Value::as_array) else {
        return Vec::new();
    };

    let now = now_epoch_ms();
    states
        .iter()
        .filter_map(|row| {
            let values = row.as_array()?;
            let id = string_value(values.first());
            let callsign = string_value(values.get(1));
            if id.is_empty() || !is_military_callsign(&callsign) {
                return None;
            }
            let longitude = parse_f64(values.get(5))?;
            let latitude = parse_f64(values.get(6))?;
            if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
                return None;
            }
            if parse_bool(values.get(8)) {
                return None;
            }
            let altitude = parse_f64(values.get(7)).unwrap_or(0.0);
            let speed = parse_f64(values.get(9)).unwrap_or(0.0);
            let heading = parse_f64(values.get(10)).unwrap_or(0.0);
            let vertical_rate = parse_f64(values.get(11)).unwrap_or(0.0);
            Some(RawFlight {
                id,
                callsign,
                lat: latitude,
                lon: longitude,
                altitude,
                heading,
                speed,
                vertical_rate,
                first_seen_at: now.saturating_sub(300_000),
                last_seen_at: now,
            })
        })
        .collect()
}

fn to_military_flight(raw: &RawFlight) -> MilitaryFlight {
    MilitaryFlight {
        id: raw.id.clone(),
        callsign: raw.callsign.clone(),
        hex_code: raw.id.clone(),
        registration: String::new(),
        aircraft_type: detect_aircraft_type(&raw.callsign).to_string(),
        aircraft_model: String::new(),
        operator: detect_operator(&raw.callsign).to_string(),
        operator_country: String::new(),
        location: Some(GeoCoordinates {
            latitude: raw.lat,
            longitude: raw.lon,
        }),
        altitude: raw.altitude,
        heading: raw.heading,
        speed: raw.speed,
        vertical_rate: raw.vertical_rate,
        on_ground: false,
        squawk: String::new(),
        origin: String::new(),
        destination: String::new(),
        last_seen_at: raw.last_seen_at,
        first_seen_at: raw.first_seen_at,
        confidence: "MILITARY_CONFIDENCE_LOW".to_string(),
        is_interesting: false,
        note: String::new(),
        enrichment: None,
    }
}

fn matches_operator_filter(requested: &str, actual: &str) -> bool {
    requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("MILITARY_OPERATOR_UNSPECIFIED")
        || requested.eq_ignore_ascii_case(actual)
}

fn matches_aircraft_type_filter(requested: &str, actual: &str) -> bool {
    requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("MILITARY_AIRCRAFT_TYPE_UNSPECIFIED")
        || requested.eq_ignore_ascii_case(actual)
}

fn parse_flights_cache_key(request: &ListMilitaryFlightsRequest) -> String {
    let mut key = String::new();
    if let Some(bounding_box) = request.bounding_box.as_ref()
        && let (Some(south_west), Some(north_east)) =
            (&bounding_box.south_west, &bounding_box.north_east)
    {
        key.push_str(&format!(
            "{:.4}:{:.4}:{:.4}:{:.4}:",
            south_west.latitude, south_west.longitude, north_east.latitude, north_east.longitude
        ));
    }
    key.push_str(request.operator.trim());
    key.push(':');
    key.push_str(request.aircraft_type.trim());
    key.push(':');
    key.push_str(&parse_page_size(request).to_string());
    key
}

fn get_flights_cache(key: &str) -> Result<Option<ListMilitaryFlightsResponse>, AppError> {
    let cache = FLIGHTS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("military flights cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_flights_cache(key: String, value: &ListMilitaryFlightsResponse) -> Result<(), AppError> {
    let mut cache = FLIGHTS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("military flights cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        FlightsCacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + FLIGHTS_CACHE_TTL,
        },
    );
    Ok(())
}

fn get_theater_cache() -> Result<Option<GetTheaterPostureResponse>, AppError> {
    let cache = THEATER_CACHE
        .lock()
        .map_err(|_| AppError::Internal("military theater cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_theater_cache(value: &GetTheaterPostureResponse) -> Result<(), AppError> {
    let mut cache = THEATER_CACHE
        .lock()
        .map_err(|_| AppError::Internal("military theater cache lock poisoned".to_string()))?;
    *cache = Some(TheaterCacheEntry {
        value: value.clone(),
        expires_at: Instant::now() + THEATER_CACHE_TTL,
    });
    Ok(())
}

fn get_wingbits_cache(icao24: &str) -> Result<Option<AircraftDetails>, AppError> {
    let cache = WINGBITS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("wingbits cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(icao24)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_wingbits_cache(icao24: String, value: &AircraftDetails) -> Result<(), AppError> {
    let mut cache = WINGBITS_CACHE
        .lock()
        .map_err(|_| AppError::Internal("wingbits cache lock poisoned".to_string()))?;
    cache.insert(
        icao24,
        WingbitsCacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + WINGBITS_CACHE_TTL,
        },
    );
    Ok(())
}

fn map_wingbits_details(icao24: &str, payload: &Value) -> AircraftDetails {
    AircraftDetails {
        icao24: icao24.to_string(),
        registration: string_value(payload.get("registration")),
        manufacturer_icao: string_value(payload.get("manufacturerIcao")),
        manufacturer_name: string_value(payload.get("manufacturerName")),
        model: string_value(payload.get("model")),
        typecode: string_value(payload.get("typecode")),
        serial_number: string_value(payload.get("serialNumber")),
        icao_aircraft_type: string_value(payload.get("icaoAircraftType")),
        operator: string_value(payload.get("operator")),
        operator_callsign: string_value(payload.get("operatorCallsign")),
        operator_icao: string_value(payload.get("operatorIcao")),
        owner: string_value(payload.get("owner")),
        built: string_value(payload.get("built")),
        engines: string_value(payload.get("engines")),
        category_description: string_value(payload.get("categoryDescription")),
    }
}

async fn fetch_wingbits_details(
    state: &AppState,
    icao24: &str,
    api_key: &str,
) -> Option<AircraftDetails> {
    let endpoint = format!(
        "https://customer-api.wingbits.com/v1/flights/details/{}",
        icao24.to_ascii_lowercase()
    );
    let response = state
        .http_client
        .get(endpoint)
        .header("x-api-key", api_key)
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
    Some(map_wingbits_details(icao24, &payload))
}

fn calculate_theater_postures(flights: &[RawFlight], theater_filter: &str) -> Vec<TheaterPosture> {
    THEATERS
        .iter()
        .filter(|theater| {
            theater_filter.trim().is_empty()
                || theater_filter.eq_ignore_ascii_case(theater.id)
                || theater_filter
                    .to_ascii_lowercase()
                    .contains(&theater.id.to_ascii_lowercase())
        })
        .map(|theater| {
            let theater_flights = flights
                .iter()
                .filter(|flight| {
                    flight.lat >= theater.south
                        && flight.lat <= theater.north
                        && flight.lon >= theater.west
                        && flight.lon <= theater.east
                })
                .collect::<Vec<_>>();
            let count = theater_flights.len();
            let tankers = theater_flights
                .iter()
                .filter(|flight| {
                    detect_aircraft_type(&flight.callsign) == "MILITARY_AIRCRAFT_TYPE_TANKER"
                })
                .count();
            let awacs = theater_flights
                .iter()
                .filter(|flight| {
                    detect_aircraft_type(&flight.callsign) == "MILITARY_AIRCRAFT_TYPE_AWACS"
                })
                .count();

            let posture_level = if count >= theater.critical {
                "critical"
            } else if count >= theater.elevated {
                "elevated"
            } else {
                "normal"
            };

            let mut active_operations = Vec::new();
            if tankers > 0 {
                active_operations.push("aerial_refueling".to_string());
            }
            if awacs > 0 {
                active_operations.push("airborne_early_warning".to_string());
            }
            if count >= theater.elevated && tankers > 0 && awacs > 0 {
                active_operations.push("strike_capable".to_string());
            }

            TheaterPosture {
                theater: theater.id.to_string(),
                posture_level: posture_level.to_string(),
                active_flights: count,
                tracked_vessels: 0,
                active_operations,
                assessed_at: now_epoch_ms(),
            }
        })
        .collect()
}

fn strip_html(html: &str) -> String {
    Regex::new(r"<[^>]+>")
        .expect("valid html strip regex")
        .replace_all(html, " ")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&#8217;", "'")
        .replace("&#8220;", "\"")
        .replace("&#8221;", "\"")
        .replace("\n", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn hull_to_type(hull: &str) -> &'static str {
    let uppercase = hull.to_ascii_uppercase();
    if uppercase.starts_with("CVN") || uppercase == "CV" {
        "carrier"
    } else if uppercase.starts_with("DDG") || uppercase.starts_with("CG") {
        "destroyer"
    } else if uppercase.starts_with("LHD")
        || uppercase.starts_with("LHA")
        || uppercase.starts_with("LPD")
        || uppercase.starts_with("LSD")
    {
        "amphibious"
    } else if uppercase.starts_with("SSN")
        || uppercase.starts_with("SSBN")
        || uppercase.starts_with("SSGN")
    {
        "submarine"
    } else if uppercase.starts_with("FFG") || uppercase.starts_with("LCS") {
        "frigate"
    } else if uppercase.starts_with("T-AO")
        || uppercase.starts_with("T-AKE")
        || uppercase.starts_with("T-EPF")
    {
        "auxiliary"
    } else {
        "unknown"
    }
}

fn parse_battle_force_summary(text: &str) -> Option<BattleForceSummary> {
    let number_re = Regex::new(r"(\d{1,3}(?:,\d{3})*)").expect("valid battle force number regex");
    let total =
        Regex::new(r"(?i)(battle[- ]?force|total ships|ships)[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)")
            .expect("valid total regex")
            .captures(text)
            .and_then(|captures| captures.get(2))
            .and_then(|value| value.as_str().replace(',', "").parse::<i64>().ok())
            .or_else(|| {
                number_re
                    .captures(text)
                    .and_then(|captures| captures.get(1))
                    .and_then(|value| value.as_str().replace(',', "").parse::<i64>().ok())
            })?;
    let deployed = Regex::new(r"(?i)deployed[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)")
        .expect("valid deployed regex")
        .captures(text)
        .and_then(|captures| captures.get(1))
        .and_then(|value| value.as_str().replace(',', "").parse::<i64>().ok())
        .unwrap_or(0);
    let underway = Regex::new(r"(?i)underway[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)")
        .expect("valid underway regex")
        .captures(text)
        .and_then(|captures| captures.get(1))
        .and_then(|value| value.as_str().replace(',', "").parse::<i64>().ok())
        .unwrap_or(0);

    Some(BattleForceSummary {
        total_ships: total,
        deployed,
        underway,
    })
}

fn parse_usni_report(
    article_url: &str,
    article_date: &str,
    title: &str,
    html: &str,
) -> USNIFleetReport {
    let heading_re = Regex::new(r"(?is)<h2[^>]*>(.*?)</h2>").expect("valid heading regex");
    let ship_re = Regex::new(r"(?is)\b(USS|USNS)\s+<(?:em|i)>([^<]+)</(?:em|i)>\s*\(([^)]+)\)")
        .expect("valid vessel regex");
    let strike_group_re =
        Regex::new(r"(?is)<h3[^>]*>(.*?)</h3>").expect("valid strike group regex");

    let mut regions = heading_re
        .captures_iter(html)
        .filter_map(|captures| captures.get(1))
        .map(|region| strip_html(region.as_str()))
        .filter(|region| !region.is_empty())
        .collect::<Vec<_>>();
    regions.sort();
    regions.dedup();

    let mut vessels = Vec::new();
    let default_region = regions
        .first()
        .cloned()
        .unwrap_or_else(|| "Unknown".to_string());
    for captures in ship_re.captures_iter(html) {
        let prefix = captures.get(1).map(|value| value.as_str()).unwrap_or("USS");
        let name = captures
            .get(2)
            .map(|value| strip_html(value.as_str()))
            .unwrap_or_default();
        let hull = captures
            .get(3)
            .map(|value| strip_html(value.as_str()))
            .unwrap_or_default();
        if name.is_empty() || hull.is_empty() {
            continue;
        }
        vessels.push(USNIVessel {
            name: format!("{prefix} {name}"),
            hull_number: hull.clone(),
            vessel_type: hull_to_type(&hull).to_string(),
            region: default_region.clone(),
            region_lat: 0.0,
            region_lon: 0.0,
            deployment_status: "unknown".to_string(),
            home_port: String::new(),
            strike_group: String::new(),
            activity_description: String::new(),
            article_url: article_url.to_string(),
            article_date: article_date.to_string(),
        });
    }

    let strike_groups = strike_group_re
        .captures_iter(html)
        .filter_map(|captures| captures.get(1))
        .map(|value| strip_html(value.as_str()))
        .filter(|name| !name.is_empty())
        .map(|name| USNIStrikeGroup {
            name,
            carrier: String::new(),
            air_wing: String::new(),
            destroyer_squadron: String::new(),
            escorts: Vec::new(),
        })
        .collect::<Vec<_>>();

    let text = strip_html(html);
    let summary = parse_battle_force_summary(&text);

    USNIFleetReport {
        article_url: article_url.to_string(),
        article_date: article_date.to_string(),
        article_title: title.to_string(),
        battle_force_summary: summary,
        vessels,
        strike_groups,
        regions,
        parsing_warnings: Vec::new(),
        timestamp: now_epoch_ms(),
    }
}

fn get_usni_cache() -> Result<Option<USNIFleetReport>, AppError> {
    let cache = USNI_CACHE
        .lock()
        .map_err(|_| AppError::Internal("usni cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn get_usni_stale_cache() -> Result<Option<USNIFleetReport>, AppError> {
    let cache = USNI_STALE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("usni stale cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref()
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_usni_cache(report: &USNIFleetReport) -> Result<(), AppError> {
    {
        let mut cache = USNI_CACHE
            .lock()
            .map_err(|_| AppError::Internal("usni cache lock poisoned".to_string()))?;
        *cache = Some(UsniCacheEntry {
            value: report.clone(),
            expires_at: Instant::now() + USNI_CACHE_TTL,
        });
    }
    let mut stale_cache = USNI_STALE_CACHE
        .lock()
        .map_err(|_| AppError::Internal("usni stale cache lock poisoned".to_string()))?;
    *stale_cache = Some(UsniCacheEntry {
        value: report.clone(),
        expires_at: Instant::now() + USNI_STALE_TTL,
    });
    Ok(())
}

pub async fn list_military_flights(
    State(state): State<AppState>,
    Json(request): Json<ListMilitaryFlightsRequest>,
) -> Result<Json<ListMilitaryFlightsResponse>, AppError> {
    if request
        .bounding_box
        .as_ref()
        .and_then(|bbox| bbox.south_west.as_ref())
        .is_none()
        || request
            .bounding_box
            .as_ref()
            .and_then(|bbox| bbox.north_east.as_ref())
            .is_none()
    {
        return Ok(Json(ListMilitaryFlightsResponse {
            flights: Vec::new(),
            clusters: Vec::new(),
            pagination: Some(PaginationResponse {
                next_cursor: String::new(),
                total_count: 0,
            }),
        }));
    }

    let cache_key = parse_flights_cache_key(&request);
    if let Some(cached) = get_flights_cache(&cache_key)? {
        return Ok(Json(cached));
    }

    let limit = parse_page_size(&request);
    let mut flights = fetch_opensky_flights(&state, request.bounding_box.as_ref())
        .await
        .into_iter()
        .map(|raw| to_military_flight(&raw))
        .collect::<Vec<_>>();
    flights.retain(|flight| matches_operator_filter(&request.operator, &flight.operator));
    flights.retain(|flight| {
        matches_aircraft_type_filter(&request.aircraft_type, &flight.aircraft_type)
    });
    flights.sort_by(|a, b| b.last_seen_at.cmp(&a.last_seen_at));

    let total_count = flights.len();
    flights.truncate(limit);

    let response = ListMilitaryFlightsResponse {
        flights,
        clusters: Vec::new(),
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };
    if !response.flights.is_empty() {
        set_flights_cache(cache_key, &response)?;
    }
    Ok(Json(response))
}

pub async fn get_theater_posture(
    State(state): State<AppState>,
    Json(request): Json<GetTheaterPostureRequest>,
) -> Result<Json<GetTheaterPostureResponse>, AppError> {
    if request.theater.trim().is_empty()
        && let Some(cached) = get_theater_cache()?
    {
        return Ok(Json(cached));
    }

    let flights = fetch_opensky_flights(&state, None).await;
    let response = GetTheaterPostureResponse {
        theaters: calculate_theater_postures(&flights, &request.theater),
    };
    if request.theater.trim().is_empty() {
        set_theater_cache(&response)?;
    }
    Ok(Json(response))
}

pub async fn get_aircraft_details(
    State(state): State<AppState>,
    Json(request): Json<GetAircraftDetailsRequest>,
) -> Result<Json<GetAircraftDetailsResponse>, AppError> {
    let api_key = std::env::var("WINGBITS_API_KEY").unwrap_or_default();
    if api_key.trim().is_empty() {
        return Ok(Json(GetAircraftDetailsResponse {
            details: None,
            configured: false,
        }));
    }

    let icao24 = request.icao24.trim().to_ascii_lowercase();
    if icao24.is_empty() {
        return Ok(Json(GetAircraftDetailsResponse {
            details: None,
            configured: true,
        }));
    }

    if let Some(cached) = get_wingbits_cache(&icao24)? {
        return Ok(Json(GetAircraftDetailsResponse {
            details: Some(cached),
            configured: true,
        }));
    }

    let details = fetch_wingbits_details(&state, &icao24, &api_key).await;
    if let Some(details) = details.clone() {
        set_wingbits_cache(icao24, &details)?;
    }

    Ok(Json(GetAircraftDetailsResponse {
        details,
        configured: true,
    }))
}

pub async fn get_aircraft_details_batch(
    State(state): State<AppState>,
    Json(request): Json<GetAircraftDetailsBatchRequest>,
) -> Result<Json<GetAircraftDetailsBatchResponse>, AppError> {
    let api_key = std::env::var("WINGBITS_API_KEY").unwrap_or_default();
    if api_key.trim().is_empty() {
        return Ok(Json(GetAircraftDetailsBatchResponse {
            results: HashMap::new(),
            fetched: 0,
            requested: 0,
            configured: false,
        }));
    }

    let requested = request
        .icao24s
        .iter()
        .map(|icao| icao.trim().to_ascii_lowercase())
        .filter(|icao| !icao.is_empty())
        .take(20)
        .collect::<Vec<_>>();

    let mut results = HashMap::new();
    let mut pending = Vec::new();

    for icao24 in requested.iter() {
        if let Some(cached) = get_wingbits_cache(icao24)? {
            results.insert(icao24.clone(), cached);
        } else {
            pending.push(icao24.clone());
        }
    }

    let tasks = pending
        .iter()
        .map(|icao24| fetch_wingbits_details(&state, icao24, &api_key));
    let fetched = join_all(tasks).await;
    for (icao24, details) in pending.into_iter().zip(fetched.into_iter()) {
        if let Some(details) = details {
            set_wingbits_cache(icao24.clone(), &details)?;
            results.insert(icao24, details);
        }
    }

    Ok(Json(GetAircraftDetailsBatchResponse {
        fetched: results.len(),
        requested: requested.len(),
        configured: true,
        results,
    }))
}

pub async fn get_wingbits_status(
    State(_state): State<AppState>,
    Json(_request): Json<GetWingbitsStatusRequest>,
) -> Result<Json<GetWingbitsStatusResponse>, AppError> {
    let configured = std::env::var("WINGBITS_API_KEY")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    Ok(Json(GetWingbitsStatusResponse { configured }))
}

pub async fn get_usni_fleet_report(
    State(state): State<AppState>,
    Json(request): Json<GetUSNIFleetReportRequest>,
) -> Result<Json<GetUSNIFleetReportResponse>, AppError> {
    if !request.force_refresh
        && let Some(cached) = get_usni_cache()?
    {
        return Ok(Json(GetUSNIFleetReportResponse {
            report: Some(cached),
            cached: true,
            stale: false,
            error: String::new(),
        }));
    }

    let endpoint = "https://news.usni.org/wp-json/wp/v2/posts?categories=4137&per_page=1";
    let response = state
        .http_client
        .get(endpoint)
        .header("Accept", "application/json")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        .send()
        .await;

    let report = match response {
        Ok(response) if response.status().is_success() => {
            let payload = response.json::<Value>().await.ok();
            payload.and_then(|value| value.as_array().cloned())
        }
        _ => None,
    };

    if let Some(posts) = report
        && let Some(post) = posts.first()
    {
        let article_url = string_value(post.get("link"));
        let article_date = string_value(post.get("date"));
        let article_title = strip_html(
            post.get("title")
                .and_then(|title| title.get("rendered"))
                .and_then(Value::as_str)
                .unwrap_or("USNI Fleet Tracker"),
        );
        let html = post
            .get("content")
            .and_then(|content| content.get("rendered"))
            .and_then(Value::as_str)
            .unwrap_or_default();

        if !html.trim().is_empty() {
            let parsed = parse_usni_report(&article_url, &article_date, &article_title, html);
            set_usni_cache(&parsed)?;
            return Ok(Json(GetUSNIFleetReportResponse {
                report: Some(parsed),
                cached: false,
                stale: false,
                error: String::new(),
            }));
        }
    }

    if let Some(stale) = get_usni_stale_cache()? {
        return Ok(Json(GetUSNIFleetReportResponse {
            report: Some(stale),
            cached: true,
            stale: true,
            error: "Using cached data".to_string(),
        }));
    }

    Ok(Json(GetUSNIFleetReportResponse {
        report: None,
        cached: false,
        stale: false,
        error: "No USNI fleet report available".to_string(),
    }))
}
