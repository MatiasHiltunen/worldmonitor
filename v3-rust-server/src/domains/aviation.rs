use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, extract::State};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListAirportDelaysRequest {
    #[serde(default)]
    pub pagination: Option<PaginationRequest>,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub min_severity: String,
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
pub struct ListAirportDelaysResponse {
    pub alerts: Vec<AirportDelayAlert>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationResponse>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AirportDelayAlert {
    pub id: String,
    pub iata: String,
    pub icao: String,
    pub name: String,
    pub city: String,
    pub country: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
    pub region: String,
    pub delay_type: String,
    pub severity: String,
    pub avg_delay_minutes: u32,
    pub delayed_flights_pct: u32,
    pub cancelled_flights: u32,
    pub total_flights: u32,
    pub reason: String,
    pub source: String,
    pub updated_at: i64,
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
    value: ListAirportDelaysResponse,
    expires_at: Instant,
}

static AVIATION_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy)]
struct AirportSeed {
    iata: &'static str,
    icao: &'static str,
    name: &'static str,
    city: &'static str,
    country: &'static str,
    latitude: f64,
    longitude: f64,
    region: &'static str,
}

const AIRPORTS: &[AirportSeed] = &[
    AirportSeed {
        iata: "JFK",
        icao: "KJFK",
        name: "John F. Kennedy International",
        city: "New York",
        country: "USA",
        latitude: 40.6413,
        longitude: -73.7781,
        region: "AIRPORT_REGION_AMERICAS",
    },
    AirportSeed {
        iata: "LAX",
        icao: "KLAX",
        name: "Los Angeles International",
        city: "Los Angeles",
        country: "USA",
        latitude: 33.9416,
        longitude: -118.4085,
        region: "AIRPORT_REGION_AMERICAS",
    },
    AirportSeed {
        iata: "ORD",
        icao: "KORD",
        name: "Chicago O'Hare International",
        city: "Chicago",
        country: "USA",
        latitude: 41.9742,
        longitude: -87.9073,
        region: "AIRPORT_REGION_AMERICAS",
    },
    AirportSeed {
        iata: "LHR",
        icao: "EGLL",
        name: "London Heathrow",
        city: "London",
        country: "UK",
        latitude: 51.47,
        longitude: -0.4543,
        region: "AIRPORT_REGION_EUROPE",
    },
    AirportSeed {
        iata: "CDG",
        icao: "LFPG",
        name: "Paris Charles de Gaulle",
        city: "Paris",
        country: "France",
        latitude: 49.0097,
        longitude: 2.5479,
        region: "AIRPORT_REGION_EUROPE",
    },
    AirportSeed {
        iata: "FRA",
        icao: "EDDF",
        name: "Frankfurt Airport",
        city: "Frankfurt",
        country: "Germany",
        latitude: 50.0379,
        longitude: 8.5622,
        region: "AIRPORT_REGION_EUROPE",
    },
    AirportSeed {
        iata: "AMS",
        icao: "EHAM",
        name: "Amsterdam Schiphol",
        city: "Amsterdam",
        country: "Netherlands",
        latitude: 52.3105,
        longitude: 4.7683,
        region: "AIRPORT_REGION_EUROPE",
    },
    AirportSeed {
        iata: "DXB",
        icao: "OMDB",
        name: "Dubai International",
        city: "Dubai",
        country: "UAE",
        latitude: 25.2532,
        longitude: 55.3657,
        region: "AIRPORT_REGION_MENA",
    },
    AirportSeed {
        iata: "DOH",
        icao: "OTHH",
        name: "Hamad International",
        city: "Doha",
        country: "Qatar",
        latitude: 25.2731,
        longitude: 51.6081,
        region: "AIRPORT_REGION_MENA",
    },
    AirportSeed {
        iata: "TLV",
        icao: "LLBG",
        name: "Ben Gurion Airport",
        city: "Tel Aviv",
        country: "Israel",
        latitude: 32.0114,
        longitude: 34.8867,
        region: "AIRPORT_REGION_MENA",
    },
    AirportSeed {
        iata: "CAI",
        icao: "HECA",
        name: "Cairo International",
        city: "Cairo",
        country: "Egypt",
        latitude: 30.1219,
        longitude: 31.4056,
        region: "AIRPORT_REGION_MENA",
    },
    AirportSeed {
        iata: "JNB",
        icao: "FAOR",
        name: "OR Tambo International",
        city: "Johannesburg",
        country: "South Africa",
        latitude: -26.1337,
        longitude: 28.242,
        region: "AIRPORT_REGION_AFRICA",
    },
    AirportSeed {
        iata: "NBO",
        icao: "HKJK",
        name: "Jomo Kenyatta International",
        city: "Nairobi",
        country: "Kenya",
        latitude: -1.3192,
        longitude: 36.9278,
        region: "AIRPORT_REGION_AFRICA",
    },
    AirportSeed {
        iata: "ADD",
        icao: "HAAB",
        name: "Addis Ababa Bole International",
        city: "Addis Ababa",
        country: "Ethiopia",
        latitude: 8.9779,
        longitude: 38.7993,
        region: "AIRPORT_REGION_AFRICA",
    },
    AirportSeed {
        iata: "HND",
        icao: "RJTT",
        name: "Tokyo Haneda",
        city: "Tokyo",
        country: "Japan",
        latitude: 35.5494,
        longitude: 139.7798,
        region: "AIRPORT_REGION_APAC",
    },
    AirportSeed {
        iata: "NRT",
        icao: "RJAA",
        name: "Narita International",
        city: "Tokyo",
        country: "Japan",
        latitude: 35.772,
        longitude: 140.3929,
        region: "AIRPORT_REGION_APAC",
    },
    AirportSeed {
        iata: "SIN",
        icao: "WSSS",
        name: "Singapore Changi",
        city: "Singapore",
        country: "Singapore",
        latitude: 1.3644,
        longitude: 103.9915,
        region: "AIRPORT_REGION_APAC",
    },
    AirportSeed {
        iata: "ICN",
        icao: "RKSI",
        name: "Incheon International",
        city: "Seoul",
        country: "South Korea",
        latitude: 37.4602,
        longitude: 126.4407,
        region: "AIRPORT_REGION_APAC",
    },
];

fn now_epoch_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn severity_rank(severity: &str) -> usize {
    match severity {
        "FLIGHT_DELAY_SEVERITY_NORMAL" => 1,
        "FLIGHT_DELAY_SEVERITY_MINOR" => 2,
        "FLIGHT_DELAY_SEVERITY_MODERATE" => 3,
        "FLIGHT_DELAY_SEVERITY_MAJOR" => 4,
        "FLIGHT_DELAY_SEVERITY_SEVERE" => 5,
        _ => 0,
    }
}

fn classify_severity(delay_minutes: u32) -> &'static str {
    if delay_minutes >= 75 {
        "FLIGHT_DELAY_SEVERITY_SEVERE"
    } else if delay_minutes >= 55 {
        "FLIGHT_DELAY_SEVERITY_MAJOR"
    } else if delay_minutes >= 35 {
        "FLIGHT_DELAY_SEVERITY_MODERATE"
    } else if delay_minutes >= 15 {
        "FLIGHT_DELAY_SEVERITY_MINOR"
    } else {
        "FLIGHT_DELAY_SEVERITY_NORMAL"
    }
}

fn parse_delay_type(seed: u64) -> (&'static str, &'static str) {
    match seed % 5 {
        0 => (
            "FLIGHT_DELAY_TYPE_GROUND_STOP",
            "Air traffic control ground stop",
        ),
        1 => (
            "FLIGHT_DELAY_TYPE_GROUND_DELAY",
            "Ground delay program due to congestion",
        ),
        2 => (
            "FLIGHT_DELAY_TYPE_DEPARTURE_DELAY",
            "Departure backlog from weather impacts",
        ),
        3 => (
            "FLIGHT_DELAY_TYPE_ARRIVAL_DELAY",
            "Arrival sequencing constraints",
        ),
        _ => ("FLIGHT_DELAY_TYPE_GENERAL", "General operational delay"),
    }
}

fn page_size(request: &ListAirportDelaysRequest) -> usize {
    request
        .pagination
        .as_ref()
        .map(|pagination| pagination.page_size)
        .filter(|size| *size > 0)
        .unwrap_or(500)
        .min(1_000)
}

fn should_include_region(requested_region: &str, airport_region: &str) -> bool {
    let requested = requested_region.trim();
    requested.is_empty()
        || requested.eq_ignore_ascii_case("AIRPORT_REGION_UNSPECIFIED")
        || requested.eq_ignore_ascii_case(airport_region)
}

fn min_severity_rank(requested: &str) -> usize {
    if requested.trim().is_empty()
        || requested.eq_ignore_ascii_case("FLIGHT_DELAY_SEVERITY_UNSPECIFIED")
    {
        return 0;
    }
    severity_rank(requested.trim())
}

fn synthesize_alert(seed: &AirportSeed, slot: i64, now_ms: i64) -> Option<AirportDelayAlert> {
    let entropy = stable_hash(&format!("{}:{slot}", seed.iata));
    let probability = (entropy % 100) as u32;
    if probability > 40 {
        return None;
    }

    let avg_delay_minutes = 12 + ((entropy >> 11) % 95) as u32;
    let severity = classify_severity(avg_delay_minutes);
    if severity == "FLIGHT_DELAY_SEVERITY_NORMAL" {
        return None;
    }

    let (delay_type, reason) = parse_delay_type(entropy >> 23);
    let total_flights = 180 + ((entropy >> 31) % 220) as u32;
    let delayed_flights_pct = (avg_delay_minutes / 2).min(95);
    let cancelled_flights = if severity_rank(severity) >= 4 {
        ((entropy >> 43) % 12) as u32
    } else {
        ((entropy >> 43) % 4) as u32
    };

    Some(AirportDelayAlert {
        id: format!("sim-{}-{slot}", seed.iata.to_ascii_lowercase()),
        iata: seed.iata.to_string(),
        icao: seed.icao.to_string(),
        name: seed.name.to_string(),
        city: seed.city.to_string(),
        country: seed.country.to_string(),
        location: Some(GeoCoordinates {
            latitude: seed.latitude,
            longitude: seed.longitude,
        }),
        region: seed.region.to_string(),
        delay_type: delay_type.to_string(),
        severity: severity.to_string(),
        avg_delay_minutes,
        delayed_flights_pct,
        cancelled_flights,
        total_flights,
        reason: reason.to_string(),
        source: "FLIGHT_DELAY_SOURCE_COMPUTED".to_string(),
        updated_at: now_ms,
    })
}

fn get_cache(key: &str) -> Result<Option<ListAirportDelaysResponse>, AppError> {
    let cache = AVIATION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("aviation cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cache(key: String, value: &ListAirportDelaysResponse) -> Result<(), AppError> {
    let mut cache = AVIATION_CACHE
        .lock()
        .map_err(|_| AppError::Internal("aviation cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        },
    );
    Ok(())
}

pub async fn list_airport_delays(
    State(_state): State<AppState>,
    Json(request): Json<ListAirportDelaysRequest>,
) -> Result<Json<ListAirportDelaysResponse>, AppError> {
    let now_ms = now_epoch_ms();
    let slot = now_ms / 900_000;
    let size = page_size(&request);
    let min_rank = min_severity_rank(&request.min_severity);
    let region = request.region.trim().to_ascii_uppercase();

    let cache_key = format!("{slot}:{size}:{region}:{min_rank}");
    if let Some(cached) = get_cache(&cache_key)? {
        return Ok(Json(cached));
    }

    let mut alerts = AIRPORTS
        .iter()
        .filter(|airport| should_include_region(&region, airport.region))
        .filter_map(|airport| synthesize_alert(airport, slot, now_ms))
        .filter(|alert| severity_rank(&alert.severity) >= min_rank)
        .collect::<Vec<_>>();

    alerts.sort_by(|a, b| {
        severity_rank(&b.severity)
            .cmp(&severity_rank(&a.severity))
            .then_with(|| b.avg_delay_minutes.cmp(&a.avg_delay_minutes))
            .then_with(|| a.iata.cmp(&b.iata))
    });

    let total_count = alerts.len();
    alerts.truncate(size);

    let response = ListAirportDelaysResponse {
        alerts,
        pagination: Some(PaginationResponse {
            next_cursor: String::new(),
            total_count,
        }),
    };

    set_cache(cache_key, &response)?;
    Ok(Json(response))
}
