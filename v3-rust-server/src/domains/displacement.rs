use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::{Datelike, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{AppState, error::AppError};

const CACHE_TTL: Duration = Duration::from_secs(43_200);

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetDisplacementSummaryRequest {
    #[serde(default)]
    pub year: i32,
    #[serde(default)]
    pub country_limit: usize,
    #[serde(default)]
    pub flow_limit: usize,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetDisplacementSummaryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<DisplacementSummary>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DisplacementSummary {
    pub year: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_totals: Option<GlobalDisplacementTotals>,
    pub countries: Vec<CountryDisplacement>,
    pub top_flows: Vec<DisplacementFlow>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GlobalDisplacementTotals {
    pub refugees: i64,
    pub asylum_seekers: i64,
    pub idps: i64,
    pub stateless: i64,
    pub total: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CountryDisplacement {
    pub code: String,
    pub name: String,
    pub refugees: i64,
    pub asylum_seekers: i64,
    pub idps: i64,
    pub stateless: i64,
    pub total_displaced: i64,
    pub host_refugees: i64,
    pub host_asylum_seekers: i64,
    pub host_total: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<GeoCoordinates>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DisplacementFlow {
    pub origin_code: String,
    pub origin_name: String,
    pub asylum_code: String,
    pub asylum_name: String,
    pub refugees: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_location: Option<GeoCoordinates>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asylum_location: Option<GeoCoordinates>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetPopulationExposureRequest {
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub lat: f64,
    #[serde(default)]
    pub lon: f64,
    #[serde(default)]
    pub radius: f64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetPopulationExposureResponse {
    pub success: bool,
    pub countries: Vec<CountryPopulationEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exposure: Option<ExposureResult>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CountryPopulationEntry {
    pub code: String,
    pub name: String,
    pub population: i64,
    pub density_per_km2: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExposureResult {
    pub exposed_population: i64,
    pub exposure_radius_km: f64,
    pub nearest_country: String,
    pub density_per_km2: i64,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct UnhcrRawItem {
    #[serde(default)]
    coo_iso: Option<String>,
    #[serde(default)]
    coo_name: Option<String>,
    #[serde(default)]
    coa_iso: Option<String>,
    #[serde(default)]
    coa_name: Option<String>,
    #[serde(default)]
    refugees: Option<f64>,
    #[serde(default)]
    asylum_seekers: Option<f64>,
    #[serde(default)]
    idps: Option<f64>,
    #[serde(default)]
    stateless: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct UnhcrPage {
    #[serde(default)]
    items: Vec<UnhcrRawItem>,
    #[serde(default)]
    max_pages: i32,
}

#[derive(Clone)]
struct CacheEntry {
    value: GetDisplacementSummaryResponse,
    expires_at: Instant,
}

static DISPLACEMENT_CACHE: Lazy<Mutex<HashMap<String, CacheEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Default, Clone)]
struct OriginAgg {
    name: String,
    refugees: i64,
    asylum_seekers: i64,
    idps: i64,
    stateless: i64,
}

#[derive(Default, Clone)]
struct AsylumAgg {
    name: String,
    refugees: i64,
    asylum_seekers: i64,
}

#[derive(Default, Clone)]
struct FlowAgg {
    origin_code: String,
    origin_name: String,
    asylum_code: String,
    asylum_name: String,
    refugees: i64,
}

fn to_i64(value: Option<f64>) -> i64 {
    value.unwrap_or(0.0).round().max(0.0) as i64
}

fn get_cache(key: &str) -> Result<Option<GetDisplacementSummaryResponse>, AppError> {
    let cache = DISPLACEMENT_CACHE
        .lock()
        .map_err(|_| AppError::Internal("displacement cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get(key)
        && Instant::now() <= entry.expires_at
    {
        return Ok(Some(entry.value.clone()));
    }
    Ok(None)
}

fn set_cache(key: String, value: &GetDisplacementSummaryResponse) -> Result<(), AppError> {
    let mut cache = DISPLACEMENT_CACHE
        .lock()
        .map_err(|_| AppError::Internal("displacement cache lock poisoned".to_string()))?;
    cache.insert(
        key,
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        },
    );
    Ok(())
}

fn country_centroid(code: &str) -> Option<GeoCoordinates> {
    let (latitude, longitude) = match code {
        "AFG" => (33.9, 67.7),
        "SYR" => (35.0, 38.0),
        "UKR" => (48.4, 31.2),
        "SDN" => (15.5, 32.5),
        "SSD" => (6.9, 31.3),
        "SOM" => (5.2, 46.2),
        "COD" => (-4.0, 21.8),
        "MMR" => (19.8, 96.7),
        "YEM" => (15.6, 48.5),
        "ETH" => (9.1, 40.5),
        "VEN" => (6.4, -66.6),
        "IRQ" => (33.2, 43.7),
        "COL" => (4.6, -74.1),
        "NGA" => (9.1, 7.5),
        "PSE" => (31.9, 35.2),
        "TUR" => (39.9, 32.9),
        "DEU" => (51.2, 10.4),
        "PAK" => (30.4, 69.3),
        "UGA" => (1.4, 32.3),
        "BGD" => (23.7, 90.4),
        "KEN" => (0.0, 38.0),
        "TCD" => (15.5, 19.0),
        "JOR" => (31.0, 36.0),
        "LBN" => (33.9, 35.5),
        "EGY" => (26.8, 30.8),
        "IRN" => (32.4, 53.7),
        "TZA" => (-6.4, 34.9),
        "RWA" => (-1.9, 29.9),
        "CMR" => (7.4, 12.4),
        "MLI" => (17.6, -4.0),
        "BFA" => (12.3, -1.6),
        "NER" => (17.6, 8.1),
        "CAF" => (6.6, 20.9),
        "MOZ" => (-18.7, 35.5),
        "USA" => (37.1, -95.7),
        "FRA" => (46.2, 2.2),
        "GBR" => (55.4, -3.4),
        "IND" => (20.6, 79.0),
        "CHN" => (35.9, 104.2),
        "RUS" => (61.5, 105.3),
        _ => return None,
    };
    Some(GeoCoordinates {
        latitude,
        longitude,
    })
}

async fn fetch_unhcr_year_items(state: &AppState, year: i32) -> Option<Vec<UnhcrRawItem>> {
    if year <= 0 {
        return None;
    }
    let limit = 10_000;
    let max_page_guard = 25;
    let mut items = Vec::new();

    for page in 1..=max_page_guard {
        let mut url =
            reqwest::Url::parse("https://api.unhcr.org/population/v1/population/").ok()?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("year", &year.to_string());
            query.append_pair("limit", &limit.to_string());
            query.append_pair("page", &page.to_string());
        }

        let response = state
            .http_client
            .get(url)
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

        let payload = response.json::<UnhcrPage>().await.ok()?;
        if payload.items.is_empty() {
            break;
        }
        items.extend(payload.items);
        if payload.max_pages > 0 && page >= payload.max_pages {
            break;
        }
        if items.len() < (page as usize * limit) {
            break;
        }
    }

    Some(items)
}

fn empty_summary(year: i32) -> GetDisplacementSummaryResponse {
    GetDisplacementSummaryResponse {
        summary: Some(DisplacementSummary {
            year,
            global_totals: Some(GlobalDisplacementTotals {
                refugees: 0,
                asylum_seekers: 0,
                idps: 0,
                stateless: 0,
                total: 0,
            }),
            countries: Vec::new(),
            top_flows: Vec::new(),
        }),
    }
}

pub async fn get_displacement_summary(
    State(state): State<AppState>,
    Json(request): Json<GetDisplacementSummaryRequest>,
) -> Result<Json<GetDisplacementSummaryResponse>, AppError> {
    let current_year = Utc::now().year();
    let requested_year = if request.year > 0 { request.year } else { 0 };
    let cache_key = format!(
        "{}:{}:{}",
        if requested_year > 0 {
            requested_year
        } else {
            current_year
        },
        request.country_limit,
        request.flow_limit
    );
    if let Some(cached) = get_cache(&cache_key)? {
        return Ok(Json(cached));
    }

    let mut data_year_used = if requested_year > 0 {
        requested_year
    } else {
        current_year
    };
    let mut raw_items = Vec::new();

    if requested_year > 0 {
        if let Some(items) = fetch_unhcr_year_items(&state, requested_year).await
            && !items.is_empty()
        {
            raw_items = items;
        }
    } else {
        for year in (current_year - 2..=current_year).rev() {
            let Some(items) = fetch_unhcr_year_items(&state, year).await else {
                continue;
            };
            if items.is_empty() {
                continue;
            }
            raw_items = items;
            data_year_used = year;
            break;
        }
    }

    if raw_items.is_empty() {
        let response = empty_summary(data_year_used);
        return Ok(Json(response));
    }

    let mut by_origin = HashMap::<String, OriginAgg>::new();
    let mut by_asylum = HashMap::<String, AsylumAgg>::new();
    let mut flow_map = HashMap::<String, FlowAgg>::new();

    let mut total_refugees = 0i64;
    let mut total_asylum_seekers = 0i64;
    let mut total_idps = 0i64;
    let mut total_stateless = 0i64;

    for item in raw_items {
        let origin_code = item.coo_iso.unwrap_or_default();
        let asylum_code = item.coa_iso.unwrap_or_default();
        let refugees = to_i64(item.refugees);
        let asylum_seekers = to_i64(item.asylum_seekers);
        let idps = to_i64(item.idps);
        let stateless = to_i64(item.stateless);

        total_refugees += refugees;
        total_asylum_seekers += asylum_seekers;
        total_idps += idps;
        total_stateless += stateless;

        if !origin_code.is_empty() {
            let entry = by_origin
                .entry(origin_code.clone())
                .or_insert_with(|| OriginAgg {
                    name: item.coo_name.clone().unwrap_or_else(|| origin_code.clone()),
                    ..OriginAgg::default()
                });
            entry.refugees += refugees;
            entry.asylum_seekers += asylum_seekers;
            entry.idps += idps;
            entry.stateless += stateless;
        }

        if !asylum_code.is_empty() {
            let entry = by_asylum
                .entry(asylum_code.clone())
                .or_insert_with(|| AsylumAgg {
                    name: item.coa_name.clone().unwrap_or_else(|| asylum_code.clone()),
                    ..AsylumAgg::default()
                });
            entry.refugees += refugees;
            entry.asylum_seekers += asylum_seekers;
        }

        if !origin_code.is_empty() && !asylum_code.is_empty() && refugees > 0 {
            let flow_key = format!("{origin_code}->{asylum_code}");
            let flow_entry = flow_map.entry(flow_key).or_insert_with(|| FlowAgg {
                origin_code: origin_code.clone(),
                origin_name: item.coo_name.clone().unwrap_or_else(|| origin_code.clone()),
                asylum_code: asylum_code.clone(),
                asylum_name: item.coa_name.clone().unwrap_or_else(|| asylum_code.clone()),
                ..FlowAgg::default()
            });
            flow_entry.refugees += refugees;
        }
    }

    let mut countries = HashMap::<String, CountryDisplacement>::new();
    for (code, data) in by_origin {
        countries.insert(
            code.clone(),
            CountryDisplacement {
                code: code.clone(),
                name: data.name,
                refugees: data.refugees,
                asylum_seekers: data.asylum_seekers,
                idps: data.idps,
                stateless: data.stateless,
                total_displaced: data.refugees + data.asylum_seekers + data.idps + data.stateless,
                host_refugees: 0,
                host_asylum_seekers: 0,
                host_total: 0,
                location: country_centroid(&code),
            },
        );
    }

    for (code, data) in by_asylum {
        let host_refugees = data.refugees;
        let host_asylum_seekers = data.asylum_seekers;
        let host_total = host_refugees + host_asylum_seekers;

        countries
            .entry(code.clone())
            .and_modify(|entry| {
                entry.host_refugees = host_refugees;
                entry.host_asylum_seekers = host_asylum_seekers;
                entry.host_total = host_total;
            })
            .or_insert(CountryDisplacement {
                code: code.clone(),
                name: data.name,
                refugees: 0,
                asylum_seekers: 0,
                idps: 0,
                stateless: 0,
                total_displaced: 0,
                host_refugees,
                host_asylum_seekers,
                host_total,
                location: country_centroid(&code),
            });
    }

    let mut country_rows = countries.into_values().collect::<Vec<_>>();
    country_rows.sort_by(|a, b| {
        let a_size = a.total_displaced.max(a.host_total);
        let b_size = b.total_displaced.max(b.host_total);
        b_size.cmp(&a_size)
    });
    if request.country_limit > 0 {
        country_rows.truncate(request.country_limit);
    }

    let mut flows = flow_map.into_values().collect::<Vec<_>>();
    flows.sort_by(|a, b| b.refugees.cmp(&a.refugees));
    let flow_limit = if request.flow_limit > 0 {
        request.flow_limit
    } else {
        50
    };
    let top_flows = flows
        .into_iter()
        .take(flow_limit)
        .map(|flow| DisplacementFlow {
            origin_code: flow.origin_code.clone(),
            origin_name: flow.origin_name,
            asylum_code: flow.asylum_code.clone(),
            asylum_name: flow.asylum_name,
            refugees: flow.refugees,
            origin_location: country_centroid(&flow.origin_code),
            asylum_location: country_centroid(&flow.asylum_code),
        })
        .collect::<Vec<_>>();

    let response = GetDisplacementSummaryResponse {
        summary: Some(DisplacementSummary {
            year: data_year_used,
            global_totals: Some(GlobalDisplacementTotals {
                refugees: total_refugees,
                asylum_seekers: total_asylum_seekers,
                idps: total_idps,
                stateless: total_stateless,
                total: total_refugees + total_asylum_seekers + total_idps + total_stateless,
            }),
            countries: country_rows,
            top_flows,
        }),
    };

    set_cache(cache_key, &response)?;
    Ok(Json(response))
}

const PRIORITY_COUNTRIES: &[(&str, &str, i64, f64)] = &[
    ("UKR", "Ukraine", 37_000_000, 603_550.0),
    ("RUS", "Russia", 144_100_000, 17_098_242.0),
    ("ISR", "Israel", 9_800_000, 22_072.0),
    ("PSE", "Palestine", 5_400_000, 6_020.0),
    ("SYR", "Syria", 22_100_000, 185_180.0),
    ("IRN", "Iran", 88_600_000, 1_648_195.0),
    ("TWN", "Taiwan", 23_600_000, 36_193.0),
    ("ETH", "Ethiopia", 126_500_000, 1_104_300.0),
    ("SDN", "Sudan", 48_100_000, 1_861_484.0),
    ("SSD", "South Sudan", 11_400_000, 619_745.0),
    ("SOM", "Somalia", 18_100_000, 637_657.0),
    ("YEM", "Yemen", 34_400_000, 527_968.0),
    ("AFG", "Afghanistan", 42_200_000, 652_230.0),
    ("PAK", "Pakistan", 240_500_000, 881_913.0),
    ("IND", "India", 1_428_600_000, 3_287_263.0),
    ("MMR", "Myanmar", 54_200_000, 676_578.0),
    ("COD", "DR Congo", 102_300_000, 2_344_858.0),
    ("NGA", "Nigeria", 223_800_000, 923_768.0),
    ("MLI", "Mali", 22_600_000, 1_240_192.0),
    ("BFA", "Burkina Faso", 22_700_000, 274_200.0),
];

const EXPOSURE_CENTROIDS: &[(&str, f64, f64)] = &[
    ("UKR", 48.4, 31.2),
    ("RUS", 61.5, 105.3),
    ("ISR", 31.0, 34.8),
    ("PSE", 31.9, 35.2),
    ("SYR", 35.0, 38.0),
    ("IRN", 32.4, 53.7),
    ("TWN", 23.7, 121.0),
    ("ETH", 9.1, 40.5),
    ("SDN", 15.5, 32.5),
    ("SSD", 6.9, 31.3),
    ("SOM", 5.2, 46.2),
    ("YEM", 15.6, 48.5),
    ("AFG", 33.9, 67.7),
    ("PAK", 30.4, 69.3),
    ("IND", 20.6, 79.0),
    ("MMR", 19.8, 96.7),
    ("COD", -4.0, 21.8),
    ("NGA", 9.1, 7.5),
    ("MLI", 17.6, -4.0),
    ("BFA", 12.3, -1.6),
];

fn country_population(code: &str) -> Option<(&'static str, i64, f64)> {
    PRIORITY_COUNTRIES
        .iter()
        .find(|(country_code, _, _, _)| *country_code == code)
        .map(|(_, name, population, area)| (*name, *population, *area))
}

pub async fn get_population_exposure(
    State(_state): State<AppState>,
    Json(request): Json<GetPopulationExposureRequest>,
) -> Result<Json<GetPopulationExposureResponse>, AppError> {
    if request.mode.eq_ignore_ascii_case("exposure") {
        let mut nearest = "";
        let mut nearest_distance = f64::MAX;
        for (code, latitude, longitude) in EXPOSURE_CENTROIDS {
            let delta_lat = request.lat - *latitude;
            let delta_lon = request.lon - *longitude;
            let distance = (delta_lat.powi(2) + delta_lon.powi(2)).sqrt();
            if distance < nearest_distance {
                nearest_distance = distance;
                nearest = code;
            }
        }

        let (_, population, area) =
            country_population(nearest).unwrap_or(("Unknown", 50_000_000, 500_000.0));
        let density = population as f64 / area.max(1.0);
        let radius_km = if request.radius > 0.0 {
            request.radius
        } else {
            50.0
        };
        let area_km2 = std::f64::consts::PI * radius_km * radius_km;
        let exposed = (density * area_km2).round() as i64;

        return Ok(Json(GetPopulationExposureResponse {
            success: true,
            countries: Vec::new(),
            exposure: Some(ExposureResult {
                exposed_population: exposed.max(0),
                exposure_radius_km: radius_km,
                nearest_country: nearest.to_string(),
                density_per_km2: density.round() as i64,
            }),
        }));
    }

    let countries = PRIORITY_COUNTRIES
        .iter()
        .map(|(code, name, population, area)| CountryPopulationEntry {
            code: (*code).to_string(),
            name: (*name).to_string(),
            population: *population,
            density_per_km2: (*population as f64 / area.max(1.0)).round() as i64,
        })
        .collect::<Vec<_>>();

    Ok(Json(GetPopulationExposureResponse {
        success: true,
        countries,
        exposure: None,
    }))
}
