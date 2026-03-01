use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{Json, extract::State};
use chrono::Utc;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::{AppState, error::AppError};

const CHROME_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const STOCK_INDEX_CACHE_TTL: Duration = Duration::from_secs(3_600);
const CRYPTO_CACHE_TTL: Duration = Duration::from_secs(180);

const DEFAULT_CRYPTO_IDS: [&str; 4] = ["bitcoin", "ethereum", "solana", "ripple"];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetCountryStockIndexRequest {
    pub country_code: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetCountryStockIndexResponse {
    pub available: bool,
    pub code: String,
    pub symbol: String,
    pub index_name: String,
    pub price: f64,
    pub week_change_percent: f64,
    pub currency: String,
    pub fetched_at: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListCryptoQuotesRequest {
    #[serde(default)]
    pub ids: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListCryptoQuotesResponse {
    pub quotes: Vec<CryptoQuote>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CryptoQuote {
    pub name: String,
    pub symbol: String,
    pub price: f64,
    pub change: f64,
    pub sparkline: Vec<f64>,
}

#[derive(Clone)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

static STOCK_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<GetCountryStockIndexResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static CRYPTO_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListCryptoQuotesResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Deserialize, Default)]
struct YahooChartResponse {
    #[serde(default)]
    chart: YahooChart,
}

#[derive(Debug, Deserialize, Default)]
struct YahooChart {
    #[serde(default)]
    result: Vec<YahooResult>,
}

#[derive(Debug, Deserialize, Default)]
struct YahooResult {
    #[serde(default)]
    meta: YahooMeta,
    #[serde(default)]
    indicators: YahooIndicators,
}

#[derive(Debug, Deserialize, Default)]
struct YahooMeta {
    #[serde(default)]
    currency: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct YahooIndicators {
    #[serde(default)]
    quote: Vec<YahooQuote>,
}

#[derive(Debug, Deserialize, Default)]
struct YahooQuote {
    #[serde(default)]
    close: Vec<Option<f64>>,
}

#[derive(Debug, Deserialize, Default)]
struct CoinGeckoSparkline {
    #[serde(default)]
    price: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct CoinGeckoMarketItem {
    #[serde(default)]
    id: String,
    #[serde(default)]
    current_price: Option<f64>,
    #[serde(default)]
    price_change_percentage_24h: Option<f64>,
    #[serde(default)]
    sparkline_in_7d: Option<CoinGeckoSparkline>,
}

fn is_valid_country_code(value: &str) -> bool {
    value.len() == 2 && value.chars().all(|ch| ch.is_ascii_alphabetic())
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn country_index(code: &str) -> Option<(&'static str, &'static str)> {
    match code {
        "US" => Some(("^GSPC", "S&P 500")),
        "GB" => Some(("^FTSE", "FTSE 100")),
        "DE" => Some(("^GDAXI", "DAX")),
        "FR" => Some(("^FCHI", "CAC 40")),
        "JP" => Some(("^N225", "Nikkei 225")),
        "CN" => Some(("000001.SS", "SSE Composite")),
        "HK" => Some(("^HSI", "Hang Seng")),
        "IN" => Some(("^BSESN", "BSE Sensex")),
        "KR" => Some(("^KS11", "KOSPI")),
        "TW" => Some(("^TWII", "TAIEX")),
        "AU" => Some(("^AXJO", "ASX 200")),
        "BR" => Some(("^BVSP", "Bovespa")),
        "CA" => Some(("^GSPTSE", "TSX Composite")),
        "MX" => Some(("^MXX", "IPC Mexico")),
        "AR" => Some(("^MERV", "MERVAL")),
        "RU" => Some(("IMOEX.ME", "MOEX")),
        "ZA" => Some(("^J203.JO", "JSE All Share")),
        "SA" => Some(("^TASI.SR", "Tadawul")),
        "AE" => Some(("DFMGI.AE", "DFM General")),
        "IL" => Some(("^TA125.TA", "TA-125")),
        "TR" => Some(("XU100.IS", "BIST 100")),
        "PL" => Some(("^WIG20", "WIG 20")),
        "NL" => Some(("^AEX", "AEX")),
        "CH" => Some(("^SSMI", "SMI")),
        "ES" => Some(("^IBEX", "IBEX 35")),
        "IT" => Some(("FTSEMIB.MI", "FTSE MIB")),
        "SE" => Some(("^OMX", "OMX Stockholm 30")),
        "NO" => Some(("^OSEAX", "Oslo All Share")),
        "SG" => Some(("^STI", "STI")),
        "TH" => Some(("^SET.BK", "SET")),
        "MY" => Some(("^KLSE", "KLCI")),
        "ID" => Some(("^JKSE", "Jakarta Composite")),
        "PH" => Some(("PSEI.PS", "PSEi")),
        "NZ" => Some(("^NZ50", "NZX 50")),
        "EG" => Some(("^EGX30.CA", "EGX 30")),
        "CL" => Some(("^IPSA", "IPSA")),
        "PE" => Some(("^SPBLPGPT", "S&P Lima")),
        "AT" => Some(("^ATX", "ATX")),
        "BE" => Some(("^BFX", "BEL 20")),
        "FI" => Some(("^OMXH25", "OMX Helsinki 25")),
        "DK" => Some(("^OMXC25", "OMX Copenhagen 25")),
        "IE" => Some(("^ISEQ", "ISEQ Overall")),
        "PT" => Some(("^PSI20", "PSI 20")),
        "CZ" => Some(("^PX", "PX Prague")),
        "HU" => Some(("^BUX", "BUX")),
        _ => None,
    }
}

fn crypto_meta(id: &str) -> Option<(&'static str, &'static str)> {
    match id {
        "bitcoin" => Some(("Bitcoin", "BTC")),
        "ethereum" => Some(("Ethereum", "ETH")),
        "solana" => Some(("Solana", "SOL")),
        "ripple" => Some(("XRP", "XRP")),
        _ => None,
    }
}

fn unavailable_stock(code: &str) -> GetCountryStockIndexResponse {
    GetCountryStockIndexResponse {
        available: false,
        code: code.to_string(),
        symbol: String::new(),
        index_name: String::new(),
        price: 0.0,
        week_change_percent: 0.0,
        currency: String::new(),
        fetched_at: String::new(),
    }
}

fn get_stock_cache(code: &str) -> Result<Option<(GetCountryStockIndexResponse, bool)>, AppError> {
    let cache = STOCK_CACHE
        .lock()
        .map_err(|_| AppError::Internal("stock cache lock poisoned".to_string()))?;
    let now = Instant::now();
    Ok(cache.get(code).map(|entry| {
        let fresh = now <= entry.expires_at;
        (entry.value.clone(), fresh)
    }))
}

fn set_stock_cache(code: &str, value: &GetCountryStockIndexResponse) -> Result<(), AppError> {
    let mut cache = STOCK_CACHE
        .lock()
        .map_err(|_| AppError::Internal("stock cache lock poisoned".to_string()))?;
    cache.insert(
        code.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + STOCK_INDEX_CACHE_TTL,
        },
    );
    Ok(())
}

fn get_crypto_cache(key: &str) -> Result<Option<(ListCryptoQuotesResponse, bool)>, AppError> {
    let cache = CRYPTO_CACHE
        .lock()
        .map_err(|_| AppError::Internal("crypto cache lock poisoned".to_string()))?;
    let now = Instant::now();
    Ok(cache.get(key).map(|entry| {
        let fresh = now <= entry.expires_at;
        (entry.value.clone(), fresh)
    }))
}

fn set_crypto_cache(key: &str, value: &ListCryptoQuotesResponse) -> Result<(), AppError> {
    let mut cache = CRYPTO_CACHE
        .lock()
        .map_err(|_| AppError::Internal("crypto cache lock poisoned".to_string()))?;
    cache.insert(
        key.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + CRYPTO_CACHE_TTL,
        },
    );
    Ok(())
}

async fn fetch_stock_index(
    state: &AppState,
    code: &str,
    symbol: &str,
    index_name: &str,
) -> Option<GetCountryStockIndexResponse> {
    let encoded_symbol = urlencoding::encode(symbol);
    let yahoo_url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range=1mo&interval=1d",
        encoded_symbol
    );

    let response = state
        .http_client
        .get(yahoo_url)
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let data = response.json::<YahooChartResponse>().await.ok()?;
    let result = data.chart.result.first()?;
    let all_closes = result
        .indicators
        .quote
        .first()
        .map(|quote| {
            quote
                .close
                .iter()
                .flatten()
                .copied()
                .filter(|value| value.is_finite())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if all_closes.len() < 2 {
        return None;
    }

    let start_idx = all_closes.len().saturating_sub(6);
    let closes = &all_closes[start_idx..];
    let latest = *closes.last()?;
    let oldest = *closes.first()?;
    if oldest == 0.0 {
        return None;
    }

    let week_change = ((latest - oldest) / oldest) * 100.0;
    Some(GetCountryStockIndexResponse {
        available: true,
        code: code.to_string(),
        symbol: symbol.to_string(),
        index_name: index_name.to_string(),
        price: round_two(latest),
        week_change_percent: round_two(week_change),
        currency: result
            .meta
            .currency
            .clone()
            .unwrap_or_else(|| "USD".to_string()),
        fetched_at: Utc::now().to_rfc3339(),
    })
}

async fn fetch_coingecko_markets(
    state: &AppState,
    ids: &[String],
) -> Result<Vec<CoinGeckoMarketItem>, AppError> {
    let url = format!(
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids={}&order=market_cap_desc&sparkline=true&price_change_percentage=24h",
        ids.join(",")
    );

    let response = state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .map_err(|error| AppError::Internal(format!("CoinGecko request failed: {}", error)))?;

    if !response.status().is_success() {
        return Err(AppError::Internal(format!(
            "CoinGecko API error: HTTP {}",
            response.status().as_u16()
        )));
    }

    let items = response
        .json::<Vec<CoinGeckoMarketItem>>()
        .await
        .map_err(|error| AppError::Internal(format!("CoinGecko decode failed: {}", error)))?;

    Ok(items)
}

pub async fn get_country_stock_index(
    State(state): State<AppState>,
    Json(request): Json<GetCountryStockIndexRequest>,
) -> Result<Json<GetCountryStockIndexResponse>, AppError> {
    let code = request.country_code.trim().to_uppercase();
    if !is_valid_country_code(code.as_str()) {
        return Err(AppError::BadRequest(
            "countryCode must be a two-letter ISO code".to_string(),
        ));
    }

    let Some((symbol, index_name)) = country_index(&code) else {
        return Ok(Json(unavailable_stock(&code)));
    };

    let stale_cached = get_stock_cache(&code)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let response = match fetch_stock_index(&state, &code, symbol, index_name).await {
        Some(payload) => {
            set_stock_cache(&code, &payload)?;
            payload
        }
        None => stale_cached
            .map(|(cached, _)| cached)
            .unwrap_or_else(|| unavailable_stock(&code)),
    };

    Ok(Json(response))
}

pub async fn list_crypto_quotes(
    State(state): State<AppState>,
    Json(request): Json<ListCryptoQuotesRequest>,
) -> Result<Json<ListCryptoQuotesResponse>, AppError> {
    let ids = if request.ids.is_empty() {
        DEFAULT_CRYPTO_IDS
            .iter()
            .map(|id| (*id).to_string())
            .collect::<Vec<_>>()
    } else {
        request
            .ids
            .iter()
            .map(|id| id.trim().to_ascii_lowercase())
            .filter(|id| !id.is_empty())
            .collect::<Vec<_>>()
    };

    if ids.is_empty() {
        return Ok(Json(ListCryptoQuotesResponse { quotes: Vec::new() }));
    }

    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    let cache_key = sorted_ids.join(",");

    let stale_cached = get_crypto_cache(&cache_key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let items = match fetch_coingecko_markets(&state, &ids).await {
        Ok(items) => items,
        Err(_) => {
            return Ok(Json(
                stale_cached
                    .map(|(cached, _)| cached)
                    .unwrap_or(ListCryptoQuotesResponse { quotes: Vec::new() }),
            ));
        }
    };

    if items.is_empty() {
        return Ok(Json(
            stale_cached
                .map(|(cached, _)| cached)
                .unwrap_or(ListCryptoQuotesResponse { quotes: Vec::new() }),
        ));
    }

    let by_id = items
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<HashMap<_, _>>();

    let mut quotes = Vec::new();
    for id in ids {
        let Some(coin) = by_id.get(&id) else {
            continue;
        };

        let (default_name, default_symbol) = crypto_meta(&id).unwrap_or((id.as_str(), id.as_str()));

        let sparkline = coin
            .sparkline_in_7d
            .as_ref()
            .map(|sparkline| {
                if sparkline.price.len() > 24 {
                    sparkline
                        .price
                        .iter()
                        .rev()
                        .take(48)
                        .copied()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                } else {
                    sparkline.price.clone()
                }
            })
            .unwrap_or_default();

        quotes.push(CryptoQuote {
            name: default_name.to_string(),
            symbol: default_symbol.to_string(),
            price: coin.current_price.unwrap_or(0.0),
            change: coin.price_change_percentage_24h.unwrap_or(0.0),
            sparkline,
        });
    }

    if quotes.iter().all(|quote| quote.price == 0.0) {
        return Ok(Json(
            stale_cached
                .map(|(cached, _)| cached)
                .unwrap_or(ListCryptoQuotesResponse { quotes: Vec::new() }),
        ));
    }

    let response = ListCryptoQuotesResponse { quotes };
    set_crypto_cache(&cache_key, &response)?;
    Ok(Json(response))
}
