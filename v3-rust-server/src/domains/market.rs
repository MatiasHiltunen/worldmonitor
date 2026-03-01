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
const MARKET_QUOTES_CACHE_TTL: Duration = Duration::from_secs(120);
const COMMODITY_CACHE_TTL: Duration = Duration::from_secs(180);
const SECTOR_CACHE_TTL: Duration = Duration::from_secs(180);
const STABLECOIN_CACHE_TTL: Duration = Duration::from_secs(120);
const ETF_CACHE_TTL: Duration = Duration::from_secs(900);

const DEFAULT_CRYPTO_IDS: [&str; 4] = ["bitcoin", "ethereum", "solana", "ripple"];
const DEFAULT_STABLECOIN_IDS: [&str; 5] = [
    "tether",
    "usd-coin",
    "dai",
    "first-digital-usd",
    "ethena-usde",
];
const SECTOR_SYMBOLS: [&str; 12] = [
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC", "SMH",
];

const YAHOO_ONLY_SYMBOLS: [&str; 9] = [
    "^GSPC", "^DJI", "^IXIC", "^VIX", "GC=F", "CL=F", "NG=F", "SI=F", "HG=F",
];

const ETF_LIST: [(&str, &str); 10] = [
    ("IBIT", "BlackRock"),
    ("FBTC", "Fidelity"),
    ("ARKB", "ARK/21Shares"),
    ("BITB", "Bitwise"),
    ("GBTC", "Grayscale"),
    ("HODL", "VanEck"),
    ("BRRR", "Valkyrie"),
    ("EZBC", "Franklin"),
    ("BTCO", "Invesco"),
    ("BTCW", "WisdomTree"),
];

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

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListMarketQuotesRequest {
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListMarketQuotesResponse {
    pub quotes: Vec<MarketQuote>,
    pub finnhub_skipped: bool,
    pub skip_reason: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarketQuote {
    pub symbol: String,
    pub name: String,
    pub display: String,
    pub price: f64,
    pub change: f64,
    pub sparkline: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListCommodityQuotesRequest {
    #[serde(default)]
    pub symbols: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListCommodityQuotesResponse {
    pub quotes: Vec<CommodityQuote>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommodityQuote {
    pub symbol: String,
    pub name: String,
    pub display: String,
    pub price: f64,
    pub change: f64,
    pub sparkline: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetSectorSummaryRequest {
    #[serde(default)]
    pub period: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetSectorSummaryResponse {
    pub sectors: Vec<SectorPerformance>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SectorPerformance {
    pub symbol: String,
    pub name: String,
    pub change: f64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListStablecoinMarketsRequest {
    #[serde(default)]
    pub coins: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListStablecoinMarketsResponse {
    pub timestamp: String,
    pub summary: StablecoinSummary,
    pub stablecoins: Vec<Stablecoin>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Stablecoin {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub deviation: f64,
    pub peg_status: String,
    pub market_cap: f64,
    pub volume_24h: f64,
    pub change_24h: f64,
    pub change_7d: f64,
    pub image: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StablecoinSummary {
    pub total_market_cap: f64,
    pub total_volume_24h: f64,
    pub coin_count: i32,
    pub depegged_count: i32,
    pub health_status: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListEtfFlowsRequest {}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ListEtfFlowsResponse {
    pub timestamp: String,
    pub summary: EtfFlowsSummary,
    pub etfs: Vec<EtfFlow>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EtfFlow {
    pub ticker: String,
    pub issuer: String,
    pub price: f64,
    pub price_change: f64,
    pub volume: i64,
    pub avg_volume: i64,
    pub volume_ratio: f64,
    pub direction: String,
    pub est_flow: i64,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EtfFlowsSummary {
    pub etf_count: i32,
    pub total_volume: i64,
    pub total_est_flow: i64,
    pub net_direction: String,
    pub inflow_count: i32,
    pub outflow_count: i32,
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
static MARKET_QUOTES_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListMarketQuotesResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static COMMODITY_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListCommodityQuotesResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static SECTOR_CACHE: Lazy<Mutex<Option<CacheEntry<GetSectorSummaryResponse>>>> =
    Lazy::new(|| Mutex::new(None));
static STABLECOIN_CACHE: Lazy<Mutex<HashMap<String, CacheEntry<ListStablecoinMarketsResponse>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ETF_CACHE: Lazy<Mutex<Option<CacheEntry<ListEtfFlowsResponse>>>> =
    Lazy::new(|| Mutex::new(None));

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

#[derive(Debug, Deserialize, Default, Clone)]
struct YahooResult {
    #[serde(default)]
    meta: YahooMeta,
    #[serde(default)]
    indicators: YahooIndicators,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct YahooMeta {
    #[serde(default)]
    currency: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct YahooIndicators {
    #[serde(default)]
    quote: Vec<YahooQuote>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct YahooQuote {
    #[serde(default)]
    close: Vec<Option<f64>>,
    #[serde(default)]
    volume: Vec<Option<f64>>,
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
    symbol: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    current_price: Option<f64>,
    #[serde(default)]
    market_cap: Option<f64>,
    #[serde(default)]
    total_volume: Option<f64>,
    #[serde(default)]
    price_change_percentage_24h: Option<f64>,
    #[serde(default)]
    price_change_percentage_7d_in_currency: Option<f64>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    sparkline_in_7d: Option<CoinGeckoSparkline>,
}

#[derive(Debug, Deserialize)]
struct FinnhubQuoteResponse {
    c: f64,
    dp: f64,
    h: f64,
    l: f64,
}

fn is_valid_country_code(value: &str) -> bool {
    value.len() == 2 && value.chars().all(|ch| ch.is_ascii_alphabetic())
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn cache_key(items: &[String]) -> String {
    let mut sorted = items.to_vec();
    sorted.sort();
    sorted.join(",")
}

fn values_from_options(values: &[Option<f64>]) -> Vec<f64> {
    values
        .iter()
        .flatten()
        .copied()
        .filter(|v| v.is_finite())
        .collect::<Vec<_>>()
}

fn extract_close_values(result: &YahooResult) -> Vec<f64> {
    result
        .indicators
        .quote
        .first()
        .map(|quote| values_from_options(&quote.close))
        .unwrap_or_default()
}

fn extract_volume_values(result: &YahooResult) -> Vec<f64> {
    result
        .indicators
        .quote
        .first()
        .map(|quote| values_from_options(&quote.volume))
        .unwrap_or_default()
}

fn quote_change(closes: &[f64]) -> Option<(f64, f64)> {
    if closes.is_empty() {
        return None;
    }
    let latest = *closes.last()?;
    let prev = if closes.len() > 1 {
        closes[closes.len() - 2]
    } else {
        latest
    };
    let change = if prev != 0.0 {
        ((latest - prev) / prev) * 100.0
    } else {
        0.0
    };
    Some((latest, change))
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

fn is_yahoo_only_symbol(symbol: &str) -> bool {
    YAHOO_ONLY_SYMBOLS.iter().any(|value| *value == symbol)
}

fn health_status(depegged_count: usize) -> String {
    if depegged_count == 0 {
        "HEALTHY".to_string()
    } else if depegged_count == 1 {
        "CAUTION".to_string()
    } else {
        "WARNING".to_string()
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

fn get_market_quotes_cache(
    key: &str,
) -> Result<Option<(ListMarketQuotesResponse, bool)>, AppError> {
    let cache = MARKET_QUOTES_CACHE
        .lock()
        .map_err(|_| AppError::Internal("market quotes cache lock poisoned".to_string()))?;
    let now = Instant::now();
    Ok(cache.get(key).map(|entry| {
        let fresh = now <= entry.expires_at;
        (entry.value.clone(), fresh)
    }))
}

fn set_market_quotes_cache(key: &str, value: &ListMarketQuotesResponse) -> Result<(), AppError> {
    let mut cache = MARKET_QUOTES_CACHE
        .lock()
        .map_err(|_| AppError::Internal("market quotes cache lock poisoned".to_string()))?;
    cache.insert(
        key.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + MARKET_QUOTES_CACHE_TTL,
        },
    );
    Ok(())
}

fn get_commodity_cache(key: &str) -> Result<Option<(ListCommodityQuotesResponse, bool)>, AppError> {
    let cache = COMMODITY_CACHE
        .lock()
        .map_err(|_| AppError::Internal("commodity cache lock poisoned".to_string()))?;
    let now = Instant::now();
    Ok(cache.get(key).map(|entry| {
        let fresh = now <= entry.expires_at;
        (entry.value.clone(), fresh)
    }))
}

fn set_commodity_cache(key: &str, value: &ListCommodityQuotesResponse) -> Result<(), AppError> {
    let mut cache = COMMODITY_CACHE
        .lock()
        .map_err(|_| AppError::Internal("commodity cache lock poisoned".to_string()))?;
    cache.insert(
        key.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + COMMODITY_CACHE_TTL,
        },
    );
    Ok(())
}

fn get_sector_cache() -> Result<Option<(GetSectorSummaryResponse, bool)>, AppError> {
    let cache = SECTOR_CACHE
        .lock()
        .map_err(|_| AppError::Internal("sector cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref() {
        let fresh = Instant::now() <= entry.expires_at;
        return Ok(Some((entry.value.clone(), fresh)));
    }
    Ok(None)
}

fn set_sector_cache(value: &GetSectorSummaryResponse) -> Result<(), AppError> {
    let mut cache = SECTOR_CACHE
        .lock()
        .map_err(|_| AppError::Internal("sector cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: value.clone(),
        expires_at: Instant::now() + SECTOR_CACHE_TTL,
    });
    Ok(())
}

fn get_stablecoin_cache(
    key: &str,
) -> Result<Option<(ListStablecoinMarketsResponse, bool)>, AppError> {
    let cache = STABLECOIN_CACHE
        .lock()
        .map_err(|_| AppError::Internal("stablecoin cache lock poisoned".to_string()))?;
    let now = Instant::now();
    Ok(cache.get(key).map(|entry| {
        let fresh = now <= entry.expires_at;
        (entry.value.clone(), fresh)
    }))
}

fn set_stablecoin_cache(key: &str, value: &ListStablecoinMarketsResponse) -> Result<(), AppError> {
    let mut cache = STABLECOIN_CACHE
        .lock()
        .map_err(|_| AppError::Internal("stablecoin cache lock poisoned".to_string()))?;
    cache.insert(
        key.to_string(),
        CacheEntry {
            value: value.clone(),
            expires_at: Instant::now() + STABLECOIN_CACHE_TTL,
        },
    );
    Ok(())
}

fn get_etf_cache() -> Result<Option<(ListEtfFlowsResponse, bool)>, AppError> {
    let cache = ETF_CACHE
        .lock()
        .map_err(|_| AppError::Internal("etf cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.as_ref() {
        let fresh = Instant::now() <= entry.expires_at;
        return Ok(Some((entry.value.clone(), fresh)));
    }
    Ok(None)
}

fn set_etf_cache(value: &ListEtfFlowsResponse) -> Result<(), AppError> {
    let mut cache = ETF_CACHE
        .lock()
        .map_err(|_| AppError::Internal("etf cache lock poisoned".to_string()))?;
    *cache = Some(CacheEntry {
        value: value.clone(),
        expires_at: Instant::now() + ETF_CACHE_TTL,
    });
    Ok(())
}

async fn fetch_yahoo_chart(
    state: &AppState,
    symbol: &str,
    range: &str,
    interval: &str,
) -> Option<YahooResult> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range={}&interval={}",
        urlencoding::encode(symbol),
        range,
        interval
    );

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

    let data = response.json::<YahooChartResponse>().await.ok()?;
    data.chart.result.first().cloned()
}

async fn fetch_yahoo_quote(
    state: &AppState,
    symbol: &str,
) -> Option<(f64, f64, Vec<f64>, Vec<f64>)> {
    let result = fetch_yahoo_chart(state, symbol, "5d", "1d").await?;
    let closes = extract_close_values(&result);
    let (price, change) = quote_change(&closes)?;
    let volumes = extract_volume_values(&result);
    Some((price, change, closes, volumes))
}

async fn fetch_finnhub_quote(state: &AppState, symbol: &str, api_key: &str) -> Option<(f64, f64)> {
    let url = format!(
        "https://finnhub.io/api/v1/quote?symbol={}&token={}",
        urlencoding::encode(symbol),
        urlencoding::encode(api_key)
    );

    let response = state
        .http_client
        .get(url)
        .header("Accept", "application/json")
        .header("User-Agent", CHROME_UA)
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let payload = response.json::<FinnhubQuoteResponse>().await.ok()?;
    if payload.c == 0.0 && payload.h == 0.0 && payload.l == 0.0 {
        return None;
    }

    Some((payload.c, payload.dp))
}

async fn fetch_stock_index(
    state: &AppState,
    code: &str,
    symbol: &str,
    index_name: &str,
) -> Option<GetCountryStockIndexResponse> {
    let result = fetch_yahoo_chart(state, symbol, "1mo", "1d").await?;
    let closes = extract_close_values(&result);
    if closes.len() < 2 {
        return None;
    }

    let start_idx = closes.len().saturating_sub(6);
    let window = &closes[start_idx..];
    let latest = *window.last()?;
    let oldest = *window.first()?;
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
    sparkline: bool,
    include_7d: bool,
) -> Result<Vec<CoinGeckoMarketItem>, AppError> {
    let url = format!(
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids={}&order=market_cap_desc&sparkline={}&price_change_percentage={}",
        ids.join(","),
        if sparkline { "true" } else { "false" },
        if include_7d { "7d" } else { "24h" }
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

    response
        .json::<Vec<CoinGeckoMarketItem>>()
        .await
        .map_err(|error| AppError::Internal(format!("CoinGecko decode failed: {}", error)))
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

    let key = cache_key(&ids);
    let stale_cached = get_crypto_cache(&key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let items = match fetch_coingecko_markets(&state, &ids, true, false).await {
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
            name: if coin.name.is_empty() {
                default_name.to_string()
            } else {
                coin.name.clone()
            },
            symbol: if coin.symbol.is_empty() {
                default_symbol.to_string()
            } else {
                coin.symbol.to_uppercase()
            },
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
    set_crypto_cache(&key, &response)?;
    Ok(Json(response))
}

pub async fn list_market_quotes(
    State(state): State<AppState>,
    Json(request): Json<ListMarketQuotesRequest>,
) -> Result<Json<ListMarketQuotesResponse>, AppError> {
    let symbols = request
        .symbols
        .iter()
        .map(|symbol| symbol.trim().to_string())
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>();

    let key = cache_key(&symbols);
    let stale_cached = get_market_quotes_cache(&key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let finnhub_key = state.config.finnhub_api_key.clone();
    if symbols.is_empty() {
        return Ok(Json(ListMarketQuotesResponse {
            quotes: Vec::new(),
            finnhub_skipped: finnhub_key.is_none(),
            skip_reason: if finnhub_key.is_none() {
                "FINNHUB_API_KEY not configured".to_string()
            } else {
                String::new()
            },
        }));
    }

    let mut finnhub_symbols = Vec::new();
    let mut yahoo_symbols = Vec::new();
    for symbol in &symbols {
        if is_yahoo_only_symbol(symbol) {
            yahoo_symbols.push(symbol.clone());
        } else {
            finnhub_symbols.push(symbol.clone());
        }
    }

    let mut quotes = Vec::new();

    if let Some(api_key) = finnhub_key.as_deref() {
        for symbol in &finnhub_symbols {
            if let Some((price, change)) = fetch_finnhub_quote(&state, symbol, api_key).await {
                quotes.push(MarketQuote {
                    symbol: symbol.clone(),
                    name: symbol.clone(),
                    display: symbol.clone(),
                    price,
                    change,
                    sparkline: Vec::new(),
                });
            }
        }
    }

    for symbol in &yahoo_symbols {
        if let Some((price, change, sparkline, _)) = fetch_yahoo_quote(&state, symbol).await {
            quotes.push(MarketQuote {
                symbol: symbol.clone(),
                name: symbol.clone(),
                display: symbol.clone(),
                price,
                change,
                sparkline,
            });
        }
    }

    if quotes.is_empty() {
        if let Some((cached, _)) = stale_cached {
            return Ok(Json(cached));
        }
    }

    let response = ListMarketQuotesResponse {
        quotes,
        finnhub_skipped: finnhub_key.is_none(),
        skip_reason: if finnhub_key.is_none() {
            "FINNHUB_API_KEY not configured".to_string()
        } else {
            String::new()
        },
    };

    if !response.quotes.is_empty() {
        set_market_quotes_cache(&key, &response)?;
    }

    Ok(Json(response))
}

pub async fn list_commodity_quotes(
    State(state): State<AppState>,
    Json(request): Json<ListCommodityQuotesRequest>,
) -> Result<Json<ListCommodityQuotesResponse>, AppError> {
    let symbols = request
        .symbols
        .iter()
        .map(|symbol| symbol.trim().to_string())
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>();

    if symbols.is_empty() {
        return Ok(Json(ListCommodityQuotesResponse { quotes: Vec::new() }));
    }

    let key = cache_key(&symbols);
    let stale_cached = get_commodity_cache(&key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let mut quotes = Vec::new();
    for symbol in &symbols {
        if let Some((price, change, sparkline, _)) = fetch_yahoo_quote(&state, symbol).await {
            quotes.push(CommodityQuote {
                symbol: symbol.clone(),
                name: symbol.clone(),
                display: symbol.clone(),
                price,
                change,
                sparkline,
            });
        }
    }

    if quotes.is_empty() {
        return Ok(Json(
            stale_cached
                .map(|(cached, _)| cached)
                .unwrap_or(ListCommodityQuotesResponse { quotes: Vec::new() }),
        ));
    }

    let response = ListCommodityQuotesResponse { quotes };
    set_commodity_cache(&key, &response)?;
    Ok(Json(response))
}

pub async fn get_sector_summary(
    State(state): State<AppState>,
    Json(_request): Json<GetSectorSummaryRequest>,
) -> Result<Json<GetSectorSummaryResponse>, AppError> {
    let stale_cached = get_sector_cache()?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let Some(api_key) = state.config.finnhub_api_key.as_deref() else {
        return Ok(Json(GetSectorSummaryResponse {
            sectors: stale_cached
                .map(|(cached, _)| cached.sectors)
                .unwrap_or_default(),
        }));
    };

    let mut sectors = Vec::new();
    for symbol in SECTOR_SYMBOLS {
        if let Some((_, change)) = fetch_finnhub_quote(&state, symbol, api_key).await {
            sectors.push(SectorPerformance {
                symbol: symbol.to_string(),
                name: symbol.to_string(),
                change,
            });
        }
    }

    if sectors.is_empty() {
        return Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
            GetSectorSummaryResponse {
                sectors: Vec::new(),
            },
        )));
    }

    let response = GetSectorSummaryResponse { sectors };
    set_sector_cache(&response)?;
    Ok(Json(response))
}

pub async fn list_stablecoin_markets(
    State(state): State<AppState>,
    Json(request): Json<ListStablecoinMarketsRequest>,
) -> Result<Json<ListStablecoinMarketsResponse>, AppError> {
    let mut coins = request
        .coins
        .iter()
        .map(|coin| coin.trim().to_ascii_lowercase())
        .filter(|coin| {
            !coin.is_empty()
                && coin
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        })
        .collect::<Vec<_>>();

    if coins.is_empty() {
        coins = DEFAULT_STABLECOIN_IDS
            .iter()
            .map(|coin| (*coin).to_string())
            .collect::<Vec<_>>();
    }

    let key = cache_key(&coins);
    let stale_cached = get_stablecoin_cache(&key)?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let items = match fetch_coingecko_markets(&state, &coins, false, true).await {
        Ok(items) => items,
        Err(_) => {
            return Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
                ListStablecoinMarketsResponse {
                    timestamp: Utc::now().to_rfc3339(),
                    summary: StablecoinSummary {
                        total_market_cap: 0.0,
                        total_volume_24h: 0.0,
                        coin_count: 0,
                        depegged_count: 0,
                        health_status: "UNAVAILABLE".to_string(),
                    },
                    stablecoins: Vec::new(),
                },
            )));
        }
    };

    let stablecoins = items
        .into_iter()
        .map(|coin| {
            let price = coin.current_price.unwrap_or(0.0);
            let deviation = (price - 1.0).abs();
            let peg_status = if deviation <= 0.005 {
                "ON PEG".to_string()
            } else if deviation <= 0.01 {
                "SLIGHT DEPEG".to_string()
            } else {
                "DEPEGGED".to_string()
            };

            Stablecoin {
                id: coin.id,
                symbol: coin.symbol.to_uppercase(),
                name: coin.name,
                price,
                deviation: round_two(deviation * 100.0),
                peg_status,
                market_cap: coin.market_cap.unwrap_or(0.0),
                volume_24h: coin.total_volume.unwrap_or(0.0),
                change_24h: coin.price_change_percentage_24h.unwrap_or(0.0),
                change_7d: coin.price_change_percentage_7d_in_currency.unwrap_or(0.0),
                image: coin.image.unwrap_or_default(),
            }
        })
        .collect::<Vec<_>>();

    let total_market_cap = stablecoins.iter().map(|coin| coin.market_cap).sum::<f64>();
    let total_volume_24h = stablecoins.iter().map(|coin| coin.volume_24h).sum::<f64>();
    let depegged_count = stablecoins
        .iter()
        .filter(|coin| coin.peg_status == "DEPEGGED")
        .count();

    let response = ListStablecoinMarketsResponse {
        timestamp: Utc::now().to_rfc3339(),
        summary: StablecoinSummary {
            total_market_cap,
            total_volume_24h,
            coin_count: stablecoins.len() as i32,
            depegged_count: depegged_count as i32,
            health_status: health_status(depegged_count),
        },
        stablecoins,
    };

    set_stablecoin_cache(&key, &response)?;
    Ok(Json(response))
}

pub async fn list_etf_flows(
    State(state): State<AppState>,
    Json(_request): Json<ListEtfFlowsRequest>,
) -> Result<Json<ListEtfFlowsResponse>, AppError> {
    let stale_cached = get_etf_cache()?;
    if let Some((cached, true)) = stale_cached.clone() {
        return Ok(Json(cached));
    }

    let mut etfs = Vec::new();
    for (ticker, issuer) in ETF_LIST {
        let Some((_, _, closes, volumes)) = fetch_yahoo_quote(&state, ticker).await else {
            continue;
        };
        if closes.len() < 2 {
            continue;
        }

        let latest_price = *closes.last().unwrap_or(&0.0);
        let prev_price = closes[closes.len() - 2];
        let price_change = if prev_price != 0.0 {
            ((latest_price - prev_price) / prev_price) * 100.0
        } else {
            0.0
        };

        let latest_volume = *volumes.last().unwrap_or(&0.0);
        let avg_volume = if volumes.len() > 1 {
            volumes[..volumes.len() - 1].iter().sum::<f64>() / (volumes.len() - 1) as f64
        } else {
            latest_volume
        };
        let volume_ratio = if avg_volume > 0.0 {
            latest_volume / avg_volume
        } else {
            1.0
        };

        let direction = if price_change > 0.1 {
            "inflow"
        } else if price_change < -0.1 {
            "outflow"
        } else {
            "neutral"
        };

        let sign = if price_change > 0.0 { 1.0 } else { -1.0 };
        let est_flow = (latest_volume * latest_price * sign * 0.1).round() as i64;

        etfs.push(EtfFlow {
            ticker: ticker.to_string(),
            issuer: issuer.to_string(),
            price: round_two(latest_price),
            price_change: round_two(price_change),
            volume: latest_volume.round() as i64,
            avg_volume: avg_volume.round() as i64,
            volume_ratio: round_two(volume_ratio),
            direction: direction.to_string(),
            est_flow,
        });
    }

    if etfs.is_empty() {
        return Ok(Json(stale_cached.map(|(cached, _)| cached).unwrap_or(
            ListEtfFlowsResponse {
                timestamp: Utc::now().to_rfc3339(),
                summary: EtfFlowsSummary {
                    etf_count: 0,
                    total_volume: 0,
                    total_est_flow: 0,
                    net_direction: "UNAVAILABLE".to_string(),
                    inflow_count: 0,
                    outflow_count: 0,
                },
                etfs: Vec::new(),
            },
        )));
    }

    etfs.sort_by(|a, b| b.volume.cmp(&a.volume));

    let total_volume = etfs.iter().map(|etf| etf.volume).sum::<i64>();
    let total_est_flow = etfs.iter().map(|etf| etf.est_flow).sum::<i64>();
    let inflow_count = etfs.iter().filter(|etf| etf.direction == "inflow").count() as i32;
    let outflow_count = etfs.iter().filter(|etf| etf.direction == "outflow").count() as i32;

    let response = ListEtfFlowsResponse {
        timestamp: Utc::now().to_rfc3339(),
        summary: EtfFlowsSummary {
            etf_count: etfs.len() as i32,
            total_volume,
            total_est_flow,
            net_direction: if total_est_flow > 0 {
                "NET INFLOW".to_string()
            } else if total_est_flow < 0 {
                "NET OUTFLOW".to_string()
            } else {
                "NEUTRAL".to_string()
            },
            inflow_count,
            outflow_count,
        },
        etfs,
    };

    set_etf_cache(&response)?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_country_codes() {
        assert!(is_valid_country_code("US"));
        assert!(!is_valid_country_code("U"));
        assert!(!is_valid_country_code("123"));
    }

    #[test]
    fn maps_known_country_indices() {
        assert_eq!(country_index("US"), Some(("^GSPC", "S&P 500")));
        assert!(country_index("ZZ").is_none());
    }

    #[test]
    fn returns_unavailable_shape() {
        let payload = unavailable_stock("ZZ");
        assert!(!payload.available);
        assert_eq!(payload.code, "ZZ");
        assert!(payload.symbol.is_empty());
    }

    #[test]
    fn maps_known_crypto_meta() {
        assert_eq!(crypto_meta("bitcoin"), Some(("Bitcoin", "BTC")));
        assert!(crypto_meta("dogecoin").is_none());
    }

    #[test]
    fn classifies_yahoo_only_symbols() {
        assert!(is_yahoo_only_symbol("^GSPC"));
        assert!(is_yahoo_only_symbol("CL=F"));
        assert!(!is_yahoo_only_symbol("AAPL"));
    }

    #[test]
    fn computes_health_status() {
        assert_eq!(health_status(0), "HEALTHY");
        assert_eq!(health_status(1), "CAUTION");
        assert_eq!(health_status(2), "WARNING");
    }
}
