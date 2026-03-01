# WorldMonitor v3 Rust Server (Scaffold)

Rust-native server migration target for WorldMonitor API parity.

This crate currently provides:
- Gateway middleware scaffold (CORS + API-key checks).
- Phase A + Phase B route coverage for market/intelligence/infrastructure/economic/conflict/research/prediction.
- In-memory caching and upstream-backed implementations for shipped routes.

## Run

```bash
cd v3-rust-server
cargo run
```

Server binds to `127.0.0.1:3000` by default.

Override bind address:

```bash
WM_SERVER_ADDR=127.0.0.1:8787 cargo run
```

## Environment

- `WM_SERVER_ADDR` (default: `127.0.0.1:3000`)
- `WORLDMONITOR_VALID_KEYS` (comma-separated)
- `VERCEL_ENV` / `NODE_ENV` (used for CORS mode)
- `GROQ_API_KEY` (intelligence classification/brief)
- `ACLED_ACCESS_TOKEN` (unrest/risk scoring inputs)
- `FINNHUB_API_KEY` (market quote enrichment)
- `FRED_API_KEY` (economic FRED series)
- `EIA_API_KEY` (economic energy prices)

## Implemented Route Surface

- `POST /api/intelligence/v1/get-country-intel-brief`
- `POST /api/intelligence/v1/get-pizzint-status`
- `POST /api/intelligence/v1/classify-event`
- `POST /api/intelligence/v1/search-gdelt-documents`
- `POST /api/intelligence/v1/get-risk-scores`
- `POST /api/market/v1/get-country-stock-index`
- `POST /api/market/v1/list-market-quotes`
- `POST /api/market/v1/list-commodity-quotes`
- `POST /api/market/v1/get-sector-summary`
- `POST /api/market/v1/list-stablecoin-markets`
- `POST /api/market/v1/list-etf-flows`
- `POST /api/seismology/v1/list-earthquakes`
- `POST /api/unrest/v1/list-unrest-events`
- `POST /api/infrastructure/v1/list-service-statuses`
- `POST /api/infrastructure/v1/list-internet-outages`
- `POST /api/infrastructure/v1/get-temporal-baseline`
- `POST /api/infrastructure/v1/record-baseline-snapshot`
- `POST /api/infrastructure/v1/get-cable-health`
- `POST /api/market/v1/list-crypto-quotes`
- `POST /api/economic/v1/get-fred-series`
- `POST /api/economic/v1/list-world-bank-indicators`
- `POST /api/economic/v1/get-energy-prices`
- `POST /api/economic/v1/get-macro-signals`
- `POST /api/conflict/v1/list-acled-events`
- `POST /api/conflict/v1/list-ucdp-events`
- `POST /api/conflict/v1/get-humanitarian-summary`
- `POST /api/research/v1/list-arxiv-papers`
- `POST /api/research/v1/list-trending-repos`
- `POST /api/research/v1/list-hackernews-items`
- `POST /api/research/v1/list-tech-events`
- `POST /api/prediction/v1/list-prediction-markets`
- `GET /healthz`

See [ROADMAP.md](/data/data/com.termux/files/home/worldmonitor/v3-rust-server/ROADMAP.md) for migration phases and done criteria.
