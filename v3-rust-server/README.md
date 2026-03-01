# WorldMonitor v3 Rust Server (Scaffold)

Rust-native server migration target for WorldMonitor API parity.

This crate currently provides:
- Gateway middleware scaffold (CORS + API-key checks).
- Phase A route surface needed by the Rust TUI.
- Contract-shaped placeholder JSON responses for iterative cutover.

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

## Implemented Route Surface (Phase A scaffold)

- `POST /api/intelligence/v1/get-country-intel-brief`
- `POST /api/intelligence/v1/get-risk-scores`
- `POST /api/market/v1/get-country-stock-index`
- `POST /api/seismology/v1/list-earthquakes`
- `POST /api/unrest/v1/list-unrest-events`
- `POST /api/infrastructure/v1/list-service-statuses`
- `POST /api/market/v1/list-crypto-quotes`
- `GET /healthz`

See [ROADMAP.md](/data/data/com.termux/files/home/worldmonitor/v3-rust-server/ROADMAP.md) for migration phases and done criteria.
