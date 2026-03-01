# WorldMonitor v2 (Pure Rust)

Rust-first terminal client for WorldMonitor APIs using `ratatui`.

## What this includes

- Text UI with endpoint picker + response panel
- Direct in-process calls to the `v3-rust-server` library by default (`--api-mode library`)
- Optional HTTP transport for remote hosts (`--api-mode http`)
- Dedicated RSS intelligence workspace with curated `WORLD`/`TECH`/`FINANCE` feed packs
- Dedicated BRIEF workspace for per-country intelligence briefs, CII risk, and stock index snapshots
- Inline JSON request editor (per-endpoint saved payloads)
- Request templates auto-generated from local OpenAPI docs (`docs/api/*.openapi.json`) with endpoint seed overrides
- Feed deduplication, per-feed failure tracking with cooldown backoff, keyword monitors, and spike detection
- Optional auto-refresh polling
- Typed response parsing for:
  - `seismology/list-earthquakes`
  - `unrest/list-unrest-events`
  - `infrastructure/list-service-statuses`
  - `market/list-crypto-quotes`
  - `climate/get-global-radiation-situation`
  - `aviation/get-flight-radar`
  - `maritime/get-marine-traffic`
  - `intelligence/get-country-intel-brief`
  - `intelligence/get-risk-scores`
  - `market/get-country-stock-index`

## Run

```bash
cd v2-rust
cargo run
```

Force HTTP mode (for remote hosts):

```bash
cargo run -- --api-mode http --base-url https://api.worldmonitor.app --api-key <KEY>
```

Auto-select transport from base URL (`localhost` => library, otherwise HTTP):

```bash
cargo run -- --api-mode auto --base-url http://127.0.0.1:3000
```

Enable periodic polling:

```bash
cargo run -- --base-url http://127.0.0.1:3000 --auto-refresh-secs 30
```

You can also set env vars:

- `WM_BASE_URL`
- `WORLDMONITOR_API_KEY`
- `WM_API_MODE` (`library`, `http`, or `auto`)

## Keybindings

Global:

- `Tab`: switch `API` / `RSS` / `BRIEF` workspace
- `a`: toggle auto-refresh (when configured)
- `q` or `Esc`: quit

API workspace:

- `Up/Down`: select endpoint
- `Enter` or `r`: fetch selected endpoint
- `j/k`: scroll response
- `e`: enter request JSON edit mode
- `t`: reset selected endpoint request JSON to template

In edit mode:

- `Ctrl+S`: validate + save request JSON
- `Ctrl+R`: validate + save + fetch
- `Ctrl+T`: reset unsaved editor content to template
- `Esc`: discard unsaved edits and return

RSS workspace:

- `v`: cycle feed variant (`WORLD` → `TECH` → `FINANCE`)
- `f` or `r`: refresh feeds now
- `Left/Right`: cycle category filter
- `Up/Down` or `j/k`: select headline
- `u/d`: scroll detail panel
- `/`: edit search query
- `m`: edit keyword monitors
- `t`: clear RSS filters

RSS text-input mode:

- `Enter`: apply
- `Esc`: cancel

BRIEF workspace:

- `Enter`, `r`, or `f`: refresh country brief
- `n/p` or `Right/Left`: cycle Tier-1 country presets
- `c`: edit country code (ISO-2)
- `x`: export current brief to `v2-rust/exports/briefs` (`json` + `txt`)
- `j/k`: scroll intelligence brief text
- `Up/Down`: select related RSS headline
- `u/d`: scroll related headline detail panel

BRIEF text-input mode:

- `Enter`: apply country code and fetch
- `Esc`: cancel

## Notes

- `worldmonitor.app` is protected by a browser checkpoint. Use `--api-mode library` for local in-process execution, or `--api-mode http` with a key-enabled API host.
