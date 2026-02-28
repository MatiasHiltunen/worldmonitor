# WorldMonitor v2 (Pure Rust)

Rust-first terminal client for WorldMonitor APIs using `ratatui`.

## What this includes

- Text UI with endpoint picker + response panel
- Live HTTP calls to text-based JSON APIs
- Inline JSON request editor (per-endpoint saved payloads)
- Request templates auto-generated from local OpenAPI docs (`docs/api/*.openapi.json`) with endpoint seed overrides
- Optional auto-refresh polling
- Typed response parsing for:
  - `seismology/list-earthquakes`
  - `unrest/list-unrest-events`
  - `infrastructure/list-service-statuses`
  - `market/list-crypto-quotes`

## Run

```bash
cd v2-rust
cargo run -- --base-url http://127.0.0.1:3000
```

If your target requires auth:

```bash
cargo run -- --base-url https://api.worldmonitor.app --api-key <KEY>
```

Enable periodic polling:

```bash
cargo run -- --base-url http://127.0.0.1:3000 --auto-refresh-secs 30
```

You can also set env vars:

- `WM_BASE_URL`
- `WORLDMONITOR_API_KEY`

## Keybindings

- `Up/Down`: select endpoint
- `Enter` or `r`: fetch selected endpoint
- `j/k`: scroll response
- `e`: enter request JSON edit mode
- `t`: reset selected endpoint request JSON to template
- `a`: toggle auto-refresh (when configured)
- `q` or `Esc`: quit

In edit mode:

- `Ctrl+S`: validate + save request JSON
- `Ctrl+R`: validate + save + fetch
- `Ctrl+T`: reset unsaved editor content to template
- `Esc`: discard unsaved edits and return

## Notes

- `worldmonitor.app` is protected by a browser checkpoint, so CLI calls should target local/self-hosted API or a key-enabled API host.
