# Crate Selection Notes (2026-02-28)

Searched with `cargo search` and verified details via `cargo info`.

## Selected

- `ratatui` (`0.30.0`): core TUI framework.
- `crossterm` (`0.29.0`): terminal events/raw mode and alternate screen support.
- `ratatui-textarea` (`0.8.0`): request-body editor widget (avoids custom text-edit implementation).
- `reqwest` (`0.13.2`, blocking+json): simple HTTP + JSON without async runtime complexity.
- `serde` (`1.0.228`) + `serde_json` (`1.0.149`): typed API decoding.
- `clap` (`4.5.60`): small CLI surface (`--base-url`, `--api-key`, timeout).
- `anyhow` (`1.0.102`): context-rich error handling with low ceremony.
- `strum` (`0.28.0` with derive): concise endpoint enum display/iteration.

## Evaluated Alternatives

- `ureq` (`3.2.0`): lighter HTTP client; viable if we want fewer transitive deps.
- `thiserror` (`2.0.18`): useful when introducing explicit error enums later.
- `color-eyre` (`0.6.5`): stronger diagnostics for debugging sessions.
- `tui-textarea` (`0.7.0`): alternative textarea crate; `ratatui-textarea` selected for tighter ratatui alignment.
- `tracing-subscriber` (`0.3.22`): useful once we add structured logs.

## Why this set for v2 start

Goal was to ship a working Rust TUI quickly with real API integration and minimal code. Using blocking HTTP keeps the event loop simple; we can swap in async later if concurrency requirements grow.
