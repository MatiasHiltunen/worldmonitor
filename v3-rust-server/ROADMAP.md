# Rust Server Migration Roadmap

## Objective

Replace TS API handler runtime with a Rust server while preserving:
- Path compatibility (`/api/{domain}/v1/{rpc}`).
- JSON field compatibility (camelCase, existing enums).
- Security/runtime behavior (CORS + API-key gate + error contracts).

## Current Inventory (from proto contracts)

- Services: `17`
- RPC methods: `46`

Service breakdown:
- `aviation(1) climate(1) conflict(3) cyber(1) displacement(2) economic(4)`
- `infrastructure(5) intelligence(5) maritime(2) market(7) military(6)`
- `news(1) prediction(1) research(4) seismology(1) unrest(1) wildfire(1)`

## Parity Constraints (must match TS behavior)

1. CORS:
- Disallow unknown `Origin` with `403`.
- Return `204` for `OPTIONS`.
- Attach `Access-Control-*` + `Vary: Origin` for allowed requests.

2. API key:
- Desktop origins require `X-WorldMonitor-Key`.
- Non-desktop: key optional, but if present must validate.

3. Error mapping:
- 400 validation contract for bad request body.
- 401 for key failures.
- 404 for unknown RPC path.
- 502/500 for upstream/runtime failures (phase-by-phase refinement).

4. JSON contracts:
- Preserve camelCase fields from OpenAPI/proto generated TS interfaces.

## Phased Implementation Plan

## Phase 0: Platform Foundation

Scope:
- App config/env loader.
- Middleware stack (CORS + API key).
- Structured logging/tracing.
- Cache abstraction trait and no-op implementation.

Done criteria:
- `GET /healthz` works.
- CORS behavior matches `server/cors.ts` for allowed/disallowed origin cases.
- API-key behavior matches `api/_api-key.js`.

## Phase A: Rust TUI Unblock (high-priority parity)

Scope:
- `intelligence/get-country-intel-brief`
- `intelligence/get-risk-scores`
- `market/get-country-stock-index`
- `seismology/list-earthquakes`
- `unrest/list-unrest-events`
- `infrastructure/list-service-statuses`
- `market/list-crypto-quotes`

Current status:
- Route scaffolds are present with contract-shaped placeholder responses.

Done criteria:
- Real upstream implementations replace placeholders.
- `v2-rust` TUI can run end-to-end against Rust server with no schema regressions.

## Phase B: Domain Completion (data-heavy core)

Scope:
- Remaining `market`, `intelligence`, `infrastructure`.
- `economic`, `conflict`, `research`, `prediction`.

Done criteria:
- Snapshot diff harness (TS vs Rust) on representative requests passes.
- Cache behavior parity for endpoints using Upstash in TS.

## Phase C: Advanced Integrations

Scope:
- `military`, `cyber`, `maritime`, `news`, `wildfire`, `aviation`, `climate`, `displacement`.

Done criteria:
- External provider auth/timeouts/retry policy parity.
- Graceful degradation behavior documented and tested.

## Phase D: Cutover

Scope:
- Shadow traffic.
- Progressive domain-by-domain switch.
- Rollback toggle retained.

Done criteria:
- Error rate, latency, and response contract parity within target thresholds.
- TS gateway no longer required for migrated routes.

## Test Plan

1. Contract tests:
- Deserialize/serialize fixtures for each endpoint response.

2. Integration tests:
- Mock upstream providers with deterministic payloads.
- Assert status mapping and fallback behavior.

3. Parity tests:
- Run identical request corpus against TS and Rust handlers.
- Diff status code + normalized JSON body.

## Open Items

- Choose final cache backend implementation (`Upstash REST` vs `Redis TCP`).
- Decide deployment target for Rust server runtime.
- Determine whether non-sebuf legacy endpoints stay in TS or move to Rust.
