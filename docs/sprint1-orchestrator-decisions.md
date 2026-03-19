# Sprint 1 Orchestrator Architecture Decisions

**Date:** 2026-03-16
**Status:** Approved
**Context:** Decisions made during 4-section plan-mode review (Architecture, Code Quality, Tests, Performance) before building the data ingestion orchestrator layer.

---

## Architecture

| # | Decision | Choice | Reasoning |
|---|----------|--------|-----------|
| 1 | Scheduler | APScheduler + `--run-once` CLI | Handles missed jobs, concurrent execution, graceful shutdown. CLI enables testing without scheduler. |
| 2 | Code layout | `ingestion/` package (weather.py, kalshi.py, factories.py) | Separation of concerns. Each module owns one job. Testable independently. |
| 3 | City config | Load once at startup, dict cache | 42 cities never change mid-run. Eliminates ~168 redundant DB queries per cycle. |
| 4 | Kalshi market data mode | REST-only this sprint: discovery 2h / snapshots 5m / settlements 2h | Keeps the orchestrator simple and observable while the WebSocket path is deferred. Different cadences match data volatility. |

## Code Quality

| # | Decision | Choice | Reasoning |
|---|----------|--------|-----------|
| 5 | Client construction | Factory functions (`create_weather_clients`, `create_kalshi_client`) | Single place to add clients. Validates credentials. Returns only clients with valid keys. |
| 6 | Error isolation | Per-city try/except + summary log | One city's failure must never kill the run. Summary counts are essential for monitoring. |
| 7 | Deduplication | `INSERT ON CONFLICT DO NOTHING` + count | Atomic, no race conditions. Skip count reveals stale model data. |
| 8 | Correlation IDs | Per-source `correlation_id` + shared `run_id` | Filter by run_id for full cycle, correlation_id for single-source debugging. |

## Testing

| # | Decision | Choice | Reasoning |
|---|----------|--------|-----------|
| 9 | Orchestrator testability | Function arguments (DI) | No monkeypatching. Tests pass mock clients + session factory. main.py builds real objects. |
| 10 | DB integration tests | `@pytest.mark.integration` gated | Fast default runs. `pytest -m integration` with real PostgreSQL in CI. |
| 11 | Factory tests | Real construction + close | httpx.Client is cheap. Tests actual code path, not mocks. |
| 12 | Error isolation tests | Failure injection | Mock clients that raise on specific cities. Verifies the core safety property. |

## Performance

| # | Decision | Choice | Reasoning |
|---|----------|--------|-----------|
| 13 | Source concurrency | One APScheduler job per source | Sources run concurrently. Cycle = slowest source (~84s NWS) not sum (~130s). |
| 14 | Snapshot scope | Tomorrow's markets only (24-48h) | 90% reduction in DB volume. Distant markets barely move. |
| 15 | NWS gridpoint cache | File-based persistence (`./data/nws_gridpoints.json`) | Code already exists. Eliminates 84s cold-start on restarts. |
| 16 | Raw response storage | Trim to relevant day | ~10x storage reduction. OWM already does this. Add retention policy later. |

---

## Runtime Topology

- Active Sprint 1 containers: `postgres`, `data-ingestion`, `prediction-engine`, `notification-service`
- `data-ingestion` owns the APScheduler loop and all Kalshi/weather polling jobs
- `prediction-engine` and `notification-service` remain separate runtime services, but they are not part of the orchestrator process
- No Kalshi WebSocket worker exists in this sprint; all Kalshi reads come from REST jobs

---

## Verification Plan

After implementation, verify each decision by checking:
- [ ] APScheduler runs 4 weather + 2 Kalshi jobs concurrently
- [ ] `--run-once` CLI flag works for manual runs
- [ ] Factory returns only clients with valid credentials
- [ ] Per-city failures don't propagate; summary logs show counts
- [ ] Duplicate forecasts are silently skipped (ON CONFLICT)
- [ ] Logs contain both `run_id` and `correlation_id`
- [ ] Integration tests run with `pytest -m integration`
- [ ] Failure injection tests verify error isolation
- [ ] Kalshi runtime remains REST-only for Sprint 1; no WebSocket dependency exists
- [ ] Kalshi snapshots filter to 24-48h settlement window
- [ ] NWS gridpoint cache persists across restarts
- [ ] Raw responses are trimmed to target day only
