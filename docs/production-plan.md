# Kalshi Weather Trading Bot - Production Plan

## Context

**Problem:** Kalshi offers binary weather contracts (will NYC's high be 62-63F?) priced by market sentiment. Weather forecasting models can predict temperatures more accurately than crowds, creating exploitable mispricings. When our model says 12% probability but Kalshi prices a contract at 54%, that's a BUY NO opportunity.

**What we're building:** A weather prediction and trading recommendation system that:
1. Ingests forecasts from 4 JSON weather sources (MVP) + ECMWF/GEM/ENS via GRIB2 (later sprint)
2. Aggregates them into temperature probability distributions per city per day
3. Compares model probabilities against Kalshi contract prices for ALL cities (~44+)
4. Generates BUY YES and BUY NO recommendations with risk scoring
5. Auto-executes paper trades on every recommendation to track accuracy
6. Sends daily email digests with recommendations
7. Provides a web dashboard showing recommendations, paper trade history, user trade tracking, and performance analytics

**Target:** 70%+ accuracy on recommended trades.

**Key decisions made:**
- All Kalshi cities from day 1 (44+)
- High AND Low temperature markets
- Both BUY YES and BUY NO directions
- Recommendation-only (human places real trades), but bot auto-executes all paper trades
- Multi-user architecture planned, single admin user for MVP
- Hybrid tech stack: Python for data/ML/recommendation logic, Spring Boot for web API only, Docker Compose on VPS
- Paper trading tracks both fixed-size accuracy AND virtual portfolio performance
- Kalshi API credentials already available (head start on market data ingestion)

**Post-approval first steps:**
1. Save this plan to Obsidian vault (`trading bots/weather/`) AND this repo (`docs/`)
2. Save project context to memory for future sessions

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION (Python)                          │
│                                                                         │
│  MVP (4 JSON sources):                                                  │
│  ┌───────┐ ┌──────┐ ┌────────┐ ┌────────┐                             │
│  │  NWS  │ │  VC  │ │   PW   │ │  OWM   │                             │
│  │ (free)│ │($35) │ │ ($2)   │ │ (free) │                             │
│  └───┬───┘ └──┬───┘ └───┬────┘ └───┬────┘                             │
│      └────────┴─────────┴──────────┘                                    │
│                                                                         │
│  Later sprint (3 GRIB2 sources):                                        │
│  ┌────────┐ ┌──────┐ ┌─────┐                                          │
│  │ ECMWF  │ │ GEM  │ │ ENS │                                          │
│  │ (free) │ │(free)│ │(free)│                                          │
│  └───┬────┘ └──┬───┘ └──┬──┘                                          │
│      └─────────┴────────┘                                               │
│                              │                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │              KALSHI MARKET DATA (Python via pykalshi)             │  │
│  │  WebSocket: real-time prices + settlements                        │  │
│  │  REST: market discovery, historical candlesticks, fallback        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                         ┌─────▼─────┐
                         │PostgreSQL │
                         └─────┬─────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                  PREDICTION + RECOMMENDATION ENGINE (Python)             │
│  Multi-model aggregation → Bracket probs → Gap detection → Risk score   │
│  → Paper trade execution (entry price locked at recommendation time)    │
└──────────┬──────────────────────────────────────────────┬───────────────┘
           │                                              │
    ┌──────▼──────┐                              ┌───────▼────────┐
    │   EMAIL     │                              │   WEB API      │
    │  (SendGrid) │                              │ (Spring Boot)  │
    └─────────────┘                              │   + React SPA  │
                                                 └────────────────┘
```

---

## Phase 1: Foundation & Data Layer

### 1.1 Project Scaffolding
- **Monorepo structure:**
  ```
  kalshi-weather-bot/
  ├── services/
  │   ├── data-ingestion/          # Python - weather + Kalshi data
  │   ├── prediction-engine/       # Python - ML models + recommendation + risk + paper trades
  │   ├── web-api/                 # Spring Boot - REST API serving only (reads from DB)
  │   ├── notification-service/    # Python - email sending
  │   └── web-frontend/            # React SPA
  ├── shared/
  │   ├── config/                  # City mappings, station IDs, bracket defs
  │   └── db-migrations/          # Alembic migrations
  ├── docker-compose.yml
  ├── docker-compose.prod.yml
  └── .github/workflows/
  ```
- Docker Compose for local dev AND production (VPS deployment)
- K8s-ready Dockerfiles (single concern per container, health endpoints, graceful shutdown)
- Shared city config: maps each Kalshi city → NWS station ID → timezone → lat/lon

### 1.2 Weather Data Ingestion Service (Python)

**MVP: 4 JSON API clients (Sprint 1):**

| Source | API | Cost | Format | Notes |
|--------|-----|------|--------|-------|
| NWS | api.weather.gov | Free, no key | JSON | Official Kalshi settlement source. GFS-based forecasts. 1s between requests |
| Visual Crossing | visualcrossingweather.com | $35/mo | JSON | 50+ year historical archive for backtesting |
| PirateWeather | pirateweather.net | $2/mo | JSON | Dark Sky-compatible. Raw NOAA models (HRRR 3km, GFS, NBM). 10K free calls/mo |
| OpenWeatherMap | api.openweathermap.org | Free | JSON | 1M calls/month, 60/min. 5-day/3-hr forecast. Adds non-NOAA model diversity |

**Later sprint: 3 GRIB2 sources (ECMWF/GEM/ENS) via Herbie + cfgrib:**

| Source | API | Cost | Format | Notes |
|--------|-----|------|--------|-------|
| ECMWF | data.ecmwf.int (open data) | Free | GRIB2 | World's best global NWP model. Herbie handles download + subsetting |
| GEM | dd.weather.gc.ca (ECCC Datamart) | Free | GRIB2 | Canadian model, strong for North America. GDPS 15km |
| ENS (ECMWF Ensemble) | data.ecmwf.int (open data) | Free | GRIB2 | 51-member ensemble for probabilistic forecasts |

**Why JSON-first:** NWS already serves GFS-quality forecasts as JSON. PirateWeather wraps GFS/HRRR/NBM into JSON. These cover the same underlying models that GRIB2 would give us, just processed. GRIB2 adds ECMWF (the world's best model) and GEM -- worth adding later, but not needed for a working MVP.

**Why NOT AccuWeather:** ToS explicitly prohibits (1) AI/ML training with their data, (2) commingling with other weather sources, (3) caching beyond 2 weeks. All three are core to our architecture. Disqualified.

**Ingestion schedule (cron-based, no Airflow needed for MVP):**
- NWS: Every 2 hours (rate-limit friendly for 44 cities, ~1s between requests)
- Visual Crossing: Every 6 hours (conserve API quota, $35/mo plan)
- PirateWeather: Every 2 hours (10K free calls/mo = ~44 cities x 7 calls/day fits)
- OpenWeatherMap: Every 6 hours (1M free calls/mo, well within limits)

**For each city, each run, store:**
- `source`, `city_id`, `forecast_date`, `issued_at`
- `temp_high`, `temp_low` (the two market types we track)
- `raw_response` (JSONB, for debugging and reprocessing)

**Resilience:**
- Retry with exponential backoff on API failures
- Alert (log + optional email) if a source is down for >2 consecutive cycles
- Never block other sources if one fails
- Validate temperature ranges (sanity check: reject values outside -50F to 150F)

### 1.3 Kalshi Market Data Service (Python)

**Use `pykalshi` (community library, v0.4.0, updated March 2026)** -- NOT the official SDK which is REST-only with no WebSocket/rate limiting.

**Why pykalshi over official SDK:**
- Official `kalshi_python_sync`: auto-generated REST wrapper, no WebSocket, no rate limit handling, no retry logic
- `pykalshi`: REST + WebSocket streaming + `Feed` class + `OrderbookManager` + automatic exponential backoff on rate limits + typed Pydantic models + Python 3.9-3.13
- PyPI: `pip install pykalshi` | GitHub: github.com/ArshKA/kalshi-client

**Authentication:** RSA-PSS signed headers (KALSHI-ACCESS-KEY + signature + timestamp). Key pair generated in Kalshi dashboard (Account > Security > API Keys). Private key .pem file downloaded once. pykalshi handles signing automatically.

**Market discovery pipeline:**
1. `GET /series` with category filter → find all weather series tickers (e.g., `KXHIGHNY`, `KXLOWCHI`)
2. `GET /events?series_ticker=X&with_nested_markets=true` → get all bracket markets per event, including strike ranges
3. Map each series ticker → city → NWS station ID for forecast matching

**Real-time data via WebSocket (preferred over polling):**
- **`ticker` channel:** Subscribe to all weather market tickers. Receives real-time: last_price, yes_bid, yes_ask, volume, open_interest. Used for recommendation generation — lock entry price at the moment of recommendation.
- **`market_lifecycle_v2` channel:** Receives settlement notifications in real-time (market `determined` with `result` and `settlement_value`). No need to poll for resolved markets.
- **`trade` channel:** Trade execution notifications for volume/liquidity analysis.
- WebSocket connection: `wss://api.elections.kalshi.com/trade-api/ws/v2`

**REST endpoints for batch/historical data:**
- `GET /markets?status=settled&series_ticker=X` → bulk query settled market outcomes
- `GET /series/{series}/markets/{ticker}/candlesticks` → OHLC historical price data (1min, 60min, daily intervals). Enables historical price analysis even for past markets.
- `GET /historical/markets/{ticker}/candlesticks` → archived market candlesticks beyond the live cutoff
- `GET /markets/{ticker}/orderbook` → full depth orderbook for liquidity assessment (risk scoring factor)
- Store snapshots from WebSocket stream into `kalshi_market_snapshots` table

**Rate limits:** Basic tier = 20 reads/sec, 10 writes/sec. WebSocket has no rate limit for receiving. With WebSocket as primary data source, REST is only needed for initial discovery and historical backfill — well within limits.

**Historical price backfill:** The candlestick endpoints provide OHLC data for settled markets. On first deployment, backfill available historical weather market prices to bootstrap the profit backtesting dataset.

### 1.4 Database Schema (PostgreSQL)

```sql
-- Core reference data
cities (id, name, kalshi_ticker_prefix, nws_station_id, timezone, lat, lon)

-- Weather data
weather_forecasts (id, source, city_id, forecast_date, issued_at,
                   temp_high, temp_low, raw_response JSONB,
                   created_at)
-- Index: (city_id, forecast_date, source, issued_at DESC)

-- Kalshi market data
kalshi_markets (id, event_id, market_id, ticker, city_id, forecast_date,
                market_type [HIGH|LOW], bracket_low, bracket_high,
                is_edge_bracket, status, settlement_value,
                created_at, updated_at)

kalshi_market_snapshots (id, market_id, timestamp,
                         yes_bid, yes_ask, no_bid, no_ask,
                         volume, open_interest)
-- Index: (market_id, timestamp DESC)

-- Model outputs
predictions (id, city_id, forecast_date, market_type, model_version,
             predicted_temp, std_dev,
             probability_distribution JSONB,  -- {bracket: probability}
             created_at)

-- Recommendations
recommendations (id, prediction_id, market_id, direction [BUY_YES|BUY_NO],
                  model_probability, kalshi_probability, gap,
                  expected_value, risk_score, risk_factors JSONB,
                  created_at)

-- Paper trading (dual mode)
paper_trades_fixed (id, recommendation_id, entry_price, contracts_qty=1,
                    settled_at, settlement_outcome [WIN|LOSS],
                    pnl, created_at)

paper_trades_portfolio (id, recommendation_id, portfolio_id,
                        entry_price, contracts_qty, position_size_usd,
                        settled_at, settlement_outcome, pnl, created_at)

paper_portfolios (id, name, initial_balance, current_balance,
                  sizing_strategy [FIXED_PCT|KELLY|CONFIDENCE_SCALED],
                  created_at)

-- Users (multi-user ready, single admin for MVP)
users (id, email, password_hash, role [ADMIN|USER], preferences JSONB,
       created_at)

user_trades (id, user_id, market_id, direction, entry_price,
             contracts_qty, notes, settled_at, settlement_outcome,
             pnl, created_at)

-- Authentication
refresh_tokens (id, user_id, token_hash, expires_at, created_at, revoked_at)

-- Notification tracking
email_logs (id, user_id, email_type, sent_at, recommendation_ids[])
```

---

## Phase 2: Prediction Engine (Python)

### 2.1 Multi-Model Temperature Aggregation

**Three model tiers (build incrementally):**

**Tier 1 - Baseline (build first):**
- Equal-weight average of all 4 JSON sources for temp_high and temp_low (scales to 7 when GRIB2 sources added)
- Simple but proven — multi-model averaging consistently outperforms any single model
- Standard deviation across sources = uncertainty measure

**Tier 2 - Performance-Weighted (build after 30+ days of data):**
- Weight each source by inverse of its rolling 30-60 day RMSE against actual NWS settlement values
- Recalculate weights daily
- Per-city weights (some models are better for coastal vs. inland cities)

**Tier 3 - EMOS (build after 60+ days of data):**
- Ensemble Model Output Statistics: nonhomogeneous Gaussian regression
- Predicts calibrated mean AND variance from ensemble inputs
- Trained on recent forecast-vs-observation pairs with sliding window
- This is the state-of-the-art for probabilistic temperature forecasting

### 2.2 Probability Distribution → Bracket Mapping

For each city + date + market_type (HIGH/LOW):
1. Model outputs: predicted_temp (mean) and std_dev
2. Assume Gaussian distribution: N(predicted_temp, std_dev²)
3. For each Kalshi bracket [low, high]: P(bracket) = Φ((high - mean) / std) - Φ((low - mean) / std)
4. Edge brackets: P(below X) = Φ((X - mean) / std), P(above Y) = 1 - Φ((Y - mean) / std)
5. Verify: sum of all bracket probabilities ≈ 100%

**Calibration (critical for accuracy):**
- Track predicted vs. actual frequencies using reliability diagrams
- If model says 30% probability events happen 40% of the time → recalibrate
- Platt scaling or isotonic regression for post-hoc calibration
- Evaluate with Brier score (lower = better calibrated)

### 2.3 Model Evaluation & Tracking (MLflow)

**Metrics to track per model version:**
- MAE / RMSE on raw temperature predictions
- Bracket accuracy: % of times the actual temp falls in the highest-probability bracket
- Brier score: calibration quality of probability estimates
- CRPS: overall quality of the probability distribution
- Profitability: simulated P&L if we traded on these predictions

**Backtesting (two-phase approach):**
- **Phase A (immediate):** Weather-only accuracy backtest using Visual Crossing's 50+ year historical archive. Measures: "how accurately does our multi-model aggregation predict actual temperatures?" No Kalshi data needed.
- **Phase B (immediate via candlestick API):** Kalshi provides OHLC candlestick data for settled markets via `GET /series/{series}/markets/{ticker}/candlesticks` and `GET /historical/markets/{ticker}/candlesticks`. On first deployment, backfill all available historical weather market prices. This gives us both weather accuracy AND profit backtesting data from day one.
- **Ongoing collection:** WebSocket stream + REST snapshots continue building our dataset for richer backtesting over time.

---

## Phase 3: Recommendation & Risk Engine (Python)

### 3.1 Recommendation Generation

**Trigger:** New prediction generated → compare against current Kalshi prices
**Entry price:** Locked at the yes_ask/no_ask price at the exact moment the recommendation is created. This is the paper trade entry price.

**Logic:**
```
For each bracket in each market:
  gap = model_probability - kalshi_probability

  if gap > +threshold (e.g., 15%):
    → BUY YES (model thinks event is MORE likely than market prices)
    → cost = yes_ask price
    → EV = (model_prob * $1.00) - cost - fees

  if gap < -threshold (e.g., -15%):
    → BUY NO (model thinks event is LESS likely than market prices)
    → cost = no_ask price (= 1 - yes_bid)
    → EV = ((1 - model_prob) * $1.00) - cost - fees

  Only recommend if EV > min_ev_threshold (e.g., $0.05 per contract)
```

**Fee calculation:** `round_up(0.07 * contracts * price * (1 - price))` for taker orders

### 3.2 Risk Scoring System

Each recommendation gets a risk score (1-10) based on weighted factors:

| Factor | Weight | Low Risk (1-3) | High Risk (7-10) |
|--------|--------|----------------|-------------------|
| Forecast spread | 25% | All sources within 2°F | Sources differ by 7°F+ |
| Source agreement | 20% | All 4 sources agree on direction | 2 or fewer agree |
| City historical accuracy | 15% | Model has 80%+ accuracy for this city | Below 60% accuracy |
| Market liquidity | 10% | Volume > 100 contracts | Volume < 10 contracts |
| Bracket edge proximity | 15% | Predicted temp is center of bracket | Predicted temp is within 0.5°F of bracket edge |
| Forecast lead time | 15% | Same-day forecast | 2+ days out |

**Risk thresholds (configurable):**
- Risk 1-3: "HIGH CONFIDENCE" - safe to trade
- Risk 4-6: "MODERATE" - proceed with caution
- Risk 7-10: "HIGH RISK" - flagged with explicit warning, not included in email by default

**HIGH SPREAD flag:** When source spread > 5°F (matching the email example behavior)

### 3.3 Paper Trading Engine (Dual Mode)

**Mode 1: Fixed-Size Accuracy Tracking**
- 1 contract per recommendation, regardless of confidence
- Measures pure win rate (% of settled trades that are profitable)
- Primary metric: "X% of recommended trades were correct"
- This is the number that validates the 70% accuracy target

**Mode 2: Virtual Portfolio**
- Starts with configurable virtual balance (default: $10,000)
- Position sizing strategies (configurable):
  - **Fixed percentage:** Risk X% of portfolio per trade (e.g., 2%)
  - **Confidence-scaled:** Larger positions on higher-confidence, lower-risk recommendations
  - **Kelly criterion:** Mathematically optimal sizing based on edge and odds
- Tracks: total P&L, ROI, max drawdown, Sharpe ratio, win rate by position size
- Shows "if you had traded every recommendation with $10K, you'd have $X"

**Settlement process (dual-source verification):**
- **Primary:** Poll Kalshi API for settlement outcomes on resolved markets (WIN/LOSS per contract). Most reliable, zero DST risk — Kalshi handles all edge cases
- **Secondary:** Pull NWS Daily Climate Report values for each city's actual high/low as independent verification and for model accuracy tracking
- DST note: Kalshi shifts reporting windows during DST (1:00 AM to 12:59 AM next day). By using Kalshi's own settlement values we avoid implementing this ourselves
- Update P&L, running accuracy stats, portfolio balance

---

## Phase 4: Notification Service (Python)

### 4.1 Daily Email Digest

**Format (matching your example):**
```
Kalshi Bot · Mar 09 · 11:55 PM
X markets shown

——— TOMORROW Mar 10 ———

MIAMI — HIGH 71–72°F
Kalshi: 54%
Model: 12% → BUY NO (gap: -42%)  Risk: 3/10
Models: NWS 72° | VC 71° | PW 73° | OWM 74° | Spread: 3°
(ECMWF/GEM/ENS shown here when available after Sprint 7)
————————————————
[... more markets ...]
────────────────────
Paper Trading Stats (last 7 days): 14/18 correct (78%)
Portfolio: $10,450 (+4.5%)
────────────────────
Not financial advice.
```

**Email configuration:**
- Provider: SendGrid (free tier: 100 emails/day, plenty for MVP)
- Send time: configurable, default 11:55 PM for next-day markets
- Filters: minimum gap threshold, maximum risk score, specific cities
- Include paper trading performance summary in each email

### 4.2 Additional Alerts (Phase 2+)
- High-confidence opportunity alerts (gap > 40%, risk < 4)
- Weekly performance report (accuracy breakdown by city, risk level, direction)
- Data pipeline health alerts (source down, stale data)

---

## Phase 5: Web Application

### 5.1 Backend API (Spring Boot)

**Endpoints:**

```
Auth:
  POST /api/auth/login
  POST /api/auth/refresh                # exchange refresh token for new access token
  POST /api/auth/logout                 # revoke refresh token (server-side)
  POST /api/auth/register (disabled for MVP, admin-only)

Recommendations:
  GET  /api/recommendations              # current recs, filterable
  GET  /api/recommendations/{id}         # single rec with full detail
  GET  /api/recommendations/history      # past recs with outcomes

Paper Trading:
  GET  /api/paper-trades                 # all paper trades
  GET  /api/paper-trades/performance     # accuracy stats, P&L charts
  GET  /api/paper-trades/portfolio       # virtual portfolio state

Markets:
  GET  /api/markets                      # all tracked Kalshi markets
  GET  /api/markets/{city}               # markets for a specific city
  GET  /api/markets/{id}/predictions     # our model vs. market for a market

User Trades:
  GET  /api/user/trades                  # user's logged trades
  POST /api/user/trades                  # log a new real trade
  PUT  /api/user/trades/{id}             # update trade (e.g., mark settled)
  GET  /api/user/trades/performance      # user's real trade performance

Settings:
  GET  /api/user/settings                # email prefs, risk threshold, cities
  PUT  /api/user/settings

System:
  GET  /api/health                       # service health
  GET  /api/data-status                  # last ingestion times per source
```

### 5.2 Frontend (React)

**Pages:**

1. **Dashboard** (home)
   - Today's top recommendations sorted by EV, with risk badges
   - Quick stats: paper trade accuracy (7d/30d/all), portfolio value
   - Data freshness indicators per source

2. **Markets Browser**
   - All Kalshi weather markets in a filterable table
   - For each: city, date, bracket, Kalshi price, our model probability, gap, risk
   - Click to expand: see all 4 source forecasts (7 after Sprint 7), spread, historical accuracy

3. **Paper Trading**
   - Trade history table with filters (date, city, direction, outcome)
   - Performance charts: accuracy over time, cumulative P&L, drawdown
   - Breakdown: by city, by risk level, by direction (YES vs. NO), by market type (HIGH vs. LOW)
   - Portfolio mode: balance over time, position sizing analysis

4. **My Trades**
   - Log real trades manually (market, direction, price, quantity)
   - Compare personal performance vs. bot recommendations
   - Track: "did I follow the bot's advice? What was my accuracy vs. the bot's?"

5. **Analytics**
   - Model performance: reliability diagrams, Brier scores, RMSE trends
   - City-level accuracy heatmap
   - Source contribution analysis (which sources are most accurate?)
   - Seasonal patterns

6. **Settings**
   - Email preferences: on/off, send time, filters
   - Risk threshold for recommendations
   - City watchlist
   - Paper portfolio configuration

### 5.3 Tech Details
- React with TypeScript
- Charting: Recharts or Chart.js for performance visualizations
- State management: React Query for server state
- UI framework: Tailwind CSS or Material UI
- Responsive design (usable on mobile for quick checks)

---

## Phase 6: Infrastructure & DevOps

### 6.1 Docker Setup
- One Dockerfile per service (5 services)
- `docker-compose.yml` for local dev (all services + PostgreSQL)
- `docker-compose.prod.yml` for VPS deployment (production configs, restart policies, resource limits)
- Named volumes for PostgreSQL data persistence
- `.env` files for secrets (API keys, DB credentials, SendGrid key)
- **Non-root container users:** Each Dockerfile creates an `app` user (uid 1000) and runs as `USER app`. Limits blast radius of container compromise.
- **Minimal base images:** Use `python:3.12-slim` for Python services, standard JDK slim for Spring Boot. Smaller attack surface, fewer CVEs.

### 6.2 VPS Deployment
- Target: DigitalOcean or Linode VPS (4GB RAM, 2 vCPU should suffice for MVP)
- Docker Compose on the VPS
- Nginx reverse proxy for the web app (SSL via Let's Encrypt)
- **Same-origin routing:** Nginx serves React SPA at `/` and proxies `/api/` to Spring Boot. Same origin = no CORS configuration needed.
- Automated backups: PostgreSQL pg_dump daily, **GPG-encrypted** (AES-256, symmetric passphrase stored at `/root/.backup_passphrase` chmod 600), uploaded to object storage (S3/B2)
- UFW firewall: only expose 80/443
- **SSH hardening:** Key-only authentication, password auth disabled in sshd_config

### 6.3 CI/CD
- GitHub Actions pipeline:
  - On PR: lint + unit tests + build Docker images + **security scans**
  - On merge to main: build, push images, SSH deploy to VPS
- Deployment: `docker compose pull && docker compose up -d` on VPS
- **Dependency scanning:** Dependabot (auto-PRs for vulnerable deps) + `pip-audit --strict` in CI for Python + `npm audit --audit-level=high` for React
- **Docker image scanning:** Trivy scans built images for OS-level CVEs, fails build on HIGH/CRITICAL

### 6.4 Monitoring
- **Prometheus** metrics from each service (via micrometer for Spring Boot, prometheus_client for Python)
- **Grafana** dashboards:
  - System: CPU, memory, disk, container health
  - Data pipeline: ingestion success/failure rates, data freshness per source
  - Model: prediction accuracy trends, Brier score over time
  - Business: daily recommendations count, paper trade P&L, accuracy trend
- **Alerting:** Grafana alerts → email/Discord for critical issues (data source down >4 hours, accuracy drop below 60%)

### 6.5 Security

**Secrets Management:**
- Kalshi RSA private key (.pem): volume-mounted into containers as read-only (`./secrets/kalshi_private.pem:/app/secrets/kalshi_private.pem:ro`), file permissions 600 on host. Referenced via `KALSHI_KEY_PATH` env var.
- All other secrets (API keys, DB password, JWT secret, SendGrid key): `.env` file with 600 permissions, never committed to git
- SendGrid API key scoped to "Mail Send" permission only — no account management access

**Database Security:**
- PostgreSQL port NOT published to host — only accessible via internal Docker `backend` network (`internal: true`)
- PostgreSQL SSL enabled: self-signed cert for container-to-container encryption (defense-in-depth)
- Connection strings use `?sslmode=require`

**Web API Security:**
- **Rate limiting (two layers):**
  - Nginx: 3 req/min per IP on `/api/auth/login` (brute-force protection), 10 req/sec per IP on `/api/*` (general abuse)
  - Spring Boot Bucket4j: per-user token bucket rate limiting on authenticated endpoints (60 req/min per user)
- **Input validation:** Jakarta Bean Validation (`@Valid`, `@NotNull`, `@Size`, `@Min`, `@Max`, `@Pattern`) on all request DTOs. JPA parameterized queries prevent SQL injection. User-provided text (trade notes) sanitized with Jsoup before storage.
- **CORS:** Not needed — same-origin via Nginx path routing (React at `/`, API at `/api/`)
- **CSP headers:** Nginx adds `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';`

**JWT Authentication:**
- Signing: HMAC-SHA256 (HS256) with 256-bit random secret in `.env`
- Access token: 15-minute expiration, stored in httpOnly + Secure + SameSite=Strict cookie
- Refresh token: 14-day expiration, stored in `refresh_tokens` DB table (hashed), rotated on each use
- CSRF protection: SameSite=Strict cookie + custom `X-Requested-With` header check
- Revocation: delete refresh token from DB forces re-login
- **New DB table:**
  ```sql
  refresh_tokens (id, user_id, token_hash, expires_at, created_at, revoked_at)
  ```

**Logging Security:**
- Structured JSON logging (Python: structlog, Java: Logback JSON encoder)
- Automatic redaction filter: fields matching `api_key`, `password`, `token`, `authorization`, `private_key`, `secret`, `credential` replaced with `[REDACTED]`
- Never log full HTTP request/response bodies from external APIs (may contain credentials in headers)
- Log: status codes, URLs (without sensitive query params), timing, error categories

**Secrets Rotation Runbook:**
- Document at `docs/secrets-rotation.md`: for each secret (Kalshi RSA key, weather API keys, SendGrid key, JWT secret, DB password), steps to generate a new value, update configuration, deploy without downtime, and verify
- Manual process for MVP, no automated rotation

### 6.6 Error Handling & Traceability

**Correlation IDs:**
- Each pipeline run (ingestion → prediction → recommendation → paper trade) generates a unique correlation ID at the start
- Correlation ID flows through every function call and is included in all log entries and error responses
- API error responses include the correlation ID so frontend errors can be traced to backend logs

**Structured Error Categories:**
```
WEATHER_API_ERROR    — External weather API failures (HTTP errors, timeouts, invalid responses)
KALSHI_API_ERROR     — Kalshi REST/WebSocket failures (auth, rate limits, connection drops)
PREDICTION_ERROR     — Model failures (insufficient data, NaN results, bracket sum mismatch)
RECOMMENDATION_ERROR — Gap/EV calculation failures, missing market data
PAPER_TRADE_ERROR    — Settlement failures, missing outcomes, P&L calculation errors
DB_ERROR             — PostgreSQL connection, migration, constraint violations
NOTIFICATION_ERROR   — SendGrid failures, email template errors
VALIDATION_ERROR     — Input validation failures (API requests, weather data range checks)
```

**Error Context:**
- Every error log includes: `correlation_id`, `timestamp`, `service`, `error_category`, `source` (which API/city/market), `operation` (which step), `error_message`, `stack_trace` (internal logs only)
- API error responses include: `error_code`, user-friendly `message`, `correlation_id` — never stack traces

**Custom Exception Hierarchy (Python):**
```python
class WeatherBotError(Exception):
    def __init__(self, message, category, correlation_id=None, city=None, source=None, **context):
        self.category = category
        self.correlation_id = correlation_id
        self.city = city
        self.source = source
        self.context = context
        super().__init__(message)

class WeatherApiError(WeatherBotError): pass
class KalshiApiError(WeatherBotError): pass
class PredictionError(WeatherBotError): pass
class PaperTradeError(WeatherBotError): pass
```

---

## Phase 7: Testing Strategy

### 7.1 Unit Tests (mandatory for all logic)
- Temperature aggregation math (weighted averages, std dev)
- Gaussian CDF bracket probability calculations
- Risk scoring formula with known inputs → known outputs
- EV and fee calculations
- Paper trade settlement logic
- API request/response serialization

### 7.2 Integration Tests (real database, no mocks)
- Full pipeline: mock weather API responses → ingest → predict → recommend → paper trade
- Database migrations run cleanly on empty database
- API endpoints return correct data with seeded database
- External API clients tested with recorded responses (WireMock for Kalshi, VCR for weather APIs)

### 7.3 Backtesting Suite
- Historical weather data (Visual Crossing provides 50+ years)
- Historical Kalshi market data (collect from API, or source from community datasets)
- Run full pipeline on historical data → measure hypothetical P&L
- Statistical significance: is 70% accuracy real or could it be luck? (binomial test)
- Segmented analysis: accuracy by city, season, market type, risk level

### 7.4 Live Paper Trading Validation
- Minimum 30-day paper trading period before trusting the system
- Daily monitoring of accuracy vs. 70% target
- If accuracy < 65% after 30 days: investigate model, don't trust recommendations
- Compare: all recs vs. low-risk-only vs. high-EV-only

---

## Tech Stack Summary

| Layer | Technology | Justification |
|-------|-----------|---------------|
| Data Ingestion | Python, Requests, APScheduler, pykalshi | Best ecosystem for API clients; pykalshi covers REST + WebSocket + rate limiting |
| Prediction Engine | Python, scikit-learn, SciPy, NumPy | Gaussian CDF, statistical models, ML |
| ML Tracking | MLflow | Experiment tracking, model versioning |
| Recommendation Engine | Python (same as prediction) | All math in one language, no cross-service boundary |
| GRIB2 Parsing | cfgrib, xarray, eccodes | Parse ECMWF/GEM/ENS/GFS native format |
| Web API (serving only) | Java Spring Boot | Reads finished results from DB, serves to frontend |
| Frontend | React + TypeScript | Rich interactive dashboard with charts |
| Database | PostgreSQL | Reliable, JSONB support for flexible data |
| Email | SendGrid | Free tier sufficient, good deliverability |
| Containerization | Docker + Docker Compose | Simple deployment on VPS |
| Reverse Proxy | Nginx + Let's Encrypt | SSL, static file serving |
| Monitoring | Prometheus + Grafana | Industry standard, rich dashboards |
| CI/CD | GitHub Actions | Free for public repos, integrated with GitHub |

**What we're NOT using initially (but architecture supports later):**
- Kafka (overkill for MVP — cron-based pipeline is sufficient)
- Kubernetes (Docker Compose on VPS is simpler and cheaper)
- Airflow (APScheduler or cron handles our scheduling needs)
- TensorFlow (scikit-learn + SciPy covers our statistical models)
- GRIB2/eccodes/cfgrib (deferred to Sprint 7 — JSON sources cover MVP)
- AccuWeather (ToS prohibits AI/ML training, data commingling, and caching >2 weeks)
- Official Kalshi SDK (REST-only, no WebSocket/rate limiting — pykalshi is superior)

---

## Development Phases (Recommended Order)

### Sprint 1: Data Foundation (JSON sources + Kalshi)
- [ ] Project scaffolding, monorepo structure, Docker Compose
- [ ] **Error handling foundation:** correlation ID generator, custom exception hierarchy (WeatherBotError, KalshiApiError, etc.), structured JSON logging with redaction filter
- [ ] **Docker security:** non-root `app` user in all Dockerfiles, `python:3.12-slim` base images
- [ ] **Secrets setup:** volume-mount Kalshi RSA .pem (read-only, chmod 600), `.env` for API keys (chmod 600, `.gitignore`d)
- [ ] PostgreSQL schema + migrations (Alembic), including `refresh_tokens` table
- [ ] **Database security:** internal-only Docker network (`internal: true`), PostgreSQL SSL enabled, port NOT published to host
- [ ] City config: query Kalshi API for all weather series → map to NWS station IDs → lat/lon
- [ ] NWS API client + ingestion job (api.weather.gov, JSON, no key)
- [ ] Visual Crossing API client + ingestion job (JSON, $35/mo)
- [ ] PirateWeather API client + ingestion job (JSON, Dark Sky-compatible, $2/mo)
- [ ] OpenWeatherMap API client + ingestion job (JSON, free 1M calls/mo)
- [ ] pykalshi integration: auth setup (RSA-PSS key pair via mounted .pem file)
- [ ] Kalshi market discovery: series → events → markets pipeline
- [ ] Kalshi WebSocket client via pykalshi Feed: `ticker` channel for real-time prices
- [ ] Kalshi WebSocket: `market_lifecycle_v2` channel for settlement notifications
- [ ] Kalshi historical backfill: candlestick endpoints for settled weather markets
- [ ] Kalshi REST fallback: snapshot polling for WebSocket reconnects
- [ ] Unit tests for all API clients, data validation, and error handling (verify correlation IDs flow through, error categories are correct)
- [ ] Integration test: full ingestion cycle with real APIs
- [ ] Verify all 44+ cities return valid data from all 4 weather sources

### Sprint 2: Prediction & Recommendation Engine (all Python)
- [ ] Tier 1 model: equal-weight temperature averaging across 4 JSON sources
- [ ] Gaussian probability distribution → bracket probability mapping
- [ ] Recommendation engine: gap detection, direction, EV calculation
- [ ] Entry price capture: lock yes_ask/no_ask at recommendation generation time
- [ ] Fee calculation matching Kalshi's formula
- [ ] Risk scoring system (6 factors, weighted)
- [ ] Paper trade auto-creation for every recommendation
- [ ] Unit tests for all prediction and recommendation math
- [ ] Manual validation: compare output to the email example format

### Sprint 3: Paper Trading & Email
- [ ] Paper trading engine: fixed-size mode
- [ ] Paper trading engine: virtual portfolio mode (with position sizing)
- [ ] Settlement job: primary = Kalshi API settlement outcomes (WIN/LOSS), secondary = NWS CLI reports for verification
- [ ] Daily email digest generation (matching example format)
- [ ] SendGrid integration (**API key scoped to "Mail Send" only**)
- [ ] Performance statistics calculation (accuracy, P&L, breakdown)
- [ ] Integration test: full pipeline from ingestion → email

### Sprint 4: Web Application
- [ ] Spring Boot API (serving only, reads from DB): all endpoints listed in 5.1
- [ ] **JWT authentication:** HS256 signing, 15-min access tokens in httpOnly/Secure/SameSite=Strict cookies, 14-day refresh tokens (DB-stored, rotated on use), `/api/auth/refresh` endpoint
- [ ] **Input validation:** Jakarta Bean Validation (`@Valid`) on all request DTOs, Jsoup sanitization on user text fields (trade notes)
- [ ] **Rate limiting:** Bucket4j per-user token bucket on authenticated endpoints (60 req/min)
- [ ] **CSRF protection:** SameSite=Strict + `X-Requested-With` header check
- [ ] **Global error handler:** `@ControllerAdvice` returning structured JSON errors with error_code, message, correlation_id (never stack traces)
- [ ] React app: Dashboard page
- [ ] React app: Markets Browser page
- [ ] React app: Paper Trading page with charts
- [ ] React app: My Trades page (manual trade logging)
- [ ] React app: Analytics page
- [ ] React app: Settings page

### Sprint 5: Deployment & Monitoring
- [ ] Production Docker Compose config (internal-only `backend` network, no published DB port)
- [ ] VPS provisioning and deployment
- [ ] **SSH hardening:** key-only auth, disable password auth in sshd_config
- [ ] **Nginx setup:** SSL (Let's Encrypt) + same-origin routing (React at `/`, API at `/api/`) + rate limiting (3 req/min on `/api/auth/login`, 10 req/sec on `/api/*`) + CSP headers
- [ ] Prometheus metrics instrumentation
- [ ] Grafana dashboards (system, pipeline, model, business)
- [ ] Alerting rules
- [ ] **Encrypted backups:** GPG-encrypted pg_dump (AES-256) → object storage, daily cron
- [ ] **CI/CD pipeline:** GitHub Actions + Dependabot config + `pip-audit` + `npm audit` + Trivy image scanning
- [ ] **Secrets rotation runbook:** `docs/secrets-rotation.md` — steps for each secret (Kalshi, weather APIs, SendGrid, JWT, DB password)

### Sprint 6: Validation Period (30+ days)
- [ ] Run full system live with paper trading
- [ ] Daily accuracy monitoring
- [ ] Identify and fix data quality issues
- [ ] Tune risk scoring thresholds
- [ ] Tune recommendation gap thresholds
- [ ] Collect enough data to train Tier 2 (performance-weighted) model
- [ ] Backtesting with accumulated historical data

### Sprint 7: GRIB2 Sources (ECMWF/GEM/ENS)
- [ ] Herbie + cfgrib + xarray + eccodes installation and validation
- [ ] ECMWF open data client: download 2m temperature forecasts via Herbie
- [ ] GEM (ECCC Datamart) client: download GDPS temperature forecasts
- [ ] ENS (ECMWF Ensemble) client: download 51-member ensemble temperature data
- [ ] Point extraction: lat/lon lookup for all 44+ cities from GRIB2 grids
- [ ] Integration into prediction engine (re-weight model with 7 sources)
- [ ] Unit tests for GRIB2 parsing and point extraction
- [ ] Validate ECMWF forecasts improve model accuracy vs. JSON-only baseline

### Ongoing: Model Improvement
- [ ] Tier 2: performance-weighted averaging (after 30 days of data)
- [ ] Tier 3: EMOS (after 60 days of data)
- [ ] Calibration tuning via Brier score and reliability diagrams
- [ ] City-specific model adjustments
- [ ] Seasonal pattern adaptation
- [ ] Evaluate whether GRIB2 sources (ECMWF/GEM/ENS) improve accuracy enough to justify ongoing maintenance

---

## Verification Plan

| What | How | Success Criteria |
|------|-----|-----------------|
| Data ingestion | Run all 4 API clients for 44 cities, verify stored data | All cities return valid forecasts, no gaps >6 hours |
| Forecast sanity | Compare 4 MVP sources for same city/date | Sources generally agree within 5-10°F, outliers flagged |
| Bracket probabilities | Sum probabilities across all brackets for a market | Sum = 99-101% (rounding tolerance) |
| Recommendation math | Manually verify 10 recommendations against known inputs | Gap, EV, fees all match hand calculations |
| Risk scoring | Test edge cases: high spread, low liquidity, bracket edges | Scores match expected ranges for known scenarios |
| Paper trading | Settle 7 days of paper trades against actual NWS data | All settlements correct, P&L math verified |
| Email format | Compare generated email to the example in project description | Format matches, all fields present, HIGH SPREAD flags work |
| Web dashboard | Manual QA of all 6 pages | Data matches email, charts render, trade logging works |
| End-to-end | Full daily cycle: ingest → predict → recommend → paper trade → email | Complete without errors for 7 consecutive days |
| Accuracy target | 30-day paper trading period | 70%+ accuracy on recommended trades |

---

## Recommendations Beyond Original Description

**Research-backed decisions (all verified via deep dives):**

1. **JSON-first, GRIB later:** MVP uses 4 JSON weather APIs (NWS, Visual Crossing, PirateWeather, OpenWeatherMap). NWS already serves GFS-quality forecasts as JSON. PirateWeather wraps GFS/HRRR/NBM into JSON. GRIB2 sources (ECMWF/GEM/ENS) deferred to Sprint 7 — they add the world's best forecast model (ECMWF) but require eccodes/cfgrib/xarray setup. Proven: eccodes now installs via pip on Windows (since v2.43.0), but ECMWF labels Windows support "untested."

2. **AccuWeather disqualified:** ToS explicitly prohibits AI/ML training with their data, commingling with other weather sources, and caching beyond 2 weeks. All three are core to our architecture. Replaced with OpenWeatherMap (free 1M calls/mo, commercial use allowed, no ToS conflicts).

3. **pykalshi over official Kalshi SDK:** Official `kalshi_python_sync` is an auto-generated REST wrapper — no WebSocket, no rate limiting, no retry logic. `pykalshi` (v0.4.0, March 2026, github.com/ArshKA/kalshi-client) provides REST + WebSocket `Feed` class + `OrderbookManager` + automatic exponential backoff on rate limits + typed Pydantic models.

4. **All math in Python:** Recommendation logic, risk scoring, EV calculation, and paper trading all live in the prediction engine (Python). Spring Boot only serves the web API. Eliminates a cross-language boundary.

5. **Dual-source settlement:** Kalshi API for settlement outcomes (WIN/LOSS) as primary source, NWS CLI reports as secondary verification. Avoids implementing DST edge cases ourselves — Kalshi handles DST shifting (1:00 AM to 12:59 AM next day) internally.

6. **Entry price locked at recommendation time:** Paper trades capture the exact yes_ask/no_ask price when the recommendation is generated. Most realistic simulation.

7. **Two-phase backtesting:** Weather accuracy backtest immediately (Visual Crossing 50+ year archive). Profit backtest also available from day one — Kalshi's candlestick API provides OHLC data for settled markets (1min, 60min, daily intervals). Backfill on first deployment.

8. **Calibration is the secret weapon:** Well-calibrated probability estimates matter more than raw temperature accuracy. Brier score tracking and reliability diagrams are first-class features.

9. **Start collecting data immediately:** Deploy ingestion service first, even before the prediction engine. Every day of weather + Kalshi market data collected enables better backtesting.

10. **Skip Kafka/Airflow/K8s initially:** Cron/APScheduler + Docker Compose on VPS. Architecture supports adding these later when scale justifies it.
