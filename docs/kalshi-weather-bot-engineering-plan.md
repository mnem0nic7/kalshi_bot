# Kalshi Weather Trading Bot — Engineering Plan

**Version:** 1.0
**Audience:** Coding team (implementation-ready spec)
**Prepared for:** Grant
**Last updated:** April 21, 2026

---

## 1. Executive Summary

Build a Python service that trades Kalshi's daily-resolving weather contracts (temperature highs, lows, and precipitation) using probabilistic forecasts derived from **free NOAA data**. The system is deliberately **selective**: it only places trades when modeled probability diverges materially from market price *and* model confidence is high. Target profile is high win-rate, low volume, positive Sharpe — not market-making.

### Why this setup wins

1. **Kalshi weather contracts settle on a known, public source** — the NWS Daily Climate Report for a specific station (e.g., KNYC = Central Park for NYC highs). Our model consumes the same upstream feeds that ultimately produce that report.
2. **Uncertainty collapses through the trading day.** By early afternoon on the settlement day, most of the day's high temperature is already observed in METARs. This creates an intraday edge that most retail traders don't systematically exploit.
3. **NOAA ensembles (GEFS) give calibrated probabilities for free.** Commercial weather APIs add no edge — they derive from the same public models. A 30-member ensemble gives us P(high > threshold) directly.
4. **Selectivity > volume.** Every trade has fees and slippage. A confidence-gated bot that trades 5 markets a day with 70% realized win-rate beats a market-maker with 55% win-rate and 100x the volume.

---

## 2. Scope

### In scope (v1)

| Series tickers (confirmed / inferred) | Market | Resolution source |
|---|---|---|
| `KXHIGHNY` | NYC daily high (KNYC) | NWS Daily Climate Report |
| `KXHIGHCHI`, `KXHIGHMIA`, `KXHIGHAUS`, `KXHIGHLA`, `KXHIGHDEN`, `KXHIGHPHI` | Daily high, other cities | NWS Daily Climate Report |
| `KXLOW*` series | Daily low | NWS Daily Climate Report |
| Precipitation / snowfall daily markets | Daily rain/snow total | NWS Daily Climate Report |

Engineers: pull the current list dynamically from `GET /series` filtered by `category=Climate` — do not hard-code. Ticker names change.

### Out of scope (v1)

- Hurricanes, named storms, seasonal climate contracts (different dynamics, don't resolve daily).
- Non-weather markets.
- Cross-venue arbitrage (Polymarket, etc.).

---

## 3. Strategy Specification

### 3.1 Edge hypothesis

For each open weather market, compute `P_model` = modeled probability the YES side resolves true. Compare to `P_market` = mid of best bid/ask (or the side we'd have to cross). Trade only when:

```
edge = P_model − P_market        (signed; trade YES if positive, NO if negative)
|edge| ≥ EDGE_THRESHOLD           (default 0.08, i.e. 8 cents of probability)
confidence ≥ CONFIDENCE_THRESHOLD (see §3.3)
```

### 3.2 Probability model

Three-layer blend, in order of increasing weight as we approach settlement:

**Layer 1 — Ensemble (GEFS, days 1–5 out).** For each of 30 GEFS members, interpolate the forecast high at the station grid point. `P_model_ens = (# members satisfying condition) / 30`, then apply a logistic calibration trained on historical ensemble-vs-observed pairs.

**Layer 2 — Deterministic blend (NBM, same-day morning).** NBM's probabilistic temperature outputs already include quantile forecasts. Use NBM's P(T > threshold) directly, bias-corrected per station.

**Layer 3 — Observation-driven (same-day afternoon).** Once hourly METARs are available, track the max observed so far, apply a diurnal climatology to estimate remaining day's temperature ceiling, and compute P(daily_high > threshold | max_so_far, hour_of_day, station).

Final `P_model` is a weighted combination whose weights depend on time-to-resolution:
- T > 48h: Layer 1 only
- 48h > T > 12h: 0.3 × L1 + 0.7 × L2
- T < 12h: 0.2 × L2 + 0.8 × L3

### 3.3 Confidence score

Independent of edge. A signal is only actionable when:

- **Ensemble agreement:** GEFS standard deviation at the relevant grid point below a per-station threshold (tighter for summer, looser for shoulder seasons).
- **Model-observation consistency:** current NBM matches observed-so-far METAR trajectory within 2°F.
- **Liquidity:** best bid-ask spread ≤ 4 cents; top-of-book depth ≥ target position size.
- **Time bucket:** not within 15 minutes of close (avoid settlement-race conditions).

All four must pass. `confidence_score` = simple product of the four normalized components, threshold 0.6 default.

### 3.4 Sizing

Fractional Kelly at **25% of full Kelly**, with hard caps:

```
f_kelly = (edge) / (1 − P_market)       # for YES buy at ask
position_size = min(
    0.25 * f_kelly * bankroll,
    MAX_POSITION_DOLLARS_PER_MARKET,    # default $100
    available_liquidity_at_price
)
```

Kelly is the right theoretical sizing but real Kelly over-sizes when `P_model` is miscalibrated. Quarter-Kelly is standard protection.

### 3.5 Execution style

- **Limit orders only** at the current ask (YES buy) or bid (NO buy). Never market orders.
- **No chasing.** If order doesn't fill within 30s, cancel. If edge still exists, requote at the new touch. Max 3 requotes per market per signal.
- **No averaging down.** One position per market per signal.
- **Exit policy:** hold to resolution. Only early-exit if confidence flips (e.g., surprise front moves through, observations diverge from model). Early-exit logic is v1.5 — v1 just holds.

---

## 4. System Architecture

```
             ┌───────────────────────┐   ┌──────────────────────┐
             │  NOAA / NWS ingesters │   │  Kalshi market data  │
             │  - NWS API            │   │  - REST: series/mkts │
             │  - NBM grids (NOMADS) │   │  - WS: orderbook     │
             │  - GEFS grids         │   └──────────┬───────────┘
             │  - METAR feed         │              │
             │  - Climate Reports    │              │
             └──────────┬────────────┘              │
                        │                           │
                        ▼                           ▼
             ┌──────────────────────────────────────────────┐
             │              Postgres + TimescaleDB           │
             │   forecasts, observations, markets, orders,   │
             │   positions, pnl, trades, audit_log           │
             └──────────┬───────────────────────────────────┘
                        │
                        ▼
             ┌──────────────────────────────────────────────┐
             │            Probability Model Service          │
             │  Layer 1 (GEFS) → Layer 2 (NBM) → Layer 3     │
             │  (METAR) → weighted P_model + confidence      │
             └──────────┬───────────────────────────────────┘
                        │
                        ▼
             ┌──────────────────────────────────────────────┐
             │           Signal & Risk Engine                │
             │  edge gate → confidence gate → sizing → risk  │
             │  checks (daily loss, total exposure, caps)    │
             └──────────┬───────────────────────────────────┘
                        │
                        ▼
             ┌──────────────────────────────────────────────┐
             │             Execution Service                 │
             │  RSA-signed Kalshi client, order state mgmt,  │
             │  fill tracking, cancel/requote logic          │
             └──────────┬───────────────────────────────────┘
                        │
                        ▼
             ┌──────────────────────────────────────────────┐
             │          Monitoring & Alerting                │
             │  Prometheus metrics, Grafana dashboards,      │
             │  Teams/Slack alerts, kill-switch endpoint     │
             └──────────────────────────────────────────────┘
```

---

## 5. Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Language | Python 3.12 | Team familiarity; ecosystem for scientific data (xarray, cfgrib, numpy) |
| Orchestration | APScheduler (v1) → Prefect or Azure Functions (v2) | Simple in-process cron → managed workflow when scale demands |
| Database | Postgres 16 + TimescaleDB | Native time-series for forecasts/observations; relational integrity for orders |
| Grid data | `xarray` + `cfgrib` + `herbie` | Herbie handles NOMADS / AWS Open Data fetching of NBM/GEFS |
| Kalshi client | `kalshi-python` official SDK, wrapped | Handles RSA-PSS signing; we add retry/rate-limit/auditing |
| WebSocket | `websockets` library | Live orderbook subscription |
| HTTP | `httpx` async | Non-blocking for parallel market polling |
| Metrics | `prometheus_client` | Standard, minimal deps |
| Logging | Structured JSON via `structlog` | Audit-friendly (important for a regulated venue) |
| Deploy target | Azure Container Apps or Azure VM (aligns with existing Libra stack) | Grant's team already operates Azure |
| Secrets | Azure Key Vault | RSA private key and any fallback creds |
| IaC | Terraform | Repeatable across dev/demo/prod |

---

## 6. Component Specifications

### 6.1 Kalshi API Client (`kalshi_client/`)

**Responsibilities**
- Wrap official SDK with RSA-PSS request signing.
- Maintain WebSocket subscriptions to orderbook channels for active markets.
- Respect rate limits (Basic tier = 20 reads/s, 10 writes/s — check `GET /account/limits` at startup and size internal semaphores accordingly).
- Emit structured audit logs for every order-related call (required for operational defensibility).

**Key endpoints used**
- `GET /series` (discovery, filter `category=Climate`)
- `GET /markets` (filter by series ticker, `status=open`)
- `GET /markets/{ticker}/orderbook`
- WebSocket `orderbook_delta` channel for live updates
- `POST /portfolio/orders` (place)
- `DELETE /portfolio/orders/{order_id}` (cancel)
- `GET /portfolio/positions`
- `GET /portfolio/balance`
- `GET /exchange/status` (ensure exchange is up before trading)

**Base URLs**
- Production: `https://api.elections.kalshi.com/trade-api/v2`
- Demo: `https://demo-api.kalshi.co/trade-api/v2`
- WebSocket prod: `wss://api.elections.kalshi.com/trade-api/ws/v2`
- WebSocket demo: `wss://demo-api.kalshi.co/trade-api/ws/v2`

**Pricing note.** As of March 2026, Kalshi uses `yes_price_dollars` as a string (e.g. `"0.6500"`). Legacy integer-cents fields are removed. Validate with a `Decimal` round-trip before placing orders.

**Acceptance criteria**
- 100% of orders are placed with signed requests verified against Kalshi's documented spec.
- Rate-limit breaches never exceed 0 per day in staging.
- Reconnection logic handles WebSocket drops within 5 seconds with no lost events (use sequence numbers).

---

### 6.2 Weather Data Ingestion (`weather_ingest/`)

Four sub-ingesters, each idempotent.

**6.2.1 GEFS ensemble** — runs every 6 hours at `00/06/12/18Z + 90min`. Pulls the 30-member ensemble for forecast hours 0–120 at 3-hour steps for all station grid points in our registry. Storage: `forecasts_ensemble` hypertable, columns: `(station_id, run_time, valid_time, member, variable, value)`. Source: NOAA AWS Open Data S3 bucket `noaa-gefs-pds` (Herbie handles this).

**6.2.2 NBM** — runs every hour at `:45`. Pulls NBM's deterministic and probabilistic fields (including quantile forecasts for 2m temperature) for forecast hours 1–36. Source: AWS `noaa-nbm-grib2-pds`.

**6.2.3 METAR observations** — runs every 5 minutes. Polls the NWS Aviation Weather API for current METARs at our target stations. Store every observation; derive `max_temp_since_midnight_local` on the fly.

**6.2.4 NWS Daily Climate Report (CLI)** — runs every 15 minutes starting 90 minutes before local midnight. Parses the CLI product for each station to detect when the official high is posted (this is the settlement value). Stops polling a station once CLI is detected. Storage: `climate_reports` table with raw text + parsed high/low/precip.

**Station registry** — seed file `stations.yaml` mapping Kalshi series → ICAO/NWS identifier:

```yaml
- kalshi_series: KXHIGHNY
  station_id: KNYC
  cli_office: OKX
  timezone: America/New_York
  grid_lat: 40.7794
  grid_lon: -73.9692
# ...one entry per city
```

**Acceptance criteria**
- GEFS / NBM ingest completes within 20 minutes of availability with < 0.1% missing fields.
- METAR lag from observation time to DB-insert time ≤ 10 minutes p99.
- CLI detection has zero false-positives (validated against 30 days of historical reports).

---

### 6.3 Probability Model Service (`model/`)

**Input:** `market_id`, `settlement_station`, `threshold_value`, `settlement_date`, `current_time`
**Output:** `P_model` (float 0–1), `confidence_score` (0–1), `diagnostics` (dict for logging)

**Layer 1 — Ensemble**
```
members = load_gefs_members(station, settlement_date, latest_run)
raw_p = count(m where m.high > threshold) / 30
P_l1 = logistic_calibration.transform(raw_p, station, month)
```
Calibration model trained on 3 years of historical pairs. Retrain monthly.

**Layer 2 — NBM**
```
q_forecast = load_nbm_quantiles(station, settlement_date)
P_l2 = P(T > threshold | quantile curve)  # interpolate quantile CDF
P_l2 = bias_correct(P_l2, station, month)
```

**Layer 3 — Observation-driven**
```
max_so_far = query_metars(station, since=local_midnight)
remaining_uncertainty = diurnal_model.remaining_max_uncertainty(
    station, hour_of_day, month, max_so_far
)
P_l3 = P(final_high > threshold | max_so_far, remaining_uncertainty)
```

**Blending** per §3.2, weights by time-to-settlement.

**Confidence** per §3.3 — returned as 4-component dict for debugging plus a single scalar.

**Acceptance criteria**
- Brier score on holdout set ≤ 0.18 for day-of signals.
- Calibration curve within ±5% of diagonal at 0.1 bucket resolution.
- Service responds within 200ms p95 for a single market evaluation (cache ensemble reads).

---

### 6.4 Signal & Risk Engine (`signal/`, `risk/`)

Runs on a 60-second cadence (configurable). For each open weather market:

1. Fetch latest orderbook snapshot.
2. Call model service for `P_model` and `confidence`.
3. Apply signal gates in order:
   - Exchange healthy? (from `/exchange/status`)
   - Confidence ≥ threshold?
   - |edge| ≥ threshold?
   - Spread ≤ max_spread?
   - Time-to-close ≥ min_time?
4. Apply risk gates:
   - Position in this market within per-market cap?
   - Total exposure across all markets ≤ bankroll cap?
   - Daily realized P&L ≥ daily loss limit? (Kill-switch if breached.)
   - Order rate across last 60s within rate-limit budget?
5. Size per §3.4. Emit signal to execution queue.

**Risk limits (defaults — Grant to confirm final numbers)**

| Control | Default | Notes |
|---|---|---|
| Max position per market | $100 | Per signal, not cumulative |
| Max total open exposure | $2,000 | Across all positions |
| Max daily gross loss | $300 | Kill-switch triggers |
| Max orders per minute | 6 | Well below rate limit ceiling |
| Min edge | 8¢ | Tune in paper |
| Min confidence | 0.6 | Tune in paper |
| Min time-to-close | 15 min | Avoid close-race conditions |
| Max spread | 4¢ | Liquidity proxy |

**Kill switch.** A single endpoint `POST /admin/halt` sets a flag read on every loop; when set, no new orders placed, existing orders cancelled. Exposed via authenticated admin interface and callable from monitoring alerts.

---

### 6.5 Execution Service (`execution/`)

Subscribes to signal queue. For each signal:

1. Place limit order at current touch.
2. Record order in `orders` table with full signal context (for later P&L attribution).
3. Watch WebSocket for fills. On fill, update `positions`.
4. If unfilled after 30s, cancel. If signal still valid (re-check edge at new touch), requote — max 3 times.
5. Log everything in `audit_log` with immutable hash chain (Grant's CISO background — give him an audit trail his compliance team will respect).

---

### 6.6 Backtest & Paper Harness (`backtest/`)

Two modes:

**Historical backtest.** Replay last 180 days. Inputs: archived GEFS/NBM/METAR/CLI data + Kalshi historical market data (available via Kalshi API — paginate `GET /markets/{ticker}/trades`). Outputs: per-day P&L, Brier score, realized win rate, Sharpe, max drawdown, calibration plot. Critical because it sets realistic expectations before capital is risked.

**Paper trading against demo.** Same codebase, demo base URL, demo API key. Run for minimum 3 weeks in parallel with live before enabling production trading. Requires zero code differences — just a `KALSHI_ENV=demo` env var.

**Acceptance criteria for go-live**
- Historical backtest Sharpe > 1.5 on last 6 months of data (realistic — prediction markets with fees rarely clear 2.0 without curve-fitting).
- Paper-trading realized win rate within 5pp of backtested win rate across ≥ 3 weeks / ≥ 40 trades.
- Zero unreconciled positions or order mismatches across the paper period.

---

### 6.7 Monitoring & Alerting (`ops/`)

**Metrics (Prometheus)**
- `kalshi_orders_placed_total{market, side}`
- `kalshi_fills_total{market, side}`
- `signal_edge_bps{market}` (histogram)
- `model_confidence{market}` (histogram)
- `position_dollars{market}`
- `daily_realized_pnl_dollars`
- `ingest_lag_seconds{source}`
- `api_rate_limit_remaining{bucket}`
- `kill_switch_enabled` (gauge)

**Dashboards (Grafana)** — one per component plus a trader-view panel showing current positions, live model-vs-market deltas, and intraday P&L.

**Alerts (to Teams via webhook — integrate with existing Libra channels)**
- Any kill-switch activation (page immediately)
- Ingest lag > 30 min for any source
- Unreconciled position detected
- Daily loss > 50% of limit (warning) or 80% (urgent)
- WebSocket disconnection > 60s
- Model calibration drift (weekly check: rolling-7d Brier score increase > 20%)

---

## 7. Data Model

Postgres schema (abbreviated — full DDL in companion `schema.sql`).

```sql
-- Reference
CREATE TABLE stations (
  station_id TEXT PRIMARY KEY,         -- 'KNYC'
  kalshi_series TEXT,                  -- 'KXHIGHNY'
  cli_office TEXT,                     -- 'OKX'
  timezone TEXT NOT NULL,
  grid_lat NUMERIC, grid_lon NUMERIC
);

-- Time series (Timescale hypertables)
CREATE TABLE forecasts_ensemble (
  station_id TEXT, run_time TIMESTAMPTZ, valid_time TIMESTAMPTZ,
  member INT, variable TEXT, value NUMERIC,
  PRIMARY KEY (station_id, run_time, valid_time, member, variable)
);
SELECT create_hypertable('forecasts_ensemble', 'valid_time');

CREATE TABLE forecasts_nbm (
  station_id TEXT, run_time TIMESTAMPTZ, valid_time TIMESTAMPTZ,
  variable TEXT, quantile NUMERIC, value NUMERIC
);
SELECT create_hypertable('forecasts_nbm', 'valid_time');

CREATE TABLE observations_metar (
  station_id TEXT, obs_time TIMESTAMPTZ,
  temp_f NUMERIC, dewpoint_f NUMERIC, raw_text TEXT,
  PRIMARY KEY (station_id, obs_time)
);
SELECT create_hypertable('observations_metar', 'obs_time');

CREATE TABLE climate_reports (
  station_id TEXT, report_date DATE, received_at TIMESTAMPTZ,
  high_f NUMERIC, low_f NUMERIC, precip_in NUMERIC,
  raw_text TEXT,
  PRIMARY KEY (station_id, report_date, received_at)
);

-- Trading
CREATE TABLE markets (
  ticker TEXT PRIMARY KEY, series_ticker TEXT,
  settlement_date DATE, settlement_station TEXT,
  threshold_value NUMERIC, direction TEXT,   -- 'above'/'below'/'between'
  close_time TIMESTAMPTZ, status TEXT
);

CREATE TABLE signals (
  signal_id UUID PRIMARY KEY, generated_at TIMESTAMPTZ,
  ticker TEXT, p_model NUMERIC, p_market NUMERIC,
  edge NUMERIC, confidence NUMERIC, side TEXT,
  size_dollars NUMERIC, diagnostics JSONB
);

CREATE TABLE orders (
  order_id TEXT PRIMARY KEY, signal_id UUID REFERENCES signals,
  ticker TEXT, side TEXT, limit_price NUMERIC, quantity INT,
  placed_at TIMESTAMPTZ, filled_qty INT, status TEXT,
  kalshi_raw_response JSONB
);

CREATE TABLE positions (
  ticker TEXT PRIMARY KEY, side TEXT,
  quantity INT, avg_price NUMERIC, opened_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ, realized_pnl NUMERIC
);

CREATE TABLE audit_log (
  seq BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ DEFAULT now(),
  actor TEXT, action TEXT, payload JSONB,
  prev_hash BYTEA, this_hash BYTEA          -- chain for tamper evidence
);
```

---

## 8. Security & Compliance

A short list, because Grant will read it twice.

- **Kalshi is CFTC-regulated.** Algorithmic trading is explicitly permitted; this is not grey-area. But the Developer Agreement governs the API — read it before go-live. No scraping, no unauthorized redistribution of market data, no wash trading.
- **RSA private key** lives in Azure Key Vault, fetched via managed identity. Never in git, never in env vars in plaintext, never in logs. Rotate every 90 days.
- **Demo and prod credentials are separate.** Different API keys; environment switched via config, never via code change.
- **Audit log is append-only** with hash-chained entries. Keep 7 years by default (matches CFTC record-keeping expectations for regulated venue participants, conservative).
- **No PII flows through this system.** The only "personal" data is Grant's account identifier and position history.
- **Tax reporting.** Kalshi issues 1099s. Set up a dedicated funding account; don't commingle with other trading activity.

None of this is legal or tax advice — confirm specifics with a professional before meaningful capital is deployed.

---

## 9. Phased Roadmap

Estimates assume 1–2 engineers full-time-equivalent. Adjust against actual team.

### Phase 0 — Foundations (Week 1)

- [ ] Kalshi account created; demo API key + prod API key generated
- [ ] Azure resource group, Key Vault, Postgres + Timescale, Container Apps env
- [ ] Repo scaffolded with pre-commit hooks, CI (lint + typecheck + tests)
- [ ] Structured logging + Prometheus scaffolding
- [ ] `stations.yaml` seeded for 6–8 Kalshi weather cities

**Exit:** `hello-kalshi` smoke test hits demo, auths with RSA key, reads a series, round-trips.

### Phase 1 — Data pipeline (Weeks 2–3)

- [ ] GEFS, NBM, METAR, CLI ingesters (§6.2), all idempotent
- [ ] Schema + Timescale policies (retention, compression)
- [ ] Dashboard: ingest lag, row counts, missing-data alerts
- [ ] 30 days of historical backfill successfully loaded

**Exit:** All four sources update on cadence for 7 consecutive days with zero data gaps.

### Phase 2 — Model & backtest (Weeks 3–5, overlaps Phase 1)

- [ ] Layer 1 / 2 / 3 probability components implemented
- [ ] Calibration trained on 3 years of archived data (NOAA reforecast archive)
- [ ] Confidence score implemented
- [ ] Historical backtest harness + P&L attribution + calibration plots
- [ ] **Go/no-go review**: does historical performance clear the go-live bar in §6.6?

**Exit:** Reviewed backtest report delivered to Grant. If bar not cleared, refine before Phase 3.

### Phase 3 — Paper trading (Weeks 5–8)

- [ ] Execution service against demo environment
- [ ] Signal engine live-running on demo
- [ ] Full monitoring stack up
- [ ] 3 weeks of continuous paper trading
- [ ] Reconciliation: every demo trade accounted for, P&L matches Kalshi-reported

**Exit:** Paper results within tolerance of backtest; zero unexplained breaks.

### Phase 4 — Production pilot (Weeks 8–10)

- [ ] Live API key provisioned, Key Vault updated
- [ ] Kill switch tested end-to-end in prod
- [ ] Start at $500 bankroll, $25 max per market — scale only after 10 successful trading days
- [ ] Daily review ritual for first 2 weeks

**Exit:** 10 trading days with zero operational incidents and P&L within modeled Sharpe envelope.

### Phase 5 — Scale & v1.5 improvements (ongoing)

- Additional cities as Kalshi expands offerings
- Early-exit logic (§3.5)
- ML-based calibration refinements
- Consider rate-limit tier upgrade (Advanced = 30 r/w per second) if volume justifies

---

## 10. Open Decisions for Grant

Before engineering starts, confirm:

1. **Bankroll & limits.** Starting capital? Tolerable max daily loss?
2. **Entity vs personal.** Trade under personal Kalshi account, or set up an LLC? Affects tax treatment and audit defensibility.
3. **Hosting.** Azure (leverages Libra stack) vs personal AWS/home server (air-gapped from Libra assets — cleaner separation).
4. **Rate-limit tier.** Basic is fine for v1. Advanced tier requires a form and justifies itself only with higher volume.
5. **On-call expectation.** Bot halts gracefully on failure — but who gets paged when it does?
6. **Code ownership & IP.** If this is built by Libra engineers on Libra time, the IP question matters. Probably personal project → personal repo → personal time.

---

## 11. Appendix — Reference Links

- **Kalshi API docs:** https://docs.kalshi.com
- **Kalshi weather market FAQ:** https://help.kalshi.com/markets/popular-markets/weather-markets
- **Kalshi Python SDK:** `kalshi-python` on PyPI
- **NWS API:** https://api.weather.gov (no key required)
- **NOAA NOMADS:** https://nomads.ncep.noaa.gov
- **NOAA Open Data on AWS:** `noaa-gefs-pds`, `noaa-nbm-grib2-pds` S3 buckets
- **Herbie (model data access library):** https://github.com/blaylockbk/Herbie
- **NWS CLI product reference:** https://www.weather.gov/documentation/services-web-api

---

*End of spec. Questions, push back on any of the gates/thresholds — the numbers in §3 and §6.4 are defaults, not doctrine.*
