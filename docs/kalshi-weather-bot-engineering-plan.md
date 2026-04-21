# Kalshi Weather Trading Bot — Engineering Plan

**Version:** 2.0
**Audience:** Coding team
**Last updated:** April 21, 2026

---

## 1. Executive Summary

A Python async service that trades Kalshi's daily-resolving weather contracts (temperature highs) using probabilistic fair-value estimates derived from free NWS/NOAA data. The system is **deliberately selective**: it only opens positions when modeled probability diverges materially from market price and model confidence is high. Target profile is high win-rate, low volume — not market-making.

The core trading loop is **fully deterministic**: signal → risk → execution runs without any LLM involvement. An optional LLM agent suite (8 roles backed by Gemini 2.5) is available for research, posture-setting, and audit but is disabled by default (`llm_trading_enabled = False`). A self-improvement pipeline critiques completed rooms and proposes updated agent packs; a historical intelligence pipeline mines 365 days of archived weather and market data to tune heuristics.

### Why this setup wins

1. **Kalshi weather contracts settle on a known, public source.** NWS Daily Climate Reports for specific ICAO stations (e.g., KNYC for NYC highs) are the authoritative settlement source. Our model consumes the same upstream feeds.
2. **Uncertainty collapses through the trading day.** By early afternoon on the settlement day, most of the day's high is already observed in METARs. This creates an intraday edge most retail participants don't exploit systematically.
3. **Free NWS data is sufficient.** NWS gridpoint forecasts provide unrounded sub-degree resolution; commercial APIs add no incremental edge.
4. **Selectivity > volume.** Every trade has fees and slippage. A confidence-gated bot with a 70%+ realized win-rate and 5 trades per day beats a market-maker running 55% on 100x the volume.

---

## 2. Scope

### Active markets

Markets are configured in `weather_market_map_path` (YAML). The weather directory maps each Kalshi series ticker to a specific ICAO station and NWS gridpoint. All series below are currently mapped and receiving live feeds.

| Series | Market | NWS Station | Notes |
|---|---|---|---|
| `KXHIGHAUS` | Austin daily high | EWX/KSAT | Active |
| `KXHIGHNY` | NYC daily high | OKX/KNYC | Active |
| `KXHIGHCHI` | Chicago daily high | LOT/KORD | Active |
| `KXHIGHMIA` | Miami daily high | MFL/KMIA | Active |
| `KXHIGHDEN` | Denver daily high | BOU/KDEN | Active |
| `KXHIGHLAX` | Los Angeles daily high | LOX/KLAX | Active |
| `KXHIGHPHIL` | Philadelphia daily high | PHI/KPHL | Active |
| `KXHIGHTBOS` | Boston daily high | BOX/KBOS | Active |
| `KXHIGHTDAL` | Dallas daily high | FWD/KDFW | Active |
| `KXHIGHTDC` | Washington DC daily high | LWX/KDCA | Active |
| `KXHIGHTHOU` | Houston daily high | HGX/KHOU | Active |
| `KXHIGHTATL` | Atlanta daily high | FFC/KATL | Active |
| `KXHIGHTMIN` | Minneapolis daily high | MPX/KMSP | Active |
| `KXHIGHTNOLA` | New Orleans daily high | LIX/KMSY | Active |
| `KXHIGHTLV` | Las Vegas daily high | VEF/KLAS | Active |
| `KXHIGHTOKC` | Oklahoma City daily high | OUN/KOKC | Active |
| `KXHIGHTPHX` | Phoenix daily high | PSR/KPHX | Active |
| `KXHIGHTSATX` | San Antonio daily high | EWX/KSAT | Active |
| `KXHIGHTSEA` | Seattle daily high | SEW/KSEA | Active |
| `KXHIGHTSFO` | San Francisco daily high | MTR/KSFO | Active |

### Out of scope

- Hurricanes, named storms, seasonal climate contracts.
- Low-temperature, precipitation, snowfall markets (not yet mapped).
- Non-weather markets.
- Cross-venue arbitrage.

---

## 3. Strategy Specification

### 3.1 Edge hypothesis

For each open weather market, compute `fair_yes_dollars` = modeled probability the YES side resolves true. Compare to the live market ask (for YES buys) or bid (for NO buys). Trade only when:

```
edge_bps = |fair_yes_dollars − market_touch| × 10,000
edge_bps ≥ 100 bps (1 cent)         ← risk_min_edge_bps
confidence ≥ 0.60                   ← derived from delta_f and observation availability
trade_regime == "standard"          ← near-threshold and longshot trades are blocked
```

### 3.2 Probability model

Two-layer model, Layer 2 preferred when NWS gridpoint data is available.

**Layer 1 — Logistic (fallback).**
```
spread_f = 6.0 if |delta_f| < 2°F else 4.5 if |delta_f| < 4°F else 3.5
P = 1 / (1 + exp(−delta_f / spread_f))
```
Used when only the NWS point forecast (rounded integer °F) is available.

**Layer 2 — Gaussian CDF (primary).**
```
sigma_f = seasonal_sigma(month)     ← 1.0°F (summer) to 4.5°F (winter)
P = Φ(delta_f / sigma_f)
```
Used when the NWS `forecastGridData` endpoint returns unrounded Celsius values. The gridpoint payload gives sub-degree resolution and is available for all configured stations.

**Fair-value adjustments.**
- `near_threshold` (|delta_f| ≤ 2°F): −5% penalty applied to fair_yes. These trades are blocked by the risk engine.
- `longshot_yes` (fair < 0.08) or `longshot_no` (fair > 0.92): −1.5% penalty. Also blocked by the risk engine.
- `standard`: no adjustment. Only standard-regime trades are allowed through.

**Confidence score.**
```
confidence = min(0.95, 0.45 + min(|delta_f| / 12, 0.35) + (0.15 if current_obs available else 0.0))
```

### 3.3 Trade regime classification

| Regime | Condition | Allowed |
|---|---|---|
| `standard` | |delta_f| > 2°F and 0.08 ≤ fair ≤ 0.92 | Yes |
| `near_threshold` | |delta_f| ≤ 2°F | No — risk engine blocks |
| `longshot_yes` | fair < 0.08 | No — risk engine blocks |
| `longshot_no` | fair > 0.92 | No — risk engine blocks |

### 3.4 Sizing

Position size is derived from live account balance at trade time:

```
max_order_notional  = total_capital × risk_order_pct    (5%)
max_position_notional = total_capital × risk_position_pct  (10%)
count_fp = floor((max_order_notional × confidence) / yes_price_dollars)
count_fp = min(count_fp, risk_max_order_count_fp)        (500 contracts hard cap)
```

If live capital cannot be determined (reconcile not yet run, API unreachable), trading is blocked entirely rather than falling back to an assumed amount.

A `size_factor` (0–1) is applied when entering market gates:
- Spread > 60% of mid → reject entirely.
- Volume < 5 contracts → reject.
- Adverse 60-minute momentum → reject.
- Otherwise size_factor scales linearly with volume.

### 3.5 Execution style

- Limit orders only. Never market orders.
- Shadow mode (`APP_SHADOW_MODE=true`) prevents live submission; all logic runs but orders are recorded as simulated. Default on.
- No averaging down. One active position per ticker.
- Stop-loss and profit-protection exits (§3.6). No other early exits.

### 3.6 Stop loss and position protection

Three exit triggers checked every 60 seconds:

| Trigger | Condition | Cooldown before re-entry |
|---|---|---|
| Loss threshold | Unrealized loss ≥ 25% of cost basis | 5-min momentum check required |
| Adverse momentum | Held ≥ 30 min AND slope ≤ −0.2 ¢/min | 5-min momentum check required |
| Profit protection | Unrealized gain ≥ 15% AND slope ≤ −0.2 ¢/min | 5-min momentum check required |

**Re-entry gate**: After any stop-loss exit, a `stop_loss_reentry` checkpoint is set. Auto-trigger will not open a new room for that ticker until a 5-minute price history shows sustained directional momentum (|slope| ≥ 0.2 ¢/min in the favorable direction). The checkpoint is overwritten on the next stop-loss exit, not deleted.

**Submit cooldown**: 300 seconds between stop-loss order submissions on the same ticker, to prevent thrashing.

---

## 4. System Architecture

```
  ┌─────────────────────────────────┐   ┌──────────────────────────────┐
  │   NWS / NOAA Feeds              │   │   Kalshi Market Data          │
  │   - Point forecast (NWS API)    │   │   - REST: markets, portfolio  │
  │   - ForecastGridData (unrounded)│   │   - WebSocket: orderbook_delta│
  │   - Latest observation (METAR)  │   │     user_orders, fills        │
  └────────────────┬────────────────┘   └──────────────┬───────────────┘
                   │                                    │
                   ▼                                    ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                    PostgreSQL + pgvector                            │
  │   rooms, messages, signals, orders, fills, positions, market_state │
  │   market_price_history, checkpoints, research_dossiers, memories   │
  │   agent_packs, heuristic_packs, historical_snapshots, strategies   │
  └────────────────────────────────┬───────────────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
  ┌───────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
  │  Auto-Trigger     │  │  Stop-Loss      │  │  Historical Pipeline  │
  │  (orderbook feed  │  │  Service        │  │  + Intelligence       │
  │   → room create)  │  │  (position mon) │  │  (daily heuristic     │
  └────────┬──────────┘  └─────────────────┘  │   calibration)       │
           │                                   └──────────────────────┘
           ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                    WorkflowSupervisor                               │
  │   Deterministic fast path (llm_trading_enabled=False):             │
  │   market gates → signal → size → risk engine → execute             │
  │                                                                     │
  │   Optional LLM path (llm_trading_enabled=True):                    │
  │   researcher → president → trader → risk officer → exec clerk       │
  │   → auditor → ops monitor → memory librarian                       │
  └────────────────────────────────────────────────────────────────────┘
              │
              ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                   Execution Service                                 │
  │   RSA-signed Kalshi client · deployment lock check · kill switch   │
  │   shadow mode · order state → fill tracking via WebSocket          │
  └────────────────────────────────────────────────────────────────────┘
              │
              ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                FastAPI Control Room (web/)                          │
  │   Rooms, Agent Packs, Self-Improve, Historical, Watchdog, Strategy │
  │   SSE transcript stream · Prometheus /metrics · /readyz            │
  └────────────────────────────────────────────────────────────────────┘
```

---

## 5. Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Language | Python 3.12 async | asyncio throughout |
| Web framework | FastAPI + Jinja2 | Control room UI + REST API |
| Database | PostgreSQL 16 + pgvector | Relational orders/positions; vector similarity for semantic memory |
| ORM | SQLAlchemy async + Alembic | 13 migrations applied |
| Kalshi client | Custom (`integrations/kalshi.py`) | RSA-PSS signing; REST + WebSocket with sequence tracking |
| Weather client | Custom (`integrations/weather.py`) | NWS API, no key required |
| LLM providers | Google Gemini 2.5 (primary) / OpenAI (fallback) / Ollama (local) | Routed per role via `agents/providers.py` |
| WebSocket | `websockets` library | `ping_interval=20`, `ping_timeout=60`; exponential reconnect backoff |
| HTTP | `httpx` async | All outbound calls |
| Metrics | `prometheus_client` | Scraped at `/metrics` |
| Logging | Structured JSON | All services |
| Deploy | Docker Compose blue/green | nginx routes to active color; watchdog handles failover |
| Secrets | Environment / mounted key files | RSA PEM paths configured per env |

---

## 6. Component Specifications

### 6.1 Kalshi API Client (`integrations/kalshi.py`)

- REST client with RSA-PSS request signing (`ECDSA` / `RSA` key auto-detected).
- WebSocket client subscribing to `orderbook_delta`, `market_lifecycle_v2`, `user_orders`, `fill` channels.
- Sequence-gap detection per subscription ID — raises `SequenceGapError` on non-consecutive seq numbers.
- Exponential backoff on reconnect: starts at 2s, doubles up to 60s max, resets after successful connect.
- Separate read and write API keys configurable; falls back to `live_kalshi_*` / `demo_kalshi_*` env-specific keys.
- Subaccount support (`kalshi_subaccount` setting, default 0).

**Accepted endpoints:**
- `GET /markets`, `GET /markets/{ticker}`, `GET /exchange/status`
- `POST /portfolio/orders`, `DELETE /portfolio/orders/{id}`
- `GET /portfolio/positions`, `GET /portfolio/balance`

**Pricing:** All prices use `yes_price_dollars` as `Decimal` strings (e.g., `"0.6500"`). Integer-cents fields are not used.

---

### 6.2 Weather Data Ingestion (`integrations/weather.py`)

Each room fetches a fresh weather bundle on demand via `WeatherSignalEngine.build_weather_bundle()`:

1. **NWS point forecast** — `GET /points/{lat},{lon}` then `GET /gridpoints/{office}/{x},{y}/forecast` — provides daily high in integer °F.
2. **NWS forecastGridData** — `GET /gridpoints/{office}/{x},{y}/forecast/hourly` + `GET /gridpoints/{office}/{x},{y}` — provides unrounded `maxTemperature` values in Celsius for Layer 2 Gaussian model.
3. **Current observation** — `GET /stations/{ICAO}/observations/latest` — current temperature for confidence boost and resolution-state detection.

Weather bundles are cached in-room (not persisted to DB for live trading; historical versions are archived to `historical_weather_archive_path` for the training pipeline).

---

### 6.3 Probability Model Service (`weather/scoring.py`, `services/signal.py`)

`WeatherSignalEngine` is the entry point. For each room:

1. Fetches the market snapshot (from live WebSocket state or reconcile).
2. Builds a weather bundle (forecast + observation + gridpoint).
3. Calls `score_weather_market()` to produce a `WeatherSignalSnapshot`.
4. Calls `_trade_recommendation()` to compute edge and recommend action.
5. Returns a `StrategySignal` with `fair_yes_dollars`, `edge_bps`, `confidence`, `trade_regime`.

`annotate_signal_quality()` is a second pass that tags signals with `model_quality_status`, `model_quality_reasons`, and `recommended_size_cap_fp` before the room executes.

Resolution-state detection: if `current_temp_f >= threshold`, the market is `LOCKED_YES` and the signal recommends `SELL_NO` or `STAND_DOWN`.

---

### 6.4 Signal & Risk Engine (`services/risk.py`)

`DeterministicRiskEngine.evaluate()` runs a fixed sequence of guards. All guards are enforced regardless of LLM output.

**Guards (in order):**

| # | Check | Block condition |
|---|---|---|
| 1 | Kill switch | `control.kill_switch_enabled` |
| 2 | Signal eligibility | No recommended action, side, or price |
| 3 | Resolution state | Market not UNRESOLVED |
| 4 | Edge threshold | `edge_bps < risk_min_edge_bps` (100 bps) |
| 5 | Market staleness | `market_observed_at` older than 30s |
| 6 | Research staleness | `research_observed_at` older than 900s |
| 7 | Order count cap | `count_fp > risk_max_order_count_fp` |
| 8 | Position count cap | `current_position_count_fp >= risk_max_position_count_fp_per_ticker` |
| 9 | Concurrent tickers | `open_ticker_count >= risk_max_concurrent_tickers` (10) |
| 10 | Trade regime | regime in `{near_threshold, longshot_yes, longshot_no}` |
| 11 | Order notional | `order_notional > total_capital × 5%` |
| 12 | Position notional | `(position + order) > total_capital × 10%` |
| 13 | Capital bucket | Risky bucket full, or safe reserve target not met |

**Risk limits (current production defaults):**

| Control | Value | How computed |
|---|---|---|
| Max order notional | 5% of live balance | Derived at trade time; blocks if balance unknown |
| Max position notional | 10% of live balance | Derived at trade time |
| Max concurrent tickers | 10 | Unique open-position tickers |
| Min edge | 100 bps (1 cent) | Hard cutoff |
| Allowed trade regimes | standard only | near_threshold and longshot blocked |
| Safe capital reserve | 0% | No portion reserved |
| Risky capital max | 0% | No risky-bucket trades |

**Kill switch.** `DeploymentControl.kill_switch_enabled` blocks all execution. Toggleable via the control room UI. Default: enabled.

---

### 6.5 Execution Service (`services/execution.py`)

- Requires `execution_enabled` (i.e., `APP_SHADOW_MODE=false`) AND active deployment color lock.
- Acquires the execution lock per deployment color before submitting.
- Shadow mode path: records a `TradeTicketRecord` with `shadow=True` and skips the Kalshi API call.
- Live path: `POST /portfolio/orders`, records `OrderRecord`, streams fills via WebSocket.
- Reconciliation: every 60 seconds, the daemon fetches live positions from Kalshi API and reconciles against local `PositionRecord` state.

---

### 6.6 Room Orchestration (`orchestration/supervisor.py`)

Each room progresses through stages: `triggered → researching → posture → proposing → risk → executing → auditing → memory → complete`.

**Deterministic fast path** (`llm_trading_enabled = False`, default):
1. Load market snapshot + weather bundle.
2. Run market gates (spread, edge, momentum, volume).
3. Build `TradeTicket` from signal recommendation.
4. Evaluate via `DeterministicRiskEngine`.
5. If APPROVED: acquire lock, submit via `ExecutionService`.
6. Write one SUPERVISOR message summarizing the decision, mark room COMPLETE.
7. Record `StrategyRecord` for training corpus.

**LLM agent path** (`llm_trading_enabled = True`):
Runs the full 8-role suite using Gemini 2.5 models. LLM output informs the SUPERVISOR message and memory notes but **never bypasses the deterministic risk engine** — the risk verdict is authoritative.

---

### 6.7 Auto-Trigger (`services/auto_trigger.py`)

Disabled by default (`trigger_enable_auto_rooms = False`). When enabled:

1. Receives market ticker updates from the WebSocket stream.
2. Skips tickers not in the weather directory.
3. Checks spread ≤ `trigger_max_spread_bps` (1200 bps) and both sides quoted.
4. Enforces per-ticker cooldown (300s normal, 30s after broken-book event).
5. Enforces `trigger_max_concurrent_rooms` (4) limit.
6. Checks `stop_loss_reentry` checkpoint: if set, requires 5-minute sustained directional momentum (|slope| ≥ 0.2 ¢/min) before opening.
7. Creates a room and runs it via supervisor.

---

### 6.8 Historical Data Pipeline (`services/historical_pipeline.py`, `services/historical_intelligence.py`)

**Bootstrap** (one-time): Ingests 365 days of Kalshi market snapshots (paginated from API) and archived weather bundles into `HistoricalMarketSnapshotRecord` and `HistoricalWeatherSnapshotRecord`.

**Daily incremental**: Fetches the last 7 days of market/weather data to stay current.

**Historical Intelligence** (daily run):
- Replays historical rooms using the current heuristic pack.
- Segments by city, trade regime, delta difficulty, and market outcome.
- If a candidate heuristic pack shows composite improvement > 2% with no critical regression > 1%, auto-promotes to `active_heuristic_pack_version`.

**Weather archive**: Live weather bundles for each room are snapshotted at checkpoint intervals (`historical_checkpoint_capture_lead_seconds = 300`, `grace_seconds = 900`) and stored to `historical_weather_archive_path` for later pipeline ingestion. Open-Meteo historical forecast API (`single-runs-api.open-meteo.com`) provides fallback when NWS data is unavailable at a historical timestamp.

---

### 6.9 Self-Improvement Pipeline (`services/self_improve.py`)

Three-stage workflow triggered manually via the control room or on a configured schedule:

1. **Critique**: Selects recent rooms from the training corpus, bundles their messages + signals + verdicts, sends to an LLM critic, and proposes changes to agent pack configuration (e.g., edge buffer adjustments, model assignments).
2. **Evaluate**: Replays the holdout set (20% of training rooms) under the candidate pack. Promotes if win-rate improvement ≥ 2% and no critical regression > 1%.
3. **Promote**: Assigns the candidate pack to the inactive deployment color. The next blue/green failover or manual switch activates it.

**Readiness gates for training corpus:**

| Gate | Threshold |
|---|---|
| Minimum complete rooms | 25 |
| Minimum settled rooms | 10 |
| Minimum market diversity | 4 unique series |
| Minimum trade-positive rooms | 8 |
| Research quality threshold | 0.70 |

---

### 6.10 Monitoring & Control Room (`web/`)

**FastAPI app** with Jinja2-rendered control room and REST API.

**Key REST endpoints:**
- `GET /api/rooms` — paginated room list with filters
- `POST /api/rooms` — manual room trigger
- `GET /api/agent-packs` — list agent pack versions
- `POST /api/self-improve/critique|evaluate|promote` — self-improvement controls
- `POST /api/historical/pipeline/bootstrap` — trigger historical ingest
- `GET /api/strategies` — city-strategy performance map
- `GET /readyz` — container health check
- `GET /metrics` — Prometheus scrape endpoint

**Control room tabs:** Summary · Rooms · Agent Packs · Self-Improve · Historical Intelligence · Watchdog · Strategies

**Prometheus metrics:**
- `kalshi_orders_placed_total{market, side}`
- `kalshi_fills_total{market, side}`
- `active_rooms`
- `room_runs_total{status}`
- `feed_freshness_seconds{feed}` (WebSocket staleness)

---

## 7. Data Model

Full schema managed by Alembic (13 migrations). Key tables:

```sql
-- Trading lifecycle
rooms           (id, market_ticker, stage, shadow_mode, active_color, agent_pack_version)
room_messages   (id, room_id, role, kind, stage, content, payload)
signals         (id, room_id, fair_yes_dollars, edge_bps, confidence, trade_regime)
trade_ticket_records  (id, room_id, client_order_id, side, yes_price_dollars, count_fp)
risk_verdict_records  (id, room_id, status, reasons, approved_count_fp)
order_records   (id, room_id, client_order_id, kalshi_order_id, status)
fill_records    (id, room_id, trade_id, side, count_fp, settlement_result)

-- Portfolio state
positions       (id, market_ticker, side, count_fp, avg_price, kalshi_env, subaccount)
market_state    (market_ticker, snapshot JSON, yes_bid, yes_ask, observed_at)
market_price_history  (market_ticker, mid_dollars, observed_at)

-- Research
research_dossier_records  (id, room_id, status, confidence, source_count)
memory_notes    (id, room_id, content, embedding vector(16))

-- Agent packs & self-improvement
agent_pack_records    (id, version, status, parent_version, configuration JSON)
critique_run_records  (id, agent_pack_id, rooms_critiqued, candidate_pack_id)
evaluation_run_records (id, candidate_pack_id, passed, improvement, regression)
promotion_event_records (id, from_version, to_version, color)

-- Historical
historical_market_snapshot_records  (market_ticker, snapshot_ts, snapshot JSON)
historical_weather_snapshot_records (market_ticker, checkpoint_label, weather JSON)
historical_checkpoint_archive_records (market_ticker, checkpoint_ts, weather JSON)
historical_intelligence_run_records (id, run_at, heuristic_pack_id, segment_results JSON)
heuristic_pack_records (id, version, status, configuration JSON)

-- Infrastructure
deployment_control  (id, active_color, kill_switch_enabled, execution_lock_holder, notes JSON)
checkpoints         (name, cursor, payload JSON, updated_at)
ops_events          (id, severity, summary, source, payload JSON)
```

---

## 8. Security & Compliance

- **Kalshi is CFTC-regulated.** Algorithmic trading is explicitly permitted. The Developer Agreement governs API use — no scraping, no unauthorized data redistribution, no wash trading.
- **RSA private keys** are mounted as read-only files; paths are configured per deployment color. Never committed to git. Separate keys for read and write operations.
- **Demo and production credentials are completely separate.** Switched via `KALSHI_ENV` env var; no code changes required. The system prevents write operations against the wrong environment.
- **Shadow mode** is the default. Live orders require explicitly setting `APP_SHADOW_MODE=false` in the environment.
- **Kill switch** defaults to enabled. No live orders are possible until it is explicitly cleared via the control room.
- **Deployment lock** ensures only the active color can submit orders, even if both containers are running.
- **Audit trail**: every order, fill, risk verdict, and ops event is persisted with timestamps and full payloads.

---

## 9. Infrastructure & Operations

### Blue/Green Deployment

```
postgres ←→ nginx (routes :80 to active color)
              ├── app_blue  :8000  (FastAPI + control room)
              ├── app_green :8000
              ├── daemon_blue      (reconcile, stop-loss, historical pipeline)
              └── daemon_green
```

Both colors run simultaneously. Only the active color holds the execution lock. Switching is atomic via `DeploymentControl.active_color` in the database.

### Watchdog

Runs in both daemon containers. Checks every `daemon_heartbeat_interval_seconds` (60s):
- **App health**: HTTP GET to `http://app_{color}:8000/readyz`
- **Daemon health**: heartbeat checkpoint freshness (`daemon_heartbeat:{color}`)

Actions on failure:
1. Inactive color unhealthy → restart it.
2. Active color unhealthy → restart it, record `pending_recovery`.
3. Active color still unhealthy on next check → failover to inactive color.
4. Both colors unhealthy → restart both.

### Daemon tasks (per color)

| Task | Interval | Description |
|---|---|---|
| Heartbeat | 60s | Write checkpoint to signal daemon is alive |
| Reconcile | 60s | Sync positions/orders with Kalshi API |
| Market history | 60s | Append mid-price to `market_price_history` (24h retention) |
| Stop-loss check | 60s | Evaluate open positions for exit triggers |
| Historical pipeline | Daily | Incremental 7-day market + weather ingest |
| Historical intelligence | Daily | Heuristic pack evaluation + auto-promote |

---

## 10. Configuration Reference

All settings in `config.py` (`Settings`), loaded from `.env`.

### Key environment variables

| Variable | Default | Notes |
|---|---|---|
| `KALSHI_ENV` | `demo` | `demo` or `production` |
| `APP_SHADOW_MODE` | `true` | `false` to enable live orders |
| `APP_ENABLE_KILL_SWITCH` | `true` | `false` to permit execution |
| `APP_COLOR` | `blue` | Blue/green identity |
| `LLM_TRADING_ENABLED` | `false` | Enable full 8-role LLM agent suite |
| `GEMINI_API_KEY` | — | Primary LLM provider |
| `WEATHER_MARKET_MAP_PATH` | `docs/examples/weather_markets.example.yaml` | Market → NWS mapping |
| `DEMO_KALSHI_API_KEY` | — | Demo API key ID |
| `DEMO_KALSHI_READ_PRIVATE_KEY_PATH` | — | Demo RSA key path |
| `LIVE_KALSHI_API_KEY` | — | Production API key ID |
| `LIVE_KALSHI_READ_PRIVATE_KEY_PATH` | — | Production RSA key path |

### Risk parameters

| Parameter | Default | Notes |
|---|---|---|
| `RISK_ORDER_PCT` | 0.05 | 5% of live balance per order |
| `RISK_POSITION_PCT` | 0.10 | 10% of live balance per position |
| `RISK_DAILY_LOSS_PCT` | 0.05 | 5% daily loss limit (self-improve gate) |
| `RISK_MIN_EDGE_BPS` | 100 | Minimum 1-cent edge required |
| `RISK_MAX_CONCURRENT_TICKERS` | 10 | Max open-position tickers |
| `RISK_MAX_ORDER_NOTIONAL_DOLLARS` | None | Optional hard-cap override |
| `RISK_MAX_POSITION_NOTIONAL_DOLLARS` | None | Optional hard-cap override |

### Stop-loss parameters

| Parameter | Default | Notes |
|---|---|---|
| `STOP_LOSS_THRESHOLD_PCT` | 0.25 | Exit at 25% unrealized loss |
| `STOP_LOSS_PROFIT_PROTECTION_THRESHOLD_PCT` | 0.15 | Exit profitable positions on adverse momentum |
| `STOP_LOSS_MOMENTUM_SLOPE_THRESHOLD_CENTS_PER_MIN` | −0.2 | Adverse momentum sensitivity |
| `STOP_LOSS_MOMENTUM_MIN_HOLD_MINUTES` | 30 | Minimum hold before momentum exit |
| `STOP_LOSS_REENTRY_COOLDOWN_SECONDS` | 14400 | 4h max; overridden by momentum re-entry gate |
| `STOP_LOSS_MOMENTUM_REENTRY_WINDOW_SECONDS` | 300 | 5-min window for momentum re-entry check |
| `STOP_LOSS_SUBMIT_COOLDOWN_SECONDS` | 300 | Min 5 min between stop-loss submissions |

---

## 11. Current Status and Roadmap

### What is built and running

- [x] Kalshi REST + WebSocket client (RSA-signed, sequence-tracked, exponential backoff reconnect)
- [x] NWS weather ingestion (point forecast + gridpoint + METAR observation)
- [x] Two-layer probability model (Logistic fallback + Gaussian CDF with seasonal sigma)
- [x] Trade regime classification and signal quality review
- [x] Deterministic risk engine (12 sequential guards)
- [x] Percentage-based sizing from live account balance
- [x] Shadow mode and kill switch with deployment color lock
- [x] Blue/green deployment with watchdog and automatic failover
- [x] Stop-loss service with loss threshold, momentum, and profit-protection exits
- [x] Momentum-based re-entry gate after stop-loss
- [x] Auto-trigger from live orderbook feed (disabled by default)
- [x] 20-city weather market map
- [x] 8-role LLM agent suite (Gemini 2.5-based, disabled by default)
- [x] Research dossier system and semantic memory (pgvector)
- [x] Self-improvement pipeline (critique → evaluate → promote)
- [x] Historical data pipeline (365-day bootstrap, daily incremental)
- [x] Historical intelligence (heuristic pack auto-calibration)
- [x] FastAPI control room (rooms, agent packs, strategies, watchdog)
- [x] Prometheus metrics + SSE room transcript stream
- [x] Alembic migrations (13 applied)
- [x] Reconciliation daemon (positions, orders, market prices)

### Known gaps and future work

| Item | Priority | Notes |
|---|---|---|
| GEFS / NBM ensemble integration | Medium | Current model uses only NWS point + gridpoint data; ensemble forecasts (30-member GEFS) would reduce σ uncertainty in Layer 2 |
| TimescaleDB hypertables | Low | Currently standard PostgreSQL; add if market_price_history table grows unwieldy |
| Kelly-based dynamic sizing | Low | Current % of capital is simpler; true Kelly requires calibrated P_model variance estimates |
| Low-temp and precipitation markets | Medium | Probability model supports it; needs station mappings |
| Live P&L streaming in control room | Low | Current UI shows snapshots; no SSE P&L stream |
| End-to-end backtest with fees/slippage | Medium | Historical intelligence replays but doesn't simulate fill costs |
| Per-city strategy differentiation | In progress | `CityStrategyAssignmentRecord` schema exists; logistic regression assignment pending |
| Production go-live | Pending | Run demo for ≥ 2 weeks with positive paper results before switching `KALSHI_ENV=production` |

### Go-live checklist

- [ ] Demo paper trading shows positive edge over ≥ 2 weeks / ≥ 20 resolved trades
- [ ] Zero unreconciled positions or order mismatches during paper period
- [ ] Kill switch and shadow mode tested end-to-end in demo
- [ ] Production RSA key loaded and verified
- [ ] `APP_SHADOW_MODE=false` and `APP_ENABLE_KILL_SWITCH=false` confirmed in production env
- [ ] `KALSHI_ENV=production` set
- [ ] Daily review ritual established for first 2 weeks

---

## 12. Appendix — Reference Links

- **Kalshi API docs:** https://docs.kalshi.com
- **NWS API:** https://api.weather.gov (no key required)
- **NWS forecastGridData:** `GET /gridpoints/{office}/{x},{y}` (unrounded temperature)
- **Open-Meteo historical archive:** https://single-runs-api.open-meteo.com/v1/forecast
- **pgvector:** https://github.com/pgvector/pgvector
- **websockets library:** https://websockets.readthedocs.io

---

*Questions or corrections — push back on any parameter. The numbers in §3 and §6.4 are calibrated defaults, not immutable doctrine.*
