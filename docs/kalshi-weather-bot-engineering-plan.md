# Kalshi Weather Trading Bot — Engineering Plan

**Version:** 2.1
**Audience:** Coding team
**Last updated:** April 23, 2026

---

## 1. Executive Summary

A Python async service that trades Kalshi's daily-resolving weather contracts (temperature highs) using probabilistic fair-value estimates derived from free NWS/NOAA data. The system is **deliberately selective**: it only opens positions when modeled probability diverges materially from market price and model confidence is high. Target profile is high win-rate, low volume — not market-making.

The core trading loop is **fully deterministic**: signal → risk → execution runs without any LLM involvement (`llm_trading_enabled = False`, permanently). The LLM agent suite (8 roles backed by Gemini 2.5) is scaffolding that is not used in production. A self-improvement pipeline critiques completed rooms and proposes updated agent packs; a historical intelligence pipeline mines 365 days of archived weather and market data to tune heuristics.

**v2.1 additions (Path A):** Three new edge sources added behind shadow-first feature flags: (1) Strategy C — resolution-lag cleanup for locked contracts, (2) Per-station σ calibration with three-layer architecture (static defaults → regression overlay → YAML sparse overrides), (3) Monotonicity Arb Scanner — taker-only arb on orderbook monotonicity violations across KXHIGH* threshold markets.

### Why this setup wins

1. **Kalshi weather contracts settle on a known, public source.** NWS Daily Climate Reports for specific ICAO stations (e.g., KNYC for NYC highs) are the authoritative settlement source. Our model consumes the same upstream feeds.
2. **Uncertainty collapses through the trading day.** By early afternoon on the settlement day, most of the day's high is already observed in METARs. This creates an intraday edge most retail participants don't exploit systematically.
3. **Free NWS data is sufficient.** NWS gridpoint forecasts provide unrounded sub-degree resolution; commercial APIs add no incremental edge.
4. **Selectivity > volume.** Every trade has fees and slippage. A confidence-gated bot with a 70%+ realized win-rate and 5 trades per day beats a market-maker running 55% on 100x the volume.

---

## 2. Scope

### Active markets

Markets are configured in `weather_market_map_path` (YAML). The weather directory maps each Kalshi series ticker to a specific ICAO station and NWS gridpoint. All series below are currently mapped and receiving live feeds.

**Adding/removing cities:** Edit the YAML and restart. At startup, `WeatherMarketDirectory.validate()` checks every `market_type=weather` entry for required fields (`station_id`, `location_name`, `latitude`, `longitude`, `threshold_f`). Missing fields produce a `WARNING` log — the service starts normally but the incomplete city will not be tradeable until the YAML is corrected.

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
500 bps ≤ edge_bps ≤ 5000 bps       ← risk_min_edge_bps (default 500, self-improve range [5,500]) / risk_max_credible_edge_bps
confidence ≥ 0.70                   ← risk_min_confidence; hard block in risk engine
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
sigma_f = seasonal_sigma(month)     ← Jan=3.0, Feb=3.5, Mar=4.0, Apr=6.0, May–Nov=2.8–4.0, Dec=3.0 (empirically derived)
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
max_order_notional    = total_capital × risk_order_pct     (5%)
max_position_notional = total_capital × risk_position_pct  (10%)

confidence_factor:
  confidence ≥ 0.90 → 100% of max_order_notional
  confidence ≥ 0.80 → 75%
  confidence ≥ 0.70 → 50%   (trades below 0.70 are blocked entirely)

count_fp = floor((max_order_notional × confidence_factor) / yes_price_dollars)
count_fp = min(count_fp, risk_max_order_count_fp)          (500 contracts hard cap)
```

If live capital cannot be determined (reconcile not yet run, API unreachable), trading is blocked entirely rather than falling back to an assumed amount.

**Daily loss sensitivity.** If today's realized P&L represents a loss ≥ `risk_daily_loss_sensitivity_pct` (10%) of total capital, the risk engine automatically tightens parameters for subsequent trades: `risk_min_edge_bps` doubles (×2.0) and `risk_max_order_notional_dollars` is halved (×0.5). This does not block trading — a genuinely high-confidence setup still executes, but at smaller size and with a harder edge requirement. The sensitivity state is logged in the SUPERVISOR room message.

A `size_factor` (0–1) is applied when entering market gates:
- Spread > 60% of mid → reject entirely.
- Volume < 50 contracts → reject (floor ensures meaningful book depth for IOC fills).
- Adverse 60-minute momentum → reject.
- Otherwise `size_factor = 1.0` — no partial scaling; the 50-contract gate is a hard binary, not a ramp.

### 3.5 Execution style

- Limit orders only. Never market orders.
- Shadow mode (`APP_SHADOW_MODE=true`) prevents live submission; all logic runs but orders are recorded as simulated. Default on.
- No averaging down. One active position per ticker.
- Stop-loss and profit-protection exits (§3.6). No other early exits.

### 3.6 Stop loss and position protection

Three exit triggers checked every 60 seconds. Each evaluation first verifies that the ticker's `market_state.observed_at` is within `risk_stale_market_seconds` (60s) — if the WebSocket feed is down, evaluation is skipped rather than acting on a stale price:

| Trigger | Condition | Cooldown before re-entry |
|---|---|---|
| Trailing stop | Price drops ≥ 10% from today's intraday peak (not cost basis) | 5-min momentum check required |
| Adverse momentum | Held ≥ 30 min AND slope ≤ −0.2 ¢/min | 5-min momentum check required |
| Profit protection | Unrealized gain ≥ 15% AND slope ≤ −0.2 ¢/min | 5-min momentum check required |

**Re-entry gate**: After any stop-loss exit, a `stop_loss_reentry` checkpoint is set. Re-entry rules (in priority order):

1. **Reverse-side evaluation (immediate)**: One room is triggered immediately after a stop-loss to evaluate the opposite side. If the price move that caused the stop-loss has created a favorable edge on the other side (e.g., a YES stop-loss followed by falling prices → NO edge), this room trades it. Marked `reverse_evaluated: true` after firing.
2. **4-hour timeout**: After `stop_loss_reentry_cooldown_seconds` (4h) from the stop-loss timestamp, re-entry is allowed unconditionally.
3. **Momentum confirmation**: Within the 4h window (after reverse evaluation), re-entry requires a 5-minute price history with a *directional* slope confirming recovery: slope_yes > +0.2 ¢/min for a stopped YES position, or slope_yes < −0.2 ¢/min for a stopped NO position.

Each daily weather contract has a unique ticker (e.g., `KXHIGHTBOS-26APR21-T55`), so yesterday's stop-loss checkpoint never affects today's contract — daily reset is implicit in the ticker structure. The checkpoint is overwritten on the next stop-loss exit on the same ticker.

**Submit cooldown**: 300 seconds between stop-loss order submissions on the same ticker, to prevent thrashing.

---

### 3.7 Strategy C — Resolution-Lag Cleanup (Addition 1, §4.1)

**Thesis.** By afternoon on settlement day, the ASOS observation directly confirms whether the high has already exceeded the threshold. Markets settle 1–3 hours after the meteorological observation with no mechanism to accelerate settlement — a structural lag. Locked contracts can often still be purchased at prices below 97–99¢.

**Lock-confirmation gate (three layers):**
- **Part A — Threshold confirmation**: `observed_max_f ≥ threshold_f` for `LOCKED_YES` (or the inverse for `LOCKED_NO`), confirmed by at least `strategy_c_required_consecutive_confirmations` (default 2) consecutive ASOS readings.
- **Part B — Forecast agreement**: If a gridpoint forecast is available, it must be within `strategy_c_max_forecast_residual_f` (8°F) of the observed value; large disagreements indicate the ASOS sensor may be anomalous.
- **Part C — Observation freshness**: Observation timestamp must be within `strategy_c_max_observation_age_minutes` (30 min) of sweep time.

**Edge model.** Locked-YES signals target `ask_yes ≤ 1 - strategy_c_locked_yes_discount_cents/100`. Fair value is 1.00 with a small discount for execution risk. Signals with `edge_cents < strategy_c_min_edge_cents` are suppressed.

**Risk gates.** `evaluate_cleanup_risk()` in `risk.py` checks: kill switch, `strategy_c_enabled` flag, per-trade notional cap (`strategy_c_max_order_notional_dollars`, default $50), per-position notional cap, and opposite-side guard (no adding to an existing opposing position).

**Shadow-only default.** `strategy_c_shadow_only=True`. All signals and outcomes are persisted to `strategy_c_rooms` for audit. The service sweeps on a configurable cadence and records `execution_outcome` as `shadow`, `risk_blocked`, or `suppressed`.

**Counterfactual fill-rate harness.** `compute_counterfactual_fill_rate()` estimates what fraction of shadow signals would have filled by joining `strategy_c_rooms` to `market_price_history` within a latency budget. Returns `None` (not 0%) when fewer than 10 shadow signals exist.

**CLI commands.** `shadow-c-sweep` (one-shot sweep), `strategy-c-status` (aggregate metrics).

---

### 3.8 Per-Station σ Calibration (Addition 2, §4.2)

**Thesis.** The Gaussian CDF model uses a single seasonal σ per month for all cities. Coastal cities, inland cities, and high-elevation cities have genuinely different day-to-day forecast error distributions.

**Three-layer σ architecture:**

1. **Static defaults** (`weather/scoring.py`) — seasonal σ table: Jan=3.0°F, Feb=3.5°F, Mar–Apr=4.0–6.0°F, etc. Fallback when no per-station data exists.
2. **Regression overlay** (`station_sigma_params` table) — empirically fitted per-station, per-month σ values derived from historical Open-Meteo forecast vs. ASOS observation errors. Applied when `station_sigma_override_enabled=True` (default `False`).
3. **YAML sparse overrides** — operator can pin individual station/month cells in the market map YAML. YAML overrides take precedence over regression values.

**Resolution.** `sigma_f = yaml_override ?? regression_sigma ?? static_default`.

**Calibration cadence.** The historical intelligence pipeline recomputes regression σ values from the rolling 90-day error window and updates `station_sigma_params` automatically.

---

### 3.9 Monotonicity Arb Scanner (Addition 3, §4.3)

**Thesis.** For a station/day, `P(high > T)` must be non-increasing in T. When the orderbook violates that — the higher-threshold YES bid exceeds the lower-threshold YES ask — a risk-free arb exists.

**Trade structure.** Buy YES on the lower-threshold ticker + buy NO on the higher-threshold ticker. Guaranteed gross payout ≥ $1.00 in all three scenarios:

| Scenario | YES T_i | NO T_j | Gross payout |
|---|---|---|---|
| high > T_j | Win | Lose | $1.00 |
| T_i < high ≤ T_j | Win | Win | $2.00 |
| high ≤ T_i | Lose | Win | $1.00 |

If total cost < $1.00 after fees, the trade is a true arb with positive expected value in every scenario.

**Detection (two-step).** Fast pass: `bid_yes(T_j) - ask_yes(T_i) > 4¢ + min_edge_cents` (safe-side 2¢/leg fee estimate). Re-validate with actual `no_ask_dollars` to compute real net edge. Only pairs passing re-validation are emitted as proposals.

**Fee calculation.** `ceil(0.07 · C · P · (1−P) · 100) / 100` per leg, evaluated at each leg's actual price.

**Risk gates.** Kill switch, `monotonicity_arb_enabled`, notional cap (`monotonicity_arb_max_notional_dollars`, default $25 per pair).

**Shadow-only default.** `monotonicity_arb_shadow_only=True`. Proposals written to `monotonicity_arb_proposals` for audit.

**CLI commands.** `monotonicity-scan` (one-shot scan), `monotonicity-status` (aggregate metrics).

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
  │   strategy_c_rooms, cli_reconciliation, cli_station_variance       │
  │   station_sigma_params, monotonicity_arb_proposals                 │
  └────────────────────────────────┬───────────────────────────────────┘
                                   │
     ┌──────────────────────────────┼──────────────────────────────────┐
     │                             │                                    │
     ▼                             ▼                                    ▼
  ┌───────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
  │  Auto-Trigger     │  │  Stop-Loss      │  │  Historical Pipeline  │
  │  (orderbook feed  │  │  Service        │  │  + Intelligence       │
  │   → room create)  │  │  (position mon) │  │  (daily heuristic +  │
  └────────┬──────────┘  └─────────────────┘  │   σ calibration)     │
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
  │   Strategy C summary strip · Monotonicity Arb status               │
  └────────────────────────────────────────────────────────────────────┘

  ─── v2.1 additions (periodic, run in daemon alongside above) ────────

  ┌──────────────────────────────┐  ┌──────────────────────────────────┐
  │  StrategyCleanupService      │  │  MonotonicityArbScannerService   │
  │  (Strategy C, §3.7)          │  │  (Addition 3, §3.9)              │
  │  - ASOS lock confirmation    │  │  - Groups KXHIGH* by station/day │
  │  - 3-gate filter (A/B/C)     │  │  - Two-step violation detection  │
  │  - evaluate_cleanup_risk()   │  │  - evaluate_arb_risk()           │
  │  - shadow → strategy_c_rooms │  │  - shadow → arb_proposals        │
  │  - Cadence: configurable     │  │  - Cadence: 60s                  │
  └──────────────────────────────┘  └──────────────────────────────────┘
```

---

## 5. Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Language | Python 3.12 async | asyncio throughout |
| Web framework | FastAPI + Jinja2 | Control room UI + REST API |
| Database | PostgreSQL 16 + pgvector | Relational orders/positions; vector similarity for semantic memory |
| ORM | SQLAlchemy async + Alembic | 19 migrations applied |
| Kalshi client | Custom (`integrations/kalshi.py`) | RSA-PSS signing; REST + WebSocket with sequence tracking |
| Weather client | Custom (`integrations/weather.py`) | NWS API, no key required |
| LLM providers | Four slots routed via `agents/providers.py`: `gemini` (primary, Gemini 2.5 per-role models), `hosted` (`LLM_HOSTED_*`, generic OpenAI-compatible endpoint), `codex` (`CODEX_*`, separate key/URL for a second OpenAI-compatible provider), `local` (`LLM_LOCAL_*`, Ollama or any local OpenAI-compatible server) | Role assignments configured in agent pack |
| WebSocket | `websockets` library | `ping_interval=20`, `ping_timeout=60`; exponential reconnect backoff |
| HTTP | `httpx` async | All outbound calls |
| Metrics | `prometheus_client` | Scraped at `/metrics` |
| Logging | Structured JSON | All services |
| Testing | `hypothesis` (property-based) | Required for all paired-leg arb strategies (§5.2); added in v2.1 dev dependencies |
| Deploy | Docker Compose blue/green | Caddy reverse proxy routes per-host to `web_demo`, `web_production`, `web_strategies`; watchdog handles failover |
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
| 4 | Min edge | `edge_bps < risk_min_edge_bps` (500 bps) |
| 5 | Max edge (credibility) | `edge_bps > risk_max_credible_edge_bps` (5000 bps) — model error signal |
| 6 | Confidence floor | `signal.confidence < risk_min_confidence` (0.70) |
| 7 | Contract price floor | contract price < `risk_min_contract_price_dollars` (0.25) — market pricing it as nearly impossible |
| 7b | Probability extremity | `fair_yes` between 25% and 75% — too close to coin-flip; forecast noise exceeds edge signal (`risk_min_probability_extremity_pct=25.0`, disabled by default, enable in production) |
| 8 | Market staleness | `market_observed_at` older than 60s |
| 9 | Research staleness | `research_observed_at` older than 900s |
| 10 | Order count cap | `count_fp > risk_max_order_count_fp` |
| 11 | Position count cap | `current_position_count_fp >= risk_max_position_count_fp_per_ticker` |
| 12 | Concurrent tickers | `open_ticker_count >= risk_max_concurrent_tickers` (10) |
| 13 | Trade regime | regime in `{near_threshold, longshot_yes, longshot_no}` |
| 14 | Order notional | `order_notional > total_capital × 5%` |
| 15 | Position notional | `(position + order) > total_capital × 10%` |
| 16 | Capital bucket | Risky bucket full, or safe reserve target not met — **intentionally disabled** (`risk_safe_capital_reserve_ratio=0.0`, `risk_risky_capital_max_ratio=0.0`); regime filtering (guard #13) already excludes all risky trades |

**Risk limits (current production defaults):**

| Control | Value | How computed |
|---|---|---|
| Max order notional | 5% of live balance | Derived at trade time; blocks if balance unknown |
| Max position notional | 10% of live balance | Derived at trade time |
| Max concurrent tickers | 10 | Unique open-position tickers |
| Min edge | 100 bps (1 cent) | Hard cutoff |
| Allowed trade regimes | standard only | near_threshold and longshot blocked |
| Safe capital reserve | 0% | Disabled — no reserve held back |
| Risky capital max | 0% | Disabled — risky-regime trades blocked upstream at guard #13 |

**Kill switch.** `DeploymentControl.kill_switch_enabled` blocks all execution. Toggleable via the control room UI. Default: enabled. The watchdog auto-enables it if the active color's `daemon_reconcile` checkpoint is stale by more than `daemon_reconcile_stale_kill_switch_seconds` (300s, ~5 missed reconcile cycles) and logs a `critical` ops event. Clearing the kill switch after an auto-trip still requires a successful post-clear reconcile (see below).

**Post-clear reconcile gate.** When the kill switch is cleared, `kill_switch_cleared_at` is stamped in `DeploymentControl.notes`. The supervisor refuses to execute until the `daemon_reconcile:{color}` checkpoint carries a `reconciled_at` timestamp newer than that clear time — typically one 60s reconcile cycle. This ensures positions and orders are synchronized before any live order is submitted after a kill switch event.

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

Enabled in all running environments (`TRIGGER_ENABLE_AUTO_ROOMS=true`). This is a fully autonomous system — no manual room triggers. All rooms originate from the auto-trigger on live orderbook events. When triggered:

1. Receives market ticker updates from the WebSocket stream.
2. Skips tickers not in the weather directory.
3. Checks spread ≤ `trigger_max_spread_bps` (1200 bps) and both sides quoted.
4. Enforces per-ticker cooldown (300s normal, 30s after broken-book event). **Bypass:** if the YES mid price has moved ≥ `trigger_price_move_bypass_bps` (1500 bps) since the last trigger, the cooldown is overridden — the move magnitude itself justifies a fresh evaluation.
5. Enforces `trigger_max_concurrent_rooms` (12) limit.
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
2. **Evaluate**: Replays the holdout set (20% of training rooms) under the candidate pack. Scores each room on a composite metric: `research_quality × 0.40 + directional_agreement × 0.25 + risk_compliance × 0.20 + memory_usefulness × 0.15`. Promotes if composite improvement ≥ `SELF_IMPROVE_MIN_IMPROVEMENT` (2%) AND no segment shows critical regression > `SELF_IMPROVE_MAX_CRITICAL_REGRESSION` (1%).
3. **Promote**: Writes a pending pack-promotion checkpoint for the inactive deployment color, restarts that color, and lets its daemon apply the candidate pack on startup before canary rooms begin.

Canary rollout is bounded:

- `self_improve_canary_min_rooms` and `self_improve_canary_min_seconds` define the minimum evidence window before promotion
- `self_improve_canary_max_seconds` defines the maximum time a staged canary may remain `running` before the status surface marks it `stalled`

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

**Control room layout:** Top summary strip plus lazy-loaded `Overview`, `Training & Historical`, `Research`, `Rooms`, and `Operations` tabs. The `web_strategies` site (`WEB_SITE_KIND=strategies`) renders a focused view of the `Research` tab — specifically the 180d assignment review queue and city strategy drilldown — without exposing the full trading control room.
The summary bootstrap avoids live all-city discovery and uses lightweight room snapshots so `/` and `/api/control-room/summary` stay responsive as configured cities and room history grow.
The `Research` tab includes an 180d-only assignment review queue for canonical city strategy assignments, including `drifted_assignment`, `evidence_weakened`, `ready_for_approval`, `aligned`, and `waiting_for_evidence` states plus the latest approval note in city detail.
The summary strip now includes a `strategy_c` sub-object with shadow graduation metrics (signal precision, fill rate, per-station variance, race rate) and live post-graduation metrics (fill rate, realized edge, win rate on fills).

**Prometheus metrics:**
- `kalshi_orders_placed_total{market, side}`
- `kalshi_fills_total{market, side}`
- `active_rooms`
- `room_runs_total{status}`
- `feed_freshness_seconds{feed}` (WebSocket staleness)

---

### 6.11 Strategy C — StrategyCleanupService (`services/strategy_cleanup_service.py`)

Periodic service injected into `DaemonService` and `AppContainer`. Key methods:

- `sweep()` — evaluates all configured `WeatherMarketDirectory` mappings in one pass. For each mapping: fetches NWS observation + Kalshi market data concurrently, computes lock state via `LockStateTracker.observe()`, evaluates all three gates (A/B/C) via `evaluate_cleanup_signal()`, runs `evaluate_cleanup_risk()` against the live `DeploymentControl`, persists a `StrategyCRoom` record with `execution_outcome`.
- `compute_counterfactual_fill_rate(lookback_days, latency_budget_seconds)` — joins shadow `strategy_c_rooms` to `market_price_history` to estimate fill rate. Returns `None` (not 0.0) when fewer than 10 shadow signals exist.
- `get_status()` — aggregate signal counts for the control room summary strip.

Observation timestamps are extracted from the NWS response (`properties.timestamp`) so Part C freshness check reflects actual data age, not fetch time.

---

### 6.12 Per-Station σ Calibration (`services/historical_pipeline.py`, `db/models.py`)

`station_sigma_params` table stores per-station, per-month σ values. The historical intelligence pipeline fits these from the rolling 90-day Open-Meteo forecast vs. ASOS observation error distribution. Resolution order in `score_weather_market()`:

```
sigma_f = yaml_override(station, month) ?? station_sigma_params(station, month) ?? SEASONAL_SIGMA[month]
```

Setting `station_sigma_override_enabled=True` activates the regression overlay. The YAML sparse override format allows per-cell pinning without touching code.

---

### 6.13 Monotonicity Arb Scanner (`services/monotonicity_scanner.py`, `services/monotonicity_scanner_service.py`)

Two-module split:

**`monotonicity_scanner.py`** (pure functions, no I/O):
- `group_markets_by_station_date(markets)` — groups KXHIGH* market dicts by `(station, event_date)`, sorted by threshold ascending.
- `detect_violations(group, station, event_date, min_net_edge_cents)` — two-step detection: fast bid_yes pass + ask_no re-validation. Computes actual per-leg Kalshi fee (`ceil(0.07·C·P·(1-P)·100)/100`).
- `evaluate_arb_risk(violation, control, settings)` — kill switch, enabled flag, notional cap.
- `scan_for_violations(markets, control, settings)` — full pipeline returning `list[ArbProposal]`.
- Ticker parsing: `_parse_station_date_threshold(ticker)` handles `KXHIGH{STATION}-{YYMONDD}-T{N}` format.

**`monotonicity_scanner_service.py`** (orchestration layer):
- `sweep()` — fetches open KXHIGH* markets from Kalshi (`list_markets(status=open, series_ticker=KXHIGH)`), runs `scan_for_violations()`, persists `MonotonicityArbProposal` records.
- `get_status()` — aggregate metrics for operator review.

Property-based tests using `hypothesis` assert positive net PnL across all 3 outcome scenarios for any pair with total cost < $1.00.

---

## 7. Data Model

Full schema managed by Alembic (19 migrations). Key tables:

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

-- Web auth (migration 0015)
web_users     (id, email, password_hash, password_salt, is_active, last_login_at)
web_sessions  (id, user_id → web_users, token_hash, expires_at)

-- Strategies / per-city assignment (migration 0013–0014)
strategies                 (id, series_ticker, strategy_name, config JSON)
strategy_results           (id, strategy_id, market_ticker, outcome JSON)
city_strategy_assignments  (id, series_ticker, strategy_name, approved_at, approval_note)
strategy_codex_runs        (id, series_ticker, run_at, payload JSON)

-- v2.1 additions (migrations 0017–0019)
station_sigma_params    (station, month, sigma_f, sample_count, last_fitted_at)
                        -- per-station monthly σ from 90-day Open-Meteo error regression
cli_reconciliation      (station, observation_date, asos_observed_max, cli_value, delta_degf)
                        -- ASOS vs. CLI published-high reconciliation for variance tracking
cli_station_variance    (station, sample_count, p95_abs_delta_degf, last_refreshed_at)
                        -- rolling P95 of |ASOS - CLI| per station; consumed by Part B gate
strategy_c_rooms        (room_id, ticker, station, decision_time, resolution_state, fair_value_dollars,
                         modeled_edge_cents, target_price_cents, contracts_requested, execution_outcome,
                         settlement_outcome, outcome_pnl_dollars)
                        -- per-decision audit trail for Strategy C; execution_outcome ∈ {shadow, risk_blocked, suppressed}
monotonicity_arb_proposals (proposal_id, station, event_date, ticker_low, ticker_high,
                            threshold_low_f, threshold_high_f, ask_yes_low_cents, ask_no_high_cents,
                            total_cost_cents, gross_edge_cents, fee_estimate_cents, net_edge_cents,
                            contracts_proposed, execution_outcome, suppression_reason, detected_at)
                        -- per-proposal audit trail for Addition 3; execution_outcome ∈ {shadow, risk_blocked}

-- Infrastructure
deployment_control  (id, active_color, kill_switch_enabled, execution_lock_holder, notes JSON)
                    -- notes keys: kill_switch_cleared_at (ISO timestamp set when kill switch is cleared;
                    --   supervisor refuses to execute until daemon_reconcile checkpoint is newer than this)
checkpoints         (name, cursor, payload JSON, updated_at)
                    -- well-known name patterns:
                    --   daemon_heartbeat:{kalshi_env}:{color}       — daemon liveness (60s cadence)
                    --   daemon_reconcile:{kalshi_env}:{color}       — last successful reconcile timestamp
                    --   daemon_settlement_followup:{kalshi_env}:{color} — settlement backfill cursor
                    --   reconcile:{kalshi_env}                      — reconcile run cursor
                    --   auto_trigger:{kalshi_env}:{ticker}          — per-ticker trigger cooldown state
                    --   kalshi_ws:{kalshi_env}:{color}:{sid}        — WebSocket sequence tracking
                    --   stop_loss_reentry:{ticker}                  — post-stop-loss re-entry gate
                    --   pending_pack_promotion:{kalshi_env}:{color} — staged agent-pack awaiting daemon pickup
ops_events          (id, severity, summary, source, payload JSON)
```

---

## 8. Security & Compliance

- **Kalshi is CFTC-regulated.** Algorithmic trading is explicitly permitted. The Developer Agreement governs API use — no scraping, no unauthorized data redistribution, no wash trading.
- **RSA private keys** are mounted as read-only files (`:ro`); paths are configured per environment. Never committed to git. Separate keys for read and write operations.
- **Demo and production credentials are completely isolated at the filesystem level.** Demo containers mount only `DEMO_KALSHI_KEY_PATH_HOST`; production containers mount only `LIVE_KALSHI_KEY_PATH_HOST`. The production key is never present in any demo container's filesystem, and vice versa. Environment selection is via `KALSHI_ENV`; no code changes required.
- **Shadow mode** is the default. Live orders require explicitly setting `APP_SHADOW_MODE=false` in the environment.
- **Kill switch** defaults to enabled. No live orders are possible until it is explicitly cleared via the control room.
- **Deployment lock** ensures only the active color can submit orders, even if both containers are running.
- **Audit trail**: every order, fill, risk verdict, and ops event is persisted with timestamps and full payloads.

---

## 9. Infrastructure & Operations

### Blue/Green Deployment

```
postgres_demo      ←─┐
postgres_production ←─┤
                      │
          ┌───────────┼──────────────────────────────────┐
          │           │  Trading containers               │
          │  app_demo_blue  / app_demo_green    :8000     │
          │  app_production_blue / _green       :8000     │
          │  daemon_demo_blue  / daemon_demo_green        │
          │  daemon_production_blue / _green              │
          └───────────┬──────────────────────────────────┘
                      │
          ┌───────────┴──────────────────────────────────┐
          │           Web (Caddy-facing)                  │
          │  web_demo        (WEB_SITE_KIND=demo)         │
          │  web_production  (WEB_SITE_KIND=production)   │
          │  web_strategies  (WEB_SITE_KIND=strategies)   │
          └───────────┬──────────────────────────────────┘
                      │
          ┌───────────┴──────────────────────────────────┐
          │  Caddy  :80/:443                              │
          │  demo.ai-al.site      → web_demo             │
          │  prod.ai-al.site      → web_production       │
          │  strategy.ai-al.site  → web_strategies       │
          └──────────────────────────────────────────────┘
```

`postgres_demo` (host port `POSTGRES_DEMO_PORT`, default 5432) and `postgres_production` (host port `POSTGRES_PRODUCTION_PORT`, default 5433) are completely isolated volumes — demo load or failures cannot affect the production DB.

`migrate_demo` and `migrate_production` run Alembic per environment before any app or daemon container starts (`depends_on: service_completed_successfully`).

Both colors run simultaneously. Only the active color holds the execution lock. Switching is atomic via `DeploymentControl.active_color` in the database.

The three web containers (`web_demo`, `web_production`, `web_strategies`) each run a FastAPI app scoped to their environment (`KALSHI_ENV`) and site kind (`WEB_SITE_KIND`). `WEB_APP_COLOR` controls which color's data the web containers read from (default `blue`); update it alongside `active_color` when promoting.

### Watchdog

Runs in all daemon containers (demo/production × blue/green = four containers). Checks every `daemon_heartbeat_interval_seconds` (60s):
- **App health**: HTTP GET to `http://app_{color}:8000/readyz`
- **Daemon health**: heartbeat checkpoint freshness (`daemon_heartbeat:{kalshi_env}:{color}`)

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
| Strategy C sweep | configurable | Lock-confirmation cleanup signals; default `strategy_c_cadence_idle_seconds=3600` |
| Monotonicity arb scan | 60s | Scan open KXHIGH* markets for monotonicity violations |
| Historical pipeline | Daily | Incremental 7-day market + weather ingest |
| Historical intelligence | Daily | Heuristic pack evaluation + auto-promote + σ recalibration |

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
| `LLM_TRADING_ENABLED` | `false` | Not used in production; deterministic fast path only |
| `GEMINI_API_KEY` | — | Primary LLM provider |
| `WEATHER_MARKET_MAP_PATH` | `docs/examples/weather_markets.example.yaml` | Market → NWS mapping |
| `WEATHER_USER_AGENT` | `kalshi-bot/0.1 (ops@example.com)` | NWS API requires a real app identifier + contact email; change before production |
| `DEMO_KALSHI_API_KEY` | — | Demo API key ID |
| `DEMO_KALSHI_READ_PRIVATE_KEY_PATH` | — | Demo RSA key path |
| `LIVE_KALSHI_API_KEY` | — | Production API key ID |
| `LIVE_KALSHI_READ_PRIVATE_KEY_PATH` | — | Production RSA key path |
| `POSTGRES_DEMO_PORT` | `5432` | Host port for `postgres_demo` container |
| `POSTGRES_PRODUCTION_PORT` | `5433` | Host port for `postgres_production` container |
| `WEB_APP_COLOR` | `blue` | Color badge shown in the dashboard header for web containers; update alongside `active_color` on promotion |
| `WEB_DEMO_HOST` | `demo.ai-al.site` | Caddy hostname for demo control room |
| `WEB_PRODUCTION_HOST` | `prod.ai-al.site` | Caddy hostname for production control room |
| `WEB_STRATEGIES_HOST` | `strategy.ai-al.site` | Caddy hostname for strategies dashboard |

### Risk parameters

| Parameter | Default | Notes |
|---|---|---|
| `RISK_ORDER_PCT` | 0.05 | 5% of live balance per order |
| `RISK_POSITION_PCT` | 0.10 | 10% of live balance per position |
| `RISK_DAILY_LOSS_PCT` | 0.20 | 20% daily loss limit (self-improve gate) |
| `RISK_MIN_EDGE_BPS` | 500 | Minimum edge required; self-improvement pipeline can tune in range [5, 500] bps |
| `RISK_MAX_CREDIBLE_EDGE_BPS` | 5000 | Maximum credible edge; larger values indicate model error |
| `RISK_MIN_CONFIDENCE` | 0.70 | Hard block below this confidence score; 0.70–0.80 gets 50% size, 0.80–0.90 gets 75%, ≥0.90 gets 100% |
| `RISK_MIN_CONTRACT_PRICE_DOLLARS` | 0.25 | Hard block if the traded side costs less than 25¢ |
| `RISK_MIN_PROBABILITY_EXTREMITY_PCT` | 0.0 (prod: 25.0) | Block trades where fair_yes is within this many pct-points of 50%; set 25.0 in production |
| `RISK_MAX_CONCURRENT_TICKERS` | 10 | Max open-position tickers |
| `RISK_MAX_ORDER_COUNT_FP` | 500 | Hard contract count cap per order (guard #10) |
| `RISK_MAX_POSITION_COUNT_FP_PER_TICKER` | 200 | Max contracts held per ticker (guard #11) |
| `RISK_MAX_ORDER_NOTIONAL_DOLLARS` | None | Optional hard-cap override |
| `RISK_MAX_POSITION_NOTIONAL_DOLLARS` | None | Optional hard-cap override |
| `RISK_DAILY_LOSS_LIMIT_DOLLARS` | None | Hard daily loss cap in dollars; disabled when unset |
| `RISK_DAILY_LOSS_SENSITIVITY_PCT` | 0.10 | If today's loss ≥ this fraction of total capital, tighten subsequent trades |
| `RISK_DAILY_LOSS_SENSITIVITY_EDGE_MULTIPLIER` | 2.0 | Multiply `risk_min_edge_bps` by this when sensitivity is active |
| `RISK_DAILY_LOSS_SENSITIVITY_SIZE_MULTIPLIER` | 0.50 | Multiply max order notional by this when sensitivity is active |

### Stop-loss parameters

| Parameter | Default | Notes |
|---|---|---|
| `STOP_LOSS_THRESHOLD_PCT` | 0.10 | Exit when price drops ≥ 10% from today's intraday peak |
| `STOP_LOSS_PROFIT_PROTECTION_THRESHOLD_PCT` | 0.15 | Exit profitable positions on adverse momentum |
| `STOP_LOSS_MOMENTUM_SLOPE_THRESHOLD_CENTS_PER_MIN` | −0.2 | Adverse momentum sensitivity |
| `STOP_LOSS_MOMENTUM_MIN_HOLD_MINUTES` | 30 | Minimum hold before momentum exit |
| `STOP_LOSS_REENTRY_COOLDOWN_SECONDS` | 14400 | 4h max; overridden by momentum re-entry gate |
| `STOP_LOSS_MOMENTUM_REENTRY_WINDOW_SECONDS` | 300 | 5-min window for momentum re-entry check |
| `STOP_LOSS_SUBMIT_COOLDOWN_SECONDS` | 300 | Min 5 min between stop-loss submissions |

### Self-improvement parameters

| Parameter | Default | Notes |
|---|---|---|
| `SELF_IMPROVE_CANARY_MAX_SECONDS` | 21600 | Max staged-canary lifetime before status becomes `stalled` |
| `SELF_IMPROVE_WINDOW_DAYS` | 14 | How many days of rooms to include in critique and evaluation runs |
| `SELF_IMPROVE_HOLDOUT_RATIO` | 0.2 | Fraction of training rooms reserved for evaluation holdout |
| `SELF_IMPROVE_MIN_IMPROVEMENT` | 0.02 | Minimum win-rate improvement required to promote a candidate pack |
| `SELF_IMPROVE_MAX_CRITICAL_REGRESSION` | 0.01 | Maximum allowed win-rate regression on any segment before promotion is blocked |
| `SELF_IMPROVE_CANARY_MIN_ROOMS` | 25 | Minimum canary rooms before live promotion |
| `SELF_IMPROVE_CANARY_MIN_SECONDS` | 7200 | Minimum canary duration (2h) before live promotion |

### Training corpus readiness gates

| Parameter | Default | Notes |
|---|---|---|
| `TRAINING_MIN_COMPLETE_ROOMS` | 25 | Minimum complete rooms before critique/eval will run |
| `TRAINING_MIN_SETTLED_ROOMS` | 10 | Minimum rooms with settled outcomes |
| `TRAINING_MIN_MARKET_DIVERSITY` | 4 | Minimum unique series in corpus |
| `TRAINING_MIN_TRADE_POSITIVE_ROOMS` | 8 | Minimum rooms that attempted a trade |
| `TRAINING_GOOD_RESEARCH_THRESHOLD` | 0.70 | Research quality floor; rooms below this are excluded from training corpus |
| `TRAINING_WINDOW_DAYS` | 30 | Lookback window for corpus assembly |

### Daemon parameters

| Parameter | Default | Notes |
|---|---|---|
| `DAEMON_RECONCILE_INTERVAL_SECONDS` | 60 | How often the daemon syncs positions/orders with Kalshi API |
| `DAEMON_RECONCILE_STALE_KILL_SWITCH_SECONDS` | 300 | Auto-enable kill switch if reconcile checkpoint is older than this (~5 missed cycles) |
| `DAEMON_HEARTBEAT_INTERVAL_SECONDS` | 60 | How often the daemon writes a liveness checkpoint |
| `DAEMON_START_WITH_RECONCILE` | `true` | Run a reconcile immediately on daemon startup before entering the main loop |

### Strategy C parameters (Addition 1)

| Parameter | Default | Notes |
|---|---|---|
| `STRATEGY_C_ENABLED` | `false` | Master switch; must also clear kill switch before live orders |
| `STRATEGY_C_SHADOW_ONLY` | `true` | Shadow-only mode; flip to `false` after operator sign-off |
| `STRATEGY_C_CADENCE_IDLE_SECONDS` | `3600` | Sweep interval far from threshold |
| `STRATEGY_C_CADENCE_APPROACH_SECONDS` | `900` | Sweep interval when within `approach_margin_f` (5°F) |
| `STRATEGY_C_CADENCE_NEAR_THRESHOLD_SECONDS` | `150` | Sweep interval when within `near_threshold_margin_f` (2°F) |
| `STRATEGY_C_REQUIRED_CONSECUTIVE_CONFIRMATIONS` | `2` | Min consecutive ASOS readings above threshold |
| `STRATEGY_C_MAX_OBSERVATION_AGE_MINUTES` | `30` | Part C freshness gate |
| `STRATEGY_C_MAX_FORECAST_RESIDUAL_F` | `8.0` | Part B divergence gate |
| `STRATEGY_C_MAX_CLI_VARIANCE_DEGF` | `1.5` | Part B CLI variance gate |
| `STRATEGY_C_MIN_EDGE_CENTS` | `2` | Minimum edge to emit a signal |
| `STRATEGY_C_MAX_ORDER_NOTIONAL_DOLLARS` | `50.0` | Per-trade cap |
| `STRATEGY_C_MAX_POSITION_NOTIONAL_DOLLARS` | `50.0` | Per-position cap |

### Per-station σ calibration parameters (Addition 2)

| Parameter | Default | Notes |
|---|---|---|
| `STATION_SIGMA_OVERRIDE_ENABLED` | `false` | Activates regression overlay; enable after first calibration run |

### Monotonicity Arb Scanner parameters (Addition 3)

| Parameter | Default | Notes |
|---|---|---|
| `MONOTONICITY_ARB_ENABLED` | `false` | Master switch |
| `MONOTONICITY_ARB_SHADOW_ONLY` | `true` | Shadow-only mode; flip after operator sign-off |
| `MONOTONICITY_ARB_MIN_NET_EDGE_CENTS` | `2` | Minimum net edge (after fees) to emit a proposal |
| `MONOTONICITY_ARB_MAX_NOTIONAL_DOLLARS` | `25.0` | Max notional per pair |
| `MONOTONICITY_ARB_MAX_PROPOSALS_PER_MINUTE` | `5` | Rate limit on proposal emission |
| `MONOTONICITY_ARB_CADENCE_SECONDS` | `60` | Scan interval |

### Auto-trigger parameters

| Parameter | Default | Notes |
|---|---|---|
| `TRIGGER_ENABLE_AUTO_ROOMS` | `true` | Master switch for autonomous room creation from orderbook events |
| `TRIGGER_COOLDOWN_SECONDS` | 300 | Per-ticker cooldown between trigger evaluations (30s after broken-book events) |
| `TRIGGER_MAX_SPREAD_BPS` | 1200 | Reject trigger if bid/ask spread exceeds this |
| `TRIGGER_MAX_CONCURRENT_ROOMS` | 12 | Hard cap on simultaneously running rooms |
| `TRIGGER_PRICE_MOVE_BYPASS_BPS` | 1500 | If YES mid moves ≥ this many bps since last trigger, bypass the cooldown |

---

## 11. Current Status and Roadmap

### What is built and running

- [x] Kalshi REST + WebSocket client (RSA-signed, sequence-tracked, exponential backoff reconnect)
- [x] NWS weather ingestion (point forecast + gridpoint + METAR observation)
- [x] Two-layer probability model (Logistic fallback + Gaussian CDF with seasonal sigma)
- [x] Trade regime classification and signal quality review
- [x] Deterministic risk engine (16 sequential guards)
- [x] Percentage-based sizing from live account balance
- [x] Shadow mode and kill switch with deployment color lock
- [x] Blue/green deployment with watchdog and automatic failover
- [x] Stop-loss service with loss threshold, momentum, and profit-protection exits
- [x] Momentum-based re-entry gate after stop-loss
- [x] Auto-trigger from live orderbook feed (enabled by default via `TRIGGER_ENABLE_AUTO_ROOMS=true`)
- [x] 20-city weather market map
- [x] 8-role LLM agent suite (Gemini 2.5-based, disabled by default)
- [x] Research dossier system and semantic memory (pgvector)
- [x] Self-improvement pipeline (critique → evaluate → promote)
- [x] Historical data pipeline (365-day bootstrap, daily incremental)
- [x] Historical intelligence (heuristic pack auto-calibration)
- [x] FastAPI control room (rooms, agent packs, strategies, watchdog)
- [x] Prometheus metrics + SSE room transcript stream
- [x] Alembic migrations (19 applied)
- [x] Reconciliation daemon (positions, orders, market prices)
- [x] Split Postgres per environment (`postgres_demo` / `postgres_production` with isolated volumes)
- [x] Caddy reverse proxy replacing nginx (per-host routing to `web_demo`, `web_production`, `web_strategies`)
- [x] Three-way web container split: demo control room, production control room, strategies dashboard
- [x] Per-environment migrate services with `service_completed_successfully` gating on all app/daemon containers
- [x] `.dockerignore` and two-stage Dockerfile pip layer caching (source-only rebuilds ~2s vs 25s)
- [x] **v2.1** Strategy C — Resolution-Lag Cleanup (shadow mode, `strategy_c_enabled=False` default)
- [x] **v2.1** Per-station σ calibration — three-layer architecture (static → regression → YAML override)
- [x] **v2.1** Monotonicity Arb Scanner — two-step detection, property-based tests, shadow mode (`monotonicity_arb_enabled=False` default)
- [x] **v2.1** `hypothesis` property-based testing for all paired-leg strategies
- [x] **v2.1** CLI commands: `shadow-c-sweep`, `strategy-c-status`, `monotonicity-scan`, `monotonicity-status`
- [x] **v2.1** Strategy C control room summary strip (shadow graduation metrics + live post-graduation metrics)

### Known gaps and future work

| Item | Priority | Notes |
|---|---|---|
| GEFS / NBM ensemble integration | Medium | Current model uses only NWS point + gridpoint data; ensemble forecasts (30-member GEFS) would reduce σ uncertainty in Layer 2 |
| TimescaleDB hypertables | Low | Currently standard PostgreSQL; add if market_price_history table grows unwieldy |
| Kelly-based dynamic sizing | Low | Current % of capital is simpler; true Kelly requires calibrated P_model variance estimates |
| Low-temp and precipitation markets | Medium | Probability model supports it; needs station mappings |
| Live P&L streaming in control room | Low | 30d win-rate snapshot exists (`get_fill_win_rate_30d()` in summary strip); missing is a real-time SSE feed showing running unrealized P&L across open positions |
| End-to-end backtest with fees/slippage | Medium | Historical intelligence replays but doesn't simulate fill costs; `is_taker` flag and actual fill prices are already stored per fill record, giving the data foundation — missing is a fee-rate constant and a simulation pass that subtracts fees from realized P&L before training corpus scoring |
| Per-city strategy differentiation | In progress | Research assignment review queue built; canonical assignments (ready_for_approval, drifted_assignment, evidence_weakened, aligned, waiting_for_evidence) visible in dashboard Research tab; auto-application to live routing deferred to post-launch self-improvement pipeline |
| Strategy C live graduation | Pending | Shadow run must complete 30 days with metrics meeting §4.1.6; requires operator sign-off before `strategy_c_shadow_only=false` |
| Monotonicity arb live graduation | Pending | Shadow run must complete 30 days; backtest against 90 days of `market_price_history` must show zero losing arbs |
| σ calibration activation | Pending | Enable `station_sigma_override_enabled=true` after first regression fit and CRPS improvement confirmed in production metrics |
| Path B additions | Future | NGR A/B testing, Bayesian Kelly sizing — separate spec to be written after Path A metrics are observed |
| Production go-live | Pending | Run demo for ≥ 2 weeks with positive paper results before switching `KALSHI_ENV=production` |

### Go-live checklist

- [ ] ≥ 2 weeks of continuous autonomous demo trading (no manual interventions)
- [ ] ≥ 20 resolved trades in demo with ≥ 70% win rate and positive P&L overall (win = realized P&L positive: sell price beat entry for stopped-out positions; contract settled on our side for positions held to expiry — tracked by `get_fill_win_rate_30d()`)
- [ ] σ calibration gate: run `python scripts/calibrate_sigma.py` from the **host virtualenv** (not inside a container) — median bps error < 2700 bps across all city/month cells with ≥ 5 samples (the 2500 bps threshold accounts for structural sensitivity of near-50% contracts; σ values in `weather/scoring.py` were derived from 2911 market-days of empirical data and should be re-run after each full season of new data)
- [ ] Zero unreconciled positions or order mismatches during demo period
- [ ] No stop-loss exits with `possible_model_error=true` (trailing_loss_ratio > 30% within first 2 hours of hold — ops events are tagged automatically; review the ops log before go-live)
- [ ] Kill switch and shadow mode tested end-to-end in demo
- [ ] Production RSA key loaded and verified (`chmod 600 <key_path>` recommended; startup raises `PermissionError` if the key has any write bits set — group/other-writable — but allows 0o644 read-only mounts as used by Docker secrets)
- [ ] `WEATHER_USER_AGENT` set to a real app identifier and contact email (e.g., `kalshi-bot/1.0 (your@email.com)`) — NWS API requires a valid User-Agent or will rate-limit; default placeholder `ops@example.com` should not reach production
- [ ] `RISK_MIN_PROBABILITY_EXTREMITY_PCT=25.0` set in production env (guard #7b — blocks near-50% coin-flip trades; intentionally 0.0 in demo)
- [ ] `APP_SHADOW_MODE=false` and `APP_ENABLE_KILL_SWITCH=false` confirmed in production env
- [ ] `KALSHI_ENV=production` set
- [ ] Auth cookie domain verified: `WEB_AUTH_COOKIE_DOMAIN` set to the correct shared domain (e.g., `.ai-al.site`) so sessions are valid across `web_demo`, `web_production`, and `web_strategies` — confirm in browser devtools that the `Set-Cookie` domain matches before exposing `web_production` externally
- [ ] Daily review ritual established for first 2 weeks post-launch

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
