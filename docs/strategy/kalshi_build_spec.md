# Kalshi Bot — Augmentation Spec

**Base repository:** `https://github.com/mnem0nic7/kalshi_bot`
**Audience:** Coding assistant (Claude Code or equivalent) implementing targeted additions against the existing codebase.
**Operator:** Grant Funk, CISO, Libra Solutions Group.

This is an **augmentation spec, not a greenfield build**. The repository is a mature multi-agent Kalshi weather trading platform with 60+ Python modules, 16 Alembic migrations, blue/green deployment, FastAPI control room, self-improvement loops, and production Docker/systemd tooling. Do not propose scaffolding, authentication, or core Kalshi integration work — all of that exists and is hardened.

Before writing any code, read — in this order:
1. `README.md` (full) — project overview and CLI catalog
2. `CLAUDE.md` — coding-assistant guidance already in repo; defer to it on conventions
3. `docs/kalshi-weather-bot-engineering-plan.md` — current production strategy (v2.0)
4. `docs/strategy/weather-microstructure-roadmap.md` — Strategy A / B / C roadmap
5. `docs/strategy/weather-temp-taker.md` — current production strategy specification
6. `docs/architecture.md`, `docs/database.md`, `docs/operations.md`, `docs/security.md`
7. Reference docs in the arbitrage playbook package (outside repo):
   - `kalshi_hightemp_playbook.md`
   - `kalshi_technical_deepdive.md`
   - `kalshi_incident_playbook.md`
   - `kalshi_pnl_model.md`
   - `kalshi_backtest.py`

The playbook docs describe a fuller arbitrage surface than this codebase currently pursues. This spec reconciles the two.

---

## 1. Existing System Assessment

### 1.1 What the repo already implements

| Playbook concept | Repo status | Where it lives |
|---|---|---|
| Kalshi REST client with RSA-PSS auth | ✅ Production | `src/kalshi_bot/integrations/kalshi.py` |
| Kalshi WebSocket ingestion + checkpointing | ✅ Production | `src/kalshi_bot/services/streaming.py` |
| NWS/NOAA forecast + observation ingestion | ✅ Production | `src/kalshi_bot/integrations/weather.py`, `forecast_archive.py` |
| Fair-value model (Layer 2 Gaussian CDF) | ✅ Production | `src/kalshi_bot/weather/scoring.py` |
| Confidence scoring + regime classification | ✅ Production | `weather/scoring.py`, `services/signal.py` |
| Deterministic risk engine | ✅ Production | `src/kalshi_bot/services/risk.py` |
| Execution service + kill switch | ✅ Production | `src/kalshi_bot/services/execution.py` |
| Blue/green deployment with DB-lock | ✅ Production | DB migrations 0001–0016 + `services/container.py` |
| Shadow mode (default on) | ✅ Production | `APP_SHADOW_MODE` env + `services/shadow.py` |
| Control room (FastAPI + Jinja2) | ✅ Production | `src/kalshi_bot/web/` |
| Historical replay + backtest infrastructure | ✅ Production | `services/historical_*`, replay corpus |
| Training corpus + self-improvement loop | ✅ Production | `services/training*`, `services/self_improve.py` |
| Watchdog + systemd recovery | ✅ Production | `infra/systemd/`, `services/watchdog.py` |
| CI smoke tests (demo + live + shadow) | ✅ Production | `.github/workflows/*-smoke.yml` |
| Weather market mapping | ✅ 20 cities | `docs/examples/weather_markets.example.yaml` |
| Resolution-state tracking (observed-max-vs-threshold) | ✅ Detects | `core/enums.py::WeatherResolutionState` + scoring |

**Conclusion:** The "build the bot" part of the project is done. What remains is strategy augmentation.

### 1.2 Current strategic posture

Per `docs/kalshi-weather-bot-engineering-plan.md` v2.0:

- **Selective taker**, not market-making
- Target **70%+ realized win rate, ~5 trades per day**
- Layer 2 Gaussian CDF fair-value from NWS gridpoint forecasts (unrounded °F via `forecastGridData` endpoint)
- Seasonal σ parameters, empirically tuned
- Flat risk caps: `5%` order, `10%` position, `5%` daily loss
- Confidence gate ≥ 0.70; edge band 500–5000 bps
- Deterministic core is authoritative; LLM agents (Gemini) are **scaffolding only** (`llm_trading_enabled = False` permanently)
- Blocks near-threshold (|delta_f| ≤ 2°F) and longshot (fair < 0.08 or > 0.92) trades
- Weather markets only; resolved-contract cleanup **not currently traded** (planned Strategy C)

### 1.3 Roadmap already declared in repo

- **Strategy A** — current production (directional-unresolved only)
- **Strategy B** — deeper microstructure filtering (quote quality, payout left, spread regime, late-day)
- **Strategy C** — resolution-lag cleanup trading (explicitly not active; "shadow-only first, separately documented, separately evaluated")

---

## 2. Playbook ↔ Codebase Reconciliation

The arbitrage playbook describes **four edges**: (1) ensemble-vs-market, (2) intraday observation-driven, (3) market-making, (4) structural bin arb. The existing repo has made conscious strategic choices about each.

| Playbook edge | Repo approach | Alignment |
|---|---|---|
| **Edge 1: Forecast vs market mispricing** | Implemented via Layer 2 Gaussian CDF on NWS gridpoint data (not 31-member ensemble) | Philosophically aligned; different mathematical approach |
| **Edge 2a: Observation hard-floor kills** | `WeatherResolutionState` detects locked states but system **does not trade** resolved contracts. Planned as "Strategy C." | Aligned in intent; not yet implemented |
| **Edge 2b: Diurnal peak confirmation** | Not implemented. Would be enhancement to Strategy C. | Would align |
| **Edge 3: Market-making** | **Explicitly excluded** in `weather-temp-taker.md`: "taker orders only, no market making" | Conflicts with playbook; respect repo choice |
| **Edge 4: Structural bin arbitrage** | Not implemented; 20 cities mapped as threshold markets (> or ≥) which are single-bin binaries, so classical 6-bin structural arb doesn't apply directly. | Needs adaptation to repo's market structure |
| **NGR bias correction (ensemble)** | Not used; seasonal σ tuning instead. | Different approach to same problem |
| **Bayesian Kelly sizing** | Not used; flat % caps. | Different approach; repo is deliberately conservative |
| **Joint event Kelly** | Not applicable (flat sizing) | N/A |
| **Backtest harness** | Historical replay system + `historical_pipeline` | Repo's is more sophisticated |
| **Monitoring, alerting, kill switches** | Extensive — watchdog, blue/green, smoke CI | Repo's is production-grade |

### 2.1 A notable nuance on market structure

My playbook assumed **6-bin event markets** (one event → 6 mutually-exclusive bins that sum to 100¢). The `KXHIGH*` tickers used by this system are actually **threshold-style markets**: each ticker is one binary contract `"high > T"` at a specific threshold. A single day in NYC has many such contracts at different T values.

This changes Edge 4 (structural arb). You cannot do the clean "6 bins sum to 100¢" scan. Instead, you can exploit **monotonicity across thresholds**: `P(high > 85)` must be ≤ `P(high > 80)` always. When the orderbook violates that (higher threshold priced above lower threshold), there is a directly tradeable arb: **buy YES on the lower-threshold ticker and buy NO on the higher-threshold ticker** for the same city/day. See §4.3.3.1 for the payoff proof.

That's a real edge and still aligned with "selective taker" philosophy. See §4.3.

---

## 3. Strategic Alignment Decision

The arbitrage playbook advocates a full-spectrum approach (all four edges, ensemble modeling, Kelly sizing, market-making). The existing repo has chosen a narrower, more conservative posture for a single-operator setup. **Both are defensible.**

Three paths forward. The operator (Grant) chooses one before the coding assistant starts work:

### Path A — Conservative: Activate what's aligned, skip what isn't

- Activate **Strategy C** (resolution-lag cleanup) — playbook's Edge 2a; already on repo roadmap
- Add **monotonicity arb scanner** — playbook's Edge 4, adapted to threshold markets
- Add **per-station σ calibration** — incremental improvement to existing fair-value model
- Respect the "no market making," "no Kelly," and "no ensemble model" choices
- **Recommended default.** Lowest risk, respects existing architecture, incremental.

### Path B — Aggressive: Push toward full-spectrum arb playbook

- All of Path A, plus:
- Add **NGR ensemble model** as A/B test alongside Layer 2 Gaussian
- Add **Bayesian/joint Kelly sizing** as opt-in mode (flag-gated, tested in shadow first)
- Possibly add **market-making mode** on a separate strategy code path
- Higher risk, more divergent from existing philosophy. Only pursue if Path A saturates.

### Path C — Don't build more; optimize what's there

- Focus on **calibration and regime tuning** of existing Strategy A
- Activate only Strategy B (microstructure filtering) as already documented
- Defer Strategy C and playbook additions
- Appropriate if operator's time budget is tight or Strategy A is already profitable.

**Default recommendation: Path A.** The remainder of this spec is written for Path A. If operator selects B or C, the coding assistant should reference specific sections only.

---

## 4. Path A — Recommended Additions

Three additions, in priority order. Each section specifies scope, files to touch, acceptance criteria, tests, and a stop condition.

### 4.1 Addition 1: Activate Strategy C (Resolution-Lag Cleanup)

**Playbook reference:** Edge 2a — "Hard-floor kills" (§2 of `kalshi_hightemp_playbook.md`).
**Repo roadmap reference:** Strategy C in `docs/strategy/weather-microstructure-roadmap.md`.

**Thesis:** When intraday ASOS observations have already met or exceeded a threshold before settlement (e.g., at 2 PM local time the observed max is 87°F and the market still has `"high > 85"` trading at 88¢), the YES side is deterministically worth $1.00 subject only to CLI/METAR reconciliation. Small residual mispricings (88¢ → 99¢) are clean edge.

**Why it fits the repo's philosophy:**
- High win-rate by construction (near-deterministic outcomes)
- Low volume (only fires when resolution state locks)
- Fits "selective taker" model
- Already on the declared roadmap

#### 4.1.1 Scope

Implement Strategy C as a **separate strategy code path**, per the roadmap's explicit requirement:
> "a future cleanup strategy should not be mixed into the base directional weather logic"

Strategy C is shadow-only in its initial deployment.

#### 4.1.2 Partition invariant (Strategy A × C)

At most one strategy may hold an open position per `(ticker, event_date)` tuple at any moment. Strategy A and Strategy C operate as **mutually exclusive strategies per market**, not as additive layers.

| Strategy A position | Strategy C signal | Correct action |
|---|---|---|
| None | CleanupSignal emitted | Strategy C may enter |
| Open YES | CleanupSignal YES | Block — Strategy A already holds the expected-value position |
| Open YES | CleanupSignal NO | Block — conflicting sides; one of the signals is wrong |
| Open NO | CleanupSignal YES | Block — conflicting sides |

**Implementation.** Market selection in both strategy engines must enforce this as a two-sided gate:
- Strategy A's supervisor routing skips any ticker where `resolution_state == LOCKED_YES` or `LOCKED_NO` **and** Strategy C is enabled — even if Strategy C holds no position yet (the lock itself marks the ticker as a Strategy C candidate).
- Strategy C's signal engine skips any ticker where an open Strategy A position exists for that ticker.

**Stage 2 re-check.** In addition to market-selection filters, the execution pre-flight re-verifies the invariant immediately before placing any order. If a concurrent Strategy A fill completed while a Strategy C signal was in-flight, the re-check blocks the Strategy C order and emits a SEV-2 alert.

**Lock-reversal exit.** If Strategy A holds a YES position and the market subsequently reaches `resolution_state == LOCKED_NO` (temperature drops below threshold before settlement), Strategy A's position management must unwind at market. This is not a Strategy C trade — do not route it through `strategy_cleanup.py`. Add this scenario to Strategy A's risk path as an explicit handled case.

**SEV-2 alert format.** `"PARTITION VIOLATION BLOCKED: ticker={ticker}, a_position={...}, c_signal={...}"`. Emitted to the watchdog ops-event stream. Do not suppress or rate-limit — each event requires investigation.

#### 4.1.3 Files to touch

**New files:**
- `src/kalshi_bot/services/strategy_cleanup.py` — the Strategy C signal engine, parallel to `services/signal.py`
- `src/kalshi_bot/services/cli_reconciliation.py` — per-station CLI-vs-METAR historical variance stats
- `alembic/versions/YYYYMMDD_NNNN_strategy_cleanup.py` — new tables (see §4.1.6)
- `tests/unit/test_strategy_cleanup.py`
- `tests/integration/test_strategy_cleanup_service.py`
- `docs/strategy/strategy_c_cleanup.md` — design doc with activation criteria

**Modified files:**
- `src/kalshi_bot/core/enums.py` — add `StrategyMode.RESOLUTION_CLEANUP`; extend `StandDownReason` with cleanup-specific reasons
- `src/kalshi_bot/core/schemas.py` — extend `StrategySignal` or create `CleanupSignal` with cleanup-specific fields
- `src/kalshi_bot/services/container.py` — register `StrategyCleanupService`
- `src/kalshi_bot/orchestration/supervisor.py` — add strategy router selecting between directional (A) and cleanup (C) based on `resolution_state`; enforce two-sided partition gate (§4.1.2)
- `src/kalshi_bot/services/risk.py` — add cleanup-specific risk path with tighter caps during shadow period
- `src/kalshi_bot/config.py` — new settings: `strategy_c_enabled: bool = False`, `strategy_c_shadow_only: bool = True`, plus the full adaptive-polling and lock-confirmation config block in §4.1.5 (do not duplicate here — §4.1.5 is authoritative)
- `docs/kalshi-weather-bot-engineering-plan.md` — section 5.x added for Strategy C activation criteria
- `src/kalshi_bot/cli.py` — add `shadow-c-sweep`, `strategy-c-status`, and `strategy-c approve` commands

#### 4.1.4 Strategy C logic

For each open weather threshold market where `resolution_state == LOCKED_YES` or `LOCKED_NO`:

1. **Confirm lock.** Pull latest NWS `/observations/latest` for the station. Confirm observed max since 00:00 LST (not midnight DST — LST per `kalshi_hightemp_playbook.md` §1.2 trap) has actually crossed the threshold with margin ≥ `cli_reconciliation_buffer_degf` (default 0.5°F, per-station from historical variance table).

   **Stale-observation guard.** NWS `/observations/latest` occasionally returns a reading that is 2–4 hours old (sensor gap or backhaul delay). Before treating a reading as current, verify its `timestamp` is within the last 30 minutes. If stale: log, emit an ops warning if the staleness persists across two consecutive polls, and do **not** trigger a Strategy C trade. The existing `integrations/weather.py` ingestion should be audited for this check in Session 1 before Strategy C work begins.

   **Polling cadence — adaptive by threshold proximity.** Strategy C's edge decays between observation publication and market reprice; Kalshi's market makers see the same NWS feed. The cadence therefore scales with how close the station is to a tradeable lock, not with a fixed interval:

   | Station state | Condition | Cadence |
   |---|---|---|
   | idle | current_temp > `strategy_c_approach_margin_f` (5°F) from all open thresholds | 60 min |
   | approach | within 5°F of any open threshold, before forecast peak | 15 min |
   | near-threshold | within `strategy_c_near_threshold_margin_f` (2°F) of any open threshold | 2–3 min |
   | post-peak | station confirmed diurnal peak and is cooling | 15 min |

   Tier assignment is recomputed every polling cycle. Typically only 3–5 stations are in the near-threshold tier simultaneously, so the fast cadence is not globally expensive. NWS per-station rate limits are generous.

   **Implementation decomposition.** Do not build the adaptive cadence logic inside the Strategy C signal pass. Build it as a separate `ThresholdProximityMonitor` service in Session 5 (see §7) — with its own unit tests for tier transitions — that Session 7's signal logic consumes. This mirrors the Prerequisite 0 decomposition principle: cadence behavior must be testable independently from lock detection.

2. **Compute fair value.** For a locked-YES contract:

   ```
   fair = 1.00 - p_lose
   ```

   where `p_lose` is the probability that NWS CLI QC revises the final daily max *downward* below threshold (reversing the lock). Default: flat `strategy_c_locked_yes_discount_cents = 1` (1¢, i.e. p_lose = 0.01). This is conservative relative to observed CLI/ASOS divergence history for contracts with ≥ 0.5°F margin above threshold. For locked-NO: `fair = 0.00 + (strategy_c_locked_no_discount_cents / 100)`.

   These discounts are deliberately flat and are not derived per-station from the variance table. Tail-probability estimation (`p_lose` lives at roughly the Φ(-4) regime for well-buffered locks) is unreliable at the sample sizes available per station, regardless of how well bulk σ is estimated. The `cli_station_variance` table is retained for dashboard display and anomaly detection — not for discount computation. Do not add station-specific overrides here; CLI revision behavior is driven by NWS QC processes that are roughly station-agnostic (unlike weather σ, where marine microclimate creates genuine station-specific signal).

3. **Calculate edge.** `edge_cents = fair - market_ask` (for YES buy; invert for NO buy). Edge must exceed `strategy_c_min_edge_cents` (default **2**). No upper-bound edge check — large edges are opportunities, not errors; the failure modes that produce apparent large edges are handled by the freshness and market-status gates below.

   **Fee model.** Kalshi charges `ceil(0.07 × C × P × (1-P) × 100) / 100` per contract, where C is contract count and P is price in dollars. For Strategy C contracts priced 97–99¢ YES (or 1–3¢ NO), the `P(1-P)` term is tiny: `0.07 × 0.97 × 0.03 ≈ 0.002`. This rounds up to **1¢/contract** regardless of size — effectively a fixed minimum-floor fee, not a percentage of edge. A 2¢ gross edge therefore yields 1¢ net per contract, or ~$0.52 net per $50 notional (52 contracts). Update `strategy_c_min_edge_cents` only if Kalshi's fee schedule changes. Do not make this threshold price-dependent — per-contract edge is the right metric and price-dependent thresholds create selection effects toward poorly-priced locks.

   **Maker rebates.** If Kalshi's maker rebate is active at implementation time, Strategy C may sometimes provide liquidity as maker (slow windows, thin books). Effective fee can go negative in that case. Do not assume taker-always in code; read fee type from fill receipts and account for it in PnL tracking.

4. **Freshness and market-status gates** (distinct from observation freshness in step 1, which addresses sensor staleness):
   - `market_snapshot.observed_at` within last `strategy_c_max_book_age_seconds` (default **30**). A stale orderbook can show 85¢ bid from 3 minutes ago after a lock. Reject and wait for next poll.
   - `market.status == 'active'`. Reject any ticker with status `suspended`, `paused`, or `closed_pending_settlement`. Large apparent edges on non-active tickers are common and are not tradeable.
   - No recent adverse Kalshi platform messages about this ticker or station within `strategy_c_recent_adverse_window_minutes` (default **15**). This is a best-effort check — if the data is unavailable, do not suppress the trade; log a warning instead.

5. **Lock-confirmation gates (in addition to existing deterministic engine):**

   Lock confirmation is a three-part check addressing three distinct failure modes. Do not collapse these into a single time-based gate — they address independent risks.

   **Part A: persistence check (transient sensor error).** The locked state must hold across `strategy_c_required_consecutive_confirmations` consecutive ASOS transmissions (default **2**, not tunable below 2). "Consecutive" means the current observation and the immediately prior ASOS cycle both show the threshold crossed. A retracted single-cycle spike breaks the sequence and suppresses the trade. Note: persistence is tracked **per station**, not per contract. If a station confirms a lock, all open threshold contracts for that station at or below the observed max are confirmed simultaneously — do not re-wait per ticker.

   **Part B: cross-source sanity check (sustained sensor anomaly).** The ASOS observed max must be within `strategy_c_max_forecast_residual_f` (default **8°F**) of the day's NWS gridpoint forecast. If the observed max diverges from the gridpoint by more than this margin, flag as a potential sensor anomaly and suppress the trade. The gridpoint forecast is already in memory for every Strategy A evaluation — this check is free. The 8°F default is intentionally wide to avoid false suppression; tighten only if shadow data shows frequent false positives. `strategy_c_required_consecutive_confirmations` is not tunable from shadow data — rare transient errors cannot be calibrated reliably in a 30-day shadow window.

   **Part C: freshness gate (stale observation — already specified in step 1).** Observation `timestamp` within `strategy_c_max_observation_age_minutes` (default 30 min). Distinct concern from Part A/B — a stale reading might satisfy the persistence check if both the current and prior cached observations are old.

   **Additional gates:**
   - Station has ≤ `strategy_c_max_cli_variance_degf` historical variance (computed from `cli_reconciliation` table; guards against stations with chronic CLI/ASOS divergence)
   - Time until market settlement ≥ `strategy_c_min_time_to_settlement_minutes` (default 60 min; avoid settling-soon liquidity issues)
   - No open position on the opposite side of the same ticker

6. **Sizing.** Strategy C uses its **own** size caps, much smaller than Strategy A:
   - `strategy_c_max_order_notional_dollars` (default $50)
   - `strategy_c_max_position_notional_dollars` (default $50)
   - No Kelly; flat sizing at min(cap, orderbook depth).

7. **Shadow-first.** While `strategy_c_shadow_only=true`, produce `CleanupSignal` → `TradeTicket` with `mode=shadow`; do **not** hit Kalshi write endpoints. Log the simulated decision.

#### 4.1.5 Adaptive polling configuration

```python
# config.py additions for ThresholdProximityMonitor (adaptive cadence)
strategy_c_cadence_idle_seconds: int = 3600
strategy_c_cadence_approach_seconds: int = 900
strategy_c_cadence_near_threshold_seconds: int = 150
strategy_c_cadence_post_peak_seconds: int = 900
strategy_c_near_threshold_margin_f: float = 2.0
strategy_c_approach_margin_f: float = 5.0

# config.py additions for lock-confirmation gates
strategy_c_required_consecutive_confirmations: int = 2   # ASOS cycles; not tunable below 2
strategy_c_max_observation_age_minutes: int = 30         # freshness rejection (Part C)
strategy_c_max_forecast_residual_f: float = 8.0          # sustained-anomaly gate (Part B)
strategy_c_max_cli_variance_degf: float = 1.5            # per-station CLI/ASOS variance ceiling
strategy_c_min_time_to_settlement_minutes: int = 60
strategy_c_locked_yes_discount_cents: int = 1   # flat; not per-station
strategy_c_locked_no_discount_cents: int = 1    # flat; not per-station

# config.py additions for edge and book-freshness gates
strategy_c_min_edge_cents: int = 2              # gross, before fees; see fee model in §4.1.4 step 3
strategy_c_max_book_age_seconds: int = 30
strategy_c_recent_adverse_window_minutes: int = 15
strategy_c_race_detection_enabled: bool = True  # classify zero-fill cancels as 'raced' when True;
                                                  # False → all zero-fill orders classified 'cancelled'
                                                  # for manual triage (debugging mode only)
```

**Note on Strategy A.** The two-column execution/settlement decomposition should be adopted for Strategy A's outcome tracking when that table is next refactored. The root-cause pattern is the same: a "lost" trade in Strategy A could mean "directionally wrong" (the forecast was wrong) or "filled correctly but market moved against position before settlement" (exposure risk). Different bugs; same collapsed taxonomy. Out of scope for this spec, but flag it for the next Strategy A review.

Cadence values are conservative defaults. Tune `strategy_c_cadence_near_threshold_seconds` down only if shadow data shows the system is consistently late (>40% of qualifying locks reprice before the poll fires). `strategy_c_required_consecutive_confirmations` is fixed at 2 — 30 days of shadow data is insufficient to calibrate transient-error frequency; do not lower based on shadow P&L.

#### 4.1.6 New database tables

```sql
-- CLI vs real-time observation variance per station, for buffering the lock detection
CREATE TABLE cli_reconciliation (
    station           TEXT NOT NULL,
    observation_date  DATE NOT NULL,
    asos_observed_max REAL NOT NULL,
    asos_observed_at  TIMESTAMPTZ,
    cli_value         REAL NOT NULL,
    cli_published_at  TIMESTAMPTZ,
    delta_degf        REAL NOT NULL,          -- cli_value - asos_observed_max
    note              TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (station, observation_date)
);

-- Strategy C rooms (parallel to existing rooms table)
-- Two outcome columns, two lifecycle dimensions — do not collapse them.
-- execution_outcome: what happened between signal and order resolution (stage 1).
-- settlement_outcome: did CLI agree with the asserted lock (stage 2, diagnostic).
CREATE TABLE strategy_c_rooms (
    room_id              UUID PRIMARY KEY,
    ticker               TEXT NOT NULL,
    station              TEXT NOT NULL,
    decision_time        TIMESTAMPTZ NOT NULL,
    resolution_state     TEXT NOT NULL,
    observed_max_at_decision REAL NOT NULL,
    threshold            REAL NOT NULL,
    buffer_at_decision_f REAL NOT NULL,          -- observed_max_at_decision - threshold; signed (negative = below threshold)
    fair_value_dollars   NUMERIC(10,4) NOT NULL,
    modeled_edge_cents   REAL NOT NULL,
    target_price_cents   REAL NOT NULL,
    contracts_requested  INT NOT NULL,
    contracts_filled     INT NOT NULL DEFAULT 0,
    avg_fill_price_cents REAL,                  -- NULL if contracts_filled == 0
    realized_edge_cents  REAL,                  -- modeled_fair - avg_fill_price; NULL if no fill
    execution_outcome    TEXT NOT NULL,
        -- 'filled'       all requested contracts filled
        -- 'partial_fill' some filled; rest raced or expired
        -- 'raced'        zero fills; market moved before order landed (see race detection below)
        -- 'cancelled'    operator or kill-switch cancelled before fill
        -- 'rejected'     risk engine blocked
        -- 'error'        Kalshi rejected, network error, or other non-race failure
        -- 'shadow'       shadow-mode, no real order placed
        -- 'pending'      in flight
    settlement_outcome   TEXT,                  -- NULL until settled
        -- 'lock_held'    CLI matched asserted lock direction (expected)
        -- 'lock_reversed' CLI disagreed (signal precision bug; investigate immediately)
        -- 'void'         Kalshi voided the market
    outcome_pnl_dollars  NUMERIC(10,4),         -- settlement P&L on filled contracts; NULL if unsettled
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Note: 'raced' classification is set by post-fill analysis in execution.py / a post-fill
-- analysis service, not by the database. A zero-fill order with the target ask gone from the
-- orderbook at cancel-time is classified 'raced'; operator-cancelled is 'cancelled';
-- Kalshi-rejected is 'error'. Do not infer it from raw Kalshi responses via SQL.

-- Per-station CLI variance rollup (materialized periodically)
-- Signed columns: used for future parametric calibration (retained, not consumed by default pricing path)
-- Abs-value columns: used for monitoring dashboards and anomaly detection only
CREATE TABLE cli_station_variance (
    station                  TEXT PRIMARY KEY,
    sample_count             INT NOT NULL,
    signed_mean_delta_degf   REAL NOT NULL,   -- E[cli_value - asos_observed_max]; sign matters
    signed_stddev_delta_degf REAL NOT NULL,   -- σ of signed delta; needed for Gaussian p_lose formula
    mean_abs_delta_degf      REAL NOT NULL,   -- dashboard display
    p95_abs_delta_degf       REAL NOT NULL,   -- dashboard display / anomaly threshold
    last_refreshed_at        TIMESTAMPTZ NOT NULL,
    note                     TEXT
);
```

```sql
-- ThresholdProximityMonitor: DB-persisted write-through state.
-- Written on every tier transition; read by monitor on startup (to reconstruct working set)
-- and by Strategy C signal engine on each evaluation cycle.
CREATE TABLE threshold_proximity_state (
    station                TEXT NOT NULL,
    event_date             DATE NOT NULL,
    tier                   TEXT NOT NULL,        -- 'idle' | 'approach' | 'near_threshold' | 'post_peak'
    current_temp_f         REAL,                 -- most recent observed reading; NULL if not yet observed
    nearest_threshold_f    REAL,                 -- closest open threshold for this station/date
    entered_tier_at        TIMESTAMPTZ NOT NULL,
    last_updated_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (station, event_date)
);
```

**Write path:** `ThresholdProximityMonitor` calls `upsert(station, event_date, tier, ...)` on every tier change; also writes on startup to clear stale rows from prior daemon runs.
**Read path:** On startup the monitor reads all rows to reconstruct its in-memory working set without re-polling NWS. Strategy C signal engine reads the row for `(station, event_date)` on each evaluation — it does not call `ThresholdProximityMonitor` directly (decoupled via DB).

Populate `cli_reconciliation` by backfilling across the existing `historical_replay` corpus (reuse `services/historical_pipeline.py` patterns). Populate `cli_station_variance` via a weekly rollup job.

#### 4.1.7 Acceptance criteria

The three core metrics — signal precision, fill feasibility, and edge realization — are distinct and must be evaluated separately. A single "win rate" target collapses them in ways that make it impossible to diagnose failures. Each has its own hard-block threshold.

1. **Unit tests pass.** Coverage ≥ 85% on `strategy_cleanup.py` and `cli_reconciliation.py`.

2. **Shadow-C runs for 30 calendar days** producing ≥ **50 qualifying events** (shadow-mode `strategy_c_rooms` rows where `execution_outcome != 'shadow_blocked'`). If 30 days completes with fewer than 50 qualifying events, extend in 15-day increments until the event-count gate is met. A 30-day shadow window that logs only 10 events provides insufficient statistical power to evaluate signal precision or fill-rate distribution — an estimated ~70–75 qualifying Strategy C events per 30 days is expected across 20 cities, so this gate should be met within the base window barring unusual market closures.

3. **Signal precision.** Of shadow `CleanupSignal`s asserting a locked-YES state, ≥ 99% of the corresponding contracts settled YES (and analogously for locked-NO). Computed by joining `strategy_c_rooms` against `historical_settlement_labels` for all settled rows in the shadow window. Hard block at < 95% — a miss rate above 5% indicates a bug in lock-detection logic, not a calibration issue.

4. **Counterfactual fill rate.** Of shadow `CleanupSignal`s, estimate what fraction would have filled at the asserted price within a 5-second latency budget, by replaying `market_price_history` orderbook snapshots against the signal timestamp. Target range: 40–70%. Hard block: < 20% (strategy is too slow to be viable at that fill rate; fix latency before live switch). No upper threshold — high fill rates are fine. Before setting a definitive threshold, measure the actual distribution first; this number is uncalibrated across the industry.

   **Latency budget note.** Instrument the shadow pipeline end-to-end with timestamps: `CleanupSignal` created → `TradeTicket` emitted → risk-verdict complete → would-have-placed. P99 of this total must be ≤ 5 seconds. If it exceeds 5 seconds, fix the latency before any live deployment — the fill-rate analysis is only meaningful if the latency budget is enforced.

5. **Counterfactual edge realization.** Of signals that would have filled (from criterion 4), median realized edge (fill price minus fair value) ≥ 80% of modeled edge, and P10 realized edge > 0. Captures adverse selection cost: consistently low realized edges indicate fills are happening when the counterparty has information the model lacks.

6. **Per-station CLI variance populated.** `cli_station_variance` has rows for all 20 stations with `signed_mean_delta_degf` and `signed_stddev_delta_degf` based on ≥ 30 CLI/ASOS pairs each. (Requires 30+ days of historical CLI data — use `historical-backfill weather-archive` and `historical-backfill settlements` first.)

7. **Divergence event analyzed.** At least one day in the shadow window where CLI differed from ASOS by ≥ 1°F has been identified and reviewed. The shadow decisions on that day must be consistent with post-hoc analysis — specifically, the Part A persistence check and Part B gridpoint-residual gate must have correctly suppressed trades on any erroneous readings. If the 30-day window contains no divergence events across all 20 stations, extend shadow to 60 days or require N ≥ 3 events. (Divergences at this magnitude occur roughly 3–5 times per year per station across the fleet, so a 30-day window typically yields 5–15 events total.)

8. **Dashboard row added.** Control room `/api/control-room/summary` surfaces Strategy C shadow metrics: signal count/day, counterfactual fill rate, median realized edge, signal precision, race rate, per-station variance status.

9. **Operator review and DB activation.** Grant reviews all three core metrics (criteria 3–5) and explicitly approves the live transition in writing (commit a dated approval note to `docs/strategy/strategy_c_cleanup.md`). Additionally, create a DB activation record using the CLI:
   ```bash
   kalshi-bot-cli strategy-c approve --shadow-window-id <window_uuid>
   ```
   This writes a row to the `strategy_activations` table (see §5.8) referencing the shadow window being graduated. The live deployment verifies this record exists at startup and blocks live execution if it is absent — a config flag alone (`strategy_c_shadow_only=false`) is not sufficient. Do not flip `strategy_c_shadow_only=false` without the DB activation record in place.

**Live monitoring (post-graduation).** Track five numbers together in the control room dashboard; no single number tells the full story:
- **Signal count** — how often Strategy C triggers
- **Fill rate** — fraction of signals where `execution_outcome in ('filled', 'partial_fill')`
- **Race rate** — fraction where `execution_outcome = 'raced'`; a rising trend is your early warning that the edge window is closing (bots faster, Kalshi tick cadence changed, or your latency degraded)
- **Realized edge per fill** — distribution (median + P10), not mean; drawn from `realized_edge_cents`
- **Win rate on fills** — fraction of settled fills where `settlement_outcome = 'lock_held'`; target ≥ 99%; any drift below 99% is a signal-precision issue requiring immediate investigation, not a tolerable degradation

#### 4.1.8 Stop condition

Do not promote Strategy C out of shadow mode until all nine acceptance criteria (§4.1.7) pass and operator has signed off. Specifically:
- Shadow window ≥ 30 calendar days (or 60 days if no divergence events observed)
- Signal precision ≥ 99% (hard block at 95%)
- Counterfactual fill rate ≥ 20% (hard block; 40–70% is the calibrated target)
- Counterfactual edge realization median ≥ 80% of modeled, P10 > 0
- At least one CLI/METAR divergence event reviewed and gates validated
- Operator explicitly signs off in writing

---

### 4.2 Addition 2: Per-Station σ Calibration

**Playbook reference:** `kalshi_technical_deepdive.md` §1.5 (stratified bias correction).
**Repo baseline:** `src/kalshi_bot/weather/scoring.py` uses a single seasonal σ table regardless of station.

**Thesis:** KNYC (Central Park) and KDEN (Denver) have fundamentally different diurnal temperature behavior. A single `sigma_f` calibrated on "all cities averaged" is suboptimal for both. The fair-value model improves measurably from station-specific σ, without requiring philosophical change to the fair-value approach.

**Why it fits the repo's philosophy:** Pure improvement to existing Layer 2 Gaussian model. No new strategy. No new sizing framework. Works entirely within their chosen probabilistic approach.

**PR discipline.** Prerequisite 0 (§4.2.0) ships as its own PR and is merged + deployed before §4.2.1 work begins. Do not bundle the pagination bug fix with the sigma fitting work. If Addition 2 hits review friction, the bug fix should not be blocked behind it.

---

#### 4.2.0 Prerequisite 0: Fix silent pagination bug in `_list_historical_markets`

**Severity.** Data integrity. Silent — no error surface; backfill "succeeds" and returns zero errors but produces gaps.

**Cause.** `_list_historical_markets()` calls `list_historical_markets(limit=N, cursor=cursor)` without `series_ticker`. Kalshi's `/historical/markets` pagination is global-newest-first across all series. With `historical_import_max_pages=25` and 500 markets per page, the 12,500-market budget is consumed by unrelated series before reaching the target series' historical range. Each series crawl starts over from the top, so the bug compounds with series count.

**Evidence.** Probe dated 2026-04-22: the Kalshi API *does* accept `series_ticker` as a query parameter on `/historical/markets` and returns filtered results correctly (verified live). Tier 1 cities (`KXHIGHNY`, `KXHIGHCHI`, `KXHIGHLAX`, `KXHIGHAUS`, `KXHIGHDEN`, `KXHIGHMIA`, `KXHIGHPHIL`) have markets back to Dec 2024 confirmed via direct API call with `series_ticker` filter. Without the fix, `historical-import weather` returns zero results for those series within the 25-page budget because global pagination reaches only to Feb 2026.

**Station tier map (as of 2026-04-22):**
| Tier | Series | Oldest available | Estimated unique station-days |
|---|---|---|---|
| 1 | KXHIGHAUS, KXHIGHNY, KXHIGHCHI, KXHIGHDEN, KXHIGHLAX, KXHIGHMIA, KXHIGHPHIL | Dec 2024 | ~470 |
| 2 | KXHIGHTDC, KXHIGHTLV, KXHIGHTNOLA, KXHIGHTSEA, KXHIGHTSFO | Jan 2026 | ~40 |
| 3 | KXHIGHTATL, KXHIGHTBOS, KXHIGHTDAL, KXHIGHTHOU, KXHIGHTMIN, KXHIGHTOKC, KXHIGHTPHX, KXHIGHTSATX | Feb 2026 | ~10–20 |

**Fix.** One line in `services/historical_training.py::_list_historical_markets`:
```python
# Before:
response = await self.kalshi.list_historical_markets(limit=..., cursor=cursor)
# After:
response = await self.kalshi.list_historical_markets(limit=..., cursor=cursor, series_ticker=template.series_ticker)
```

**Tests (both required before merging):**
- **Unit:** mock `list_historical_markets`; assert `series_ticker` is present in call kwargs for every template iteration. Protects against argument deletion.
- **Integration:** crawl two series (`KXHIGHNY` and `KXHIGHMIA`) in the same `_import_market_definitions` call; assert that (a) no cross-series tickers appear in either result set and (b) the total market count per series is independent of crawl order. This catches the category of bug — series-mixed pagination — not only the current instance.

**RCA note.** Document in `docs/adrs/001-historical-markets-series-filter.md`. Two paragraphs: what the bug was, why it was silent (successful return, zero errors), and how the live probe revealed it. The ADR's purpose is to explain to future contributors why the integration test exists. Tests without context get deleted during refactors.

**Acceptance gate before §4.2.1:**
- Prerequisite 0 PR merged and deployed to production.
- Operator runs `KALSHI_ENV=production kalshi-bot-cli historical-import weather --date-from 2024-12-01 --date-to 2026-04-10 --series KXHIGHAUS KXHIGHNY KXHIGHCHI KXHIGHDEN KXHIGHLAX KXHIGHMIA KXHIGHPHIL`
- Production DB query confirms ≥ 100 `crosscheck_high_f`-populated rows per Tier 1 station.
- Tier 2 and Tier 3 series accumulate naturally from live trading; no forced backfill needed for them to participate in σ fitting — they are simply below threshold and fall back to YAML/global until they cross it.

---

#### 4.2.1 Architecture: three-layer σ resolution

σ is resolved at runtime by walking three layers in order, from most-specific to least:

```
Layer 1 (DB-fit, per station/season):  station_sigma_params (station, season) — when ≥100 samples AND CRPS > global
Layer 2 (YAML anchor, monthly):        sigma_f_by_month in weather_markets YAML — operator-specified cells only
Layer 3 (global fallback):             _MONTHLY_SIGMA_F in scoring.py — always present
```

**Two-stage σ computation.** The DB-fit layer separates into two independent fits to preserve both station-specific accuracy and lead-time physics without fragmenting the sample budget:

- `σ_base(station, season)` — fit per (station, season) with all samples pooled across lead times. Uses the full Tier 1 corpus (~470 station-days, ~17 per stratum) and scales well as data accumulates.
- `lead_factor(lead_bucket)` — fit once across the entire dataset (3,000+ samples). Captures the physics that forecast error grows with lead time (σ(D-0) ≈ 2–3°F vs σ(D-1) ≈ 3–5°F). Estimated from log|residuals| vs lead_hours via OLS.

At query time: `σ_effective = σ_base(station, season) × lead_factor(lead_bucket)`. The lead factor is controlled by `sigma_lead_correction_enabled` (kill switch for when the measured factor is ≈ 1.0 and correction adds no value).

**Plumbing prerequisite.** This requires `WeatherSignalEngine` to know its lead time at σ lookup. Verify in Session 1 whether `signal.py` currently tracks `(room_decision_time, target_settlement_date)` in a structured way. If it does, lead is derivable with no new schema. If not, note it as a small upstream change required before DB-fit wiring.

**Resolution logic:**
1. If a DB-fit row exists for `(station, season)` with `sample_count ≥ sigma_min_samples_beats_yaml` (200) AND `crps_improvement_vs_global ≥ sigma_min_crps_improvement` (0.05): use `σ_base × lead_factor`, with bias correction applied.
2. Else if DB-fit row exists with `sample_count ≥ sigma_min_samples_beats_global` (100) AND `crps_improvement_vs_global ≥ 0.05` but YAML anchor is absent: use `σ_base × lead_factor`.
3. Else if a YAML cell exists for `(station, month)`: use it (× lead_factor if `sigma_lead_correction_enabled`).
4. Else use global `_MONTHLY_SIGMA_F[month]` (× lead_factor if `sigma_lead_correction_enabled`).

The `crps_improvement_vs_global ≥ 0.05` gate means the DB-fit must reduce CRPS by at least 5% vs the global fallback (`fit_crps / global_crps ≤ 0.95`). A DB-fit that barely improves — or shows CRPS regression — falls back to YAML/global even when `is_active=True`. This prevents a numerically-fit row from displacing a well-understood prior when evidence is marginal. The 5% threshold is not tunable via config; it encodes the minimum practical improvement worth the added complexity of a DB-derived σ.

**CRPS regression protection.** If a subsequent refit produces a row where `crps_improvement_vs_global < 0` (regression vs prior version), mark the new row `is_active=FALSE` and retain the prior active row. Log an ops warning with the CRPS delta. Do not silently regress the model.

**Why two sample thresholds?** Sample SE on σ scales as `σ/√(2n)`. At n=100 and σ=4°F, SE ≈ 0.28°F. Global fallback is a rough average across stations — beating it is easier, so n=100 suffices. YAML overrides encode explicit operator knowledge (e.g., SFO marine layer) calibrated to ≈ 0.3°F precision — DB-fit needs n≥200 (SE ≈ 0.2°F) to compete.

This is **not** a simple "YAML wins" hierarchy. YAML provides an operator anchor for cells where domain knowledge is strong; DB-fit provides refinement as data accumulates. YAML does not block DB-fit from winning — DB-fit wins when it has sufficient evidence and beats the global fallback.

**YAML is sparse by design.** Operators specify only the `(station, month)` cells where they have genuine prior knowledge. Cells not specified in YAML fall through to DB-fit or global. Over-specifying YAML (e.g., entering DJF values for a station where you have no real prior) prevents DB-fit from correcting bad guesses.

**Example:** SFO JJA marine layer suppresses daily-high variability to ~2°F. That is documented, repeatable phenomenon — strong YAML prior. SFO DJF is dominated by frontal variability — no strong prior, leave it to data. See §4.2.7 for YAML hygiene requirements.

**This design is the end state, not transitional.** Layer 2 (YAML) does not get deprecated as data grows. Operator knowledge is genuinely better than data during climate regime shifts, model updates, or station relocations. The steady state is: most cells resolved by DB-fit, operator YAML as escape hatch for cells where domain knowledge beats statistics.

**Do not use hierarchical partial pooling.** Partial pooling (shrinking thin strata toward a hyperprior) would be the rigorous statistical answer. Don't implement it: it requires Stan/PyMC machinery, creates explanation surface for future contributors, and adds diagnostic complexity not justified at this data scale. Two-stage estimation (σ_base + lead_factor) captures 80% of the value with 10% of the machinery.

#### 4.2.2 Scope

Implement the three-layer lookup. The current codebase already has Layer 2 (shipped as `sigma_f_by_month` on `WeatherMarketMapping`) and Layer 3 (global `_MONTHLY_SIGMA_F`). This addition adds Layer 1 (DB-fit) and wires the full resolution chain.

#### 4.2.3 Files to touch

**New files:**
- `src/kalshi_bot/weather/sigma_calibration.py` — fitting logic (two-stage σ_base + lead_factor, chronological holdout, rolling-origin CV for n≥200)
- `src/kalshi_bot/services/sigma_resolver.py` — `SigmaResolver` service: encapsulates three-layer lookup, in-process 1h TTL cache with per-key `asyncio.Lock`, returns `ResolvedSigma`
- `alembic/versions/YYYYMMDD_NNNN_station_sigma.py` — `station_sigma_params` and `global_lead_factor` tables
- `tests/unit/test_sigma_calibration.py`
- `tests/unit/test_sigma_resolver.py` — cache TTL test, concurrent-caller lock test, fallback-layer selection tests
- `scripts/refit_station_sigma.py` — manual refit trigger
- `docs/strategy/sigma_calibration.md` — design doc

**Modified files:**
- `src/kalshi_bot/weather/scoring.py` — `sigma_f_for_mapping()` refactored to accept a pre-resolved `ResolvedSigma` dataclass (injected by `SigmaResolver`); no longer calls DB directly
- `src/kalshi_bot/core/schemas.py` — add `ResolvedSigma` dataclass (six fields per §4.2.6)
- `src/kalshi_bot/services/signal.py` — pass station identifier and forecast lead hours into scoring; verify whether `(room_decision_time, target_settlement_date)` is already tracked before adding new fields (Session 1 audit)
- `src/kalshi_bot/config.py`:
  - `sigma_calibration_enabled: bool = True`
  - `sigma_calibration_refit_cadence_days: int = 7`
  - `sigma_min_samples_beats_global: int = 100` — threshold for DB-fit to supersede global fallback
  - `sigma_min_samples_beats_yaml: int = 200` — threshold for DB-fit to supersede YAML anchor
  - `sigma_min_crps_improvement: float = 0.05` — DB-fit must reduce CRPS by ≥5% vs global (fit_crps/global_crps ≤ 0.95)
  - `sigma_lead_correction_enabled: bool = True` — kill switch; set False if lead_factor ≈ 1.0 at measurement
- `src/kalshi_bot/services/daemon.py` — register weekly refit task
- `docs/examples/weather_markets.example.yaml` — audit YAML overrides for sparsity (see §4.2.7)

#### 4.2.4 Fitting methodology

**Stage 1: per-(station, season) σ_base.** For each `(station, season_bucket)`:

1. Pull all matched pairs `(forecast_high_f, crosscheck_high_f)` from `historical_weather_snapshots` joined to `historical_settlement_labels`, across all lead times.
2. Compute residuals `r_i = crosscheck_high_f_i - forecast_high_f_i`.
3. Fit `sigma_base_f` = std(r_i), `mean_bias_f` = mean(r_i).
4. Compute `sigma_se_f` = std(r_i) / √(2n) — standard error on σ estimate.
5. Compute `residual_skewness` — flag non-Gaussianity for dashboard display.
6. Compute `crps_improvement_vs_global` — evaluate both this fit and the global fallback on a **chronological holdout** (last 20% of pairs ordered by date, not a random split). Random splits leak future data into the training set via temporal autocorrelation; chronological holdout reflects true out-of-sample performance. For strata with `sample_count ≥ 200`, use **rolling-origin cross-validation** (expanding window, 5 folds) instead of a single split and report the mean CRPS improvement across folds.
7. Persist to `station_sigma_params` with version timestamp. Write the row regardless of sample count — the resolver gates on sample count and CRPS improvement (≥5%) at lookup time.

**Stage 2: global lead_factor.** Run once across the entire corpus (all stations, all seasons):

1. Compute `lead_hours` per pair from `historical_weather_snapshots.asof_ts` and `historical_settlement_labels.settlement_ts`. Bin into buckets: `'D-0'` (0–18h), `'D-1'` (18–42h), `'D-2+'` (42h+). Note: lead time is not currently a first-class column; compute from timestamp difference in the join query.
2. Fit `log|r_i|` vs `lead_bucket` via OLS to get a multiplicative scaling factor per bucket.
3. Normalize so the reference bucket (D-0) has factor = 1.0; other buckets are expressed as ratios.
4. Persist to `global_lead_factor` table. If `lead_factor(D-0) / lead_factor(D-1) ≈ 0.95–1.05`, the correction is negligible — set `sigma_lead_correction_enabled=False` in `.env` and document the finding. Do not pre-decide based on theory; measure first.

**Bias correction is active when DB-fit wins.** When the DB-fit row is the active layer, the fair-value formula becomes:
```
bias_corrected_delta_f = delta_f - mean_bias_f(station, season)
sigma_f = sigma_base_f(station, season) × lead_factor(lead_bucket)
P = Φ(bias_corrected_delta_f / sigma_f)
```
When YAML or global is the active layer, bias correction is not applied (no fitted bias available); lead_factor is still applied if `sigma_lead_correction_enabled`.

#### 4.2.5 New tables

```sql
-- Per-(station, season) σ_base. No lead_hours_bucket — lead correction is in global_lead_factor.
CREATE TABLE station_sigma_params (
    station                   TEXT NOT NULL,
    season_bucket             TEXT NOT NULL,   -- 'DJF', 'MAM', 'JJA', 'SON'
    sigma_base_f              REAL NOT NULL,
    mean_bias_f               REAL NOT NULL,
    sample_count              INT NOT NULL,
    sigma_se_f                REAL NOT NULL,   -- std error on σ; σ/√(2n)
    residual_skewness         REAL,            -- non-Gaussianity indicator; dashboard only
    crps_improvement_vs_global REAL,           -- > 0 means DB-fit beats global on held-out 20%
    fitted_at                 TIMESTAMPTZ NOT NULL,
    version                   TEXT NOT NULL,   -- timestamp-based
    is_active                 BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (station, season_bucket, version)
);

CREATE INDEX station_sigma_active ON station_sigma_params (station, season_bucket)
  WHERE is_active = TRUE;

-- Global lead-time scaling factor — one fit across all stations and seasons.
CREATE TABLE global_lead_factor (
    lead_bucket   TEXT NOT NULL,   -- 'D-0' (0-18h), 'D-1' (18-42h), 'D-2+' (42h+)
    factor        REAL NOT NULL,   -- normalized so D-0 = 1.0
    sample_count  INT NOT NULL,
    fitted_at     TIMESTAMPTZ NOT NULL,
    version       TEXT NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (lead_bucket, version)
);
```

Note: `station_sigma_params` no longer stratifies by lead_hours. The YAML layer remains month-keyed (finer-grained operator control); the DB layer uses seasons (coarser, more samples per stratum); lead correction is a separate multiplicative factor from `global_lead_factor`.

#### 4.2.6 Observability requirements

Every σ lookup must be fully observable. For every room, the signal record must store:

| Field | Content |
|---|---|
| `sigma_active` | σ value actually used in scoring |
| `sigma_source` | `"db_fit"`, `"yaml_anchor"`, or `"global"` |
| `sigma_db_fitted` | DB-fit value for this cell (even if not the active source) |
| `sigma_db_sample_count` | Sample count behind DB-fit value (null if no row) |
| `sigma_yaml_anchor` | YAML-specified value for this cell (null if not specified) |
| `sigma_global` | Global fallback value for this month |

**Dashboard requirement:** the control room σ status panel shows all three layers for every active station/month/lead cell. When YAML is the active source and DB-fit differs by >10%, display both with a visual indicator.

#### 4.2.7 Divergence alerting and YAML hygiene

**Divergence alert:** When a DB-fit row has `sample_count ≥ 200` and disagrees with the YAML anchor by >20%, emit an ops event at severity `warning`:
```
"SFO JJA σ divergence: YAML=2.0°F, DB-fit=3.1°F (n=247). Review YAML anchor."
```
Either the operator prior has gone stale or the data is misleading. Silent persistence is wrong.

**Staleness warning:** Each YAML `sigma_f_by_month` override block should include an `added_at` / `reviewed_at` ISO date string (comment or field). Entries older than 12 months without a `reviewed_at` update emit:
```
"YAML sigma anchor for SFO JJA (σ=2.0) was added 14 months ago. DB-fit now suggests 2.4°F. Review?"
```
Implement as a startup check in daemon, logged as ops event severity `info`.

**YAML sparsity audit (do before session 2 coding starts):**
- Review all `sigma_f_by_month` entries in `weather_markets.example.yaml`
- Remove cells where no strong domain prior exists (guessed values are worse than data)
- Keep only cells with documented climate reasoning in an adjacent comment
- SFO example of correct sparsity: specify JJA (marine layer well-documented) and MAM/SON transition months only if confidence is high; leave DJF to data

#### 4.2.8 Acceptance criteria

1. **Unit tests pass.** Fit on synthetic data with known σ recovers within 5%. Resolution logic unit-tested: correct layer selected for each combination of (DB-fit present/absent, YAML present/absent, sample count above/below each threshold, CRPS positive/negative). Lead-factor application unit-tested with `sigma_lead_correction_enabled=True` and `False`.
2. **Fit populated.** Initial refit produces rows for all 20 stations × 4 seasons = 80 strata in `station_sigma_params`, plus 3 rows in `global_lead_factor` (D-0, D-1, D-2+). Strata below `sigma_min_samples_beats_global` (100) will have rows but `crps_improvement_vs_global` will indicate they don't qualify; the resolver falls back to global. Log which strata qualified at each threshold.
3. **Observability verified.** Every signal record written by scoring contains all six σ fields from §4.2.6. Verified by integration test.
4. **Calibration improvement.** Backtest shows **lower CRPS** under three-layer σ vs global-only, averaged across 20 cities. If CRPS does not improve, investigate before merging.
5. **Fair-value backtest.** Running Strategy A with three-layer σ on last 90 days produces equal-or-better realized win rate vs global σ (within 2 percentage points).
6. **Rollback path.** Setting `sigma_calibration_enabled=false` reverts to YAML+global chain (Layer 1 disabled). Verified via test.
7. **Divergence alerting fires.** Integration test: inject a DB-fit row disagreeing with YAML by 25%, verify ops event emitted.

#### 4.2.9 Stop condition

If backtest shows CRPS degradation or win-rate regression, do not merge. Likely causes:
- Insufficient samples per stratum despite meeting threshold (sample quality issue — outlier days)
- Lead-time approximation inaccurate (asof_ts not reflecting true forecast generation time)
- YAML cells still over-specified with weak priors — remove them and retest
- Data quality gap in `crosscheck_high_f` for specific stations

---

### 4.3 Addition 3: Monotonicity Arb Scanner

**Playbook reference:** Edge 4, adapted. Original playbook assumed 6-bin events summing to 100¢; `KXHIGH*` uses threshold-style markets.

**Thesis:** For a single station/day, `P(high > T)` must be monotonically non-increasing in T. If the market has `KXHIGHNY-26APR22-T85` trading at 45¢ YES and `KXHIGHNY-26APR22-T80` trading at 40¢ YES simultaneously (both for the same day, same station), that's a violation: the higher threshold (85°F, harder condition) appears more probable than the lower (80°F, easier condition). The correct arb is: **buy YES T80 at 40¢ + buy NO T85 at 55¢** (total cost 95¢, guaranteed payout ≥ $1 in all scenarios). See §4.3.3.1 for the full payoff table.

**Why it fits the repo's philosophy:** Near-zero variance. High selectivity (arbs are rare and usually small). Taker-only. No market-making. Uses existing infrastructure.

#### 4.3.1 Scope

A scanner service that runs every N seconds over current open `KXHIGH*` markets, groups by `(station, event_date)`, detects monotonicity violations net of fees, and emits trade proposals.

#### 4.3.2 Files to touch

**New files:**
- `src/kalshi_bot/services/monotonicity_scanner.py` — scanner + proposal generator
- `tests/unit/test_monotonicity_scanner.py`
- `tests/integration/test_monotonicity_scanner_service.py`

**Modified files:**
- `src/kalshi_bot/services/container.py` — register scanner
- `src/kalshi_bot/services/daemon.py` — schedule scanner at **5-second** cadence (competitive arb bots run tighter cycles; a 60-second scanner systematically misses violations and only captures slow-moving ones where atomic execution is most likely to leg-break)
- `src/kalshi_bot/config.py` — `monotonicity_arb_enabled: bool = False`, `monotonicity_arb_shadow_only: bool = True`, `monotonicity_arb_min_net_edge_cents: int = 2`, `monotonicity_arb_max_notional_dollars: float = 25.0`, `monotonicity_arb_max_pairs_per_tick: int = 100` (circuit breaker — skip remaining pairs if 100 evaluated in one tick), `monotonicity_arb_suppression_ttl_seconds: int = 300` (per-(station, event_date) suppression after a proposal is emitted)
- `src/kalshi_bot/core/enums.py` — `StrategyMode.MONOTONICITY_ARB`
- `src/kalshi_bot/cli.py` — `monotonicity-scan --once` command

#### 4.3.3 Scanner logic

On each tick:

1. Query all open `KXHIGH*` markets grouped by `(station, event_date)`
2. For each group, sort by threshold ascending
3. For each threshold, read `bid_yes` and `ask_yes` from the market snapshot. **Kalshi's NO side is a computed inverse of the YES side, not an independent book; `ask_no(T) = 100 - bid_yes(T)` by construction** — confirmed in `services/streaming.py` where `best_no_ask = 1.0000 - best_yes_bid` is a plain computed property, not a field from the API. Do not expect an independent `ask_no` field in the snapshot — it does not exist. If `bid_yes(T_j)` is None or zero (no resting YES bid on the higher threshold), skip the pair — there is no alternative source for the NO-side ask price.
4. Walk **all pairs** `T_i < T_j` (not just adjacent — a violation between T80 and T90 is real even if T85 is not violated). Apply a **100-pair circuit breaker per tick**: if 100 pairs have been evaluated for a given `(station, event_date)` group, skip remaining pairs and log a warning. This prevents a single bloated snapshot from starving the scanner across other city/date groups. For each pair, flag a violation when:
   ```
   bid_yes(T_j) - ask_yes(T_i) > 2·fees + min_edge_cents
   ```
   i.e., the **higher** threshold's YES bid exceeds the **lower** threshold's YES ask by more than fees and edge floor. This is the condition under which the total pair cost is less than the guaranteed $1 payout.

   **Fee calculation.** Use actual per-leg fee from Kalshi's formula `ceil(0.07·C·P·(1-P)·100)/100` evaluated at each leg's price — do not assume the 1¢ floor that applies to Strategy C's near-par prices. At mid-range prices (e.g., 30¢ and 50¢), per-contract fees can run 1.5–2¢. Safe-side shortcut for the detection gate: assume 2¢/leg = 4¢ total. Compute actual fees separately at sizing time.

5. For each violation, generate a paired proposal: **buy YES on T_i at ask_yes(T_i) + buy NO on T_j at ask_no(T_j)** (the lower threshold YES is cheap; the higher threshold NO is cheap because the higher threshold is overpriced on YES). Skip the pair if: `bid_yes(T_j) == 0`, `ask_yes(T_i) == 0` (no quote), or the `(station, event_date)` group has had a proposal emitted within the last `monotonicity_arb_suppression_ttl_seconds` (300 s) — do not re-propose while a prior proposal is still in-flight or recently settled.
6. Pass through existing risk engine with `StrategyMode.MONOTONICITY_ARB`
7. In shadow mode, log proposals; in live mode, emit paired orders sequentially per the atomic execution protocol below

**Key: atomic execution.** If only one leg fills, you have directional risk, not arb. There is no Kalshi batch endpoint — execute sequentially with these semantics:

1. Place the less-liquid leg first (heuristic: higher-threshold YES tends to be thinner).
2. Wait up to **200 ms** for placement confirmation.
3. Immediately place the second leg.
4. If the second leg is not filled within **2000 ms**, cancel it. Attempt a closing order on the first leg at market. If the closing order also fails (market moved, no liquidity), emit a `leg_break` SEV-2 alert and record the open position for manual operator review.

**`leg_break` execution outcome taxonomy.** Add `'leg_break'` to the monotonicity scanner position records:
- `'filled'` — both legs filled within the 2000 ms window
- `'partial'` — first leg filled; second failed; closing order placed and confirmed
- `'leg_break'` — first leg filled; second failed; closing order also failed; open directional exposure requiring manual resolution
- `'raced'` — first leg filled but price moved before second placement; total pair cost now > $1 (arb math broken); cancel first leg, record as missed opportunity

A `leg_break` position must appear in the control room as an open alert. Do not classify it as a normal trading loss and do not suppress the SEV-2 event.

#### 4.3.3.1 Payoff table

For the example violation (T_i=80°F at 40¢ YES, T_j=85°F at 45¢ YES):
Trade: buy YES T80 at 40¢ + buy NO T85 at 55¢. Total cost: 95¢.

| Scenario | YES T80 | NO T85 | Gross payout | Net |
|---|---|---|---|---|
| high > 85 (high > T_j, so also > T_i) | +$1 | $0 | $1.00 | **+5¢** |
| 80 < high ≤ 85 (> T_i, ≤ T_j) | +$1 | +$1 | $2.00 | **+$1.05** |
| high ≤ 80 (≤ T_i, so also ≤ T_j) | $0 | +$1 | $1.00 | **+5¢** |

Minimum gross payout is $1.00 in all scenarios; cost is 95¢; minimum gross edge is 5¢. The middle case (temperature between the two thresholds) is actually the highest-payout scenario. There is no losing scenario — this is the property that makes it a true arb, and it must hold for any proposed pair. See §5.2 property test requirement.

The intuition: by monotonicity, high > T_j implies high > T_i (guaranteed), and high ≤ T_i implies high ≤ T_j (guaranteed). The only uncertain outcome is T_i < high ≤ T_j, but in that case *both* legs win — not a risk.

#### 4.3.4 Acceptance criteria

1. **Unit tests pass.** Scanner correctly identifies violations on synthetic orderbook fixtures.
2. **Shadow run.** 30 days of shadow-mode scanning. Expected volume: very low (estimated 6–120 filled shadow arbs across 30 days — far too few for P&L to be a statistically meaningful gate). The **primary acceptance gates for monotonicity arb are**:
   - Detection correctness: zero payoff-table-property-test failures (the property test asserts positive net PnL in all outcome scenarios for every proposed pair; a failure means the scanner proposed a losing "arb").
   - Execution feasibility: zero `leg_break` events in shadow-mode atomic execution simulation.
   P&L gate is **not** primary. The backtest (criterion 3) is the P&L evidence source, using 90 days of historical orderbook data where volume is sufficient for meaningful statistics.
3. **Backtest on historical book data.** Using `market_price_history` (migration 0011), replay the scanner over the last 90 days. Simulated PnL must be net-of-fees positive with zero losses on filled pairs (arbs should not lose; if any do, find the bug).
4. **No leg-breakage in shadow simulation.** Simulated execution tracks whether both legs would have filled atomically within the TTL window.
5. **Operator review.** Grant reviews shadow output; approves flipping to live canary with `monotonicity_arb_shadow_only=false`. Do not auto-flip.

#### 4.3.5 Stop condition

If backtest shows any "arb" trades with realized losses, stop and investigate — the math should not produce losing arbs. Likely bugs:
- Ignoring fees in edge calculation
- Treating sequential fills as atomic
- Off-by-one in threshold ordering
- Not handling `bid=0` or `ask=0` ("no quote") markets correctly

---

## 5. Cross-Cutting Requirements

### 5.1 Respect existing conventions

- **Code style.** No new linters. Match existing code formatting and docstring patterns.
- **Dependency injection.** Every new service registers via `AppContainer`. Do not introduce module-level singletons.
- **Deterministic core.** New trading logic runs in deterministic engines, never LLM-routed. LLM scaffolding may consume new signals for transcript generation but must not influence orders.
- **Blue/green safety.** New services must respect the existing DB-lock and kill-switch semantics. Use `ExecutionService` as the only path to Kalshi write endpoints — do not add alternate execution paths.
- **Shadow-first default.** Every new strategy ships with its `shadow_only` flag defaulting to `true`. Flipping to live is a manual, operator-reviewed step.

### 5.2 Testing standards

- Unit tests ≥ 85% coverage on new modules
- Integration tests exercise the full signal→risk→execution path in shadow mode using the existing `tests/integration/` patterns
- Add at least one browser test per new control-room UI element (existing `tests/browser/` pattern)
- **Property-based tests for every paired-leg or arb strategy** (hypothesis library): for any proposed trade, assert positive net PnL across all outcome scenarios. This requirement exists because payoff math for paired positions is unintuitive and the wrong trade direction can look correct until you enumerate cases. The monotonicity scanner is the current instance; apply the same requirement to any future multi-leg strategy.
- **Payoff table in every arb design doc.** Every paired-leg strategy (monotonicity arb, or any future equivalent) must ship with an explicit payoff table in its spec and design doc showing all N outcome scenarios with gross payout and net PnL. See §4.3.3.1 for the template. Tests verify the condition; the table survives refactors by making the reasoning explicit.
- Smoke tests updated: extend `demo-smoke.yml` to cover new endpoints

### 5.3 Security requirements (baked in)

- **No new secrets.** Do not introduce additional API keys or service credentials without explicit operator approval
- **Audit trail.** Every new strategy's trade proposals and outcomes written to DB with full rationale chain, matching existing rooms/positions pattern
- **Kill switch coverage.** New strategies must be halted by the existing kill switch (`app_enable_kill_switch=true`). Add integration test confirming this.
- **Private keys.** No changes to key-handling paths. Reuse existing `LIVE_*` / `DEMO_*` flow
- **Environment verification before destructive or data-writing commands.** Every `historical-backfill` subcommand logs the target `KALSHI_ENV` and target DB URL *before* making writes. Operator confirms verbally or via `--confirm-env production` flag before any multi-day backfill proceeds. The probe that discovered the Tier 2/3 data gap also revealed that the default env is `demo`, which is safe-by-default but can surprise operators running against what they believe is production.

### 5.4 Migrations hygiene

- New migrations are numbered sequentially continuing from 0016: `20260424_0017_*`, `20260425_0018_*`, etc.
- Each migration reviewed as raw SQL (follow existing pattern in `alembic/versions/`)
- Never modify an existing migration. Forward-only.

### 5.5 Documentation deliverables

For each addition, produce:
- `docs/strategy/<addition>.md` — design doc
- README update if new CLI commands are added
- Update to `docs/kalshi-weather-bot-engineering-plan.md` reflecting new strategies
- ADR (architecture decision record) in `docs/adrs/NNN-<addition>.md` if the addition introduces a pattern worth documenting

### 5.6 Rollout process

Every addition follows the existing deploy pattern:
1. Feature-flagged off in `config.py` defaults
2. Merged to `main` behind feature flag
3. Enabled in shadow mode in `.env` on deployed host
4. Observed for required shadow period (30 days minimum for Strategy C; 7 days minimum for sigma calibration; 30 days for monotonicity arb)
5. Operator-reviewed; flag flipped to live **only after DB activation record created** (§5.8) with small caps
6. Caps raised gradually if metrics hold

### 5.7 Strategy A × C partition invariant

At most one strategy may hold an open position per `(ticker, event_date)` at any time. See §4.1.2 for the four-state decomposition table, two-sided gate requirement, and lock-reversal exit semantics. This invariant is enforced at three independent layers:

1. **Market selection** — both strategy engines' supervisor routing is two-sided: Strategy A skips tickers where `resolution_state == LOCKED_YES | LOCKED_NO` and Strategy C is enabled; Strategy C skips tickers where Strategy A holds an open position.
2. **Pre-execution re-check** — the execution pre-flight re-verifies the invariant immediately before order submission. This covers the TOCTOU window between signal generation and order placement (the two are async with a non-trivial gap).
3. **SEV-2 alert** — any attempt to violate the invariant (including one that was correctly blocked) emits a non-suppressible SEV-2 alert to the watchdog ops-event stream. Each alert requires investigation — do not rate-limit.

Add an integration test explicitly for the race condition: Strategy A fill completing while a Strategy C signal is in-flight. The re-check must block the Strategy C order and emit the alert.

### 5.8 DB activation gate

Strategy C and monotonicity arb require a DB-persisted activation record before the system will permit live execution. A config flag alone (`strategy_c_shadow_only=false`) is insufficient — the activation record proves the operator completed the full shadow review and approval process, and it is verifiable at startup independently of the config file.

**`strategy_activations` table** (add to the same migration as the relevant strategy's other tables):

```sql
CREATE TABLE strategy_activations (
    activation_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_mode        TEXT NOT NULL,       -- 'RESOLUTION_CLEANUP' | 'MONOTONICITY_ARB'
    shadow_window_id     UUID NOT NULL,       -- references the shadow window being graduated
    shadow_days_elapsed  INT NOT NULL,
    qualifying_events    INT NOT NULL,        -- events meeting shadow adequacy gate
    operator_note        TEXT,
    activated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_by         TEXT
);
```

**CLI commands:**
```bash
kalshi-bot-cli strategy-c approve --shadow-window-id <uuid>
kalshi-bot-cli monotonicity-arb approve --shadow-window-id <uuid>
```

**Startup check.** During daemon startup, for any strategy whose shadow flag is false, verify a `strategy_activations` row exists for that `strategy_mode` with `shadow_days_elapsed >= 30` and `qualifying_events >= 50`. If absent, log an error, revert to shadow mode, and emit an ops warning. This prevents a misconfigured `.env` from silently enabling live trading.

### 5.9 SigmaResolver and ResolvedSigma

The three-layer σ resolution chain is encapsulated in `src/kalshi_bot/services/sigma_resolver.py`. `SigmaResolver` is registered in `AppContainer` and injected into `WeatherSignalEngine` — `scoring.py` receives a pre-resolved `ResolvedSigma` dataclass, not raw DB queries.

**Cache semantics.** `SigmaResolver` maintains an in-process TTL cache (1 hour) with a per-key `asyncio.Lock` to prevent concurrent cache misses from triggering multiple simultaneous DB reads for the same `(station, season)` stratum. This is critical because the service runs under multiple concurrent async supervisors, not a single discrete daemon loop — "per-daemon-cycle caching" does not apply.

**`ResolvedSigma` dataclass** (in `src/kalshi_bot/core/schemas.py`):

```python
@dataclass
class ResolvedSigma:
    sigma_active: float
    sigma_source: Literal["db_fit", "yaml_anchor", "global"]
    sigma_db_fitted: float | None
    sigma_db_sample_count: int | None
    sigma_yaml_anchor: float | None
    sigma_global: float
```

All six fields are written to the signal record for every room, satisfying the §4.2.6 observability requirement. Tests in `test_sigma_resolver.py` must cover: cache TTL expiry, concurrent-caller lock (one DB read per key, not N), and correct layer selection across all eight combinations of (DB-fit present/absent, YAML present/absent, CRPS gate passing/failing).

---

## 6. What NOT to Do

Hard-stop list for the coding assistant. Any of these requires operator override:

- **Do not add market-making logic.** Explicitly excluded by `weather-temp-taker.md`. The monotonicity scanner is taker-only arb, not market-making — do not confuse the two.
- **Do not replace Layer 2 Gaussian fair-value** with ensemble/NGR model as primary. NGR may be proposed as an A/B path later (Path B), but not in Path A.
- **Do not introduce Kelly sizing** in Path A. Flat caps remain authoritative. Operator may request Bayesian Kelly as future Path B work.
- **Do not merge strategies into one code path.** Strategy C stays separate from Strategy A per the roadmap directive.
- **Do not weaken existing risk gates** to make Strategy C or monotonicity arb more tradeable. Those gates are authoritative.
- **Do not trade Strategy C live** until operator signs off explicitly in writing.
- **Do not remove or bypass shadow mode** on new strategies before their shadow period completes.
- **Do not commit test fixtures that include real private keys or real Kalshi API responses with account-identifying data.**
- **Do not modify blue/green deployment or kill-switch code paths** without explicit operator review. This is load-bearing production code.
- **Do not introduce new dependencies without justification.** Existing stack covers every need in Path A.
- **Do not add LLM calls to the new trading paths.** LLM is scaffolding; deterministic logic is authoritative.
- **Do not modify settlement-source assumptions.** Kalshi's CLI-based settlement is the authoritative source. Do not introduce alternative settlement inference.

---

## 7. Session Plan for Coding Assistant

A reasonable breakdown into Claude Code sessions. Each session should end at a reviewable state (branch pushed, PR opened, tests passing, operator can review).

Prerequisite 0 ships as its own PR and must merge before Addition 2 work begins. Do not bundle it with Addition work.

| Session | Scope | Blocking dependency | Est. duration |
|---|---|---|---|
| 1 | **Repo audit + verification memo.** No code. Produce a FOUND / NOT FOUND / AMBIGUOUS assessment for each item in the verification checklist below. Flag any deviations to operator before Session 2 begins. | None | 2–3 hours |
| 2 | **Prerequisite 0 PR** — Fix silent pagination bug in `_list_historical_markets` (§4.2.0): pass `series_ticker` param, unit + integration tests, ADR `docs/adrs/001-historical-markets-series-filter.md`. Merge and deploy before Session 3. | Session 1 operator sign-off | 2–3 hours |
| 3 | Addition 2 — σ calibration: schema migrations (`station_sigma_params`, `global_lead_factor`), `sigma_calibration.py` fitting logic (two-stage: σ_base + lead_factor, chronological holdout, rolling-origin CV for n≥200), `SigmaResolver` service + `ResolvedSigma` dataclass, `refit_station_sigma.py` script. | Session 2 merged | 4–5 hours |
| 4 | Addition 2 — resolver integration: refactor `scoring.py` to accept pre-resolved `ResolvedSigma`; wire lead_factor; update `signal.py` for lead-hours pass-through; tests; backtest validation showing CRPS improvement; docs. | Session 3 | 3–4 hours |
| 5 | **`ThresholdProximityMonitor` service** — stand-alone service with `threshold_proximity_state` DB table, write-through persistence, unit tests for tier transitions (idle/approach/near-threshold/post-peak). No Strategy C signal logic — only the adaptive polling machinery that Session 7 signal logic consumes. | Session 4 | 2–3 hours |
| 6 | Addition 1 (Strategy C) — schema migrations (`cli_reconciliation`, `strategy_c_rooms` with `buffer_at_decision_f`, `cli_station_variance`); `strategy_activations` table; `cli_reconciliation.py` backfill from historical corpus. | Session 5 | 3–4 hours |
| 7 | Addition 1 — `strategy_cleanup.py` signal engine: lock confirmation (Parts A/B/C), fair-value with flat discount, freshness + market-status gates, partition-invariant two-sided gate (§4.1.2), consumes `ThresholdProximityMonitor` via DB read. | Session 6 | 4–5 hours |
| 8 | Addition 1 — risk path extensions, container wiring, CLI commands (`shadow-c-sweep`, `strategy-c-status`, `strategy-c approve`), supervisor routing, partition-invariant pre-execution re-check + SEV-2 alert. | Session 7 | 3–4 hours |
| 9 | Addition 1 — integration tests: kill-switch coverage, stale-observation handling, divergence event simulation, partition-invariant race-condition test (A fill during C in-flight). Control room surfacing (five live metrics from §4.1.7). Counterfactual fill-rate harness. | Session 8 | 3–4 hours |
| 10 | Addition 1 — **30-day shadow run** (elapsed, not active work; operator reviews weekly). Dual gate: 30 calendar days + ≥ 50 qualifying events. Extend in 15-day increments if event count not met. | Session 9 | 30 days |
| 11 | Addition 3 — monotonicity scanner: `monotonicity_scanner.py` (5-second cadence, all-pairs with 100-pair circuit breaker, per-group suppression, `leg_break` execution taxonomy), unit + integration tests, property test, payoff-table unit test, backtest against `market_price_history`. | Session 10 merged | 4–5 hours |
| 12 | Addition 3 — **30-day shadow run** (elapsed). Primary gates: detection correctness (zero property-test failures) + execution feasibility (zero `leg_break` events). P&L is not the primary gate. | Session 11 | 30 days |
| 13 | **§5.X assumption reconciliation** — apply any spec corrections surfaced by Session 1 memo that were not incorporated at the time; update §8 open questions based on Session 1 findings; close any AMBIGUOUS items. | After Session 1 memo reviewed | 1–2 hours |
| 14 | **Bankroll scale-up evaluation** — after ≥ 30 days of live data for at least one strategy, apply `--factor 1.5` step-up if all 7 criteria in §8 are met. Record decision in `risk_limit_changes` table with `gate_report_json`. | ≥30 live days | 2–3 hours |
| 15 | **Consolidation** — docs, engineering plan update to v2.1, operator review of all three additions, sign-off commits. | Sessions 12 + 14 | 2–3 hours |

Total active engineering: ~46–60 hours across sessions. Elapsed wall time: ~90 days minimum before all three additions are live-eligible.

**Session 1 is non-optional.** The coding assistant produces a verification memo with a FOUND / NOT FOUND / AMBIGUOUS assessment for each item below. **No code is written until operator confirms the memo.** Any item assessed as NOT FOUND or AMBIGUOUS becomes a §5.X spec assumption correction (Session 13) and is flagged to operator before Session 2 begins.

| # | Assertion to verify | Expected location |
|---|---|---|
| 1 | `WeatherResolutionState` enum with `LOCKED_YES` / `LOCKED_NO` variants exists | `src/kalshi_bot/core/enums.py` |
| 2 | Stale-observation guard in weather ingestion checks observation timestamp ≤ 30 min | `src/kalshi_bot/integrations/weather.py` |
| 3 | `signal.py` tracks `(room_decision_time, target_settlement_date)` in a structured way enabling lead_hours derivation | `src/kalshi_bot/services/signal.py` |
| 4 | Latest Alembic migration is numbered 0016 with no gaps since spec was written | `alembic/versions/` |
| 5 | `best_no_ask` is a computed property (`1.0000 - best_yes_bid`), not an independent API field | `src/kalshi_bot/services/streaming.py` |
| 6 | `sigma_f_by_month` is a field on `WeatherMarketMapping` (Layer 2 YAML anchor exists) | `src/kalshi_bot/config.py` or mapping schema |
| 7 | `_MONTHLY_SIGMA_F` global fallback table exists in `scoring.py` (Layer 3) | `src/kalshi_bot/weather/scoring.py` |
| 8 | `APP_SHADOW_MODE` kill switch halts order submission end-to-end (verified by existing integration test) | `src/kalshi_bot/services/shadow.py` |
| 9 | `market_price_history` table exists (migration 0011) with orderbook snapshot columns for backtest replay | `alembic/versions/` |

---

## 8. Open Questions for Operator

Items 1–5 were open at spec v1.0 and are now settled. Items 6–8 remain open.

**Settled — do not revisit:**

1. **Strategic path**: Path A confirmed.
2. **Shadow period lengths**: 30 days per addition, with 15-day extensions if the event-count gate is not met.
3. **Bankroll scale-up parameters**: Start at existing caps. First step: `--factor 1.5` (50% increase) after ≥ 30 live days per strategy, if all 7 criteria below are met. Scale-down is permitted at operator discretion with no criteria. Each change recorded in `risk_limit_changes` with `gate_report_json`.

   **7 criteria required for +50% step:**
   1. ≥ 30 days of live data for the strategy being scaled
   2. Win rate on fills ≥ 99% (Strategy C) or net PnL positive on ≥ 75% of filled pairs (monotonicity arb)
   3. Race rate stable or declining (no rising trend in the trailing 14 days)
   4. Median realized edge ≥ 80% of modeled edge
   5. Zero `leg_break` events in the preceding 30 days (monotonicity arb only)
   6. Zero SEV-2 partition-invariant alerts in the preceding 30 days
   7. Kill-switch and shadow-mode reversion tested within the preceding 7 days

4. **Integration target**: `main` branch confirmed.
5. **Operator availability**: Grant confirmed for weekly shadow reviews.

**Still open — answer before the indicated session:**

6. **Libra employment disclosure**: Any disclosure still pending on this project? (Was flagged in earlier P&L model; assumed resolved — verify before Session 2.)

7. **CI target**: Is there a Bitbucket mirror of this repo for Libra-internal CI integration, or is CI staying on GitHub Actions? (Answer before Session 15 — affects handoff criteria.)

8. **Session 1 deviations**: Any assertions in the Session 1 verification memo assessed as NOT FOUND or AMBIGUOUS become open questions for the operator. The specific list is unknown until Session 1 completes. Answer before Session 2.

---

## 9. Appendix — File-Level Reference Map

For the coding assistant's quick navigation:

**Core trading path (do not modify in Path A):**
- `src/kalshi_bot/services/risk.py::DeterministicRiskEngine` — authoritative risk gates
- `src/kalshi_bot/services/signal.py::compute_strategy_signal` — Strategy A signal generation
- `src/kalshi_bot/services/execution.py::ExecutionService` — only write path to Kalshi
- `src/kalshi_bot/weather/scoring.py::score_weather_market` — fair-value model entry point
- `src/kalshi_bot/orchestration/supervisor.py::WorkflowSupervisor` — 12-step workflow

**Extension points (touch in Path A):**
- `src/kalshi_bot/services/container.py::AppContainer` — DI registration
- `src/kalshi_bot/core/enums.py` — new strategy modes, stand-down reasons
- `src/kalshi_bot/core/schemas.py` — new signal types
- `src/kalshi_bot/config.py::Settings` — new config flags
- `src/kalshi_bot/cli.py` — new operator commands

**Data infrastructure (reuse):**
- `src/kalshi_bot/services/historical_pipeline.py` — backfill patterns
- `src/kalshi_bot/services/historical_archive.py` — checkpoint capture
- `src/kalshi_bot/services/market_history.py` — market price history (migration 0011)

**Observability (integrate with):**
- `src/kalshi_bot/core/metrics.py` — Prometheus metric definitions
- `src/kalshi_bot/web/control_room.py` — dashboard surfacing
- `src/kalshi_bot/services/watchdog.py` — watchdog integration

**CI / smoke (extend):**
- `.github/workflows/demo-smoke.yml` — read-only demo smoke
- `.github/workflows/compose-shadow-smoke.yml` — full-stack shadow smoke
- `.github/workflows/self-improve.yml` — agent pack promotion

---

## 10. Handoff Criteria

Path A is complete when:
- **Prerequisite 0** merged and deployed: pagination bug fixed, ADR committed
- All three additions merged to `main` behind feature flags
- Strategy C has completed 30-day shadow + operator sign-off (all criteria in §4.1.7 met)
- Monotonicity scanner has completed 30-day shadow + operator sign-off
- Per-station σ calibration is live and improving CRPS in production metrics
- `ThresholdProximityMonitor` deployed and confirming near-threshold tier transitions in logs
- Engineering plan (`docs/kalshi-weather-bot-engineering-plan.md`) updated to v2.1 reflecting additions
- Dashboards surface all three new signal paths plus the five Strategy C live metrics
- Shadow and live CI smoke tests still green
- Operator holds a written record of all sign-offs

After that, Path B additions (NGR A/B, Bayesian Kelly) become viable to evaluate with a separate spec. That spec will be written based on observed performance of Path A additions — not pre-committed.
