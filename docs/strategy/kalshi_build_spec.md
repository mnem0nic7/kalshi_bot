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

This changes Edge 4 (structural arb). You cannot do the clean "6 bins sum to 100¢" scan. Instead, you can exploit **monotonicity across thresholds**: `P(high > 85)` must be ≤ `P(high > 80)` always. When the orderbook violates that (because different thresholds trade on different depths/latencies), there is a directly tradeable arb: buy YES on the higher-threshold ticker and buy NO on the lower-threshold ticker for the same city/day, capturing the probability-ordering violation.

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

#### 4.1.2 Files to touch

**New files:**
- `src/kalshi_bot/services/strategy_cleanup.py` — the Strategy C signal engine, parallel to `services/signal.py`
- `src/kalshi_bot/services/cli_reconciliation.py` — per-station CLI-vs-METAR historical variance stats
- `alembic/versions/YYYYMMDD_NNNN_strategy_cleanup.py` — new tables (see §4.1.4)
- `tests/unit/test_strategy_cleanup.py`
- `tests/integration/test_strategy_cleanup_service.py`
- `docs/strategy/strategy_c_cleanup.md` — design doc with activation criteria

**Modified files:**
- `src/kalshi_bot/core/enums.py` — add `StrategyMode.RESOLUTION_CLEANUP`; extend `StandDownReason` with cleanup-specific reasons
- `src/kalshi_bot/core/schemas.py` — extend `StrategySignal` or create `CleanupSignal` with cleanup-specific fields
- `src/kalshi_bot/services/container.py` — register `StrategyCleanupService`
- `src/kalshi_bot/orchestration/supervisor.py` — add strategy router selecting between directional (A) and cleanup (C) based on `resolution_state`
- `src/kalshi_bot/services/risk.py` — add cleanup-specific risk path with tighter caps during shadow period
- `src/kalshi_bot/config.py` — new settings: `strategy_c_enabled: bool = False`, `strategy_c_shadow_only: bool = True`, `strategy_c_min_residual_cents: int = 3`, `strategy_c_max_residual_cents: int = 10`, `strategy_c_max_position_notional_dollars: float = 50.0`
- `docs/kalshi-weather-bot-engineering-plan.md` — section 5.x added for Strategy C activation criteria
- `src/kalshi_bot/cli.py` — add `shadow-c-sweep` and `strategy-c-status` commands

#### 4.1.3 Strategy C logic

For each open weather threshold market where `resolution_state == LOCKED_YES` or `LOCKED_NO`:

1. **Confirm lock.** Pull latest ASOS 5-min observation for the station. Confirm observed max since 00:00 LST (not midnight DST — LST per `kalshi_hightemp_playbook.md` §1.2 trap) has actually crossed the threshold with margin ≥ `cli_reconciliation_buffer_degf` (default 0.5°F, per-station from historical variance table).

2. **Compute fair value.** If locked YES: `fair = 1.00 - cli_uncertainty_discount`. If locked NO: `fair = 0.00 + cli_uncertainty_discount`. The discount reflects probability that CLI QC produces a different value than the ASOS-observed max.

3. **Calculate edge.** `edge_cents = |fair - market_cents|`. Edge must be in configurable range (default: 3–10 cents; too small = fees eat it, too large = probably means you're misreading the lock).

4. **Risk gates (in addition to existing deterministic engine):**
   - Current observed max was achieved ≥ `strategy_c_min_confirmation_minutes` ago (default 30 min)
   - Station has ≤ `strategy_c_max_cli_variance_degf` historical variance (computed from `cli_reconciliation` table)
   - Time until market settlement ≥ `strategy_c_min_time_to_settlement_minutes` (default 60 min; avoid settling-soon liquidity issues)
   - No open position on the opposite side of the same ticker

5. **Sizing.** Strategy C uses its **own** size caps, much smaller than Strategy A:
   - `strategy_c_max_order_notional_dollars` (default $50)
   - `strategy_c_max_position_notional_dollars` (default $50)
   - No Kelly; flat sizing at min(cap, orderbook depth).

6. **Shadow-first.** While `strategy_c_shadow_only=true`, produce `CleanupSignal` → `TradeTicket` with `mode=shadow`; do **not** hit Kalshi write endpoints. Log the simulated decision.

#### 4.1.4 New database tables

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
CREATE TABLE strategy_c_rooms (
    room_id              UUID PRIMARY KEY,
    ticker               TEXT NOT NULL,
    station              TEXT NOT NULL,
    decision_time        TIMESTAMPTZ NOT NULL,
    resolution_state     TEXT NOT NULL,
    observed_max_at_decision REAL NOT NULL,
    threshold            REAL NOT NULL,
    fair_value_dollars   NUMERIC(10,4) NOT NULL,
    market_touch_dollars NUMERIC(10,4) NOT NULL,
    edge_cents           REAL NOT NULL,
    sized_notional       NUMERIC(10,2),
    was_shadow           BOOLEAN NOT NULL,
    outcome              TEXT,                -- 'win', 'loss', 'cancelled', 'pending'
    outcome_pnl_dollars  NUMERIC(10,4),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-station CLI variance rollup (materialized periodically)
CREATE TABLE cli_station_variance (
    station                  TEXT PRIMARY KEY,
    sample_count             INT NOT NULL,
    mean_delta_degf          REAL NOT NULL,
    stddev_delta_degf        REAL NOT NULL,
    p95_abs_delta_degf       REAL NOT NULL,
    last_refreshed_at        TIMESTAMPTZ NOT NULL,
    note                     TEXT
);
```

Populate `cli_reconciliation` by backfilling across the existing `historical_replay` corpus (reuse `services/historical_pipeline.py` patterns). Populate `cli_station_variance` via a weekly rollup job.

#### 4.1.5 Acceptance criteria

1. **Unit tests pass.** Coverage ≥ 85% on `strategy_cleanup.py` and `cli_reconciliation.py`.
2. **Shadow-C runs for 30 calendar days** producing ≥ 30 shadow-mode `strategy_c_rooms` rows across all 20 cities.
3. **Shadow outcomes track expected distribution:** simulated win rate ≥ 95% (this is near-deterministic edge; <95% indicates a bug or bad calibration).
4. **Per-station CLI variance populated.** `cli_station_variance` has rows for all 20 stations with sample_count ≥ 60 each (requires 60+ days of historical CLI data — use `historical-backfill weather-archive` and `historical-backfill settlements` first).
5. **Dashboard row added.** Control room `/api/control-room/summary` surfaces Strategy C shadow metrics: trades/day, avg edge, shadow win rate, per-station variance status.
6. **Operator review.** Grant reviews 30 days of shadow output and explicitly approves flipping `strategy_c_shadow_only=false` for a live canary. Do not flip it programmatically.

#### 4.1.6 Stop condition

Do not promote Strategy C out of shadow mode until:
- 30 days of shadow operation with ≥ 95% simulated win rate
- At least one full CLI/METAR divergence event captured and analyzed (i.e., a day where CLI differed from ASOS by ≥ 1°F — study what Strategy C would have done)
- Operator explicitly signs off in writing (commit a dated approval note to `docs/strategy/strategy_c_cleanup.md`)

---

### 4.2 Addition 2: Per-Station σ Calibration

**Playbook reference:** `kalshi_technical_deepdive.md` §1.5 (stratified bias correction).
**Repo baseline:** `src/kalshi_bot/weather/scoring.py` uses a single seasonal σ table regardless of station.

**Thesis:** KNYC (Central Park) and KDEN (Denver) have fundamentally different diurnal temperature behavior. A single `sigma_f` calibrated on "all cities averaged" is suboptimal for both. The fair-value model improves measurably from station-specific σ, without requiring philosophical change to the fair-value approach.

**Why it fits the repo's philosophy:** Pure improvement to existing Layer 2 Gaussian model. No new strategy. No new sizing framework. Works entirely within their chosen probabilistic approach.

#### 4.2.1 Architecture: three-layer σ resolution

σ is resolved at runtime by walking three layers in order, from most-specific to least:

```
Layer 1 (DB-fit, per lead):   station_sigma_params (station, season, lead_hours) — when ≥200 samples
Layer 2 (YAML anchor, monthly): sigma_f_by_month in weather_markets YAML — operator-specified cells only
Layer 3 (global fallback):    _MONTHLY_SIGMA_F in scoring.py — always present
```

**Resolution logic:**
1. If a DB-fit row exists for `(station, season, lead_bucket)` with `sample_count ≥ 200`, use it.
2. Else if a YAML cell exists for `(station, month)`, use it — applies across all lead buckets.
3. Else use global `_MONTHLY_SIGMA_F[month]`.

This is **not** a simple "YAML wins" hierarchy. YAML provides an operator anchor for cells where domain knowledge is strong; DB-fit provides per-lead refinement as data accumulates. YAML does not block DB-fit from winning — DB-fit wins when it has sufficient evidence.

**YAML is sparse by design.** Operators specify only the `(station, month)` cells where they have genuine prior knowledge. Cells not specified in YAML fall through to DB-fit or global. Over-specifying YAML (e.g., entering DJF values for a station where you have no real prior) prevents DB-fit from correcting bad guesses.

**Example:** SFO JJA marine layer suppresses daily-high variability to ~2°F. That is documented, repeatable phenomenon — strong YAML prior. SFO DJF is dominated by frontal variability — no strong prior, leave it to data. See §4.2.7 for YAML hygiene requirements.

**This design is the end state, not transitional.** Layer 2 (YAML) does not get deprecated as data grows. Operator knowledge is genuinely better than data during climate regime shifts, model updates, or station relocations. The steady state is: most cells resolved by DB-fit, operator YAML as escape hatch for cells where domain knowledge beats statistics.

#### 4.2.2 Scope

Implement the three-layer lookup. The current codebase already has Layer 2 (shipped as `sigma_f_by_month` on `WeatherMarketMapping`) and Layer 3 (global `_MONTHLY_SIGMA_F`). This addition adds Layer 1 (DB-fit) and wires the full resolution chain.

#### 4.2.3 Files to touch

**New files:**
- `src/kalshi_bot/weather/sigma_calibration.py` — fitting logic, `resolve_sigma()` function
- `alembic/versions/YYYYMMDD_NNNN_station_sigma.py` — `station_sigma_params` table
- `tests/unit/test_sigma_calibration.py`
- `scripts/refit_station_sigma.py` — manual refit trigger
- `docs/strategy/sigma_calibration.md` — design doc

**Modified files:**
- `src/kalshi_bot/weather/scoring.py` — `sigma_f_for_mapping()` extended to accept `lead_hours` and consult DB-fit layer; falls back to existing YAML/global chain
- `src/kalshi_bot/services/signal.py` — pass station identifier and forecast lead hours into scoring
- `src/kalshi_bot/config.py`:
  - `sigma_calibration_enabled: bool = True`
  - `sigma_calibration_refit_cadence_days: int = 7`
  - `sigma_db_min_samples: int = 200` — threshold for DB-fit to win over YAML anchor
  - `sigma_global_min_samples: int = 100` — threshold for DB-fit to win over global fallback when no YAML present
- `src/kalshi_bot/services/daemon.py` — register weekly refit task
- `docs/examples/weather_markets.example.yaml` — audit YAML overrides for sparsity (see §4.2.7)

#### 4.2.4 Fitting methodology

For each `(station, season_bucket, lead_hours_bucket)`:

1. Pull all matched pairs `(forecast_high_f, crosscheck_high_f)` from `historical_weather_snapshots` joined to `historical_settlement_labels`
2. Require ≥ `sigma_global_min_samples` (100) to beat global; require ≥ `sigma_db_min_samples` (200) to beat YAML anchor
3. Compute residuals `r_i = crosscheck_high_f_i - forecast_high_f_i`
4. Fit `sigma_f` = std(r_i), `mean_bias_f` = mean(r_i)
5. Persist to `station_sigma_params` with version timestamp

**Bias correction is active when DB-fit wins.** When the DB-fit row is the active layer, the fair-value formula becomes:
```
bias_corrected_delta_f = delta_f - mean_bias_f(station, season, lead)
sigma_f = sigma_f(station, season, lead)
P = Φ(bias_corrected_delta_f / sigma_f)
```
When YAML or global is the active layer, bias correction is not applied (no fitted bias available).

**Note on lead-hours data:** `historical_weather_snapshots.asof_ts` and `historical_settlement_labels.settlement_ts` allow computing approximate lead time. Lead time is not currently a first-class column; add it to the join query rather than the schema.

#### 4.2.5 New table

```sql
CREATE TABLE station_sigma_params (
    station            TEXT NOT NULL,
    season_bucket      TEXT NOT NULL,         -- 'DJF', 'MAM', 'JJA', 'SON'
    lead_hours_bucket  TEXT NOT NULL,         -- '0-6', '6-12', '12-24', '24-48', '48+'
    sigma_f            REAL NOT NULL,
    mean_bias_f        REAL NOT NULL,
    sample_count       INT NOT NULL,
    fitted_at          TIMESTAMPTZ NOT NULL,
    version            TEXT NOT NULL,         -- timestamp-based
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (station, season_bucket, lead_hours_bucket, version)
);

CREATE INDEX station_sigma_active ON station_sigma_params (station, season_bucket, lead_hours_bucket)
  WHERE is_active = TRUE;
```

Note: column renamed from `month_bucket` → `season_bucket` to reflect actual 4-bucket grouping (DJF/MAM/JJA/SON). The YAML layer remains month-keyed (finer-grained operator control); the DB layer uses seasons (coarser, more samples per stratum).

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

1. **Unit tests pass.** Fit on synthetic data with known σ recovers within 5%. Resolution logic unit-tested: correct layer selected for each of the eight combinations of (DB-fit present/absent, YAML present/absent, sample count above/below threshold).
2. **Fit populated.** Initial refit produces rows for all 20 stations × 4 seasons × 5 lead buckets = 400 strata (fewer if some strata lack 100 samples; those fall back to global).
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

**Thesis:** For a single station/day, `P(high > T)` must be monotonically non-increasing in T. If the market has `KXHIGHNY-26APR22-T85` trading at 45¢ YES and `KXHIGHNY-26APR22-T80` trading at 40¢ YES simultaneously (both for the same day, same station), that's an arbitrage: buy YES on T85, sell YES on T80 (or equivalently buy NO on T80). By monotonicity, if "high > 85" resolves YES, "high > 80" must also resolve YES; if "high > 80" resolves NO, "high > 85" must also resolve NO. The pair is risk-free.

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
- `src/kalshi_bot/services/daemon.py` — schedule scanner at 60-second cadence
- `src/kalshi_bot/config.py` — `monotonicity_arb_enabled: bool = False`, `monotonicity_arb_shadow_only: bool = True`, `monotonicity_arb_min_net_edge_cents: int = 2`, `monotonicity_arb_max_notional_dollars: float = 25.0`, `monotonicity_arb_max_proposals_per_minute: int = 5`
- `src/kalshi_bot/core/enums.py` — `StrategyMode.MONOTONICITY_ARB`
- `src/kalshi_bot/cli.py` — `monotonicity-scan --once` command

#### 4.3.3 Scanner logic

On each tick:

1. Query all open `KXHIGH*` markets grouped by `(station, event_date)`
2. For each group, sort by threshold ascending
3. Compute implied probabilities from bid/ask for each threshold
4. Walk the sequence; flag any `T_i < T_j` where `bid(T_i) < ask(T_j) - 2*fees - min_edge_cents` (i.e., you can buy NO on the lower-threshold cheap and buy YES on the higher-threshold above its fair implied value — violating monotonicity)
5. For each violation, generate a paired proposal: buy YES on `T_j` at ask + buy NO on `T_i` at (100 - bid)
6. Pass through existing risk engine with `StrategyMode.MONOTONICITY_ARB`
7. In shadow mode, log proposals; in live mode, emit paired orders **as a single atomic unit** (both or neither)

**Key: atomic execution.** If only one leg fills, you have directional risk, not arb. Either use Kalshi's multi-order batch endpoint if available, or place the less-liquid leg first with a TTL and cancel if the other leg can't fill within 1 second.

#### 4.3.4 Acceptance criteria

1. **Unit tests pass.** Scanner correctly identifies violations on synthetic orderbook fixtures.
2. **Shadow run.** 30 days of shadow-mode scanning produces a distribution of proposals. Expected volume: low (maybe 0–3 per day across 20 cities); this is a rare-opportunity strategy.
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
- Property-based tests for monotonicity scanner (hypothesis library) — scanner should never propose a trade that isn't mathematically valid
- Smoke tests updated: extend `demo-smoke.yml` to cover new endpoints

### 5.3 Security requirements (baked in)

- **No new secrets.** Do not introduce additional API keys or service credentials without explicit operator approval
- **Audit trail.** Every new strategy's trade proposals and outcomes written to DB with full rationale chain, matching existing rooms/positions pattern
- **Kill switch coverage.** New strategies must be halted by the existing kill switch (`app_enable_kill_switch=true`). Add integration test confirming this.
- **Private keys.** No changes to key-handling paths. Reuse existing `LIVE_*` / `DEMO_*` flow

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
5. Operator-reviewed; flag flipped to live with small caps
6. Caps raised gradually if metrics hold

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

| Session | Scope | Est. duration |
|---|---|---|
| 1 | Read repo + reference docs; no code. Produce an assessment memo identifying any gaps between this spec and actual repo state. | 1 hour |
| 2 | Addition 2 — Per-station σ calibration (most isolated, lowest risk, quick win) | 3–4 hours |
| 3 | Addition 2 — tests, backtest validation, docs | 2–3 hours |
| 4 | Addition 1 (Strategy C) — schema migration + `cli_reconciliation.py` backfill | 3–4 hours |
| 5 | Addition 1 — `strategy_cleanup.py` signal engine | 4–5 hours |
| 6 | Addition 1 — risk path, container wiring, CLI commands | 3–4 hours |
| 7 | Addition 1 — integration tests + control room surfacing | 3–4 hours |
| 8 | Addition 1 — 30-day shadow run (elapsed, not active work) | 30 days |
| 9 | Addition 3 — monotonicity scanner + tests | 4–5 hours |
| 10 | Addition 3 — 30-day shadow run (elapsed) | 30 days |
| 11 | Consolidation — docs, engineering plan update, operator review | 2–3 hours |

Total active engineering: ~25–35 hours across sessions. Elapsed wall time: ~90 days minimum before all three additions are live-eligible.

**Session 1 is non-optional.** The coding assistant produces a memo confirming:
- Repo state matches this spec's assumptions
- No breaking changes landed since this spec was written
- Specific migration numbers and file paths are still valid
- Any deviations flagged to operator before coding begins

---

## 8. Open Questions for Operator

Answer before Session 2:

1. **Strategic path (A / B / C).** Default A. Confirm.
2. **Shadow period lengths.** Default 30 days per addition. Confirm or adjust.
3. **Bankroll scale-up** for Strategy C and monotonicity arb once live-eligible. Suggest: start at $100 notional cap per addition, step up only after 30 days of green metrics.
4. **Is the repo's `main` branch the right integration target,** or does the operator use a personal fork with different conventions?
5. **Operator availability for 30-day shadow reviews.** Strategy C output needs at least weekly operator eyes. Confirm Grant (or a backup) can commit to this.
6. **Any Libra employment disclosure still pending** on this project? (Was flagged in earlier P&L model; assumed resolved, verify.)
7. **Is there a Bitbucket mirror** of this GitHub repo for Libra-internal CI integration, or is CI staying on GitHub Actions?

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
- All three additions merged to `main` behind feature flags
- Strategy C has completed 30-day shadow + operator sign-off
- Monotonicity scanner has completed 30-day shadow + operator sign-off
- Per-station σ calibration is live and improving CRPS in production metrics
- Engineering plan (`docs/kalshi-weather-bot-engineering-plan.md`) updated to v2.1 reflecting additions
- Dashboards surface all three new signal paths
- Shadow and live CI smoke tests still green
- Operator holds a written record of all sign-offs

After that, Path B additions (NGR A/B, Bayesian Kelly) become viable to evaluate with a separate spec. That spec will be written based on observed performance of Path A additions — not pre-committed.
