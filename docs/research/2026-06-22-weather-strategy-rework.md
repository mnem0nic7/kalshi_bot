# Weather strategy rework — deep-research-grounded design (2026-06-22)

**Status:** Design. Operator: "rework the weather strategy with deep research and intelligence";
also "get weather live" + "weather model training in the dedicated container" (the deployment
goals that follow the rework). Weather is currently FULLY DISABLED (crypto-only since 2026-05-20).

**Evaluation that frames this:** weather has **never traded live — 0 fills, any env, ever**. It
ran as a research/signal system (settlement crosscheck agrees with Kalshi's actual result
**916/918 = 99.8%** — the *forecast/settlement-tracking* is excellent), then was disabled as
overhead. So the question is NOT "is our forecast good" (it is) — it's "where is **tradeable
edge after fees**", the same wall crypto hit. Architecture map: `services/signal.py`
(WeatherSignalEngine), `forecast/` (Gumbel+KDE probability_engine, ensemble_fuser, online
calibrator, CatBoost learned_head, source_health), `weather/scoring.py` (high-so-far nowcast,
line ~610), `services/weather_prediction.py` (intraday logistic + residual linear training,
manual CLI only — no continuous loop), `integrations/weather.py` + `forecast_archive.py` (NWS +
Open-Meteo).

## Deep research verdict (105-agent run, 19 claims confirmed / 6 refuted)

### ⭐ The headline (and a correction to the naive thesis)
**There is NO verified evidence of a forecast-vs-market mispricing edge.** Every specific
"we beat the temperature market" claim the research surfaced was REFUTED 0-3:
- "market over-prices uncertainty 1.27×" ✗
- "boosted-tree edge, Diebold-Mariano p=0.006" ✗
- "backtest 49.8% return over ~1000 markets" ✗

So do **not** build a strategy that tries to out-forecast the market price. The verified,
fee-survivable edge is **structural and intraday**: the **high-so-far lock-in**.

### Confirmed (3-0) findings the rework is built on
1. **Edge is observation-based, not forecast-based.** Forecast skill is *worst* exactly during
   the afternoon-peak window (when the daily HIGH is set) and *best* near the morning minimum
   (HRRR 2-m errors lowest ~sunrise, grow into afternoon convective mixing). → **Trade the
   observed high-so-far, not the afternoon forecast.** Once observed high-so-far ≥ strike, YES is
   deterministic; the market is slower to fully reprice that certainty.
2. **Peak timing VARIES — do not hard-code it.** The claim "daily max occurs at a fixed
   mid-afternoon time" was REFUTED. The diurnal peak shifts with season/cloud/location, so the
   "remaining heating potential" must be modeled adaptively (HRRR hourly + conditional
   climatology), NOT a fixed 3pm clock. (Our current code has no peak model at all — just
   `max(forecast, high_so_far)`.)
3. **Raw NWP/HRRR temperature is biased + underdispersive — post-processing is MANDATORY.**
   No raw model output is a usable P(high≥T). Use **EMOS/NGR**: predictive Gaussian with
   mean = bias-corrected affine of member forecasts, variance = affine of ensemble variance →
   `P(high≥T) = 1 − Φ((T − μ)/σ)`. QRF gives near-ideal coverage as an alternative.
4. **Calibration is the gate.** Recalibrate binary probabilities with **isotonic / CORP-PAV**
   (no tuning; CORP gives a Brier-decomposition reliability diagram). Evaluate with proper
   scoring rules (log score, CRPS) and PIT — but a uniform PIT is **necessary, not sufficient**
   (a biased forecaster can pass it), so pair it with reliability + sharpness.
5. **Official guidance sources:** **NBM** (National Blend of Models) is the calibrated official
   guidance baseline; **HRRR** is the hourly intraday-updating model. Station-specific predictors
   (elevation/lat/microclimate) measurably help — relevant for Central-Park-vs-airport quirks.
6. **Fees/sizing (from our own rate·p(1−p) math + crypto experience; research supports
   indirectly, doesn't independently quantify):** concentrate trades where **p is extreme**
   (late-day near-certainty in the tails) where the fee →0; avoid mid-day afternoon-peak
   forecasts (least reliable AND p≈0.5 = max fee). Fractional Kelly on the rare high-certainty
   entries.

## The reworked strategy (design)

**Thesis: harvest the intraday high-so-far lock-in the market is slow to price; do NOT chase
forecast-vs-market mispricing (unverified). Trade the tails where fees vanish. Gate shadow→live
exactly like crypto, since it has never traded live.**

1. **Intraday terminal-certainty engine (the edge — new):** replace the crude
   `max(forecast, high_so_far)` (scoring.py:610) with `P(daily_high ≥ strike | high_so_far,
   current_temp, hour, adaptive diurnal-peak model, HRRR hourly trajectory, cloud/wind)`.
   - Locked-YES: observed high-so-far ≥ strike → P≈1.
   - Locked-NO: peak passed AND adaptive remaining-heating can't reach strike → P≈0.
   - The remaining-heating model is HRRR-hourly + conditional climatology (NOT fixed-clock).
2. **EMOS/NGR forecast layer (supporting, calibrated):** post-process NBM/HRRR/ensemble into a
   calibrated Gaussian; never use raw output. This feeds the pre-peak probability and σ.
3. **Calibration gate (mandatory):** isotonic/CORP recalibration per station; only act when
   calibrated AND (for any forecast-based trade) the model-vs-market gap exceeds a fee-aware
   threshold — but expect most trades to be lock-in/tail, not forecast-gap.
4. **Fee-aware, tail-only entry + fractional Kelly:** trade where p∈(near 0 or near 1); skip the
   mid-band. Same engine discipline as crypto.
5. **Gate shadow→live (mandatory — never traded live):** re-enable **training in the dedicated
   container** (build EMOS + intraday terminal model + per-station calibration) → run **shadow**
   to accrue an OOS track record → a **replay/backtest gate on fee-adjusted OOS profit +
   calibration** → **live only where it clears**. Mirror `_crypto_select_champion` + replay gate.

## Honest ceiling
- **No verified forecast-mispricing edge exists** — if the market prices the high-so-far lock-in
  efficiently, there is little to harvest. This MUST be proven in shadow before any live money.
- The edge, if real, is **late-day, tail-priced, low-frequency** (few high-certainty entries/day)
  — same small-bankroll, fee-constrained regime as crypto.
- Do not re-enable live trading until the shadow + gate prove fee-survivable OOS profit.

## Data assessment (2026-06-22) — the high-so-far input is missing but backfillable
`historical_weather_snapshots`: **1,380 rows, 2026-05-29→06-21, 20 stations, ONE source_kind**
(`external_forecast_archive_weather_bundle` = Open-Meteo *forecast* archive). **Zero NWS intraday
*observation* snapshots** (`checkpoint_archived_weather_bundle`) — so there is **no observed-high-
so-far time series**, which is the ENTIRE verified edge. Current data is also too thin/single-source
to train a meaningful intraday model (needs ≥500 train + ≥100 holdout; the high-so-far feature is
absent). `historical_settlement_labels`: 918 rows, recent (06-21), 99.8% crosscheck — labels are fine.

**KEY (more tractable than crypto):** the high-so-far signal needs intraday HOURLY temperatures, and
**Open-Meteo serves historical hourly temperature** → observed-high-so-far is **reconstructable by
backfill** (no multi-week forward wait, unlike crypto's multi-venue gap). So the data blocker is
removable now. For LIVE, real-time intraday NWS observations must be re-enabled (weather obs
collection is currently off).

## Backtest feasibility (2026-06-22) — CORRECTED: market-edge needs shadow; only forecast-quality is offline
`historical_market_snapshots` has ~3,400 KXHIGH* price snapshots — BUT a date-alignment check
showed **ALL 3,634 were captured AFTER the market settled (0 on the market day, 0 before).** They
are post-hoc archival prices, NOT live intraday quotes, so the **fee-edge-vs-market backtest is NOT
offline-runnable** (no intraday/pre-settlement price series). Like crypto, the market-edge go/no-go
requires **SHADOW** to collect live intraday KXHIGH quotes first. (Earlier "offline fee-edge
feasible" was wrong — corrected.)
**What IS offline-runnable: FORECAST-QUALITY validation** (no market prices needed) — fetch
observed hourly (Open-Meteo) → reconstruct_high_so_far → terminal_high_ge_probability at intraday
checkpoints → Brier/calibration/sharpness vs the 918 settlements, by hour. This proves whether the
model is sound and quantifies the lock-in (sharpness should rise through the day). If it fails, weather
is dead cheaply; if it passes, proceed to shadow for the market-edge question. The four rework
primitives are shipped+tested (commits 6262a31, c5e9297, 8badb40); the backtest harness (ticker→
station/strike parse + station-coord map + the eval loop) is the next build.

## ⭐ Forecast-quality backtest RESULT (2026-06-22) — model mis-specified; two structural bugs found
Ran `scripts/weather_forecast_quality_backtest.py` (Open-Meteo observed hourly + 918 settlements,
20 cities). **The naive terminal-≥-strike model FAILED — confidently wrong:** YES base rate **0.060**;
baseline Brier (predict base rate) **0.0568**; our model Brier **0.264 @08:00 rising to 0.410 @18:00**
with sharpness rising 0.29→0.50. It gets *more confident and less accurate* through the day — i.e. it
increasingly predicts YES (high-so-far ≥ strike in our data) while markets settle NO. Two structural
causes (both fixable, the backtest's whole point — caught before any live money):
1. **SETTLEMENT-BASIS MISMATCH (weather twin of crypto's CF-index issue):** the market map gives each
   city a separate `daily_summary_station_id` (the official NOAA climate station Kalshi settles on,
   e.g. NYC `USW00094728`) DISTINCT from the forecast `station_id`/Open-Meteo gridpoint we fetched.
   Our observed high can exceed the strike while the OFFICIAL high didn't → confidently-wrong YES.
   Must fetch/settle against the daily-summary station, not the gridpoint.
2. **STRIKE SEMANTICS:** only ~2 strikes/day, 7°F apart, ~0.12 YES/day → KXHIGH "T{n}" is NOT a simple
   "≥n" (that would yield a far higher base rate); likely tail/extreme thresholds or buckets.
   `terminal_high_ge_probability` assumed plain ≥strike and must be reformulated to the true contract.
**The high-so-far PRIMITIVES remain valid;** the terminal model + the data basis are what's wrong.

### Both fixes CONFIRMED via Kalshi docs (2026-06-22)
- **Fix #1 settlement source:** Kalshi weather markets settle on the **NWS Daily Climate Report**
  (official `daily_summary_station`, local STANDARD time) — NOT the Open-Meteo gridpoint we fetched.
  Re-source the observed/high-so-far series to the NWS Daily Climate Report (or its station feed).
  (sources: help.kalshi.com/en/articles/13823837-weather-markets;
  kalshi-public-docs.s3.amazonaws.com/contract_terms/NHIGH.pdf)
- **Fix #2 contract shape:** daily-high markets are **bracketed — 6 brackets, the middle 4 are 2°F
  wide, the 2 edges are open-ended (≤lo / ≥hi)**; "greater than" strikes are strict. So the terminal
  probability must be a BRACKET probability `P(high∈[a,b)) = Φ-terms` (difference of two thresholds),
  not a single ≥strike. Confirm the exact KXHIGH "T{n}" bracket mapping from the KXHIGH contract-terms
  PDF before reformulating.
**Remaining (multi-step):** source historical NWS Daily Climate Report highs per station →
reformulate terminal probability to brackets → re-backtest → (if it passes) shadow for market-edge →
gate → live. Weather is NOT viable until the re-backtest passes.

### Fix #1 isolated (2026-06-22) — ASOS alone does NOT fix it; fix #2 is dominant
Built `integrations/asos_archive.py` (IEM ASOS official-station hourly, fix #1's substrate) and
re-ran the backtest with ASOS data (6 cities, 276 markets, threshold model): **still confidently
wrong — Brier 0.327→0.486 (08→18h), base rate 0.091, baseline 0.082.** So the settlement-source
swap alone does NOT calibrate the model. The dominant bug is **fix #2 (contract shape)**: a
≥-threshold model on a BRACKETED contract is structurally wrong — when high-so-far clears the lower
strike the threshold model says YES, but the bracket settles YES only if the high lands IN the band;
summer highs overshoot into a higher bracket → confidently-wrong YES, ~6–9% base rate.
`terminal_high_in_bracket_probability` (fix #2, shipped) is the right form.
**BLOCKER to the bracket re-backtest:** it needs each market's exact bracket bounds [lo,hi), i.e. the
precise KXHIGH "T{n}" → bracket mapping (the contract-terms detail not fully retrieved) AND the full
per-day strike ladder (our settlement labels hold only ~2 strikes/day, not all 6 brackets). Next:
pull the exact KXHIGH contract spec + a complete strike ladder (Kalshi API), map T{n}→[lo,hi),
re-backtest with the bracket model on ASOS data. Until then the bracket model is built but unvalidated.

## Staged plan
- **Stage 0 (done):** evaluate + map + research + data assessment + feasibility + first backtest (failed informatively).
- **Stage 0.9 (next): the BACKTEST** — build the harness over the 4 shipped primitives; answer
  "does the high-so-far lock-in beat the KXHIGH market after fees?" This is the weather go/no-go.
- **Stage 0.5 (data, next — requires NEW CODE, not turnkey):** the high-so-far substrate does not
  exist and is not recoverable from existing tables: `raw_weather_events` and
  `weather_bootstrap_events` are **both EMPTY (0 rows)**, so `historical-backfill weather-archive`
  (which only repackages raw events) has nothing to work with; and our `OpenMeteoForecastArchiveClient`
  hits the *single-runs* (forecast) API, not the *observed* archive. To get observed hourly temps for
  high-so-far reconstruction, build a small fetch against **Open-Meteo archive-api**
  (`archive-api.open-meteo.com/v1/archive`, `hourly=temperature_2m`) per station/date → reconstruct
  observed-high-so-far per market day → store as the training/backtest substrate. Bounded code build
  (~1 new client method + a reconstruction pass), mind host I/O. For LIVE, separately re-enable
  real-time NWS observation collection.
- **Stage 1 (code, TDD):** intraday terminal-certainty model (adaptive diurnal peak + HRRR
  hourly) replacing the `max()`; EMOS/NGR calibrated layer; isotonic/CORP per-station calibration.
- **Stage 2 (dedicated container):** re-enable weather training in the trainer (build the models);
  add a weather train loop or nightly, analogous to crypto.
- **Stage 3 (shadow):** run weather in shadow (prediction enabled, execution shadow) to accrue OOS.
- **Stage 4 (gate→live):** replay/backtest gate on fee-adjusted OOS profit; flip live per-station
  only where it passes. Restore flags per `docs/operations/weather-disabled-2026-05-20.md`.

## Sources (verified)
HRRR diurnal skill: journals.ametsoc.org/view/journals/wefo/37/8/WAF-D-21-0130.1.xml;
royalsocietypublishing.org/doi/10.1098/rsta.2020.0099. EMOS/NGR: stat.washington.edu/raftery/
Research/PDF/Gneiting2005.pdf; journals.ametsoc.org/view/journals/mwre/143/3/mwr-d-14-00210.1.xml.
Calibration/CORP + proper scores: Gneiting/Raftery probabilistic-forecasting literature (fetched).
