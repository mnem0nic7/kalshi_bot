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

## Staged plan
- **Stage 0 (done):** evaluate + map + research.
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
