# Empirical Calibration Study: Live Crypto 15m `prediction_yes`

**Date:** 2026-06-14
**Scope:** production, 15m, 3,157 settled markets / 102,570 labeled decisions, 2026-06-05 → 2026-06-14.
**Method:** lightweight on-host analysis (no feature-store materialize, no model re-scoring — the heavy path that caused the 2026-06-14 host-OOM incident). Labels derived by joining `crypto_decision_outcomes.prediction_yes` to per-market settlement (`bool_or(settlement_result='yes')`) in `crypto_market_snapshots`, server-side aggregated. Recalibration shootout is leak-free with a **market-level** time split (first 70% of markets by first decision time → train; last 30% → test; no market straddles the boundary).

> This closes the calibration gap the external research flagged as unanswerable from outside literature ([the external report](2026-06-14-external-research-prediction-market-trading.md) — "No claim on calibration methodology survived verification … needs a dedicated empirical study on our own settlement data"). Answer, on our data: **our live calibration is broken at the tails, and isotonic recalibration fixes it.**

## Headline

The live `prediction_yes` (which resolves to `calibrated_probability or raw_probability` at [crypto/services.py:7408](../../src/kalshi_bot/crypto/services.py#L7408)) **discriminates but is badly miscalibrated — severely overconfident at the tails, and catastrophic near the strike.** A post-hoc isotonic remap fit on our own settlement data cuts overall ECE 3.4× and LogLoss 1.6×.

## Evidence

### 1. Live reliability (all 102,570 decisions, 10-bin)
Mid bins track; **extremes (which hold ~60% of the mass) are wildly over-spread:**

| bin | n | mean_pred | empirical |
|---|---|---|---|
| [0.0–0.1) | 34,507 | 0.017 | **0.356** |
| [0.2–0.7) | ~26k | tracks (e.g. 0.55→0.50, 0.64→0.62) | ✓ |
| [0.8–0.9) | 4,692 | 0.849 | 0.646 |
| [0.9–1.0) | 27,394 | 0.979 | **0.701** |
| OVERALL | 102,570 | 0.467 | 0.490 (Brier **0.284**) |

Brier 0.284 is **worse than a constant 0.5** (0.25): confident-but-wrong tails dominate.

**Confounds ruled out:** (a) sign — yes-side selections avg `prediction_yes`=0.776, no-side=0.189, so it is genuinely P(YES); (b) within-market autocorrelation + early-vs-late pooling — collapsing to the **last decision per market** (3,157 independent, near-expiry points) still shows ~2%→26% and ~98%→76% with Brier 0.235. The miscalibration is real and stable, not an artifact.

### 2. Leak-free recalibration shootout (test = 76,095 held-out rows)

| method | Brier | ECE | LogLoss | near-strike Brier | near-strike ECE |
|---|---|---|---|---|---|
| raw (live output) | 0.2657 | 0.1990 | 1.0405 | 0.4662 | 0.4720 |
| **isotonic** | **0.2231** | **0.0576** | **0.6385** | 0.2687 | 0.1455 |
| platt (logistic) | 0.2291 | 0.0815 | 0.6506 | 0.2614 | 0.1320 |
| temperature (T=5.0) | 0.2250 | 0.0902 | 0.6426 | 0.2824 | 0.2009 |
| beta | 0.2310 | 0.0629 | 0.6546 | **0.2593** | 0.1396 |
| ensemble(iso+platt+beta) | 0.2271 | 0.0727 | 0.6467 | 0.2629 | 0.1358 |

- **Isotonic wins overall** (lowest Brier, ECE, LogLoss). ECE 0.199→0.058; LogLoss 1.04→0.64.
- **Near the strike (price∈[0.40,0.60], n=474)** the raw live output is worse than a coin flip (ECE 0.47); **Platt/beta** recover it best (nsBrier ≈0.26). This thin slice has wide CIs — treat the *direction* (raw is broken near-strike) as solid, the method ranking there as indicative.
- Tail remap (test): raw mean 0.017 → isotonic 0.378 (actual 0.345); raw mean 0.979 → isotonic 0.646 (actual 0.736). Isotonic learned the overconfidence from past data and corrects forward.

## Why this matters (mechanism)

Edge = `prediction_yes` vs market price. Overconfident tails **manufacture phantom edge**: a "98% YES" that is really 74% looks like large edge versus an 0.85 market but is actually negative. This is a concrete driver of the −$111 P&L / 66%-win-rate paradox and the "extreme-price entries cause 85% of losses" finding (`project_trading_behavior`). It also rhymes with the known bucket-matrix bug (calibration built from the wrong model; commit 9f746ea, gate disabled / image rebuild pending) — this study is consistent with the live calibration stage simply **not working**.

## Recommendation

1. **Fix the in-pipeline calibrator, don't stack a second one.** The shootout proves calibration is the lever and **isotonic** is the method overall; but the correct implementation is to repair the pipeline's calibration stage (likely the bucket-matrix / isotonic step) so `calibrated_probability` is actually calibrated — not to bolt a second remap onto a broken first one.
2. **Consider a near-strike-specific calibrator** (Platt or beta) for price∈[0.40,0.60], where isotonic is edged out and the raw output is worst.
3. **Promote the definitive study into the trainer** (now mem-capped at 32g — the safe home): run the same shootout on the **raw uncalibrated model score** (from `decision_traces.trace` or feature-store re-scoring) across the **full history**, with **per-asset** and **intraday-vol-regime** slices, and a rolling/online refit cadence. Decide method per (asset, freq, near-strike) cell.
4. **Re-check edge-gate thresholds after recalibration** — once probabilities are honest, the phantom edge at the tails disappears and the band-pass may need re-tuning (via `autonomous_gate_tuning`, the sole threshold authority — not manual `.env` edits).

## Caveats

- 15m only, 9-day window, production. Short.
- Recalibrated the already-"calibrated" live output (a second layer) because raw scores aren't persisted in `crypto_decision_outcomes` (payload holds only `room_id`); this proves the lever and ranks methods but is not the production implementation.
- Train/test split is row-unbalanced (26k train / 76k test) because per-market decision volume grew over the window; still leak-free, train ample for 1–2-param + isotonic fits. The forward direction (fit on older, test on newer) is the realistic deployment check and the win holds.
- Near-strike slice n=474 — directional only.

*Reproducer:* `crypto_decision_outcomes ⋈ crypto_market_snapshots` settlement join (server-side aggregate, ~100k small rows) + sklearn isotonic/logistic. No feature-store materialize.
