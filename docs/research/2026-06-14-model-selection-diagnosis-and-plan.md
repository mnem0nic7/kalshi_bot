# Model-Selection Diagnosis & Plan: why 5/7 majors run `market_mid_baseline`

**Date:** 2026-06-14
**Trigger:** the [calibration study](2026-06-14-crypto-calibration-empirical-study.md) found the live `prediction_yes` tail-overconfident; tracing the cause led here. **The calibrator is correct** ([`_apply_probability_calibration`](../../src/kalshi_bot/crypto/services.py#L11444)); the real issue is which model is live per asset.

## A — Diagnosis: why BTC/ETH/SOL/XRP/BNB fell back to baseline

**Live state (2026-06-13 nightly, `crypto_model_artifacts`, production 15m):**

| asset | `model_type` | calibration | sample_count |
|---|---|---|---|
| HYPE | `spot_distance_residual` | isotonic (71 knots) | 152,921 |
| DOGE | `spot_distance_residual` | isotonic (51 knots) | 108,014 |
| **BTC** | **`market_mid_baseline`** | none | 106,418 |
| **ETH** | **`market_mid_baseline`** | none | 150,909 |
| **SOL** | **`market_mid_baseline`** | none | 151,749 |
| **XRP** | **`market_mid_baseline`** | none | 151,659 |
| **BNB** | **`market_mid_baseline`** | none | 109,544 |

`market_mid_baseline` returns the market mid verbatim ([services.py:11517](../../src/kalshi_bot/crypto/services.py#L11517)) — no model, no calibration, edge≈`mid−ask`<0 ⇒ those assets effectively stand down.

**Mechanism (not a bug — a safety gate).** Champion selection ([`_crypto_select_champion`](../../src/kalshi_bot/crypto/services.py#L11961)) is a cascade:
1. **profit-deployable** — [`_crypto_candidate_is_profit_deployable`](../../src/kalshi_bot/crypto/services.py#L11931): requires policy net P&L **> 0** *and* advantage vs market-mid **> 0** *and* min policy support, non-baseline → pick best by net P&L;
2. else **profit-supported**;
3. else **probability-deployable** → pick best by Brier;
4. else **`market_mid_baseline`**;
5. else `sklearn_logistic`.

So a trained model goes live only if it demonstrates **profitable, market-beating OOS performance net of fees**. BTC/ETH/SOL/XRP/BNB's 2026-06-13 candidates cleared *neither* the profit bar *nor* the probability bar → baseline. HYPE/DOGE did. This is the **efficient-market signature**: the liquid majors are hard to beat after the taker fee that peaks mid-book (see [external research](2026-06-14-external-research-prediction-market-trading.md)); the smaller/less-efficient minors are beatable. It is *not* downstream of the OOM incident, and not a calibration defect.

**Evidence quality / gap.** The decision is made in-process during the nightly from OOS folds. The per-candidate OOS numbers (net P&L, advantage-vs-mid, Brier, selected reason) are **not persisted** — artifact `metrics` holds only `fees_dollars` — and the 2026-06-13 trainer logs have rotated out. So we know the *criterion* and the *outcome* precisely, but not the *margins* by which the majors failed. Recovering margins requires re-running training in the (now mem-capped) trainer or adding selection-metric persistence and waiting for the next nightly.

## B — Audit: the two assets that DO have real models

Per-asset reliability on settled 15m decisions (`crypto_decision_outcomes ⋈` settlement):

| asset | n | mean_pred | empirical | Brier (all) | Brier (post-2026-06-13 model) |
|---|---|---|---|---|---|
| DOGE | 28,201 | 0.469 | 0.501 | 0.2415 | **0.1747** |
| HYPE | 2,402 | 0.455 | 0.488 | **0.1728** | n/a (0 decisions post-retrain) |

The real models are **well-calibrated** (Brier ~0.17, vs 0.28 for the baseline-dominated pool). DOGE post-retrain reliability:

| bin | n | mean_pred | empirical |
|---|---|---|---|
| [0.0–0.1) | 762 | 0.020 | **0.043** ✓ tails excellent |
| [0.2–0.3) | 1,353 | 0.252 | 0.116 ⚠ mid overconfident |
| [0.4–0.5) | 707 | 0.432 | 0.307 ⚠ |
| [0.5–0.6) | 846 | 0.565 | 0.376 ⚠ |
| [0.7–0.8) | 2,148 | 0.736 | 0.716 ✓ |
| [0.9–1.0] | 967 | 0.942 | 0.987 ✓ (mildly under-confident = safe) |

**Findings:** (1) the real model's **tails are excellent** — the exact opposite of the baseline assets, confirming the isotonic calibrator works when there is a model to calibrate; (2) a mild **mid-range (0.25–0.65) overconfidence** residual on DOGE (predicts ~0.1–0.2 too high); (3) **HYPE has made 0 decisions since its 2026-06-13 retrain** — low volume / possible decision-flow gap worth a separate check.

## The Plan

**P0 — Observability (cheap, low-risk, do first).** Persist per-candidate OOS selection metrics into the model artifact `metrics` (net P&L, advantage-vs-market-mid, Brier, min-policy-support count, and the chosen `selected_reason` / runner-up) in `_crypto_select_champion`'s caller. Then every future baseline fallback is self-explaining — no re-running heavy training to learn *why*. Add a one-line nightly log of the selection table per asset.

**P0 — Docs.** CLAUDE.md corrected to "2 of 7 assets on a trained model; 5 on baseline" (done in this change).

**P1 — Model quality on the majors (the real lever; trainer-side, in the mem-capped trainer).** Investigate why BTC/ETH/SOL/XRP/BNB can't beat market-mid OOS net of fees: feature quality/coverage (spot-distance, momentum, microstructure features actually populated for the majors?), label/settlement timing, the v10 feature-store recency, and whether the profit gate is mis-specified for efficient markets (e.g. fee model in the OOS simulation). Decide per asset: improve features, accept baseline (stand down on efficient majors), or pursue a maker leg where the structural edge lives (external research). Use the P0 observability output to target this — don't guess.

**P2 — Minor calibration tightening.** DOGE's mid-range overconfidence is a small isotonic residual; fold a fix into the next retrain rather than a separate change. Re-audit HYPE/DOGE per-model after the next nightly with more post-retrain data.

**P3 — Investigate HYPE's 0 post-retrain decisions** (decision-flow / volume gap).

### Explicitly NOT doing
- **No recalibration layer on `market_mid_baseline`** — nothing to calibrate; it echoes the market.
- **No fading of the market's near-expiry overconfidence** as an edge — favorite-longshot / NO-side taker edge was REFUTED in the external research; only act via an OOS-replay-validated, fee-net strategy.
- **No manual edge-gate threshold edits** — `autonomous_gate_tuning` is the sole authority; revisit only after a real model is live on an asset.
- **No heavy training/materialize on the host** — P1 runs in the trainer ([host-OOM guardrails](2026-06-14-crypto-calibration-empirical-study.md)).
