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

## Execution update (2026-06-14, multi-agent workflow `wf_87c229de`)

**P0 — DONE & shipped (commit 971e3c0).** Per-candidate `candidate_selection_table` + `champion_model_type` projected into artifact `metrics`, plus one INFO log line per asset/freq. Pure read-side projection; `_crypto_select_champion` untouched (unit-tested; correctness + safety reviewed). A fallback is now self-explaining from the artifact/logs.

**Real OOS margins (now read directly from artifact `metrics` — an earlier "metrics holds only fees_dollars" claim was a SQL bug):**

| asset | champion | `champion_selection_reason` | oos_net | oos_selected_count |
|---|---|---|---|---|
| HYPE | spot_distance_residual | `selected_positive_oos_pnl_non_market_candidate` | +1.60 | 3 |
| BNB | spot_distance_residual | `selected_positive_oos_pnl_non_market_candidate` | +0.86 | 3 |
| DOGE | spot_distance_residual | `diagnostic_only_best_non_market_oos_pnl` | −1.47 | 10 |
| BTC/ETH/SOL/XRP | market_mid_baseline | `fallback_market_mid_no_non_market_candidate` | 0.00 | 0 |

Two things stand out: the majors produced **0 OOS trade candidates** (nothing cleared net>0 after fee), and the "winners" were chosen on **3 trades** — selection is running on single-digit OOS counts (statistically noisy).

**P1 — refined root cause (no code change; trainer experiments next).** Feature coverage is **ruled out** (all 7 assets have real Coinbase spot: `spot_feature_status=available`, `spot_proxy_only=0` for 100% of rows). The binding constraint is the **flat cent-ceiling taker fee × market efficiency**: `estimate_kalshi_taker_fee_dollars` rounds `0.07·p·(1−p)` **up to a whole cent**, so any contract priced 0.20–0.80 pays a flat ~2¢; on ultra-efficient majors (BTC ~20 bps spread) the mid is near-fair, so no trade clears net>0 → 0 candidates → baseline. DOGE/HYPE trade at ~400 bps where the mid is exploitable. Caveat: SOL/XRP have wide spreads yet still fail → wide spread is necessary-not-sufficient (weaker fit / fewer real-quote rows). **Trainer-side experiments (run in the mem-capped trainer, not the host):** (H1) re-run BTC/ETH OOS with fee `round_up_to_cent=False` and with fee_rate halved — if net/advantage flip positive, the cent ceiling is the binding constraint; (H2) log per-asset `model_net` vs `market_mid_net` separately; (H3) report `oos_trade_candidate_count` per asset post bucket-gating. Most promising lever if action is wanted: a **fee-aware per-asset edge floor** so efficient majors aren't structurally excluded — but that is gate-threshold territory (`autonomous_gate_tuning` authority, validated against realized fills), not a manual edit.

**P2 — root cause found; fix gated to next retrain (no hot change).** `spot_distance_residual` and `sklearn_logistic` fit isotonic **in-sample on all rows** ([services.py:11133](../../src/kalshi_bot/crypto/services.py#L11133)), unlike the tree models which hold out the most-recent 15% (`_cal_split_idx = int(len*0.85)` when `len>=2000`) to fit calibration out-of-sample. In-sample isotonic on the lowest-density mid band (0.25–0.65) leaves the observed ~0.1–0.2 overconfidence. The genuine fix is **holdout parity** for the residual/logistic calibration — but it changes how the *only live models* (HYPE/DOGE) are calibrated, so it belongs in a reviewed change validated through the backtest/replay-gate flow, **not** a hot edit. Decision: let the next nightly regenerate calibration, watch the residual via P0 observability + a re-audit, and make the holdout-parity edit only if it persists.

**P3 — not a defect; needs an operator intent decision.** HYPE is **intentionally shadowed**; with `PRODUCTION_CRYPTO_PRODUCTION_AUTONOMY_ENABLED=true`, `shadow_evidence_mode` is False ([services.py:6470](../../src/kalshi_bot/crypto/services.py#L6470)), so shadow-mode assets are skipped with `not_live_eligible` before any room/decision is created. This is why ETH/SOL/XRP/HYPE all went silent ~2026-06-10 when the 3 live assets were promoted. Options: **(a)** decouple `shadow_evidence_mode` from `production_autonomy_enabled` (small code change) so shadowed assets resume emitting *shadow* decisions (evidence for promotion, **no live orders** — live execution still gated by `asset_mode==LIVE`); **(b)** promote HYPE to live `asset_mode` (deployment-control metadata) to actually trade it; **(c)** leave as-is. Awaiting operator direction.

## P1 DEFINITIVE root cause (2026-06-14, from artifact candidate_report — fee hypothesis H1 is MOOT)

Reading the existing 2026-06-13 artifacts' `payload.candidate_report.candidates` (no replay re-run needed) settles it. For **BTC and ETH the only `available` candidate is `market_mid_baseline`** — every trained model is `guardrail_failed`:

| asset | trained candidates | status |
|---|---|---|
| BTC, ETH | xgboost, lightgbm, sklearn_logistic, spot_distance_residual, … | **all `guardrail_failed`: `ece_regressed_vs_market_mid` / `log_loss_regressed_vs_market_mid`** |
| DOGE | current_heuristic, xgboost, spot_distance_residual, market_mid | available (pass guardrail); champion = spot_distance_residual |

The gate is `_crypto_candidate_guardrail_failures` ([services.py:11863](../../src/kalshi_bot/crypto/services.py#L11863)): a non-baseline candidate is rejected if its `log_loss` or `ece` exceeds the market-mid reference (plus a small tolerance). **On the efficient majors the market mid is so well-calibrated that our trained models' calibration *regresses against it* → rejected before the profit/fee stage ever runs.**

**Consequences:**
- **H1 (cent-ceiling fee) is moot** — candidates die at the calibration guardrail, never reaching the fee/profit simulation. No fee experiment needed (a bounded BTC replay was started in the mem-capped trainer to confirm, then stopped once the artifact gave the answer directly — the guardrail did its job: trainer peaked 18.5/32 G, host never at risk).
- **P1 and P2 are the same root cause.** The lever for putting real models live on the majors is **better model calibration** (lower ECE/log-loss than the market mid) — exactly P2's out-of-sample-isotonic fix plus genuinely better probability estimates. Fix calibration → models may clear `ece_regressed_vs_market_mid` → majors get real models.
- DOGE's live champion (`spot_distance_residual`) passed the calibration guardrail but had **negative OOS net P&L (−1.47 over 10 trades, `diagnostic_only`)** — well-calibrated (Brier 0.175) yet not demonstrably profitable; worth watching via P0.

**Revised priority:** elevate **P2 (calibration)** — out-of-sample isotonic holdout parity for residual/logistic, and a calibration-method improvement validated through the gate — since it is the shared lever for P1 (majors) and P2 (DOGE residual). The fee-aware edge-floor idea is deferred (not the binding constraint).

### Explicitly NOT doing
- **No recalibration layer on `market_mid_baseline`** — nothing to calibrate; it echoes the market.
- **No fading of the market's near-expiry overconfidence** as an edge — favorite-longshot / NO-side taker edge was REFUTED in the external research; only act via an OOS-replay-validated, fee-net strategy.
- **No manual edge-gate threshold edits** — `autonomous_gate_tuning` is the sole authority; revisit only after a real model is live on an asset.
- **No heavy training/materialize on the host** — P1 runs in the trainer ([host-OOM guardrails](2026-06-14-crypto-calibration-empirical-study.md)).
