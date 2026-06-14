# Crypto Modeling & Trading Strategy — Deep Dive

**Date:** 2026-06-14
**Scope:** Synthesis of the live crypto modeling/trading stack as implemented, plus accumulated internal research (docs + operator findings). Internal-only; external/academic research not included (available as a follow-up).
**Method:** Six parallel code-reading passes over `forecast/`, `crypto/services.py`, `services/risk.py`+`risk/`, `services/execution.py`, `services/autonomous_gate_tuning.py`+packs, `integrations/kalshi.py`, `docs/`, and memory. Config values are the **deployed** values from `.env` (config.py defaults noted where they differ).

---

## Executive summary

**What we are:** a crypto-only, deterministic, fully-automated market-maker/taker on Kalshi short-horizon binary contracts ("will ASSET be ≥ $X at T?"), 15-minute and 1-hour. LLMs are hard-disabled; every trading decision is a deterministic function of a trained model + risk gates. Weather is fully decommissioned.

**The core loop:** Coinbase/Kraken spot + Kalshi quotes/candles → a per-asset feature vector → a pooled XGBoost/LightGBM model (isotonic-calibrated) → a fair-value probability → anchored against market mid → edge in bps → a stack of deterministic risk gates → sized order → kill-switch/color-locked execution → stop-loss/take-profit exit.

**Five things that matter most:**

1. **The economics are dominated by fees and bankroll, not model accuracy.** At ~66% win rate the gross edge per trade is ~+$0.07; Kalshi's 7% taker fee (`rate·price·(1−price)`, max at $0.50) plus a ~$10 bankroll (positions ~$2, fees 3–5% each) turns a gross-positive strategy net-negative (−$111 over two weeks). **Fees are invisible to internal P&L** — realized P&L is computed pre-fee. This single blind spot has driven most strategic decisions.

2. **The live edge is now a tight band-pass.** Deployed gates (commit eaa46a4): decision edge **750–1500 bps**, entry price **$0.35–$0.45**. This is exactly the empirically net-positive region (the only band that survived fee accounting). Expect very low entry frequency by design.

3. **There are two probability stacks; only one is live.** The `forecast/` package (Gumbel/KDE/CatBoost/online-calibrator) is **weather-domain scaffolding, fully dead code for crypto** (and weather is off). The live crypto engine is entirely inside `CryptoForecastService` — a different, simpler, well-instrumented stack (tree models + isotonic calibration + mid-anchoring).

4. **Significant research infrastructure is built but NOT wired into the live path:** fee-aware Kelly sizing (`risk/sizing.py`), survival/uncertainty/exit-score modules (`risk/`), the online calibrator, and the learned residual head. Live sizing is a simpler notional-target + cap stack; live exits are fixed-threshold stop/take-profit. There is latent upside in connecting these — and latent risk in maintaining dead code.

5. **Governance is strong; the data layer is the drag.** Gate thresholds are learned and promoted only through one audited path (`autonomous_gate_tuning` → backtest/model validate → canary). The recurring operational pain is data: a ~71 GB snapshots table whose full-window scan is slow, spot-coverage gaps (now resolved via 60s re-backfill), and settlement-label lag.

**Top risks right now:** (a) fee-blind P&L masking true performance; (b) micro-bankroll fee drag; (c) the pooled single model spanning 7 heterogeneous assets; (d) the live/shadow asset set is ambiguous between the (stale) promotion doc and the operator's latest decision; (e) the trainer's preflight crawl can hang (discovered 2026-06-14, separate from the materialize work).

---

## 1. Kalshi crypto market mechanics & integration

A Kalshi crypto market is a binary "will [ASSET] be **at or above** $X at time T?" contract, identified by a **series ticker** (e.g. `KXBTC15M`, the template) and a **market ticker** (the individual strike). The strike (`target_price_dollars`) is recovered from `floor_strike`/`cap_strike`/subtitle (`crypto/parsing.py:199`). Two frequencies are active: **15m** (live for all 7 assets) and **1h** (shadowed), classified from open→close duration (`crypto/parsing.py:305`).

**Settlement** is resolved by Kalshi from a Coinbase Real-Time-Index 60s average at expiry; we read `settlement_result` ("yes"/"no") from the API and never recompute it. Internally we approximate a settlement benchmark via a spot TWAP over the closing window (`CRYPTO_SETTLEMENT_BENCHMARK_SOURCE="cfb_rti_60s_average"`) for **training labels and diagnostics only**.

**Pricing:** four quote fields (`yes/no_bid/ask_dollars`) as `Decimal` on [0,1] = dollars per $1-payout contract, plus derived `mid_yes_dollars`, `spread_bps`. The parser tries `*_dollars` first, then cent fallbacks — the root of the **deci-cent gotcha**: 15m markets publish only `*_dollars`, so an older parser reading bare integer-cent fields silently got `None`. Tick is `$0.0001` (`core/fixed_point.py`); passive orders clamp to cent granularity.

**Fees:** taker fee = `round_up(rate · count · price · (1−price))`, symmetric, maximal at $0.50, `rate=0.07` (`services/fee_model.py`). Fee awareness exists at **entry** (edge check in `execution.py`, optimizer grid) but **fill-level fees are not captured from the API** — `upsert_fill` stores price/count but no fee, so realized P&L is pre-fee.

**Market data:** market candlesticks (live + historical endpoints) feed training; Coinbase OHLC spot is the fair-value anchor (per-provider staleness: Coinbase ≤5s, Kraken ≤960s). Orderbook depth (top-5 levels) is REST-polled; NO-side bids are transformed to YES asks.

**Reconciliation** (`services/reconcile.py`) syncs balance/positions/orders/fills/settlements each cycle, attributes fills to strategy codes by ticker frequency, and zeros settled positions. Settlement labels are propagated to stored snapshots when the settled-market scanner runs (creating a brief stale-label window for very recent rows).

**Safety:** RSA-PSS/SHA-256 signing per call; `KalshiSigner` refuses group/world-writable PEMs; separate read/write credentials; token-bucket rate limit (8/s, burst 16) with 429 backoff. Before any write: shadow-mode, kill-switch, and **deployment-color lock** (only the active color trades); `CryptoExecutionService` re-fetches `DeploymentControl` immediately pre-order to close the decision→execute race.

**Weaknesses:** fee-blind P&L; discovery cost (up to ~400 paged requests/cycle, mitigated only by a 20s TTL cache); real-time spot gaps degrade settlement features; settlement-label lag; passive-only default order mode (`crypto_taker_fallback_close_seconds=0`) can go unfilled.

---

## 2. The probability stack (live vs. dead)

**`forecast/` is not used by crypto.** Its `__init__` labels it "shadow-first Phase 1 building blocks" off the live path. It implements a weather pipeline: weighted ensemble fusion (`ensemble_fuser.py`), a Gumbel-CDF + Gaussian-KDE bucket estimator with Bayesian shrinkage to climatology (`probability_engine.py`), a **CatBoost** learned residual head capped at 0.5 weight (`learned_head.py`, **no trained model exists**), an SGD-logistic online calibrator (`online_calibrator.py`, **`update()` never called**), and a source-health scorer. None of it is wired into `container.py` for crypto, and weather is disabled. Treat it as design scaffolding / dead code.

**The live crypto engine** lives in `CryptoForecastService`:
- Produces a fair-value YES probability clamped to [0.0001, 0.9999].
- **Mid-anchoring at inference:** `fair_yes_anchored = w·market_mid + (1−w)·model_fair`, with `w = 0.30 + 0.70·min(1, |mid−0.5|/0.25)` — a ≥30% floor on the market's own price, rising toward the extremes, that prevents the model from taking large unilateral bets against a confident market.
- **Post-training isotonic calibration** on a held-out 15% split (needs ≥12 samples, both classes).
- A guardrail (`CRYPTO_PROBABILITY_GUARDRAIL_TOLERANCE=0.02`) forbids log-loss/ECE regressing >2% vs baselines.

**Notable weaknesses:** live inference deserializes the booster per call (training was fixed via `_batch_predict_rows`, inference was not); the `confidence` score is a heuristic ramp `0.80 + |edge_bps|/20000`, not a calibrated uncertainty; isotonic calibration is one-shot per nightly retrain (no intra-day recalibration); and the dead `forecast/` package is a maintenance/confusion hazard.

---

## 3. Model training & feature set

**Target:** `label_yes` ∈ {0,1} from the market's `settlement_result`. The model predicts P(contract settles YES).

**Lifecycle:** materialize → train → walk-forward backtest → gate. `_materialize_once` builds `crypto_training_feature_rows` from snapshots+candles+spot+funding (rows require a resolved settlement; incremental materialize now only rebuilds the tail). `_fit_crypto_calibration` fits candidates, runs the walk-forward tournament, writes the champion. Only a model clearing the replay gate is promoted.

**Row assembly** (`_crypto_decision_rows`): aligns Kalshi snapshots (quotes, spread, OI, target price), 15m candlesticks (momentum), and 60s spot OHLC (moneyness, returns, volatility, microstructure, settlement-window context), plus cross-asset and funding context, then `_crypto_add_recent_asset_features` (rolling 20-prior-settled-market yes-rate and mid-error, strictly causal). Candlestick-proxy rows are synthesized for settled markets lacking live snapshots (corpus volume; `strict_trade_eligible=False`, excluded from live).

**Feature set (`crypto-rich-v10`, ~70 numeric + ≤7 asset one-hots):** market mid (prob + logit), spot moneyness/returns (1–24 period)/momentum/target-distance-vol, realized vol (8 & 32), candle momentum, **settlement-window TWAP distance/return/vol** (closest-in-time predictor), recency (asset yes-rate delta, mid-error), microstructure (exchange spread bps, recent trade count, bid pressure — constant on historical corpora, flagged data-density), funding rate + delta, time-to-close ratio + buckets, market age, cyclical time-of-day/day-of-week + session flags, Kalshi-mid-vs-spot gap, quote-quality/staleness meta-features, cross-asset returns (all 7, self zeroed), asset one-hots.

**Candidates (9, `_fit_crypto_model_candidates`):** `market_mid_baseline` (the bar to beat), global/per-asset heuristics, spot-distance residual/contrarian variants, asset×time calibration, **`sklearn_logistic`** (C=0.75, scaled, balanced), **`xgboost_classifier`** (100 trees, depth 3, lr 0.05, subsample/colsample 0.8, λ=1.5, hist; GPU when `device=cuda` and ≥20k rows, fit on a worker thread while logistic+LightGBM run on CPU), **`lightgbm_classifier`** (depth 3, 15 leaves, CPU). Tree models use an 85/15 chronological split (15% reserved for isotonic calibration). Sample weights combine a quality score (penalties for non-strict/stale/proxy rows), time-proximity weighting, and market-balanced normalization so high-cadence markets don't dominate.

**Champion selection** (`_crypto_select_champion`): time-ordered walk-forward CV scoring **OOS net P&L vs the market-mid baseline** as the primary criterion, with log-loss/ECE guardrails (≤2% regression), falling back to lowest OOS Brier, then logistic, then heuristic. A `calibrated_weighted_ensemble` (inverse-Brier weights across folds) is also eligible.

**Two safety layers on the raw model:** (1) isotonic calibration; (2) the **`bucket_matrix`** — an empirical per-entry-price-band win-rate/P&L table from OOS rows that can hard-block a price band even when the model clears the edge floor (guards the <$0.20/>$0.80 loss concentration). *Currently the empirical bucket gate is disabled* (`CRYPTO_EMPIRICAL_BUCKET_GATE_ENABLED=false`) pending a rebuild after the bucket_matrix-from-wrong-model bug (9f746ea).

**Risks:** one pooled model across 7 heterogeneous assets (per-asset behavior only via one-hot intercepts); microstructure features near-dead on historical data; shallow trees (depth 3) limit expressiveness; no online update between nightly retrains; label-leakage surface around synthesized proxy rows (mitigated by strict-eligible gating).

---

## 4. Decision → risk → execution → exit lifecycle

**Edge:** for each (side, cost) from the live book: `raw_edge = prob − cost`; `expected_net_edge = raw_edge − fee`; `raw_edge_bps = raw_edge·10000`. The effective floor is `max(pack.min_fee_adjusted_edge_bps, settings.risk_min_edge_bps)`.

**Candidate-level gates** (pre-ticket) emit explicit skip reasons: `fee_adjusted_edge_below_live_min`, `spread_above_live_max`, `contract_price_below_crypto_min` ($0.35), `remaining_payout_below_crypto_min` (20%), `edge_above_crypto_credible_max`, `contract_price_above_crypto_max_entry`, `shrunk_fee_adjusted_edge_not_positive`, `empirical_bucket_not_allowed`.

**Deterministic risk engine** (`services/risk.py`) accumulates ALL blocking reasons. Deployed thresholds (`.env`): **min edge 750 bps**, **max credible edge 1500 bps**, min confidence 0.80, min contract price $0.35, **max entry price $0.45**, remaining-payout floor 20%, data staleness 60s, max entry loss $20 (resizes count down), max 500 contracts/order, 200/ticker, 10 concurrent tickers, 10% capital/ticker notional, per-asset daily loss cap $20. A **post-sizing fee-adjusted edge re-check** blocks orders whose net edge falls below the floor *after* fee dilution at the final (often small) count — the critical micro-stakes defense.

**Sizing (live):** notional-target (10% of capital) minus current/pending, floored by unit cost and caps, then `apply_fee_to_edge_floor` (fee ≤ 50% of edge per contract) — grows count to clear the ratio or blocks (`blocked_fee_ratio`). The principled **fee-aware Kelly** in `risk/sizing.py` (with survival-mode reduction) is **not wired into live**.

**Execution** (`services/execution.py` / `CryptoExecutionService`): five guards (app shadow, room shadow, kill switch, color lock, write creds) → GTC limit with ≤3 requotes (re-checking fee-adjusted edge each requote; `requote_edge_lost` if the spread moved). `close_position` (IOC) can bypass shadow for risk-reducing exits.

**Exits:** stop-loss (`services/stop_loss.py`) monitors **all** positions (incl. manual) with hard stop (−10% net), trailing (−10% from peak), rapid-adverse (two ≥$0.07 drops), and a momentum trigger (dead on 15m: 30-min hold gate > contract life). Take-profit (`crypto_take_profit.py`) fires at a threshold resolved frequency→asset→global; 15m now seeded to 0.50 (empirically 15m profit-taking was −$11 vs holding). A shared exit checkpoint prevents double-submits. The research exit modules (`exit_score`/`survival`/`uncertainty`) are **not wired into live**.

**Weaknesses:** extreme-price entries (now band-capped at $0.45); fee churn at micro-bankroll; Kelly/uncertainty sizing unused; momentum exit inert on 15m; requote adverse-selection window; sophisticated exit research unused.

---

## 5. Gates, parameter packs & autonomous tuning

**Principle:** thresholds are runtime data, not code. `config.py`/`default_pack()` are fallbacks; authoritative values live in the DB, written **only** by `AutonomousGateTuningService`. Packs (`builtin-deterministic-v1`) carry `int|None`/`float|None` fields — `None` = defer to settings; a settings floor is always enforced (`max(pack, settings.risk_min_edge_bps)`), and `sanitize_candidate_pack` clamps every field to engineering bounds (e.g. min-edge ∈ [250,5000]).

**Seven tunable fields:** `risk_min_contract_price_dollars`, `strategy_min_remaining_payout_bps`, `trigger_max_spread_bps`, `risk_min_confidence`, `risk_min_edge_bps`, `strategy_min_abs_delta_f`, `risk_max_credible_edge_bps`. (Caps, cooldowns, sizing are excluded — the tuner controls entry selectivity only.)

**Learning:** a per-asset walk-forward grid sweep over edge/spread/confidence/price/payout/credible-edge, scored on decision-corpus rows annotated with walk-forward predictions; a winner must beat current net-P&L AND not worsen drawdown-proxy. Duplicate recommendations are SHA-suppressed; a one-time historical bootstrap is allowed only while still on `builtin-deterministic-v1`.

**Validation → staging → canary → promote:** candidates pass parallel backtesting + modeling builders, are staged as a new pack version with a canary clock, then promoted **only** if post-staging live decision-corpus rows beat current on net-P&L and drawdown; otherwise `canary_pending`/`canary_support_timeout`/reject. The replay gate (`CryptoReplayService.gate`) independently checks artifact presence, OOS coverage, leakage, spot coverage, calibration metrics, and per-price-bucket floors.

**Risks:** canary thrash if nightly model regen re-ranks the scoring baseline mid-window (relevant now that the trainer will retrain more often); sparse labeled data on low-volume assets timing out the canary; grid-sweep overfitting (threshold selection not itself cross-validated); and bucket-matrix integrity depending on the correct champion artifact (the 9f746ea bug).

---

## 6. Empirical findings, P&L & current live state

**Live state:** crypto-only; weather fully disabled. All 7 assets trained on the CRYPTO_15M tree-model path (promoted 2026-06-05). **1h is fully shadowed** (BTC/ETH 1h passed the gate but a 12h live window lost −$5.20/69 entries → flipped to shadow 2026-06-10). **Discrepancy to resolve:** `crypto-live-asset-promotion.md` still lists all 7 as live 15m, but the operator's latest decision (`project_live_trading_state.md`, 2026-06-10) narrowed live 15m to the four gross-positive assets — **BTC, XRP, BNB, DOGE** — with ETH/SOL/HYPE 15m shadowed. The doc is stale on this. Touch20 is a separate, fully-disabled strategy.

**Headline result:** first two weeks = **−$111 on a 65.8% win rate** (556 settled fills). Root cause = **extreme-price entries**: 156 fills <20¢/>80¢ lost −$85 (~85% of losses); HYPE alone −$90 (it traded live on a `market_mid_baseline` champion with zero OOS edge).

**Refined band analysis** (14-day fills): only decision edge **750–1500 bps is net-positive (+$8.99)**; 1500–2500 = −$38; <750 negative after fees. Only entry cost **0.25–0.45 positive**; 0.60–0.75 = −$88. **Maker loses more than taker** (adverse selection > fee savings) → passive entry is not the fix. Combined filter (cost ≤0.45 ∧ edge 750–1500) flipped the sign (n=15, +$6.76) and cut fee churn ~96% — this is the deployed band-pass.

**15m take-profit is premature** (−$11 vs holding; near-expiry winners run to settlement). 1h take-profit is the opposite (+$17). Loss-side stops are good across both.

**Fees are the structural killer** and are invisible to internal P&L. At ~+$0.07 gross/trade and a $10 bankroll (positions ~$2, fees 3–5% each), the account ground to $0.03 by 2026-06-10 (refunded to ~$10).

**Bugs found & fixed:** bucket_matrix from wrong model (9f746ea; gate disabled meanwhile); stop-loss deci-cent blindness (15f22d7); self-inflicted spot-coverage gaps fixed by 60s re-backfill (probe the provider before assuming a gap is time-bound); per-row booster deserialization (`_batch_predict_rows`).

**Recently shipped:** review items 2–8 (effective-N weighting, point-in-time settlement features, **edge shrinkage** enforced, **fee-to-edge floor**, passive replay metrics, pooled nightly, funding loop, orderbook depth, Kraken venue) → expect reduced entry frequency; the **entry band-pass** (eaa46a4, live); and the **dedicated-trainer throughput** work (incremental materialize, bulk write, asset-parallel rebuild, supervised loops; deployed 2026-06-14).

---

## 7. Cross-cutting synthesis & recommendations

**Strengths.** The architecture is disciplined where it counts: deterministic gates authoritative over any model opinion; a single audited path for threshold changes; mid-anchoring + isotonic calibration + (when enabled) an empirical bucket gate as layered guards on the raw model; champion selection on OOS net-P&L-vs-mid rather than accuracy; strong execution safety (signing, kill switch, color lock). The empirical research loop is genuinely closing — each P&L post-mortem has produced a concrete, shipped control.

**The central problem is economic, not statistical.** A ~66% win rate is fine; the system loses because fee + bankroll economics are punishing at micro-stakes and **fees aren't in the P&L**. Everything else is secondary.

**Recommendations, roughly in priority order:**

1. **Make fees first-class in P&L.** Capture `fee_cost` from the settlements API into `upsert_fill` and compute net realized P&L. Until this exists, every performance number is optimistic and the tuner/canary optimize a biased objective. *(Highest leverage, modest effort.)*
2. **Resolve bankroll.** The band-pass is the right call, but at $10 the fee/stake ratio may make even the +EV band unprofitable. Decide: top up to a level where fees are <1% of stake, or accept this as a calibration phase and judge on **fee-adjusted** per-trade EV, not balance.
3. **Re-enable the empirical bucket gate** once the champion artifact is rebuilt (the bucket_matrix fix needs a clean nightly) — it directly targets the documented loss concentration.
4. **Reconcile the live/shadow asset set** between the promotion doc and the operator's 2026-06-10 decision, and make one source authoritative. Ambiguity here risks trading assets thought to be shadowed (the original HYPE failure mode).
5. **Wire in the research that's already built**, incrementally and behind flags: fee-aware Kelly + survival sizing (replaces the blunt 10% notional cap with confidence/uncertainty-aware sizing), and the exit-score/uncertainty exit logic. This is latent, tested-in-isolation upside.
6. **Watch canary stability now that the trainer retrains more often.** Decouple model-artifact regen cadence from gate-threshold derivation (already flagged for the matched-cadence roadmap) so frequent retrains don't thrash the canary baseline.
7. **Address the trainer preflight hang** (HTTP timeout on the candlestick crawl, or scope it) — discovered 2026-06-14; it can wedge a nightly and `C`'s loop supervision can't catch a hang-on-await.
8. **Delete or quarantine `forecast/`** for crypto purposes (or clearly mark it weather-only) to remove a confusion/maintenance hazard.
9. **Reconsider the single pooled model** longer-term: per-asset heads or a hierarchical model may capture per-asset tail behavior the one-hot intercepts can't (the 1h-toward-promotion track is a natural place to experiment).

**Open questions worth a dedicated look:** intra-day recalibration (the isotonic fit is one-shot/nightly); live-inference per-call booster deserialization (training was fixed, inference wasn't); and whether the requote window introduces measurable adverse selection on entries.
