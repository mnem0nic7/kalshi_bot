# Deep research — small-bankroll Kalshi short-horizon crypto binary strategy (2026-06-25)

**Source:** deep-research harness run (`wf_b2929d1f-30e`): 5 angles → 23 sources fetched → 101 claims
extracted → 25 adversarially verified (2/3 refutes kills). **21 confirmed, 4 killed.** The harness's
auto-synthesis stage returned a stub (workflow bug); this report is the synthesis reconstructed from
the recovered verify-stage verdicts. Votes shown as `keep-refute` (3-0 = unanimous confirm; 2-1 =
confirmed with a dissent).

---

## 1. Market-making / taker strategies for binary & prediction markets

- **Optimal spread rises monotonically with the informed-trader (toxic) fraction** — an explicit
  Avellaneda-Stoikov-type spread as a function of market composition (3-0, arXiv 2501.03658).
- **Optimal quote displacement is a capped Avellaneda-Stoikov closed form:** `δ* = max(1/k − V, −δ∞)`,
  where `V` is the marginal value of changing inventory (3-0, 2501.03658).
- **Glosten-Milgrom pricing:** track a probability estimate of true value, set bid/ask from it — the
  formal bridge from a probability forecast to quotes under informed flow (3-0, das-qf / GMU).
- **Adverse selection (informed vs liquidity traders) is the core spread driver;** information
  *heterogeneity* → large spreads, homogeneity → small (2-1, das-qf).
- **Single-name markets carry more adverse selection than broad-based ones** — i.e. per-asset crypto
  strikes are *more* exposed to informed flow than index/event markets (2-1, Stanford Law 2026).
- **Latent-state filtering helps:** when the "fad"/fundamental is unobserved, a Kalman-Bucy filtered
  estimate yields a partial-information strategy that provably beats the misspecified one that ignores
  it (3-0, 2501.03658). *Actionable analogue:* filter the noisy cross-venue/settlement signal rather
  than using instantaneous spot.
- **Price-history outcome inference collapses when informed flow is scarce:** posterior accuracy
  degrades sharply once the informed-type weight ω₁ < ~0.15 (3-0, arXiv 2601.18815). *Implication:* in
  thin 15m books, reading "smart money" from tape is unreliable.
- ❌ **KILLED (0-3):** "near expiry, optimal spreads must widen substantially." Do not assume a
  mechanical near-expiry spread blow-up.

## 2. Position sizing, fees, and when NOT to trade

- **Makers vastly outperform takers, but both lose on average:** −9.64% (makers) vs −31.46% (takers)
  across all contracts; makers buying ≥50¢ averaged **+2.6%** (2-1, Whelan). Caveat the dissent
  flagged: "higher" = "less negative"; the +2.6% is small, conditional, and fragile.
- **Profitable maker edge can coexist with adverse selection** (single-name: effective spreads only
  modestly wider, makers earn ~2× per contract) (2-1, Stanford). But see §3 — Kalshi now charges
  maker fees, eroding this.
- ❌ **KILLED (0-3):** "threshold/no-trade region is the provably optimal policy under proportional
  costs." Our fee-driven stand-down still rests on direct fee math, not this theorem.
- ⚠️ **Thin evidence:** the harness did *not* surface a clean, verified fractional-Kelly-with-fees
  formula or risk-of-ruin bound. Treat our Kelly/fee-floor sizing as in-house, not externally
  validated.

## 3. Kalshi / Polymarket fee structure & microstructure

- **Fee is assessed on the p·(1−p) "expected-earnings" quantity → peaks near $0.50** (3-0,
  help.kalshi.com). This is exactly the bot's `rate·p·(1−p)` model. Confirms the strategy of trading
  the tails where the fee →0.
- **Pre-2025: `$0.07·P·(1−P)` per contract, takers only, rounded up to the cent** → ~1.77% effective
  at 50¢; makers paid zero (2-1, Whelan).
- 🔴 **UPDATE that supersedes the "makers pay zero" assumption: Kalshi now charges maker fees on
  resting (non-immediately-matched) orders** — passive quoting does **not** avoid all fees; both sides
  can incur them (3-0, help.kalshi.com/trading/fees). **This directly weakens any "switch to maker to
  dodge fees" plan** and corroborates our own finding that passive entry isn't a fee escape.
- **Favorite-longshot bias is real (3-0, Whelan):** low-price contracts win less than break-even after
  fees; high-price contracts slightly positive. Supports **price-scaled minimum-edge thresholds**.
- **Behavioral cross-subsidy:** traders systematically overbet YES in markets that settle NO → a
  persistent directional mispricing a disciplined NO-side counterparty can exploit (2-1, Stanford).
  *Worth testing as a mild NO-side prior — but the source is event/political markets, not 15m crypto
  (scope caveat the verifier flagged).*
- ❌ **KILLED (0-3):** "after fees only >70¢ contracts are significantly positive → tilt to high-price
  favorites / avoid the $0.35–0.45 band." The favorite-longshot *bias* is confirmed, but the strong
  "favorites are profitable, so tilt there" conclusion was refuted. **Our low-price-tail focus is not
  contradicted.**

## 4. Calibration & probability forecasting (short-horizon level events)

- ⭐ **Small calibration sets (<~200–1000 cases): Platt scaling beats isotonic** — isotonic overfits
  when data is scarce; isotonic only wins with sufficient data (3-0, Niculescu-Mizil & Caruana
  ICML'05). **→ IMPLEMENTED & DEPLOYED this session** (weather +20.0% vs +15.2% Brier; crypto
  carryover). See [[project_platt_calibration]].
- **Platt fits a sigmoid (only corrects sigmoid-shaped distortion); isotonic corrects any monotonic
  distortion but overfits when scarce** (3-0, same). → method choice should depend on sample size,
  which is exactly what we shipped.
- ⭐ **Venn-Abers calibration gives distribution-free FINITE-SAMPLE guarantees** (a set-valued [p₀,p₁]
  containing ≥1 marginally calibrated point), vs isotonic/binning which are only asymptotic — **better
  for a small recent-settlement corpus** (3-0, arXiv 2502.05676). *Candidate next upgrade beyond
  Platt; the interval width also signals calibration uncertainty.*
- **CORP / PAV reliability:** isotonic via pool-adjacent-violators yields non-decreasing reliability
  diagrams with nonnegative MCB/DSC components, avoiding arbitrary-binning artifacts (3-0, 2108.03210).
- **Proper-score decomposition `S̄ = MCB − DSC + UNC`** (miscalibration − discrimination + uncertainty)
  — a quantitative way to separate calibration error from discrimination (3-0, 2108.03210). *Useful
  diagnostic for the replay gate: report MCB and DSC, not just Brier.*
- ⚠️ **Conformal prediction caution:** classical conformal assumes exchangeability, which time-series
  violates — a fundamental limitation for sequential financial forecasting (3-0, 2511.13608). *Don't
  reach for vanilla conformal here; Venn-Abers/online variants are the safer route.*

---

## Net actionable takeaways (ranked)

1. **Small-sample Platt calibration** — DONE/deployed. The headline win.
2. **Venn-Abers calibration** — the strongest *next* calibration upgrade (finite-sample guarantees +
   uncertainty signal) for per-asset/per-station small corpora.
3. **Tail-only trading + price-scaled min-edge** — reconfirmed by the p·(1−p) fee peak and the
   favorite-longshot bias. Our low-price-tail focus stands; the "tilt to favorites" alternative was
   killed.
4. **Maker entry is NOT a fee escape** — Kalshi now charges maker fees on resting orders, and the
   maker "edge" is small/fragile/adverse-selection-bounded. Corroborates our prior decision against a
   passive-entry pivot.
5. **Report MCB/DSC (calibration vs discrimination) in the replay gate**, not just Brier — cheap
   diagnostic upgrade.
6. **Possible mild NO-side prior** from the YES-overbetting cross-subsidy — worth a *test*, with a
   scope caveat (evidence is from event markets, not 15m crypto).

## Follow-up build (2026-06-25): #2 + #5 implemented
- **#5 MCB/DSC decomposition** shipped as a shared pure-python util (`forecast/calibration_metrics.py`,
  CORP via PAV); surfaced in weather intraday metrics and the crypto replay-gate market-weighted
  metrics (`score_decomposition`). Diagnostic only.
- **#2 Venn-Abers** shipped as a selectable calibrator (`weather_intraday_calibration_method=venn_abers`).
  Real-data A/B/C (same weather splits): **Platt Brier 0.0832 / MCB 0.0062** (best Brier); **Venn-Abers
  Brier 0.0842 / MCB 0.0047** (best calibrated); isotonic 0.0883 / MCB 0.0080. **Decision: default stays
  Platt** (best Brier); Venn-Abers retained as the lowest-miscalibration option. The MCB/DSC split (#5)
  is what made the trade-off legible (Venn trades a little discrimination for better calibration).

## Killed claims (do NOT act on)
- Near-expiry spreads must widen (0-3).
- No-trade region provably optimal under proportional costs (0-3).
- High-price-favorite tilt is profitable after fees / avoid 0.35–0.45 (0-3).
- Max-margin sigmoid-distortion is the calibration villain (1-2, killed).

## Caveats
- Synthesis reconstructed from verify-stage verdicts (auto-synthesis stub bug); claim texts are
  faithful but were recovered from transcripts.
- Several MM/microstructure sources are event/political-market studies; transfer to 15m/1h crypto
  price-level binaries is an assumption, not established (verifiers flagged this repeatedly).
- No verified fractional-Kelly-with-fees / risk-of-ruin formula surfaced — that part of the question
  remains thin externally.
