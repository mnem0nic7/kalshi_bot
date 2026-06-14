# External Research: Prediction-Market Trading, Fees & Sizing

**Date:** 2026-06-14
**Method:** deep-research harness — 6 angles, 27 sources fetched, 102 claims extracted, 25 verified via 3-vote adversarial refutation (18 confirmed, 7 killed). Companion to [the internal modeling deep-dive](2026-06-14-crypto-modeling-strategy-deep-dive.md).
**Scope:** prediction-market making/taker strategy, fee-aware sizing, Kalshi/Polymarket fees & microstructure, short-horizon calibration. Confidence labels and votes are the harness's.

> **Read this first — the one big signal:** every strong, primary-sourced finding points the same direction: **the taker side is structurally disadvantaged and the maker side is structurally favored.** That is the most actionable strategic conclusion, and it reframes our fee problem from "trim entries" to "change which side of the book we're on."

---

## Confirmed findings (primary-sourced unless noted)

1. **Fee formula & shape — confirms ours.** Kalshi taker fee = `0.07·P·(1−P)` per contract, rounded up to the cent, peaking at P=0.50 ($0.0175/contract); makers paid **nothing pre-April-2025**. *(high, 3-0; Whelan, karlwhelan.com/Papers/Kalshi.pdf.)* Implication: our $0.35–$0.45 band sits near the fee maximum, and the per-contract cent round-up hits hardest at our tiny size.

2. **Takers lose, makers win — structurally.** Across 72.1M Kalshi trades / $18.26B (2021–2025), **takers ≈ −1.12% gross per trade, makers ≈ +1.12%** — and this is *gross of fees*, so net taker loss is worse. *(high, 2-1; Becker, jbecker.dev, via predictionhunt.)* A pure-taker strategy fights a ~1%+ headwind **before** fees.

3. **You can't reprice your way out of the fee.** A binary's delta is `S'(x)=p(1−p)` — the *same* `p(1−p)` that drives the fee. Edge/liquidity is largest near 0.5 and vanishes at the boundaries, so the swing zone is simultaneously highest-edge and highest-fee. **The decision variable is net edge per fill, not the price band.** *(high, 3-0; arxiv 2510.15205.)*

4. **The right edge threshold = fee + expected adverse selection + margin** (Glosten-Milgrom applied to binaries). When measured edge compresses below that, **stand down or widen** rather than chase fills. *(high, 3-0; arxiv 2510.15205.)*

5. **Easy fills are a red flag.** Live Binance BTC-perp experiment: **negative correlation between fill likelihood and post-fill returns** — orders that fill fast are selected against. *(high, 3-0; arxiv 2502.18625.)* Directly relevant to any maker leg: getting filled quickly correlates with being wrong.

6. **Single-name markets favor makers ~2×.** Single-name event contracts show ~2× the adverse selection of broad markets but only modestly wider spreads, and **makers earn ~2× per contract** — because traders overbet YES (~61% buy YES while YES wins ~32%), a behavioral surplus that cross-subsidizes liquidity providers. *(high, 3-0; Bartlett & O'Hara 2026, SSRN 6615739 / Stanford Law.)* **Caveat:** this accrues to **makers**, not takers — the corollary "exploitable NO-side taker edge" was **refuted** (see below).

7. **Sizing — Kelly baselines (pre-fee).** `f* = (b·p − q)/b` with net odds `b=(1−price)/price`; asymmetric form `f* = p/a − (1−p)/b`. For a $1 binary, `a=price` (you lose your stake). These **must be fee-adjusted** (shrink win payoff by the fee, grow the loss by the fee). *(high, 3-0; Kelly 1956, corroborated CFI/CQF/Wikipedia.)*

8. **Use fractional Kelly.** Half-Kelly ≈ 75% of full growth at ~half the volatility; **full Kelly has a 33% chance of halving before doubling** (half-Kelly drops that to 1/9). A downside-percentile objective lowers size further. **Use quarter-to-half Kelly** given our small bankroll + model uncertainty. *(high, 3-0; MacLean-Ziemba-Blazenko, Thorp.)*

9. **Provable drawdown control — Risk-Constrained Kelly (RCK).** Maximize `E log(rᵀb)` s.t. simplex constraints and **`E[(rᵀb)^(−λ)] ≤ 1`** with `λ = log(β)/log(α)`; satisfying it guarantees `Prob(W_min < α) < β`. Convex, solvable with cvxpy; `λ=0` recovers ordinary Kelly, `λ` is a single growth-vs-drawdown knob. *(high, 3-0; Busseti, Ryu & Boyd 2016, arxiv 1603.06183.)* This is a rigorous answer to our survival/risk-of-ruin requirement.

10. **Optimal maker quoting (only if we add a maker leg).** Avellaneda-Stoikov in log-odds: half-spread `δ_x = γ·σ²·(T−t) + (2/k)·log(1+γ/k)`, displayed price spread `δ_p = p(1−p)·δ_x` (compresses near boundaries). Widen with time-to-expiry, vol, and risk aversion. *(medium, 2-1; arxiv 2510.15205 — note half-vs-full-spread ambiguity.)* Makers also face a second risk — **price reading** (quotes reveal inventory) — so avoid predictable inventory-skew. *(high, 3-0; Gueant et al., arxiv 2508.20225.)*

---

## Refuted / unsupported — do NOT build on these

- **Favorite-longshot bias on Kalshi** ("high-price entries systematically better post-fee; our 0.35–0.45 band is the wrong side") — **REFUTED 0-3.** Our band-pass is *not* on a provably-bad side; it stands on our own fill data, which is the right basis.
- **"Positive post-fee returns only above $0.70"** — refuted 1-2.
- **Exploitable NO-side mispricing** for takers (from the YES-overbetting surplus) — **REFUTED 0-3.** The surplus accrues to makers, not NO-side takers.
- **"Makers must be contrarian"** — split 1-2 (not established).
- **Polymarket longshot spread premium**, and **"short-horizon = most calibrated"** / generic favorite-longshot — split 1-2.
- **Calibration methodology gap:** the question asked which of isotonic / Platt / ensemble works best for short-horizon crypto *level* events near a strike. **No claim survived verification** — the external literature simply doesn't answer this. Our isotonic choice is defensible but **must be validated on our own settlement data**, not on outside authority.

---

## Important caveats

- **Fee schedule changed after April 2025:** makers now pay a fee (~¼ the taker rate per current docs). The `0.07·P·(1−P)` taker formula still holds; any maker-leg economics must use the *current* maker schedule, not the pre-2025 "makers free" papers.
- **Maker-vs-taker relevance gap:** most of the strongest evidence describes the **maker** side; we are a **taker**. The consistent through-line (maker side favored) is the signal, but none of it is a turnkey taker edge.
- **Applicability by analogy:** the Kalshi/Polymarket adverse-selection work covers event/single-name markets broadly, not specifically 15m/1h crypto level binaries.

---

## What this means for us (mapped to our system)

**1. The headline reframes the whole problem.** Our internal deep-dive concluded "the problem is economic (fees), not statistical." The external evidence sharpens it: **as a taker we fight a structural ~1%+ headwind plus the fee that peaks exactly in our band.** The highest-leverage strategic move isn't a better model — it's **becoming a (disciplined) maker** where feasible.

**2. Reconcile a real tension first.** Our 14-day data found *maker entries lost more* than taker (−$0.54 vs −$0.36), seemingly contradicting the "makers win" literature. Finding #5 resolves it: **a naive maker leg gets adversely selected** (easy fills = wrong). The literature's maker edge requires *disciplined* quoting — fair prices, widen/stand-down when edge compresses, and treating fast fills as a warning. Before adding a maker leg, we'd need to measure our own realized adverse selection per fill. This is the single most important follow-up.

**3. Make the edge threshold principled, not a fixed band.** Replace the static 750–1500 bps / $0.35–$0.45 band with a **price-dependent, fee-adjusted minimum edge = fee(P) + expected-adverse-selection(P) + margin** (finding #4). The fee term is closed-form (`0.07·P·(1−P)`); the adverse-selection term we estimate from our fills. This generalizes what the band-pass approximates.

**4. Wire in the sizing research we've already built.** `risk/sizing.py` has fee-aware Kelly + a survival reduction but is **unused live**. The research validates: (a) fee-adjust the Kelly fraction, (b) apply a **quarter-to-half** multiplier, (c) consider the **RCK** convex constraint for a provable drawdown bound (`λ` knob). This is concrete, implementable, and directly addresses the bankroll/ruin problem.

**5. Calibration is on us.** No external answer exists — run a dedicated empirical comparison (isotonic vs Platt vs ensemble) on our own settlement data, especially near-strike and across intraday-vol regimes.

**6. Don't chase the refuted edges** — no favorite-longshot tilt, no NO-side "free" edge. Our band-pass is empirically grounded, which the refutations vindicate.

### Suggested next steps (in priority order)
1. **Instrument realized adverse selection per fill** (and capture `fee_cost` — already the #1 item from the internal deep-dive). Without it, neither the maker decision nor the principled edge threshold can be set.
2. **Prototype a disciplined maker leg** on one liquid asset (BTC 15m), resting limit orders at fair value, with the "widen/stand-down on edge compression" and "fast-fill = caution" rules — measured in shadow first.
3. **Wire fractional fee-aware Kelly** (¼–½) into live sizing; evaluate RCK as the drawdown governor.
4. **Replace the static band** with the fee-adjusted, price-dependent edge threshold once adverse selection is measured.
5. **Empirically pick the calibration method** on our settlement data.

### Key sources
Whelan (Kalshi fee paper); Becker, *Microstructure of Wealth Transfer in Prediction Markets*; Bartlett & O'Hara 2026 (Kalshi adverse selection, SSRN 6615739); Busseti-Ryu-Boyd, *Risk-Constrained Kelly Gambling* (arxiv 1603.06183); arxiv 2510.15205 (event-contract market-making handbook); arxiv 2502.18625 (crypto MM fill-vs-return); MacLean-Ziemba-Blazenko / Thorp (fractional Kelly).
