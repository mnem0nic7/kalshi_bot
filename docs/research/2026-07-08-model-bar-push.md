# Model-bar push #1 — can anything clear the market mid? (2026-07-08)

Operator directive ("do those in order") after the model-divergence-TP strategy
refutation (`2026-07-08-model-divergence-tp-strategy.md`): work the ranked
levers for getting a model that beats the 15m mid. Status per leg:

## Leg 1 — basis-aligned labels (WAITING, ~2026-07-22)

Nothing to run. Multi-venue settlement-basis accumulation (Gemini venue live
since 06-22) reaches ~30d around 07-22; retrain + conditional-calibration gate
on that basis is the main event. This doc's other legs bound what to expect.

## Leg 2 — TWAP partial-realization fair value: REFUTED

Hypothesis: Kalshi 15m settles on a 60s TWAP of the CF index; near close part
of the settlement is already realized arithmetic and remaining variance
shrinks as tau_eff = tau−40s (tau≥60s) / tau³/10800 (inside the window), so a
mechanics-aware fair value should beat quotes that don't tighten at that rate.

Method (`scripts/twap_mechanics_fair_eval.py`, READ-ONLY): production
`_crypto_vol_normal_fair_up` math with (m_eff, tau_eff) swapped in; realized
partial TWAP from spot ticks (median 8s cadence ⇒ ~7 pts/60s window); fresh
LIVE-mode spot context; Brier per ttc bucket vs plain fair and mid; dedup
final-180s taker sim net of fees. 14d, all 7 assets, ~35k rows/asset.

Result: **the market prices settlement mechanics better than we can compute
them from our feed.** Mid Brier in the 0–30s bucket is 0.008–0.016 across
assets (near-omniscient) vs our best fair ~0.02–0.11; the TWAP variant is
WORSE than the plain fair on 6/7 assets (more confidence on a slightly-wrong
basis — Coinbase-proxy vs real CF index — costs Brier). Final-180s sim at
|edge|≥0.10: BNB −5.5¢, XRP −6.0¢, ETH −5.0¢, DOGE −4.8¢, BTC −5.3¢,
SOL −0.8¢, HYPE **+3.8¢**/ct. HYPE's lone positive (n=202, 66% win) looks
like its slow book (it is the strongest live stale-quote asset), i.e. the
known speed edge in disguise — logged as a possible sub-90s HYPE extension
lead for the stale-quote pilot, NOT as fair-value evidence.

## Leg 3 — σ materialization gap: DIAGNOSED (two findings)

`scripts/sigma_gap_decompose.py` scores the raw vol fair on v15 materialized
rows with per-field fresh substitution (12d, ttc 90–870s):

| arm | HYPE | BNB |
|---|---|---|
| A materialized row | 0.1944 | 0.2366 |
| B fresh moneyness only | 0.1776 | 0.2263 |
| C fresh σ only | 0.2189 | 0.2600 |
| D full fresh | 0.1633 | 0.2232 |
| mid | 0.1454 | 0.1582 |

1. **The residual materialized-vs-fresh loss is still mostly MONEYNESS**, even
   on v15 (arm B recovers most of A→D; mean |Δm| ≈ 2–8 bps log-moneyness).
2. **σ train/serve skew is large and real: materialized `spot_realized_
   volatility_32` differs from the LIVE-recomputed σ by 21–31% mean abs.**
   Models train on one σ series and serve another. Fresh σ ALONE scores worse
   (interaction with stale m) which is why this never surfaced as a simple
   fix. Fix direction: make materialize produce byte-identical ctx to the
   LIVE path (or persist live-computed σ), next time the materialize code is
   touched.

Bound: even full-fresh (D) trails the mid on both assets — pipeline hygiene
narrows the gap but cannot cross it. Crossing still requires leg 1 (labels)
or new information.

## Leg 4 — 1h champions: first fresh artifact, gate blocked on candidates

First post-catch-up 1h artifact (ETH, 07-08 13:58Z): trained
`lightgbm_classifier` selected, replay gate **blocked** with
`trade_candidate_count: 0` over 40k replay rows — zero decisions cleared the
decision-time fee/edge gates, so no OOS profit evidence exists either way.
1h is fee-edge-squeezed exactly like 15m; the gate is doing its job. Watch:
remaining assets refresh over the interleave (~days); report any gate that
passes with candidates > 0.

## Consolidated read

Every level-model angle measured this push (divergence entry, model-pegged
TP, TWAP mechanics, pipeline freshness) loses to the mid; the only positive
cells are on assets/moments where the BOOK is slow — the speed-edge class the
stale-quote pilot already monetizes. Expectation setting for leg 1: basis
labels remove a measured 13–19% near-money label-noise handicap, the one
structural disadvantage not yet fixed; if champions still lose to the mid on
basis-aligned labels, the 15m level game is closed and the roadmap is speed
edges + 1h/other-venue breadth.

## Weather addendum (2026-07-08): first surviving conditional angle — longshot-NO

Operator asked why weather isn't trading → quantified how close the best
conditional angle gets to the fee line, using the accumulated shadow corpus
(`historical_market_snapshots`, 806k KXHIGH intraday quote rows since 06-05;
results parsed from post-settlement snapshot payloads; ~600 settled markets
since 06-22).

Method: per-market quote aggregates (avg mid/spread per hours-to-close
bucket) joined to settlement results; calibration bias per price-band × ttc
bucket vs a fee + half-spread hurdle; then two honesty gates — dedup to ONE
strike per city-day (nearest 0.175 mid), and day-clustered standard errors
(weather outcomes correlate across cities within a day).

**Finding: 10–25¢ YES longshots quoted 12–48h before close are overpriced.**
Mid says 16.5¢, realized P(yes) = 8.6% (n=58 city-days, 18 cities, 15 days).
Bias −8¢ holds at ~2.2–2.3σ through city-day dedup AND day clustering on
mids. At EXECUTABLE prices (sell YES at the bid = buy NO, taker fee):
**+5.96¢/ct, 91% win** — but day-clustered t = **+1.7, below significance**.

Why this is a LEAD and not a trade (the crypto favorite-longshot lesson,
which was real in-sample and lost OOS): the payoff shape is many small wins
+ rare large losses on heat-spike days, and 15 midsummer days cannot contain
the tail — the market's 16¢ may be correctly pricing spikes that simply
didn't occur in the window. Capacity is also small (~4 qualifying city-days
per day, thin books).

**Pre-registered forward test:** re-run this exact analysis (executable
prices, city-day dedup, day-clustered t) around **2026-08-15** (~45–60 days
of corpus, tail-event coverage). Pass = day-clustered t ≥ 2.5 at executable
prices with the effect stable across the added weeks; then design a capped
pilot (operator go required). No collection change needed — the corpus
accrues automatically. This supersedes waiting until 10-01 for this one
angle; the blanket NO-GO on everything else stands.
