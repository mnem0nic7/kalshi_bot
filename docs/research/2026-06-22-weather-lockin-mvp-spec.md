# Weather lock-in MVP — the simplest tradable weather strategy

**Date:** 2026-06-22
**Status:** SPEC (deploys nothing; design for the simplest weather live path)
**Supersedes complexity in:** `docs/research/2026-06-22-weather-strategy-rework.md` (the full oriented
bracket+EMOS model stays as a *later* stage; this is the MVP that goes live first)

## Why simplify

The oriented backtest showed the entire weather edge is concentrated in the **already-decided** cases:

| strike_type | Brier | regime |
|---|---|---|
| `greater` (YES = high ≥ floor) | **0.038** | near-deterministic once high-so-far ≥ floor |
| `between` | 0.136 | needs μ/σ remaining-rise model |
| `less` (YES = high ≤ cap) | 0.170 | needs μ/σ except the locked-NO tail |

By hour: `0.182@08 → 0.099@14 → 0.0745@17`. The edge appears as the day's outcome becomes
physically certain. That subset needs **no model**: no remaining-rise μ/σ fit, no EMOS, no isotonic.
It is observed-vs-priced arbitrage on a fact that can no longer change.

This also sidesteps the rework's biggest open risk: the probabilistic model's calibration is
in-sample, 2 cities, diurnal-fit. The lock-in path is true-by-construction, so the shadow→gate→live
proof is "did the market leave fee-edge on a decided contract," not "is σ̂ calibrated OOS."

## The strategy already (mostly) exists

`score_weather_market` already computes `WeatherResolutionState.{LOCKED_YES,LOCKED_NO,UNRESOLVED}`
(`scoring.py:611-643`, enum `core/enums.py:55`). On a lock it returns `fair = 1.0000 / 0.0000`,
`confidence = 1.0`. The MVP is: **trade only when the state is LOCKED, and only the cases the lock
is provably correct for.** Everything else (UNRESOLVED) stands down — no model needed live.

### Correctness gaps to fix first (TDD, RED→GREEN)

1. **YES lock must read high-so-far, not current temp.** `scoring.py:613-616` keys `LOCKED_YES` off
   `current_temp_f`. The contract settles on the *daily high* (a running max). Once high-so-far ≥ the
   upper threshold it is locked YES permanently, but `current_temp_f` falls every afternoon, so the
   current code silently reverts a genuine lock to `UNRESOLVED` when temp drops.
   - Fix: for the `>`/`>=` (YES-above) orientation, compare `effective_observed_high_so_far_f`
     (already computed at `scoring.py:599-601`) against `threshold_f`, not `current_temp_f`.
   - `LOCKED_NO` (`<`/`<=`, temp ≥ cap) is already correct with either value — exceeding the cap is
     also permanent — but switch it to high-so-far too for symmetry/clarity.
   - RED test: feed a series where high-so-far crosses the floor at 13:00 then temp falls by 16:00;
     assert state stays `LOCKED_YES` at 16:00 (currently reverts to `UNRESOLVED`).

2. **Settlement-station basis** (do NOT simplify away). The observed high must come from the official
   NWS daily-summary station (IEM ASOS substrate, `integrations/asos_archive.py`), not the Open-Meteo
   gridpoint. This was bug #1 of the two that made earlier runs confidently wrong; the lock-in is only
   "certain" if it's measured on the same station Kalshi settles against.

3. **`between` and all UNRESOLVED → stand down in the MVP.** A `between` bracket is never fully locked
   by high-so-far alone before end-of-day (needs the upper edge too), so the MVP skips it. Smaller
   opportunity set, but every trade is on a decided outcome.

## Live decision predicate (what the MVP actually trades)

For each open KXHIGH market, at each scoring pass:

```
state = score_weather_market(...).resolution_state         # already produced
if state == LOCKED_YES:   want = BUY YES   at fair 1.0
elif state == LOCKED_NO:  want = BUY NO    at fair 0.0      # i.e. sell-to-open / buy NO leg
else:                     STAND DOWN       (no model, no order)
```

Then the **fee/edge gate** (the go/no-go that needs shadow data):

```
edge      = |fair - market_price|                          # fair is 1.0 or 0.0 → edge = mispricing
fee       = rate * count * price * (1 - price)             # Kalshi taker, rate≈0.07
net_edge  = edge * count - fee
GATE: net_edge > 0  AND  edge >= weather_min_lockin_edge   (seed ~ a few ¢, tuner-adjustable later)
```

Because a locked contract's market price is near 0/1, `price*(1-price)` is small → fees are *least*
punishing exactly here (same tail-pricing logic that governs crypto). That is the structural reason
the lock-in can clear fees where mid-day forecasts cannot.

## The gate that must pass before live (calendar-bound)

Identical shape to the crypto replay gate, but on the lock-in subset:
1. Shadow-collect intraday KXHIGH quotes (live now via `WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=300`,
   landing in `historical_market_snapshots`).
2. Offline harness: for each historically-locked market, take the market quote *after* the lock
   timestamp and compute realized fee-adjusted edge (did the market still offer the cheap side?).
3. GO only if locked-subset net fee-edge > 0 with adequate coverage (n) per station/strike_type.

If the market reprices the lock instantly, edge → 0 and we trade nothing — that's the honest ceiling,
and the gate enforces it rather than us assuming edge exists.

## Wiring + go-live (operator-gated)

- Plumb `resolution_state` + the fee/edge gate into the weather order path. The ONLY thing currently
  preventing live weather orders is `*_TRIGGER_ENABLE_AUTO_ROOMS=false` (prod is NOT shadow:
  `PRODUCTION_APP_SHADOW_MODE=false`, kill switch off). **Do not flip auto-rooms until the gate passes**
  — that flip is the operator's final go.
- Enable live **per station + strike_type**, only where step-3 coverage proves fee-edge (start with
  `greater`, the Brier-0.038 subset; add `less`-locked-NO only if it clears).

## What this buys toward "more markets live"

A 3rd live market (weather, lock-in only) on the fastest legitimate path — no unproven probabilistic
model, no settlement-basis fragility, validated by the same shadow→gate discipline as crypto. It does
NOT skip the shadow proof; it shortens it from "calibrate σ̂ OOS" to "confirm fee-edge on decided
contracts," which the accruing quotes can answer in days rather than a season.

## ⛔ VERDICT 2026-06-22 — harness ran: NO-GO (market is efficient on locked contracts)

`scripts/weather_lockin_fee_edge_harness.py` (DB quotes + outcomes + ASOS official-station intraday
temps via `weather_lockin_fetch_asos.py`) ran on 280 settled KXHIGH markets (06-14→06-20, 20 cities):

- **111 markets reached a deterministic lock** (greater: high strictly > floor; less/between: high > cap).
- **111 of 111 had ZERO fee-edge at any post-lock quote.** The best (lowest) winning-side ask after the
  lock was **$1.000 in every single case** (median 1.000, min 1.000). The market reprices the lock-in to
  $1 instantly — you cannot buy the certain side below a dollar, so there is no taker edge to capture.
- Semantics nailed down: KXHIGH "greater" settles YES iff official high **strictly >** floor_strike
  (every boundary settle==floor → NO); the first harness run's 2 "trades" were boundary false-locks
  (one lost). With strict semantics: 0 trades, 0 edge.

This confirms the spec's "honest ceiling": forecast-quality was a GO (Brier 0.0745) but is **profit-
irrelevant** — our model is confident only where the market is already confident (decided contracts),
so profit and calibration never co-occur. Same structural lesson as crypto. **Do NOT take the weather
lock-in live as a taker strategy.** Auto-rooms stay off. Residual angles (all weaker / previously
refuted): maker resting bids below $1 (adverse-selection dominated — sellers only hit you when wrong),
or the pre-lock probabilistic regime (deep research already refuted forecast-vs-market edge there).

## Build order (TDD)

1. RED test: high-so-far YES-lock persistence (gap #1) → GREEN fix in `scoring.py`.
2. RED test: fee/edge gate predicate (locked fair vs market price, net-of-fee) → GREEN helper.
3. Offline lock-in fee-edge harness against accrued `historical_market_snapshots` (no orders).
4. (operator-gated) wire order path; enable per station/strike_type where the gate passes.
