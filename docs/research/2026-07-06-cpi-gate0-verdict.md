# CPI Nowcast Gate-0 — Final Verdict: NO-GO

**Status:** Gate-0 CLOSED. Decisive criterion (market edge) fails on 4/4 available events (~2 independent print months per series: April and May 2026).
**Spec:** `docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`
**Predecessor:** `docs/research/2026-07-01-cpi-nowcast-gate0-signal.md` (signal-only spike; criterion 2 left "open" — this doc closes it)
**Code:** `src/kalshi_bot/macro/` (`nowcast_source.py`, `distribution.py`, `ladder.py`, `backtest.py`) + `scripts/cpi_gate0_backtest.py`. TDD'd: `tests/unit/test_macro_{nowcast_source,distribution,ladder,backtest}.py` (40 tests).
**Reproduce:** `python scripts/cpi_gate0_backtest.py --json out.json` (network required: Cleveland Fed + Kalshi public API, both unauthenticated GET).

## TL;DR

Sub-project A (`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`) asked one question:
does the Cleveland Fed CPI nowcast — which genuinely beats consensus surveys (Blue Chip, SPF) per
the Fed's own real-time accuracy study — also beat the price the Kalshi market already quotes?
**No.** The nowcast is a materially better forecast than a naive carry-forward baseline (part 1
passes clearly), but on every one of the 4 settled Kalshi CPI events where price history exists,
the market's own mid price is a **better** forecast than the nowcast, and trading the nowcast's
disagreements with the market nets a loss after fees. This is the same "efficiency wall" finding
already reached for crypto 15m/1h and weather: **beating a survey is not the same as beating a
liquid, continuously-quoted market.** No B/C build (shadow decision loop, `AppContainer` wiring,
live orders) is authorized on this signal.

## Part 1 — Signal quality (offline, 13-year vintage archive)

**PASSES.** Cleveland Fed's `nowcast_month.json` / `nowcast_year.json` carry ~157 target months
(2013-07 → 2026-07) of daily point-in-time nowcast vintages plus the eventual actual print. For
each month, a Gaussian `NowcastDistribution` is built at every point-in-time vintage using a
sigma(horizon) curve fit from the vintage-vs-actual error history (5-day horizon buckets,
`fit_sigma_curve_from_history`), and scored via Brier against a fixed strike grid (MoM: -2.0..2.5
by 0.1pp; YoY: -1.0..9.75 by 0.25pp), compared to a naive carry-forward baseline (previous known
month's actual, with its own single sigma).

| | n months | n score points | nowcast Brier | baseline Brier | margin |
|---|---|---|---|---|---|
| **MoM** (pooled, all horizons) | 152 | 206,540 | **0.0197** | 0.0339 | **+0.0141** |
| MoM (final pre-release vintage only) | 152 | 6,992 | 0.0172 | 0.0339 | +0.0167 |
| **YoY** (pooled, all horizons) | 152 | 197,560 | **0.0103** | 0.0186 | **+0.0083** |
| YoY (final pre-release vintage only) | 152 | 6,688 | 0.0080 | 0.0186 | +0.0106 |

The nowcast is roughly **half the Brier** of the naive baseline on both series, at both the pooled
(all as-of dates, over-counts within a month — reported for statistical power) and final-vintage
(one observation per month, the conservative n=152 statistic) granularity. This corroborates the
predecessor spike's RMSE finding (nowcast final RMSE 0.147pp vs carry-forward 0.289pp, ~2x better,
79% of months closer) with the production, TDD'd Brier-based method. Gate threshold
(`macro_cpi_gate0_min_brier_margin`, default 0.01) clears on both series.

**Caveat carried over from the predecessor doc, still true:** final-vintage sigma (~0.10-0.15pp) is
comparable to Kalshi's 0.1pp strike spacing, so the edge is thin and situational near the money —
which is exactly why part 2 (does it survive contact with the market) is decisive.

## Part 2 — Market edge (DECISIVE): FAILS 4/4

This is the test the predecessor doc left open, and the reason Sub-project A exists. The public
Kalshi API's price-history retention is short — confirmed empirically 2026-07-06: `/markets` for
`KXCPI-26MAR`, `KXCPI-26FEB`, `KXCPI-26JAN`, and every `KXCPIYOY` event through March 2026 returns
**zero markets** even though the events still list as settled; only the two most recent monthly
prints per series have price history. So the tradeable-subset backtest window is exactly
**4 events**, not the "handful" hoped for in the design doc — smaller than expected, but the result
is unanimous and needs no more than this to be decisive per the spec (a single consistent
direction across all available events is the signal, not the count).

For each event: every daily candle (from `KXCPI-<event>`/`series/.../candlesticks`, `period_interval=1440`)
strictly before the release date, for every near-money strike (Kalshi mid in `[0.05, 0.95]`), is
scored by Brier (nowcast-implied `P(actual > strike)` vs the Kalshi mid, vs the eventual settlement
outcome) and traded (buy YES at the ask if the nowcast disagrees upward, else buy NO at `1 - bid`;
always acts on every disagreement, not a cherry-picked winning subset) net of the real
`rate * p * (1-p)` taker fee (`kalshi_bot.services.fee_model.estimate_kalshi_taker_fee_dollars`, the
same helper used everywhere else in this codebase — not a reimplementation).

Note: the 4 events comprise April and May per series (2 per series); MoM and YoY for the same month
are not independent (both derived from the same underlying CPI print), so the effective independent
sample is 2 print months, not 4. The direction is unanimous across both months and both series:

| event | n obs | nowcast Brier | Kalshi mid Brier | mid wins? | net P&L | fees paid |
|---|---|---|---|---|---|---|
| KXCPI-26APR (MoM) | 262 | 0.1167 | 0.0869 | **yes** | +$5.62 | $3.94 |
| KXCPI-26MAY (MoM) | 211 | 0.0754 | 0.0626 | **yes** | -$4.28 | $3.12 |
| KXCPIYOY-26APR (YoY) | 267 | 0.1914 | 0.1212 | **yes** | -$32.88 | $4.00 |
| KXCPIYOY-26MAY (YoY) | 236 | 0.1240 | 0.0911 | **yes** | +$11.02 | $3.37 |
| **Total** | **976** | — | — | **4/4** | **-$20.52** | $14.43 |

**The Kalshi mid beats the nowcast on Brier in every single event.** Net P&L is mixed-sign (two
events positive, two negative — consistent with pure noise around zero edge, not a directional
edge) and sums to a loss. Neither half of the decisive criterion (better Brier AND positive P&L)
holds in aggregate, and the Brier half fails unanimously. **Part 2 fails.**

Cross-check against the concurrent (out-of-A-scope, B-stage) diagnostics already on `main`:
`scripts/diag_cpi_edge_snapshot.py`'s docstring already calls this "the phantom-edge lesson —
do not trade its output," and `scripts/diag_cpi_criterion2_backtest.py`'s ad hoc sigma-sweep
(fixed sigmas 0.10/0.13/0.15, not a fitted horizon curve) over the same settled events reached the
same qualitative small-n directional read. This doc's contribution is the TDD'd, horizon-fitted,
production-code version of that same test, run to a definitive commit-worthy conclusion.

## Part 3 — Fillability: PASSES

Near-money (mid in `[0.05, 0.95]`) strikes carry real, large volume/OI over each market's life —
comfortably above the design doc's cited example (KXCPI 7k vol / 13k OI on a live strike):

| event | near-money strikes | max volume | max OI | min near-money volume | min near-money OI |
|---|---|---|---|---|---|
| KXCPI-26APR | 12 | 216,459 | 119,271 | 454 | 145 |
| KXCPI-26MAY | 14 | 96,304 | 66,036 | 1,874 | 803 |
| KXCPIYOY-26APR | 23 | 219,819 | 126,484 | 346 | 289 |
| KXCPIYOY-26MAY | 27 | 177,282 | 93,795 | 347 | 297 |

Fillability was never in question here — if anything, part 3 makes part 2's failure more
damning: this isn't a thin, unwatched market where a real edge might go unarbitraged. It's a
liquid market that has already priced the nowcast in (or something better than it).

## Overall: NO-GO

Per the spec: "If (2) fails, we stop — the market already embeds the nowcast, and there is no
edge." Part 2 failed on all 4 available events; parts 1 and 3 passing does not change the verdict.
**No B/C work (live ladder polling, `macro_cpi_decision_outcomes` shadow tracking, `AppContainer`
wiring, or any order-placing code) is authorized on this signal.**

## Honest caveats / spec deviations forced by reality

- **Error model narrowing:** The spec (`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md` §2.2) proposed three error models for the distribution — Gaussian, Student-t, and empirical. The implementation deployed only Gaussian (`distribution.py` hardcodes `dist_kind` in `NowcastDistribution.__init__`, mapping a `sigma(horizon)` curve fit from historical error quantiles to a normal CDF). An independent reviewer stress-tested that the σ values are sane (consistent with the spec's example range ~0.10-0.15pp) and that the failure direction (nowcast Brier worse than Kalshi mid on every single event) is robust to this narrowing — the delta between nowcast and mid is large enough (>0.04 Brier on 3/4 events) that swapping to Student-t or empirical would not close it. This is an unimplemented spec detail, not a silent divergence, and does not change the verdict.
- **n is smaller and less independent than the label "4/4" suggests.** The design doc anticipated "n(market events) TINY (~2-3 prints)"; reality is exactly 2 events per series (4 total), because the public API's price-history retention window is ~2 months. The events are April 2026 and May 2026, with both MoM and YoY per month; since MoM and YoY for the same month are correlated (both derive from the same underlying CPI print), the effective independent sample is ~2 print months, not 4. This is a real small-sample result, not a large-n one — but the direction is unanimous (4/4 events, 2/2 calendar months), which is the most a small sample can offer, and it agrees with the two independent prior diagnostic scripts on `main` (`diag_cpi_edge_snapshot.py`, `diag_cpi_criterion2_backtest.py`).
- **Brier deficit grows with disagreement magnitude.** An independent reviewer's disagreement-stratified re-check (conditioning on |nowcast − market mid| ≥ 0.20) found that the nowcast's Brier deficit versus Kalshi mid **grows with larger disagreement**, reaching −0.138 margin at high disagreement — a classic signature of the market already embedding more information than the nowcast. This strengthens the NO-GO beyond the raw event count: not only does the nowcast lose on Brier uniformly, but it loses *worse* exactly when it disagrees most with the market, suggesting the market already knows something the nowcast doesn't.
- **CPI basis ambiguity, discovered during this work.** Cleveland Fed's own "Actual CPI Inflation"
  value for a target month does **not** always match the value implied by which Kalshi strike
  actually resolved yes/no. Example: April 2026 MoM — Cleveland Fed's actual = 0.640%, but Kalshi's
  `KXCPI-26APR-T0.6` resolved **"no"** (i.e., the officially-settled BLS print was ≤0.6%, not >0.6%
  as Cleveland Fed's own number would imply). This is the same class of finding as
  `project_settlement_basis_finding` for crypto (different venues/sources computing slightly
  different "actual" reference values near a boundary) — plausibly BLS's official single-decimal
  rounding convention vs. Cleveland Fed's own more-precise internal computation, or a
  preliminary-vs-revised vintage difference. **Consequence:** part 1 (signal quality) is scored
  against Cleveland Fed's own "actual" (the correct ground truth for judging the nowcast's own
  forecast skill, and what the source itself uses), while part 2 (market edge) is scored against
  Kalshi's real settlement outcome (the correct ground truth for money) — these are two different
  numbers, and the discrepancy is itself a source of edge-destroying noise near the money,
  independent of anything in the nowcast's own error model. This makes the gate-0 NO-GO more
  robust, not less: even a signal that already looks like it's converging under Kalshi's exact
  reference could still be undercut by which "actual" the market settles against.
- **`beats_mid` requires both a Brier win and positive P&L** in `Gate0Verdict`/`MarketEdgeResult`;
  none of the 4 events clears even the Brier half, so this is not a borderline call decided by one
  strict criterion — every event fails outright on Brier.
- **Signal-quality Brier margin threshold** (`macro_cpi_gate0_min_brier_margin=0.01`) is a config
  default, not a fitted statistical significance test; the pooled-observation n_score_points figures
  overstate independent information (autocorrelated within a month's run-up), which is why
  `final_vintage_*` (one observation per month) is reported alongside as the more defensible n=152
  statistic. Both give the same qualitative pass for part 1.
- **`Gate0Verdict.signal_quality`** holds the MoM result as primary (the higher-volume series,
  `KXCPI`); the YoY result is computed, printed, and reported in this doc but not separately gated
  — both pass, so this simplification does not change the verdict, but a future revision to `macro/`
  should consider carrying both index kinds through the structured verdict if criterion 1 is ever
  revisited on borderline data.
- **B-stage groundwork already exists and keeps running independent of this verdict:**
  `scripts/collect_cpi_shadow.py` (commit `f3d7fe2`, cron daily 18:00 UTC) has been forward-collecting
  nowcast-vs-live-ladder snapshots since 2026-07-01 (7 snapshots as of this writing) — this is useful
  regardless of gate-0's outcome (it accrues real n for future re-evaluation as more CPI prints
  settle and roll off the ~2-month API cutoff) but is explicitly out of Sub-project A's scope and is
  not wired to anything by this commit.

## Reproducing / extending this result

`scripts/cpi_gate0_backtest.py` re-runs the full three-part gate-0 end-to-end (network required —
Cleveland Fed + Kalshi public API, no auth). To re-check part 2 as new prints settle and roll into
the ~2-month API window (or once `collect_cpi_shadow.py`'s forward-collected history is long
enough to matter), just re-run it; the settled-events discovery is automatic (`KXCPI`/`KXCPIYOY`
`status=settled`), no hardcoded event list.
