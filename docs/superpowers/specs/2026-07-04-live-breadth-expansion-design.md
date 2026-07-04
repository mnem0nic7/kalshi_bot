# Live-breadth expansion: 15m / 1h / weather / commodities — design (2026-07-04)

Operator ask: "how do we get more live 1hour 15min weather commodities" —
clarified to: get more markets trading live across 1h crypto, 15m crypto, and
weather, AND scope Kalshi commodity markets (oil/gas/gold) as a new vertical.
Risk posture (operator choice): **capped pilots are an acceptable path to
live**; the full gate chain (trained champion + replay gate) remains the path
to uncapped size.

Approach chosen: **A — edge-replication first** (of three: A edge-replication,
B gate-chain-only, C pilots-everywhere). Rationale: the four verticals are
blocked by different things — 15m by edge, 1h by trainer infrastructure,
weather by an unproven market-edge gate, commodities by zero scoping. The
stale-quote taker edge (docs/research/2026-07-02-stale-quote-taker-edge.md) is
the only fee-clearing OOS-validated edge in the system; replicating it is
cheaper than proving new edges, and every extension stays inside a hard
dollar cap.

## Leg 1 — 15m stale-quote pilot extension (HYPE, DOGE, ETH)

Add HYPE, DOGE, ETH to the pilot's asset list (currently BTC, BNB in
`scripts/stale_quote_pilot.py`). All three are backtest-positive: per-contract
at ≥10¢ fresh-move threshold — HYPE +3.8¢ (n=1933), DOGE +2.9¢ (n=2425); ETH
positive in the cross-sectional holdout. SOL was ≈flat and is excluded.

**Risk budget stays flat.** The existing global caps are unchanged and shared
across all 5 assets: $3 daily stop, 10 trades/day, 1 open position at a time,
≤$0.75 entry, 1 contract, 15¢ credible-edge ceiling. This widens the
opportunity funnel without widening worst-case exposure.

**Per-asset kill rule:** drop any asset that reaches ≥$2 cumulative negative
after ≥15 settles while the others are positive. The pilot JSONL gains a
per-asset rollup in the evaluation readout (records already carry the ticker;
this is an analysis change, not a logging change).

## Leg 2 — 1h crypto (two parallel tracks)

### 2a. Stale-quote scanner on hourly brackets — shadow first

Run the pilot's signal scan against 1h bracket tickers in **signal-only mode**
(no orders; emit records with a `mode: "shadow_1h"` marker). Hypothesis:
hourly books go stale more than 15m books (fewer participants spread across
~50-strike ladders) — to be measured, not assumed.

- **Universe bound (mandatory):** nearest ±N strikes to spot only (N≈5). The
  full hourly ladder is the working-set churn that drove the crypto_1h daemon
  to its 8g cap (docs/operations/2026-07-02-daemon-reconcile-wedge.md); the
  scanner must never walk the whole ladder.
- **Graduation:** ≥1 week of shadow signals joined to settlements. If the
  edge clears fees OOS with the same three-check rigor as the 15m validation
  (dedup, threshold monotonicity, time-OOS), it graduates to a capped live
  pilot **inside the same global risk budget as Leg 1** (shared $3/day stop),
  pending operator go.

### 2b. Trainer 1h fit OOM fix

The 1h candidate report OOMs the 32g trainer cgroup because it fits all
models × 4 walk-forward folds on the much larger 1h sample set (CLAUDE.md,
2026-06-19). Fix: per-frequency fit bounds — new settings (names final at
implementation) for a 1h max-training-row cap and 1h fold count (4 → 2);
15m fitting is untouched. Then restore
`CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES=15m,1h` on the trainer and watch memory
through one full asset cycle before calling it fixed.

This restarts the *gate path* for 1h model trading. It does not shortcut any
gate: 1h model live trading still requires a champion to pass the replay gate.

## Leg 3 — Weather lock-in market-edge evaluation

The daemon has collected intraday KXHIGH quote snapshots since 2026-06-22
(`WEATHER_RESEARCH_REFRESH_INTERVAL_SECONDS=300`). Offline job (light,
quote-table-scale — safe on the prod host):

- Simulate the intraday high-so-far lock-in strategy (buy YES on a bracket
  once the observed running high makes settlement near-certain) against the
  collected quotes.
- **Honest fill model:** fill at the *next* snapshot's ask after the signal
  (not signal-time), full fees. If the edge only exists with optimistic
  fills, it fails.
- Deliverable: edge after fees **vs the market**, as a number, in a research
  doc. Positive → design a capped standalone pilot (stale-quote-pilot style;
  `*_TRIGGER_ENABLE_AUTO_ROOMS` stays false — the pilot path deliberately
  bypasses the auto-room machinery, per the existing safety rule). Negative →
  keep collecting, no live weather.

## Leg 4 — Commodities scoping spike (research only)

Kalshi's Commodities category includes daily series (KXBRENTD Brent,
KXCOPPERD copper, KXAAAGASD US gas) and weekly series (KXNGASW natgas,
KXWHEATW wheat, KXSTEELW steel, KXCOCOAW cocoa) — no intraday. Spike
deliverable: a ranked go/no-go research doc measuring per series:

- settles per week, volume / open interest, typical spread,
- settlement source and rules,
- whether a **fresh underlying feed** exists (ICE Brent, COMEX copper,
  AAA/EIA gas) that supports the fair-value-vs-stale-book angle.

No code in the trading path, no orders, no new subsystem until the doc says
go. Prior scoping context: the 2026-06-26 instrument sweep found data-rich
families efficient and behavioral ones too sparse; commodities were not
covered and sit in between (daily settles + a real underlying).

## Sequencing

1. Leg 1 ships first (small diff to the pilot script + restart).
2. Leg 2a scanner and Leg 3 evaluation run this week in parallel.
3. Leg 2b is its own commit + trainer redeploy.
4. Leg 4 fills slack; target the doc within the week.

## Risks

- Shared caps across 5 assets slow per-asset sample accumulation — accepted,
  safety first.
- Hourly scanner memory — mitigated by the ±N-strike universe bound.
- Weather sim overstating fillability at 300s cadence — mitigated by the
  next-snapshot fill rule.
- Live-fill risk on any stale-quote extension (latency-arb trap) is the same
  as the running pilot; the caps are the containment.

## Success criteria

- Leg 1: 3 new assets live under unchanged risk budget; per-asset W/L visible.
- Leg 2a: a measured yes/no on hourly stale-quote edge within ~1 week of
  shadow data.
- Leg 2b: trainer completes a full 15m+1h cycle under 32g with fresh 1h
  artifacts.
- Leg 3: a fee-adjusted market-edge number for weather lock-in from data we
  already own.
- Leg 4: a ranked commodities go/no-go doc.
