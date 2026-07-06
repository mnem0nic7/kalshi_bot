# Stale-quote taker edge on 15m crypto (2026-07-02)

**Status:** OOS-validated in backtest on 3 independent checks. **Live fill reality UNPROVEN** — the
one remaining link before any live deployment. No orders placed; research only.
**Scripts:** `scripts/diag_stale_quote_edge.py` (materialized-row window), `scripts/diag_stale_quote_oos.py`
(fresh-tick recompute for pre-v15 windows), `scripts/xrp_stale_backtest.py` (2026-07-06 per-asset
extension: same fresh-tick method + one-trade-per-market dedup, used for the XRP verdict below).

## Hypothesis (from the settlement-basis study's residual)

Residual 15m edge is "stale-quote-on-alts": when spot moves sharply but a thin Kalshi quote hasn't
updated, crossing the quote in the spot's direction is positive-EV without out-forecasting anyone —
it's reaction speed vs a slow MM, not prediction.

## Rule (all information point-in-time; no lookahead)

For consecutive same-market feature-row snapshots (~18s cadence, gap ≤ 150s, 90s ≤ ttc ≤ 870s,
spread ≤ 20¢): compute `dfair` = change in analytic vol fair value `Φ(ln(S/K)/(σ√τ))` from the spot
move alone (fresh tick spot; σ, τ held at current values). Signal when `|dfair| ≥ 0.10` **and** the
quote mid moved ≤ 1¢ over the same interval ("stale"). Trade: cross the current top-of-book (buy YES
at ask if dfair>0, else buy NO at 1−bid), hold to settlement. Fee = `0.07·p·(1−p)`.

## Evidence (three independent checks, all net of fees)

1. **Recent week (6/25–7/2), v15 materialized rows, threshold selected here** (in-sample for the
   threshold): stale-conditioned +1.8¢/ct @ ≥0.10 (n=1782), +3.4¢ @ ≥0.15 (n=1103); baseline
   (same signal, quote moved) −0.6¢/+0.2¢. Conditioning on quote-lag is what pays.
2. **Cross-sectional holdout, same week, assets not used to pick the threshold** (SOL/BNB/ETH/BTC):
   stale +1.1¢ @ ≥0.10 (n=3232), +2.3¢ @ ≥0.15 (n=1800); baseline ≈ 0. Confirms across assets
   (SOL ≈ flat; DOGE weakest).
3. **True time-OOS, prior disjoint week (6/18–6/25), fresh-tick recompute** (pre-v15 rows have frozen
   candle spot, so spot context was recomputed from raw `crypto_spot_ohlc` ticks exactly as the
   freshfix diagnostic): per-contract at ≥0.10 — BTC +6.7¢ (n=2600), BNB +5.5¢ (n=2133), HYPE +3.8¢
   (n=1933), DOGE +2.9¢ (n=2425). Positive on all 4 assets, monotone in threshold, stale ≥ base.

**Dedup realism (one trade per market — clustering removed), OOS week, ≥0.10:**

| asset | trades | net $/wk (1 ct) | avg/ct | win% |
|-------|--------|-----------------|--------|------|
| BTC   | 602    | +33.16          | +5.5¢  | 59%  |
| BNB   | 550    | +19.86          | +3.6¢  | 59%  |
| HYPE  | 520    | +18.19          | +3.5¢  | 58%  |
| DOGE  | 533    | +5.71           | +1.1¢  | 57%  |
| **total** | **2205** | **+76.92** | **+3.5¢** | **58%** |

## Why this can coexist with "mids are efficient"

The mid is efficient *conditional on being fresh*. This rule only fires in the ~40% of moments when
the quote failed to react to a spot move within ~18s — exactly the moments excluded from the
model-vs-mid comparisons (which score against the *current* quote whatever its age). It is a taker
latency/reaction edge, not a forecasting edge, so it does not contradict the v15 champion result.

## Honest caveats (in order of severity)

1. **Fill reality is unproven and is the classic latency-arb backtest trap.** The backtest crosses
   the snapshot's top-of-book at the snapshot timestamp. Live: the quote may be pulled before our
   order lands, top size may be < wanted size, and we race other takers. The stale premise (MM slow
   for ≥18s) argues fills exist, but only a live test settles it. **Expect live capture to be a
   fraction of backtest capture.**
2. Two weeks, one vol regime (June/July 2026). Decay risk: as Kalshi MMs speed up, this shrinks.
3. Snapshot cadence ~18s is our *detection* latency in backtest; the live loop must react at least
   this fast at signal time (it already runs continuously on the active color).
4. DOGE persistently weakest (thicker/faster MM?) — per-asset gating needed.
5. σ comes from the same feature pipeline; miscalibrated σ shifts the threshold meaning per asset.

## Proposed path (operator decision required)

1. **Shadow first** (no orders): log live would-trade signals with the quote captured at decision
   time; settle against outcomes. Proves signal timing/frequency live; still can't prove fills.
2. **Micro-capped live pilot** (operator authorization, like the 2026-06-29 $10 SOL experiment):
   1-contract taker IOC orders, strict daily cap, BTC+BNB only. Directly measures fill rate + realized
   capture vs backtest. This is the only way to close the fill question.
   **BUILT (default OFF):** `scripts/stale_quote_pilot.py` + TDD'd guards in
   `kalshi_bot/crypto/stale_quote_pilot.py` (tests: `tests/unit/test_stale_quote_pilot.py`). Orders
   route ONLY through `ExecutionService` (kill-switch/color/creds rails). Without `STALE_PILOT_ENABLED=1`
   it dry-runs the full path as `shadow_skipped`. Enabling requires explicit env:
   `STALE_PILOT_ENABLED=1 STALE_PILOT_ASSETS=BTC,BNB STALE_PILOT_MAX_TRADES_PER_DAY=10
   STALE_PILOT_MAX_OPEN=1 STALE_PILOT_DAILY_LOSS_STOP=3.0 STALE_PILOT_MAX_ENTRY=0.75` —
   all caps default to 0 (refuse).
3. If live capture ≥ ~half of backtest: productionize as a new event-triggered strategy in the
   autonomy loop (NOT the champion-model path), with per-asset enable, replay-gate-style shadow
   accounting, and the existing kill-switch/risk rails.

## XRP verdict (2026-07-06)

XRP was never tested on 15m in the original three checks (only BTC/BNB/HYPE/DOGE + a SOL/ETH
cross-sectional holdout) — flagged for a look because XRP shows +$7.72 in the separate 1h paper
shadow. Ran the **exact validated fresh-tick-recompute method** (`scripts/diag_stale_quote_oos.py`'s
approach — recomputes point-in-time spot context from raw `crypto_spot_ohlc` ticks so materialized
pre-v15 frozen-candle-spot rows don't collapse `dfair`), same detection constants
(`VOL_MODEL`=`vol_normal_fair_value`/`spot_realized_volatility_32`, `MAX_GAP_S=150` i.e. ~18s cadence,
`QUOTE_EPS=0.01`, `DFAIR_GRID` incl. 0.10/0.15) as `scripts/stale_quote_pilot.py`, in
`scripts/xrp_stale_backtest.py` (new script — the "one trade per market" dedup used for the
doc's dedup-realism table was ad hoc, not committed as a script, so it was rebuilt here on top of
the committed fresh-tick method rather than invented from scratch). Ran inside
`infra-app_production_green-1` (DB access) via `docker cp` + `docker exec`.

**Primary window — most recent 2 weeks (06-22 → 07-06), dedup one trade/market:**

| threshold | n (dedup trades) | net $ (1 ct) | avg/ct | win% |
|-----------|-------------------|--------------|--------|------|
| dfair≥0.10 | 746 | +27.91 | +3.74¢ | 59% |
| dfair≥0.15 | 654 | +38.96 | +5.96¢ | 61% |

Baseline (same signal, quote not conditioned on staleness) at ≥0.10 was **negative** (n=806,
net −23.19, avg −2.88¢, win 55%) — same pattern as the other assets: conditioning on quote-lag is
what pays, not the raw spot-move signal.

**Robustness check — disjoint prior 2 weeks (06-08 → 06-22), same method, same script:**

| threshold | n (dedup trades) | net $ (1 ct) | avg/ct | win% |
|-----------|-------------------|--------------|--------|------|
| dfair≥0.10 | 1082 | +17.25 | +1.59¢ | 55% |
| dfair≥0.15 | 948  | +34.97 | +3.69¢ | 56% |

Positive at ≥0.10 and ≥0.15 in **both** independent 2-week windows, n well above the ~200 dedup-trade
bar in both. Weaker than BTC/BNB/HYPE (3.5–5.5¢/ct historical) but stronger than DOGE (1.1¢/ct, the
previously-weakest asset) at ≥0.10, and comparable to the pack at ≥0.15. Monotone win-rate improvement
with threshold, consistent with the mechanism (larger stale mispricings are easier to detect
correctly).

**Verdict: ADD.** Positive dedup net at ≥0.10 with n=746 (≥~200 bar) in the primary window, confirmed
in a disjoint OOS window. `XRP` appended to `STALE_PILOT_ASSETS` in
`scripts/stale_quote_pilot_watchdog.sh` (now `BNB,HYPE,DOGE,ETH,XRP`); pilot restarted under the
existing operator-authorized caps (unchanged: `STALE_PILOT_MAX_TRADES_PER_DAY=10`,
`STALE_PILOT_MAX_OPEN=2`, `STALE_PILOT_DAILY_LOSS_STOP=6.0`, `STALE_PILOT_MAX_ENTRY=0.75`,
`STALE_PILOT_CONTRACTS=2`). Same fill-reality caveat as every other pilot asset applies (honest
capture is unproven until live fills accumulate); XRP inherits the shared per-asset kill-rule readout
in `kalshi_bot.crypto.stale_pilot_readout`.

## Edge decay watch (2026-07-06)

While validating XRP (above), the same rebuilt fresh-tick backtest (`scripts/xrp_stale_backtest.py`)
was re-run per asset across the two disjoint 2-week windows already in the XRP table, and the
comparison surfaced **observed** decay, not just the theoretical risk flagged as caveat #2:

| asset | prior window (06-08→06-22) avg/ct | newest window (06-22→07-06) avg/ct | direction |
|-------|-----------------------------------|--------------------------------------|-----------|
| DOGE  | +3.70¢                            | **−0.23¢**                           | flipped negative |
| BTC   | (positive, see body above)        | **−0.30¢**                           | softened, flipped negative |
| BNB   | —                                 | +5.02¢                               | still strong |
| HYPE  | —                                 | +5.76¢                               | still strong |

DOGE went from the third-strongest asset in the original checks to net-negative in the current
window. BTC softened the same way in the current window and was **independently kill-ruled out of
the live pilot the same day** (per-asset kill-rule readout in `kalshi_bot.crypto.stale_pilot_readout`)
— two independent signals (backtest decay + live kill rule) agreeing on the same asset the same day.
BNB and HYPE remain strong in the current window, so this is per-asset, not a blanket edge collapse.

**Interpretation:** caveat #2 ("decay risk: as Kalshi MMs speed up, this shrinks") is now an observed
fact for at least one asset (DOGE) per rebuilt window-over-window comparison, with BTC corroborating
via the independent live kill rule. The edge is not static — it needs to be re-measured on a cadence,
not assumed from the original three checks.

**Mitigation — weekly cross-asset recheck.** `scripts/stale_edge_weekly_recheck.sh` reuses the
active-color resolution block from `scripts/stale_quote_pilot_watchdog.sh` and runs
`scripts/xrp_stale_backtest.py` (read-only, no orders — it only queries settled feature rows + spot
ticks) inside the active app container for each of `BNB HYPE DOGE ETH XRP`, appending output to
`/home/user1/kalshi_stale_pilot/edge_recheck/<date>.log`. Scheduled Monday 08:00 UTC (`crontab`:
`0 8 * * 1 .../scripts/stale_edge_weekly_recheck.sh >> /tmp/stale_edge_recheck_cron.log 2>&1`) —
deliberately after the Sunday 09:00 UTC VACUUM FULL window, not competing with it. This is a
detection-only recheck (no automatic pilot asset removal); an operator reviews the log and drops any
asset whose current-window avg/ct goes negative, same as the BTC kill-rule precedent above.
