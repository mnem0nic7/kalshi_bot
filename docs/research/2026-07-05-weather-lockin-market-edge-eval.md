# Weather lock-in market-edge eval vs collected KXHIGH quotes (2026-07-05)

**Status:** COMPLETE — verdict below. Leg 3 of the 2026-07-04 live-breadth-expansion design.
**Script:** `scripts/weather_lockin_eval.py` (run inside `infra-app_production_green-1`).
**Question:** since shadow quote collection was re-enabled on 2026-06-22, has the market ever left
fee-clearing taker edge on a KXHIGH contract that was already deterministically decided by the
station's running high? (The lock-in MVP go/no-go, re-asked on the new, larger quote corpus —
the 2026-06-22 harness answered NO-GO on the 06-14→06-20 window.)

## Headline

**0 trades. 144 of 144 deterministically locked markets had the winning-side ask at $1.00 at the
first post-lock snapshot AND at every later snapshot. Net P&L $0.00. 0 lock violations. NO-GO.**

The market reprices a locked KXHIGH contract to exactly $1.00 before (or within one ~70s snapshot
of) the lock becoming knowable from official-station data. There is no taker edge to capture —
not thin edge, not sometimes-edge: literally zero instances in 11 days across 20 cities.

## Data window and quality

- Quotes: `historical_market_snapshots`, `series_ticker LIKE 'KXHIGH%'`, 2026-06-22 → 2026-07-04
  (~535k rows, 20 series × 30 markets each). Cadence during active collection ≈ 70s (far finer
  than the 300s refresh floor the STOP rule required); `yes_bid/ask_dollars` populated. Data is
  usable — the STOP rule did not trigger.
- Observed temps: IEM ASOS official settlement stations (same substrate as `asos_archive.py`),
  343–437 hourly obs per station over the window, all 20 stations fetched successfully.
- Settled markets in window: **480** (24 per city). Reached a deterministic lock with a
  tradeable next-snapshot: **144** (142 LOCKED_NO on `less` markets — high strictly above cap;
  2 LOCKED_YES on `greater` — high strictly above floor). Locks occurred on every one of the 11
  full days (10–16/day). Remainder: 336 never locked by high-so-far alone (undecidable without a
  remaining-rise model → out of MVP scope), 120 markets still unsettled at eval time.

## Method (and the two alignment decisions)

Per settled market: reconstruct the station's running high from ASOS obs (station-local naive
timestamps; quote timestamps converted UTC→station tz), find the first snapshot at which the
contract is deterministically locked, then simulate a taker BUY of the winning side at the **next**
snapshot's ask (honest-fill rule — we never assume the lock-time quote is still there), fee = the
production fee model (`estimate_kalshi_taker_fee_dollars`, 7% × p × (1−p), rounded up to the
cent), gate = the tested `evaluate_lockin_fee_edge_gate` (min edge 2¢, net-of-fee > 0).

Alignment decisions (resolved from real rows/code, not the skeleton's guesses):

1. Strikes/result live in `payload["market"]` (`floor_strike` only on `greater`, `cap_strike`
   only on `less`, `strike_type`, `result` — empty string until settled). No `between` brackets
   exist in this window (0 `-B` tickers); KXHIGH tickers are all `-T<strike>` style.
2. Boundary semantics are **strict**: `greater` settles YES iff official high **>** floor
   (settle == floor → NO), so the lock fires only strictly past the strike — this is the exact
   fix the 06-22 harness needed after 2 boundary false-locks. `less` mirrors (LOCKED_NO once
   high **>** cap).
3. `local_market_day` in the DB is inconsistently formatted (mix of ISO and raw `26JUL02` ticker
   codes) — the market day is parsed from the ticker instead.
4. Station map = the deployed `WEATHER_MARKET_MAP_PATH` YAML (`station_id`, e.g. KXHIGHNY→KNYC
   →IEM `NYC`), identical to `scripts/weather_lockin_fetch_asos.py`.

The headline "$1.00 at fill" and "$1.00 is the floor across the whole post-lock tail" numbers are
produced directly by the committed `scripts/weather_lockin_eval.py` (not a side script): for every
market that reaches a lock, the summary JSON's `ask_at_fill` block records the winning-side ask at
the fill snapshot for all 144 locks (gate-skipped or not), and `post_lock_tail_min_ask` records,
per locked market, the minimum winning-side ask across every snapshot after the lock — both
reported as count/min/max/num_at_100 in the emitted `summary`.

**Data quality — duplicate `asof_ts` rows:** `historical_market_snapshots` has known duplicate
`(market_ticker, asof_ts)` groups (opening-snapshot backfill artifacts, up to several hundred rows
sharing one timestamp). The query now orders `market_ticker, asof_ts, id` so row selection is
deterministic despite them, and the summary's `duplicate_asof_ts_check` reports the group/row
counts for this run (572 duplicate groups, 87,496 extra rows in the 06-22→07-04 window). This was
checked against the result: the 144-lock count and the $1.00 ask-at-fill / post-lock-tail-min
figures reproduce byte-for-byte with the tiebreaker in place, so the duplicates do not affect the
144 lock/fill events analyzed here. (They do occasionally change which duplicate row is picked as
the "latest" metadata snapshot for markets that are still open/uncollected-to-settlement — a
cosmetic shift in the `skipped` breakdown's still-open bucket, not in the settled/locked counts.)

## Results

| metric | value |
|---|---|
| settled markets in window | 480 |
| reached deterministic lock (with next snapshot) | 144 |
| winning-side ask at fill snapshot | **$1.00 in 144/144** |
| minimum winning-side ask at ANY post-lock snapshot | **$1.00 in 144/144** |
| trades passing the fee/edge gate | **0** |
| wins / lock violations | 0 / **0** |
| net P&L after fees | **$0.00** |

Per-city locked counts (all with 0 tradeable): MIA 11, LAX 10, THOU 10, TSFO 10, PHIL 9, TATL 9,
TDC 9, TLV 9, TPHX 9, NY 8, AUS 7, DEN 7, TSATX 7, TSEA 6, CHI 5, TNOLA 5, TOKC 5, TBOS 3,
TMIN 3, TDAL 2.

`lock_violations = 0` holds vacuously (no trades) **and** substantively: in all 144 locked
markets the lock direction agreed with the eventual Kalshi settlement (no sub-$0.99 winning-side
ask ever appeared, and no locked market settled against the lock — the strict-inequality basis
is correct on this window).

## Interpretation

This replicates and strengthens the 2026-06-22 NO-GO (`2026-06-22-weather-lockin-mvp-spec.md`
§VERDICT: 111/111 repriced to $1.00 on 06-14→06-20) on an independent, newer, larger window with
a stricter honest-fill rule: **111 (06-14→06-20) + 144 (this window) = 255/255 locked markets
across both windows, zero fee-edge, ever.**
The snapshot cadence (~70s) upper-bounds how much staleness we could exploit at this collection
rate — but the *minimum over every post-lock snapshot* being $1.00 means there was never a
mispriced quote at any sampled moment, not merely a too-slow reaction. The KXHIGH order book is
efficient on decided contracts; whoever is quoting these markets watches the running high too.

Residual angles remain the ones the spec already rated weaker: maker resting bids below $1
(adverse-selection dominated) and the pre-lock probabilistic regime (deep research previously
refuted forecast-vs-market edge there). Neither is unlocked by this result.

## Go/no-go

Rule: net > 0 with ≥ 20 trades and 0 unexplained lock violations → design the capped pilot;
otherwise keep collecting and revisit.

**Verdict: NO-GO.** 0 trades (rule requires ≥20 and net>0). Do **not** design a lock-in taker
pilot; `*_TRIGGER_ENABLE_AUTO_ROOMS` stays off for weather.

**Revisit:** not calendar-bound — more of the same collection will not change this (two
independent windows, 255/255 at $1.00). Re-run `scripts/weather_lockin_eval.py` only if the
regime plausibly changed (e.g. Kalshi fee structure changes, market-maker withdrawal from KXHIGH,
or a new city/series with thinner books), or if pursuing the maker-side variant with an
adverse-selection model — that would be a new design, not this gate. Suggested earliest recheck:
**2026-10-01** or on any KXHIGH microstructure change, whichever comes first.
