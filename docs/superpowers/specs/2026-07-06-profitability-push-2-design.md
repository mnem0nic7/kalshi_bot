# Profitability push #2 — design (2026-07-06)

Operator ask: "anything else we can do to improve profitability." All four
proposed levers selected; approved with the explicit $6 daily-stop note.
Excluded because already in flight: 1h shadow graduation (~07-11),
basis-aligned labels (~07-22), TP-scaling evaluation (needs ~a week of ops
events). Prior push: `2026-07-04-live-breadth-expansion-design.md`.

## Leg 1 — Scale the stale-quote pilot (ships first)

Evidence: ~58% win rate ex-BTC across the live sample; BTC dropped 2026-07-06
by kill rule; DOGE/HYPE/ETH/BNB remain.

Cap changes (watchdog env, `scripts/stale_quote_pilot_watchdog.sh`):
- `STALE_PILOT_CONTRACTS=2` (new env; script currently hardcodes 1-contract
  tickets via `build_pilot_ticket` — add a count parameter, default 1)
- `STALE_PILOT_MAX_OPEN=2` (was 1)
- `STALE_PILOT_DAILY_LOSS_STOP=6.0` (was 3.0 — 2× contracts requires 2× stop;
  operator approved the doubled worst case ≈ $180/mo if every day maxed)
- Daily budget windowed: `STALE_PILOT_MAX_TRADES_PER_WINDOW=5` with two 12h
  UTC windows (00–12, 12–24) replacing the flat 10/day — stops the overnight
  session from consuming the whole budget before US hours. Guard logic in
  `kalshi_bot/crypto/stale_quote_pilot.py` (TDD; `PilotState` gains
  window-scoped trade counting; flat `max_trades_per_day` remains as a
  fallback when the window env is unset).

Unchanged: 15¢ credible-edge ceiling, $0.75 max entry, per-asset kill rule
(−$2 @ ≥15 settles with positive peer — now evaluated on 2-contract nets,
i.e. the rule keys on dollars, not settles×contracts).

**XRP admission is conditional:** run the same fresh-tick recompute backtest
that validated BTC/BNB/HYPE/DOGE (`docs/research/2026-07-02-stale-quote-taker-edge.md`
methodology, most recent 2 weeks of feature rows) for XRP. Positive net of
fees at ≥0.10 threshold with a non-trivial sample → add XRP to the allowlist
in the same deploy; otherwise XRP stays out and the result is recorded in the
research doc.

## Leg 2 — CPI nowcast gate-0 (offline; July print ~07-15 is the live test)

Implement gate-0 exactly per the committed spec (`69e456f`,
`docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`): point-in-time
Cleveland Fed daily nowcast vintages (nowcast_month.json, UA-gated) joined to
Kalshi KXCPI / KXCPIYOY market prices at matching timestamps, fee-adjusted
edge vs the MARKET (beating SPF/consensus does not count). Deliverable: a
research doc with the measured edge and an explicit gate-0 pass/fail. No
trading code, no shadow loop yet — gate B (shadow) is a separate decision on
a passing gate-0.

## Leg 3 — Pyth auth check, then Brent shadow collector

Step 1 (fact-check, ~30 min): determine Pyth Hermes post-2026-07-31 auth
pricing from official docs. If a free/cheap tier does not cover ~1 req/s
Brent spot, the Brent leg STOPS here and the finding goes in the commodities
scoping doc.

Step 2 (only if viable): `scripts/brent_stale_shadow.py` — signal-only JSONL
collector in the stale-quote-shadow mold: Pyth Brent spot vs KXBRENTD book,
staleness detection reusing the same dfair/stale-quote structure adapted to a
daily-settle market (parameters chosen at implementation from the first
day's observed book cadence), NO order path, NO ExecutionService import.
Graduation to any live Brent order requires its own validated backtest +
operator go.

## Leg 4 — Maker-side stale-quote shadow (signal-only)

The taker pilot currently discards `live_edge_too_small` rejects (~16/day —
signals where the edge at the live book is under the 3¢ taker floor). Log
the maker counterfactual instead of discarding: on each such reject, record
where a resting bid at `fair − 3¢` (side-appropriate) would sit vs the live
book, then at settlement record whether that price was ever traded through
(fill proxy: subsequent snapshot/settle path) and the resulting markout.
Implementation lives inside `scripts/stale_quote_pilot.py`'s reject branch
(emit an enriched record; a small offline analyzer summarizes maker-fill
proxy rate + markout). Zero new order risk. Live maker orders are OUT OF
SCOPE — separate design gated on this shadow showing positive markout.

## Sequencing

1. Leg 1 (caps + windowed budget + XRP backtest) — immediately.
2. Leg 3 step 1 (Pyth pricing check) — early, kills or confirms Brent cheaply.
3. Leg 2 (CPI gate-0) — this week, ahead of the ~07-15 CPI print.
4. Leg 3 step 2 (Brent collector, if viable) and Leg 4 (maker shadow).

## Risks

- Doubled contracts double per-trade variance; the $6 stop and unchanged
  entry/ceiling gates bound it. Approved explicitly.
- 2 open positions weakens the correlated-exposure guard (signals cluster on
  the same spot move); mitigated by per-window budget and the kill rule.
- Maker shadow's fill proxy (traded-through) overstates real maker fills
  (queue position ignored) — the analyzer must label it an UPPER BOUND.
- CPI sample is small (monthly prints); gate-0 is calibration-style evidence,
  not a large-n backtest — the doc must say so.

## Success criteria

- Leg 1: new caps live; XRP verdict recorded; per-window budget observable in
  pilot JSONL guards.
- Leg 2: gate-0 pass/fail number committed before the July CPI print.
- Leg 3: Pyth cost answer documented; if GO, Brent signals accruing.
- Leg 4: maker counterfactual records accruing with an analyzer readout.
