# CPI Nowcast — Sub-project A: Offline Nowcast + Gate-0 Proof

**Date:** 2026-07-01
**Status:** Design — awaiting review
**Author:** Claude (Opus 4.8) + operator
**Related memory:** `project_instrument_scoping`, `project_settlement_basis_finding`

## Why this exists

Every crypto/Kalshi market we tested (crypto 15m/1h, financials, commodities, macro daily) is
efficient/arb-free: no deployable positive-EV edge for a small-bankroll automated system. The full
v15 crypto retrain confirmed no champion beats the mid near the money, and the fee `rate·p·(1−p)`
peaks exactly where volume is. The one avenue with a *plausible* informational edge is a **macro
nowcast**: trade a slow-moving macro release against a signal the crowd underuses.

The operator chose **CPI**, priced against the **Cleveland Fed Inflation Nowcast**. The Cleveland
Fed's own real-time assessment (1999–2022) shows its CPI nowcast beats both the Blue Chip consensus
and the Philadelphia Fed SPF (smaller RMSE across all four months of the quarter), so the *signal*
is real. **What no published result answers — and what this sub-project must settle — is whether the
Kalshi CPI market already embeds that nowcast.** Beating surveys ≠ beating the market. This
sub-project measures nowcast-vs-market and produces a hard go/no-go before any trading code is written.

## Scope

**In scope (Sub-project A):**
- Ingest the Cleveland Fed CPI nowcast (headline, month-over-month **and** year-over-year), with
  point-in-time daily vintages persisted for lookahead-free backtesting.
- Build a per-strike probability distribution over the Kalshi `KXCPI` (MoM) and `KXCPIYOY` (YoY)
  ladders from the nowcast + a calibrated nowcast-error model.
- Gate-0 backtest: (a) signal quality — nowcast Brier vs actual BLS CPI, vs a consensus baseline;
  (b) **market edge** — nowcast-implied ladder vs Kalshi-implied prices, net of fees, on the
  tradeable subset. Emit a single go/no-go verdict.

**Explicitly OUT of scope (later sub-projects, separate specs):**
- B: live Kalshi ladder integration, shadow decision tracking, `AppContainer` wiring for a live loop.
- C: live order placement (routes through the existing `ExecutionService` + risk engine only after B).
- Core CPI, PCE, payrolls, or any non-CPI series.
- Any change to crypto or weather paths.

## Success criteria (the go/no-go)

Gate-0 **passes** only if, on the reconstructed point-in-time history:
1. **Signal:** nowcast-implied probabilities are better-calibrated than a naive baseline (last-release
   carry-forward or trailing-mean) against actual BLS outcomes — Brier lower, by a margin outside noise.
2. **Market edge (decisive):** nowcast-implied ladder beats Kalshi-implied prices on the tradeable
   (near-money) subset — positive simulated net-of-fees P&L using `rate·p·(1−p)` fees + observed
   spread, and better Brier-vs-outcome on that subset than the Kalshi mid.
3. **Fillability sanity:** the near-money strikes that generate the edge actually have volume/OI
   (from the live scan: `KXCPI` had 7k vol / 13k OI on a live strike; confirm this holds per event).

If (2) fails, we **stop** — the market already embeds the nowcast, and there is no edge. This is the
whole point of doing A before B/C.

## Architecture

New self-contained subsystem `src/kalshi_bot/macro/`, mirroring the structure of `crypto/` but far
smaller. Sub-project A adds **no** `AppContainer` wiring and **no** trading code — it is libraries +
a backtest script, unit-tested, runnable offline.

### Components

**`macro/nowcast_source.py` — `ClevelandFedNowcastSource`**
- Fetches the daily Cleveland Fed CPI nowcast: current target month's headline CPI, as MoM % and
  YoY %. (One nowcast of the current-month CPI index yields both: MoM vs last month's known actual,
  YoY vs the month-12 known actual. Cleveland Fed also publishes both directly.)
- **Verified data source (2026-07-01 feasibility probe):** the primary endpoint is
  `https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json`
  (siblings: `nowcast_quarter.json`, `nowcast_year.json`). **UA-gated** — a browser `User-Agent`
  returns 200; naive fetchers (incl. WebFetch) get **403**. No auth / API key. Clean JSON.
- **Structure (verified):** a list of ~157 frames, **one per target month from 2013-07 to the current
  month**. Each frame's x-axis is the **daily nowcast path over that month's run-up** (x-labels are
  dates like `03/02, 03/03, …` interleaved with `CPI Feb` / `PCE Feb` actual-release markers), across
  8 series: `CPI Inflation`, `Core CPI Inflation`, `PCE Inflation`, `Core PCE Inflation`, and the four
  matching `Actual …` series. **Values are MoM %** (e.g. `0.1333` = 0.13% MoM). This file is
  **inherently point-in-time** — each x is a date, so it *is* the lookahead-free vintage archive.
- **Consequence for A:** ~13 years of daily point-in-time CPI-nowcast vintages + actuals are available
  in a single fetch — the signal-vs-actual backtest needs **no forward-collection** to start. Parse the
  frames into `NowcastReading(as_of_date, target_month, index, mom_pct, source, raw)` rows and persist
  them (idempotent) so later daily fetches append new dates.
- Fallbacks (config-ordered, only if the primary format breaks): the DOI research dataset
  (`10.26509/frbc-inflationnowcast`), FRED (if a nowcast series exists), MacroMicro mirror. One adapter
  per source behind a single interface; first to answer wins. Never silently fall back to a stale/None
  value — that is the stale-spot class of bug; fail loud with per-source status.
- **Point-in-time discipline (the crypto stale-spot lesson):** the backtest reads the nowcast *as it
  was known on each date*, never a later value. The frame x-axis already encodes this; assert it.
- **Data-gap caveat:** the source publishes methodology notes for missing-CPI months (e.g. the
  Oct/Nov 2025 CPI data gap) — the parser must tolerate months with absent actuals rather than crash.

**`macro/distribution.py` — `NowcastDistribution`**
- Input: nowcast point estimate (MoM and YoY) for a target month + days-to-release.
- Nowcast-error model: `σ(horizon)` fit from vintage-vs-actual history (error shrinks toward release).
  Start with a Gaussian; gate-0 compares Gaussian vs Student-t vs empirical-error CDF and keeps
  whichever calibrates best (fat tails likely matter — cf. the settlement-basis finding).
- Output: for each ladder strike `k`, `P(value > k) = 1 − F((k − point)/σ(horizon))`. Monotonic by
  construction from a single CDF; assert monotonicity explicitly (empirical mode can violate it).
- Pure function of inputs — no I/O, trivially unit-testable.

**`macro/ladder.py` — Kalshi ladder parsing (read-only helpers)**
- Parse `KXCPI-<MON>` and `KXCPIYOY-<MON>` events into sorted strike ladders with
  `(strike, yes_bid, yes_ask, mid, vol, oi)` using the correct fixed-point/dollar fields
  (`yes_bid_dollars`, `yes_ask_dollars`, `volume_24h_fp`, `open_interest_fp` — the field-name gotcha
  from the scan). Maps the contract's `strike_type=greater` / `floor_strike` to the "above X%"
  semantics. GET-only; no order fields touched.

**`macro/backtest.py` — gate-0 evaluation (script + importable fn)**
- For each historical target month, walk the point-in-time vintage path; at each date build the
  nowcast-implied ladder probabilities; compare to (i) the eventual BLS actual (Brier, calibration)
  and (ii) Kalshi-implied ladder prices where history exists / forward-collected.
- Simulates net-of-fees P&L on the tradeable subset with the real fee model + observed spread.
- Prints the two metrics + the go/no-go verdict. No trading, no DB writes beyond reading vintages.

### Storage

One new table (Alembic migration), append-only:
- `macro_nowcast_vintages(id, as_of_date, target_month, index_type, mom_pct, yoy_pct, source,
  raw_payload, created_at)`, unique on `(as_of_date, target_month, index_type)`.

No decision/outcome tables in A (those belong to B's shadow tracking). **The nowcast history is NOT
thin** — the monthly JSON carries ~13 years of daily point-in-time vintages, so the signal-vs-actual
backtest runs fully offline today. What *is* thin is the **Kalshi ladder price history** (API cutoff
~2 months) — so the decisive market-edge metric can only be measured on recent + forward-collected
prices. A therefore delivers a complete signal-quality verdict now and a *preliminary* market-edge
verdict that strengthens as Kalshi price history accrues (forward collection is a B concern).

### Config

`macro_*` settings in `config.py`, all inert in A (no loop reads them yet): source URLs, ordered
fallback list, `User-Agent`, and gate-0 thresholds (min Brier margin, min net-of-fees edge). Defaults
OFF. Not tuner-managed.

## Data flow (A)

```
Cleveland Fed (daily)  ──httpx+UA──►  nowcast_source  ──►  macro_nowcast_vintages (point-in-time)
                                                              │
BLS actual (monthly)  ─────────────────────────────────────┐ │
                                                            ▼ ▼
                        distribution (σ curve) ──►  backtest.py  ──►  {signal Brier, market edge, GO/NO-GO}
                                                            ▲
Kalshi ladder history (thin/forward) ── ladder.py ──────────┘
```

## Error handling

- Source fetch: try ordered fallbacks; if all fail, raise with the per-source status (no silent
  fallback to a stale/None value — that was the stale-spot class of bug). The daily job logs which
  source answered.
- Vintage upsert: idempotent on `(as_of_date, target_month, index_type)`; re-running a day is safe.
- Ladder parse: skip malformed strikes with a logged count, never silently drop the whole ladder.
- Distribution: assert `σ > 0` and monotonic `P(>k)`; fail loud on violation.

## Testing (TDD — test first, watch it fail)

- `distribution`: `P(>k)` math for known point/σ; monotonicity across strikes; σ→0 limit → step
  function; fat-tail vs Gaussian branch selection.
- `nowcast_source`: normalization of a fixture payload; point-in-time vintage selection returns the
  value known on date D, never a later vintage (lookahead guard); fallback ordering when the primary
  raises.
- `ladder`: parse a fixture `KXCPI` / `KXCPIYOY` event with the real dollar/fp field names; correct
  strike ordering and above-X semantics.
- `backtest`: on a synthetic month where the nowcast is deliberately better/worse than a baseline,
  the verdict flips correctly; fee model matches `rate·p·(1−p)`.

## Roadmap (B/C — not this spec)

- **B:** live ladder polling + `macro_cpi_decision_outcomes` shadow records + `AppContainer` wiring +
  forward-collection of Kalshi CPI quotes. Gated on A passing.
- **C:** live orders via the existing `ExecutionService` + `DeterministicRiskEngine` (kill switch,
  deployment lock, fee/edge gates). Default shadow; auto-trade only after a shadow period. Gated on B.

## Risks & honest caveats

- **Market may already embed the nowcast** → edge ≈ 0. This is the expected null; A is designed to
  detect it cheaply. Do not rationalize a weak edge into a live trade.
- **Monthly cadence** → live-P&L validation is slow (a handful of prints/year). Signal accuracy is
  backtestable over years, but market-edge validation depends on accruing Kalshi price history.
- **Regime dependence:** nowcast RMSE blew out during 2021–22 high-inflation volatility; σ(horizon)
  must be estimated on recent-enough data and treated as regime-sensitive.
- **Ingestion fragility:** dependence on one public source (Cleveland Fed) with UA-gating; mitigated
  by ordered fallbacks, but a source format change breaks the daily job — must fail loud.

## Documentation policy

Per `CLAUDE.md`: when B/C land, update `CLAUDE.md` (new subsystem + commands + config), `README.md`
(CLI), and add `docs/research/` findings for the gate-0 result. Sub-project A adds this spec + a
gate-0 result note when the backtest runs.
