# Crypto Shadow Trading Performance Tracking — Design

**Date:** 2026-05-19
**Status:** Approved (design)
**Scope:** Crypto only (weather trading is paused). Excludes gate-tuning/decision-corpus wiring (deferred).

## Problem

Production runs in shadow mode (`APP_SHADOW_MODE=true` + kill switch), so the crypto
autonomy loop evaluates candidates and returns `shadow_skipped` without ever placing an
order. Today nothing about those would-have-traded decisions is persisted:

- `decision_traces`, `trade_tickets` are empty (0 rows ever); the crypto path doesn't use them.
- `rooms` holds only old weather shadow rooms.
- `ops_events` source `crypto_workflow` has never logged.

The only persisted "simulated P&L" is the nightly backtest artifact (model validation on
historical data), not a live rolling tally. Result: we cannot answer "how is our shadow
trading doing?" This is an observability gap, not just an empty window.

## Goal

Persist every would-have-gone-live shadow decision, score each against market settlement,
and surface a rolling P&L per asset/frequency. Per-decision log **and** rolling aggregate.

## Decisions (from brainstorming)

1. **Output:** per-decision log + rolling P&L aggregate.
2. **Population:** only would-have-gone-live decisions — candidate is `CRYPTO_LIVE_QUALITY`
   and every deterministic gate passes, suppressed *solely* by `app_shadow_mode`/kill-switch.
3. **P&L model:** mirror the live path — capture the exact intended order (side, entry price,
   dynamic `count_fp`); score real dollars at settlement.
4. **Scoring:** periodic sweep on the active-color daemon heartbeat (decoupled, idempotent).
5. **Surfacing:** a CLI command.
6. **Safety:** read-only w.r.t. trading; gated by a config flag.

## Architecture

### New service: `CryptoShadowService`
Lives in `crypto/services.py`, wired through `AppContainer`. Three responsibilities,
isolated from execution/autonomy so it can never affect trading:

- `record_decision(...)` — persist a would-have-gone-live decision (called from the execution path).
- `score_open_decisions(...)` — heartbeat sweep filling realized P&L once markets settle.
- `report(frequency, days, asset_symbols)` — aggregation for the CLI.

### Data model: new table `crypto_shadow_decisions`

| Field | Purpose |
|---|---|
| `kalshi_env`, `frequency`, `asset_symbol`, `series_ticker`, `market_ticker` | scope + settlement join key |
| `decided_at` | when the decision was made |
| `side` (`yes`/`no`), `entry_price_dollars` (Numeric(10,4)), `count_fp` (Numeric(10,2)) | the exact intended order (mirror-live) |
| `candidate_status`, `agent_pack_version`, `app_color`, `suppressed_by` (JSON, e.g. `["app_shadow_mode"]`) | provenance / proof it was would-have-gone-live |
| `decision_context` (JSON) | forecast prob, market mid, edge bps — for later analysis |
| `settled_at`, `settlement_result`, `won`, `realized_pl_dollars` | null until scored by the sweep |

- Unique key: `(kalshi_env, market_ticker, decided_at, side)` to dedupe per cycle.
- Indexes: `(settled_at)` for the sweep; `(kalshi_env, frequency, decided_at)` for reporting.
- Alembic migration adds the table.

### Capture seam
"Would-have-gone-live" requires that the candidate is `live_quality`, **all** deterministic
gates pass (trading enabled, replay gate passed, market open), and the only suppressor is
`app_shadow_mode`/kill-switch. Today the shadow check short-circuits *before* the gate checks
(`services.py:3056` vs gates at `3093–3138`). The implementation will evaluate the gates
regardless of shadow mode and record only when they would all have passed, capturing where
the intended `ticket` (side/price/`count_fp`) is fully formed. The insert is **best-effort
(try/except)** so a tracking failure can never break the decision loop.

### Scoring sweep (heartbeat)
On the active-color daemon heartbeat, `score_open_decisions` loads rows where
`settled_at IS NULL`, looks up `settlement_result` for each `market_ticker` from
`crypto_market_snapshots`, and writes:

- `won` = (side matches settlement result)
- `realized_pl_dollars = count_fp × (payout − entry_price_dollars)`, where `payout = 1` if won else `0`
- `settled_at`, `settlement_result`

Idempotent: only touches unscored rows; markets not yet settled are skipped.

### CLI: `crypto-shadow-pnl`
`crypto-shadow-pnl --kalshi-env production --frequency 15m --days 7 [--assets …]`
→ per asset/frequency: trades, win-rate, net realized P&L, avg edge; plus open (unsettled)
count and totals. Uses the existing `add_kalshi_env_argument` pattern; calls
`CryptoShadowService.report(...)`.

### Config / safety
- Add `crypto_shadow_tracking_enabled: bool = True` (disables capture + sweep).
- Read-only w.r.t. trading: observes and scores only; never touches `ExecutionService`
  behavior, the kill switch, or shadow mode.

## Testing (TDD)

Unit tests (SQLite, per existing tiers):

- `record_decision` writes the intended-order fields.
- Capture fires **only** for live-quality-blocked-by-shadow — not exploratory-shadow, not
  actual-live execution, not gate-blocked candidates.
- `score_open_decisions` computes correct P&L for yes-win / yes-loss / no-win / no-loss, and
  is idempotent (re-run does not double-count).
- `report` aggregates trades/win-rate/net P&L correctly across assets/frequencies and windows.

## Out of scope (deferred)

- Weather shadow tracking (weather trading paused).
- Feeding crypto shadow outcomes into the decision-corpus / `autonomous_gate_tuning` pipeline.
- Web control-room card (CLI only for now).
