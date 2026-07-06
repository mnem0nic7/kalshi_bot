# Profitability Push #2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the four approved profitability levers per `docs/superpowers/specs/2026-07-06-profitability-push-2-design.md`: scaled pilot caps (2ct/2open/$6 stop/windowed budget + conditional XRP), CPI nowcast gate-0, Pyth-check-then-Brent-shadow, and the maker-side counterfactual shadow.

**Architecture:** Leg 1 extends the TDD'd pilot guard module + script/watchdog env. Leg 2 builds the `macro/` subsystem per its own committed spec. Legs 3–4 are signal-only collectors in the established shadow mold. No changes to the model path, weather, trainer, or ExecutionService.

**Tech Stack:** Python 3.12, pytest (asyncio auto), docker (app container green), Kalshi REST via existing `KalshiClient`, Pyth Hermes REST (leg 3 only).

## Global Constraints

- Orders only via `ExecutionService`; the ONLY order-placing artifact in this plan is the existing pilot script. Brent/maker legs are signal-only — no ExecutionService import.
- Leg-1 approved caps, exact values: `STALE_PILOT_CONTRACTS=2`, `STALE_PILOT_MAX_OPEN=2`, `STALE_PILOT_DAILY_LOSS_STOP=6.0`, `STALE_PILOT_MAX_TRADES_PER_WINDOW=5` (12h UTC windows), `STALE_PILOT_MAX_TRADES_PER_DAY=10` (kept as the belt), unchanged `STALE_PILOT_MAX_ENTRY=0.75` and the 15¢ credible-edge ceiling. Allowlist `BNB,HYPE,DOGE,ETH` (+`XRP` only on a positive Task-3 backtest).
- Doc policy: every behavior-changing commit updates CLAUDE.md/dials/research docs in the same commit. `.env` never committed. Direct-to-main, footer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Restarting the pilot: kill the HOST wrapper pid AND the in-container `stale_quote_pilot.py` process (known gotcha), then run `scripts/stale_quote_pilot_watchdog.sh`; verify the new `start` JSONL record's `assets`/`caps`.

---

### Task 1: Pilot guard module — windowed budget + contract count (TDD)

**Files:**
- Modify: `src/kalshi_bot/crypto/stale_quote_pilot.py`
- Test: `tests/unit/test_stale_quote_pilot_scaling.py` (new; existing `tests/unit/test_stale_quote_pilot.py` must keep passing unchanged)

**Interfaces:**
- Produces: `PilotConfig` gains `contracts: int = 1`, `max_trades_per_window: int = 0`, `window_hours: int = 12`. `PilotState` gains `window_index: int | None = None`, `trades_this_window: int = 0`. `evaluate_guards` returns reason `"window_trade_cap"` when the window cap binds. `build_pilot_ticket(..., count: int = 1)`.
- Consumes: current definitions in `src/kalshi_bot/crypto/stale_quote_pilot.py` (read it first — guards are ordered cheapest-first and every default refuses to trade; preserve both properties).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_stale_quote_pilot_scaling.py
"""Leg-1 scaling (spec 2026-07-06-profitability-push-2): per-12h-window trade
budget (stops overnight burning the whole day), contract count on tickets.
Flat daily cap remains as the belt on top of the window cap."""
from datetime import UTC, datetime
from decimal import Decimal

from kalshi_bot.crypto.stale_quote_pilot import (
    PilotConfig, PilotState, build_pilot_ticket, evaluate_guards,
)


def _cfg(**kw):
    base = dict(enabled=True, assets=("BNB",), max_trades_per_day=10,
                max_open_positions=2, daily_loss_stop_dollars=6.0,
                max_entry_dollars=0.75, max_trades_per_window=5, window_hours=12)
    base.update(kw)
    return PilotConfig(**base)


def _at(hour):
    return datetime(2026, 7, 6, hour, 30, tzinfo=UTC)


def test_window_cap_binds_within_window():
    state = PilotState()
    cfg = _cfg()
    for _ in range(5):
        ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(3))
        assert ok, why
        state.trades_today += 1
        state.trades_this_window += 1
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(4))
    assert not ok and why == "window_trade_cap"


def test_window_rollover_resets_window_counter_not_daily():
    state = PilotState()
    cfg = _cfg()
    state.day = _at(3).date()
    state.trades_today = 5
    state.window_index = 0
    state.trades_this_window = 5
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(13))
    assert ok, why                      # second UTC window: fresh window budget
    assert state.trades_this_window == 0
    assert state.trades_today == 5      # daily belt untouched by rollover


def test_daily_belt_still_binds_across_windows():
    state = PilotState()
    cfg = _cfg(max_trades_per_day=6)
    state.day = _at(13).date()
    state.trades_today = 6
    state.window_index = 1
    state.trades_this_window = 1
    ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(13))
    assert not ok and why == "daily_trade_cap"


def test_flat_daily_only_when_window_cap_unset():
    state = PilotState()
    cfg = _cfg(max_trades_per_window=0)   # window feature off -> legacy behavior
    for _ in range(7):
        ok, why = evaluate_guards(cfg, state, asset="BNB", entry_dollars=0.5, now=_at(3))
        assert ok, why
        state.trades_today += 1


def test_ticket_carries_contract_count():
    t1 = build_pilot_ticket(market_ticker="X", side="yes",
                            yes_bid=Decimal("0.40"), yes_ask=Decimal("0.42"))
    assert t1.count_fp == Decimal("1")
    t2 = build_pilot_ticket(market_ticker="X", side="no",
                            yes_bid=Decimal("0.40"), yes_ask=Decimal("0.42"), count=2)
    assert t2.count_fp == Decimal("2")
```

- [ ] **Step 2: Run to verify failure**

Run: `source .venv/bin/activate && python -m pytest tests/unit/test_stale_quote_pilot_scaling.py -q`
Expected: FAIL — unexpected keyword `max_trades_per_window`.

- [ ] **Step 3: Implement**

In `src/kalshi_bot/crypto/stale_quote_pilot.py`:

```python
@dataclass(frozen=True)
class PilotConfig:
    """Hard caps for the micro-pilot. Defaults refuse to trade (enabled=False)."""

    enabled: bool = False
    assets: tuple[str, ...] = ()
    max_trades_per_day: int = 0
    max_open_positions: int = 0
    daily_loss_stop_dollars: float = 0.0
    max_entry_dollars: float = 0.0
    contracts: int = 1
    max_trades_per_window: int = 0   # 0 = window budget off (flat daily only)
    window_hours: int = 12


@dataclass
class PilotState:
    """Mutable per-run accounting the guards evaluate against."""

    day: date | None = None
    trades_today: int = 0
    realized_pnl_today: float = 0.0
    open_positions: int = 0
    window_index: int | None = None
    trades_this_window: int = 0
```

In `evaluate_guards`, after the existing day-reset block (which must also reset
`state.window_index = None; state.trades_this_window = 0` on a new day) and
after the `daily_trade_cap` check, insert:

```python
    if config.max_trades_per_window > 0:
        idx = now.hour // max(1, config.window_hours)
        if state.window_index != idx:
            state.window_index = idx
            state.trades_this_window = 0
        if state.trades_this_window >= config.max_trades_per_window:
            return False, "window_trade_cap"
```

In `build_pilot_ticket`, add `count: int = 1` keyword-only param and use
`count_fp=Decimal(count)` (docstring: "IOC taker ticket (count contracts)…").

- [ ] **Step 4: Run both test files**

Run: `python -m pytest tests/unit/test_stale_quote_pilot_scaling.py tests/unit/test_stale_quote_pilot.py -q`
Expected: all pass (legacy file unchanged and green).

- [ ] **Step 5: Commit**

```bash
git add src/kalshi_bot/crypto/stale_quote_pilot.py tests/unit/test_stale_quote_pilot_scaling.py
git commit -m "feat(pilot): per-window trade budget + contract count in guard module

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 2: Pilot script + watchdog wiring, deploy scaled caps

**Files:**
- Modify: `scripts/stale_quote_pilot.py` (`_env_cfg`, ticket build, trade accounting, settle math)
- Modify: `scripts/stale_quote_pilot_watchdog.sh` (env block)
- Modify: `CLAUDE.md` (pilot caps note, same commit)

**Interfaces:**
- Consumes: Task 1's `PilotConfig(contracts=, max_trades_per_window=)`, `build_pilot_ticket(count=)`, reason `"window_trade_cap"`.

- [ ] **Step 1: Wire env → config in `_env_cfg`**

Add to the `PilotConfig(...)` construction:

```python
        contracts=int(os.environ.get("STALE_PILOT_CONTRACTS", "1")),
        max_trades_per_window=int(os.environ.get("STALE_PILOT_MAX_TRADES_PER_WINDOW", "0")),
```

- [ ] **Step 2: Use the count on tickets and in accounting**

- Ticket: `build_pilot_ticket(..., count=cfg.contracts)`.
- After a submitted (non-shadow/blocked/rejected) order: also `state.trades_this_window += 1` next to the existing `state.trades_today += 1`.
- Fill tracking: `filled` from the exchange is a contract count — store it on the open trade: `open_trades.append({..., "count": filled})`.
- Settle math in `reconcile()`: net is per-contract × count:
  `net = (gross - 0.07 * tr["entry"] * (1 - tr["entry"])) * tr.get("count", 1.0)`.
- The docstring env example block gains the two new vars with the approved values.

- [ ] **Step 3: Watchdog env (exact values)**

In `scripts/stale_quote_pilot_watchdog.sh` replace the env block values:

```bash
  STALE_PILOT_ASSETS=BNB,HYPE,DOGE,ETH \
  STALE_PILOT_CONTRACTS=2 \
  STALE_PILOT_MAX_TRADES_PER_DAY=10 \
  STALE_PILOT_MAX_TRADES_PER_WINDOW=5 \
  STALE_PILOT_MAX_OPEN=2 \
  STALE_PILOT_DAILY_LOSS_STOP=6.0 \
  STALE_PILOT_MAX_ENTRY=0.75 \
```

Also add `"contracts"` and `"trades_window"` to the `start` record's `caps` dict in the script so the deploy is verifiable from JSONL.

- [ ] **Step 4: CLAUDE.md** — update the pilot sentence: scaled 2026-07-06 to 2 contracts / 2 open / $6 daily stop / 5-per-12h-UTC-window budget (10/day belt), per `docs/superpowers/specs/2026-07-06-profitability-push-2-design.md`.

- [ ] **Step 5: Commit, deploy, verify**

```bash
git add scripts/stale_quote_pilot.py scripts/stale_quote_pilot_watchdog.sh CLAUDE.md
git commit -m "feat(pilot): scale to 2ct/2open/\$6 stop with 5-per-12h-window budget

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
kill "$(cat /home/user1/kalshi_stale_pilot/runner.pid)" 2>/dev/null || true
docker exec infra-app_production_green-1 python - <<'EOF'
import os, signal
for p in os.listdir('/proc'):
    if p.isdigit():
        try:
            cmd = open(f'/proc/{p}/cmdline').read()
        except OSError:
            continue
        if 'stale_quote_pilot.py' in cmd:
            os.kill(int(p), signal.SIGTERM)
EOF
bash scripts/stale_quote_pilot_watchdog.sh
sleep 10
grep '"type": "start"' /home/user1/kalshi_stale_pilot/pilot.jsonl | tail -1
```

Expected: new start record with `"assets": ["BNB","HYPE","DOGE","ETH"]` and caps showing `contracts: 2`, `trades_window: 5`, `loss_stop: 6.0`, `open: 2`. (Confirm active color is still green first; if flipped, target the blue container.)

---

### Task 3: XRP backtest → conditional allowlist add (research)

**Files:**
- Create: `scripts/xrp_stale_backtest.py` (or reuse — see Step 1)
- Modify: `docs/research/2026-07-02-stale-quote-taker-edge.md` (append XRP verdict section)
- Modify (only if positive): `scripts/stale_quote_pilot_watchdog.sh` allowlist + CLAUDE.md

- [ ] **Step 1: Find the existing tick-recompute backtest machinery**

The validation method is in `docs/research/2026-07-02-stale-quote-taker-edge.md` ("fresh-tick recompute", per-asset table). Locate its script: `ls scripts/ | grep -i "stale\|freshfix"` and `grep -rln "fresh.*tick\|recompute" scripts/ docs/research/2026-07-02*`. Reuse that script/method verbatim — do NOT invent a new methodology. If the original was ad-hoc (not committed), rebuild it from the doc's exact recipe: consecutive feature-row snapshots at ~18s cadence over the most recent 2 weeks, spot context recomputed from raw ticks (pre-v15 rows have frozen candle spot), signal = |dfair| ≥ 0.10 with quote mid moved ≤1¢, entry at quote-side price, settle at market result, fee `0.07·p·(1−p)`, dedup to 1 trade/market.

- [ ] **Step 2: Run for XRP, most recent 2 weeks**

Run inside the app container (DB access). Output: n signals, dedup trades, per-contract net at thresholds 0.10/0.15, win rate.

- [ ] **Step 3: Verdict + conditional deploy**

- Positive net at ≥0.10 with n ≥ ~200 dedup trades → append `,XRP` to `STALE_PILOT_ASSETS` in the watchdog, restart the runner (Task 2 Step 5 procedure), note in CLAUDE.md.
- Otherwise → no config change.
- Either way: append an "XRP verdict (2026-07-06)" section to the research doc with the numbers, and commit:

```bash
git add docs/research/2026-07-02-stale-quote-taker-edge.md scripts/xrp_stale_backtest.py scripts/stale_quote_pilot_watchdog.sh CLAUDE.md
git commit -m "research(pilot): XRP stale-quote backtest verdict — <headline>

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 4: CPI nowcast gate-0 (build per its own committed spec)

**Files:**
- Create: `src/kalshi_bot/macro/__init__.py`, `src/kalshi_bot/macro/nowcast_source.py`, `src/kalshi_bot/macro/ladder.py`, plus whatever the spec's component list names
- Create: `tests/unit/test_macro_nowcast_source.py`, `tests/unit/test_macro_ladder.py`
- Create: `scripts/cpi_gate0_backtest.py`
- Create: `docs/research/2026-07-06-cpi-gate0-verdict.md`

**Requirements document (read FIRST, it is the full spec):** `docs/superpowers/specs/2026-07-01-cpi-nowcast-gate0-design.md`. It contains the verified endpoint (`nowcast_month.json`, UA-gated — browser User-Agent required, naive fetch gets 403), the verified frame structure (~157 monthly frames, daily x-axis vintages, MoM % values), the component breakdown, and the three-part go/no-go (signal Brier vs baseline; market edge net-of-fees vs Kalshi-implied on the tradeable subset — DECISIVE; fillability sanity). Scope is sub-project A only: libraries + backtest script, NO AppContainer wiring, NO trading code, NO shadow loop.

- [ ] **Step 1:** Read the spec end-to-end. TDD the JSON-frame parser against a fixture captured from the real endpoint (commit a trimmed fixture, not the full file).
- [ ] **Step 2:** TDD the ladder mapping (nowcast + error model → per-strike probabilities for KXCPI MoM and KXCPIYOY YoY) per the spec's formulas.
- [ ] **Step 3:** Kalshi side: pull settled KXCPI/KXCPIYOY markets + historical prices via the public API from the app container; the spec's fillability numbers came from the live scan — record per-event volume/OI. Note the known API history cutoff (2026-04-27, `project_instrument_scoping`): the tradeable-subset backtest window is [2026-04-27, latest settled print]; the signal-quality backtest uses the full 13y vintages.
- [ ] **Step 4:** Run `scripts/cpi_gate0_backtest.py`; write `docs/research/2026-07-06-cpi-gate0-verdict.md` with the three-part verdict, explicitly labeled small-n where n is small. Gate-0 fail on part (2) = STOP per spec.
- [ ] **Step 5:** Commit code+tests+doc in one commit (`research(cpi): gate-0 verdict — <headline>` + footer); push.

---

### Task 5: Pyth Hermes auth pricing check (research, ~30 min, BLOCKS Task 6)

**Files:**
- Modify: `docs/research/2026-07-05-commodities-scoping.md` (append "Pyth auth resolution" section)

- [ ] **Step 1:** WebSearch/WebFetch official Pyth docs (docs.pyth.network, Hermes changelog/blog) for the post-2026-07-31 auth requirement: free-tier existence, rate limits, key issuance process. Cross-check two sources; record URLs.
- [ ] **Step 2:** Verdict against our need (~1 request/sec single feed): `GO` (free/cheap tier covers it — include the signup step if a key is needed) or `STOP` (paid-only above our appetite). Append the section + verdict to the scoping doc, commit (`research(commodities): Pyth Hermes auth resolution — <verdict>` + footer), push.

---

### Task 6: Brent shadow collector (ONLY if Task 5 = GO)

**Files:**
- Create: `scripts/brent_stale_shadow.py`
- Create: `scripts/brent_stale_shadow_watchdog.sh` (clone `scripts/stale_quote_shadow_1h_watchdog.sh` pattern: active-color resolution, name-scoped sweep, docker cp, pidfile `/home/user1/kalshi_stale_pilot/brent_shadow.pid`)
- Modify: `CLAUDE.md` (one sentence, same commit)

- [ ] **Step 1: Discover the Brent feed id** — Hermes price-feeds index (`https://hermes.pyth.network/v2/price_feeds?query=brent`) or docs; record the feed id in the script as a named constant with the source URL in a comment.

- [ ] **Step 2: Write the collector** — v1 is a pure logger (detection thresholds come later from observed cadence, per spec):

```python
# scripts/brent_stale_shadow.py
"""Brent (KXBRENTD) shadow logger — signal-only, NEVER orders.

v1 logs, every POLL_S: Pyth Brent spot (price, conf, publish_time) and the
KXBRENTD ladder top-of-book (cached market quotes). No detection yet — the
staleness thresholds get chosen from the first day of observed cadence
(spec 2026-07-06-profitability-push-2). No ExecutionService import.
"""
from __future__ import annotations

import asyncio
import json
import os
import urllib.request
from datetime import UTC, datetime

from kalshi_bot.config import get_settings
from kalshi_bot.integrations.kalshi import KalshiClient

PYTH_FEED_ID = "<from Step 1>"  # https://hermes.pyth.network/v2/price_feeds?query=brent
PYTH_URL = f"https://hermes.pyth.network/v2/updates/price/latest?ids[]={PYTH_FEED_ID}"
SERIES = "KXBRENTD"
POLL_S = 10.0
OUT_DIR = os.environ.get("BRENT_SHADOW_OUT", "/app/data/brent_shadow")


def emit(rec: dict) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "brent_shadow.jsonl"), "a") as fh:
        fh.write(json.dumps(rec) + "\n")


def pyth_spot() -> dict | None:
    try:
        req = urllib.request.Request(PYTH_URL, headers={"User-Agent": "kalshi-bot-shadow/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.load(r)
        p = d["parsed"][0]["price"]
        return {"price": int(p["price"]) * 10 ** int(p["expo"]),
                "conf": int(p["conf"]) * 10 ** int(p["expo"]),
                "publish_time": p["publish_time"]}
    except Exception as e:  # log-and-continue: a feed hiccup must not kill the logger
        emit({"type": "pyth_error", "ts": datetime.now(UTC).isoformat(), "err": str(e)[:150]})
        return None


async def main() -> None:
    settings = get_settings()
    client = KalshiClient(settings)
    emit({"type": "start", "ts": datetime.now(UTC).isoformat(), "series": SERIES})
    while True:
        now = datetime.now(UTC)
        spot = pyth_spot()
        markets = []
        try:
            resp = await client.list_markets(series_ticker=SERIES, status="open", limit=100)
            for m in resp.get("markets") or []:
                markets.append({"ticker": m.get("ticker"),
                                "yes_bid": m.get("yes_bid_dollars"),
                                "yes_ask": m.get("yes_ask_dollars"),
                                "floor": m.get("floor_strike"), "cap": m.get("cap_strike"),
                                "close": m.get("close_time")})
        except Exception as e:
            emit({"type": "kalshi_error", "ts": now.isoformat(), "err": str(e)[:150]})
        if spot or markets:
            emit({"type": "tick", "ts": now.isoformat(), "spot": spot, "markets": markets})
        await asyncio.sleep(POLL_S)


if __name__ == "__main__":
    asyncio.run(main())
```

If auth is required already / a key was obtained in Task 5, add the documented header to the Pyth request (from the Task 5 write-up), sourced from env `PYTH_API_KEY` passed by the watchdog — never committed.

- [ ] **Step 3:** Smoke-run 60s in the active app container (stdout via `head` on the JSONL), then launch via the new watchdog + crontab line (same `*/10` pattern; preserve existing lines). Verify a `tick` record with both `spot` and a non-empty `markets` ladder.
- [ ] **Step 4:** Commit (`feat(brent): signal-only shadow logger for KXBRENTD vs Pyth spot` + footer) with the CLAUDE.md sentence; push.

---

### Task 7: Maker-side counterfactual shadow in the pilot (signal-only)

**Files:**
- Modify: `scripts/stale_quote_pilot.py` (the `live_edge_too_small` branch + reconcile)
- Create: `scripts/stale_maker_shadow_report.py`
- Modify: `CLAUDE.md` (one sentence, same commit)

**Interfaces:**
- Consumes: the pilot's reject branch (`rec["guard"] = "live_edge_too_small"` — currently `emit(rec); continue`), its `hist` mid-history dict, `open_trades`-style reconcile loop, `f()` float helper.

- [ ] **Step 1: Enrich the reject branch.** Replace the `live_edge_too_small` block body:

```python
                if edge_live < 0.03:
                    rec["guard"] = "live_edge_too_small"
                    # Maker counterfactual (spec 2026-07-06): where would a resting
                    # bid at fair-minus-3c sit? Tracked to settlement, fills judged
                    # by a traded-through proxy — an UPPER BOUND on real maker fills
                    # (queue position ignored).
                    maker_yes_price = (round(float(fair_now) - 0.03, 2) if side == "yes"
                                       else round(float(fair_now) + 0.03, 2))
                    if 0.01 <= maker_yes_price <= 0.99:
                        rec["maker_yes_price"] = maker_yes_price
                        maker_shadows.append({
                            "ticker": tk, "side": side, "yes_price": maker_yes_price,
                            "fair": float(fair_now), "signal_ts": now.isoformat(),
                            "filled_proxy": False, "settle_by": cl + timedelta(seconds=60),
                        })
                    emit(rec)
                    continue
```

(`maker_shadows: list[dict] = []` is initialized next to `open_trades` in `main()`. For a NO position the resting NO bid at `(1-fair)-0.03` in NO-price space equals yes-price `fair+0.03` — hence the side-dependent sign.)

- [ ] **Step 2: Fill-proxy + settle in the poll/reconcile paths.**

In the per-market loop, right after `h.append((now, mid_now, spot_now))`, mark traded-through maker shadows for this ticker (cached quote crossing our resting price):

```python
                for ms in maker_shadows:
                    if ms["ticker"] == tk and not ms["filled_proxy"]:
                        if (ms["side"] == "yes" and ya is not None and ya <= ms["yes_price"]) or \
                           (ms["side"] == "no" and yb is not None and yb >= ms["yes_price"]):
                            ms["filled_proxy"] = True
                            ms["filled_ts"] = now.isoformat()
```

In `reconcile()`, settle matured maker shadows (same result-fetch pattern as `open_trades`; keep the two loops separate):

```python
        still_ms: list[dict] = []
        for ms in maker_shadows:
            if nowt < ms["settle_by"]:
                still_ms.append(ms)
                continue
            try:
                m = (await client.get_market(ms["ticker"])).get("market") or {}
            except Exception:
                still_ms.append(ms)
                continue
            res = m.get("result")
            if res not in ("yes", "no"):
                still_ms.append(ms)
                continue
            y = 1.0 if res == "yes" else 0.0
            entry = ms["yes_price"] if ms["side"] == "yes" else 1.0 - ms["yes_price"]
            gross = (y - ms["yes_price"]) if ms["side"] == "yes" else ((1.0 - y) - (1.0 - ms["yes_price"]))
            emit({"type": "maker_settle", "ts": nowt.isoformat(), **{k: ms[k] for k in
                  ("ticker", "side", "yes_price", "fair", "signal_ts", "filled_proxy")},
                  "result": res, "gross_if_filled": round(gross, 4), "entry": round(entry, 4)})
        maker_shadows[:] = still_ms
```

- [ ] **Step 3: Analyzer.** `scripts/stale_maker_shadow_report.py`: reads the pilot JSONL, filters `maker_settle`, prints: signals, proxy-fill rate, and for proxy-filled rows gross and net under two fee assumptions (maker fee 0, and taker-formula `0.07·p·(1−p)` as the conservative bound — Kalshi's maker fee schedule should be confirmed in the output header, labeled as assumption). Header line must say "proxy fill rate is an UPPER BOUND (queue position ignored)".

- [ ] **Step 4:** Deploy = the Task 2 restart procedure (docker cp via watchdog handles the script copy). Verify a `live_edge_too_small` reject now carries `maker_yes_price` in the JSONL within ~an hour of running.

- [ ] **Step 5:** Commit (`feat(pilot): maker-side counterfactual shadow on live_edge_too_small rejects` + footer, CLAUDE.md sentence included); push.

---

### Task 8: Memory + close-out

- [ ] **Step 1:** Update `/home/user1/.claude/projects/-home-user1-workspace-kalshi-bot/memory/`: `project_stale_quote_edge.md` (scaled caps + maker shadow), `project_cpi_nowcast_project.md` (gate-0 verdict), new or updated commodities memory (Pyth verdict, Brent collector if launched), `MEMORY.md` hooks.
- [ ] **Step 2:** Operator report: what went live, gate-0 verdict, Pyth/Brent status, maker shadow accruing, and the review calendar (pilot scaled-caps readout after ~50 settles; CPI print ~07-15; 1h graduation ~07-11).
