# Live-Breadth Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Get more markets live per spec `docs/superpowers/specs/2026-07-04-live-breadth-expansion-design.md`: extend the 15m stale-quote pilot to HYPE/DOGE/ETH, stand up a 1h stale-quote shadow scanner, unblock 1h training (fit-row cap), evaluate the weather lock-in market edge from collected quotes, and scope commodities.

**Architecture:** Legs are independent. Leg 1 is a runner-env change plus a per-asset readout module. Leg 2a is a new signal-only script reusing the pilot's detection math with a strike-band bound and range-market fair value. Leg 2b is a per-frequency row cap in `CryptoForecastService.train()`. Legs 3–4 are offline analysis scripts whose deliverables are research docs, not trading code.

**Tech Stack:** Python 3.12, pytest (asyncio auto), SQLAlchemy async, docker compose (`infra/docker-compose.yml`), Kalshi public REST.

## Global Constraints

- Live risk budget UNCHANGED: $3 daily stop, 10 trades/day, 1 open position, ≤$0.75 entry, 1 contract, 15¢ credible-edge ceiling — shared across all pilot assets.
- Orders only via `ExecutionService` (kill switch + color checks). The 1h scanner places NO orders.
- Doc policy: every behavior-changing commit updates CLAUDE.md / dials docs in the same commit.
- Never run heavy analysis on the prod host; Legs 3–4 are light (quote-table scale / public API).
- `.env` is never committed.
- Commits direct to main, footer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- `*_TRIGGER_ENABLE_AUTO_ROOMS` stays false (weather).

---

### Task 1: Leg 1 — pilot per-asset readout + extend live assets to HYPE/DOGE/ETH

The pilot's asset universe is the env allowlist `STALE_PILOT_ASSETS` read by `scripts/stale_quote_pilot.py:_env_cfg()`; `SERIES` already contains all 7 assets. So going live on 3 more assets is a runner-env change — but first ship the per-asset readout (kill-rule visibility) so we can see per-asset P&L from day one.

**Files:**
- Create: `src/kalshi_bot/crypto/stale_pilot_readout.py`
- Create: `tests/unit/test_stale_pilot_readout.py`
- Modify: `scripts/stale_quote_pilot.py:8` (docstring allowlist example)
- Modify: `CLAUDE.md` (pilot asset list note, same commit)

**Interfaces:**
- Produces: `summarize_pilot_records(records: list[dict]) -> dict` — keys: `per_asset` (dict asset → `{settles, wins, losses, net, kill}`), `total_net`, `total_settles`. `kill` is True when that asset's `net <= -2.0` and `settles >= 15` and at least one other asset has `net > 0`.
- Consumes: pilot JSONL records as emitted today: settle records `{"type": "settle", "ticker": ..., "net": ..., "result": ...}` (no asset field — derive from ticker prefix), signal records `{"type": "signal", "asset": ...}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_stale_pilot_readout.py
"""Per-asset rollup + kill rule for the stale-quote pilot
(spec: docs/superpowers/specs/2026-07-04-live-breadth-expansion-design.md, Leg 1)."""
from kalshi_bot.crypto.stale_pilot_readout import asset_for_ticker, summarize_pilot_records


def _settle(ticker, net):
    return {"type": "settle", "ticker": ticker, "net": net,
            "result": "yes" if net > 0 else "no"}


def test_asset_for_ticker_matches_15m_series():
    assert asset_for_ticker("KXBTC15M-26JUL0418-T107249.99") == "BTC"
    assert asset_for_ticker("KXHYPE15M-26JUL0418-T38.5") == "HYPE"
    assert asset_for_ticker("KXWEIRD-123") is None


def test_summarize_per_asset_and_totals():
    recs = [_settle("KXBTC15M-a", 0.40), _settle("KXBTC15M-b", -0.30),
            _settle("KXDOGE15M-a", 0.10)]
    out = summarize_pilot_records(recs)
    assert out["per_asset"]["BTC"]["settles"] == 2
    assert out["per_asset"]["BTC"]["net"] == 0.10
    assert out["per_asset"]["DOGE"]["wins"] == 1
    assert out["total_settles"] == 3
    assert abs(out["total_net"] - 0.20) < 1e-9


def test_kill_rule_needs_15_settles_and_2_dollars_and_a_positive_peer():
    losers = [_settle("KXETH15M-x%d" % i, -0.15) for i in range(15)]  # net -2.25
    winner = [_settle("KXBTC15M-w", 0.5)]
    out = summarize_pilot_records(losers + winner)
    assert out["per_asset"]["ETH"]["kill"] is True
    assert out["per_asset"]["BTC"]["kill"] is False
    # below 15 settles: no kill even at -$2+
    out2 = summarize_pilot_records(losers[:14] + winner)
    assert out2["per_asset"]["ETH"]["kill"] is False
    # no positive peer: no kill (whole edge may be off — operator call, not auto)
    out3 = summarize_pilot_records(losers)
    assert out3["per_asset"]["ETH"]["kill"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source .venv/bin/activate && pytest tests/unit/test_stale_pilot_readout.py -v`
Expected: FAIL — `ModuleNotFoundError: kalshi_bot.crypto.stale_pilot_readout`

- [ ] **Step 3: Write minimal implementation**

```python
# src/kalshi_bot/crypto/stale_pilot_readout.py
"""Per-asset rollup for stale-quote pilot JSONL (Leg 1 kill-rule visibility).

Kill rule (design 2026-07-04): flag an asset once it is >= $2 cumulative
negative after >= 15 settles while at least one other asset is positive.
Flag only — dropping the asset stays an operator action on the runner env.
"""
from __future__ import annotations

SERIES_15M = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M",
              "XRP": "KXXRP15M", "BNB": "KXBNB15M", "DOGE": "KXDOGE15M",
              "HYPE": "KXHYPE15M"}
KILL_NET_DOLLARS = -2.0
KILL_MIN_SETTLES = 15


def asset_for_ticker(ticker: str) -> str | None:
    for asset, series in SERIES_15M.items():
        if ticker.startswith(series + "-"):
            return asset
    return None


def summarize_pilot_records(records: list[dict]) -> dict:
    per: dict[str, dict] = {}
    for rec in records:
        if rec.get("type") != "settle":
            continue
        asset = asset_for_ticker(str(rec.get("ticker", "")))
        if asset is None:
            continue
        row = per.setdefault(asset, {"settles": 0, "wins": 0, "losses": 0, "net": 0.0})
        net = float(rec.get("net") or 0.0)
        row["settles"] += 1
        row["net"] += net
        row["wins" if net > 0 else "losses"] += 1
    any_positive = {a for a, r in per.items() if r["net"] > 0}
    for asset, row in per.items():
        row["kill"] = (row["net"] <= KILL_NET_DOLLARS
                       and row["settles"] >= KILL_MIN_SETTLES
                       and bool(any_positive - {asset}))
        row["net"] = round(row["net"], 4)
    return {"per_asset": per,
            "total_net": round(sum(r["net"] for r in per.values()), 4),
            "total_settles": sum(r["settles"] for r in per.values())}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_stale_pilot_readout.py -v`
Expected: 3 PASS

- [ ] **Step 5: Update the pilot docstring + CLAUDE.md**

In `scripts/stale_quote_pilot.py` line 8, change the example allowlist:
`STALE_PILOT_ASSETS=BTC,BNB` → `STALE_PILOT_ASSETS=BTC,BNB,HYPE,DOGE,ETH   (allowlist; SOL flat / XRP unvalidated in backtest)`.
In `CLAUDE.md`, in the stale-quote/pilot mention (search "stale-quote"), note: pilot extended to HYPE/DOGE/ETH 2026-07-04 under unchanged global caps; per-asset readout `kalshi_bot.crypto.stale_pilot_readout` (kill rule −$2 @ ≥15 settles with positive peer).

- [ ] **Step 6: Commit**

```bash
git add src/kalshi_bot/crypto/stale_pilot_readout.py tests/unit/test_stale_pilot_readout.py scripts/stale_quote_pilot.py CLAUDE.md
git commit -m "feat(pilot): per-asset readout + kill rule; extend allowlist example to HYPE/DOGE/ETH

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

- [ ] **Step 7: Roll the live runner onto 5 assets**

The runner lives OUTSIDE the repo at `/home/user1/kalshi_stale_pilot/` (wrapper + watchdog `scripts/stale_quote_pilot_watchdog.sh`, cron every 10 min). Discovery + restart:

```bash
ls /home/user1/kalshi_stale_pilot/
grep -rn "STALE_PILOT_ASSETS" /home/user1/kalshi_stale_pilot/ scripts/stale_quote_pilot_watchdog.sh
```

Edit every place that sets `STALE_PILOT_ASSETS` to `BTC,BNB,HYPE,DOGE,ETH` (leave all other caps untouched). Copy the updated script into the ACTIVE app container and bounce the runner:

```bash
docker cp scripts/stale_quote_pilot.py infra-app_production_green-1:/app/stale_quote_pilot.py
kill $(cat /home/user1/kalshi_stale_pilot/runner.pid)   # watchdog relaunches within 10 min
```

Verify within ~15 min: the new `start` record in `/home/user1/kalshi_stale_pilot/pilot.jsonl` shows `"assets": ["BTC","BNB","HYPE","DOGE","ETH"]` and scan/`signal` records appear for a new asset. Confirm active color is still green first (`kalshi-bot-cli` or deployment_control query); if blue, `docker cp` to blue instead.

---

### Task 2: Leg 2a — 1h stale-quote shadow scanner (signal-only)

New script + pure-helper module. Detection math identical to the 15m pilot; differences: hourly series, ±2% log-moneyness strike band (the ladder-churn guard), range (floor+cap) markets supported via two-sided fair value, NO orders, NO live-book calls — emit signals and settle them against market results for the graduation backtest.

**Files:**
- Create: `src/kalshi_bot/crypto/stale_quote_shadow_1h.py`
- Create: `tests/unit/test_stale_quote_shadow_1h.py`
- Create: `scripts/stale_quote_shadow_1h.py`
- Modify: `CLAUDE.md` (same commit)

**Interfaces:**
- Produces (module): `SERIES_1H: dict[str, str]`; `in_strike_band(spot: float, strike: float, band: float = 0.02) -> bool`; `range_fair(fair_up_floor: float, fair_up_cap: float | None) -> float`; `cap_moneyness(mny_floor: float, floor_strike: float, cap_strike: float) -> float`.
- Consumes: `_crypto_vol_normal_fair_up`, `_spot_context_for_decision`, `_prepare_spot_context_series`, `_decimal`, `CRYPTO_SPOT_CONTEXT_LIVE` from `kalshi_bot.crypto.services` (exactly as `scripts/stale_quote_pilot.py:41-47` imports them).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_stale_quote_shadow_1h.py
"""1h shadow scanner helpers (Leg 2a). Range markets: P(floor<S<cap) =
fair_up(floor) - fair_up(cap); moneyness is a log-ratio ln(S/K) so the cap
row is mny_floor + ln(floor/cap)."""
import math

import pytest

from kalshi_bot.crypto.stale_quote_shadow_1h import (
    SERIES_1H, cap_moneyness, in_strike_band, range_fair,
)


def test_series_map_covers_active_assets_hourly():
    assert SERIES_1H["BTC"] == "KXBTC"
    assert set(SERIES_1H) == {"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"}


def test_strike_band_is_log_symmetric():
    assert in_strike_band(100.0, 101.9)          # +1.9%
    assert in_strike_band(100.0, 98.1)           # -1.9%
    assert not in_strike_band(100.0, 103.0)      # +3%
    assert not in_strike_band(100.0, 0.0)        # degenerate strike
    assert not in_strike_band(0.0, 100.0)        # degenerate spot


def test_range_fair_is_two_sided_and_clamped():
    assert range_fair(0.8, 0.3) == pytest.approx(0.5)
    assert range_fair(0.8, None) == pytest.approx(0.8)   # above/below market
    assert range_fair(0.3, 0.8) == 0.0                   # numeric noise clamps


def test_cap_moneyness_shifts_log_ratio():
    # mny = ln(S/floor); ln(S/cap) = mny + ln(floor/cap)
    spot, floor, cap = 105.0, 100.0, 110.0
    mny_floor = math.log(spot / floor)
    assert cap_moneyness(mny_floor, floor, cap) == pytest.approx(math.log(spot / cap))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_stale_quote_shadow_1h.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Write the module**

```python
# src/kalshi_bot/crypto/stale_quote_shadow_1h.py
"""Pure helpers for the 1h stale-quote SHADOW scanner (no orders).

The hourly ladders are wide (~50 strikes); the band bound is mandatory —
scanning the full ladder is the working-set churn that drove the crypto_1h
daemon to its 8g cap (docs/operations/2026-07-02-daemon-reconcile-wedge.md).
"""
from __future__ import annotations

import math

SERIES_1H = {"BTC": "KXBTC", "ETH": "KXETH", "SOL": "KXSOL", "XRP": "KXXRP",
             "BNB": "KXBNB", "DOGE": "KXDOGE", "HYPE": "KXHYPE"}

STRIKE_BAND = 0.02  # |ln(S/K)| <= 2% — plenty for a <=1h horizon


def in_strike_band(spot: float, strike: float, band: float = STRIKE_BAND) -> bool:
    if not spot or not strike or spot <= 0 or strike <= 0:
        return False
    return abs(math.log(spot / strike)) <= band


def range_fair(fair_up_floor: float, fair_up_cap: float | None) -> float:
    if fair_up_cap is None:
        return fair_up_floor
    return max(0.0, fair_up_floor - fair_up_cap)


def cap_moneyness(mny_floor: float, floor_strike: float, cap_strike: float) -> float:
    return mny_floor + math.log(floor_strike / cap_strike)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_stale_quote_shadow_1h.py -v`
Expected: 4 PASS

- [ ] **Step 5: Write the scanner script**

`scripts/stale_quote_shadow_1h.py` — copy the structure of `scripts/stale_quote_pilot.py` with these EXACT deltas (everything else identical; keep the detection constants in lockstep):

```python
# scripts/stale_quote_shadow_1h.py
"""1h stale-quote SHADOW scanner — signals only, NEVER orders.

Measures whether hourly bracket/above-below books go stale the way 15m books
do (docs/research/2026-07-02-stale-quote-taker-edge.md). Emits signal JSONL +
settle records for the graduation backtest. Graduation to a capped live pilot
requires the same 3-check OOS validation as 15m, then operator go.

  docker exec infra-app_production_green-1 env STALE_1H_STDOUT=1 \
      python /app/stale_quote_shadow_1h.py
"""
from __future__ import annotations

import asyncio
import json
import math
import os
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.crypto.stale_quote_shadow_1h import (
    SERIES_1H, cap_moneyness, in_strike_band, range_fair,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_LIVE,
    _crypto_vol_normal_fair_up,
    _decimal,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

VOL_MODEL = {  # lockstep with scripts/stale_quote_pilot.py
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
POLL_S = 5.0          # shadow: no fill race, keep request load low
LOOKBACK_S = 18.0
DFAIR_TH = 0.10
QUOTE_EPS = 0.01
MIN_TTC_S = 90
MAX_TTC_S = 3300      # up to 55 min out on an hourly market
MAX_SPREAD = 0.20

OUT_DIR = os.environ.get("STALE_1H_OUT", "/app/data/stale_shadow_1h")
STDOUT = os.environ.get("STALE_1H_STDOUT") == "1"


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def emit(rec: dict) -> None:
    line = json.dumps(rec)
    if STDOUT:
        print(line, flush=True)
    else:
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "shadow_1h.jsonl"), "a") as fh:
            fh.write(line + "\n")


def fair_for_market(row: dict, floor_strike: float, cap_strike: float | None):
    up_floor = _crypto_vol_normal_fair_up(row, VOL_MODEL)
    if up_floor is None:
        return None
    if cap_strike is None:
        return float(up_floor)
    mny = row.get("spot_moneyness_pct")
    if mny is None:
        return None
    cap_row = {**row, "spot_moneyness_pct": cap_moneyness(float(mny), floor_strike, cap_strike)}
    up_cap = _crypto_vol_normal_fair_up(cap_row, VOL_MODEL)
    if up_cap is None:
        return None
    return range_fair(float(up_floor), float(up_cap))


async def main() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    client = KalshiClient(settings)
    hist: dict[str, list] = {}
    signaled: set[str] = set()
    open_signals: list[dict] = []   # {ticker, side, entry, settle_by}
    emit({"type": "start", "ts": datetime.now(UTC).isoformat(),
          "mode": "shadow_1h", "assets": sorted(SERIES_1H)})

    async def reconcile() -> None:
        nowt = datetime.now(UTC)
        still = []
        for tr in open_signals:
            if nowt < tr["settle_by"]:
                still.append(tr)
                continue
            try:
                m = (await client.get_market(tr["ticker"])).get("market") or {}
            except Exception:
                still.append(tr)
                continue
            res = m.get("result")
            if res not in ("yes", "no"):
                still.append(tr)
                continue
            y = 1.0 if res == "yes" else 0.0
            gross = (y - tr["entry"]) if tr["side"] == "yes" else ((1.0 - y) - tr["entry"])
            net = gross - 0.07 * tr["entry"] * (1 - tr["entry"])
            emit({"type": "settle", "ts": nowt.isoformat(), "mode": "shadow_1h",
                  "ticker": tr["ticker"], "result": res, "net": round(net, 4)})
        open_signals[:] = still

    last_reconcile = 0.0
    while True:
        cycle_t0 = asyncio.get_event_loop().time()
        if cycle_t0 - last_reconcile > 120:
            await reconcile()
            last_reconcile = cycle_t0
        now = datetime.now(UTC)
        for asset, series in SERIES_1H.items():
            try:
                async with factory() as session:
                    repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
                    rows = await repo.list_crypto_spot_ohlc(
                        frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
                        since=now - timedelta(hours=2), limit=100_000,
                    )
                rows = sorted(rows, key=lambda r: r.end_ts)
                prep = _prepare_spot_context_series(rows)
                resp = await client.list_markets(series_ticker=series, status="open", limit=200)
            except Exception as e:
                emit({"type": "error", "ts": now.isoformat(), "asset": asset, "err": str(e)[:200]})
                continue
            for m in resp.get("markets") or []:
                tk = str(m.get("ticker"))
                yb, ya = f(m.get("yes_bid_dollars")), f(m.get("yes_ask_dollars"))
                strike = f(m.get("floor_strike"))
                cap = f(m.get("cap_strike"))
                ct = m.get("close_time")
                if yb is None or ya is None or strike is None or not ct:
                    continue
                try:
                    cl = datetime.fromisoformat(str(ct).replace("Z", "+00:00"))
                except ValueError:
                    continue
                ttc = (cl - now).total_seconds()
                if not (MIN_TTC_S <= ttc <= MAX_TTC_S) or (ya - yb) > MAX_SPREAD:
                    continue
                ctx = _spot_context_for_decision(
                    rows, prepared=prep, decision_ts=now, target_price=_decimal(str(strike)),
                    mid_yes=_decimal(str((yb + ya) / 2)), settings=settings,
                    mode=CRYPTO_SPOT_CONTEXT_LIVE,
                )
                if not ctx:
                    continue
                spot_now = f(ctx.get("spot_close_dollars"))
                mny = f(ctx.get("spot_moneyness_pct"))
                if spot_now is None or not in_strike_band(spot_now, strike):
                    continue          # ladder-churn guard: near-money strikes only
                row = {**ctx, "time_to_close_seconds": int(ttc)}
                fair_now = fair_for_market(row, strike, cap)
                mid_now = (yb + ya) / 2
                h = hist.setdefault(tk, [])
                h.append((now, mid_now, spot_now))
                if len(h) > 40:
                    del h[: len(h) - 40]
                if tk in signaled or fair_now is None or mny is None:
                    continue
                ref = None
                for ts0, mid0, sp0 in reversed(h[:-1]):
                    if (now - ts0).total_seconds() >= LOOKBACK_S:
                        ref = (ts0, mid0, sp0)
                        break
                if not ref or not ref[2]:
                    continue
                ref_row = {**row, "spot_moneyness_pct": mny - math.log(spot_now / ref[2])}
                fair_ref = fair_for_market(ref_row, strike, cap)
                if fair_ref is None:
                    continue
                dfair = float(fair_now) - float(fair_ref)
                stale = abs(mid_now - ref[1]) <= QUOTE_EPS
                if abs(dfair) < DFAIR_TH or not stale:
                    continue
                signaled.add(tk)
                side = "yes" if dfair > 0 else "no"
                entry = ya if side == "yes" else 1.0 - yb
                if not (0.03 <= entry <= 0.97):
                    continue
                emit({"type": "signal", "ts": now.isoformat(), "mode": "shadow_1h",
                      "asset": asset, "ticker": tk, "side": side, "is_range": cap is not None,
                      "dfair": round(dfair, 4), "entry": round(entry, 4),
                      "fair": round(float(fair_now), 4), "mid": round(mid_now, 4),
                      "ttc_s": int(ttc)})
                open_signals.append({"ticker": tk, "side": side, "entry": entry,
                                     "settle_by": cl + timedelta(seconds=120)})
        elapsed = asyncio.get_event_loop().time() - cycle_t0
        await asyncio.sleep(max(1.0, POLL_S - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Smoke-run in the active app container (stdout mode, ~60s)**

```bash
docker cp scripts/stale_quote_shadow_1h.py infra-app_production_green-1:/app/stale_quote_shadow_1h.py
docker exec infra-app_production_green-1 timeout 60 env STALE_1H_STDOUT=1 python /app/stale_quote_shadow_1h.py | head -20
```

Expected: `start` record then either silence (no near-money staleness — normal) or `signal`/`error` records. Any `error` records: fix before proceeding (likely field-name drift on hourly markets, e.g. missing `floor_strike` on some market shapes — skip those shapes).

- [ ] **Step 7: Launch as a persistent background process + doc**

```bash
nohup bash -c 'while true; do docker exec infra-app_production_green-1 python /app/stale_quote_shadow_1h.py; sleep 10; done' \
  > /home/user1/kalshi_stale_pilot/shadow_1h_runner.log 2>&1 &
echo $! > /home/user1/kalshi_stale_pilot/shadow_1h_runner.pid
```

CLAUDE.md: add one sentence to the crypto section — 1h stale-quote SHADOW scanner (signals only, `scripts/stale_quote_shadow_1h.py`, output `/app/data/stale_shadow_1h/shadow_1h.jsonl`); graduation needs the 15m-grade 3-check OOS validation + operator go.

- [ ] **Step 8: Commit**

```bash
git add src/kalshi_bot/crypto/stale_quote_shadow_1h.py tests/unit/test_stale_quote_shadow_1h.py scripts/stale_quote_shadow_1h.py CLAUDE.md
git commit -m "feat(1h): stale-quote shadow scanner — signal-only, strike-band bounded, range-market fair

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 3: Leg 2b — trainer 1h fit-row cap + re-enable 1h training

The 1h candidate fit OOMs 32g because it fits all models on the full 1h sample. Bound the FIT INPUT: per-frequency row cap applied where `CryptoForecastService.train()` (src/kalshi_bot/crypto/services.py:4124) loads feature rows (`limit=self.settings.crypto_train_max_snapshots` at services.py:4148 and the non-feature-store loads at 4174/4181). Folds are sequential (time, not peak-mem) — fold count unchanged, deviating from the spec's "4→2 folds" mention deliberately; row cap is the memory lever.

**Files:**
- Modify: `src/kalshi_bot/config.py` (near `crypto_train_max_snapshots`, line ~218)
- Modify: `src/kalshi_bot/crypto/services.py` (`train()` row loads)
- Create: `tests/unit/test_crypto_train_row_limit.py`
- Modify: `docs/training-dials-and-knobs.md`, `CLAUDE.md` (same commit)
- Modify: trainer service env in `infra/docker-compose.yml` OR `.env` (wherever `CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES=15m` is set — discover with `grep -rn "CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES" infra/ .env`)

**Interfaces:**
- Produces: `_crypto_train_fit_row_limit(settings, frequency: str) -> int` (module-level function in `crypto/services.py`), setting `crypto_train_max_fit_rows_1h: int | None = 150_000`.

- [ ] **Step 1: Verify feature-row ordering (cap must keep the NEWEST rows)**

```bash
grep -n "def list_crypto_training_feature_rows" -A 30 src/kalshi_bot/db/*.py | grep -n "order_by\|desc\|asc"
```

Expected: `order_by(...desc())` — newest first (consistent with `reversed(feature_records)` at services.py:4165 producing chronological rows). If it is ASCENDING, do not proceed with a plain limit — instead change the `train()` call to keep the tail: load then `feature_records = feature_records[-limit:]` after fetch with the unchanged DB limit, and note it in the commit message.

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_crypto_train_row_limit.py
"""Per-frequency fit-row cap (Leg 2b): bounds the 1h candidate-fit sample so
the trainer stays inside its 32g cgroup; 15m is unaffected."""
from types import SimpleNamespace

from kalshi_bot.crypto.services import _crypto_train_fit_row_limit


def _s(base=500_000, cap_1h=150_000):
    return SimpleNamespace(crypto_train_max_snapshots=base,
                           crypto_train_max_fit_rows_1h=cap_1h)


def test_1h_capped_15m_not():
    assert _crypto_train_fit_row_limit(_s(), "1h") == 150_000
    assert _crypto_train_fit_row_limit(_s(), "15m") == 500_000


def test_cap_never_raises_above_base():
    assert _crypto_train_fit_row_limit(_s(base=100_000), "1h") == 100_000


def test_none_disables_cap():
    assert _crypto_train_fit_row_limit(_s(cap_1h=None), "1h") == 500_000
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_crypto_train_row_limit.py -v`
Expected: FAIL — ImportError

- [ ] **Step 4: Implement**

`config.py`, directly below `crypto_train_max_snapshots: int = 500_000`:

```python
    # 1h candidate-fit memory bound: cap feature rows loaded for the 1h fit
    # (newest rows win; the 1h sample OOM'd the 32g trainer cgroup 2026-06-19).
    # None disables. 15m is never capped by this.
    crypto_train_max_fit_rows_1h: int | None = 150_000
```

`crypto/services.py`, module level (near `_crypto_model_candidate_report_max_folds`, line ~13707):

```python
def _crypto_train_fit_row_limit(settings: Settings, frequency: str) -> int:
    base = int(settings.crypto_train_max_snapshots)
    cap = getattr(settings, "crypto_train_max_fit_rows_1h", None)
    if frequency == "1h" and cap is not None:
        return min(base, max(1, int(cap)))
    return base
```

In `train()` replace `limit=self.settings.crypto_train_max_snapshots` with `limit=_crypto_train_fit_row_limit(self.settings, freq)` at ALL FOUR loads inside `train()` (services.py:4148, 4157-region, 4174, 4181 — the feature-store read and the snapshot/live-quote fallback reads; leave `crypto_train_max_candlesticks`/`crypto_train_max_spot_rows` untouched).

- [ ] **Step 5: Run tests**

Run: `pytest tests/unit/test_crypto_train_row_limit.py tests/unit/test_crypto_chunked_materialize.py -v`
Expected: new tests PASS; materialize tests still PASS (materialize path untouched — its loads are in the materialize method, not `train()`).

- [ ] **Step 6: Docs**

`docs/training-dials-and-knobs.md`: add `CRYPTO_TRAIN_MAX_FIT_ROWS_1H` (default 150000, None disables, 1h-only fit-row cap).
`CLAUDE.md`: in the "1h TRAINING TEMPORARILY DISABLED" paragraph, append: 2026-07-04 fit-row cap `crypto_train_max_fit_rows_1h` (150k newest) shipped; `CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES=15m,1h` restored — watch one full cycle under 32g before trusting it.

- [ ] **Step 7: Commit**

```bash
git add src/kalshi_bot/config.py src/kalshi_bot/crypto/services.py tests/unit/test_crypto_train_row_limit.py docs/training-dials-and-knobs.md CLAUDE.md
git commit -m "feat(trainer): 1h fit-row cap (150k newest) to fix the 1h candidate-fit 32g OOM; re-enable 15m,1h

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

- [ ] **Step 8: Deploy the trainer + watch one cycle**

```bash
grep -rn "CRYPTO_CONTINUOUS_TRAIN_FREQUENCIES" infra/docker-compose.yml .env
# set the trainer's value to "15m,1h" where found (compose fallback and/or .env)
docker compose --env-file .env -f infra/docker-compose.yml build trainer_production
docker compose --env-file .env -f infra/docker-compose.yml up -d --no-deps --force-recreate trainer_production
```

(Trainer is not blue/green; recreating it does not touch trading.) Then monitor: `docker stats --no-stream trainer_production` periodically through the first 1h asset fit and check `docker logs trainer_production --since 10m | grep -i "1h\|candidate"` — success = a 1h candidate report completes with peak mem < ~28g and no unhealthy flap. If it OOMs again, halve `CRYPTO_TRAIN_MAX_FIT_ROWS_1H` via env (no code change) and recreate.

---

### Task 4: Leg 3 — weather lock-in market-edge evaluation (offline, deliverable = research doc)

Simulate the lock-in strategy (`docs/research/2026-06-22-weather-lockin-mvp-spec.md`) against the KXHIGH intraday quotes collected since 2026-06-22 in `historical_market_snapshots` (models.py:1043 — `market_ticker`, `series_ticker`, `station_id`, `asof_ts`, `yes_bid_dollars`, `yes_ask_dollars`, `payload`). Observed highs come from IEM ASOS (`kalshi_bot.integrations.asos_archive.IemAsosClient` — the settlement-station source; do NOT use Open-Meteo). The eval computes lock state directly from running-high vs strike — deliberately NOT via `score_weather_market`, which has the known LOCKED_YES current-temp bug (MVP spec gap #1).

**Files:**
- Create: `scripts/weather_lockin_eval.py`
- Create: `docs/research/2026-07-05-weather-lockin-market-edge-eval.md` (the deliverable, written from the script's output)

**Interfaces:**
- Consumes: `IemAsosClient` (check its exact fetch signature first: `grep -n "class IemAsosClient" -A 25 src/kalshi_bot/integrations/asos_archive.py`), station map from the weather YAML (`series_ticker → station_id`, e.g. KXHIGHNY→KNYC in `docs/examples/weather_markets.example.yaml`; the deployed map path is `WEATHER_MARKET_MAP_PATH`).

- [ ] **Step 1: Scope the collected data (exact queries, run in the postgres container)**

```bash
docker exec infra-postgres_production-1 psql -U postgres -d kalshi_bot -c \
 "SELECT series_ticker, count(*), min(asof_ts), max(asof_ts), count(DISTINCT market_ticker)
  FROM historical_market_snapshots
  WHERE asof_ts >= '2026-06-22' AND series_ticker LIKE 'KXHIGH%'
  GROUP BY 1 ORDER BY 2 DESC;"
docker exec infra-postgres_production-1 psql -U postgres -d kalshi_bot -c \
 "SELECT market_ticker, asof_ts, yes_bid_dollars, yes_ask_dollars
  FROM historical_market_snapshots
  WHERE series_ticker LIKE 'KXHIGH%' ORDER BY asof_ts DESC LIMIT 5;"
```

If snapshot cadence is far coarser than 300s or quotes are mostly NULL, STOP and report — the eval is not runnable yet and the finding is "keep collecting until <date>".

- [ ] **Step 2: Write the eval script**

`scripts/weather_lockin_eval.py`, run INSIDE the app container (DB + integrations available). Structure (complete the ASOS call after the Step-1 signature check):

```python
# scripts/weather_lockin_eval.py
"""Offline weather lock-in market-edge eval (design 2026-07-04, Leg 3).

For every settled KXHIGH market with intraday quote snapshots since
2026-06-22: compute the station's running high (IEM ASOS, settlement
station), find the first snapshot where the market is LOCKED (running high
>= floor strike -> YES certain for 'greater' markets), then simulate a
taker BUY at the NEXT snapshot's ask (honest-fill rule) with the 7% fee.
LOCKED_NO (running high already > cap on 'less' markets) mirrors with NO.
'between' brackets are skipped per the MVP spec. Prints per-trade JSONL and
a summary; the research doc is written from this output.

  docker cp scripts/weather_lockin_eval.py infra-app_production_green-1:/app/
  docker exec infra-app_production_green-1 python /app/weather_lockin_eval.py
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import UTC, datetime

from sqlalchemy import text

from kalshi_bot.config import get_settings
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.asos_archive import IemAsosClient

SINCE = "2026-06-22"
FEE = 0.07
MIN_EDGE = 0.02          # only take locks the market prices >=2c away from certainty
STATIONS = {"KXHIGHAUS": "KAUS", "KXHIGHNY": "KNYC", "KXHIGHCHI": "KMDW",
            "KXHIGHDEN": "KDEN", "KXHIGHLAX": "KLAX", "KXHIGHMIA": "KMIA",
            "KXHIGHPHIL": "KPHL"}   # extend from the deployed YAML in Step 1


async def main() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    async with factory() as session:
        rows = (await session.execute(text("""
            SELECT market_ticker, series_ticker, station_id, local_market_day,
                   asof_ts, yes_bid_dollars, yes_ask_dollars, payload
            FROM historical_market_snapshots
            WHERE asof_ts >= :since AND series_ticker LIKE 'KXHIGH%'
            ORDER BY market_ticker, asof_ts
        """), {"since": SINCE})).mappings().all()

    by_market: dict[str, list] = defaultdict(list)
    for r in rows:
        by_market[r["market_ticker"]].append(r)

    asos = IemAsosClient(settings)   # adjust ctor/fetch to the real signature (Step 1 grep)
    trades, skipped = [], defaultdict(int)
    for ticker, snaps in sorted(by_market.items()):
        p0 = snaps[0]["payload"] or {}
        floor = p0.get("floor_strike")
        cap = p0.get("cap_strike")
        result = p0.get("result") or (snaps[-1]["payload"] or {}).get("result")
        # strike orientation: greater-style (floor only) locks YES; between skipped
        if floor is None or (floor is not None and cap is not None):
            skipped["between_or_no_strike"] += 1
            continue
        if result not in ("yes", "no"):
            skipped["unsettled"] += 1
            continue
        station = STATIONS.get(snaps[0]["series_ticker"] or "")
        if not station:
            skipped["no_station_map"] += 1
            continue
        day = snaps[0]["local_market_day"]
        obs = await asos.fetch_hourly_temps(station, day)   # [(ts_utc, temp_f)] — confirm name in Step 1
        if not obs:
            skipped["no_obs"] += 1
            continue
        # running high at each obs timestamp
        run_high, highs = float("-inf"), []
        for ts, tf in sorted(obs):
            run_high = max(run_high, tf)
            highs.append((ts, run_high))

        def high_at(t: datetime) -> float | None:
            best = None
            for ts, h in highs:
                if ts <= t:
                    best = h
                else:
                    break
            return best

        lock_i = None
        for i, s in enumerate(snaps):
            h = high_at(s["asof_ts"].astimezone(UTC))
            if h is not None and h >= float(floor):
                lock_i = i
                break
        if lock_i is None or lock_i + 1 >= len(snaps):
            skipped["never_locked_or_no_next_snap"] += 1
            continue
        fill = snaps[lock_i + 1]                    # honest fill: NEXT snapshot
        ask = fill["yes_ask_dollars"]
        if ask is None:
            skipped["no_ask_at_fill"] += 1
            continue
        ask = float(ask)
        edge = 1.0 - ask                            # locked YES ⇒ fair 1.0
        if edge < MIN_EDGE:
            skipped["edge_below_min"] += 1
            continue
        fee = FEE * ask * (1.0 - ask)
        won = result == "yes"                       # lock says this should ALWAYS be yes
        net = (1.0 - ask - fee) if won else (-ask - fee)
        trades.append({"ticker": ticker, "day": day, "ask": round(ask, 4),
                       "edge": round(edge, 4), "won": won, "net": round(net, 4),
                       "lock_ts": snaps[lock_i]["asof_ts"].isoformat(),
                       "fill_ts": fill["asof_ts"].isoformat()})
        print(json.dumps(trades[-1]), flush=True)

    wins = sum(1 for t in trades if t["won"])
    print(json.dumps({"summary": {
        "trades": len(trades), "wins": wins,
        "lock_violations": len(trades) - wins,      # >0 ⇒ basis bug, investigate before trusting ANY of it
        "net_total": round(sum(t["net"] for t in trades), 4),
        "avg_net": round(sum(t["net"] for t in trades) / len(trades), 4) if trades else None,
        "skipped": dict(skipped)}}, flush=True))


if __name__ == "__main__":
    asyncio.run(main())
```

Before running: `grep -n "class IemAsosClient" -A 25 src/kalshi_bot/integrations/asos_archive.py` and align the constructor + fetch call (name/args) with reality; also confirm `payload` carries `floor_strike`/`cap_strike`/`result` from the Step-1 sample query — if strikes live elsewhere (e.g. parsed from ticker), parse the ticker suffix instead (KXHIGH tickers end `-T<strike>` / `-B<low>-<high>` style; inspect 5 real tickers first).

- [ ] **Step 3: Run it and write the research doc**

```bash
docker cp scripts/weather_lockin_eval.py infra-app_production_green-1:/app/weather_lockin_eval.py
docker exec infra-app_production_green-1 python /app/weather_lockin_eval.py > /tmp/claude-1000/-home-user1-workspace-kalshi-bot/*/scratchpad/lockin_eval.jsonl 2>&1 || true
tail -3 <that file>
```

Write `docs/research/2026-07-05-weather-lockin-market-edge-eval.md`: data window, per-city trade counts, win rate, `lock_violations` (MUST be explained if >0 — station basis or obs-lag bug), net after fees, avg edge captured, and the explicit go/no-go: net>0 with ≥20 trades and 0 unexplained violations → design the capped pilot (separate follow-up); otherwise → keep collecting, revisit at <date>.

- [ ] **Step 4: Commit**

```bash
git add scripts/weather_lockin_eval.py docs/research/2026-07-05-weather-lockin-market-edge-eval.md
git commit -m "research(weather): lock-in market-edge eval vs collected KXHIGH quotes — <headline number>

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 5: Leg 4 — commodities scoping spike (deliverable = ranked go/no-go doc)

Public-API only; runs on the host venv (light). No trading code.

**Files:**
- Create: `scripts/commodities_scope.py`
- Create: `docs/research/2026-07-05-commodities-scoping.md`

- [ ] **Step 1: Write the scoping script**

```python
# scripts/commodities_scope.py
"""Commodities vertical scoping (design 2026-07-04, Leg 4). Public API only.

For each series in Kalshi's Commodities category: markets, settle cadence,
volume/open interest, spread. Output: JSON rows to rank in the research doc.
Run: source .venv/bin/activate && python scripts/commodities_scope.py
"""
from __future__ import annotations

import json
import statistics
import time
import urllib.request

BASE = "https://api.elections.kalshi.com/trade-api/v2"


def get(path: str) -> dict:
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "kalshi-bot-scope/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main() -> None:
    series = get("/series?category=Commodities").get("series") or []
    print(f"# {len(series)} commodity series", flush=True)
    for s in series:
        st = s.get("ticker")
        try:
            mk = get(f"/markets?series_ticker={st}&status=open&limit=200").get("markets") or []
            settled = get(f"/markets?series_ticker={st}&status=settled&limit=200").get("markets") or []
        except Exception as e:
            print(json.dumps({"series": st, "error": str(e)[:120]}), flush=True)
            continue
        spreads, vols, ois = [], [], []
        for m in mk:
            yb, ya = m.get("yes_bid_dollars"), m.get("yes_ask_dollars")
            if yb is not None and ya is not None:
                try:
                    spreads.append(float(ya) - float(yb))
                except (TypeError, ValueError):
                    pass
            for key, acc in (("volume", vols), ("open_interest", ois)):
                v = m.get(key)
                if v is not None:
                    acc.append(float(v))
        closes = sorted(str(m.get("close_time") or "") for m in settled if m.get("close_time"))
        row = {"series": st, "title": s.get("title"), "frequency": s.get("frequency"),
               "open_markets": len(mk),
               "settled_recent": len(settled),
               "first_settled_close": closes[0][:10] if closes else None,
               "last_settled_close": closes[-1][:10] if closes else None,
               "median_spread": round(statistics.median(spreads), 3) if spreads else None,
               "total_volume_open": sum(vols) or None,
               "total_oi_open": sum(ois) or None}
        print(json.dumps(row), flush=True)
        time.sleep(0.3)   # be polite; unauthenticated endpoint


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

```bash
source .venv/bin/activate && python scripts/commodities_scope.py | tee /tmp/claude-1000/-home-user1-workspace-kalshi-bot/*/scratchpad/commodities_scope.jsonl
```

Expected: one JSON row per series (~40). Daily series (KXBRENTD, KXCOPPERD, KXAAAGASD) and weekly (KXNGASW, KXWHEATW, KXSTEELW, KXCOCOAW) are the a-priori candidates — verify with numbers.

- [ ] **Step 3: Write the ranked go/no-go doc**

`docs/research/2026-07-05-commodities-scoping.md` with:
- The measured table (settles/week, spread, volume, OI per series), ranked by `settles_per_week × volume`, spread as tiebreak.
- Settlement-source column per top-5 series (from each series' rules page — `get("/series/{ticker}")` exposes `settlement_sources` if present; otherwise note manually from kalshi.com rules).
- Fresh-underlying-feed assessment per top-5: Brent/WTI (ICE/CME delayed quotes — is a free ≤1min feed available?), copper (COMEX), natgas (Henry Hub), AAA gas (daily-published — no intraday feed, note it).
- Explicit verdict per series: GO-candidate (liquid + fresh feed + ≥daily settles) / NO (illiquid, no feed, or too-slow cadence), and a final recommendation paragraph. No implementation is authorized by this doc — next step would be its own design.

- [ ] **Step 4: Commit**

```bash
git add scripts/commodities_scope.py docs/research/2026-07-05-commodities-scoping.md
git commit -m "research(commodities): scoping spike — measured liquidity/cadence + go/no-go ranking

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 6: Memory + rollout notes

- [ ] **Step 1: Update memory**

Update `/home/user1/.claude/projects/-home-user1-workspace-kalshi-bot/memory/project_stale_quote_edge.md` (pilot now 5 assets + 1h shadow scanner exists), and add `project_live_breadth_push.md` (type: project) summarizing: the 4 legs, what shipped, what's pending measurement (1h shadow ≥1 week; trainer 1h first cycle; weather eval verdict; commodities verdict), with `[[project-stale-quote-edge]]`-style links. Add index lines to `MEMORY.md`.

- [ ] **Step 2: Report to operator**

Final message: what is now live (5-asset pilot), what is measuring (1h shadow, trainer 1h cycle), the weather eval number + verdict, the commodities ranking headline, and the calendar checkpoints (1h graduation review date, weather revisit date).
