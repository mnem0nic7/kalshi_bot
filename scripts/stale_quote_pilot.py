"""Stale-quote micro-pilot — operator-gated LIVE taker orders with hard caps.

DEFAULT: DISABLED. Without STALE_PILOT_ENABLED=1 every ticket is routed with
shadow_mode=True, so the FULL ExecutionService path runs (kill switch, color,
creds) but returns shadow_skipped — a true dry-run. Enabling requires the
operator to set, explicitly:
  STALE_PILOT_ENABLED=1
  STALE_PILOT_ASSETS=BTC,BNB                (allowlist)
  STALE_PILOT_MAX_TRADES_PER_DAY=10
  STALE_PILOT_MAX_OPEN=1                    (correlated-exposure cap)
  STALE_PILOT_DAILY_LOSS_STOP=3.0           (dollars)
  STALE_PILOT_MAX_ENTRY=0.75                (dollars)

Detection is identical to scripts/stale_quote_shadow.py (imported); guards and
ticket construction are the TDD'd kalshi_bot.crypto.stale_quote_pilot. Orders go
ONLY through ExecutionService (architecture rule) as 1-contract IOC takers.
Every action is logged as JSONL (stdout with STALE_PILOT_STDOUT=1, else
$STALE_PILOT_OUT). Purpose: measure LIVE fill rate + realized capture vs the
backtest (docs/research/2026-07-02-stale-quote-taker-edge.md) under strict caps.

  docker exec infra-app_production_blue-1 env STALE_PILOT_STDOUT=1 \
      python /app/stale_quote_pilot.py            # dry-run (no orders)
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kalshi_bot.config import get_settings
from kalshi_bot.core.fixed_point import make_client_order_id
from kalshi_bot.crypto.stale_quote_pilot import PilotConfig, PilotState, build_pilot_ticket, evaluate_guards
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory
from kalshi_bot.integrations.kalshi import KalshiClient
from kalshi_bot.services.execution import ExecutionService
from kalshi_bot.crypto.services import (
    CRYPTO_SPOT_CONTEXT_LIVE,
    _crypto_vol_normal_fair_up,
    _decimal,
    _prepare_spot_context_series,
    _spot_context_for_decision,
)

# detection constants — keep in lockstep with scripts/stale_quote_shadow.py
VOL_MODEL = {
    "model_type": "vol_normal_fair_value",
    "volatility_field": "spot_realized_volatility_32",
    "step_interval_seconds": 60,
    "max_abs_z": 3.0,
}
SERIES = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M",
          "BNB": "KXBNB15M", "DOGE": "KXDOGE15M", "HYPE": "KXHYPE15M"}
POLL_S = 2.0  # was 5.0 — a measured 8.35c-edge fill was lost to loop latency;
              # the token bucket in KalshiClient keeps request rate safe
LOOKBACK_S = 18.0
DFAIR_TH = 0.10
QUOTE_EPS = 0.01
MIN_TTC_S = 90
MAX_TTC_S = 870
MAX_SPREAD = 0.20

OUT_DIR = os.environ.get("STALE_PILOT_OUT", "/app/data/stale_pilot")
STDOUT = os.environ.get("STALE_PILOT_STDOUT") == "1"


def _env_cfg() -> PilotConfig:
    return PilotConfig(
        enabled=os.environ.get("STALE_PILOT_ENABLED") == "1",
        assets=tuple(a.strip().upper() for a in os.environ.get("STALE_PILOT_ASSETS", "").split(",") if a.strip()),
        max_trades_per_day=int(os.environ.get("STALE_PILOT_MAX_TRADES_PER_DAY", "0")),
        max_open_positions=int(os.environ.get("STALE_PILOT_MAX_OPEN", "0")),
        daily_loss_stop_dollars=float(os.environ.get("STALE_PILOT_DAILY_LOSS_STOP", "0")),
        max_entry_dollars=float(os.environ.get("STALE_PILOT_MAX_ENTRY", "0")),
    )


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
        with open(os.path.join(OUT_DIR, "pilot.jsonl"), "a") as fh:
            fh.write(line + "\n")


@dataclass
class _RoomShim:
    """ExecutionService.execute only reads shadow_mode; disabled pilot => shadow."""

    shadow_mode: bool


async def live_book_top(client, ticker: str) -> tuple[float | None, float | None]:
    """Real-time (best_yes_bid, best_yes_ask) from the orderbook endpoint.

    list_markets quote fields lag during fast moves (first 5 live IOCs at the
    cached quote all canceled unfilled against DEEP books); the orderbook is the
    real book: yes_dollars = resting YES bids, no_dollars = resting NO bids, and
    the effective YES ask = 1 - best NO bid.
    """
    ob = (await client._request("GET", f"/markets/{ticker}/orderbook")).get("orderbook_fp") or {}

    def best(levels):
        px = [f(p) for p, _cnt in (levels or []) if f(p) is not None]
        return max(px) if px else None

    yes_bid = best(ob.get("yes_dollars"))
    no_bid = best(ob.get("no_dollars"))
    yes_ask = (1.0 - no_bid) if no_bid is not None else None
    return yes_bid, yes_ask


async def main() -> None:
    settings = get_settings()
    cfg = _env_cfg()
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    client = KalshiClient(settings)
    execution = ExecutionService(settings, client)
    state = PilotState()
    open_trades: list[dict] = []   # {ticker, side, entry, settle_by}
    hist: dict[str, list] = {}
    signaled: set[str] = set()
    emit({"type": "start", "ts": datetime.now(UTC).isoformat(), "enabled": cfg.enabled,
          "assets": list(cfg.assets), "caps": {"trades_day": cfg.max_trades_per_day,
          "open": cfg.max_open_positions, "loss_stop": cfg.daily_loss_stop_dollars,
          "max_entry": cfg.max_entry_dollars}})

    watch = [a for a in (cfg.assets or tuple(SERIES))]

    async def reconcile() -> None:
        """Settle finished trades: update realized P&L + open-position count."""
        nowt = datetime.now(UTC)
        still: list[dict] = []
        for tr in open_trades:
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
            state.realized_pnl_today += net
            state.open_positions = max(0, state.open_positions - 1)
            emit({"type": "settle", "ts": nowt.isoformat(), "ticker": tr["ticker"],
                  "result": res, "net": round(net, 4), "pnl_today": round(state.realized_pnl_today, 4)})
        open_trades[:] = still

    last_reconcile = 0.0
    while True:
        cycle_t0 = asyncio.get_event_loop().time()
        if cycle_t0 - last_reconcile > 60:
            await reconcile()
            last_reconcile = cycle_t0
        now = datetime.now(UTC)
        for asset in watch:
            series = SERIES.get(asset)
            if not series:
                continue
            try:
                async with factory() as session:
                    repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
                    rows = await repo.list_crypto_spot_ohlc(
                        frequency="15m", kalshi_env=settings.kalshi_env, asset_symbol=asset,
                        since=now - timedelta(hours=2), limit=100_000,
                    )
                    control = await repo.get_deployment_control(kalshi_env=settings.kalshi_env)
                rows = sorted(rows, key=lambda r: r.end_ts)
                prep = _prepare_spot_context_series(rows)
                resp = await client.list_markets(series_ticker=series, status="open", limit=100)
            except Exception as e:
                emit({"type": "error", "ts": now.isoformat(), "asset": asset, "err": str(e)[:200]})
                continue
            for m in resp.get("markets") or []:
                tk = str(m.get("ticker"))
                yb, ya = f(m.get("yes_bid_dollars")), f(m.get("yes_ask_dollars"))
                strike = f(m.get("floor_strike"))
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
                row = {**ctx, "time_to_close_seconds": int(ttc)}
                fair_now = _crypto_vol_normal_fair_up(row, VOL_MODEL)
                mid_now = (yb + ya) / 2
                h = hist.setdefault(tk, [])
                h.append((now, mid_now, spot_now))
                if len(h) > 40:
                    del h[: len(h) - 40]
                if tk in signaled or fair_now is None or spot_now is None or mny is None:
                    continue
                ref = None
                for ts0, mid0, sp0 in reversed(h[:-1]):
                    if (now - ts0).total_seconds() >= LOOKBACK_S:
                        ref = (ts0, mid0, sp0)
                        break
                if not ref or not ref[2]:
                    continue
                fair_ref = _crypto_vol_normal_fair_up(
                    {**row, "spot_moneyness_pct": mny - math.log(spot_now / ref[2])}, VOL_MODEL)
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
                allowed, reason = evaluate_guards(cfg, state, asset=asset, entry_dollars=entry, now=now)
                rec = {"type": "signal", "ts": now.isoformat(), "asset": asset, "ticker": tk,
                       "side": side, "dfair": round(dfair, 4), "entry": round(entry, 4),
                       "guard": reason}
                if not allowed:
                    emit(rec)
                    continue
                # Re-price off the REAL-TIME book right before submit — the cached
                # market quote lags during fast moves (0/5 fills at cached prices
                # against deep books). Log both to quantify the lag.
                try:
                    live_bid, live_ask = await live_book_top(client, tk)
                except Exception as e:
                    rec["book_error"] = str(e)[:120]
                    live_bid, live_ask = None, None
                rec["cached_quote"] = [yb, ya]
                rec["live_book"] = [live_bid, live_ask]
                if live_bid is None or live_ask is None:
                    rec["guard"] = "no_live_book"
                    emit(rec)
                    continue
                live_entry = live_ask if side == "yes" else 1.0 - live_bid
                rec["live_entry"] = round(live_entry, 4)
                # Require the edge to SURVIVE at the live price. The signal
                # fires vs the lagging cached quote; when the live book has
                # already moved to fair, filling is easy but EV is negative
                # (adverse selection — e.g. BTC 12:45: fair ~0.73, live ask
                # 0.72, edge ~1c < ~1.4c fee). 3c clears the worst-case fee
                # (~1.75c) with margin.
                edge_live = (float(fair_now) - live_ask) if side == "yes" else (live_bid - float(fair_now))
                rec["edge_live"] = round(edge_live, 4)
                if edge_live < 0.03:
                    rec["guard"] = "live_edge_too_small"
                    emit(rec)
                    continue
                # Credible-edge ceiling (same principle as RISK_MAX_CREDIBLE_EDGE_BPS):
                # live day-1 results — claimed edge >=15c went 0W/3L (fair estimate
                # breaks in violent moves: whipsaw vs the 18s spot ref / sigma blowup),
                # while 9-11c went 2W/0L. Beyond the ceiling, "edge" is model error.
                if edge_live > 0.15:
                    rec["guard"] = "live_edge_not_credible"
                    emit(rec)
                    continue
                # re-check price guards at the live price
                if not (0.03 <= live_entry <= 0.97) or live_entry > cfg.max_entry_dollars:
                    rec["guard"] = "live_entry_out_of_bounds"
                    emit(rec)
                    continue
                # Price AT the live touch: the exchange audit showed 3/7 touch
                # IOCs actually filled (43%) with one price-improved — no need
                # to pay through. (The earlier "0/7 at touch" was a fill-check
                # bug, not reality.)
                ticket = build_pilot_ticket(market_ticker=tk, side=side,
                                            yes_bid=_decimal(f"{live_bid:.4f}"), yes_ask=_decimal(f"{live_ask:.4f}"))
                coid = make_client_order_id("stale-quote-pilot", tk, ticket.nonce)
                receipt = await execution.execute(
                    room=_RoomShim(shadow_mode=not cfg.enabled),
                    control=control,
                    ticket=ticket,
                    client_order_id=coid,
                )
                rec["order_status"] = receipt.status
                rec["client_order_id"] = coid
                if receipt.status not in ("shadow_skipped", "kill_switch_blocked",
                                          "inactive_color_skipped", "write_credentials_missing") \
                        and not receipt.status.startswith("rejected"):
                    state.trades_today += 1  # every submitted order consumes the daily budget
                    # IOC can cancel with 0 fills — only a real fill is an open
                    # position. Look the order up BY client_order_id via
                    # get_orders(ticker=...): receipt.external_order_id proved
                    # unreliable (3 real fills were logged filled=0.0 and went
                    # untracked until the exchange audit caught them).
                    filled = 0.0
                    fill_price = None
                    await asyncio.sleep(2.0)
                    try:
                        oresp = await client.get_orders(ticker=tk, limit=20)
                        for o in (oresp.get("orders") or []):
                            if o.get("client_order_id") == coid:
                                filled = f(o.get("fill_count_fp")) or 0.0
                                fill_price = f(o.get("yes_price_dollars"))
                                rec["final_order_status"] = o.get("status")
                                break
                    except Exception as e:
                        rec["fill_check_error"] = str(e)[:120]
                    rec["filled"] = filled
                    if filled > 0:
                        eff_entry = fill_price if (fill_price is not None and side == "yes") \
                            else (1.0 - fill_price if fill_price is not None else live_entry)
                        state.open_positions += 1
                        open_trades.append({"ticker": tk, "side": side, "entry": eff_entry,
                                            "settle_by": cl + timedelta(seconds=60)})
                emit(rec)
        elapsed = asyncio.get_event_loop().time() - cycle_t0
        await asyncio.sleep(max(0.5, POLL_S - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
