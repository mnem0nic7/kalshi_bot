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
POLL_S = 5.0
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
                ticket = build_pilot_ticket(market_ticker=tk, side=side,
                                            yes_bid=_decimal(str(yb)), yes_ask=_decimal(str(ya)))
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
                    # IOC can cancel with 0 fills (the latency race) — only a real
                    # fill is an open position; a phantom would block MAX_OPEN and
                    # poison settle P&L (first live order: submitted -> canceled, 0 fills).
                    filled = 0.0
                    fill_price = None
                    oid = receipt.external_order_id
                    await asyncio.sleep(2.0)
                    try:
                        if oid:
                            o = (await client.get_order(oid)).get("order") or {}
                            filled = f(o.get("fill_count_fp")) or 0.0
                            fill_price = f(o.get("yes_price_dollars"))
                            rec["final_order_status"] = o.get("status")
                    except Exception as e:
                        rec["fill_check_error"] = str(e)[:120]
                    rec["filled"] = filled
                    if filled > 0:
                        eff_entry = fill_price if (fill_price is not None and side == "yes") \
                            else (1.0 - fill_price if fill_price is not None else entry)
                        state.open_positions += 1
                        open_trades.append({"ticker": tk, "side": side, "entry": eff_entry,
                                            "settle_by": cl + timedelta(seconds=60)})
                emit(rec)
        elapsed = asyncio.get_event_loop().time() - cycle_t0
        await asyncio.sleep(max(0.5, POLL_S - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
