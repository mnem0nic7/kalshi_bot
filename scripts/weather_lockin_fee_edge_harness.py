"""Weather lock-in fee-edge harness (spec step 3) — the market-edge go/no-go.

Joins three bounded DB dumps (no heavy scans, no external API):
  quotes.csv  : KXHIGH intraday market quotes (market_ticker, asof_ts, yes_ask, no_ask)
  obs.csv     : per-station intraday observed temps (series_ticker, local_market_day, obs_ts, current_temp_f)
  specs.csv   : settled markets (market_ticker, series_ticker, local_market_day, kalshi_result,
                floor_strike, cap_strike, expiration_value)

For each settled market we reconstruct observed high-so-far over the local day, find the EARLIEST
intraday quote at which the contract is DETERMINISTICALLY locked by high-so-far alone, run the
already-tested fee/edge gate, and check the realized outcome vs settlement. This answers: does the
market leave positive fee-adjusted edge on already-decided contracts? (the lock-in MVP go/no-go).

Deterministic-early locks (knowable from high-so-far alone, no remaining-rise model):
  greater (floor only, YES=high>=floor): LOCKED_YES once high_so_far >= floor
  less    (cap only,   YES=high<=cap):   LOCKED_NO  once high_so_far >  cap
  between (floor&cap):                    LOCKED_NO  once high_so_far >  cap
LOCKED_YES for less/between needs end-of-day certainty (a remaining-rise model) → out of MVP scope.

Usage: python scripts/weather_lockin_fee_edge_harness.py /tmp/wxlockin
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from kalshi_bot.core.enums import WeatherResolutionState
from kalshi_bot.weather.lockin_gate import evaluate_lockin_fee_edge_gate

# series_ticker -> IANA tz (ASOS obs are local-naive; quotes are UTC-aware → convert quotes to local)
SERIES_TZ = {
    "KXHIGHAUS": "America/Chicago", "KXHIGHCHI": "America/Chicago", "KXHIGHDEN": "America/Denver",
    "KXHIGHLAX": "America/Los_Angeles", "KXHIGHMIA": "America/New_York", "KXHIGHNY": "America/New_York",
    "KXHIGHPHIL": "America/New_York", "KXHIGHTATL": "America/New_York", "KXHIGHTBOS": "America/New_York",
    "KXHIGHTDAL": "America/Chicago", "KXHIGHTDC": "America/New_York", "KXHIGHTHOU": "America/Chicago",
    "KXHIGHTLV": "America/Los_Angeles", "KXHIGHTMIN": "America/Chicago", "KXHIGHTNOLA": "America/Chicago",
    "KXHIGHTOKC": "America/Chicago", "KXHIGHTPHX": "America/Phoenix", "KXHIGHTSATX": "America/Chicago",
    "KXHIGHTSEA": "America/Los_Angeles", "KXHIGHTSFO": "America/Los_Angeles",
}


def _f(s):
    s = (s or "").strip()
    if s in ("", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _ts(s):
    return datetime.fromisoformat(s.strip())


def load(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def lock_state(floor, cap, high_so_far):
    """Return (WeatherResolutionState, strike_type) for an early-deterministic lock, else UNRESOLVED."""
    if floor is not None and cap is None:
        stype = "greater"
        # KXHIGH "greater" settles YES iff high STRICTLY > floor (boundary settle==floor => NO,
        # verified across all boundary cases). So lock YES only strictly past the floor.
        if high_so_far > floor:
            return WeatherResolutionState.LOCKED_YES, stype
    elif cap is not None and floor is None:
        stype = "less"
        if high_so_far > cap:
            return WeatherResolutionState.LOCKED_NO, stype
    elif floor is not None and cap is not None:
        stype = "between"
        if high_so_far > cap:
            return WeatherResolutionState.LOCKED_NO, stype
    else:
        stype = "unknown"
    return WeatherResolutionState.UNRESOLVED, stype


def main(base: str) -> None:
    quotes = load(f"{base}/quotes.csv")
    obs = load(f"{base}/obs.csv")
    specs = load(f"{base}/specs.csv")

    # obs indexed by (series_ticker, local_market_day) -> sorted [(obs_ts, temp_f)]
    obs_idx: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    for r in obs:
        t = _f(r["current_temp_f"])
        if t is None:
            continue
        obs_idx[(r["series_ticker"], r["local_market_day"])].append((_ts(r["obs_ts"]), t))
    for k in obs_idx:
        obs_idx[k].sort()

    # quotes indexed by market_ticker -> sorted [(asof_ts, yes_ask, no_ask)]
    q_idx: dict[str, list[tuple[datetime, float, float]]] = defaultdict(list)
    for r in quotes:
        ya, na = _f(r["yes_ask_dollars"]), _f(r["no_ask_dollars"])
        q_idx[r["market_ticker"]].append((_ts(r["asof_ts"]), ya, na))
    for k in q_idx:
        q_idx[k].sort()

    def high_so_far_at(series, day, when):
        series_obs = obs_idx.get((series, day))
        if not series_obs:
            return None
        # obs_ts is local-naive; convert the (UTC-aware) quote time to station-local naive.
        if when.tzinfo is not None:
            tz = SERIES_TZ.get(series)
            when = when.astimezone(ZoneInfo(tz)).replace(tzinfo=None) if tz else when.replace(tzinfo=None)
        vals = [t for (ts, t) in series_obs if ts <= when]
        return max(vals) if vals else None

    # per-market: earliest quote at which it is locked AND the gate says trade
    per_type = defaultdict(lambda: {"decided": 0, "traded": 0, "wins": 0, "pnl": Decimal("0"),
                                    "edge_avail": 0, "false_lock": 0})
    traded_markets = []
    locked_but_no_edge = 0
    locked_markets = 0

    for s in specs:
        mkt = s["market_ticker"]
        if mkt not in q_idx:
            continue
        floor, cap = _f(s["floor_strike"]), _f(s["cap_strike"])
        result = (s["kalshi_result"] or "").strip()
        if result not in ("yes", "no"):
            continue
        series, day = s["series_ticker"], s["local_market_day"]

        decided_here = False
        for (when, ya, na) in q_idx[mkt]:
            hsf = high_so_far_at(series, day, when)
            if hsf is None:
                continue
            state, stype = lock_state(floor, cap, hsf)
            if state == WeatherResolutionState.UNRESOLVED:
                continue
            if not decided_here:
                per_type[stype]["decided"] += 1
                locked_markets += 1
                decided_here = True
            gate = evaluate_lockin_fee_edge_gate(
                resolution_state=state,
                yes_ask_dollars=Decimal(str(ya)) if ya is not None else None,
                no_ask_dollars=Decimal(str(na)) if na is not None else None,
            )
            if not gate.should_trade:
                continue
            # first tradeable lock for this market — record and stop
            won = (gate.side == "yes" and result == "yes") or (gate.side == "no" and result == "no")
            pnl = gate.net_edge_dollars if won else -(gate.entry_price_dollars + gate.fee_dollars)
            per_type[stype]["traded"] += 1
            per_type[stype]["edge_avail"] += 1
            per_type[stype]["wins"] += 1 if won else 0
            per_type[stype]["false_lock"] += 0 if won else 1
            per_type[stype]["pnl"] += pnl
            traded_markets.append((mkt, stype, gate.side, str(gate.entry_price_dollars),
                                   "WIN" if won else "LOSE", str(pnl), f"hsf={hsf:.1f}",
                                   f"floor={floor}", f"cap={cap}", f"settle={s['expiration_value']}"))
            break
        else:
            if decided_here:
                locked_but_no_edge += 1

    print(f"settled markets with quotes considered = {sum(1 for s in specs if s['market_ticker'] in q_idx)}")
    print(f"markets that reached a deterministic lock = {locked_markets}")
    print(f"locked but NO fee-edge at any quote (market already repriced) = {locked_but_no_edge}")
    print()
    print(f"{'strike_type':10s} {'decided':>8s} {'traded':>7s} {'wins':>5s} {'win%':>6s} {'net_pnl$':>10s} {'avg$/trade':>11s}")
    tot_traded = tot_wins = 0
    tot_pnl = Decimal("0")
    for stype in ("greater", "less", "between"):
        d = per_type[stype]
        if d["decided"] == 0 and d["traded"] == 0:
            continue
        wr = (d["wins"] / d["traded"] * 100) if d["traded"] else float("nan")
        avg = (d["pnl"] / d["traded"]) if d["traded"] else Decimal("0")
        print(f"{stype:10s} {d['decided']:>8d} {d['traded']:>7d} {d['wins']:>5d} {wr:>5.1f}% "
              f"{float(d['pnl']):>10.3f} {float(avg):>11.4f}")
        tot_traded += d["traded"]
        tot_wins += d["wins"]
        tot_pnl += d["pnl"]
    wr = (tot_wins / tot_traded * 100) if tot_traded else float("nan")
    print(f"{'TOTAL':10s} {'':>8s} {tot_traded:>7d} {tot_wins:>5d} {wr:>5.1f}% {float(tot_pnl):>10.3f} "
          f"{float(tot_pnl/tot_traded) if tot_traded else 0:>11.4f}")
    print()
    print("sample traded lock-in decisions (first 25):")
    for row in traded_markets[:25]:
        print("  " + "  ".join(row))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/wxlockin")
