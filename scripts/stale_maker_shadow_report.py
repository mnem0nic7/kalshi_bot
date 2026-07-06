"""Analyze the maker-side counterfactual shadow (spec 2026-07-06, Task 7).

scripts/stale_quote_pilot.py now records a "maker counterfactual" every time a
signal is rejected by the `live_edge_too_small` guard: where would a resting
bid at fair-minus-3c (fair-plus-3c for NO) have sat, and would it have been
traded through before settlement? This is a SIGNAL-ONLY measurement — the
pilot places zero extra orders for it — so it can tell us whether relaxing
the live-edge floor into a passive/maker order is worth building, without any
additional order risk.

Reads the pilot's JSONL, keeps `type == "maker_settle"` records (each already
carries the settlement result + gross P&L if the proxy fill occurred), and
reports:
  - how many `live_edge_too_small` rejects got a trackable maker shadow
    (`maker_yes_price` present) vs how many of those have matured/settled
  - the proxy fill rate — the fraction of settled shadows where the cached
    quote traded through our resting price before settlement. THIS IS AN
    UPPER BOUND: the proxy only checks that the market crossed our price, not
    that our order would have reached the front of the queue there
    (queue position ignored).
  - for proxy-filled rows: gross P&L, and net P&L under two fee assumptions —
    (a) maker fee = $0 (Kalshi does not currently charge resting-order maker
        fees on these series; treat this as the optimistic case and confirm
        against the live fee schedule before trusting it), and
    (b) the pilot's own taker fee formula (0.07 * p * (1-p)) applied as a
        deliberately CONSERVATIVE bound, in case a live maker order here
        would in practice cross like a taker (e.g. IOC-vs-resting execution
        nuances) — this is not a claim that maker orders are charged the
        taker rate, just a worst-case sanity bound.

  docker cp scripts/stale_maker_shadow_report.py infra-trainer_production-1:/tmp/ && \
    docker exec -i infra-trainer_production-1 python /tmp/stale_maker_shadow_report.py \
      < /home/user1/kalshi_stale_pilot/pilot.jsonl
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict

TAKER_FEE_RATE = 0.07  # conservative bound only — NOT a claim maker orders pay this


def main() -> None:
    signals: list[dict] = []
    settles: list[dict] = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("type") == "signal" and rec.get("guard") == "live_edge_too_small" \
                and "maker_yes_price" in rec:
            signals.append(rec)
        elif rec.get("type") == "maker_settle":
            settles.append(rec)

    print("Maker-side counterfactual shadow report (signal-only, zero order risk).")
    print("NOTE: shadows spawn only where the live book ALREADY repriced through fair "
          "(live_edge_too_small rejects) — a structurally adverse-selected sample; expect "
          "pessimistic numbers vs a real always-on maker.")
    print("Fee assumptions: (a) maker fee = $0 [confirm against Kalshi's live maker fee "
          "schedule before trusting], (b) taker formula 0.07*p*(1-p) as a CONSERVATIVE "
          "upper-bound fee, not a claim of the actual maker rate.")
    print("Proxy fill rate is an UPPER BOUND (queue position ignored) — it only checks "
          "that the market traded through our resting price, not that we'd have been "
          "front of queue there.\n")

    print(f"live_edge_too_small rejects with a trackable maker shadow: {len(signals)}")
    print(f"maker shadows matured/settled: {len(settles)}")
    if not settles:
        return

    filled = [s for s in settles if s.get("filled_proxy")]
    fill_rate = 100 * len(filled) / len(settles)
    print(f"proxy fill rate (UPPER BOUND): {len(filled)}/{len(settles)} = {fill_rate:.0f}%\n")
    if not filled:
        return

    gross_total, net0_total, netfee_total = 0.0, 0.0, 0.0
    wins0, winsfee = 0, 0
    by_asset: dict[str, list] = defaultdict(lambda: [0.0, 0.0, 0])

    for s in filled:
        gross = float(s.get("gross_if_filled", 0.0))
        entry = float(s.get("entry", s.get("yes_price", 0.0)))
        net0 = gross  # maker fee = $0 assumption
        netfee = gross - TAKER_FEE_RATE * entry * (1 - entry)  # conservative bound
        gross_total += gross
        net0_total += net0
        netfee_total += netfee
        wins0 += 1 if net0 > 0 else 0
        winsfee += 1 if netfee > 0 else 0
        asset = s.get("ticker", "?").split("-")[0]
        a = by_asset[asset]
        a[0] += net0
        a[1] += netfee
        a[2] += 1

    n = len(filled)
    print(f"proxy-filled: n={n}")
    print(f"  gross:                 ${gross_total:+.4f}  avg/ct=${gross_total / n:+.4f}")
    print(f"  net @ maker-fee=$0:    ${net0_total:+.4f}  avg/ct=${net0_total / n:+.4f}  "
          f"win={100 * wins0 / n:.0f}%")
    print(f"  net @ taker-fee-bound: ${netfee_total:+.4f}  avg/ct=${netfee_total / n:+.4f}  "
          f"win={100 * winsfee / n:.0f}%")
    print("\nby asset (net @ maker-fee=$0 / net @ taker-fee-bound):")
    for asset, (a0, afee, an) in sorted(by_asset.items()):
        print(f"  {asset}: n={an}  ${a0:+.4f} / ${afee:+.4f}  "
              f"avg=${a0 / an:+.4f} / ${afee / an:+.4f}")


if __name__ == "__main__":
    main()
