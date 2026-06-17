"""Market-making backtest core: maker entry rule + realistic fill P&L.

Plan §4.4/§5: post a maker buy only when fair − price exceeds the round-trip
cost + buffer; avoid the ~45–55¢ peak-fee band; Kalshi pays no maker rebate; a
resting order fills only if the market trades to it, and settlement (not mid)
decides P&L so adverse selection is captured.
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.mm.backtest import maker_entry, maker_fills, settle_net_pnl


def test_maker_entry_picks_yes_side_with_edge():
    entry = maker_entry(Decimal("0.80"), edge_buffer=Decimal("0.02"), fee_rate=Decimal("0.0175"))
    assert entry is not None
    assert entry["side"] == "yes"
    assert entry["limit"] == Decimal("0.78")  # side_prob 0.80 − buffer 0.02
    assert entry["edge"] > 0


def test_maker_entry_picks_no_side_when_fair_below_half():
    entry = maker_entry(Decimal("0.20"), edge_buffer=Decimal("0.02"), fee_rate=Decimal("0.0175"))
    assert entry is not None
    assert entry["side"] == "no"          # no-side prob = 0.80
    assert entry["limit"] == Decimal("0.78")


def test_maker_entry_avoids_peak_fee_band():
    # fair 0.52 → limit 0.50, inside the 0.45–0.55 avoid band → stand down.
    assert maker_entry(Decimal("0.52"), edge_buffer=Decimal("0.02"), fee_rate=Decimal("0.0175")) is None


def test_maker_entry_none_when_edge_below_min():
    # Tiny buffer → edge can't cover the fee → no order.
    assert maker_entry(
        Decimal("0.80"), edge_buffer=Decimal("0.0005"), fee_rate=Decimal("0.0175"), min_edge=Decimal("0.01")
    ) is None


def test_maker_fills_only_when_market_trades_through_limit():
    # Resting yes buy at 0.78 fills only if a later ask ≤ 0.78.
    assert maker_fills(Decimal("0.78"), future_side_asks=[Decimal("0.80"), Decimal("0.77")]) is True
    assert maker_fills(Decimal("0.78"), future_side_asks=[Decimal("0.80"), Decimal("0.79")]) is False


def test_settle_net_pnl_winner_and_loser():
    # Win: payoff 1 − limit 0.78 − fee.
    fee = Decimal("0.0175") * Decimal("0.78") * (Decimal("1") - Decimal("0.78"))
    win = settle_net_pnl(side="yes", limit=Decimal("0.78"), label_yes=1, fee=fee)
    loss = settle_net_pnl(side="yes", limit=Decimal("0.78"), label_yes=0, fee=fee)
    assert win == (Decimal("1") - Decimal("0.78") - fee)
    assert loss == (Decimal("0") - Decimal("0.78") - fee)
