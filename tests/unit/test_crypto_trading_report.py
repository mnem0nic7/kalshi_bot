"""Pure builder for the crypto trading-evaluation report.

Codifies the per-asset evaluation we kept hand-writing in SQL: from
crypto_decision_outcomes aggregates (grouped by asset + gate_status), derive
decisions, the gate/block breakdown, fills, win rate, realized (gross) and
simulated P&L, net-of-fees P&L when fee estimates are supplied, plus the live
champion model per asset and a portfolio roll-up.
"""
from __future__ import annotations

from decimal import Decimal

from kalshi_bot.crypto.reporting import build_crypto_trading_report


def _agg(asset, gate_status, *, decisions, filled=0, wins=0, losses=0, realized="0", simulated="0"):
    return {
        "asset_symbol": asset,
        "gate_status": gate_status,
        "decisions": decisions,
        "filled": filled,
        "wins": wins,
        "losses": losses,
        "realized_pnl": Decimal(realized),
        "simulated_pnl": Decimal(simulated),
    }


def _rows():
    return [
        _agg("BTC", "blocked_max_entry_price", decisions=300),
        _agg("BTC", "blocked_fee_edge", decisions=40),
        _agg("BTC", "eligible", decisions=10, filled=8, wins=5, losses=3, realized="2.50", simulated="3.10"),
        _agg("ETH", "blocked_shrunk_edge", decisions=120),
    ]


def test_per_asset_aggregation_and_win_rate():
    report = build_crypto_trading_report(_rows())
    btc = report["assets"]["BTC"]
    assert btc["decisions"] == 350
    assert btc["filled"] == 8
    assert btc["wins"] == 5 and btc["losses"] == 3
    assert abs(btc["win_rate"] - 5 / 8) < 1e-9
    assert btc["realized_pnl"] == Decimal("2.50")
    assert btc["simulated_pnl"] == Decimal("3.10")


def test_win_rate_none_when_no_settled_trades():
    report = build_crypto_trading_report([_agg("ETH", "blocked_shrunk_edge", decisions=120)])
    assert report["assets"]["ETH"]["win_rate"] is None


def test_gate_breakdown_and_top_blockers_sorted():
    report = build_crypto_trading_report(_rows())
    btc = report["assets"]["BTC"]
    assert btc["gate_breakdown"]["blocked_max_entry_price"] == 300
    # blockers are the blocked_* statuses, largest first
    assert btc["top_blockers"][0] == ("blocked_max_entry_price", 300)
    assert ("eligible", 10) not in btc["top_blockers"]
    assert btc["trade_rate"] == 8 / 350


def test_champion_attached_per_asset():
    champions = {"BTC": {"model_type": "vol_normal_fair_value", "status": "trained"}}
    report = build_crypto_trading_report(_rows(), champions=champions)
    assert report["assets"]["BTC"]["champion_model_type"] == "vol_normal_fair_value"
    assert report["assets"]["ETH"]["champion_model_type"] is None


def test_net_pnl_subtracts_supplied_fees():
    report = build_crypto_trading_report(_rows(), fees={"BTC": Decimal("0.90")})
    btc = report["assets"]["BTC"]
    assert btc["estimated_fees"] == Decimal("0.90")
    assert btc["net_pnl"] == Decimal("2.50") - Decimal("0.90")
    # No fee supplied -> net falls back to gross with zero fees.
    assert report["assets"]["ETH"]["net_pnl"] == Decimal("0")


def test_portfolio_rollup():
    report = build_crypto_trading_report(_rows(), fees={"BTC": Decimal("0.90")})
    pf = report["portfolio"]
    assert pf["decisions"] == 470
    assert pf["filled"] == 8
    assert pf["realized_pnl"] == Decimal("2.50")
    assert pf["net_pnl"] == Decimal("1.60")
    assert pf["wins"] == 5 and pf["losses"] == 3
    assert abs(pf["win_rate"] - 5 / 8) < 1e-9
