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
