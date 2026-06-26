"""Deterministic favorite-longshot near-close decision rule.

Empirical finding (2026-06-25 bounded snapshot extract, 7d/15m, all 7 assets): the FAVORITE
side (higher-probability side) is systematically underpriced near close — buying it at the ask
nets +3..9c/contract after the 7% fee at a 9-12min lead. Longshots (cheap side) are overpriced.
This rule buys the favorite at the ask when the fee-adjusted expected edge clears a threshold.
"""

from __future__ import annotations

import pytest

from kalshi_bot.crypto.favorite_longshot import (
    FavoriteQuote,
    evaluate_favorite_trade,
    favorite_win_probability,
    fit_favorite_calibration,
)


def _q(yes_bid, yes_ask, seconds_to_close=300.0, no_bid=None, no_ask=None) -> FavoriteQuote:
    # For a binary, no = 1 - yes. Default the no side from the yes side.
    if no_ask is None:
        no_ask = round(1.0 - yes_bid, 4)
    if no_bid is None:
        no_bid = round(1.0 - yes_ask, 4)
    return FavoriteQuote(
        yes_bid=yes_bid, yes_ask=yes_ask, no_bid=no_bid, no_ask=no_ask,
        seconds_to_close=seconds_to_close,
    )


# A calibration where the favorite wins MORE than its price (the measured edge):
# entry 0.70 -> wins 0.78, entry 0.86 -> wins 0.99.
_CAL = {"entries": [0.55, 0.70, 0.86, 0.95], "win_probs": [0.62, 0.78, 0.99, 0.999]}


def test_win_probability_interpolates_and_clamps():
    assert favorite_win_probability(0.70, _CAL) == pytest.approx(0.78, abs=1e-9)
    # midpoint interpolation between 0.70->0.78 and 0.86->0.99
    mid = favorite_win_probability(0.78, _CAL)
    assert 0.78 < mid < 0.99
    # below/above range clamps to endpoints
    assert favorite_win_probability(0.10, _CAL) == pytest.approx(0.62, abs=1e-9)
    assert favorite_win_probability(0.999, _CAL) == pytest.approx(0.999, abs=1e-9)


def test_win_probability_none_calibration_falls_back_to_price():
    # No calibration => assume no edge (win prob == price).
    assert favorite_win_probability(0.73, None) == pytest.approx(0.73, abs=1e-9)


def test_identifies_yes_favorite_and_buys_at_yes_ask():
    q = _q(yes_bid=0.84, yes_ask=0.86)  # yes_mid 0.85 -> favorite YES
    d = evaluate_favorite_trade(q, calibration=_CAL)
    assert d.trade is True
    assert d.side == "yes"
    assert d.limit_price == pytest.approx(0.86, abs=1e-9)
    assert d.expected_net > 0


def test_identifies_no_favorite_and_buys_at_no_ask():
    q = _q(yes_bid=0.12, yes_ask=0.14)  # yes_mid 0.13 -> favorite NO; no_ask = 1-0.12 = 0.88
    d = evaluate_favorite_trade(q, calibration=_CAL)
    assert d.side == "no"
    assert d.limit_price == pytest.approx(0.88, abs=1e-9)


def test_rejects_when_too_early():
    q = _q(yes_bid=0.84, yes_ask=0.86, seconds_to_close=5000.0)
    d = evaluate_favorite_trade(q, calibration=_CAL, max_seconds_to_close=720)
    assert d.trade is False
    assert d.reason == "too_early"


def test_rejects_entry_out_of_band():
    # favorite ask 0.97 is above max_entry 0.92 -> skip the thin-edge zone
    q = _q(yes_bid=0.96, yes_ask=0.97)
    d = evaluate_favorite_trade(q, calibration=_CAL, max_entry=0.92)
    assert d.trade is False
    assert d.reason == "entry_out_of_band"


def test_rejects_wide_spread():
    q = _q(yes_bid=0.60, yes_ask=0.86)  # spread 0.26
    d = evaluate_favorite_trade(q, calibration=_CAL, max_spread=0.05)
    assert d.trade is False
    assert d.reason == "spread_too_wide"


def test_no_trade_when_edge_below_threshold():
    # No calibration => p_win == entry => net = -fee < threshold => skip.
    q = _q(yes_bid=0.84, yes_ask=0.86)
    d = evaluate_favorite_trade(q, calibration=None, min_expected_net=0.01)
    assert d.trade is False
    assert d.reason == "edge_below_threshold"
    assert d.expected_net < 0


def test_expected_net_accounts_for_fee():
    q = _q(yes_bid=0.84, yes_ask=0.86)
    d = evaluate_favorite_trade(q, calibration=_CAL, fee_rate=0.07)
    p_win = 0.99  # from _CAL at 0.86
    fee = 0.07 * 0.86 * (1 - 0.86)
    assert d.expected_net == pytest.approx(p_win - 0.86 - fee, abs=1e-6)


# ---- calibration fitter (isotonic win-prob vs entry price) ----


def test_fit_returns_monotone_calibration():
    # Build a dataset where higher entry price wins more often (the measured edge).
    entries, wins = [], []
    for price, rate in [(0.60, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 0.99)]:
        for i in range(100):
            entries.append(price)
            wins.append(1 if i < int(rate * 100) else 0)
    cal = fit_favorite_calibration(entries, wins, min_samples=50)
    assert cal is not None
    wp = cal["win_probs"]
    assert all(wp[i] <= wp[i + 1] + 1e-9 for i in range(len(wp) - 1))  # monotone
    # recovered win prob near the empirical group rate
    assert favorite_win_probability(0.80, cal) == pytest.approx(0.90, abs=0.05)
    assert cal["n"] == 400


def test_fit_returns_none_below_min_samples():
    assert fit_favorite_calibration([0.7, 0.8], [1, 1], min_samples=50) is None


def test_fit_empty_is_none():
    assert fit_favorite_calibration([], [], min_samples=1) is None
