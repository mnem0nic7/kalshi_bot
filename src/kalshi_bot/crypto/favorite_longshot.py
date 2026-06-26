"""Deterministic favorite-longshot near-close decision rule for crypto binaries.

⛔ STATUS 2026-06-25: OOS-FAILED — DO NOT DEPLOY. Out-of-sample validation (fit win-prob
calibration on 8-15d-ago, apply to the unseen 1-8d window) netted **-$7.07 over 1593 trades
(-0.0044/contract)** after spread+fees — net negative. The favorite-longshot *bias* is real, but
after crossing the spread (paying the ask) it is NOT profitably tradeable as a blanket 15m rule
(mids are ~efficient). This module + `fit_favorite_calibration` + the OOS harness are kept as
reusable research infrastructure; any future use MUST pass a forward/OOS profitability gate first.
See memory `project_favorite_longshot_edge`.

Empirical basis (2026-06-25 bounded `crypto_market_snapshots` extract, 7d, 15m, all 7 assets):
the FAVORITE side (the higher-probability side, price > 0.5) is systematically *underpriced*
near close. Buying it at the ask, after the ~7% fee, nets +3..9c/contract at a 9-12min lead
(win rates 0.72-0.89); the cheap "longshot" side is symmetrically *overpriced*. The classic
favorite-longshot bias, independently confirmed 3-0 by the deep-research run.

The rule is intentionally simple and deterministic (no ML): identify the favorite from the
quote, look up its empirical win probability for that entry price, and trade at the ask iff the
fee-adjusted expected edge clears a threshold. The win-probability calibration is fit separately
(per-asset/lead) from settled snapshots; with no calibration the rule assumes zero edge and
stands down. This path is meant to run shadow first, then live, bypassing the ML-model edge gates
(the edge is a market-structure signal the model does not express) while still respecting the
kill switch / shadow mode at execution time.
"""

from __future__ import annotations

from dataclasses import dataclass

from kalshi_bot.forecast.calibration_metrics import pav_isotonic


@dataclass(frozen=True)
class FavoriteQuote:
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    seconds_to_close: float


@dataclass(frozen=True)
class FavoriteDecision:
    trade: bool
    side: str | None
    limit_price: float | None
    p_win: float | None
    expected_net: float | None
    reason: str


def favorite_win_probability(entry_price: float, calibration: dict | None) -> float:
    """Empirical P(favorite wins | entry price), monotone-interpolated from the calibration.

    ``calibration`` is ``{"entries": [...ascending...], "win_probs": [...]}`` fit from settled
    snapshots. With no calibration we assume no edge (win prob == price), so the rule stands down.
    """

    p = max(0.0, min(1.0, float(entry_price)))
    if not calibration:
        return p
    xs = [float(v) for v in calibration.get("entries") or []]
    ys = [float(v) for v in calibration.get("win_probs") or []]
    if not xs or len(xs) != len(ys):
        return p
    if p <= xs[0]:
        return max(0.0, min(1.0, ys[0]))
    if p >= xs[-1]:
        return max(0.0, min(1.0, ys[-1]))
    for i in range(1, len(xs)):
        if p > xs[i]:
            continue
        left_x, right_x, left_y, right_y = xs[i - 1], xs[i], ys[i - 1], ys[i]
        if right_x == left_x:
            return max(0.0, min(1.0, right_y))
        w = (p - left_x) / (right_x - left_x)
        return max(0.0, min(1.0, left_y + (right_y - left_y) * w))
    return p


def fit_favorite_calibration(
    entries: list[float],
    wins: list[int],
    *,
    grid: float = 0.01,
    min_samples: int = 50,
) -> dict | None:
    """Fit a monotone P(favorite wins | entry price) map from settled favorite trades.

    ``entries`` are favorite entry (ask) prices, ``wins`` are 0/1 (did the favorite settle in
    its favor). Prices are snapped to ``grid`` and isotonic-regressed (PAV) so win prob is
    non-decreasing in price. Returns the compact breakpoint map consumed by
    ``favorite_win_probability``, or ``None`` below ``min_samples`` (don't trade on thin data).
    """

    pairs = [
        (round(float(e) / grid) * grid, int(w))
        for e, w in zip(entries, wins, strict=False)
        if e is not None and w is not None
    ]
    if len(pairs) < int(min_samples):
        return None
    xs = [p for p, _ in pairs]
    ys = [float(w) for _, w in pairs]
    fitted = pav_isotonic(xs, ys)
    # PAV pools equal x to a common fitted value -> collapse to one point per grid price.
    collapsed: dict[float, float] = {}
    for x, f in zip(xs, fitted, strict=True):
        collapsed[round(x, 6)] = f
    items = sorted(collapsed.items())
    return {
        "entries": [x for x, _ in items],
        "win_probs": [f for _, f in items],
        "n": len(pairs),
    }


def evaluate_favorite_trade(
    quote: FavoriteQuote,
    *,
    calibration: dict | None = None,
    min_entry: float = 0.55,
    max_entry: float = 0.90,
    max_seconds_to_close: float = 720.0,
    max_spread: float = 0.05,
    fee_rate: float = 0.07,
    min_expected_net: float = 0.01,
) -> FavoriteDecision:
    """Decide whether to buy the favorite side at the ask, hold to settlement."""

    yes_mid = (float(quote.yes_bid) + float(quote.yes_ask)) / 2.0
    if yes_mid >= 0.5:
        side, entry = "yes", float(quote.yes_ask)
    else:
        side, entry = "no", float(quote.no_ask)
    spread = abs(float(quote.yes_ask) - float(quote.yes_bid))

    def _skip(reason: str) -> FavoriteDecision:
        return FavoriteDecision(False, side, entry, None, None, reason)

    if float(quote.seconds_to_close) > float(max_seconds_to_close):
        return _skip("too_early")
    if entry < float(min_entry) or entry > float(max_entry):
        return _skip("entry_out_of_band")
    if spread > float(max_spread):
        return _skip("spread_too_wide")

    p_win = favorite_win_probability(entry, calibration)
    fee = float(fee_rate) * entry * (1.0 - entry)
    expected_net = p_win - entry - fee
    if expected_net < float(min_expected_net):
        return FavoriteDecision(False, side, entry, p_win, expected_net, "edge_below_threshold")
    return FavoriteDecision(True, side, entry, p_win, expected_net, "eligible")
