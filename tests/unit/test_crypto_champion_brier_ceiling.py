"""Generalized lesson: do not deploy a champion whose calibration is materially
worse than the market mid, even when it is profitable in simulation.

Live evidence (2026-06-17, v11): XRP/DOGE/HYPE champions were sklearn_logistic
with brier 0.17-0.19 — far worse than mid (~0.15) — yet showed large simulated
P&L over 100-500 trades. That badly-calibrated-but-profitable pattern is the
artifact signature; genuine edge (BTC/SOL/BNB) keeps brier at/below mid. The
selection loop must reject a profit-"deployable" candidate whose brier regresses
beyond a ceiling vs mid, so every asset/cycle gets this skepticism automatically.
"""
from __future__ import annotations

from kalshi_bot.crypto.services import _crypto_select_champion


def _entry(name, brier, *, net=0.0, adv=0.0, sel=0, status="available"):
    e = {"name": name, "status": status, "metrics": {"brier": brier}}
    if sel or net or adv:
        e["policy_metrics"] = {
            "net_pnl": str(net),
            "pnl_advantage_vs_market_mid_dollars": str(adv),
            "selected_count": sel,
        }
    return e


def _candidates():
    return [
        _entry("market_mid_baseline", 0.150),
        # Well-calibrated, modest profit (the credible BTC/SOL/BNB pattern).
        _entry("lightgbm_classifier", 0.155, net=2.0, adv=2.0, sel=11),
        # Badly calibrated, big simulated profit (the XRP/DOGE/HYPE artifact).
        _entry("sklearn_logistic", 0.190, net=30.0, adv=30.0, sel=171),
    ]


def test_without_ceiling_badly_calibrated_high_pnl_wins():
    # Current behavior (no ceiling): highest P&L wins regardless of calibration.
    champ = _crypto_select_champion(_candidates(), min_selected_count=1)
    assert champ == "sklearn_logistic"


def test_ceiling_rejects_badly_calibrated_champion_keeps_credible_one():
    champ = _crypto_select_champion(
        _candidates(), min_selected_count=1, max_brier_regression_vs_mid=0.07
    )
    # sklearn (brier 0.190 > 0.150*1.07=0.1605) is rejected; the well-calibrated
    # profitable candidate wins instead of falling back to mid.
    assert champ == "lightgbm_classifier"


def test_ceiling_falls_back_to_mid_when_only_candidate_is_miscalibrated():
    candidates = [
        _entry("market_mid_baseline", 0.150),
        _entry("sklearn_logistic", 0.190, net=30.0, adv=30.0, sel=171),
    ]
    champ = _crypto_select_champion(
        candidates, min_selected_count=1, max_brier_regression_vs_mid=0.07
    )
    assert champ == "market_mid_baseline"


def test_ceiling_allows_candidate_better_than_mid():
    candidates = [
        _entry("market_mid_baseline", 0.150),
        _entry("sklearn_logistic", 0.142, net=18.0, adv=18.0, sel=100),
    ]
    champ = _crypto_select_champion(
        candidates, min_selected_count=1, max_brier_regression_vs_mid=0.07
    )
    assert champ == "sklearn_logistic"
