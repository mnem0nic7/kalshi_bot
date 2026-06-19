"""Operator directive (2026-06-19): keep training ALL models but trade SIGMA only.

`vol_normal_fair_value` (sigma) is one of the champion candidates, so "sigma only"
means the LIVE champion selection must be restricted to sigma — ML models
(xgboost/lightgbm/logistic/spot_distance/...) keep training + reporting for
research but can NEVER be selected as the deployable champion. Existing gates
(profit / brier-vs-mid / replay) still apply on top: sigma only goes live where it
passes, otherwise the asset stands down to the mid baseline.

The knob is `live_champion_allowlist` on `_crypto_select_champion`. Empty/None ==
no restriction (legacy behavior).
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
        # ML model: best simulated profit, well-calibrated -> would normally win.
        _entry("xgboost_classifier", 0.145, net=10.0, adv=10.0, sel=50),
        # Sigma: deployable but lower profit than the ML model.
        _entry("vol_normal_fair_value", 0.148, net=3.0, adv=3.0, sel=12),
    ]


def test_without_allowlist_best_profit_ml_model_wins():
    # Baseline behavior: highest-profit deployable candidate wins (an ML model).
    champ = _crypto_select_champion(_candidates(), min_selected_count=1)
    assert champ == "xgboost_classifier"


def test_allowlist_restricts_live_champion_to_sigma():
    # With the allowlist, the ML model is excluded even though it has higher P&L;
    # the gate-passing sigma candidate becomes the live champion.
    champ = _crypto_select_champion(
        _candidates(),
        min_selected_count=1,
        live_champion_allowlist={"vol_normal_fair_value"},
    )
    assert champ == "vol_normal_fair_value"


def test_allowlist_stands_down_to_baseline_when_sigma_not_available():
    # Sigma unavailable (untrained / failed this cycle); the profitable ML model is
    # excluded by the allowlist -> stand down to the mid baseline, NOT the ML model.
    # This is the key safety property: the allowlist must never let an ML champion
    # through just because sigma isn't deployable.
    candidates = [
        _entry("market_mid_baseline", 0.150),
        _entry("xgboost_classifier", 0.145, net=10.0, adv=10.0, sel=50),
        _entry("vol_normal_fair_value", 0.149, status="unavailable"),
    ]
    champ = _crypto_select_champion(
        candidates,
        min_selected_count=1,
        live_champion_allowlist={"vol_normal_fair_value"},
    )
    assert champ == "market_mid_baseline"
