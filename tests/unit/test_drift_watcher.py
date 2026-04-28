from __future__ import annotations

from kalshi_bot.learning.drift_watcher import DriftWindow, evaluate_calibration_drift


def test_drift_watcher_allows_clean_window() -> None:
    decision = evaluate_calibration_drift(
        DriftWindow(
            rolling_7d_brier=0.20,
            trailing_30d_brier=0.19,
            rolling_ece=0.04,
            predicted_win_rate=0.55,
            realized_win_rate=0.53,
            trade_count=120,
        )
    )

    assert decision.pause_new_entries is False
    assert decision.trigger_pack_search is False
    assert decision.reasons == []


def test_drift_watcher_pauses_and_triggers_search_on_drift() -> None:
    decision = evaluate_calibration_drift(
        DriftWindow(
            rolling_7d_brier=0.24,
            trailing_30d_brier=0.20,
            rolling_ece=0.09,
            predicted_win_rate=0.60,
            realized_win_rate=0.52,
            trade_count=150,
        )
    )

    assert decision.pause_new_entries is True
    assert decision.trigger_pack_search is True
    assert decision.reasons == [
        "brier_relative_drift",
        "ece_above_limit",
        "win_rate_divergence",
    ]
    assert decision.to_dict()["pause_new_entries"] is True


def test_drift_window_from_dict_uses_numeric_defaults() -> None:
    window = DriftWindow.from_dict({"rolling_7d_brier": "0.15", "trade_count": "7"})

    assert window.rolling_7d_brier == 0.15
    assert window.trailing_30d_brier == 0.0
    assert window.trade_count == 7
