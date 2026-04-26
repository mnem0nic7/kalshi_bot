from __future__ import annotations

from kalshi_bot.web.strategy_dashboard_formatting import (
    _bps_display,
    _compact_number,
    _coverage_display,
    _group_thresholds,
    _ratio_display,
    _sortino_display,
    _strategy_window_display,
)


def test_strategy_dashboard_scalar_displays() -> None:
    assert _strategy_window_display(180) == "180d"
    assert _ratio_display(0.625) == "62%"
    assert _ratio_display(None) == "—"
    assert _bps_display(42.4) == "42bps"
    assert _bps_display(None) == "—"
    assert _compact_number(12345) == "12,345"
    assert _coverage_display(12, 14) == "12/14 scored"
    assert _coverage_display(0, 0) == "—"
    assert _sortino_display(1.234) == "+1.23"
    assert _sortino_display(None) == "—"


def test_group_thresholds_labels_and_buckets_values() -> None:
    assert _group_thresholds(
        {
            "strategy_min_win_rate": 0.6,
            "risk_max_notional": 12,
            "trigger_enabled": True,
            "capital_bucket": "safe",
            "custom_flag": False,
        }
    ) == [
        {
            "label": "Risk",
            "items": [{"key": "risk_max_notional", "label": "Risk Max Notional", "value": "12"}],
        },
        {
            "label": "Trigger",
            "items": [{"key": "trigger_enabled", "label": "Trigger Enabled", "value": "true"}],
        },
        {
            "label": "Strategy",
            "items": [{"key": "strategy_min_win_rate", "label": "Strategy Min Win Rate", "value": "0.60"}],
        },
        {
            "label": "Capital",
            "items": [{"key": "capital_bucket", "label": "Capital Bucket", "value": "safe"}],
        },
        {
            "label": "Other",
            "items": [{"key": "custom_flag", "label": "Custom Flag", "value": "false"}],
        },
    ]
