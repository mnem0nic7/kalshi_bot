from __future__ import annotations

from typing import Any


def _strategy_window_display(window_days: int) -> str:
    return f"{window_days}d"


def _ratio_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.0%}"


def _bps_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.0f}bps"


def _compact_number(value: int) -> str:
    return f"{value:,d}"


def _coverage_display(resolved_trade_count: int, trade_count: int) -> str:
    if trade_count <= 0:
        return "—"
    return f"{resolved_trade_count}/{trade_count} scored"


def _sortino_display(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:+.2f}"


def _threshold_value_display(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


def _threshold_label(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _group_thresholds(thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    groups: list[tuple[str, str, list[tuple[str, Any]]]] = [
        ("risk", "Risk", []),
        ("trigger", "Trigger", []),
        ("strategy", "Strategy", []),
        ("capital", "Capital", []),
        ("other", "Other", []),
    ]
    bucket_index = {key: idx for idx, (key, _, _) in enumerate(groups)}
    for threshold_key, threshold_value in thresholds.items():
        if threshold_key.startswith("risk_"):
            target = bucket_index["risk"]
        elif threshold_key.startswith("trigger_"):
            target = bucket_index["trigger"]
        elif threshold_key.startswith("strategy_"):
            target = bucket_index["strategy"]
        elif "capital" in threshold_key:
            target = bucket_index["capital"]
        else:
            target = bucket_index["other"]
        groups[target][2].append((threshold_key, threshold_value))

    grouped: list[dict[str, Any]] = []
    for _, label, items in groups:
        if not items:
            continue
        grouped.append(
            {
                "label": label,
                "items": [
                    {
                        "key": key,
                        "label": _threshold_label(key),
                        "value": _threshold_value_display(value),
                    }
                    for key, value in sorted(items, key=lambda pair: pair[0])
                ],
            }
        )
    return grouped
