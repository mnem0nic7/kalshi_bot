from __future__ import annotations

from typing import Any


def capital_bucket_from_signal_payload(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return "risky"
    explicit = str(payload.get("capital_bucket") or "").strip().lower()
    if explicit in {"safe", "risky"}:
        return explicit
    trade_regime = str(payload.get("trade_regime") or "").strip().lower()
    if trade_regime in {"near_threshold", "longshot_yes", "longshot_no"}:
        return "risky"
    if trade_regime == "standard":
        return "safe"
    return "risky"
