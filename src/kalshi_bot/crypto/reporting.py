"""Crypto trading-evaluation report builder.

Pure functions that turn ``crypto_decision_outcomes`` aggregates (grouped by
asset + gate_status) into a per-asset + portfolio evaluation: decisions, the
gate/block breakdown, fills, win rate, realized (gross) and simulated P&L, and
net-of-fees P&L when fee estimates are supplied. Kept free of DB/IO so it is
unit-testable; the repository supplies the aggregate rows and the CLI renders
the result.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def _d(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _win_rate(wins: int, losses: int) -> float | None:
    settled = wins + losses
    return (wins / settled) if settled > 0 else None


def build_crypto_trading_report(
    aggregate_rows: list[dict[str, Any]],
    *,
    champions: dict[str, dict[str, Any]] | None = None,
    fees: dict[str, Decimal] | None = None,
) -> dict[str, Any]:
    """Build the evaluation report from per-(asset, gate_status) aggregates.

    ``aggregate_rows`` items carry: asset_symbol, gate_status, decisions, filled,
    wins, losses, realized_pnl, simulated_pnl. ``champions`` maps asset ->
    {model_type, status, ...}; ``fees`` maps asset -> estimated fee dollars
    (since internal realized P&L is gross — fees are otherwise invisible).
    """
    champions = champions or {}
    fees = fees or {}
    assets: dict[str, dict[str, Any]] = {}

    for row in aggregate_rows:
        asset = str(row.get("asset_symbol") or "?")
        gate = str(row.get("gate_status") or "unknown")
        bucket = assets.setdefault(
            asset,
            {
                "decisions": 0,
                "filled": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "simulated_pnl": Decimal("0"),
                "gate_breakdown": {},
            },
        )
        decisions = int(row.get("decisions") or 0)
        bucket["decisions"] += decisions
        bucket["filled"] += int(row.get("filled") or 0)
        bucket["wins"] += int(row.get("wins") or 0)
        bucket["losses"] += int(row.get("losses") or 0)
        bucket["realized_pnl"] += _d(row.get("realized_pnl"))
        bucket["simulated_pnl"] += _d(row.get("simulated_pnl"))
        bucket["gate_breakdown"][gate] = bucket["gate_breakdown"].get(gate, 0) + decisions

    portfolio = {
        "decisions": 0,
        "filled": 0,
        "wins": 0,
        "losses": 0,
        "realized_pnl": Decimal("0"),
        "simulated_pnl": Decimal("0"),
        "estimated_fees": Decimal("0"),
        "net_pnl": Decimal("0"),
    }

    for asset, bucket in assets.items():
        bucket["win_rate"] = _win_rate(bucket["wins"], bucket["losses"])
        bucket["trade_rate"] = (bucket["filled"] / bucket["decisions"]) if bucket["decisions"] else 0.0
        bucket["top_blockers"] = sorted(
            ((gate, count) for gate, count in bucket["gate_breakdown"].items() if gate.startswith("blocked")),
            key=lambda item: item[1],
            reverse=True,
        )
        champion = champions.get(asset) or {}
        bucket["champion_model_type"] = champion.get("model_type")
        bucket["champion_status"] = champion.get("status")
        fee = _d(fees.get(asset))
        bucket["estimated_fees"] = fee
        bucket["net_pnl"] = bucket["realized_pnl"] - fee

        for key in ("decisions", "filled", "wins", "losses"):
            portfolio[key] += bucket[key]
        for key in ("realized_pnl", "simulated_pnl", "estimated_fees", "net_pnl"):
            portfolio[key] += bucket[key]

    portfolio["win_rate"] = _win_rate(portfolio["wins"], portfolio["losses"])
    portfolio["trade_rate"] = (portfolio["filled"] / portfolio["decisions"]) if portfolio["decisions"] else 0.0

    return {"assets": assets, "portfolio": portfolio}
