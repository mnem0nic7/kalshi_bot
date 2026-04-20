from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)

WINDOW_DAYS = 180
MIN_TRADE_COUNT = 20
MIN_WIN_RATE_GAP = 0.05

_SKIP_STAND_DOWN_REASONS = {"longshot_bet", "resolved_contract", "book_effectively_broken"}

STRATEGY_PRESETS: list[dict[str, Any]] = [
    {
        "name": "aggressive",
        "description": "Loose filters, higher trade frequency, wider spread tolerance.",
        "thresholds": {
            "risk_min_edge_bps": 20,
            "risk_max_order_notional_dollars": 15.0,
            "risk_max_position_notional_dollars": 40.0,
            "trigger_max_spread_bps": 800,
            "trigger_cooldown_seconds": 180,
            "strategy_quality_edge_buffer_bps": 0,
            "strategy_min_remaining_payout_bps": 300,
            "risk_safe_capital_reserve_ratio": 0.60,
            "risk_risky_capital_max_ratio": 0.40,
        },
    },
    {
        "name": "moderate",
        "description": "Balanced filters matching current live settings.",
        "thresholds": {
            "risk_min_edge_bps": 50,
            "risk_max_order_notional_dollars": 10.0,
            "risk_max_position_notional_dollars": 25.0,
            "trigger_max_spread_bps": 500,
            "trigger_cooldown_seconds": 300,
            "strategy_quality_edge_buffer_bps": 20,
            "strategy_min_remaining_payout_bps": 500,
            "risk_safe_capital_reserve_ratio": 0.70,
            "risk_risky_capital_max_ratio": 0.30,
        },
    },
    {
        "name": "conservative",
        "description": "Tight filters, high-confidence setups only.",
        "thresholds": {
            "risk_min_edge_bps": 90,
            "risk_max_order_notional_dollars": 6.0,
            "risk_max_position_notional_dollars": 15.0,
            "trigger_max_spread_bps": 300,
            "trigger_cooldown_seconds": 600,
            "strategy_quality_edge_buffer_bps": 50,
            "strategy_min_remaining_payout_bps": 800,
            "risk_safe_capital_reserve_ratio": 0.80,
            "risk_risky_capital_max_ratio": 0.20,
        },
    },
]


def _thresholds_from_dict(d: dict[str, Any]) -> RuntimeThresholds:
    return RuntimeThresholds(
        risk_min_edge_bps=int(d["risk_min_edge_bps"]),
        risk_max_order_notional_dollars=float(d["risk_max_order_notional_dollars"]),
        risk_max_position_notional_dollars=float(d["risk_max_position_notional_dollars"]),
        trigger_max_spread_bps=int(d["trigger_max_spread_bps"]),
        trigger_cooldown_seconds=int(d["trigger_cooldown_seconds"]),
        strategy_quality_edge_buffer_bps=int(d["strategy_quality_edge_buffer_bps"]),
        strategy_min_remaining_payout_bps=int(d["strategy_min_remaining_payout_bps"]),
        risk_safe_capital_reserve_ratio=float(d["risk_safe_capital_reserve_ratio"]),
        risk_risky_capital_max_ratio=float(d["risk_risky_capital_max_ratio"]),
    )


def _would_have_traded(room: dict[str, Any], t: RuntimeThresholds) -> bool:
    edge_bps: int = room["edge_bps"]
    payload: dict[str, Any] = room.get("signal_payload") or {}
    eligibility: dict[str, Any] = payload.get("eligibility") or {}

    spread_bps: int | None = eligibility.get("market_spread_bps")
    remaining_payout_raw = eligibility.get("remaining_payout_dollars")
    remaining_payout_bps: int | None = None
    if remaining_payout_raw is not None:
        try:
            remaining_payout_bps = round(float(remaining_payout_raw) * 10000)
        except (TypeError, ValueError):
            remaining_payout_bps = None

    if edge_bps < t.risk_min_edge_bps:
        return False
    if spread_bps is not None and spread_bps > t.trigger_max_spread_bps:
        return False
    net_edge = edge_bps - (spread_bps or 0)
    if net_edge < t.strategy_quality_edge_buffer_bps:
        return False
    if remaining_payout_bps is not None and remaining_payout_bps < t.strategy_min_remaining_payout_bps:
        return False
    return True


def _stand_down_reason(room: dict[str, Any]) -> str | None:
    payload: dict[str, Any] = room.get("signal_payload") or {}
    return payload.get("stand_down_reason")


class StrategyRegressionService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service

    async def seed_strategies(self, repo: PlatformRepository) -> None:
        await repo.seed_strategies(STRATEGY_PRESETS)

    async def run_regression(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        date_from = now - timedelta(days=WINDOW_DAYS)
        date_to = now

        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            strategies = await repo.list_strategies(active_only=True)
            if not strategies:
                return {"status": "no_strategies"}

            rooms = await repo.get_strategy_regression_rooms(date_from, date_to)
            if not rooms:
                return {"status": "no_rooms", "window_days": WINDOW_DAYS}

            # Group rooms by series_ticker via weather directory
            rooms_by_series: dict[str, list[dict[str, Any]]] = {}
            for room in rooms:
                stand_down = _stand_down_reason(room)
                if stand_down in _SKIP_STAND_DOWN_REASONS:
                    continue
                mapping = self.weather_directory.resolve_market(room["market_ticker"])
                if mapping is None or not mapping.series_ticker:
                    continue
                series_ticker = mapping.series_ticker
                rooms_by_series.setdefault(series_ticker, []).append(room)

            result_rows: list[dict[str, Any]] = []
            city_results: dict[str, dict[str, dict[str, Any]]] = {}

            for strategy in strategies:
                t = _thresholds_from_dict(strategy.thresholds)
                for series_ticker, city_rooms in rooms_by_series.items():
                    stats = self._evaluate_city(strategy.id, series_ticker, city_rooms, t, now, date_from, date_to)
                    result_rows.append(stats)
                    city_results.setdefault(series_ticker, {})[strategy.name] = {
                        "win_rate": stats["win_rate"],
                        "trade_count": stats["trade_count"],
                        "strategy_id": strategy.id,
                        "strategy_name": strategy.name,
                    }

            await repo.save_strategy_results(result_rows)

            promotions = await self._maybe_promote(repo, now, city_results, strategies)

            await repo.set_checkpoint(
                "strategy_regression",
                None,
                {
                    "ran_at": now.isoformat(),
                    "rooms_scanned": len(rooms),
                    "series_evaluated": len(rooms_by_series),
                    "strategies_evaluated": len(strategies),
                    "promotions": promotions,
                    "window_days": WINDOW_DAYS,
                },
            )
            await session.commit()

        return {
            "status": "ok",
            "rooms_scanned": len(rooms),
            "series_evaluated": len(rooms_by_series),
            "result_rows": len(result_rows),
            "promotions": promotions,
        }

    def _evaluate_city(
        self,
        strategy_id: int,
        series_ticker: str,
        rooms: list[dict[str, Any]],
        t: RuntimeThresholds,
        run_at: datetime,
        date_from: datetime,
        date_to: datetime,
    ) -> dict[str, Any]:
        trade_count = 0
        win_count = 0
        total_pnl = 0.0
        edge_sum = 0.0

        for room in rooms:
            if not _would_have_traded(room, t):
                continue
            trade_count += 1
            edge_sum += room["edge_bps"]

            payload = room.get("signal_payload") or {}
            eligibility = payload.get("eligibility") or {}
            # Use settlement_result from eligibility outcome if available
            # Otherwise treat as unknown (skip from win/loss tally)
            outcome = eligibility.get("settlement_result")
            counterfactual_pnl = payload.get("counterfactual_pnl_dollars")
            if outcome == "win":
                win_count += 1
            if counterfactual_pnl is not None:
                try:
                    total_pnl += float(counterfactual_pnl)
                except (TypeError, ValueError):
                    pass

        rooms_evaluated = len(rooms)
        trade_rate = trade_count / rooms_evaluated if rooms_evaluated > 0 else None
        win_rate = win_count / trade_count if trade_count > 0 else None
        avg_edge_bps = edge_sum / trade_count if trade_count > 0 else None

        return {
            "strategy_id": strategy_id,
            "run_at": run_at,
            "date_from": date_from.date().isoformat(),
            "date_to": date_to.date().isoformat(),
            "series_ticker": series_ticker,
            "rooms_evaluated": rooms_evaluated,
            "trade_count": trade_count,
            "win_count": win_count,
            "total_pnl_dollars": round(total_pnl, 4),
            "trade_rate": round(trade_rate, 4) if trade_rate is not None else None,
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
            "avg_edge_bps": round(avg_edge_bps, 2) if avg_edge_bps is not None else None,
        }

    async def _maybe_promote(
        self,
        repo: PlatformRepository,
        run_at: datetime,
        city_results: dict[str, dict[str, dict[str, Any]]],
        strategies: list[Any],
    ) -> list[dict[str, Any]]:
        strategy_names = {s.name for s in strategies}
        promotions: list[dict[str, Any]] = []

        for series_ticker, results_by_strategy in city_results.items():
            # All active strategies must have data for this city
            if not strategy_names.issubset(results_by_strategy.keys()):
                continue

            # Find best strategy (highest win rate with sufficient trade count)
            best: dict[str, Any] | None = None
            for strat_data in results_by_strategy.values():
                win_rate = strat_data.get("win_rate")
                trade_count = strat_data.get("trade_count", 0)
                if win_rate is None or trade_count < MIN_TRADE_COUNT:
                    continue
                if best is None or win_rate > best["win_rate"]:
                    best = strat_data

            if best is None:
                continue

            current = await repo.get_city_strategy_assignment(series_ticker)
            current_name = current.strategy_name if current is not None else None
            current_win_rate = results_by_strategy.get(current_name, {}).get("win_rate") if current_name else None

            should_promote = (
                current_name is None
                or current_win_rate is None
                or (best["win_rate"] - current_win_rate) >= MIN_WIN_RATE_GAP
            )
            if not should_promote or best["strategy_name"] == current_name:
                continue

            await repo.set_city_strategy_assignment(series_ticker, best["strategy_name"])
            await repo.log_ops_event(
                severity="info",
                summary=(
                    f"Strategy auto-promoted for {series_ticker}: "
                    f"{current_name or 'none'} → {best['strategy_name']} "
                    f"(win_rate={best['win_rate']:.1%}, trades={best['trade_count']})"
                ),
                source="strategy_regression",
                payload={
                    "series_ticker": series_ticker,
                    "previous_strategy": current_name,
                    "new_strategy": best["strategy_name"],
                    "new_win_rate": best["win_rate"],
                    "previous_win_rate": current_win_rate,
                    "trade_count": best["trade_count"],
                    "run_at": run_at.isoformat(),
                },
            )
            promotions.append({
                "series_ticker": series_ticker,
                "previous": current_name,
                "promoted_to": best["strategy_name"],
                "win_rate": best["win_rate"],
                "trade_count": best["trade_count"],
            })

        return promotions
