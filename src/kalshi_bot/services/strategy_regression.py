from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import logging
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from kalshi_bot.config import Settings
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.services.agent_packs import AgentPackService, RuntimeThresholds
from kalshi_bot.services.counterfactuals import CounterfactualTradeOutcome, score_counterfactual_trade
from kalshi_bot.weather.mapping import WeatherMarketDirectory

logger = logging.getLogger(__name__)

WINDOW_DAYS = 180
MIN_TRADE_COUNT = 20
MIN_WIN_RATE_GAP = 0.05
RECOMMENDATION_MODE = "recommendation_only"
RECOMMENDATION_MIN_OUTCOME_COVERAGE_RATE = 0.95
STRONG_RECOMMENDATION_MIN_GAP = 0.02
LEAN_RECOMMENDATION_MIN_GAP = 0.01

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


@dataclass(slots=True)
class RegressionStrategySpec:
    id: int | None
    name: str
    description: str | None
    thresholds: dict[str, Any]


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


def _resolve_strategy_room_market(weather_directory: WeatherMarketDirectory, market_ticker: str) -> Any | None:
    resolve_market_stub = getattr(weather_directory, "resolve_market_stub", None)
    if callable(resolve_market_stub):
        mapping = resolve_market_stub(market_ticker)
        if mapping is not None:
            return mapping
    resolve_market = getattr(weather_directory, "resolve_market", None)
    if callable(resolve_market):
        try:
            return resolve_market(market_ticker)
        except TypeError:
            return None
    return None


def _group_strategy_rooms_by_series(
    rooms: list[dict[str, Any]],
    weather_directory: WeatherMarketDirectory,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    rooms_by_series: dict[str, list[dict[str, Any]]] = {}
    rooms_skipped_stand_down = 0
    rooms_skipped_unmapped = 0
    included_rooms = 0
    series_prefixes_seen: set[str] = set()

    for room in rooms:
        stand_down = _stand_down_reason(room)
        if stand_down in _SKIP_STAND_DOWN_REASONS:
            rooms_skipped_stand_down += 1
            continue
        series_ticker = str(room.get("series_ticker") or "").strip()
        if not series_ticker:
            mapping = _resolve_strategy_room_market(weather_directory, room["market_ticker"])
            if mapping is None or not getattr(mapping, "series_ticker", None):
                rooms_skipped_unmapped += 1
                continue
            series_ticker = str(mapping.series_ticker)
        series_prefixes_seen.add(series_ticker)
        rooms_by_series.setdefault(series_ticker, []).append(room)
        included_rooms += 1

    diagnostics = {
        "rooms_scanned": len(rooms),
        "rooms_included": included_rooms,
        "rooms_skipped_stand_down": rooms_skipped_stand_down,
        "rooms_skipped_unmapped": rooms_skipped_unmapped,
        "series_evaluated": len(rooms_by_series),
        "series_prefixes_seen": sorted(series_prefixes_seen),
    }
    return rooms_by_series, diagnostics


def _strategy_room_trade_ticket(room: dict[str, Any]) -> dict[str, Any] | None:
    side = room.get("ticket_side")
    yes_price = room.get("ticket_yes_price_dollars")
    count_fp = room.get("ticket_count_fp")
    if side in (None, "") or yes_price in (None, "") or count_fp in (None, ""):
        return None
    return {
        "side": side,
        "yes_price_dollars": yes_price,
        "count_fp": count_fp,
    }


def _strategy_room_settlement(room: dict[str, Any]) -> dict[str, Any] | None:
    settlement_value = room.get("settlement_value_dollars")
    kalshi_result = room.get("kalshi_result")
    if settlement_value in (None, "") and kalshi_result in (None, ""):
        return None
    return {
        "settlement_value_dollars": settlement_value,
        "kalshi_result": kalshi_result,
    }


def _score_strategy_room_outcome(room: dict[str, Any]) -> CounterfactualTradeOutcome | None:
    return score_counterfactual_trade(
        trade_ticket=_strategy_room_trade_ticket(room),
        settlement=_strategy_room_settlement(room),
    )


def _strategy_result_rank_key(row: dict[str, Any]) -> tuple[float, int, float]:
    total_pnl = row.get("total_pnl_dollars")
    total_pnl_value = float(total_pnl) if total_pnl is not None else float("-inf")
    return (
        row.get("win_rate") if row.get("win_rate") is not None else -1.0,
        int(row.get("resolved_trade_count") or 0),
        total_pnl_value,
    )


def _rank_scored_strategy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [
            row
            for row in rows
            if row.get("win_rate") is not None and int(row.get("resolved_trade_count") or 0) > 0
        ],
        key=_strategy_result_rank_key,
        reverse=True,
    )


def _wilson_confidence_interval(
    win_count: int,
    sample_count: int,
    *,
    z_score: float = 1.96,
) -> tuple[float | None, float | None]:
    if sample_count <= 0:
        return None, None
    proportion = win_count / sample_count
    z_squared = z_score**2
    denominator = 1 + (z_squared / sample_count)
    center = proportion + (z_squared / (2 * sample_count))
    margin = z_score * math.sqrt(
        ((proportion * (1 - proportion)) + (z_squared / (4 * sample_count))) / sample_count
    )
    lower = max(0.0, (center - margin) / denominator)
    upper = min(1.0, (center + margin) / denominator)
    return lower, upper


def _recommendation_status_label(status: str) -> str:
    labels = {
        "strong_recommendation": "Strong recommendation",
        "lean_recommendation": "Lean recommendation",
        "too_close": "Too close",
        "low_sample": "Low sample",
        "no_outcomes": "No outcomes",
    }
    return labels.get(status, "No outcomes")


def _recommendation_sort_priority(status: str) -> int:
    priorities = {
        "strong_recommendation": 0,
        "lean_recommendation": 1,
        "too_close": 2,
        "low_sample": 3,
        "no_outcomes": 4,
    }
    return priorities.get(status, 5)


def _recommendation_decision(
    *,
    results_by_strategy: dict[str, dict[str, Any]],
    current_name: str | None,
) -> dict[str, Any]:
    ranked_rows = _rank_scored_strategy_rows(list(results_by_strategy.values()))
    best_row = ranked_rows[0] if ranked_rows else None
    runner_up_row = ranked_rows[1] if len(ranked_rows) > 1 else None
    current_row = results_by_strategy.get(current_name) if current_name is not None else None
    gap_to_runner_up = None
    if best_row is not None and runner_up_row is not None:
        gap_to_runner_up = best_row["win_rate"] - runner_up_row["win_rate"]
    gap_to_current = None
    if (
        best_row is not None
        and current_row is not None
        and current_row.get("win_rate") is not None
        and best_row["strategy_name"] != current_name
    ):
        gap_to_current = best_row["win_rate"] - current_row["win_rate"]
    best_trade_count = int(best_row.get("trade_count") or 0) if best_row is not None else 0
    best_resolved_trade_count = int(best_row.get("resolved_trade_count") or 0) if best_row is not None else 0
    best_outcome_coverage_rate = (
        best_resolved_trade_count / best_trade_count if best_trade_count > 0 else None
    )
    best_total_pnl = float(best_row["total_pnl_dollars"]) if best_row is not None and best_row.get("total_pnl_dollars") is not None else None
    clears_trade_threshold = bool(best_row is not None and best_resolved_trade_count >= MIN_TRADE_COUNT)
    clears_coverage_threshold = bool(
        best_outcome_coverage_rate is not None
        and best_outcome_coverage_rate >= RECOMMENDATION_MIN_OUTCOME_COVERAGE_RATE
    )
    clears_strong_gap = bool(
        gap_to_runner_up is not None and gap_to_runner_up >= STRONG_RECOMMENDATION_MIN_GAP
    )
    clears_lean_gap = bool(
        gap_to_runner_up is not None and gap_to_runner_up >= LEAN_RECOMMENDATION_MIN_GAP
    )
    has_non_negative_pnl = bool(best_total_pnl is not None and best_total_pnl >= 0.0)

    if best_row is None or best_resolved_trade_count == 0:
        recommendation_status = "no_outcomes"
    elif not clears_trade_threshold or not clears_coverage_threshold or runner_up_row is None:
        recommendation_status = "low_sample"
    elif clears_strong_gap and has_non_negative_pnl:
        recommendation_status = "strong_recommendation"
    elif clears_lean_gap:
        recommendation_status = "lean_recommendation"
    else:
        recommendation_status = "too_close"

    winner_wilson_lower, winner_wilson_upper = _wilson_confidence_interval(
        int(best_row.get("win_count") or 0) if best_row is not None else 0,
        best_resolved_trade_count,
    )
    runner_up_wilson_lower, runner_up_wilson_upper = _wilson_confidence_interval(
        int(runner_up_row.get("win_count") or 0) if runner_up_row is not None else 0,
        int(runner_up_row.get("resolved_trade_count") or 0) if runner_up_row is not None else 0,
    )

    return {
        "ranked_rows": ranked_rows,
        "best_row": best_row,
        "runner_up_row": runner_up_row,
        "current_row": current_row,
        "gap_to_runner_up": gap_to_runner_up,
        "gap_to_current_assignment": gap_to_current,
        "clears_trade_threshold": clears_trade_threshold,
        "clears_coverage_threshold": clears_coverage_threshold,
        "clears_strong_gap": clears_strong_gap,
        "clears_lean_gap": clears_lean_gap,
        "best_outcome_coverage_rate": best_outcome_coverage_rate,
        "best_total_pnl_dollars": best_total_pnl,
        "has_non_negative_pnl": has_non_negative_pnl,
        "winner_wilson_lower": winner_wilson_lower,
        "winner_wilson_upper": winner_wilson_upper,
        "runner_up_wilson_lower": runner_up_wilson_lower,
        "runner_up_wilson_upper": runner_up_wilson_upper,
        "recommendation": {
            "strategy_name": best_row["strategy_name"] if best_row is not None else None,
            "status": recommendation_status,
            "label": _recommendation_status_label(recommendation_status),
            "resolved_trade_count": best_resolved_trade_count,
            "outcome_coverage_rate": best_outcome_coverage_rate,
            "gap_to_runner_up": gap_to_runner_up,
            "writes_assignment": False,
        },
    }


def _aggregate_strategy_leaderboard(
    *,
    strategies: list[RegressionStrategySpec],
    result_rows: list[dict[str, Any]],
    city_results: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows_by_strategy: dict[str, list[dict[str, Any]]] = {strategy.name: [] for strategy in strategies}
    for row in result_rows:
        rows_by_strategy.setdefault(str(row["strategy_name"]), []).append(row)

    strategy_leaders: Counter[str] = Counter()
    for results_by_strategy in city_results.values():
        ranked_rows = _rank_scored_strategy_rows(list(results_by_strategy.values()))
        if ranked_rows:
            strategy_leaders[ranked_rows[0]["strategy_name"]] += 1

    leaderboard: list[dict[str, Any]] = []
    for strategy in strategies:
        strategy_rows = rows_by_strategy.get(strategy.name, [])
        total_rooms = sum(int(row["rooms_evaluated"]) for row in strategy_rows)
        total_trades = sum(int(row["trade_count"]) for row in strategy_rows)
        total_resolved_trades = sum(int(row["resolved_trade_count"]) for row in strategy_rows)
        total_unscored_trades = sum(int(row["unscored_trade_count"]) for row in strategy_rows)
        total_wins = sum(int(row["win_count"]) for row in strategy_rows)
        total_pnl = sum((row["total_pnl_dollars"] or Decimal("0")) for row in strategy_rows)
        edge_numerator = sum((row["avg_edge_bps"] or 0.0) * int(row["trade_count"]) for row in strategy_rows)
        overall_win_rate = (total_wins / total_resolved_trades) if total_resolved_trades > 0 else None
        overall_trade_rate = (total_trades / total_rooms) if total_rooms > 0 else None
        outcome_coverage_rate = (total_resolved_trades / total_trades) if total_trades > 0 else None
        overall_avg_edge = (edge_numerator / total_trades) if total_trades > 0 else None
        leaderboard.append({
            "id": strategy.id,
            "name": strategy.name,
            "description": strategy.description,
            "thresholds": strategy.thresholds,
            "overall_win_rate": round(overall_win_rate, 4) if overall_win_rate is not None else None,
            "overall_trade_rate": round(overall_trade_rate, 4) if overall_trade_rate is not None else None,
            "total_pnl_dollars": float(total_pnl) if total_resolved_trades > 0 else None,
            "avg_edge_bps": round(overall_avg_edge, 2) if overall_avg_edge is not None else None,
            "total_rooms_evaluated": total_rooms,
            "total_trade_count": total_trades,
            "total_resolved_trade_count": total_resolved_trades,
            "total_unscored_trade_count": total_unscored_trades,
            "outcome_coverage_rate": round(outcome_coverage_rate, 4) if outcome_coverage_rate is not None else None,
            "cities_led": strategy_leaders.get(strategy.name, 0),
        })

    leaderboard.sort(
        key=lambda row: (
            row["overall_win_rate"] if row["overall_win_rate"] is not None else -1.0,
            row["total_resolved_trade_count"],
            row["total_pnl_dollars"] if row["total_pnl_dollars"] is not None else float("-inf"),
        ),
        reverse=True,
    )
    return leaderboard


class StrategyRegressionService:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        weather_directory: WeatherMarketDirectory,
        agent_pack_service: AgentPackService,
        *,
        read_session_factory: async_sessionmaker | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        # Read factory: historical rooms, settlements, strategies come from here.
        # Write factory (session_factory above): regression snapshots + checkpoints.
        self.read_session_factory = read_session_factory or session_factory
        self.weather_directory = weather_directory
        self.agent_pack_service = agent_pack_service

    async def seed_strategies(self, repo: PlatformRepository) -> None:
        await repo.seed_strategies(STRATEGY_PRESETS)

    async def evaluate_strategy_specs(
        self,
        *,
        strategies: list[RegressionStrategySpec],
        window_days: int,
        run_at: datetime | None = None,
    ) -> dict[str, Any]:
        run_at = run_at or datetime.now(UTC)
        date_from = run_at - timedelta(days=window_days)
        date_to = run_at
        async with self.read_session_factory() as session:
            repo = PlatformRepository(session)
            rooms = await repo.get_strategy_regression_rooms(date_from, date_to)
        if not rooms:
            return {"status": "no_rooms", "window_days": window_days}
        return self.evaluate_strategy_specs_from_rooms(
            strategies=strategies,
            rooms=rooms,
            run_at=run_at,
            date_from=date_from,
            date_to=date_to,
            window_days=window_days,
        )

    def evaluate_strategy_specs_from_rooms(
        self,
        *,
        strategies: list[RegressionStrategySpec],
        rooms: list[dict[str, Any]],
        run_at: datetime,
        date_from: datetime,
        date_to: datetime,
        window_days: int,
    ) -> dict[str, Any]:
        rooms_by_series, diagnostics = _group_strategy_rooms_by_series(rooms, self.weather_directory)
        result_rows: list[dict[str, Any]] = []
        city_results: dict[str, dict[str, dict[str, Any]]] = {}

        for strategy in strategies:
            thresholds = _thresholds_from_dict(strategy.thresholds)
            for series_ticker, city_rooms in rooms_by_series.items():
                stats = self._evaluate_city(
                    strategy_id=strategy.id,
                    strategy_name=strategy.name,
                    series_ticker=series_ticker,
                    rooms=city_rooms,
                    t=thresholds,
                    run_at=run_at,
                    date_from=date_from,
                    date_to=date_to,
                )
                result_rows.append(stats)
                city_results.setdefault(series_ticker, {})[strategy.name] = {
                    "strategy_id": strategy.id,
                    "strategy_name": strategy.name,
                    "trade_count": stats["trade_count"],
                    "resolved_trade_count": stats["resolved_trade_count"],
                    "unscored_trade_count": stats["unscored_trade_count"],
                    "win_count": stats["win_count"],
                    "win_rate": stats["win_rate"],
                    "total_pnl_dollars": stats["total_pnl_dollars"],
                    "avg_edge_bps": stats["avg_edge_bps"],
                }

        leaderboard = _aggregate_strategy_leaderboard(
            strategies=strategies,
            result_rows=result_rows,
            city_results=city_results,
        )
        return {
            "status": "ok",
            "window_days": window_days,
            "run_at": run_at.isoformat(),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "diagnostics": diagnostics,
            "result_rows": result_rows,
            "city_results": city_results,
            "leaderboard": leaderboard,
        }

    async def run_regression(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        date_from = now - timedelta(days=WINDOW_DAYS)
        date_to = now

        # Read phase — strategies + rooms come from the (possibly remote) read source.
        async with self.read_session_factory() as read_session:
            read_repo = PlatformRepository(read_session)
            strategy_rows = await read_repo.list_strategies(active_only=True)
            strategies = [
                RegressionStrategySpec(
                    id=strategy.id,
                    name=strategy.name,
                    description=strategy.description,
                    thresholds=strategy.thresholds,
                )
                for strategy in strategy_rows
            ]
            if not strategies:
                return {"status": "no_strategies"}

            rooms = await read_repo.get_strategy_regression_rooms(date_from, date_to)
            if not rooms:
                return {"status": "no_rooms", "window_days": WINDOW_DAYS}

        evaluation = self.evaluate_strategy_specs_from_rooms(
            strategies=strategies,
            rooms=rooms,
            run_at=now,
            date_from=date_from,
            date_to=date_to,
            window_days=WINDOW_DAYS,
        )
        diagnostics = evaluation["diagnostics"]
        result_rows = evaluation["result_rows"]
        city_results = evaluation["city_results"]

        # Write phase — regression snapshots and checkpoints stay on local primary.
        async with self.session_factory() as session:
            repo = PlatformRepository(session)
            await repo.save_strategy_results(result_rows)

            recommendation_counts: Counter[str] = Counter()
            top_candidates: list[dict[str, Any]] = []
            for series_ticker, results_by_strategy in city_results.items():
                decision = _recommendation_decision(results_by_strategy=results_by_strategy, current_name=None)
                recommendation = decision["recommendation"]
                recommendation_counts[recommendation["status"]] += 1
                if recommendation["strategy_name"] is None:
                    continue
                top_candidates.append({
                    "series_ticker": series_ticker,
                    "strategy_name": recommendation["strategy_name"],
                    "status": recommendation["status"],
                    "resolved_trade_count": recommendation["resolved_trade_count"],
                    "outcome_coverage_rate": recommendation["outcome_coverage_rate"],
                    "gap_to_runner_up": recommendation["gap_to_runner_up"],
                    "win_rate": decision["best_row"]["win_rate"] if decision["best_row"] is not None else None,
                })

            top_candidates.sort(
                key=lambda candidate: (
                    _recommendation_sort_priority(candidate["status"]),
                    -(candidate["resolved_trade_count"] or 0),
                    -(candidate["gap_to_runner_up"] if candidate["gap_to_runner_up"] is not None else -1.0),
                    -(candidate["win_rate"] if candidate["win_rate"] is not None else -1.0),
                    candidate["series_ticker"],
                )
            )
            top_candidates = top_candidates[:5]

            await repo.set_checkpoint(
                "strategy_regression",
                None,
                {
                    "ran_at": now.isoformat(),
                    "rooms_scanned": diagnostics["rooms_scanned"],
                    "rooms_included": diagnostics["rooms_included"],
                    "rooms_skipped_stand_down": diagnostics["rooms_skipped_stand_down"],
                    "rooms_skipped_unmapped": diagnostics["rooms_skipped_unmapped"],
                    "series_evaluated": diagnostics["series_evaluated"],
                    "series_prefixes_seen": diagnostics["series_prefixes_seen"],
                    "strategies_evaluated": len(strategies),
                    "recommendation_mode": RECOMMENDATION_MODE,
                    "cleared_auto_assignments": 0,
                    "promotions": [],
                    "recommendation_counts": dict(recommendation_counts),
                    "top_candidates": top_candidates,
                    "window_days": WINDOW_DAYS,
                },
            )
            if diagnostics["rooms_scanned"] > 0 and diagnostics["series_evaluated"] == 0:
                logger.warning(
                    "strategy_regression mapped zero series",
                    extra={
                        "rooms_scanned": diagnostics["rooms_scanned"],
                        "rooms_skipped_stand_down": diagnostics["rooms_skipped_stand_down"],
                        "rooms_skipped_unmapped": diagnostics["rooms_skipped_unmapped"],
                    },
                )
                await repo.log_ops_event(
                    severity="warning",
                    summary=(
                        f"Strategy regression scanned {diagnostics['rooms_scanned']} rooms "
                        "but mapped 0 city series."
                    ),
                    source="strategy_regression",
                    payload={
                        "event_kind": "diagnostic",
                        **diagnostics,
                        "run_at": now.isoformat(),
                    },
                )
            await session.commit()

        return {
            "status": "ok",
            "rooms_scanned": diagnostics["rooms_scanned"],
            "rooms_included": diagnostics["rooms_included"],
            "rooms_skipped_stand_down": diagnostics["rooms_skipped_stand_down"],
            "rooms_skipped_unmapped": diagnostics["rooms_skipped_unmapped"],
            "series_evaluated": diagnostics["series_evaluated"],
            "series_prefixes_seen": diagnostics["series_prefixes_seen"],
            "result_rows": len(result_rows),
            "recommendation_mode": RECOMMENDATION_MODE,
            "recommendation_counts": dict(recommendation_counts),
            "top_candidates": top_candidates,
            "cleared_auto_assignments": 0,
            "promotions": [],
        }

    def _evaluate_city(
        self,
        strategy_id: int | None,
        strategy_name: str,
        series_ticker: str,
        rooms: list[dict[str, Any]],
        t: RuntimeThresholds,
        run_at: datetime,
        date_from: datetime,
        date_to: datetime,
    ) -> dict[str, Any]:
        trade_count = 0
        resolved_trade_count = 0
        win_count = 0
        total_pnl = Decimal("0")
        edge_sum = 0.0

        for room in rooms:
            if not _would_have_traded(room, t):
                continue
            trade_count += 1
            edge_sum += room["edge_bps"]

            outcome = _score_strategy_room_outcome(room)
            if outcome is None:
                continue
            resolved_trade_count += 1
            if outcome.settlement_result == "win":
                win_count += 1
            total_pnl += outcome.pnl_dollars

        rooms_evaluated = len(rooms)
        unscored_trade_count = max(0, trade_count - resolved_trade_count)
        trade_rate = trade_count / rooms_evaluated if rooms_evaluated > 0 else None
        win_rate = win_count / resolved_trade_count if resolved_trade_count > 0 else None
        avg_edge_bps = edge_sum / trade_count if trade_count > 0 else None

        return {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "run_at": run_at,
            "date_from": date_from.date().isoformat(),
            "date_to": date_to.date().isoformat(),
            "series_ticker": series_ticker,
            "rooms_evaluated": rooms_evaluated,
            "trade_count": trade_count,
            "resolved_trade_count": resolved_trade_count,
            "unscored_trade_count": unscored_trade_count,
            "win_count": win_count,
            "total_pnl_dollars": (
                total_pnl.quantize(Decimal("0.0001")) if resolved_trade_count > 0 else None
            ),
            "trade_rate": round(trade_rate, 4) if trade_rate is not None else None,
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
            "avg_edge_bps": round(avg_edge_bps, 2) if avg_edge_bps is not None else None,
        }
