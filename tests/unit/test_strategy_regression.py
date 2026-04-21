from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

import kalshi_bot.services.strategy_regression as strategy_regression_module
from kalshi_bot.services.strategy_regression import (
    LEAN_RECOMMENDATION_MIN_GAP,
    RECOMMENDATION_MIN_OUTCOME_COVERAGE_RATE,
    RECOMMENDATION_MODE,
    STRATEGY_PRESETS,
    STRONG_RECOMMENDATION_MIN_GAP,
    StrategyRegressionService,
    _recommendation_decision,
    _would_have_traded,
    _thresholds_from_dict,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory
from kalshi_bot.weather.models import WeatherSeriesTemplate


def _room(edge_bps: int, spread_bps: int | None = 100, remaining_payout_dollars: float | None = 0.60, stand_down_reason: str | None = None) -> dict:
    eligibility: dict = {}
    if spread_bps is not None:
        eligibility["market_spread_bps"] = spread_bps
    if remaining_payout_dollars is not None:
        eligibility["remaining_payout_dollars"] = str(remaining_payout_dollars)
    return {
        "edge_bps": edge_bps,
        "signal_payload": {
            "stand_down_reason": stand_down_reason,
            "eligibility": eligibility,
        },
    }


@pytest.fixture
def aggressive():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "aggressive")["thresholds"])


@pytest.fixture
def moderate():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "moderate")["thresholds"])


@pytest.fixture
def conservative():
    return _thresholds_from_dict(next(p for p in STRATEGY_PRESETS if p["name"] == "conservative")["thresholds"])


def test_aggressive_trades_on_min_edge(aggressive):
    # spread=10, net_edge=10 >= quality_buffer=0
    room = _room(edge_bps=20, spread_bps=10)
    assert _would_have_traded(room, aggressive) is True


def test_moderate_rejects_below_min_edge(moderate):
    room = _room(edge_bps=30, spread_bps=10)
    assert _would_have_traded(room, moderate) is False


def test_moderate_trades_on_sufficient_edge(moderate):
    # edge=80, spread=50, net=30 >= quality_buffer=20
    room = _room(edge_bps=80, spread_bps=50)
    assert _would_have_traded(room, moderate) is True


def test_conservative_rejects_wide_spread(conservative):
    room = _room(edge_bps=150, spread_bps=400)
    assert _would_have_traded(room, conservative) is False


def test_conservative_rejects_low_remaining_payout(conservative):
    # remaining_payout_bps = 0.05 * 10000 = 500, conservative requires 800
    room = _room(edge_bps=150, spread_bps=100, remaining_payout_dollars=0.05)
    assert _would_have_traded(room, conservative) is False


def test_conservative_trades_high_quality(conservative):
    room = _room(edge_bps=150, spread_bps=100, remaining_payout_dollars=0.15)
    assert _would_have_traded(room, conservative) is True


def test_quality_buffer_blocks_trade(moderate):
    # edge=60, spread=50, net=10 which is < quality_buffer=20
    room = _room(edge_bps=60, spread_bps=50)
    assert _would_have_traded(room, moderate) is False


def test_quality_buffer_allows_trade(moderate):
    # edge=80, spread=50, net=30 which is > quality_buffer=20
    room = _room(edge_bps=80, spread_bps=50)
    assert _would_have_traded(room, moderate) is True


def test_missing_spread_skips_spread_gate(moderate):
    # No spread data — spread gate is skipped, only edge gate applies
    room = _room(edge_bps=60, spread_bps=None)
    assert _would_have_traded(room, moderate) is True


def test_missing_remaining_payout_skips_payout_gate(moderate):
    # spread=30, net=30 >= quality_buffer=20; payout gate skipped (None)
    room = _room(edge_bps=60, spread_bps=30, remaining_payout_dollars=None)
    assert _would_have_traded(room, moderate) is True


def test_all_three_presets_have_required_fields():
    required = {
        "risk_min_edge_bps", "risk_max_order_notional_dollars", "risk_max_position_notional_dollars",
        "trigger_max_spread_bps", "trigger_cooldown_seconds", "strategy_quality_edge_buffer_bps",
        "strategy_min_remaining_payout_bps", "risk_safe_capital_reserve_ratio", "risk_risky_capital_max_ratio",
    }
    for preset in STRATEGY_PRESETS:
        assert required.issubset(preset["thresholds"].keys()), f"{preset['name']} missing fields"


@pytest.mark.parametrize(
    ("best_row", "runner_up_row", "expected_status"),
    [
        (
            {
                "strategy_name": "moderate",
                "trade_count": 20,
                "resolved_trade_count": 20,
                "win_count": 15,
                "win_rate": 0.75,
                "total_pnl_dollars": Decimal("1.20"),
            },
            {
                "strategy_name": "aggressive",
                "trade_count": 22,
                "resolved_trade_count": 22,
                "win_count": 16,
                "win_rate": 0.73,
                "total_pnl_dollars": Decimal("0.90"),
            },
            "strong_recommendation",
        ),
        (
            {
                "strategy_name": "moderate",
                "trade_count": 20,
                "resolved_trade_count": 20,
                "win_count": 14,
                "win_rate": 0.70,
                "total_pnl_dollars": Decimal("0.50"),
            },
            {
                "strategy_name": "aggressive",
                "trade_count": 21,
                "resolved_trade_count": 21,
                "win_count": 14,
                "win_rate": 0.685,
                "total_pnl_dollars": Decimal("0.40"),
            },
            "lean_recommendation",
        ),
        (
            {
                "strategy_name": "moderate",
                "trade_count": 20,
                "resolved_trade_count": 20,
                "win_count": 14,
                "win_rate": 0.70,
                "total_pnl_dollars": Decimal("0.50"),
            },
            {
                "strategy_name": "aggressive",
                "trade_count": 20,
                "resolved_trade_count": 20,
                "win_count": 14,
                "win_rate": 0.695,
                "total_pnl_dollars": Decimal("0.30"),
            },
            "too_close",
        ),
    ],
)
def test_recommendation_decision_classifies_gap_boundaries(
    best_row: dict[str, object],
    runner_up_row: dict[str, object],
    expected_status: str,
) -> None:
    decision = _recommendation_decision(
        results_by_strategy={
            best_row["strategy_name"]: dict(best_row),
            runner_up_row["strategy_name"]: dict(runner_up_row),
        },
        current_name=None,
    )

    assert decision["recommendation"]["status"] == expected_status
    assert decision["recommendation"]["resolved_trade_count"] == 20
    assert decision["clears_trade_threshold"] is True
    assert decision["clears_coverage_threshold"] is True
    assert decision["recommendation"]["writes_assignment"] is False


def test_recommendation_decision_marks_low_sample_and_no_outcomes() -> None:
    low_sample = _recommendation_decision(
        results_by_strategy={
            "moderate": {
                "strategy_name": "moderate",
                "trade_count": 18,
                "resolved_trade_count": 18,
                "win_count": 13,
                "win_rate": 13 / 18,
                "total_pnl_dollars": Decimal("0.75"),
            },
            "aggressive": {
                "strategy_name": "aggressive",
                "trade_count": 18,
                "resolved_trade_count": 18,
                "win_count": 11,
                "win_rate": 11 / 18,
                "total_pnl_dollars": Decimal("0.10"),
            },
        },
        current_name=None,
    )
    no_outcomes = _recommendation_decision(
        results_by_strategy={
            "moderate": {
                "strategy_name": "moderate",
                "trade_count": 22,
                "resolved_trade_count": 0,
                "win_count": 0,
                "win_rate": None,
                "total_pnl_dollars": None,
            }
        },
        current_name=None,
    )

    assert low_sample["recommendation"]["status"] == "low_sample"
    assert low_sample["recommendation"]["resolved_trade_count"] < 20
    assert low_sample["clears_trade_threshold"] is False
    assert low_sample["clears_coverage_threshold"] is True
    assert no_outcomes["recommendation"]["status"] == "no_outcomes"
    assert no_outcomes["recommendation"]["resolved_trade_count"] == 0
    assert no_outcomes["winner_wilson_lower"] is None
    assert RECOMMENDATION_MIN_OUTCOME_COVERAGE_RATE == pytest.approx(0.95)
    assert STRONG_RECOMMENDATION_MIN_GAP == pytest.approx(0.02)
    assert LEAN_RECOMMENDATION_MIN_GAP == pytest.approx(0.01)


class _FakeSession:
    async def commit(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> "_FakeSessionFactory":
        return self

    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


@pytest.mark.asyncio
async def test_strategy_regression_runs_with_template_only_weather_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _session) -> None:
            self.saved_results: list[dict[str, object]] = []
            self.assignments: dict[str, str] = {"STALE": "aggressive"}
            self.checkpoint_payload: dict[str, object] | None = None
            self.events: list[dict[str, object]] = []

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            assert active_only is True
            return [
                SimpleNamespace(id=1, name="aggressive", thresholds=next(p["thresholds"] for p in STRATEGY_PRESETS if p["name"] == "aggressive")),
                SimpleNamespace(id=2, name="moderate", thresholds=next(p["thresholds"] for p in STRATEGY_PRESETS if p["name"] == "moderate")),
            ]

        async def get_strategy_regression_rooms(self, date_from: datetime, date_to: datetime) -> list[dict[str, object]]:
            assert date_to > date_from
            winning_payload = {
                "eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"},
            }
            losing_payload = {
                "eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"},
            }
            rooms = [
                {
                    "market_ticker": f"KXHIGHNY-26APR18-T68-WIN-{idx}",
                    "edge_bps": 65,
                    "signal_payload": winning_payload,
                    "ticket_side": "yes",
                    "ticket_yes_price_dollars": "0.40",
                    "ticket_count_fp": "1.00",
                    "settlement_value_dollars": "1.0000",
                    "kalshi_result": "yes",
                }
                for idx in range(20)
            ]
            rooms.extend(
                {
                    "market_ticker": f"KXHIGHNY-26APR18-T68-LOSS-{idx}",
                    "edge_bps": 35,
                    "signal_payload": losing_payload,
                    "ticket_side": "yes",
                    "ticket_yes_price_dollars": "0.55",
                    "ticket_count_fp": "1.00",
                    "settlement_value_dollars": "0.0000",
                    "kalshi_result": "no",
                }
                for idx in range(5)
            )
            rooms.append({
                "market_ticker": "KXHIGHNY-26APR18-T68-SKIP",
                "edge_bps": 80,
                "signal_payload": {
                    "stand_down_reason": "resolved_contract",
                    "eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"},
                },
            })
            rooms.append({
                "market_ticker": "UNMAPPED-26APR18-T68",
                "edge_bps": 80,
                "signal_payload": winning_payload,
            })
            return rooms

        async def save_strategy_results(self, results: list[dict[str, object]]) -> None:
            self.saved_results = list(results)

        async def get_city_strategy_assignment(self, series_ticker: str):
            strategy_name = self.assignments.get(series_ticker)
            if strategy_name is None:
                return None
            return SimpleNamespace(strategy_name=strategy_name)

        async def clear_city_strategy_assignments(self, *, assigned_by: str | None = None) -> int:
            cleared = len(self.assignments)
            self.assignments = {}
            return cleared

        async def set_city_strategy_assignment(self, series_ticker: str, strategy_name: str, assigned_by: str = "auto_regression") -> None:
            self.assignments[series_ticker] = strategy_name

        async def log_ops_event(self, *, severity: str, summary: str, source: str, payload: dict[str, object], room_id: str | None = None):
            event = {
                "severity": severity,
                "summary": summary,
                "source": source,
                "payload": dict(payload),
                "room_id": room_id,
            }
            self.events.append(event)
            return SimpleNamespace(**event)

        async def set_checkpoint(self, stream_name: str, room_id, payload: dict[str, object]) -> None:
            assert stream_name == "strategy_regression"
            self.checkpoint_payload = dict(payload)

    fake_repo = FakeRepo(None)
    monkeypatch.setattr(strategy_regression_module, "PlatformRepository", lambda _session: fake_repo)

    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHNY": WeatherSeriesTemplate(
                series_ticker="KXHIGHNY",
                location_name="New York City",
                station_id="KNYC",
                daily_summary_station_id="USW00094728",
                latitude=40.7146,
                longitude=-74.0071,
            )
        },
    )
    service = StrategyRegressionService(
        settings=SimpleNamespace(),
        session_factory=_FakeSessionFactory(),
        weather_directory=directory,
        agent_pack_service=SimpleNamespace(),
    )

    result = await service.run_regression()

    assert result["status"] == "ok"
    assert result["rooms_scanned"] == 27
    assert result["rooms_included"] == 25
    assert result["rooms_skipped_stand_down"] == 1
    assert result["rooms_skipped_unmapped"] == 1
    assert result["series_evaluated"] == 1
    assert result["series_prefixes_seen"] == ["KXHIGHNY"]
    assert len(fake_repo.saved_results) == 2
    aggressive_row = next(row for row in fake_repo.saved_results if row["strategy_id"] == 1)
    moderate_row = next(row for row in fake_repo.saved_results if row["strategy_id"] == 2)
    assert aggressive_row["trade_count"] == 25
    assert aggressive_row["resolved_trade_count"] == 25
    assert aggressive_row["unscored_trade_count"] == 0
    assert aggressive_row["win_rate"] == 0.8
    assert aggressive_row["total_pnl_dollars"] is not None
    assert moderate_row["trade_count"] == 20
    assert moderate_row["resolved_trade_count"] == 20
    assert moderate_row["unscored_trade_count"] == 0
    assert moderate_row["win_rate"] == 1.0
    assert fake_repo.assignments == {"STALE": "aggressive"}
    assert fake_repo.checkpoint_payload is not None
    assert fake_repo.checkpoint_payload["series_evaluated"] == 1
    assert fake_repo.checkpoint_payload["recommendation_mode"] == RECOMMENDATION_MODE
    assert fake_repo.checkpoint_payload["cleared_auto_assignments"] == 0
    assert fake_repo.checkpoint_payload["rooms_skipped_unmapped"] == 1
    assert fake_repo.checkpoint_payload["series_prefixes_seen"] == ["KXHIGHNY"]
    assert fake_repo.checkpoint_payload["promotions"] == []
    assert fake_repo.checkpoint_payload["recommendation_counts"]["strong_recommendation"] == 1
    assert fake_repo.checkpoint_payload["top_candidates"][0]["series_ticker"] == "KXHIGHNY"
    assert fake_repo.checkpoint_payload["top_candidates"][0]["strategy_name"] == "moderate"
    assert fake_repo.checkpoint_payload["top_candidates"][0]["gap_to_runner_up"] == pytest.approx(0.2)
    assert result["recommendation_mode"] == RECOMMENDATION_MODE
    assert result["recommendation_counts"]["strong_recommendation"] == 1


@pytest.mark.asyncio
async def test_strategy_regression_tracks_unscored_trades_without_promoting(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _session) -> None:
            self.saved_results: list[dict[str, object]] = []
            self.assignments: dict[str, str] = {}
            self.checkpoint_payload: dict[str, object] | None = None

        async def list_strategies(self, *, active_only: bool = True) -> list[SimpleNamespace]:
            assert active_only is True
            return [
                SimpleNamespace(id=1, name="aggressive", thresholds=next(p["thresholds"] for p in STRATEGY_PRESETS if p["name"] == "aggressive")),
                SimpleNamespace(id=2, name="moderate", thresholds=next(p["thresholds"] for p in STRATEGY_PRESETS if p["name"] == "moderate")),
            ]

        async def get_strategy_regression_rooms(self, date_from: datetime, date_to: datetime) -> list[dict[str, object]]:
            assert date_to > date_from
            return [
                {
                    "market_ticker": f"KXHIGHNY-26APR18-T68-{idx}",
                    "edge_bps": 65,
                    "signal_payload": {"eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"}},
                }
                for idx in range(25)
            ]

        async def save_strategy_results(self, results: list[dict[str, object]]) -> None:
            self.saved_results = list(results)

        async def clear_city_strategy_assignments(self, *, assigned_by: str | None = None) -> int:
            return 0

        async def get_city_strategy_assignment(self, series_ticker: str):
            return None

        async def set_city_strategy_assignment(self, series_ticker: str, strategy_name: str, assigned_by: str = "auto_regression") -> None:
            self.assignments[series_ticker] = strategy_name

        async def log_ops_event(self, *, severity: str, summary: str, source: str, payload: dict[str, object], room_id: str | None = None):
            return SimpleNamespace(severity=severity, summary=summary, source=source, payload=payload, room_id=room_id)

        async def set_checkpoint(self, stream_name: str, room_id, payload: dict[str, object]) -> None:
            assert stream_name == "strategy_regression"
            self.checkpoint_payload = dict(payload)

    fake_repo = FakeRepo(None)
    monkeypatch.setattr(strategy_regression_module, "PlatformRepository", lambda _session: fake_repo)

    directory = WeatherMarketDirectory(
        {},
        {
            "KXHIGHNY": WeatherSeriesTemplate(
                series_ticker="KXHIGHNY",
                location_name="New York City",
                station_id="KNYC",
                daily_summary_station_id="USW00094728",
                latitude=40.7146,
                longitude=-74.0071,
            )
        },
    )
    service = StrategyRegressionService(
        settings=SimpleNamespace(),
        session_factory=_FakeSessionFactory(),
        weather_directory=directory,
        agent_pack_service=SimpleNamespace(),
    )

    result = await service.run_regression()

    assert result["status"] == "ok"
    assert fake_repo.assignments == {}
    aggressive_row = next(row for row in fake_repo.saved_results if row["strategy_id"] == 1)
    assert aggressive_row["trade_count"] == 25
    assert aggressive_row["resolved_trade_count"] == 0
    assert aggressive_row["unscored_trade_count"] == 25
    assert aggressive_row["win_rate"] is None
    assert aggressive_row["total_pnl_dollars"] is None
    assert fake_repo.checkpoint_payload is not None
    assert fake_repo.checkpoint_payload["recommendation_mode"] == RECOMMENDATION_MODE
    assert fake_repo.checkpoint_payload["promotions"] == []
    assert fake_repo.checkpoint_payload["recommendation_counts"]["no_outcomes"] == 1
