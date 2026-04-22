from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import StrategyCodexRunRequest, StrategyThresholdPreset
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.strategy_codex import StrategyCodexService, _json_safe
from kalshi_bot.services.strategy_regression import (
    STRATEGY_PRESETS,
    RegressionStrategySpec,
    StrategyRegressionService,
)
from kalshi_bot.weather.mapping import WeatherMarketDirectory


class FakeProviderRouter:
    def __init__(self, *, gemini=None, codex=None) -> None:
        self.gemini = gemini
        self.codex = codex

    async def close(self) -> None:
        return None


def _valid_thresholds() -> dict[str, object]:
    return {
        "risk_min_edge_bps": 50,
        "risk_max_order_notional_dollars": 10.0,
        "risk_max_position_notional_dollars": 25.0,
        "trigger_max_spread_bps": 500,
        "trigger_cooldown_seconds": 300,
        "strategy_quality_edge_buffer_bps": 20,
        "strategy_min_remaining_payout_bps": 500,
        "risk_safe_capital_reserve_ratio": 0.70,
        "risk_risky_capital_max_ratio": 0.30,
    }


def _strategy_rooms() -> list[dict[str, object]]:
    return [
        {
            "market_ticker": "KXHIGHNY-26APR22-T70-A",
            "series_ticker": "KXHIGHNY",
            "edge_bps": 65,
            "signal_payload": {"eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.90"}},
            "ticket_side": "yes",
            "ticket_yes_price_dollars": "0.40",
            "ticket_count_fp": "1.00",
            "settlement_value_dollars": "1.0000",
            "kalshi_result": "yes",
        },
        {
            "market_ticker": "KXHIGHNY-26APR22-T70-B",
            "series_ticker": "KXHIGHNY",
            "edge_bps": 62,
            "signal_payload": {"eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.85"}},
            "ticket_side": "yes",
            "ticket_yes_price_dollars": "0.45",
            "ticket_count_fp": "1.00",
            "settlement_value_dollars": "1.0000",
            "kalshi_result": "yes",
        },
        {
            "market_ticker": "KXHIGHNY-26APR22-T70-C",
            "series_ticker": "KXHIGHNY",
            "edge_bps": 35,
            "signal_payload": {"eligibility": {"market_spread_bps": 10, "remaining_payout_dollars": "0.80"}},
            "ticket_side": "yes",
            "ticket_yes_price_dollars": "0.55",
            "ticket_count_fp": "1.00",
            "settlement_value_dollars": "0.0000",
            "kalshi_result": "no",
        },
    ]


def test_strategy_threshold_preset_accepts_valid_thresholds() -> None:
    preset = StrategyThresholdPreset.model_validate(_valid_thresholds())

    assert preset.risk_min_edge_bps == 50
    assert preset.risk_safe_capital_reserve_ratio == pytest.approx(0.70)
    assert preset.risk_risky_capital_max_ratio == pytest.approx(0.30)


@pytest.mark.parametrize(
    "payload",
    [
        {**_valid_thresholds(), "unexpected": 1},
        {key: value for key, value in _valid_thresholds().items() if key != "trigger_cooldown_seconds"},
        {**_valid_thresholds(), "risk_min_edge_bps": "50"},
        {**_valid_thresholds(), "risk_safe_capital_reserve_ratio": 0.6, "risk_risky_capital_max_ratio": 0.5},
    ],
)
def test_strategy_threshold_preset_rejects_invalid_shapes_and_values(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        StrategyThresholdPreset.model_validate(payload)


@pytest.mark.asyncio
async def test_strategy_codex_unique_strategy_name_uses_deterministic_suffixes() -> None:
    class FakeRepo:
        def __init__(self) -> None:
            self.names = {"balanced-plus", "balanced-plus-2"}

        async def get_strategy_by_name(self, name: str):
            return SimpleNamespace(name=name) if name in self.names else None

    service = object.__new__(StrategyCodexService)

    unique_name = await service._unique_strategy_name(FakeRepo(), "balanced-plus")

    assert unique_name == "balanced-plus-3"


def test_strategy_codex_service_prefers_gemini_when_available() -> None:
    service = StrategyCodexService(
        Settings(database_url="sqlite+aiosqlite:///./strategy-codex-gemini.db"),
        SimpleNamespace(),
        SimpleNamespace(),
        FakeProviderRouter(gemini=object(), codex=object()),
    )

    assert service.is_available() is True
    assert service._preferred_provider_id() == "gemini"
    assert service._default_model_for_provider("gemini") == "gemini-2.5-pro"
    provider_options = service._provider_options()
    assert provider_options[0]["id"] == "gemini"
    assert provider_options[0]["suggested_models"] == ["gemini-2.5-pro", "gemini-2.5-flash"]


def test_strategy_codex_service_reports_unavailable_without_strategy_providers() -> None:
    service = StrategyCodexService(
        Settings(database_url="sqlite+aiosqlite:///./strategy-codex-none.db"),
        SimpleNamespace(),
        SimpleNamespace(),
        FakeProviderRouter(gemini=None, codex=None),
    )

    assert service.is_available() is False
    assert service._provider_options() == []


def test_strategy_codex_json_safe_normalizes_decimal_payloads() -> None:
    payload = {
        "kind": "suggest",
        "backtest": {
            "candidate_metrics": {"total_pnl_dollars": Decimal("2.5000")},
            "candidate_result_rows": [{"total_pnl_dollars": Decimal("1.2500")}],
            "strongest_cities": [{"total_pnl_dollars": Decimal("0.7500")}],
        },
    }

    encoded = _json_safe(payload)
    decoded = json.loads(json.dumps(encoded))

    assert decoded["backtest"]["candidate_metrics"]["total_pnl_dollars"] == 2.5
    assert decoded["backtest"]["candidate_result_rows"][0]["total_pnl_dollars"] == 1.25
    assert decoded["backtest"]["strongest_cities"][0]["total_pnl_dollars"] == 0.75


@pytest.mark.asyncio
async def test_strategy_codex_create_run_persists_selected_provider_and_model(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-codex-provider.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    service = StrategyCodexService(
        settings,
        session_factory,
        SimpleNamespace(),
        FakeProviderRouter(gemini=object(), codex=object()),
    )

    run = await service.create_run(
        request=StrategyCodexRunRequest(mode="evaluate", window_days=180, provider="codex", model="gpt-4o"),
        dashboard_snapshot={"summary": {"window_days": 180}, "leaderboard": [], "city_matrix": []},
        trigger_source="manual",
    )

    async with session_factory() as session:
        repo = PlatformRepository(session)
        record = await repo.get_strategy_codex_run(run["run_id"])
        await session.commit()

    assert record is not None
    assert record.provider == "codex"
    assert record.model == "gpt-4o"

    await engine.dispose()


def test_candidate_backtest_uses_dashboard_metric_shape() -> None:
    aggressive = next(preset for preset in STRATEGY_PRESETS if preset["name"] == "aggressive")
    moderate = next(preset for preset in STRATEGY_PRESETS if preset["name"] == "moderate")
    service = StrategyRegressionService(
        settings=SimpleNamespace(),
        session_factory=SimpleNamespace(),
        weather_directory=WeatherMarketDirectory({}, {}),
        agent_pack_service=SimpleNamespace(),
    )
    run_at = datetime(2026, 4, 22, tzinfo=UTC)

    result = service.evaluate_strategy_specs_from_rooms(
        strategies=[
            RegressionStrategySpec(
                id=1,
                name=aggressive["name"],
                description=aggressive["description"],
                thresholds=aggressive["thresholds"],
            ),
            RegressionStrategySpec(
                id=2,
                name=moderate["name"],
                description=moderate["description"],
                thresholds=moderate["thresholds"],
            ),
        ],
        rooms=_strategy_rooms(),
        run_at=run_at,
        date_from=run_at - timedelta(days=180),
        date_to=run_at,
        window_days=180,
    )

    assert result["status"] == "ok"
    assert result["diagnostics"]["series_evaluated"] == 1
    assert set(result.keys()) >= {"leaderboard", "city_results", "result_rows", "diagnostics"}
    leaderboard_row = result["leaderboard"][0]
    assert set(leaderboard_row.keys()) >= {
        "name",
        "thresholds",
        "overall_win_rate",
        "overall_trade_rate",
        "total_pnl_dollars",
        "avg_edge_bps",
        "cities_led",
        "outcome_coverage_rate",
    }
    city_row = result["city_results"]["KXHIGHNY"]["aggressive"]
    assert set(city_row.keys()) >= {
        "strategy_name",
        "trade_count",
        "resolved_trade_count",
        "win_rate",
        "total_pnl_dollars",
        "avg_edge_bps",
    }
    result_row = result["result_rows"][0]
    assert set(result_row.keys()) >= {
        "strategy_id",
        "strategy_name",
        "series_ticker",
        "trade_count",
        "resolved_trade_count",
        "unscored_trade_count",
        "win_rate",
        "total_pnl_dollars",
        "avg_edge_bps",
    }


@pytest.mark.asyncio
async def test_strategy_codex_payloads_include_trigger_source(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/strategy-codex.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        run = await repo.create_strategy_codex_run(
            mode="evaluate",
            status="completed",
            trigger_source="nightly",
            window_days=180,
            series_ticker=None,
            strategy_name=None,
            operator_brief=None,
            provider="codex-cli",
            model="gpt-4o",
            payload={
                "result": {
                    "kind": "evaluate",
                    "evaluation": {"summary": "Nightly global landscape review."},
                }
            },
        )
        await session.commit()

    service = StrategyCodexService(
        settings,
        session_factory,
        SimpleNamespace(),
        FakeProviderRouter(gemini=None, codex=object()),
    )

    dashboard_payload = await service.dashboard_payload()
    run_view = await service.get_run_view(run.id)

    assert dashboard_payload["recent_runs"][0]["trigger_source"] == "nightly"
    assert run_view is not None
    assert run_view["trigger_source"] == "nightly"

    await engine.dispose()
