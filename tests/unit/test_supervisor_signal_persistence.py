from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

import kalshi_bot.orchestration.supervisor as supervisor_module
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction, WeatherResolutionState
from kalshi_bot.core.schemas import (
    ResearchDelta,
    ResearchDossier,
    ResearchFreshness,
    ResearchGateVerdict,
    ResearchQualitySummary,
    ResearchSummary,
    ResearchTraderContext,
)
from kalshi_bot.orchestration.supervisor import WorkflowSupervisor
from kalshi_bot.services.agent_packs import RuntimeThresholds
from kalshi_bot.services.signal import StrategySignal
from kalshi_bot.weather.scoring import WeatherSignalSnapshot


class FakeSession:
    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def commit(self) -> None:
        return None


class FakeSessionFactory:
    def __call__(self) -> FakeSession:
        return FakeSession()


class FakePlatformRepository:
    saved_signal: dict | None = None

    def __init__(self, session: FakeSession, *, kalshi_env: str | None = None) -> None:
        self.session = session
        self.kalshi_env = kalshi_env

    async def ensure_deployment_control(self, color: str):
        return SimpleNamespace(kill_switch_enabled=False)

    async def get_room(self, room_id: str):
        return SimpleNamespace(id=room_id, market_ticker="WX-WIDE", kalshi_env="demo")

    async def append_message(self, *args, **kwargs):
        return SimpleNamespace(id="message-1")

    async def update_room_runtime(self, *args, **kwargs) -> None:
        return None

    async def get_city_strategy_assignment(self, *args, **kwargs):
        return None

    async def log_exchange_event(self, *args, **kwargs) -> None:
        return None

    async def upsert_market_state(self, *args, **kwargs):
        return SimpleNamespace(observed_at=datetime.now(UTC))

    async def save_signal(self, **kwargs):
        type(self).saved_signal = kwargs
        return SimpleNamespace(id="signal-1", **kwargs)

    async def list_positions_for_ticker(self, *args, **kwargs) -> list:
        return []


class FakeAgentPackService:
    async def get_pack_for_color(self, repo, color: str):
        return SimpleNamespace(version="test-pack", roles={})

    def runtime_thresholds(self, pack) -> RuntimeThresholds:
        return RuntimeThresholds(
            risk_min_edge_bps=10,
            risk_max_order_notional_dollars=50.0,
            risk_max_position_notional_dollars=100.0,
            trigger_max_spread_bps=1200,
            trigger_cooldown_seconds=0,
            strategy_quality_edge_buffer_bps=0,
            strategy_min_remaining_payout_bps=100,
        )


class FakeKalshi:
    async def get_market(self, ticker: str) -> dict:
        return {
            "market": {
                "ticker": ticker,
                "yes_bid_dollars": "0.0800",
                "yes_ask_dollars": "0.2000",
                "no_ask_dollars": "0.8000",
                "last_price_dollars": "0.1400",
                "volume": 200,
            }
        }


class FakeWeatherDirectory:
    def resolve_market(self, market_ticker: str, market: dict):
        return None


class FakeResearchCoordinator:
    async def ensure_fresh_dossier(self, market_ticker: str, *, reason: str) -> ResearchDossier:
        now = datetime.now(UTC)
        return ResearchDossier(
            market_ticker=market_ticker,
            status="ready",
            mode="structured_weather",
            summary=ResearchSummary(
                narrative="Structured weather signal is ready.",
                bullish_case="Forecast is above threshold.",
                bearish_case="Market structure may block entry.",
                settlement_mechanics="Daily high settles the contract.",
                source_coverage="structured",
                research_confidence=0.9,
            ),
            freshness=ResearchFreshness(
                refreshed_at=now,
                expires_at=now + timedelta(minutes=30),
                stale=False,
                max_source_age_seconds=0,
            ),
            quality=ResearchQualitySummary(overall_score=0.9),
            trader_context=ResearchTraderContext(
                fair_yes_dollars=Decimal("0.9000"),
                confidence=0.9,
                thesis="Above-threshold setup.",
                structured_source_used=True,
                autonomous_ready=True,
                forecast_delta_f=8.0,
                confidence_band="high",
            ),
            gate=ResearchGateVerdict(passed=True, cited_source_keys=["structured"]),
            settlement_covered=True,
            trade_regime="standard",
            capital_bucket="safe",
            forecast_delta_f=8.0,
            confidence_band="high",
            last_run_id="fake-research-run",
        )

    def build_room_delta(
        self,
        *,
        dossier: ResearchDossier,
        market_response: dict,
        weather_bundle: dict | None,
    ) -> ResearchDelta:
        return ResearchDelta(summary="No prior room research delta.")

    def build_signal_from_dossier(
        self,
        dossier: ResearchDossier,
        market_response: dict,
        *,
        min_edge_bps: int,
    ) -> StrategySignal:
        now = datetime.now(UTC)
        return StrategySignal(
            fair_yes_dollars=Decimal("0.9000"),
            confidence=0.9,
            edge_bps=7600,
            recommended_action=TradeAction.BUY,
            recommended_side=ContractSide.YES,
            target_yes_price_dollars=Decimal("0.2000"),
            summary="Candidate clears base signal checks.",
            weather=WeatherSignalSnapshot(
                fair_yes_dollars=Decimal("0.9000"),
                confidence=0.9,
                forecast_high_f=88.0,
                current_temp_f=77.0,
                forecast_delta_f=8.0,
                confidence_band="high",
                trade_regime="standard",
                resolution_state=WeatherResolutionState.UNRESOLVED,
                observation_time=now,
                forecast_updated_time=now,
                summary="Forecast high is above threshold.",
            ),
            candidate_trace={"outcome": "candidate_selected"},
            forecast_delta_f=8.0,
            confidence_band="high",
        )


@pytest.mark.asyncio
async def test_supervisor_persists_post_market_gate_signal_state(monkeypatch) -> None:
    FakePlatformRepository.saved_signal = None
    monkeypatch.setattr(supervisor_module, "PlatformRepository", FakePlatformRepository)
    settings = Settings(
        app_color="blue",
        app_shadow_mode=False,
        llm_trading_enabled=False,
        risk_min_edge_bps=10,
        trigger_max_spread_bps=1200,
    )
    supervisor = WorkflowSupervisor(
        settings=settings,
        session_factory=FakeSessionFactory(),  # type: ignore[arg-type]
        kalshi=FakeKalshi(),  # type: ignore[arg-type]
        weather=SimpleNamespace(),
        weather_directory=FakeWeatherDirectory(),  # type: ignore[arg-type]
        agent_pack_service=FakeAgentPackService(),  # type: ignore[arg-type]
        signal_engine=SimpleNamespace(),
        risk_engine=SimpleNamespace(),
        execution_service=SimpleNamespace(),
        memory_service=SimpleNamespace(),
        research_coordinator=FakeResearchCoordinator(),  # type: ignore[arg-type]
        training_corpus_service=SimpleNamespace(),
        agents=SimpleNamespace(),
    )

    async def no_momentum(signal, **kwargs):
        return signal, None

    async def no_fast_path(**kwargs) -> None:
        return None

    monkeypatch.setattr(supervisor, "_try_apply_momentum_post_processor", no_momentum)
    monkeypatch.setattr(supervisor, "_run_deterministic_fast_path", no_fast_path)

    await supervisor.run_room("room-1", reason="test")

    saved_signal = FakePlatformRepository.saved_signal
    assert saved_signal is not None
    assert saved_signal["summary"].startswith("Stand down: Bid-ask spread")
    assert saved_signal["payload"]["stand_down_reason"] == "market_spread_over_60pct"
    assert saved_signal["payload"]["eligibility"]["eligible"] is False
    assert saved_signal["payload"]["eligibility"]["stand_down_reason"] == "market_spread_over_60pct"
    assert saved_signal["payload"]["candidate_trace"]["eligibility_stand_down_reason"] == "market_spread_over_60pct"
