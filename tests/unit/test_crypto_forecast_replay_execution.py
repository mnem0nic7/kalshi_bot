from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, RiskStatus, RoomOrigin, StandDownReason, TradeAction
from kalshi_bot.core.schemas import ExecReceiptPayload, RiskVerdictPayload, RoomCreate, TradeEligibilityVerdict, TradeTicket
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import (
    CryptoAssetControlService,
    CryptoAutonomyService,
    CryptoWorkflowService,
    CryptoExecutionService,
    CryptoForecastService,
    CryptoReplayService,
)
from kalshi_bot.db.models import OrderRecord
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.signal import StrategySignal


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto.db",
        "web_auth_enabled": False,
        "risk_min_edge_bps": 50,
        "risk_min_confidence": 0.50,
        "risk_min_probability_extremity_pct": 0.0,
        "crypto_min_training_samples": 2,
        "crypto_replay_min_resolved_markets": 2,
        "crypto_replay_min_trade_candidates": 1,
    }
    values.update(overrides)
    return Settings(**values)


def _market(**overrides) -> CryptoMarket:
    values = {
        "market_ticker": "KXBTC15M-26APR30-B76468",
        "series_ticker": "KXBTC15M",
        "asset_symbol": "BTC",
        "frequency": "15m",
        "target_price_dollars": Decimal("76468.89000000"),
        "yes_bid_dollars": Decimal("0.4700"),
        "yes_ask_dollars": Decimal("0.4900"),
        "no_ask_dollars": Decimal("0.5300"),
        "last_price_dollars": Decimal("0.4800"),
        "volume": 105_941,
        "close_time": datetime.now(UTC) + timedelta(minutes=5),
        "status": "open",
    }
    values.update(overrides)
    return CryptoMarket(**values)


def _signal() -> StrategySignal:
    return StrategySignal(
        fair_yes_dollars=Decimal("0.6500"),
        confidence=0.90,
        edge_bps=1600,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.4900"),
        summary="BTC crypto test signal.",
        eligibility=TradeEligibilityVerdict(eligible=True),
    )


class _FakeBaseExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        return ExecReceiptPayload(
            status="submitted",
            client_order_id=kwargs["client_order_id"],
            details={"called": True},
        )


class _FakeWorkflowMarketService:
    def __init__(self, market: CryptoMarket) -> None:
        self.market = market
        self.kalshi = type("_FakeKalshi", (), {"write_credentials": None})()

    async def get_market(self, market_ticker: str, *, persist: bool = True) -> CryptoMarket:
        assert market_ticker == self.market.market_ticker
        assert persist is True
        return self.market


class _FakeForecastService:
    async def forecast(self, market: CryptoMarket) -> StrategySignal:
        del market
        return _signal()


class _ApproveRiskEngine:
    def evaluate(self, *, ticket: TradeTicket, **kwargs) -> RiskVerdictPayload:
        del kwargs
        return RiskVerdictPayload(
            status=RiskStatus.APPROVED,
            approved_count_fp=ticket.count_fp,
            approved_notional_dollars=ticket.yes_price_dollars * ticket.count_fp,
        )


@pytest.mark.asyncio
async def test_crypto_forecast_stands_down_without_trained_artifact(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    signal = await CryptoForecastService(settings=settings, session_factory=session_factory).forecast(_market())

    assert signal.recommended_side is None
    assert signal.stand_down_reason == StandDownReason.CRYPTO_MODEL_UNAVAILABLE
    assert signal.candidate_trace["market_domain"] == "crypto"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_forecast_uses_deterministic_model_artifact(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="model",
            version="test-model",
            status="trained",
            sample_count=2,
            metrics={"sample_count": 2},
            payload={"global_adjustment_bps": 800, "asset_adjustments_bps": {"BTC": 200}},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    signal = await CryptoForecastService(settings=settings, session_factory=session_factory).forecast(_market())

    assert signal.recommended_side == ContractSide.YES
    assert signal.edge_bps > 50
    assert signal.candidate_trace["model_version"] == "test-model"
    await engine.dispose()


def test_crypto_replay_gate_requires_positive_coverage_and_calibration(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = CryptoReplayService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    blocked = service.evaluate_gate(
        {
            "resolved_sample_count": 1,
            "trade_candidate_count": 0,
            "net_simulated_pl_dollars": -1.0,
            "hard_cap_breaches": 0,
            "calibration_brier": 0.30,
            "market_mid_brier": 0.25,
        }
    )
    passed = service.evaluate_gate(
        {
            "resolved_sample_count": 2,
            "trade_candidate_count": 1,
            "net_simulated_pl_dollars": 1.0,
            "hard_cap_breaches": 0,
            "calibration_brier": 0.20,
            "market_mid_brier": 0.25,
        }
    )

    assert blocked["passed"] is False
    assert passed["passed"] is True


def test_passive_prices_do_not_cross_crypto_touch(tmp_path) -> None:
    settings = _settings(tmp_path)
    service = CryptoExecutionService(  # noqa: F841 - documents constructor compatibility.
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        base_execution_service=None,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(settings=settings, session_factory=None),  # type: ignore[arg-type]
    )
    market = _market(yes_bid_dollars=Decimal("0.4700"), yes_ask_dollars=Decimal("0.4900"))

    yes_price = service.passive_yes_price(market, ContractSide.YES)
    no_price = service.passive_yes_price(market, ContractSide.NO)

    assert yes_price == Decimal("0.4701")
    assert no_price == Decimal("0.4899")


@pytest.mark.asyncio
async def test_crypto_asset_modes_default_shadow_and_persist(tmp_path) -> None:
    settings = _settings(tmp_path)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    service = CryptoAssetControlService(settings=settings, session_factory=session_factory)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.update_deployment_notes(
            {
                "kill_switch": {"mode": "manual_or_external"},
                "agent_packs": {"active_version": "green-pack"},
                "crypto_replay_gate": {"status": "passed"},
            }
        )
        await session.commit()

    default_modes = await service.list_asset_modes(asset_symbols=["btc", "ETH"])
    updated = await service.set_asset_mode("btc", "live", actor="test")
    listed = await service.list_asset_modes(asset_symbols=["BTC", "ETH"])
    async with session_factory() as session:
        repo = PlatformRepository(session)
        control = await repo.get_deployment_control()
        preserved_notes = control.notes
        await session.commit()

    assert default_modes["modes"] == {"BTC": "shadow", "ETH": "shadow"}
    assert updated["previous_mode"] == "shadow"
    assert listed["modes"]["BTC"] == "live"
    assert listed["modes"]["ETH"] == "shadow"
    assert listed["counts"]["live"] == 1
    assert preserved_notes["kill_switch"] == {"mode": "manual_or_external"}
    assert preserved_notes["agent_packs"] == {"active_version": "green-pack"}
    assert preserved_notes["crypto_replay_gate"] == {"status": "passed"}
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_blocks_non_live_asset_before_base_execution(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate",
            version="passed-gate",
            status="passed",
            sample_count=1000,
            metrics={},
            payload={"passed": True},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "crypto_asset_live_disabled"
    assert receipt.details["asset_mode"] == "shadow"
    assert fake_base.calls == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_preserves_shadow_status_for_shadow_room(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC shadow crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-shadow-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "shadow_skipped"
    assert receipt.details["asset_mode"] == "shadow"
    assert fake_base.calls == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_execution_live_asset_reaches_base_execution(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    fake_base = _FakeBaseExecution()
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "live", actor="test")
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        control = await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC crypto", market_ticker=_market().market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await repo.record_crypto_model_artifact(
            frequency="15m",
            artifact_type="replay_gate",
            version="passed-gate",
            status="passed",
            sample_count=1000,
            metrics={},
            payload={"passed": True},
            kalshi_env=settings.kalshi_env,
            trained_at=datetime.now(UTC),
        )
        await session.commit()

    receipt = await service.execute(
        room=room,
        control=control,
        ticket=TradeTicket(
            market_ticker=room.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-test",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "submitted"
    assert fake_base.calls[0]["client_order_id"] == "crypto-test:maker"
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_workflow_non_live_asset_receipt_does_not_save_order(tmp_path) -> None:
    settings = _settings(tmp_path, app_shadow_mode=False, crypto_trading_enabled=True)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    market = _market(close_time=datetime.now(UTC) + timedelta(minutes=10))
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    execution = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,
        base_execution_service=_FakeBaseExecution(),  # type: ignore[arg-type]
        asset_control_service=asset_control,
    )
    workflow = CryptoWorkflowService(
        settings=settings,
        session_factory=session_factory,
        market_service=_FakeWorkflowMarketService(market),  # type: ignore[arg-type]
        forecast_service=_FakeForecastService(),  # type: ignore[arg-type]
        risk_engine=_ApproveRiskEngine(),  # type: ignore[arg-type]
        execution_service=execution,
        asset_control_service=asset_control,
    )
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        room = await repo.create_room(
            RoomCreate(name="BTC non-live workflow", market_ticker=market.market_ticker),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.LIVE.value,
        )
        await session.commit()

    await workflow.run_room(room.id, reason="test")

    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        ticket = await repo.get_latest_trade_ticket_for_room(room.id)
        orders = list((await session.execute(select(OrderRecord))).scalars())
        await session.commit()

    assert ticket is not None
    assert ticket.status == "crypto_asset_live_disabled"
    assert orders == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_crypto_autonomy_skips_off_assets_and_existing_rooms(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        app_shadow_mode=True,
        crypto_autonomy_enabled=True,
        crypto_autonomy_min_seconds_to_close=120,
    )
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=session_factory)
    await asset_control.set_asset_mode("BTC", "off", actor="test")
    async with session_factory() as session:
        repo = PlatformRepository(session, kalshi_env=settings.kalshi_env)
        await repo.ensure_deployment_control(
            settings.app_color,
            initial_active_color=settings.app_color,
            initial_kill_switch_enabled=False,
        )
        await repo.create_room(
            RoomCreate(name="ETH duplicate", market_ticker="KXETH15M-TEST"),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=False,
            kalshi_env=settings.kalshi_env,
            room_origin=RoomOrigin.SHADOW.value,
        )
        await session.commit()

    class _FakeKalshi:
        write_credentials = None

    class _FakeMarketService:
        def __init__(self) -> None:
            self.kalshi = _FakeKalshi()
            self.created: list[str] = []
            base = datetime.now(UTC)
            self.markets = [
                _market(market_ticker="KXBTC15M-TEST", asset_symbol="BTC", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXETH15M-TEST", asset_symbol="ETH", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXSOL15M-TEST", asset_symbol="SOL", close_time=base + timedelta(minutes=10)),
                _market(market_ticker="KXADA15M-SOON", asset_symbol="ADA", close_time=base + timedelta(seconds=45)),
                _market(market_ticker="KXADA15M-LATER", asset_symbol="ADA", close_time=base + timedelta(minutes=5)),
            ]

        async def discover_markets(self, **kwargs) -> list[CryptoMarket]:
            return self.markets

        async def create_room_for_market(self, market_ticker: str, *, reason: str) -> dict[str, object]:
            self.created.append(market_ticker)
            market = next(item for item in self.markets if item.market_ticker == market_ticker)
            return {
                "room_id": f"room-{market_ticker}",
                "market_ticker": market_ticker,
                "asset_symbol": market.asset_symbol,
                "asset_mode": "shadow",
                "live_eligible": False,
                "live_blockers": ["shadow"],
            }

    class _FakeWorkflowService:
        def __init__(self) -> None:
            self.ran: list[str] = []

        async def run_room(self, room_id: str, *, reason: str) -> None:
            self.ran.append(room_id)

    market_service = _FakeMarketService()
    workflow_service = _FakeWorkflowService()
    result = await CryptoAutonomyService(
        settings=settings,
        session_factory=session_factory,
        market_service=market_service,  # type: ignore[arg-type]
        asset_control_service=asset_control,
        workflow_service=workflow_service,  # type: ignore[arg-type]
    ).run_once()

    assert result["status"] == "ok"
    assert set(market_service.created) == {"KXADA15M-LATER", "KXSOL15M-TEST"}
    assert set(workflow_service.ran) == {"room-KXADA15M-LATER", "room-KXSOL15M-TEST"}
    assert {item["reason"] for item in result["skipped"]} == {"asset_mode_off", "room_already_exists"}
    assert "KXADA15M-SOON" not in market_service.created
    await engine.dispose()
