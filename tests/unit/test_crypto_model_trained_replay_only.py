from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import CRYPTO_LIVE_QUALITY, CryptoAssetControlService, CryptoExecutionService
from kalshi_bot.services.signal import StrategySignal


class _FakeSession:
    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def commit(self) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakePlatformRepository:
    def __init__(self, session, *, kalshi_env: str) -> None:
        self.kalshi_env = kalshi_env

    async def get_deployment_control(self, *, kalshi_env: str):
        return SimpleNamespace(notes={"crypto_asset_modes": {"BTC": "live"}})


class _FakeBaseExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.fixed_calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        return ExecReceiptPayload(status="submitted", client_order_id=kwargs["client_order_id"])

    async def execute_fixed_limit_until_close(self, **kwargs) -> ExecReceiptPayload:
        self.fixed_calls.append(kwargs)
        return ExecReceiptPayload(status="submitted", client_order_id=kwargs["client_order_id"])


def _signal() -> StrategySignal:
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.2000"),
        confidence=0.90,
        edge_bps=-100,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5500"),
        summary="last-minute passive test signal",
    )
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "last_minute_passive_market_confidence": True,
        "last_minute_passive": {
            "bid_threshold_dollars": "0.55",
            "market_side_probability": "0.7000",
        },
    }
    return signal


def _touch20_signal() -> StrategySignal:
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6500"),
        confidence=0.90,
        edge_bps=1600,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.4900"),
        summary="BTC 1h touch20 test signal",
    )
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "objective": "touch_20pct_before_close",
        "touch_strategy": {
            "policy": "btc_1h_touch20",
            "objective": "exitably_up_20pct_before_close",
        },
        "touch_replay_gate": {
            "status": "passed",
            "artifact_type": "replay_gate_touch20:BTC",
        },
        "trade_selection_model": {
            "candidate_status": CRYPTO_LIVE_QUALITY,
            "expected_net_edge": "0.0600",
        },
    }
    return signal


def _btc_1h_market() -> CryptoMarket:
    return CryptoMarket(
        market_ticker="KXBTCD-26JUN011300-85000",
        series_ticker="KXBTCD",
        asset_symbol="BTC",
        frequency="1h",
        status="open",
        close_time=datetime.now(UTC) + timedelta(minutes=45),
        yes_bid_dollars=Decimal("0.4700"),
        yes_ask_dollars=Decimal("0.4900"),
        no_ask_dollars=Decimal("0.5300"),
    )


@pytest.mark.asyncio
async def test_execution_model_trained_replay_only_blocks_last_minute_passive(monkeypatch) -> None:
    import kalshi_bot.crypto.services as crypto_services

    async def fake_latest_crypto_artifact_for_asset(*args, **kwargs):
        return SimpleNamespace(status="passed", version="passed-gate", metrics={})

    monkeypatch.setattr(crypto_services, "PlatformRepository", _FakePlatformRepository)
    monkeypatch.setattr(crypto_services, "_latest_crypto_artifact_for_asset", fake_latest_crypto_artifact_for_asset)
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        app_shadow_mode=False,
        crypto_trading_enabled=True,
    )
    session_factory = _FakeSessionFactory()
    fake_base = _FakeBaseExecution()
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,  # type: ignore[arg-type]
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(  # type: ignore[arg-type]
            settings=settings,
            session_factory=session_factory,
        ),
    )
    market = CryptoMarket(
        market_ticker="KXBTC15M-LAST-MINUTE",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        frequency="15m",
        status="open",
        close_time=datetime.now(UTC) + timedelta(seconds=45),
        yes_bid_dollars=Decimal("0.5000"),
        yes_ask_dollars=Decimal("0.6000"),
    )

    receipt = await service.execute(
        room=SimpleNamespace(kalshi_env="production", shadow_mode=False),
        control=SimpleNamespace(),
        ticket=TradeTicket(
            market_ticker=market.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5500"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-last-minute",
        fair_yes_dollars=Decimal("0.2000"),
        market=market,
        signal=_signal(),
    )

    assert receipt.status == "crypto_candidate_not_live_eligible"
    assert receipt.details["reason"] == "model_trained_replay_only_blocks_last_minute_passive"
    assert fake_base.calls == []
    assert fake_base.fixed_calls == []


@pytest.mark.asyncio
async def test_execution_uses_touch20_gate_for_btc_1h_touch_signal(monkeypatch) -> None:
    import kalshi_bot.crypto.services as crypto_services

    requested_artifact_types: list[str] = []

    async def fake_latest_crypto_artifact_for_asset(*args, **kwargs):
        del args
        artifact_type = kwargs["artifact_type"]
        requested_artifact_types.append(artifact_type)
        if artifact_type == "replay_gate_touch20":
            return SimpleNamespace(
                status="passed",
                version="passed-touch20-gate",
                metrics={},
                payload={"passed": True},
                artifact_type="replay_gate_touch20:BTC",
            )
        return SimpleNamespace(
            status="blocked",
            version="blocked-settlement-gate",
            metrics={},
            payload={"passed": False},
            artifact_type="replay_gate:BTC",
        )

    monkeypatch.setattr(crypto_services, "PlatformRepository", _FakePlatformRepository)
    monkeypatch.setattr(crypto_services, "_latest_crypto_artifact_for_asset", fake_latest_crypto_artifact_for_asset)
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_1h_touch_strategy_enabled=True,
        crypto_order_mode="taker_only",
    )
    session_factory = _FakeSessionFactory()
    fake_base = _FakeBaseExecution()
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,  # type: ignore[arg-type]
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(  # type: ignore[arg-type]
            settings=settings,
            session_factory=session_factory,
        ),
    )

    async def fake_live_pnl_gate(**kwargs):
        del kwargs
        return {"status": "disabled", "blockers": []}

    service._live_pnl_gate = fake_live_pnl_gate  # type: ignore[method-assign]
    market = _btc_1h_market()

    receipt = await service.execute(
        room=SimpleNamespace(kalshi_env="production", shadow_mode=False),
        control=SimpleNamespace(),
        ticket=TradeTicket(
            market_ticker=market.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-touch20",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=_touch20_signal(),
    )

    assert receipt.status == "submitted"
    assert fake_base.calls[0]["client_order_id"] == "crypto-touch20:taker"
    assert requested_artifact_types == ["replay_gate", "replay_gate_touch20"]


@pytest.mark.asyncio
async def test_execution_blocks_btc_1h_touch_signal_when_touch20_gate_blocked(monkeypatch) -> None:
    import kalshi_bot.crypto.services as crypto_services

    async def fake_latest_crypto_artifact_for_asset(*args, **kwargs):
        del args
        artifact_type = kwargs["artifact_type"]
        if artifact_type == "replay_gate_touch20":
            return SimpleNamespace(
                status="blocked",
                version="blocked-touch20-gate",
                metrics={},
                payload={"passed": False},
                artifact_type="replay_gate_touch20:BTC",
            )
        return SimpleNamespace(
            status="passed",
            version="passed-settlement-gate",
            metrics={},
            payload={"passed": True},
            artifact_type="replay_gate:BTC",
        )

    monkeypatch.setattr(crypto_services, "PlatformRepository", _FakePlatformRepository)
    monkeypatch.setattr(crypto_services, "_latest_crypto_artifact_for_asset", fake_latest_crypto_artifact_for_asset)
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        app_shadow_mode=False,
        crypto_trading_enabled=True,
        crypto_1h_touch_strategy_enabled=True,
    )
    session_factory = _FakeSessionFactory()
    fake_base = _FakeBaseExecution()
    service = CryptoExecutionService(
        settings=settings,
        session_factory=session_factory,  # type: ignore[arg-type]
        base_execution_service=fake_base,  # type: ignore[arg-type]
        asset_control_service=CryptoAssetControlService(  # type: ignore[arg-type]
            settings=settings,
            session_factory=session_factory,
        ),
    )
    market = _btc_1h_market()

    receipt = await service.execute(
        room=SimpleNamespace(kalshi_env="production", shadow_mode=False),
        control=SimpleNamespace(),
        ticket=TradeTicket(
            market_ticker=market.market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.4900"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="crypto-touch20-blocked",
        fair_yes_dollars=Decimal("0.6500"),
        market=market,
        signal=_touch20_signal(),
    )

    assert receipt.status == "crypto_replay_gate_blocked"
    assert receipt.details["gate_status"] == "blocked"
    assert receipt.details["gate_artifact_type"] == "replay_gate_touch20:BTC"
    assert receipt.details["gate_runtime_blockers"] == ["BTC 1h touch replay gate is blocked."]
    assert fake_base.calls == []
