from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, TradeAction
from kalshi_bot.core.schemas import ExecReceiptPayload, TradeTicket
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import (
    CRYPTO_LIVE_QUALITY,
    CryptoAssetControlService,
    CryptoExecutionService,
)
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


class _ShadowControlRepository:
    def __init__(self, session, *, kalshi_env: str) -> None:
        del session
        self.kalshi_env = kalshi_env

    async def get_deployment_control(self, *, kalshi_env: str):
        del kalshi_env
        return SimpleNamespace(notes={"crypto_asset_modes": {"BTC": "shadow"}})


class _FakeBaseExecution:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def execute(self, **kwargs) -> ExecReceiptPayload:
        self.calls.append(kwargs)
        return ExecReceiptPayload(status="submitted", client_order_id=kwargs["client_order_id"])


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/crypto.db",
        "kalshi_env": "production",
        "app_shadow_mode": False,
        "crypto_trading_enabled": True,
        "crypto_order_mode": "taker",
    }
    values.update(overrides)
    return Settings(**values)


def _control(notes: dict[str, object] | None = None, *, app_color: str = "blue") -> SimpleNamespace:
    return SimpleNamespace(
        notes=notes or {},
        kill_switch_enabled=False,
        active_color=app_color,
    )


def _market() -> CryptoMarket:
    return CryptoMarket(
        market_ticker="KXBTC15M-GATE",
        series_ticker="KXBTC15M",
        asset_symbol="BTC",
        frequency="15m",
        status="open",
        close_time=datetime.now(UTC) + timedelta(minutes=10),
        yes_bid_dollars=Decimal("0.4800"),
        yes_ask_dollars=Decimal("0.5000"),
        no_ask_dollars=Decimal("0.5200"),
    )


def _gate(status: str) -> SimpleNamespace:
    return SimpleNamespace(
        status=status,
        version=f"{status}-gate",
        metrics={},
        payload={"passed": status == "passed"},
        artifact_type="replay_gate:BTC",
    )


def _signal() -> StrategySignal:
    signal = StrategySignal(
        fair_yes_dollars=Decimal("0.6500"),
        confidence=0.90,
        edge_bps=1500,
        recommended_action=TradeAction.BUY,
        recommended_side=ContractSide.YES,
        target_yes_price_dollars=Decimal("0.5000"),
        summary="gate resolved asset mode test signal",
    )
    signal.candidate_trace = {
        "candidate_status": CRYPTO_LIVE_QUALITY,
        "trade_selection_model": {"candidate_status": CRYPTO_LIVE_QUALITY},
    }
    return signal


def test_passed_gate_makes_shadow_control_note_live(tmp_path) -> None:
    settings = _settings(tmp_path)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    status = asset_control.market_live_status(
        control=_control({"crypto_asset_modes": {"BTC": "shadow"}}, app_color=settings.app_color),
        replay_gate=_gate("passed"),
        market=_market(),
        has_write_credentials=True,
    )

    assert status["asset_mode"] == "live"
    assert status["asset_mode_source"] == "replay_gate_passed"
    assert status["control_asset_mode"] == "shadow"
    assert status["live_eligible"] is True
    assert status["live_blockers"] == []


def test_failed_gate_makes_live_control_note_shadow(tmp_path) -> None:
    settings = _settings(tmp_path)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    status = asset_control.market_live_status(
        control=_control({"crypto_asset_modes": {"BTC": "live"}}, app_color=settings.app_color),
        replay_gate=_gate("blocked"),
        market=_market(),
        has_write_credentials=True,
    )

    assert status["asset_mode"] == "shadow"
    assert status["asset_mode_source"] == "replay_gate_blocked"
    assert status["control_asset_mode"] == "live"
    assert status["live_eligible"] is False
    assert status["live_blockers"] == ["Crypto replay gate is blocked."]


def test_off_control_note_overrides_passed_gate(tmp_path) -> None:
    settings = _settings(tmp_path)
    asset_control = CryptoAssetControlService(settings=settings, session_factory=None)  # type: ignore[arg-type]

    status = asset_control.market_live_status(
        control=_control({"crypto_asset_modes": {"BTC:15m": "off"}}, app_color=settings.app_color),
        replay_gate=_gate("passed"),
        market=_market(),
        has_write_credentials=True,
    )

    assert status["asset_mode"] == "off"
    assert status["asset_mode_source"] == "control_off"
    assert status["control_asset_mode"] == "off"
    assert status["live_eligible"] is False
    assert status["live_blockers"] == ["Asset BTC mode is off."]


@pytest.mark.asyncio
async def test_executor_uses_passed_gate_instead_of_shadow_control_note(monkeypatch, tmp_path) -> None:
    import kalshi_bot.crypto.services as crypto_services

    async def fake_latest_crypto_artifact_for_asset(*args, **kwargs):
        del args, kwargs
        return _gate("passed")

    monkeypatch.setattr(crypto_services, "PlatformRepository", _ShadowControlRepository)
    monkeypatch.setattr(crypto_services, "_latest_crypto_artifact_for_asset", fake_latest_crypto_artifact_for_asset)
    settings = _settings(tmp_path)
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

    receipt = await service.execute(
        room=SimpleNamespace(kalshi_env="production", shadow_mode=False),
        control=SimpleNamespace(),
        ticket=TradeTicket(
            market_ticker=_market().market_ticker,
            action=TradeAction.BUY,
            side=ContractSide.YES,
            yes_price_dollars=Decimal("0.5000"),
            count_fp=Decimal("1.00"),
        ),
        client_order_id="gate-live",
        fair_yes_dollars=Decimal("0.6500"),
        market=_market(),
        signal=_signal(),
    )

    assert receipt.status == "submitted"
    assert receipt.client_order_id == "gate-live:taker"
    assert fake_base.calls[0]["client_order_id"] == "gate-live:taker"
