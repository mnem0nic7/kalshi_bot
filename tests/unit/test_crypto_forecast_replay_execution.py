from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import ContractSide, StandDownReason
from kalshi_bot.crypto.models import CryptoMarket
from kalshi_bot.crypto.services import CryptoExecutionService, CryptoForecastService, CryptoReplayService
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models


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
    service = CryptoExecutionService(  # noqa: F841 - documents constructor compatibility.
        settings=_settings(tmp_path),
        session_factory=None,  # type: ignore[arg-type]
        base_execution_service=None,  # type: ignore[arg-type]
    )
    market = _market(yes_bid_dollars=Decimal("0.4700"), yes_ask_dollars=Decimal("0.4900"))

    yes_price = service.passive_yes_price(market, ContractSide.YES)
    no_price = service.passive_yes_price(market, ContractSide.NO)

    assert yes_price == Decimal("0.4701")
    assert no_price == Decimal("0.4899")
