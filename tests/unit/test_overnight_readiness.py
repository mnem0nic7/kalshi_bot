from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings
from kalshi_bot.core.schemas import RoomCreate
from kalshi_bot.crypto.services import CryptoAssetControlService
from kalshi_bot.db.models import (
    Checkpoint,
    CryptoMarketCandlestickRecord,
    CryptoMarketSnapshotRecord,
    CryptoSpotOHLCRecord,
    DeploymentControl,
    Room,
)
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.services.overnight_readiness import OvernightReadinessService, OvernightWindow


NOW = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)


class FakeTradeAnalysisService:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def build_dataset(self, **_kwargs):
        return SimpleNamespace(rows=self.rows, summary={"row_count": len(self.rows)})


class FakeTradingAuditService:
    async def build_report(self, **_kwargs):
        return {
            "counts": {"rooms": 3},
            "execution_funnel": {"filled_count": 1},
            "signal_funnel": {"selected_count": 2},
            "risk": {"approved": 1},
            "issues": [],
        }


async def _settings_and_factory(tmp_path, **settings_kwargs):
    defaults = {
        "database_url": f"sqlite+aiosqlite:///{tmp_path}/overnight-readiness.db",
        "kalshi_env": "production",
        "app_shadow_mode": False,
        "trigger_enable_auto_rooms": True,
        "crypto_trading_enabled": True,
        "crypto_autonomy_enabled": True,
        "trade_behavior_production_entry_freeze_enabled": False,
    }
    defaults.update(settings_kwargs)
    settings = Settings(**defaults)
    engine = create_engine(settings)
    await init_models(engine)
    factory = create_session_factory(engine)
    return settings, factory


async def _seed_control(factory, *, notes: dict | None = None) -> None:
    async with factory() as session:
        session.add(
            DeploymentControl(
                id="production",
                active_color="blue",
                kill_switch_enabled=False,
                notes=notes or {},
            )
        )
        session.add(
            Checkpoint(
                stream_name="daemon_heartbeat:production:blue",
                payload={"heartbeat_at": NOW.isoformat()},
            )
        )
        session.add(
            Checkpoint(
                stream_name="daemon_reconcile:production:blue",
                payload={"reconciled_at": NOW.isoformat()},
            )
        )
        await session.commit()


def test_overnight_window_crosses_midnight_and_uses_pacific_dst() -> None:
    window = OvernightWindow("America/Los_Angeles", start_hour=22, end_hour=6)

    assert window.contains(datetime(2026, 5, 10, 6, 30, tzinfo=UTC)) is True  # 23:30 PDT
    assert window.contains(datetime(2026, 5, 10, 12, 30, tzinfo=UTC)) is True  # 05:30 PDT
    assert window.contains(datetime(2026, 5, 10, 19, 0, tzinfo=UTC)) is False  # 12:00 PDT
    assert window.contains(datetime(2026, 3, 8, 12, 30, tzinfo=UTC)) is True  # 05:30 after spring DST jump
    assert window.contains(datetime(2026, 3, 8, 13, 30, tzinfo=UTC)) is False  # 06:30 PDT


@pytest.mark.asyncio
async def test_weather_readiness_filters_overnight_rows_and_blocks_freeze(tmp_path) -> None:
    settings, factory = await _settings_and_factory(
        tmp_path,
        trade_behavior_production_entry_freeze_enabled=True,
    )
    await _seed_control(factory)
    rows = [
        {
            "decision_ts": "2026-05-10T06:30:00+00:00",
            "training_eligible": True,
            "decision_status": "filled",
            "series_ticker": "KXHIGHAUS",
            "lifecycle_net_pnl_dollars": "1.2500",
            "exclusion_reasons": [],
            "market_snapshot_source_kind": "decision_signal",
        },
        {
            "decision_ts": "2026-05-10T12:30:00+00:00",
            "training_eligible": False,
            "decision_status": "risk_rejected",
            "series_ticker": "KXHIGHAUS",
            "gross_pnl_dollars": None,
            "exclusion_reasons": ["pending_settlement"],
            "market_snapshot_source_kind": "decision_signal",
        },
        {
            "decision_ts": "2026-05-10T19:00:00+00:00",
            "training_eligible": True,
            "decision_status": "filled",
            "series_ticker": "KXHIGHAUS",
            "gross_pnl_dollars": "9.0000",
            "exclusion_reasons": [],
            "market_snapshot_source_kind": "decision_signal",
        },
    ]
    service = OvernightReadinessService(
        settings=settings,
        session_factory=factory,
        trade_analysis_service=FakeTradeAnalysisService(rows),
        trading_audit_service=FakeTradingAuditService(),
        crypto_asset_control_service=CryptoAssetControlService(settings=settings, session_factory=factory),
        has_write_credentials=True,
    )

    report = await service.build_report(
        kalshi_env="production",
        domains="weather",
        weather_analysis_mode="detailed",
        now=NOW,
    )

    assert report["domains"]["weather"]["evidence"]["row_count"] == 2
    assert report["domains"]["weather"]["evidence"]["training_eligible_count"] == 1
    assert report["domains"]["weather"]["evidence"]["pnl"]["scored_count"] == 1
    codes = {issue["code"] for issue in report["hard_blockers"]}
    assert "trade_behavior_production_entry_freeze_enabled" in codes
    assert "weather_auto_rooms_disabled" not in codes


@pytest.mark.asyncio
async def test_weather_fast_readiness_summarizes_overnight_rooms_without_trade_analysis(tmp_path) -> None:
    settings, factory = await _settings_and_factory(tmp_path)
    await _seed_control(factory)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        await repo.create_room(
            room=RoomCreate(
                name="overnight weather",
                market_ticker="KXHIGHAUS-26MAY10-T70",
            ),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="production",
            room_origin="live",
        )
        room = await repo.create_room(
            room=RoomCreate(
                name="day weather",
                market_ticker="KXHIGHAUS-26MAY10-T71",
            ),
            active_color="blue",
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="production",
            room_origin="live",
        )
        room.created_at = datetime(2026, 5, 10, 19, 0, tzinfo=UTC)
        first = (
            await session.execute(
                select(Room).where(Room.market_ticker == "KXHIGHAUS-26MAY10-T70")
            )
        ).scalar_one()
        first.created_at = datetime(2026, 5, 10, 6, 30, tzinfo=UTC)
        await session.commit()

    service = OvernightReadinessService(
        settings=settings,
        session_factory=factory,
        trade_analysis_service=FakeTradeAnalysisService([]),
        trading_audit_service=FakeTradingAuditService(),
        crypto_asset_control_service=CryptoAssetControlService(settings=settings, session_factory=factory),
        has_write_credentials=True,
    )

    report = await service.build_report(kalshi_env="production", domains="weather", now=NOW)

    evidence = report["domains"]["weather"]["evidence"]
    assert evidence["analysis_mode"] == "fast"
    assert evidence["row_count"] == 1
    assert evidence["by_series"] == {"KXHIGHAUS": 1}


@pytest.mark.asyncio
async def test_crypto_readiness_scopes_blockers_to_live_assets(tmp_path) -> None:
    settings, factory = await _settings_and_factory(tmp_path, crypto_production_autonomy_enabled=False)
    await _seed_control(factory, notes={"crypto_asset_modes": {"BTC": "live", "ETH": "shadow"}})
    decision_ts = datetime(2026, 5, 10, 6, 30, tzinfo=UTC)
    close_ts = datetime(2026, 5, 10, 6, 45, tzinfo=UTC)
    async with factory() as session:
        repo = PlatformRepository(session, kalshi_env="production")
        session.add(
            CryptoMarketSnapshotRecord(
                kalshi_env="production",
                series_ticker="KXBTC15M",
                market_ticker="KXBTC15M-OVERNIGHT",
                event_ticker="KXBTC15M-OVERNIGHT",
                asset_symbol="BTC",
                frequency="15m",
                status="closed",
                close_time=close_ts,
                expected_expiration_time=close_ts,
                target_price_dollars=Decimal("70000"),
                yes_bid_dollars=Decimal("0.3000"),
                yes_ask_dollars=Decimal("0.3100"),
                no_ask_dollars=Decimal("0.7000"),
                settlement_result="yes",
                observed_at=decision_ts,
                source_kind="test",
            )
        )
        session.add(
            CryptoMarketCandlestickRecord(
                kalshi_env="production",
                series_ticker="KXBTC15M",
                market_ticker="KXBTC15M-OVERNIGHT",
                asset_symbol="BTC",
                frequency="15m",
                period_interval=1,
                end_period_ts=decision_ts,
                close_dollars=Decimal("0.3050"),
            )
        )
        session.add(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=900,
                end_ts=decision_ts,
                close_dollars=Decimal("70050"),
                observed_at=decision_ts,
            )
        )
        session.add(
            CryptoSpotOHLCRecord(
                kalshi_env="production",
                provider="coinbase",
                asset_symbol="BTC",
                quote_currency="USD",
                frequency="15m",
                interval_seconds=900,
                end_ts=NOW,
                close_dollars=Decimal("70060"),
                observed_at=NOW,
            )
        )
        for artifact_type, version, status in (
            ("model:BTC", "crypto-model-btc-trained", "trained"),
            ("backtest:BTC", "crypto-backtest-btc-pass", "pass"),
            ("replay_gate:BTC", "crypto-gate-btc-passed", "passed"),
            ("replay_gate", "crypto-gate-aggregate-blocked", "blocked"),
        ):
            await repo.record_crypto_model_artifact(
                frequency="15m",
                artifact_type=artifact_type,
                version=version,
                status=status,
                sample_count=100,
                metrics={"trade_candidate_count": 1},
                payload={"global_adjustment_bps": 6000} if artifact_type == "model:BTC" else {},
                kalshi_env="production",
                trained_at=NOW,
            )
        await session.commit()

    service = OvernightReadinessService(
        settings=settings,
        session_factory=factory,
        trade_analysis_service=FakeTradeAnalysisService([]),
        trading_audit_service=FakeTradingAuditService(),
        crypto_asset_control_service=CryptoAssetControlService(settings=settings, session_factory=factory),
        has_write_credentials=True,
    )

    report = await service.build_report(kalshi_env="production", domains="crypto", now=NOW)

    evidence = report["domains"]["crypto"]["evidence"]
    assert evidence["asset_modes"]["BTC"] == "live"
    assert evidence["asset_modes"]["ETH"] == "shadow"
    assert evidence["live_asset_symbols"] == ["BTC"]
    assert evidence["overnight_decision_rows_by_asset"]["BTC"] == 1
    codes = {issue["code"] for issue in report["hard_blockers"]}
    warning_codes = {issue["code"] for issue in report["warnings"]}
    assert "crypto_production_autonomy_not_supported" in warning_codes
    assert "crypto_asset_mode_not_live_eth" in warning_codes
    assert "crypto_aggregate_replay_gate_not_passed" in warning_codes
    assert "crypto_asset_mode_not_live_eth" not in codes
    assert "crypto_replay_gate_not_passed" not in codes
