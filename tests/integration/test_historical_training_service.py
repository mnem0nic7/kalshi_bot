from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from kalshi_bot.config import Settings, get_settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RoomOrigin, RoomStage
from kalshi_bot.core.schemas import RoomCreate, RoomMessageCreate
from kalshi_bot.db.models import OpsEvent
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
from kalshi_bot.integrations.forecast_archive import ForecastArchiveLookupResult
from tests.integration.asgi_sync_client import SameThreadASGITestClient as TestClient
from kalshi_bot.web.app import create_app


@pytest.mark.asyncio
async def test_list_rooms_for_learning_excludes_historical_replay_by_default(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite+aiosqlite:///{tmp_path}/learning.db")
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    await init_models(engine)

    async with session_factory() as session:
        repo = PlatformRepository(session)
        await repo.ensure_deployment_control(settings.app_color)
        shadow_room = await repo.create_room(
            RoomCreate(name="Shadow Room", market_ticker="WX-SHADOW"),
            active_color=settings.app_color,
            shadow_mode=True,
            kill_switch_enabled=True,
            kalshi_env="demo",
            room_origin=RoomOrigin.SHADOW.value,
        )
        live_room = await repo.create_room(
            RoomCreate(name="Live Room", market_ticker="WX-LIVE"),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="demo",
            room_origin=RoomOrigin.LIVE.value,
        )
        historical_room = await repo.create_room(
            RoomCreate(name="Historical Room", market_ticker="WX-HISTORY"),
            active_color=settings.app_color,
            shadow_mode=False,
            kill_switch_enabled=False,
            kalshi_env="demo",
            room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
        )
        await repo.update_room_stage(shadow_room.id, RoomStage.COMPLETE)
        await repo.update_room_stage(live_room.id, RoomStage.COMPLETE)
        await repo.update_room_stage(historical_room.id, RoomStage.COMPLETE)
        await session.commit()

    async with session_factory() as session:
        repo = PlatformRepository(session)
        rooms = await repo.list_rooms_for_learning(
            since=datetime.now(UTC) - timedelta(days=1),
            limit=10,
        )
        await session.commit()

    assert {room.id for room in rooms} == {shadow_room.id, live_room.id}
    await engine.dispose()


def test_historical_status_api_and_build_route(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text("markets: []\n", encoding="utf-8")
    db_path = tmp_path / "historical-api.db"
    output_path = tmp_path / "historical_bundles.jsonl"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        assert "historical_build_readiness" in historical_status.json()

        build_response = client.post(
            "/api/training/historical/build",
            json={
                "mode": "bundles",
                "date_from": "2026-04-10",
                "date_to": "2026-04-10",
                "output": str(output_path),
            },
        )
        assert build_response.status_code == 200
        assert output_path.exists()

    get_settings.cache_clear()


def test_historical_settlement_backfill_updates_api_status(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-backfill-api.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                room = await repo.create_room(
                    RoomCreate(name="Shadow Settlement Gap", market_ticker="KXHIGHNY-26APR10-T68"),
                    active_color=container.settings.app_color,
                    shadow_mode=True,
                    kill_switch_enabled=control.kill_switch_enabled,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.SHADOW.value,
                )
                await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                await session.commit()

        asyncio.run(seed())

        async def fake_fetch_market_for_backfill(market_ticker: str) -> dict:
            return {
                "market": {
                    "ticker": market_ticker,
                    "result": "yes",
                    "settlement_value_dollars": "1.0000",
                    "close_time": "2026-04-10T23:59:59+00:00",
                    "settlement_ts": "2026-04-11T00:30:00+00:00",
                }
            }

        async def fake_daily_summary_crosscheck(mapping, local_day: str, *, kalshi_result: str | None):
            return {
                "status": "match",
                "daily_high_f": Decimal("81.00"),
                "result": "yes",
                "mismatch_reason": None,
            }

        container.historical_training_service._fetch_market_for_backfill = fake_fetch_market_for_backfill  # type: ignore[method-assign]
        container.historical_training_service._daily_summary_crosscheck = fake_daily_summary_crosscheck  # type: ignore[method-assign]

        result = asyncio.run(
            container.historical_training_service.backfill_settlements(
                date_from=datetime(2026, 4, 10, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 10, tzinfo=UTC).date(),
            )
        )
        assert result["backfilled_count"] == 1

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["settlement_backfilled_count"] == 1
        assert "checkpoint_coverage_counts" in body
        assert "historical_build_readiness" in body
        assert "confidence_state" in body

        training_status = client.get("/api/training/status")
        assert training_status.status_code == 200
        training_body = training_status.json()
        assert training_body["unsettled_complete_room_count"] == 0

    get_settings.cache_clear()


def test_historical_settlement_refresh_reclassifies_threshold_edge_mismatches(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHMIA
    display_name: Miami Daily High Temperature
    station_id: KMIA
    daily_summary_station_id: USW00012839
    location_name: Miami
    timezone_name: America/New_York
    latitude: 25.7617
    longitude: -80.1918
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-crosscheck-refresh.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.upsert_historical_settlement_label(
                    market_ticker="KXHIGHMIA-26APR08-T83",
                    series_ticker="KXHIGHMIA",
                    local_market_day="2026-04-08",
                    source_kind="kalshi_primary",
                    kalshi_result="no",
                    settlement_value_dollars=Decimal("0.0000"),
                    settlement_ts=datetime(2026, 4, 9, 0, 30, tzinfo=UTC),
                    crosscheck_status="mismatch",
                    crosscheck_high_f=Decimal("83.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": "KXHIGHMIA-26APR08-T83",
                            "strike_type": "greater",
                            "floor_strike": 83,
                            "close_time": "2026-04-08T23:59:59+00:00",
                            "result": "no",
                        },
                        "crosscheck": {
                            "status": "mismatch",
                            "daily_high_f": "83.00",
                            "result": "yes",
                            "mismatch_reason": "daily_summary_disagreement",
                        },
                    },
                )
                await session.commit()

        asyncio.run(seed())

        async def fake_daily_summary_crosscheck(mapping, local_day: str, *, kalshi_result: str | None):
            return {
                "status": "match",
                "daily_high_f": Decimal("83.00"),
                "result": "no",
                "mismatch_reason": None,
            }

        container.historical_training_service._daily_summary_crosscheck = fake_daily_summary_crosscheck  # type: ignore[method-assign]

        result = asyncio.run(
            container.historical_training_service.backfill_settlements(
                date_from=datetime(2026, 4, 8, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 8, tzinfo=UTC).date(),
                series=["KXHIGHMIA"],
            )
        )

        assert result["settlement_label_refresh_count"] == 1
        assert result["settlement_label_changed_count"] == 1

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["settlement_mismatch_count"] == 0
        assert body["settlement_mismatch_breakdown"]["threshold_edge_strictness"] == 0

    get_settings.cache_clear()


def test_historical_weather_archive_backfill_promotes_recoverable_checkpoint_archives(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-archive-promotion.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                await repo.upsert_historical_settlement_label(
                    market_ticker="KXHIGHNY-26APR10-T68",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=datetime(2026, 4, 11, 0, 30, tzinfo=UTC),
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": "KXHIGHNY-26APR10-T68",
                            "strike_type": "greater",
                            "floor_strike": 68,
                            "close_time": "2026-04-10T23:59:59+00:00",
                            "result": "yes",
                        },
                        "crosscheck": {
                            "status": "match",
                            "daily_high_f": "81.00",
                            "result": "yes",
                            "mismatch_reason": None,
                        },
                    },
                )
                weather_asof = datetime(2026, 4, 10, 12, 55, tzinfo=UTC)
                await repo.upsert_historical_weather_snapshot(
                    station_id="KNYC",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    asof_ts=weather_asof,
                    source_kind="archived_weather_bundle",
                    source_id="weather-source-1",
                    source_hash="weather-hash-1",
                    observation_ts=weather_asof,
                    forecast_updated_ts=weather_asof - timedelta(minutes=20),
                    forecast_high_f=Decimal("81.00"),
                    current_temp_f=Decimal("70.00"),
                    payload={"_archive": {"archive_path": "data/historical_weather/test.json"}},
                )
                await session.commit()

        asyncio.run(seed())

        result = asyncio.run(
            container.historical_training_service.backfill_weather_archives(
                date_from=datetime(2026, 4, 10, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 10, tzinfo=UTC).date(),
                series=["KXHIGHNY"],
            )
        )

        assert result["checkpoint_archive_promotion_count"] >= 2

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["checkpoint_archive_promotion_count"] >= 2
        assert body["coverage_repair_summary"]["checkpoint_archive_promotion_count"] >= 2

    get_settings.cache_clear()


def test_external_forecast_archive_backfill_recovers_full_checkpoint_coverage(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-external-archive.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                ticker = "KXHIGHNY-26APR10-T68"
                local_market_day = "2026-04-10"
                close_ts = datetime(2026, 4, 10, 23, 59, 59, tzinfo=UTC)
                settlement_ts = datetime(2026, 4, 11, 0, 30, tzinfo=UTC)
                await repo.upsert_historical_settlement_label(
                    market_ticker=ticker,
                    series_ticker="KXHIGHNY",
                    local_market_day=local_market_day,
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=settlement_ts,
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": ticker,
                            "strike_type": "greater",
                            "floor_strike": 68,
                            "close_time": close_ts.isoformat(),
                            "result": "yes",
                        }
                    },
                )
                for checkpoint_label, asof_ts in (
                    ("checkpoint_1", datetime(2026, 4, 10, 13, 0, tzinfo=UTC) - timedelta(minutes=5)),
                    ("checkpoint_2", datetime(2026, 4, 10, 17, 0, tzinfo=UTC) - timedelta(minutes=5)),
                    ("checkpoint_3", datetime(2026, 4, 10, 21, 0, tzinfo=UTC) - timedelta(minutes=5)),
                ):
                    await repo.upsert_historical_market_snapshot(
                        market_ticker=ticker,
                        series_ticker="KXHIGHNY",
                        station_id="KNYC",
                        local_market_day=local_market_day,
                        asof_ts=asof_ts,
                        source_kind="captured_market_snapshot",
                        source_id=f"market-{checkpoint_label}",
                        source_hash=f"hash-{checkpoint_label}",
                        close_ts=close_ts,
                        settlement_ts=settlement_ts,
                        yes_bid_dollars=Decimal("0.5100"),
                        yes_ask_dollars=Decimal("0.5500"),
                        no_ask_dollars=Decimal("0.4900"),
                        last_price_dollars=Decimal("0.5300"),
                        payload={
                            "market": {
                                "ticker": ticker,
                                "strike_type": "greater",
                                "floor_strike": 68,
                                "updated_time": asof_ts.isoformat(),
                            }
                        },
                    )
                await session.commit()

        asyncio.run(seed())

        async def fake_fetch(mapping, *, local_market_day: str, checkpoint_ts: datetime, checkpoint_label: str | None = None):
            raw_payload = {
                "timezone": mapping.timezone_name,
                "hourly": {
                    "time": [
                        f"{local_market_day}T09:00",
                        f"{local_market_day}T12:00",
                        f"{local_market_day}T15:00",
                    ],
                        "temperature_2m": [70.0, 77.0, 81.0],
                },
            }
            return ForecastArchiveLookupResult(
                snapshot=container.historical_training_service.forecast_archive_client._normalize_snapshot(
                    mapping,
                    payload=raw_payload,
                    local_market_day=local_market_day,
                    checkpoint_ts=checkpoint_ts,
                    checkpoint_label=checkpoint_label,
                    model="best_match",
                    run_ts=checkpoint_ts - timedelta(hours=1),
                ),
            )

        container.historical_training_service.forecast_archive_client.fetch_point_in_time_forecast_with_diagnostics = fake_fetch  # type: ignore[method-assign]

        result = asyncio.run(
            container.historical_training_service.backfill_external_forecast_archives(
                date_from=datetime(2026, 4, 10, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 10, tzinfo=UTC).date(),
                series=["KXHIGHNY"],
            )
        )

        assert result["checkpoint_archive_promotion_count"] == 3

        historical_status = client.get("/api/historical/status?verbose=true")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["full_checkpoint_coverage_count"] == 1
        assert body["checkpoint_archive_coverage"]["source_counts"]["external_archive_assisted_checkpoint_count"] == 3
        assert body["external_archive_coverage"]["source_counts"]["assisted_checkpoint_count"] == 3
        assert body["external_archive_recovery"]["recovered_via_external_archive_market_day_count"] == 1
        assert body["external_archive_backfill_reason_counts"] == {}
        return


def test_external_forecast_archive_backfill_surfaces_failure_reasons_in_status(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-external-archive-failure.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                ticker = "KXHIGHNY-26APR10-T68"
                await repo.upsert_historical_settlement_label(
                    market_ticker=ticker,
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=datetime(2026, 4, 11, 0, 30, tzinfo=UTC),
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": ticker,
                            "strike_type": "greater",
                            "floor_strike": 68,
                            "close_time": datetime(2026, 4, 10, 23, 59, 59, tzinfo=UTC).isoformat(),
                            "result": "yes",
                        }
                    },
                )
                await session.commit()

        asyncio.run(seed())

        async def fake_failure(mapping, *, local_market_day: str, checkpoint_ts: datetime, checkpoint_label: str | None = None):
            return ForecastArchiveLookupResult(
                snapshot=None,
                failure_reason="request_bad_request",
                reason_counts={"request_bad_request": 1},
                attempts=[{"run_ts": checkpoint_ts.isoformat(), "model": "best_match", "reason": "request_bad_request"}],
            )

        container.historical_training_service.forecast_archive_client.fetch_point_in_time_forecast_with_diagnostics = fake_failure  # type: ignore[method-assign]

        result = asyncio.run(
            container.historical_training_service.backfill_external_forecast_archives(
                date_from=datetime(2026, 4, 10, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 10, tzinfo=UTC).date(),
                series=["KXHIGHNY"],
            )
        )

        assert result["checkpoint_archive_promotion_count"] == 0
        assert result["reason_counts"]["request_bad_request"] == 3

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["external_archive_backfill_reason_counts"]["request_bad_request"] == 3
        assert body["external_archive_last_backfill"]["skipped_unavailable_count"] == 3

def test_historical_gemini_build_becomes_training_ready_with_three_full_days(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    output_dir = tmp_path / "gemini_weather"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-gemini-ready.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                for ticker, local_day, checkpoint_ts in (
                    ("KXHIGHNY-26APR10-T68", "2026-04-10", datetime(2026, 4, 10, 13, 0, tzinfo=UTC)),
                    ("KXHIGHNY-26APR11-T68", "2026-04-11", datetime(2026, 4, 11, 13, 0, tzinfo=UTC)),
                    ("KXHIGHNY-26APR12-T68", "2026-04-12", datetime(2026, 4, 12, 13, 0, tzinfo=UTC)),
                ):
                    room = await repo.create_room(
                        RoomCreate(name=f"Historical Replay {local_day}", market_ticker=ticker),
                        active_color=container.settings.app_color,
                        shadow_mode=False,
                        kill_switch_enabled=False,
                        kalshi_env=container.settings.kalshi_env,
                        room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                        agent_pack_version="builtin-gemini-v1",
                        role_models={"researcher": {"provider": "gemini", "model": "gemini-2.5-pro"}},
                    )
                    await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                    for role, kind, content in (
                        (AgentRole.RESEARCHER, MessageKind.OBSERVATION, f"Research trace {local_day}"),
                        (AgentRole.PRESIDENT, MessageKind.POLICY_MEMO, f"Posture trace {local_day}"),
                        (AgentRole.TRADER, MessageKind.TRADE_IDEA, f"Trade trace {local_day}"),
                        (AgentRole.MEMORY_LIBRARIAN, MessageKind.MEMORY_NOTE, f"Memory trace {local_day}"),
                    ):
                        await repo.append_message(
                            room.id,
                            RoomMessageCreate(role=role, kind=kind, stage=RoomStage.COMPLETE, content=content, payload={}),
                        )
                    await repo.upsert_historical_settlement_label(
                        market_ticker=ticker,
                        series_ticker="KXHIGHNY",
                        local_market_day=local_day,
                        source_kind="kalshi_primary",
                        kalshi_result="yes",
                        settlement_value_dollars=Decimal("1.0000"),
                        settlement_ts=checkpoint_ts + timedelta(hours=10),
                        crosscheck_status="match",
                        crosscheck_high_f=Decimal("81.00"),
                        crosscheck_result="yes",
                        payload={"market": {"ticker": ticker}},
                    )
                    await repo.create_historical_replay_run(
                        room_id=room.id,
                        market_ticker=ticker,
                        series_ticker="KXHIGHNY",
                        local_market_day=local_day,
                        checkpoint_label="checkpoint_1",
                        checkpoint_ts=checkpoint_ts,
                        status="completed",
                        agent_pack_version="builtin-gemini-v1",
                        payload={
                            "historical_provenance": {
                                "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                                "local_market_day": local_day,
                                "checkpoint_label": "checkpoint_1",
                                "checkpoint_ts": checkpoint_ts.isoformat(),
                                "timezone_name": "America/New_York",
                                "market_snapshot_source_id": f"market-snapshot-{local_day}",
                                "weather_snapshot_source_id": f"weather-snapshot-{local_day}",
                                "market_source_kind": "checkpoint_captured_market_snapshot",
                                "weather_source_kind": "archived_weather_bundle",
                                "settlement_label_id": f"settlement-{local_day}",
                                "settlement_crosscheck_status": "match",
                                "coverage_class": "full_checkpoint_coverage",
                                "source_coverage": {
                                    "market_snapshot": True,
                                    "weather_snapshot": True,
                                    "settlement_label": True,
                                },
                            }
                        },
                    )
                await session.commit()

        asyncio.run(seed())

        build_response = client.post(
            "/api/training/historical/build",
            json={
                "mode": "gemini-finetune",
                "date_from": "2026-04-10",
                "date_to": "2026-04-12",
                "series": ["KXHIGHNY"],
                "output": str(output_dir),
            },
        )

        assert build_response.status_code == 200
        payload = build_response.json()
        assert payload["build"]["room_count"] == 3
        assert payload["build"]["draft_only"] is False
        assert payload["build"]["training_ready"] is True

        manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["draft_only"] is False
        assert manifest["training_ready"] is True
        assert len(manifest["split_boundaries"]["train_room_ids"]) == 1
        assert len(manifest["split_boundaries"]["validation_room_ids"]) == 1
        assert len(manifest["split_boundaries"]["holdout_room_ids"]) == 1

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        assert historical_status.json()["historical_build_readiness"]["training_ready"] is True

    get_settings.cache_clear()


def test_historical_status_reports_market_checkpoint_capture_coverage(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-market-checkpoint-status.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                ticker = "KXHIGHNY-26APR11-T68"
                local_market_day = "2026-04-11"
                close_ts = datetime(2026, 4, 11, 23, 59, 59, tzinfo=UTC)
                settlement_ts = datetime(2026, 4, 12, 0, 30, tzinfo=UTC)
                await repo.upsert_historical_settlement_label(
                    market_ticker=ticker,
                    series_ticker="KXHIGHNY",
                    local_market_day=local_market_day,
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=settlement_ts,
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": ticker,
                            "strike_type": "greater",
                            "floor_strike": 68,
                            "close_time": close_ts.isoformat(),
                        }
                    },
                )
                for checkpoint_label, asof_ts in (
                    ("open_0900", datetime(2026, 4, 11, 12, 55, tzinfo=UTC)),
                    ("midday_1300", datetime(2026, 4, 11, 16, 55, tzinfo=UTC)),
                    ("late_1700", datetime(2026, 4, 11, 20, 55, tzinfo=UTC)),
                ):
                    await repo.upsert_historical_market_snapshot(
                        market_ticker=ticker,
                        series_ticker="KXHIGHNY",
                        station_id="KNYC",
                        local_market_day=local_market_day,
                        asof_ts=asof_ts,
                        source_kind="checkpoint_captured_market_snapshot",
                        source_id=f"market-{checkpoint_label}",
                        source_hash=f"hash-market-{checkpoint_label}",
                        close_ts=close_ts,
                        settlement_ts=settlement_ts,
                        yes_bid_dollars=Decimal("0.5100"),
                        yes_ask_dollars=Decimal("0.5500"),
                        no_ask_dollars=Decimal("0.4900"),
                        last_price_dollars=Decimal("0.5300"),
                        payload={
                            "market": {
                                "ticker": ticker,
                                "strike_type": "greater",
                                "floor_strike": 68,
                                "updated_time": asof_ts.isoformat(),
                            }
                        },
                    )
                    await repo.upsert_historical_weather_snapshot(
                        station_id="KNYC",
                        series_ticker="KXHIGHNY",
                        local_market_day=local_market_day,
                        asof_ts=asof_ts,
                        source_kind="archived_weather_bundle",
                        source_id=f"weather-{checkpoint_label}",
                        source_hash=f"hash-weather-{checkpoint_label}",
                        observation_ts=asof_ts,
                        forecast_updated_ts=asof_ts - timedelta(minutes=15),
                        forecast_high_f=Decimal("81.00"),
                        current_temp_f=Decimal("72.00"),
                        payload={"asof_ts": asof_ts.isoformat()},
                    )
                await session.commit()

        asyncio.run(seed())

        historical_status = client.get("/api/historical/status?verbose=true")
        assert historical_status.status_code == 200
        body = historical_status.json()
        assert body["full_checkpoint_coverage_count"] == 1
        assert body["market_checkpoint_capture_coverage"]["checkpoint_coverage_counts"]["full_checkpoint_coverage"] == 1
        assert body["market_checkpoint_coverage_counts"]["full_checkpoint_coverage"] == 1
        assert "market_checkpoint_capture_gaps" in body
        assert "market_checkpoint_market_day_coverage" in body

    get_settings.cache_clear()


def test_historical_repair_refresh_rebuilds_replay_corpus_and_marks_builds_stale(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    stale_output_path = tmp_path / "historical_stale_build.jsonl"
    stale_output_path.write_text('{"status":"old"}\n', encoding="utf-8")
    map_path.write_text(
        """
series_templates:
  - series_ticker: KXHIGHNY
    display_name: NYC Daily High Temperature
    station_id: KNYC
    daily_summary_station_id: USW00094728
    location_name: New York City
    timezone_name: America/New_York
    latitude: 40.7146
    longitude: -74.0071
""".strip()
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "historical-refresh.db"

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("APP_AUTO_INIT_DB", "true")
    monkeypatch.setenv("WEATHER_MARKET_MAP_PATH", str(map_path))
    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> tuple[str, str]:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                stale_room = await repo.create_room(
                    RoomCreate(name="Historical Stale Replay", market_ticker="KXHIGHNY-26APR11-T61"),
                    active_color=container.settings.app_color,
                    shadow_mode=False,
                    kill_switch_enabled=False,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                    agent_pack_version="builtin-gemini-v1",
                )
                await repo.update_room_stage(stale_room.id, RoomStage.COMPLETE)
                close_ts = datetime(2026, 4, 11, 23, 59, 59, tzinfo=UTC)
                settlement_ts = datetime(2026, 4, 12, 0, 30, tzinfo=UTC)
                await repo.upsert_historical_settlement_label(
                    market_ticker="KXHIGHNY-26APR11-T61",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-11",
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=settlement_ts,
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={
                        "market": {
                            "ticker": "KXHIGHNY-26APR11-T61",
                            "strike_type": "greater",
                            "floor_strike": 61,
                            "close_time": close_ts.isoformat(),
                        }
                    },
                )
                for label, asof_ts, yes_bid in (
                    ("open_0900", datetime(2026, 4, 11, 12, 55, tzinfo=UTC), Decimal("0.5100")),
                    ("midday_1300", datetime(2026, 4, 11, 16, 55, tzinfo=UTC), Decimal("0.5400")),
                    ("late_1700", datetime(2026, 4, 11, 20, 55, tzinfo=UTC), Decimal("0.5700")),
                ):
                    await repo.upsert_historical_market_snapshot(
                        market_ticker="KXHIGHNY-26APR11-T61",
                        series_ticker="KXHIGHNY",
                        station_id="KNYC",
                        local_market_day="2026-04-11",
                        asof_ts=asof_ts,
                        source_kind="captured_market_snapshot",
                        source_id=f"market-{label}",
                        source_hash=f"hash-market-{label}",
                        close_ts=close_ts,
                        settlement_ts=settlement_ts,
                        yes_bid_dollars=yes_bid,
                        yes_ask_dollars=yes_bid + Decimal("0.0400"),
                        no_ask_dollars=Decimal("1.0000") - yes_bid,
                        last_price_dollars=yes_bid + Decimal("0.0100"),
                        payload={
                            "market": {
                                "ticker": "KXHIGHNY-26APR11-T61",
                                "strike_type": "greater",
                                "floor_strike": 61,
                                "updated_time": asof_ts.isoformat(),
                            }
                        },
                    )
                    await repo.upsert_historical_weather_snapshot(
                        station_id="KNYC",
                        series_ticker="KXHIGHNY",
                        local_market_day="2026-04-11",
                        asof_ts=asof_ts,
                        source_kind="archived_weather_bundle",
                        source_id=f"weather-{label}",
                        source_hash=f"hash-weather-{label}",
                        observation_ts=asof_ts,
                        forecast_updated_ts=asof_ts - timedelta(minutes=15),
                        forecast_high_f=Decimal("81.00"),
                        current_temp_f=Decimal("72.00"),
                        payload={"asof_ts": asof_ts.isoformat()},
                    )
                await repo.create_historical_replay_run(
                    room_id=stale_room.id,
                    market_ticker="KXHIGHNY-26APR11-T61",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-11",
                    checkpoint_label="late_1700",
                    checkpoint_ts=datetime(2026, 4, 11, 21, 0, tzinfo=UTC),
                    status="completed",
                    agent_pack_version="builtin-gemini-v1",
                    payload={
                        "historical_provenance": {
                            "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                            "local_market_day": "2026-04-11",
                            "checkpoint_label": "late_1700",
                            "checkpoint_ts": "2026-04-11T21:00:00+00:00",
                            "timezone_name": "America/New_York",
                            "market_snapshot_source_id": "stale-market-source",
                            "weather_snapshot_source_id": "weather-late_1700",
                            "market_source_kind": "captured_market_snapshot",
                            "weather_source_kind": "archived_weather_bundle",
                            "settlement_label_id": "settlement-1",
                            "coverage_class": "late_only_coverage",
                            "replay_logic_version": "historical_replay_old_logic",
                            "source_coverage": {
                                "market_snapshot": True,
                                "weather_snapshot": True,
                                "settlement_label": True,
                            },
                        }
                    },
                )
                build = await repo.create_training_dataset_build(
                    build_version="historical-bundles-test-refresh",
                    mode="historical-bundles",
                    status="completed",
                    selection_window_start=datetime(2026, 4, 11, 13, 0, tzinfo=UTC),
                    selection_window_end=datetime(2026, 4, 11, 21, 0, tzinfo=UTC),
                    room_count=1,
                    filters={"date_from": "2026-04-11", "date_to": "2026-04-11"},
                    label_stats={},
                    pack_versions=["builtin-gemini-v1"],
                    payload={"room_ids": [stale_room.id], "output": str(stale_output_path)},
                    completed_at=datetime(2026, 4, 12, 0, 0, tzinfo=UTC),
                )
                await repo.set_training_dataset_build_items(
                    dataset_build_id=build.id,
                    items=[{"room_id": stale_room.id, "sequence": 1}],
                )
                decision_build = await repo.create_decision_corpus_build(
                    version="decision-corpus-refresh-fixture",
                    date_from=datetime(2026, 4, 11, tzinfo=UTC).date(),
                    date_to=datetime(2026, 4, 11, tzinfo=UTC).date(),
                    source={
                        "type": "historical_replay_rooms",
                        "kalshi_env": container.settings.kalshi_env,
                    },
                    filters={
                        "date_from": "2026-04-11",
                        "date_to": "2026-04-11",
                        "source": "historical-replay",
                    },
                )
                await repo.add_decision_corpus_row(
                    corpus_build_id=decision_build.id,
                    room_id=stale_room.id,
                    market_ticker="KXHIGHNY-26APR11-T61",
                    series_ticker="KXHIGHNY",
                    station_id="KNYC",
                    local_market_day="2026-04-11",
                    checkpoint_ts=datetime(2026, 4, 11, 21, 0, tzinfo=UTC),
                    kalshi_env=container.settings.kalshi_env,
                    deployment_color=control.active_color,
                    model_version="builtin-gemini-v1",
                    policy_version="builtin-gemini-v1",
                    source_asof_ts=datetime(2026, 4, 11, 20, 55, tzinfo=UTC),
                    quote_observed_at=datetime(2026, 4, 11, 20, 55, tzinfo=UTC),
                    quote_captured_at=datetime(2026, 4, 11, 20, 55, tzinfo=UTC),
                    time_to_settlement_at_checkpoint_minutes=180,
                    fair_yes_dollars=Decimal("0.5500"),
                    confidence=0.82,
                    edge_bps=700,
                    recommended_side="yes",
                    target_yes_price_dollars=Decimal("0.6000"),
                    eligibility_status="eligible",
                    trade_regime="standard",
                    liquidity_regime="tight",
                    support_status="exploratory",
                    support_level="L5_global",
                    support_n=30,
                    support_market_days=10,
                    support_recency_days=1,
                    backoff_path=[],
                    settlement_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    pnl_counterfactual_target_frictionless=Decimal("0.400000"),
                    pnl_counterfactual_target_with_fees=Decimal("0.383200"),
                    pnl_model_fair_frictionless=Decimal("0.450000"),
                    fee_counterfactual_dollars=Decimal("0.016800"),
                    counterfactual_count=Decimal("1.00"),
                    fee_model_version="fixture",
                    source_provenance="historical_replay_late_only",
                    source_details={
                        "coverage_class": "late_only_coverage",
                        "market_source_kind": "captured_market_snapshot",
                        "weather_source_kind": "archived_weather_bundle",
                    },
                    signal_payload={},
                    quote_snapshot={},
                    settlement_payload={},
                    diagnostics={},
                )
                await repo.mark_decision_corpus_build_successful(decision_build.id, row_count=1)
                await repo.promote_decision_corpus_build(
                    decision_build.id,
                    kalshi_env=container.settings.kalshi_env,
                    actor="test",
                )
                await session.commit()
                return stale_room.id, decision_build.id

        stale_room_id, decision_build_id = asyncio.run(seed())

        status_before = client.get("/api/historical/status?verbose=true")
        assert status_before.status_code == 200
        before_payload = status_before.json()
        assert before_payload["source_replay_coverage"]["full_checkpoint_coverage_count"] == 1
        assert before_payload["checkpoint_archive_coverage"]["checkpoint_coverage_counts"]["full_checkpoint_coverage"] == 0
        assert before_payload["replay_corpus"]["coverage_class_counts"]["late_only_coverage"] == 1
        assert before_payload["refresh_needed"] is True

        async def fake_replay_weather_history(*, date_from, date_to, series=None):
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                labels = await repo.list_historical_settlement_labels(
                    series_tickers=series or None,
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    limit=100,
                )
                control = await repo.get_deployment_control()
                created = 0
                for label in labels:
                    mapping = container.historical_training_service._mapping_for_market(
                        label.market_ticker,
                        (label.payload or {}).get("market"),
                    )
                    assert mapping is not None
                    selections = await container.historical_training_service._resolve_market_day_selections(
                        repo,
                        label=label,
                        mapping=mapping,
                    )
                    coverage_class = container.historical_training_service._coverage_class(selections)
                    for selection in selections:
                        if not selection.replayable:
                            continue
                        room = await repo.create_room(
                            RoomCreate(
                                name=f"Historical Refresh {selection.checkpoint_label}",
                                market_ticker=label.market_ticker,
                            ),
                            active_color=control.active_color,
                            shadow_mode=False,
                            kill_switch_enabled=False,
                            kalshi_env=container.settings.kalshi_env,
                            room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                            agent_pack_version="builtin-gemini-v1",
                        )
                        await repo.update_room_stage(room.id, RoomStage.COMPLETE)
                        await repo.create_historical_replay_run(
                            room_id=room.id,
                            market_ticker=label.market_ticker,
                            series_ticker=label.series_ticker,
                            local_market_day=label.local_market_day,
                            checkpoint_label=selection.checkpoint_label,
                            checkpoint_ts=selection.checkpoint_ts,
                            status="completed",
                            agent_pack_version="builtin-gemini-v1",
                            payload={
                                "historical_provenance": {
                                    "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                                    "local_market_day": label.local_market_day,
                                    "checkpoint_label": selection.checkpoint_label,
                                    "checkpoint_ts": selection.checkpoint_ts.isoformat(),
                                    "timezone_name": "America/New_York",
                                    "market_snapshot_source_id": selection.market_snapshot.id,
                                    "weather_snapshot_source_id": selection.weather_snapshot.id,
                                    "market_source_kind": selection.market_source_kind,
                                    "weather_source_kind": selection.weather_source_kind,
                                    "settlement_label_id": label.id,
                                    "settlement_crosscheck_status": label.crosscheck_status,
                                    "settlement_mismatch_reason": container.historical_training_service._crosscheck_mismatch_reason_from_label(label),
                                    "settlement_label_signature": container.historical_training_service._settlement_label_signature(label),
                                    "coverage_class": coverage_class,
                                    "replay_logic_version": container.historical_training_service.replay_logic_version(),
                                    "source_coverage": {
                                        "market_snapshot": True,
                                        "weather_snapshot": True,
                                        "settlement_label": True,
                                    },
                                },
                            },
                        )
                        created += 1
                await session.commit()
            return {
                "status": "completed",
                "created_room_count": created,
                "replayed_market_day_count": 1,
                "skipped_existing_count": 0,
                "missing_reason_counts": {},
                "samples": [],
            }

        container.historical_training_service.replay_weather_history = fake_replay_weather_history  # type: ignore[method-assign]

        refresh = asyncio.run(
            container.historical_training_service.refresh_historical_replay(
                date_from=datetime(2026, 4, 11, tzinfo=UTC).date(),
                date_to=datetime(2026, 4, 11, tzinfo=UTC).date(),
                series=["KXHIGHNY"],
            )
        )
        assert refresh["deleted_room_count"] == 1
        assert refresh["stale_build_count"] >= 1
        assert refresh["stale_decision_corpus_build_count"] == 1
        assert refresh["stale_decision_corpus_build_ids"] == [decision_build_id]
        assert refresh["replay"]["created_room_count"] == 3
        assert stale_output_path.exists() is True

        status_after = client.get("/api/historical/status?verbose=true")
        assert status_after.status_code == 200
        after_payload = status_after.json()
        assert after_payload["refresh_needed"] is False
        assert after_payload["replay_corpus"]["coverage_class_counts"]["full_checkpoint_coverage"] == 1
        assert after_payload["replay_corpus"]["coverage_class_counts"]["late_only_coverage"] == 0
        assert after_payload["stale_build_count"] >= 1
        assert after_payload["replayed_checkpoint_count"] == 3

        async def verify_room_deleted() -> None:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                assert await repo.get_room(stale_room_id) is None
                assert await repo.get_current_decision_corpus_build(kalshi_env=container.settings.kalshi_env) is None
                decision_build = await repo.get_decision_corpus_build(decision_build_id)
                assert decision_build is not None
                assert decision_build.status == "stale"
                assert decision_build.failure_reason == "historical_replay_refresh"
                builds = await repo.list_training_dataset_builds(limit=10, mode_prefix="historical-")
                assert any(build.status == "stale" for build in builds)
                events = list(
                    (
                        await session.execute(
                            select(OpsEvent).where(OpsEvent.source == "decision_corpus")
                        )
                    ).scalars()
                )
                events = [
                    event
                    for event in events
                    if (event.payload or {}).get("event_kind") == "decision_corpus_builds_marked_stale"
                ]
                assert events
                assert events[0].payload["build_ids"] == [decision_build_id]
                await session.commit()

        asyncio.run(verify_room_deleted())

    get_settings.cache_clear()
