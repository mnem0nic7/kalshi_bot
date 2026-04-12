from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from kalshi_bot.config import Settings, get_settings
from kalshi_bot.core.enums import AgentRole, MessageKind, RoomOrigin, RoomStage
from kalshi_bot.core.schemas import RoomCreate, RoomMessageCreate
from kalshi_bot.db.repositories import PlatformRepository
from kalshi_bot.db.session import create_engine, create_session_factory, init_models
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


def test_historical_settlement_backfill_updates_api_status(tmp_path, monkeypatch) -> None:
    map_path = tmp_path / "markets.yaml"
    output_path = tmp_path / "historical_build.jsonl"
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
            return {"status": "match", "daily_high_f": Decimal("81.00"), "result": "yes"}

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

        training_status = client.get("/api/training/status")
        assert training_status.status_code == 200
        training_body = training_status.json()
        assert training_body["unsettled_complete_room_count"] == 0

    get_settings.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        container = client.app.state.container

        async def seed() -> tuple[str, str]:
            async with container.session_factory() as session:
                repo = PlatformRepository(session)
                control = await repo.get_deployment_control()
                shadow_room = await repo.create_room(
                    RoomCreate(name="Visible Room", market_ticker="WX-SHADOW"),
                    active_color=container.settings.app_color,
                    shadow_mode=True,
                    kill_switch_enabled=control.kill_switch_enabled,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.SHADOW.value,
                )
                historical_room = await repo.create_room(
                    RoomCreate(name="Historical Replay Room", market_ticker="WX-HISTORY"),
                    active_color=container.settings.app_color,
                    shadow_mode=False,
                    kill_switch_enabled=False,
                    kalshi_env=container.settings.kalshi_env,
                    room_origin=RoomOrigin.HISTORICAL_REPLAY.value,
                    agent_pack_version="builtin-gemini-v1",
                    role_models={"researcher": {"provider": "gemini", "model": "gemini-2.5-pro"}},
                )
                await repo.update_room_stage(shadow_room.id, RoomStage.COMPLETE)
                await repo.update_room_stage(historical_room.id, RoomStage.COMPLETE)
                for role, kind, content in (
                    (AgentRole.RESEARCHER, MessageKind.OBSERVATION, "Research trace"),
                    (AgentRole.PRESIDENT, MessageKind.POLICY_MEMO, "Posture trace"),
                    (AgentRole.TRADER, MessageKind.TRADE_IDEA, "Stand down"),
                    (AgentRole.MEMORY_LIBRARIAN, MessageKind.MEMORY_NOTE, "Memory trace"),
                ):
                    await repo.append_message(
                        historical_room.id,
                        RoomMessageCreate(role=role, kind=kind, stage=RoomStage.COMPLETE, content=content, payload={}),
                    )
                await repo.upsert_historical_settlement_label(
                    market_ticker="WX-HISTORY",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    source_kind="kalshi_primary",
                    kalshi_result="yes",
                    settlement_value_dollars=Decimal("1.0000"),
                    settlement_ts=datetime(2026, 4, 10, 23, 0, tzinfo=UTC),
                    crosscheck_status="match",
                    crosscheck_high_f=Decimal("81.00"),
                    crosscheck_result="yes",
                    payload={"market": {"ticker": "WX-HISTORY"}},
                )
                await repo.create_historical_replay_run(
                    room_id=historical_room.id,
                    market_ticker="WX-HISTORY",
                    series_ticker="KXHIGHNY",
                    local_market_day="2026-04-10",
                    checkpoint_label="checkpoint_1",
                    checkpoint_ts=datetime(2026, 4, 10, 13, 0, tzinfo=UTC),
                    status="completed",
                    agent_pack_version="builtin-gemini-v1",
                    payload={
                        "historical_provenance": {
                            "room_origin": RoomOrigin.HISTORICAL_REPLAY.value,
                            "local_market_day": "2026-04-10",
                            "checkpoint_label": "checkpoint_1",
                            "checkpoint_ts": "2026-04-10T13:00:00+00:00",
                            "timezone_name": "America/New_York",
                            "market_snapshot_source_id": "market-snapshot-1",
                            "weather_snapshot_source_id": "weather-snapshot-1",
                            "settlement_label_id": "settlement-1",
                            "source_coverage": {
                                "market_snapshot": True,
                                "weather_snapshot": True,
                                "settlement_label": True,
                            },
                        }
                    },
                )
                await session.commit()
                return shadow_room.id, historical_room.id

        shadow_room_id, historical_room_id = asyncio.run(seed())

        historical_status = client.get("/api/historical/status")
        assert historical_status.status_code == 200
        assert historical_status.json()["replayed_checkpoint_count"] == 1
        assert "full_checkpoint_coverage_count" in historical_status.json()
        assert "draft_training_ready" in historical_status.json()

        status_response = client.get("/api/status")
        assert status_response.status_code == 200
        status_body = status_response.json()
        assert any(room["market_ticker"] == "WX-SHADOW" for room in status_body["rooms"])
        assert all(room["market_ticker"] != "WX-HISTORY" for room in status_body["rooms"])
        assert status_body["training"]["historical"]["replayed_checkpoint_count"] == 1

        index_response = client.get("/")
        assert index_response.status_code == 200
        assert "Historical Corpus" in index_response.text
        assert "Visible Room" in index_response.text
        assert "Historical Replay Room" not in index_response.text

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
        payload = build_response.json()
        assert payload["build"]["room_count"] == 1
        assert payload["build"]["draft_only"] is False
        assert payload["build"]["training_ready"] is True
        lines = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert lines[0]["room_origin"] == "historical_replay"
        assert lines[0]["historical_provenance"]["local_market_day"] == "2026-04-10"
        assert lines[0]["settlement_label"]["crosscheck_status"] == "match"
        assert lines[0]["draft_only"] is False
        assert historical_room_id == lines[0]["room"]["id"]

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

        async def seed() -> str:
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
                await session.commit()
                return stale_room.id

        stale_room_id = asyncio.run(seed())

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
                                        "coverage_class": coverage_class,
                                        "replay_logic_version": container.historical_training_service.replay_logic_version(),
                                        "source_coverage": {
                                            "market_snapshot": True,
                                            "weather_snapshot": True,
                                            "settlement_label": True,
                                        },
                                    }
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
                builds = await repo.list_training_dataset_builds(limit=10, mode_prefix="historical-")
                assert any(build.status == "stale" for build in builds)
                await session.commit()

        asyncio.run(verify_room_deleted())

    get_settings.cache_clear()
