from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from kalshi_bot.core.enums import AgentRole
from kalshi_bot.web import room_snapshot


class FakeSession:
    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def commit(self) -> None:
        return None


class FakeAppContainer:
    def session_factory(self) -> FakeSession:
        return FakeSession()


class FakeRoomSnapshotRepo:
    def __init__(self) -> None:
        now = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        self.room = SimpleNamespace(
            id="room-1",
            name="Boston high temp",
            market_ticker="KXHIGHBOS-26APR26-T70",
            room_origin="shadow",
            prompt="Check threshold.",
            kalshi_env="demo",
            stage="researching",
            active_color="blue",
            shadow_mode=True,
            kill_switch_enabled=False,
            agent_pack_version="pack-v1",
            evaluation_run_id=None,
            role_models={},
            created_at=now,
            updated_at=now,
        )
        self.messages = [
            SimpleNamespace(
                id="msg-1",
                room_id="room-1",
                sequence=1,
                role=AgentRole.RESEARCHER,
                kind="note",
                stage="researching",
                content="Research started.",
                payload={},
                created_at=now,
            )
        ]

    async def get_room(self, room_id: str):
        return self.room if room_id == "room-1" else None

    async def list_messages(self, room_id: str):
        return self.messages

    async def get_latest_signal_for_room(self, room_id: str):
        return None

    async def get_latest_trade_ticket_for_room(self, room_id: str):
        return None

    async def get_latest_risk_verdict_for_room(self, room_id: str):
        return None

    async def list_orders_for_room(self, room_id: str):
        return []

    async def list_fills_for_room(self, room_id: str):
        return []

    async def get_latest_memory_note_for_room(self, room_id: str):
        return None

    async def get_room_campaign(self, room_id: str):
        return None

    async def get_room_research_health(self, room_id: str):
        return None

    async def get_room_strategy_audit(self, room_id: str):
        return None

    async def get_latest_artifact(self, *, room_id: str, artifact_type: str):
        return None

    async def list_artifacts(self, *, room_id: str, artifact_type: str, limit: int):
        return []

    async def get_research_dossier(self, market_ticker: str):
        return None

    async def list_research_runs(self, *, market_ticker: str, limit: int):
        return []


@pytest.mark.asyncio
async def test_load_room_snapshot_builds_payload_and_controls_message_inclusion(monkeypatch) -> None:
    fake_repo = FakeRoomSnapshotRepo()
    monkeypatch.setattr(room_snapshot, "PlatformRepository", lambda _session: fake_repo)
    app_container = FakeAppContainer()

    without_messages = await room_snapshot.load_room_snapshot(app_container, "room-1")
    with_messages = await room_snapshot.load_room_snapshot(app_container, "room-1", include_messages=True)

    assert without_messages["room"]["id"] == "room-1"
    assert without_messages["stage_timeline"][1]["stage"] == "researching"
    assert without_messages["analytics"]["decision"]["execution_status"] == "stand_down"
    assert "messages" not in without_messages
    assert with_messages["messages"][0]["role"] == AgentRole.RESEARCHER.value


@pytest.mark.asyncio
async def test_load_room_snapshot_raises_key_error_for_unknown_room(monkeypatch) -> None:
    fake_repo = FakeRoomSnapshotRepo()
    monkeypatch.setattr(room_snapshot, "PlatformRepository", lambda _session: fake_repo)

    with pytest.raises(KeyError):
        await room_snapshot.load_room_snapshot(FakeAppContainer(), "missing-room")
