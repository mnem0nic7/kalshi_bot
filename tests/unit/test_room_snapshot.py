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
        self.decision_trace = SimpleNamespace(
            id="trace-1",
            room_id="room-1",
            market_ticker="KXHIGHBOS-26APR26-T70",
            kalshi_env="demo",
            decision_kind="stand_down",
            path_version="deterministic-fast-path.v1",
            input_hash="input-hash",
            trace_hash="trace-hash",
            decision_time=now,
            created_at=now,
        )
        self.signal = None

    async def get_room(self, room_id: str):
        return self.room if room_id == "room-1" else None

    async def list_messages(self, room_id: str):
        return self.messages

    async def get_latest_signal_for_room(self, room_id: str):
        return self.signal

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

    async def get_latest_decision_trace_for_room(self, room_id: str):
        return self.decision_trace if room_id == "room-1" else None

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
    assert without_messages["decision_trace"]["id"] == "trace-1"
    assert "messages" not in without_messages
    assert with_messages["messages"][0]["role"] == AgentRole.RESEARCHER.value


@pytest.mark.asyncio
async def test_room_snapshot_displays_price_floor_stand_down(monkeypatch) -> None:
    fake_repo = FakeRoomSnapshotRepo()
    now = datetime(2026, 5, 9, 19, 35, tzinfo=UTC)
    fake_repo.signal = SimpleNamespace(
        id="signal-1",
        room_id="room-1",
        market_ticker="KXHIGHTSATX-26MAY09-T90",
        fair_yes_dollars="0.4375",
        edge_bps=4025,
        confidence=0.68,
        summary="No taker trade clears the configured edge threshold.",
        payload={
            "stand_down_reason": "no_actionable_edge",
            "candidate_trace": {
                "outcome": "pre_risk_filtered",
                "min_contract_price_dollars": "0.2500",
                "yes": {
                    "side": "yes",
                    "status": "skipped",
                    "reason": "below_min_edge",
                    "traded_price_dollars": "0.8700",
                    "quality_adjusted_edge_bps": -4350,
                },
                "no": {
                    "side": "no",
                    "status": "skipped",
                    "reason": "below_min_contract_price",
                    "traded_price_dollars": "0.1600",
                    "quality_adjusted_edge_bps": 4000,
                },
            },
        },
        created_at=now,
    )
    monkeypatch.setattr(room_snapshot, "PlatformRepository", lambda _session: fake_repo)

    payload = await room_snapshot.load_room_snapshot(FakeAppContainer(), "room-1")

    decision = payload["analytics"]["decision"]
    assert decision["stand_down_reason"] == "no_actionable_edge"
    assert decision["stand_down_display"] == "Price below minimum entry"
    assert "NO had edge (4000bps after buffer)" in decision["stand_down_detail"]
    assert "$0.1600" in decision["stand_down_detail"]
    assert "$0.2500" in decision["stand_down_detail"]


@pytest.mark.asyncio
async def test_room_snapshot_uses_signal_payload_fallbacks_for_shadow_room_analytics(monkeypatch) -> None:
    fake_repo = FakeRoomSnapshotRepo()
    now = datetime(2026, 5, 14, 12, 0, tzinfo=UTC)
    fake_repo.signal = SimpleNamespace(
        id="signal-crypto-1",
        room_id="room-1",
        market_ticker="KXBTC-26MAY14-B95000",
        fair_yes_dollars="0.6200",
        edge_bps=810,
        confidence=0.84,
        summary="Shadow room captured crypto signal context.",
        payload={
            "market_domain": "crypto",
            "frequency": "15m",
            "recommended_side": "yes",
            "research_delta": {"summary": "Fresh signal delta."},
            "market_snapshot": {
                "market": {
                    "ticker": "KXBTC-26MAY14-B95000",
                    "yes_bid_dollars": "0.5800",
                    "yes_ask_dollars": "0.6100",
                    "no_ask_dollars": "0.4200",
                    "last_price_dollars": "0.6000",
                }
            },
            "candidate_trace": {
                "model_version": "crypto-model-v1",
                "features": {
                    "asset": "BTC",
                    "target_price_dollars": "95000.0000",
                    "spread_bps": 300,
                    "mid_yes_dollars": "0.5950",
                },
            },
            "trader_context": {
                "numeric_facts": {
                    "threshold_f": 72,
                    "operator": ">=",
                    "forecast_high_f": 74,
                    "current_temp_f": 68,
                    "station_id": "KBOS",
                    "location_name": "Boston",
                    "forecast_updated_at": "2026-05-14T11:30:00+00:00",
                    "observation_at": "2026-05-14T11:45:00+00:00",
                }
            },
        },
        created_at=now,
    )
    monkeypatch.setattr(room_snapshot, "PlatformRepository", lambda _session: fake_repo)

    payload = await room_snapshot.load_room_snapshot(FakeAppContainer(), "room-1")

    assert payload["research_delta"]["summary"] == "Fresh signal delta."
    assert payload["analytics"]["pricing"]["yes_bid_dollars"] == "0.5800"
    assert payload["analytics"]["crypto"]["asset_symbol"] == "BTC"
    assert payload["analytics"]["crypto"]["target_price_dollars"] == "95000.0000"
    assert payload["analytics"]["crypto"]["spread_bps"] == 300
    assert payload["analytics"]["weather"]["station_id"] == "KBOS"
    assert payload["analytics"]["weather"]["forecast_high_f"] == 74


@pytest.mark.asyncio
async def test_load_room_snapshot_raises_key_error_for_unknown_room(monkeypatch) -> None:
    fake_repo = FakeRoomSnapshotRepo()
    monkeypatch.setattr(room_snapshot, "PlatformRepository", lambda _session: fake_repo)

    with pytest.raises(KeyError):
        await room_snapshot.load_room_snapshot(FakeAppContainer(), "missing-room")
