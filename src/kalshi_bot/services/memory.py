from __future__ import annotations

from typing import Any

from kalshi_bot.core.schemas import AgentPackMemoryConfig, AgentPackRoleConfig, MemoryNotePayload, RoomMessageRead
from kalshi_bot.db.models import Room


class MemoryService:
    def __init__(self, providers: Any | None = None) -> None:
        self.providers = providers

    async def build_note(
        self,
        room: Room,
        messages: list[RoomMessageRead],
        *,
        memory_config: AgentPackMemoryConfig | None = None,
        role_config: AgentPackRoleConfig | None = None,
    ) -> tuple[MemoryNotePayload, dict]:
        last_messages = messages[-4:]
        summary = (
            f"Room {room.name} on {room.market_ticker} moved through {room.stage} with "
            f"{len(messages)} messages and a final outcome captured in the transcript."
        )
        return (
            MemoryNotePayload(
                title=f"{room.market_ticker} {room.stage}",
                summary=summary,
                tags=[room.market_ticker, room.stage, room.active_color],
                linked_message_ids=[message.id for message in last_messages],
            ),
            {"provider": "none", "model": None, "temperature": 0.0, "fallback_used": True},
        )
