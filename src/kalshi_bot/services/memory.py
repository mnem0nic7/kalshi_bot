from __future__ import annotations

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import MemoryNotePayload, RoomMessageRead
from kalshi_bot.db.models import Room


class MemoryService:
    def __init__(self, providers: ProviderRouter) -> None:
        self.providers = providers

    async def build_note(self, room: Room, messages: list[RoomMessageRead]) -> MemoryNotePayload:
        last_messages = messages[-4:]
        fallback = (
            f"Room {room.name} on {room.market_ticker} moved through {room.stage} with "
            f"{len(messages)} messages and a final outcome captured in the transcript."
        )
        rewritten = await self.providers.maybe_rewrite(
            role=AgentRole.MEMORY_LIBRARIAN,
            fallback_text=fallback,
            system_prompt="You write concise trading memory notes for future retrieval.",
            user_prompt=f"Summarize this room in 2 sentences.\n\n{fallback}",
        )
        return MemoryNotePayload(
            title=f"{room.market_ticker} {room.stage}",
            summary=rewritten,
            tags=[room.market_ticker, room.stage, room.active_color],
            linked_message_ids=[message.id for message in last_messages],
        )

