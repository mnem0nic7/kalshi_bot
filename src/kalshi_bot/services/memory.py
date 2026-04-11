from __future__ import annotations

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentPackMemoryConfig, AgentPackRoleConfig, MemoryNotePayload, RoomMessageRead
from kalshi_bot.db.models import Room


class MemoryService:
    def __init__(self, providers: ProviderRouter) -> None:
        self.providers = providers

    @staticmethod
    def _usage_dict(usage) -> dict:
        return usage.to_dict() if hasattr(usage, "to_dict") else dict(usage)

    async def build_note(
        self,
        room: Room,
        messages: list[RoomMessageRead],
        *,
        memory_config: AgentPackMemoryConfig | None = None,
        role_config: AgentPackRoleConfig | None = None,
    ) -> tuple[MemoryNotePayload, dict]:
        last_messages = messages[-4:]
        fallback = (
            f"Room {room.name} on {room.market_ticker} moved through {room.stage} with "
            f"{len(messages)} messages and a final outcome captured in the transcript."
        )
        rewritten, usage = await self.providers.rewrite_with_metadata(
            role=AgentRole.MEMORY_LIBRARIAN,
            fallback_text=fallback,
            system_prompt=(
                role_config.system_prompt
                if role_config is not None
                else (memory_config.system_prompt if memory_config is not None else "You write concise trading memory notes for future retrieval.")
            ),
            user_prompt=f"Summarize this room in {memory_config.max_sentences if memory_config is not None else 2} sentences.\n\n{fallback}",
            role_config=role_config,
        )
        return (
            MemoryNotePayload(
                title=f"{room.market_ticker} {room.stage}",
                summary=rewritten,
                tags=[room.market_ticker, room.stage, room.active_color],
                linked_message_ids=[message.id for message in last_messages],
            ),
            self._usage_dict(usage),
        )
