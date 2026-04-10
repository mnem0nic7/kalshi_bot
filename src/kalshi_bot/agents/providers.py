from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import httpx

from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole


@dataclass(slots=True)
class ProviderConfig:
    base_url: str
    model: str
    api_key: str | None


class OpenAICompatibleProvider:
    def __init__(self, config: ProviderConfig, timeout_seconds: float) -> None:
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={
                "Authorization": f"Bearer {config.api_key}" if config.api_key else "",
                "Content-Type": "application/json",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def complete_text(self, *, system_prompt: str, user_prompt: str) -> str:
        response = await self.client.post(
            f"{self.config.base_url.rstrip('/')}/chat/completions",
            json={
                "model": self.config.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.2,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return payload["choices"][0]["message"]["content"].strip()

    async def complete_json(self, *, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        response = await self.client.post(
            f"{self.config.base_url.rstrip('/')}/chat/completions",
            json={
                "model": self.config.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
            },
        )
        response.raise_for_status()
        payload = response.json()
        return json.loads(payload["choices"][0]["message"]["content"])


class ProviderRouter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.hosted = (
            OpenAICompatibleProvider(
                ProviderConfig(
                    base_url=settings.llm_hosted_base_url,
                    model=settings.llm_hosted_model,
                    api_key=settings.llm_hosted_api_key,
                ),
                timeout_seconds=settings.llm_request_timeout_seconds,
            )
            if settings.llm_hosted_api_key
            else None
        )
        self.local = OpenAICompatibleProvider(
            ProviderConfig(
                base_url=settings.llm_local_base_url,
                model=settings.llm_local_model,
                api_key=settings.llm_local_api_key,
            ),
            timeout_seconds=settings.llm_request_timeout_seconds,
        )

    async def close(self) -> None:
        if self.hosted is not None:
            await self.hosted.close()
        await self.local.close()

    def _provider_for_role(self, role: AgentRole) -> OpenAICompatibleProvider | None:
        if role in (AgentRole.TRADER, AgentRole.PRESIDENT):
            return self.hosted or self.local
        if role in (AgentRole.RESEARCHER, AgentRole.OPS_MONITOR, AgentRole.MEMORY_LIBRARIAN):
            return self.local
        return self.hosted or self.local

    async def maybe_rewrite(self, *, role: AgentRole, fallback_text: str, system_prompt: str, user_prompt: str) -> str:
        provider = self._provider_for_role(role)
        if provider is None:
            return fallback_text
        try:
            text = await provider.complete_text(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception:
            return fallback_text
        return text or fallback_text

    async def maybe_complete_json(
        self,
        *,
        role: AgentRole,
        fallback_payload: dict[str, Any],
        system_prompt: str,
        user_prompt: str,
    ) -> dict[str, Any]:
        provider = self._provider_for_role(role)
        if provider is None:
            return fallback_payload
        try:
            payload = await provider.complete_json(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception:
            return fallback_payload
        return payload or fallback_payload

    def embed_text(self, text: str) -> list[float]:
        dims = self.settings.memory_embedding_dimensions
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values = []
        for index in range(dims):
            byte = digest[index % len(digest)]
            values.append((byte / 255.0) * 2 - 1)
        return values
