from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from kalshi_bot.agents.codex_cli import CodexCLIProvider
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentRoleRuntime

logger = logging.getLogger(__name__)


def _is_retryable_llm_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503}
    return False


_llm_retry = retry(
    retry=retry_if_exception(_is_retryable_llm_error),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=True,
    before_sleep=lambda retry_state: logger.warning(
        "LLM provider call failed (attempt %d), retrying: %s",
        retry_state.attempt_number,
        retry_state.outcome.exception(),
    ),
)


@dataclass(slots=True)
class ProviderConfig:
    base_url: str
    model: str
    api_key: str | None


@dataclass(slots=True)
class ProviderUsage:
    provider: str
    model: str | None
    temperature: float
    fallback_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "temperature": self.temperature,
            "fallback_used": self.fallback_used,
        }


class OpenAICompatibleProvider:
    def __init__(self, config: ProviderConfig, timeout_seconds: float) -> None:
        self.config = config
        headers = {"Content-Type": "application/json"}
        if config.api_key:
            headers["Authorization"] = f"Bearer {config.api_key}"
        self.client = httpx.AsyncClient(timeout=timeout_seconds, headers=headers)

    async def close(self) -> None:
        await self.client.aclose()

    @_llm_retry
    async def complete_text(self, *, system_prompt: str, user_prompt: str, model: str, temperature: float) -> str:
        response = await self.client.post(
            f"{self.config.base_url.rstrip('/')}/chat/completions",
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return payload["choices"][0]["message"]["content"].strip()

    @_llm_retry
    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        schema_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        response = await self.client.post(
            f"{self.config.base_url.rstrip('/')}/chat/completions",
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
                "response_format": {"type": "json_object"},
            },
        )
        response.raise_for_status()
        payload = response.json()
        decoded = json.loads(payload["choices"][0]["message"]["content"])
        if schema_model is not None:
            decoded = schema_model.model_validate(decoded).model_dump(mode="json")
        return decoded


class NativeGeminiProvider:
    def __init__(self, config: ProviderConfig, timeout_seconds: float) -> None:
        self.config = config
        self.client = httpx.AsyncClient(timeout=timeout_seconds, headers={"Content-Type": "application/json"})

    async def close(self) -> None:
        await self.client.aclose()

    def _endpoint(self, model: str) -> str:
        if not self.config.api_key:
            raise RuntimeError("Gemini API key not configured")
        return f"{self.config.base_url.rstrip('/')}/models/{model}:generateContent?key={self.config.api_key}"

    @staticmethod
    def _extract_text(payload: dict[str, Any]) -> str:
        candidates = payload.get("candidates") or []
        if not candidates:
            return ""
        parts = candidates[0].get("content", {}).get("parts", [])
        texts = [str(part.get("text", "")) for part in parts if part.get("text")]
        return "\n".join(texts).strip()

    @_llm_retry
    async def complete_text(self, *, system_prompt: str, user_prompt: str, model: str, temperature: float) -> str:
        response = await self.client.post(
            self._endpoint(model),
            json={
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": {"temperature": temperature},
            },
        )
        response.raise_for_status()
        return self._extract_text(response.json())

    @_llm_retry
    async def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        temperature: float,
        schema_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        schema = schema_model.model_json_schema() if schema_model is not None else None
        generation_config: dict[str, Any] = {
            "temperature": temperature,
            "responseMimeType": "application/json",
        }
        if schema is not None:
            generation_config["responseSchema"] = schema
        response = await self.client.post(
            self._endpoint(model),
            json={
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": generation_config,
            },
        )
        response.raise_for_status()
        text = self._extract_text(response.json())
        decoded = json.loads(text or "{}")
        if schema_model is not None:
            decoded = schema_model.model_validate(decoded).model_dump(mode="json")
        return decoded


def build_codex_provider(
    settings: Settings,
    *,
    timeout_seconds: float | None = None,
) -> tuple[CodexCLIProvider | None, str]:
    """Return (provider, description). Only the Codex CLI binary is supported."""
    effective_timeout = timeout_seconds if timeout_seconds is not None else settings.llm_request_timeout_seconds
    if CodexCLIProvider.is_available():
        logger.info("Codex provider: CLI binary (codex exec)")
        return CodexCLIProvider(model=settings.codex_model, timeout_seconds=effective_timeout), "codex-cli"
    return None, "unavailable"


class ProviderRouter:
    GEMINI_ROLE_DEFAULTS = {
        AgentRole.RESEARCHER: "gemini_model_researcher",
        AgentRole.PRESIDENT: "gemini_model_president",
        AgentRole.TRADER: "gemini_model_trader",
        AgentRole.RISK_OFFICER: "gemini_model_risk_officer",
        AgentRole.OPS_MONITOR: "gemini_model_ops_monitor",
        AgentRole.MEMORY_LIBRARIAN: "gemini_model_memory_librarian",
    }

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.gemini = (
            NativeGeminiProvider(
                ProviderConfig(
                    base_url=settings.gemini_base_url,
                    model=settings.gemini_model_researcher,
                    api_key=settings.gemini_api_key,
                ),
                timeout_seconds=settings.llm_request_timeout_seconds,
            )
            if settings.gemini_api_key
            else None
        )
        self.codex, _codex_mode = build_codex_provider(settings)
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
        if self.gemini is not None:
            await self.gemini.close()
        if self.codex is not None:
            await self.codex.close()
        if self.hosted is not None:
            await self.hosted.close()
        await self.local.close()

    def default_role_runtime(self, role: AgentRole) -> AgentRoleRuntime:
        if self.gemini is not None and role in self.GEMINI_ROLE_DEFAULTS:
            default_model = getattr(self.settings, self.GEMINI_ROLE_DEFAULTS[role])
            return AgentRoleRuntime(provider="gemini", model=default_model, temperature=0.2)
        if self.codex is not None:
            return AgentRoleRuntime(provider="codex", model=self.settings.codex_model, temperature=0.2)
        return AgentRoleRuntime(provider="local", model=self.settings.llm_local_model, temperature=0.2)

    def resolve_usage(self, *, role: AgentRole, role_config: AgentRoleRuntime | None = None) -> tuple[Any | None, ProviderUsage]:
        config = role_config or self.default_role_runtime(role)
        requested_provider = config.provider.lower()
        temperature = config.temperature
        if requested_provider == "gemini":
            if self.gemini is not None:
                model = config.model or getattr(self.settings, self.GEMINI_ROLE_DEFAULTS.get(role, "gemini_model_researcher"))
                return self.gemini, ProviderUsage(provider="gemini", model=model, temperature=temperature)
            requested_provider = "codex"
        if requested_provider == "codex":
            if self.codex is not None:
                return self.codex, ProviderUsage(
                    provider="codex",
                    model=config.model or self.settings.codex_model,
                    temperature=temperature,
                    fallback_used=(role_config is not None and role_config.provider.lower() not in {"codex"}),
                )
            requested_provider = "hosted"
        if requested_provider == "hosted":
            if self.hosted is not None:
                return self.hosted, ProviderUsage(
                    provider="hosted",
                    model=config.model or self.settings.llm_hosted_model,
                    temperature=temperature,
                )
            requested_provider = "local"
        if requested_provider == "local":
            return self.local, ProviderUsage(
                provider="local",
                model=config.model or self.settings.llm_local_model,
                temperature=temperature,
                fallback_used=(role_config is not None and role_config.provider.lower() not in {"local"}),
            )
        return None, ProviderUsage(provider="none", model=None, temperature=temperature, fallback_used=True)

    async def rewrite_with_metadata(
        self,
        *,
        role: AgentRole,
        fallback_text: str,
        system_prompt: str,
        user_prompt: str,
        role_config: AgentRoleRuntime | None = None,
    ) -> tuple[str, ProviderUsage]:
        provider, usage = self.resolve_usage(role=role, role_config=role_config)
        if provider is None or usage.model is None:
            usage.fallback_used = True
            return fallback_text, usage
        try:
            text = await provider.complete_text(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=usage.model,
                temperature=usage.temperature,
            )
        except Exception:
            usage.fallback_used = True
            return fallback_text, usage
        return text or fallback_text, usage

    async def maybe_rewrite(
        self,
        *,
        role: AgentRole,
        fallback_text: str,
        system_prompt: str,
        user_prompt: str,
        role_config: AgentRoleRuntime | None = None,
    ) -> str:
        text, _ = await self.rewrite_with_metadata(
            role=role,
            fallback_text=fallback_text,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            role_config=role_config,
        )
        return text

    async def complete_json_with_metadata(
        self,
        *,
        role: AgentRole,
        fallback_payload: dict[str, Any],
        system_prompt: str,
        user_prompt: str,
        role_config: AgentRoleRuntime | None = None,
        schema_model: type[BaseModel] | None = None,
    ) -> tuple[dict[str, Any], ProviderUsage]:
        provider, usage = self.resolve_usage(role=role, role_config=role_config)
        if provider is None or usage.model is None:
            usage.fallback_used = True
            return fallback_payload, usage
        try:
            payload = await provider.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=usage.model,
                temperature=usage.temperature,
                schema_model=schema_model,
            )
        except Exception:
            usage.fallback_used = True
            return fallback_payload, usage
        return payload or fallback_payload, usage

    async def maybe_complete_json(
        self,
        *,
        role: AgentRole,
        fallback_payload: dict[str, Any],
        system_prompt: str,
        user_prompt: str,
        role_config: AgentRoleRuntime | None = None,
        schema_model: type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        payload, _ = await self.complete_json_with_metadata(
            role=role,
            fallback_payload=fallback_payload,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            role_config=role_config,
            schema_model=schema_model,
        )
        return payload

    def embed_text(self, text: str) -> list[float]:
        dims = self.settings.memory_embedding_dimensions
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values = []
        for index in range(dims):
            byte = digest[index % len(digest)]
            values.append((byte / 255.0) * 2 - 1)
        return values
