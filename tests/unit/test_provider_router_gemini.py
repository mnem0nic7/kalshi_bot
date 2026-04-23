from __future__ import annotations

import json

import httpx
import pytest
from pydantic import BaseModel

from kalshi_bot.agents.providers import NativeGeminiProvider, OpenAICompatibleProvider, ProviderConfig, ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentRoleRuntime


class SampleProviderPayload(BaseModel):
    summary: str


def _openai_provider_with_transport(handler) -> OpenAICompatibleProvider:
    provider = object.__new__(OpenAICompatibleProvider)
    provider.config = ProviderConfig(base_url="https://api.openai.com/v1", model="gpt-5.4", api_key="openai-key")
    provider.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        timeout=5,
        headers={"Content-Type": "application/json", "Authorization": "Bearer openai-key"},
    )
    return provider


def _gemini_provider_with_transport(handler) -> NativeGeminiProvider:
    provider = object.__new__(NativeGeminiProvider)
    provider.config = ProviderConfig(
        base_url="https://generativelanguage.googleapis.com/v1beta",
        model="gemini-2.5-pro",
        api_key="gemini-key",
    )
    provider.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        timeout=5,
        headers={"Content-Type": "application/json"},
    )
    return provider


@pytest.mark.asyncio
async def test_provider_router_prefers_gemini_when_available() -> None:
    router = ProviderRouter(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            gemini_api_key="test-key",
            llm_local_base_url="http://localhost:11434/v1",
            llm_local_api_key="dummy",
        )
    )
    provider, usage = router.resolve_usage(role=AgentRole.TRADER, role_config=AgentRoleRuntime(provider="gemini", model="gemini-test"))

    assert provider is not None
    assert usage.provider == "gemini"
    assert usage.model == "gemini-test"

    await router.close()


@pytest.mark.asyncio
async def test_provider_router_falls_back_to_local_without_gemini_or_openai() -> None:
    router = ProviderRouter(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            gemini_api_key=None,
            llm_hosted_api_key=None,
            llm_local_base_url="http://localhost:11434/v1",
            llm_local_api_key="dummy",
        )
    )
    provider, usage = router.resolve_usage(role=AgentRole.RESEARCHER, role_config=AgentRoleRuntime(provider="gemini", model="gemini-test"))

    assert provider is not None
    assert usage.provider == "local"
    assert usage.fallback_used is True

    await router.close()


@pytest.mark.asyncio
async def test_provider_router_uses_openai_key_alias_and_never_builds_codex() -> None:
    settings = Settings(
        database_url="sqlite+aiosqlite:///./test.db",
        gemini_api_key=None,
        OPENAI_API_KEY="openai-alias-key",
    )

    assert settings.llm_hosted_api_key == "openai-alias-key"

    router = ProviderRouter(settings)
    provider, usage = router.resolve_usage(
        role=AgentRole.RESEARCHER,
        role_config=AgentRoleRuntime(provider="gemini", model="gemini-test"),
    )

    assert provider is router.hosted
    assert not hasattr(router, "codex")
    assert usage.provider == "hosted"
    assert usage.model == "gemini-test"
    assert usage.fallback_used is True

    codex_provider, codex_usage = router.resolve_usage(
        role=AgentRole.RESEARCHER,
        role_config=AgentRoleRuntime(provider="codex", model="gpt-4o"),
    )

    assert codex_provider is None
    assert codex_usage.provider == "none"
    assert codex_usage.fallback_used is True

    await router.close()


def test_provider_router_uses_gemini_defaults_for_llm_roles() -> None:
    router = ProviderRouter(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            gemini_api_key="test-key",
        )
    )

    runtime = router.default_role_runtime(AgentRole.PRESIDENT)

    assert runtime.provider == "gemini"
    assert runtime.model == "gemini-2.5-pro"


@pytest.mark.asyncio
async def test_openai_provider_sends_chat_completion_json_schema_request() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps({"summary": "ok"})}}]},
        )

    provider = _openai_provider_with_transport(handler)

    payload = await provider.complete_json(
        system_prompt="system",
        user_prompt="user",
        model="gpt-5.4",
        temperature=0.2,
        schema_model=SampleProviderPayload,
    )

    body = captured["body"]
    assert payload == {"summary": "ok"}
    assert captured["url"] == "https://api.openai.com/v1/chat/completions"
    assert captured["headers"]["authorization"] == "Bearer openai-key"
    assert body["model"] == "gpt-5.4"
    assert body["messages"] == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "user"},
    ]
    assert body["response_format"]["type"] == "json_schema"
    assert body["response_format"]["json_schema"]["name"] == "SampleProviderPayload"
    assert body["response_format"]["json_schema"]["schema"]["properties"]["summary"]["type"] == "string"

    await provider.close()


@pytest.mark.asyncio
async def test_gemini_provider_sends_keyed_json_schema_request() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {"text": json.dumps({"summary": "ok"})},
                            ],
                        }
                    }
                ]
            },
        )

    provider = _gemini_provider_with_transport(handler)

    payload = await provider.complete_json(
        system_prompt="system",
        user_prompt="user",
        model="gemini-2.5-pro",
        temperature=0.2,
        schema_model=SampleProviderPayload,
    )

    body = captured["body"]
    assert payload == {"summary": "ok"}
    assert captured["url"] == (
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=gemini-key"
    )
    assert body["systemInstruction"]["parts"] == [{"text": "system"}]
    assert body["contents"] == [{"role": "user", "parts": [{"text": "user"}]}]
    assert body["generationConfig"]["temperature"] == 0.2
    assert body["generationConfig"]["responseMimeType"] == "application/json"
    assert body["generationConfig"]["responseSchema"]["properties"]["summary"]["type"] == "string"

    await provider.close()
