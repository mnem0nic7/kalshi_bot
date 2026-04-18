from __future__ import annotations

import pytest

from kalshi_bot.agents.providers import ProviderRouter
from kalshi_bot.config import Settings
from kalshi_bot.core.enums import AgentRole
from kalshi_bot.core.schemas import AgentRoleRuntime


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
async def test_provider_router_falls_back_to_local_without_gemini() -> None:
    router = ProviderRouter(
        Settings(
            database_url="sqlite+aiosqlite:///./test.db",
            gemini_api_key=None,
            codex_api_key=None,
            codex_auth_json_path="/nonexistent/auth.json",
            llm_local_base_url="http://localhost:11434/v1",
            llm_local_api_key="dummy",
        )
    )
    provider, usage = router.resolve_usage(role=AgentRole.RESEARCHER, role_config=AgentRoleRuntime(provider="gemini", model="gemini-test"))

    assert provider is not None
    assert usage.provider == "local"
    assert usage.fallback_used is True

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
