from __future__ import annotations

import os

import pytest
from pydantic import BaseModel

from kalshi_bot.agents.providers import NativeGeminiProvider, OpenAICompatibleProvider, ProviderConfig
from kalshi_bot.config import Settings

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_EVAL_LAB_LIVE_PROVIDER_SMOKE") != "1",
    reason="Live Evaluation Lab provider smoke tests are opt-in.",
)


class LiveSmokePayload(BaseModel):
    summary: str


@pytest.mark.asyncio
async def test_live_evaluation_lab_gemini_provider_smoke() -> None:
    settings = Settings()
    if not settings.gemini_api_key:
        pytest.skip("GEMINI_API_KEY or GEMINI_KEY is not configured")
    provider = NativeGeminiProvider(
        ProviderConfig(
            base_url=settings.gemini_base_url,
            model=settings.gemini_model_president,
            api_key=settings.gemini_api_key,
        ),
        timeout_seconds=settings.llm_request_timeout_seconds,
    )
    try:
        payload = await provider.complete_json(
            system_prompt="Return only valid JSON matching the requested schema.",
            user_prompt='Return {"summary":"gemini smoke ok"}.',
            model=settings.gemini_model_president,
            temperature=0.0,
            schema_model=LiveSmokePayload,
        )
    finally:
        await provider.close()

    assert payload["summary"]


@pytest.mark.asyncio
async def test_live_evaluation_lab_openai_provider_smoke() -> None:
    settings = Settings()
    if not settings.llm_hosted_api_key:
        pytest.skip("OPENAI_API_KEY or LLM_HOSTED_API_KEY is not configured")
    provider = OpenAICompatibleProvider(
        ProviderConfig(
            base_url=settings.llm_hosted_base_url,
            model=settings.llm_hosted_model,
            api_key=settings.llm_hosted_api_key,
        ),
        timeout_seconds=settings.llm_request_timeout_seconds,
    )
    try:
        payload = await provider.complete_json(
            system_prompt="Return only valid JSON matching the requested schema.",
            user_prompt='Return {"summary":"openai smoke ok"}.',
            model=settings.llm_hosted_model,
            temperature=0.0,
            schema_model=LiveSmokePayload,
        )
    finally:
        await provider.close()

    assert payload["summary"]
