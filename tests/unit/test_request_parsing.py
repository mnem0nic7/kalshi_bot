from __future__ import annotations

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from kalshi_bot.web.request_parsing import parse_json_model


class ExampleRequest(BaseModel):
    name: str
    count: int = 0


class EmptyRequest(BaseModel):
    enabled: bool = True


class FakeRequest:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def body(self) -> bytes:
        return self._body


@pytest.mark.asyncio
async def test_parse_json_model_validates_json_body() -> None:
    payload = await parse_json_model(FakeRequest(b'{"name":"demo","count":3}'), ExampleRequest)

    assert payload.name == "demo"
    assert payload.count == 3


@pytest.mark.asyncio
async def test_parse_json_model_supports_default_on_empty() -> None:
    payload = await parse_json_model(FakeRequest(b""), EmptyRequest, default_on_empty=True)

    assert payload.enabled is True


@pytest.mark.asyncio
async def test_parse_json_model_reports_malformed_body() -> None:
    with pytest.raises(HTTPException) as exc_info:
        await parse_json_model(FakeRequest(b"{"), ExampleRequest)

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Malformed JSON body"
