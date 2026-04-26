from __future__ import annotations

import json
from collections.abc import Awaitable
from typing import Protocol, TypeVar

from fastapi import HTTPException, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ValidationError

ModelT = TypeVar("ModelT", bound=BaseModel)


class ParseJsonModel(Protocol):
    def __call__(
        self,
        request: Request,
        model_cls: type[ModelT],
        *,
        default_on_empty: bool = False,
    ) -> Awaitable[ModelT]: ...


async def parse_json_model(
    request: Request,
    model_cls: type[ModelT],
    *,
    default_on_empty: bool = False,
) -> ModelT:
    raw_body = await request.body()
    if not raw_body.strip():
        if default_on_empty:
            return model_cls()
        raise HTTPException(status_code=400, detail="Request body is required")
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Malformed JSON body") from exc
    try:
        return model_cls.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=jsonable_encoder(exc.errors(include_url=False)),
        ) from exc
