from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import JSON
from sqlalchemy.types import TypeDecorator


class EmbeddingType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def __init__(self, dimensions: int, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.dimensions = dimensions

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(Vector(self.dimensions))
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Sequence[float] | None, dialect):
        if value is None:
            return None
        return list(value)

    def process_result_value(self, value, dialect):
        return value

