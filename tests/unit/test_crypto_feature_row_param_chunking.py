"""Regression: bulk_upsert_crypto_training_feature_rows chunked at a fixed 2000
rows. The asyncpg bind-param ceiling is 32767 and a multi-row INSERT uses
rows*columns params, so on the wide crypto-rich-v10 feature schema (~40+ cols)
a 2000-row chunk blew the limit:

    asyncpg ... InterfaceError: the number of query arguments cannot exceed 32767

That crashed the materialize feature-row persist, erroring every asset in the
continuous trainer (no models produced). The effective chunk size must adapt to
the column count so rows*columns stays under the ceiling.
"""
from __future__ import annotations

import pytest

from kalshi_bot.db.repositories import _PG_MAX_BIND_PARAMS, _param_safe_chunk_size


def test_param_safe_chunk_size_caps_by_columns():
    # Wide schema: 2000 rows * 50 cols = 100k params > ceiling -> capped.
    assert _param_safe_chunk_size(2000, 50) == _PG_MAX_BIND_PARAMS // 50
    # The cap must keep rows*cols under the real asyncpg ceiling.
    assert _param_safe_chunk_size(2000, 50) * 50 <= 32767


def test_param_safe_chunk_size_keeps_request_when_narrow():
    # Narrow schema: requested chunk already safe -> unchanged.
    assert _param_safe_chunk_size(2000, 5) == 2000


def test_param_safe_chunk_size_never_below_one():
    # Absurd column count must still yield a usable (>=1) chunk.
    assert _param_safe_chunk_size(2000, 100_000) == 1


def test_pg_max_bind_params_under_asyncpg_ceiling():
    assert _PG_MAX_BIND_PARAMS <= 32767
