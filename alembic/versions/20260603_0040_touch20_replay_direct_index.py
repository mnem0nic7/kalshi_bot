"""Add direct live-quote replay index for crypto Touch20.

The non-model Touch20 replay reads settled live quote-path rows ordered by
observed time for a single env/frequency/asset. The existing live quote
training index keys market_ticker before observed_at, which is good for
per-market lookups but slow for bounded recent replay slices.

Revision ID: 20260603_0040
Revises: 20260525_0039
Create Date: 2026-06-03
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260603_0040"
down_revision = "20260525_0039"
branch_labels = None
depends_on = None


INDEX_NAME = "ix_crypto_market_snapshots_touch20_replay_direct"
CREATE_INDEX = f"""
CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME}
ON crypto_market_snapshots (kalshi_env, frequency, asset_symbol, observed_at DESC, market_ticker)
WHERE settlement_result IN ('yes', 'no')
  AND source_kind <> 'settled_backfill'
  AND yes_bid_dollars IS NOT NULL
  AND yes_ask_dollars IS NOT NULL
  AND no_bid_dollars IS NOT NULL
  AND no_ask_dollars IS NOT NULL
"""


def _index_validity(index_name: str) -> bool | None:
    bind = op.get_bind()
    return bind.execute(
        sa.text(
            """
            SELECT i.indisvalid
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = :index_name
            """
        ),
        {"index_name": index_name},
    ).scalar_one_or_none()


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    if _index_validity(INDEX_NAME) is True:
        return
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
        op.execute(CREATE_INDEX)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
