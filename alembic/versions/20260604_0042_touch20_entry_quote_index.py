"""Add entry quote index for joined Touch20 replay fallback.

Revision ID: 20260604_0042
Revises: 20260604_0041
Create Date: 2026-06-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260604_0042"
down_revision = "20260604_0041"
branch_labels = None
depends_on = None


INDEX_NAME = "ix_crypto_market_snapshots_touch20_entry_quote"
CREATE_INDEX = f"""
CREATE INDEX CONCURRENTLY {INDEX_NAME}
ON crypto_market_snapshots (kalshi_env, frequency, asset_symbol, observed_at DESC, market_ticker)
WHERE source_kind <> 'settled_backfill'
  AND close_time IS NOT NULL
  AND (status IS NULL OR status IN ('open', 'active'))
  AND (
    (
      yes_bid_dollars > 0
      AND yes_bid_dollars < 1
      AND yes_ask_dollars > 0
      AND yes_ask_dollars < 1
      AND yes_ask_dollars >= yes_bid_dollars
    )
    OR (
      no_bid_dollars > 0
      AND no_bid_dollars < 1
      AND no_ask_dollars > 0
      AND no_ask_dollars < 1
      AND no_ask_dollars >= no_bid_dollars
    )
  )
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
