"""Add crypto asset refresh indexes for 1h live promotion.

Revision ID: 20260607_0043
Revises: 20260604_0042
Create Date: 2026-06-07
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260607_0043"
down_revision = "20260604_0042"
branch_labels = None
depends_on = None


INDEXES = {
    "ix_crypto_market_snapshots_env_freq_asset_observed": """
        CREATE INDEX CONCURRENTLY ix_crypto_market_snapshots_env_freq_asset_observed
        ON crypto_market_snapshots (kalshi_env, frequency, asset_symbol, observed_at DESC)
    """,
    "ix_crypto_spot_ohlc_env_freq_asset_end": """
        CREATE INDEX CONCURRENTLY ix_crypto_spot_ohlc_env_freq_asset_end
        ON crypto_spot_ohlc (kalshi_env, frequency, asset_symbol, end_ts DESC)
    """,
}


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
    with op.get_context().autocommit_block():
        for index_name, create_sql in INDEXES.items():
            if _index_validity(index_name) is True:
                continue
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
            op.execute(create_sql)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        for index_name in INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
