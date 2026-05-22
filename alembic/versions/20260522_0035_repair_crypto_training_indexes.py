"""Repair invalid crypto training covering indexes.

Revision 20260521_0034 introduced the intended covering indexes, but a
concurrent index build can leave an invalid index behind if interrupted. This
follow-up is idempotent: healthy indexes are left alone, while missing or
invalid indexes are rebuilt online.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260522_0035"
down_revision = "20260521_0034"
branch_labels = None
depends_on = None


INDEXES = {
    "ix_crypto_candles_training": (
        "crypto_market_candlesticks",
        "kalshi_env, frequency, end_period_ts, asset_symbol",
    ),
    "ix_crypto_spot_ohlc_training": (
        "crypto_spot_ohlc",
        "kalshi_env, frequency, end_ts, asset_symbol",
    ),
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

    rebuilds = [
        (index_name, table_name, columns)
        for index_name, (table_name, columns) in INDEXES.items()
        if _index_validity(index_name) is not True
    ]
    if not rebuilds:
        return

    with op.get_context().autocommit_block():
        for index_name, table_name, columns in rebuilds:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
            op.execute(f"CREATE INDEX CONCURRENTLY {index_name} ON {table_name} ({columns})")


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        for index_name in INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
