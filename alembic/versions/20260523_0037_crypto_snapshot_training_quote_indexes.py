"""Add partial indexes for crypto training snapshot loaders.

The training preflight fetches two disjoint slices from crypto_market_snapshots:
settled_backfill rows for labels and one latest live quote row per settled
market for strict trade-quality examples. On production-sized snapshot tables,
both paths need predicates that match the query shape to avoid broad heap scans.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260523_0037"
down_revision = "20260523_0036"
branch_labels = None
depends_on = None


INDEXES = {
    "ix_crypto_market_snapshots_settled_training": (
        "CREATE INDEX CONCURRENTLY ix_crypto_market_snapshots_settled_training "
        "ON crypto_market_snapshots (kalshi_env, frequency, asset_symbol, observed_at DESC) "
        "WHERE settlement_result IN ('yes', 'no') AND source_kind = 'settled_backfill'"
    ),
    "ix_crypto_market_snapshots_live_quote_training": (
        "CREATE INDEX CONCURRENTLY ix_crypto_market_snapshots_live_quote_training "
        "ON crypto_market_snapshots (kalshi_env, frequency, asset_symbol, market_ticker, observed_at DESC) "
        "WHERE settlement_result IN ('yes', 'no') "
        "AND source_kind <> 'settled_backfill' "
        "AND yes_bid_dollars IS NOT NULL "
        "AND yes_ask_dollars IS NOT NULL"
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

    rebuilds = [index_name for index_name in INDEXES if _index_validity(index_name) is not True]
    if not rebuilds:
        return

    with op.get_context().autocommit_block():
        for index_name in rebuilds:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
            op.execute(INDEXES[index_name])


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        for index_name in reversed(INDEXES):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
