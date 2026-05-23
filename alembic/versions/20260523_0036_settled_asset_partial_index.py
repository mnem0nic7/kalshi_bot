"""Add partial index for fast settled-ticker lookup by asset.

Replaces the expensive bitmap-AND plan in list_crypto_settled_market_snapshots
Phase 1 (SELECT DISTINCT market_ticker WHERE settlement_result IN ('yes','no')
AND asset_symbol = ?) with a near-instant partial index scan.
"""

from __future__ import annotations

from alembic import op


revision = "20260523_0036"
down_revision = "20260522_0036"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_crypto_market_snapshots_settled_asset"
TABLE_NAME = "crypto_market_snapshots"


def upgrade() -> None:
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
            f"ON {TABLE_NAME} (kalshi_env, frequency, asset_symbol, market_ticker) "
            "WHERE settlement_result IN ('yes', 'no')"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
