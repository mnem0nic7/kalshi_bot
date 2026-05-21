"""Covering index for the crypto asset-discovery DISTINCT scan.

Daemon startup and the live-path/nightly refresh run
``SELECT DISTINCT asset_symbol FROM crypto_market_snapshots
WHERE kalshi_env = ? AND frequency = ? AND observed_at >= ?`` to learn which
assets have recent data. The 20260520_0032 index (kalshi_env, frequency,
observed_at) satisfies the *filter*, but asset_symbol is not in it, so Postgres
fetches ~234k matching rows from the 85GB heap (scattered DataFileRead IO) just
to project one column. With every color/daemon restarting together this becomes
a thundering-herd of full-table reads.

Appending asset_symbol as a trailing key column makes the query an index-only
scan: the (kalshi_env, frequency, observed_at>=X) range is read straight from
the index, asset_symbol included, heap untouched.

Built CONCURRENTLY (online, no write lock) and IF NOT EXISTS so it converges
with any index already created by hand on a live database.

Revision ID: 20260520_0033
Revises: 20260520_0032
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op

revision = "20260520_0033"
down_revision = "20260520_0032"
branch_labels = None
depends_on = None

DISCOVERY_IDX = "ix_crypto_market_snapshots_asset_discovery"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        # SQLite test tier builds the schema via create_all; this is a
        # Postgres-only performance concern.
        return
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {DISCOVERY_IDX} "
            "ON crypto_market_snapshots (kalshi_env, frequency, observed_at, asset_symbol)"
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {DISCOVERY_IDX}")
