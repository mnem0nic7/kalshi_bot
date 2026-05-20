"""Performance indexes for crypto snapshot/candlestick time-range scans.

The nightly model regen's ``status()`` precondition and the settled-snapshot
training loader scan crypto_market_snapshots (~73GB) and crypto_market_candlesticks
filtered by (kalshi_env, frequency) and ordered by time. Without a composite
index the planner falls back to a BitmapAnd over the low-cardinality frequency
index plus a heap sort that spills to disk (~32s for a 1-day window). These
composite indexes give an ordered index scan; the partial index makes the
settled-market lookup an index-only scan over just the settled rows.

Built CONCURRENTLY (online, no write lock) and IF NOT EXISTS so it converges
with any index already created by hand on a live database.

Revision ID: 20260520_0032
Revises: 20260511_0031
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op

revision = "20260520_0032"
down_revision = "20260511_0031"
branch_labels = None
depends_on = None

SNAP_IDX = "ix_crypto_market_snapshots_env_freq_observed"
SETTLED_IDX = "ix_crypto_market_snapshots_settled_lookup"
CANDLE_IDX = "ix_crypto_market_candlesticks_env_freq_endts"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        # SQLite test tier builds the schema via create_all; these are a
        # Postgres-only performance concern.
        return
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {SNAP_IDX} "
            "ON crypto_market_snapshots (kalshi_env, frequency, observed_at)"
        )
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {SETTLED_IDX} "
            "ON crypto_market_snapshots (kalshi_env, frequency, market_ticker) "
            "WHERE settlement_result IN ('yes', 'no')"
        )
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {CANDLE_IDX} "
            "ON crypto_market_candlesticks (kalshi_env, frequency, end_period_ts)"
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {CANDLE_IDX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {SETTLED_IDX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {SNAP_IDX}")
