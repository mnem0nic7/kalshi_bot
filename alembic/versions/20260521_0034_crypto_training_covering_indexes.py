"""Covering indexes for crypto training data queries.

Training loads three tables per run: settled snapshots, candlesticks, and spot
OHLC. The candlestick query filters on (kalshi_env, frequency, end_period_ts,
asset_symbol) but the closest existing index omits asset_symbol, forcing a
post-index heap filter. The spot OHLC query has the same four-column pattern
but no matching index at all.

These indexes turn both training fetches from index-scan-plus-heap-filter into
a single index-only range scan.
"""

from alembic import op

revision = "20260521_0034"
down_revision = "20260520_0033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_crypto_candles_training
            ON crypto_market_candlesticks (kalshi_env, frequency, end_period_ts, asset_symbol)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_crypto_spot_ohlc_training
            ON crypto_spot_ohlc (kalshi_env, frequency, end_ts, asset_symbol)
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_crypto_candles_training")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_crypto_spot_ohlc_training")
