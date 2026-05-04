"""Add crypto external spot OHLC storage.

Revision ID: 20260504_0030
Revises: 20260430_0029
Create Date: 2026-05-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260504_0030"
down_revision = "20260430_0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crypto_spot_ohlc",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("quote_currency", sa.String(length=8), nullable=False),
        sa.Column("frequency", sa.String(length=32), nullable=False),
        sa.Column("interval_seconds", sa.Integer(), nullable=False),
        sa.Column("start_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("high_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("low_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("close_dollars", sa.Numeric(18, 8), nullable=True),
        sa.Column("volume", sa.Numeric(24, 8), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "kalshi_env",
            "provider",
            "asset_symbol",
            "quote_currency",
            "interval_seconds",
            "end_ts",
            name="uq_crypto_spot_ohlc_period",
        ),
        if_not_exists=True,
    )
    for name, columns in {
        "ix_crypto_spot_ohlc_kalshi_env": ["kalshi_env"],
        "ix_crypto_spot_ohlc_provider": ["provider"],
        "ix_crypto_spot_ohlc_asset_symbol": ["asset_symbol"],
        "ix_crypto_spot_ohlc_quote_currency": ["quote_currency"],
        "ix_crypto_spot_ohlc_frequency": ["frequency"],
        "ix_crypto_spot_ohlc_start_ts": ["start_ts"],
        "ix_crypto_spot_ohlc_end_ts": ["end_ts"],
        "ix_crypto_spot_ohlc_observed_at": ["observed_at"],
        "ix_crypto_spot_ohlc_source_id": ["source_id"],
        "ix_crypto_spot_ohlc_asset_period": ["asset_symbol", "end_ts"],
        "ix_crypto_spot_ohlc_provider_asset": ["provider", "asset_symbol"],
    }.items():
        op.create_index(name, "crypto_spot_ohlc", columns, if_not_exists=True)


def downgrade() -> None:
    op.drop_index("ix_crypto_spot_ohlc_provider_asset", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_asset_period", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_source_id", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_observed_at", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_end_ts", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_start_ts", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_frequency", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_quote_currency", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_asset_symbol", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_provider", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_index("ix_crypto_spot_ohlc_kalshi_env", table_name="crypto_spot_ohlc", if_exists=True)
    op.drop_table("crypto_spot_ohlc", if_exists=True)
