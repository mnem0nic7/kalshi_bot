"""Add crypto perpetual funding rates table.

Revision ID: 20260516_0032
Revises: 20260511_0031
Create Date: 2026-05-16
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260516_0032"
down_revision = "20260511_0031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crypto_funding_rates",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("asset_symbol", sa.String(length=16), nullable=False),
        sa.Column("quote_currency", sa.String(length=8), nullable=False),
        sa.Column("settlement_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("funding_rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("realized_rate", sa.Numeric(20, 10), nullable=False),
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
            "provider",
            "asset_symbol",
            "settlement_ts",
            name="uq_crypto_funding_rates_period",
        ),
        if_not_exists=True,
    )
    for name, columns in {
        "ix_crypto_funding_rates_provider": ["provider"],
        "ix_crypto_funding_rates_asset_symbol": ["asset_symbol"],
        "ix_crypto_funding_rates_settlement_ts": ["settlement_ts"],
        "ix_crypto_funding_rates_asset_settlement": ["asset_symbol", "settlement_ts"],
    }.items():
        op.create_index(name, "crypto_funding_rates", columns, if_not_exists=True)


def downgrade() -> None:
    op.drop_index("ix_crypto_funding_rates_asset_settlement", table_name="crypto_funding_rates", if_exists=True)
    op.drop_index("ix_crypto_funding_rates_settlement_ts", table_name="crypto_funding_rates", if_exists=True)
    op.drop_index("ix_crypto_funding_rates_asset_symbol", table_name="crypto_funding_rates", if_exists=True)
    op.drop_index("ix_crypto_funding_rates_provider", table_name="crypto_funding_rates", if_exists=True)
    op.drop_table("crypto_funding_rates", if_exists=True)
