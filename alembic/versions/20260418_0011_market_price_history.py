"""add market price history table

Revision ID: 20260418_0011
Revises: 20260417_0010
Create Date: 2026-04-18 00:11:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260418_0011"
down_revision = "20260417_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_price_history",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("mid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_trade_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("volume", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_market_price_history_ticker_observed"),
        "market_price_history",
        ["market_ticker", "observed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_market_price_history_ticker_observed"), table_name="market_price_history")
    op.drop_table("market_price_history")
