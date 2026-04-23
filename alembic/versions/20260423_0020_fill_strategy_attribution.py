"""Add strategy_code attribution to trade_tickets, orders, fills.

Enables per-strategy P&L attribution so Strategy A / C / ARB fills can be
segregated in win-rate / Sharpe / calibration queries.

Revision ID: 20260423_0020
Revises: 20260423_0019
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260423_0020"
down_revision = "20260423_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("trade_tickets", sa.Column("strategy_code", sa.String(16), nullable=True))
    op.add_column("orders", sa.Column("strategy_code", sa.String(16), nullable=True))
    op.add_column("fills", sa.Column("strategy_code", sa.String(16), nullable=True))
    op.create_index("ix_orders_strategy_code", "orders", ["strategy_code"])
    op.create_index("ix_fills_strategy_code", "fills", ["strategy_code"])


def downgrade() -> None:
    op.drop_index("ix_fills_strategy_code", "fills")
    op.drop_index("ix_orders_strategy_code", "orders")
    op.drop_column("fills", "strategy_code")
    op.drop_column("orders", "strategy_code")
    op.drop_column("trade_tickets", "strategy_code")
