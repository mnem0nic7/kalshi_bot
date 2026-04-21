"""Track scored strategy outcomes and nullable pnl evidence.

Revision ID: 20260421_0014
Revises: 20260420_0013
Create Date: 2026-04-21
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260421_0014"
down_revision = "20260420_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("strategy_results") as batch_op:
        batch_op.add_column(sa.Column("resolved_trade_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("unscored_trade_count", sa.Integer(), nullable=False, server_default="0"))
        batch_op.alter_column(
            "total_pnl_dollars",
            existing_type=sa.Numeric(12, 4),
            nullable=True,
            existing_server_default="0",
            server_default=None,
        )


def downgrade() -> None:
    op.execute(sa.text("UPDATE strategy_results SET total_pnl_dollars = 0 WHERE total_pnl_dollars IS NULL"))
    with op.batch_alter_table("strategy_results") as batch_op:
        batch_op.alter_column(
            "total_pnl_dollars",
            existing_type=sa.Numeric(12, 4),
            nullable=False,
            server_default="0",
        )
        batch_op.drop_column("unscored_trade_count")
        batch_op.drop_column("resolved_trade_count")
