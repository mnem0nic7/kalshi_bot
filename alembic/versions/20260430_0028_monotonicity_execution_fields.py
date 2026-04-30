"""Add monotonicity arb execution lifecycle fields.

Revision ID: 20260430_0028
Revises: 20260427_0027
Create Date: 2026-04-30
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260430_0028"
down_revision = "20260427_0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("monotonicity_arb_proposals", sa.Column("pair_id", sa.String(length=64), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("leg1_client_order_id", sa.String(length=128), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("leg2_client_order_id", sa.String(length=128), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("unwind_client_order_id", sa.String(length=128), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("leg1_order_id", sa.String(length=128), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("leg2_order_id", sa.String(length=128), nullable=True))
    op.add_column("monotonicity_arb_proposals", sa.Column("unwind_order_id", sa.String(length=128), nullable=True))
    op.add_column(
        "monotonicity_arb_proposals",
        sa.Column("execution_payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.create_index("ix_monotonicity_arb_proposals_pair_id", "monotonicity_arb_proposals", ["pair_id"])


def downgrade() -> None:
    op.drop_index("ix_monotonicity_arb_proposals_pair_id", table_name="monotonicity_arb_proposals")
    op.drop_column("monotonicity_arb_proposals", "execution_payload")
    op.drop_column("monotonicity_arb_proposals", "unwind_order_id")
    op.drop_column("monotonicity_arb_proposals", "leg2_order_id")
    op.drop_column("monotonicity_arb_proposals", "leg1_order_id")
    op.drop_column("monotonicity_arb_proposals", "unwind_client_order_id")
    op.drop_column("monotonicity_arb_proposals", "leg2_client_order_id")
    op.drop_column("monotonicity_arb_proposals", "leg1_client_order_id")
    op.drop_column("monotonicity_arb_proposals", "pair_id")
