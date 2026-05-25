"""Add decision-context lineage columns to fills.

Persists the decision-time edge/confidence/spread/fair-value/price/timestamp onto
each fill so win/loss can be analyzed per bucket and slippage (fill price vs
decision price) can be measured. All columns are nullable + additive; population
happens only in the reconcile loop.

Revision ID: 20260525_0039
Revises: 20260525_0038
Create Date: 2026-05-25
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260525_0039"
down_revision = "20260525_0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("fills", sa.Column("decision_edge_bps", sa.Integer(), nullable=True))
    op.add_column("fills", sa.Column("decision_confidence", sa.Float(), nullable=True))
    op.add_column("fills", sa.Column("decision_spread_bps", sa.Integer(), nullable=True))
    op.add_column("fills", sa.Column("decision_fair_yes", sa.Numeric(10, 4), nullable=True))
    op.add_column("fills", sa.Column("decision_price", sa.Numeric(10, 4), nullable=True))
    op.add_column("fills", sa.Column("decision_ts", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_fills_decision_ts", "fills", ["decision_ts"])


def downgrade() -> None:
    op.drop_index("ix_fills_decision_ts", table_name="fills")
    op.drop_column("fills", "decision_ts")
    op.drop_column("fills", "decision_price")
    op.drop_column("fills", "decision_fair_yes")
    op.drop_column("fills", "decision_spread_bps")
    op.drop_column("fills", "decision_confidence")
    op.drop_column("fills", "decision_edge_bps")
