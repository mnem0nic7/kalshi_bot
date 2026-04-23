"""Add monotonicity_arb_proposals table (Addition 3, §4.3).

Revision ID: 20260423_0019
Revises: 20260423_0018
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260423_0019"
down_revision = "20260423_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monotonicity_arb_proposals",
        sa.Column("proposal_id", sa.String(length=36), nullable=False),
        sa.Column("station", sa.String(length=32), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("ticker_low", sa.String(length=128), nullable=False),
        sa.Column("ticker_high", sa.String(length=128), nullable=False),
        sa.Column("threshold_low_f", sa.Float(), nullable=False),
        sa.Column("threshold_high_f", sa.Float(), nullable=False),
        sa.Column("ask_yes_low_cents", sa.Float(), nullable=False),
        sa.Column("ask_no_high_cents", sa.Float(), nullable=False),
        sa.Column("total_cost_cents", sa.Float(), nullable=False),
        sa.Column("gross_edge_cents", sa.Float(), nullable=False),
        sa.Column("fee_estimate_cents", sa.Float(), nullable=False),
        sa.Column("net_edge_cents", sa.Float(), nullable=False),
        sa.Column("contracts_proposed", sa.Integer(), nullable=False),
        sa.Column("execution_outcome", sa.String(length=32), nullable=False),
        sa.Column("suppression_reason", sa.String(length=256), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("proposal_id"),
    )
    op.create_index("ix_monotonicity_arb_proposals_station", "monotonicity_arb_proposals", ["station"])
    op.create_index("ix_monotonicity_arb_proposals_event_date", "monotonicity_arb_proposals", ["event_date"])
    op.create_index("ix_monotonicity_arb_proposals_execution_outcome", "monotonicity_arb_proposals", ["execution_outcome"])
    op.create_index("ix_monotonicity_arb_proposals_detected_at", "monotonicity_arb_proposals", ["detected_at"])


def downgrade() -> None:
    op.drop_index("ix_monotonicity_arb_proposals_detected_at", "monotonicity_arb_proposals")
    op.drop_index("ix_monotonicity_arb_proposals_execution_outcome", "monotonicity_arb_proposals")
    op.drop_index("ix_monotonicity_arb_proposals_event_date", "monotonicity_arb_proposals")
    op.drop_index("ix_monotonicity_arb_proposals_station", "monotonicity_arb_proposals")
    op.drop_table("monotonicity_arb_proposals")
