"""Add strategy_promotion_events audit table (P2-3).

One row per shadow→live (or live→shadow) transition, inserted via a CLI
helper rather than DB surgery so the operator's intent and evidence are
preserved alongside the environment change.

Revision ID: 20260423_0021
Revises: 20260423_0020
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260423_0021"
down_revision = "20260423_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_promotion_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("strategy", sa.String(length=16), nullable=False),
        sa.Column("from_state", sa.String(length=32), nullable=False),
        sa.Column("to_state", sa.String(length=32), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("evidence_ref", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("kalshi_env", sa.String(length=16), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_strategy_promotion_events_strategy",
        "strategy_promotion_events",
        ["strategy"],
    )
    op.create_index(
        "ix_strategy_promotion_events_created_at",
        "strategy_promotion_events",
        ["created_at"],
    )
    op.create_index(
        "ix_strategy_promotion_events_kalshi_env",
        "strategy_promotion_events",
        ["kalshi_env"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_strategy_promotion_events_kalshi_env",
        table_name="strategy_promotion_events",
    )
    op.drop_index(
        "ix_strategy_promotion_events_created_at",
        table_name="strategy_promotion_events",
    )
    op.drop_index(
        "ix_strategy_promotion_events_strategy",
        table_name="strategy_promotion_events",
    )
    op.drop_table("strategy_promotion_events")
