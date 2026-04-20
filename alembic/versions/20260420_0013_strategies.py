"""Add strategy presets, regression results, and city assignments

Revision ID: 20260420_0013
Revises: 20260420_0012
Create Date: 2026-04-20
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260420_0013"
down_revision = "20260420_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(64), nullable=False, unique=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("thresholds", sa.JSON, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "strategy_results",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("strategy_id", sa.Integer, sa.ForeignKey("strategies.id"), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("date_from", sa.Date, nullable=False),
        sa.Column("date_to", sa.Date, nullable=False),
        sa.Column("series_ticker", sa.String(64), nullable=False),
        sa.Column("rooms_evaluated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("trade_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("win_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_pnl_dollars", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("trade_rate", sa.Numeric(6, 4), nullable=True),
        sa.Column("win_rate", sa.Numeric(6, 4), nullable=True),
        sa.Column("avg_edge_bps", sa.Numeric(8, 2), nullable=True),
    )
    op.create_index("ix_strategy_results_strategy_id", "strategy_results", ["strategy_id"])
    op.create_index("ix_strategy_results_series_ticker", "strategy_results", ["series_ticker"])
    op.create_index("ix_strategy_results_run_at", "strategy_results", ["run_at"])

    op.create_table(
        "city_strategy_assignments",
        sa.Column("series_ticker", sa.String(64), primary_key=True),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("assigned_by", sa.String(64), nullable=False, server_default="auto_regression"),
    )


def downgrade() -> None:
    op.drop_table("city_strategy_assignments")
    op.drop_index("ix_strategy_results_run_at", "strategy_results")
    op.drop_index("ix_strategy_results_series_ticker", "strategy_results")
    op.drop_index("ix_strategy_results_strategy_id", "strategy_results")
    op.drop_table("strategy_results")
    op.drop_table("strategies")
