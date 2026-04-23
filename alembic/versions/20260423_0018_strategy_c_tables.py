"""Add Strategy C tables: cli_reconciliation, strategy_c_rooms, cli_station_variance.

Revision ID: 20260423_0018
Revises: 20260422_0017
Create Date: 2026-04-23
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260423_0018"
down_revision = "20260422_0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cli_reconciliation",
        sa.Column("station", sa.String(length=32), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("asos_observed_max", sa.Float(), nullable=False),
        sa.Column("asos_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cli_value", sa.Float(), nullable=False),
        sa.Column("cli_published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delta_degf", sa.Float(), nullable=False),  # cli_value - asos_observed_max
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("station", "observation_date"),
    )
    op.create_index("ix_cli_reconciliation_date", "cli_reconciliation", ["observation_date"])
    op.create_index("ix_cli_reconciliation_delta", "cli_reconciliation", ["delta_degf"])

    op.create_table(
        "strategy_c_rooms",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("ticker", sa.String(length=128), nullable=False),
        sa.Column("station", sa.String(length=32), nullable=False),
        sa.Column("decision_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolution_state", sa.String(length=32), nullable=False),
        sa.Column("observed_max_at_decision", sa.Float(), nullable=False),
        sa.Column("threshold", sa.Float(), nullable=False),
        sa.Column("fair_value_dollars", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("modeled_edge_cents", sa.Float(), nullable=False),
        sa.Column("target_price_cents", sa.Float(), nullable=False),
        sa.Column("contracts_requested", sa.Integer(), nullable=False),
        sa.Column("contracts_filled", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("avg_fill_price_cents", sa.Float(), nullable=True),
        sa.Column("realized_edge_cents", sa.Float(), nullable=True),
        sa.Column("execution_outcome", sa.String(length=32), nullable=False),
        sa.Column("settlement_outcome", sa.String(length=32), nullable=True),
        sa.Column("outcome_pnl_dollars", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("room_id"),
    )
    op.create_index("ix_strategy_c_rooms_ticker", "strategy_c_rooms", ["ticker"])
    op.create_index("ix_strategy_c_rooms_station", "strategy_c_rooms", ["station"])
    op.create_index("ix_strategy_c_rooms_decision_time", "strategy_c_rooms", ["decision_time"])
    op.create_index("ix_strategy_c_rooms_execution_outcome", "strategy_c_rooms", ["execution_outcome"])

    op.create_table(
        "cli_station_variance",
        sa.Column("station", sa.String(length=32), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("signed_mean_delta_degf", sa.Float(), nullable=False),
        sa.Column("signed_stddev_delta_degf", sa.Float(), nullable=False),
        sa.Column("mean_abs_delta_degf", sa.Float(), nullable=False),
        sa.Column("p95_abs_delta_degf", sa.Float(), nullable=False),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("station"),
    )


def downgrade() -> None:
    op.drop_table("cli_station_variance")
    op.drop_index("ix_strategy_c_rooms_execution_outcome", table_name="strategy_c_rooms")
    op.drop_index("ix_strategy_c_rooms_decision_time", table_name="strategy_c_rooms")
    op.drop_index("ix_strategy_c_rooms_station", table_name="strategy_c_rooms")
    op.drop_index("ix_strategy_c_rooms_ticker", table_name="strategy_c_rooms")
    op.drop_table("strategy_c_rooms")
    op.drop_index("ix_cli_reconciliation_delta", table_name="cli_reconciliation")
    op.drop_index("ix_cli_reconciliation_date", table_name="cli_reconciliation")
    op.drop_table("cli_reconciliation")
