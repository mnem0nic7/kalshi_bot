"""add historical pipeline run records

Revision ID: 20260412_0009
Revises: 20260412_0008
Create Date: 2026-04-12 00:09:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0009"
down_revision = "20260412_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "historical_pipeline_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("pipeline_kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("date_from", sa.String(length=16), nullable=False),
        sa.Column("date_to", sa.String(length=16), nullable=False),
        sa.Column("rolling_days", sa.Integer(), nullable=False, server_default="365"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_historical_pipeline_runs_pipeline_kind"), "historical_pipeline_runs", ["pipeline_kind"], unique=False)
    op.create_index(op.f("ix_historical_pipeline_runs_status"), "historical_pipeline_runs", ["status"], unique=False)
    op.create_index(op.f("ix_historical_pipeline_runs_date_from"), "historical_pipeline_runs", ["date_from"], unique=False)
    op.create_index(op.f("ix_historical_pipeline_runs_date_to"), "historical_pipeline_runs", ["date_to"], unique=False)
    op.create_index(op.f("ix_historical_pipeline_runs_started_at"), "historical_pipeline_runs", ["started_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_historical_pipeline_runs_started_at"), table_name="historical_pipeline_runs")
    op.drop_index(op.f("ix_historical_pipeline_runs_date_to"), table_name="historical_pipeline_runs")
    op.drop_index(op.f("ix_historical_pipeline_runs_date_from"), table_name="historical_pipeline_runs")
    op.drop_index(op.f("ix_historical_pipeline_runs_status"), table_name="historical_pipeline_runs")
    op.drop_index(op.f("ix_historical_pipeline_runs_pipeline_kind"), table_name="historical_pipeline_runs")
    op.drop_table("historical_pipeline_runs")
