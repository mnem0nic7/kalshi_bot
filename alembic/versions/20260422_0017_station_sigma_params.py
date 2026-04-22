"""Add station_sigma_params and global_lead_factor tables for per-station sigma calibration.

Revision ID: 20260422_0017
Revises: 20260421_0016
Create Date: 2026-04-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260422_0017"
down_revision = "20260421_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "station_sigma_params",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("station", sa.String(length=16), nullable=False),
        sa.Column("season_bucket", sa.String(length=4), nullable=False),  # DJF/MAM/JJA/SON
        sa.Column("sigma_base_f", sa.Float(), nullable=False),
        sa.Column("mean_bias_f", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("sigma_se_f", sa.Float(), nullable=False),
        sa.Column("residual_skewness", sa.Float(), nullable=True),
        sa.Column("crps_improvement_vs_global", sa.Float(), nullable=True),
        sa.Column("fitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("station", "season_bucket", "version", name="uq_station_sigma_version"),
    )
    op.create_index(
        "ix_station_sigma_active",
        "station_sigma_params",
        ["station", "season_bucket"],
        unique=False,
        postgresql_where=sa.text("is_active = true"),
    )

    op.create_table(
        "global_lead_factor",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("lead_bucket", sa.String(length=8), nullable=False),  # D-0, D-1, D-2+
        sa.Column("factor", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("fitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lead_bucket", "version", name="uq_lead_factor_version"),
    )
    op.create_index(
        "ix_global_lead_factor_active",
        "global_lead_factor",
        ["lead_bucket"],
        unique=False,
        postgresql_where=sa.text("is_active = true"),
    )


def downgrade() -> None:
    op.drop_index("ix_global_lead_factor_active", table_name="global_lead_factor")
    op.drop_table("global_lead_factor")
    op.drop_index("ix_station_sigma_active", table_name="station_sigma_params")
    op.drop_table("station_sigma_params")
