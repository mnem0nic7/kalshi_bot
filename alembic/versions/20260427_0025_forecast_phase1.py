"""Add forecast snapshot and climatology prior tables.

Revision ID: 20260427_0025
Revises: 20260427_0024
Create Date: 2026-04-27
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_0025"
down_revision = "20260427_0024"
branch_labels = None
depends_on = None


def _jsonb() -> sa.TypeEngine:
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "forecast_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("kalshi_env", sa.String(length=16), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("parameter_pack_version", sa.String(length=128), nullable=True),
        sa.Column("source_members", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("fused_pdf", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("probability_output", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("source_set_used", _jsonb(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_forecast_snapshots_fetched_at", "forecast_snapshots", ["fetched_at"])
    op.create_index("ix_forecast_snapshots_kalshi_env", "forecast_snapshots", ["kalshi_env"])
    op.create_index("ix_forecast_snapshots_market_env_fetched", "forecast_snapshots", ["market_ticker", "kalshi_env", "fetched_at"])
    op.create_index("ix_forecast_snapshots_market_ticker", "forecast_snapshots", ["market_ticker"])
    op.create_index("ix_forecast_snapshots_parameter_pack_version", "forecast_snapshots", ["parameter_pack_version"])

    op.create_table(
        "climatology_priors",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("station_id", sa.String(length=32), nullable=False),
        sa.Column("series_ticker", sa.String(length=64), nullable=True),
        sa.Column("day_of_year", sa.Integer(), nullable=False),
        sa.Column("bucket_low_f", sa.Float(), nullable=True),
        sa.Column("bucket_high_f", sa.Float(), nullable=True),
        sa.Column("p_yes", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("normal_window_years", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("smoothing_days", sa.Integer(), nullable=False, server_default="14"),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="historical_archive"),
        sa.Column("payload", _jsonb(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_climatology_priors_day_of_year", "climatology_priors", ["day_of_year"])
    op.create_index("ix_climatology_priors_series_day", "climatology_priors", ["series_ticker", "day_of_year"])
    op.create_index("ix_climatology_priors_series_ticker", "climatology_priors", ["series_ticker"])
    op.create_index("ix_climatology_priors_station_day", "climatology_priors", ["station_id", "day_of_year"])
    op.create_index("ix_climatology_priors_station_id", "climatology_priors", ["station_id"])


def downgrade() -> None:
    op.drop_index("ix_climatology_priors_station_id", table_name="climatology_priors")
    op.drop_index("ix_climatology_priors_station_day", table_name="climatology_priors")
    op.drop_index("ix_climatology_priors_series_ticker", table_name="climatology_priors")
    op.drop_index("ix_climatology_priors_series_day", table_name="climatology_priors")
    op.drop_index("ix_climatology_priors_day_of_year", table_name="climatology_priors")
    op.drop_table("climatology_priors")
    op.drop_index("ix_forecast_snapshots_parameter_pack_version", table_name="forecast_snapshots")
    op.drop_index("ix_forecast_snapshots_market_ticker", table_name="forecast_snapshots")
    op.drop_index("ix_forecast_snapshots_market_env_fetched", table_name="forecast_snapshots")
    op.drop_index("ix_forecast_snapshots_kalshi_env", table_name="forecast_snapshots")
    op.drop_index("ix_forecast_snapshots_fetched_at", table_name="forecast_snapshots")
    op.drop_table("forecast_snapshots")
