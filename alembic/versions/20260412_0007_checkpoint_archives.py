"""add historical checkpoint archive records

Revision ID: 20260412_0007
Revises: 20260412_0006
Create Date: 2026-04-12 00:07:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0007"
down_revision = "20260412_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "historical_checkpoint_archives",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=True),
        sa.Column("station_id", sa.String(length=32), nullable=False),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("checkpoint_label", sa.String(length=32), nullable=False),
        sa.Column("checkpoint_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=255), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=True),
        sa.Column("observation_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("forecast_updated_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archive_path", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("series_ticker", "local_market_day", "checkpoint_label", name="uq_historical_checkpoint_archive_slot"),
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_series_ticker"),
        "historical_checkpoint_archives",
        ["series_ticker"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_market_ticker"),
        "historical_checkpoint_archives",
        ["market_ticker"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_station_id"),
        "historical_checkpoint_archives",
        ["station_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_local_market_day"),
        "historical_checkpoint_archives",
        ["local_market_day"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_checkpoint_label"),
        "historical_checkpoint_archives",
        ["checkpoint_label"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_checkpoint_ts"),
        "historical_checkpoint_archives",
        ["checkpoint_ts"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_captured_at"),
        "historical_checkpoint_archives",
        ["captured_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_source_kind"),
        "historical_checkpoint_archives",
        ["source_kind"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_source_id"),
        "historical_checkpoint_archives",
        ["source_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_source_hash"),
        "historical_checkpoint_archives",
        ["source_hash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_observation_ts"),
        "historical_checkpoint_archives",
        ["observation_ts"],
        unique=False,
    )
    op.create_index(
        op.f("ix_historical_checkpoint_archives_forecast_updated_ts"),
        "historical_checkpoint_archives",
        ["forecast_updated_ts"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_historical_checkpoint_archives_forecast_updated_ts"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_observation_ts"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_source_hash"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_source_id"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_source_kind"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_captured_at"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_checkpoint_ts"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_checkpoint_label"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_local_market_day"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_station_id"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_market_ticker"), table_name="historical_checkpoint_archives")
    op.drop_index(op.f("ix_historical_checkpoint_archives_series_ticker"), table_name="historical_checkpoint_archives")
    op.drop_table("historical_checkpoint_archives")
