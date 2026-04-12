"""add historical replay training tables

Revision ID: 20260412_0006
Revises: 20260412_0005
Create Date: 2026-04-12 00:06:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0006"
down_revision = "20260412_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "rooms",
        sa.Column("room_origin", sa.String(length=32), server_default="shadow", nullable=False),
    )
    op.create_index(op.f("ix_rooms_room_origin"), "rooms", ["room_origin"], unique=False)
    op.execute("UPDATE rooms SET room_origin = 'live' WHERE shadow_mode IS FALSE")
    op.execute("UPDATE rooms SET room_origin = 'shadow' WHERE shadow_mode IS TRUE OR room_origin IS NULL")

    op.create_table(
        "historical_import_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("import_kind", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_historical_import_runs_import_kind"), "historical_import_runs", ["import_kind"], unique=False)
    op.create_index(op.f("ix_historical_import_runs_source"), "historical_import_runs", ["source"], unique=False)
    op.create_index(op.f("ix_historical_import_runs_status"), "historical_import_runs", ["status"], unique=False)
    op.create_index(op.f("ix_historical_import_runs_started_at"), "historical_import_runs", ["started_at"], unique=False)
    op.create_index(op.f("ix_historical_import_runs_finished_at"), "historical_import_runs", ["finished_at"], unique=False)

    op.create_table(
        "historical_market_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=True),
        sa.Column("station_id", sa.String(length=64), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=False),
        sa.Column("source_hash", sa.String(length=128), nullable=False),
        sa.Column("close_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("settlement_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("yes_bid_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("yes_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("no_ask_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("last_price_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("market_ticker", "source_kind", "source_id", name="uq_historical_market_snapshot_source"),
    )
    op.create_index(op.f("ix_historical_market_snapshots_market_ticker"), "historical_market_snapshots", ["market_ticker"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_series_ticker"), "historical_market_snapshots", ["series_ticker"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_station_id"), "historical_market_snapshots", ["station_id"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_local_market_day"), "historical_market_snapshots", ["local_market_day"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_asof_ts"), "historical_market_snapshots", ["asof_ts"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_source_kind"), "historical_market_snapshots", ["source_kind"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_source_id"), "historical_market_snapshots", ["source_id"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_source_hash"), "historical_market_snapshots", ["source_hash"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_close_ts"), "historical_market_snapshots", ["close_ts"], unique=False)
    op.create_index(op.f("ix_historical_market_snapshots_settlement_ts"), "historical_market_snapshots", ["settlement_ts"], unique=False)

    op.create_table(
        "historical_weather_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("station_id", sa.String(length=64), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=False),
        sa.Column("source_hash", sa.String(length=128), nullable=False),
        sa.Column("observation_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("forecast_updated_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("forecast_high_f", sa.Numeric(10, 2), nullable=True),
        sa.Column("current_temp_f", sa.Numeric(10, 2), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("station_id", "source_kind", "source_id", name="uq_historical_weather_snapshot_source"),
    )
    op.create_index(op.f("ix_historical_weather_snapshots_station_id"), "historical_weather_snapshots", ["station_id"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_series_ticker"), "historical_weather_snapshots", ["series_ticker"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_local_market_day"), "historical_weather_snapshots", ["local_market_day"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_asof_ts"), "historical_weather_snapshots", ["asof_ts"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_source_kind"), "historical_weather_snapshots", ["source_kind"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_source_id"), "historical_weather_snapshots", ["source_id"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_source_hash"), "historical_weather_snapshots", ["source_hash"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_observation_ts"), "historical_weather_snapshots", ["observation_ts"], unique=False)
    op.create_index(op.f("ix_historical_weather_snapshots_forecast_updated_ts"), "historical_weather_snapshots", ["forecast_updated_ts"], unique=False)

    op.create_table(
        "historical_settlement_labels",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("source_kind", sa.String(length=64), nullable=False),
        sa.Column("kalshi_result", sa.String(length=16), nullable=True),
        sa.Column("settlement_value_dollars", sa.Numeric(10, 4), nullable=True),
        sa.Column("settlement_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("crosscheck_status", sa.String(length=32), nullable=False),
        sa.Column("crosscheck_high_f", sa.Numeric(10, 2), nullable=True),
        sa.Column("crosscheck_result", sa.String(length=16), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("market_ticker", name="uq_historical_settlement_labels_market_ticker"),
    )
    op.create_index(op.f("ix_historical_settlement_labels_market_ticker"), "historical_settlement_labels", ["market_ticker"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_series_ticker"), "historical_settlement_labels", ["series_ticker"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_local_market_day"), "historical_settlement_labels", ["local_market_day"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_source_kind"), "historical_settlement_labels", ["source_kind"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_kalshi_result"), "historical_settlement_labels", ["kalshi_result"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_settlement_ts"), "historical_settlement_labels", ["settlement_ts"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_crosscheck_status"), "historical_settlement_labels", ["crosscheck_status"], unique=False)
    op.create_index(op.f("ix_historical_settlement_labels_crosscheck_result"), "historical_settlement_labels", ["crosscheck_result"], unique=False)

    op.create_table(
        "historical_replay_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("room_id", sa.String(length=36), nullable=True),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("series_ticker", sa.String(length=128), nullable=True),
        sa.Column("local_market_day", sa.String(length=16), nullable=False),
        sa.Column("checkpoint_label", sa.String(length=32), nullable=False),
        sa.Column("checkpoint_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("agent_pack_version", sa.String(length=128), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("room_id", name="uq_historical_replay_runs_room"),
        sa.UniqueConstraint("market_ticker", "checkpoint_ts", name="uq_historical_replay_runs_checkpoint"),
    )
    op.create_index(op.f("ix_historical_replay_runs_room_id"), "historical_replay_runs", ["room_id"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_market_ticker"), "historical_replay_runs", ["market_ticker"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_series_ticker"), "historical_replay_runs", ["series_ticker"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_local_market_day"), "historical_replay_runs", ["local_market_day"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_checkpoint_ts"), "historical_replay_runs", ["checkpoint_ts"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_status"), "historical_replay_runs", ["status"], unique=False)
    op.create_index(op.f("ix_historical_replay_runs_agent_pack_version"), "historical_replay_runs", ["agent_pack_version"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_historical_replay_runs_agent_pack_version"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_status"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_checkpoint_ts"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_local_market_day"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_series_ticker"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_market_ticker"), table_name="historical_replay_runs")
    op.drop_index(op.f("ix_historical_replay_runs_room_id"), table_name="historical_replay_runs")
    op.drop_table("historical_replay_runs")

    op.drop_index(op.f("ix_historical_settlement_labels_crosscheck_result"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_crosscheck_status"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_settlement_ts"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_kalshi_result"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_source_kind"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_local_market_day"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_series_ticker"), table_name="historical_settlement_labels")
    op.drop_index(op.f("ix_historical_settlement_labels_market_ticker"), table_name="historical_settlement_labels")
    op.drop_table("historical_settlement_labels")

    op.drop_index(op.f("ix_historical_weather_snapshots_forecast_updated_ts"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_observation_ts"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_source_hash"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_source_id"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_source_kind"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_asof_ts"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_local_market_day"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_series_ticker"), table_name="historical_weather_snapshots")
    op.drop_index(op.f("ix_historical_weather_snapshots_station_id"), table_name="historical_weather_snapshots")
    op.drop_table("historical_weather_snapshots")

    op.drop_index(op.f("ix_historical_market_snapshots_settlement_ts"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_close_ts"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_source_hash"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_source_id"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_source_kind"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_asof_ts"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_local_market_day"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_station_id"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_series_ticker"), table_name="historical_market_snapshots")
    op.drop_index(op.f("ix_historical_market_snapshots_market_ticker"), table_name="historical_market_snapshots")
    op.drop_table("historical_market_snapshots")

    op.drop_index(op.f("ix_historical_import_runs_finished_at"), table_name="historical_import_runs")
    op.drop_index(op.f("ix_historical_import_runs_started_at"), table_name="historical_import_runs")
    op.drop_index(op.f("ix_historical_import_runs_status"), table_name="historical_import_runs")
    op.drop_index(op.f("ix_historical_import_runs_source"), table_name="historical_import_runs")
    op.drop_index(op.f("ix_historical_import_runs_import_kind"), table_name="historical_import_runs")
    op.drop_table("historical_import_runs")

    op.drop_index(op.f("ix_rooms_room_origin"), table_name="rooms")
    op.drop_column("rooms", "room_origin")
