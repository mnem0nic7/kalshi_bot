"""agent pack and self-improve schema

Revision ID: 20260410_0003
Revises: 20260410_0002
Create Date: 2026-04-10 02:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260410_0003"
down_revision = "20260410_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("rooms", sa.Column("kalshi_env", sa.String(length=32), nullable=False, server_default="demo"))
    op.add_column("rooms", sa.Column("agent_pack_version", sa.String(length=128), nullable=True))
    op.add_column("rooms", sa.Column("evaluation_run_id", sa.String(length=36), nullable=True))
    op.add_column("rooms", sa.Column("role_models", sa.JSON(), nullable=False, server_default=sa.text("'{}'")))
    op.create_index("ix_rooms_kalshi_env", "rooms", ["kalshi_env"], unique=False)
    op.create_index("ix_rooms_agent_pack_version", "rooms", ["agent_pack_version"], unique=False)
    op.create_index("ix_rooms_evaluation_run_id", "rooms", ["evaluation_run_id"], unique=False)

    op.create_table(
        "agent_packs",
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("parent_version", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version", name="uq_agent_packs_version"),
    )
    op.create_index("ix_agent_packs_version", "agent_packs", ["version"], unique=False)
    op.create_index("ix_agent_packs_status", "agent_packs", ["status"], unique=False)

    op.create_table(
        "critique_runs",
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_pack_version", sa.String(length=128), nullable=False),
        sa.Column("candidate_version", sa.String(length=128), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("room_count", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_critique_runs_status", "critique_runs", ["status"], unique=False)
    op.create_index("ix_critique_runs_source_pack_version", "critique_runs", ["source_pack_version"], unique=False)
    op.create_index("ix_critique_runs_candidate_version", "critique_runs", ["candidate_version"], unique=False)
    op.create_index("ix_critique_runs_started_at", "critique_runs", ["started_at"], unique=False)

    op.create_table(
        "evaluation_runs",
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("champion_version", sa.String(length=128), nullable=False),
        sa.Column("candidate_version", sa.String(length=128), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("holdout_room_count", sa.Integer(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_evaluation_runs_status", "evaluation_runs", ["status"], unique=False)
    op.create_index("ix_evaluation_runs_champion_version", "evaluation_runs", ["champion_version"], unique=False)
    op.create_index("ix_evaluation_runs_candidate_version", "evaluation_runs", ["candidate_version"], unique=False)
    op.create_index("ix_evaluation_runs_started_at", "evaluation_runs", ["started_at"], unique=False)
    op.create_index("ix_evaluation_runs_passed", "evaluation_runs", ["passed"], unique=False)

    op.create_table(
        "promotion_events",
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("candidate_version", sa.String(length=128), nullable=False),
        sa.Column("previous_version", sa.String(length=128), nullable=True),
        sa.Column("target_color", sa.String(length=16), nullable=False),
        sa.Column("evaluation_run_id", sa.String(length=36), nullable=True),
        sa.Column("rollback_reason", sa.Text(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_promotion_events_status", "promotion_events", ["status"], unique=False)
    op.create_index("ix_promotion_events_candidate_version", "promotion_events", ["candidate_version"], unique=False)
    op.create_index("ix_promotion_events_previous_version", "promotion_events", ["previous_version"], unique=False)
    op.create_index("ix_promotion_events_target_color", "promotion_events", ["target_color"], unique=False)
    op.create_index("ix_promotion_events_evaluation_run_id", "promotion_events", ["evaluation_run_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_promotion_events_evaluation_run_id", table_name="promotion_events")
    op.drop_index("ix_promotion_events_target_color", table_name="promotion_events")
    op.drop_index("ix_promotion_events_previous_version", table_name="promotion_events")
    op.drop_index("ix_promotion_events_candidate_version", table_name="promotion_events")
    op.drop_index("ix_promotion_events_status", table_name="promotion_events")
    op.drop_table("promotion_events")

    op.drop_index("ix_evaluation_runs_passed", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_started_at", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_candidate_version", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_champion_version", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_status", table_name="evaluation_runs")
    op.drop_table("evaluation_runs")

    op.drop_index("ix_critique_runs_started_at", table_name="critique_runs")
    op.drop_index("ix_critique_runs_candidate_version", table_name="critique_runs")
    op.drop_index("ix_critique_runs_source_pack_version", table_name="critique_runs")
    op.drop_index("ix_critique_runs_status", table_name="critique_runs")
    op.drop_table("critique_runs")

    op.drop_index("ix_agent_packs_status", table_name="agent_packs")
    op.drop_index("ix_agent_packs_version", table_name="agent_packs")
    op.drop_table("agent_packs")

    op.drop_index("ix_rooms_evaluation_run_id", table_name="rooms")
    op.drop_index("ix_rooms_agent_pack_version", table_name="rooms")
    op.drop_index("ix_rooms_kalshi_env", table_name="rooms")
    op.drop_column("rooms", "role_models")
    op.drop_column("rooms", "evaluation_run_id")
    op.drop_column("rooms", "agent_pack_version")
    op.drop_column("rooms", "kalshi_env")
