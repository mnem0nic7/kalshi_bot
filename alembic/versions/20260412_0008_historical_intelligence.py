"""add historical intelligence and heuristic pack records

Revision ID: 20260412_0008
Revises: 20260412_0007
Create Date: 2026-04-12 00:08:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0008"
down_revision = "20260412_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "historical_intelligence_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("date_from", sa.String(length=16), nullable=False),
        sa.Column("date_to", sa.String(length=16), nullable=False),
        sa.Column("active_pack_version", sa.String(length=128), nullable=True),
        sa.Column("candidate_pack_version", sa.String(length=128), nullable=True),
        sa.Column("promoted_pack_version", sa.String(length=128), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("room_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_historical_intelligence_runs_status"), "historical_intelligence_runs", ["status"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_date_from"), "historical_intelligence_runs", ["date_from"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_date_to"), "historical_intelligence_runs", ["date_to"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_active_pack_version"), "historical_intelligence_runs", ["active_pack_version"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_candidate_pack_version"), "historical_intelligence_runs", ["candidate_pack_version"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_promoted_pack_version"), "historical_intelligence_runs", ["promoted_pack_version"], unique=False)
    op.create_index(op.f("ix_historical_intelligence_runs_started_at"), "historical_intelligence_runs", ["started_at"], unique=False)

    op.create_table(
        "heuristic_packs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("parent_version", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version", name="uq_heuristic_packs_version"),
    )
    op.create_index(op.f("ix_heuristic_packs_version"), "heuristic_packs", ["version"], unique=False)
    op.create_index(op.f("ix_heuristic_packs_status"), "heuristic_packs", ["status"], unique=False)
    op.create_index(op.f("ix_heuristic_packs_parent_version"), "heuristic_packs", ["parent_version"], unique=False)

    op.create_table(
        "heuristic_pack_promotions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("candidate_version", sa.String(length=128), nullable=False),
        sa.Column("previous_version", sa.String(length=128), nullable=True),
        sa.Column("intelligence_run_id", sa.String(length=36), nullable=True),
        sa.Column("rollback_reason", sa.Text(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_heuristic_pack_promotions_status"), "heuristic_pack_promotions", ["status"], unique=False)
    op.create_index(op.f("ix_heuristic_pack_promotions_candidate_version"), "heuristic_pack_promotions", ["candidate_version"], unique=False)
    op.create_index(op.f("ix_heuristic_pack_promotions_previous_version"), "heuristic_pack_promotions", ["previous_version"], unique=False)
    op.create_index(op.f("ix_heuristic_pack_promotions_intelligence_run_id"), "heuristic_pack_promotions", ["intelligence_run_id"], unique=False)

    op.create_table(
        "heuristic_patch_suggestions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("heuristic_pack_version", sa.String(length=128), nullable=False),
        sa.Column("intelligence_run_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_heuristic_patch_suggestions_heuristic_pack_version"), "heuristic_patch_suggestions", ["heuristic_pack_version"], unique=False)
    op.create_index(op.f("ix_heuristic_patch_suggestions_intelligence_run_id"), "heuristic_patch_suggestions", ["intelligence_run_id"], unique=False)
    op.create_index(op.f("ix_heuristic_patch_suggestions_status"), "heuristic_patch_suggestions", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_heuristic_patch_suggestions_status"), table_name="heuristic_patch_suggestions")
    op.drop_index(op.f("ix_heuristic_patch_suggestions_intelligence_run_id"), table_name="heuristic_patch_suggestions")
    op.drop_index(op.f("ix_heuristic_patch_suggestions_heuristic_pack_version"), table_name="heuristic_patch_suggestions")
    op.drop_table("heuristic_patch_suggestions")

    op.drop_index(op.f("ix_heuristic_pack_promotions_intelligence_run_id"), table_name="heuristic_pack_promotions")
    op.drop_index(op.f("ix_heuristic_pack_promotions_previous_version"), table_name="heuristic_pack_promotions")
    op.drop_index(op.f("ix_heuristic_pack_promotions_candidate_version"), table_name="heuristic_pack_promotions")
    op.drop_index(op.f("ix_heuristic_pack_promotions_status"), table_name="heuristic_pack_promotions")
    op.drop_table("heuristic_pack_promotions")

    op.drop_index(op.f("ix_heuristic_packs_parent_version"), table_name="heuristic_packs")
    op.drop_index(op.f("ix_heuristic_packs_status"), table_name="heuristic_packs")
    op.drop_index(op.f("ix_heuristic_packs_version"), table_name="heuristic_packs")
    op.drop_table("heuristic_packs")

    op.drop_index(op.f("ix_historical_intelligence_runs_started_at"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_promoted_pack_version"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_candidate_pack_version"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_active_pack_version"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_date_to"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_date_from"), table_name="historical_intelligence_runs")
    op.drop_index(op.f("ix_historical_intelligence_runs_status"), table_name="historical_intelligence_runs")
    op.drop_table("historical_intelligence_runs")
