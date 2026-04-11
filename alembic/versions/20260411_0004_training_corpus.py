"""training corpus, campaign, and research health schema

Revision ID: 20260411_0004
Revises: 20260410_0003
Create Date: 2026-04-11 18:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260411_0004"
down_revision = "20260410_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "room_campaigns",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("campaign_id", sa.String(length=64), nullable=False),
        sa.Column("trigger_source", sa.String(length=64), nullable=False),
        sa.Column("city_bucket", sa.String(length=128), nullable=True),
        sa.Column("market_regime_bucket", sa.String(length=64), nullable=True),
        sa.Column("difficulty_bucket", sa.String(length=64), nullable=True),
        sa.Column("outcome_bucket", sa.String(length=64), nullable=True),
        sa.Column("dossier_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dossier_artifact_id"], ["artifacts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("room_id", name="uq_room_campaign_room_id"),
    )
    op.create_index("ix_room_campaigns_room_id", "room_campaigns", ["room_id"], unique=False)
    op.create_index("ix_room_campaigns_campaign_id", "room_campaigns", ["campaign_id"], unique=False)
    op.create_index("ix_room_campaigns_trigger_source", "room_campaigns", ["trigger_source"], unique=False)
    op.create_index("ix_room_campaigns_city_bucket", "room_campaigns", ["city_bucket"], unique=False)
    op.create_index("ix_room_campaigns_market_regime_bucket", "room_campaigns", ["market_regime_bucket"], unique=False)
    op.create_index("ix_room_campaigns_difficulty_bucket", "room_campaigns", ["difficulty_bucket"], unique=False)
    op.create_index("ix_room_campaigns_outcome_bucket", "room_campaigns", ["outcome_bucket"], unique=False)
    op.create_index("ix_room_campaigns_dossier_artifact_id", "room_campaigns", ["dossier_artifact_id"], unique=False)

    op.create_table(
        "room_research_health",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("dossier_status", sa.String(length=32), nullable=False),
        sa.Column("gate_passed", sa.Boolean(), nullable=False),
        sa.Column("valid_dossier", sa.Boolean(), nullable=False),
        sa.Column("good_for_training", sa.Boolean(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("citation_coverage_score", sa.Float(), nullable=False),
        sa.Column("settlement_clarity_score", sa.Float(), nullable=False),
        sa.Column("freshness_score", sa.Float(), nullable=False),
        sa.Column("contradiction_count", sa.Integer(), nullable=False),
        sa.Column("structured_completeness_score", sa.Float(), nullable=False),
        sa.Column("fair_value_score", sa.Float(), nullable=False),
        sa.Column("dossier_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dossier_artifact_id"], ["artifacts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("room_id"),
    )
    op.create_index("ix_room_research_health_market_ticker", "room_research_health", ["market_ticker"], unique=False)
    op.create_index("ix_room_research_health_dossier_status", "room_research_health", ["dossier_status"], unique=False)
    op.create_index("ix_room_research_health_gate_passed", "room_research_health", ["gate_passed"], unique=False)
    op.create_index("ix_room_research_health_valid_dossier", "room_research_health", ["valid_dossier"], unique=False)
    op.create_index("ix_room_research_health_good_for_training", "room_research_health", ["good_for_training"], unique=False)
    op.create_index("ix_room_research_health_quality_score", "room_research_health", ["quality_score"], unique=False)
    op.create_index("ix_room_research_health_dossier_artifact_id", "room_research_health", ["dossier_artifact_id"], unique=False)

    op.create_table(
        "training_dataset_builds",
        sa.Column("build_version", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("selection_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("selection_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("room_count", sa.Integer(), nullable=False),
        sa.Column("filters", sa.JSON(), nullable=False),
        sa.Column("label_stats", sa.JSON(), nullable=False),
        sa.Column("pack_versions", sa.JSON(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("build_version", name="uq_training_dataset_builds_version"),
    )
    op.create_index("ix_training_dataset_builds_build_version", "training_dataset_builds", ["build_version"], unique=False)
    op.create_index("ix_training_dataset_builds_mode", "training_dataset_builds", ["mode"], unique=False)
    op.create_index("ix_training_dataset_builds_status", "training_dataset_builds", ["status"], unique=False)
    op.create_index("ix_training_dataset_builds_completed_at", "training_dataset_builds", ["completed_at"], unique=False)

    op.create_table(
        "training_dataset_build_items",
        sa.Column("dataset_build_id", sa.String(length=36), nullable=False),
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dataset_build_id"], ["training_dataset_builds.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_build_id", "room_id", name="uq_training_dataset_build_items_room"),
    )
    op.create_index("ix_training_dataset_build_items_dataset_build_id", "training_dataset_build_items", ["dataset_build_id"], unique=False)
    op.create_index("ix_training_dataset_build_items_room_id", "training_dataset_build_items", ["room_id"], unique=False)
    op.create_index("ix_training_dataset_build_items_sequence", "training_dataset_build_items", ["sequence"], unique=False)

    op.create_table(
        "training_readiness",
        sa.Column("ready_for_sft_export", sa.Boolean(), nullable=False),
        sa.Column("ready_for_critique", sa.Boolean(), nullable=False),
        sa.Column("ready_for_evaluation", sa.Boolean(), nullable=False),
        sa.Column("ready_for_promotion", sa.Boolean(), nullable=False),
        sa.Column("complete_room_count", sa.Integer(), nullable=False),
        sa.Column("market_diversity_count", sa.Integer(), nullable=False),
        sa.Column("settled_room_count", sa.Integer(), nullable=False),
        sa.Column("trade_positive_room_count", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_training_readiness_ready_for_sft_export", "training_readiness", ["ready_for_sft_export"], unique=False)
    op.create_index("ix_training_readiness_ready_for_critique", "training_readiness", ["ready_for_critique"], unique=False)
    op.create_index("ix_training_readiness_ready_for_evaluation", "training_readiness", ["ready_for_evaluation"], unique=False)
    op.create_index("ix_training_readiness_ready_for_promotion", "training_readiness", ["ready_for_promotion"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_training_readiness_ready_for_promotion", table_name="training_readiness")
    op.drop_index("ix_training_readiness_ready_for_evaluation", table_name="training_readiness")
    op.drop_index("ix_training_readiness_ready_for_critique", table_name="training_readiness")
    op.drop_index("ix_training_readiness_ready_for_sft_export", table_name="training_readiness")
    op.drop_table("training_readiness")

    op.drop_index("ix_training_dataset_build_items_sequence", table_name="training_dataset_build_items")
    op.drop_index("ix_training_dataset_build_items_room_id", table_name="training_dataset_build_items")
    op.drop_index("ix_training_dataset_build_items_dataset_build_id", table_name="training_dataset_build_items")
    op.drop_table("training_dataset_build_items")

    op.drop_index("ix_training_dataset_builds_completed_at", table_name="training_dataset_builds")
    op.drop_index("ix_training_dataset_builds_status", table_name="training_dataset_builds")
    op.drop_index("ix_training_dataset_builds_mode", table_name="training_dataset_builds")
    op.drop_index("ix_training_dataset_builds_build_version", table_name="training_dataset_builds")
    op.drop_table("training_dataset_builds")

    op.drop_index("ix_room_research_health_dossier_artifact_id", table_name="room_research_health")
    op.drop_index("ix_room_research_health_quality_score", table_name="room_research_health")
    op.drop_index("ix_room_research_health_good_for_training", table_name="room_research_health")
    op.drop_index("ix_room_research_health_valid_dossier", table_name="room_research_health")
    op.drop_index("ix_room_research_health_gate_passed", table_name="room_research_health")
    op.drop_index("ix_room_research_health_dossier_status", table_name="room_research_health")
    op.drop_index("ix_room_research_health_market_ticker", table_name="room_research_health")
    op.drop_table("room_research_health")

    op.drop_index("ix_room_campaigns_dossier_artifact_id", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_outcome_bucket", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_difficulty_bucket", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_market_regime_bucket", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_city_bucket", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_trigger_source", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_campaign_id", table_name="room_campaigns")
    op.drop_index("ix_room_campaigns_room_id", table_name="room_campaigns")
    op.drop_table("room_campaigns")
