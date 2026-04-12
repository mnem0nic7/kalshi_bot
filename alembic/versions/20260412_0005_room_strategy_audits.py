"""add room strategy audits

Revision ID: 20260412_0005
Revises: 20260411_0004
Create Date: 2026-04-12 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0005"
down_revision = "20260411_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "room_strategy_audits",
        sa.Column("room_id", sa.String(length=36), nullable=False),
        sa.Column("market_ticker", sa.String(length=128), nullable=False),
        sa.Column("audit_source", sa.String(length=32), nullable=False),
        sa.Column("audit_version", sa.String(length=64), nullable=False),
        sa.Column("thesis_correctness", sa.String(length=32), nullable=False),
        sa.Column("trade_quality", sa.String(length=32), nullable=False),
        sa.Column("block_correctness", sa.String(length=32), nullable=False),
        sa.Column("missed_stand_down", sa.Boolean(), nullable=False),
        sa.Column("stale_data_mismatch", sa.Boolean(), nullable=False),
        sa.Column("effective_freshness_agreement", sa.Boolean(), nullable=False),
        sa.Column("resolution_state", sa.String(length=32), nullable=True),
        sa.Column("eligibility_passed", sa.Boolean(), nullable=True),
        sa.Column("stand_down_reason", sa.String(length=64), nullable=True),
        sa.Column("trainable_default", sa.Boolean(), nullable=False),
        sa.Column("exclude_reason", sa.String(length=128), nullable=True),
        sa.Column("quality_warnings", sa.JSON(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("room_id"),
    )
    op.create_index(op.f("ix_room_strategy_audits_market_ticker"), "room_strategy_audits", ["market_ticker"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_audit_source"), "room_strategy_audits", ["audit_source"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_audit_version"), "room_strategy_audits", ["audit_version"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_thesis_correctness"), "room_strategy_audits", ["thesis_correctness"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_trade_quality"), "room_strategy_audits", ["trade_quality"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_block_correctness"), "room_strategy_audits", ["block_correctness"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_missed_stand_down"), "room_strategy_audits", ["missed_stand_down"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_stale_data_mismatch"), "room_strategy_audits", ["stale_data_mismatch"], unique=False)
    op.create_index(
        op.f("ix_room_strategy_audits_effective_freshness_agreement"),
        "room_strategy_audits",
        ["effective_freshness_agreement"],
        unique=False,
    )
    op.create_index(op.f("ix_room_strategy_audits_resolution_state"), "room_strategy_audits", ["resolution_state"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_eligibility_passed"), "room_strategy_audits", ["eligibility_passed"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_stand_down_reason"), "room_strategy_audits", ["stand_down_reason"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_trainable_default"), "room_strategy_audits", ["trainable_default"], unique=False)
    op.create_index(op.f("ix_room_strategy_audits_exclude_reason"), "room_strategy_audits", ["exclude_reason"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_room_strategy_audits_exclude_reason"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_trainable_default"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_stand_down_reason"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_eligibility_passed"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_resolution_state"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_effective_freshness_agreement"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_stale_data_mismatch"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_missed_stand_down"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_block_correctness"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_trade_quality"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_thesis_correctness"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_audit_version"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_audit_source"), table_name="room_strategy_audits")
    op.drop_index(op.f("ix_room_strategy_audits_market_ticker"), table_name="room_strategy_audits")
    op.drop_table("room_strategy_audits")
