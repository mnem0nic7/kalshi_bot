"""Add web auth users and sessions.

Revision ID: 20260421_0015
Revises: 20260421_0014
Create Date: 2026-04-21
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260421_0015"
down_revision = "20260421_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "web_users",
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("password_salt", sa.String(length=64), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_web_users_email", "web_users", ["email"], unique=True)
    op.create_index("ix_web_users_last_login_at", "web_users", ["last_login_at"], unique=False)

    op.create_table(
        "web_sessions",
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["web_users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_web_sessions_user_id", "web_sessions", ["user_id"], unique=False)
    op.create_index("ix_web_sessions_token_hash", "web_sessions", ["token_hash"], unique=True)
    op.create_index("ix_web_sessions_expires_at", "web_sessions", ["expires_at"], unique=False)
    op.create_index("ix_web_sessions_last_seen_at", "web_sessions", ["last_seen_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_web_sessions_last_seen_at", table_name="web_sessions")
    op.drop_index("ix_web_sessions_expires_at", table_name="web_sessions")
    op.drop_index("ix_web_sessions_token_hash", table_name="web_sessions")
    op.drop_index("ix_web_sessions_user_id", table_name="web_sessions")
    op.drop_table("web_sessions")

    op.drop_index("ix_web_users_last_login_at", table_name="web_users")
    op.drop_index("ix_web_users_email", table_name="web_users")
    op.drop_table("web_users")
