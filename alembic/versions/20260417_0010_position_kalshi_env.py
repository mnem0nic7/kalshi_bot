"""add kalshi_env to positions

Revision ID: 20260417_0010
Revises: 20260412_0009
Create Date: 2026-04-17 00:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_0010"
down_revision = "20260412_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("positions", sa.Column("kalshi_env", sa.String(16), nullable=False, server_default=""))


def downgrade() -> None:
    op.drop_column("positions", "kalshi_env")
