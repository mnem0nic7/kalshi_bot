"""Add env/time index for ops event status reads.

Revision ID: 20260615_0044
Revises: 20260607_0043
Create Date: 2026-06-15
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260615_0044"
down_revision = "20260607_0043"
branch_labels = None
depends_on = None


INDEX_NAME = "ix_ops_events_env_updated"


def _index_validity(index_name: str) -> bool | None:
    bind = op.get_bind()
    return bind.execute(
        sa.text(
            """
            SELECT i.indisvalid
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = :index_name
            """
        ),
        {"index_name": index_name},
    ).scalar_one_or_none()


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        op.create_index(INDEX_NAME, "ops_events", ["kalshi_env", "updated_at"], if_not_exists=True)
        return

    with op.get_context().autocommit_block():
        if _index_validity(INDEX_NAME) is True:
            return
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY {INDEX_NAME}
            ON ops_events (kalshi_env, updated_at DESC)
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        op.drop_index(INDEX_NAME, table_name="ops_events", if_exists=True)
        return

    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
