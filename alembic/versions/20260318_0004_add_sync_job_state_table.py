"""add sync job state table

Revision ID: 20260318_0004
Revises: 20260317_0003
Create Date: 2026-03-18 09:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260318_0004"
down_revision = "20260317_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sync_job_state",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_name", sa.String(length=64), nullable=False, comment="Job name"),
        sa.Column("job_date", sa.Date(), nullable=False, comment="Business job date"),
        sa.Column("status", sa.String(length=32), nullable=False, comment="Job status"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0", comment="Retry count after first attempt"),
        sa.Column("last_started_at", sa.DateTime(), nullable=True, comment="Last started at"),
        sa.Column("last_finished_at", sa.DateTime(), nullable=True, comment="Last finished at"),
        sa.Column("last_success_start", sa.Date(), nullable=True, comment="Last success start date"),
        sa.Column("last_success_end", sa.Date(), nullable=True, comment="Last success end date"),
        sa.Column("last_success_at", sa.DateTime(), nullable=True, comment="Last success at"),
        sa.Column("last_error", sa.Text(), nullable=True, comment="Last error message"),
        sa.Column("request_payload", sa.JSON(), nullable=True, comment="Last request payload"),
        sa.Column("result_payload", sa.JSON(), nullable=True, comment="Last result payload"),
        sa.Column("heartbeat_at", sa.DateTime(), nullable=False, comment="Last heartbeat at"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="Created at"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="Updated at"),
        sa.PrimaryKeyConstraint("id", name="pk_sync_job_state"),
        sa.UniqueConstraint("job_name", "job_date", name="uq_sync_job_state_job_name_job_date"),
        comment="Incremental sync job state and retry watermark.",
    )
    op.create_index("ix_sync_job_state_status_job_date", "sync_job_state", ["status", "job_date"], unique=False)
    op.create_index("ix_sync_job_state_job_name_updated_at", "sync_job_state", ["job_name", "updated_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_sync_job_state_job_name_updated_at", table_name="sync_job_state")
    op.drop_index("ix_sync_job_state_status_job_date", table_name="sync_job_state")
    op.drop_table("sync_job_state")
