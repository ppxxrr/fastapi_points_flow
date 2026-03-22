"""add message board entry table

Revision ID: 20260321_0008
Revises: 20260321_0007
Create Date: 2026-03-21 19:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260321_0008"
down_revision = "20260321_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "message_board_entry",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("request_name", sa.String(length=200), nullable=False, comment="Requested feature title"),
        sa.Column("detail", sa.Text(), nullable=False, comment="Detailed request description"),
        sa.Column("system_name", sa.String(length=120), nullable=False, comment="Target system name"),
        sa.Column("expected_completion_date", sa.Date(), nullable=True, comment="Expected delivery date"),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'new'"),
            comment="Message processing status",
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="鍒涘缓鏃堕棿"),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            comment="鏇存柊鏃堕棿",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_message_board_entry"),
        comment="Anonymous message board entries for product feedback.",
    )
    op.create_index(
        "ix_message_board_entry_system_name_created_at",
        "message_board_entry",
        ["system_name", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_message_board_entry_status_created_at",
        "message_board_entry",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_message_board_entry_status_created_at", table_name="message_board_entry")
    op.drop_index("ix_message_board_entry_system_name_created_at", table_name="message_board_entry")
    op.drop_table("message_board_entry")
