"""add dim parking policy table

Revision ID: 20260320_0006
Revises: 20260320_0005
Create Date: 2026-03-20 23:20:00
"""
from __future__ import annotations

from datetime import datetime

from alembic import op
import sqlalchemy as sa


revision = "20260320_0006"
down_revision = "20260320_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_parking_policy",
        sa.Column("version_id", sa.String(length=32), nullable=False, comment="Policy version id"),
        sa.Column("start_date", sa.DateTime(), nullable=False, comment="Version effective start datetime"),
        sa.Column("end_date", sa.DateTime(), nullable=True, comment="Version effective end datetime"),
        sa.Column("member_level", sa.String(length=32), nullable=False, comment="Normalized member level"),
        sa.Column("base_free_hours", sa.Integer(), nullable=False, server_default="0", comment="Base free hours"),
        sa.Column(
            "is_diamond_full_free",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
            comment="Whether this level is full free regardless of duration",
        ),
        sa.PrimaryKeyConstraint("version_id", "member_level", name="pk_dim_parking_policy"),
        comment="Parking policy dimension by version and normalized member level.",
    )
    op.create_index(
        "ix_dim_parking_policy_member_level_start_date_end_date",
        "dim_parking_policy",
        ["member_level", "start_date", "end_date"],
        unique=False,
    )

    policy_table = sa.table(
        "dim_parking_policy",
        sa.column("version_id", sa.String(length=32)),
        sa.column("start_date", sa.DateTime()),
        sa.column("end_date", sa.DateTime()),
        sa.column("member_level", sa.String(length=32)),
        sa.column("base_free_hours", sa.Integer()),
        sa.column("is_diamond_full_free", sa.Boolean()),
    )

    v1_start = datetime(2000, 1, 1, 0, 0, 0)
    v1_end = datetime(2026, 3, 2, 23, 59, 59)
    v2_start = datetime(2026, 3, 3, 0, 0, 0)

    op.bulk_insert(
        policy_table,
        [
            {
                "version_id": "PARKING_RULE_V1",
                "start_date": v1_start,
                "end_date": v1_end,
                "member_level": "非会员",
                "base_free_hours": 0,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V1",
                "start_date": v1_start,
                "end_date": v1_end,
                "member_level": "普卡",
                "base_free_hours": 3,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V1",
                "start_date": v1_start,
                "end_date": v1_end,
                "member_level": "银卡",
                "base_free_hours": 3,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V1",
                "start_date": v1_start,
                "end_date": v1_end,
                "member_level": "金卡",
                "base_free_hours": 3,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V1",
                "start_date": v1_start,
                "end_date": v1_end,
                "member_level": "钻石卡",
                "base_free_hours": 0,
                "is_diamond_full_free": True,
            },
            {
                "version_id": "PARKING_RULE_V2",
                "start_date": v2_start,
                "end_date": None,
                "member_level": "非会员",
                "base_free_hours": 0,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V2",
                "start_date": v2_start,
                "end_date": None,
                "member_level": "普卡",
                "base_free_hours": 1,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V2",
                "start_date": v2_start,
                "end_date": None,
                "member_level": "银卡",
                "base_free_hours": 2,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V2",
                "start_date": v2_start,
                "end_date": None,
                "member_level": "金卡",
                "base_free_hours": 3,
                "is_diamond_full_free": False,
            },
            {
                "version_id": "PARKING_RULE_V2",
                "start_date": v2_start,
                "end_date": None,
                "member_level": "钻石卡",
                "base_free_hours": 0,
                "is_diamond_full_free": True,
            },
        ],
    )

def downgrade() -> None:
    op.drop_index("ix_dim_parking_policy_member_level_start_date_end_date", table_name="dim_parking_policy")
    op.drop_table("dim_parking_policy")
