"""add device layout point table

Revision ID: 20260321_0007
Revises: 20260320_0006
Create Date: 2026-03-21 11:40:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260321_0007"
down_revision = "20260320_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "device_layout_point",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("point_type", sa.String(length=32), nullable=False, comment="Point type key"),
        sa.Column("point_code", sa.String(length=64), nullable=False, comment="Point code"),
        sa.Column("point_name", sa.String(length=255), nullable=False, comment="Point display name"),
        sa.Column("floor_code", sa.String(length=16), nullable=False, comment="Floor code"),
        sa.Column("x_ratio", sa.Numeric(precision=10, scale=6), nullable=True, comment="Normalized X coordinate ratio (0-1)"),
        sa.Column("y_ratio", sa.Numeric(precision=10, scale=6), nullable=True, comment="Normalized Y coordinate ratio (0-1)"),
        sa.Column("source_file", sa.String(length=500), nullable=True, comment="Latest import source file"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="创建时间"),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            comment="更新时间",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_device_layout_point"),
        sa.UniqueConstraint("point_type", "point_code", name="uq_device_layout_point_point_type_point_code"),
        comment="Device layout points and normalized map coordinates.",
    )
    op.create_index(
        "ix_device_layout_point_point_type_floor_code",
        "device_layout_point",
        ["point_type", "floor_code"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_device_layout_point_point_type_floor_code", table_name="device_layout_point")
    op.drop_table("device_layout_point")
