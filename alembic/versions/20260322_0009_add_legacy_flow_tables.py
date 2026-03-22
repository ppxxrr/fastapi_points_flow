"""add legacy flow tables

Revision ID: 20260322_0009
Revises: 20260321_0008
Create Date: 2026-03-22 01:20:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260322_0009"
down_revision = "20260321_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "traffic_node_daily_flow",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("node_code", sa.String(length=64), nullable=False, comment="Traffic node code"),
        sa.Column("node_name", sa.String(length=255), nullable=True, comment="Traffic node name"),
        sa.Column("business_date", sa.Date(), nullable=False, comment="Business date"),
        sa.Column("passenger_flow", sa.Integer(), nullable=False, comment="Passenger flow count"),
        sa.Column("passenger_flow_ratio", sa.Numeric(precision=12, scale=6), nullable=True, comment="Passenger flow contribution ratio"),
        sa.Column("source_updated_at", sa.DateTime(), nullable=True, comment="Updated time from source system"),
        sa.Column("source_origin", sa.String(length=32), nullable=True, comment="sqlite_import or api_sync"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="Raw source payload"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="created at"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="updated at"),
        sa.PrimaryKeyConstraint("id", name="pk_traffic_node_daily_flow"),
        sa.UniqueConstraint("node_code", "business_date", name="uq_traffic_node_daily_flow_node_code_business_date"),
        comment="Daily passenger flow by traffic node imported from legacy traffic_data.db or API sync.",
    )
    op.create_index("ix_traffic_node_daily_flow_business_date", "traffic_node_daily_flow", ["business_date"], unique=False)
    op.create_index(
        "ix_traffic_node_daily_flow_node_code_business_date",
        "traffic_node_daily_flow",
        ["node_code", "business_date"],
        unique=False,
    )

    op.create_table(
        "railinli_probe_daily_flow",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("probe_id", sa.String(length=64), nullable=False, comment="Probe id"),
        sa.Column("probe_name", sa.String(length=255), nullable=True, comment="Probe display name"),
        sa.Column("business_date", sa.Date(), nullable=False, comment="Business date"),
        sa.Column("entry_count", sa.Integer(), nullable=False, comment="Daily entry count"),
        sa.Column("source_record_time", sa.DateTime(), nullable=True, comment="Original record time from source database"),
        sa.Column("source_origin", sa.String(length=32), nullable=True, comment="sqlite_import or receiver_upload"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="Raw source payload"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="created at"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="updated at"),
        sa.PrimaryKeyConstraint("id", name="pk_railinli_probe_daily_flow"),
        sa.UniqueConstraint("probe_id", "business_date", name="uq_railinli_probe_daily_flow_probe_id_business_date"),
        comment="Daily entry counts by railinli probe imported from legacy railinliKL.db or receiver uploads.",
    )
    op.create_index("ix_railinli_probe_daily_flow_business_date", "railinli_probe_daily_flow", ["business_date"], unique=False)
    op.create_index(
        "ix_railinli_probe_daily_flow_probe_id_business_date",
        "railinli_probe_daily_flow",
        ["probe_id", "business_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_railinli_probe_daily_flow_probe_id_business_date", table_name="railinli_probe_daily_flow")
    op.drop_index("ix_railinli_probe_daily_flow_business_date", table_name="railinli_probe_daily_flow")
    op.drop_table("railinli_probe_daily_flow")
    op.drop_index("ix_traffic_node_daily_flow_node_code_business_date", table_name="traffic_node_daily_flow")
    op.drop_index("ix_traffic_node_daily_flow_business_date", table_name="traffic_node_daily_flow")
    op.drop_table("traffic_node_daily_flow")
