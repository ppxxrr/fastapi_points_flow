"""extend point flow table and add parking record table

Revision ID: 20260317_0003
Revises: 20260315_0002
Create Date: 2026-03-17 14:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260317_0003"
down_revision = "20260315_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("member_point_flow") as batch_op:
        batch_op.alter_column(
            "member_phone",
            existing_type=sa.String(length=32),
            existing_nullable=True,
            new_column_name="mobile_no",
        )
        batch_op.alter_column(
            "plaza_bu_name",
            existing_type=sa.String(length=128),
            type_=sa.String(length=255),
            existing_nullable=True,
            new_column_name="plaza_name",
        )
        batch_op.add_column(sa.Column("source_file", sa.String(length=500), nullable=True, comment="Imported source file"))
        batch_op.add_column(sa.Column("source_row_no", sa.Integer(), nullable=True, comment="Imported source row number"))
        batch_op.add_column(sa.Column("row_hash", sa.String(length=64), nullable=True, comment="Normalized row hash"))
        batch_op.drop_index("ix_member_point_flow_flow_no")
        batch_op.drop_index("ix_member_point_flow_member_id_create_time")
        batch_op.drop_index("ix_member_point_flow_member_phone_create_time")
        batch_op.drop_index("ix_member_point_flow_consume_time")

    op.create_index("uq_member_point_flow_flow_no", "member_point_flow", ["flow_no"], unique=True)
    op.create_index("ix_member_point_flow_mobile_no_consume_time", "member_point_flow", ["mobile_no", "consume_time"], unique=False)
    op.create_index("ix_member_point_flow_member_id_consume_time", "member_point_flow", ["member_id", "consume_time"], unique=False)
    op.create_index("ix_member_point_flow_create_time", "member_point_flow", ["create_time"], unique=False)
    op.create_index("ix_member_point_flow_plaza_bu_id_create_time", "member_point_flow", ["plaza_bu_id", "create_time"], unique=False)
    op.create_index("ix_member_point_flow_plaza_name_create_time", "member_point_flow", ["plaza_name", "create_time"], unique=False)
    op.execute("UPDATE member_point_flow SET row_hash = event_key WHERE row_hash IS NULL")

    op.create_table(
        "parking_record",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_key", sa.String(length=64), nullable=False, comment="Fallback idempotency key"),
        sa.Column("record_id", sa.String(length=64), nullable=True, comment="Source record id"),
        sa.Column("parking_serial_no", sa.String(length=64), nullable=True, comment="Parking serial number"),
        sa.Column("mobile_no", sa.String(length=32), nullable=True, comment="Matched mobile number"),
        sa.Column("member_id", sa.String(length=64), nullable=True, comment="Resolved member business id"),
        sa.Column("plaza_bu_id", sa.String(length=64), nullable=True, comment="Resolved plaza BU id"),
        sa.Column("plaza_name", sa.String(length=255), nullable=True, comment="Parking plaza name"),
        sa.Column("plate_no", sa.String(length=32), nullable=True, comment="Plate number"),
        sa.Column("entry_plate_no", sa.String(length=32), nullable=True, comment="Entry plate number"),
        sa.Column("plate_color", sa.String(length=32), nullable=True, comment="Plate color"),
        sa.Column("plate_type", sa.String(length=32), nullable=True, comment="Plate type"),
        sa.Column("vehicle_type_code", sa.String(length=32), nullable=True, comment="Vehicle type code"),
        sa.Column("vehicle_type_name", sa.String(length=128), nullable=True, comment="Vehicle type name"),
        sa.Column("vehicle_type_name_2", sa.String(length=128), nullable=True, comment="Secondary vehicle type"),
        sa.Column("entry_time", sa.DateTime(), nullable=True, comment="Entry time"),
        sa.Column("exit_time", sa.DateTime(), nullable=True, comment="Exit time"),
        sa.Column("parking_duration_seconds", sa.Integer(), nullable=True, comment="Parking duration in seconds"),
        sa.Column("status", sa.String(length=64), nullable=True, comment="Parking status"),
        sa.Column("card_no", sa.String(length=64), nullable=True, comment="Card number"),
        sa.Column("card_id", sa.String(length=64), nullable=True, comment="Card id"),
        sa.Column("ticket_no", sa.String(length=255), nullable=True, comment="Ticket number"),
        sa.Column("entry_image_url", sa.String(length=1000), nullable=True, comment="Entry image url"),
        sa.Column("entry_channel", sa.String(length=128), nullable=True, comment="Entry channel"),
        sa.Column("entry_guard_name", sa.String(length=128), nullable=True, comment="Entry guard name"),
        sa.Column("exit_image_url", sa.String(length=1000), nullable=True, comment="Exit image url"),
        sa.Column("exit_channel", sa.String(length=128), nullable=True, comment="Exit channel"),
        sa.Column("exit_guard_name", sa.String(length=128), nullable=True, comment="Exit guard name"),
        sa.Column("auto_pay_flag", sa.Boolean(), nullable=True, comment="Auto pay flag"),
        sa.Column("total_fee_cent", sa.Integer(), nullable=True, comment="Total fee in cents"),
        sa.Column("discount_fee_cent", sa.Integer(), nullable=True, comment="Discount fee in cents"),
        sa.Column("online_pay_fee_cent", sa.Integer(), nullable=True, comment="Online pay fee in cents"),
        sa.Column("balance_pay_fee_cent", sa.Integer(), nullable=True, comment="Balance pay fee in cents"),
        sa.Column("cash_pay_fee_cent", sa.Integer(), nullable=True, comment="Cash pay fee in cents"),
        sa.Column("prepaid_fee_cent", sa.Integer(), nullable=True, comment="Prepaid fee in cents"),
        sa.Column("merchant_no", sa.String(length=64), nullable=True, comment="Merchant number"),
        sa.Column("parking_uuid", sa.String(length=64), nullable=True, comment="Parking lot uuid"),
        sa.Column("created_time", sa.DateTime(), nullable=True, comment="Source created time"),
        sa.Column("updated_time", sa.DateTime(), nullable=True, comment="Source updated time"),
        sa.Column("source_file", sa.String(length=500), nullable=True, comment="Imported source file"),
        sa.Column("source_row_no", sa.Integer(), nullable=True, comment="Imported source row number"),
        sa.Column("row_hash", sa.String(length=64), nullable=True, comment="Normalized row hash"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="Normalized raw source row"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="Created at"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="Updated at"),
        sa.PrimaryKeyConstraint("id", name="pk_parking_record"),
        sa.UniqueConstraint("event_key", name="uq_parking_record_event_key"),
        sa.UniqueConstraint("record_id", name="uq_parking_record_record_id"),
        comment="Parking entry and exit records imported from CSV exports.",
    )
    op.create_index("ix_parking_record_mobile_no_exit_time", "parking_record", ["mobile_no", "exit_time"], unique=False)
    op.create_index("ix_parking_record_member_id_exit_time", "parking_record", ["member_id", "exit_time"], unique=False)
    op.create_index("ix_parking_record_plate_no_exit_time", "parking_record", ["plate_no", "exit_time"], unique=False)
    op.create_index("ix_parking_record_entry_time", "parking_record", ["entry_time"], unique=False)
    op.create_index("ix_parking_record_exit_time", "parking_record", ["exit_time"], unique=False)
    op.create_index("ix_parking_record_parking_serial_no", "parking_record", ["parking_serial_no"], unique=False)
    op.create_index("ix_parking_record_plaza_bu_id_exit_time", "parking_record", ["plaza_bu_id", "exit_time"], unique=False)
    op.create_index("ix_parking_record_plaza_name_exit_time", "parking_record", ["plaza_name", "exit_time"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_parking_record_plaza_name_exit_time", table_name="parking_record")
    op.drop_index("ix_parking_record_plaza_bu_id_exit_time", table_name="parking_record")
    op.drop_index("ix_parking_record_parking_serial_no", table_name="parking_record")
    op.drop_index("ix_parking_record_exit_time", table_name="parking_record")
    op.drop_index("ix_parking_record_entry_time", table_name="parking_record")
    op.drop_index("ix_parking_record_plate_no_exit_time", table_name="parking_record")
    op.drop_index("ix_parking_record_member_id_exit_time", table_name="parking_record")
    op.drop_index("ix_parking_record_mobile_no_exit_time", table_name="parking_record")
    op.drop_table("parking_record")

    op.drop_index("ix_member_point_flow_plaza_name_create_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_plaza_bu_id_create_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_create_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_member_id_consume_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_mobile_no_consume_time", table_name="member_point_flow")
    op.drop_index("uq_member_point_flow_flow_no", table_name="member_point_flow")

    with op.batch_alter_table("member_point_flow") as batch_op:
        batch_op.drop_column("row_hash")
        batch_op.drop_column("source_row_no")
        batch_op.drop_column("source_file")
        batch_op.alter_column(
            "mobile_no",
            existing_type=sa.String(length=32),
            existing_nullable=True,
            new_column_name="member_phone",
        )
        batch_op.alter_column(
            "plaza_name",
            existing_type=sa.String(length=255),
            type_=sa.String(length=128),
            existing_nullable=True,
            new_column_name="plaza_bu_name",
        )

    op.create_index("ix_member_point_flow_flow_no", "member_point_flow", ["flow_no"], unique=False)
    op.create_index("ix_member_point_flow_member_id_create_time", "member_point_flow", ["member_id", "create_time"], unique=False)
    op.create_index("ix_member_point_flow_member_phone_create_time", "member_point_flow", ["member_phone", "create_time"], unique=False)
    op.create_index("ix_member_point_flow_consume_time", "member_point_flow", ["consume_time"], unique=False)
