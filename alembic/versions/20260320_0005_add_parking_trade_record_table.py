"""add parking trade record table

Revision ID: 20260320_0005
Revises: 20260318_0004
Create Date: 2026-03-20 09:30:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260320_0005"
down_revision = "20260318_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parking_trade_record",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_key", sa.String(length=64), nullable=False, comment="Fallback idempotency key"),
        sa.Column("trade_id", sa.String(length=64), nullable=True, comment="Source trade id"),
        sa.Column("merchant_no", sa.String(length=64), nullable=True, comment="Merchant number"),
        sa.Column("app_id", sa.String(length=64), nullable=True, comment="App id"),
        sa.Column("pay_serial", sa.String(length=64), nullable=True, comment="Payment serial number"),
        sa.Column("trade_no", sa.String(length=64), nullable=True, comment="Channel trade number"),
        sa.Column("subject", sa.String(length=255), nullable=True, comment="Trade subject"),
        sa.Column("body", sa.String(length=1000), nullable=True, comment="Trade body"),
        sa.Column("business", sa.String(length=64), nullable=True, comment="Business code"),
        sa.Column("business_voucher", sa.String(length=128), nullable=True, comment="Business voucher"),
        sa.Column("pay_order", sa.String(length=128), nullable=True, comment="Pay order"),
        sa.Column("plaza_name", sa.String(length=255), nullable=True, comment="Parking plaza name"),
        sa.Column("plate_no", sa.String(length=32), nullable=True, comment="Extracted plate number"),
        sa.Column("mobile_no", sa.String(length=32), nullable=True, comment="User mobile number"),
        sa.Column("payer", sa.String(length=128), nullable=True, comment="Payer"),
        sa.Column("user_identity", sa.String(length=128), nullable=True, comment="User identity"),
        sa.Column("channel_code", sa.String(length=32), nullable=True, comment="Channel code"),
        sa.Column("channel_name", sa.String(length=64), nullable=True, comment="Channel name"),
        sa.Column("pay_mode_code", sa.Integer(), nullable=True, comment="Pay mode code"),
        sa.Column("pay_mode_name", sa.String(length=128), nullable=True, comment="Pay mode name"),
        sa.Column("pay_type_code", sa.Integer(), nullable=True, comment="Pay type code"),
        sa.Column("result_code", sa.Integer(), nullable=True, comment="Result code"),
        sa.Column("process_code", sa.Integer(), nullable=True, comment="Process code"),
        sa.Column("refund_code", sa.Integer(), nullable=True, comment="Refund code"),
        sa.Column("synced_flag", sa.Integer(), nullable=True, comment="Synced flag"),
        sa.Column("value_cent", sa.Integer(), nullable=True, comment="Trade value in cents"),
        sa.Column("discount_cent", sa.Integer(), nullable=True, comment="Discount in cents"),
        sa.Column("reduce_value_cent", sa.Integer(), nullable=True, comment="Reduce value in cents"),
        sa.Column("pay_value_cent", sa.Integer(), nullable=True, comment="Pay value in cents"),
        sa.Column("actual_pay_unit_cent", sa.Integer(), nullable=True, comment="Actual pay unit in cents"),
        sa.Column("refund_value_cent", sa.Integer(), nullable=True, comment="Refund value in cents"),
        sa.Column("actual_value_cent", sa.Integer(), nullable=True, comment="Actual value in cents"),
        sa.Column("actual_fee_cent", sa.Integer(), nullable=True, comment="Actual fee in cents"),
        sa.Column("fee_cent", sa.Integer(), nullable=True, comment="Fee in cents"),
        sa.Column("refund_fee_cent", sa.Integer(), nullable=True, comment="Refund fee in cents"),
        sa.Column("coupon_id", sa.String(length=128), nullable=True, comment="Coupon id"),
        sa.Column("coupon_purpose", sa.String(length=255), nullable=True, comment="Coupon purpose"),
        sa.Column("notify_service", sa.String(length=128), nullable=True, comment="Notify service"),
        sa.Column("notify_url", sa.String(length=500), nullable=True, comment="Notify url"),
        sa.Column("settle_id", sa.String(length=64), nullable=True, comment="Settle id"),
        sa.Column("deduct_mode", sa.String(length=64), nullable=True, comment="Deduct mode"),
        sa.Column("remark", sa.String(length=500), nullable=True, comment="Remark"),
        sa.Column("trade_scene", sa.String(length=64), nullable=True, comment="Trade scene"),
        sa.Column("create_time", sa.DateTime(), nullable=True, comment="Source create time"),
        sa.Column("update_time", sa.DateTime(), nullable=True, comment="Source update time"),
        sa.Column("expire_time", sa.DateTime(), nullable=True, comment="Source expire time"),
        sa.Column("pay_time", sa.DateTime(), nullable=True, comment="Source pay time"),
        sa.Column("result_time", sa.DateTime(), nullable=True, comment="Source result time"),
        sa.Column("settle_time", sa.DateTime(), nullable=True, comment="Source settle time"),
        sa.Column("source_file", sa.String(length=500), nullable=True, comment="Imported source file"),
        sa.Column("source_row_no", sa.Integer(), nullable=True, comment="Imported source row number"),
        sa.Column("row_hash", sa.String(length=64), nullable=True, comment="Normalized row hash"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="Normalized raw source row"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="Created at"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="Updated at"),
        sa.PrimaryKeyConstraint("id", name="pk_parking_trade_record"),
        sa.UniqueConstraint("event_key", name="uq_parking_trade_event_key"),
        sa.UniqueConstraint("trade_id", name="uq_parking_trade_trade_id"),
        comment="Parking payment trade detail records imported from 4pyun API.",
    )
    op.create_index("ix_parking_trade_pay_time", "parking_trade_record", ["pay_time"], unique=False)
    op.create_index("ix_parking_trade_result_time", "parking_trade_record", ["result_time"], unique=False)
    op.create_index("ix_parking_trade_create_time", "parking_trade_record", ["create_time"], unique=False)
    op.create_index("ix_parking_trade_plate_no_pay_time", "parking_trade_record", ["plate_no", "pay_time"], unique=False)
    op.create_index("ix_parking_trade_mobile_no_pay_time", "parking_trade_record", ["mobile_no", "pay_time"], unique=False)
    op.create_index("ix_parking_trade_merchant_no_pay_time", "parking_trade_record", ["merchant_no", "pay_time"], unique=False)
    op.create_index("ix_parking_trade_business_pay_time", "parking_trade_record", ["business", "pay_time"], unique=False)
    op.create_index("ix_parking_trade_trade_no", "parking_trade_record", ["trade_no"], unique=False)
    op.create_index("ix_parking_trade_pay_serial", "parking_trade_record", ["pay_serial"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_parking_trade_pay_serial", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_trade_no", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_business_pay_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_merchant_no_pay_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_mobile_no_pay_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_plate_no_pay_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_create_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_result_time", table_name="parking_trade_record")
    op.drop_index("ix_parking_trade_pay_time", table_name="parking_trade_record")
    op.drop_table("parking_trade_record")
