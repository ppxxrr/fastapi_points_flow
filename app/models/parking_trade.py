from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin


class ParkingTradeRecord(TimestampMixin, Base):
    __tablename__ = "parking_trade_record"
    __table_args__ = (
        UniqueConstraint("event_key", name="uq_parking_trade_event_key"),
        UniqueConstraint("trade_id", name="uq_parking_trade_trade_id"),
        Index("ix_parking_trade_pay_time", "pay_time"),
        Index("ix_parking_trade_result_time", "result_time"),
        Index("ix_parking_trade_create_time", "create_time"),
        Index("ix_parking_trade_plate_no_pay_time", "plate_no", "pay_time"),
        Index("ix_parking_trade_mobile_no_pay_time", "mobile_no", "pay_time"),
        Index("ix_parking_trade_merchant_no_pay_time", "merchant_no", "pay_time"),
        Index("ix_parking_trade_business_pay_time", "business", "pay_time"),
        Index("ix_parking_trade_trade_no", "trade_no"),
        Index("ix_parking_trade_pay_serial", "pay_serial"),
        {"comment": "Parking payment trade detail records imported from 4pyun API."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(64), nullable=False, comment="Fallback idempotency key")
    trade_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Source trade id")
    merchant_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Merchant number")
    app_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="App id")
    pay_serial: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Payment serial number")
    trade_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Channel trade number")
    subject: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Trade subject")
    body: Mapped[str | None] = mapped_column(String(1000), nullable=True, comment="Trade body")
    business: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Business code")
    business_voucher: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Business voucher")
    pay_order: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Pay order")
    plaza_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Parking plaza name")
    plate_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Extracted plate number")
    mobile_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="User mobile number")
    payer: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Payer")
    user_identity: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="User identity")
    channel_code: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Channel code")
    channel_name: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Channel name")
    pay_mode_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Pay mode code")
    pay_mode_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Pay mode name")
    pay_type_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Pay type code")
    result_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Result code")
    process_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Process code")
    refund_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Refund code")
    synced_flag: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Synced flag")
    value_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Trade value in cents")
    discount_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Discount in cents")
    reduce_value_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Reduce value in cents")
    pay_value_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Pay value in cents")
    actual_pay_unit_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Actual pay unit in cents")
    refund_value_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Refund value in cents")
    actual_value_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Actual value in cents")
    actual_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Actual fee in cents")
    fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Fee in cents")
    refund_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Refund fee in cents")
    coupon_id: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Coupon id")
    coupon_purpose: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Coupon purpose")
    notify_service: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Notify service")
    notify_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Notify url")
    settle_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Settle id")
    deduct_mode: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Deduct mode")
    remark: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Remark")
    trade_scene: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Trade scene")
    create_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source create time")
    update_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source update time")
    expire_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source expire time")
    pay_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source pay time")
    result_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source result time")
    settle_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source settle time")
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Imported source file")
    source_row_no: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Imported source row number")
    row_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Normalized row hash")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Normalized raw source row")
