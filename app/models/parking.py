from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin


class ParkingRecord(TimestampMixin, Base):
    __tablename__ = "parking_record"
    __table_args__ = (
        UniqueConstraint("event_key", name="uq_parking_record_event_key"),
        UniqueConstraint("record_id", name="uq_parking_record_record_id"),
        Index("ix_parking_record_mobile_no_exit_time", "mobile_no", "exit_time"),
        Index("ix_parking_record_member_id_exit_time", "member_id", "exit_time"),
        Index("ix_parking_record_plate_no_exit_time", "plate_no", "exit_time"),
        Index("ix_parking_record_entry_time", "entry_time"),
        Index("ix_parking_record_exit_time", "exit_time"),
        Index("ix_parking_record_parking_serial_no", "parking_serial_no"),
        Index("ix_parking_record_plaza_bu_id_exit_time", "plaza_bu_id", "exit_time"),
        Index("ix_parking_record_plaza_name_exit_time", "plaza_name", "exit_time"),
        {"comment": "Parking entry and exit records imported from CSV exports."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(64), nullable=False, comment="Fallback idempotency key")
    record_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Source record id")
    parking_serial_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Parking serial number")
    mobile_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Matched mobile number")
    member_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Resolved member business id")
    plaza_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Resolved plaza BU id")
    plaza_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Parking plaza name")
    plate_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Plate number")
    entry_plate_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Entry plate number")
    plate_color: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Plate color")
    plate_type: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Plate type")
    vehicle_type_code: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Vehicle type code")
    vehicle_type_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Vehicle type name")
    vehicle_type_name_2: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="Secondary vehicle type",
    )
    entry_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Entry time")
    exit_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Exit time")
    parking_duration_seconds: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Parking duration in seconds",
    )
    status: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Parking status")
    card_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Card number")
    card_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Card id")
    ticket_no: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Ticket number")
    entry_image_url: Mapped[str | None] = mapped_column(String(1000), nullable=True, comment="Entry image url")
    entry_channel: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Entry channel")
    entry_guard_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Entry guard name")
    exit_image_url: Mapped[str | None] = mapped_column(String(1000), nullable=True, comment="Exit image url")
    exit_channel: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Exit channel")
    exit_guard_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Exit guard name")
    auto_pay_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="Auto pay flag")
    total_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Total fee in cents")
    discount_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Discount fee in cents")
    online_pay_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Online pay fee in cents")
    balance_pay_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Balance pay fee in cents")
    cash_pay_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Cash pay fee in cents")
    prepaid_fee_cent: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Prepaid fee in cents")
    merchant_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Merchant number")
    parking_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Parking lot uuid")
    created_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source created time")
    updated_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Source updated time")
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Imported source file")
    source_row_no: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Imported source row number")
    row_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Normalized row hash")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Normalized raw source row")
