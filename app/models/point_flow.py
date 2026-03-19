from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin


class MemberPointFlow(TimestampMixin, Base):
    __tablename__ = "member_point_flow"
    __table_args__ = (
        UniqueConstraint("event_key", name="uq_member_point_flow_event_key"),
        Index("uq_member_point_flow_flow_no", "flow_no", unique=True),
        Index("ix_member_point_flow_mobile_no_consume_time", "mobile_no", "consume_time"),
        Index("ix_member_point_flow_member_id_consume_time", "member_id", "consume_time"),
        Index("ix_member_point_flow_create_time", "create_time"),
        Index("ix_member_point_flow_plaza_bu_id_create_time", "plaza_bu_id", "create_time"),
        Index("ix_member_point_flow_plaza_name_create_time", "plaza_name", "create_time"),
        {"comment": "Member point flow records imported from ICSP or CSV exports."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(64), nullable=False, comment="Fallback idempotency key")
    flow_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Point flow number")
    member_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Member business id")
    member_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Member name")
    mobile_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Member mobile number")
    out_trade_no: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="External trade number")
    plaza_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Plaza BU id")
    plaza_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Plaza name")
    store_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Store BU id")
    store_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Store code")
    store_bu_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Store name")
    point_operate: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Point operation")
    change_point_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Changed point amount",
    )
    signed_change_points: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Signed changed points",
    )
    current_effective_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Current effective point balance",
    )
    consume_amount_raw: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Consume amount in cents")
    consume_amount: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Consume amount in yuan",
    )
    point_rate: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True, comment="Point rate")
    point_ratio: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True, comment="Point ratio")
    change_type_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Change type code")
    change_type_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Change type name")
    business_type_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Business type name")
    source_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Source code")
    source_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Source name")
    remark: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Remark")
    market_activity_no: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="Marketing activity number",
    )
    market_activity_type: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="Marketing activity type",
    )
    market_activity_name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Marketing activity name",
    )
    create_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Flow create time")
    consume_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Consume time")
    expire_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Expire time")
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Imported source file")
    source_row_no: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Imported source row number")
    row_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Normalized row hash")
    extra: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Source extra payload")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Normalized raw source row")
