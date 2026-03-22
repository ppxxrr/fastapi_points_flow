from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin


class TrafficNodeDailyFlow(TimestampMixin, Base):
    __tablename__ = "traffic_node_daily_flow"
    __table_args__ = (
        UniqueConstraint("node_code", "business_date", name="uq_traffic_node_daily_flow_node_code_business_date"),
        Index("ix_traffic_node_daily_flow_business_date", "business_date"),
        Index("ix_traffic_node_daily_flow_node_code_business_date", "node_code", "business_date"),
        {"comment": "Daily passenger flow by traffic node imported from legacy traffic_data.db or API sync."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    node_code: Mapped[str] = mapped_column(String(64), nullable=False, comment="Traffic node code")
    node_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Traffic node name")
    business_date: Mapped[date] = mapped_column(Date, nullable=False, comment="Business date")
    passenger_flow: Mapped[int] = mapped_column(Integer, nullable=False, comment="Passenger flow count")
    passenger_flow_ratio: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 6),
        nullable=True,
        comment="Passenger flow contribution ratio",
    )
    source_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        comment="Updated time from source system",
    )
    source_origin: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="sqlite_import or api_sync")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Raw source payload")


class RailinliProbeDailyFlow(TimestampMixin, Base):
    __tablename__ = "railinli_probe_daily_flow"
    __table_args__ = (
        UniqueConstraint("probe_id", "business_date", name="uq_railinli_probe_daily_flow_probe_id_business_date"),
        Index("ix_railinli_probe_daily_flow_business_date", "business_date"),
        Index("ix_railinli_probe_daily_flow_probe_id_business_date", "probe_id", "business_date"),
        {"comment": "Daily entry counts by railinli probe imported from legacy railinliKL.db or receiver uploads."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    probe_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="Probe id")
    probe_name: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Probe display name")
    business_date: Mapped[date] = mapped_column(Date, nullable=False, comment="Business date")
    entry_count: Mapped[int] = mapped_column(Integer, nullable=False, comment="Daily entry count")
    source_record_time: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        comment="Original record time from source database",
    )
    source_origin: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="sqlite_import or receiver_upload",
    )
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Raw source payload")
