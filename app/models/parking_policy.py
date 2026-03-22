from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ParkingPolicyDim(Base):
    __tablename__ = "dim_parking_policy"
    __table_args__ = (
        Index(
            "ix_dim_parking_policy_member_level_start_date_end_date",
            "member_level",
            "start_date",
            "end_date",
        ),
        {"comment": "Parking policy dimension by version and normalized member level."},
    )

    version_id: Mapped[str] = mapped_column(String(32), primary_key=True, comment="Policy version id")
    start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="Version effective start datetime")
    end_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Version effective end datetime")
    member_level: Mapped[str] = mapped_column(String(32), primary_key=True, comment="Normalized member level")
    base_free_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="Base free hours")
    is_diamond_full_free: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Whether this level is full free regardless of duration",
    )
