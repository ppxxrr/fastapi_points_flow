from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.common import TimestampMixin


class DeviceLayoutPoint(TimestampMixin, Base):
    __tablename__ = "device_layout_point"
    __table_args__ = (
        UniqueConstraint("point_type", "point_code", name="uq_device_layout_point_point_type_point_code"),
        Index("ix_device_layout_point_point_type_floor_code", "point_type", "floor_code"),
        {"comment": "Device layout points and normalized map coordinates."},
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    point_type: Mapped[str] = mapped_column(String(32), nullable=False, comment="Point type key")
    point_code: Mapped[str] = mapped_column(String(64), nullable=False, comment="Point code")
    point_name: Mapped[str] = mapped_column(String(255), nullable=False, comment="Point display name")
    floor_code: Mapped[str] = mapped_column(String(16), nullable=False, comment="Floor code")
    x_ratio: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 6),
        nullable=True,
        comment="Normalized X coordinate ratio (0-1)",
    )
    y_ratio: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 6),
        nullable=True,
        comment="Normalized Y coordinate ratio (0-1)",
    )
    source_file: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Latest import source file")
