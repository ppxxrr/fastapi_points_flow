from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.common import TimestampMixin


class MessageBoardEntry(TimestampMixin, Base):
    __tablename__ = "message_board_entry"
    __table_args__ = (
        Index("ix_message_board_entry_system_name_created_at", "system_name", "created_at"),
        Index("ix_message_board_entry_status_created_at", "status", "created_at"),
        {"comment": "Anonymous message board entries for product feedback."},
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_name: Mapped[str] = mapped_column(String(200), nullable=False, comment="Requested feature title")
    detail: Mapped[str] = mapped_column(Text, nullable=False, comment="Detailed request description")
    system_name: Mapped[str] = mapped_column(String(120), nullable=False, comment="Target system name")
    expected_completion_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Expected delivery date")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="new", comment="Message processing status")
