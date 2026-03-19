from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin, utcnow


class SyncJobState(TimestampMixin, Base):
    __tablename__ = "sync_job_state"
    __table_args__ = (
        UniqueConstraint("job_name", "job_date", name="uq_sync_job_state_job_name_job_date"),
        Index("ix_sync_job_state_status_job_date", "status", "job_date"),
        Index("ix_sync_job_state_job_name_updated_at", "job_name", "updated_at"),
        {"comment": "Incremental sync job state and retry watermark."},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="Job name")
    job_date: Mapped[date] = mapped_column(Date, nullable=False, comment="Business job date")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", comment="Job status")
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="Retry count after first attempt")
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Last started at")
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Last finished at")
    last_success_start: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Last success start date")
    last_success_end: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Last success end date")
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Last success at")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Last error message")
    request_payload: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Last request payload")
    result_payload: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="Last result payload")
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow, comment="Last heartbeat at")
