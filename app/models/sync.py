from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin, utcnow


class SyncTaskLog(TimestampMixin, Base):
    __tablename__ = "sync_task_log"
    __table_args__ = (
        Index("ix_sync_task_log_module_name_status_started_at", "module_name", "status", "started_at"),
        Index("ix_sync_task_log_target_type_target_value", "target_type", "target_value"),
        {"comment": "同步任务日志"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    module_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="模块名称")
    action: Mapped[str] = mapped_column(String(64), nullable=False, comment="操作名称")
    target_type: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="目标类型")
    target_value: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="目标值")
    triggered_by: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="触发用户")
    triggered_source: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="触发来源")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running", comment="执行状态")
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow, comment="开始时间")
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="结束时间")
    request_payload: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="请求参数")
    result_payload: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="执行结果")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True, comment="错误信息")
