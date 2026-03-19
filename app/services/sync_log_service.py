from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.sync import SyncTaskLog


class SyncTaskLogService:
    def __init__(self, db: Session):
        self.db = db

    def create_log(
        self,
        *,
        module_name: str,
        action: str,
        target_type: str | None = None,
        target_value: str | None = None,
        triggered_by: str | None = None,
        triggered_source: str | None = None,
        request_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncTaskLog:
        log = SyncTaskLog(
            module_name=module_name,
            action=action,
            target_type=target_type,
            target_value=target_value,
            triggered_by=triggered_by,
            triggered_source=triggered_source,
            status="running",
            request_payload=request_payload,
            started_at=datetime.utcnow(),
        )
        self.db.add(log)
        self.db.flush()
        if commit:
            self.db.commit()
            self.db.refresh(log)
        return log

    def mark_success(
        self,
        log: SyncTaskLog,
        *,
        result_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncTaskLog:
        log.status = "success"
        log.finished_at = datetime.utcnow()
        log.result_payload = result_payload
        log.error_message = None
        self.db.add(log)
        if commit:
            self.db.commit()
            self.db.refresh(log)
        return log

    def mark_failure(
        self,
        log: SyncTaskLog,
        *,
        error_message: str,
        result_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncTaskLog:
        log.status = "failed"
        log.finished_at = datetime.utcnow()
        log.result_payload = result_payload
        log.error_message = error_message
        self.db.add(log)
        if commit:
            self.db.commit()
            self.db.refresh(log)
        return log
