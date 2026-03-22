from __future__ import annotations

import logging
import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.services.reconciliation_service import (
    ReconciliationJobResult,
    run_coupon_tool_export,
    run_new_icsp_dz_export,
)
from app.utils.error_text import normalize_error_text

task_logger = logging.getLogger("uvicorn.error")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class ReconciliationTaskParams:
    kind: str
    start_date: str
    end_date: str
    project_key: str | None = None
    upload_session_id: str | None = None


@dataclass(slots=True)
class ReconciliationTaskLog:
    time: str
    level: str
    message: str


@dataclass(slots=True)
class ReconciliationTaskRecord:
    task_id: str
    type: str
    status: str
    created_at: str
    updated_at: str
    owner_username: str
    params: ReconciliationTaskParams
    logs: list[ReconciliationTaskLog] = field(default_factory=list)
    result_file: str | None = None
    result_count: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("owner_username", None)
        return payload


class ReconciliationTaskManager:
    def __init__(self, export_dir: str | Path, upload_dir: str | Path):
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.upload_dir = Path(upload_dir)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self._tasks: dict[str, ReconciliationTaskRecord] = {}
        self._lock = threading.Lock()

    def create_task(
        self,
        *,
        kind: str,
        owner_username: str,
        auth_state: dict[str, Any],
        start_date: str,
        end_date: str,
        project_key: str | None = None,
        captcha_code: str | None = None,
        captcha_uuid: str | None = None,
        upload_session_id: str | None = None,
    ) -> dict[str, Any]:
        task_id = uuid4().hex
        now = utc_now_iso()
        task = ReconciliationTaskRecord(
            task_id=task_id,
            type=kind,
            status="pending",
            created_at=now,
            updated_at=now,
            owner_username=owner_username,
            params=ReconciliationTaskParams(
                kind=kind,
                start_date=start_date,
                end_date=end_date,
                project_key=project_key,
                upload_session_id=upload_session_id,
            ),
        )

        with self._lock:
            self._tasks[task_id] = task

        self.append_log(task, "INFO", "Task created.")
        worker = threading.Thread(
            target=self._run_task,
            args=(task, deepcopy(auth_state), start_date, end_date, project_key, captcha_code, captcha_uuid, upload_session_id),
            daemon=True,
            name=f"reconciliation-{kind}-{task_id[:8]}",
        )
        worker.start()
        return self.get_task(task_id, owner_username=owner_username)

    def get_task(self, task_id: str, owner_username: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if owner_username is not None and task.owner_username != owner_username:
                return None
            return deepcopy(task.to_dict())

    def get_download_path_by_filename(self, filename: str, owner_username: str | None = None) -> Path | None:
        export_root = self.export_dir.resolve()
        file_path = (export_root / filename).resolve()
        if export_root not in file_path.parents:
            return None
        if not file_path.is_file():
            return None
        with self._lock:
            matched_task = next(
                (
                    task
                    for task in self._tasks.values()
                    if task.result_file == filename
                    and (owner_username is None or task.owner_username == owner_username)
                ),
                None,
            )
        if matched_task is None:
            return None
        return file_path

    def append_log(self, task: ReconciliationTaskRecord, level: str, message: str) -> None:
        now = utc_now_iso()
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.logs.append(ReconciliationTaskLog(time=now, level=level, message=message))
            current.logs = current.logs[-200:]
            current.updated_at = now
        normalized = level.upper()
        if normalized in {"ERROR", "WARN", "WARNING"}:
            task_logger.warning("Reconciliation task %s %s: %s", task.task_id, normalized, message)
        elif normalized in {"SUCCESS"}:
            task_logger.info("Reconciliation task %s %s: %s", task.task_id, normalized, message)

    def _set_status(self, task: ReconciliationTaskRecord, status: str) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = status
            current.updated_at = utc_now_iso()

    def _mark_success(self, task: ReconciliationTaskRecord, result: ReconciliationJobResult) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = "success"
            current.result_file = result.output_file.name
            current.result_count = result.result_count
            current.error = None
            current.updated_at = utc_now_iso()

    def _mark_failed(self, task: ReconciliationTaskRecord, error: str) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = "failed"
            current.error = error
            current.updated_at = utc_now_iso()

    def _run_task(
        self,
        task: ReconciliationTaskRecord,
        auth_state: dict[str, Any],
        start_date: str,
        end_date: str,
        project_key: str | None,
        captcha_code: str | None,
        captcha_uuid: str | None,
        upload_session_id: str | None,
    ) -> None:
        self._set_status(task, "running")
        self.append_log(task, "INFO", "Task execution started.")

        try:
            if task.type == "coupon_tool":
                result = run_coupon_tool_export(
                    auth_state=auth_state,
                    start_date=start_date,
                    end_date=end_date,
                    output_dir=self.export_dir,
                    file_tag=task.task_id[:8],
                    logger=lambda level, message: self.append_log(task, level, message),
                )
            elif task.type == "new_icsp_dz":
                result = run_new_icsp_dz_export(
                    auth_state=auth_state,
                    start_date=start_date,
                    end_date=end_date,
                    output_dir=self.export_dir,
                    project_key=project_key,
                    captcha_code=captcha_code,
                    captcha_uuid=captcha_uuid,
                    upload_root=self.upload_dir,
                    upload_session_id=upload_session_id,
                    file_tag=task.task_id[:8],
                    logger=lambda level, message: self.append_log(task, level, message),
                )
            else:
                raise RuntimeError(f"Unsupported reconciliation task type: {task.type}")

            self._mark_success(task, result)
            self.append_log(task, "SUCCESS", f"Export completed: {result.output_file.name}, rows={result.result_count}")
        except Exception as exc:
            error_text = normalize_error_text(exc)
            self._mark_failed(task, error_text)
            self.append_log(task, "ERROR", f"Task failed: {error_text}")
