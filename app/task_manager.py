from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from app.services.points_flow_service import ExportJobResult, PointsFlowExportService


POINTS_FLOW_TASK_TYPE = "points_flow_export"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class TaskParams:
    username: str
    start_date: str
    end_date: str


@dataclass(slots=True)
class TaskLog:
    time: str
    level: str
    message: str


@dataclass(slots=True)
class TaskRecord:
    task_id: str
    type: str
    status: str
    created_at: str
    updated_at: str
    params: TaskParams
    logs: list[TaskLog] = field(default_factory=list)
    result_file: str | None = None
    result_count: int = 0
    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class TaskManager:
    def __init__(self, export_dir: str | Path, service: PointsFlowExportService):
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.service = service
        self._tasks: dict[str, TaskRecord] = {}
        self._lock = threading.Lock()

    def create_task(self, username: str, password: str, start_date: str, end_date: str) -> dict:
        task_id = uuid4().hex
        now = utc_now_iso()
        task = TaskRecord(
            task_id=task_id,
            type=POINTS_FLOW_TASK_TYPE,
            status="pending",
            created_at=now,
            updated_at=now,
            params=TaskParams(
                username=username,
                start_date=start_date,
                end_date=end_date,
            ),
        )

        with self._lock:
            self._tasks[task_id] = task

        self.append_log(task, "INFO", "Task created.")

        worker = threading.Thread(
            target=self._run_task,
            args=(task, username, password, start_date, end_date),
            daemon=True,
            name=f"points-flow-{task_id[:8]}",
        )
        worker.start()
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            return deepcopy(task.to_dict())

    def get_download_path_by_filename(self, filename: str) -> Path | None:
        export_root = self.export_dir.resolve()
        file_path = (export_root / filename).resolve()
        if export_root not in file_path.parents:
            return None
        if not file_path.is_file():
            return None
        return file_path

    def append_log(self, task: TaskRecord, level: str, message: str) -> None:
        now = utc_now_iso()
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.logs.append(TaskLog(time=now, level=level, message=message))
            current.logs = current.logs[-200:]
            current.updated_at = now

    def _set_status(self, task: TaskRecord, status: str) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = status
            current.updated_at = utc_now_iso()

    def _mark_success(self, task: TaskRecord, result: ExportJobResult) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = "success"
            current.result_file = result.output_file.name
            current.result_count = result.result_count
            current.error = None
            current.updated_at = utc_now_iso()

    def _mark_failed(self, task: TaskRecord, error: str) -> None:
        with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                return
            current.status = "failed"
            current.error = error
            current.updated_at = utc_now_iso()

    def _run_task(
        self,
        task: TaskRecord,
        username: str,
        password: str,
        start_date: str,
        end_date: str,
    ) -> None:
        self._set_status(task, "running")
        self.append_log(task, "INFO", "Task execution started.")

        try:
            result = self.service.run_export(
                task_id=task.task_id,
                username=username,
                password=password,
                start_date=start_date,
                end_date=end_date,
                export_dir=self.export_dir,
                log_callback=lambda level, message: self.append_log(task, level, message),
            )
            self._mark_success(task, result)
            self.append_log(
                task,
                "SUCCESS",
                f"Export completed: {result.output_file.name}, rows={result.result_count}",
            )
        except Exception as exc:
            self._mark_failed(task, str(exc))
            self.append_log(task, "ERROR", f"Task failed: {exc}")
