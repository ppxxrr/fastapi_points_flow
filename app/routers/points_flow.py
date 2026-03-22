from __future__ import annotations

import logging
import mimetypes
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from app.auth import get_current_auth_session
from app.schemas import ExportTaskCreateRequest, ExportTaskResponse
from app.services.points_flow_service import PointsFlowExportService
from app.task_manager import TaskManager


BASE_DIR = Path(__file__).resolve().parents[2]
EXPORT_DIR = BASE_DIR / "data" / "exports"

router = APIRouter(prefix="/api/points-flow", tags=["points-flow"])
task_manager = TaskManager(export_dir=EXPORT_DIR, service=PointsFlowExportService())
route_logger = logging.getLogger("uvicorn.error")


def task_log_callback(level: str, message: str) -> None:
    normalized = level.upper()
    if normalized == "ERROR":
        route_logger.error(message)
        return
    if normalized in {"WARN", "WARNING"}:
        route_logger.warning(message)
        return
    route_logger.info(message)


def serialize_task(task: dict) -> ExportTaskResponse:
    return ExportTaskResponse(
        task_id=task["task_id"],
        type=task["type"],
        status=task["status"],
        created_at=task["created_at"],
        updated_at=task["updated_at"],
        params=task["params"],
        logs=task.get("logs", []),
        result_file=task.get("result_file"),
        result_count=task.get("result_count", 0),
        error=task.get("error"),
    )


@router.post("/tasks", response_model=ExportTaskResponse, status_code=status.HTTP_202_ACCEPTED)
def create_points_flow_task(
    payload: ExportTaskCreateRequest,
    current_session=Depends(get_current_auth_session),
) -> ExportTaskResponse:
    if payload.start_date > payload.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be later than end_date",
        )

    task = task_manager.create_task(
        owner_username=current_session.username,
        auth_state=current_session.icsp_auth_state,
        start_date=payload.start_date.isoformat(),
        end_date=payload.end_date.isoformat(),
    )
    route_logger.info("Points-flow task created task_id=%s user=%s source=database", task["task_id"], current_session.username)
    return serialize_task(task)


@router.get("/tasks/{task_id}", response_model=ExportTaskResponse)
def get_points_flow_task(task_id: str, current_session=Depends(get_current_auth_session)) -> ExportTaskResponse:
    task = task_manager.get_task(task_id, owner_username=current_session.username)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return serialize_task(task)


@router.get("/downloads/{filename}")
def download_points_flow_file(filename: str, current_session=Depends(get_current_auth_session)) -> FileResponse:
    file_path = task_manager.get_download_path_by_filename(filename, owner_username=current_session.username)
    if file_path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="export file not found")

    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(
        path=file_path,
        media_type=media_type or "application/octet-stream",
        filename=file_path.name,
    )
