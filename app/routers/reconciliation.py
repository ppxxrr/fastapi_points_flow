from __future__ import annotations

import base64
import logging
import mimetypes
import shutil
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse

from app.auth import get_current_auth_session
from app.schemas import (
    ReconciliationConfigResponse,
    ReconciliationParkingCaptchaResponse,
    ReconciliationParkingProjectOptionResponse,
    ReconciliationTaskCreateRequest,
    ReconciliationTaskResponse,
    ReconciliationWechatCsvUploadResponse,
)
from app.services.reconciliation_service import (
    FIXED_RECONCILIATION_USERNAME,
    UPLOAD_FUND_FILENAME,
    UPLOAD_TRADE_FILENAME,
    fetch_parking_captcha,
    list_parking_projects,
)
from app.services.parking_captcha_store import CAPTCHA_TTL_SECONDS
from app.services.reconciliation_task_manager import ReconciliationTaskManager
from app.utils.error_text import normalize_error_text


BASE_DIR = Path(__file__).resolve().parents[2]
EXPORT_DIR = BASE_DIR / "data" / "reconciliation_exports"
UPLOAD_DIR = BASE_DIR / "data" / "reconciliation_uploads"

router = APIRouter(prefix="/api/reconciliation", tags=["reconciliation"])
task_manager = ReconciliationTaskManager(export_dir=EXPORT_DIR, upload_dir=UPLOAD_DIR)
route_logger = logging.getLogger("uvicorn.error")


def _ensure_fixed_username(current_session) -> None:
    if current_session.username.lower() != FIXED_RECONCILIATION_USERNAME.lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"请使用固定账号 {FIXED_RECONCILIATION_USERNAME} 登录后再使用该功能。",
        )


def _validate_csv_file(upload: UploadFile | None, label: str) -> UploadFile:
    if upload is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"请上传{label}。")
    filename = upload.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{label}必须为 CSV 文件。")
    return upload


def serialize_task(task: dict) -> ReconciliationTaskResponse:
    return ReconciliationTaskResponse(
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


@router.get("/config", response_model=ReconciliationConfigResponse)
def get_reconciliation_config(current_session=Depends(get_current_auth_session)) -> ReconciliationConfigResponse:
    _ensure_fixed_username(current_session)
    return ReconciliationConfigResponse(
        fixed_username=FIXED_RECONCILIATION_USERNAME,
        parking_projects=[
            ReconciliationParkingProjectOptionResponse(key=item.key, label=item.label, enable_parking=item.enable_parking)
            for item in list_parking_projects()
        ],
    )


@router.get("/parking-captcha", response_model=ReconciliationParkingCaptchaResponse)
def get_parking_captcha(
    project_key: str = Query(..., min_length=1),
    current_session=Depends(get_current_auth_session),
) -> ReconciliationParkingCaptchaResponse:
    _ensure_fixed_username(current_session)
    try:
        captcha_uuid, image_bytes = fetch_parking_captcha(project_key)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=normalize_error_text(exc)) from exc
    except Exception as exc:
        route_logger.exception("Failed to fetch parking captcha for project=%s", project_key)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="获取停车验证码失败，请稍后重试。") from exc

    return ReconciliationParkingCaptchaResponse(
        project_key=project_key,
        captcha_uuid=captcha_uuid,
        image_base64=base64.b64encode(image_bytes).decode("ascii"),
        expires_in_seconds=CAPTCHA_TTL_SECONDS,
    )


@router.post("/wechat-csvs", response_model=ReconciliationWechatCsvUploadResponse, status_code=status.HTTP_201_CREATED)
def upload_wechat_csvs(
    wechat_fund_csv: UploadFile | None = File(default=None),
    wechat_trade_csv: UploadFile | None = File(default=None),
    current_session=Depends(get_current_auth_session),
) -> ReconciliationWechatCsvUploadResponse:
    _ensure_fixed_username(current_session)
    fund_file = _validate_csv_file(wechat_fund_csv, "微信支付资金账单 CSV")
    trade_file = _validate_csv_file(wechat_trade_csv, "微信支付交易订单 CSV")

    session_id = f"wechat_{uuid4().hex[:12]}"
    session_dir = UPLOAD_DIR / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    fund_path = session_dir / UPLOAD_FUND_FILENAME
    trade_path = session_dir / UPLOAD_TRADE_FILENAME
    with fund_file.file as src, fund_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    with trade_file.file as src, trade_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    route_logger.info(
        "Reconciliation CSV upload session=%s user=%s fund=%s trade=%s",
        session_id,
        current_session.username,
        fund_file.filename,
        trade_file.filename,
    )
    return ReconciliationWechatCsvUploadResponse(
        session_id=session_id,
        fund_file_name=fund_file.filename or UPLOAD_FUND_FILENAME,
        trade_file_name=trade_file.filename or UPLOAD_TRADE_FILENAME,
    )


@router.post("/tasks", response_model=ReconciliationTaskResponse, status_code=status.HTTP_202_ACCEPTED)
def create_reconciliation_task(
    payload: ReconciliationTaskCreateRequest,
    current_session=Depends(get_current_auth_session),
) -> ReconciliationTaskResponse:
    _ensure_fixed_username(current_session)
    if payload.start_date > payload.end_date:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start_date cannot be later than end_date")

    task = task_manager.create_task(
        kind=payload.kind,
        owner_username=current_session.username,
        auth_state=current_session.icsp_auth_state,
        start_date=payload.start_date.isoformat(),
        end_date=payload.end_date.isoformat(),
        project_key=payload.project_key,
        captcha_code=payload.captcha_code,
        captcha_uuid=payload.captcha_uuid,
        upload_session_id=payload.upload_session_id,
    )
    route_logger.info(
        "Reconciliation task created task_id=%s user=%s kind=%s project=%s upload=%s",
        task["task_id"],
        current_session.username,
        payload.kind,
        payload.project_key or "",
        payload.upload_session_id or "",
    )
    return serialize_task(task)


@router.get("/tasks/{task_id}", response_model=ReconciliationTaskResponse)
def get_reconciliation_task(task_id: str, current_session=Depends(get_current_auth_session)) -> ReconciliationTaskResponse:
    _ensure_fixed_username(current_session)
    task = task_manager.get_task(task_id, owner_username=current_session.username)
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="task not found")
    return serialize_task(task)


@router.get("/downloads/{filename}")
def download_reconciliation_file(filename: str, current_session=Depends(get_current_auth_session)) -> FileResponse:
    _ensure_fixed_username(current_session)
    file_path = task_manager.get_download_path_by_filename(filename, owner_username=current_session.username)
    if file_path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="export file not found")

    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(
        path=file_path,
        media_type=media_type or "application/octet-stream",
        filename=file_path.name,
    )
