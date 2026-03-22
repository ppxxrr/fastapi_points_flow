from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse, Response

from app.auth import get_current_auth_session
from app.db.session import SessionLocal
from app.schemas import (
    DeviceLayoutConfigResponse,
    DeviceLayoutFloorResponse,
    DeviceLayoutImportResponse,
    DeviceLayoutPointResponse,
    DeviceLayoutPointsResponse,
    DeviceLayoutPointTypeResponse,
    DeviceLayoutSaveRequest,
    DeviceLayoutSaveResponse,
)
from app.services.device_layout_service import (
    DeviceLayoutService,
    build_workbook_bytes,
    normalize_point_type,
    resolve_map_path,
)
from app.utils.error_text import normalize_error_text


router = APIRouter(prefix="/api/device-layout", tags=["device-layout"])


def _ensure_authenticated(_current_session=Depends(get_current_auth_session)):
    return _current_session


@router.get("/config", response_model=DeviceLayoutConfigResponse)
def get_device_layout_config(_current_session=Depends(_ensure_authenticated)) -> DeviceLayoutConfigResponse:
    with SessionLocal() as session:
        service = DeviceLayoutService(session)
        payload = service.build_config()
    return DeviceLayoutConfigResponse(
        default_point_type=payload["default_point_type"],
        default_floor_code=payload["default_floor_code"],
        point_types=[DeviceLayoutPointTypeResponse(**item) for item in payload["point_types"]],
        floors=[DeviceLayoutFloorResponse(**item) for item in payload["floors"]],
    )


@router.get("/points", response_model=DeviceLayoutPointsResponse)
def get_device_layout_points(
    point_type: str = Query(..., min_length=1),
    _current_session=Depends(_ensure_authenticated),
) -> DeviceLayoutPointsResponse:
    try:
        normalized_type = normalize_point_type(point_type)
        with SessionLocal() as session:
            service = DeviceLayoutService(session)
            points = service.list_points(normalized_type)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=normalize_error_text(exc)) from exc

    return DeviceLayoutPointsResponse(
        point_type=normalized_type,
        points=[DeviceLayoutPointResponse(**item) for item in points],
    )


@router.post("/points/save", response_model=DeviceLayoutSaveResponse)
def save_device_layout_points(
    payload: DeviceLayoutSaveRequest,
    _current_session=Depends(_ensure_authenticated),
) -> DeviceLayoutSaveResponse:
    try:
        with SessionLocal() as session:
            service = DeviceLayoutService(session)
            saved_count = service.save_points(
                payload.point_type,
                [
                    {
                        "point_code": item.point_code,
                        "point_name": item.point_name,
                        "floor_code": item.floor_code,
                        "x_ratio": item.x_ratio,
                        "y_ratio": item.y_ratio,
                    }
                    for item in payload.points
                ],
            )
        return DeviceLayoutSaveResponse(point_type=normalize_point_type(payload.point_type), saved_count=saved_count)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=normalize_error_text(exc)) from exc


@router.post("/points/import", response_model=DeviceLayoutImportResponse)
def import_device_layout_points(
    file: UploadFile | None = File(default=None),
    point_type: str | None = Form(default=None),
    _current_session=Depends(_ensure_authenticated),
) -> DeviceLayoutImportResponse:
    if file is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请先上传点位模板文件。")
    filename = file.filename or "device_layout_import.xlsx"
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="仅支持 Excel 模板文件。")

    try:
        file_bytes = file.file.read()
        with SessionLocal() as session:
            service = DeviceLayoutService(session)
            summary = service.import_points(
                file_bytes=file_bytes,
                file_name=filename,
                selected_point_type=point_type,
            )
        return DeviceLayoutImportResponse(
            point_type=summary.point_type,
            total_rows=summary.total_rows,
            created_count=summary.created_count,
            updated_count=summary.updated_count,
            skipped_count=summary.skipped_count,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=normalize_error_text(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=normalize_error_text(exc)) from exc


@router.get("/export")
def export_device_layout_points(
    point_type: str = Query(..., min_length=1),
    _current_session=Depends(_ensure_authenticated),
) -> Response:
    try:
        normalized_type = normalize_point_type(point_type)
        with SessionLocal() as session:
            service = DeviceLayoutService(session)
            workbook = service.build_export_workbook(normalized_type)
        content = build_workbook_bytes(workbook)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=normalize_error_text(exc)) from exc

    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="device_layout_{normalized_type}.xlsx"',
        },
    )


@router.get("/maps/{floor_code}")
def get_device_layout_map(
    floor_code: str,
    _current_session=Depends(_ensure_authenticated),
) -> FileResponse:
    try:
        file_path = resolve_map_path(floor_code)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"楼层地图不存在：{Path(exc.filename).name}") from exc

    return FileResponse(path=file_path, media_type="image/png", filename=file_path.name)
