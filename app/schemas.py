from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field
from typing import Literal


class ExportTaskCreateRequest(BaseModel):
    start_date: date
    end_date: date


class AuthLoginRequest(BaseModel):
    username: str = Field(..., min_length=1, description="ICSP username")
    password: str = Field(..., min_length=1, description="ICSP password")


class AuthUserResponse(BaseModel):
    username: str
    display_name: str
    user_id: str
    user_code: str
    created_at: str
    updated_at: str


class AuthLogoutResponse(BaseModel):
    success: bool


class TaskParamsResponse(BaseModel):
    username: str
    start_date: str
    end_date: str


class TaskLogResponse(BaseModel):
    time: str
    level: str
    message: str


class ExportTaskResponse(BaseModel):
    task_id: str
    type: str
    status: str
    created_at: str
    updated_at: str
    params: TaskParamsResponse
    logs: list[TaskLogResponse] = Field(default_factory=list)
    result_file: str | None = None
    result_count: int = 0
    error: str | None = None


class ReconciliationTaskCreateRequest(BaseModel):
    kind: Literal["new_icsp_dz", "coupon_tool"]
    start_date: date
    end_date: date
    project_key: str | None = None
    captcha_code: str | None = None
    captcha_uuid: str | None = None
    upload_session_id: str | None = None


class ReconciliationTaskParamsResponse(BaseModel):
    kind: Literal["new_icsp_dz", "coupon_tool"]
    start_date: str
    end_date: str
    project_key: str | None = None
    upload_session_id: str | None = None


class ReconciliationTaskResponse(BaseModel):
    task_id: str
    type: str
    status: str
    created_at: str
    updated_at: str
    params: ReconciliationTaskParamsResponse
    logs: list[TaskLogResponse] = Field(default_factory=list)
    result_file: str | None = None
    result_count: int = 0
    error: str | None = None


class ReconciliationParkingProjectOptionResponse(BaseModel):
    key: str
    label: str
    enable_parking: bool


class ReconciliationConfigResponse(BaseModel):
    fixed_username: str
    parking_projects: list[ReconciliationParkingProjectOptionResponse] = Field(default_factory=list)


class ReconciliationParkingCaptchaResponse(BaseModel):
    project_key: str
    captcha_uuid: str
    image_base64: str
    expires_in_seconds: int = 0


class ReconciliationWechatCsvUploadResponse(BaseModel):
    session_id: str
    fund_file_name: str
    trade_file_name: str


class DeviceLayoutPointTypeResponse(BaseModel):
    key: str
    label: str


class DeviceLayoutFloorResponse(BaseModel):
    code: str
    label: str
    image_width: int
    image_height: int


class DeviceLayoutConfigResponse(BaseModel):
    default_point_type: str
    default_floor_code: str
    point_types: list[DeviceLayoutPointTypeResponse] = Field(default_factory=list)
    floors: list[DeviceLayoutFloorResponse] = Field(default_factory=list)


class DeviceLayoutPointResponse(BaseModel):
    point_type: str
    point_code: str
    point_name: str
    floor_code: str
    x_ratio: float | None = None
    y_ratio: float | None = None


class DeviceLayoutPointsResponse(BaseModel):
    point_type: str
    points: list[DeviceLayoutPointResponse] = Field(default_factory=list)


class DeviceLayoutPointSaveItem(BaseModel):
    point_code: str = Field(..., min_length=1)
    point_name: str | None = None
    floor_code: str = Field(..., min_length=1)
    x_ratio: float | None = Field(default=None, ge=0, le=1)
    y_ratio: float | None = Field(default=None, ge=0, le=1)


class DeviceLayoutSaveRequest(BaseModel):
    point_type: str = Field(..., min_length=1)
    points: list[DeviceLayoutPointSaveItem] = Field(default_factory=list)


class DeviceLayoutSaveResponse(BaseModel):
    point_type: str
    saved_count: int


class DeviceLayoutImportResponse(BaseModel):
    point_type: str
    total_rows: int
    created_count: int
    updated_count: int
    skipped_count: int


class MessageBoardEntryCreateRequest(BaseModel):
    request_name: str = Field(..., min_length=2, max_length=200)
    detail: str = Field(..., min_length=10, max_length=5000)
    system_name: str = Field(..., min_length=2, max_length=120)
    expected_completion_date: date | None = None


class MessageBoardEntryCreateResponse(BaseModel):
    id: int
    status: str
    created_at: str
    message: str


class MemberSyncByMobileRequest(BaseModel):
    mobile_no: str = Field(..., min_length=1, description="Member mobile number")


class MemberSyncByMemberIdRequest(BaseModel):
    member_id: str = Field(..., min_length=1, description="Member business id")


class MemberSyncSummaryResponse(BaseModel):
    member_id: str
    profile_upserted: bool
    account_upserted: bool
    attr_count: int
    level_dict_upserts: int
    change_logs_inserted: int
    change_logs_updated: int
    periods_rebuilt: int
    warnings: list[str] = Field(default_factory=list)


class MemberFetchSyncResponse(BaseModel):
    requested_mobile_no: str | None = None
    matched_member_ids: list[str] = Field(default_factory=list)
    summaries: list[MemberSyncSummaryResponse] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ToolFileResponse(BaseModel):
    file_id: str
    file_name: str
    size_bytes: int


class PdfEditorPageResponse(BaseModel):
    page_id: str
    file_id: str
    source_file_name: str
    page_index: int
    page_number: int
    width: float
    height: float
    thumbnail_url: str


class PdfEditorDocumentResponse(ToolFileResponse):
    page_count: int
    pages: list[PdfEditorPageResponse] = Field(default_factory=list)


class PdfEditorUploadResponse(BaseModel):
    session_id: str
    documents: list[PdfEditorDocumentResponse] = Field(default_factory=list)
    total_pages: int


class PdfEditorPageRefRequest(BaseModel):
    file_id: str = Field(..., min_length=1)
    page_index: int = Field(..., ge=0)


class PdfEditorOverlayRequest(BaseModel):
    kind: Literal["text", "image", "erase"]
    x: float = Field(..., ge=0)
    y: float = Field(..., ge=0)
    width: float = Field(..., gt=0)
    height: float = Field(..., gt=0)
    text: str | None = None
    font_size: float | None = Field(default=None, gt=0)
    data_url: str | None = None


class PdfEditorPageMergeRequest(PdfEditorPageRefRequest):
    overlays: list[PdfEditorOverlayRequest] = Field(default_factory=list)


class PdfEditorMergeRequest(BaseModel):
    session_id: str = Field(..., min_length=1)
    pages: list[PdfEditorPageMergeRequest] = Field(default_factory=list)


class ToolDownloadResponse(BaseModel):
    session_id: str
    file_name: str
    download_url: str
    content_type: str


class BankInfoUploadResponse(BaseModel):
    session_id: str
    files: list[ToolFileResponse] = Field(default_factory=list)


class BankInfoProcessRequest(BaseModel):
    session_id: str = Field(..., min_length=1)


class BankInfoProcessedFileResponse(BaseModel):
    file_name: str
    output_file_name: str | None = None
    download_url: str | None = None
    success: bool
    messages: list[str] = Field(default_factory=list)


class BankInfoProcessResponse(BaseModel):
    session_id: str
    processed_count: int
    success_count: int
    failed_count: int
    files: list[BankInfoProcessedFileResponse] = Field(default_factory=list)
    download_url: str | None = None


class PptConverterUploadFileResponse(ToolFileResponse):
    slide_count: int


class PptConverterUploadResponse(BaseModel):
    session_id: str
    files: list[PptConverterUploadFileResponse] = Field(default_factory=list)
    total_slides: int


class PptConverterProcessRequest(BaseModel):
    session_id: str = Field(..., min_length=1)


class PptConverterProcessStartResponse(BaseModel):
    session_id: str
    job_id: str
    status: str


class PptConverterProcessedFileResponse(BaseModel):
    file_name: str
    slide_count: int
    output_file_name: str | None = None
    download_url: str | None = None
    success: bool
    messages: list[str] = Field(default_factory=list)


class PptConverterJobStatusResponse(BaseModel):
    session_id: str
    job_id: str
    status: str
    total_files: int
    processed_files: int
    total_slides: int
    processed_slides: int
    progress_percent: float
    current_file_name: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    files: list[PptConverterProcessedFileResponse] = Field(default_factory=list)


class K2PrintStartRequest(BaseModel):
    k2_no: str = Field(..., min_length=1)


class K2PrintJobStartResponse(BaseModel):
    job_id: str
    status: str


class K2PrintJobLogResponse(BaseModel):
    at: str
    stage: str
    level: str
    message: str


class K2PrintJobStatusResponse(BaseModel):
    job_id: str
    k2_no: str
    status: str
    stage: str
    started_at: str | None = None
    finished_at: str | None = None
    resolved_workflow_url: str | None = None
    resolved_print_url: str | None = None
    download_url: str | None = None
    error: str | None = None
    logs: list[K2PrintJobLogResponse] = Field(default_factory=list)


class AdminParkingSyncRunRequest(BaseModel):
    job_date: date | None = None
    force: bool = True


class AdminParkingSyncRunResponse(BaseModel):
    status: str
    job_date: str
    detail: str


class AdminParkingTradeSyncRunRequest(BaseModel):
    job_date: date | None = None
    force: bool = True


class AdminParkingTradeSyncRunResponse(BaseModel):
    status: str
    job_date: str
    detail: str
