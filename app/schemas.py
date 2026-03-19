from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


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
