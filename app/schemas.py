from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class ExportTaskCreateRequest(BaseModel):
    username: str = Field(..., min_length=1, description="ICSP username")
    password: str = Field(..., min_length=1, description="ICSP password")
    start_date: date
    end_date: date


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
