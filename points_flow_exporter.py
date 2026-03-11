#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from pathlib import Path

from app.services.points_flow_service import (
    DEFAULT_FIELD_FILES,
    ICSP_BASE,
    ICSP_CLIENT_ID,
    ICSP_SALT,
    MAX_PAGE_WORKERS,
    ORG_TYPE_CODE,
    PAGE_SIZE,
    PLAZA_BU_ID,
    PLAZA_CODE,
    POINT_FLOW_URL,
    TENANT_ID,
    ICSPClient,
    export_to_excel,
    format_cell_value,
    load_fields_from_sample,
    run_points_flow_export,
)


@dataclass(slots=True)
class ExportResult:
    file_path: Path
    row_count: int


def run_export(
    username: str,
    password: str,
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    log_callback=None,
    stop_checker=None,
    file_tag: str | None = None,
) -> ExportResult:
    result = run_points_flow_export(
        username=username,
        password=password,
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        logger=log_callback,
        stop_checker=stop_checker,
        file_tag=file_tag,
    )
    return ExportResult(file_path=result.output_file, row_count=result.result_count)


__all__ = [
    "DEFAULT_FIELD_FILES",
    "ICSP_BASE",
    "ICSP_CLIENT_ID",
    "ICSP_SALT",
    "MAX_PAGE_WORKERS",
    "ORG_TYPE_CODE",
    "PAGE_SIZE",
    "PLAZA_BU_ID",
    "PLAZA_CODE",
    "POINT_FLOW_URL",
    "TENANT_ID",
    "ExportResult",
    "ICSPClient",
    "export_to_excel",
    "format_cell_value",
    "load_fields_from_sample",
    "run_export",
]
