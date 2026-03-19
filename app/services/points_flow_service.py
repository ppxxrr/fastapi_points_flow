from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services.icsp_client import (
    ICSP_BASE,
    ICSP_CLIENT_ID,
    ICSP_SALT,
    ORG_TYPE_CODE,
    PLAZA_BU_ID,
    PLAZA_CODE,
    POINT_FLOW_MAX_PAGE_WORKERS,
    POINT_FLOW_PAGE_SIZE,
    POINT_FLOW_URL,
    TENANT_ID,
    ICSPClient,
    LoggerCallback,
    StopChecker,
)
from app.utils.excel_export import export_to_excel, load_fields_from_sample


MAX_PAGE_WORKERS = POINT_FLOW_MAX_PAGE_WORKERS
PAGE_SIZE = POINT_FLOW_PAGE_SIZE


@dataclass(slots=True)
class ExportJobResult:
    output_file: Path
    result_count: int


@dataclass(slots=True)
class ICSPAuthResult:
    username: str
    display_name: str
    user_id: str
    user_code: str
    auth_state: dict[str, Any]


def run_points_flow_export(
    username: str,
    password: str,
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    logger: LoggerCallback | None = None,
    stop_checker: StopChecker | None = None,
    file_tag: str | None = None,
) -> ExportJobResult:
    if logger:
        logger("INFO", "Starting login to ICSP.")
    client = ICSPClient(logger=logger, stop_checker=stop_checker)
    if not client.login(username, password):
        raise RuntimeError(client.last_login_error or "ICSP 登录失败，请检查账号或密码。")
    if not client.ensure_authenticated_session(username):
        raise RuntimeError("ICSP 登录成功，但未能获取用户上下文，请稍后重试。")
    if logger:
        logger("INFO", "Starting points flow data fetch.")
    rows = client.fetch_point_flow(start_date, end_date)
    if logger:
        logger("INFO", "Starting Excel export.")
    output_file = export_to_excel(
        rows=rows,
        fields=load_fields_from_sample(),
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_tag=file_tag,
    )
    if logger:
        logger("SUCCESS", f"Excel export completed: {output_file.name}")
    return ExportJobResult(output_file=output_file, result_count=len(rows))


def authenticate_icsp_user(
    username: str,
    password: str,
    logger: LoggerCallback | None = None,
) -> ICSPAuthResult:
    client = ICSPClient(logger=logger)
    if not client.login(username, password):
        raise RuntimeError(client.last_login_error or "ICSP 登录失败，请检查账号或密码。")
    if not client.has_serializable_auth_state():
        client.log("WARN", "[ICSP] reusable session build failed: login succeeded but cookies are empty")
        raise RuntimeError("ICSP 登录成功，但未获取到可复用的认证信息，请稍后重试。")
    if not client.ensure_authenticated_session(username):
        client.log("WARN", "[ICSP] reusable session build failed: original login session has no user context")
        raise RuntimeError("ICSP 登录成功，但未能建立可复用会话，请稍后重试。")

    serialized_auth_state = client.export_auth_state(username)
    recovered_client = ICSPClient.from_auth_state(serialized_auth_state, logger=logger)
    if not recovered_client.has_serializable_auth_state():
        recovered_client.log("WARN", "[ICSP] reusable session build failed: cookies restore failed")
        raise RuntimeError("ICSP 登录成功，但 cookies 恢复失败，请稍后重试。")
    if not recovered_client.validate_authenticated_session(username):
        recovered_client.log("WARN", "[ICSP] reusable session build failed: recovered session validation failed")
        raise RuntimeError("ICSP 登录成功，但未能建立可复用会话，请稍后重试。")

    profile = recovered_client.get_profile(username)
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=recovered_client.export_auth_state(profile["username"]),
    )


def run_points_flow_export_with_auth_state(
    auth_state: dict[str, Any],
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    logger: LoggerCallback | None = None,
    stop_checker: StopChecker | None = None,
    file_tag: str | None = None,
) -> ExportJobResult:
    client = ICSPClient.from_auth_state(auth_state, logger=logger, stop_checker=stop_checker)
    login_username = str(auth_state.get("login_username", "")).strip()
    if not client.validate_authenticated_session(login_username):
        raise RuntimeError("登录失效，请重新登录")
    if logger:
        logger("INFO", "Using authenticated ICSP session.")
        logger("INFO", "Starting points flow data fetch.")
    rows = client.fetch_point_flow(start_date, end_date)
    if logger:
        logger("INFO", "Starting Excel export.")
    output_file = export_to_excel(
        rows=rows,
        fields=load_fields_from_sample(),
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_tag=file_tag,
    )
    if logger:
        logger("SUCCESS", f"Excel export completed: {output_file.name}")
    return ExportJobResult(output_file=output_file, result_count=len(rows))


def refresh_icsp_auth_state(auth_state: dict[str, Any], logger: LoggerCallback | None = None) -> ICSPAuthResult:
    login_username = str(auth_state.get("login_username", "")).strip()
    client = ICSPClient.from_auth_state(auth_state, logger=logger)
    if not client.validate_authenticated_session(login_username):
        raise RuntimeError("登录失效，请重新登录")
    profile = client.get_profile(login_username or client.user_info.get("usercode", ""))
    refreshed_auth_state = client.export_auth_state(profile["username"])
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=refreshed_auth_state,
    )


class PointsFlowExportService:
    def authenticate_user(
        self,
        username: str,
        password: str,
        log_callback: LoggerCallback | None = None,
    ) -> ICSPAuthResult:
        if log_callback:
            log_callback("INFO", "Starting ICSP login verification.")
        return authenticate_icsp_user(username=username, password=password, logger=log_callback)

    def refresh_authenticated_session(
        self,
        auth_state: dict[str, Any],
        log_callback: LoggerCallback | None = None,
    ) -> ICSPAuthResult:
        return refresh_icsp_auth_state(auth_state=auth_state, logger=log_callback)

    def run_export(
        self,
        task_id: str,
        auth_state: dict[str, Any],
        start_date: str,
        end_date: str,
        export_dir: str | Path,
        log_callback: LoggerCallback | None = None,
    ) -> ExportJobResult:
        return run_points_flow_export_with_auth_state(
            auth_state=auth_state,
            start_date=start_date,
            end_date=end_date,
            output_dir=export_dir,
            logger=log_callback,
            file_tag=task_id[:8] if task_id else None,
        )
