from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from app.db.session import SessionLocal
from app.models.point_flow import MemberPointFlow
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

POINT_FLOW_EXPORT_FIELDS = [
    "flowNo",
    "createTime",
    "consumeAmount",
    "consumeTime",
    "plazaBuId",
    "plazaBuName",
    "storeBuId",
    "storeCode",
    "storeBuName",
    "outTradeNo",
    "memberName",
    "memberPhone",
    "memberId",
    "pointOperate",
    "changePointAmount",
    "signedChangePoints",
    "changeTypeCode",
    "changeTypeName",
    "businessTypeName",
    "sourceCode",
    "sourceName",
    "remark",
    "pointRate",
    "marketActivityNo",
    "marketActivityType",
    "marketActivityName",
    "extra",
    "expireTime",
    "currentEffectiveAmount",
    "pointRatio",
]


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


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _decimal_to_export_number(value: Decimal | None) -> int | float | str:
    if value is None:
        return ""
    if value == value.to_integral_value():
        return int(value)
    normalized = value.normalize()
    exponent = normalized.as_tuple().exponent
    scale = abs(exponent) if exponent < 0 else 0
    digits = min(max(scale, 1), 4)
    return round(float(value), digits)


def _consume_amount_to_cents(record: MemberPointFlow) -> int | str:
    if record.consume_amount_raw is not None:
        return record.consume_amount_raw
    if record.consume_amount is None:
        return ""
    return int((record.consume_amount * Decimal("100")).quantize(Decimal("1")))


def _serialize_json_like(value: Any) -> Any:
    if value is None:
        return ""
    return value


def _record_to_export_row(record: MemberPointFlow) -> dict[str, Any]:
    return {
        "flowNo": record.flow_no or "",
        "createTime": _format_datetime(record.create_time),
        "consumeAmount": _consume_amount_to_cents(record),
        "consumeTime": _format_datetime(record.consume_time),
        "plazaBuId": record.plaza_bu_id or "",
        "plazaBuName": record.plaza_name or "",
        "storeBuId": record.store_bu_id or "",
        "storeCode": record.store_code or "",
        "storeBuName": record.store_bu_name or "",
        "outTradeNo": record.out_trade_no or "",
        "memberName": record.member_name or "",
        "memberPhone": record.mobile_no or "",
        "memberId": record.member_id or "",
        "pointOperate": record.point_operate or "",
        "changePointAmount": _decimal_to_export_number(record.change_point_amount),
        "signedChangePoints": _decimal_to_export_number(record.signed_change_points),
        "changeTypeCode": record.change_type_code or "",
        "changeTypeName": record.change_type_name or "",
        "businessTypeName": record.business_type_name or "",
        "sourceCode": record.source_code or "",
        "sourceName": record.source_name or "",
        "remark": record.remark or "",
        "pointRate": _decimal_to_export_number(record.point_rate),
        "marketActivityNo": record.market_activity_no or "",
        "marketActivityType": record.market_activity_type or "",
        "marketActivityName": record.market_activity_name or "",
        "extra": _serialize_json_like(record.extra),
        "expireTime": _format_datetime(record.expire_time),
        "currentEffectiveAmount": _decimal_to_export_number(record.current_effective_amount),
        "pointRatio": _decimal_to_export_number(record.point_ratio),
    }


def _load_export_fields() -> list[str]:
    sample_fields = load_fields_from_sample()
    if not sample_fields:
        return POINT_FLOW_EXPORT_FIELDS
    return sample_fields


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
        raise RuntimeError(client.last_login_error or "ICSP login failed, please check username or password.")
    if not client.ensure_authenticated_session(username):
        raise RuntimeError("ICSP login succeeded, but user context could not be established.")
    if logger:
        logger("INFO", "Starting points flow data fetch.")
    rows = client.fetch_point_flow(start_date, end_date)
    if logger:
        logger("INFO", "Starting Excel export.")
    output_file = export_to_excel(
        rows=rows,
        fields=_load_export_fields(),
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
        raise RuntimeError(client.last_login_error or "ICSP login failed, please check username or password.")
    if not client.has_serializable_auth_state():
        client.log("WARN", "[ICSP] reusable session build failed: login succeeded but cookies are empty")
        raise RuntimeError("ICSP login succeeded, but reusable authentication data was not captured.")
    if not client.ensure_authenticated_session(username):
        client.log("WARN", "[ICSP] reusable session build failed: original login session has no user context")
        raise RuntimeError("ICSP login succeeded, but a reusable session could not be established.")

    serialized_auth_state = client.export_auth_state(username)
    recovered_client = ICSPClient.from_auth_state(serialized_auth_state, logger=logger)
    if not recovered_client.has_serializable_auth_state():
        recovered_client.log("WARN", "[ICSP] reusable session build failed: cookies restore failed")
        raise RuntimeError("ICSP login succeeded, but cookie restore failed.")
    if not recovered_client.validate_authenticated_session(username):
        recovered_client.log("WARN", "[ICSP] reusable session build failed: recovered session validation failed")
        raise RuntimeError("ICSP login succeeded, but a reusable session could not be validated.")

    profile = recovered_client.get_profile(username)
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=recovered_client.export_auth_state(profile["username"]),
    )


def refresh_icsp_auth_state(auth_state: dict[str, Any], logger: LoggerCallback | None = None) -> ICSPAuthResult:
    login_username = str(auth_state.get("login_username", "")).strip()
    client = ICSPClient.from_auth_state(auth_state, logger=logger)
    if not client.validate_authenticated_session(login_username):
        raise RuntimeError("Login session is no longer valid, please log in again.")
    profile = client.get_profile(login_username or client.user_info.get("usercode", ""))
    refreshed_auth_state = client.export_auth_state(profile["username"])
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=refreshed_auth_state,
    )


def run_points_flow_export_from_database(
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    logger: LoggerCallback | None = None,
    file_tag: str | None = None,
) -> ExportJobResult:
    start_day = date.fromisoformat(start_date)
    end_day = date.fromisoformat(end_date)
    range_start = datetime.combine(start_day, time.min)
    range_end = datetime.combine(end_day, time.max)

    if logger:
        logger("INFO", "Starting database query for member_point_flow.")

    with SessionLocal() as session:
        business_time = func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)
        statement = (
            select(MemberPointFlow)
            .where(business_time >= range_start, business_time <= range_end)
            .order_by(
                func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time).desc(),
                MemberPointFlow.id.desc(),
            )
        )
        records = list(session.scalars(statement))

    if logger:
        logger("INFO", f"Database query completed, rows={len(records)}")

    rows = [_record_to_export_row(record) for record in records]

    if logger:
        logger("INFO", "Starting Excel export.")

    output_file = export_to_excel(
        rows=rows,
        fields=POINT_FLOW_EXPORT_FIELDS,
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_tag=file_tag,
    )

    if logger:
        logger("SUCCESS", f"Excel export completed: {output_file.name}")

    return ExportJobResult(output_file=output_file, result_count=len(rows))


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
        return run_points_flow_export_from_database(
            start_date=start_date,
            end_date=end_date,
            output_dir=export_dir,
            logger=log_callback,
            file_tag=task_id[:8] if task_id else None,
        )
