from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth import AUTH_COOKIE_NAME, auth_session_store, get_current_auth_session
from app.db.session import get_db_session
from app.schemas import (
    MemberFetchSyncResponse,
    MemberSyncByMemberIdRequest,
    MemberSyncByMobileRequest,
    MemberSyncSummaryResponse,
)
from app.services.icsp_client import ICSPClient
from app.services.member_sync_service import ICSPMemberSyncService, MemberFetchSyncResult, MemberSyncSummary
from app.services.sync_log_service import SyncTaskLogService


router = APIRouter(prefix="/api/member", tags=["member"])
route_logger = logging.getLogger("uvicorn.error")


def member_log_callback(level: str, message: str) -> None:
    normalized = level.upper()
    if normalized == "ERROR":
        route_logger.error(message)
        return
    if normalized in {"WARN", "WARNING"}:
        route_logger.warning(message)
        return
    route_logger.info(message)


def serialize_member_summary(summary: MemberSyncSummary) -> MemberSyncSummaryResponse:
    return MemberSyncSummaryResponse(**summary.to_dict())


def serialize_member_fetch_result(result: MemberFetchSyncResult) -> MemberFetchSyncResponse:
    return MemberFetchSyncResponse(
        requested_mobile_no=result.requested_mobile_no,
        matched_member_ids=result.matched_member_ids,
        summaries=[serialize_member_summary(item) for item in result.summaries],
        warnings=result.warnings,
    )


def build_authenticated_member_client(auth_state: dict, username: str) -> ICSPClient:
    client = ICSPClient.from_auth_state(auth_state, logger=member_log_callback)
    if not client.validate_member_session(username):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="登录失效，请重新登录")
    return client


def refresh_request_auth_state(request: Request, client: ICSPClient, username: str) -> None:
    session_id = request.cookies.get(AUTH_COOKIE_NAME)
    if not session_id:
        return

    profile = client.get_profile(username)
    auth_session_store.update_session_auth_state(
        session_id,
        client.export_auth_state(profile["username"]),
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
    )


@router.post("/sync/by-mobile", response_model=MemberFetchSyncResponse)
def sync_member_by_mobile(
    payload: MemberSyncByMobileRequest,
    request: Request,
    current_session=Depends(get_current_auth_session),
    db: Session = Depends(get_db_session),
) -> MemberFetchSyncResponse:
    client = build_authenticated_member_client(current_session.icsp_auth_state, current_session.username)
    refresh_request_auth_state(request, client, current_session.username)

    sync_log_service = SyncTaskLogService(db)
    sync_log = sync_log_service.create_log(
        module_name="member_info",
        action="sync_by_mobile",
        target_type="mobile_no",
        target_value=payload.mobile_no,
        triggered_by=current_session.username,
        triggered_source="api",
        request_payload={"mobile_no": payload.mobile_no},
        commit=True,
    )

    try:
        service = ICSPMemberSyncService(db=db, icsp_client=client)
        result = service.sync_member_by_mobile(payload.mobile_no, commit=False)
        sync_log_service.mark_success(sync_log, result_payload=result.to_dict(), commit=False)
        db.commit()
        route_logger.info("Member sync by mobile succeeded mobile=%s user=%s", payload.mobile_no, current_session.username)
        return serialize_member_fetch_result(result)
    except Exception as exc:
        db.rollback()
        sync_log_service.mark_failure(sync_log, error_message=str(exc), commit=True)
        route_logger.exception("Member sync by mobile failed mobile=%s user=%s", payload.mobile_no, current_session.username)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.post("/sync/by-member-id", response_model=MemberSyncSummaryResponse)
def sync_member_by_member_id(
    payload: MemberSyncByMemberIdRequest,
    request: Request,
    current_session=Depends(get_current_auth_session),
    db: Session = Depends(get_db_session),
) -> MemberSyncSummaryResponse:
    client = build_authenticated_member_client(current_session.icsp_auth_state, current_session.username)
    refresh_request_auth_state(request, client, current_session.username)

    sync_log_service = SyncTaskLogService(db)
    sync_log = sync_log_service.create_log(
        module_name="member_info",
        action="sync_by_member_id",
        target_type="member_id",
        target_value=payload.member_id,
        triggered_by=current_session.username,
        triggered_source="api",
        request_payload={"member_id": payload.member_id},
        commit=True,
    )

    try:
        service = ICSPMemberSyncService(db=db, icsp_client=client)
        summary = service.sync_member_by_member_id(payload.member_id, commit=False)
        sync_log_service.mark_success(sync_log, result_payload=summary.to_dict(), commit=False)
        db.commit()
        route_logger.info(
            "Member sync by member_id succeeded member_id=%s user=%s",
            payload.member_id,
            current_session.username,
        )
        return serialize_member_summary(summary)
    except Exception as exc:
        db.rollback()
        sync_log_service.mark_failure(sync_log, error_message=str(exc), commit=True)
        route_logger.exception(
            "Member sync by member_id failed member_id=%s user=%s",
            payload.member_id,
            current_session.username,
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
