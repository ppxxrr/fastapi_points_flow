from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.auth import (
    AUTH_COOKIE_NAME,
    auth_session_store,
    clear_auth_cookie,
    get_current_auth_session,
    set_auth_cookie,
)
from app.schemas import AuthLoginRequest, AuthLogoutResponse, AuthUserResponse
from app.services.points_flow_service import PointsFlowExportService


router = APIRouter(prefix="/api/auth", tags=["auth"])
auth_service = PointsFlowExportService()


@router.post("/login", response_model=AuthUserResponse)
def login(payload: AuthLoginRequest, response: Response) -> AuthUserResponse:
    try:
        auth_result = auth_service.authenticate_user(
            username=payload.username,
            password=payload.password,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    session = auth_session_store.create_session(
        username=auth_result.username,
        display_name=auth_result.display_name,
        user_id=auth_result.user_id,
        user_code=auth_result.user_code,
        icsp_auth_state=auth_result.auth_state,
    )
    set_auth_cookie(response, session.session_id)
    return AuthUserResponse(**session.to_public_dict())


@router.post("/logout", response_model=AuthLogoutResponse)
def logout(request: Request, response: Response) -> AuthLogoutResponse:
    session_id = request.cookies.get(AUTH_COOKIE_NAME)
    if session_id:
        auth_session_store.delete_session(session_id)
    clear_auth_cookie(response)
    return AuthLogoutResponse(success=True)


@router.get("/me", response_model=AuthUserResponse)
def me(current_session=Depends(get_current_auth_session)) -> AuthUserResponse:
    return AuthUserResponse(**current_session.to_public_dict())
