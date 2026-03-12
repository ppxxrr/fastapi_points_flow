from __future__ import annotations

import os
import secrets
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, Request, Response, status


AUTH_COOKIE_NAME = os.getenv("AUTH_COOKIE_NAME", "points_flow_session")
AUTH_COOKIE_DOMAIN = os.getenv("AUTH_COOKIE_DOMAIN") or None
AUTH_COOKIE_PATH = os.getenv("AUTH_COOKIE_PATH", "/")
AUTH_COOKIE_SAMESITE = os.getenv("AUTH_COOKIE_SAMESITE", "lax")
AUTH_COOKIE_SECURE = os.getenv("AUTH_COOKIE_SECURE", "false").lower() == "true"
AUTH_SESSION_MAX_AGE = int(os.getenv("AUTH_SESSION_MAX_AGE", "43200"))


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class AuthSession:
    session_id: str
    username: str
    display_name: str
    user_id: str
    user_code: str
    created_at: str
    updated_at: str
    expires_at: float
    icsp_auth_state: dict[str, Any]

    def to_public_dict(self) -> dict[str, str]:
        return {
            "username": self.username,
            "display_name": self.display_name,
            "user_id": self.user_id,
            "user_code": self.user_code,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class AuthSessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, AuthSession] = {}
        self._lock = threading.Lock()

    def create_session(
        self,
        username: str,
        display_name: str,
        user_id: str,
        user_code: str,
        icsp_auth_state: dict[str, Any],
    ) -> AuthSession:
        now = utc_now_iso()
        session = AuthSession(
            session_id=secrets.token_urlsafe(32),
            username=username,
            display_name=display_name,
            user_id=user_id,
            user_code=user_code,
            created_at=now,
            updated_at=now,
            expires_at=time.time() + AUTH_SESSION_MAX_AGE,
            icsp_auth_state=deepcopy(icsp_auth_state),
        )
        with self._lock:
            self._sessions[session.session_id] = session
        return session

    def get_session(self, session_id: str, touch: bool = True) -> AuthSession | None:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            if session.expires_at <= time.time():
                self._sessions.pop(session_id, None)
                return None
            if touch:
                session.updated_at = utc_now_iso()
                session.expires_at = time.time() + AUTH_SESSION_MAX_AGE
            return session

    def delete_session(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def update_session_auth_state(
        self,
        session_id: str,
        icsp_auth_state: dict[str, Any],
        *,
        username: str | None = None,
        display_name: str | None = None,
        user_id: str | None = None,
        user_code: str | None = None,
    ) -> AuthSession | None:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None

            session.icsp_auth_state = deepcopy(icsp_auth_state)
            if username is not None:
                session.username = username
            if display_name is not None:
                session.display_name = display_name
            if user_id is not None:
                session.user_id = user_id
            if user_code is not None:
                session.user_code = user_code

            session.updated_at = utc_now_iso()
            session.expires_at = time.time() + AUTH_SESSION_MAX_AGE
            return session


auth_session_store = AuthSessionStore()


def set_auth_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=session_id,
        max_age=AUTH_SESSION_MAX_AGE,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite=AUTH_COOKIE_SAMESITE,
        path=AUTH_COOKIE_PATH,
        domain=AUTH_COOKIE_DOMAIN,
    )


def clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(
        key=AUTH_COOKIE_NAME,
        path=AUTH_COOKIE_PATH,
        domain=AUTH_COOKIE_DOMAIN,
    )


def get_current_auth_session(request: Request) -> AuthSession:
    session_id = request.cookies.get(AUTH_COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    session = auth_session_store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired, please log in again")

    return session
