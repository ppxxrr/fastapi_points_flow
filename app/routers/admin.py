from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.auth import get_current_auth_session
from app.db.session import get_db_session
from app.services.admin_overview_service import AdminOverviewService


router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/overview")
def get_admin_overview(
    current_session=Depends(get_current_auth_session),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    del current_session
    return AdminOverviewService(db).build_overview()
