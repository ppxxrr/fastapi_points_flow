from __future__ import annotations

import logging
import threading
from copy import deepcopy
from datetime import date, timedelta
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.auth import get_current_auth_session
from app.db.session import get_db_session
from app.schemas import (
    AdminParkingSyncRunRequest,
    AdminParkingSyncRunResponse,
    AdminParkingTradeSyncRunRequest,
    AdminParkingTradeSyncRunResponse,
)
from app.services.bi_analytics_service import BiAnalyticsService
from app.services.admin_overview_service import AdminOverviewService
from app.services.incremental_sync_service import DEFAULT_PARKING_PROVIDER
from app.services.parking_daily_sync_service import parking_target_date, start_parking_sync_background
from app.services.parking_trade_daily_sync_service import (
    DEFAULT_PARKING_TRADE_PROVIDER,
    parking_trade_target_date,
    start_parking_trade_sync_background,
)


router = APIRouter(prefix="/api/admin", tags=["admin"])
route_logger = logging.getLogger("uvicorn.error")
BI_CACHE_TTL_SECONDS = 300
MAX_BI_DAY_COUNT = 93
_bi_cache_lock = threading.Lock()
_bi_cache: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
BI_CATEGORY_PATTERN = "^(regular|policy|passenger|exception)$"


def _default_bi_date() -> date:
    return date.today() - timedelta(days=1)


def _get_cached_bi_dashboard(cache_key: tuple[str, str, str], now_monotonic: float) -> dict[str, Any] | None:
    with _bi_cache_lock:
        cached = _bi_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now_monotonic:
            _bi_cache.pop(cache_key, None)
            return None
        return deepcopy(payload)


def _set_cached_bi_dashboard(cache_key: tuple[str, str, str], payload: dict[str, Any], now_monotonic: float) -> None:
    with _bi_cache_lock:
        _bi_cache[cache_key] = (now_monotonic + BI_CACHE_TTL_SECONDS, deepcopy(payload))


@router.get("/overview")
def get_admin_overview(
    current_session=Depends(get_current_auth_session),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    del current_session
    return AdminOverviewService(db).build_overview()


@router.get("/bi")
def get_admin_bi_dashboard(
    current_session=Depends(get_current_auth_session),
    db: Session = Depends(get_db_session),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    mode: str = Query(default="daily", pattern="^(daily|range)$"),
    category: str = Query(default="regular", pattern=BI_CATEGORY_PATTERN),
) -> dict[str, Any]:
    del current_session
    start_date = start_date or _default_bi_date()
    end_date = end_date or _default_bi_date()
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    day_count = (end_date - start_date).days + 1
    if day_count > MAX_BI_DAY_COUNT:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"BI 查询范围不能超过 {MAX_BI_DAY_COUNT} 天，请缩小日期范围后重试。",
        )

    cache_key = (start_date.isoformat(), end_date.isoformat(), f"{mode}:{category}")
    now_monotonic = perf_counter()
    cached = _get_cached_bi_dashboard(cache_key, now_monotonic)
    if cached is not None:
        route_logger.info(
            "Admin BI cache hit start=%s end=%s mode=%s day_count=%s",
            start_date.isoformat(),
            end_date.isoformat(),
            f"{mode}:{category}",
            day_count,
        )
        return cached

    started = perf_counter()
    payload = BiAnalyticsService(db).build_dashboard(start_date=start_date, end_date=end_date, mode=mode, category=category)
    elapsed = perf_counter() - started
    _set_cached_bi_dashboard(cache_key, payload, perf_counter())
    route_logger.info(
        "Admin BI built start=%s end=%s mode=%s day_count=%s elapsed=%.3fs",
        start_date.isoformat(),
        end_date.isoformat(),
        f"{mode}:{category}",
        day_count,
        elapsed,
    )
    return payload


@router.post("/parking-sync/run", response_model=AdminParkingSyncRunResponse)
def run_parking_incremental_sync(
    payload: AdminParkingSyncRunRequest,
    current_session=Depends(get_current_auth_session),
) -> AdminParkingSyncRunResponse:
    result = start_parking_sync_background(
        job_date=payload.job_date or parking_target_date(),
        provider_name=DEFAULT_PARKING_PROVIDER,
        force=payload.force,
        triggered_by=current_session.username,
        triggered_source="admin_ui",
    )
    return AdminParkingSyncRunResponse(**result)


@router.post("/parking-trade-sync/run", response_model=AdminParkingTradeSyncRunResponse)
def run_parking_trade_incremental_sync(
    payload: AdminParkingTradeSyncRunRequest,
    current_session=Depends(get_current_auth_session),
) -> AdminParkingTradeSyncRunResponse:
    result = start_parking_trade_sync_background(
        job_date=payload.job_date or parking_trade_target_date(),
        provider_name=DEFAULT_PARKING_TRADE_PROVIDER,
        force=payload.force,
        triggered_by=current_session.username,
        triggered_source="admin_ui",
    )
    return AdminParkingTradeSyncRunResponse(**result)
