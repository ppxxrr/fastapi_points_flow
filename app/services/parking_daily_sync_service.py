from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.incremental_sync_service import (
    PARKING_JOB_NAME,
    DateSyncSummary,
    WindowSyncService,
    resolve_parking_provider,
)
from app.services.member_point_flow_service import ParkingRecordCsvImportService
from app.services.script_lock import FileScriptLock
from app.services.script_logger import build_script_logger
from app.services.sync_job_state_service import SyncJobStateService
from app.services.sync_log_service import SyncTaskLogService


BASE_DIR = Path(__file__).resolve().parents[2]
PARKING_DAILY_JOB_NAME = "daily_parking_incremental_sync"
PARKING_DAILY_TARGET_LAG_DAYS = int(os.getenv("PARKING_DAILY_TARGET_LAG_DAYS", "2"))
PARKING_INCREMENTAL_LOCK_PATH = BASE_DIR / "data" / "scheduler" / "run_daily_parking_incremental_sync.lock"

LoggerCallback = Callable[[str, str], None] | None


def _noop_logger(level: str, message: str) -> None:
    return None


def parking_target_date() -> date:
    return datetime.now().date() - timedelta(days=PARKING_DAILY_TARGET_LAG_DAYS)


@dataclass(slots=True)
class ParkingDailyRunSummary:
    job_date: str
    provider: str
    dry_run: bool = False
    retry_pending_only: bool = False
    force: bool = False
    status: str = "pending"
    source_mode: str | None = None
    source_available: bool = False
    fetched_rows: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    retry_count: int = 0
    last_error: str | None = None
    stats: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ParkingDailyIncrementalSyncService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger
        self.task_log_service = SyncTaskLogService(db)
        self.job_state_service = SyncJobStateService(db)

    def run(
        self,
        *,
        job_date: date,
        provider_name: str,
        dry_run: bool = False,
        retry_pending_only: bool = False,
        force: bool = False,
        triggered_by: str | None = None,
        triggered_source: str = "script",
    ) -> ParkingDailyRunSummary:
        summary = ParkingDailyRunSummary(
            job_date=job_date.isoformat(),
            provider=provider_name,
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
        )

        existing_business_job = self.job_state_service.get_job(job_name=PARKING_JOB_NAME, job_date=job_date)
        if retry_pending_only and existing_business_job and existing_business_job.status == "success" and not dry_run:
            summary.status = "skipped_existing_success"
            summary.retry_count = existing_business_job.retry_count
            return summary

        provider = resolve_parking_provider(provider_name, logger=self.logger)
        importer = ParkingRecordCsvImportService(self.db, logger=self.logger)
        window_sync_service = WindowSyncService(
            self.db,
            dataset_name="parking_record",
            job_name=PARKING_JOB_NAME,
            importer=importer,
            logger=self.logger,
        )

        wrapper_job = None
        wrapper_log = None
        if not dry_run:
            wrapper_job = self.job_state_service.start_job(
                job_name=PARKING_DAILY_JOB_NAME,
                job_date=job_date,
                request_payload={
                    "provider": provider_name,
                    "job_date": job_date.isoformat(),
                    "retry_pending_only": retry_pending_only,
                    "force": force,
                },
                commit=True,
            )
            wrapper_log = self.task_log_service.create_log(
                module_name="parking_incremental_sync",
                action="run_daily_parking_incremental_sync",
                target_type="date",
                target_value=job_date.isoformat(),
                triggered_by=triggered_by,
                triggered_source=triggered_source,
                request_payload={
                    "provider": provider_name,
                    "retry_pending_only": retry_pending_only,
                    "force": force,
                },
                commit=True,
            )

        try:
            date_summary = window_sync_service.sync_date(
                target_date=job_date,
                provider=provider,
                dry_run=dry_run,
                force=force,
            )
            summary.status = date_summary.status
            summary.source_mode = provider.provider_name
            summary.source_available = date_summary.source_available
            summary.fetched_rows = date_summary.fetched_rows
            summary.inserted_count = date_summary.inserted_count
            summary.updated_count = date_summary.updated_count
            summary.skipped_count = date_summary.skipped_count
            summary.failed_count = date_summary.failed_count
            summary.last_error = date_summary.error_message
            if hasattr(provider, "get_stats"):
                summary.stats = getattr(provider, "get_stats")()

            business_job = self.job_state_service.get_job(job_name=PARKING_JOB_NAME, job_date=job_date)
            if business_job is not None:
                summary.retry_count = business_job.retry_count

            if not dry_run and wrapper_job is not None and wrapper_log is not None:
                if summary.status == "success" or summary.status == "skipped_existing_success":
                    self.job_state_service.mark_success(
                        wrapper_job,
                        success_start=job_date,
                        success_end=job_date,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                    self.task_log_service.mark_success(wrapper_log, result_payload=summary.to_dict(), commit=False)
                else:
                    self.job_state_service.mark_failure(
                        wrapper_job,
                        error_message=summary.last_error or summary.status,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                    self.task_log_service.mark_failure(
                        wrapper_log,
                        error_message=summary.last_error or summary.status,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                self.db.commit()
            return summary
        except Exception as exc:
            summary.status = "failed"
            summary.last_error = str(exc)
            if not dry_run and wrapper_job is not None and wrapper_log is not None:
                self.db.rollback()
                self.job_state_service.mark_failure(
                    wrapper_job,
                    error_message=summary.last_error,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_failure(
                    wrapper_log,
                    error_message=summary.last_error,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.db.commit()
            raise
        finally:
            if hasattr(provider, "close"):
                provider.close()


def build_parking_sync_logger():
    return build_script_logger("run_daily_parking_incremental_sync", "daily_parking_incremental_sync.log")


def run_parking_sync_once(
    *,
    job_date: date,
    provider_name: str,
    dry_run: bool = False,
    retry_pending_only: bool = False,
    force: bool = False,
    triggered_by: str | None = None,
    triggered_source: str = "script",
) -> ParkingDailyRunSummary:
    logger = build_parking_sync_logger()

    def log_callback(level: str, message: str) -> None:
        getattr(logger, level.lower(), logger.info)(message)

    with SessionLocal() as session:
        service = ParkingDailyIncrementalSyncService(session, logger=log_callback)
        summary = service.run(
            job_date=job_date,
            provider_name=provider_name,
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
            triggered_by=triggered_by,
            triggered_source=triggered_source,
        )
    logger.info(
        "daily parking sync completed job_date=%s status=%s provider=%s retry_only=%s force=%s",
        summary.job_date,
        summary.status,
        provider_name,
        retry_pending_only,
        force,
    )
    return summary


def start_parking_sync_background(
    *,
    job_date: date,
    provider_name: str,
    force: bool,
    triggered_by: str | None,
    triggered_source: str,
) -> dict[str, Any]:
    lock = FileScriptLock(PARKING_INCREMENTAL_LOCK_PATH)
    if not lock.acquire():
        return {
            "status": "skipped_locked",
            "job_date": job_date.isoformat(),
            "detail": str(PARKING_INCREMENTAL_LOCK_PATH.resolve()),
        }
    lock.release()

    def worker() -> None:
        process_lock = FileScriptLock(PARKING_INCREMENTAL_LOCK_PATH)
        if not process_lock.acquire():
            return
        try:
            summary = run_parking_sync_once(
                job_date=job_date,
                provider_name=provider_name,
                dry_run=False,
                retry_pending_only=False,
                force=force,
                triggered_by=triggered_by,
                triggered_source=triggered_source,
            )
            logger = build_parking_sync_logger()
            logger.info("manual parking sync summary=%s", json.dumps(summary.to_dict(), ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001
            logger = build_parking_sync_logger()
            logger.exception("manual parking sync failed: %s", exc)
        finally:
            process_lock.release()

    threading.Thread(target=worker, daemon=True).start()
    return {
        "status": "queued",
        "job_date": job_date.isoformat(),
        "detail": "parking incremental sync started in background",
    }
