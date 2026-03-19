from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.common import utcnow
from app.models.sync_job import SyncJobState


class SyncJobStateService:
    def __init__(self, db: Session):
        self.db = db

    def get_job(self, *, job_name: str, job_date: date) -> SyncJobState | None:
        return self.db.scalar(
            select(SyncJobState).where(
                SyncJobState.job_name == job_name,
                SyncJobState.job_date == job_date,
            )
        )

    def start_job(
        self,
        *,
        job_name: str,
        job_date: date,
        request_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncJobState:
        job = self.get_job(job_name=job_name, job_date=job_date)
        now = utcnow()
        if job is None:
            job = SyncJobState(
                job_name=job_name,
                job_date=job_date,
                status="running",
                retry_count=0,
                last_started_at=now,
                heartbeat_at=now,
                request_payload=request_payload,
            )
            self.db.add(job)
        else:
            if job.status == "failed":
                job.retry_count += 1
            job.status = "running"
            job.last_started_at = now
            job.heartbeat_at = now
            if request_payload is not None:
                job.request_payload = request_payload
            self.db.add(job)

        try:
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            job = self.get_job(job_name=job_name, job_date=job_date)
            if job is None:
                raise
            if job.status == "failed":
                job.retry_count += 1
            job.status = "running"
            job.last_started_at = now
            job.heartbeat_at = now
            if request_payload is not None:
                job.request_payload = request_payload
            self.db.add(job)
            self.db.flush()
        if commit:
            self.db.commit()
            self.db.refresh(job)
        return job

    def mark_success(
        self,
        job: SyncJobState,
        *,
        success_start: date,
        success_end: date,
        result_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncJobState:
        job = self.db.merge(job)
        now = utcnow()
        job.status = "success"
        job.last_finished_at = now
        job.last_success_start = success_start
        job.last_success_end = success_end
        job.last_success_at = now
        job.last_error = None
        job.result_payload = result_payload
        job.heartbeat_at = now
        self.db.add(job)
        if commit:
            self.db.commit()
            self.db.refresh(job)
        return job

    def mark_failure(
        self,
        job: SyncJobState,
        *,
        error_message: str,
        result_payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> SyncJobState:
        job = self.db.merge(job)
        now = utcnow()
        job.status = "failed"
        job.last_finished_at = now
        job.last_error = error_message
        job.result_payload = result_payload
        job.heartbeat_at = now
        self.db.add(job)
        if commit:
            self.db.commit()
            self.db.refresh(job)
        return job

    def heartbeat(self, job: SyncJobState, *, commit: bool = True) -> SyncJobState:
        job = self.db.merge(job)
        job.heartbeat_at = utcnow()
        self.db.add(job)
        if commit:
            self.db.commit()
            self.db.refresh(job)
        return job
