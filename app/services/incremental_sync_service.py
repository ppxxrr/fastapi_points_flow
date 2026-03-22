from __future__ import annotations

import csv
import os
import traceback
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.member import MemberProfile
from app.models.parking import ParkingRecord
from app.models.point_flow import MemberPointFlow
from app.models.sync_job import SyncJobState
from app.services.icsp_client import ICSPClient
from app.services.import_utils import clean_text, load_csv_header, parse_datetime_value
from app.services.member_point_flow_service import (
    MemberPointFlowCsvImportService,
    ParkingRecordCsvImportService,
)
from app.services.member_sync_service import ICSPMemberSyncService
from app.services.parking_api_sync_service import ParkingApiClient
from app.services.sync_job_state_service import SyncJobStateService
from app.services.sync_log_service import SyncTaskLogService


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_POINT_FLOW_SOURCE_DIR = Path(
    os.getenv(
        "POINT_FLOW_SOURCE_DIR",
        r"D:\python\menbers\claude\members\points_flow_quarterly_exports",
    )
)
DEFAULT_PARKING_SOURCE_DIR = Path(
    os.getenv(
        "PARKING_SOURCE_DIR",
        r"D:\python\menbers\claude\backup\parking\exports_leave_time_20250101_20260228",
    )
)
DEFAULT_POINT_FLOW_START_DATE = date.fromisoformat(os.getenv("POINT_FLOW_SYNC_START_DATE", "2024-01-01"))
DEFAULT_PARKING_START_DATE = date.fromisoformat(os.getenv("PARKING_SYNC_START_DATE", "2025-01-01"))
DEFAULT_POINT_FLOW_PROVIDER = os.getenv("POINT_FLOW_INCREMENTAL_PROVIDER", "auto").strip().lower() or "auto"
DEFAULT_PARKING_PROVIDER = os.getenv("PARKING_INCREMENTAL_PROVIDER", "auto").strip().lower() or "auto"

POINT_FLOW_JOB_NAME = "point_flow_window_sync"
PARKING_JOB_NAME = "parking_record_window_sync"
NEW_MEMBER_JOB_NAME = "new_member_window_sync"
DAILY_JOB_NAME = "daily_full_incremental_sync"

LoggerCallback = Callable[[str, str], None] | None


def _noop_logger(level: str, message: str) -> None:
    return None


def iter_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        return []
    current = start_date
    values: list[date] = []
    while current <= end_date:
        values.append(current)
        current += timedelta(days=1)
    return values


def chunk_dates(values: list[date], size: int) -> list[list[date]]:
    if size <= 0:
        return [values]
    return [values[index : index + size] for index in range(0, len(values), size)]


def yesterday() -> date:
    return datetime.now().date() - timedelta(days=1)


def parse_date_arg(value: str | None, default: date) -> date:
    if not value:
        return default
    return date.fromisoformat(value)


def normalize_business_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    return None


def extract_date_range_from_name(name: str) -> tuple[date, date] | None:
    import re

    patterns = (
        r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})",
        r"(\d{8})_(\d{8})",
    )
    for pattern in patterns:
        match = re.search(pattern, name)
        if not match:
            continue
        left, right = match.group(1), match.group(2)
        if len(left) == 8:
            left = f"{left[:4]}-{left[4:6]}-{left[6:]}"
        if len(right) == 8:
            right = f"{right[:4]}-{right[4:6]}-{right[6:]}"
        return date.fromisoformat(left), date.fromisoformat(right)
    return None


def coalesce_point_flow_business_date(raw_row: dict[str, Any]) -> date | None:
    parsed = parse_datetime_value(raw_row.get("consumeTime")) or parse_datetime_value(raw_row.get("createTime"))
    return parsed.date() if parsed else None


def coalesce_parking_business_date(raw_row: dict[str, Any]) -> date | None:
    parsed = parse_datetime_value(raw_row.get("出场时间")) or parse_datetime_value(raw_row.get("进场时间"))
    return parsed.date() if parsed else None


@dataclass(slots=True)
class MissingDateSummary:
    dataset_name: str
    start_date: str
    end_date: str
    total_days: int
    covered_dates: list[str] = field(default_factory=list)
    missing_dates: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DateSyncSummary:
    dataset_name: str
    job_name: str
    target_date: str
    source_provider: str
    dry_run: bool = False
    status: str = "pending"
    source_available: bool = False
    fetched_rows: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BackfillSummary:
    dataset_name: str
    provider: str
    start_date: str
    end_date: str
    dry_run: bool = False
    check_only: bool = False
    force: bool = False
    missing_dates: list[str] = field(default_factory=list)
    synced_dates: list[DateSyncSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["synced_dates"] = [item.to_dict() for item in self.synced_dates]
        return payload


@dataclass(slots=True)
class ExtractedMobileRow:
    mobile_no: str
    source_types: list[str] = field(default_factory=list)
    source_record_count: int = 0
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    exists_in_member_profile: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "mobile_no": self.mobile_no,
            "source_types": ",".join(self.source_types),
            "source_record_count": self.source_record_count,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "exists_in_member_profile": self.exists_in_member_profile,
        }


@dataclass(slots=True)
class MobileExtractionSummary:
    start_date: str
    end_date: str
    total_source_mobiles: int = 0
    existing_member_count: int = 0
    new_member_count: int = 0
    dry_run: bool = False
    output_path: str | None = None
    rows: list[ExtractedMobileRow] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_source_mobiles": self.total_source_mobiles,
            "existing_member_count": self.existing_member_count,
            "new_member_count": self.new_member_count,
            "dry_run": self.dry_run,
            "output_path": self.output_path,
            "rows": [row.to_dict() for row in self.rows],
        }


@dataclass(slots=True)
class NewMemberSyncRow:
    mobile_no: str
    status: str
    member_ids: list[str] = field(default_factory=list)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewMemberSyncSummary:
    start_date: str
    end_date: str
    dry_run: bool = False
    total_new_mobiles: int = 0
    processed_count: int = 0
    success_count: int = 0
    not_found_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    rows: list[NewMemberSyncRow] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "dry_run": self.dry_run,
            "total_new_mobiles": self.total_new_mobiles,
            "processed_count": self.processed_count,
            "success_count": self.success_count,
            "not_found_count": self.not_found_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "rows": [row.to_dict() for row in self.rows],
        }


@dataclass(slots=True)
class DailyIncrementalSyncSummary:
    job_date: str
    dry_run: bool = False
    point_backfill: BackfillSummary | None = None
    parking_backfill: BackfillSummary | None = None
    point_daily: DateSyncSummary | None = None
    parking_daily: DateSyncSummary | None = None
    member_sync: NewMemberSyncSummary | None = None
    status: str = "pending"
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_date": self.job_date,
            "dry_run": self.dry_run,
            "point_backfill": self.point_backfill.to_dict() if self.point_backfill else None,
            "parking_backfill": self.parking_backfill.to_dict() if self.parking_backfill else None,
            "point_daily": self.point_daily.to_dict() if self.point_daily else None,
            "parking_daily": self.parking_daily.to_dict() if self.parking_daily else None,
            "member_sync": self.member_sync.to_dict() if self.member_sync else None,
            "status": self.status,
            "error_message": self.error_message,
        }


class BaseWindowSourceProvider:
    provider_name: str
    dataset_name: str

    def available_dates(self, target_dates: set[date]) -> set[date]:
        raise NotImplementedError

    def fetch_rows_by_date(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        raise NotImplementedError


class BaseCsvWindowSourceProvider(BaseWindowSourceProvider):
    required_headers: set[str]
    business_date_getter: Callable[[dict[str, Any]], date | None]

    def __init__(self, input_dir: str | Path, pattern: str = "*.csv"):
        self.input_dir = Path(input_dir)
        self.pattern = pattern

    def _list_candidate_files(self, target_dates: set[date]) -> list[tuple[Path, str]]:
        if not self.input_dir.exists() or not target_dates:
            return []
        min_date = min(target_dates)
        max_date = max(target_dates)
        candidates: list[tuple[Path, str]] = []
        for csv_file in sorted(self.input_dir.glob(self.pattern)):
            headers, encoding = load_csv_header(csv_file)
            if headers is None or encoding is None:
                continue
            if not self.required_headers.issubset(set(headers)):
                continue
            file_range = extract_date_range_from_name(csv_file.name)
            if file_range is not None and (file_range[1] < min_date or file_range[0] > max_date):
                continue
            candidates.append((csv_file, encoding))
        return candidates

    def available_dates(self, target_dates: set[date]) -> set[date]:
        available: set[date] = set()
        for csv_file, _ in self._list_candidate_files(target_dates):
            file_range = extract_date_range_from_name(csv_file.name)
            if file_range is None:
                available.update(target_dates)
                continue
            for value in target_dates:
                if file_range[0] <= value <= file_range[1]:
                    available.add(value)
        return available

    def fetch_rows_by_date(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        grouped: dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]] = {}
        target_dates = set(target_dates)
        for csv_file, encoding in self._list_candidate_files(target_dates):
            with csv_file.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                for row_no, raw_row in enumerate(reader, start=2):
                    if raw_row is None or not any(clean_text(value) is not None for value in raw_row.values()):
                        continue
                    business_date = self.business_date_getter(raw_row)
                    if business_date is None or business_date not in target_dates:
                        continue
                    grouped.setdefault(business_date, []).append((raw_row, csv_file, row_no))
        return grouped


class PointFlowCsvSourceProvider(BaseCsvWindowSourceProvider):
    provider_name = "csv"
    dataset_name = "member_point_flow"
    required_headers = {"flowNo", "createTime", "changePointAmount", "memberId", "memberPhone"}
    business_date_getter = staticmethod(coalesce_point_flow_business_date)


class ParkingCsvSourceProvider(BaseCsvWindowSourceProvider):
    provider_name = "csv"
    dataset_name = "parking_record"
    required_headers = {"记录ID", "停车流水号", "进场时间", "出场时间", "车牌号"}
    business_date_getter = staticmethod(coalesce_parking_business_date)


class ParkingApiSourceProvider(BaseWindowSourceProvider):
    provider_name = "api"
    dataset_name = "parking_record"

    def __init__(self, logger: LoggerCallback = None):
        self.logger = logger or _noop_logger
        self.client = ParkingApiClient(logger=self.logger)

    def available_dates(self, target_dates: set[date]) -> set[date]:
        return self.client.available_dates(target_dates)

    def fetch_rows_by_date(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        return self.client.fetch_rows_for_dates(target_dates)

    def get_stats(self) -> dict[str, Any]:
        return self.client.get_stats()

    def close(self) -> None:
        self.client.close()


class ICSPPointFlowSourceProvider(BaseWindowSourceProvider):
    provider_name = "api"
    dataset_name = "member_point_flow"

    def __init__(self, username: str, password: str, logger: LoggerCallback = None):
        self.username = username
        self.password = password
        self.logger = logger or _noop_logger
        self._client: ICSPClient | None = None

    def _ensure_client(self) -> ICSPClient:
        if self._client is not None:
            return self._client
        if not self.username or not self.password:
            raise ValueError("ICSP username/password is required for api provider")
        client = ICSPClient(logger=self.logger)
        if not client.login(self.username, self.password):
            raise RuntimeError(client.last_login_error or "ICSP login failed")
        if not client.validate_authenticated_session(self.username):
            raise RuntimeError("ICSP point-flow session validation failed")
        self._client = client
        return client

    def available_dates(self, target_dates: set[date]) -> set[date]:
        if not target_dates:
            return set()
        self._ensure_client()
        return set(target_dates)

    def fetch_rows_by_date(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        client = self._ensure_client()
        grouped: dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]] = {}
        for target_date in sorted(target_dates):
            rows = client.fetch_point_flow(target_date.isoformat(), target_date.isoformat())
            virtual_path = BASE_DIR / "logs" / f"point_flow_api_{target_date.isoformat()}.virtual"
            grouped[target_date] = [(row, virtual_path, index) for index, row in enumerate(rows, start=1)]
        return grouped


def resolve_point_flow_provider(
    *,
    provider_name: str,
    username: str | None,
    password: str | None,
    logger: LoggerCallback = None,
) -> BaseWindowSourceProvider:
    normalized = (provider_name or DEFAULT_POINT_FLOW_PROVIDER).strip().lower()
    if normalized == "auto":
        normalized = "api" if username and password else "csv"
    if normalized == "api":
        return ICSPPointFlowSourceProvider(username or "", password or "", logger=logger)
    if normalized == "csv":
        return PointFlowCsvSourceProvider(DEFAULT_POINT_FLOW_SOURCE_DIR)
    raise ValueError(f"Unsupported point-flow provider: {provider_name}")


def resolve_parking_provider(
    provider_name: str,
    *,
    logger: LoggerCallback = None,
) -> BaseWindowSourceProvider:
    normalized = (provider_name or DEFAULT_PARKING_PROVIDER).strip().lower()
    if normalized == "auto":
        normalized = "api"
    if normalized == "api":
        return ParkingApiSourceProvider(logger=logger)
    if normalized == "csv":
        return ParkingCsvSourceProvider(DEFAULT_PARKING_SOURCE_DIR)
    raise ValueError(f"Unsupported parking provider: {provider_name}")


class CoverageService:
    def __init__(self, db: Session):
        self.db = db

    def detect_point_flow_missing_dates(self, *, start_date: date, end_date: date) -> MissingDateSummary:
        covered = self._load_point_flow_covered_dates(start_date=start_date, end_date=end_date)
        return self._build_summary(
            dataset_name="member_point_flow",
            start_date=start_date,
            end_date=end_date,
            covered_dates=covered,
        )

    def detect_parking_missing_dates(self, *, start_date: date, end_date: date) -> MissingDateSummary:
        covered = self._load_parking_covered_dates(start_date=start_date, end_date=end_date)
        return self._build_summary(
            dataset_name="parking_record",
            start_date=start_date,
            end_date=end_date,
            covered_dates=covered,
        )

    def _load_point_flow_covered_dates(self, *, start_date: date, end_date: date) -> set[date]:
        data_rows = self.db.execute(
            select(func.date(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time))).where(
                func.date(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)) >= start_date.isoformat(),
                func.date(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)) <= end_date.isoformat(),
            )
        ).all()
        job_rows = self.db.scalars(
            select(SyncJobState.job_date).where(
                SyncJobState.job_name == POINT_FLOW_JOB_NAME,
                SyncJobState.status == "success",
                SyncJobState.job_date >= start_date,
                SyncJobState.job_date <= end_date,
            )
        ).all()
        return {
            *{normalized for (value,) in data_rows if (normalized := normalize_business_date(value)) is not None},
            *set(job_rows),
        }

    def _load_parking_covered_dates(self, *, start_date: date, end_date: date) -> set[date]:
        data_rows = self.db.execute(
            select(func.date(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time))).where(
                func.date(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)) >= start_date.isoformat(),
                func.date(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)) <= end_date.isoformat(),
            )
        ).all()
        job_rows = self.db.scalars(
            select(SyncJobState.job_date).where(
                SyncJobState.job_name == PARKING_JOB_NAME,
                SyncJobState.status == "success",
                SyncJobState.job_date >= start_date,
                SyncJobState.job_date <= end_date,
            )
        ).all()
        return {
            *{normalized for (value,) in data_rows if (normalized := normalize_business_date(value)) is not None},
            *set(job_rows),
        }

    @staticmethod
    def _build_summary(
        *,
        dataset_name: str,
        start_date: date,
        end_date: date,
        covered_dates: set[date],
    ) -> MissingDateSummary:
        full_range = iter_dates(start_date, end_date)
        missing_dates = [value for value in full_range if value not in covered_dates]
        return MissingDateSummary(
            dataset_name=dataset_name,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            total_days=len(full_range),
            covered_dates=[value.isoformat() for value in sorted(covered_dates)],
            missing_dates=[value.isoformat() for value in missing_dates],
        )


class WindowSyncService:
    def __init__(
        self,
        db: Session,
        *,
        dataset_name: str,
        job_name: str,
        importer: MemberPointFlowCsvImportService | ParkingRecordCsvImportService,
        logger: LoggerCallback = None,
    ):
        self.db = db
        self.dataset_name = dataset_name
        self.job_name = job_name
        self.importer = importer
        self.logger = logger or _noop_logger
        self.task_log_service = SyncTaskLogService(db)
        self.job_state_service = SyncJobStateService(db)

    def sync_date(
        self,
        *,
        target_date: date,
        provider: BaseWindowSourceProvider,
        rows: list[tuple[dict[str, Any], str | Path | None, int | None]] | None = None,
        source_available: bool | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> DateSyncSummary:
        summary = DateSyncSummary(
            dataset_name=self.dataset_name,
            job_name=self.job_name,
            target_date=target_date.isoformat(),
            source_provider=provider.provider_name,
            dry_run=dry_run,
        )

        existing_job = self.job_state_service.get_job(job_name=self.job_name, job_date=target_date)
        if existing_job and existing_job.status == "success" and not force and not dry_run:
            summary.status = "skipped_existing_success"
            summary.source_available = True
            return summary

        if source_available is None:
            source_available = target_date in provider.available_dates({target_date})
        summary.source_available = source_available
        if not source_available:
            summary.status = "failed"
            summary.error_message = f"no source available for {target_date.isoformat()}"
            if not dry_run:
                job = self.job_state_service.start_job(
                    job_name=self.job_name,
                    job_date=target_date,
                    request_payload={"provider": provider.provider_name, "target_date": target_date.isoformat()},
                    commit=False,
                )
                self.job_state_service.mark_failure(job, error_message=summary.error_message, commit=True)
            return summary

        if rows is None:
            rows = provider.fetch_rows_by_date({target_date}).get(target_date, [])
        summary.fetched_rows = len(rows)

        job = None
        task_log = None
        if not dry_run:
            job = self.job_state_service.start_job(
                job_name=self.job_name,
                job_date=target_date,
                request_payload={"provider": provider.provider_name, "target_date": target_date.isoformat()},
                commit=False,
            )
            task_log = self.task_log_service.create_log(
                module_name=self.dataset_name,
                action="sync_single_date",
                target_type="date",
                target_value=target_date.isoformat(),
                triggered_source="script",
                request_payload={"provider": provider.provider_name, "target_date": target_date.isoformat()},
                commit=False,
            )
            self.db.commit()

        try:
            import_summary = self.importer.import_rows(
                rows=rows,
                dry_run=dry_run,
                source_name=f"{provider.provider_name}:{self.dataset_name}:{target_date.isoformat()}",
            )
            summary.inserted_count = import_summary.inserted_count
            summary.updated_count = import_summary.updated_count
            summary.skipped_count = import_summary.skipped_count
            summary.failed_count = import_summary.failed_count
            summary.status = "success"

            if not dry_run and job is not None and task_log is not None:
                self.job_state_service.mark_success(
                    job,
                    success_start=target_date,
                    success_end=target_date,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_success(task_log, result_payload=summary.to_dict(), commit=False)
                self.db.commit()
            return summary
        except Exception as exc:
            summary.status = "failed"
            summary.error_message = str(exc)
            if not dry_run and job is not None and task_log is not None:
                self.db.rollback()
                self.job_state_service.mark_failure(
                    job,
                    error_message=summary.error_message,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_failure(
                    task_log,
                    error_message=summary.error_message,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.db.commit()
            raise


class BackfillService:
    def __init__(
        self,
        db: Session,
        *,
        dataset_name: str,
        coverage_loader: Callable[[date, date], MissingDateSummary],
        window_sync_service: WindowSyncService,
        logger: LoggerCallback = None,
    ):
        self.db = db
        self.dataset_name = dataset_name
        self.coverage_loader = coverage_loader
        self.window_sync_service = window_sync_service
        self.logger = logger or _noop_logger

    def backfill_missing_dates(
        self,
        *,
        provider: BaseWindowSourceProvider,
        start_date: date,
        end_date: date,
        dry_run: bool = False,
        check_only: bool = False,
        force: bool = False,
        fetch_chunk_days: int = 31,
    ) -> BackfillSummary:
        summary = BackfillSummary(
            dataset_name=self.dataset_name,
            provider=provider.provider_name,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dry_run=dry_run,
            check_only=check_only,
            force=force,
        )
        try:
            if force:
                target_dates = iter_dates(start_date, end_date)
            else:
                missing = self.coverage_loader(start_date, end_date)
                summary.missing_dates = missing.missing_dates
                target_dates = [date.fromisoformat(value) for value in missing.missing_dates]

            if check_only:
                if force:
                    summary.missing_dates = [value.isoformat() for value in target_dates]
                return summary

            if not target_dates:
                return summary

            for target_chunk in chunk_dates(target_dates, fetch_chunk_days):
                available_dates = provider.available_dates(set(target_chunk))
                rows_by_date = provider.fetch_rows_by_date(available_dates) if available_dates else {}

                for target_date in target_chunk:
                    try:
                        date_summary = self.window_sync_service.sync_date(
                            target_date=target_date,
                            provider=provider,
                            rows=rows_by_date.get(target_date, []),
                            source_available=target_date in available_dates,
                            dry_run=dry_run,
                            force=force,
                        )
                    except Exception as exc:
                        date_summary = DateSyncSummary(
                            dataset_name=self.dataset_name,
                            job_name=self.window_sync_service.job_name,
                            target_date=target_date.isoformat(),
                            source_provider=provider.provider_name,
                            dry_run=dry_run,
                            status="failed",
                            source_available=target_date in available_dates,
                            error_message=str(exc),
                        )
                        self.logger("ERROR", f"[{self.dataset_name}] failed on {target_date.isoformat()}: {exc}")
                    summary.synced_dates.append(date_summary)
            return summary
        finally:
            if hasattr(provider, "close"):
                provider.close()


class BusinessMobileExtractionService:
    def __init__(self, db: Session):
        self.db = db

    def extract_new_mobiles(
        self,
        *,
        start_date: date,
        end_date: date,
        dry_run: bool = False,
        output_path: str | Path | None = None,
    ) -> MobileExtractionSummary:
        summary = MobileExtractionSummary(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dry_run=dry_run,
        )

        rows = self._load_source_rows(start_date=start_date, end_date=end_date)
        record_map: dict[str, ExtractedMobileRow] = {}
        for mobile_no, source_type, source_count, first_seen_at, last_seen_at in rows:
            if not mobile_no:
                continue
            record = record_map.get(mobile_no)
            if record is None:
                record = ExtractedMobileRow(
                    mobile_no=mobile_no,
                    first_seen_at=first_seen_at.isoformat(sep=" ", timespec="seconds") if first_seen_at else None,
                    last_seen_at=last_seen_at.isoformat(sep=" ", timespec="seconds") if last_seen_at else None,
                )
                record_map[mobile_no] = record
            if source_type not in record.source_types:
                record.source_types.append(source_type)
            record.source_record_count += int(source_count or 0)
            if first_seen_at and (record.first_seen_at is None or first_seen_at.isoformat(sep=" ", timespec="seconds") < record.first_seen_at):
                record.first_seen_at = first_seen_at.isoformat(sep=" ", timespec="seconds")
            if last_seen_at and (record.last_seen_at is None or last_seen_at.isoformat(sep=" ", timespec="seconds") > record.last_seen_at):
                record.last_seen_at = last_seen_at.isoformat(sep=" ", timespec="seconds")

        existing_mobiles = set(
            self.db.scalars(
                select(MemberProfile.mobile_no).where(MemberProfile.mobile_no.in_(list(record_map.keys())))
            ).all()
        )
        for record in record_map.values():
            record.exists_in_member_profile = record.mobile_no in existing_mobiles

        all_rows = sorted(record_map.values(), key=lambda item: item.mobile_no)
        summary.total_source_mobiles = len(all_rows)
        summary.existing_member_count = sum(1 for row in all_rows if row.exists_in_member_profile)
        summary.new_member_count = sum(1 for row in all_rows if not row.exists_in_member_profile)
        summary.rows = [row for row in all_rows if not row.exists_in_member_profile]

        if output_path and not dry_run:
            self.write_output(output_path, summary.rows)
            summary.output_path = str(Path(output_path).resolve())
        return summary

    def _load_source_rows(self, *, start_date: date, end_date: date) -> list[tuple[str, str, int, datetime | None, datetime | None]]:
        point_rows = self.db.execute(
            select(
                MemberPointFlow.mobile_no,
                func.count(MemberPointFlow.id),
                func.min(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)),
                func.max(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)),
            ).where(
                MemberPointFlow.mobile_no.is_not(None),
                MemberPointFlow.mobile_no != "",
                func.date(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)) >= start_date.isoformat(),
                func.date(func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)) <= end_date.isoformat(),
            ).group_by(MemberPointFlow.mobile_no)
        ).all()
        parking_rows = self.db.execute(
            select(
                ParkingRecord.mobile_no,
                func.count(ParkingRecord.id),
                func.min(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)),
                func.max(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)),
            ).where(
                ParkingRecord.mobile_no.is_not(None),
                ParkingRecord.mobile_no != "",
                func.date(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)) >= start_date.isoformat(),
                func.date(func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)) <= end_date.isoformat(),
            ).group_by(ParkingRecord.mobile_no)
        ).all()

        combined: list[tuple[str, str, int, datetime | None, datetime | None]] = []
        combined.extend(
            (mobile_no, "member_point_flow", count, first_seen, last_seen)
            for mobile_no, count, first_seen, last_seen in point_rows
        )
        combined.extend(
            (mobile_no, "parking_record", count, first_seen, last_seen)
            for mobile_no, count, first_seen, last_seen in parking_rows
        )
        return combined

    @staticmethod
    def write_output(output_path: str | Path, rows: list[ExtractedMobileRow]) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "mobile_no",
                    "source_types",
                    "source_record_count",
                    "first_seen_at",
                    "last_seen_at",
                    "exists_in_member_profile",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row.to_dict())
        return path


class NewMemberSyncService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger
        self.mobile_extraction_service = BusinessMobileExtractionService(db)
        self.task_log_service = SyncTaskLogService(db)
        self.job_state_service = SyncJobStateService(db)

    def sync_from_business_data(
        self,
        *,
        start_date: date,
        end_date: date,
        username: str | None,
        password: str | None,
        dry_run: bool = False,
        limit: int | None = None,
    ) -> NewMemberSyncSummary:
        extraction = self.mobile_extraction_service.extract_new_mobiles(
            start_date=start_date,
            end_date=end_date,
            dry_run=True,
        )
        candidate_rows = [row for row in extraction.rows if not row.exists_in_member_profile]
        if limit is not None and limit >= 0:
            candidate_rows = candidate_rows[:limit]

        summary = NewMemberSyncSummary(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dry_run=dry_run,
            total_new_mobiles=len(candidate_rows),
            processed_count=len(candidate_rows),
        )

        if dry_run:
            for row in candidate_rows:
                summary.rows.append(NewMemberSyncRow(mobile_no=row.mobile_no, status="dry_run"))
            return summary

        if not username or not password:
            raise ValueError("ICSP username/password is required when dry_run is False")

        client = ICSPClient(logger=self.logger)
        if not client.login(username, password):
            raise RuntimeError(client.last_login_error or "ICSP login failed")
        if not client.validate_member_session(username):
            raise RuntimeError("ICSP member session validation failed")

        job = self.job_state_service.start_job(
            job_name=NEW_MEMBER_JOB_NAME,
            job_date=end_date,
            request_payload={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "limit": limit,
            },
            commit=True,
        )

        batch_log = self.task_log_service.create_log(
            module_name="member_incremental",
            action="sync_new_members_by_date_range",
            target_type="date_range",
            target_value=f"{start_date.isoformat()}~{end_date.isoformat()}",
            triggered_by=username,
            triggered_source="script",
            request_payload={"limit": limit, "total_new_mobiles": len(candidate_rows)},
            commit=True,
        )

        member_sync_service = ICSPMemberSyncService(db=self.db, icsp_client=client)
        try:
            for row in candidate_rows:
                item_log = self.task_log_service.create_log(
                    module_name="member_incremental",
                    action="sync_by_mobile",
                    target_type="mobile_no",
                    target_value=row.mobile_no,
                    triggered_by=username,
                    triggered_source="script",
                    request_payload={"mobile_no": row.mobile_no, "date_range": [start_date.isoformat(), end_date.isoformat()]},
                    commit=True,
                )
                try:
                    result = member_sync_service.sync_member_by_mobile(row.mobile_no, commit=False)
                    if result.matched_member_ids:
                        summary.success_count += 1
                        status = "success"
                        message = "; ".join(result.warnings)
                    else:
                        summary.not_found_count += 1
                        status = "not_found"
                        message = "; ".join(result.warnings) or "No member matched by mobile."
                    summary.rows.append(
                        NewMemberSyncRow(
                            mobile_no=row.mobile_no,
                            status=status,
                            member_ids=result.matched_member_ids,
                            message=message,
                        )
                    )
                    self.task_log_service.mark_success(item_log, result_payload=result.to_dict(), commit=False)
                    self.db.commit()
                except Exception as exc:
                    self.db.rollback()
                    summary.failed_count += 1
                    summary.rows.append(NewMemberSyncRow(mobile_no=row.mobile_no, status="failed", message=str(exc)))
                    self.task_log_service.mark_failure(item_log, error_message=str(exc), commit=True)

            self.task_log_service.mark_success(batch_log, result_payload=summary.to_dict(), commit=False)
            self.job_state_service.mark_success(
                job,
                success_start=start_date,
                success_end=end_date,
                result_payload=summary.to_dict(),
                commit=False,
            )
            self.db.commit()
            return summary
        except Exception as exc:
            self.db.rollback()
            self.task_log_service.mark_failure(batch_log, error_message=str(exc), result_payload=summary.to_dict(), commit=False)
            self.job_state_service.mark_failure(job, error_message=str(exc), result_payload=summary.to_dict(), commit=False)
            self.db.commit()
            raise


class DailyIncrementalSyncService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger
        self.coverage_service = CoverageService(db)
        self.task_log_service = SyncTaskLogService(db)
        self.job_state_service = SyncJobStateService(db)

    def run(
        self,
        *,
        job_date: date,
        point_provider_name: str,
        parking_provider_name: str,
        username: str | None,
        password: str | None,
        dry_run: bool = False,
        retry_pending_only: bool = False,
        skip_member_sync: bool = False,
        member_limit: int | None = None,
    ) -> DailyIncrementalSyncSummary:
        summary = DailyIncrementalSyncSummary(job_date=job_date.isoformat(), dry_run=dry_run)

        existing_job = self.job_state_service.get_job(job_name=DAILY_JOB_NAME, job_date=job_date)
        if retry_pending_only and existing_job and existing_job.status == "success" and not dry_run:
            summary.status = "skipped_existing_success"
            return summary

        task_log = None
        job = None
        if not dry_run:
            job = self.job_state_service.start_job(
                job_name=DAILY_JOB_NAME,
                job_date=job_date,
                request_payload={
                    "point_provider": point_provider_name,
                    "parking_provider": parking_provider_name,
                    "retry_pending_only": retry_pending_only,
                    "skip_member_sync": skip_member_sync,
                    "member_limit": member_limit,
                },
                commit=True,
            )
            task_log = self.task_log_service.create_log(
                module_name="daily_incremental_sync",
                action="run_daily_incremental_sync",
                target_type="date",
                target_value=job_date.isoformat(),
                triggered_by=username,
                triggered_source="script",
                request_payload={
                    "point_provider": point_provider_name,
                    "parking_provider": parking_provider_name,
                    "dry_run": dry_run,
                    "retry_pending_only": retry_pending_only,
                    "skip_member_sync": skip_member_sync,
                    "member_limit": member_limit,
                },
                commit=True,
            )

        try:
            point_provider = resolve_point_flow_provider(
                provider_name=point_provider_name,
                username=username,
                password=password,
                logger=self.logger,
            )
            parking_provider = resolve_parking_provider(parking_provider_name, logger=self.logger)

            point_window_sync = WindowSyncService(
                self.db,
                dataset_name="member_point_flow",
                job_name=POINT_FLOW_JOB_NAME,
                importer=MemberPointFlowCsvImportService(self.db, logger=self.logger),
                logger=self.logger,
            )
            parking_window_sync = WindowSyncService(
                self.db,
                dataset_name="parking_record",
                job_name=PARKING_JOB_NAME,
                importer=ParkingRecordCsvImportService(self.db, logger=self.logger),
                logger=self.logger,
            )

            point_backfill = BackfillService(
                self.db,
                dataset_name="member_point_flow",
                coverage_loader=lambda start_value, end_value: self.coverage_service.detect_point_flow_missing_dates(
                    start_date=start_value,
                    end_date=end_value,
                ),
                window_sync_service=point_window_sync,
                logger=self.logger,
            )
            parking_backfill = BackfillService(
                self.db,
                dataset_name="parking_record",
                coverage_loader=lambda start_value, end_value: self.coverage_service.detect_parking_missing_dates(
                    start_date=start_value,
                    end_date=end_value,
                ),
                window_sync_service=parking_window_sync,
                logger=self.logger,
            )

            summary.point_backfill = point_backfill.backfill_missing_dates(
                provider=point_provider,
                start_date=DEFAULT_POINT_FLOW_START_DATE,
                end_date=job_date,
                dry_run=dry_run,
                check_only=False,
                force=False,
            )
            summary.parking_backfill = parking_backfill.backfill_missing_dates(
                provider=parking_provider,
                start_date=DEFAULT_PARKING_START_DATE,
                end_date=job_date,
                dry_run=dry_run,
                check_only=False,
                force=False,
            )

            point_available_dates = point_provider.available_dates({job_date})
            point_rows = point_provider.fetch_rows_by_date(point_available_dates) if point_available_dates else {}
            summary.point_daily = point_window_sync.sync_date(
                target_date=job_date,
                provider=point_provider,
                rows=point_rows.get(job_date, []),
                source_available=job_date in point_available_dates,
                dry_run=dry_run,
                force=False,
            )

            parking_available_dates = parking_provider.available_dates({job_date})
            parking_rows = parking_provider.fetch_rows_by_date(parking_available_dates) if parking_available_dates else {}
            summary.parking_daily = parking_window_sync.sync_date(
                target_date=job_date,
                provider=parking_provider,
                rows=parking_rows.get(job_date, []),
                source_available=job_date in parking_available_dates,
                dry_run=dry_run,
                force=False,
            )

            if skip_member_sync:
                summary.member_sync = NewMemberSyncSummary(
                    start_date=job_date.isoformat(),
                    end_date=job_date.isoformat(),
                    dry_run=dry_run,
                    skipped_count=1,
                )
            else:
                member_sync_service = NewMemberSyncService(self.db, logger=self.logger)
                summary.member_sync = member_sync_service.sync_from_business_data(
                    start_date=job_date,
                    end_date=job_date,
                    username=username,
                    password=password,
                    dry_run=dry_run,
                    limit=member_limit,
                )

            summary.status = "success"
            if not dry_run and job is not None and task_log is not None:
                self.job_state_service.mark_success(
                    job,
                    success_start=job_date,
                    success_end=job_date,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_success(task_log, result_payload=summary.to_dict(), commit=False)
                self.db.commit()
            return summary
        except Exception as exc:
            summary.status = "failed"
            summary.error_message = str(exc)
            if not dry_run and job is not None and task_log is not None:
                self.db.rollback()
                self.job_state_service.mark_failure(job, error_message=str(exc), result_payload=summary.to_dict(), commit=False)
                self.task_log_service.mark_failure(
                    task_log,
                    error_message=f"{exc}\n{traceback.format_exc()}",
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.db.commit()
            raise
