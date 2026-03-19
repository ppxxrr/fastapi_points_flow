from __future__ import annotations

import csv
import os
import re
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app.db.config import ROOT_DIR, get_database_settings
from app.models.member import MemberAccount, MemberProfile
from app.models.parking import ParkingRecord
from app.models.point_flow import MemberPointFlow
from app.models.sync import SyncTaskLog
from app.models.sync_job import SyncJobState
from app.services.import_utils import clean_text, load_csv_header
from app.services.incremental_sync_service import (
    DEFAULT_PARKING_SOURCE_DIR,
    DEFAULT_POINT_FLOW_SOURCE_DIR,
    yesterday,
)
from app.services.member_point_flow_service import PARKING_HEADER_FIELDS


FILE_DATE_RE = re.compile(r"(20\d{6})_(20\d{6})")
PARKING_SCAN_CACHE_TTL_SECONDS = 300
_parking_scan_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(sep=" ", timespec="seconds")


def _truncate(value: str | None, limit: int = 240) -> str | None:
    if not value:
        return None
    if len(value) <= limit:
        return value
    return f"{value[:limit]}..."


def _parse_date_range_from_name(file_name: str) -> tuple[str | None, str | None]:
    matched = FILE_DATE_RE.search(file_name)
    if not matched:
        return None, None
    return matched.group(1), matched.group(2)


def _count_csv_data_rows(path: Path, encoding: str) -> int:
    count = 0
    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row is None:
                continue
            if not any(clean_text(value) is not None for value in row.values()):
                continue
            count += 1
    return count


class AdminOverviewService:
    def __init__(self, db: Session):
        self.db = db

    def build_overview(self) -> dict[str, Any]:
        settings = get_database_settings()
        url = make_url(settings.database_url)
        database_backend = url.get_backend_name()
        database_connected = bool(self.db.execute(text("SELECT 1")).scalar())

        parking_table = self._build_parking_table_summary()
        parking_source = self._scan_parking_source(DEFAULT_PARKING_SOURCE_DIR)
        parking_risk_flags = self._build_parking_risk_flags(
            parking_table=parking_table,
            parking_source=parking_source,
        )

        return {
            "status": "ok",
            "generated_at": _format_datetime(datetime.now()),
            "server": {
                "web_status": "ok",
                "environment": os.getenv("APP_ENV", "unknown"),
                "hostname": socket.gethostname(),
                "app_host": os.getenv("APP_HOST", "0.0.0.0"),
                "app_port": os.getenv("APP_PORT", "8000"),
                "log_dir": str((ROOT_DIR / "logs").resolve()),
                "point_flow_source_dir": str(DEFAULT_POINT_FLOW_SOURCE_DIR),
                "parking_source_dir": str(DEFAULT_PARKING_SOURCE_DIR),
                "database_backend": database_backend,
            },
            "database": {
                "backend": database_backend,
                "connected": database_connected,
                "database_name": url.database,
                "host": url.host,
                "port": url.port,
                "table_counts": self._build_table_counts(),
            },
            "sync": {
                "recent_jobs": self._load_recent_jobs(limit=15),
                "recent_failures": self._load_recent_failures(limit=10),
            },
            "parking_integrity": {
                "severity": "warning" if parking_risk_flags else "info",
                "integrity_pending": True,
                "headline": "停车场数据完整性待核查",
                "table_summary": parking_table,
                "csv_source": parking_source,
                "risk_flags": parking_risk_flags,
            },
        }

    def _build_table_counts(self) -> dict[str, int]:
        return {
            "member_profile": int(self.db.scalar(select(func.count()).select_from(MemberProfile)) or 0),
            "member_account": int(self.db.scalar(select(func.count()).select_from(MemberAccount)) or 0),
            "member_point_flow": int(self.db.scalar(select(func.count()).select_from(MemberPointFlow)) or 0),
            "parking_record": int(self.db.scalar(select(func.count()).select_from(ParkingRecord)) or 0),
            "sync_job_state": int(self.db.scalar(select(func.count()).select_from(SyncJobState)) or 0),
            "sync_task_log": int(self.db.scalar(select(func.count()).select_from(SyncTaskLog)) or 0),
        }

    def _load_recent_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.db.scalars(
            select(SyncJobState).order_by(SyncJobState.updated_at.desc(), SyncJobState.id.desc()).limit(limit)
        ).all()
        return [
            {
                "id": row.id,
                "job_name": row.job_name,
                "job_date": row.job_date.isoformat(),
                "status": row.status,
                "retry_count": row.retry_count,
                "updated_at": _format_datetime(row.updated_at),
                "last_error": _truncate(row.last_error),
            }
            for row in rows
        ]

    def _load_recent_failures(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.db.scalars(
            select(SyncTaskLog)
            .where(SyncTaskLog.status == "failed")
            .order_by(SyncTaskLog.finished_at.desc(), SyncTaskLog.id.desc())
            .limit(limit)
        ).all()
        return [
            {
                "id": row.id,
                "module_name": row.module_name,
                "action": row.action,
                "status": row.status,
                "target_value": row.target_value,
                "error_message": _truncate(row.error_message, limit=320),
                "finished_at": _format_datetime(row.finished_at),
            }
            for row in rows
        ]

    def _build_parking_table_summary(self) -> dict[str, Any]:
        min_entry = self.db.scalar(select(func.min(ParkingRecord.entry_time)))
        max_exit = self.db.scalar(select(func.max(ParkingRecord.exit_time)))
        return {
            "parking_record_count": int(self.db.scalar(select(func.count()).select_from(ParkingRecord)) or 0),
            "distinct_source_file_count": int(
                self.db.scalar(
                    select(func.count(func.distinct(ParkingRecord.source_file))).where(
                        ParkingRecord.source_file.is_not(None)
                    )
                )
                or 0
            ),
            "null_mobile_no_count": int(
                self.db.scalar(
                    select(func.count()).select_from(ParkingRecord).where(
                        ParkingRecord.mobile_no.is_(None) | (ParkingRecord.mobile_no == "")
                    )
                )
                or 0
            ),
            "null_plate_no_count": int(
                self.db.scalar(
                    select(func.count()).select_from(ParkingRecord).where(
                        ParkingRecord.plate_no.is_(None) | (ParkingRecord.plate_no == "")
                    )
                )
                or 0
            ),
            "null_record_id_count": int(
                self.db.scalar(
                    select(func.count()).select_from(ParkingRecord).where(
                        ParkingRecord.record_id.is_(None) | (ParkingRecord.record_id == "")
                    )
                )
                or 0
            ),
            "null_parking_serial_no_count": int(
                self.db.scalar(
                    select(func.count()).select_from(ParkingRecord).where(
                        ParkingRecord.parking_serial_no.is_(None) | (ParkingRecord.parking_serial_no == "")
                    )
                )
                or 0
            ),
            "min_entry_time": _format_datetime(min_entry),
            "max_exit_time": _format_datetime(max_exit),
        }

    def _scan_parking_source(self, input_dir: Path) -> dict[str, Any]:
        cache_key = str(input_dir.resolve()) if input_dir.exists() else str(input_dir)
        cached = _parking_scan_cache.get(cache_key)
        now = time.monotonic()
        if cached and now - cached[0] < PARKING_SCAN_CACHE_TTL_SECONDS:
            return cached[1]

        total_files = 0
        valid_files = 0
        invalid_files = 0
        total_rows = 0
        min_source_start: str | None = None
        max_source_end: str | None = None
        sample_files: list[dict[str, Any]] = []

        if input_dir.exists() and input_dir.is_dir():
            for csv_file in sorted(input_dir.glob("*.csv")):
                total_files += 1
                headers, encoding = load_csv_header(csv_file)
                source_start_date, source_end_date = _parse_date_range_from_name(csv_file.name)

                if headers is None or encoding is None or not PARKING_HEADER_FIELDS.issubset(set(headers)):
                    invalid_files += 1
                    if len(sample_files) < 8:
                        sample_files.append(
                            {
                                "file_name": csv_file.name,
                                "is_valid_parking_csv": False,
                                "encoding": encoding,
                                "source_start_date": source_start_date,
                                "source_end_date": source_end_date,
                            }
                        )
                    continue

                valid_files += 1
                if source_start_date and (min_source_start is None or source_start_date < min_source_start):
                    min_source_start = source_start_date
                if source_end_date and (max_source_end is None or source_end_date > max_source_end):
                    max_source_end = source_end_date

                data_rows = _count_csv_data_rows(csv_file, encoding)
                total_rows += data_rows
                if len(sample_files) < 8:
                    sample_files.append(
                        {
                            "file_name": csv_file.name,
                            "is_valid_parking_csv": True,
                            "encoding": encoding,
                            "data_rows": data_rows,
                            "source_start_date": source_start_date,
                            "source_end_date": source_end_date,
                        }
                    )

        summary = {
            "input_dir": str(input_dir),
            "exists": input_dir.exists(),
            "is_dir": input_dir.is_dir(),
            "total_files_found": total_files,
            "valid_parking_csv_files": valid_files,
            "invalid_or_skipped_files": invalid_files,
            "total_csv_data_rows": total_rows,
            "min_source_start_date": min_source_start,
            "max_source_end_date": max_source_end,
            "sample_files": sample_files,
            "cached_at": _format_datetime(datetime.now()),
        }
        _parking_scan_cache[cache_key] = (now, summary)
        return summary

    def _build_parking_risk_flags(
        self,
        *,
        parking_table: dict[str, Any],
        parking_source: dict[str, Any],
    ) -> list[str]:
        flags: list[str] = ["停车场数据完整性待核查，当前不能默认认定 parking_record 已覆盖完整历史数据。"]
        if not parking_source["exists"] or not parking_source["is_dir"]:
            flags.append("服务器本地停车 CSV 目录不存在，停车增量会直接失败。")
            return flags

        if parking_source["valid_parking_csv_files"] == 0:
            flags.append("服务器本地停车 CSV 目录为空或文件表头不匹配，停车增量无法继续。")
            return flags

        csv_total_rows = parking_source.get("total_csv_data_rows") or 0
        parking_count = parking_table["parking_record_count"]
        if csv_total_rows > parking_count:
            flags.append(
                f"停车 CSV 原始总行数约 {csv_total_rows}，但 parking_record 当前仅 {parking_count} 条，数量级明显不匹配。"
            )

        max_source_end = parking_source.get("max_source_end_date")
        if max_source_end:
            source_end_date = datetime.strptime(max_source_end, "%Y%m%d").date()
            if source_end_date < yesterday():
                flags.append(
                    f"服务器停车 CSV 覆盖仅到 {source_end_date.isoformat()}，晚于该日期的 daily sync 仍可能报 no source available。"
                )

        if parking_table["distinct_source_file_count"] < parking_source["valid_parking_csv_files"]:
            flags.append("库内 source_file 覆盖文件数少于服务器有效停车 CSV 文件数，历史文件可能尚未全部入库。")

        if parking_table["null_mobile_no_count"] > 0:
            flags.append(
                f"parking_record 中仍有 {parking_table['null_mobile_no_count']} 条记录缺少手机号，不能直接视为会员增量来源已完整。"
            )

        return flags
