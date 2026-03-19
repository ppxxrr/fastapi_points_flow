from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
from sqlalchemy.orm import Session

from app.models.member import MemberProfile
from app.services.icsp_client import ICSPClient
from app.services.member_sync_service import ICSPMemberSyncService
from app.services.sync_log_service import SyncTaskLogService


LoggerCallback = Callable[[str, str], None]
CSV_ENCODINGS = ("utf-8", "utf-8-sig", "gb18030", "gbk")


def default_logger(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def clean_mobile_value(value: object) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"null", "none", "nan", "<na>", "nat"}:
        return None

    if text.startswith('="') and text.endswith('"'):
        text = text[2:-1]

    text = text.strip().strip('"').strip("'").strip()
    if not text:
        return None

    if text.endswith(".0"):
        text = text[:-2]

    digits = "".join(char for char in text if char.isdigit())
    if len(digits) != 11:
        return None
    if not digits.startswith("1"):
        return None
    return digits


@dataclass(slots=True)
class CsvFileIssue:
    file_path: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(slots=True)
class ExtractedMobileRecord:
    mobile_no: str
    csv_member_id: str | None = None
    csv_member_name: str | None = None
    source_files: list[str] = field(default_factory=list)
    record_count: int = 0

    def touch(self, *, file_path: str, member_id: str | None, member_name: str | None) -> None:
        self.record_count += 1
        if file_path not in self.source_files:
            self.source_files.append(file_path)
        if not self.csv_member_id and member_id:
            self.csv_member_id = member_id
        if not self.csv_member_name and member_name:
            self.csv_member_name = member_name


@dataclass(slots=True)
class MemberCsvSyncResultRow:
    mobile_no: str
    sync_status: str
    error_message: str = ""
    synced_member_id: str = ""
    synced_member_name: str = ""
    sync_time: str = ""
    csv_member_id: str = ""
    csv_member_name: str = ""
    source_files: str = ""
    source_record_count: int = 0

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


@dataclass(slots=True)
class MemberCsvSyncSummary:
    input_dir: str
    pattern: str
    total_csv_files: int = 0
    csv_files_read: int = 0
    csv_files_skipped: int = 0
    total_records: int = 0
    valid_mobile_records: int = 0
    deduplicated_mobile_count: int = 0
    processed_mobile_count: int = 0
    success_count: int = 0
    not_found_count: int = 0
    failed_count: int = 0
    dry_run: bool = False
    results: list[MemberCsvSyncResultRow] = field(default_factory=list)
    file_issues: list[CsvFileIssue] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "input_dir": self.input_dir,
            "pattern": self.pattern,
            "total_csv_files": self.total_csv_files,
            "csv_files_read": self.csv_files_read,
            "csv_files_skipped": self.csv_files_skipped,
            "total_records": self.total_records,
            "valid_mobile_records": self.valid_mobile_records,
            "deduplicated_mobile_count": self.deduplicated_mobile_count,
            "processed_mobile_count": self.processed_mobile_count,
            "success_count": self.success_count,
            "not_found_count": self.not_found_count,
            "failed_count": self.failed_count,
            "dry_run": self.dry_run,
            "results": [row.to_dict() for row in self.results],
            "file_issues": [issue.to_dict() for issue in self.file_issues],
        }


class MemberCsvSyncService:
    def __init__(self, db: Session, logger: LoggerCallback | None = None):
        self.db = db
        self.logger = logger or default_logger

    def log(self, level: str, message: str) -> None:
        self.logger(level, message)

    def sync_from_directory(
        self,
        *,
        input_dir: str | Path,
        pattern: str = "*.csv",
        limit: int | None = None,
        dry_run: bool = False,
        username: str | None = None,
        password: str | None = None,
    ) -> MemberCsvSyncSummary:
        summary, mobile_records = self.extract_mobile_records(input_dir=input_dir, pattern=pattern)
        if limit is not None and limit >= 0:
            mobile_records = mobile_records[:limit]
        summary.deduplicated_mobile_count = len(mobile_records)
        summary.processed_mobile_count = len(mobile_records)
        summary.dry_run = dry_run

        if dry_run:
            for record in mobile_records:
                summary.results.append(self._build_result_row(record=record, sync_status="dry_run"))
            return summary

        if not username or not password:
            raise ValueError("username and password are required when dry_run is False")

        client = ICSPClient(logger=self.logger)
        if not client.login(username, password):
            raise RuntimeError(client.last_login_error or "ICSP login failed")
        if not client.validate_member_session(username):
            raise RuntimeError("ICSP member session validation failed")

        sync_log_service = SyncTaskLogService(self.db)
        batch_log = sync_log_service.create_log(
            module_name="member_info",
            action="sync_from_point_csv",
            target_type="directory",
            target_value=str(Path(input_dir).resolve()),
            triggered_by=username,
            triggered_source="script",
            request_payload={
                "input_dir": str(Path(input_dir).resolve()),
                "pattern": pattern,
                "limit": limit,
                "dry_run": dry_run,
                "deduplicated_mobile_count": len(mobile_records),
            },
            commit=True,
        )

        member_sync_service = ICSPMemberSyncService(db=self.db, icsp_client=client)
        for index, record in enumerate(mobile_records, start=1):
            self.log("INFO", f"[member-csv-sync] syncing {index}/{len(mobile_records)} mobile={record.mobile_no}")
            item_log = sync_log_service.create_log(
                module_name="member_info",
                action="sync_by_mobile",
                target_type="mobile_no",
                target_value=record.mobile_no,
                triggered_by=username,
                triggered_source="script",
                request_payload={
                    "mobile_no": record.mobile_no,
                    "csv_member_id": record.csv_member_id,
                    "csv_member_name": record.csv_member_name,
                    "source_files": record.source_files,
                    "batch_log_id": batch_log.id,
                },
                commit=True,
            )
            try:
                result = member_sync_service.sync_member_by_mobile(record.mobile_no, commit=False)
                synced_member_id = result.matched_member_ids[0] if result.matched_member_ids else ""
                synced_member_name = self._query_member_name(synced_member_id) or record.csv_member_name or ""

                if result.matched_member_ids:
                    summary.success_count += 1
                    sync_status = "success"
                    error_message = "; ".join(result.warnings)
                else:
                    summary.not_found_count += 1
                    sync_status = "not_found"
                    error_message = "; ".join(result.warnings) or "No member matched by mobile."

                summary.results.append(
                    self._build_result_row(
                        record=record,
                        sync_status=sync_status,
                        error_message=error_message,
                        synced_member_id=synced_member_id,
                        synced_member_name=synced_member_name,
                    )
                )
                sync_log_service.mark_success(
                    item_log,
                    result_payload=result.to_dict(),
                    commit=False,
                )
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                summary.failed_count += 1
                summary.results.append(
                    self._build_result_row(
                        record=record,
                        sync_status="failed",
                        error_message=str(exc),
                    )
                )
                sync_log_service.mark_failure(item_log, error_message=str(exc), commit=True)

        sync_log_service.mark_success(batch_log, result_payload=summary.to_dict(), commit=True)
        return summary

    def extract_mobile_records(
        self,
        *,
        input_dir: str | Path,
        pattern: str = "*.csv",
    ) -> tuple[MemberCsvSyncSummary, list[ExtractedMobileRecord]]:
        directory = Path(input_dir)
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"input_dir does not exist or is not a directory: {directory}")

        csv_files = sorted(directory.glob(pattern))
        summary = MemberCsvSyncSummary(input_dir=str(directory.resolve()), pattern=pattern, total_csv_files=len(csv_files))
        record_map: dict[str, ExtractedMobileRecord] = {}

        if not csv_files:
            self.log("WARN", f"[member-csv-sync] no csv files matched pattern={pattern} under {directory}")
            return summary, []

        for csv_file in csv_files:
            issue = self._process_single_csv(csv_file=csv_file, summary=summary, record_map=record_map)
            if issue is not None:
                summary.file_issues.append(issue)
                summary.csv_files_skipped += 1
            else:
                summary.csv_files_read += 1

        mobile_records = list(record_map.values())
        summary.deduplicated_mobile_count = len(mobile_records)
        return summary, mobile_records

    @staticmethod
    def write_result_csv(output_path: str | Path, rows: list[MemberCsvSyncResultRow]) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8-sig", newline="") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=[
                    "mobile_no",
                    "sync_status",
                    "error_message",
                    "synced_member_id",
                    "synced_member_name",
                    "sync_time",
                    "csv_member_id",
                    "csv_member_name",
                    "source_files",
                    "source_record_count",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row.to_dict())
        return path

    def _process_single_csv(
        self,
        *,
        csv_file: Path,
        summary: MemberCsvSyncSummary,
        record_map: dict[str, ExtractedMobileRecord],
    ) -> CsvFileIssue | None:
        header_frame, encoding, header_error = self._read_csv_header(csv_file)
        if header_error is not None or header_frame is None:
            reason = header_error or "Unknown CSV read error"
            self.log("WARN", f"[member-csv-sync] failed to read header for {csv_file.name}: {reason}")
            return CsvFileIssue(file_path=str(csv_file), reason=reason)

        resolved_columns = self._resolve_required_columns(header_frame.columns.tolist())
        if resolved_columns["memberPhone"] is None:
            reason = "memberPhone column not found"
            self.log("WARN", f"[member-csv-sync] skipped {csv_file.name}: {reason}")
            return CsvFileIssue(file_path=str(csv_file), reason=reason)

        try:
            use_columns = [column for column in resolved_columns.values() if column is not None]
            frame = pd.read_csv(
                csv_file,
                encoding=encoding,
                dtype=str,
                usecols=use_columns,
                on_bad_lines="skip",
            )
        except Exception as exc:
            self.log("WARN", f"[member-csv-sync] failed to read {csv_file.name}: {exc}")
            return CsvFileIssue(file_path=str(csv_file), reason=str(exc))

        summary.total_records += len(frame)
        member_phone_column = resolved_columns["memberPhone"]
        member_id_column = resolved_columns["memberId"]
        member_name_column = resolved_columns["memberName"]

        for row in frame.itertuples(index=False):
            row_data = row._asdict()
            cleaned_mobile = clean_mobile_value(row_data.get(member_phone_column or ""))
            if not cleaned_mobile:
                continue
            summary.valid_mobile_records += 1
            record = record_map.get(cleaned_mobile)
            if record is None:
                record = ExtractedMobileRecord(mobile_no=cleaned_mobile)
                record_map[cleaned_mobile] = record
            record.touch(
                file_path=str(csv_file.resolve()),
                member_id=self._clean_optional_text(row_data.get(member_id_column)) if member_id_column else None,
                member_name=self._clean_optional_text(row_data.get(member_name_column)) if member_name_column else None,
            )
        return None

    @staticmethod
    def _clean_optional_text(value: object) -> str | None:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip()
        if text.startswith('="') and text.endswith('"'):
            text = text[2:-1]
        text = text.strip().strip('"').strip("'").strip()
        if text.lower() in {"null", "none", "nan", "<na>", "nat"}:
            return None
        return text or None

    @staticmethod
    def _resolve_required_columns(columns: list[str]) -> dict[str, str | None]:
        normalized = {str(column).strip().lower(): column for column in columns}
        return {
            "memberPhone": normalized.get("memberphone"),
            "memberId": normalized.get("memberid"),
            "memberName": normalized.get("membername"),
        }

    @staticmethod
    def _read_csv_header(csv_file: Path) -> tuple[pd.DataFrame | None, str | None, str | None]:
        for encoding in CSV_ENCODINGS:
            try:
                frame = pd.read_csv(csv_file, encoding=encoding, dtype=str, nrows=0, on_bad_lines="skip")
                return frame, encoding, None
            except Exception as exc:
                last_error = str(exc)
        return None, None, last_error

    def _query_member_name(self, member_id: str | None) -> str | None:
        if not member_id:
            return None
        profile = self.db.get(MemberProfile, member_id)
        if profile is None:
            return None
        return profile.member_name or None

    @staticmethod
    def _build_result_row(
        *,
        record: ExtractedMobileRecord,
        sync_status: str,
        error_message: str = "",
        synced_member_id: str = "",
        synced_member_name: str = "",
    ) -> MemberCsvSyncResultRow:
        return MemberCsvSyncResultRow(
            mobile_no=record.mobile_no,
            sync_status=sync_status,
            error_message=error_message,
            synced_member_id=synced_member_id,
            synced_member_name=synced_member_name,
            sync_time=datetime.now().isoformat(timespec="seconds"),
            csv_member_id=record.csv_member_id or "",
            csv_member_name=record.csv_member_name or "",
            source_files=" | ".join(record.source_files),
            source_record_count=record.record_count,
        )
