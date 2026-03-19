from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select


BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.models.parking import ParkingRecord
from app.models.sync import SyncTaskLog
from app.services.import_utils import clean_text, load_csv_header
from app.services.member_point_flow_service import PARKING_HEADER_FIELDS


DEFAULT_INPUT_DIR = Path(
    os.getenv(
        "PARKING_SOURCE_DIR",
        r"D:\python\menbers\claude\backup\parking\exports_leave_time_20250101_20260228",
    )
)

FILE_DATE_RE = re.compile(r"(20\d{6})_(20\d{6})")


@dataclass(slots=True)
class FileScanRow:
    file_name: str
    encoding: str | None
    is_valid_parking_csv: bool
    data_rows: int | None
    source_start_date: str | None = None
    source_end_date: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check parking CSV volume vs parking_record table volume.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Parking CSV directory.")
    parser.add_argument("--pattern", default="*.csv", help="File match pattern. Default: *.csv")
    parser.add_argument("--limit-files", type=int, help="Only inspect the first N files.")
    parser.add_argument("--scan-csv-rows", action="store_true", help="Scan each valid CSV and count data rows.")
    parser.add_argument("--sample-files", type=int, default=20, help="Include the first N file summaries in output.")
    parser.add_argument("--recent-log-limit", type=int, default=10, help="Recent parking-related task logs to show.")
    return parser


def parse_date_range_from_name(file_name: str) -> tuple[str | None, str | None]:
    match = FILE_DATE_RE.search(file_name)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def count_csv_data_rows(path: Path, encoding: str) -> int:
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


def scan_files(
    *,
    input_dir: Path,
    pattern: str,
    limit_files: int | None,
    scan_csv_rows: bool,
) -> tuple[list[FileScanRow], dict[str, Any]]:
    rows: list[FileScanRow] = []
    total_files = 0
    valid_files = 0
    total_data_rows = 0
    invalid_files = 0
    scanned_files = 0
    min_source_start: str | None = None
    max_source_end: str | None = None

    for csv_file in sorted(input_dir.glob(pattern)):
        total_files += 1
        if limit_files is not None and scanned_files >= limit_files:
            break
        scanned_files += 1
        source_start_date, source_end_date = parse_date_range_from_name(csv_file.name)

        headers, encoding = load_csv_header(csv_file)
        if headers is None or encoding is None:
            invalid_files += 1
            rows.append(
                FileScanRow(
                    file_name=csv_file.name,
                    encoding=None,
                    is_valid_parking_csv=False,
                    data_rows=None,
                    source_start_date=source_start_date,
                    source_end_date=source_end_date,
                    error="failed to detect CSV encoding",
                )
            )
            continue

        is_valid = PARKING_HEADER_FIELDS.issubset(set(headers))
        if not is_valid:
            invalid_files += 1
            rows.append(
                FileScanRow(
                    file_name=csv_file.name,
                    encoding=encoding,
                    is_valid_parking_csv=False,
                    data_rows=None,
                    source_start_date=source_start_date,
                    source_end_date=source_end_date,
                    error="header does not match parking schema",
                )
            )
            continue

        valid_files += 1
        if source_start_date and (min_source_start is None or source_start_date < min_source_start):
            min_source_start = source_start_date
        if source_end_date and (max_source_end is None or source_end_date > max_source_end):
            max_source_end = source_end_date

        data_rows: int | None = None
        error: str | None = None
        if scan_csv_rows:
            try:
                data_rows = count_csv_data_rows(csv_file, encoding)
                total_data_rows += data_rows
            except Exception as exc:
                error = str(exc)

        rows.append(
            FileScanRow(
                file_name=csv_file.name,
                encoding=encoding,
                is_valid_parking_csv=True,
                data_rows=data_rows,
                source_start_date=source_start_date,
                source_end_date=source_end_date,
                error=error,
            )
        )

    aggregate = {
        "input_dir": str(input_dir.resolve()),
        "total_files_found": total_files,
        "scanned_files": scanned_files,
        "valid_parking_csv_files": valid_files,
        "invalid_or_skipped_files": invalid_files,
        "total_csv_data_rows": total_data_rows if scan_csv_rows else None,
        "min_source_start_date": min_source_start,
        "max_source_end_date": max_source_end,
        "scan_csv_rows": scan_csv_rows,
    }
    return rows, aggregate


def build_recent_log_summary(session, limit: int) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(SyncTaskLog)
        .where(
            SyncTaskLog.module_name.in_(
                ["parking_record_csv_import", "parking_record_incremental", "daily_incremental_sync"]
            )
        )
        .order_by(SyncTaskLog.id.desc())
        .limit(limit)
    ).all()

    summaries: list[dict[str, Any]] = []
    for row in rows:
        result_payload = row.result_payload if isinstance(row.result_payload, dict) else None
        summaries.append(
            {
                "id": row.id,
                "module_name": row.module_name,
                "action": row.action,
                "status": row.status,
                "target_value": row.target_value,
                "started_at": row.started_at.isoformat(sep=" ", timespec="seconds") if row.started_at else None,
                "finished_at": row.finished_at.isoformat(sep=" ", timespec="seconds") if row.finished_at else None,
                "result_payload": result_payload,
                "error_message": row.error_message,
            }
        )
    return summaries


def build_table_summary(session) -> dict[str, Any]:
    total_count = int(session.scalar(select(func.count()).select_from(ParkingRecord)) or 0)
    null_mobile_count = int(
        session.scalar(
            select(func.count()).select_from(ParkingRecord).where(
                ParkingRecord.mobile_no.is_(None) | (ParkingRecord.mobile_no == "")
            )
        )
        or 0
    )
    null_plate_count = int(
        session.scalar(
            select(func.count()).select_from(ParkingRecord).where(
                ParkingRecord.plate_no.is_(None) | (ParkingRecord.plate_no == "")
            )
        )
        or 0
    )
    null_record_id_count = int(
        session.scalar(
            select(func.count()).select_from(ParkingRecord).where(
                ParkingRecord.record_id.is_(None) | (ParkingRecord.record_id == "")
            )
        )
        or 0
    )
    null_parking_serial_count = int(
        session.scalar(
            select(func.count()).select_from(ParkingRecord).where(
                ParkingRecord.parking_serial_no.is_(None) | (ParkingRecord.parking_serial_no == "")
            )
        )
        or 0
    )
    source_file_count = int(
        session.scalar(
            select(func.count(func.distinct(ParkingRecord.source_file))).where(ParkingRecord.source_file.is_not(None))
        )
        or 0
    )
    min_entry = session.scalar(select(func.min(ParkingRecord.entry_time)))
    max_exit = session.scalar(select(func.max(ParkingRecord.exit_time)))

    return {
        "parking_record_count": total_count,
        "distinct_source_file_count": source_file_count,
        "null_mobile_no_count": null_mobile_count,
        "null_plate_no_count": null_plate_count,
        "null_record_id_count": null_record_id_count,
        "null_parking_serial_no_count": null_parking_serial_count,
        "min_entry_time": min_entry.isoformat(sep=" ", timespec="seconds") if isinstance(min_entry, datetime) else None,
        "max_exit_time": max_exit.isoformat(sep=" ", timespec="seconds") if isinstance(max_exit, datetime) else None,
    }


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input_dir)

    file_rows, file_summary = scan_files(
        input_dir=input_dir,
        pattern=args.pattern,
        limit_files=args.limit_files,
        scan_csv_rows=args.scan_csv_rows,
    )

    with SessionLocal() as session:
        table_summary = build_table_summary(session)
        recent_logs = build_recent_log_summary(session, args.recent_log_limit)

    csv_rows = file_summary.get("total_csv_data_rows")
    table_count = table_summary["parking_record_count"]
    gap_summary = {
        "csv_total_data_rows": csv_rows,
        "parking_record_count": table_count,
        "difference": (csv_rows - table_count) if isinstance(csv_rows, int) else None,
        "warning": (
            "Parking CSV volume is much larger than parking_record row count. Do not assume parking data is complete."
            if isinstance(csv_rows, int) and csv_rows > table_count
            else None
        ),
    }

    summary = {
        "input_dir": str(input_dir.resolve()),
        "file_scan": file_summary,
        "parking_record_table": table_summary,
        "gap_summary": gap_summary,
        "recent_parking_logs": recent_logs,
        "sample_files": [row.to_dict() for row in file_rows[: max(0, args.sample_files)]],
        "notes": [
            "Use this script to compare raw parking CSV volume with parking_record table volume.",
            "If the gap is very large, keep checking whether all historical files were uploaded, whether only sample files were imported, whether failed/skipped rows are high, and whether the current source directory covers the target dates.",
            "Do not treat the current parking_record count as proof that historical parking data is complete.",
        ],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
