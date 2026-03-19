from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.member_point_flow_service import MemberPointFlowCsvImportService
from app.services.sync_log_service import SyncTaskLogService


DEFAULT_INPUT_DIR = Path(r"D:\python\menbers\claude\members\points_flow_quarterly_exports")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import member point flow CSV files into the database.")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Directory that contains point flow CSV files.")
    parser.add_argument("--pattern", default="*.csv", help="File match pattern. Default: *.csv")
    parser.add_argument("--limit-files", type=int, help="Only process the first N valid CSV data files.")
    parser.add_argument("--limit-rows", type=int, help="Only process the first N data rows across all files.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per import batch. Default: 1000")
    parser.add_argument("--dry-run", action="store_true", help="Parse and compare rows without committing changes.")
    return parser


def log_callback(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def main() -> None:
    args = build_parser().parse_args()

    with SessionLocal() as session:
        log_service = SyncTaskLogService(session)
        sync_log = log_service.create_log(
            module_name="member_point_flow_csv_import",
            action="import_from_csv_directory",
            target_type="directory",
            target_value=str(Path(args.input_dir).resolve()),
            triggered_source="script",
            request_payload={
                "input_dir": args.input_dir,
                "pattern": args.pattern,
                "limit_files": args.limit_files,
                "limit_rows": args.limit_rows,
                "batch_size": args.batch_size,
                "dry_run": args.dry_run,
            },
            commit=True,
        )

        try:
            service = MemberPointFlowCsvImportService(db=session, logger=log_callback, batch_size=args.batch_size)
            summary = service.import_directory(
                input_dir=args.input_dir,
                pattern=args.pattern,
                limit_files=args.limit_files,
                limit_rows=args.limit_rows,
                dry_run=args.dry_run,
            )
            if args.dry_run:
                session.rollback()
            log_service.mark_success(sync_log, result_payload=summary.to_dict(), commit=True)
        except Exception as exc:
            session.rollback()
            log_service.mark_failure(sync_log, error_message=str(exc), commit=True)
            raise

    print("\n=== Summary ===")
    print(f"input_dir: {summary.input_dir}")
    print(f"pattern: {summary.pattern}")
    print(f"total_csv_files: {summary.total_csv_files}")
    print(f"processed_csv_files: {summary.processed_csv_files}")
    print(f"skipped_csv_files: {summary.skipped_csv_files}")
    print(f"total_rows: {summary.total_rows}")
    print(f"inserted_count: {summary.inserted_count}")
    print(f"updated_count: {summary.updated_count}")
    print(f"skipped_count: {summary.skipped_count}")
    print(f"failed_count: {summary.failed_count}")
    print(f"dry_run: {summary.dry_run}")
    print("\n=== JSON Summary ===")
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
