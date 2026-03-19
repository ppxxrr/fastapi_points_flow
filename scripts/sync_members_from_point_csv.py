from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.member_csv_sync_service import MemberCsvSyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract memberPhone from point flow CSV files and sync member information into database.",
    )
    parser.add_argument("--input-dir", required=True, help="Directory that contains point flow CSV files.")
    parser.add_argument("--pattern", default="*.csv", help="File match pattern. Default: *.csv")
    parser.add_argument("--limit", type=int, help="Only process the first N deduplicated mobiles.")
    parser.add_argument("--dry-run", action="store_true", help="Only extract and count mobiles, do not sync.")
    parser.add_argument("--output", help="Optional output CSV path.")
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username.")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password.")
    return parser


def build_default_output_path() -> Path:
    export_dir = BASE_DIR / "data" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"member_sync_from_point_csv_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return export_dir / file_name


def log_callback(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def main() -> None:
    args = build_parser().parse_args()
    output_path = Path(args.output) if args.output else (None if args.dry_run else build_default_output_path())

    if not args.dry_run and (not args.username or not args.password):
        raise SystemExit("ICSP username/password is required unless --dry-run is used.")

    with SessionLocal() as session:
        service = MemberCsvSyncService(db=session, logger=log_callback)
        summary = service.sync_from_directory(
            input_dir=args.input_dir,
            pattern=args.pattern,
            limit=args.limit,
            dry_run=args.dry_run,
            username=args.username or None,
            password=args.password or None,
        )

        if output_path is not None:
            service.write_result_csv(output_path, summary.results)

    print("\n=== Summary ===")
    print(f"input_dir: {summary.input_dir}")
    print(f"pattern: {summary.pattern}")
    print(f"total_csv_files: {summary.total_csv_files}")
    print(f"csv_files_read: {summary.csv_files_read}")
    print(f"csv_files_skipped: {summary.csv_files_skipped}")
    print(f"total_records: {summary.total_records}")
    print(f"valid_mobile_records: {summary.valid_mobile_records}")
    print(f"deduplicated_mobile_count: {summary.deduplicated_mobile_count}")
    print(f"processed_mobile_count: {summary.processed_mobile_count}")
    print(f"dry_run: {summary.dry_run}")
    if not summary.dry_run:
        print(f"success_count: {summary.success_count}")
        print(f"not_found_count: {summary.not_found_count}")
        print(f"failed_count: {summary.failed_count}")
    if output_path is not None:
        print(f"output: {output_path.resolve()}")

    if summary.file_issues:
        print("\n=== File Issues ===")
        for issue in summary.file_issues:
            print(f"{issue.file_path}: {issue.reason}")

    print("\n=== JSON Summary ===")
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
