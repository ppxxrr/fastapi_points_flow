from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.incremental_sync_service import BusinessMobileExtractionService, parse_date_arg, yesterday
from app.services.script_logger import build_script_logger
from app.services.sync_log_service import SyncTaskLogService


logger = build_script_logger("extract_new_member_mobiles", "new_member_sync.log")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract new member mobiles from business tables.")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD. Default: yesterday")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD. Default: yesterday")
    parser.add_argument("--output", help="Optional output CSV path.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output CSV.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    default_date = yesterday()
    start_date = parse_date_arg(args.start_date, default_date)
    end_date = parse_date_arg(args.end_date, default_date)

    with SessionLocal() as session:
        log_service = SyncTaskLogService(session)
        task_log = log_service.create_log(
            module_name="member_incremental",
            action="extract_new_member_mobiles",
            target_type="date_range",
            target_value=f"{start_date.isoformat()}~{end_date.isoformat()}",
            triggered_source="script",
            request_payload={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "output": args.output,
                "dry_run": args.dry_run,
            },
            commit=True,
        )
        try:
            service = BusinessMobileExtractionService(session)
            summary = service.extract_new_mobiles(
                start_date=start_date,
                end_date=end_date,
                dry_run=args.dry_run,
                output_path=args.output,
            )
            log_service.mark_success(task_log, result_payload=summary.to_dict(), commit=True)
        except Exception as exc:
            logger.exception("extract_new_member_mobiles failed")
            log_service.mark_failure(task_log, error_message=str(exc), commit=True)
            raise

    logger.info("extract_new_member_mobiles completed start=%s end=%s new=%s", start_date, end_date, summary.new_member_count)
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
