from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.incremental_sync_service import NewMemberSyncService, parse_date_arg, yesterday
from app.services.script_logger import build_script_logger
from app.services.sync_log_service import SyncTaskLogService


logger = build_script_logger("sync_new_members_from_business_data", "new_member_sync.log")


def log_callback(level: str, message: str) -> None:
    getattr(logger, level.lower(), logger.info)(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync only new member mobiles extracted from business data.")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD. Default: yesterday")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD. Default: yesterday")
    parser.add_argument("--limit", type=int, help="Only sync the first N new mobiles.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call ICSP member sync.")
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password")
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
            action="sync_new_members_from_business_data_script",
            target_type="date_range",
            target_value=f"{start_date.isoformat()}~{end_date.isoformat()}",
            triggered_by=args.username or None,
            triggered_source="script",
            request_payload={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "limit": args.limit,
                "dry_run": args.dry_run,
            },
            commit=True,
        )
        try:
            service = NewMemberSyncService(session, logger=log_callback)
            summary = service.sync_from_business_data(
                start_date=start_date,
                end_date=end_date,
                username=args.username or None,
                password=args.password or None,
                dry_run=args.dry_run,
                limit=args.limit,
            )
            log_service.mark_success(task_log, result_payload=summary.to_dict(), commit=True)
        except Exception as exc:
            logger.exception("sync_new_members_from_business_data failed")
            log_service.mark_failure(task_log, error_message=str(exc), commit=True)
            raise

    logger.info(
        "sync_new_members_from_business_data completed start=%s end=%s success=%s failed=%s dry_run=%s",
        start_date,
        end_date,
        summary.success_count,
        summary.failed_count,
        summary.dry_run,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
