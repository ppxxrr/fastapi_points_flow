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
from app.services.incremental_sync_service import (
    DEFAULT_PARKING_PROVIDER,
    DEFAULT_POINT_FLOW_PROVIDER,
    DailyIncrementalSyncService,
    parse_date_arg,
    yesterday,
)
from app.services.script_logger import build_script_logger
from app.services.script_lock import FileScriptLock


logger = build_script_logger("run_daily_incremental_sync", "daily_incremental_sync.log")
LOCK_PATH = BASE_DIR / "data" / "scheduler" / "run_daily_incremental_sync.lock"


def log_callback(level: str, message: str) -> None:
    getattr(logger, level.lower(), logger.info)(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run daily incremental sync and retry unfinished business dates.")
    parser.add_argument("--job-date", help="Business date to sync. Default: yesterday")
    parser.add_argument(
        "--point-provider",
        default=DEFAULT_POINT_FLOW_PROVIDER,
        choices=["auto", "api", "csv"],
        help=f"Point-flow provider. Default: {DEFAULT_POINT_FLOW_PROVIDER}",
    )
    parser.add_argument(
        "--parking-provider",
        default=DEFAULT_PARKING_PROVIDER,
        choices=["csv"],
        help=f"Parking provider. Default: {DEFAULT_PARKING_PROVIDER}",
    )
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the full workflow without committing.")
    parser.add_argument(
        "--retry-pending-only",
        action="store_true",
        help="Hourly retry mode: if yesterday already succeeded, skip immediately.",
    )
    parser.add_argument("--skip-member-sync", action="store_true", help="Skip new-member sync step.")
    parser.add_argument("--member-limit", type=int, help="Only sync the first N new mobiles.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_date = parse_date_arg(args.job_date, yesterday())
    lock = FileScriptLock(LOCK_PATH)

    if not lock.acquire():
        summary = {
            "job_date": job_date.isoformat(),
            "status": "skipped_locked",
            "lock_path": str(LOCK_PATH.resolve()),
        }
        logger.warning("run_daily_incremental_sync skipped because another process holds %s", LOCK_PATH)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    with lock:
        with SessionLocal() as session:
            service = DailyIncrementalSyncService(session, logger=log_callback)
            try:
                summary = service.run(
                    job_date=job_date,
                    point_provider_name=args.point_provider,
                    parking_provider_name=args.parking_provider,
                    username=args.username or None,
                    password=args.password or None,
                    dry_run=args.dry_run,
                    retry_pending_only=args.retry_pending_only,
                    skip_member_sync=args.skip_member_sync,
                    member_limit=args.member_limit,
                )
            except Exception:
                logger.exception("run_daily_incremental_sync failed")
                raise

    logger.info(
        "run_daily_incremental_sync completed job_date=%s status=%s dry_run=%s retry_pending_only=%s",
        job_date,
        summary.status,
        args.dry_run,
        args.retry_pending_only,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
