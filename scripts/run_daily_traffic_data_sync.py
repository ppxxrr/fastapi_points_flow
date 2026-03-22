from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.legacy_flow_service import LegacyFlowService
from app.services.script_lock import FileScriptLock
from app.services.script_logger import build_script_logger


LOCK_PATH = BASE_DIR / "data" / "locks" / "daily_traffic_data_sync.lock"
logger = build_script_logger("run_daily_traffic_data_sync", "daily_traffic_data_sync.log")


def target_date() -> date:
    return datetime.now().date() - timedelta(days=1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync daily passenger traffic nodes from API into server database.")
    parser.add_argument("--job-date", help="Business date to sync. Default: yesterday")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_date = date.fromisoformat(args.job_date) if args.job_date else target_date()
    lock = FileScriptLock(LOCK_PATH)
    if not lock.acquire():
        print(
            json.dumps(
                {"job_date": job_date.isoformat(), "status": "skipped_locked", "lock_path": str(LOCK_PATH.resolve())},
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    with lock:
        with SessionLocal() as session:
            service = LegacyFlowService(session, logger=lambda level, message: getattr(logger, level.lower(), logger.info)(message))
            sync_summary = service.sync_traffic_from_api(job_date=job_date)
            integrity_summary = service.build_traffic_integrity_summary()
    print(
        json.dumps(
            {
                "job_date": job_date.isoformat(),
                "status": "success",
                "sync_summary": sync_summary.to_dict(),
                "integrity_summary": integrity_summary.to_dict(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
