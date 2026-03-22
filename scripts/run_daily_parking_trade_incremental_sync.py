from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.incremental_sync_service import parse_date_arg
from app.services.parking_trade_daily_sync_service import (
    DEFAULT_PARKING_TRADE_PROVIDER,
    PARKING_TRADE_INCREMENTAL_LOCK_PATH,
    build_parking_trade_sync_logger,
    parking_trade_target_date,
    run_parking_trade_sync_once,
)
from app.services.script_lock import FileScriptLock


logger = build_parking_trade_sync_logger()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run daily parking trade incremental sync.")
    parser.add_argument("--job-date", help="Business date to sync. Default: today - lag days")
    parser.add_argument(
        "--parking-trade-provider",
        default=DEFAULT_PARKING_TRADE_PROVIDER,
        choices=["auto", "api"],
        help=f"Parking trade provider. Default: {DEFAULT_PARKING_TRADE_PROVIDER}",
    )
    parser.add_argument("--dry-run", action="store_true", help="Simulate without committing.")
    parser.add_argument("--retry-pending-only", action="store_true", help="Retry mode: skip when target date already succeeded.")
    parser.add_argument("--force", action="store_true", help="Force sync even if target date already succeeded.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_date = parse_date_arg(args.job_date, parking_trade_target_date())
    lock = FileScriptLock(PARKING_TRADE_INCREMENTAL_LOCK_PATH)

    if not lock.acquire():
        summary = {
            "job_date": job_date.isoformat(),
            "status": "skipped_locked",
            "lock_path": str(PARKING_TRADE_INCREMENTAL_LOCK_PATH.resolve()),
        }
        logger.warning(
            "daily parking trade incremental sync skipped because another process holds %s",
            PARKING_TRADE_INCREMENTAL_LOCK_PATH,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    with lock:
        summary = run_parking_trade_sync_once(
            job_date=job_date,
            provider_name=args.parking_trade_provider,
            dry_run=args.dry_run,
            retry_pending_only=args.retry_pending_only,
            force=args.force,
            triggered_source="script",
        )

    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
