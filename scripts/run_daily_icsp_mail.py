from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.icsp_mail_service import icsp_mail_target_date, run_icsp_mail_once
from app.services.incremental_sync_service import parse_date_arg
from app.services.script_lock import FileScriptLock


LOCK_PATH = BASE_DIR / "data" / "scheduler" / "run_daily_icsp_mail.lock"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send daily ICSP verification and analysis email.")
    parser.add_argument("--job-date", help="Business date to verify and report. Default: yesterday")
    parser.add_argument("--dry-run", action="store_true", help="Build report without sending email.")
    parser.add_argument(
        "--retry-pending-only",
        action="store_true",
        help="Retry mode: if the target date already succeeded, skip immediately.",
    )
    parser.add_argument("--force", action="store_true", help="Send email even if the target date already has result state.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    job_date = parse_date_arg(args.job_date, icsp_mail_target_date())
    lock = FileScriptLock(LOCK_PATH)
    if not lock.acquire():
        summary = {
            "job_date": job_date.isoformat(),
            "status": "skipped_locked",
            "lock_path": str(LOCK_PATH.resolve()),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    with lock:
        summary = run_icsp_mail_once(
            job_date=job_date,
            dry_run=args.dry_run,
            retry_pending_only=args.retry_pending_only,
            force=args.force,
            triggered_source="script",
        )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
