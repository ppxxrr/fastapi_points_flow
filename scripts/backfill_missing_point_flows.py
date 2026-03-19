from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.incremental_sync_service import (
    DEFAULT_POINT_FLOW_PROVIDER,
    DEFAULT_POINT_FLOW_START_DATE,
    BackfillService,
    CoverageService,
    MemberPointFlowCsvImportService,
    POINT_FLOW_JOB_NAME,
    WindowSyncService,
    parse_date_arg,
    resolve_point_flow_provider,
    yesterday,
)
from app.services.script_logger import build_script_logger
from app.services.sync_log_service import SyncTaskLogService


logger = build_script_logger("backfill_missing_point_flows", "backfill_point_flow.log")


def log_callback(level: str, message: str) -> None:
    getattr(logger, level.lower(), logger.info)(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detect and backfill missing member point flow business dates.")
    parser.add_argument("--start-date", help=f"Coverage scan start date. Default: {DEFAULT_POINT_FLOW_START_DATE.isoformat()}")
    parser.add_argument("--end-date", help="Coverage scan end date. Default: yesterday")
    parser.add_argument("--check-only", action="store_true", help="Only detect missing dates; do not backfill.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate import without committing.")
    parser.add_argument("--force-start-date", help="Force sync start date and ignore missing-date detection.")
    parser.add_argument("--force-end-date", help="Force sync end date and ignore missing-date detection.")
    parser.add_argument(
        "--provider",
        default=DEFAULT_POINT_FLOW_PROVIDER,
        choices=["auto", "api", "csv"],
        help=f"Point-flow source provider. Default: {DEFAULT_POINT_FLOW_PROVIDER}",
    )
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username for api provider")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password for api provider")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start_date = parse_date_arg(args.start_date, DEFAULT_POINT_FLOW_START_DATE)
    end_date = parse_date_arg(args.end_date, yesterday())
    force = bool(args.force_start_date or args.force_end_date)
    if force:
        start_date = parse_date_arg(args.force_start_date, start_date)
        end_date = parse_date_arg(args.force_end_date, end_date)

    with SessionLocal() as session:
        log_service = SyncTaskLogService(session)
        task_log = log_service.create_log(
            module_name="member_point_flow_incremental",
            action="backfill_missing_dates",
            target_type="date_range",
            target_value=f"{start_date.isoformat()}~{end_date.isoformat()}",
            triggered_by=args.username or None,
            triggered_source="script",
            request_payload={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "provider": args.provider,
                "check_only": args.check_only,
                "dry_run": args.dry_run,
                "force": force,
            },
            commit=True,
        )
        try:
            coverage_service = CoverageService(session)
            window_sync_service = WindowSyncService(
                session,
                dataset_name="member_point_flow",
                job_name=POINT_FLOW_JOB_NAME,
                importer=MemberPointFlowCsvImportService(session, logger=log_callback),
                logger=log_callback,
            )
            service = BackfillService(
                session,
                dataset_name="member_point_flow",
                coverage_loader=lambda scan_start, scan_end: coverage_service.detect_point_flow_missing_dates(
                    start_date=scan_start,
                    end_date=scan_end,
                ),
                window_sync_service=window_sync_service,
                logger=log_callback,
            )
            provider = resolve_point_flow_provider(
                provider_name=args.provider,
                username=args.username or None,
                password=args.password or None,
                logger=log_callback,
            )
            summary = service.backfill_missing_dates(
                provider=provider,
                start_date=start_date,
                end_date=end_date,
                dry_run=args.dry_run,
                check_only=args.check_only,
                force=force,
            )
            log_service.mark_success(task_log, result_payload=summary.to_dict(), commit=True)
        except Exception as exc:
            logger.exception("backfill_missing_point_flows failed")
            log_service.mark_failure(task_log, error_message=str(exc), commit=True)
            raise

    logger.info(
        "backfill_missing_point_flows completed start=%s end=%s missing=%s synced=%s dry_run=%s check_only=%s",
        start_date,
        end_date,
        len(summary.missing_dates),
        len(summary.synced_dates),
        args.dry_run,
        args.check_only,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
