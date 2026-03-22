from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import func, select


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.models.parking_trade import ParkingTradeRecord
from app.models.sync_job import SyncJobState
from app.services.incremental_sync_service import iter_dates, normalize_business_date, parse_date_arg, yesterday
from app.services.parking_trade_daily_sync_service import (
    DEFAULT_PARKING_TRADE_PROVIDER,
    DEFAULT_PARKING_TRADE_START_DATE,
    PARKING_TRADE_JOB_NAME,
    run_parking_trade_sync_once,
)


@dataclass(slots=True)
class ParkingTradeBackfillSummary:
    start_date: str
    end_date: str
    provider: str
    dry_run: bool
    check_only: bool
    force: bool
    covered_dates: list[str] = field(default_factory=list)
    missing_dates: list[str] = field(default_factory=list)
    synced_dates: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill parking trade records from API.")
    parser.add_argument("--start-date", help="Default: 2025-01-01")
    parser.add_argument("--end-date", help="Default: yesterday")
    parser.add_argument(
        "--provider",
        default=DEFAULT_PARKING_TRADE_PROVIDER,
        choices=["auto", "api"],
        help=f"Parking trade provider. Default: {DEFAULT_PARKING_TRADE_PROVIDER}",
    )
    parser.add_argument("--dry-run", action="store_true", help="Simulate without committing.")
    parser.add_argument("--check-only", action="store_true", help="Only detect missing dates.")
    parser.add_argument("--force", action="store_true", help="Run the whole range even if already covered.")
    return parser


def load_covered_dates(start_date: date, end_date: date) -> set[date]:
    with SessionLocal() as session:
        data_rows = session.execute(
            select(func.date(func.coalesce(ParkingTradeRecord.result_time, ParkingTradeRecord.pay_time, ParkingTradeRecord.create_time))).where(
                func.date(func.coalesce(ParkingTradeRecord.result_time, ParkingTradeRecord.pay_time, ParkingTradeRecord.create_time)) >= start_date.isoformat(),
                func.date(func.coalesce(ParkingTradeRecord.result_time, ParkingTradeRecord.pay_time, ParkingTradeRecord.create_time)) <= end_date.isoformat(),
            )
        ).all()
        job_rows = session.scalars(
            select(SyncJobState.job_date).where(
                SyncJobState.job_name == PARKING_TRADE_JOB_NAME,
                SyncJobState.status == "success",
                SyncJobState.job_date >= start_date,
                SyncJobState.job_date <= end_date,
            )
        ).all()
    return {
        *{normalized for (value,) in data_rows if (normalized := normalize_business_date(value)) is not None},
        *set(job_rows),
    }


def main() -> None:
    args = build_parser().parse_args()
    start_date = parse_date_arg(args.start_date, DEFAULT_PARKING_TRADE_START_DATE)
    end_date = parse_date_arg(args.end_date, yesterday())
    covered_dates = load_covered_dates(start_date, end_date)
    target_dates = iter_dates(start_date, end_date) if args.force else [value for value in iter_dates(start_date, end_date) if value not in covered_dates]

    summary = ParkingTradeBackfillSummary(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        provider=args.provider,
        dry_run=args.dry_run,
        check_only=args.check_only,
        force=args.force,
        covered_dates=[value.isoformat() for value in sorted(covered_dates)],
        missing_dates=[value.isoformat() for value in target_dates],
    )

    if args.check_only:
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return

    for target_date in target_dates:
        result = run_parking_trade_sync_once(
            job_date=target_date,
            provider_name=args.provider,
            dry_run=args.dry_run,
            retry_pending_only=False,
            force=args.force,
            triggered_source="script",
        )
        summary.synced_dates.append(result.to_dict())

    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
