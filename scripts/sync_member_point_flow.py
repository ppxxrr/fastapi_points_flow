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
from app.services.icsp_client import ICSPClient
from app.services.member_point_flow_service import MemberPointFlowSyncService
from app.services.sync_log_service import SyncTaskLogService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync member point flow into database.")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD.")
    parser.add_argument("--member-id", help="Optional member id filter.")
    parser.add_argument("--mobile", help="Optional member mobile filter.")
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username.")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password.")
    return parser


def log_callback(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def main() -> None:
    args = build_parser().parse_args()
    if not args.username or not args.password:
        raise SystemExit("ICSP username/password is required. Use --username/--password or ICSP_USERNAME/ICSP_PASSWORD.")

    client = ICSPClient(logger=log_callback)
    if not client.login(args.username, args.password):
        raise SystemExit(client.last_login_error or "ICSP login failed.")
    if not client.validate_authenticated_session(args.username):
        raise SystemExit("ICSP points-flow session validation failed.")

    with SessionLocal() as session:
        log_service = SyncTaskLogService(session)
        sync_log = log_service.create_log(
            module_name="member_point_flow",
            action="sync_by_date_range",
            target_type="date_range",
            target_value=f"{args.start_date}~{args.end_date}",
            triggered_by=args.username,
            triggered_source="script",
            request_payload={
                "start_date": args.start_date,
                "end_date": args.end_date,
                "member_id": args.member_id,
                "mobile_no": args.mobile,
            },
            commit=True,
        )

        try:
            service = MemberPointFlowSyncService(db=session, icsp_client=client)
            summary = service.sync_point_flow_range(
                start_date=args.start_date,
                end_date=args.end_date,
                member_id=args.member_id,
                mobile_no=args.mobile,
                commit=False,
            )
            log_service.mark_success(sync_log, result_payload=summary.to_dict(), commit=False)
            session.commit()
            print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        except Exception as exc:
            session.rollback()
            log_service.mark_failure(sync_log, error_message=str(exc), commit=True)
            raise


if __name__ == "__main__":
    main()
