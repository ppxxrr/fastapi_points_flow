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
from app.services.member_sync_service import ICSPMemberSyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync member info by member id.")
    parser.add_argument("--member-id", required=True, help="Member business id.")
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
    if not client.validate_member_session(args.username):
        raise SystemExit("ICSP member session validation failed.")

    with SessionLocal() as session:
        sync_service = ICSPMemberSyncService(db=session, icsp_client=client)
        summary = sync_service.sync_member_by_member_id(args.member_id, commit=True)

    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
