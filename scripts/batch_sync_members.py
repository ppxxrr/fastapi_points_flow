from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.icsp_client import ICSPClient
from app.services.member_sync_service import ICSPMemberSyncService
from app.services.sync_log_service import SyncTaskLogService
from app.utils.member_file_reader import read_mobile_list_from_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch sync member info from Excel or CSV.")
    parser.add_argument("--input", required=True, help="Excel or CSV file path.")
    parser.add_argument("--mobile-column", help="Mobile column header name if auto detection is not enough.")
    parser.add_argument("--output", help="Optional output CSV path.")
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username.")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password.")
    return parser


def log_callback(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def default_output_path() -> Path:
    export_dir = BASE_DIR / "data" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir / f"member_batch_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def main() -> None:
    args = build_parser().parse_args()
    if not args.username or not args.password:
        raise SystemExit("ICSP username/password is required. Use --username/--password or ICSP_USERNAME/ICSP_PASSWORD.")

    read_result = read_mobile_list_from_file(args.input, mobile_column=args.mobile_column)
    output_path = Path(args.output) if args.output else default_output_path()

    client = ICSPClient(logger=log_callback)
    if not client.login(args.username, args.password):
        raise SystemExit(client.last_login_error or "ICSP login failed.")
    if not client.validate_member_session(args.username):
        raise SystemExit("ICSP member session validation failed.")

    result_rows: list[dict[str, str]] = []
    success_count = 0
    failed_count = 0
    not_found_count = 0

    with SessionLocal() as session:
        log_service = SyncTaskLogService(session)
        batch_log = log_service.create_log(
            module_name="member_info",
            action="batch_sync_file",
            target_type="file",
            target_value=str(Path(args.input).resolve()),
            triggered_by=args.username,
            triggered_source="script",
            request_payload={
                "input": str(Path(args.input).resolve()),
                "mobile_column": args.mobile_column,
                "total_rows": read_result.total_rows,
                "valid_rows": read_result.valid_rows,
                "unique_mobile_count": len(read_result.unique_mobiles),
            },
            commit=True,
        )

        service = ICSPMemberSyncService(db=session, icsp_client=client)

        for invalid in read_result.invalid_rows:
            result_rows.append(
                {
                    "mobile_no": invalid["raw_value"],
                    "status": "invalid_mobile",
                    "member_ids": "",
                    "message": f"Invalid mobile at row {invalid['row_no']}",
                }
            )

        for index, mobile_no in enumerate(read_result.unique_mobiles, start=1):
            print(f"[INFO] processing {index}/{len(read_result.unique_mobiles)} mobile={mobile_no}")
            item_log = log_service.create_log(
                module_name="member_info",
                action="sync_by_mobile",
                target_type="mobile_no",
                target_value=mobile_no,
                triggered_by=args.username,
                triggered_source="script",
                request_payload={"mobile_no": mobile_no, "batch_log_id": batch_log.id},
                commit=True,
            )
            try:
                result = service.sync_member_by_mobile(mobile_no, commit=False)
                if result.matched_member_ids:
                    status = "success"
                    success_count += 1
                    message = "; ".join(result.warnings) if result.warnings else "ok"
                else:
                    status = "not_found"
                    not_found_count += 1
                    message = "; ".join(result.warnings) if result.warnings else "No member matched."

                result_rows.append(
                    {
                        "mobile_no": mobile_no,
                        "status": status,
                        "member_ids": ",".join(result.matched_member_ids),
                        "message": message,
                    }
                )
                log_service.mark_success(item_log, result_payload=result.to_dict(), commit=False)
                session.commit()
            except Exception as exc:
                session.rollback()
                failed_count += 1
                result_rows.append(
                    {
                        "mobile_no": mobile_no,
                        "status": "failed",
                        "member_ids": "",
                        "message": str(exc),
                    }
                )
                log_service.mark_failure(item_log, error_message=str(exc), commit=True)

        summary = {
            "input": read_result.file_path,
            "total_rows": read_result.total_rows,
            "valid_rows": read_result.valid_rows,
            "unique_mobile_count": len(read_result.unique_mobiles),
            "success_count": success_count,
            "not_found_count": not_found_count,
            "failed_count": failed_count,
            "invalid_mobile_count": len(read_result.invalid_rows),
            "output": str(output_path.resolve()),
        }
        log_service.mark_success(batch_log, result_payload=summary, commit=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["mobile_no", "status", "member_ids", "message"])
        writer.writeheader()
        writer.writerows(result_rows)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

