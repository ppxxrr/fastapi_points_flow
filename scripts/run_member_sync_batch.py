from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.models.member import MemberAccount, MemberLevelChangeLog, MemberLevelPeriod, MemberProfile
from app.models.sync import SyncTaskLog
from app.services.member_csv_sync_service import MemberCsvSyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one scheduled member sync batch without changing core sync logic.")
    parser.add_argument("--input-dir", default="./csv", help="Directory that contains historical point-flow CSV files.")
    parser.add_argument("--pattern", default="*.csv", help="File match pattern. Default: *.csv")
    parser.add_argument("--batch-size", type=int, default=1000, help="How many deduplicated mobiles to process in one batch.")
    parser.add_argument("--batch-no", type=int, help="Batch number to run. If omitted, use next_batch_no from state.")
    parser.add_argument("--work-dir", default="./data/scheduler/member_sync", help="Working directory for scheduler artifacts.")
    parser.add_argument("--manifest-file", help="Optional manifest CSV path.")
    parser.add_argument("--state-file", help="Optional state JSON path.")
    parser.add_argument("--username", default=os.getenv("ICSP_USERNAME", ""), help="ICSP username.")
    parser.add_argument("--password", default=os.getenv("ICSP_PASSWORD", ""), help="ICSP password.")
    parser.add_argument("--force-refresh-manifest", action="store_true", help="Rebuild manifest CSV before running.")
    parser.add_argument("--prepare-manifest-only", action="store_true", help="Only rebuild the manifest, do not run a batch.")
    parser.add_argument("--force-rerun", action="store_true", help="Allow rerunning a batch even if it already has a success report.")
    return parser


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def snapshot_counts() -> dict[str, int]:
    with SessionLocal() as session:
        return {
            "member_profile": int(session.scalar(select(func.count()).select_from(MemberProfile)) or 0),
            "member_account": int(session.scalar(select(func.count()).select_from(MemberAccount)) or 0),
            "member_level_change_log": int(session.scalar(select(func.count()).select_from(MemberLevelChangeLog)) or 0),
            "member_level_period": int(session.scalar(select(func.count()).select_from(MemberLevelPeriod)) or 0),
            "sync_task_log": int(session.scalar(select(func.count()).select_from(SyncTaskLog)) or 0),
        }


def quiet_logger(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def build_manifest(input_dir: Path, pattern: str, manifest_file: Path, manifest_meta_file: Path) -> dict:
    with SessionLocal() as session:
        service = MemberCsvSyncService(db=session, logger=quiet_logger)
        summary, mobile_records = service.extract_mobile_records(input_dir=input_dir, pattern=pattern)

    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    with manifest_file.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=["mobile_no", "csv_member_id", "csv_member_name", "source_files", "source_record_count"],
        )
        writer.writeheader()
        for record in mobile_records:
            writer.writerow(
                {
                    "mobile_no": record.mobile_no,
                    "csv_member_id": record.csv_member_id or "",
                    "csv_member_name": record.csv_member_name or "",
                    "source_files": " | ".join(record.source_files),
                    "source_record_count": record.record_count,
                }
            )

    meta = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input_dir": str(input_dir.resolve()),
        "pattern": pattern,
        "total_csv_files": summary.total_csv_files,
        "csv_files_read": summary.csv_files_read,
        "csv_files_skipped": summary.csv_files_skipped,
        "total_records": summary.total_records,
        "valid_mobile_records": summary.valid_mobile_records,
        "deduplicated_mobile_count": len(mobile_records),
        "manifest_file": str(manifest_file.resolve()),
        "file_issues": [issue.to_dict() for issue in summary.file_issues],
    }
    write_json(manifest_meta_file, meta)
    return meta


def count_manifest_rows(manifest_file: Path) -> int:
    with manifest_file.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.reader(fp)
        next(reader, None)
        return sum(1 for _ in reader)


def load_manifest_slice(manifest_file: Path, *, start_index: int, batch_size: int) -> list[dict[str, str]]:
    end_index = start_index + batch_size
    rows: list[dict[str, str]] = []
    with manifest_file.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for index, row in enumerate(reader):
            if index < start_index:
                continue
            if index >= end_index:
                break
            rows.append(row)
    return rows


def write_batch_input_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["memberPhone", "memberId", "memberName"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "memberPhone": row.get("mobile_no", ""),
                    "memberId": row.get("csv_member_id", ""),
                    "memberName": row.get("csv_member_name", ""),
                }
            )


def resolve_state_file(work_dir: Path, batch_size: int, configured: str | None) -> Path:
    if configured:
        return Path(configured).resolve()
    return (work_dir / f"member_sync_state_{batch_size}.json").resolve()


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return read_json(path)


def save_state(path: Path, payload: dict) -> None:
    write_json(path, payload)


def main() -> None:
    args = build_parser().parse_args()
    if args.batch_size <= 0:
        raise SystemExit("batch_size must be greater than 0.")
    if not args.prepare_manifest_only and (not args.username or not args.password):
        raise SystemExit("ICSP username/password is required unless --prepare-manifest-only is used.")

    work_dir = Path(args.work_dir).resolve()
    manifest_file = Path(args.manifest_file).resolve() if args.manifest_file else (work_dir / "member_sync_manifest.csv")
    manifest_meta_file = manifest_file.with_suffix(".meta.json")
    state_file = resolve_state_file(work_dir, args.batch_size, args.state_file)
    input_dir = Path(args.input_dir).resolve()

    if args.force_refresh_manifest or not manifest_file.exists():
        manifest_meta = build_manifest(input_dir, args.pattern, manifest_file, manifest_meta_file)
    elif manifest_meta_file.exists():
        manifest_meta = read_json(manifest_meta_file)
    else:
        manifest_meta = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "input_dir": str(input_dir),
            "pattern": args.pattern,
            "deduplicated_mobile_count": count_manifest_rows(manifest_file),
            "manifest_file": str(manifest_file),
        }
        write_json(manifest_meta_file, manifest_meta)

    total_mobile_count = int(manifest_meta.get("deduplicated_mobile_count") or 0)
    total_batches = math.ceil(total_mobile_count / args.batch_size) if total_mobile_count else 0

    if args.prepare_manifest_only:
        print(f"manifest_file: {manifest_file}")
        print(f"manifest_meta_file: {manifest_meta_file}")
        print(f"deduplicated_mobile_count: {total_mobile_count}")
        print(f"total_batches_at_batch_size_{args.batch_size}: {total_batches}")
        raise SystemExit(0)

    state = load_state(state_file)
    configured_batch_no = args.batch_no
    next_batch_no = int(state.get("next_batch_no") or 1)
    batch_no = configured_batch_no or next_batch_no

    if total_batches == 0:
        print("No deduplicated mobiles found in manifest. Nothing to run.")
        raise SystemExit(0)
    if batch_no > total_batches:
        print(f"No more batches to run. total_batches={total_batches}, requested_batch_no={batch_no}")
        raise SystemExit(0)

    start_index = (batch_no - 1) * args.batch_size
    rows = load_manifest_slice(manifest_file, start_index=start_index, batch_size=args.batch_size)
    expected_count = len(rows)
    if expected_count == 0:
        print(f"Batch {batch_no} is empty. Nothing to run.")
        raise SystemExit(0)

    batch_label = f"batch_{batch_no:06d}"
    batch_dir = work_dir / f"batches_{args.batch_size}" / batch_label
    batch_input_dir = batch_dir / "input"
    batch_input_csv = batch_input_dir / f"{batch_label}.csv"
    result_file = batch_dir / f"{batch_label}_result.csv"
    log_file = batch_dir / f"{batch_label}.log"
    counts_before_file = batch_dir / f"{batch_label}_counts_before.json"
    report_file = batch_dir / f"{batch_label}_report.json"
    run_meta_file = batch_dir / f"{batch_label}_run.json"

    if report_file.exists() and not args.force_rerun:
        existing_report = read_json(report_file)
        if existing_report.get("validation", {}).get("fully_successful"):
            last_completed = int(state.get("last_completed_batch_no") or 0)
            current_next = int(state.get("next_batch_no") or 1)
            state.update(
                {
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                    "input_dir": str(input_dir),
                    "pattern": args.pattern,
                    "manifest_file": str(manifest_file),
                    "manifest_meta_file": str(manifest_meta_file),
                    "total_mobile_count": total_mobile_count,
                    "total_batches": total_batches,
                    "batch_size": args.batch_size,
                    "last_batch_no": batch_no,
                    "last_completed_batch_no": max(last_completed, batch_no),
                    "last_batch_status": "success",
                    "last_report_file": str(report_file),
                    "last_result_file": str(result_file),
                    "next_batch_no": max(current_next, batch_no + 1),
                }
            )
            save_state(state_file, state)
            print(f"Batch {batch_no} already completed successfully.")
            print(f"report_file: {report_file}")
            print(f"state_file: {state_file}")
            print(f"next_batch_no: {state.get('next_batch_no')}")
            raise SystemExit(0)

    write_batch_input_csv(batch_input_csv, rows)

    counts_before = snapshot_counts()
    write_json(counts_before_file, counts_before)

    command = [
        sys.executable,
        str((BASE_DIR / "scripts" / "sync_members_from_point_csv.py").resolve()),
        "--input-dir",
        str(batch_input_dir),
        "--limit",
        str(expected_count),
        "--output",
        str(result_file),
        "--username",
        args.username,
        "--password",
        args.password,
    ]

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    batch_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now().isoformat(timespec="seconds")
    start_ts = time.perf_counter()
    with log_file.open("w", encoding="utf-8", errors="replace") as log_fp:
        log_fp.write(f"started_at={started_at}\n")
        log_fp.write(f"command={' '.join(command)}\n")
        log_fp.write(f"batch_no={batch_no}\n")
        log_fp.write(f"batch_size={args.batch_size}\n")
        log_fp.write(f"expected_count={expected_count}\n\n")
        log_fp.flush()
        completed = subprocess.run(
            command,
            cwd=BASE_DIR,
            env=env,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed_seconds = round(time.perf_counter() - start_ts, 3)

    check_command = [
        sys.executable,
        str((BASE_DIR / "scripts" / "check_member_sync_batch.py").resolve()),
        "--result-file",
        str(result_file),
        "--expected-count",
        str(expected_count),
        "--counts-before-file",
        str(counts_before_file),
        "--report-file",
        str(report_file),
        "--batch-no",
        str(batch_no),
        "--batch-size",
        str(args.batch_size),
        "--core-exit-code",
        str(completed.returncode),
    ]
    checked = subprocess.run(
        check_command,
        cwd=BASE_DIR,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    report = read_json(report_file)

    run_meta = {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "elapsed_seconds": elapsed_seconds,
        "batch_no": batch_no,
        "batch_size": args.batch_size,
        "expected_count": expected_count,
        "input_dir": str(input_dir),
        "pattern": args.pattern,
        "manifest_file": str(manifest_file),
        "state_file": str(state_file),
        "batch_input_csv": str(batch_input_csv),
        "result_file": str(result_file),
        "log_file": str(log_file),
        "report_file": str(report_file),
        "core_exit_code": completed.returncode,
        "check_exit_code": checked.returncode,
        "check_stdout": checked.stdout,
        "check_stderr": checked.stderr,
    }
    write_json(run_meta_file, run_meta)

    validation = report.get("validation", {})
    fully_successful = bool(validation.get("fully_successful"))
    completed_ok = bool(validation.get("completed_ok"))

    last_completed = int(state.get("last_completed_batch_no") or 0)
    current_next = int(state.get("next_batch_no") or 1)
    if fully_successful:
        state.update(
            {
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "input_dir": str(input_dir),
                "pattern": args.pattern,
                "manifest_file": str(manifest_file),
                "manifest_meta_file": str(manifest_meta_file),
                "total_mobile_count": total_mobile_count,
                "total_batches": total_batches,
                "batch_size": args.batch_size,
                "last_batch_no": batch_no,
                "last_completed_batch_no": max(last_completed, batch_no),
                "last_batch_status": "success",
                "last_core_exit_code": completed.returncode,
                "last_report_file": str(report_file),
                "last_result_file": str(result_file),
                "next_batch_no": max(current_next, batch_no + 1),
            }
        )
    else:
        state.update(
            {
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "input_dir": str(input_dir),
                "pattern": args.pattern,
                "manifest_file": str(manifest_file),
                "manifest_meta_file": str(manifest_meta_file),
                "total_mobile_count": total_mobile_count,
                "total_batches": total_batches,
                "batch_size": args.batch_size,
                "last_batch_no": batch_no,
                "last_completed_batch_no": last_completed,
                "last_batch_status": "completed_with_attention" if completed_ok else "validation_failed",
                "last_core_exit_code": completed.returncode,
                "last_report_file": str(report_file),
                "last_result_file": str(result_file),
                "next_batch_no": current_next,
            }
        )
    save_state(state_file, state)

    result_summary = report.get("result_summary", {})
    counts_delta = report.get("counts_delta", {})
    print(f"batch_no: {batch_no}")
    print(f"batch_size: {args.batch_size}")
    print(f"expected_count: {expected_count}")
    print(f"elapsed_seconds: {elapsed_seconds}")
    print(f"core_exit_code: {completed.returncode}")
    print(f"success_count: {result_summary.get('success_count', 0)}")
    print(f"failed_count: {result_summary.get('failed_count', 0)}")
    print(f"not_found_count: {result_summary.get('not_found_count', 0)}")
    print(f"member_profile_delta: {counts_delta.get('member_profile', 0)}")
    print(f"member_account_delta: {counts_delta.get('member_account', 0)}")
    print(f"member_level_change_log_delta: {counts_delta.get('member_level_change_log', 0)}")
    print(f"member_level_period_delta: {counts_delta.get('member_level_period', 0)}")
    print(f"sync_task_log_delta: {counts_delta.get('sync_task_log', 0)}")
    print(f"completed_ok: {completed_ok}")
    print(f"fully_successful: {fully_successful}")
    print(f"result_file: {result_file}")
    print(f"log_file: {log_file}")
    print(f"report_file: {report_file}")
    print(f"state_file: {state_file}")
    print(f"next_batch_no: {state.get('next_batch_no')}")

    if fully_successful:
        raise SystemExit(0)
    if completed_ok:
        raise SystemExit(2)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
