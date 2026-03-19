import json
import os
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(r"D:\python\menbers\codex\FastAPI")
WORK_DIR = BASE_DIR / "data" / "scheduler" / "member_sync"
CSV_DIR = BASE_DIR / "csv"
STATE_FILE = WORK_DIR / "member_sync_state_1000.json"
MANIFEST_FILE = WORK_DIR / "member_sync_manifest.csv"
BATCH_SIZE = 1000

ICSP_USERNAME = "h-pengxr01"
ICSP_PASSWORD = "q###W67326"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def run_cmd(args: list[str], env: dict[str, str]) -> int:
    proc = subprocess.run(args, cwd=str(BASE_DIR), env=env)
    return proc.returncode


def ensure_manifest(env: dict[str, str]) -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    if MANIFEST_FILE.exists():
        print(f"[INIT] manifest exists: {MANIFEST_FILE}")
        return

    print("[INIT] preparing manifest...")
    rc = run_cmd(
        [
            sys.executable,
            str(BASE_DIR / "scripts" / "run_member_sync_batch.py"),
            "--prepare-manifest-only",
            "--batch-size",
            str(BATCH_SIZE),
            "--input-dir",
            str(CSV_DIR),
            "--work-dir",
            str(WORK_DIR),
        ],
        env,
    )
    if rc != 0:
        raise SystemExit(f"[ERROR] prepare manifest failed, exit_code={rc}")

    if not MANIFEST_FILE.exists():
        raise SystemExit(f"[ERROR] manifest was not created: {MANIFEST_FILE}")


def get_state() -> dict:
    if not STATE_FILE.exists():
        raise SystemExit(f"[ERROR] state file missing: {STATE_FILE}")
    return load_json(STATE_FILE)


def get_batch_report_path(batch_no: int) -> Path:
    return (
        WORK_DIR
        / f"batches_{BATCH_SIZE}"
        / f"batch_{batch_no:06d}"
        / f"batch_{batch_no:06d}_report.json"
    )


def force_advance_if_completed(cur_batch: int) -> bool:
    if not STATE_FILE.exists():
        print("[WARN] state file missing, cannot force advance")
        return False

    report_file = get_batch_report_path(cur_batch)
    if not report_file.exists():
        print(f"[WARN] report file missing: {report_file}")
        return False

    state = load_json(STATE_FILE)
    report = load_json(report_file)

    validation = report.get("validation", {})
    raw_value = validation.get("completed_ok", None)

    if isinstance(raw_value, bool):
        completed_ok = raw_value
    elif isinstance(raw_value, (int, float)):
        completed_ok = bool(raw_value)
    elif isinstance(raw_value, str):
        completed_ok = raw_value.strip().lower() in {"true", "1", "yes", "y"}
    else:
        completed_ok = False

    print(f"[DEBUG] validation.completed_ok raw value = {raw_value!r}, parsed = {completed_ok}")

    if not completed_ok:
        print(f"[WARN] batch {cur_batch} validation.completed_ok is not truthy")
        return False

    current_next = int(state.get("next_batch_no", cur_batch))
    target_next = cur_batch + 1

    if current_next >= target_next:
        print(f"[INFO] batch {cur_batch} already advanced to {current_next}")
        return True

    state["next_batch_no"] = target_next
    state["last_completed_batch_no"] = cur_batch
    state["last_batch_status"] = "forced_advanced"
    state["last_forced_advance"] = True
    state["last_forced_advance_from_batch"] = cur_batch

    save_json(STATE_FILE, state)
    print(f"[FORCE-ADVANCE] batch {cur_batch} -> {target_next}")
    return True


def main() -> None:
    env = os.environ.copy()
    env["ICSP_USERNAME"] = ICSP_USERNAME
    env["ICSP_PASSWORD"] = ICSP_PASSWORD
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    ensure_manifest(env)

    while True:
        state = get_state()
        cur_batch = int(state.get("next_batch_no", 1))
        total_batches = int(state.get("total_batches", 0))

        if total_batches <= 0:
            raise SystemExit("[ERROR] total_batches invalid in state file")

        if cur_batch > total_batches:
            print("=" * 60)
            print("[DONE] all batches finished.")
            print("=" * 60)
            return

        print("=" * 60)
        print(f"[PROGRESS] batch {cur_batch} / {total_batches}")
        print("=" * 60)

        rc = run_cmd(
            [
                sys.executable,
                str(BASE_DIR / "scripts" / "run_member_sync_batch.py"),
                "--batch-size",
                str(BATCH_SIZE),
                "--input-dir",
                str(CSV_DIR),
                "--work-dir",
                str(WORK_DIR),
            ],
            env,
        )

        print(f"[INFO] core exit code: {rc}")

        # 关键逻辑：只要 completed_ok=True，就推进
        advanced = force_advance_if_completed(cur_batch)

        state2 = get_state()
        next_batch = int(state2.get("next_batch_no", cur_batch))
        total_batches2 = int(state2.get("total_batches", total_batches))

        print(f"[STATUS] next_batch_no={next_batch} / total_batches={total_batches2}")

        if next_batch > total_batches2:
            print("=" * 60)
            print("[DONE] all batches finished.")
            print("=" * 60)
            return

        # 若本批既未推进又未完成，停止，避免无限死循环
        if not advanced and next_batch == cur_batch:
            print(f"[STOP] batch {cur_batch} not advanced. Please inspect report/log.")
            return


if __name__ == "__main__":
    main()