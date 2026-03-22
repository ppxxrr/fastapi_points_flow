from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.k2_print_service import BPM_KEEPALIVE_URLS, BPM_LOGIN_URL
from app.services.script_logger import build_script_logger


logger = build_script_logger("run_bpm_browser", "bpm_browser.log")

CHROME_PATH = Path(os.getenv("BPM_CHROME_PATH", "/usr/bin/google-chrome"))
MANAGED_PROFILE_DIR = Path(
    os.getenv("BPM_MANAGED_CHROME_PROFILE_DIR", str(Path.home() / ".config" / "google-chrome-bpm"))
)
SOURCE_PROFILE_DIR = Path(
    os.getenv("BPM_BOOTSTRAP_CHROME_PROFILE_DIR", str(Path.home() / ".config" / "google-chrome"))
)
MANAGED_REMOTE_DEBUGGING_PORT = int(os.getenv("BPM_MANAGED_CHROME_PORT", "9224"))
MANAGED_HEADLESS = os.getenv("BPM_MANAGED_CHROME_HEADLESS", "false").lower() == "true"
BOOTSTRAP_MARKER = MANAGED_PROFILE_DIR / ".bootstrap_complete"

BOOTSTRAP_ITEMS = [
    "Local State",
    "Default/Preferences",
    "Default/Secure Preferences",
    "Default/Cookies",
    "Default/Cookies-journal",
    "Default/Local Storage",
    "Default/IndexedDB",
    "Default/Session Storage",
    "Default/WebStorage",
]


def _hydrate_desktop_session_env() -> dict[str, str]:
    env = os.environ.copy()
    wanted = {"DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR", "DISPLAY", "WAYLAND_DISPLAY", "XAUTHORITY"}
    if env.get("XDG_RUNTIME_DIR") and (env.get("DISPLAY") or env.get("WAYLAND_DISPLAY")):
        return env

    proc_root = Path("/proc")
    if not proc_root.exists():
        return env

    for proc_entry in proc_root.iterdir():
        if not proc_entry.name.isdigit():
            continue
        try:
            cmdline = (proc_entry / "cmdline").read_text(encoding="utf-8", errors="ignore").replace("\x00", " ").lower()
        except OSError:
            continue
        if "chrome" not in cmdline and "gnome-shell" not in cmdline and "wayland" not in cmdline:
            continue
        try:
            env_blob = (proc_entry / "environ").read_bytes().split(b"\x00")
        except OSError:
            continue
        loaded = 0
        for entry in env_blob:
            if b"=" not in entry:
                continue
            key_bytes, value_bytes = entry.split(b"=", 1)
            key = key_bytes.decode("utf-8", errors="ignore")
            if key not in wanted or env.get(key):
                continue
            value = value_bytes.decode("utf-8", errors="ignore")
            if not value:
                continue
            env[key] = value
            loaded += 1
        if loaded and env.get("XDG_RUNTIME_DIR") and (env.get("DISPLAY") or env.get("WAYLAND_DISPLAY")):
            break
    return env


def _copy_path(source_root: Path, target_root: Path, relative_path: str) -> None:
    source = source_root / relative_path
    target = target_root / relative_path
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        shutil.copy2(source, target)


def bootstrap_profile() -> None:
    MANAGED_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for singleton_name in ("SingletonCookie", "SingletonLock", "SingletonSocket"):
        singleton_path = MANAGED_PROFILE_DIR / singleton_name
        if singleton_path.exists() or singleton_path.is_symlink():
            singleton_path.unlink(missing_ok=True)

    if BOOTSTRAP_MARKER.exists():
        return

    if SOURCE_PROFILE_DIR.exists():
        logger.info("bootstrapping managed BPM chrome profile from %s", SOURCE_PROFILE_DIR)
        for relative_path in BOOTSTRAP_ITEMS:
            try:
                _copy_path(SOURCE_PROFILE_DIR, MANAGED_PROFILE_DIR, relative_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("bootstrap copy skipped path=%s error=%s", relative_path, exc)
    else:
        logger.warning("bootstrap source profile missing: %s", SOURCE_PROFILE_DIR)

    BOOTSTRAP_MARKER.write_text(str(int(time.time())), encoding="utf-8")


def build_command(env: dict[str, str]) -> list[str]:
    args = [
        str(CHROME_PATH),
        f"--user-data-dir={MANAGED_PROFILE_DIR}",
        "--profile-directory=Default",
        f"--remote-debugging-port={MANAGED_REMOTE_DEBUGGING_PORT}",
        "--remote-allow-origins=*",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-dev-shm-usage",
        "--disable-background-networking",
    ]

    headless = MANAGED_HEADLESS or not (env.get("DISPLAY") or env.get("WAYLAND_DISPLAY"))
    if headless:
        args.extend(["--headless=new", "--disable-gpu"])

    urls = [BPM_LOGIN_URL]
    for url in BPM_KEEPALIVE_URLS:
        if url not in urls:
            urls.append(url)
    args.extend(urls)
    return args


def main() -> None:
    if not CHROME_PATH.exists():
        raise SystemExit(f"Chrome executable not found: {CHROME_PATH}")

    bootstrap_profile()
    env = _hydrate_desktop_session_env()
    command = build_command(env)
    logger.info("starting managed BPM chrome profile=%s port=%s", MANAGED_PROFILE_DIR, MANAGED_REMOTE_DEBUGGING_PORT)
    process = subprocess.Popen(command, env=env)
    raise SystemExit(process.wait())


if __name__ == "__main__":
    main()
