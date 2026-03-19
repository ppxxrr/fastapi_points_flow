from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.config import get_database_settings


REQUIRED_DIRS = (
    BASE_DIR / "logs",
    BASE_DIR / "data",
    BASE_DIR / "data" / "scheduler",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preflight check for MySQL/systemd deployment.")
    parser.add_argument("--expect-mysql", action="store_true", help="Fail if the effective backend is not MySQL/MariaDB.")
    parser.add_argument(
        "--require-icsp-credentials",
        action="store_true",
        help="Fail if ICSP_USERNAME or ICSP_PASSWORD is missing.",
    )
    parser.add_argument("--create-dirs", action="store_true", help="Create logs/ and scheduler directories if missing.")
    return parser


def _config_source() -> str:
    if os.getenv("DATABASE_URL", "").strip():
        return "DATABASE_URL"
    if os.getenv("MYSQL_HOST", "").strip() and os.getenv("MYSQL_DATABASE", "").strip():
        return "MYSQL_*"
    return "sqlite_fallback"


def _check_directories(create_dirs: bool) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in REQUIRED_DIRS:
        if create_dirs:
            path.mkdir(parents=True, exist_ok=True)
        results.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "is_dir": path.is_dir(),
                "writable": os.access(path, os.W_OK) if path.exists() else False,
            }
        )
    return results


def _summarize_source_dir(path_value: str) -> dict[str, Any]:
    path_text = (path_value or "").strip()
    if not path_text:
        return {
            "configured": False,
            "path": None,
            "exists": False,
            "is_dir": False,
            "csv_file_count": 0,
        }

    path = Path(path_text)
    csv_file_count = 0
    if path.exists() and path.is_dir():
        csv_file_count = sum(1 for item in path.glob("*.csv") if item.is_file())

    return {
        "configured": True,
        "path": str(path),
        "exists": path.exists(),
        "is_dir": path.is_dir(),
        "csv_file_count": csv_file_count,
    }


def _masked_database_url(database_url: str) -> str:
    return make_url(database_url).render_as_string(hide_password=True)


def _check_database(database_url: str) -> tuple[dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    url = make_url(database_url)
    backend = url.get_backend_name()
    connect_args: dict[str, object] = {}
    if backend == "sqlite":
        connect_args["check_same_thread"] = False
        warnings.append("Current effective database backend is still SQLite.")

    engine = create_engine(database_url, future=True, pool_pre_ping=backend != "sqlite", connect_args=connect_args)
    db_info: dict[str, Any] = {
        "backend": backend,
        "driver": url.drivername,
        "host": url.host,
        "port": url.port,
        "database": url.database,
        "effective_database_url": _masked_database_url(database_url),
    }

    try:
        with engine.connect() as connection:
            select_one = connection.execute(text("SELECT 1")).scalar_one()
            version_sql = "SELECT sqlite_version()" if backend == "sqlite" else "SELECT VERSION()"
            database_version = connection.execute(text(version_sql)).scalar_one()
            db_info["connectivity_ok"] = select_one == 1
            db_info["database_version"] = str(database_version)
    except Exception as exc:
        errors.append(f"Database connection failed: {exc}")
        db_info["connectivity_ok"] = False
    finally:
        engine.dispose()
    return db_info, errors, warnings


def main() -> int:
    args = build_parser().parse_args()
    settings = get_database_settings()
    config_source = _config_source()

    env_summary = {
        "DATABASE_URL": bool(os.getenv("DATABASE_URL", "").strip()),
        "MYSQL_HOST": bool(os.getenv("MYSQL_HOST", "").strip()),
        "MYSQL_PORT": bool(os.getenv("MYSQL_PORT", "").strip()),
        "MYSQL_USER": bool(os.getenv("MYSQL_USER", "").strip()),
        "MYSQL_PASSWORD": bool(os.getenv("MYSQL_PASSWORD", "").strip()),
        "MYSQL_DATABASE": bool(os.getenv("MYSQL_DATABASE", "").strip()),
        "ICSP_USERNAME": bool(os.getenv("ICSP_USERNAME", "").strip()),
        "ICSP_PASSWORD": bool(os.getenv("ICSP_PASSWORD", "").strip()),
        "POINT_FLOW_SOURCE_DIR": os.getenv("POINT_FLOW_SOURCE_DIR", ""),
        "PARKING_SOURCE_DIR": os.getenv("PARKING_SOURCE_DIR", ""),
    }

    directory_results = _check_directories(args.create_dirs)
    database_info, errors, warnings = _check_database(settings.database_url)
    point_flow_source = _summarize_source_dir(env_summary["POINT_FLOW_SOURCE_DIR"])
    parking_source = _summarize_source_dir(env_summary["PARKING_SOURCE_DIR"])

    if args.expect_mysql and database_info["backend"] == "sqlite":
        errors.append("Expected MySQL/MariaDB, but the effective backend is still SQLite.")

    if args.require_icsp_credentials:
        if not env_summary["ICSP_USERNAME"] or not env_summary["ICSP_PASSWORD"]:
            errors.append("Missing ICSP_USERNAME or ICSP_PASSWORD.")

    for item in directory_results:
        if not item["exists"] or not item["is_dir"] or not item["writable"]:
            errors.append(f"Directory is not writable/usable: {item['path']}")

    if parking_source["configured"]:
        if not parking_source["exists"] or not parking_source["is_dir"]:
            warnings.append("PARKING_SOURCE_DIR does not exist on this host.")
        elif parking_source["csv_file_count"] == 0:
            warnings.append(
                "PARKING_SOURCE_DIR exists but contains no CSV files. "
                "The Linux service only reads local server files, so Windows source files must be synced first."
            )

    summary = {
        "base_dir": str(BASE_DIR),
        "config_source": config_source,
        "database": database_info,
        "directories": directory_results,
        "source_dirs": {
            "point_flow": point_flow_source,
            "parking": parking_source,
        },
        "env_summary": env_summary,
        "warnings": warnings,
        "errors": errors,
        "status": "ok" if not errors else "failed",
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
