from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.mysql_migration_service import ensure_mysql_database_exists, resolve_target_mysql_url
from app.services.script_logger import build_script_logger


logger = build_script_logger("prepare_mysql_schema", "mysql_migration.log")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create target MySQL database if needed and run Alembic migrations.")
    parser.add_argument("--target-url", help="Target MySQL url. Defaults to MYSQL_* env vars.")
    parser.add_argument("--skip-create-database", action="store_true", help="Do not issue CREATE DATABASE IF NOT EXISTS.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    target_url = resolve_target_mysql_url(args.target_url)

    if not args.skip_create_database:
        ensure_mysql_database_exists(target_url)

    os.environ["DATABASE_URL"] = target_url
    alembic_cfg = Config(str(BASE_DIR / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(BASE_DIR / "alembic"))
    alembic_cfg.set_main_option("sqlalchemy.url", target_url.replace("%", "%%"))
    command.upgrade(alembic_cfg, "head")

    payload = {"status": "success", "target_url": target_url}
    logger.info("prepare_mysql_schema completed target=%s", target_url)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
