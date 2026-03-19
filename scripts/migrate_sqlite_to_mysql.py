from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.mysql_migration_service import (
    MySQLMigrationService,
    create_db_engine,
    resolve_source_url,
    resolve_table_names,
    resolve_target_mysql_url,
)
from app.services.script_logger import build_script_logger


logger = build_script_logger("migrate_sqlite_to_mysql", "mysql_migration.log")


def log_callback(level: str, message: str) -> None:
    getattr(logger, level.lower(), logger.info)(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch migrate SQLite business/member data into MySQL.")
    parser.add_argument("--source-url", help="Source SQLite url. Default: local member_module.db")
    parser.add_argument("--target-url", help="Target MySQL url. Defaults to MYSQL_* env vars.")
    parser.add_argument("--tables", nargs="*", help="Optional subset of tables to migrate.")
    parser.add_argument("--batch-size", type=int, default=2000, help="Rows per batch. Default: 2000")
    parser.add_argument("--truncate-target", action="store_true", help="Delete target table data before migration.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    source_url = resolve_source_url(args.source_url)
    target_url = resolve_target_mysql_url(args.target_url)
    table_names = resolve_table_names(args.tables)

    source_engine = create_db_engine(source_url)
    target_engine = create_db_engine(target_url)
    try:
        service = MySQLMigrationService(source_engine=source_engine, target_engine=target_engine)
        summary = service.migrate_tables(
            table_names=table_names,
            batch_size=args.batch_size,
            truncate_target=args.truncate_target,
            logger=log_callback,
        )
    finally:
        source_engine.dispose()
        target_engine.dispose()

    logger.info(
        "migrate_sqlite_to_mysql completed tables=%s batch_size=%s truncate_target=%s",
        ",".join(table_names),
        args.batch_size,
        args.truncate_target,
    )
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
