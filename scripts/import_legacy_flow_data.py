from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.legacy_flow_service import (
    DEFAULT_RAILINLI_SQLITE_PATH,
    DEFAULT_TRAFFIC_SQLITE_PATH,
    LegacyFlowService,
)
from app.services.script_logger import build_script_logger


logger = build_script_logger("import_legacy_flow_data", "legacy_flow_import.log")


def log_callback(level: str, message: str) -> None:
    getattr(logger, level.lower(), logger.info)(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import legacy traffic_data.db and railinliKL.db into server database.")
    parser.add_argument("--traffic-sqlite", default=str(DEFAULT_TRAFFIC_SQLITE_PATH), help="Path to traffic_data.db")
    parser.add_argument("--railinli-sqlite", default=str(DEFAULT_RAILINLI_SQLITE_PATH), help="Path to railinliKL.db")
    parser.add_argument(
        "--dataset",
        choices=["all", "traffic", "railinli"],
        default="all",
        help="Dataset to import. Default: all",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result: dict[str, object] = {}
    with SessionLocal() as session:
        service = LegacyFlowService(session, logger=log_callback)
        if args.dataset in {"all", "traffic"}:
            result["traffic_import"] = service.import_traffic_sqlite(args.traffic_sqlite).to_dict()
            result["traffic_integrity"] = service.build_traffic_integrity_summary().to_dict()
        if args.dataset in {"all", "railinli"}:
            result["railinli_import"] = service.import_railinli_sqlite(args.railinli_sqlite).to_dict()
            result["railinli_integrity"] = service.build_railinli_integrity_summary().to_dict()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
