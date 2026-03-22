from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.services.parking_api_sync_service import KEEPALIVE_INTERVAL_SECONDS, ParkingApiClient
from app.services.script_logger import build_script_logger


logger = build_script_logger("run_4pyun_keepalive", "4pyun_keepalive.log")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keep 4pyun parking/trade portal session warm.")
    parser.add_argument("--once", action="store_true", help="Run a single keepalive cycle and exit.")
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=KEEPALIVE_INTERVAL_SECONDS,
        help=f"Seconds between keepalive cycles. Default: {KEEPALIVE_INTERVAL_SECONDS}",
    )
    return parser


def log_callback(level: str, message: str) -> None:
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message)


def main() -> None:
    args = build_parser().parse_args()
    client = ParkingApiClient(logger=log_callback)
    try:
        while True:
            try:
                result = client.run_keepalive_cycle()
                logger.info("4pyun keepalive cycle success %s", json.dumps(result, ensure_ascii=False))
            except Exception:  # noqa: BLE001
                logger.exception("4pyun keepalive cycle failed")
            if args.once:
                break
            time.sleep(max(5, args.sleep_seconds))
    finally:
        client.close()


if __name__ == "__main__":
    main()
