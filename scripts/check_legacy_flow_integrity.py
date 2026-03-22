from __future__ import annotations

import json
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.session import SessionLocal
from app.services.legacy_flow_service import LegacyFlowService


def main() -> None:
    with SessionLocal() as session:
        service = LegacyFlowService(session)
        payload = {
            "traffic_integrity": service.build_traffic_integrity_summary().to_dict(),
            "railinli_integrity": service.build_railinli_integrity_summary().to_dict(),
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
