from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.legacy_flow import RailinliProbeDailyFlow, TrafficNodeDailyFlow


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_TRAFFIC_SQLITE_PATH = BASE_DIR / "data" / "legacy" / "traffic_data.db"
DEFAULT_RAILINLI_SQLITE_PATH = BASE_DIR / "data" / "legacy" / "railinliKL.db"
TRAFFIC_API_HOST = os.getenv("ICSP_TRAFFIC_API_HOST", "https://10.95.17.101")
TRAFFIC_API_LOGIN_URL = os.getenv("ICSP_TRAFFIC_API_LOGIN_URL", f"{TRAFFIC_API_HOST}/auth/getAccessTokenV2")
TRAFFIC_API_DATA_URL = os.getenv("ICSP_TRAFFIC_API_DATA_URL", f"{TRAFFIC_API_HOST}/snapdata/report/nodes/overview/period")
TRAFFIC_API_USERNAME = os.getenv("ICSP_TRAFFIC_API_USERNAME", "18719207571")
TRAFFIC_API_PASSWORD = os.getenv(
    "ICSP_TRAFFIC_API_PASSWORD",
    "EVukO+xHda9lBivAmXlaQxd38XtfDNUv4sbmI0Zmren+5H6NCiZI6NO2LprG67//b7cPY5ZnrTrG1EXPIaNJMcUaHtOAOMNHsPj+wXo7iwpDzluaLlkvceJExD+QNWn54WoLridI1T+RCaLO8OmW2mUetxrob0cXJA2YEJ/+gda7ZuTCFucRLX0cnDYJdQ8lWVk1CJ4NKsvGMu0/7LNKnYKVq1cMMxMqapdOPrYd3dCBe452IGKrnXrVckhg0dZTjtqMY54lB8AYOnKCq6PEKk0AQwINurQMysz5YlmAShj4sAmXdzK8fL79Pymg8AQV3c2sMXBr81juoptuA90SjQ==",
)
TRAFFIC_API_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Referer": f"{TRAFFIC_API_HOST}/",
}
RAILINLI_RECEIVER_TOKEN = os.getenv("RAILINLI_RECEIVER_TOKEN", "q#W67326")
RAILINLI_PROBE_NAME_MAP = {
    "00000001-0000-0000-0000-c0a80afd6348": "G层肯德基入口",
    "00000004-0000-0000-0000-c0a80afd6348": "G层客梯厅",
    "00000011-0000-0000-0000-c0a80afd6348": "L层外街01",
    "00000014-0000-0000-0000-c0a80afd6348": "L层-客梯厅2",
    "00000021-0000-0000-0000-c0a80afd6348": "G层5号门（超市入口）01",
    "00000024-0000-0000-0000-c0a80afd6348": "L层-3号门01",
    "00000031-0000-0000-0000-c0a80afd6348": "G层1号门01",
    "00000034-0000-0000-0000-c0a80afd6348": "G层1号门02",
    "00000041-0000-0000-0000-c0a80afd6348": "G层2号门01",
    "00000044-0000-0000-0000-c0a80afd6348": "L层6号门",
    "00000051-0000-0000-0000-c0a80afd6348": "L层-客梯厅01",
    "00000054-0000-0000-0000-c0a80afd6348": "L层3号门02",
    "00000064-0000-0000-0000-c0a80afd6348": "G层扶梯",
}
LoggerCallback = Callable[[str, str], None] | None


def _noop_logger(level: str, message: str) -> None:
    return None


def _parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value).strip()).date()


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(text)


def _parse_ratio(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _daterange(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def _normalize_railinli_probe_name(probe_id: str, probe_name: str | None) -> str | None:
    canonical = RAILINLI_PROBE_NAME_MAP.get(str(probe_id).strip())
    if canonical:
        return canonical
    if probe_name in (None, ""):
        return None
    return str(probe_name).strip()


@dataclass(slots=True)
class ImportSummary:
    dataset: str
    source_path: str
    total_rows: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    skipped_rows: int = 0
    min_date: str | None = None
    max_date: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IntegrityGap:
    business_date: str
    present_count: int
    missing_count: int
    missing_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IntegritySummary:
    dataset: str
    total_rows: int
    min_date: str | None
    max_date: str | None
    expected_key_count: int
    complete_dates: int
    partial_dates: list[IntegrityGap] = field(default_factory=list)
    missing_dates: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "total_rows": self.total_rows,
            "min_date": self.min_date,
            "max_date": self.max_date,
            "expected_key_count": self.expected_key_count,
            "complete_dates": self.complete_dates,
            "partial_dates": [item.to_dict() for item in self.partial_dates],
            "missing_dates": self.missing_dates,
        }


class LegacyFlowService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger

    def import_traffic_sqlite(
        self,
        sqlite_path: Path | str = DEFAULT_TRAFFIC_SQLITE_PATH,
        *,
        exclude_node_codes: set[str] | None = None,
        commit_every: int = 500,
    ) -> ImportSummary:
        db_path = Path(sqlite_path)
        summary = ImportSummary(dataset="traffic_node_daily_flow", source_path=str(db_path))
        exclude_codes = exclude_node_codes or {"pp99"}
        if not db_path.exists():
            raise FileNotFoundError(f"traffic sqlite not found: {db_path}")

        min_date: date | None = None
        max_date: date | None = None
        processed_since_commit = 0
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT nodecode, nodename, date, passengerFlow, passengerFlowRatio, update_time
                FROM daily_passenger_flow
                ORDER BY date, nodecode
                """
            )
            for row in rows:
                node_code = str(row["nodecode"] or "").strip()
                if not node_code or node_code in exclude_codes:
                    summary.skipped_rows += 1
                    continue
                business_date = _parse_date(row["date"])
                entity = self._upsert_traffic_row(
                    node_code=node_code,
                    node_name=row["nodename"],
                    business_date=business_date,
                    passenger_flow=int(row["passengerFlow"] or 0),
                    passenger_flow_ratio=_parse_ratio(row["passengerFlowRatio"]),
                    source_updated_at=_parse_datetime(row["update_time"]),
                    source_origin="sqlite_import",
                    raw_json=dict(row),
                )
                summary.total_rows += 1
                summary.inserted_rows += 1 if entity[1] else 0
                summary.updated_rows += 0 if entity[1] else 1
                min_date = business_date if min_date is None or business_date < min_date else min_date
                max_date = business_date if max_date is None or business_date > max_date else max_date
                processed_since_commit += 1
                if processed_since_commit >= commit_every:
                    self.db.commit()
                    processed_since_commit = 0
        self.db.commit()
        summary.min_date = min_date.isoformat() if min_date else None
        summary.max_date = max_date.isoformat() if max_date else None
        return summary

    def import_railinli_sqlite(
        self,
        sqlite_path: Path | str = DEFAULT_RAILINLI_SQLITE_PATH,
        *,
        commit_every: int = 500,
    ) -> ImportSummary:
        db_path = Path(sqlite_path)
        summary = ImportSummary(dataset="railinli_probe_daily_flow", source_path=str(db_path))
        if not db_path.exists():
            raise FileNotFoundError(f"railinli sqlite not found: {db_path}")

        min_date: date | None = None
        max_date: date | None = None
        processed_since_commit = 0
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT probe_id, probe_name, entry_count, date, record_time
                FROM traffic_log
                ORDER BY date, probe_id
                """
            )
            for row in rows:
                probe_id = str(row["probe_id"] or "").strip()
                if not probe_id or row["date"] in (None, ""):
                    summary.skipped_rows += 1
                    continue
                business_date = _parse_date(row["date"])
                entity = self._upsert_railinli_row(
                    probe_id=probe_id,
                    probe_name=row["probe_name"],
                    business_date=business_date,
                    entry_count=int(row["entry_count"] or 0),
                    source_record_time=_parse_datetime(row["record_time"]),
                    source_origin="sqlite_import",
                    raw_json=dict(row),
                )
                summary.total_rows += 1
                summary.inserted_rows += 1 if entity[1] else 0
                summary.updated_rows += 0 if entity[1] else 1
                min_date = business_date if min_date is None or business_date < min_date else min_date
                max_date = business_date if max_date is None or business_date > max_date else max_date
                processed_since_commit += 1
                if processed_since_commit >= commit_every:
                    self.db.commit()
                    processed_since_commit = 0
        self.db.commit()
        summary.min_date = min_date.isoformat() if min_date else None
        summary.max_date = max_date.isoformat() if max_date else None
        return summary

    def sync_traffic_from_api(self, *, job_date: date) -> ImportSummary:
        summary = ImportSummary(dataset="traffic_node_daily_flow", source_path=TRAFFIC_API_DATA_URL)
        payload = {
            "passwd": TRAFFIC_API_PASSWORD,
            "username": TRAFFIC_API_USERNAME,
            "randStr": "",
            "ticket": "",
        }
        session = requests.Session()
        login_response = session.post(
            TRAFFIC_API_LOGIN_URL,
            headers=TRAFFIC_API_HEADERS,
            json=payload,
            verify=False,
            timeout=20,
        )
        login_response.raise_for_status()
        token = login_response.json().get("data", {}).get("token")
        if not token:
            raise RuntimeError("traffic api login returned no token")

        headers = dict(TRAFFIC_API_HEADERS)
        headers["token"] = token
        headers["areaCode"] = "0001-0001-0001"
        headers["platformId"] = "1"
        body = {
            "areaCodes": [],
            "end": job_date.isoformat(),
            "queryType": "1",
            "start": job_date.isoformat(),
            "periodType": 1,
        }
        response = session.post(
            TRAFFIC_API_DATA_URL,
            headers=headers,
            json=body,
            verify=False,
            timeout=30,
        )
        response.raise_for_status()

        rows = self._flatten_traffic_api_rows(response.json().get("data"))
        for row in rows:
            node_code = str(row.get("nodecode") or row.get("nodeCode") or "").strip()
            if not node_code or node_code == "pp99":
                summary.skipped_rows += 1
                continue
            entity = self._upsert_traffic_row(
                node_code=node_code,
                node_name=row.get("nodename") or row.get("nodeName"),
                business_date=job_date,
                passenger_flow=int(float(row.get("passengerFlow") or 0)),
                passenger_flow_ratio=_parse_ratio(row.get("passengerFlowRatio")),
                source_updated_at=datetime.utcnow(),
                source_origin="api_sync",
                raw_json=row,
            )
            summary.total_rows += 1
            summary.inserted_rows += 1 if entity[1] else 0
            summary.updated_rows += 0 if entity[1] else 1
        self.db.commit()
        summary.min_date = job_date.isoformat()
        summary.max_date = job_date.isoformat()
        return summary

    def build_traffic_integrity_summary(self, *, sample_limit: int = 10) -> IntegritySummary:
        return self._build_integrity_summary(
            dataset="traffic_node_daily_flow",
            model=TrafficNodeDailyFlow,
            key_column=TrafficNodeDailyFlow.node_code,
            date_column=TrafficNodeDailyFlow.business_date,
            sample_limit=sample_limit,
        )

    def build_railinli_integrity_summary(self, *, sample_limit: int = 10) -> IntegritySummary:
        return self._build_integrity_summary(
            dataset="railinli_probe_daily_flow",
            model=RailinliProbeDailyFlow,
            key_column=RailinliProbeDailyFlow.probe_id,
            date_column=RailinliProbeDailyFlow.business_date,
            sample_limit=sample_limit,
        )

    def upsert_railinli_upload(
        self,
        *,
        probe_id: str,
        probe_name: str | None,
        entry_count: int,
        business_date: date,
        raw_payload: dict[str, Any] | None = None,
    ) -> RailinliProbeDailyFlow:
        entity, _ = self._upsert_railinli_row(
            probe_id=probe_id,
            probe_name=probe_name,
            business_date=business_date,
            entry_count=entry_count,
            source_record_time=datetime.utcnow(),
            source_origin="receiver_upload",
            raw_json=raw_payload,
        )
        self.db.commit()
        self.db.refresh(entity)
        return entity

    def _upsert_traffic_row(
        self,
        *,
        node_code: str,
        node_name: str | None,
        business_date: date,
        passenger_flow: int,
        passenger_flow_ratio: Decimal | None,
        source_updated_at: datetime | None,
        source_origin: str,
        raw_json: dict[str, Any] | None,
    ) -> tuple[TrafficNodeDailyFlow, bool]:
        entity = self.db.scalar(
            select(TrafficNodeDailyFlow).where(
                TrafficNodeDailyFlow.node_code == node_code,
                TrafficNodeDailyFlow.business_date == business_date,
            )
        )
        created = entity is None
        if entity is None:
            entity = TrafficNodeDailyFlow(node_code=node_code, business_date=business_date, passenger_flow=passenger_flow)
        entity.node_name = str(node_name).strip() if node_name not in (None, "") else None
        entity.passenger_flow = int(passenger_flow)
        entity.passenger_flow_ratio = passenger_flow_ratio
        entity.source_updated_at = source_updated_at
        entity.source_origin = source_origin
        entity.raw_json = raw_json
        self.db.add(entity)
        return entity, created

    def _upsert_railinli_row(
        self,
        *,
        probe_id: str,
        probe_name: str | None,
        business_date: date,
        entry_count: int,
        source_record_time: datetime | None,
        source_origin: str,
        raw_json: dict[str, Any] | None,
    ) -> tuple[RailinliProbeDailyFlow, bool]:
        entity = self.db.scalar(
            select(RailinliProbeDailyFlow).where(
                RailinliProbeDailyFlow.probe_id == probe_id,
                RailinliProbeDailyFlow.business_date == business_date,
            )
        )
        created = entity is None
        if entity is None:
            entity = RailinliProbeDailyFlow(probe_id=probe_id, business_date=business_date, entry_count=entry_count)
        entity.probe_name = _normalize_railinli_probe_name(probe_id, probe_name)
        entity.entry_count = int(entry_count)
        entity.source_record_time = source_record_time
        entity.source_origin = source_origin
        entity.raw_json = raw_json
        self.db.add(entity)
        return entity, created

    def _build_integrity_summary(
        self,
        *,
        dataset: str,
        model: type[TrafficNodeDailyFlow] | type[RailinliProbeDailyFlow],
        key_column: Any,
        date_column: Any,
        sample_limit: int,
    ) -> IntegritySummary:
        total_rows = int(self.db.scalar(select(func.count()).select_from(model)) or 0)
        min_date = self.db.scalar(select(func.min(date_column)))
        max_date = self.db.scalar(select(func.max(date_column)))
        expected_keys = sorted(self.db.scalars(select(key_column).distinct().order_by(key_column)).all())
        expected_key_set = set(expected_keys)
        expected_key_count = len(expected_keys)
        partial_dates: list[IntegrityGap] = []
        missing_dates: list[str] = []
        complete_dates = 0

        if min_date and max_date:
            rows = self.db.execute(select(date_column, key_column).select_from(model).order_by(date_column.asc(), key_column.asc())).all()
            keys_by_date: dict[date, set[str]] = {}
            for business_date, key in rows:
                keys_by_date.setdefault(business_date, set()).add(str(key))
            for business_date in _daterange(min_date, max_date):
                present_keys = keys_by_date.get(business_date, set())
                if not present_keys:
                    missing_dates.append(business_date.isoformat())
                    continue
                if len(present_keys) == expected_key_count:
                    complete_dates += 1
                    continue
                missing_key_list = sorted(expected_key_set - present_keys)
                partial_dates.append(
                    IntegrityGap(
                        business_date=business_date.isoformat(),
                        present_count=len(present_keys),
                        missing_count=len(missing_key_list),
                        missing_keys=missing_key_list[:sample_limit],
                    )
                )
        return IntegritySummary(
            dataset=dataset,
            total_rows=total_rows,
            min_date=min_date.isoformat() if min_date else None,
            max_date=max_date.isoformat() if max_date else None,
            expected_key_count=expected_key_count,
            complete_dates=complete_dates,
            partial_dates=partial_dates[:sample_limit],
            missing_dates=missing_dates[:sample_limit],
        )

    def _flatten_traffic_api_rows(self, payload: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    visit(item)
                return
            if not isinstance(node, dict):
                return
            rows.append(dict(node))
            children = node.get("child")
            if isinstance(children, list):
                visit(children)

        visit(payload)
        return rows
