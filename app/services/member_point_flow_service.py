from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.common import utcnow
from app.models.member import MemberProfile
from app.models.parking import ParkingRecord
from app.models.point_flow import MemberPointFlow
from app.services.icsp_client import ICSPClient
from app.services.import_utils import (
    CSV_ENCODINGS,
    build_hash,
    clean_text,
    load_csv_header,
    normalize_mobile_no,
    normalize_plate_no,
    normalize_raw_row,
    parse_bool_value,
    parse_datetime_value,
    parse_decimal_value,
    parse_int_value,
    parse_json_like_value,
)


LoggerCallback = Callable[[str, str], None] | None
POINT_FLOW_HEADER_FIELDS = {"flowNo", "createTime", "changePointAmount", "memberId", "memberPhone"}
PARKING_HEADER_FIELDS = {"记录ID", "停车流水号", "进场时间", "出场时间", "车牌号"}
ROW_ERROR_LIMIT = 50
FILE_ERROR_LIMIT = 50
HASH_EXCLUDE_FIELDS = {"raw_json", "source_file", "source_row_no", "row_hash"}


def _noop_logger(level: str, message: str) -> None:
    return None


def build_point_flow_event_key(row: dict[str, Any]) -> str:
    flow_no = clean_text(row.get("flowNo"))
    if flow_no:
        return build_hash({"flow_no": flow_no})
    return build_hash(
        {
            "member_id": clean_text(row.get("memberId")),
            "mobile_no": normalize_mobile_no(row.get("memberPhone")),
            "out_trade_no": clean_text(row.get("outTradeNo")),
            "create_time": clean_text(row.get("createTime")),
            "change_type_code": clean_text(row.get("changeTypeCode")),
            "change_point_amount": clean_text(row.get("changePointAmount")),
        }
    )


def build_parking_event_key(row: dict[str, Any]) -> str:
    record_id = clean_text(row.get("记录ID"))
    if record_id:
        return build_hash({"record_id": record_id})
    return build_hash(
        {
            "parking_serial_no": clean_text(row.get("停车流水号")),
            "plate_no": normalize_plate_no(row.get("车牌号")),
            "entry_time": clean_text(row.get("进场时间")),
            "exit_time": clean_text(row.get("出场时间")),
            "mobile_no": normalize_mobile_no(row.get("车牌匹配手机号")),
        }
    )


def build_item_row_hash(item: dict[str, Any]) -> str:
    hash_payload = {key: value for key, value in item.items() if key not in HASH_EXCLUDE_FIELDS}
    return build_hash(hash_payload)


@dataclass(slots=True)
class MemberPointFlowSyncSummary:
    start_date: str
    end_date: str
    requested_member_id: str | None = None
    requested_mobile_no: str | None = None
    fetched_count: int = 0
    filtered_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MemberPointFlowSyncService:
    def __init__(self, db: Session, icsp_client: ICSPClient):
        self.db = db
        self.icsp_client = icsp_client

    def sync_point_flow_range(
        self,
        *,
        start_date: str,
        end_date: str,
        member_id: str | None = None,
        mobile_no: str | None = None,
        commit: bool = True,
    ) -> MemberPointFlowSyncSummary:
        summary = MemberPointFlowSyncSummary(
            start_date=start_date,
            end_date=end_date,
            requested_member_id=member_id,
            requested_mobile_no=mobile_no,
        )

        rows = self.icsp_client.fetch_point_flow(start_date=start_date, end_date=end_date)
        summary.fetched_count = len(rows)

        filtered_rows = [
            row
            for row in rows
            if self._matches_filters(row=row, member_id=member_id, mobile_no=mobile_no)
        ]
        summary.filtered_count = len(filtered_rows)

        event_keys = [build_point_flow_event_key(row) for row in filtered_rows]
        existing_records = list(
            self.db.scalars(select(MemberPointFlow).where(MemberPointFlow.event_key.in_(event_keys)))
        ) if event_keys else []
        existing_by_event_key = {record.event_key: record for record in existing_records}

        for row, event_key in zip(filtered_rows, event_keys):
            record = existing_by_event_key.get(event_key)
            is_insert = record is None
            if record is None:
                record = MemberPointFlow(event_key=event_key)
                self.db.add(record)
                existing_by_event_key[event_key] = record

            self._apply_row(record, row)
            if is_insert:
                summary.inserted_count += 1
            else:
                summary.updated_count += 1

        self.db.flush()
        if commit:
            self.db.commit()
        return summary

    @staticmethod
    def _matches_filters(row: dict[str, Any], member_id: str | None, mobile_no: str | None) -> bool:
        if member_id and clean_text(row.get("memberId")) != clean_text(member_id):
            return False
        if mobile_no and normalize_mobile_no(row.get("memberPhone")) != normalize_mobile_no(mobile_no):
            return False
        return True

    @staticmethod
    def _apply_row(record: MemberPointFlow, row: dict[str, Any]) -> None:
        normalized_row = normalize_raw_row(row)
        consume_amount_raw = parse_int_value(normalized_row.get("consumeAmount"))
        record.flow_no = clean_text(normalized_row.get("flowNo"))
        record.member_id = clean_text(normalized_row.get("memberId"))
        record.member_name = clean_text(normalized_row.get("memberName"))
        record.mobile_no = normalize_mobile_no(normalized_row.get("memberPhone"))
        record.out_trade_no = clean_text(normalized_row.get("outTradeNo"))
        record.plaza_bu_id = clean_text(normalized_row.get("plazaBuId"))
        record.plaza_name = clean_text(normalized_row.get("plazaBuName"))
        record.store_bu_id = clean_text(normalized_row.get("storeBuId"))
        record.store_code = clean_text(normalized_row.get("storeCode"))
        record.store_bu_name = clean_text(normalized_row.get("storeBuName"))
        record.point_operate = clean_text(normalized_row.get("pointOperate"))
        record.change_point_amount = parse_decimal_value(normalized_row.get("changePointAmount"))
        record.signed_change_points = parse_decimal_value(normalized_row.get("signedChangePoints"))
        record.current_effective_amount = parse_decimal_value(normalized_row.get("currentEffectiveAmount"))
        record.consume_amount_raw = consume_amount_raw
        record.consume_amount = (
            (Decimal(consume_amount_raw) / Decimal("100")) if consume_amount_raw is not None else None
        )
        record.point_rate = parse_decimal_value(normalized_row.get("pointRate"))
        record.point_ratio = parse_decimal_value(normalized_row.get("pointRatio"))
        record.change_type_code = clean_text(normalized_row.get("changeTypeCode"))
        record.change_type_name = clean_text(normalized_row.get("changeTypeName"))
        record.business_type_name = clean_text(normalized_row.get("businessTypeName"))
        record.source_code = clean_text(normalized_row.get("sourceCode"))
        record.source_name = clean_text(normalized_row.get("sourceName"))
        record.market_activity_no = clean_text(normalized_row.get("marketActivityNo"))
        record.market_activity_name = clean_text(normalized_row.get("marketActivityName"))
        record.market_activity_type = clean_text(normalized_row.get("marketActivityType"))
        record.create_time = parse_datetime_value(normalized_row.get("createTime"))
        record.consume_time = parse_datetime_value(normalized_row.get("consumeTime"))
        record.expire_time = parse_datetime_value(normalized_row.get("expireTime"))
        record.remark = clean_text(normalized_row.get("remark"))
        record.extra = parse_json_like_value(normalized_row.get("extra"))
        record.raw_json = normalized_row
        record.row_hash = build_item_row_hash(
            {
                "event_key": record.event_key,
                "flow_no": record.flow_no,
                "member_id": record.member_id,
                "member_name": record.member_name,
                "mobile_no": record.mobile_no,
                "out_trade_no": record.out_trade_no,
                "plaza_bu_id": record.plaza_bu_id,
                "plaza_name": record.plaza_name,
                "store_bu_id": record.store_bu_id,
                "store_code": record.store_code,
                "store_bu_name": record.store_bu_name,
                "point_operate": record.point_operate,
                "change_point_amount": record.change_point_amount,
                "signed_change_points": record.signed_change_points,
                "current_effective_amount": record.current_effective_amount,
                "consume_amount_raw": record.consume_amount_raw,
                "consume_amount": record.consume_amount,
                "point_rate": record.point_rate,
                "point_ratio": record.point_ratio,
                "change_type_code": record.change_type_code,
                "change_type_name": record.change_type_name,
                "business_type_name": record.business_type_name,
                "source_code": record.source_code,
                "source_name": record.source_name,
                "remark": record.remark,
                "market_activity_no": record.market_activity_no,
                "market_activity_name": record.market_activity_name,
                "market_activity_type": record.market_activity_type,
                "create_time": record.create_time,
                "consume_time": record.consume_time,
                "expire_time": record.expire_time,
                "extra": record.extra,
            }
        )


@dataclass(slots=True)
class CsvImportErrorRow:
    file_path: str
    row_no: int | None
    unique_key: str | None
    error: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CsvImportFileIssue:
    file_path: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CsvImportSummary:
    module_name: str
    input_dir: str
    pattern: str
    total_csv_files: int = 0
    processed_csv_files: int = 0
    skipped_csv_files: int = 0
    total_rows: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    dry_run: bool = False
    limit_files: int | None = None
    limit_rows: int | None = None
    file_issues: list[CsvImportFileIssue] = field(default_factory=list)
    row_errors: list[CsvImportErrorRow] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "module_name": self.module_name,
            "input_dir": self.input_dir,
            "pattern": self.pattern,
            "total_csv_files": self.total_csv_files,
            "processed_csv_files": self.processed_csv_files,
            "skipped_csv_files": self.skipped_csv_files,
            "total_rows": self.total_rows,
            "inserted_count": self.inserted_count,
            "updated_count": self.updated_count,
            "skipped_count": self.skipped_count,
            "failed_count": self.failed_count,
            "dry_run": self.dry_run,
            "limit_files": self.limit_files,
            "limit_rows": self.limit_rows,
            "file_issues": [item.to_dict() for item in self.file_issues],
            "row_errors": [item.to_dict() for item in self.row_errors],
        }


@dataclass(slots=True)
class ExistingRowState:
    db_id: int | None
    row_hash: str | None


class BaseCsvImportService:
    module_name: str = "csv_import"
    model: type[Any]
    primary_field_name: str
    required_headers: set[str]

    def __init__(self, db: Session, logger: LoggerCallback = None, batch_size: int = 1000):
        self.db = db
        self.logger = logger or _noop_logger
        self.batch_size = batch_size
        self._dry_run_state: dict[str, ExistingRowState] = {}

    def import_directory(
        self,
        *,
        input_dir: str | Path,
        pattern: str = "*.csv",
        limit_files: int | None = None,
        limit_rows: int | None = None,
        dry_run: bool = False,
    ) -> CsvImportSummary:
        directory = Path(input_dir)
        csv_files = sorted(directory.glob(pattern))
        summary = CsvImportSummary(
            module_name=self.module_name,
            input_dir=str(directory.resolve()),
            pattern=pattern,
            total_csv_files=len(csv_files),
            dry_run=dry_run,
            limit_files=limit_files,
            limit_rows=limit_rows,
        )
        self._dry_run_state = {}

        if not csv_files:
            self.log("WARN", f"[{self.module_name}] no csv files matched pattern={pattern} under {directory}")
            return summary

        processed_valid_files = 0
        for csv_file in csv_files:
            if limit_files is not None and processed_valid_files >= limit_files:
                break
            if limit_rows is not None and summary.total_rows >= limit_rows:
                break

            headers, encoding = load_csv_header(csv_file)
            if headers is None or encoding is None:
                self._record_file_issue(summary, csv_file, "failed to detect CSV encoding")
                continue
            if not self._matches_required_headers(headers):
                self._record_file_issue(summary, csv_file, "header does not match target import schema")
                continue

            processed_valid_files += 1
            summary.processed_csv_files += 1
            self.log("INFO", f"[{self.module_name}] importing {csv_file.name}")
            self._import_file(
                csv_file=csv_file,
                encoding=encoding,
                summary=summary,
                limit_rows=limit_rows,
                dry_run=dry_run,
            )
            if not dry_run:
                self.db.commit()

        if dry_run:
            self.db.rollback()
        return summary

    def import_rows(
        self,
        *,
        rows: list[tuple[dict[str, Any], str | Path | None, int | None]],
        dry_run: bool = False,
        source_name: str = "runtime_rows",
    ) -> CsvImportSummary:
        summary = CsvImportSummary(
            module_name=self.module_name,
            input_dir=source_name,
            pattern=source_name,
            total_csv_files=1 if rows else 0,
            processed_csv_files=1 if rows else 0,
            dry_run=dry_run,
        )
        self._dry_run_state = {}
        items: list[dict[str, Any]] = []

        for raw_row, source_file, row_no in rows:
            if raw_row is None:
                continue
            if not any(clean_text(value) is not None for value in raw_row.values()):
                continue

            summary.total_rows += 1
            try:
                file_path = Path(source_file) if source_file is not None else Path(source_name)
                item = self.normalize_row(raw_row=raw_row, csv_file=file_path, row_no=row_no or summary.total_rows)
                items.append(item)
            except Exception as exc:
                self._record_row_error(
                    summary,
                    csv_file=Path(source_file) if source_file is not None else Path(source_name),
                    row_no=row_no,
                    unique_key=None,
                    error=str(exc),
                )
                continue

            if len(items) >= self.batch_size:
                self._flush_items(items=items, summary=summary, dry_run=dry_run)
                items = []

        if items:
            self._flush_items(items=items, summary=summary, dry_run=dry_run)

        if dry_run:
            self.db.rollback()
        else:
            self.db.commit()
        return summary

    def log(self, level: str, message: str) -> None:
        self.logger(level, message)

    def _matches_required_headers(self, headers: list[str]) -> bool:
        return self.required_headers.issubset(set(headers))

    def _record_file_issue(self, summary: CsvImportSummary, csv_file: Path, reason: str) -> None:
        summary.skipped_csv_files += 1
        self.log("WARN", f"[{self.module_name}] skipped {csv_file.name}: {reason}")
        if len(summary.file_issues) < FILE_ERROR_LIMIT:
            summary.file_issues.append(CsvImportFileIssue(file_path=str(csv_file.resolve()), reason=reason))

    def _record_row_error(
        self,
        summary: CsvImportSummary,
        *,
        csv_file: Path,
        row_no: int | None,
        unique_key: str | None,
        error: str,
    ) -> None:
        summary.failed_count += 1
        if len(summary.row_errors) < ROW_ERROR_LIMIT:
            summary.row_errors.append(
                CsvImportErrorRow(
                    file_path=str(csv_file.resolve()),
                    row_no=row_no,
                    unique_key=unique_key,
                    error=error,
                )
            )

    def _import_file(
        self,
        *,
        csv_file: Path,
        encoding: str,
        summary: CsvImportSummary,
        limit_rows: int | None,
        dry_run: bool,
    ) -> None:
        items: list[dict[str, Any]] = []
        with csv_file.open("r", encoding=encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                self._record_file_issue(summary, csv_file, "missing header row")
                return

            for row_index, raw_row in enumerate(reader, start=2):
                if limit_rows is not None and summary.total_rows >= limit_rows:
                    break
                if raw_row is None:
                    continue
                if not any(clean_text(value) is not None for value in raw_row.values()):
                    continue

                summary.total_rows += 1
                try:
                    item = self.normalize_row(raw_row=raw_row, csv_file=csv_file, row_no=row_index)
                    items.append(item)
                except Exception as exc:
                    self._record_row_error(
                        summary,
                        csv_file=csv_file,
                        row_no=row_index,
                        unique_key=None,
                        error=str(exc),
                    )
                    continue

                if len(items) >= self.batch_size:
                    self._flush_items(items=items, summary=summary, dry_run=dry_run)
                    items = []

        if items:
            self._flush_items(items=items, summary=summary, dry_run=dry_run)

    def _flush_items(self, *, items: list[dict[str, Any]], summary: CsvImportSummary, dry_run: bool) -> None:
        if not items:
            return

        self.enrich_items(items)
        for item in items:
            item["row_hash"] = build_item_row_hash(item)

        lookup = self._load_existing_states(items)
        if dry_run:
            for key, value in self._dry_run_state.items():
                lookup.setdefault(key, value)

        insert_rows: dict[str, dict[str, Any]] = {}
        update_rows: dict[int, dict[str, Any]] = {}
        working_state = dict(lookup)
        now = utcnow()

        for item in items:
            unique_key = self._resolve_lookup_key(item)
            state = working_state.get(unique_key)
            if state is None:
                insert_rows[unique_key] = self._build_insert_mapping(item, now)
                new_state = ExistingRowState(db_id=None, row_hash=item["row_hash"])
                self._store_state(working_state, item, new_state)
                if dry_run:
                    self._store_state(self._dry_run_state, item, new_state)
                summary.inserted_count += 1
                continue

            if state.row_hash == item["row_hash"]:
                summary.skipped_count += 1
                continue

            if state.db_id is None:
                insert_rows[unique_key] = self._build_insert_mapping(item, now)
                new_state = ExistingRowState(db_id=None, row_hash=item["row_hash"])
                self._store_state(working_state, item, new_state)
                if dry_run:
                    self._store_state(self._dry_run_state, item, new_state)
                summary.updated_count += 1
                continue

            update_rows[state.db_id] = self._build_update_mapping(state.db_id, item, now)
            new_state = ExistingRowState(db_id=state.db_id, row_hash=item["row_hash"])
            self._store_state(working_state, item, new_state)
            if dry_run:
                self._store_state(self._dry_run_state, item, new_state)
            summary.updated_count += 1

        if dry_run:
            return

        if insert_rows:
            self.db.bulk_insert_mappings(self.model, list(insert_rows.values()))
        if update_rows:
            self.db.bulk_update_mappings(self.model, list(update_rows.values()))
        self.db.flush()

    def _load_existing_states(self, items: list[dict[str, Any]]) -> dict[str, ExistingRowState]:
        primary_values = [item[self.primary_field_name] for item in items if item.get(self.primary_field_name)]
        event_keys = [item["event_key"] for item in items if item.get("event_key")]
        if not primary_values and not event_keys:
            return {}

        primary_column = getattr(self.model, self.primary_field_name)
        query_columns = (self.model.id, primary_column, self.model.event_key, self.model.row_hash)
        rows: list[tuple[int, str | None, str | None, str | None]] = []
        if primary_values:
            rows.extend(self.db.execute(select(*query_columns).where(primary_column.in_(primary_values))).all())
        if event_keys:
            rows.extend(self.db.execute(select(*query_columns).where(self.model.event_key.in_(event_keys))).all())

        state_map: dict[str, ExistingRowState] = {}
        for db_id, primary_value, event_key, row_hash in rows:
            state = ExistingRowState(db_id=db_id, row_hash=row_hash)
            if primary_value:
                state_map[str(primary_value)] = state
            if event_key:
                state_map[str(event_key)] = state
        return state_map

    def _resolve_lookup_key(self, item: dict[str, Any]) -> str:
        return str(item.get(self.primary_field_name) or item["event_key"])

    def _store_state(self, state_map: dict[str, ExistingRowState], item: dict[str, Any], state: ExistingRowState) -> None:
        primary_value = item.get(self.primary_field_name)
        if primary_value:
            state_map[str(primary_value)] = state
        state_map[str(item["event_key"])] = state

    def _build_insert_mapping(self, item: dict[str, Any], now: datetime) -> dict[str, Any]:
        mapping = dict(item)
        mapping["created_at"] = now
        mapping["updated_at"] = now
        return mapping

    def _build_update_mapping(self, db_id: int, item: dict[str, Any], now: datetime) -> dict[str, Any]:
        mapping = dict(item)
        mapping["id"] = db_id
        mapping["updated_at"] = now
        return mapping

    def enrich_items(self, items: list[dict[str, Any]]) -> None:
        return None

    def normalize_row(self, *, raw_row: dict[str, Any], csv_file: Path, row_no: int) -> dict[str, Any]:
        raise NotImplementedError


class MemberPointFlowCsvImportService(BaseCsvImportService):
    module_name = "member_point_flow_csv_import"
    model = MemberPointFlow
    primary_field_name = "flow_no"
    required_headers = POINT_FLOW_HEADER_FIELDS

    def normalize_row(self, *, raw_row: dict[str, Any], csv_file: Path, row_no: int) -> dict[str, Any]:
        normalized_row = normalize_raw_row(raw_row)
        event_key = build_point_flow_event_key(normalized_row)
        flow_no = clean_text(normalized_row.get("flowNo"))
        consume_amount_raw = parse_int_value(normalized_row.get("consumeAmount"))

        if not flow_no and not event_key:
            raise ValueError("missing flow identifier")

        return {
            "event_key": event_key,
            "flow_no": flow_no,
            "member_id": clean_text(normalized_row.get("memberId")),
            "member_name": clean_text(normalized_row.get("memberName")),
            "mobile_no": normalize_mobile_no(normalized_row.get("memberPhone")),
            "out_trade_no": clean_text(normalized_row.get("outTradeNo")),
            "plaza_bu_id": clean_text(normalized_row.get("plazaBuId")),
            "plaza_name": clean_text(normalized_row.get("plazaBuName")),
            "store_bu_id": clean_text(normalized_row.get("storeBuId")),
            "store_code": clean_text(normalized_row.get("storeCode")),
            "store_bu_name": clean_text(normalized_row.get("storeBuName")),
            "point_operate": clean_text(normalized_row.get("pointOperate")),
            "change_point_amount": parse_decimal_value(normalized_row.get("changePointAmount")),
            "signed_change_points": parse_decimal_value(normalized_row.get("signedChangePoints")),
            "current_effective_amount": parse_decimal_value(normalized_row.get("currentEffectiveAmount")),
            "consume_amount_raw": consume_amount_raw,
            "consume_amount": (
                (Decimal(consume_amount_raw) / Decimal("100")) if consume_amount_raw is not None else None
            ),
            "point_rate": parse_decimal_value(normalized_row.get("pointRate")),
            "point_ratio": parse_decimal_value(normalized_row.get("pointRatio")),
            "change_type_code": clean_text(normalized_row.get("changeTypeCode")),
            "change_type_name": clean_text(normalized_row.get("changeTypeName")),
            "business_type_name": clean_text(normalized_row.get("businessTypeName")),
            "source_code": clean_text(normalized_row.get("sourceCode")),
            "source_name": clean_text(normalized_row.get("sourceName")),
            "remark": clean_text(normalized_row.get("remark")),
            "market_activity_no": clean_text(normalized_row.get("marketActivityNo")),
            "market_activity_type": clean_text(normalized_row.get("marketActivityType")),
            "market_activity_name": clean_text(normalized_row.get("marketActivityName")),
            "create_time": parse_datetime_value(normalized_row.get("createTime")),
            "consume_time": parse_datetime_value(normalized_row.get("consumeTime")),
            "expire_time": parse_datetime_value(normalized_row.get("expireTime")),
            "source_file": str(csv_file.resolve()),
            "source_row_no": row_no,
            "extra": parse_json_like_value(normalized_row.get("extra")),
            "raw_json": normalized_row,
        }


class ParkingRecordCsvImportService(BaseCsvImportService):
    module_name = "parking_record_csv_import"
    model = ParkingRecord
    primary_field_name = "record_id"
    required_headers = PARKING_HEADER_FIELDS

    def __init__(self, db: Session, logger: LoggerCallback = None, batch_size: int = 1000):
        super().__init__(db=db, logger=logger, batch_size=batch_size)
        self._member_id_by_mobile: dict[str, str | None] = {}

    def enrich_items(self, items: list[dict[str, Any]]) -> None:
        unresolved = sorted(
            {
                item["mobile_no"]
                for item in items
                if item.get("mobile_no") and item["mobile_no"] not in self._member_id_by_mobile
            }
        )
        if unresolved:
            rows = self.db.execute(
                select(MemberProfile.mobile_no, MemberProfile.member_id).where(MemberProfile.mobile_no.in_(unresolved))
            ).all()
            grouped: dict[str, set[str]] = {}
            for mobile_no, member_id in rows:
                if not mobile_no or not member_id:
                    continue
                grouped.setdefault(str(mobile_no), set()).add(str(member_id))
            for mobile_no in unresolved:
                member_ids = grouped.get(mobile_no, set())
                self._member_id_by_mobile[mobile_no] = next(iter(member_ids)) if len(member_ids) == 1 else None

        for item in items:
            mobile_no = item.get("mobile_no")
            if mobile_no and not item.get("member_id"):
                item["member_id"] = self._member_id_by_mobile.get(mobile_no)

    def normalize_row(self, *, raw_row: dict[str, Any], csv_file: Path, row_no: int) -> dict[str, Any]:
        normalized_row = normalize_raw_row(raw_row)
        event_key = build_parking_event_key(normalized_row)
        record_id = clean_text(normalized_row.get("记录ID"))
        parking_serial_no = clean_text(normalized_row.get("停车流水号"))
        if not record_id and not parking_serial_no:
            raise ValueError("missing record_id and parking_serial_no")

        return {
            "event_key": event_key,
            "record_id": record_id,
            "parking_serial_no": parking_serial_no,
            "mobile_no": normalize_mobile_no(normalized_row.get("车牌匹配手机号")),
            "member_id": None,
            "plaza_bu_id": None,
            "plaza_name": clean_text(normalized_row.get("停车场")),
            "plate_no": normalize_plate_no(normalized_row.get("车牌号")),
            "entry_plate_no": normalize_plate_no(normalized_row.get("进场车牌")),
            "plate_color": clean_text(normalized_row.get("车牌颜色")),
            "plate_type": clean_text(normalized_row.get("车牌类型")),
            "vehicle_type_code": clean_text(normalized_row.get("车辆类型编码")),
            "vehicle_type_name": clean_text(normalized_row.get("车辆类型")),
            "vehicle_type_name_2": clean_text(normalized_row.get("车辆类型2")),
            "entry_time": parse_datetime_value(normalized_row.get("进场时间")),
            "exit_time": parse_datetime_value(normalized_row.get("出场时间")),
            "parking_duration_seconds": parse_int_value(normalized_row.get("停车时长(秒)")),
            "status": clean_text(normalized_row.get("状态")),
            "card_no": clean_text(normalized_row.get("卡号")),
            "card_id": clean_text(normalized_row.get("卡ID")),
            "ticket_no": clean_text(normalized_row.get("票据")),
            "entry_image_url": clean_text(normalized_row.get("进场图片")),
            "entry_channel": clean_text(normalized_row.get("进场通道")),
            "entry_guard_name": clean_text(normalized_row.get("进场保安")),
            "exit_image_url": clean_text(normalized_row.get("出场图片")),
            "exit_channel": clean_text(normalized_row.get("出场通道")),
            "exit_guard_name": clean_text(normalized_row.get("出场保安")),
            "auto_pay_flag": parse_bool_value(normalized_row.get("自动支付")),
            "total_fee_cent": parse_int_value(normalized_row.get("总费用(分)")),
            "discount_fee_cent": parse_int_value(normalized_row.get("减免(分)")),
            "online_pay_fee_cent": parse_int_value(normalized_row.get("线上支付(分)")),
            "balance_pay_fee_cent": parse_int_value(normalized_row.get("余额支付(分)")),
            "cash_pay_fee_cent": parse_int_value(normalized_row.get("现金支付(分)")),
            "prepaid_fee_cent": parse_int_value(normalized_row.get("预付费(分)")),
            "merchant_no": clean_text(normalized_row.get("商户号")),
            "parking_uuid": clean_text(normalized_row.get("停车场UUID")),
            "created_time": parse_datetime_value(normalized_row.get("创建时间")),
            "updated_time": parse_datetime_value(normalized_row.get("更新时间")),
            "source_file": str(csv_file.resolve()),
            "source_row_no": row_no,
            "raw_json": normalized_row,
        }
