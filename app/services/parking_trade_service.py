from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from app.models.parking_trade import ParkingTradeRecord
from app.services.import_utils import (
    build_hash,
    clean_text,
    normalize_mobile_no,
    normalize_plate_no,
    parse_datetime_value,
    parse_int_value,
)
from app.services.member_point_flow_service import BaseCsvImportService


LoggerCallback = Callable[[str, str], None] | None
PLATE_PATTERNS = (
    re.compile(r"[【\[]\s*(?P<plate>[^】\]]+?)\s*[】\]]"),
    re.compile(r"[（(]\s*(?P<plate>[^）)]+?)\s*[）)]"),
)


def build_parking_trade_event_key(row: dict[str, Any]) -> str:
    trade_id = clean_text(row.get("id"))
    if trade_id:
        return build_hash({"trade_id": trade_id})
    return build_hash(
        {
            "merchant_no": clean_text(row.get("merchant")),
            "pay_serial": clean_text(row.get("pay_serial")),
            "trade_no": clean_text(row.get("trade_no")),
            "pay_time": clean_text(row.get("pay_time")),
            "value": clean_text(row.get("value")),
        }
    )


def coalesce_parking_trade_business_date(raw_row: dict[str, Any]) -> Any:
    parsed = (
        parse_datetime_value(raw_row.get("result_time"))
        or parse_datetime_value(raw_row.get("pay_time"))
        or parse_datetime_value(raw_row.get("create_time"))
    )
    return parsed.date() if parsed else None


def extract_plate_no(row: dict[str, Any]) -> str | None:
    for source in (row.get("subject"), row.get("body")):
        text = clean_text(source)
        if not text:
            continue
        for pattern in PLATE_PATTERNS:
            matched = pattern.search(text)
            if matched:
                return normalize_plate_no(matched.group("plate"))
    return None


class ParkingTradeImportService(BaseCsvImportService):
    module_name = "parking_trade_api_import"
    model = ParkingTradeRecord
    primary_field_name = "trade_id"
    required_headers: set[str] = set()

    def __init__(self, db: Session, logger: LoggerCallback = None, batch_size: int = 1000):
        super().__init__(db=db, logger=logger, batch_size=batch_size)

    def normalize_row(self, *, raw_row: dict[str, Any], csv_file: Path, row_no: int) -> dict[str, Any]:
        user = raw_row.get("user") if isinstance(raw_row.get("user"), dict) else {}
        normalized_user = user if isinstance(user, dict) else {}
        event_key = build_parking_trade_event_key(raw_row)
        trade_id = clean_text(raw_row.get("id"))
        if not trade_id and not event_key:
            raise ValueError("missing trade id")

        return {
            "event_key": event_key,
            "trade_id": trade_id,
            "merchant_no": clean_text(raw_row.get("merchant")),
            "app_id": clean_text(raw_row.get("app_id")),
            "pay_serial": clean_text(raw_row.get("pay_serial")),
            "trade_no": clean_text(raw_row.get("trade_no")),
            "subject": clean_text(raw_row.get("subject")),
            "body": clean_text(raw_row.get("body")),
            "business": clean_text(raw_row.get("business")),
            "business_voucher": clean_text(raw_row.get("business_voucher")),
            "pay_order": clean_text(raw_row.get("pay_order")),
            "plaza_name": clean_text(raw_row.get("merchant_name")),
            "plate_no": extract_plate_no(raw_row),
            "mobile_no": normalize_mobile_no(normalized_user.get("mobile")),
            "payer": clean_text(raw_row.get("payer")),
            "user_identity": clean_text(normalized_user.get("identity") or raw_row.get("identity")),
            "channel_code": clean_text(raw_row.get("channel")),
            "channel_name": clean_text(raw_row.get("channel_name")),
            "pay_mode_code": parse_int_value(raw_row.get("pay_mode")),
            "pay_mode_name": clean_text(raw_row.get("pay_mode_name")),
            "pay_type_code": parse_int_value(raw_row.get("pay_type")),
            "result_code": parse_int_value(raw_row.get("result")),
            "process_code": parse_int_value(raw_row.get("process")),
            "refund_code": parse_int_value(raw_row.get("refund")),
            "synced_flag": parse_int_value(raw_row.get("synced")),
            "value_cent": parse_int_value(raw_row.get("value")),
            "discount_cent": parse_int_value(raw_row.get("discount")),
            "reduce_value_cent": parse_int_value(raw_row.get("reduce_value")),
            "pay_value_cent": parse_int_value(raw_row.get("pay_value")),
            "actual_pay_unit_cent": parse_int_value(raw_row.get("actual_pay_unit")),
            "refund_value_cent": parse_int_value(raw_row.get("refund_value")),
            "actual_value_cent": parse_int_value(raw_row.get("actual_value")),
            "actual_fee_cent": parse_int_value(raw_row.get("actual_fee")),
            "fee_cent": parse_int_value(raw_row.get("fee")),
            "refund_fee_cent": parse_int_value(raw_row.get("refund_fee")),
            "coupon_id": clean_text(raw_row.get("coupon_id")),
            "coupon_purpose": clean_text(raw_row.get("coupon_purpose")),
            "notify_service": clean_text(raw_row.get("notify_service")),
            "notify_url": clean_text(raw_row.get("notify_url")),
            "settle_id": clean_text(raw_row.get("settle_id")),
            "deduct_mode": clean_text(raw_row.get("deduct_mode")),
            "remark": clean_text(raw_row.get("remark")),
            "trade_scene": clean_text(raw_row.get("trade_scene")),
            "create_time": parse_datetime_value(raw_row.get("create_time")),
            "update_time": parse_datetime_value(raw_row.get("update_time")),
            "expire_time": parse_datetime_value(raw_row.get("expire_time")),
            "pay_time": parse_datetime_value(raw_row.get("pay_time")),
            "result_time": parse_datetime_value(raw_row.get("result_time")),
            "settle_time": parse_datetime_value(raw_row.get("settle_time")),
            "source_file": str(csv_file),
            "source_row_no": row_no,
            "raw_json": raw_row,
        }
