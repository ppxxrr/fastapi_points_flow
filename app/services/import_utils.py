from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


CSV_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030", "gbk")
NULL_LITERALS = {"", "null", "none", "nil", "nan", "n/a"}
EXCEL_WRAPPED_RE = re.compile(r'^=\s*"(?P<value>.*)"$')


def clean_excel_wrapped_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    while True:
        matched = EXCEL_WRAPPED_RE.match(text)
        if not matched:
            break
        text = matched.group("value").strip()

    if len(text) >= 2 and text[0] == text[-1] == '"':
        text = text[1:-1].strip()

    if text.lower() in NULL_LITERALS:
        return None
    return text or None


def clean_text(value: Any) -> str | None:
    return clean_excel_wrapped_text(value)


def normalize_mobile_no(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None

    digits = re.sub(r"\D", "", text)
    if not digits:
        return text
    if len(digits) >= 11:
        return digits[-11:]
    return digits


def normalize_plate_no(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    return text.upper()


def parse_datetime_value(value: Any) -> datetime | None:
    cleaned = clean_text(value)
    if cleaned is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)

    if cleaned.isdigit():
        timestamp_value = int(cleaned)
        if timestamp_value > 1_000_000_000_000:
            timestamp_value /= 1000
        return datetime.fromtimestamp(timestamp_value, tz=timezone.utc).replace(tzinfo=None)

    normalized = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d",
    ):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def parse_decimal_value(value: Any) -> Decimal | None:
    cleaned = clean_text(value)
    if cleaned is None:
        return None
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_int_value(value: Any) -> int | None:
    parsed = parse_decimal_value(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (ValueError, OverflowError):
        return None


def parse_bool_value(value: Any) -> bool | None:
    cleaned = clean_text(value)
    if cleaned is None:
        return None

    lowered = cleaned.lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return None


def parse_json_like_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value

    cleaned = clean_text(value)
    if cleaned is None:
        return None
    if cleaned.startswith("{") or cleaned.startswith("["):
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return cleaned
    return cleaned


def normalize_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        normalized[str(key).strip()] = clean_text(value)
    return normalized


def to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    return value


def build_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(to_jsonable(payload), ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_csv_header(path: Path) -> tuple[list[str] | None, str | None]:
    for encoding in CSV_ENCODINGS:
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                header_line = handle.readline()
        except UnicodeDecodeError:
            continue
        if not header_line:
            return [], encoding
        header_line = header_line.lstrip("\ufeff").rstrip("\r\n")
        return [column.strip() for column in header_line.split(",")], encoding
    return None, None
