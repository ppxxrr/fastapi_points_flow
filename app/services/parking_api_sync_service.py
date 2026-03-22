from __future__ import annotations

import base64
import json
import math
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests
from sqlalchemy import func, select

from app.db.session import SessionLocal
from app.models.parking import ParkingRecord


LoggerCallback = Callable[[str, str], None] | None

CST = timezone(timedelta(hours=8))
UTC = timezone.utc
PARKING_ENDPOINT = "https://mapi.4pyun.com/rest/2.0/parking/parking/record/list"
PLATE_ENDPOINT = "https://mapi.4pyun.com/rest/2.0/member/plate/list"
PARKING_REFERER_URL = os.getenv(
    "PARKING_PORTAL_URL",
    "https://mch.4pyun.com/parking-merchant/parking-manage/parking",
)
PLATE_REFERER_URL = os.getenv(
    "PARKING_MEMBER_URL",
    "https://mch.4pyun.com/parking-merchant/member/manage",
)
PARKING_MANAGEMENT_ORIGIN = "https://mch.4pyun.com"
API_XOR_SECRET = os.getenv("PARKING_API_XOR_SECRET", "c67b8c67558a2e609912a532e45ebb30")
PARKING_MERCHANT_ID = os.getenv("PARKING_MERCHANT_ID", "18123302")
PLATE_RT_ID = os.getenv("PARKING_PLATE_RT_ID", "361")
PARKING_PAGE_SIZE = int(os.getenv("PARKING_API_PAGE_SIZE", "99"))
PLATE_PAGE_SIZE = int(os.getenv("PARKING_PLATE_PAGE_SIZE", "99"))
PARKING_WORKERS = int(os.getenv("PARKING_API_WORKERS", "4"))
PLATE_WORKERS = int(os.getenv("PARKING_PLATE_WORKERS", "4"))
PARKING_RATE_PER_SEC = float(os.getenv("PARKING_API_RATE_PER_SEC", "5"))
PLATE_RATE_PER_SEC = float(os.getenv("PARKING_PLATE_RATE_PER_SEC", "5"))
REQUEST_TIMEOUT = int(os.getenv("PARKING_API_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.getenv("PARKING_API_MAX_RETRIES", "5"))
BACKOFF_BASE_SECONDS = float(os.getenv("PARKING_API_BACKOFF_SECONDS", "1.5"))
KEEPALIVE_INTERVAL_SECONDS = int(os.getenv("PARKING_KEEPALIVE_SECONDS", "45"))
KEEPALIVE_AUTH_REFRESH_MARGIN_SECONDS = int(os.getenv("PARKING_KEEPALIVE_AUTH_REFRESH_MARGIN_SECONDS", "600"))
LIVE_CHROME_TOKEN_WAIT_SECONDS = int(os.getenv("PARKING_LIVE_CHROME_TOKEN_WAIT_SECONDS", "20"))
LIVE_CHROME_PROFILE_REFRESH_WAIT_SECONDS = int(os.getenv("PARKING_LIVE_CHROME_PROFILE_REFRESH_WAIT_SECONDS", "8"))
MIN_AUTH_TOKEN_TTL_SECONDS = int(os.getenv("PARKING_MIN_AUTH_TOKEN_TTL_SECONDS", "120"))
PARKING_API_AUTHORIZATION = os.getenv("PARKING_API_AUTHORIZATION", "").strip()
PARKING_USE_LIVE_CHROME_AUTH = os.getenv("PARKING_USE_LIVE_CHROME_AUTH", "true").lower() == "true"
CHROME_PATH = Path(os.getenv("PARKING_CHROME_PATH", "/usr/bin/google-chrome"))
MANAGED_CHROME_PROFILE_DIR = Path(
    os.getenv("PARKING_MANAGED_CHROME_PROFILE_DIR", str(Path.home() / ".config" / "google-chrome-4pyun"))
)
MANAGED_CHROME_REMOTE_DEBUGGING_PORT = int(os.getenv("PARKING_MANAGED_CHROME_PORT", "9223"))
MANAGED_CHROME_COOKIE_FILE = MANAGED_CHROME_PROFILE_DIR / "Default" / "Cookies"
MANAGED_CHROME_KEY_FILE = MANAGED_CHROME_PROFILE_DIR / "Local State"
JWT_PATTERN = re.compile(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
PLATE_IGNORE_CHARS = {" ", "路", "鈥", "銉", ".", "，", ","}
BASE_DIR = Path(__file__).resolve().parents[2]
PLATE_CACHE_PATH = BASE_DIR / "data" / "cache" / "parking_plate_mobile_cache.json"
PLATE_CACHE_TTL_SECONDS = int(os.getenv("PARKING_PLATE_CACHE_TTL_SECONDS", "43200"))
DB_PLATE_CACHE_LIMIT = int(os.getenv("PARKING_DB_PLATE_CACHE_LIMIT", "50000"))
PARKING_SKIP_REMOTE_PLATE_CACHE_IF_DB_SEEDED = (
    os.getenv("PARKING_SKIP_REMOTE_PLATE_CACHE_IF_DB_SEEDED", "true").lower() == "true"
)
KEEPALIVE_URLS = [
    value.strip()
    for value in os.getenv(
        "PARKING_KEEPALIVE_URLS",
        ",".join(
            [
                PARKING_REFERER_URL,
                PLATE_REFERER_URL,
                "https://mch.4pyun.com/trading-center/trade/payment",
            ]
        ),
    ).split(",")
    if value.strip()
]


def _noop_logger(level: str, message: str) -> None:
    return None


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_plate(value: Any) -> str:
    text = _clean_text(value).upper()
    return "".join(char for char in text if char not in PLATE_IGNORE_CHARS)


def _status_text(value: Any) -> str:
    mapping = {0: "未知", 1: "在场", 2: "已出场", -1: "全部"}
    return mapping.get(value, str(value or ""))


def _format_utc_to_cst(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.astimezone(CST).strftime("%Y-%m-%d %H:%M:%S")


def _utc_range_for_local_date(target_date: date) -> tuple[str, str]:
    start_local = datetime.combine(target_date, dt_time.min, tzinfo=CST)
    end_local = datetime.combine(target_date, dt_time.max.replace(microsecond=0), tzinfo=CST)
    return (
        start_local.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_local.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def _api_encode(value: str) -> str:
    plain_bytes = value.encode("utf-8")
    secret_length = len(API_XOR_SECRET)
    encrypted = bytearray()
    total_length = len(plain_bytes)
    for index, current in enumerate(plain_bytes):
        secret_index = (total_length - index) % secret_length
        encrypted.append(ord(API_XOR_SECRET[secret_index]) ^ (~current & 0xFF))
    return base64.b64encode(bytes(encrypted)).decode()


def _encode_param(name: str, value: Any) -> tuple[str, str]:
    return quote(_api_encode(name), safe=""), quote(_api_encode(str(value)), safe="")


def _build_encoded_url(endpoint: str, params: dict[str, Any]) -> str:
    pairs: list[str] = []
    for key, value in params.items():
        if value is None:
            continue
        encoded_key, encoded_value = _encode_param(key, value)
        pairs.append(f"{encoded_key}={encoded_value}")
    return f"{endpoint}?{'&'.join(pairs)}"


class RateLimiter:
    def __init__(self, rate_per_second: float) -> None:
        self.interval = 0.0 if rate_per_second <= 0 else 1.0 / rate_per_second
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        sleep_seconds = 0.0
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_seconds = self._next_allowed - now
                self._next_allowed += self.interval
            else:
                self._next_allowed = now + self.interval
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


@dataclass(slots=True)
class ParkingApiRequestStats:
    target_date: str
    fetched_rows: int = 0
    total_pages: int = 0
    total_count: int = 0
    plate_cache_size: int = 0
    unmatched_mobile_count: int = 0
    duplicate_record_id_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_date": self.target_date,
            "fetched_rows": self.fetched_rows,
            "total_pages": self.total_pages,
            "total_count": self.total_count,
            "plate_cache_size": self.plate_cache_size,
            "unmatched_mobile_count": self.unmatched_mobile_count,
            "duplicate_record_id_count": self.duplicate_record_id_count,
        }


class CDPTab:
    def __init__(self, websocket_url: str) -> None:
        from websocket import create_connection

        self._connection = create_connection(websocket_url, timeout=30, enable_multithread=True, suppress_origin=True)
        self._next_id = 0

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass

    def send(self, method: str, params: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
        self._next_id += 1
        command_id = self._next_id
        self._connection.send(json.dumps({"id": command_id, "method": method, "params": params or {}}))
        deadline = time.time() + timeout
        while time.time() < deadline:
            message = json.loads(self._connection.recv())
            if message.get("id") != command_id:
                continue
            if "error" in message:
                raise RuntimeError(f"CDP command failed: {method}: {message['error']}")
            return message.get("result", {})
        raise RuntimeError(f"CDP command timeout: {method}")

    def evaluate(self, expression: str, *, timeout: int = 30) -> Any:
        result = self.send(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
            timeout=timeout,
        )
        return result.get("result", {}).get("value")


class ParkingApiClient:
    def __init__(self, logger: LoggerCallback = None) -> None:
        self.logger = logger or _noop_logger
        self._auth_header: str | None = None
        self._auth_source: str | None = None
        self._auth_exp: int = 0
        self._live_cookies: list[Any] = []
        self._plate_mobile_map: dict[str, str] | None = None
        self._parking_stats: dict[date, ParkingApiRequestStats] = {}
        self._keepalive_stop = threading.Event()
        self._keepalive_thread: threading.Thread | None = None
        self._auth_lock = threading.Lock()

    def ensure_authorization(self) -> str:
        if self._auth_header:
            return self._auth_header
        if PARKING_API_AUTHORIZATION:
            self._auth_header = PARKING_API_AUTHORIZATION
            self._auth_source = "env"
            self._auth_exp = 0
            self.logger("INFO", "parking api auth resolved from PARKING_API_AUTHORIZATION")
            return self._auth_header
        if not PARKING_USE_LIVE_CHROME_AUTH:
            raise RuntimeError("Parking API authorization is not configured.")
        storage_token, storage_exp = self._resolve_authorization_from_profile_storage()
        if storage_token and self._is_token_usable(storage_exp):
            self._live_cookies = self._collect_live_cookies()
            self._auth_header = storage_token
            self._auth_source = "chrome_profile_storage"
            self._auth_exp = storage_exp
            self.logger("INFO", "parking api auth resolved from chrome profile local storage")
            return self._auth_header
        if storage_token and not self._is_token_usable(storage_exp):
            self.logger(
                "WARNING",
                f"parking api skipped expired chrome profile token exp={storage_exp} ttl={storage_exp - int(time.time())}",
            )
        self._auth_header, self._live_cookies = self._resolve_authorization_from_live_chrome()
        self._auth_source = "chrome"
        self._auth_exp = self._jwt_exp(self._auth_header.removeprefix("Bearer ").strip())
        self.logger("INFO", f"parking api auth resolved from live chrome session, cookies={len(self._live_cookies)}")
        return self._auth_header

    def refresh_authorization(self, reason: str) -> str:
        with self._auth_lock:
            self._auth_header = None
            self._auth_source = None
            self._auth_exp = 0
            if PARKING_USE_LIVE_CHROME_AUTH and ("401" in reason or "expiring" in reason):
                try:
                    token, live_cookies = self._resolve_authorization_from_live_chrome()
                    self._auth_header = token
                    self._auth_source = "chrome"
                    self._auth_exp = self._jwt_exp(token.removeprefix("Bearer ").strip())
                    self._live_cookies = live_cookies
                    self.logger(
                        "INFO",
                        f"parking api authorization refreshed source={self._auth_source} reason={reason}",
                    )
                    return token
                except Exception as live_exc:  # noqa: BLE001
                    self.logger("WARNING", f"parking api live chrome auth refresh failed after {reason}: {live_exc}")
            token = self.ensure_authorization()
            self.logger(
                "INFO",
                f"parking api authorization refreshed source={self._auth_source or 'unknown'} reason={reason}",
            )
            return token

    def authorization_debug(self) -> dict[str, Any]:
        expires_at = (
            datetime.fromtimestamp(self._auth_exp, tz=CST).isoformat(timespec="seconds")
            if self._auth_exp
            else None
        )
        seconds_until_expiry = self._auth_exp - int(time.time()) if self._auth_exp else None
        return {
            "source": self._auth_source or "unknown",
            "expires_at": expires_at,
            "seconds_until_expiry": seconds_until_expiry,
            "has_live_cookies": bool(self._live_cookies),
        }

    def run_keepalive_cycle(self) -> dict[str, Any]:
        self.ensure_authorization()
        if self._auth_source in {"chrome", "chrome_profile_storage"}:
            seconds_until_expiry = self._auth_exp - int(time.time()) if self._auth_exp else None
            if seconds_until_expiry is not None and seconds_until_expiry <= KEEPALIVE_AUTH_REFRESH_MARGIN_SECONDS:
                try:
                    refreshed = self._refresh_profile_token_via_running_chrome()
                    if not refreshed:
                        self.refresh_authorization(reason=f"keepalive-expiring:{seconds_until_expiry}")
                except Exception as exc:  # noqa: BLE001
                    self.logger("WARNING", f"parking api proactive auth refresh failed: {exc}")

            refreshed_header, refreshed_exp = self._resolve_authorization_from_profile_storage()
            if refreshed_header and (
                refreshed_header != self._auth_header
                or (refreshed_exp and refreshed_exp > self._auth_exp)
            ):
                self._auth_header = refreshed_header
                self._auth_source = "chrome_profile_storage"
                self._auth_exp = refreshed_exp
                self.logger(
                    "INFO",
                    f"parking api auth refreshed from chrome profile keepalive exp={self._auth_exp}",
                )

        page_statuses: list[dict[str, Any]]
        try:
            page_statuses = self._run_browser_keepalive_cycle()
        except Exception as browser_exc:  # noqa: BLE001
            self.logger("WARNING", f"parking browser keepalive failed, fallback to requests session: {browser_exc}")
            page_statuses = self._run_requests_keepalive_cycle()

        api_probe = self._run_keepalive_api_probe()
        return {
            "auth": self.authorization_debug(),
            "pages": page_statuses,
            "api_probe": api_probe,
        }

    def available_dates(self, target_dates: set[date]) -> set[date]:
        if not target_dates:
            return set()
        self.ensure_authorization()
        today_local = datetime.now(CST).date()
        return {value for value in target_dates if value < today_local}

    def fetch_rows_for_dates(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        authorization = self.ensure_authorization()
        if self._auth_source in {"chrome", "chrome_profile_storage"}:
            self._start_keepalive_if_needed()
        plate_mobile_map = self._get_plate_mobile_map(authorization)

        grouped: dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]] = {}
        for target_date in sorted(target_dates):
            rows = self._fetch_single_date_rows(target_date, authorization, plate_mobile_map)
            virtual_path = Path(f"parking_api_{target_date.isoformat()}.virtual")
            grouped[target_date] = [(row, virtual_path, index) for index, row in enumerate(rows, start=1)]
        return grouped

    def get_stats(self) -> dict[str, Any]:
        return {value.isoformat(): stats.to_dict() for value, stats in self._parking_stats.items()}

    def close(self) -> None:
        self._keepalive_stop.set()
        if self._keepalive_thread is not None:
            self._keepalive_thread.join(timeout=2)
            self._keepalive_thread = None

    def _fetch_single_date_rows(
        self,
        target_date: date,
        authorization: str,
        plate_mobile_map: dict[str, str],
    ) -> list[dict[str, Any]]:
        limiter = RateLimiter(PARKING_RATE_PER_SEC)
        start_leave_time, end_leave_time = _utc_range_for_local_date(target_date)
        base_params = {
            "status": "-1",
            "merchant": PARKING_MERCHANT_ID,
            "min_park_time": "0",
            "max_park_time": "0",
            "start_leave_time": start_leave_time,
            "end_leave_time": end_leave_time,
        }
        headers = self._build_headers(authorization=authorization, referer=PARKING_REFERER_URL)
        first_header, first_rows = self._request_page(
            endpoint=PARKING_ENDPOINT,
            headers=headers,
            params=base_params,
            page_index=1,
            page_size=PARKING_PAGE_SIZE,
            limiter=limiter,
            label=f"parking:{target_date.isoformat()}",
        )
        total_count = int(first_header.get("total_count") or 0)
        total_pages = int(first_header.get("page_count") or 0) or max(1, math.ceil(total_count / PARKING_PAGE_SIZE))
        all_rows = list(first_rows)
        if total_pages > 1:
            with ThreadPoolExecutor(max_workers=PARKING_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self._request_page,
                        endpoint=PARKING_ENDPOINT,
                        headers=headers,
                        params=base_params,
                        page_index=page_index,
                        page_size=PARKING_PAGE_SIZE,
                        limiter=limiter,
                        label=f"parking:{target_date.isoformat()}",
                    ): page_index
                    for page_index in range(2, total_pages + 1)
                }
                for future in as_completed(futures):
                    _, page_rows = future.result()
                    all_rows.extend(page_rows)

        duplicate_ids = 0
        unmatched = 0
        id_seen: set[str] = set()
        converted: list[dict[str, Any]] = []
        for row in sorted(all_rows, key=lambda item: (_clean_text(item.get("leave_time")), _clean_text(item.get("id")))):
            record_id = _clean_text(row.get("id"))
            if record_id:
                if record_id in id_seen:
                    duplicate_ids += 1
                    continue
                id_seen.add(record_id)
            plate_candidates = {_normalize_plate(row.get("plate")), _normalize_plate(row.get("enter_plate"))}
            plate_candidates.discard("")
            matched_mobiles = sorted({plate_mobile_map[plate] for plate in plate_candidates if plate in plate_mobile_map})
            matched_mobile = matched_mobiles[0] if len(matched_mobiles) == 1 else ""
            if not matched_mobile:
                unmatched += 1
            converted.append(self._build_import_row(row, matched_mobile))

        stats = ParkingApiRequestStats(
            target_date=target_date.isoformat(),
            fetched_rows=len(converted),
            total_pages=total_pages,
            total_count=total_count,
            plate_cache_size=len(plate_mobile_map),
            unmatched_mobile_count=unmatched,
            duplicate_record_id_count=duplicate_ids,
        )
        self._parking_stats[target_date] = stats
        self.logger(
            "INFO",
            f"parking api fetched {target_date.isoformat()} rows={stats.fetched_rows} total_count={stats.total_count} "
            f"pages={stats.total_pages} unmatched_mobile={stats.unmatched_mobile_count} duplicates={stats.duplicate_record_id_count}",
        )
        return converted

    def _build_import_row(self, source: dict[str, Any], matched_mobile: str) -> dict[str, Any]:
        return {
            "车牌匹配手机号": matched_mobile,
            "车牌号": _clean_text(source.get("plate")),
            "进场车牌": _clean_text(source.get("enter_plate")),
            "车牌颜色": _clean_text(source.get("plate_color")),
            "车辆类型编码": _clean_text(source.get("car_type")),
            "车辆类型": _clean_text(source.get("car_desc")),
            "车辆类型2": _clean_text(source.get("vehicle_type")),
            "停车流水号": _clean_text(source.get("parking_serial")),
            "进场时间": _format_utc_to_cst(source.get("enter_time")),
            "出场时间": _format_utc_to_cst(source.get("leave_time")),
            "停车时长(秒)": _clean_text(source.get("parking_time")),
            "停车场": _clean_text(source.get("park_name")),
            "状态": _status_text(source.get("status")),
            "卡号": _clean_text(source.get("card_no")),
            "卡ID": _clean_text(source.get("card_id")),
            "票据": _clean_text(source.get("ticket_formated")),
            "进场图片": _clean_text(source.get("enter_image")),
            "进场通道": _clean_text(source.get("enter_gate")),
            "进场保安": _clean_text(source.get("enter_security")),
            "出场图片": _clean_text(source.get("leave_image")),
            "出场通道": _clean_text(source.get("leave_gate")),
            "出场保安": _clean_text(source.get("leave_security")),
            "自动支付": _clean_text(source.get("autopay")),
            "总费用(分)": _clean_text(source.get("total_value")),
            "减免(分)": _clean_text(source.get("free_value")),
            "线上支付(分)": _clean_text(source.get("online_value")),
            "余额支付(分)": _clean_text(source.get("balance_value")),
            "现金支付(分)": _clean_text(source.get("cash_value")),
            "预付费(分)": _clean_text(source.get("prepaid_value")),
            "记录ID": _clean_text(source.get("id")),
            "商户号": _clean_text(source.get("merchant")),
            "停车场UUID": _clean_text(source.get("park_uuid")),
            "创建时间": _format_utc_to_cst(source.get("create_time")),
            "更新时间": _format_utc_to_cst(source.get("update_time")),
            "车牌类型": _clean_text(source.get("plate_type")),
        }

    def _get_plate_mobile_map(self, authorization: str) -> dict[str, str]:
        if self._plate_mobile_map is not None:
            return self._plate_mobile_map
        db_seed = self._load_plate_map_from_database()
        if db_seed and PARKING_SKIP_REMOTE_PLATE_CACHE_IF_DB_SEEDED:
            self._plate_mobile_map = db_seed
            self.logger(
                "INFO",
                f"parking plate cache using database seed only unique_plate={len(db_seed)} skip_remote=true",
            )
            return db_seed
        cached = self._load_cached_plate_map()
        if cached is not None:
            merged = dict(db_seed)
            merged.update(cached)
            self._plate_mobile_map = merged
            self.logger(
                "INFO",
                f"parking plate cache reused from {PLATE_CACHE_PATH} unique_plate={len(cached)} db_seed={len(db_seed)} merged={len(merged)}",
            )
            return merged
        limiter = RateLimiter(PLATE_RATE_PER_SEC)
        headers = self._build_headers(authorization=authorization, referer=PLATE_REFERER_URL)
        base_params = {"rt_id": PLATE_RT_ID}
        total_count = 0
        total_pages = 0
        all_rows: list[dict[str, Any]] = []
        try:
            first_header, first_rows = self._request_page(
                endpoint=PLATE_ENDPOINT,
                headers=headers,
                params=base_params,
                page_index=1,
                page_size=PLATE_PAGE_SIZE,
                limiter=limiter,
                label="parking:plate-cache",
            )
            total_count = int(first_header.get("total_count") or 0)
            total_pages = int(first_header.get("page_count") or 0) or max(1, math.ceil(total_count / PLATE_PAGE_SIZE))
            all_rows = list(first_rows)
            if total_pages > 1:
                with ThreadPoolExecutor(max_workers=PLATE_WORKERS) as executor:
                    futures = {
                        executor.submit(
                            self._request_page,
                            endpoint=PLATE_ENDPOINT,
                            headers=headers,
                            params=base_params,
                            page_index=page_index,
                            page_size=PLATE_PAGE_SIZE,
                            limiter=limiter,
                            label="parking:plate-cache",
                        ): page_index
                        for page_index in range(2, total_pages + 1)
                    }
                    for future in as_completed(futures):
                        _, page_rows = future.result()
                        all_rows.extend(page_rows)
        except Exception as exc:  # noqa: BLE001
            self.logger(
                "WARNING",
                f"parking plate cache remote load failed, fallback to database mapping only: {exc}",
            )
            self._plate_mobile_map = db_seed
            return db_seed

        plate_map: dict[str, str] = dict(db_seed)
        collisions = 0
        for row in all_rows:
            if row.get("audit_status") not in (None, 1):
                continue
            plate = _normalize_plate(row.get("plate"))
            mobile = _clean_text((row.get("member") or {}).get("mobile"))
            if not plate or not mobile:
                continue
            existing = plate_map.get(plate)
            if existing and existing != mobile:
                collisions += 1
                plate_map.pop(plate, None)
                continue
            if plate not in plate_map:
                plate_map[plate] = mobile

        self._plate_mobile_map = plate_map
        self._save_cached_plate_map(plate_map)
        self.logger(
            "INFO",
            f"parking plate cache loaded total_count={total_count} pages={total_pages} unique_plate={len(plate_map)} collisions={collisions} db_seed={len(db_seed)}",
        )
        return plate_map

    def _load_plate_map_from_database(self) -> dict[str, str]:
        plate_map: dict[str, str] = {}
        try:
            with SessionLocal() as session:
                stmt = (
                    select(ParkingRecord.plate_no, ParkingRecord.mobile_no, func.count().label("cnt"))
                    .where(
                        ParkingRecord.plate_no.is_not(None),
                        ParkingRecord.plate_no != "",
                        ParkingRecord.mobile_no.is_not(None),
                        ParkingRecord.mobile_no != "",
                    )
                    .group_by(ParkingRecord.plate_no, ParkingRecord.mobile_no)
                    .order_by(func.count().desc())
                    .limit(DB_PLATE_CACHE_LIMIT)
                )
                rows = session.execute(stmt).all()
            grouped: dict[str, set[str]] = {}
            for plate_no, mobile_no, _count in rows:
                plate = _normalize_plate(plate_no)
                mobile = _clean_text(mobile_no)
                if not plate or not mobile:
                    continue
                grouped.setdefault(plate, set()).add(mobile)
            for plate, mobiles in grouped.items():
                if len(mobiles) == 1:
                    plate_map[plate] = next(iter(mobiles))
            if plate_map:
                self.logger("INFO", f"parking plate cache seeded from database unique_plate={len(plate_map)}")
        except Exception as exc:  # noqa: BLE001
            self.logger("WARNING", f"parking plate cache database seed failed: {exc}")
        return plate_map

    def _load_cached_plate_map(self) -> dict[str, str] | None:
        if not PLATE_CACHE_PATH.exists():
            return None
        try:
            payload = json.loads(PLATE_CACHE_PATH.read_text(encoding="utf-8"))
            cached_at = datetime.fromisoformat(str(payload.get("cached_at")))
            age_seconds = (datetime.now() - cached_at).total_seconds()
            if age_seconds > PLATE_CACHE_TTL_SECONDS:
                return None
            records = payload.get("plates") or {}
            if not isinstance(records, dict):
                return None
            mapped = {str(key): str(value) for key, value in records.items() if key and value}
            return mapped or None
        except Exception:
            return None

    def _save_cached_plate_map(self, plate_map: dict[str, str]) -> None:
        if not plate_map:
            return
        PLATE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "cached_at": datetime.now().isoformat(timespec="seconds"),
            "plates": plate_map,
        }
        PLATE_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _request_page(
        self,
        *,
        endpoint: str,
        headers: dict[str, str],
        params: dict[str, Any],
        page_index: int,
        page_size: int,
        limiter: RateLimiter,
        label: str,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        query_params = dict(params)
        query_params["page_index"] = page_index
        query_params["page_size"] = page_size
        url = _build_encoded_url(endpoint, query_params)
        last_error: Exception | None = None
        auth_refreshed = False

        for attempt in range(1, MAX_RETRIES + 1):
            limiter.wait()
            response: requests.Response | None = None
            try:
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code == 401:
                    raise RuntimeError("http_401")
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise RuntimeError(f"http_{response.status_code}")
                response.raise_for_status()
                payload = response.json()
                code = str(payload.get("code", ""))
                if code == "401":
                    raise RuntimeError("api_code_401")
                if code not in {"0", "1001", "1002"}:
                    raise RuntimeError(f"api_code_{code}")
                content = payload.get("payload") or {}
                return content.get("paging_header") or {}, content.get("row") or []
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                status_code = response.status_code if response is not None else None
                excerpt = response.text[:300] if response is not None else ""
                self.logger(
                    "WARNING",
                    f"{label} request failed page={page_index} attempt={attempt}/{MAX_RETRIES} "
                    f"status={status_code} error={exc} excerpt={excerpt}",
                )
                if (
                    not auth_refreshed
                    and PARKING_USE_LIVE_CHROME_AUTH
                    and self._auth_source in {"chrome", "chrome_profile_storage"}
                    and "401" in str(exc)
                ):
                    try:
                        headers["authorization"] = self.refresh_authorization(reason=f"{label}:401")
                        auth_refreshed = True
                        continue
                    except Exception as refresh_exc:  # noqa: BLE001
                        self.logger("WARNING", f"{label} auth refresh failed after 401: {refresh_exc}")
                if attempt == MAX_RETRIES:
                    break
                time.sleep(BACKOFF_BASE_SECONDS * attempt)
        raise RuntimeError(f"{label} page={page_index} failed: {last_error}") from last_error

    def _build_headers(self, *, authorization: str, referer: str) -> dict[str, str]:
        return {
            "accept": "application/json, text/plain, */*",
            "authorization": authorization,
            "content-type": "application/x-www-form-urlencoded",
            "origin": PARKING_MANAGEMENT_ORIGIN,
            "referer": referer,
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
        }

    def _is_token_usable(self, exp: int) -> bool:
        if not exp:
            return False
        return exp - int(time.time()) > MIN_AUTH_TOKEN_TTL_SECONDS

    def _ensure_chrome_running(self) -> None:
        proc_root = Path("/proc")
        if not proc_root.exists():
            raise RuntimeError("Live chrome auth is only supported on the Linux server.")
        for proc_entry in proc_root.iterdir():
            if not proc_entry.name.isdigit():
                continue
            try:
                cmdline = (proc_entry / "cmdline").read_text(encoding="utf-8", errors="ignore").replace("\x00", " ").lower()
            except OSError:
                continue
            if "/chrome" in cmdline or "google-chrome" in cmdline:
                return
        raise RuntimeError("No running Chrome process was detected for parking live-auth.")

    def _hydrate_desktop_session_env(self) -> None:
        wanted = {"DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY"}
        if os.getenv("DBUS_SESSION_BUS_ADDRESS") and os.getenv("XDG_RUNTIME_DIR"):
            return
        proc_root = Path("/proc")
        if not proc_root.exists():
            return
        for proc_entry in proc_root.iterdir():
            if not proc_entry.name.isdigit():
                continue
            try:
                cmdline = (proc_entry / "cmdline").read_text(encoding="utf-8", errors="ignore").replace("\x00", " ").lower()
            except OSError:
                continue
            if "chrome" not in cmdline:
                continue
            try:
                env_blob = (proc_entry / "environ").read_bytes().split(b"\x00")
            except OSError:
                continue
            loaded = 0
            for entry in env_blob:
                if b"=" not in entry:
                    continue
                key, value = entry.split(b"=", 1)
                key_text = key.decode("utf-8", errors="ignore")
                if key_text not in wanted or os.getenv(key_text):
                    continue
                os.environ[key_text] = value.decode("utf-8", errors="ignore")
                loaded += 1
            if loaded > 0:
                return

    def _collect_live_cookies(self) -> list[Any]:
        import browser_cookie3

        self._ensure_chrome_running()
        self._hydrate_desktop_session_env()
        cookies: list[Any] = []
        seen: set[tuple[str, str, str]] = set()
        sources: list[tuple[Path | None, Path | None]] = []
        if MANAGED_CHROME_COOKIE_FILE.is_file() and MANAGED_CHROME_KEY_FILE.is_file():
            sources.append((MANAGED_CHROME_COOKIE_FILE, MANAGED_CHROME_KEY_FILE))
        sources.append((None, None))
        for domain in ("mch.4pyun.com", ".4pyun.com"):
            for cookie_file, key_file in sources:
                try:
                    if cookie_file is not None and key_file is not None:
                        jar = browser_cookie3.chrome(
                            cookie_file=str(cookie_file),
                            key_file=str(key_file),
                            domain_name=domain,
                        )
                    else:
                        jar = browser_cookie3.chrome(domain_name=domain)
                except Exception:
                    continue
                for cookie in jar:
                    key = (cookie.domain or "", cookie.path or "/", cookie.name)
                    if key in seen:
                        continue
                    seen.add(key)
                    cookies.append(cookie)
        if not cookies:
            raise RuntimeError("No live Chrome cookies were found for mch.4pyun.com.")
        return cookies

    def _resolve_authorization_from_profile_storage(self) -> tuple[str | None, int]:
        roots = [
            MANAGED_CHROME_PROFILE_DIR,
            Path.home() / ".config" / "google-chrome",
            Path.home() / ".config" / "chromium",
        ]
        candidates: dict[str, int] = {}
        for root in roots:
            if not root.exists():
                continue
            for leveldb_dir in root.glob("*/Local Storage/leveldb"):
                for candidate_file in sorted(list(leveldb_dir.glob("*.log")) + list(leveldb_dir.glob("*.ldb"))):
                    try:
                        text = candidate_file.read_bytes().decode("utf-8", errors="ignore")
                    except Exception:
                        continue
                    for match in JWT_PATTERN.finditer(text):
                        token = match.group(0)
                        if token not in candidates:
                            candidates[token] = self._jwt_exp(token)
        if not candidates:
            return None, 0
        token, exp = max(candidates.items(), key=lambda item: (item[1], len(item[0])))
        return f"Bearer {token}", exp

    def _jwt_exp(self, token: str) -> int:
        try:
            payload_segment = token.split(".")[1]
            padded = payload_segment + "=" * (-len(payload_segment) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
            return int(payload.get("exp") or 0)
        except Exception:
            return 0

    def _resolve_authorization_from_live_chrome(self) -> tuple[str, list[Any]]:
        cookies = self._collect_live_cookies()
        browser_tab: CDPTab | None = None
        browser_client: CDPTab | None = None
        browser_target_id: str | None = None
        runtime_dir = Path(tempfile.mkdtemp(prefix="parking_auth_"))
        process: subprocess.Popen[Any] | None = None
        tab: CDPTab | None = None
        try:
            try:
                browser_client, browser_tab, browser_target_id = self._open_running_chrome_tab()
                token = self._poll_token_from_live_browser(browser_tab)
                if token:
                    return token, cookies
                raise RuntimeError("Unable to extract a parking API bearer token from running Chrome storage.")
            except Exception:
                if browser_tab is not None:
                    browser_tab.close()
                    browser_tab = None
                if browser_client is not None and browser_target_id:
                    self._close_running_chrome_target(browser_client, browser_target_id)
                if browser_client is not None:
                    browser_client.close()
                    browser_client = None
                browser_target_id = None
            process, tab = self._launch_headless_browser(runtime_dir)
            self._inject_cookies(tab, cookies)
            token = self._poll_token_from_live_browser(tab)
            if not token:
                raise RuntimeError("Unable to extract a parking API bearer token from live Chrome storage.")
            return token, cookies
        finally:
            if browser_tab is not None:
                browser_tab.close()
            if browser_client is not None and browser_target_id:
                self._close_running_chrome_target(browser_client, browser_target_id)
            if browser_client is not None:
                browser_client.close()
            if tab is not None:
                tab.close()
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def _extract_bearer_token(self, storage_items: list[dict[str, Any]]) -> str | None:
        candidates: list[str] = []

        def walk(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    candidates.append(stripped)
                if stripped.startswith("{") or stripped.startswith("["):
                    try:
                        walk(json.loads(stripped))
                    except Exception:
                        return
                return
            if isinstance(value, dict):
                for item in value.values():
                    walk(item)
                return
            if isinstance(value, list):
                for item in value:
                    walk(item)

        for item in storage_items:
            walk(item.get("value"))

        for candidate in candidates:
            if "Bearer " in candidate and JWT_PATTERN.search(candidate):
                matched = re.search(r"Bearer\s+(eyJ[A-Za-z0-9._-]+)", candidate)
                if matched:
                    return f"Bearer {matched.group(1)}"
        for candidate in candidates:
            matched = JWT_PATTERN.search(candidate)
            if matched:
                return f"Bearer {matched.group(0)}"
        return None

    def _collect_storage_dump(self, tab: CDPTab) -> list[dict[str, Any]]:
        result = tab.evaluate(
            """
(() => {
    const collect = (scopeName, storage) => {
        const items = [];
        try {
            for (let index = 0; index < storage.length; index += 1) {
                const key = storage.key(index);
                items.push({scope: scopeName, key, value: storage.getItem(key)});
            }
        } catch (error) {
            items.push({scope: scopeName, key: '__error__', value: String(error)});
        }
        return items;
    };
    return [...collect('localStorage', window.localStorage), ...collect('sessionStorage', window.sessionStorage)];
})()
            """.strip()
        )
        return result or []

    def _poll_token_from_live_browser(self, tab: CDPTab) -> str | None:
        deadline = time.time() + LIVE_CHROME_TOKEN_WAIT_SECONDS
        urls = KEEPALIVE_URLS or [PARKING_REFERER_URL]
        while time.time() < deadline:
            for url in urls:
                self._navigate(tab, url)
                per_url_deadline = min(deadline, time.time() + 8)
                while time.time() < per_url_deadline:
                    storage_dump = self._collect_storage_dump(tab)
                    token = self._extract_bearer_token(storage_dump)
                    if token:
                        return token
                    time.sleep(1)
            try:
                tab.send("Page.reload", {"ignoreCache": False})
                self._wait_for_page_ready(tab, timeout_seconds=10)
            except Exception:
                pass
        return None

    def _refresh_profile_token_via_running_chrome(self) -> bool:
        self._ensure_chrome_running()
        self._hydrate_desktop_session_env()
        before_header = self._auth_header
        before_exp = self._auth_exp
        try:
            refreshed_header, live_cookies = self._resolve_authorization_from_live_chrome()
        except Exception as exc:  # noqa: BLE001
            self.logger("WARNING", f"parking live chrome refresh failed: {exc}")
            return False

        refreshed_exp = self._jwt_exp(refreshed_header.removeprefix("Bearer ").strip())
        if refreshed_header and (
            refreshed_header != before_header
            or (refreshed_exp and refreshed_exp > before_exp)
        ):
            self._auth_header = refreshed_header
            self._live_cookies = live_cookies
            self._auth_source = "chrome"
            self._auth_exp = refreshed_exp
            self.logger(
                "INFO",
                f"parking api auth refreshed via live chrome session exp={self._auth_exp}",
            )
            return True
        return False

    def _run_requests_keepalive_cycle(self) -> list[dict[str, Any]]:
        session = self._build_keepalive_session()
        page_statuses: list[dict[str, Any]] = []
        for url in KEEPALIVE_URLS:
            response = session.get(url, timeout=10, allow_redirects=True)
            page_status = {
                "url": url,
                "status_code": response.status_code,
                "final_url": response.url,
            }
            page_statuses.append(page_status)
            self.logger(
                "INFO",
                f"parking keepalive status={response.status_code} url={url} final_url={response.url}",
            )
        return page_statuses

    def _run_browser_keepalive_cycle(self) -> list[dict[str, Any]]:
        browser_client, tab, target_id = self._open_running_chrome_tab()
        try:
            page_statuses: list[dict[str, Any]] = []
            for url in KEEPALIVE_URLS:
                self._navigate(tab, url)
                final_url = str(
                    tab.evaluate("window.location.href || document.location.href || ''") or url
                )
                title = str(tab.evaluate("document.title || ''") or "")
                page_status = {
                    "url": url,
                    "status_code": 200,
                    "final_url": final_url,
                    "title": title,
                }
                page_statuses.append(page_status)
                self.logger(
                    "INFO",
                    f"parking browser keepalive status=200 url={url} final_url={final_url} title={title}",
                )

            storage_dump = self._collect_storage_dump(tab)
            token = self._extract_bearer_token(storage_dump)
            if token:
                token_exp = self._jwt_exp(token.removeprefix("Bearer ").strip())
                if token != self._auth_header or token_exp > self._auth_exp:
                    self._auth_header = token
                    self._auth_exp = token_exp
                    self._auth_source = "chrome"
                    self.logger("INFO", f"parking api auth refreshed from running chrome keepalive exp={token_exp}")
            return page_statuses
        finally:
            tab.close()
            self._close_running_chrome_target(browser_client, target_id)
            browser_client.close()

    def _run_keepalive_api_probe(self) -> dict[str, Any]:
        target_date = datetime.now(CST).date()
        start_leave_time, end_leave_time = _utc_range_for_local_date(target_date)
        headers = self._build_headers(authorization=self.ensure_authorization(), referer=PARKING_REFERER_URL)
        paging_header, rows = self._request_page(
            endpoint=PARKING_ENDPOINT,
            headers=headers,
            params={
                "status": "-1",
                "merchant": PARKING_MERCHANT_ID,
                "min_park_time": "0",
                "max_park_time": "0",
                "start_leave_time": start_leave_time,
                "end_leave_time": end_leave_time,
            },
            page_index=1,
            page_size=1,
            limiter=RateLimiter(0),
            label="parking_keepalive_probe",
        )
        probe = {
            "target_date": target_date.isoformat(),
            "sample_rows": len(rows),
            "total_count": int(paging_header.get("total_count") or 0),
            "page_count": int(paging_header.get("page_count") or 0),
        }
        self.logger("INFO", f"parking keepalive api probe success {json.dumps(probe, ensure_ascii=False)}")
        return probe

    def _build_keepalive_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "user-agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
            }
        )
        for cookie in self._live_cookies:
            session.cookies.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path or "/")
        return session

    def _keepalive_loop(self) -> None:
        while not self._keepalive_stop.wait(KEEPALIVE_INTERVAL_SECONDS):
            try:
                self.run_keepalive_cycle()
            except Exception as exc:  # noqa: BLE001
                self.logger("WARNING", f"parking keepalive failed: {exc}")

    def _start_keepalive_if_needed(self) -> None:
        if self._keepalive_thread is not None:
            return
        self._keepalive_stop.clear()
        self._keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self._keepalive_thread.start()
        self.logger("INFO", "parking chrome keepalive loop started")

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _resolve_running_chrome_debug_target(self) -> tuple[str, str]:
        managed_base_url = f"http://127.0.0.1:{MANAGED_CHROME_REMOTE_DEBUGGING_PORT}"
        try:
            response = requests.get(f"{managed_base_url}/json/version", timeout=1)
            response.raise_for_status()
            payload = response.json()
            websocket_url = str(payload.get("webSocketDebuggerUrl") or "")
            if websocket_url:
                return managed_base_url, websocket_url
        except Exception:
            pass

        candidates = [
            Path.home() / ".config" / "google-chrome" / "DevToolsActivePort",
            Path.home() / ".config" / "chromium" / "DevToolsActivePort",
        ]
        for candidate in candidates:
            if not candidate.is_file():
                continue
            lines = [line.strip() for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
            if len(lines) < 2:
                continue
            port = lines[0]
            browser_path = lines[1]
            base_url = f"http://127.0.0.1:{port}"
            for probe_path in ("/json/version", "/json/list"):
                try:
                    response = requests.get(f"{base_url}{probe_path}", timeout=1)
                    if response.status_code == 200:
                        return base_url, f"ws://127.0.0.1:{port}{browser_path}"
                except Exception:
                    continue
            raise RuntimeError("Running Chrome DevTools endpoint is unavailable.")
        raise RuntimeError("Running Chrome DevTools endpoint was not found.")

    def _open_running_chrome_tab(self) -> tuple[CDPTab, CDPTab, str]:
        base_url, browser_ws = self._resolve_running_chrome_debug_target()
        browser_client = CDPTab(browser_ws)
        result = browser_client.send("Target.createTarget", {"url": "about:blank", "background": True})
        target_id = str(result.get("targetId") or "")
        if not target_id:
            browser_client.close()
            raise RuntimeError("Running Chrome did not return a target id.")
        deadline = time.time() + 10
        websocket_url = ""
        while time.time() < deadline and not websocket_url:
            response = requests.get(f"{base_url}/json/list", timeout=3)
            response.raise_for_status()
            for item in response.json():
                if str(item.get("id") or "") == target_id and item.get("webSocketDebuggerUrl"):
                    websocket_url = str(item["webSocketDebuggerUrl"])
                    break
            if not websocket_url:
                time.sleep(0.2)
        if not websocket_url:
            self._close_running_chrome_target(browser_client, target_id)
            browser_client.close()
            raise RuntimeError("Running Chrome target websocket URL was not found.")
        return browser_client, CDPTab(websocket_url), target_id

    def _close_running_chrome_target(self, browser_client: CDPTab, target_id: str) -> None:
        try:
            browser_client.send("Target.closeTarget", {"targetId": target_id}, timeout=5)
        except Exception:
            pass

    def _wait_for_cdp_page(self, port: int, timeout_seconds: int = 15) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                response = requests.get(f"http://127.0.0.1:{port}/json/list", timeout=2)
                response.raise_for_status()
                for item in response.json():
                    if item.get("type") == "page" and item.get("webSocketDebuggerUrl"):
                        return str(item["webSocketDebuggerUrl"])
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError("Unable to connect to headless Chrome debugging endpoint.")

    def _launch_headless_browser(self, temp_user_data_dir: Path) -> tuple[subprocess.Popen[Any], CDPTab]:
        if not CHROME_PATH.exists():
            raise RuntimeError("Server Chrome executable was not found.")
        port = self._find_free_port()
        args = [
            str(CHROME_PATH),
            f"--remote-debugging-port={port}",
            f"--user-data-dir={temp_user_data_dir}",
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--no-default-browser-check",
            "--remote-allow-origins=*",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "about:blank",
        ]
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        websocket_url = self._wait_for_cdp_page(port)
        return process, CDPTab(websocket_url)

    def _navigate(self, tab: CDPTab, url: str) -> None:
        tab.send("Page.enable")
        tab.send("Runtime.enable")
        tab.send("Page.navigate", {"url": url})
        self._wait_for_page_ready(tab)

    def _wait_for_page_ready(self, tab: CDPTab, timeout_seconds: int = 30) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                if tab.evaluate("document.readyState") == "complete":
                    return
            except RuntimeError as exc:
                if "navigated or closed" not in str(exc).lower():
                    raise
            time.sleep(0.3)
        raise RuntimeError("Page load timed out.")

    def _inject_cookies(self, tab: CDPTab, cookies: list[Any]) -> None:
        tab.send("Network.enable")
        for cookie in cookies:
            domain = str(cookie.domain or "")
            if not domain:
                continue
            payload = {
                "name": cookie.name,
                "value": cookie.value,
                "domain": domain.lstrip("."),
                "path": cookie.path or "/",
                "secure": bool(getattr(cookie, "secure", False)),
                "url": f"https://{domain.lstrip('.')}{cookie.path or '/'}",
            }
            expires = getattr(cookie, "expires", None)
            if expires and expires > 0:
                payload["expires"] = float(expires)
            try:
                tab.send("Network.setCookie", payload)
            except Exception:
                continue
