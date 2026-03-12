from __future__ import annotations

import base64
import json
import math
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Sequence

import requests

from app.utils.excel_export import (
    DEFAULT_FIELD_FILES,
    _read_text_with_fallback,
    export_to_excel,
    format_cell_value,
    load_fields_from_sample,
)


ICSP_BASE = "https://icsp.scpgroup.com.cn"
ICSP_CLIENT_ID = "2a5c64fcf8cf475593350a6d11548711"
ICSP_SALT = "d0a8155e8e84e5832c3a908056737c2b"

PLAZA_CODE = "G002Z008C0030"
TENANT_ID = "10000"
ORG_TYPE_CODE = "10003"
PLAZA_BU_ID = 293

POINT_FLOW_URL = ICSP_BASE + "/icsp-point/web/point/water/flow/queryList"
PAGE_SIZE = 100
MAX_PAGE_WORKERS = 8
LoggerCallback = Callable[[str, str], None]
StopChecker = Callable[[], None]


@dataclass(slots=True)
class ExportJobResult:
    output_file: Path
    result_count: int


@dataclass(slots=True)
class ICSPAuthResult:
    username: str
    display_name: str
    user_id: str
    user_code: str
    auth_state: dict[str, Any]
class ICSPClient:
    def __init__(
        self,
        logger: LoggerCallback | None = None,
        stop_checker: StopChecker | None = None,
    ):
        self.logger = logger
        self.stop_checker = stop_checker
        self.session = requests.Session()
        self.user_info = {
            "userId": "",
            "userName": "",
            "userPhone": "",
            "userid": "",
            "usercode": "",
            "username": "",
        }
        self.last_login_error = ""
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Origin": ICSP_BASE,
                "Referer": ICSP_BASE + "/login.html",
            }
        )

    def log(self, level: str, message: str) -> None:
        if self.logger:
            try:
                self.logger(level, message)
            except Exception:
                return

    def check_stop(self) -> None:
        if self.stop_checker:
            self.stop_checker()

    @staticmethod
    def _make_password(plain_password: str) -> str:
        combined = (ICSP_SALT + plain_password).encode("utf-8")
        encoded = base64.b64encode(combined).decode()
        return f"{encoded}.{ICSP_SALT}"

    def _set_last_login_error(self, message: str) -> None:
        self.last_login_error = message
        self.log("WARN", message)

    def _cookie_keys(self) -> list[str]:
        return sorted({cookie.name for cookie in self.session.cookies if cookie.name})

    def _outgoing_cookie_keys_for(self, url: str) -> list[str]:
        prepared = self.session.prepare_request(requests.Request("GET", url))
        cookie_header = prepared.headers.get("Cookie", "")
        if not cookie_header:
            return []
        return [part.split("=", 1)[0].strip() for part in cookie_header.split(";") if part.strip()]

    @staticmethod
    def _mask_value(value: str, keep: int = 2) -> str:
        if not value:
            return ""
        if len(value) <= keep * 2:
            return "*" * len(value)
        return f"{value[:keep]}***{value[-keep:]}"

    def has_serializable_auth_state(self) -> bool:
        return any(cookie.name and cookie.value for cookie in self.session.cookies)

    def has_user_context(self) -> bool:
        return bool(self.user_info.get("userId"))

    def login(self, username: str, password: str) -> bool:
        self.check_stop()
        self.last_login_error = ""
        payload = {
            "clientId": ICSP_CLIENT_ID,
            "passwd": self._make_password(password),
            "user": username,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        self.log("INFO", f"[ICSP] logging in as {username}")
        try:
            auth_resp = self.session.post(
                ICSP_BASE + "/icsp-permission/web/permission/sso/auth/authCode",
                data=payload,
                headers=headers,
                allow_redirects=False,
                timeout=15,
            )
            self.log("INFO", f"[ICSP] authCode response status={auth_resp.status_code}")
            auth_code = ""
            if auth_resp.status_code == 302 and "Location" in auth_resp.headers:
                auth_code = auth_resp.headers["Location"].split("authCode=")[-1]
            elif auth_resp.status_code == 200:
                try:
                    body = auth_resp.json()
                except Exception:
                    self._set_last_login_error(f"ICSP 登录失败：未获取到 authCode，状态码={auth_resp.status_code}")
                    return False
                self.log("INFO", f"[ICSP] authCode response keys={sorted(body.keys()) if isinstance(body, dict) else []}")
                if body.get("success") and body.get("data"):
                    auth_code = str(body["data"])
                else:
                    detail = str(body.get("message") or body.get("msg") or "未获取到 authCode")
                    self._set_last_login_error(f"ICSP 登录失败：{detail}")
                    return False
            else:
                self._set_last_login_error(f"ICSP 登录失败：未获取到 authCode，状态码={auth_resp.status_code}")
                return False

            if not auth_code:
                self._set_last_login_error("ICSP 登录失败：authCode 为空")
                return False

            timestamp = str(int(time.time() * 1000))
            self.session.get(f"{ICSP_BASE}/auth.html?authCode={auth_code}", timeout=15)
            self.session.get(
                f"{ICSP_BASE}/icsp-permission/web/wd/login/login/sso?_t={timestamp}&authCode={auth_code}",
                timeout=15,
            )

            cookie_keys = self._cookie_keys()
            if not cookie_keys:
                self._set_last_login_error("[ICSP] login succeeded but cookies are empty")
                return False
            self.log("INFO", f"[ICSP] login succeeded and cookies acquired, cookie_keys={cookie_keys}")

            if self.probe_current_user(username):
                self.log("SUCCESS", "[ICSP] login succeeded")
                return True

            self.log("WARN", "[ICSP] login succeeded but current user context is still unavailable")
            self.log("SUCCESS", "[ICSP] login succeeded")
            return True
        except InterruptedError:
            raise
        except Exception as exc:
            self._set_last_login_error(f"ICSP 登录请求异常：{exc}")
            return False

    def query_current_user(self, login_username: str = "") -> bool:
        self.check_stop()
        timestamp = str(int(time.time() * 1000))
        url = f"{ICSP_BASE}/icsp-employee/web/login/query/v2?_t={timestamp}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Origin": ICSP_BASE,
            "Referer": ICSP_BASE + "/scpg.html",
        }
        try:
            self.log("INFO", f"[ICSP] current user query outgoing_cookie_keys={self._outgoing_cookie_keys_for(url)}")
            response = self.session.get(url, headers=headers, timeout=15, allow_redirects=False)
            self.log("INFO", f"[ICSP] current user query status={response.status_code}")
            if response.status_code == 302:
                self.log("WARN", "[ICSP] current user query redirected, session is not accepted")
                return False
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            payload_keys = sorted(payload.keys()) if isinstance(payload, dict) else []
            data_keys = sorted(data.keys()) if isinstance(data, dict) else []
            self.log("INFO", f"[ICSP] current user query payload_keys={payload_keys}, data_keys={data_keys}")

            if not isinstance(data, dict):
                return False

            user_id = str(data.get("userId", "")).strip()
            user_name = str(data.get("userName", "")).strip()
            user_phone = str(data.get("userPhone", "")).strip()
            if not user_id:
                self.log("WARN", "[ICSP] current user query returned no user id")
                return False

            self.user_info.update(
                {
                    "userId": user_id,
                    "userName": user_name or login_username,
                    "userPhone": user_phone,
                    "userid": user_id,
                    "usercode": user_phone or login_username,
                    "username": urllib.parse.quote(user_name or login_username),
                }
            )
            self.log(
                "INFO",
                "[ICSP] 成功提取 user context: userId=%s, userName=%s"
                % (
                    self._mask_value(user_id),
                    self._mask_value(user_name or login_username, keep=1),
                ),
            )
            return True
        except InterruptedError:
            raise
        except Exception as exc:
            self.log("WARN", f"[ICSP] failed to query current user info: {exc}")
            return False

    def probe_current_user(self, login_username: str = "", attempts: int = 8, delay_seconds: float = 1.0) -> bool:
        for attempt in range(1, attempts + 1):
            if self.query_current_user(login_username):
                if attempt > 1:
                    self.log("INFO", f"[ICSP] current user query succeeded on retry {attempt}/{attempts}")
                return True
            if attempt < attempts:
                self.log("INFO", f"[ICSP] current user query retry scheduled {attempt + 1}/{attempts}")
                time.sleep(delay_seconds)
        return False

    def ensure_authenticated_session(self, login_username: str = "") -> bool:
        return self.has_user_context() or self.probe_current_user(login_username)

    def probe_points_flow_access(self) -> bool:
        if not self.has_user_context():
            self.log("WARN", "[ICSP] points-flow probe skipped because user context is missing")
            return False
        day_str = datetime.now().strftime("%Y-%m-%d")
        payload = {
            "pageNo": 1,
            "pageSize": 1,
            "plazaBuId": PLAZA_BU_ID,
            "createStartTime": f"{day_str} 00:00:00",
            "createEndTime": f"{day_str} 23:59:59",
            "fromWeb": 1,
        }
        try:
            self.log("INFO", f"[ICSP] points-flow probe outgoing_cookie_keys={self._outgoing_cookie_keys_for(POINT_FLOW_URL)}")
            response = self.session.post(POINT_FLOW_URL, headers=self._api_headers(), json=payload, timeout=20)
            self.log("INFO", f"[ICSP] points-flow probe status={response.status_code}")
            response.raise_for_status()
            payload_json = response.json()
            payload_keys = sorted(payload_json.keys()) if isinstance(payload_json, dict) else []
            self.log("INFO", f"[ICSP] points-flow probe payload_keys={payload_keys}")
            if isinstance(payload_json, dict):
                if payload_json.get("success") is False:
                    return False
                if str(payload_json.get("code", "")) == "5000":
                    return False
            self.log("SUCCESS", "[ICSP] points-flow probe succeeded")
            return True
        except InterruptedError:
            raise
        except Exception as exc:
            self.log("WARN", f"[ICSP] points-flow probe failed: {exc}")
            return False

    def validate_authenticated_session(self, login_username: str = "") -> bool:
        self.log("INFO", "[ICSP] validating recovered authenticated session")
        cookie_keys = self._cookie_keys()
        if not cookie_keys:
            self.log("WARN", "[ICSP] validation failed: login succeeded but cookies are empty")
            return False
        self.log("INFO", f"[ICSP] validation using cookie_keys={cookie_keys}")
        self.log("INFO", f"[ICSP] validation outgoing_cookie_keys={self._outgoing_cookie_keys_for(ICSP_BASE + '/icsp-employee/web/login/query/v2')}")
        if not self.has_user_context() and not self.probe_current_user(login_username):
            self.log("WARN", "[ICSP] validation failed: missing user context and query/v2 did not recover it")
            return False
        if not self.probe_points_flow_access():
            self.log("WARN", "[ICSP] validation failed: recovered session could not access points-flow api")
            return False
        self.log("SUCCESS", "[ICSP] recovered session validation succeeded")
        return True

    def get_profile(self, login_username: str) -> dict[str, str]:
        display_name = self.user_info.get("userName", "") or urllib.parse.unquote(self.user_info.get("username", "")) or login_username
        return {
            "username": login_username,
            "display_name": display_name,
            "user_id": self.user_info.get("userId", "") or self.user_info.get("userid", ""),
            "user_code": self.user_info.get("userPhone", "") or self.user_info.get("usercode", "") or login_username,
        }

    def export_auth_state(self, login_username: str) -> dict[str, Any]:
        serialized_cookies = [
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": cookie.secure,
                "expires": cookie.expires,
            }
            for cookie in self.session.cookies
        ]
        self.log("INFO", f"[ICSP] exporting serializable auth state, cookie_keys={self._cookie_keys()}")
        return {
            "login_username": login_username,
            "authenticated_at": datetime.now().isoformat(),
            "user_info": {
                "userId": self.user_info.get("userId", ""),
                "userName": self.user_info.get("userName", ""),
                "userPhone": self.user_info.get("userPhone", ""),
            },
            "cookies": serialized_cookies,
        }

    @classmethod
    def from_auth_state(
        cls,
        auth_state: dict[str, Any],
        logger: LoggerCallback | None = None,
        stop_checker: StopChecker | None = None,
    ) -> "ICSPClient":
        client = cls(logger=logger, stop_checker=stop_checker)
        user_info = auth_state.get("user_info") or {}
        restored_user_id = str(user_info.get("userId", "")).strip()
        restored_user_name = str(user_info.get("userName", "")).strip()
        restored_user_phone = str(user_info.get("userPhone", "")).strip()
        client.user_info = {
            "userId": restored_user_id,
            "userName": restored_user_name,
            "userPhone": restored_user_phone,
            "userid": restored_user_id,
            "usercode": restored_user_phone,
            "username": urllib.parse.quote(restored_user_name),
        }
        for item in auth_state.get("cookies") or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            value = str(item.get("value", ""))
            if name and value:
                client.session.cookies.set(name, value)
        cookie_keys = client._cookie_keys()
        if cookie_keys:
            client.log("INFO", f"[ICSP] restored auth state from serialized cookies, cookie_keys={cookie_keys}")
        else:
            client.log("WARN", "[ICSP] restore failed: serialized auth state contains no cookies")
        return client

    def _api_headers(self) -> dict[str, str]:
        return {
            "plazacode": PLAZA_CODE,
            "orgcode": PLAZA_CODE,
            "orgtypecode": ORG_TYPE_CODE,
            "tenantid": TENANT_ID,
            "groupcode": "G001",
            "internalid": "1",
            "vunioncode": "U001",
            "workingorgcode": PLAZA_CODE,
            "userid": self.user_info["userid"],
            "usercode": self.user_info["usercode"],
            "username": self.user_info["username"],
            "Content-Type": "application/json;charset=utf-8",
            "Referer": ICSP_BASE + "/scpg.html",
            "Accept": "*/*",
            "accept-language": "zh-CN",
        }

    @staticmethod
    def _extract_rows_and_total(payload: dict) -> tuple[list[dict], int]:
        if not isinstance(payload, dict):
            return [], 0
        rows: list[dict] = []
        total = 0
        if isinstance(payload.get("rows"), list):
            rows = payload["rows"]
        elif isinstance(payload.get("list"), list):
            rows = payload["list"]
        elif isinstance(payload.get("resultList"), list):
            rows = payload["resultList"]
        elif isinstance(payload.get("data"), list):
            rows = payload["data"]
        elif isinstance(payload.get("data"), dict):
            nested = payload["data"]
            if isinstance(nested.get("rows"), list):
                rows = nested["rows"]
            elif isinstance(nested.get("list"), list):
                rows = nested["list"]
            elif isinstance(nested.get("resultList"), list):
                rows = nested["resultList"]
            elif isinstance(nested.get("records"), list):
                rows = nested["records"]
            total = int(nested.get("total") or nested.get("totalCount") or nested.get("totalSize") or 0)
        if not total:
            total = int(payload.get("total") or payload.get("totalCount") or payload.get("totalSize") or 0)
        if not total and rows:
            total = len(rows)
        return rows, total

    def _build_worker_session(self) -> requests.Session:
        worker_session = requests.Session()
        worker_session.headers.update(self.session.headers)
        worker_session.cookies.update(self.session.cookies)
        return worker_session

    def _fetch_point_page(
        self,
        page_no: int,
        start_date: str,
        end_date: str,
        session: requests.Session | None = None,
    ) -> tuple[list[dict], int]:
        self.check_stop()
        use_session = session or (self.session if page_no == 1 else self._build_worker_session())
        payload = {
            "pageNo": page_no,
            "pageSize": PAGE_SIZE,
            "plazaBuId": PLAZA_BU_ID,
            "createStartTime": f"{start_date} 00:00:00",
            "createEndTime": f"{end_date} 23:59:59",
            "fromWeb": 1,
        }
        response = use_session.post(POINT_FLOW_URL, headers=self._api_headers(), json=payload, timeout=20)
        response.raise_for_status()
        return self._extract_rows_and_total(response.json())

    def fetch_point_flow(self, start_date: str, end_date: str) -> list[dict]:
        first_rows, total = self._fetch_point_page(1, start_date, end_date)
        if not first_rows:
            self.log("WARN", "[points-flow] no data returned")
            return []
        all_rows = list(first_rows)
        self.log("INFO", f"[points-flow] first page rows={len(first_rows)}, total={total}")
        if total and total > PAGE_SIZE:
            total_pages = math.ceil(total / PAGE_SIZE)
            workers = min(MAX_PAGE_WORKERS, max(1, total_pages - 1))
            self.log("INFO", f"[points-flow] fetching remaining pages concurrently, pages={total_pages}, workers={workers}")
            tasks = list(range(2, total_pages + 1))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(self._fetch_point_page, page, start_date, end_date): page for page in tasks}
                done = 0
                for future in as_completed(futures):
                    self.check_stop()
                    page = futures[future]
                    try:
                        rows, _ = future.result()
                        if rows:
                            all_rows.extend(rows)
                    except Exception as exc:
                        self.log("WARN", f"[points-flow] page {page} failed: {exc}")
                    done += 1
                    if done % 5 == 0 or done == len(futures):
                        self.log("INFO", f"[points-flow] completed pages={done}/{len(futures)}, rows={len(all_rows)}")
        else:
            page_no = 2
            last_size = len(first_rows)
            while last_size >= PAGE_SIZE and page_no <= 2000:
                self.check_stop()
                rows, _ = self._fetch_point_page(page_no, start_date, end_date)
                if not rows:
                    break
                all_rows.extend(rows)
                last_size = len(rows)
                self.log("INFO", f"[points-flow] fetched page {page_no}, rows={len(all_rows)}")
                page_no += 1
        self.log("SUCCESS", f"[points-flow] fetch completed, total rows={len(all_rows)}")
        return all_rows


def run_points_flow_export(
    username: str,
    password: str,
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    logger: LoggerCallback | None = None,
    stop_checker: StopChecker | None = None,
    file_tag: str | None = None,
) -> ExportJobResult:
    if logger:
        logger("INFO", "Starting login to ICSP.")
    client = ICSPClient(logger=logger, stop_checker=stop_checker)
    if not client.login(username, password):
        raise RuntimeError(client.last_login_error or "ICSP 登录失败，请检查账号或密码。")
    if not client.ensure_authenticated_session(username):
        raise RuntimeError("ICSP 登录成功，但未能获取用户上下文，请稍后重试。")
    if logger:
        logger("INFO", "Starting points flow data fetch.")
    rows = client.fetch_point_flow(start_date, end_date)
    if logger:
        logger("INFO", "Starting Excel export.")
    output_file = export_to_excel(
        rows=rows,
        fields=load_fields_from_sample(),
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_tag=file_tag,
    )
    if logger:
        logger("SUCCESS", f"Excel export completed: {output_file.name}")
    return ExportJobResult(output_file=output_file, result_count=len(rows))


def authenticate_icsp_user(
    username: str,
    password: str,
    logger: LoggerCallback | None = None,
) -> ICSPAuthResult:
    client = ICSPClient(logger=logger)
    if not client.login(username, password):
        raise RuntimeError(client.last_login_error or "ICSP 登录失败，请检查账号或密码。")
    if not client.has_serializable_auth_state():
        client.log("WARN", "[ICSP] reusable session build failed: login succeeded but cookies are empty")
        raise RuntimeError("ICSP 登录成功，但未获取到可复用的认证信息，请稍后重试。")
    if not client.ensure_authenticated_session(username):
        client.log("WARN", "[ICSP] reusable session build failed: original login session has no user context")
        raise RuntimeError("ICSP 登录成功，但未能建立可复用会话，请稍后重试。")

    serialized_auth_state = client.export_auth_state(username)
    recovered_client = ICSPClient.from_auth_state(serialized_auth_state, logger=logger)
    if not recovered_client.has_serializable_auth_state():
        recovered_client.log("WARN", "[ICSP] reusable session build failed: cookies restore failed")
        raise RuntimeError("ICSP 登录成功，但 cookies 恢复失败，请稍后重试。")
    if not recovered_client.validate_authenticated_session(username):
        recovered_client.log("WARN", "[ICSP] reusable session build failed: recovered session validation failed")
        raise RuntimeError("ICSP 登录成功，但未能建立可复用会话，请稍后重试。")

    profile = recovered_client.get_profile(username)
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=recovered_client.export_auth_state(profile["username"]),
    )


def run_points_flow_export_with_auth_state(
    auth_state: dict[str, Any],
    start_date: str,
    end_date: str,
    output_dir: str | Path,
    logger: LoggerCallback | None = None,
    stop_checker: StopChecker | None = None,
    file_tag: str | None = None,
) -> ExportJobResult:
    client = ICSPClient.from_auth_state(auth_state, logger=logger, stop_checker=stop_checker)
    login_username = str(auth_state.get("login_username", "")).strip()
    if not client.validate_authenticated_session(login_username):
        raise RuntimeError("登录失效，请重新登录")
    if logger:
        logger("INFO", "Using authenticated ICSP session.")
        logger("INFO", "Starting points flow data fetch.")
    rows = client.fetch_point_flow(start_date, end_date)
    if logger:
        logger("INFO", "Starting Excel export.")
    output_file = export_to_excel(
        rows=rows,
        fields=load_fields_from_sample(),
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_tag=file_tag,
    )
    if logger:
        logger("SUCCESS", f"Excel export completed: {output_file.name}")
    return ExportJobResult(output_file=output_file, result_count=len(rows))


def refresh_icsp_auth_state(auth_state: dict[str, Any], logger: LoggerCallback | None = None) -> ICSPAuthResult:
    login_username = str(auth_state.get("login_username", "")).strip()
    client = ICSPClient.from_auth_state(auth_state, logger=logger)
    if not client.validate_authenticated_session(login_username):
        raise RuntimeError("登录失效，请重新登录")
    profile = client.get_profile(login_username or client.user_info.get("usercode", ""))
    refreshed_auth_state = client.export_auth_state(profile["username"])
    return ICSPAuthResult(
        username=profile["username"],
        display_name=profile["display_name"],
        user_id=profile["user_id"],
        user_code=profile["user_code"],
        auth_state=refreshed_auth_state,
    )


class PointsFlowExportService:
    def authenticate_user(
        self,
        username: str,
        password: str,
        log_callback: LoggerCallback | None = None,
    ) -> ICSPAuthResult:
        if log_callback:
            log_callback("INFO", "Starting ICSP login verification.")
        return authenticate_icsp_user(username=username, password=password, logger=log_callback)

    def refresh_authenticated_session(
        self,
        auth_state: dict[str, Any],
        log_callback: LoggerCallback | None = None,
    ) -> ICSPAuthResult:
        return refresh_icsp_auth_state(auth_state=auth_state, logger=log_callback)

    def run_export(
        self,
        task_id: str,
        auth_state: dict[str, Any],
        start_date: str,
        end_date: str,
        export_dir: str | Path,
        log_callback: LoggerCallback | None = None,
    ) -> ExportJobResult:
        return run_points_flow_export_with_auth_state(
            auth_state=auth_state,
            start_date=start_date,
            end_date=end_date,
            output_dir=export_dir,
            logger=log_callback,
            file_tag=task_id[:8] if task_id else None,
        )
