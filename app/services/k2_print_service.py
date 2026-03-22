from __future__ import annotations

import base64
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode, urlparse

import requests
from bs4 import BeautifulSoup
from fastapi import HTTPException, status
from requests.cookies import RequestsCookieJar
from websocket import create_connection

from app.schemas import K2PrintJobLogResponse, K2PrintJobStartResponse, K2PrintJobStatusResponse
from app.services.pdf_tools_service import K2_PRINT_CACHE_ROOT, PdfToolsService


BPM_LOGIN_URL = "https://wf.scpgroup.com:81/Login.aspx"
BPM_VIEWFLOW_URL = "https://wf.scpgroup.com:81/Manage/ViewFlowList.aspx?pageId=sys_Manage"
BPM_USERNAME = "h-shirl01"
BPM_PASSWORD = "sic888888@@@"
K2_QUERY_LOOKBACK_DAYS = 800
K2_COOKIE_REFRESH_SECONDS = 45
BPM_KEEPALIVE_INTERVAL_SECONDS = int(os.getenv("BPM_KEEPALIVE_SECONDS", "45"))
K2_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
CHROME_PATH = Path("/usr/bin/google-chrome")
DEFAULT_CHROME_PROFILE_DIR = Path.home() / ".config" / "google-chrome"
SHARED_MANAGED_CHROME_PROFILE_DIR = os.getenv(
    "PARKING_MANAGED_CHROME_PROFILE_DIR",
    str(Path.home() / ".config" / "google-chrome-4pyun"),
)
BPM_MANAGED_CHROME_PROFILE_DIR = Path(
    os.getenv("BPM_MANAGED_CHROME_PROFILE_DIR", SHARED_MANAGED_CHROME_PROFILE_DIR)
)
BPM_MANAGED_CHROME_REMOTE_DEBUGGING_PORT = int(
    os.getenv("BPM_MANAGED_CHROME_PORT", os.getenv("PARKING_MANAGED_CHROME_PORT", "9223"))
)
BPM_MANAGED_CHROME_COOKIE_FILE = BPM_MANAGED_CHROME_PROFILE_DIR / "Default" / "Cookies"
BPM_MANAGED_CHROME_KEY_FILE = BPM_MANAGED_CHROME_PROFILE_DIR / "Local State"
BPM_KEEPALIVE_URLS = [
    value.strip()
    for value in os.getenv(
        "BPM_KEEPALIVE_URLS",
        "https://wf.scpgroup.com/Workflow/MTApprovalView.aspx?procInstID=26070203&key=7639696416",
    ).split(",")
    if value.strip()
]
K2_JOB_LOCK = threading.Lock()
K2_JOB_STATES: dict[str, dict[str, Any]] = {}


def _noop_logger(level: str, message: str) -> None:
    return None


def utcnow() -> datetime:
    return datetime.utcnow()


def iso_now() -> str:
    return utcnow().isoformat() + "Z"


def sanitize_file_component(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {".", "-", "_"} else "_" for char in value.strip())
    return cleaned or "value"


def build_print_url(workflow_url: str) -> str | None:
    parsed = urlparse(workflow_url)
    query = dict(item.split("=", 1) for item in parsed.query.split("&") if "=" in item)
    proc_inst_id = query.get("procInstID") or query.get("ProcInstID")
    key = query.get("key") or query.get("Key")
    if not proc_inst_id or not key:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/Print/Print.aspx?ProcInstID={proc_inst_id}&key={key}&ExecuteType=Execute"


def curl_binary() -> str:
    for candidate in ("curl", "curl.exe"):
        path = shutil.which(candidate)
        if path:
            return path
    raise RuntimeError("curl binary is required for K2 BPM query")


@dataclass
class CurlResponse:
    effective_url: str
    status_code: int
    headers_text: str
    body_text: str


class CDPTab:
    def __init__(self, websocket_url: str) -> None:
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
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True,
            },
            timeout=timeout,
        )
        return result.get("result", {}).get("value")


class K2PrintService:
    def __init__(self, logger: Callable[[str, str], None] | None = None) -> None:
        self.logger = logger or _noop_logger
        PdfToolsService().ensure_daily_cleanup()
        K2_PRINT_CACHE_ROOT.mkdir(parents=True, exist_ok=True)

    def start_job(self, k2_no: str) -> K2PrintJobStartResponse:
        normalized = k2_no.strip()
        if not normalized:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="K2 号不能为空")

        PdfToolsService().ensure_daily_cleanup()
        job_id = f"k2_{utcnow().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}"
        session_dir = K2_PRINT_CACHE_ROOT / job_id
        session_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "job_id": job_id,
            "k2_no": normalized,
            "status": "queued",
            "stage": "queued",
            "started_at": None,
            "finished_at": None,
            "resolved_workflow_url": None,
            "resolved_print_url": None,
            "download_url": None,
            "error": None,
            "logs": [],
        }
        with K2_JOB_LOCK:
            K2_JOB_STATES[job_id] = state

        worker = threading.Thread(target=self._run_job, args=(job_id, normalized, session_dir), daemon=True)
        worker.start()
        return K2PrintJobStartResponse(job_id=job_id, status="queued")

    def get_job(self, job_id: str) -> K2PrintJobStatusResponse:
        PdfToolsService().ensure_daily_cleanup()
        with K2_JOB_LOCK:
            payload = K2_JOB_STATES.get(job_id)
            if payload is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="K2 打印任务不存在")
            data = json.loads(json.dumps(payload))
        return K2PrintJobStatusResponse(**data)

    def resolve_download_file(self, job_id: str, file_name: str) -> tuple[Path, str]:
        return PdfToolsService().resolve_download_file(K2_PRINT_CACHE_ROOT, job_id, file_name)

    def _run_job(self, job_id: str, k2_no: str, session_dir: Path) -> None:
        cookie_jar = session_dir / "bpm_query.cookies.txt"
        keepalive_stop = threading.Event()
        keepalive_thread: threading.Thread | None = None

        try:
            self._update_state(job_id, status="running", stage="resolve_workflow_url", started_at=iso_now())
            self._log(job_id, "resolve_workflow_url", "info", "开始解析 K2 流程地址")
            workflow_url = self._resolve_workflow_url(k2_no, cookie_jar, session_dir)
            print_url = build_print_url(workflow_url)
            if not print_url:
                raise RuntimeError("无法从流程地址解析打印地址")

            self._update_state(
                job_id,
                stage="workflow_url_resolved",
                resolved_workflow_url=workflow_url,
                resolved_print_url=print_url,
            )
            self._log(job_id, "workflow_url_resolved", "success", f"已解析完整流程地址：{workflow_url}")
            self._log(job_id, "workflow_url_resolved", "info", f"已解析打印地址：{print_url}")

            self._update_state(job_id, stage="load_live_cookies")
            live_cookies = self._collect_live_cookies()
            self._log(job_id, "load_live_cookies", "success", f"已读取服务器 Chrome 登录态 Cookie：{len(live_cookies)} 条")

            self._update_state(job_id, stage="keepalive")
            self._touch_live_session(workflow_url, print_url, live_cookies, session_dir)
            keepalive_thread = threading.Thread(
                target=self._keepalive_loop,
                args=(workflow_url, print_url, live_cookies, keepalive_stop, job_id),
                daemon=True,
            )
            keepalive_thread.start()
            self._log(job_id, "keepalive", "info", "已启动后台会话保活")

            self._update_state(job_id, stage="export_pdf")
            output_name = f"K2审批完整记录_{sanitize_file_component(k2_no)}_{utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
            output_path = session_dir / "downloads" / output_name
            rows_count = self._export_pdf_with_live_cookies(job_id, workflow_url, print_url, live_cookies, output_path)
            download_url = f"/api/tools/k2-print/jobs/{job_id}/downloads/{output_name}"

            self._log(job_id, "export_pdf", "success", f"PDF 导出成功，共捕获 {rows_count} 行审批记录")
            self._update_state(
                job_id,
                status="success",
                stage="success",
                download_url=download_url,
                error=None,
                finished_at=iso_now(),
            )
        except Exception as exc:
            self._log(job_id, "failed", "error", str(exc))
            self._update_state(job_id, status="failed", stage="failed", error=str(exc), finished_at=iso_now())
        finally:
            keepalive_stop.set()
            if keepalive_thread is not None:
                keepalive_thread.join(timeout=2)
            self._persist_state(job_id, session_dir)

    def _resolve_workflow_url(self, k2_no: str, cookie_jar: Path, session_dir: Path) -> str:
        login_page = self._curl_fetch(BPM_LOGIN_URL, cookie_jar, session_dir / "01_login_page.html")
        login_fields = self._parse_form_fields(login_page.body_text)
        login_payload = {
            "__LASTFOCUS": "",
            "__EVENTTARGET": "LoginIn",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": login_fields.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": login_fields.get("__VIEWSTATEGENERATOR", ""),
            "txtUserId": BPM_USERNAME,
            "txtPwd": BPM_PASSWORD,
        }
        self._curl_fetch(
            BPM_LOGIN_URL,
            cookie_jar,
            session_dir / "02_login_result.html",
            data=urlencode(login_payload),
            headers=["Content-Type: application/x-www-form-urlencoded"],
        )

        view_page = self._curl_fetch(BPM_VIEWFLOW_URL, cookie_jar, session_dir / "03_view_flow.html")
        soup = BeautifulSoup(view_page.body_text, "html.parser")
        fields = self._parse_form_fields(view_page.body_text)
        org_option = soup.select_one("#ctl00_contentPlace_ddlOrg option[selected]") or soup.select_one("#ctl00_contentPlace_ddlOrg option")
        org_value = org_option.get("value", "") if org_option else ""
        root_value = fields.get("ctl00$contentPlace$RootID") or org_value
        start_date = (date.today() - timedelta(days=K2_QUERY_LOOKBACK_DAYS)).strftime("%Y-%m-%d 00:00:00")
        end_date = date.today().strftime("%Y-%m-%d 23:59:59")
        query_payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": fields.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": fields.get("__VIEWSTATEGENERATOR", ""),
            "__VIEWSTATEENCRYPTED": fields.get("__VIEWSTATEENCRYPTED", ""),
            "ctl00$Header21$hdLan": fields.get("ctl00$Header21$hdLan", ""),
            "ctl00$contentPlace$ddlOrg": org_value,
            "ctl00$contentPlace$ddlDepartment": "",
            "ctl00$contentPlace$ddlDepartmentID": "",
            "ctl00$contentPlace$RootID": root_value,
            "ctl00$contentPlace$txtUsers": "",
            "ctl00$contentPlace$txtUsersID": "",
            "ctl00$contentPlace$btnSearch": "查询",
            "ctl00$contentPlace$dropParProcessCategory": "",
            "ctl00$contentPlace$dropSubProcess": "",
            "ctl00$contentPlace$hidSubProcess": "",
            "ctl00$contentPlace$txtProcessName": "",
            "ctl00$contentPlace$txtProInstID": k2_no,
            "ctl00$contentPlace$txtAmount": "",
            "ctl00$contentPlace$txtTopic": "",
            "ctl00$contentPlace$txtApprover": "",
            "ctl00$contentPlace$txtApproverID": "",
            "ctl00$contentPlace$ddlApproval": "1",
            "ctl00$contentPlace$txtStartDateFrom": start_date,
            "ctl00$contentPlace$txtStartDateTo": end_date,
            "ctl00_contentPlace_MainGrid_changepagesize": "10",
            "ctl00$contentPlace$MainGrid": "",
            "ctl00$contentPlace$HiddenField1": "",
            "ctl00$contentPlace$HiddenField2": "0",
        }
        result_page = self._curl_fetch(
            BPM_VIEWFLOW_URL,
            cookie_jar,
            session_dir / f"04_query_result_{sanitize_file_component(k2_no)}.html",
            data=urlencode(query_payload),
            headers=["Content-Type: application/x-www-form-urlencoded"],
        )
        result_soup = BeautifulSoup(result_page.body_text, "html.parser")
        for anchor in result_soup.select("a[href]"):
            href = anchor.get("href", "").strip()
            if f"procInstID={k2_no}" in href and "/Workflow/" in href:
                return href
        raise RuntimeError(f"未在 BPM 查询结果中找到 K2 号 {k2_no} 对应的完整流程地址")

    def _ensure_chrome_running(self) -> None:
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
            if "/chrome" in cmdline or "google-chrome" in cmdline:
                return
        raise RuntimeError("未检测到服务器 Chrome 正在运行，请先在服务器打开 Chrome 并保持目标系统登录态后重试。")

    def _collect_live_cookies(self) -> list[Any]:
        import browser_cookie3

        self._hydrate_desktop_session_env()
        cookies: list[Any] = []
        seen: set[tuple[str, str, str]] = set()
        sources: list[tuple[Path | None, Path | None]] = []
        if BPM_MANAGED_CHROME_COOKIE_FILE.is_file() and BPM_MANAGED_CHROME_KEY_FILE.is_file():
            sources.append((BPM_MANAGED_CHROME_COOKIE_FILE, BPM_MANAGED_CHROME_KEY_FILE))
        sources.append((None, None))
        for domain in ("wf.scpgroup.com", ".scpgroup.com", "jauth.scpgroup.com.cn"):
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
            raise RuntimeError("未读取到服务器 Chrome 登录态 Cookie，请先在服务器 Chrome 中打开并登录流程详情页后重试。")
        return cookies

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
            for raw_item in env_blob:
                if b"=" not in raw_item:
                    continue
                key_bytes, value_bytes = raw_item.split(b"=", 1)
                key = key_bytes.decode("utf-8", "ignore")
                if key not in wanted:
                    continue
                value = value_bytes.decode("utf-8", "ignore")
                if value and not os.getenv(key):
                    os.environ[key] = value
                    loaded += 1
            if loaded > 0 and os.getenv("DBUS_SESSION_BUS_ADDRESS"):
                return

    def _build_requests_session(self, cookies: list[Any]) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": K2_USER_AGENT})
        jar = RequestsCookieJar()
        for cookie in cookies:
            jar.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path)
        session.cookies = jar
        return session

    def _is_bpm_login_redirect_url(self, url: str) -> bool:
        lowered = str(url or "").lower()
        return "login.aspx" in lowered or "redirecturl=" in lowered

    def _touch_live_session(self, workflow_url: str, print_url: str, cookies: list[Any], session_dir: Path) -> None:
        session = self._build_requests_session(cookies)
        for index, url in enumerate((workflow_url, print_url), start=1):
            response = session.get(url, allow_redirects=True, timeout=30, verify=False)
            (session_dir / f"keepalive_{index}.html").write_text(response.text, encoding="utf-8", errors="replace")
            if response.status_code >= 400:
                raise RuntimeError(f"Keepalive failed: {url} -> HTTP {response.status_code}")
            if self._is_bpm_login_redirect_url(response.url):
                raise RuntimeError("Current Chrome login session is invalid or has been kicked out. Please log in again and retry.")

    def _keepalive_loop(
        self,
        workflow_url: str,
        print_url: str,
        cookies: list[Any],
        stop_event: threading.Event,
        job_id: str,
    ) -> None:
        session = self._build_requests_session(cookies)
        while not stop_event.wait(K2_COOKIE_REFRESH_SECONDS):
            try:
                for url in (workflow_url, print_url):
                    response = session.get(url, allow_redirects=True, timeout=30, verify=False)
                    if response.status_code >= 400:
                        self._log(job_id, "keepalive", "warning", f"Keepalive failed: {url} -> HTTP {response.status_code}")
                        return
                    if self._is_bpm_login_redirect_url(response.url):
                        self._log(job_id, "keepalive", "warning", "Current Chrome login session is invalid or has been kicked out.")
                        return
            except Exception as exc:  # noqa: BLE001
                self._log(job_id, "keepalive", "warning", f"Keepalive exception: {exc}")
                return

    def _cookie_origin(self, domain: str) -> str:
        normalized = domain.lstrip(".")
        return f"https://{normalized}"

    def _inject_cookies(self, tab: CDPTab, cookies: list[Any]) -> None:
        tab.send("Network.enable")
        for cookie in cookies:
            domain = (cookie.domain or "").lstrip(".")
            if not domain:
                continue
            payload: dict[str, Any] = {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain or domain,
                "path": cookie.path or "/",
                "secure": bool(getattr(cookie, "secure", False)),
                "url": self._cookie_origin(domain),
            }
            expires = getattr(cookie, "expires", None)
            if expires and expires > 0:
                payload["expires"] = float(expires)
            try:
                tab.send("Network.setCookie", payload)
            except Exception:
                continue

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _resolve_running_chrome_debug_target(self) -> tuple[str, str]:
        managed_base_url = f"http://127.0.0.1:{BPM_MANAGED_CHROME_REMOTE_DEBUGGING_PORT}"
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
            DEFAULT_CHROME_PROFILE_DIR / "DevToolsActivePort",
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

    def _wait_for_page_ready(self, tab: CDPTab, timeout_seconds: int = 30) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            ready_state = tab.evaluate("document.readyState")
            if ready_state == "complete":
                return
            time.sleep(0.3)
        raise RuntimeError("Page load timed out.")

    def _navigate(self, tab: CDPTab, url: str) -> None:
        tab.send("Page.enable")
        tab.send("Runtime.enable")
        tab.send("Page.navigate", {"url": url})
        self._wait_for_page_ready(tab)

    def _navigate_running_browser(self, tab: CDPTab, url: str) -> tuple[str, str]:
        self._navigate(tab, url)
        final_url = str(tab.evaluate("location.href", timeout=10) or "")
        title = str(tab.evaluate("document.title", timeout=10) or "")
        return final_url, title

    def _login_running_browser(self, tab: CDPTab) -> tuple[str, str]:
        self._navigate(tab, BPM_LOGIN_URL)
        login_script = f"""
(() => {{
    const user = document.querySelector('#txtUserId, input[name="txtUserId"]');
    const pwd = document.querySelector('#txtPwd, input[name="txtPwd"]');
    if (!user || !pwd) return 'missing_fields';
    user.focus();
    user.value = {json.dumps(BPM_USERNAME)};
    user.dispatchEvent(new Event('input', {{ bubbles: true }}));
    user.dispatchEvent(new Event('change', {{ bubbles: true }}));
    pwd.focus();
    pwd.value = {json.dumps(BPM_PASSWORD)};
    pwd.dispatchEvent(new Event('input', {{ bubbles: true }}));
    pwd.dispatchEvent(new Event('change', {{ bubbles: true }}));
    const loginButton = document.querySelector('#LoginIn, input[name="LoginIn"], button[name="LoginIn"]');
    if (loginButton) {{
        loginButton.click();
        return 'clicked';
    }}
    const form = user.form || document.querySelector('form');
    if (!form) return 'missing_form';
    let eventTarget = form.querySelector('input[name="__EVENTTARGET"]');
    if (!eventTarget) {{
        eventTarget = document.createElement('input');
        eventTarget.type = 'hidden';
        eventTarget.name = '__EVENTTARGET';
        form.appendChild(eventTarget);
    }}
    eventTarget.value = 'LoginIn';
    form.submit();
    return 'submitted';
}})()
"""
        action = str(tab.evaluate(login_script, timeout=15) or "")
        if action in {"missing_fields", "missing_form"}:
            raise RuntimeError(f"BPM browser login page is missing expected fields: {action}")
        deadline = time.time() + 45
        final_url = BPM_LOGIN_URL
        title = ""
        while time.time() < deadline:
            try:
                self._wait_for_page_ready(tab, timeout_seconds=10)
            except Exception:
                pass
            final_url = str(tab.evaluate("location.href", timeout=10) or "")
            title = str(tab.evaluate("document.title", timeout=10) or "")
            if not self._is_bpm_login_redirect_url(final_url):
                self.logger("INFO", f"bpm browser re-login success title={title} url={final_url}")
                return final_url, title
            time.sleep(1)
        raise RuntimeError(f"BPM browser login did not reach authenticated page: {final_url}")

    def _keepalive_browser_pages(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for url in BPM_KEEPALIVE_URLS:
            browser_client: CDPTab | None = None
            tab: CDPTab | None = None
            target_id: str | None = None
            last_error: Exception | None = None
            try:
                for attempt in range(2):
                    try:
                        browser_client, tab, target_id = self._open_running_chrome_tab()
                        final_url, title = self._navigate_running_browser(tab, url)
                        if self._is_bpm_login_redirect_url(final_url):
                            self.logger("WARNING", f"bpm browser keepalive redirected to login, attempting re-auth url={final_url}")
                            self._login_running_browser(tab)
                            final_url, title = self._navigate_running_browser(tab, url)
                        record = {"url": url, "final_url": final_url, "title": title}
                        results.append(record)
                        if self._is_bpm_login_redirect_url(final_url):
                            raise RuntimeError(f"BPM keepalive browser session is invalid: {final_url}")
                        self.logger("INFO", f"bpm browser keepalive title={title} url={final_url}")
                        last_error = None
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        self.logger("WARNING", f"bpm browser keepalive attempt={attempt + 1} failed url={url}: {exc}")
                        time.sleep(1)
                    finally:
                        if tab is not None:
                            tab.close()
                            tab = None
                        if browser_client is not None and target_id:
                            self._close_running_chrome_target(browser_client, target_id)
                            browser_client.close()
                            browser_client = None
                            target_id = None
                if last_error is not None:
                    raise last_error
            finally:
                if tab is not None:
                    tab.close()
                if browser_client is not None and target_id:
                    self._close_running_chrome_target(browser_client, target_id)
                    browser_client.close()
        return results

    def _probe_live_session(self, cookies: list[Any]) -> dict[str, Any]:
        session = self._build_requests_session(cookies)
        probe_url = BPM_KEEPALIVE_URLS[0] if BPM_KEEPALIVE_URLS else BPM_VIEWFLOW_URL
        response = session.get(probe_url, allow_redirects=True, timeout=30, verify=False)
        title = ""
        try:
            title = str(BeautifulSoup(response.text, "html.parser").title.string or "").strip()
        except Exception:
            title = ""
        result = {
            "status_code": int(response.status_code),
            "final_url": response.url,
            "title": title,
            "probe_url": probe_url,
        }
        if response.status_code >= 400:
            raise RuntimeError(f"BPM keepalive probe failed: HTTP {response.status_code}")
        if self._is_bpm_login_redirect_url(response.url):
            raise RuntimeError(f"BPM keepalive probe redirected to login: {response.url}")
        self.logger("INFO", f"bpm keepalive probe success status={response.status_code} title={title} url={response.url}")
        return result

    def run_keepalive_cycle(self) -> dict[str, Any]:
        pages: list[dict[str, Any]] = []
        browser_error: str | None = None
        try:
            pages = self._keepalive_browser_pages()
        except Exception as exc:  # noqa: BLE001
            browser_error = str(exc)
            self.logger("WARNING", f"bpm browser keepalive degraded: {browser_error}")
        cookies = self._collect_live_cookies()
        probe = self._probe_live_session(cookies)
        result = {
            "cookie_count": len(cookies),
            "pages": pages,
            "probe": probe,
        }
        if browser_error:
            result["browser_error"] = browser_error
        return result

    def _wait_for_source_table(self, tab: CDPTab, timeout_seconds: int = 60) -> tuple[str, int]:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            row_count = tab.evaluate("(() => document.querySelector('#fullApprovalHistory table')?.rows?.length || 0)()", timeout=10)
            if isinstance(row_count, (int, float)) and int(row_count) > 0:
                table_html = tab.evaluate("(() => document.querySelector('#fullApprovalHistory table')?.outerHTML || '')()", timeout=10)
                if table_html:
                    return str(table_html), int(row_count)
            tab.evaluate(
                "(() => { if (typeof showFullHistory === 'function') { showFullHistory(); return true; } return false; })()",
                timeout=10,
            )
            time.sleep(1)
        raise RuntimeError("Timed out waiting for the full approval history table.")

    def _wait_for_print_table(self, tab: CDPTab, timeout_seconds: int = 15) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            exists = tab.evaluate("(() => !!document.querySelector('#approvalHistoryCt table'))()", timeout=10)
            if exists:
                return
            time.sleep(0.5)
        raise RuntimeError("Print page table is not ready yet.")

    def _replace_table_and_print(self, tab: CDPTab, table_html: str, print_url: str, output_path: Path) -> None:
        self._navigate(tab, print_url)
        self._wait_for_print_table(tab, timeout_seconds=15)
        replace_script = f"""
(() => {{
    const newHtml = {json.dumps(table_html)};
    const styleId = 'codex-k2-print-override';
    let styleTag = document.getElementById(styleId);
    if (!styleTag) {{
        styleTag = document.createElement('style');
        styleTag.id = styleId;
        styleTag.textContent = `
            html, body, .printMain, .card, .card-print {{
                background: #ffffff !important;
                background-image: none !important;
                box-shadow: none !important;
                filter: none !important;
            }}
            .noprint, .btnPrint {{
                display: none !important;
            }}
            @page {{
                margin: 10mm;
                size: A4;
            }}
        `;
        document.head.appendChild(styleTag);
    }}
    const target = document.querySelector('#approvalHistoryCt table');
    if (!target || !target.parentNode) return false;
    const wrapper = document.createElement('div');
    wrapper.innerHTML = newHtml;
    target.parentNode.replaceChild(wrapper, target);
    return true;
}})()
"""
        replaced = tab.evaluate(replace_script, timeout=10)
        if not replaced:
            raise RuntimeError("Failed to replace the approval table on the print page.")

        result = tab.send(
            "Page.printToPDF",
            {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "preferCSSPageSize": False,
                # Match the real system-exported PDF page size captured from a
                # browser print workflow, instead of falling back to Letter.
                "paperWidth": 594.96 / 72,
                "paperHeight": 841.92 / 72,
                "scale": 1,
                # Keep a browser-like printable margin so the PDF layout stays
                # close to the manually exported system PDF.
                "marginTop": 1 / 2.54,
                "marginBottom": 1 / 2.54,
                "marginLeft": 1 / 2.54,
                "marginRight": 1 / 2.54,
            },
            timeout=60,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(base64.b64decode(result["data"]))

    def _export_pdf_with_live_cookies(
        self,
        job_id: str,
        workflow_url: str,
        print_url: str,
        cookies: list[Any],
        output_path: Path,
    ) -> int:
        runtime_dir = output_path.parent / "_runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        temp_user_data_dir = Path(tempfile.mkdtemp(prefix="k2_browser_", dir=runtime_dir))

        process: subprocess.Popen[Any] | None = None
        tab: CDPTab | None = None
        try:
            process, tab = self._launch_headless_browser(temp_user_data_dir)
            self._inject_cookies(tab, cookies)
            self._navigate(tab, workflow_url)
            table_html, rows_count = self._wait_for_source_table(tab, timeout_seconds=60)
            if rows_count <= 0:
                raise RuntimeError("Failed to capture the full approval history table.")

            last_error: Exception | None = None
            for attempt in range(2):
                try:
                    if attempt > 0:
                        self._log(job_id, "export_pdf", "warning", "Print page was not ready. Retrying once.")
                        self._navigate(tab, workflow_url)
                        table_html, rows_count = self._wait_for_source_table(tab, timeout_seconds=60)
                    self._replace_table_and_print(tab, table_html, print_url, output_path)
                    last_error = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    time.sleep(1)
            if last_error is not None:
                raise last_error

            if not output_path.exists() or output_path.stat().st_size <= 0:
                raise RuntimeError("PDF was generated but the output file is empty.")
            return rows_count
        finally:
            if tab is not None:
                tab.close()
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
            shutil.rmtree(temp_user_data_dir, ignore_errors=True)
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def _curl_fetch(
        self,
        url: str,
        cookie_jar: Path | None,
        output_path: Path,
        *,
        data: str | None = None,
        headers: list[str] | None = None,
    ) -> CurlResponse:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        header_path = output_path.with_suffix(output_path.suffix + ".headers")
        write_out_marker = "\n__CURL_EFFECTIVE_URL__=%{url_effective}\n__CURL_HTTP_CODE__=%{http_code}\n"
        command = [
            curl_binary(),
            "-k",
            "-sS",
            "-L",
            "-D",
            str(header_path),
            "-o",
            str(output_path),
            "--write-out",
            write_out_marker,
        ]
        if cookie_jar is not None:
            cookie_jar.parent.mkdir(parents=True, exist_ok=True)
            command.extend(["-c", str(cookie_jar), "-b", str(cookie_jar)])
        for header in headers or []:
            command.extend(["-H", header])
        if data is not None:
            command.extend(["--data-raw", data])
        command.append(url)

        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if result.returncode != 0:
            raise RuntimeError(f"curl request failed: {result.stderr.strip() or result.stdout.strip() or url}")

        match_url = re.search(r"__CURL_EFFECTIVE_URL__=(.+)", result.stdout)
        match_code = re.search(r"__CURL_HTTP_CODE__=(\d+)", result.stdout)
        effective_url = match_url.group(1).strip() if match_url else url
        status_code = int(match_code.group(1)) if match_code else 0
        headers_text = header_path.read_text(encoding="utf-8", errors="replace") if header_path.exists() else ""
        body_text = output_path.read_text(encoding="utf-8", errors="replace") if output_path.exists() else ""
        return CurlResponse(
            effective_url=effective_url,
            status_code=status_code,
            headers_text=headers_text,
            body_text=body_text,
        )

    def _parse_form_fields(self, html: str) -> dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        fields: dict[str, str] = {}
        for element in soup.select("input[name]"):
            name = element.get("name")
            if not name:
                continue
            fields[name] = element.get("value", "")
        return fields

    def _log(self, job_id: str, stage: str, level: str, message: str) -> None:
        entry = K2PrintJobLogResponse(at=iso_now(), stage=stage, level=level, message=message)
        with K2_JOB_LOCK:
            state = K2_JOB_STATES[job_id]
            state["logs"].append(entry.model_dump())

    def _update_state(self, job_id: str, **changes: Any) -> None:
        with K2_JOB_LOCK:
            state = K2_JOB_STATES[job_id]
            state.update(changes)

    def _persist_state(self, job_id: str, session_dir: Path) -> None:
        with K2_JOB_LOCK:
            state = json.loads(json.dumps(K2_JOB_STATES[job_id]))
        (session_dir / "job_state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
