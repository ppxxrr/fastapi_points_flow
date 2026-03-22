from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.legacy_flow import RailinliProbeDailyFlow
from app.services.k2_print_service import CDPTab
from app.services.script_logger import build_script_logger
from app.services.sync_job_state_service import SyncJobStateService
from app.services.sync_log_service import SyncTaskLogService


BASE_DIR = Path(__file__).resolve().parents[2]
ICSP_DETAIL_FILL_JOB_NAME = "icsp_data_fill"
ICSP_FILL_BROWSER_PORT = int(os.getenv("ICSP_FILL_BROWSER_PORT", os.getenv("PARKING_MANAGED_CHROME_PORT", "9223")))
ICSP_FILL_LOGIN_MARKERS = ("login", "sso", "auth")
ICSP_FILL_LIST_URL = os.getenv(
    "ICSP_FILL_LIST_URL",
    "https://inamp.scpgroup.com.cn/apps/data-filling/passenger/list",
)
VEHICLE_STATS_URL = os.getenv("ICSP_4PYUN_VEHICLE_STATS_URL", "https://mch.4pyun.com/parking/merchant/statistics/data")
VEHICLE_VALUE_XPATH = os.getenv(
    "ICSP_4PYUN_VEHICLE_VALUE_XPATH",
    '//*[@id="app"]/div[1]/div[1]/div[2]/div/div/div[2]/div[2]/div/div[2]/div/div/div[2]/div/div[3]/div[2]',
)

LoggerCallback = Callable[[str, str], None] | None


def _noop_logger(level: str, message: str) -> None:
    return None


def icsp_fill_target_date() -> date:
    return datetime.now().date() - timedelta(days=1)


def _existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


DEFAULT_TRAFFIC_DB_PATH = _existing_path(
    [
        BASE_DIR / "data" / "legacy" / "traffic_data.db",
        BASE_DIR / "traffic_data.db",
    ]
)
DEFAULT_RAILINLI_DB_PATH = _existing_path(
    [
        BASE_DIR / "data" / "legacy" / "railinliKL.db",
        BASE_DIR / "railinliKL.db",
    ]
)
PASSENGER_HOST = os.getenv("ICSP_TRAFFIC_API_HOST", "https://10.95.17.101")
PASSENGER_LOGIN_URL = os.getenv("ICSP_TRAFFIC_API_LOGIN_URL", f"{PASSENGER_HOST}/auth/getAccessTokenV2")
PASSENGER_DATA_URL = os.getenv("ICSP_TRAFFIC_API_DATA_URL", f"{PASSENGER_HOST}/snapdata/report/nodes/overview/period")
PASSENGER_USERNAME = os.getenv("ICSP_TRAFFIC_API_USERNAME", "18719207571")
PASSENGER_PASSWORD = os.getenv(
    "ICSP_TRAFFIC_API_PASSWORD",
    "EVukO+xHda9lBivAmXlaQxd38XtfDNUv4sbmI0Zmren+5H6NCiZI6NO2LprG67//b7cPY5ZnrTrG1EXPIaNJMcUaHtOAOMNHsPj+wXo7iwpDzluaLlkvceJExD+QNWn54WoLridI1T+RCaLO8OmW2mUetxrob0cXJA2YEJ/+gda7ZuTCFucRLX0cnDYJdQ8lWVk1CJ4NKsvGMu0/7LNKnYKVq1cMMxMqapdOPrYd3dCBe452IGKrnXrVckhg0dZTjtqMY54lB8AYOnKCq6PEKk0AQwINurQMysz5YlmAShj4sAmXdzK8fL79Pymg8AQV3c2sMXBr81juoptuA90SjQ==",
)
PASSENGER_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Referer": f"{PASSENGER_HOST}/",
}


@dataclass(slots=True)
class ICSPFillProjectConfig:
    key: str
    label: str
    dialog_title: str
    detail_url: str


PROJECT_CONFIGS = [
    ICSPFillProjectConfig(
        key="railinli_passenger",
        label="睿印里RAIL INLI 客流",
        dialog_title="客流数量",
        detail_url=(
            "https://inamp.scpgroup.com.cn/apps/data-filling/passenger/detail"
            "?mallId=591&dataYm={year_month}&mallName=%E7%9D%BF%E5%8D%B0%E9%87%8CRAIL%20INLI"
        ),
    ),
    ICSPFillProjectConfig(
        key="ruiyin_traffic",
        label="睿印RAIL IN 车流",
        dialog_title="车流数量",
        detail_url=(
            "https://inamp.scpgroup.com.cn/apps/data-filling/traffic/detail"
            "?mallId=526&dataYm={year_month}&mallName=%E7%9D%BF%E5%8D%B0RAIL%20IN"
        ),
    ),
]


@dataclass(slots=True)
class ICSPFillProjectResult:
    key: str
    label: str
    source_value: int | None = None
    source_path: str | None = None
    status: str = "pending"
    detail_url: str | None = None
    current_value: int | None = None
    final_value: int | None = None
    verified: bool = False
    operation_logged: bool = False
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ICSPFillRunSummary:
    job_date: str
    dry_run: bool = False
    retry_pending_only: bool = False
    force: bool = False
    status: str = "pending"
    browser_port: int = ICSP_FILL_BROWSER_PORT
    results: list[dict[str, Any]] = field(default_factory=list)
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ICSPDirectFillService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger
        self.task_log_service = SyncTaskLogService(db)
        self.job_state_service = SyncJobStateService(db)

    def run(
        self,
        *,
        job_date: date,
        dry_run: bool = False,
        retry_pending_only: bool = False,
        force: bool = False,
        triggered_by: str | None = None,
        triggered_source: str = "script",
    ) -> ICSPFillRunSummary:
        summary = ICSPFillRunSummary(
            job_date=job_date.isoformat(),
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
        )

        existing_job = self.job_state_service.get_job(job_name=ICSP_DETAIL_FILL_JOB_NAME, job_date=job_date)
        if retry_pending_only and existing_job and existing_job.status == "success" and not dry_run:
            summary.status = "skipped_existing_success"
            return summary

        wrapper_job = None
        wrapper_log = None
        if not dry_run:
            wrapper_job = self.job_state_service.start_job(
                job_name=ICSP_DETAIL_FILL_JOB_NAME,
                job_date=job_date,
                request_payload={
                    "job_date": job_date.isoformat(),
                    "retry_pending_only": retry_pending_only,
                    "force": force,
                },
                commit=True,
            )
            wrapper_log = self.task_log_service.create_log(
                module_name="icsp_data_fill",
                action="run_daily_icsp_data_fill",
                target_type="date",
                target_value=job_date.isoformat(),
                triggered_by=triggered_by,
                triggered_source=triggered_source,
                request_payload={
                    "retry_pending_only": retry_pending_only,
                    "force": force,
                },
                commit=True,
            )

        browser_client: CDPTab | None = None
        tab: CDPTab | None = None
        target_id: str | None = None
        try:
            browser_client, tab, target_id = self._open_browser_tab()
            results: list[ICSPFillProjectResult] = []
            for project in PROJECT_CONFIGS:
                result = self._fill_project(tab, project, job_date=job_date, dry_run=dry_run, force=force)
                results.append(result)
            summary.results = [item.to_dict() for item in results]
            failures = [item for item in results if item.status == "failed"]
            actionable = [item for item in results if item.status not in {"skipped_no_data"}]
            if failures:
                summary.status = "failed"
                summary.last_error = "; ".join(item.note or item.label for item in failures)
            elif actionable:
                summary.status = "success"
            else:
                summary.status = "skipped_no_data"

            if not dry_run and wrapper_job is not None and wrapper_log is not None:
                if summary.status in {"success", "skipped_no_data"}:
                    self.job_state_service.mark_success(
                        wrapper_job,
                        success_start=job_date,
                        success_end=job_date,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                    self.task_log_service.mark_success(wrapper_log, result_payload=summary.to_dict(), commit=False)
                else:
                    self.job_state_service.mark_failure(
                        wrapper_job,
                        error_message=summary.last_error or summary.status,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                    self.task_log_service.mark_failure(
                        wrapper_log,
                        error_message=summary.last_error or summary.status,
                        result_payload=summary.to_dict(),
                        commit=False,
                    )
                self.db.commit()
            return summary
        except Exception as exc:
            summary.status = "failed"
            summary.last_error = str(exc)
            if not dry_run and wrapper_job is not None and wrapper_log is not None:
                self.db.rollback()
                self.job_state_service.mark_failure(
                    wrapper_job,
                    error_message=summary.last_error,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_failure(
                    wrapper_log,
                    error_message=summary.last_error,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.db.commit()
            raise
        finally:
            if tab is not None:
                tab.close()
            if browser_client is not None and target_id:
                self._close_browser_target(browser_client, target_id)
                browser_client.close()

    def _fill_project(
        self,
        tab: CDPTab,
        project: ICSPFillProjectConfig,
        *,
        job_date: date,
        dry_run: bool,
        force: bool,
    ) -> ICSPFillProjectResult:
        result = self._load_source_value(project.key, job_date)
        result.detail_url = project.detail_url.format(year_month=job_date.strftime("%Y-%m"))
        if project.key == "ruiyin_traffic" and job_date == icsp_fill_target_date():
            vehicle_value = self._fetch_vehicle_value_from_4pyun_page(tab)
            if vehicle_value not in (None, 0):
                result.source_value = vehicle_value
                result.note = "loaded directly from live 4pyun vehicle stats page"
        if result.source_value is None or result.source_value <= 0:
            result.status = "skipped_no_data"
            result.note = "source value is empty or non-positive"
            return result

        self._navigate(tab, result.detail_url)
        self._assert_icsp_authenticated(tab)
        self._close_blocking_dialogs(tab)
        day_label = job_date.strftime("%m-%d")
        self._wait_for_calendar_ready(tab, day_label)
        current_value = self._wait_for_existing_day_value(tab, day_label)
        result.current_value = current_value
        existing_logged = self._wait_for_existing_log(tab, job_date, result.source_value)
        if (current_value == result.source_value or existing_logged) and not force:
            result.status = "skipped_existing_value"
            result.final_value = current_value if current_value is not None else result.source_value
            result.operation_logged = existing_logged
            result.verified = current_value == result.source_value or existing_logged
            return result

        if dry_run:
            result.status = "dry_run_ready"
            result.note = f"would fill {result.source_value}"
            return result

        self._open_day_editor(tab, day_label)
        self._set_dialog_value(tab, project, result.source_value)
        self._confirm_dialog(tab, project)
        result.final_value = self._wait_for_day_value(tab, day_label, result.source_value)
        result.operation_logged = self._wait_for_operation_log(tab, job_date, result.source_value)
        result.verified = result.final_value == result.source_value and result.operation_logged
        if result.verified:
            result.status = "success"
            return result

        result.status = "failed"
        result.note = "page value or operation log verification failed"
        return result

    def _fetch_vehicle_value_from_4pyun_page(self, tab: CDPTab) -> int | None:
        self._navigate(tab, VEHICLE_STATS_URL)
        current_url = str(tab.evaluate("location.href", timeout=10) or "")
        if "login" in current_url.lower():
            raise RuntimeError(f"4pyun browser session is not authenticated: {current_url}")
        deadline = time.time() + 20
        script = f"""
(() => {{
  const node = document.evaluate(
    {VEHICLE_VALUE_XPATH!r},
    document,
    null,
    XPathResult.FIRST_ORDERED_NODE_TYPE,
    null
  ).singleNodeValue;
  if (!node) return null;
  const text = (node.innerText || node.textContent || '').replace(/,/g, '').trim();
  if (!text || text === '-') return null;
  const matched = text.match(/\\d+/);
  return matched ? Number(matched[0]) : null;
}})()
"""
        while time.time() < deadline:
            value = tab.evaluate(script, timeout=10)
            if isinstance(value, (int, float)) and int(value) > 0:
                self.logger("INFO", f"icsp fill vehicle stats source value={int(value)}")
                return int(value)
            time.sleep(1)
        return None

    def _load_source_value(self, project_key: str, job_date: date) -> ICSPFillProjectResult:
        result = ICSPFillProjectResult(
            key=project_key,
            label=next(item.label for item in PROJECT_CONFIGS if item.key == project_key),
        )
        if project_key == "railinli_passenger":
            primary_value = self.db.scalar(
                select(func.coalesce(func.sum(RailinliProbeDailyFlow.entry_count), 0)).where(
                    RailinliProbeDailyFlow.business_date == job_date
                )
            )
            if primary_value not in (None, 0):
                result.source_value = int(primary_value)
                result.source_path = "server_database:railinli_probe_daily_flow"
                result.note = "railinli passenger value is sourced from the server database"
                return result
            db_path = Path(os.getenv("ICSP_RAILINLI_DB_PATH", str(DEFAULT_RAILINLI_DB_PATH)))
            query = "SELECT SUM(entry_count) FROM traffic_log WHERE date = ?"
        else:
            result.source_path = "4pyun_live_page"
            result.note = "traffic value is sourced directly from live 4pyun page for the latest business date"
            return result
        result.source_path = str(db_path)
        if not db_path.exists():
            result.note = f"source sqlite is missing: {db_path}"
            return result
        with sqlite3.connect(db_path) as connection:
            row = connection.execute(query, (job_date.isoformat(),)).fetchone()
        raw_value = row[0] if row else None
        result.source_value = int(float(raw_value)) if raw_value not in (None, "") else None
        return result

    def _fetch_traffic_value_from_api(self, job_date: date) -> int | None:
        payload = {
            "passwd": PASSENGER_PASSWORD,
            "username": PASSENGER_USERNAME,
            "randStr": "",
            "ticket": "",
        }
        session = requests.Session()
        login_response = session.post(
            PASSENGER_LOGIN_URL,
            headers=PASSENGER_HEADERS,
            json=payload,
            verify=False,
            timeout=20,
        )
        login_response.raise_for_status()
        token = login_response.json().get("data", {}).get("token")
        if not token:
            self.logger("WARNING", "icsp fill traffic api login returned no token")
            return None

        headers = dict(PASSENGER_HEADERS)
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
            PASSENGER_DATA_URL,
            headers=headers,
            json=body,
            verify=False,
            timeout=30,
        )
        response.raise_for_status()
        return self._extract_pp99_value(response.json().get("data"))

    def _extract_pp99_value(self, payload: Any) -> int | None:
        if isinstance(payload, list):
            for item in payload:
                value = self._extract_pp99_value(item)
                if value is not None:
                    return value
            return None
        if not isinstance(payload, dict):
            return None
        code = str(payload.get("nodecode") or payload.get("nodeCode") or "").strip()
        flow = payload.get("passengerFlow")
        if code == "pp99" and flow not in (None, ""):
            return int(float(flow))
        children = payload.get("child")
        if isinstance(children, list):
            return self._extract_pp99_value(children)
        return None

    def _save_traffic_value_to_sqlite(self, db_path: Path, job_date: date, value: int) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_passenger_flow (
                    nodecode TEXT NOT NULL,
                    nodename TEXT,
                    date TEXT NOT NULL,
                    passengerFlow INTEGER,
                    passengerFlowRatio REAL,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (nodecode, date)
                )
                """
            )
            connection.execute(
                """
                REPLACE INTO daily_passenger_flow
                    (nodecode, nodename, date, passengerFlow, passengerFlowRatio, update_time)
                VALUES
                    ('pp99', '入场车流', ?, ?, 0.0, CURRENT_TIMESTAMP)
                """,
                (job_date.isoformat(), int(value)),
            )
            connection.commit()

    def _resolve_browser_target(self) -> tuple[str, str]:
        base_url = f"http://127.0.0.1:{ICSP_FILL_BROWSER_PORT}"
        response = requests.get(f"{base_url}/json/version", timeout=3)
        response.raise_for_status()
        payload = response.json()
        websocket_url = str(payload.get("webSocketDebuggerUrl") or "")
        if not websocket_url:
            raise RuntimeError("Shared Chrome DevTools browser endpoint is unavailable.")
        return base_url, websocket_url

    def _open_browser_tab(self) -> tuple[CDPTab, CDPTab, str]:
        base_url, browser_ws = self._resolve_browser_target()
        browser_client = CDPTab(browser_ws)
        target_id = str(browser_client.send("Target.createTarget", {"url": "about:blank", "background": True}).get("targetId") or "")
        if not target_id:
            browser_client.close()
            raise RuntimeError("Unable to create a shared browser target.")
        deadline = time.time() + 15
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
            self._close_browser_target(browser_client, target_id)
            browser_client.close()
            raise RuntimeError("Shared browser page websocket URL was not found.")
        return browser_client, CDPTab(websocket_url), target_id

    def _close_browser_target(self, browser_client: CDPTab, target_id: str) -> None:
        try:
            browser_client.send("Target.closeTarget", {"targetId": target_id}, timeout=5)
        except Exception:
            pass

    def _navigate(self, tab: CDPTab, url: str) -> None:
        tab.send("Page.enable")
        tab.send("Runtime.enable")
        tab.send("Page.navigate", {"url": url})
        self._wait_for_page_ready(tab)

    def _wait_for_page_ready(self, tab: CDPTab, timeout_seconds: int = 30) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if tab.evaluate("document.readyState") == "complete":
                return
            time.sleep(0.3)
        raise RuntimeError("ICSP page load timed out.")

    def _assert_icsp_authenticated(self, tab: CDPTab) -> None:
        current_url = str(tab.evaluate("location.href", timeout=10) or "")
        lowered = current_url.lower()
        if any(marker in lowered for marker in ICSP_FILL_LOGIN_MARKERS):
            raise RuntimeError(f"ICSP browser session is not authenticated: {current_url}")

    def _wait_for_calendar_ready(self, tab: CDPTab, day_label: str, timeout_seconds: int = 20) -> None:
        deadline = time.time() + timeout_seconds
        script = f"""
(() => Array.from(document.querySelectorAll('.date-cell .date-cell-i span')).some(
  el => (el.textContent || '').trim() === {day_label!r}
))()
"""
        while time.time() < deadline:
            if tab.evaluate(script, timeout=10):
                return
            time.sleep(0.5)
        raise RuntimeError(f"ICSP detail calendar did not finish loading for {day_label}")

    def _close_blocking_dialogs(self, tab: CDPTab, attempts: int = 4) -> None:
        script = """
(() => {
  const actions = [];
  const dialogs = Array.from(document.querySelectorAll('.el-dialog')).filter(
    el => getComputedStyle(el).display !== 'none'
  );
  for (const dialog of dialogs) {
    const title = (dialog.querySelector('.el-dialog__title')?.textContent || '').trim();
    const text = (dialog.innerText || '').trim();
    let button = null;
    if (title.includes('发版通知') || text.includes('发版通知')) {
      button = Array.from(dialog.querySelectorAll('button')).find(el => /忽略|不再展示/.test((el.innerText || '').trim()));
      if (!button) button = dialog.querySelector('.el-dialog__headerbtn');
    } else if (title.includes('切换企业')) {
      button = Array.from(dialog.querySelectorAll('button')).find(el => /取消/.test((el.innerText || '').trim()));
      if (!button) button = dialog.querySelector('.el-dialog__headerbtn');
    } else if (!title) {
      button = dialog.querySelector('.el-dialog__headerbtn');
    }
    if (button) {
      button.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
      actions.push(title || 'untitled');
    }
  }
  return actions;
})()
"""
        for _ in range(attempts):
            actions = tab.evaluate(script, timeout=10) or []
            if not actions:
                break
            self.logger("INFO", f"icsp fill closed dialogs={actions}")
            time.sleep(0.6)

    def _read_day_value_payload(self, tab: CDPTab, day_label: str) -> dict[str, Any] | None:
        script = f"""
(() => {{
  const span = Array.from(document.querySelectorAll('.date-cell .date-cell-i span')).find(
    el => (el.textContent || '').trim() === {day_label!r}
  );
  if (!span) return null;
  const cell = span.closest('.date-cell');
  const valueNode = cell ? cell.querySelector('.passenger-day') : null;
  if (!valueNode) return null;
  const raw = (valueNode.textContent || '').trim();
  const text = raw.replace(/,/g, '').trim();
  if (!text) return {{ raw, value: null }};
  if (text === '-') return {{ raw, value: null }};
  const matched = text.match(/-?\\d+/);
  return {{ raw, value: matched ? Number(matched[0]) : null }};
}})()
"""
        payload = tab.evaluate(script, timeout=10)
        return payload if isinstance(payload, dict) else None

    def _read_day_value(self, tab: CDPTab, day_label: str) -> int | None:
        payload = self._read_day_value_payload(tab, day_label)
        value = payload.get("value") if isinstance(payload, dict) else None
        return int(value) if isinstance(value, (int, float)) else None

    def _wait_for_existing_day_value(self, tab: CDPTab, day_label: str, timeout_seconds: int = 10) -> int | None:
        deadline = time.time() + timeout_seconds
        last_payload: dict[str, Any] | None = None
        while time.time() < deadline:
            payload = self._read_day_value_payload(tab, day_label)
            last_payload = payload
            if not payload:
                time.sleep(0.5)
                continue
            raw_text = str(payload.get("raw") or "").strip()
            value = payload.get("value")
            if isinstance(value, (int, float)):
                return int(value)
            if raw_text == "-":
                return None
            time.sleep(0.5)
        if isinstance(last_payload, dict):
            value = last_payload.get("value")
            if isinstance(value, (int, float)):
                return int(value)
        return None

    def _open_day_editor(self, tab: CDPTab, day_label: str) -> None:
        script = f"""
(() => {{
  const span = Array.from(document.querySelectorAll('.date-cell .date-cell-i span')).find(
    el => (el.textContent || '').trim() === {day_label!r}
  );
  if (!span) return 'day_not_found';
  const cell = span.closest('.date-cell');
  const icon = cell ? cell.querySelector('.el-icon-edit') : null;
  if (!icon) return 'edit_icon_not_found';
  icon.dispatchEvent(new MouseEvent('click', {{ bubbles: true, cancelable: true }}));
  return 'clicked';
}})()
"""
        result = str(tab.evaluate(script, timeout=10) or "")
        if result != "clicked":
            raise RuntimeError(f"Unable to open day editor for {day_label}: {result}")
        time.sleep(0.8)

    def _set_dialog_value(self, tab: CDPTab, project: ICSPFillProjectConfig, value: int) -> None:
        script = f"""
(() => {{
  const dialog = Array.from(document.querySelectorAll('.el-dialog')).find(el => {{
    const title = (el.querySelector('.el-dialog__title')?.textContent || '').trim();
    return getComputedStyle(el).display !== 'none' && title.includes({project.dialog_title!r});
  }});
  if (!dialog) return 'dialog_not_found';
  const input = dialog.querySelector('input.el-input__inner');
  if (!input) return 'input_not_found';
  input.focus();
  input.value = {str(value)!r};
  input.dispatchEvent(new Event('input', {{ bubbles: true }}));
  input.dispatchEvent(new Event('change', {{ bubbles: true }}));
  return input.value;
}})()
"""
        result = str(tab.evaluate(script, timeout=10) or "")
        if result != str(value):
            raise RuntimeError(f"Unable to set dialog value for {project.label}: {result}")

    def _confirm_dialog(self, tab: CDPTab, project: ICSPFillProjectConfig) -> None:
        script = f"""
(() => {{
  const dialog = Array.from(document.querySelectorAll('.el-dialog')).find(el => {{
    const title = (el.querySelector('.el-dialog__title')?.textContent || '').trim();
    return getComputedStyle(el).display !== 'none' && title.includes({project.dialog_title!r});
  }});
  if (!dialog) return 'dialog_not_found';
  const normalize = (value) => (value || '').replace(/\\s+/g, '');
  let button = dialog.querySelector('.el-dialog__footer .el-button--primary');
  if (!button) {{
    button = Array.from(dialog.querySelectorAll('button')).find(el => {{
      const text = normalize(el.innerText || el.textContent || '');
      return text === '确定' || text === '保存' || text === '提交';
    }});
  }}
  if (!button) {{
    const footerButtons = Array.from(dialog.querySelectorAll('.el-dialog__footer button')).filter(
      el => !el.disabled && getComputedStyle(el).display !== 'none'
    );
    button = footerButtons.at(-1) || null;
  }}
  if (!button) return 'confirm_not_found';
  button.dispatchEvent(new MouseEvent('click', {{ bubbles: true, cancelable: true }}));
  return 'clicked';
}})()
"""
        result = str(tab.evaluate(script, timeout=10) or "")
        if result != "clicked":
            raise RuntimeError(f"Unable to confirm dialog for {project.label}: {result}")

    def _wait_for_day_value(self, tab: CDPTab, day_label: str, expected: int, timeout_seconds: int = 25) -> int | None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            self._close_blocking_dialogs(tab, attempts=1)
            current_value = self._read_day_value(tab, day_label)
            if current_value == expected:
                return current_value
            time.sleep(0.6)
        return self._read_day_value(tab, day_label)

    def _has_operation_log_value(self, tab: CDPTab, job_date: date, expected: int) -> bool:
        formatted_date = job_date.isoformat()
        formatted_value = f"{expected:,}"
        script = f"""
(() => {{
  const bodyText = document.body.innerText || '';
  const section = bodyText.includes('操作日志') ? bodyText.split('操作日志').slice(1).join('操作日志') : bodyText;
  return section.includes({formatted_date!r}) && section.includes({formatted_value!r});
}})()
"""
        return bool(tab.evaluate(script, timeout=10))

    def _wait_for_existing_log(self, tab: CDPTab, job_date: date, expected: int, timeout_seconds: int = 8) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._has_operation_log_value(tab, job_date, expected):
                return True
            time.sleep(0.6)
        return self._has_operation_log_value(tab, job_date, expected)

    def _wait_for_operation_log(self, tab: CDPTab, job_date: date, expected: int, timeout_seconds: int = 25) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._has_operation_log_value(tab, job_date, expected):
                return True
            time.sleep(0.8)
        return self._has_operation_log_value(tab, job_date, expected)


def build_icsp_fill_logger():
    return build_script_logger("run_daily_icsp_data_fill", "daily_icsp_data_fill.log")


def run_icsp_fill_once(
    *,
    job_date: date,
    dry_run: bool = False,
    retry_pending_only: bool = False,
    force: bool = False,
    triggered_by: str | None = None,
    triggered_source: str = "script",
) -> ICSPFillRunSummary:
    logger = build_icsp_fill_logger()

    def log_callback(level: str, message: str) -> None:
        getattr(logger, level.lower(), logger.info)(message)

    with SessionLocal() as session:
        service = ICSPDirectFillService(session, logger=log_callback)
        return service.run(
            job_date=job_date,
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
            triggered_by=triggered_by,
            triggered_source=triggered_source,
        )
