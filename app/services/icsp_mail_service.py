from __future__ import annotations

import html
import os
import smtplib
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.legacy_flow import RailinliProbeDailyFlow, TrafficNodeDailyFlow
from app.models.sync_job import SyncJobState
from app.services.icsp_data_fill_service import ICSPDirectFillService, icsp_fill_target_date
from app.services.script_logger import build_script_logger
from app.services.sync_job_state_service import SyncJobStateService
from app.services.sync_log_service import SyncTaskLogService


ICSP_MAIL_JOB_NAME = "icsp_daily_mail"
SMTP_SERVER = os.getenv("ICSP_MAIL_SMTP_SERVER", "smtp.qiye.aliyun.com")
SMTP_PORT = int(os.getenv("ICSP_MAIL_SMTP_PORT", "465"))
SENDER_EMAIL = os.getenv("ICSP_MAIL_SENDER_EMAIL", "p-railinit@vanke.com")
SENDER_PASS = os.getenv("ICSP_MAIL_SENDER_PASS", "xiB7z9hFSalrH42x")
SENDER_NAME = os.getenv("ICSP_MAIL_SENDER_NAME", "填报机器人")
RECEIVER_EMAILS = [
    item.strip()
    for item in os.getenv("ICSP_MAIL_RECEIVERS", "h-pengxr01@vanke.com,ppxxrr@126.com").split(",")
    if item.strip()
]

LoggerCallback = Callable[[str, str], None] | None

VERIFY_PAGE_CONFIGS = [
    {
        "key": "railinli_passenger",
        "label": "睿印里RAIL INLI客流",
        "detail_url": (
            "https://inamp.scpgroup.com.cn/apps/data-filling/passenger/detail"
            "?mallId=591&dataYm={year_month}&mallName=%E7%9D%BF%E5%8D%B0%E9%87%8CRAIL%20INLI"
        ),
        "compare_mode": "exact",
    },
    {
        "key": "ruiyin_traffic",
        "label": "睿印RAIL IN车流",
        "detail_url": (
            "https://inamp.scpgroup.com.cn/apps/data-filling/traffic/detail"
            "?mallId=526&dataYm={year_month}&mallName=%E7%9D%BF%E5%8D%B0RAIL%20IN"
        ),
        "compare_mode": "exact",
    },
    {
        "key": "ruiyin_passenger",
        "label": "睿印RAIL IN客流",
        "detail_url": (
            "https://inamp.scpgroup.com.cn/apps/data-filling/passenger/detail"
            "?mallId=526&dataYm={year_month}&mallName=%E7%9D%BF%E5%8D%B0RAIL%20IN"
        ),
        "compare_mode": "positive_only",
    },
]


def _noop_logger(level: str, message: str) -> None:
    return None


def icsp_mail_target_date() -> date:
    return datetime.now().date() - timedelta(days=1)


@dataclass(slots=True)
class VerificationItem:
    key: str
    label: str
    source_value: int | None = None
    source_label: str | None = None
    icsp_value: int | None = None
    status: str = "pending"
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ICSPMailRunSummary:
    job_date: str
    dry_run: bool = False
    retry_pending_only: bool = False
    force: bool = False
    status: str = "pending"
    verification_items: list[dict[str, Any]] = field(default_factory=list)
    mail_subject: str | None = None
    recipient_count: int = 0
    warning_count: int = 0
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ICSPDailyMailService:
    def __init__(self, db: Session, logger: LoggerCallback = None):
        self.db = db
        self.logger = logger or _noop_logger
        self.job_state_service = SyncJobStateService(db)
        self.task_log_service = SyncTaskLogService(db)
        self.fill_service = ICSPDirectFillService(db, logger=logger)

    def run(
        self,
        *,
        job_date: date,
        dry_run: bool = False,
        retry_pending_only: bool = False,
        force: bool = False,
        triggered_by: str | None = None,
        triggered_source: str = "script",
    ) -> ICSPMailRunSummary:
        summary = ICSPMailRunSummary(
            job_date=job_date.isoformat(),
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
        )
        existing_job = self.job_state_service.get_job(job_name=ICSP_MAIL_JOB_NAME, job_date=job_date)
        if retry_pending_only and existing_job and existing_job.status == "success" and not dry_run:
            summary.status = "skipped_existing_success"
            return summary

        wrapper_job = None
        wrapper_log = None
        if not dry_run:
            wrapper_job = self.job_state_service.start_job(
                job_name=ICSP_MAIL_JOB_NAME,
                job_date=job_date,
                request_payload={
                    "job_date": job_date.isoformat(),
                    "retry_pending_only": retry_pending_only,
                    "force": force,
                },
                commit=True,
            )
            wrapper_log = self.task_log_service.create_log(
                module_name="icsp_mail",
                action="run_daily_icsp_mail",
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

        browser_client = None
        tab = None
        target_id = None
        try:
            browser_client, tab, target_id = self.fill_service._open_browser_tab()
            items = self._collect_verification_items(tab, job_date)
            summary.verification_items = [item.to_dict() for item in items]
            summary.warning_count = sum(1 for item in items if item.status not in {"ok", "info"})
            traffic_html = self._build_traffic_report_html(job_date=job_date, compare_date=job_date - timedelta(days=7))
            railinli_html = self._build_railinli_report_html(job_date=job_date, compare_date=job_date - timedelta(days=7))
            subject = self._build_subject(job_date)
            summary.mail_subject = subject
            summary.recipient_count = len(RECEIVER_EMAILS)
            full_html = self._render_mail_html(
                job_date=job_date,
                verification_items=items,
                traffic_html=traffic_html,
                railinli_html=railinli_html,
                mail_id=subject.rsplit("[ID:", 1)[-1].rstrip("]") if "[ID:" in subject else uuid.uuid4().hex[:8].upper(),
            )

            if dry_run:
                summary.status = "dry_run_ready"
            else:
                self._send_email(subject=subject, html_body=full_html)
                summary.status = "success"

            if not dry_run and wrapper_job is not None and wrapper_log is not None:
                self.job_state_service.mark_success(
                    wrapper_job,
                    success_start=job_date,
                    success_end=job_date,
                    result_payload=summary.to_dict(),
                    commit=False,
                )
                self.task_log_service.mark_success(wrapper_log, result_payload=summary.to_dict(), commit=False)
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
                self.fill_service._close_browser_target(browser_client, target_id)
                browser_client.close()

    def _collect_verification_items(self, tab: Any, job_date: date) -> list[VerificationItem]:
        results: list[VerificationItem] = []
        day_label = job_date.strftime("%m-%d")
        fill_job = self.job_state_service.get_job(job_name="icsp_data_fill", job_date=job_date)
        for config in VERIFY_PAGE_CONFIGS:
            item = VerificationItem(
                key=str(config["key"]),
                label=str(config["label"]),
            )
            source_value, source_label = self._resolve_source_value(item.key, job_date, tab)
            item.source_value = source_value
            item.source_label = source_label
            detail_url = str(config["detail_url"]).format(year_month=job_date.strftime("%Y-%m"))
            self.fill_service._navigate(tab, detail_url)
            self.fill_service._assert_icsp_authenticated(tab)
            self.fill_service._close_blocking_dialogs(tab)
            self.fill_service._wait_for_calendar_ready(tab, day_label)
            item.icsp_value = self.fill_service._wait_for_existing_day_value(tab, day_label)
            compare_mode = str(config["compare_mode"])
            if compare_mode == "positive_only":
                if (item.icsp_value or 0) > 0:
                    item.status = "ok"
                    item.note = "系统对接页值大于 0 视为正常"
                else:
                    item.status = "info"
                    item.note = "系统对接页面暂未返回值，不计入人工填报异常"
            else:
                if item.source_value is None:
                    item.status = "missing_source"
                    item.note = "未找到本地采集值"
                elif item.icsp_value == item.source_value:
                    item.status = "ok"
                else:
                    fill_result = self._extract_fill_result(fill_job, item.key)
                    if self._is_verified_fill_result(fill_result, item.source_value):
                        item.icsp_value = self._coerce_int(
                            fill_result.get("final_value") or fill_result.get("source_value")
                        )
                        item.status = "ok"
                        item.note = "以直填任务校验结果为准"
                    else:
                        item.status = "mismatch"
                        item.note = "ICSP 页面值与采集值不一致"
            results.append(item)
        probe_warning = self._build_railinli_probe_warning(job_date)
        if probe_warning is not None:
            results.append(probe_warning)
        return results

    def _build_railinli_probe_warning(self, job_date: date) -> VerificationItem | None:
        expected_rows = self.db.execute(
            select(
                RailinliProbeDailyFlow.probe_id,
                func.max(RailinliProbeDailyFlow.probe_name).label("probe_name"),
            )
            .where(~RailinliProbeDailyFlow.probe_id.like("codex-%"))
            .group_by(RailinliProbeDailyFlow.probe_id)
            .order_by(RailinliProbeDailyFlow.probe_id.asc())
        ).all()
        if not expected_rows:
            return None

        current_rows = self.db.execute(
            select(
                RailinliProbeDailyFlow.probe_id,
                func.max(RailinliProbeDailyFlow.probe_name).label("probe_name"),
            )
            .where(
                RailinliProbeDailyFlow.business_date == job_date,
                ~RailinliProbeDailyFlow.probe_id.like("codex-%"),
            )
            .group_by(RailinliProbeDailyFlow.probe_id)
        ).all()
        current_ids = {str(row.probe_id) for row in current_rows}
        missing_names = [
            str(row.probe_name or row.probe_id)
            for row in expected_rows
            if str(row.probe_id) not in current_ids
        ]

        item = VerificationItem(
            key="railinli_probe_completeness",
            label="睿印里探针完整性",
            source_value=len(expected_rows),
            source_label="预期探针数",
            icsp_value=len(current_rows),
        )
        if missing_names:
            item.status = "warning"
            item.note = "缺失探针: " + "、".join(missing_names)
        else:
            item.status = "ok"
            item.note = "探针返回完整"
        return item

    def _resolve_source_value(self, key: str, job_date: date, tab: Any) -> tuple[int | None, str]:
        if key == "railinli_passenger":
            value = self.db.scalar(
                select(func.coalesce(func.sum(RailinliProbeDailyFlow.entry_count), 0)).where(
                    RailinliProbeDailyFlow.business_date == job_date
                )
            )
            return int(value or 0), "服务器主库汇总"

        if key == "ruiyin_traffic":
            job = self.job_state_service.get_job(job_name="icsp_data_fill", job_date=job_date)
            source_value = self._extract_source_from_fill_job(job, key)
            if source_value is not None:
                return source_value, "ICSP 直填任务结果"
            if job_date == icsp_fill_target_date():
                live_value = self.fill_service._fetch_vehicle_value_from_4pyun_page(tab)
                if live_value is not None:
                    return live_value, "4pyun 实时页面"
            return None, "未找到车流采集值"

        if key == "ruiyin_passenger":
            value = self.db.scalar(
                select(func.coalesce(func.sum(TrafficNodeDailyFlow.passenger_flow), 0)).where(
                    TrafficNodeDailyFlow.business_date == job_date
                )
            )
            return int(value or 0), "服务器主库汇总"

        return None, "系统对接"

    def _extract_fill_result(self, job: SyncJobState | None, key: str) -> dict[str, Any] | None:
        payload = (job.result_payload or {}) if job else {}
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            return None
        for item in results:
            if not isinstance(item, dict):
                continue
            if str(item.get("key") or "") != key:
                continue
            return item
        return None

    def _extract_source_from_fill_job(self, job: SyncJobState | None, key: str) -> int | None:
        fill_result = self._extract_fill_result(job, key)
        if not isinstance(fill_result, dict):
            return None
        return self._coerce_int(fill_result.get("source_value"))

    def _coerce_int(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if value in (None, ""):
            return None
        try:
            return int(float(str(value)))
        except Exception:
            return None

    def _is_verified_fill_result(self, fill_result: dict[str, Any] | None, source_value: int | None) -> bool:
        if not isinstance(fill_result, dict):
            return False
        if str(fill_result.get("status") or "") != "success":
            return False
        if not bool(fill_result.get("verified")):
            return False
        final_value = self._coerce_int(fill_result.get("final_value"))
        if final_value is None:
            final_value = self._coerce_int(fill_result.get("source_value"))
        return source_value is not None and final_value == source_value

    def _build_traffic_report_html(self, *, job_date: date, compare_date: date) -> str:
        current_rows = self.db.execute(
            select(
                TrafficNodeDailyFlow.node_code,
                TrafficNodeDailyFlow.node_name,
                TrafficNodeDailyFlow.passenger_flow,
                TrafficNodeDailyFlow.passenger_flow_ratio,
            )
            .where(TrafficNodeDailyFlow.business_date == job_date)
            .order_by(TrafficNodeDailyFlow.passenger_flow.desc(), TrafficNodeDailyFlow.node_code.asc())
        ).all()
        compare_rows = self.db.execute(
            select(
                TrafficNodeDailyFlow.node_code,
                TrafficNodeDailyFlow.passenger_flow_ratio,
            ).where(TrafficNodeDailyFlow.business_date == compare_date)
        ).all()
        compare_map = {str(row.node_code): float(row.passenger_flow_ratio or 0) for row in compare_rows}
        if not current_rows:
            return "<p>无昨日睿印客流节点数据。</p>"

        header = (
            f"<h4>【睿印出入口客流分析】({job_date.isoformat()})</h4>"
            "<table border='1' style='border-collapse:collapse;width:100%;font-size:12px;'>"
            "<tr style='background:#f2f2f2'><th>点位名称</th><th>客流</th><th>贡献比</th><th>同期贡献比</th><th>贡献比差异</th></tr>"
        )
        lines = [header]
        for row in current_rows:
            ratio = float(row.passenger_flow_ratio or 0)
            compare_ratio = compare_map.get(str(row.node_code), 0.0)
            diff = ratio - compare_ratio
            style = "color:red;font-weight:bold;" if abs(diff) > 0.1 else ""
            lines.append(
                "<tr>"
                f"<td>{html.escape(str(row.node_name or row.node_code))}</td>"
                f"<td>{int(row.passenger_flow or 0)}</td>"
                f"<td>{ratio * 100:.2f}%</td>"
                f"<td>{compare_ratio * 100:.2f}%</td>"
                f"<td style='{style}'>{diff * 100:+.2f}%</td>"
                "</tr>"
            )
        lines.append("</table><br>")
        return "".join(lines)

    def _build_railinli_report_html(self, *, job_date: date, compare_date: date) -> str:
        current_rows = self.db.execute(
            select(
                RailinliProbeDailyFlow.probe_name,
                func.sum(RailinliProbeDailyFlow.entry_count).label("entry_count"),
            )
            .where(RailinliProbeDailyFlow.business_date == job_date)
            .group_by(RailinliProbeDailyFlow.probe_name)
            .order_by(func.sum(RailinliProbeDailyFlow.entry_count).desc())
        ).all()
        compare_rows = self.db.execute(
            select(
                RailinliProbeDailyFlow.probe_name,
                func.sum(RailinliProbeDailyFlow.entry_count).label("entry_count"),
            )
            .where(RailinliProbeDailyFlow.business_date == compare_date)
            .group_by(RailinliProbeDailyFlow.probe_name)
        ).all()
        if not current_rows:
            return "<p>无昨日睿印里客流数据。</p>"

        compare_map = {str(row.probe_name or ""): int(row.entry_count or 0) for row in compare_rows}
        total_current = sum(int(row.entry_count or 0) for row in current_rows)
        total_compare = sum(compare_map.values())

        header = (
            f"<h4>【睿印里出入口客流分析】({job_date.isoformat()})</h4>"
            "<table border='1' style='border-collapse:collapse;width:100%;font-size:12px;'>"
            "<tr style='background:#e6f7ff'><th>点位名称</th><th>客流</th><th>贡献比</th><th>同期贡献比</th><th>贡献比差异</th></tr>"
        )
        lines = [header]
        for row in current_rows:
            name = str(row.probe_name or "未命名点位")
            current_value = int(row.entry_count or 0)
            current_ratio = (current_value / total_current) if total_current else 0.0
            compare_value = compare_map.get(name, 0)
            compare_ratio = (compare_value / total_compare) if total_compare else 0.0
            diff = current_ratio - compare_ratio
            style = "color:red;font-weight:bold;" if abs(diff * 100) > 10 else ""
            lines.append(
                "<tr>"
                f"<td>{html.escape(name)}</td>"
                f"<td>{current_value}</td>"
                f"<td>{current_ratio:.1%}</td>"
                f"<td>{compare_ratio:.1%}</td>"
                f"<td style='{style}'>{diff * 100:+.2f}%</td>"
                "</tr>"
            )
        lines.append("</table><br>")
        return "".join(lines)

    def _build_subject(self, job_date: date) -> str:
        return f"【日报】{job_date.isoformat()} 填报分析 [ID:{uuid.uuid4().hex[:8].upper()}]"

    def _render_mail_html(
        self,
        *,
        job_date: date,
        verification_items: list[VerificationItem],
        traffic_html: str,
        railinli_html: str,
        mail_id: str,
    ) -> str:
        verification_html = [
            "<h4>【填报结果验证】</h4>",
            "<table border='1' style='border-collapse:collapse;width:100%;'>",
            "<tr><th>项目</th><th>采集值</th><th>ICSP填报值</th><th>状态</th><th>备注</th></tr>",
        ]
        for item in verification_items:
            source_text = item.source_label if item.source_value is None else str(item.source_value)
            icsp_text = "-" if item.icsp_value is None else str(item.icsp_value)
            status_map = {
                "ok": "✅",
                "mismatch": "❌",
                "missing": "❌",
                "missing_source": "⚠️",
                "warning": "⚠️",
                "info": "ℹ️",
            }
            verification_html.append(
                "<tr>"
                f"<td>{html.escape(item.label)}</td>"
                f"<td>{html.escape(source_text)}</td>"
                f"<td>{html.escape(icsp_text)}</td>"
                f"<td>{status_map.get(item.status, item.status)}</td>"
                f"<td>{html.escape(item.note or '-')}</td>"
                "</tr>"
            )
        verification_html.append("</table><br>")
        send_time = datetime.now().strftime("%H:%M:%S")
        return (
            "<html><body>"
            f"<h3>填报日报 {job_date.isoformat()} <span style='font-size:12px;color:#aaa;'>[ID:{mail_id}]</span></h3>"
            + "".join(verification_html)
            + "<hr>"
            + traffic_html
            + "<hr>"
            + railinli_html
            + (
                "<br>"
                "<div style='font-size:10px;color:#cccccc;border-top:1px dashed #ddd;padding-top:5px;'>"
                f"System Ref: {mail_id} | Time: {send_time} | Auto-Generated"
                "</div>"
            )
            + "</body></html>"
        )

    def _send_email(self, *, subject: str, html_body: str) -> None:
        if not RECEIVER_EMAILS:
            raise RuntimeError("ICSP mail receivers are not configured.")
        message = MIMEMultipart()
        message["From"] = formataddr((str(Header(SENDER_NAME, "utf-8")), SENDER_EMAIL))
        message["To"] = ",".join(RECEIVER_EMAILS)
        message["Subject"] = Header(subject, "utf-8")
        message.attach(MIMEText(html_body, "html", "utf-8"))
        smtp = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=20)
        try:
            smtp.login(SENDER_EMAIL, SENDER_PASS)
            smtp.sendmail(SENDER_EMAIL, RECEIVER_EMAILS, message.as_string())
        finally:
            try:
                smtp.quit()
            except Exception:
                pass


def build_icsp_mail_logger():
    return build_script_logger("run_daily_icsp_mail", "daily_icsp_mail.log")


def run_icsp_mail_once(
    *,
    job_date: date,
    dry_run: bool = False,
    retry_pending_only: bool = False,
    force: bool = False,
    triggered_by: str | None = None,
    triggered_source: str = "script",
) -> ICSPMailRunSummary:
    logger = build_icsp_mail_logger()

    def log_callback(level: str, message: str) -> None:
        getattr(logger, level.lower(), logger.info)(message)

    with SessionLocal() as session:
        service = ICSPDailyMailService(session, logger=log_callback)
        return service.run(
            job_date=job_date,
            dry_run=dry_run,
            retry_pending_only=retry_pending_only,
            force=force,
            triggered_by=triggered_by,
            triggered_source=triggered_source,
        )
