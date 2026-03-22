from __future__ import annotations

import base64
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

# ================= 核心配置区域 =================
TARGET_JS_FUNC = "showFullHistory()"
SOURCE_TABLE_XPATH = '//*[@id="fullApprovalHistory"]/div/div/table'
TARGET_TABLE_XPATH = '//*[@id="approvalHistoryCt"]/div/div/table'
# =================================================

os.environ["no_proxy"] = "127.0.0.1,localhost"
os.environ["NO_PROXY"] = "127.0.0.1,localhost"


class K2PrintError(RuntimeError):
    pass


@dataclass
class BrowserContext:
    browser_name: str
    user_data_dir: Path
    profile_name: str


@dataclass
class HistoryMatch:
    data_url: str
    browser: BrowserContext
    title: str | None = None


@dataclass
class ExportResult:
    reference: str
    data_url: str
    print_url: str
    output_path: Path
    browser_name: str
    profile_name: str
    rows_count: int


def ensure_drission_page():
    try:
        from DrissionPage import ChromiumOptions, ChromiumPage
    except ImportError as exc:
        raise K2PrintError("缺少 DrissionPage 依赖，请先安装 requirements.txt。") from exc
    return ChromiumOptions, ChromiumPage


def ensure_browser_cookie3():
    try:
        import browser_cookie3
    except ImportError as exc:
        raise K2PrintError("缺少 browser-cookie3 依赖，请先安装 requirements.txt。") from exc
    return browser_cookie3


def get_base_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path.cwd()


def get_browser_executable(browser_name: str) -> Path | None:
    candidates: dict[str, list[str]] = {
        "edge": [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            "/usr/bin/microsoft-edge",
            "/usr/bin/microsoft-edge-stable",
        ],
        "chrome": [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ],
    }
    for raw in candidates.get(browser_name, []):
        path = Path(raw)
        if path.exists():
            return path
    return None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def sanitize_filename(filename: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {".", "-", "_"} else "_" for char in filename)
    return cleaned or "approval_record.pdf"


def iter_browser_contexts() -> list[BrowserContext]:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    contexts: list[BrowserContext] = []
    candidates = [
        ("edge", local_app_data / "Microsoft" / "Edge" / "User Data"),
        ("chrome", local_app_data / "Google" / "Chrome" / "User Data"),
    ]
    for browser_name, user_data_dir in candidates:
        if not user_data_dir.exists():
            continue
        profiles = []
        default_profile = user_data_dir / "Default"
        if default_profile.exists():
            profiles.append(default_profile)
        profiles.extend(sorted(path for path in user_data_dir.glob("Profile *") if path.is_dir()))
        for profile_dir in profiles:
            if (profile_dir / "History").exists():
                contexts.append(
                    BrowserContext(
                        browser_name=browser_name,
                        user_data_dir=user_data_dir,
                        profile_name=profile_dir.name,
                    )
                )
    return contexts


def copy_database(source: Path) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="k2_history_"))
    target = temp_dir / source.name
    shutil.copy2(source, target)
    return target


def looks_like_url(value: str) -> bool:
    lowered = value.lower()
    return lowered.startswith("http://") or lowered.startswith("https://")


def query_history(context: BrowserContext, keyword: str) -> list[tuple[str, str | None]]:
    history_path = context.user_data_dir / context.profile_name / "History"
    if not history_path.exists():
        return []
    copied_db = copy_database(history_path)
    try:
        connection = sqlite3.connect(str(copied_db))
        try:
            cursor = connection.execute(
                """
                SELECT url, title
                FROM urls
                WHERE url LIKE ? OR title LIKE ?
                ORDER BY last_visit_time DESC
                LIMIT 60
                """,
                (f"%{keyword}%", f"%{keyword}%"),
            )
            return [(str(url or ""), str(title or "")) for url, title in cursor.fetchall()]
        finally:
            connection.close()
    finally:
        shutil.rmtree(copied_db.parent, ignore_errors=True)


def find_recent_history_match(reference: str) -> HistoryMatch | None:
    best_match: HistoryMatch | None = None
    for context in iter_browser_contexts():
        rows = query_history(context, reference)
        for url, title in rows:
            lowered = url.lower()
            if "procinstid=" not in lowered and "/print/print.aspx" not in lowered:
                continue
            best_match = HistoryMatch(data_url=url, browser=context, title=title)
            return best_match
    return best_match


def find_browser_for_url(data_url: str) -> BrowserContext | None:
    host = urlparse(data_url).netloc
    if not host:
        return None
    for context in iter_browser_contexts():
        rows = query_history(context, host)
        for url, _ in rows:
            if host in url:
                return context
    contexts = iter_browser_contexts()
    return contexts[0] if contexts else None


def generate_print_url(data_url: str) -> str | None:
    try:
        parsed = urlparse(data_url)
        params = parse_qs(parsed.query)
        proc_id_list = params.get("procInstID") or params.get("ProcInstID") or params.get("procinstid") or [""]
        proc_id = proc_id_list[0]
        key_list = params.get("key") or params.get("Key") or [""]
        key = key_list[0]
        if not proc_id or not key:
            return None
        base_url = f"{parsed.scheme}://{parsed.netloc}/Print/Print.aspx"
        new_params = {"ProcInstID": proc_id, "key": key, "ExecuteType": "Execute"}
        return f"{base_url}?{urlencode(new_params)}"
    except Exception as exc:  # pragma: no cover - defensive
        raise K2PrintError(f"解析审批链接失败：{exc}") from exc


def resolve_reference(reference: str) -> HistoryMatch:
    value = reference.strip()
    if not value:
        raise K2PrintError("请输入 K2 号或审批链接。")
    if looks_like_url(value):
        browser = find_browser_for_url(value)
        if not browser:
            raise K2PrintError("未找到可用浏览器历史，无法确定对应的 Cookie 来源。")
        return HistoryMatch(data_url=value, browser=browser)
    history_match = find_recent_history_match(value)
    if not history_match:
        raise K2PrintError("未在本机浏览器历史中找到匹配的 K2 记录，请先在浏览器中打开一次该审批页面。")
    return history_match


def load_cookies_for_url(data_url: str, browser_name: str):
    browser_cookie3 = ensure_browser_cookie3()
    domain = (urlparse(data_url).hostname or "").split(":")[0]
    if not domain:
        raise K2PrintError("审批链接缺少有效域名，无法读取浏览器 Cookie。")

    getters = []
    if browser_name == "edge":
        getters = [browser_cookie3.edge, browser_cookie3.chrome]
    elif browser_name == "chrome":
        getters = [browser_cookie3.chrome, browser_cookie3.edge]
    else:
        getters = [browser_cookie3.edge, browser_cookie3.chrome]

    last_error: Exception | None = None
    for getter in getters:
        try:
            jar = getter(domain_name=domain)
        except Exception as exc:  # pragma: no cover - environment-specific
            last_error = exc
            continue
        cookies = list(jar)
        if cookies:
            return cookies
    if last_error:
        raise K2PrintError(f"读取浏览器 Cookie 失败：{last_error}") from last_error
    raise K2PrintError("未读取到当前审批站点的浏览器 Cookie，请先确认该站点已在浏览器中保持登录。")


def launch_browser(temp_user_data_dir: Path, browser_name: str):
    ChromiumOptions, ChromiumPage = ensure_drission_page()
    executable = get_browser_executable(browser_name)
    if not executable:
        raise K2PrintError(f"未找到可用的 {browser_name} 浏览器可执行文件。")

    port = find_free_port()
    args = [
        str(executable),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={temp_user_data_dir}",
        "--headless=new",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--remote-allow-origins=*",
        "about:blank",
    ]
    process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    options = ChromiumOptions()
    options.set_address(f"127.0.0.1:{port}")
    try:
        page = ChromiumPage(options)
    except Exception as exc:
        process.terminate()
        raise K2PrintError("连接浏览器失败，请确认当前环境允许启动浏览器调试。") from exc
    return process, page


def inject_cookies(page: Any, data_url: str, cookies: list[Any]) -> None:
    parsed = urlparse(data_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    page.run_cdp("Network.enable")
    for cookie in cookies:
        payload: dict[str, Any] = {
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain or (parsed.hostname or ""),
            "path": cookie.path or "/",
            "secure": bool(getattr(cookie, "secure", False)),
            "httpOnly": bool(getattr(cookie, "_rest", {}).get("HttpOnly")),
            "url": origin,
        }
        expires = getattr(cookie, "expires", None)
        if expires and expires > 0:
            payload["expires"] = float(expires)
        try:
            page.run_cdp("Network.setCookie", **payload)
        except Exception:
            # Ignore individual cookie errors; keep trying remaining cookies.
            continue


def wait_for_source_table(page: Any, timeout_seconds: int = 60):
    source_table = None
    started_at = time.time()
    while True:
        if time.time() - started_at > timeout_seconds:
            raise K2PrintError("等待完整审批历史超时。")
        try:
            if page.ele(f"xpath:{SOURCE_TABLE_XPATH}", timeout=0.1):
                table = page.ele(f"xpath:{SOURCE_TABLE_XPATH}")
                if len(table.eles("tag:tr")) > 0:
                    source_table = table
                    break
        except Exception:
            pass

        try:
            page.run_js(TARGET_JS_FUNC)
            button = page.ele("text:完整历史记录", timeout=0.1)
            if button:
                button.click(by_js=True)
        except Exception:
            pass
        time.sleep(1)
    return source_table


def replace_table_and_print(page: Any, best_table_html: str, print_url: str, output_path: Path) -> None:
    page.get(print_url)
    try:
        page.wait.ele_displayed(f"xpath:{TARGET_TABLE_XPATH}", timeout=5)
    except Exception:
        pass

    raw_html = f'<div id="replaced-wrapper">{best_table_html}</div>'
    js = f"""
        var newHtml = arguments[0];
        var xpath = '{TARGET_TABLE_XPATH}';
        var result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        var target = result.singleNodeValue;

        if (target) {{
            var div = document.createElement('div');
            div.innerHTML = newHtml;
            target.parentNode.replaceChild(div, target);
            return true;
        }} else {{
            var fallback = document.querySelector('#approvalHistoryCt table');
            if (fallback) {{
                var div = document.createElement('div');
                div.innerHTML = newHtml;
                fallback.parentNode.replaceChild(div, fallback);
                return true;
            }}
            return false;
        }}
    """
    page.run_js(js, raw_html)

    print_options = {
        "landscape": False,
        "displayHeaderFooter": False,
        "printBackground": True,
        "preferCSSPageSize": True,
        "marginTop": 0,
        "marginBottom": 0,
        "marginLeft": 0,
        "marginRight": 0,
    }
    result = page.run_cdp("Page.printToPDF", **print_options)
    pdf_bytes = base64.b64decode(result["data"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(pdf_bytes)


def close_browser(process: subprocess.Popen[Any] | None, page: Any | None) -> None:
    if page is not None:
        for method_name in ("quit", "close"):
            method = getattr(page, method_name, None)
            if callable(method):
                try:
                    method()
                    break
                except Exception:
                    pass
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()


def export_approval_record_pdf(reference: str, output_path: str | Path) -> ExportResult:
    output_file = Path(output_path).resolve()
    match = resolve_reference(reference)
    print_url = generate_print_url(match.data_url)
    if not print_url:
        raise K2PrintError("无法从审批链接中解析 ProcInstID / key。")

    cookies = load_cookies_for_url(match.data_url, match.browser.browser_name)

    runtime_dir = output_file.parent / "_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    temp_user_data_dir = Path(tempfile.mkdtemp(prefix="k2_browser_", dir=runtime_dir))

    process: subprocess.Popen[Any] | None = None
    page: Any | None = None
    try:
        process, page = launch_browser(temp_user_data_dir, match.browser.browser_name)
        inject_cookies(page, match.data_url, cookies)
        page.get(match.data_url)
        source_table = wait_for_source_table(page)
        rows_count = len(source_table.eles("tag:tr"))
        replace_table_and_print(page, source_table.html, print_url, output_file)
        return ExportResult(
            reference=reference.strip(),
            data_url=match.data_url,
            print_url=print_url,
            output_path=output_file,
            browser_name=match.browser.browser_name,
            profile_name=match.browser.profile_name,
            rows_count=rows_count,
        )
    finally:
        close_browser(process, page)
        shutil.rmtree(temp_user_data_dir, ignore_errors=True)
        shutil.rmtree(runtime_dir, ignore_errors=True)


def main() -> None:
    print("📋 --- K2 审批完整记录打印助手 ---")
    reference = input("👉 请输入 K2 号或完整审批链接: ").strip()
    if not reference:
        return
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    save_path = get_base_path() / f"K2审批完整记录_{timestamp}.pdf"
    try:
        result = export_approval_record_pdf(reference, save_path)
    except K2PrintError as exc:
        print(f"❌ {exc}")
        input("按回车退出...")
        return

    print("✅ 导出成功")
    print(f"浏览器: {result.browser_name}/{result.profile_name}")
    print(f"审批链接: {result.data_url}")
    print(f"打印链接: {result.print_url}")
    print(f"记录行数: {result.rows_count}")
    print(f"输出文件: {result.output_path}")
    input("按回车退出...")


if __name__ == "__main__":
    main()
