#!/usr/bin/env python3
"""
Batch replace Name / Bank / Account values in PDFs while keeping layout unchanged as much as possible.

Dependency:
  pip install pymupdf
"""

from __future__ import annotations

import re
import sys
import threading
from queue import Empty, Queue
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import tkinter as tk
    from tkinter import messagebox, ttk
except Exception:  # noqa: BLE001
    tk = None
    messagebox = None
    ttk = None


def load_pymupdf():
    """Import PyMuPDF safely and detect the common wrong 'fitz' package case."""
    pymupdf_error = None
    fitz_error = None

    try:
        import pymupdf as _fitz  # PyMuPDF >= 1.24

        return _fitz
    except Exception as exc:  # noqa: BLE001
        pymupdf_error = exc

    try:
        import fitz as _fitz  # old-style import name for PyMuPDF

        required_attrs = ("open", "Rect", "TEXT_ALIGN_LEFT")
        if not all(hasattr(_fitz, attr) for attr in required_attrs):
            raise RuntimeError("detected non-PyMuPDF 'fitz' package")
        return _fitz
    except Exception as exc:  # noqa: BLE001
        fitz_error = exc

    raise RuntimeError(
        "无法导入 PyMuPDF。\n"
        "请先执行以下命令修复依赖：\n"
        "  python -m pip uninstall -y fitz frontend\n"
        "  python -m pip install -U pymupdf\n"
        "如果你使用的是 Python 3.14 且安装失败，请改用 Python 3.12/3.13 环境运行本脚本。\n"
        f"pymupdf 导入错误: {pymupdf_error}\n"
        f"fitz 导入错误: {fitz_error}"
    )


fitz = None


def ensure_pymupdf():
    global fitz
    if fitz is None:
        fitz = load_pymupdf()
    return fitz


TARGET_VALUES = {
    "name": "深圳市深湾汇云投资发展有限公司",
    "bank": "江苏银行深圳分行营业部",
    "account": "19200188000452207",
}

FIELD_ALIASES = {
    "name": ("名称", "name:"),
    "bank": ("银行", "bank:"),
    "account": ("账号", "account:"),
}

FIELD_ORDER = ("name", "bank", "account")

PUNCT_ONLY_RE = re.compile(r"^[\s:：\-—_]+$")


@dataclass
class Replacement:
    field: str
    redact_rect: fitz.Rect
    insert_rect: fitz.Rect
    value: str
    font_size: float


def norm_token(token: str) -> str:
    return token.strip().lower().replace("：", ":").replace(" ", "")


def token_is_punct(token: str) -> bool:
    return bool(PUNCT_ONLY_RE.match(token))


def token_has_field_alias(token: str, field: str) -> bool:
    nt = norm_token(token)
    if not nt:
        return False
    return any(nt.startswith(alias) for alias in FIELD_ALIASES[field])


def token_has_any_field_alias(token: str) -> bool:
    return any(token_has_field_alias(token, f) for f in FIELD_ORDER)


def find_label_idx(line_words: Sequence[Tuple], field: str) -> Optional[int]:
    for i, w in enumerate(line_words):
        if token_has_field_alias(str(w[4]), field):
            return i
    return None


def build_replacement_from_value_words(
    value_words: Sequence[Tuple],
    line_words_for_right_bound: Sequence[Tuple],
    next_label_idx: Optional[int],
    field: str,
    page_rect: fitz.Rect,
) -> Optional[Replacement]:
    if not value_words:
        return None

    x0 = min(float(w[0]) for w in value_words)
    y0 = min(float(w[1]) for w in value_words)
    x1 = max(float(w[2]) for w in value_words)
    y1 = max(float(w[3]) for w in value_words)

    redact_rect = fitz.Rect(x0 - 0.4, y0 - 0.4, x1 + 0.4, y1 + 0.4)

    if next_label_idx is not None:
        right_bound = float(line_words_for_right_bound[next_label_idx][0]) - 1.0
    else:
        right_bound = float(page_rect.x1) - 8.0
    right_bound = max(right_bound, x1 + 8.0)
    insert_rect = fitz.Rect(max(0, x0 - 0.2), y0 - 0.4, right_bound, y1 + 0.4)

    font_size = max(7.0, (y1 - y0) * 0.9)
    return Replacement(
        field=field,
        redact_rect=redact_rect,
        insert_rect=insert_rect,
        value=TARGET_VALUES[field],
        font_size=font_size,
    )


def field_value_looks_reasonable(field: str, value_words: Sequence[Tuple]) -> bool:
    text = "".join(str(w[4]).strip() for w in value_words).strip()
    if not text:
        return False

    if field == "account":
        digits = "".join(ch for ch in text if ch.isdigit())
        return len(digits) >= 6

    if field == "bank":
        low = text.lower()
        return ("银行" in text) or ("bank" in low)

    if field == "name":
        return len(text) >= 4 and not token_has_any_field_alias(text)

    return True


def extract_embedded_value_after_label(token: str, field: str) -> Optional[str]:
    normalized = token.strip().replace("：", ":")
    for alias in FIELD_ALIASES[field]:
        pattern = re.compile(rf"^{re.escape(alias)}\s*:?\s*(.+)$", re.IGNORECASE)
        match = pattern.match(normalized)
        if match:
            value = match.group(1).strip()
            if value:
                return value
    return None


def project_embedded_value_word(original_word: Tuple, value_text: str) -> Tuple:
    token_text = str(original_word[4])
    x0, y0, x1, y1 = map(float, original_word[:4])
    width = max(1.0, x1 - x0)

    idx = token_text.find(value_text)
    if idx < 0:
        idx = max(0, len(token_text) - len(value_text))
    token_len = max(1, len(token_text))
    ratio = max(0.0, min(1.0, idx / token_len))
    value_x0 = x0 + width * ratio

    return (value_x0, y0, x1, y1, value_text, original_word[5], original_word[6], original_word[7])


def build_lines(words: Sequence[Tuple[float, float, float, float, str, int, int, int]]) -> List[List[Tuple]]:
    line_map: Dict[Tuple[int, int], List[Tuple]] = {}
    for w in words:
        key = (int(w[5]), int(w[6]))
        line_map.setdefault(key, []).append(w)

    lines = []
    for _, line_words in line_map.items():
        line_words.sort(key=lambda x: (float(x[0]), int(x[7])))
        lines.append(line_words)

    lines.sort(key=lambda lw: (float(lw[0][1]), float(lw[0][0])))
    return lines


def find_replacement_on_line(
    line_words: Sequence[Tuple],
    field: str,
    page_rect: fitz.Rect,
) -> Optional[Replacement]:
    label_idx = find_label_idx(line_words, field)
    if label_idx is None:
        return None

    value_words: List[Tuple] = []
    next_label_idx = None

    for k in range(label_idx, len(line_words)):
        token = str(line_words[k][4])
        if not value_words:
            if token_has_field_alias(token, field):
                embedded_value = extract_embedded_value_after_label(token, field)
                if embedded_value:
                    value_words.append(project_embedded_value_word(line_words[k], embedded_value))
                continue
            if token_is_punct(token):
                continue
            value_words.append(line_words[k])
            continue

        if token_has_any_field_alias(token):
            next_label_idx = k
            break
        value_words.append(line_words[k])

    if not value_words:
        return None

    if not field_value_looks_reasonable(field, value_words):
        return None
    return build_replacement_from_value_words(
        value_words=value_words,
        line_words_for_right_bound=line_words,
        next_label_idx=next_label_idx,
        field=field,
        page_rect=page_rect,
    )


def find_replacement_on_next_lines(
    lines: Sequence[Sequence[Tuple]],
    line_index: int,
    field: str,
    page_rect: fitz.Rect,
) -> Optional[Replacement]:
    if line_index >= len(lines):
        return None
    current_line = lines[line_index]
    if find_label_idx(current_line, field) is None:
        return None

    current_y1 = max(float(w[3]) for w in current_line)
    for next_idx in range(line_index + 1, min(line_index + 4, len(lines))):
        candidate = lines[next_idx]
        if not candidate:
            continue

        candidate_y0 = min(float(w[1]) for w in candidate)
        if candidate_y0 - current_y1 > 40:
            break

        # Skip another label line and keep searching nearby lines.
        if any(token_has_any_field_alias(str(w[4])) for w in candidate):
            continue

        start = 0
        while start < len(candidate) and token_is_punct(str(candidate[start][4])):
            start += 1
        if start >= len(candidate):
            continue

        value_end = len(candidate) - 1
        next_label_idx = None
        for k in range(start, len(candidate)):
            if token_has_any_field_alias(str(candidate[k][4])):
                value_end = k - 1
                next_label_idx = k
                break
        if value_end < start:
            continue

        value_words = candidate[start : value_end + 1]
        if not field_value_looks_reasonable(field, value_words):
            continue

        return build_replacement_from_value_words(
            value_words=value_words,
            line_words_for_right_bound=candidate,
            next_label_idx=next_label_idx,
            field=field,
            page_rect=page_rect,
        )

    return None


def choose_non_embedded_chinese_font(page):
    """
    Reuse an existing font resource on the page to avoid embedding new fonts.
    Prefer Microsoft YaHei style resources only when they are not subset fonts.
    Subset fonts (e.g. 'AAAAAA+MicrosoftYaHei') may miss glyphs and cause dropped chars.
    """
    fonts = page.get_fonts()
    if fonts:
        # tuple layout: (xref, ext, type, basefont, resource_name, encoding)
        preferred_keywords = ("microsoftyahei", "yahei", "heiti", "simhei", "simsun")
        for kw in preferred_keywords:
            for font in fonts:
                basefont = str(font[3]).lower()
                resource_name = str(font[4])
                is_subset = "+" in basefont
                if kw in basefont and resource_name and not is_subset:
                    return resource_name

    # Last fallback: built-in CJK font name (no external file embedding).
    return "china-s"


def choose_digit_font(page):
    """
    Prefer Arial-like font for digits.
    Try non-embedded existing page resources first, then local Arial, then helv.
    """
    fonts = page.get_fonts()
    if fonts:
        preferred_keywords = ("arial", "liberationsans", "helvetica")
        for kw in preferred_keywords:
            for font in fonts:
                basefont = str(font[3]).lower()
                resource_name = str(font[4])
                is_subset = "+" in basefont
                if kw in basefont and resource_name and not is_subset:
                    return resource_name

    arial = Path(r"C:\Windows\Fonts\arial.ttf")
    if arial.exists():
        try:
            page.insert_font(fontname="digit-arial", fontfile=str(arial))
            return "digit-arial"
        except RuntimeError:
            pass

    return "helv"


def apply_replacements_to_pdf(src: Path, dst: Path) -> Tuple[bool, List[str]]:
    logs: List[str] = []
    found_fields: Dict[str, bool] = {k: False for k in FIELD_ORDER}

    doc = fitz.open(src)
    try:
        for page_idx in range(len(doc)):
            page = doc[page_idx]
            words = page.get_text("words")
            if not words:
                continue

            lines = build_lines(words)
            replacements: List[Replacement] = []
            for line_index, line_words in enumerate(lines):
                for field in FIELD_ORDER:
                    if found_fields[field]:
                        continue
                    repl = find_replacement_on_line(line_words, field, page.rect)
                    if repl is None:
                        repl = find_replacement_on_next_lines(lines, line_index, field, page.rect)
                    if repl is None:
                        continue
                    replacements.append(repl)
                    found_fields[field] = True

            if not replacements:
                continue

            for repl in replacements:
                page.add_redact_annot(repl.redact_rect, fill=(1, 1, 1))
            page.apply_redactions()

            chinese_fontname = choose_non_embedded_chinese_font(page)
            digit_fontname = choose_digit_font(page)

            for repl in replacements:
                # color stays black to match common statement documents.
                insert_point = fitz.Point(repl.insert_rect.x0, repl.insert_rect.y1 - 0.6)
                fontname = digit_fontname if repl.field == "account" else chinese_fontname
                try:
                    page.insert_text(
                        insert_point,
                        repl.value,
                        fontsize=repl.font_size,
                        fontname=fontname,
                        color=(0, 0, 0),
                    )
                except RuntimeError:
                    fallback_fontname = "helv" if repl.field == "account" else "china-s"
                    page.insert_text(
                        insert_point,
                        repl.value,
                        fontsize=repl.font_size,
                        fontname=fallback_fontname,
                        color=(0, 0, 0),
                    )

        missing = [f for f, ok in found_fields.items() if not ok]
        if missing:
            logs.append(f"未找到字段: {', '.join(missing)}")

        dst.parent.mkdir(parents=True, exist_ok=True)
        doc.save(dst, garbage=3, deflate=True)
    finally:
        doc.close()

    return len([f for f in found_fields.values() if f]) > 0, logs


def iter_pdfs(root: Path, recursive: bool) -> Iterable[Path]:
    if recursive:
        yield from root.rglob("*.pdf")
        yield from root.rglob("*.PDF")
    else:
        yield from root.glob("*.pdf")
        yield from root.glob("*.PDF")


def get_runtime_dir() -> Path:
    # One-file exe runs from sys.executable directory.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def collect_input_pdfs(runtime_dir: Path) -> List[Path]:
    candidates = sorted(set(iter_pdfs(runtime_dir, recursive=False)))
    result: List[Path] = []
    for p in candidates:
        if not p.is_file():
            continue
        # Avoid reprocessing files already generated by this tool.
        if p.name.lower().startswith("new_"):
            continue
        result.append(p)
    return result


class ReplacePdfApp:
    def __init__(self) -> None:
        if tk is None or messagebox is None or ttk is None:
            raise RuntimeError("tkinter unavailable in current runtime")
        self.runtime_dir = get_runtime_dir()
        self.root = tk.Tk()
        self.root.title("PDF 批量替换工具")
        self.root.geometry("560x220")
        self.root.resizable(False, False)

        self.queue: Queue[Tuple[str, object]] = Queue()
        self.worker: Optional[threading.Thread] = None

        self.status_var = tk.StringVar(value="准备就绪")
        self.detail_var = tk.StringVar(value=f"工作目录: {self.runtime_dir}")

        self._build_ui()
        self.root.after(100, self._poll_queue)

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=14)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="同目录 PDF 批量替换", font=("Microsoft YaHei UI", 12, "bold")).pack(anchor="w")
        ttk.Label(frame, textvariable=self.detail_var).pack(anchor="w", pady=(6, 8))

        self.progress = ttk.Progressbar(frame, orient="horizontal", mode="determinate", maximum=100, value=0)
        self.progress.pack(fill="x", pady=(4, 8))

        ttk.Label(frame, textvariable=self.status_var).pack(anchor="w", pady=(0, 10))

        btn_row = ttk.Frame(frame)
        btn_row.pack(fill="x")
        self.start_btn = ttk.Button(btn_row, text="开始处理", command=self.start)
        self.start_btn.pack(side="left")
        ttk.Button(btn_row, text="退出", command=self.root.destroy).pack(side="right")

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        self.start_btn.config(state="disabled")
        self.progress["value"] = 0
        self.status_var.set("正在初始化...")

        self.worker = threading.Thread(target=self._run_task, daemon=True)
        self.worker.start()

    def _run_task(self) -> None:
        try:
            ensure_pymupdf()
        except RuntimeError as exc:
            self.queue.put(("error", str(exc)))
            return

        pdfs = collect_input_pdfs(self.runtime_dir)
        total = len(pdfs)
        if total == 0:
            self.queue.put(("empty", self.runtime_dir))
            return

        changed = 0
        errors: List[str] = []
        for idx, pdf in enumerate(pdfs, start=1):
            dst = pdf.with_name(f"new_{pdf.name}")
            try:
                ok, logs = apply_replacements_to_pdf(pdf, dst)
                if ok:
                    changed += 1
                if logs:
                    errors.append(f"{pdf.name}: {'; '.join(logs)}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{pdf.name}: {exc}")

            self.queue.put(("progress", (idx, total, pdf.name)))

        self.queue.put(("done", (total, changed, errors, self.runtime_dir)))

    def _poll_queue(self) -> None:
        try:
            while True:
                event, payload = self.queue.get_nowait()
                if event == "progress":
                    idx, total, name = payload  # type: ignore[misc]
                    pct = (idx / total) * 100 if total else 0
                    self.progress["value"] = pct
                    self.status_var.set(f"处理中 ({idx}/{total}): {name}")
                elif event == "empty":
                    folder = payload
                    self.status_var.set("未找到可处理的 PDF")
                    self.start_btn.config(state="normal")
                    messagebox.showinfo("提示", f"同目录未找到可处理的 PDF 文件。\n目录：{folder}")
                elif event == "error":
                    self.status_var.set("依赖错误")
                    self.start_btn.config(state="normal")
                    messagebox.showerror("依赖错误", str(payload))
                elif event == "done":
                    total, changed, errors, folder = payload  # type: ignore[misc]
                    self.progress["value"] = 100
                    self.status_var.set(f"处理完成：{total} 个文件，替换成功 {changed} 个")
                    self.start_btn.config(state="normal")

                    msg = (
                        "处理完成。\n"
                        "输出文件已保存在同目录（文件名前缀为 new_）。\n"
                        f"目录：{folder}\n\n"
                        f"总文件：{total}\n"
                        f"成功替换：{changed}"
                    )
                    if errors:
                        preview = "\n".join(errors[:5])
                        if len(errors) > 5:
                            preview += f"\n... 其余 {len(errors) - 5} 条省略"
                        msg += f"\n\n注意事项：\n{preview}"
                    messagebox.showinfo("完成", msg)
        except Empty:
            pass
        finally:
            self.root.after(100, self._poll_queue)

    def run(self) -> None:
        self.root.mainloop()


def main() -> int:
    app = ReplacePdfApp()
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
