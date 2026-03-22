from __future__ import annotations

import json
import mimetypes
import os
import secrets
import shutil
import threading
import base64
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile, status

from app.schemas import (
    BankInfoProcessResponse,
    BankInfoProcessedFileResponse,
    BankInfoUploadResponse,
    PptConverterJobStatusResponse,
    PptConverterProcessedFileResponse,
    PptConverterProcessStartResponse,
    PptConverterUploadFileResponse,
    PptConverterUploadResponse,
    PdfEditorDocumentResponse,
    PdfEditorPageResponse,
    PdfEditorUploadResponse,
    ToolDownloadResponse,
    ToolFileResponse,
)

from replace_pdf_bank_info import apply_replacements_to_pdf, ensure_pymupdf
from app.services.ppt_remote_worker_client import PptRemoteWorkerClient
from app.services.ppt_three_panel import (
    convert_presentation_to_three_panel,
    count_presentation_slides,
    validate_pptx_filename,
)


BASE_DIR = Path(__file__).resolve().parents[2]
TOOL_CACHE_ROOT = BASE_DIR / "data" / "tool_cache"
PDF_EDITOR_CACHE_ROOT = TOOL_CACHE_ROOT / "pdf_editor"
BANK_INFO_CACHE_ROOT = TOOL_CACHE_ROOT / "bank_info"
PPT_CONVERTER_CACHE_ROOT = TOOL_CACHE_ROOT / "ppt_converter"
K2_PRINT_CACHE_ROOT = TOOL_CACHE_ROOT / "k2_print"
MARKER_FILE = TOOL_CACHE_ROOT / ".last_cleanup"
CACHE_TTL = timedelta(days=1)
PPT_JOB_STATE_LOCK = threading.Lock()
PPT_JOB_STATES: dict[str, dict[str, Any]] = {}


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utcnow().isoformat()


def sanitize_filename(filename: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {".", "-", "_"} else "_" for char in filename)
    return cleaned or "document.pdf"


def validate_pdf_filename(filename: str) -> None:
    if not filename or Path(filename).suffix.lower() != ".pdf":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only PDF files are supported")


class PdfToolsService:
    def __init__(self) -> None:
        for path in (TOOL_CACHE_ROOT, PDF_EDITOR_CACHE_ROOT, BANK_INFO_CACHE_ROOT, PPT_CONVERTER_CACHE_ROOT, K2_PRINT_CACHE_ROOT):
            path.mkdir(parents=True, exist_ok=True)
        self.ppt_remote_worker = PptRemoteWorkerClient()

    def ensure_daily_cleanup(self) -> None:
        today = date.today().isoformat()
        if MARKER_FILE.exists():
            try:
                last_cleanup = MARKER_FILE.read_text(encoding="utf-8").strip()
            except OSError:
                last_cleanup = ""
            if last_cleanup == today:
                return

        expire_before = utcnow() - CACHE_TTL
        for root in (PDF_EDITOR_CACHE_ROOT, BANK_INFO_CACHE_ROOT, PPT_CONVERTER_CACHE_ROOT, K2_PRINT_CACHE_ROOT):
            for child in root.iterdir():
                if not child.is_dir():
                    continue
                modified_at = datetime.fromtimestamp(child.stat().st_mtime, tz=UTC)
                if modified_at < expire_before:
                    shutil.rmtree(child, ignore_errors=True)

        MARKER_FILE.write_text(today, encoding="utf-8")

    def save_editor_uploads(self, files: list[UploadFile], session_id: str | None = None) -> PdfEditorUploadResponse:
        self.ensure_daily_cleanup()
        fitz = ensure_pymupdf()
        session_id = session_id or self._generate_session_id("editor")
        session_dir = self._prepare_session_dir(PDF_EDITOR_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"
        previews_dir = session_dir / "previews"

        documents: list[PdfEditorDocumentResponse] = []
        total_pages = 0
        for upload in files:
            validate_pdf_filename(upload.filename or "")
            file_id = secrets.token_hex(8)
            original_name = sanitize_filename(upload.filename or "document.pdf")
            stored_path = uploads_dir / f"{file_id}.pdf"
            size_bytes = self._write_upload(upload, stored_path)
            document = fitz.open(stored_path)
            try:
                pages: list[PdfEditorPageResponse] = []
                for page_index in range(document.page_count):
                    page = document.load_page(page_index)
                    preview_name = f"{file_id}_{page_index + 1}.png"
                    preview_path = previews_dir / preview_name
                    pixmap = page.get_pixmap(matrix=fitz.Matrix(0.26, 0.26), alpha=False)
                    pixmap.save(preview_path)
                    pages.append(
                        PdfEditorPageResponse(
                            page_id=f"{file_id}:{page_index}",
                            file_id=file_id,
                            source_file_name=original_name,
                            page_index=page_index,
                            page_number=page_index + 1,
                            width=float(page.rect.width),
                            height=float(page.rect.height),
                            thumbnail_url=f"/api/tools/pdf-editor/sessions/{session_id}/previews/{preview_name}",
                        )
                    )
                documents.append(
                    PdfEditorDocumentResponse(
                        file_id=file_id,
                        file_name=original_name,
                        size_bytes=size_bytes,
                        page_count=document.page_count,
                        pages=pages,
                    )
                )
                total_pages += document.page_count
            finally:
                document.close()

        self._touch(session_dir)
        self._write_metadata(
            session_dir / "editor_uploads.json",
            {
                "session_id": session_id,
                "updated_at": iso_now(),
                "documents": [document.model_dump() for document in documents],
                "total_pages": total_pages,
            },
        )
        return PdfEditorUploadResponse(session_id=session_id, documents=documents, total_pages=total_pages)

    def create_merged_pdf(self, session_id: str, ordered_pages: list[dict[str, Any]]) -> ToolDownloadResponse:
        self.ensure_daily_cleanup()
        if not ordered_pages:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least one PDF page is required")

        fitz = ensure_pymupdf()
        session_dir = self._resolve_session_dir(PDF_EDITOR_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"
        downloads_dir = session_dir / "downloads"
        merged_path = downloads_dir / f"merged_{utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"

        output_doc = fitz.open()
        try:
            for item in ordered_pages:
                file_id = str(item.get("file_id", "")).strip()
                page_index = int(item.get("page_index", -1))
                source_path = uploads_dir / f"{file_id}.pdf"
                if not source_path.exists():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"PDF file not found: {file_id}")

                source_doc = fitz.open(source_path)
                try:
                    if page_index < 0 or page_index >= source_doc.page_count:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid PDF page index")
                    output_doc.insert_pdf(source_doc, from_page=page_index, to_page=page_index)
                    inserted_page = output_doc.load_page(output_doc.page_count - 1)
                    self._apply_pdf_overlays(inserted_page, item.get("overlays") or [])
                finally:
                    source_doc.close()
            output_doc.save(merged_path, garbage=3, deflate=True)
        finally:
            output_doc.close()

        self._touch(session_dir)
        return ToolDownloadResponse(
            session_id=session_id,
            file_name=merged_path.name,
            download_url=f"/api/tools/pdf-editor/sessions/{session_id}/downloads/{merged_path.name}",
            content_type="application/pdf",
        )

    def _apply_pdf_overlays(self, page, overlays: list[dict[str, Any]]) -> None:
        if not overlays:
            return
        fitz = ensure_pymupdf()
        for overlay in overlays:
            kind = str(overlay.get("kind", "")).strip()
            x = float(overlay.get("x", 0))
            y = float(overlay.get("y", 0))
            width = float(overlay.get("width", 0))
            height = float(overlay.get("height", 0))
            if width <= 0 or height <= 0:
                continue

            rect = fitz.Rect(x, y, x + width, y + height)
            if kind == "erase":
                page.draw_rect(rect, color=None, fill=(1, 1, 1), overlay=True)
                continue

            if kind == "text":
                text = str(overlay.get("text") or "").strip()
                if not text:
                    continue
                font_size = float(overlay.get("font_size") or 14)
                page.insert_textbox(rect, text, fontsize=font_size, fontname="helv", color=(0, 0, 0), overlay=True)
                continue

            if kind == "image":
                data_url = str(overlay.get("data_url") or "")
                if "," not in data_url:
                    continue
                try:
                    _, encoded = data_url.split(",", 1)
                    image_bytes = base64.b64decode(encoded)
                except Exception:
                    continue
                page.insert_image(rect, stream=image_bytes, overlay=True, keep_proportion=False)

    def save_bank_uploads(self, files: list[UploadFile], session_id: str | None = None) -> BankInfoUploadResponse:
        self.ensure_daily_cleanup()
        session_id = session_id or self._generate_session_id("bank")
        session_dir = self._prepare_session_dir(BANK_INFO_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"

        saved_files: list[ToolFileResponse] = []
        for upload in files:
            validate_pdf_filename(upload.filename or "")
            file_id = secrets.token_hex(8)
            original_name = sanitize_filename(upload.filename or "document.pdf")
            stored_path = uploads_dir / f"{file_id}_{original_name}"
            size_bytes = self._write_upload(upload, stored_path)
            saved_files.append(ToolFileResponse(file_id=file_id, file_name=original_name, size_bytes=size_bytes))

        self._touch(session_dir)
        self._write_metadata(
            session_dir / "bank_uploads.json",
            {
                "session_id": session_id,
                "updated_at": iso_now(),
                "files": [item.model_dump() for item in saved_files],
            },
        )
        return BankInfoUploadResponse(session_id=session_id, files=saved_files)

    def save_ppt_uploads(self, files: list[UploadFile], session_id: str | None = None) -> PptConverterUploadResponse:
        self.ensure_daily_cleanup()
        session_id = session_id or self._generate_session_id("ppt")
        if session_id:
            existing_session_dir = PPT_CONVERTER_CACHE_ROOT / Path(session_id).name
            if existing_session_dir.exists():
                shutil.rmtree(existing_session_dir, ignore_errors=True)
        session_dir = self._prepare_session_dir(PPT_CONVERTER_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"

        saved_files: list[PptConverterUploadFileResponse] = []
        total_slides = 0
        for upload in files:
            validate_pptx_filename(upload.filename or "")
            file_id = secrets.token_hex(8)
            original_name = sanitize_filename(upload.filename or "presentation.pptx")
            stored_path = uploads_dir / f"{file_id}_{original_name}"
            size_bytes = self._write_upload(upload, stored_path)
            slide_count = count_presentation_slides(stored_path)
            total_slides += slide_count
            saved_files.append(
                PptConverterUploadFileResponse(
                    file_id=file_id,
                    file_name=original_name,
                    size_bytes=size_bytes,
                    slide_count=slide_count,
                )
            )

        self._touch(session_dir)
        self._write_metadata(
            session_dir / "ppt_uploads.json",
            {
                "session_id": session_id,
                "updated_at": iso_now(),
                "files": [item.model_dump() for item in saved_files],
                "total_slides": total_slides,
            },
        )
        return PptConverterUploadResponse(session_id=session_id, files=saved_files, total_slides=total_slides)

    def start_ppt_conversion(self, session_id: str) -> PptConverterProcessStartResponse:
        self.ensure_daily_cleanup()
        session_dir = self._resolve_session_dir(PPT_CONVERTER_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"
        uploads = sorted(path for path in uploads_dir.glob("*.pptx") if path.is_file())
        if not uploads:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No uploaded PPT files found")

        total_slides = sum(count_presentation_slides(path) for path in uploads)
        job_id = self._generate_session_id("ppt_job")
        job_state = {
            "session_id": session_id,
            "job_id": job_id,
            "status": "running",
            "total_files": len(uploads),
            "processed_files": 0,
            "total_slides": total_slides,
            "processed_slides": 0,
            "progress_percent": 0.0,
            "current_file_name": uploads[0].name if uploads else None,
            "started_at": iso_now(),
            "finished_at": None,
            "error": None,
            "files": [],
        }
        with PPT_JOB_STATE_LOCK:
            PPT_JOB_STATES[job_id] = job_state

        worker = threading.Thread(
            target=self._run_ppt_conversion_job,
            args=(job_id, session_dir, uploads),
            daemon=True,
        )
        worker.start()
        self._touch(session_dir)
        return PptConverterProcessStartResponse(session_id=session_id, job_id=job_id, status="running")

    def get_ppt_job_status(self, job_id: str) -> PptConverterJobStatusResponse:
        self.ensure_daily_cleanup()
        with PPT_JOB_STATE_LOCK:
            job_state = PPT_JOB_STATES.get(job_id)
            if job_state is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PPT job not found")
            payload = json.loads(json.dumps(job_state))
        return PptConverterJobStatusResponse(**payload)

    def process_bank_info(self, session_id: str) -> BankInfoProcessResponse:
        self.ensure_daily_cleanup()
        ensure_pymupdf()
        session_dir = self._resolve_session_dir(BANK_INFO_CACHE_ROOT, session_id)
        uploads_dir = session_dir / "uploads"
        downloads_dir = session_dir / "downloads"

        uploads = sorted(path for path in uploads_dir.glob("*.pdf") if path.is_file())
        if not uploads:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No uploaded PDF files found")

        results: list[BankInfoProcessedFileResponse] = []
        for source_path in uploads:
            output_path = downloads_dir / f"new_{source_path.name}"
            try:
                success, logs = apply_replacements_to_pdf(source_path, output_path)
                if not success:
                    friendly_logs = ["上传文件不是正确Ole账单"]
                else:
                    friendly_logs = logs or ["处理完成"]
                results.append(
                    BankInfoProcessedFileResponse(
                        file_name=source_path.name,
                        output_file_name=output_path.name,
                        download_url=(
                            f"/api/tools/bank-info/sessions/{session_id}/downloads/{output_path.name}"
                            if output_path.exists()
                            else None
                        ),
                        success=bool(success),
                        messages=friendly_logs,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    BankInfoProcessedFileResponse(
                        file_name=source_path.name,
                        output_file_name=None,
                        download_url=None,
                        success=False,
                        messages=[str(exc)],
                    )
                )

        self._touch(session_dir)
        success_count = sum(1 for item in results if item.success)
        first_download = next((item.download_url for item in results if item.success and item.download_url), None)
        return BankInfoProcessResponse(
            session_id=session_id,
            processed_count=len(results),
            success_count=success_count,
            failed_count=len(results) - success_count,
            files=results,
            download_url=first_download,
        )

    def resolve_preview_file(self, session_id: str, preview_name: str) -> Path:
        self.ensure_daily_cleanup()
        session_dir = self._resolve_session_dir(PDF_EDITOR_CACHE_ROOT, session_id)
        path = (session_dir / "previews" / Path(preview_name).name).resolve()
        self._ensure_in_session(path, session_dir)
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Preview not found")
        self._touch(session_dir)
        return path

    def resolve_download_file(self, cache_root: Path, session_id: str, file_name: str) -> tuple[Path, str]:
        self.ensure_daily_cleanup()
        session_dir = self._resolve_session_dir(cache_root, session_id)
        path = (session_dir / "downloads" / Path(file_name).name).resolve()
        self._ensure_in_session(path, session_dir)
        if not path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Download file not found")
        self._touch(session_dir)
        return path, mimetypes.guess_type(path.name)[0] or "application/octet-stream"

    def _generate_session_id(self, prefix: str) -> str:
        return f"{prefix}_{utcnow().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(6)}"

    def _prepare_session_dir(self, cache_root: Path, session_id: str) -> Path:
        session_dir = cache_root / session_id
        (session_dir / "uploads").mkdir(parents=True, exist_ok=True)
        (session_dir / "previews").mkdir(parents=True, exist_ok=True)
        (session_dir / "downloads").mkdir(parents=True, exist_ok=True)
        (session_dir / "outputs").mkdir(parents=True, exist_ok=True)
        return session_dir

    def _resolve_session_dir(self, cache_root: Path, session_id: str) -> Path:
        safe_session_id = Path(session_id).name
        session_dir = (cache_root / safe_session_id).resolve()
        self._ensure_in_session(session_dir, cache_root.resolve())
        if not session_dir.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tool session not found")
        return session_dir

    def _ensure_in_session(self, path: Path, session_root: Path) -> None:
        try:
            path.relative_to(session_root)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file path") from exc

    def _write_upload(self, upload: UploadFile, target_path: Path) -> int:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        with target_path.open("wb") as buffer:
            while chunk := upload.file.read(1024 * 1024):
                buffer.write(chunk)
                total += len(chunk)
        upload.file.close()
        return total

    def _touch(self, session_dir: Path) -> None:
        now = datetime.now().timestamp()
        os.utime(session_dir, (now, now))

    def _write_metadata(self, path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _run_ppt_conversion_remote_job(self, job_id: str, session_dir: Path, uploads: list[Path]) -> None:
        downloads_dir = session_dir / "downloads"
        remote_job = self.ppt_remote_worker.start_job(uploads)
        remote_job_id = str(remote_job.get("job_id") or "").strip()
        if not remote_job_id:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Remote PPT worker did not return a job id")

        update_payload = {
            "status": "running",
            "current_file_name": uploads[0].name if uploads else None,
            "processed_files": 0,
            "processed_slides": 0,
            "progress_percent": 0.0,
            "error": None,
            "files": [],
            "worker_job_id": remote_job_id,
        }
        self._update_ppt_job(job_id, **update_payload)

        while True:
            remote_status = self.ppt_remote_worker.get_job(remote_job_id)
            mapped_files = []
            for item in remote_status.get("files", []) or []:
                local_download_url = None
                output_name = item.get("output_file_name")
                if item.get("success") and output_name:
                    local_download_url = f"/api/tools/ppt-converter/sessions/{session_dir.name}/downloads/{Path(output_name).name}"
                mapped_files.append(
                    PptConverterProcessedFileResponse(
                        file_name=str(item.get("file_name") or ""),
                        slide_count=int(item.get("slide_count") or 0),
                        output_file_name=output_name,
                        download_url=local_download_url,
                        success=bool(item.get("success")),
                        messages=[str(msg) for msg in (item.get("messages") or [])],
                    ).model_dump()
                )

            self._update_ppt_job(
                job_id,
                status=str(remote_status.get("status") or "running"),
                total_files=int(remote_status.get("total_files") or len(uploads)),
                processed_files=int(remote_status.get("processed_files") or 0),
                total_slides=int(remote_status.get("total_slides") or 0),
                processed_slides=int(remote_status.get("processed_slides") or 0),
                progress_percent=float(remote_status.get("progress_percent") or 0.0),
                current_file_name=remote_status.get("current_file_name"),
                started_at=remote_status.get("started_at"),
                finished_at=remote_status.get("finished_at"),
                error=remote_status.get("error"),
                files=mapped_files,
            )

            status_value = str(remote_status.get("status") or "running")
            if status_value in {"success", "failed"}:
                if status_value == "success":
                    for item in remote_status.get("files", []) or []:
                        if not item.get("success") or not item.get("output_file_name"):
                            continue
                        output_name = Path(str(item["output_file_name"])).name
                        self.ppt_remote_worker.download_file(remote_job_id, output_name, downloads_dir / output_name)
                break

            threading.Event().wait(1.0)

    def _run_ppt_conversion_job(self, job_id: str, session_dir: Path, uploads: list[Path]) -> None:
        if self.ppt_remote_worker.enabled:
            try:
                self._run_ppt_conversion_remote_job(job_id, session_dir, uploads)
                self._touch(session_dir)
            except Exception as exc:  # noqa: BLE001
                self._update_ppt_job(
                    job_id,
                    status="failed",
                    error=str(exc),
                    current_file_name=None,
                    finished_at=iso_now(),
                )
            return

        downloads_dir = session_dir / "downloads"
        processed_results: list[dict[str, Any]] = []
        processed_slides_base = 0

        try:
            for file_index, source_path in enumerate(uploads, start=1):
                slide_count = count_presentation_slides(source_path)
                output_path = downloads_dir / f"{source_path.stem}_三分屏版.pptx"
                self._update_ppt_job(
                    job_id,
                    current_file_name=source_path.name,
                    processed_files=file_index - 1,
                )

                try:
                    def progress_callback(done: int, total: int) -> None:
                        total_done = processed_slides_base + min(done, total)
                        self._update_ppt_job(
                            job_id,
                            processed_slides=total_done,
                            progress_percent=self._calc_progress(total_done, self._get_ppt_job_total_slides(job_id)),
                        )

                    convert_presentation_to_three_panel(source_path, output_path, progress_callback=progress_callback)
                    processed_results.append(
                        PptConverterProcessedFileResponse(
                            file_name=source_path.name,
                            slide_count=slide_count,
                            output_file_name=output_path.name,
                            download_url=f"/api/tools/ppt-converter/sessions/{session_dir.name}/downloads/{output_path.name}",
                            success=True,
                            messages=["转换成功"],
                        ).model_dump()
                    )
                except Exception as exc:  # noqa: BLE001
                    processed_results.append(
                        PptConverterProcessedFileResponse(
                            file_name=source_path.name,
                            slide_count=slide_count,
                            output_file_name=None,
                            download_url=None,
                            success=False,
                            messages=[str(exc)],
                        ).model_dump()
                    )

                processed_slides_base += slide_count
                self._update_ppt_job(
                    job_id,
                    files=processed_results,
                    processed_files=file_index,
                    processed_slides=processed_slides_base,
                    progress_percent=self._calc_progress(processed_slides_base, self._get_ppt_job_total_slides(job_id)),
                )

            self._update_ppt_job(
                job_id,
                status="success",
                current_file_name=None,
                finished_at=iso_now(),
                files=processed_results,
            )
            self._touch(session_dir)
        except Exception as exc:  # noqa: BLE001
            self._update_ppt_job(
                job_id,
                status="failed",
                error=str(exc),
                current_file_name=None,
                finished_at=iso_now(),
                files=processed_results,
            )

    def _get_ppt_job_total_slides(self, job_id: str) -> int:
        with PPT_JOB_STATE_LOCK:
            job_state = PPT_JOB_STATES.get(job_id, {})
            return int(job_state.get("total_slides", 0) or 0)

    def _update_ppt_job(self, job_id: str, **changes: Any) -> None:
        with PPT_JOB_STATE_LOCK:
            job_state = PPT_JOB_STATES.get(job_id)
            if job_state is None:
                return
            job_state.update(changes)

    def _calc_progress(self, processed_slides: int, total_slides: int) -> float:
        if total_slides <= 0:
            return 100.0
        return round((processed_slides / total_slides) * 100, 2)
