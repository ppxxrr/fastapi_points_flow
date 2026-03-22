from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Form, UploadFile, status
from fastapi.responses import FileResponse

from app.schemas import (
    BankInfoProcessRequest,
    BankInfoProcessResponse,
    BankInfoUploadResponse,
    K2PrintJobStartResponse,
    K2PrintJobStatusResponse,
    K2PrintStartRequest,
    PptConverterJobStatusResponse,
    PptConverterProcessRequest,
    PptConverterProcessStartResponse,
    PptConverterUploadResponse,
    PdfEditorMergeRequest,
    PdfEditorUploadResponse,
    ToolDownloadResponse,
)
from app.services.k2_print_service import K2PrintService
from app.services.pdf_tools_service import BANK_INFO_CACHE_ROOT, PDF_EDITOR_CACHE_ROOT, PPT_CONVERTER_CACHE_ROOT, PdfToolsService


router = APIRouter(prefix="/api/tools", tags=["tools"])
pdf_tools_service = PdfToolsService()
k2_print_service = K2PrintService()


@router.post("/pdf-editor/uploads", response_model=PdfEditorUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_pdf_editor_files(
    session_id: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
) -> PdfEditorUploadResponse:
    return pdf_tools_service.save_editor_uploads(files, session_id=session_id)


@router.post("/pdf-editor/merge", response_model=ToolDownloadResponse)
def merge_pdf_editor_pages(payload: PdfEditorMergeRequest) -> ToolDownloadResponse:
    ordered_pages = [item.model_dump() for item in payload.pages]
    return pdf_tools_service.create_merged_pdf(payload.session_id, ordered_pages)


@router.get("/pdf-editor/sessions/{session_id}/previews/{preview_name}")
def get_pdf_editor_preview(session_id: str, preview_name: str) -> FileResponse:
    file_path = pdf_tools_service.resolve_preview_file(session_id, preview_name)
    return FileResponse(path=file_path, media_type="image/png", filename=file_path.name)


@router.get("/pdf-editor/sessions/{session_id}/downloads/{file_name}")
def download_pdf_editor_result(session_id: str, file_name: str) -> FileResponse:
    file_path, media_type = pdf_tools_service.resolve_download_file(PDF_EDITOR_CACHE_ROOT, session_id, file_name)
    return FileResponse(path=file_path, media_type=media_type, filename=file_path.name)


@router.post("/bank-info/uploads", response_model=BankInfoUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_bank_info_files(
    session_id: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
) -> BankInfoUploadResponse:
    return pdf_tools_service.save_bank_uploads(files, session_id=session_id)


@router.post("/bank-info/process", response_model=BankInfoProcessResponse)
def process_bank_info(payload: BankInfoProcessRequest) -> BankInfoProcessResponse:
    return pdf_tools_service.process_bank_info(payload.session_id)


@router.get("/bank-info/sessions/{session_id}/downloads/{file_name}")
def download_bank_info_result(session_id: str, file_name: str) -> FileResponse:
    file_path, media_type = pdf_tools_service.resolve_download_file(BANK_INFO_CACHE_ROOT, session_id, file_name)
    return FileResponse(path=file_path, media_type=media_type, filename=file_path.name)


@router.post("/ppt-converter/uploads", response_model=PptConverterUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_ppt_converter_files(
    session_id: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
) -> PptConverterUploadResponse:
    return pdf_tools_service.save_ppt_uploads(files, session_id=session_id)


@router.post("/ppt-converter/process", response_model=PptConverterProcessStartResponse)
def start_ppt_converter(payload: PptConverterProcessRequest) -> PptConverterProcessStartResponse:
    return pdf_tools_service.start_ppt_conversion(payload.session_id)


@router.get("/ppt-converter/jobs/{job_id}", response_model=PptConverterJobStatusResponse)
def get_ppt_converter_job(job_id: str) -> PptConverterJobStatusResponse:
    return pdf_tools_service.get_ppt_job_status(job_id)


@router.get("/ppt-converter/sessions/{session_id}/downloads/{file_name}")
def download_ppt_converter_result(session_id: str, file_name: str) -> FileResponse:
    file_path, media_type = pdf_tools_service.resolve_download_file(PPT_CONVERTER_CACHE_ROOT, session_id, file_name)
    return FileResponse(path=file_path, media_type=media_type, filename=file_path.name)


@router.post("/k2-print/exports", response_model=K2PrintJobStartResponse)
def start_k2_print_export(payload: K2PrintStartRequest) -> K2PrintJobStartResponse:
    return k2_print_service.start_job(payload.k2_no)


@router.get("/k2-print/jobs/{job_id}", response_model=K2PrintJobStatusResponse)
def get_k2_print_job(job_id: str) -> K2PrintJobStatusResponse:
    return k2_print_service.get_job(job_id)


@router.get("/k2-print/jobs/{job_id}/downloads/{file_name}")
def download_k2_print_result(job_id: str, file_name: str) -> FileResponse:
    file_path, media_type = k2_print_service.resolve_download_file(job_id, file_name)
    return FileResponse(path=file_path, media_type=media_type, filename=file_path.name)
