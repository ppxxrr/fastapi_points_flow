import { ApiError, apiRequest, buildApiUrl } from "./client";

export interface PdfEditorPage {
    page_id: string;
    file_id: string;
    source_file_name: string;
    page_index: number;
    page_number: number;
    width: number;
    height: number;
    thumbnail_url: string;
}

export interface PdfEditorOverlay {
    id: string;
    kind: "text" | "image" | "erase";
    x: number;
    y: number;
    width: number;
    height: number;
    text?: string;
    font_size?: number;
    data_url?: string;
}

export interface PdfEditorDocument {
    file_id: string;
    file_name: string;
    size_bytes: number;
    page_count: number;
    pages: PdfEditorPage[];
}

export interface PdfEditorUploadResponse {
    session_id: string;
    documents: PdfEditorDocument[];
    total_pages: number;
}

export interface ToolDownloadResponse {
    session_id: string;
    file_name: string;
    download_url: string;
    content_type: string;
}

export interface BankInfoUploadFile {
    file_id: string;
    file_name: string;
    size_bytes: number;
}

export interface BankInfoUploadResponse {
    session_id: string;
    files: BankInfoUploadFile[];
}

export interface BankInfoProcessedFile {
    file_name: string;
    output_file_name: string | null;
    download_url: string | null;
    success: boolean;
    messages: string[];
}

export interface BankInfoProcessResponse {
    session_id: string;
    processed_count: number;
    success_count: number;
    failed_count: number;
    files: BankInfoProcessedFile[];
    download_url: string | null;
}

export interface PptConverterUploadFile {
    file_id: string;
    file_name: string;
    size_bytes: number;
    slide_count: number;
}

export interface PptConverterUploadResponse {
    session_id: string;
    files: PptConverterUploadFile[];
    total_slides: number;
}

export interface PptConverterProcessStartResponse {
    session_id: string;
    job_id: string;
    status: string;
}

export interface PptConverterProcessedFile {
    file_name: string;
    slide_count: number;
    output_file_name: string | null;
    download_url: string | null;
    success: boolean;
    messages: string[];
}

export interface PptConverterJobStatusResponse {
    session_id: string;
    job_id: string;
    status: string;
    total_files: number;
    processed_files: number;
    total_slides: number;
    processed_slides: number;
    progress_percent: number;
    current_file_name: string | null;
    started_at: string | null;
    finished_at: string | null;
    error: string | null;
    files: PptConverterProcessedFile[];
}

export interface K2PrintJobLog {
    at: string;
    stage: string;
    level: string;
    message: string;
}

export interface K2PrintJobStartResponse {
    job_id: string;
    status: string;
}

export interface K2PrintJobStatusResponse {
    job_id: string;
    k2_no: string;
    status: string;
    stage: string;
    started_at: string | null;
    finished_at: string | null;
    resolved_workflow_url: string | null;
    resolved_print_url: string | null;
    download_url: string | null;
    error: string | null;
    logs: K2PrintJobLog[];
}

async function uploadWithForm<T>(path: string, files: File[], sessionId?: string | null) {
    const formData = new FormData();
    if (sessionId) {
        formData.append("session_id", sessionId);
    }
    files.forEach((file) => formData.append("files", file));

    const response = await fetch(buildApiUrl(path), {
        method: "POST",
        body: formData,
        credentials: "include",
    });

    const payload = await parseUploadResponse(response);
    if (!response.ok) {
        const detail =
            typeof payload === "string"
                ? payload
                : typeof payload?.detail === "string"
                  ? payload.detail
                  : response.status === 413
                    ? "上传文件过大，已被服务器拦截，请拆分文件或联系管理员提高上传上限。"
                    : "文件上传失败";
        throw new ApiError(detail, response.status, detail, payload);
    }

    return payload as T;
}

async function parseUploadResponse(response: Response) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
        return response.json();
    }

    const text = await response.text();
    if (response.status === 413) {
        return {
            detail: "上传文件过大，已被服务器拦截，请拆分文件或联系管理员提高上传上限。",
            raw: text,
        };
    }

    return {
        detail: text ? stripHtml(text).slice(0, 240) : "文件上传失败",
        raw: text,
    };
}

function stripHtml(value: string) {
    return value.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

export async function uploadPdfEditorFiles(files: File[], sessionId?: string | null) {
    return uploadWithForm<PdfEditorUploadResponse>("/api/tools/pdf-editor/uploads", files, sessionId);
}

export async function mergePdfEditorPages(
    sessionId: string,
    pages: Array<{ file_id: string; page_index: number; overlays?: PdfEditorOverlay[] }>,
) {
    return apiRequest<ToolDownloadResponse>("/api/tools/pdf-editor/merge", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            session_id: sessionId,
            pages,
        }),
    });
}

export async function uploadBankInfoFiles(files: File[], sessionId?: string | null) {
    return uploadWithForm<BankInfoUploadResponse>("/api/tools/bank-info/uploads", files, sessionId);
}

export async function processBankInfoFiles(sessionId: string) {
    return apiRequest<BankInfoProcessResponse>("/api/tools/bank-info/process", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            session_id: sessionId,
        }),
    });
}

export async function uploadPptConverterFiles(files: File[], sessionId?: string | null) {
    return uploadWithForm<PptConverterUploadResponse>("/api/tools/ppt-converter/uploads", files, sessionId);
}

export async function startPptConverter(sessionId: string) {
    return apiRequest<PptConverterProcessStartResponse>("/api/tools/ppt-converter/process", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            session_id: sessionId,
        }),
    });
}

export async function getPptConverterJob(jobId: string) {
    return apiRequest<PptConverterJobStatusResponse>(`/api/tools/ppt-converter/jobs/${jobId}`, {
        method: "GET",
    });
}

export async function startK2PrintExport(k2No: string) {
    return apiRequest<K2PrintJobStartResponse>("/api/tools/k2-print/exports", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            k2_no: k2No,
        }),
    });
}

export async function getK2PrintJob(jobId: string) {
    return apiRequest<K2PrintJobStatusResponse>(`/api/tools/k2-print/jobs/${jobId}`, {
        method: "GET",
    });
}

export function buildAssetUrl(path: string) {
    return buildApiUrl(path);
}
