const API_BASE = normalizeBaseUrl(readEnv("VITE_API_BASE_URL"));
const HTML_RESPONSE_RE = /^\s*(<!doctype html|<html[\s>]|<head[\s>]|<body[\s>])/i;
const MAX_ERROR_TEXT_LENGTH = 240;

export class ApiError extends Error {
    status: number;
    detail: string;
    payload?: unknown;

    constructor(message: string, status: number, detail?: string, payload?: unknown) {
        super(message);
        this.name = "ApiError";
        this.status = status;
        this.detail = detail || message;
        this.payload = payload;
    }
}

export function buildApiUrl(path: string) {
    if (/^https?:\/\//i.test(path)) {
        return path;
    }
    return `${API_BASE}${path}`;
}

export function getApiErrorMessage(error: unknown) {
    if (error instanceof ApiError) {
        return error.detail;
    }
    if (error instanceof Error) {
        return error.message;
    }
    return "请求失败，请稍后重试。";
}

export function isUnauthorizedError(error: unknown) {
    return error instanceof ApiError && error.status === 401;
}

export async function apiRequest<T>(path: string, init: RequestInit = {}) {
    const headers = new Headers(init.headers || {});
    if (!headers.has("Accept")) {
        headers.set("Accept", "application/json");
    }

    let response: Response;
    try {
        response = await fetch(buildApiUrl(path), {
            ...init,
            headers,
            credentials: init.credentials ?? "include",
        });
    } catch (error) {
        if (error instanceof Error) {
            throw new Error("网络请求失败，请检查网络连接后重试。");
        }
        throw error;
    }

    const payload = await parseResponse(response);

    if (!response.ok) {
        const detail = buildErrorDetail(response.status, response.statusText, payload);
        throw new ApiError(detail, response.status, detail, payload);
    }

    return payload as T;
}

function readEnv(key: string) {
    const meta = import.meta as ImportMeta & {
        env?: Record<string, string | undefined>;
    };
    return meta.env?.[key] || "";
}

function normalizeBaseUrl(value: string) {
    if (!value) {
        return "";
    }
    return value.endsWith("/") ? value.slice(0, -1) : value;
}

async function parseResponse(response: Response) {
    if (response.status === 204) {
        return null;
    }

    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
        return response.json();
    }

    const text = await response.text();
    return text ? text : null;
}

function extractDetail(payload: unknown) {
    if (!payload) {
        return "";
    }

    if (typeof payload === "string") {
        return sanitizeErrorText(payload);
    }

    if (typeof payload === "object" && payload !== null && "detail" in payload) {
        const detail = (payload as { detail?: unknown }).detail;
        if (typeof detail === "string") {
            return sanitizeErrorText(detail);
        }
        if (Array.isArray(detail)) {
            return detail
                .map((item) => {
                    if (typeof item === "string") {
                        return sanitizeErrorText(item);
                    }
                    if (item && typeof item === "object" && "msg" in item) {
                        return sanitizeErrorText(String((item as { msg?: unknown }).msg || ""));
                    }
                    return "";
                })
                .filter(Boolean)
                .join("；");
        }
    }

    return "";
}

function sanitizeErrorText(value: string) {
    const text = value.replace(/\s+/g, " ").trim();
    if (!text || HTML_RESPONSE_RE.test(text)) {
        return "";
    }
    if (text.length <= MAX_ERROR_TEXT_LENGTH) {
        return text;
    }
    return `${text.slice(0, MAX_ERROR_TEXT_LENGTH).trim()}...`;
}

function buildErrorDetail(status: number, statusText: string, payload: unknown) {
    const detail = extractDetail(payload);
    if (detail) {
        return detail;
    }
    return buildStatusMessage(status, statusText);
}

function buildStatusMessage(status: number, statusText: string) {
    switch (status) {
        case 401:
            return "登录已过期，请重新登录。";
        case 403:
            return "没有权限执行该操作。";
        case 404:
            return "请求的资源不存在。";
        case 408:
            return "请求超时，请稍后重试。";
        case 429:
            return "请求过于频繁，请稍后再试。";
        case 500:
            return "服务器处理失败，请稍后重试。";
        case 502:
            return "网关异常（502），请稍后重试。";
        case 503:
            return "服务暂时不可用（503），请稍后重试。";
        case 504:
            return "服务响应超时（504），请稍后重试。";
        default:
            return statusText ? `请求失败（${status} ${statusText}）。` : `请求失败（${status}）。`;
    }
}
