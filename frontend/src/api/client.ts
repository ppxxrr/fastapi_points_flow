const API_BASE = normalizeBaseUrl(readEnv("VITE_API_BASE_URL"));


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

    const response = await fetch(buildApiUrl(path), {
        ...init,
        headers,
        credentials: init.credentials ?? "include",
    });
    const payload = await parseResponse(response);

    if (!response.ok) {
        const detail = extractDetail(payload) || response.statusText || "请求失败";
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
        return payload;
    }

    if (typeof payload === "object" && payload !== null && "detail" in payload) {
        const detail = (payload as { detail?: unknown }).detail;
        if (typeof detail === "string") {
            return detail;
        }
        if (Array.isArray(detail)) {
            return detail
                .map((item) => {
                    if (typeof item === "string") {
                        return item;
                    }
                    if (item && typeof item === "object" && "msg" in item) {
                        return String((item as { msg?: unknown }).msg || "");
                    }
                    return "";
                })
                .filter(Boolean)
                .join("；");
        }
    }

    return "";
}
