import { apiRequest } from "./client";

export type ReconciliationTaskKind = "new_icsp_dz" | "coupon_tool";

export interface ReconciliationTaskLog {
    time: string;
    level: string;
    message: string;
}

export interface ReconciliationTaskResponse {
    task_id: string;
    type: ReconciliationTaskKind;
    status: "pending" | "running" | "success" | "failed";
    created_at: string;
    updated_at: string;
    params: {
        kind: ReconciliationTaskKind;
        start_date: string;
        end_date: string;
        project_key?: string | null;
        upload_session_id?: string | null;
    };
    logs: ReconciliationTaskLog[];
    result_file: string | null;
    result_count: number;
    error: string | null;
}

export interface ReconciliationConfigResponse {
    fixed_username: string;
    parking_projects: Array<{
        key: string;
        label: string;
        enable_parking: boolean;
    }>;
}

export interface ReconciliationParkingCaptchaResponse {
    project_key: string;
    captcha_uuid: string;
    image_base64: string;
    expires_in_seconds: number;
}

export interface ReconciliationWechatCsvUploadResponse {
    session_id: string;
    fund_file_name: string;
    trade_file_name: string;
}

export async function getReconciliationConfig() {
    return apiRequest<ReconciliationConfigResponse>("/api/reconciliation/config");
}

export async function getParkingCaptcha(projectKey: string) {
    return apiRequest<ReconciliationParkingCaptchaResponse>(
        `/api/reconciliation/parking-captcha?project_key=${encodeURIComponent(projectKey)}`,
    );
}

export async function uploadWechatCsvs(payload: {
    wechatFundCsv: File;
    wechatTradeCsv: File;
}) {
    const formData = new FormData();
    formData.append("wechat_fund_csv", payload.wechatFundCsv);
    formData.append("wechat_trade_csv", payload.wechatTradeCsv);
    return apiRequest<ReconciliationWechatCsvUploadResponse>("/api/reconciliation/wechat-csvs", {
        method: "POST",
        body: formData,
    });
}

export async function createReconciliationTask(payload: {
    kind: ReconciliationTaskKind;
    start_date: string;
    end_date: string;
    project_key?: string | null;
    captcha_code?: string | null;
    captcha_uuid?: string | null;
    upload_session_id?: string | null;
}) {
    return apiRequest<ReconciliationTaskResponse>("/api/reconciliation/tasks", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}

export async function getReconciliationTask(taskId: string) {
    return apiRequest<ReconciliationTaskResponse>(`/api/reconciliation/tasks/${taskId}`);
}

export function buildReconciliationDownloadUrl(filename: string) {
    return `/api/reconciliation/downloads/${encodeURIComponent(filename)}`;
}
