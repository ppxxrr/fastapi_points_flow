import { apiRequest, buildApiUrl } from "./client";


export type PointsFlowTaskStatus = "pending" | "running" | "success" | "failed";


export interface HealthResponse {
    status: string;
}


export interface PointsFlowTaskLog {
    time: string;
    level: string;
    message: string;
}


export interface PointsFlowTaskParams {
    username: string;
    start_date: string;
    end_date: string;
}


export interface PointsFlowTask {
    task_id: string;
    type: string;
    status: PointsFlowTaskStatus;
    created_at: string;
    updated_at: string;
    params: PointsFlowTaskParams;
    logs: PointsFlowTaskLog[];
    result_file: string | null;
    result_count: number;
    error: string | null;
}


export interface CreatePointsFlowTaskPayload {
    start_date: string;
    end_date: string;
}


export async function getHealthStatus() {
    return apiRequest<HealthResponse>("/health");
}


export async function createPointsFlowTask(payload: CreatePointsFlowTaskPayload) {
    return apiRequest<PointsFlowTask>("/api/points-flow/tasks", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}


export async function getPointsFlowTask(taskId: string) {
    return apiRequest<PointsFlowTask>(`/api/points-flow/tasks/${encodeURIComponent(taskId)}`);
}


export function shouldPollTask(task: PointsFlowTask | null) {
    return Boolean(task && (task.status === "pending" || task.status === "running"));
}


export function buildPointsFlowDownloadUrl(filename: string) {
    return buildApiUrl(`/api/points-flow/downloads/${encodeURIComponent(filename)}`);
}
