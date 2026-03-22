import { apiRequest, buildApiUrl } from "./client";

export interface DeviceLayoutPointType {
    key: string;
    label: string;
}

export interface DeviceLayoutFloor {
    code: string;
    label: string;
    image_width: number;
    image_height: number;
}

export interface DeviceLayoutConfigResponse {
    default_point_type: string;
    default_floor_code: string;
    point_types: DeviceLayoutPointType[];
    floors: DeviceLayoutFloor[];
}

export interface DeviceLayoutPoint {
    point_type: string;
    point_code: string;
    point_name: string;
    floor_code: string;
    x_ratio: number | null;
    y_ratio: number | null;
}

export interface DeviceLayoutPointsResponse {
    point_type: string;
    points: DeviceLayoutPoint[];
}

export interface DeviceLayoutSaveResponse {
    point_type: string;
    saved_count: number;
}

export interface DeviceLayoutImportResponse {
    point_type: string;
    total_rows: number;
    created_count: number;
    updated_count: number;
    skipped_count: number;
}

export async function getDeviceLayoutConfig() {
    return apiRequest<DeviceLayoutConfigResponse>("/api/device-layout/config");
}

export async function getDeviceLayoutPoints(pointType: string) {
    return apiRequest<DeviceLayoutPointsResponse>(`/api/device-layout/points?point_type=${encodeURIComponent(pointType)}`);
}

export async function saveDeviceLayoutPoints(payload: {
    point_type: string;
    points: Array<{
        point_code: string;
        point_name?: string | null;
        floor_code: string;
        x_ratio: number | null;
        y_ratio: number | null;
    }>;
}) {
    return apiRequest<DeviceLayoutSaveResponse>("/api/device-layout/points/save", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}

export async function importDeviceLayoutPoints(payload: { pointType: string; file: File }) {
    const formData = new FormData();
    formData.append("point_type", payload.pointType);
    formData.append("file", payload.file);
    return apiRequest<DeviceLayoutImportResponse>("/api/device-layout/points/import", {
        method: "POST",
        body: formData,
    });
}

export function buildDeviceLayoutExportUrl(pointType: string) {
    return buildApiUrl(`/api/device-layout/export?point_type=${encodeURIComponent(pointType)}`);
}

export function buildDeviceLayoutMapUrl(floorCode: string) {
    return buildApiUrl(`/api/device-layout/maps/${encodeURIComponent(floorCode)}`);
}
