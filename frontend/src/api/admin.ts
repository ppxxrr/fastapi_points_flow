import { apiRequest } from "./client";

export interface AdminTableCounts {
    member_profile: number;
    member_account: number;
    member_point_flow: number;
    parking_record: number;
    sync_job_state: number;
    sync_task_log: number;
}

export interface AdminServerStatus {
    web_status: string;
    environment: string;
    hostname: string;
    app_host: string;
    app_port: string;
    log_dir: string;
    point_flow_source_dir: string;
    parking_source_dir: string;
    database_backend: string;
}

export interface AdminDatabaseStatus {
    backend: string;
    connected: boolean;
    database_name: string | null;
    host: string | null;
    port: number | null;
    table_counts: AdminTableCounts;
}

export interface AdminRecentJob {
    id: number;
    job_name: string;
    job_date: string;
    status: string;
    retry_count: number;
    updated_at: string | null;
    last_error: string | null;
}

export interface AdminRecentFailure {
    id: number;
    module_name: string;
    action: string;
    status: string;
    target_value: string | null;
    error_message: string | null;
    finished_at: string | null;
}

export interface ParkingCsvSampleFile {
    file_name: string;
    is_valid_parking_csv: boolean;
    encoding: string | null;
    data_rows?: number;
    source_start_date?: string | null;
    source_end_date?: string | null;
}

export interface ParkingCsvSourceSummary {
    input_dir: string;
    exists: boolean;
    is_dir: boolean;
    total_files_found: number;
    valid_parking_csv_files: number;
    invalid_or_skipped_files: number;
    total_csv_data_rows: number;
    min_source_start_date: string | null;
    max_source_end_date: string | null;
    sample_files: ParkingCsvSampleFile[];
    cached_at: string | null;
}

export interface ParkingTableSummary {
    parking_record_count: number;
    distinct_source_file_count: number;
    null_mobile_no_count: number;
    null_plate_no_count: number;
    null_record_id_count: number;
    null_parking_serial_no_count: number;
    min_entry_time: string | null;
    max_exit_time: string | null;
}

export interface ParkingIntegritySummary {
    severity: "info" | "warning";
    integrity_pending: boolean;
    headline: string;
    table_summary: ParkingTableSummary;
    csv_source: ParkingCsvSourceSummary;
    risk_flags: string[];
}

export interface AdminOverviewResponse {
    status: string;
    generated_at: string | null;
    server: AdminServerStatus;
    database: AdminDatabaseStatus;
    sync: {
        recent_jobs: AdminRecentJob[];
        recent_failures: AdminRecentFailure[];
    };
    parking_integrity: ParkingIntegritySummary;
}

export async function getAdminOverview() {
    return apiRequest<AdminOverviewResponse>("/api/admin/overview");
}
