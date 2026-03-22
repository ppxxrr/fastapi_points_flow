import { apiRequest } from "./client";

export interface AdminTableCounts {
    member_profile: number;
    member_account: number;
    member_point_flow: number;
    parking_record: number;
    parking_trade_record: number;
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

export interface AdminParkingSyncLog {
    id: number;
    module_name: string;
    action: string;
    status: string;
    target_value: string | null;
    error_message: string | null;
    started_at: string | null;
    finished_at: string | null;
}

export interface AdminParkingSyncJob {
    id: number;
    job_name: string;
    job_date: string;
    status: string;
    retry_count: number;
    updated_at: string | null;
    last_error: string | null;
}

export interface AdminParkingSyncSummary {
    target_job_date: string;
    target_job: AdminParkingSyncJob | null;
    latest_business_job: AdminParkingSyncJob | null;
    latest_wrapper_job: AdminParkingSyncJob | null;
    recent_logs: AdminParkingSyncLog[];
    attention_required: boolean;
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
    parking_sync: AdminParkingSyncSummary;
    parking_trade_sync: AdminParkingSyncSummary;
    parking_integrity: ParkingIntegritySummary;
}

export interface BiSummaryBlock {
    parking_count?: number;
    plate_count?: number;
    mobile_count?: number;
    member_count?: number;
    total_fee_yuan?: number;
    avg_duration_minutes?: number;
    trade_count?: number;
    actual_value_yuan?: number;
    discount_yuan?: number;
    fee_yuan?: number;
    flow_count?: number;
    consume_amount_yuan?: number;
    signed_points?: number;
    positive_points?: number;
    negative_points?: number;
}

export interface BiValidationMetric {
    metric: string;
    value: string;
    description: string;
}

export interface BiLinkageItem {
    name: string;
    value: number;
}

export interface BiDailySeriesItem {
    date: string;
    parking_count: number;
    parking_fee_yuan: number;
    matched_mobile_count: number;
    matched_member_count: number;
    trade_count: number;
    trade_amount_yuan: number;
    trade_discount_yuan: number;
    point_flow_count: number;
    consume_amount_yuan: number;
    positive_points: number;
    negative_points: number;
}

export interface BiPlazaRankingItem {
    plaza_name: string;
    parking_count: number;
    trade_count: number;
    trade_amount_yuan: number;
    point_flow_count: number;
    consume_amount_yuan: number;
    matched_member_count: number;
}

export interface BiLevelDistributionItem {
    level_name: string;
    member_count: number;
    parking_members: number;
    trade_members: number;
    point_members: number;
}

export interface BiBucketItem {
    bucket: string;
    count: number;
}

export interface BiHourlyItem {
    hour: string;
    parking_count: number;
    trade_count: number;
    point_flow_count: number;
}

export interface BiNamedAmountItem {
    name: string;
    count: number;
    amount_yuan: number;
}

export interface BiPolicyImpactSummary {
    parking_count: number;
    actual_receivable_yuan: number;
    simulated_old_policy_yuan: number;
    receivable_uplift_yuan: number;
    recorded_fee_yuan: number;
    realized_growth_rate_pct: number;
    zero_to_paid_count: number;
    zero_to_paid_rate_pct: number;
    cross_day_count: number;
    cross_day_rate_pct: number;
    point_bonus_saved_yuan: number;
}

export interface BiPolicyImpactDailyItem {
    date: string;
    parking_count: number;
    actual_receivable_yuan: number;
    simulated_old_policy_yuan: number;
    receivable_uplift_yuan: number;
    recorded_fee_yuan: number;
    zero_to_paid_count: number;
    zero_to_paid_rate_pct: number;
    cross_day_count: number;
    cross_day_rate_pct: number;
}

export interface BiPolicyImpactLevelItem {
    member_level: string;
    parking_count: number;
    actual_receivable_yuan: number;
    simulated_old_policy_yuan: number;
    receivable_uplift_yuan: number;
    recorded_fee_yuan: number;
    realized_growth_rate_pct: number;
    zero_to_paid_count: number;
    zero_to_paid_rate_pct: number;
    cross_day_count: number;
    cross_day_rate_pct: number;
    avg_stay_duration_hours: number;
}

export interface BiCrossDaySummary {
    cross_day_count: number;
    avg_cross_day_billed_hours: number;
    avg_cross_day_day_count: number;
    cross_day_actual_receivable_yuan: number;
    cross_day_simulated_old_policy_yuan: number;
    cross_day_refinement_delta_yuan: number;
}

export interface BiPolicyImpactBlock {
    summary: BiPolicyImpactSummary;
    daily: BiPolicyImpactDailyItem[];
    by_member_level: BiPolicyImpactLevelItem[];
    cross_day_summary: BiCrossDaySummary;
}

export interface BiDurationShiftSummaryItem {
    member_level: string;
    before_count: number;
    after_count: number;
    before_avg_hours: number;
    after_avg_hours: number;
    before_avg_receivable_yuan: number;
    after_avg_receivable_yuan: number;
}

export interface BiDurationShiftDistributionItem {
    member_level: string;
    duration_bucket: string;
    before_count: number;
    after_count: number;
    before_share_pct: number;
    after_share_pct: number;
}

export interface BiDurationShiftBlock {
    summary: BiDurationShiftSummaryItem[];
    distributions: BiDurationShiftDistributionItem[];
}

export interface BiPointsLeverageSummary {
    total_parking_count: number;
    point_earned_count: number;
    point_bonus_triggered_count: number;
    point_earned_rate_pct: number;
    point_bonus_trigger_rate_pct: number;
    leverage_conversion_rate_pct: number;
    point_bonus_saved_yuan: number;
}

export interface BiPointsLeverageLevelItem {
    member_level: string;
    parking_count: number;
    point_earned_count: number;
    point_bonus_triggered_count: number;
    point_bonus_saved_yuan: number;
    point_earned_rate_pct: number;
    point_bonus_trigger_rate_pct: number;
    leverage_conversion_rate_pct: number;
    avg_stay_duration_hours: number;
}

export interface BiPointsLeverageBlock {
    summary: BiPointsLeverageSummary;
    funnel: BiLinkageItem[];
    by_member_level: BiPointsLeverageLevelItem[];
}

export interface BiPassengerTrendSummary {
    current_total: number;
    previous_total: number;
    current_avg: number;
    previous_avg: number;
    diff_rate_pct: number;
    current_peak: number;
    previous_peak: number;
}

export interface BiPassengerTrendItem {
    label: string;
    current_date: string;
    previous_date: string;
    current_value: number;
    previous_value: number;
}

export interface BiPassengerTrendBlock {
    title: string;
    current_year: number | null;
    previous_year: number | null;
    summary: BiPassengerTrendSummary;
    daily_compare: BiPassengerTrendItem[];
}

export interface BiPassengerAnalysisBlock {
    period_label: string;
    ruiyin: BiPassengerTrendBlock;
}

export interface BiDashboardResponse {
    mode: "daily" | "range";
    category?: "policy" | "regular" | "passenger" | "exception";
    period: {
        start_date: string;
        end_date: string;
        day_count: number;
    };
    summary: {
        parking: BiSummaryBlock;
        trade: BiSummaryBlock;
        point_flow: BiSummaryBlock;
        linked_mobile_count: number;
        linked_member_count: number;
    };
    daily_series: BiDailySeriesItem[];
    plaza_ranking: BiPlazaRankingItem[];
    level_distribution: BiLevelDistributionItem[];
    parking_duration_buckets: BiBucketItem[];
    hourly_distribution: BiHourlyItem[];
    payment_channel_distribution: BiNamedAmountItem[];
    trade_business_distribution: BiNamedAmountItem[];
    passenger_analysis?: BiPassengerAnalysisBlock | null;
    policy_impact?: BiPolicyImpactBlock | null;
    duration_shift?: BiDurationShiftBlock | null;
    points_leverage?: BiPointsLeverageBlock | null;
    linkage_funnel: BiLinkageItem[];
    validation_metrics: BiValidationMetric[];
}

export async function getAdminOverview() {
    return apiRequest<AdminOverviewResponse>("/api/admin/overview");
}

export async function getBiDashboard(params: {
    startDate: string;
    endDate: string;
    mode: "daily" | "range";
    category: "policy" | "regular" | "passenger" | "exception";
}) {
    const query = new URLSearchParams({
        start_date: params.startDate,
        end_date: params.endDate,
        mode: params.mode,
        category: params.category,
    });
    return apiRequest<BiDashboardResponse>(`/api/admin/bi?${query.toString()}`);
}

export async function runParkingIncrementalSync(jobDate?: string, force = true) {
    return apiRequest<{ status: string; job_date: string; detail: string }>("/api/admin/parking-sync/run", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            job_date: jobDate || null,
            force,
        }),
    });
}

export async function runParkingTradeIncrementalSync(jobDate?: string, force = true) {
    return apiRequest<{ status: string; job_date: string; detail: string }>("/api/admin/parking-trade-sync/run", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            job_date: jobDate || null,
            force,
        }),
    });
}
