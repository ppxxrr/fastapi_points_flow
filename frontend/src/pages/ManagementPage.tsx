import { useEffect, useMemo, useState, type ReactNode } from "react";

import { getAdminOverview, type AdminOverviewResponse } from "../api/admin";
import { getApiErrorMessage, isUnauthorizedError } from "../api/client";

interface ManagementPageProps {
    onLogout: () => Promise<void> | void;
}

function toneClass(status: string) {
    const normalized = status.toLowerCase();
    if (normalized === "success" || normalized === "ok" || normalized === "online") {
        return "bg-emerald-100 text-emerald-700";
    }
    if (normalized === "failed" || normalized === "offline") {
        return "bg-rose-100 text-rose-700";
    }
    if (normalized === "warning" || normalized === "degraded") {
        return "bg-amber-100 text-amber-700";
    }
    return "bg-blue-100 text-blue-700";
}

function formatDateTime(value?: string | null) {
    if (!value) {
        return "-";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }
    return parsed.toLocaleString("zh-CN");
}

function formatCount(value?: number | null) {
    return new Intl.NumberFormat("zh-CN").format(value || 0);
}

function MiniMetric({
    label,
    value,
    note,
}: {
    label: string;
    value: string;
    note?: string;
}) {
    return (
        <div className="rounded-[1.2rem] bg-white/92 px-4 py-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
            <div className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</div>
            <div className="mt-2 text-[1.55rem] font-semibold tracking-[-0.04em] text-slate-950">{value}</div>
            {note ? <div className="mt-1 text-sm leading-6 text-slate-500">{note}</div> : null}
        </div>
    );
}

function SectionCard({
    eyebrow,
    title,
    right,
    children,
}: {
    eyebrow: string;
    title: string;
    right?: ReactNode;
    children: ReactNode;
}) {
    return (
        <section className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-5 shadow-[0_18px_42px_rgba(15,23,42,0.06)] backdrop-blur-xl">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-[0.72rem] font-medium uppercase tracking-[0.22em] text-slate-400">{eyebrow}</p>
                    <h3 className="mt-2 text-[1.5rem] font-semibold tracking-[-0.045em] text-slate-950">{title}</h3>
                </div>
                {right}
            </div>
            <div className="mt-5">{children}</div>
        </section>
    );
}

export default function ManagementPage({ onLogout }: ManagementPageProps) {
    const [overview, setOverview] = useState<AdminOverviewResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [error, setError] = useState("");

    async function loadOverview(showRefreshing = false) {
        if (showRefreshing) {
            setRefreshing(true);
        } else {
            setLoading(true);
        }

        try {
            const nextOverview = await getAdminOverview();
            setOverview(nextOverview);
            setError("");
        } catch (requestError) {
            if (isUnauthorizedError(requestError)) {
                await onLogout();
                return;
            }
            setError(getApiErrorMessage(requestError));
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    }

    useEffect(() => {
        let active = true;

        async function bootstrap() {
            if (active) {
                await loadOverview(false);
            }
        }

        void bootstrap();
        const timer = window.setInterval(() => {
            if (active) {
                void loadOverview(true);
            }
        }, 60000);

        return () => {
            active = false;
            window.clearInterval(timer);
        };
    }, []);

    const tableCounts = overview?.database.table_counts;
    const parkingRiskFlags = overview?.parking_integrity.risk_flags || [];
    const csvSource = overview?.parking_integrity.csv_source;
    const parkingSummary = overview?.parking_integrity.table_summary;

    const serverCards = useMemo(
        () =>
            overview
                ? [
                      {
                          label: "\u8fd0\u884c\u73af\u5883",
                          value: overview.server.environment || "unknown",
                          note: `${overview.server.hostname} / ${overview.server.app_host}:${overview.server.app_port}`,
                      },
                      {
                          label: "\u5f53\u524d\u6570\u636e\u5e93",
                          value: overview.database.backend.toUpperCase(),
                          note: overview.database.connected ? "\u8fde\u63a5\u6b63\u5e38" : "\u8fde\u63a5\u5f02\u5e38",
                      },
                      {
                          label: "\u65e5\u5fd7\u76ee\u5f55",
                          value: "\u5df2\u914d\u7f6e",
                          note: overview.server.log_dir,
                      },
                      {
                          label: "\u505c\u8f66\u6e90\u76ee\u5f55",
                          value: csvSource?.valid_parking_csv_files ? `${csvSource.valid_parking_csv_files} CSV` : "0 CSV",
                          note: overview.server.parking_source_dir,
                      },
                  ]
                : [],
        [csvSource?.valid_parking_csv_files, overview],
    );

    if (loading && !overview) {
        return (
            <div className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-8 text-center shadow-[0_18px_42px_rgba(15,23,42,0.06)]">
                <div className="mx-auto h-10 w-10 animate-spin rounded-full border-2 border-blue-200 border-t-blue-600" />
                <div className="mt-4 text-sm font-medium text-slate-700">
                    {"\u6b63\u5728\u52a0\u8f7d\u7ba1\u7406\u9875\u8fd0\u884c\u72b6\u6001..."}
                </div>
            </div>
        );
    }

    return (
        <div className="space-y-5">
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-[1.6rem] border border-white/80 bg-white/76 px-5 py-4 shadow-[0_14px_30px_rgba(15,23,42,0.05)]">
                <div>
                    <div className="text-[0.72rem] font-medium uppercase tracking-[0.22em] text-slate-400">
                        {"\u7cfb\u7edf\u603b\u89c8"}
                    </div>
                    <div className="mt-1 text-sm text-slate-600">
                        {overview?.generated_at
                            ? `\u6700\u540e\u5237\u65b0\uff1a${formatDateTime(overview.generated_at)}`
                            : "\u6682\u65e0\u5237\u65b0\u65f6\u95f4"}
                    </div>
                </div>

                <button
                    className="inline-flex h-11 items-center justify-center rounded-[1.1rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.88))] px-4 text-sm font-medium text-slate-700 shadow-[0_16px_32px_rgba(76,108,180,0.08)] transition hover:text-slate-950"
                    onClick={() => void loadOverview(true)}
                    type="button"
                >
                    {refreshing ? "\u5237\u65b0\u4e2d..." : "\u7acb\u5373\u5237\u65b0"}
                </button>
            </div>

            {error ? (
                <div className="rounded-[1.4rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,245,247,0.98),rgba(255,241,244,0.92))] px-5 py-4 text-sm leading-6 text-rose-700">
                    {error}
                </div>
            ) : null}

            <div className="rounded-[1.6rem] border border-amber-200/90 bg-[linear-gradient(135deg,rgba(255,251,235,0.98),rgba(255,247,214,0.95))] px-5 py-4 shadow-[0_18px_34px_rgba(217,119,6,0.08)]">
                <div className="flex flex-wrap items-center gap-3">
                    <span className="rounded-full bg-amber-100 px-3 py-1 text-sm font-medium text-amber-800">
                        {"\u91cd\u70b9\u98ce\u9669"}
                    </span>
                    <div className="text-base font-semibold text-slate-950">
                        {overview?.parking_integrity.headline || "\u505c\u8f66\u573a\u6570\u636e\u5b8c\u6574\u6027\u5f85\u6838\u67e5"}
                    </div>
                </div>
                <ul className="mt-3 space-y-2 text-sm leading-6 text-slate-700">
                    {parkingRiskFlags.map((flag, index) => (
                        <li key={`${flag}-${index}`}>• {flag}</li>
                    ))}
                </ul>
            </div>

            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
                <SectionCard
                    eyebrow="Service"
                    title="\u670d\u52a1\u5668\u8fd0\u884c\u72b6\u6001"
                    right={
                        <span className={["rounded-full px-3 py-1.5 text-sm font-medium", toneClass(overview?.server.web_status || "warning")].join(" ")}>
                            {overview?.server.web_status || "unknown"}
                        </span>
                    }
                >
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                        {serverCards.map((item) => (
                            <MiniMetric key={item.label} label={item.label} value={item.value} note={item.note} />
                        ))}
                    </div>
                </SectionCard>

                <SectionCard
                    eyebrow="Database"
                    title="\u6570\u636e\u5e93\u8fd0\u884c\u72b6\u6001"
                    right={
                        <span className={["rounded-full px-3 py-1.5 text-sm font-medium", toneClass(overview?.database.connected ? "success" : "failed")].join(" ")}>
                            {overview?.database.connected ? "\u5df2\u8fde\u63a5" : "\u672a\u8fde\u63a5"}
                        </span>
                    }
                >
                    <div className="grid gap-3 sm:grid-cols-2">
                        <MiniMetric
                            label="\u6570\u636e\u5e93\u7c7b\u578b"
                            value={overview?.database.backend.toUpperCase() || "-"}
                            note={`${overview?.database.host || "-"}:${overview?.database.port || "-"}`}
                        />
                        <MiniMetric
                            label="\u6570\u636e\u5e93\u540d"
                            value={overview?.database.database_name || "-"}
                            note={overview?.database.connected ? "\u5f53\u524d\u8fde\u63a5\u6b63\u5e38" : "\u9700\u8981\u68c0\u67e5\u8fde\u63a5"}
                        />
                    </div>
                </SectionCard>
            </div>

            <SectionCard eyebrow="Tables" title="\u6838\u5fc3\u6570\u636e\u8868\u8bb0\u5f55\u6570">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    <MiniMetric label="member_profile" value={formatCount(tableCounts?.member_profile)} />
                    <MiniMetric label="member_account" value={formatCount(tableCounts?.member_account)} />
                    <MiniMetric label="member_point_flow" value={formatCount(tableCounts?.member_point_flow)} />
                    <MiniMetric label="parking_record" value={formatCount(tableCounts?.parking_record)} />
                    <MiniMetric label="sync_job_state" value={formatCount(tableCounts?.sync_job_state)} />
                    <MiniMetric label="sync_task_log" value={formatCount(tableCounts?.sync_task_log)} />
                </div>
            </SectionCard>

            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
                <SectionCard eyebrow="Parking" title="\u505c\u8f66\u573a\u6570\u636e\u5feb\u7167">
                    <div className="grid gap-3 sm:grid-cols-2">
                        <MiniMetric
                            label="parking_record"
                            value={formatCount(parkingSummary?.parking_record_count)}
                            note={`source_file: ${formatCount(parkingSummary?.distinct_source_file_count)}`}
                        />
                        <MiniMetric
                            label="\u505c\u8f66 CSV"
                            value={formatCount(csvSource?.total_csv_data_rows)}
                            note={`${formatCount(csvSource?.valid_parking_csv_files)} CSV / ${csvSource?.min_source_start_date || "-"} ~ ${csvSource?.max_source_end_date || "-"}`}
                        />
                        <MiniMetric
                            label="\u7f3a\u5931\u624b\u673a\u53f7"
                            value={formatCount(parkingSummary?.null_mobile_no_count)}
                            note="\u8fd9\u7c7b\u8bb0\u5f55\u4e0d\u80fd\u76f4\u63a5\u7528\u4e8e\u4f1a\u5458\u589e\u91cf\u8865\u5168"
                        />
                        <MiniMetric
                            label="\u65f6\u95f4\u8986\u76d6"
                            value={parkingSummary?.max_exit_time ? formatDateTime(parkingSummary.max_exit_time) : "-"}
                            note={`entry: ${formatDateTime(parkingSummary?.min_entry_time)}`}
                        />
                    </div>

                    <div className="mt-5 overflow-hidden rounded-[1.45rem] border border-slate-200/90 bg-white/88">
                        <div className="grid grid-cols-[1.35fr_104px_130px_130px] gap-3 border-b border-slate-200/90 px-4 py-3 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">
                            <div>{"\u6587\u4ef6\u540d"}</div>
                            <div>{"\u7f16\u7801"}</div>
                            <div>{"\u6570\u636e\u884c"}</div>
                            <div>{"\u8986\u76d6\u65e5\u671f"}</div>
                        </div>
                        <div className="max-h-[16rem] overflow-y-auto">
                            {(csvSource?.sample_files || []).map((file) => (
                                <div
                                    key={file.file_name}
                                    className="grid grid-cols-[1.35fr_104px_130px_130px] gap-3 border-b border-slate-100/90 px-4 py-3 text-sm last:border-b-0"
                                >
                                    <div className="break-all text-slate-800">{file.file_name}</div>
                                    <div className="text-slate-500">{file.encoding || "-"}</div>
                                    <div className="text-slate-500">
                                        {typeof file.data_rows === "number" ? formatCount(file.data_rows) : "-"}
                                    </div>
                                    <div className="text-slate-500">
                                        {file.source_start_date && file.source_end_date
                                            ? `${file.source_start_date} ~ ${file.source_end_date}`
                                            : "-"}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </SectionCard>

                <SectionCard eyebrow="Sync Jobs" title="\u6700\u8fd1 sync_job_state">
                    <div className="overflow-hidden rounded-[1.45rem] border border-slate-200/90 bg-white/88">
                        <div className="grid grid-cols-[180px_110px_100px_88px_150px_1fr] gap-3 border-b border-slate-200/90 px-4 py-3 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">
                            <div>job_name</div>
                            <div>job_date</div>
                            <div>status</div>
                            <div>retry</div>
                            <div>updated_at</div>
                            <div>last_error</div>
                        </div>
                        <div className="max-h-[24rem] overflow-y-auto">
                            {(overview?.sync.recent_jobs || []).map((job) => (
                                <div
                                    key={job.id}
                                    className="grid grid-cols-[180px_110px_100px_88px_150px_1fr] gap-3 border-b border-slate-100/90 px-4 py-3 text-sm last:border-b-0"
                                >
                                    <div className="font-medium text-slate-900">{job.job_name}</div>
                                    <div className="text-slate-500">{job.job_date}</div>
                                    <div>
                                        <span className={["rounded-full px-2.5 py-1 text-[0.72rem] font-medium", toneClass(job.status)].join(" ")}>
                                            {job.status}
                                        </span>
                                    </div>
                                    <div className="text-slate-500">{job.retry_count}</div>
                                    <div className="text-slate-500">{formatDateTime(job.updated_at)}</div>
                                    <div className="leading-6 text-slate-600">{job.last_error || "-"}</div>
                                </div>
                            ))}
                        </div>
                    </div>
                </SectionCard>
            </div>

            <SectionCard eyebrow="Failures" title="\u6700\u8fd1\u5931\u8d25\u8bb0\u5f55">
                <div className="overflow-hidden rounded-[1.45rem] border border-slate-200/90 bg-white/88">
                    <div className="grid grid-cols-[120px_160px_160px_140px_1fr_150px] gap-3 border-b border-slate-200/90 px-4 py-3 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">
                        <div>module</div>
                        <div>action</div>
                        <div>status</div>
                        <div>target</div>
                        <div>error</div>
                        <div>finished_at</div>
                    </div>
                    <div className="max-h-[22rem] overflow-y-auto">
                        {(overview?.sync.recent_failures || []).length === 0 ? (
                            <div className="px-4 py-10 text-center text-sm text-slate-500">
                                {"\u6682\u65e0\u6700\u8fd1\u5931\u8d25\u8bb0\u5f55"}
                            </div>
                        ) : (
                            (overview?.sync.recent_failures || []).map((item) => (
                                <div
                                    key={item.id}
                                    className="grid grid-cols-[120px_160px_160px_140px_1fr_150px] gap-3 border-b border-slate-100/90 px-4 py-3 text-sm last:border-b-0"
                                >
                                    <div className="font-medium text-slate-900">{item.module_name}</div>
                                    <div className="text-slate-600">{item.action}</div>
                                    <div>
                                        <span className={["rounded-full px-2.5 py-1 text-[0.72rem] font-medium", toneClass(item.status)].join(" ")}>
                                            {item.status}
                                        </span>
                                    </div>
                                    <div className="break-all text-slate-500">{item.target_value || "-"}</div>
                                    <div className="leading-6 text-slate-600">{item.error_message || "-"}</div>
                                    <div className="text-slate-500">{formatDateTime(item.finished_at)}</div>
                                </div>
                            ))
                        )}
                    </div>
                </div>
            </SectionCard>
        </div>
    );
}
