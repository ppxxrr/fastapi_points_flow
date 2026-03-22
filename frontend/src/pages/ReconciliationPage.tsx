import { useEffect, useMemo, useRef, useState } from "react";

import { getApiErrorMessage, isUnauthorizedError } from "../api/client";
import {
    buildReconciliationDownloadUrl,
    createReconciliationTask,
    getParkingCaptcha,
    getReconciliationConfig,
    getReconciliationTask,
    uploadWechatCsvs,
    type ReconciliationConfigResponse,
    type ReconciliationParkingCaptchaResponse,
    type ReconciliationTaskKind,
    type ReconciliationTaskResponse,
} from "../api/reconciliation";

interface ReconciliationPageProps {
    onLogout: () => Promise<void> | void;
}

const POLL_INTERVAL_MS = 3000;

function yesterdayString() {
    const now = new Date();
    now.setDate(now.getDate() - 1);
    return now.toISOString().slice(0, 10);
}

function formatDateTime(value?: string | null) {
    if (!value) return "-";
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString("zh-CN");
}

function getProgress(task: ReconciliationTaskResponse | null) {
    if (!task) return { label: "未开始", percent: 0 };
    if (task.status === "pending") return { label: "等待执行", percent: 20 };
    if (task.status === "running") return { label: "处理中", percent: 68 };
    if (task.status === "success") return { label: "已完成", percent: 100 };
    return { label: "失败", percent: 100 };
}

function getStatusClass(status?: string) {
    if (status === "success") return "bg-emerald-100 text-emerald-700";
    if (status === "failed") return "bg-rose-100 text-rose-700";
    if (status === "running") return "bg-blue-100 text-blue-700";
    if (status === "pending") return "bg-amber-100 text-amber-700";
    return "bg-slate-100 text-slate-600";
}

function shouldRefreshCaptchaForTaskError(task: ReconciliationTaskResponse | null) {
    if (!task || task.status !== "failed") return false;
    const text = `${task.error || ""} ${task.logs?.[task.logs.length - 1]?.message || ""}`;
    return /验证码|获取验证码/.test(text);
}

function ProgressCard({ task, error }: { task: ReconciliationTaskResponse | null; error: string }) {
    const progress = getProgress(task);
    const lastLog = task?.logs?.[task.logs.length - 1]?.message || "";

    return (
        <div className="w-full rounded-[1.2rem] border border-blue-100/80 bg-[linear-gradient(180deg,rgba(239,246,255,0.95),rgba(255,255,255,0.92))] px-4 py-4 shadow-[0_12px_24px_rgba(37,99,235,0.08)]">
            <div className="flex flex-wrap items-center gap-3 text-sm">
                <span className={`rounded-full px-3 py-1 font-medium ${getStatusClass(task?.status)}`}>{progress.label}</span>
                <span className="text-slate-500">结果条数 {task?.result_count ?? 0}</span>
                <span className="text-slate-500">更新时间 {formatDateTime(task?.updated_at)}</span>
            </div>
            <div className="mt-3 h-2 overflow-hidden rounded-full bg-slate-200">
                <div
                    className="h-full rounded-full bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 transition-all duration-300"
                    style={{ width: `${progress.percent}%` }}
                />
            </div>
            {lastLog ? <div className="mt-3 text-sm text-slate-500">{lastLog}</div> : null}
            {error ? <div className="mt-3 text-sm text-rose-600">{error}</div> : null}
            {!error && task?.error ? <div className="mt-3 text-sm text-rose-600">{task.error}</div> : null}
        </div>
    );
}

function CaptchaPanel({
    enabled,
    loading,
    image,
    input,
    expiresInSeconds,
    onInputChange,
    onRefresh,
}: {
    enabled: boolean;
    loading: boolean;
    image: string;
    input: string;
    expiresInSeconds: number;
    onInputChange: (value: string) => void;
    onRefresh: () => void;
}) {
    if (!enabled) {
        return null;
    }

    return (
        <div className="flex flex-wrap items-end gap-3">
            <button
                className="flex h-14 w-[168px] items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-2 py-1 shadow-[0_10px_22px_rgba(15,23,42,0.05)]"
                onClick={onRefresh}
                title="点击刷新验证码"
                type="button"
            >
                {image ? (
                    <img alt="停车验证码" className="max-h-full max-w-full object-contain" src={`data:image/png;base64,${image}`} />
                ) : (
                    <span className="text-sm text-slate-400">{loading ? "加载中..." : "加载验证码"}</span>
                )}
            </button>
            <label className="flex items-center gap-2 text-sm text-slate-600">
                <span>验证码</span>
                <input
                    autoCapitalize="none"
                    autoComplete="off"
                    className="h-11 w-[140px] rounded-[1rem] border border-white/80 bg-white/92 px-4 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)] outline-none"
                    maxLength={8}
                    placeholder="请输入验证码"
                    spellCheck={false}
                    type="text"
                    value={input}
                    onChange={(event) => onInputChange(event.target.value)}
                />
            </label>
            <button
                className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-4 text-sm font-medium text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] transition hover:text-slate-950"
                onClick={onRefresh}
                type="button"
            >
                刷新验证码
            </button>
            {expiresInSeconds > 0 ? (
                <div className="pb-2 text-xs text-slate-500">验证码约 {Math.max(1, Math.round(expiresInSeconds / 60))} 分钟内有效</div>
            ) : null}
        </div>
    );
}

function UploadPill({ children, tone = "slate" }: { children: React.ReactNode; tone?: "slate" | "green" }) {
    const className = tone === "green"
        ? "border-emerald-200 bg-emerald-50 text-emerald-700"
        : "border-slate-200 bg-slate-50 text-slate-600";
    return <div className={`inline-flex h-10 items-center rounded-[1rem] border px-4 text-sm ${className}`}>{children}</div>;
}

function ToolCard({
    title,
    task,
    startDate,
    endDate,
    submitting,
    error,
    onStartDateChange,
    onEndDateChange,
    onSubmit,
    extra,
}: {
    title: string;
    task: ReconciliationTaskResponse | null;
    startDate: string;
    endDate: string;
    submitting: boolean;
    error: string;
    onStartDateChange: (value: string) => void;
    onEndDateChange: (value: string) => void;
    onSubmit: () => void;
    extra?: React.ReactNode;
}) {
    return (
        <section className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-6 shadow-[0_20px_50px_rgba(15,23,42,0.06)] backdrop-blur-xl">
            <div className="flex flex-col gap-5">
                <div>
                    <h3 className="text-[1.45rem] font-semibold tracking-[-0.04em] text-slate-950">{title}</h3>
                </div>
                {extra ?? null}
                <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                    <div className="flex flex-wrap items-end gap-3">
                        <label className="flex items-center gap-2 text-sm text-slate-600">
                            <span>开始日期</span>
                            <input className="h-11 w-[160px] rounded-[1rem] border border-white/80 bg-white/92 px-4 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)] outline-none" type="date" value={startDate} onChange={(event) => onStartDateChange(event.target.value)} />
                        </label>
                        <label className="flex items-center gap-2 text-sm text-slate-600">
                            <span>结束日期</span>
                            <input className="h-11 w-[160px] rounded-[1rem] border border-white/80 bg-white/92 px-4 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)] outline-none" type="date" value={endDate} onChange={(event) => onEndDateChange(event.target.value)} />
                        </label>
                        <button className="inline-flex h-11 items-center justify-center rounded-[1rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-5 text-sm font-medium text-white shadow-[0_18px_38px_rgba(59,130,246,0.22)] transition hover:brightness-[1.04] disabled:cursor-not-allowed disabled:opacity-60" disabled={submitting} onClick={onSubmit} type="button">
                            {submitting ? "导出中..." : "导出"}
                        </button>
                        {task?.status === "success" && task.result_file ? (
                            <a className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-5 text-sm font-medium text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] transition hover:text-slate-950" href={buildReconciliationDownloadUrl(task.result_file)}>
                                下载文件
                            </a>
                        ) : null}
                    </div>
                    <div className="w-full max-w-[360px]">
                        <ProgressCard error={error} task={task} />
                    </div>
                </div>
            </div>
        </section>
    );
}

export default function ReconciliationPage({ onLogout }: ReconciliationPageProps) {
    const defaultDate = useMemo(() => yesterdayString(), []);
    const [config, setConfig] = useState<ReconciliationConfigResponse | null>(null);
    const [configError, setConfigError] = useState("");

    const [dzStartDate, setDzStartDate] = useState(defaultDate);
    const [dzEndDate, setDzEndDate] = useState(defaultDate);
    const [couponStartDate, setCouponStartDate] = useState(defaultDate);
    const [couponEndDate, setCouponEndDate] = useState(defaultDate);

    const [projectKey, setProjectKey] = useState("");
    const [captcha, setCaptcha] = useState<ReconciliationParkingCaptchaResponse | null>(null);
    const [captchaInput, setCaptchaInput] = useState("");
    const [captchaLoading, setCaptchaLoading] = useState(false);
    const [captchaFetchedAt, setCaptchaFetchedAt] = useState<number | null>(null);

    const [fundCsvFile, setFundCsvFile] = useState<File | null>(null);
    const [tradeCsvFile, setTradeCsvFile] = useState<File | null>(null);
    const [uploadSessionId, setUploadSessionId] = useState("");
    const [uploadedFundName, setUploadedFundName] = useState("");
    const [uploadedTradeName, setUploadedTradeName] = useState("");
    const [uploadingCsv, setUploadingCsv] = useState(false);

    const [dzTask, setDzTask] = useState<ReconciliationTaskResponse | null>(null);
    const [couponTask, setCouponTask] = useState<ReconciliationTaskResponse | null>(null);
    const [dzSubmitting, setDzSubmitting] = useState(false);
    const [couponSubmitting, setCouponSubmitting] = useState(false);
    const [dzError, setDzError] = useState("");
    const [couponError, setCouponError] = useState("");
    const lastCaptchaRefreshTaskIdRef = useRef("");

    const selectedProject = useMemo(() => config?.parking_projects.find((item) => item.key === projectKey) ?? null, [config, projectKey]);
    const parkingEnabled = Boolean(selectedProject?.enable_parking);

    useEffect(() => {
        let active = true;
        async function loadConfig() {
            try {
                const response = await getReconciliationConfig();
                if (!active) return;
                setConfig(response);
                setProjectKey(response.parking_projects[0]?.key || "");
                setConfigError("");
            } catch (error) {
                if (!active) return;
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                setConfigError(getApiErrorMessage(error));
            }
        }
        void loadConfig();
        return () => {
            active = false;
        };
    }, [onLogout]);

    useEffect(() => {
        if (!projectKey || !parkingEnabled) {
            setCaptcha(null);
            setCaptchaInput("");
            setCaptchaFetchedAt(null);
            return;
        }
        let active = true;
        setCaptchaLoading(true);
        setDzError("");
        void getParkingCaptcha(projectKey)
            .then((response) => {
                if (!active) return;
                setCaptcha(response);
                setCaptchaInput("");
                setCaptchaFetchedAt(Date.now());
            })
            .catch(async (error) => {
                if (!active) return;
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                setDzError(getApiErrorMessage(error));
            })
            .finally(() => {
                if (active) setCaptchaLoading(false);
            });
        return () => {
            active = false;
        };
    }, [projectKey, parkingEnabled, onLogout]);

    useEffect(() => {
        if (!dzTask || !["pending", "running"].includes(dzTask.status)) return;
        const timer = window.setInterval(async () => {
            try {
                setDzTask(await getReconciliationTask(dzTask.task_id));
            } catch (error) {
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                setDzError(getApiErrorMessage(error));
                window.clearInterval(timer);
            }
        }, POLL_INTERVAL_MS);
        return () => window.clearInterval(timer);
    }, [dzTask, onLogout]);

    useEffect(() => {
        if (!couponTask || !["pending", "running"].includes(couponTask.status)) return;
        const timer = window.setInterval(async () => {
            try {
                setCouponTask(await getReconciliationTask(couponTask.task_id));
            } catch (error) {
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                setCouponError(getApiErrorMessage(error));
                window.clearInterval(timer);
            }
        }, POLL_INTERVAL_MS);
        return () => window.clearInterval(timer);
    }, [couponTask, onLogout]);

    useEffect(() => {
        if (!parkingEnabled || !projectKey || !dzTask || !shouldRefreshCaptchaForTaskError(dzTask)) return;
        const failedTaskId = dzTask.task_id;
        if (lastCaptchaRefreshTaskIdRef.current === failedTaskId) return;
        lastCaptchaRefreshTaskIdRef.current = failedTaskId;

        let active = true;
        setCaptchaLoading(true);
        void getParkingCaptcha(projectKey)
            .then((response) => {
                if (!active) return;
                setCaptcha(response);
                setCaptchaInput("");
                setCaptchaFetchedAt(Date.now());
            })
            .catch(async (error) => {
                if (!active) return;
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                setDzError(getApiErrorMessage(error));
            })
            .finally(() => {
                if (active) setCaptchaLoading(false);
            });

        return () => {
            active = false;
        };
    }, [dzTask, onLogout, parkingEnabled, projectKey]);

    async function refreshCaptcha() {
        if (!projectKey || !parkingEnabled) return;
        setCaptchaLoading(true);
        setDzError("");
        try {
            const response = await getParkingCaptcha(projectKey);
            setCaptcha(response);
            setCaptchaInput("");
            setCaptchaFetchedAt(Date.now());
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setDzError(getApiErrorMessage(error));
        } finally {
            setCaptchaLoading(false);
        }
    }

    async function handleWechatCsvUpload() {
        if (!fundCsvFile || !tradeCsvFile) {
            setDzError("请先选择微信支付资金账单和交易订单 CSV。");
            return;
        }
        setUploadingCsv(true);
        setDzError("");
        try {
            const response = await uploadWechatCsvs({ wechatFundCsv: fundCsvFile, wechatTradeCsv: tradeCsvFile });
            setUploadSessionId(response.session_id);
            setUploadedFundName(response.fund_file_name);
            setUploadedTradeName(response.trade_file_name);
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setDzError(getApiErrorMessage(error));
        } finally {
            setUploadingCsv(false);
        }
    }

    async function createTask(kind: ReconciliationTaskKind, startDate: string, endDate: string) {
        return createReconciliationTask({
            kind,
            start_date: startDate,
            end_date: endDate,
            project_key: kind === "new_icsp_dz" ? projectKey : undefined,
            captcha_code: kind === "new_icsp_dz" && parkingEnabled ? captchaInput.trim() : undefined,
            captcha_uuid: kind === "new_icsp_dz" && parkingEnabled ? captcha?.captcha_uuid || undefined : undefined,
            upload_session_id: kind === "new_icsp_dz" ? uploadSessionId || undefined : undefined,
        });
    }

    async function handleDzExport() {
        if (dzStartDate > dzEndDate) {
            setDzError("开始日期不能晚于结束日期。");
            return;
        }
        if (!projectKey) {
            setDzError("请选择项目。");
            return;
        }
        if (!uploadSessionId) {
            setDzError("请先上传微信账单 CSV。");
            return;
        }
        if (parkingEnabled && (!captcha?.captcha_uuid || !captchaInput.trim())) {
            setDzError("当前项目需要先输入停车验证码。");
            return;
        }
        if (
            parkingEnabled &&
            captcha &&
            captchaFetchedAt &&
            captcha.expires_in_seconds > 0 &&
            Date.now() - captchaFetchedAt > captcha.expires_in_seconds * 1000
        ) {
            setDzError("当前停车验证码已过期，请刷新后重试。");
            return;
        }
        setDzSubmitting(true);
        setDzError("");
        try {
            setDzTask(await createTask("new_icsp_dz", dzStartDate, dzEndDate));
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setDzError(getApiErrorMessage(error));
        } finally {
            setDzSubmitting(false);
        }
    }

    async function handleCouponExport() {
        if (couponStartDate > couponEndDate) {
            setCouponError("开始日期不能晚于结束日期。");
            return;
        }
        setCouponSubmitting(true);
        setCouponError("");
        try {
            setCouponTask(await createTask("coupon_tool", couponStartDate, couponEndDate));
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setCouponError(getApiErrorMessage(error));
        } finally {
            setCouponSubmitting(false);
        }
    }

    return (
        <div className="space-y-6">
            {configError ? <div className="rounded-[1.2rem] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{configError}</div> : null}

            <ToolCard
                title="对账工具"
                task={dzTask}
                startDate={dzStartDate}
                endDate={dzEndDate}
                submitting={dzSubmitting}
                error={dzError}
                onStartDateChange={setDzStartDate}
                onEndDateChange={setDzEndDate}
                onSubmit={handleDzExport}
                extra={
                    <div className="space-y-4 rounded-[1.2rem] border border-white/80 bg-white/70 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                        <div className="flex flex-wrap items-center gap-3">
                            <label className="flex items-center gap-2 text-sm text-slate-600">
                                <span>项目</span>
                                <select className="h-11 w-[150px] rounded-[1rem] border border-white/80 bg-white/92 px-4 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)] outline-none" value={projectKey} onChange={(event) => setProjectKey(event.target.value)}>
                                    {config?.parking_projects.map((item) => (
                                        <option key={item.key} value={item.key}>{item.label}</option>
                                    ))}
                                </select>
                            </label>
                            <CaptchaPanel
                                enabled={parkingEnabled}
                                expiresInSeconds={captcha?.expires_in_seconds || 0}
                                image={captcha?.image_base64 || ""}
                                input={captchaInput}
                                loading={captchaLoading}
                                onInputChange={setCaptchaInput}
                                onRefresh={() => void refreshCaptcha()}
                            />
                        </div>

                        <div className="flex flex-wrap items-end gap-3">
                            <label className="flex flex-col gap-2">
                                <span className="text-sm font-medium text-slate-500">微信支付资金账单</span>
                                <input className="block h-11 w-[260px] rounded-[1rem] border border-white/80 bg-white/92 px-3 py-2 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)]" accept=".csv,text/csv" type="file" onChange={(event) => setFundCsvFile(event.target.files?.[0] || null)} />
                            </label>
                            <label className="flex flex-col gap-2">
                                <span className="text-sm font-medium text-slate-500">微信支付交易订单</span>
                                <input className="block h-11 w-[260px] rounded-[1rem] border border-white/80 bg-white/92 px-3 py-2 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.05)]" accept=".csv,text/csv" type="file" onChange={(event) => setTradeCsvFile(event.target.files?.[0] || null)} />
                            </label>
                            <button className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-5 text-sm font-medium text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] transition hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-60" disabled={uploadingCsv} onClick={() => void handleWechatCsvUpload()} type="button">
                                {uploadingCsv ? "上传中..." : "上传微信账单"}
                            </button>
                            {uploadSessionId ? <UploadPill tone="green">已上传：{uploadedFundName} / {uploadedTradeName}</UploadPill> : null}
                        </div>
                    </div>
                }
            />

            <ToolCard
                title="券核销导出"
                task={couponTask}
                startDate={couponStartDate}
                endDate={couponEndDate}
                submitting={couponSubmitting}
                error={couponError}
                onStartDateChange={setCouponStartDate}
                onEndDateChange={setCouponEndDate}
                onSubmit={handleCouponExport}
            />
        </div>
    );
}
