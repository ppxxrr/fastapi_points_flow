import { type FormEvent, useEffect, useState } from "react";

export interface TaskFormValues {
    startDate: string;
    endDate: string;
}

interface TaskFormProps {
    username: string;
    displayName?: string;
    defaultValues?: Partial<TaskFormValues>;
    onSubmit: (values: TaskFormValues) => void;
    submitting?: boolean;
    errorMessage?: string;
}

function buildDefaultRange() {
    const now = new Date();
    const start = new Date(now.getFullYear(), now.getMonth(), 1);
    return {
        startDate: start.toISOString().slice(0, 10),
        endDate: now.toISOString().slice(0, 10),
    };
}

export default function TaskForm({
    username,
    displayName,
    defaultValues,
    onSubmit,
    submitting = false,
    errorMessage = "",
}: TaskFormProps) {
    const [form, setForm] = useState<TaskFormValues>(() => {
        const fallbackDates = buildDefaultRange();
        return {
            startDate: defaultValues?.startDate || fallbackDates.startDate,
            endDate: defaultValues?.endDate || fallbackDates.endDate,
        };
    });

    useEffect(() => {
        const fallbackDates = buildDefaultRange();
        setForm({
            startDate: defaultValues?.startDate || fallbackDates.startDate,
            endDate: defaultValues?.endDate || fallbackDates.endDate,
        });
    }, [defaultValues?.endDate, defaultValues?.startDate]);

    function updateField<Key extends keyof TaskFormValues>(key: Key, value: TaskFormValues[Key]) {
        setForm((current) => ({ ...current, [key]: value }));
    }

    function handleSubmit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        onSubmit(form);
    }

    return (
        <div>
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Create Task</p>
                    <h3 className="mt-2 text-[2rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        积分流水导出
                    </h3>
                    <p className="mt-3 max-w-xl text-sm leading-6 text-slate-600">
                        当前任务会默认使用已登录的 ICSP 身份。你只需要选择导出区间并提交后台任务。
                    </p>
                </div>

                <div className="rounded-[1.25rem] border border-blue-100/80 bg-[linear-gradient(135deg,rgba(237,247,255,0.96),rgba(241,239,255,0.9))] px-4 py-3 text-right shadow-[0_14px_28px_rgba(88,123,255,0.08)]">
                    <div className="text-xs uppercase tracking-[0.18em] text-blue-400">Task Type</div>
                    <div className="mt-1 text-sm font-medium text-blue-900">points_flow_export</div>
                </div>
            </div>

            <form className="mt-6 space-y-5" onSubmit={handleSubmit}>
                <div className="rounded-[1.2rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.86),rgba(245,247,252,0.72))] px-4 py-4 text-sm shadow-[0_12px_24px_rgba(15,23,42,0.04)]">
                    <div className="text-slate-400">当前登录身份</div>
                    <div className="mt-2 text-base font-medium text-slate-900">
                        {displayName ? `${displayName} (${username})` : username}
                    </div>
                    <div className="mt-2 text-sm leading-6 text-slate-500">
                        这个身份来自 ICSP 登录态。导出任务默认以当前登录账号执行。
                    </div>
                </div>

                <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-800">开始日期</label>
                        <input
                            className="h-11 w-full rounded-[1.1rem] border border-slate-200/90 bg-white/82 px-4 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                            type="date"
                            value={form.startDate}
                            onChange={(event) => updateField("startDate", event.target.value)}
                            required
                        />
                    </div>

                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-800">结束日期</label>
                        <input
                            className="h-11 w-full rounded-[1.1rem] border border-slate-200/90 bg-white/82 px-4 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                            type="date"
                            value={form.endDate}
                            onChange={(event) => updateField("endDate", event.target.value)}
                            required
                        />
                    </div>
                </div>

                <div className="rounded-[1.25rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.78),rgba(245,247,252,0.72))] px-4 py-3 text-sm text-slate-600">
                    登录凭证只用于认证阶段，任务创建接口不再重复传输密码。
                </div>

                {errorMessage && (
                    <div className="rounded-[1.25rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.9))] px-4 py-3 text-sm leading-6 text-rose-700">
                        {errorMessage}
                    </div>
                )}

                <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                    <div className="text-sm text-slate-500">
                        任务创建后将自动开始轮询，并在成功或失败时停止。
                    </div>
                    <button
                        className="inline-flex h-11 cursor-pointer items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-6 text-sm font-medium text-white shadow-[0_20px_40px_rgba(59,130,246,0.24)] transition hover:brightness-[1.04] disabled:cursor-not-allowed disabled:opacity-50"
                        disabled={submitting}
                        type="submit"
                    >
                        {submitting ? "提交中..." : "创建导出任务"}
                    </button>
                </div>
            </form>
        </div>
    );
}
