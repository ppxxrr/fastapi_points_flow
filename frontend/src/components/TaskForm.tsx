import { type FormEvent, useEffect, useState } from "react";

export interface TaskFormValues {
    startDate: string;
    endDate: string;
}

interface TaskProgressMeta {
    label: string;
    percent: number;
    bar: string;
}

interface TaskFormProps {
    defaultValues?: Partial<TaskFormValues>;
    onSubmit: (values: TaskFormValues) => void;
    submitting?: boolean;
    errorMessage?: string;
    progress: TaskProgressMeta;
    statusLabel: string;
    statusClassName: string;
    resultCount: number;
    updatedAt?: string;
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
    defaultValues,
    onSubmit,
    submitting = false,
    errorMessage = "",
    progress,
    statusLabel,
    statusClassName,
    resultCount,
    updatedAt,
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
        <form className="space-y-4" onSubmit={handleSubmit}>
            <div className="grid gap-4 xl:grid-cols-[10.5rem_10.5rem_7.5rem_minmax(20rem,1fr)] xl:items-end">
                <div className="space-y-2">
                    <label className="text-sm font-medium text-slate-700">开始日期</label>
                    <input
                        className="h-11 w-full rounded-[1.05rem] border border-slate-200/90 bg-white/86 px-3 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                        type="date"
                        value={form.startDate}
                        onChange={(event) => updateField("startDate", event.target.value)}
                        required
                    />
                </div>

                <div className="space-y-2">
                    <label className="text-sm font-medium text-slate-700">结束日期</label>
                    <input
                        className="h-11 w-full rounded-[1.05rem] border border-slate-200/90 bg-white/86 px-3 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                        type="date"
                        value={form.endDate}
                        onChange={(event) => updateField("endDate", event.target.value)}
                        required
                    />
                </div>

                <button
                    className="inline-flex h-11 cursor-pointer items-center justify-center rounded-[1.05rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-4 text-sm font-medium text-white shadow-[0_20px_42px_rgba(59,130,246,0.22)] transition hover:brightness-[1.04] disabled:cursor-not-allowed disabled:opacity-50"
                    disabled={submitting}
                    type="submit"
                >
                    {submitting ? "导出中..." : "导出"}
                </button>

                <div className="rounded-[1.15rem] border border-slate-200/80 bg-white/88 px-4 py-3 shadow-[0_12px_28px_rgba(15,23,42,0.04)]">
                    <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
                        <div className="flex items-center gap-3">
                            <span className="text-sm font-medium text-slate-500">任务进度</span>
                            <span className={["rounded-full px-3 py-1.5 text-sm font-medium", statusClassName].join(" ")}>
                                {statusLabel}
                            </span>
                        </div>

                        <div className="flex flex-wrap items-center gap-4 text-sm text-slate-500">
                            <span>
                                结果条数 <span className="ml-1 font-semibold text-slate-950">{resultCount}</span>
                            </span>
                            {updatedAt ? (
                                <span>
                                    更新时间 <span className="ml-1 font-semibold text-slate-950">{updatedAt}</span>
                                </span>
                            ) : null}
                        </div>
                    </div>

                    <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-slate-100">
                        <div
                            className={`h-full rounded-full bg-gradient-to-r ${progress.bar} transition-[width] duration-300`}
                            style={{ width: `${progress.percent}%` }}
                        />
                    </div>
                </div>
            </div>

            {errorMessage ? (
                <div className="rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                    {errorMessage}
                </div>
            ) : null}
        </form>
    );
}
