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
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                <div>
                    <p className="text-[0.72rem] font-medium uppercase tracking-[0.24em] text-slate-400">
                        Task Toolbar
                    </p>
                    <h2 className="mt-2 text-[2rem] font-semibold tracking-[-0.05em] text-slate-950">
                        导出任务
                    </h2>
                </div>

                <div className="inline-flex items-center gap-3 rounded-[1.25rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(247,249,255,0.84))] px-4 py-3 shadow-[0_14px_32px_rgba(76,108,180,0.07)]">
                    <div className="flex h-9 w-9 items-center justify-center rounded-full bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500 text-xs font-semibold text-white">
                        {(displayName || username).slice(0, 1).toUpperCase()}
                    </div>
                    <div className="min-w-0">
                        <div className="truncate text-sm font-medium text-slate-900">
                            {displayName || username}
                        </div>
                        <div className="truncate text-xs text-slate-500">{username}</div>
                    </div>
                </div>
            </div>

            <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
                <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] xl:items-end">
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-700">开始日期</label>
                        <input
                            className="h-12 w-full rounded-[1.15rem] border border-slate-200/90 bg-white/86 px-4 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                            type="date"
                            value={form.startDate}
                            onChange={(event) => updateField("startDate", event.target.value)}
                            required
                        />
                    </div>

                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-700">结束日期</label>
                        <input
                            className="h-12 w-full rounded-[1.15rem] border border-slate-200/90 bg-white/86 px-4 text-sm text-slate-900 outline-none transition focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                            type="date"
                            value={form.endDate}
                            onChange={(event) => updateField("endDate", event.target.value)}
                            required
                        />
                    </div>

                    <button
                        className="inline-flex h-12 cursor-pointer items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-6 text-sm font-medium text-white shadow-[0_20px_42px_rgba(59,130,246,0.22)] transition hover:brightness-[1.04] disabled:cursor-not-allowed disabled:opacity-50 xl:min-w-[10rem]"
                        disabled={submitting}
                        type="submit"
                    >
                        {submitting ? "导出中..." : "创建导出任务"}
                    </button>
                </div>

                {errorMessage && (
                    <div className="rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                        {errorMessage}
                    </div>
                )}
            </form>
        </div>
    );
}
