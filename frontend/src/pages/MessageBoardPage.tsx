import { useState } from "react";

import { getApiErrorMessage } from "../api/client";
import { createMessageBoardEntry } from "../api/messageBoard";

const panelClass =
    "rounded-[1.8rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.95),rgba(247,249,255,0.88))] shadow-[0_22px_54px_rgba(15,23,42,0.06)] backdrop-blur-xl";
const fieldClass =
    "w-full rounded-[1.15rem] border border-slate-200/80 bg-white/92 px-4 py-3 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-sky-400 focus:ring-4 focus:ring-sky-100";

const INITIAL_FORM = {
    requestName: "",
    systemName: "",
    expectedCompletionDate: "",
    detail: "",
};

type MessageBoardFormState = typeof INITIAL_FORM;

function formatSubmittedAt(value: string) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }
    return parsed.toLocaleString("zh-CN");
}

export default function MessageBoardPage() {
    const [form, setForm] = useState<MessageBoardFormState>(INITIAL_FORM);
    const [submitting, setSubmitting] = useState(false);
    const [errorMessage, setErrorMessage] = useState("");
    const [successMessage, setSuccessMessage] = useState("");
    const [successMeta, setSuccessMeta] = useState<{ id: number; createdAt: string } | null>(null);

    function updateField<Key extends keyof MessageBoardFormState>(key: Key, value: MessageBoardFormState[Key]) {
        setForm((current) => ({
            ...current,
            [key]: value,
        }));
    }

    async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
        event.preventDefault();

        const requestName = form.requestName.trim();
        const systemName = form.systemName.trim();
        const detail = form.detail.trim();
        const expectedCompletionDate = form.expectedCompletionDate || null;

        if (requestName.length < 2) {
            setErrorMessage("需求名称至少填写 2 个字符。");
            return;
        }
        if (systemName.length < 2) {
            setErrorMessage("系统名称至少填写 2 个字符。");
            return;
        }
        if (detail.length < 10) {
            setErrorMessage("详细描述至少填写 10 个字符。");
            return;
        }

        setSubmitting(true);
        setErrorMessage("");
        setSuccessMessage("");

        try {
            const response = await createMessageBoardEntry({
                request_name: requestName,
                system_name: systemName,
                detail,
                expected_completion_date: expectedCompletionDate,
            });
            setForm(INITIAL_FORM);
            setSuccessMeta({
                id: response.id,
                createdAt: response.created_at,
            });
            setSuccessMessage(response.message);
        } catch (error) {
            setErrorMessage(getApiErrorMessage(error));
        } finally {
            setSubmitting(false);
        }
    }

    return (
        <div className="space-y-6">
            <section className={`${panelClass} overflow-hidden`}>
                <div className="grid gap-0 lg:grid-cols-[1.05fr_0.95fr]">
                    <div className="relative overflow-hidden border-b border-white/70 px-6 py-7 lg:border-b-0 lg:border-r lg:px-7 lg:py-8">
                        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,_rgba(14,165,233,0.1),_transparent_36%),linear-gradient(180deg,rgba(240,249,255,0.72),rgba(255,255,255,0.08))]" />
                        <div className="relative">
                            <div className="inline-flex items-center rounded-full border border-sky-200/80 bg-sky-50/90 px-3 py-1 text-xs font-semibold tracking-[0.16em] text-sky-700">
                                匿名收集
                            </div>
                            <h2 className="mt-4 max-w-[12ch] text-[2rem] font-semibold tracking-[-0.05em] text-slate-950 lg:text-[2.4rem]">
                                留下需求，我们按系统归档评估
                            </h2>
                            <p className="mt-4 max-w-[48rem] text-sm leading-7 text-slate-600">
                                这个页面面向所有用户开放，游客无需登录也可以提交。留言仅用于产品需求整理和排期评估，不会在前台公开展示。
                            </p>

                            <div className="mt-6 grid gap-3 sm:grid-cols-3">
                                <div className="rounded-[1.3rem] border border-white/80 bg-white/88 p-4 shadow-[0_14px_32px_rgba(15,23,42,0.05)]">
                                    <div className="text-sm font-semibold text-slate-900">游客可用</div>
                                    <div className="mt-2 text-xs leading-6 text-slate-500">不依赖登录状态，打开页面即可直接留言。</div>
                                </div>
                                <div className="rounded-[1.3rem] border border-white/80 bg-white/88 p-4 shadow-[0_14px_32px_rgba(15,23,42,0.05)]">
                                    <div className="text-sm font-semibold text-slate-900">按系统归类</div>
                                    <div className="mt-2 text-xs leading-6 text-slate-500">支持填写系统名称，便于后续分系统整理需求池。</div>
                                </div>
                                <div className="rounded-[1.3rem] border border-white/80 bg-white/88 p-4 shadow-[0_14px_32px_rgba(15,23,42,0.05)]">
                                    <div className="text-sm font-semibold text-slate-900">记录期望时间</div>
                                    <div className="mt-2 text-xs leading-6 text-slate-500">可填写期望完成日期，帮助业务侧表达时效优先级。</div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="px-6 py-7 lg:px-7 lg:py-8">
                        <form className="space-y-4" onSubmit={handleSubmit}>
                            <div className="grid gap-4 md:grid-cols-2">
                                <label className="block">
                                    <div className="mb-2 text-sm font-medium text-slate-700">需求名称</div>
                                    <input
                                        className={fieldClass}
                                        maxLength={200}
                                        onChange={(event) => updateField("requestName", event.target.value)}
                                        placeholder="例如：BI 报表支持导出 PPT"
                                        type="text"
                                        value={form.requestName}
                                    />
                                </label>

                                <label className="block">
                                    <div className="mb-2 text-sm font-medium text-slate-700">系统名称</div>
                                    <input
                                        className={fieldClass}
                                        maxLength={120}
                                        onChange={(event) => updateField("systemName", event.target.value)}
                                        placeholder="例如：会员工具 / BI / 对账核销"
                                        type="text"
                                        value={form.systemName}
                                    />
                                </label>
                            </div>

                            <label className="block">
                                <div className="mb-2 text-sm font-medium text-slate-700">期望完成时间</div>
                                <input
                                    className={fieldClass}
                                    onChange={(event) => updateField("expectedCompletionDate", event.target.value)}
                                    type="date"
                                    value={form.expectedCompletionDate}
                                />
                            </label>

                            <label className="block">
                                <div className="mb-2 text-sm font-medium text-slate-700">详细描述</div>
                                <textarea
                                    className={`${fieldClass} min-h-[220px] resize-y leading-7`}
                                    maxLength={5000}
                                    onChange={(event) => updateField("detail", event.target.value)}
                                    placeholder="请尽量描述业务场景、期望结果、当前痛点和使用频率，便于后续快速评估。"
                                    value={form.detail}
                                />
                            </label>

                            {errorMessage ? (
                                <div className="rounded-[1.1rem] border border-rose-200 bg-rose-50 px-4 py-3 text-sm leading-6 text-rose-700">
                                    {errorMessage}
                                </div>
                            ) : null}

                            {successMessage ? (
                                <div className="rounded-[1.1rem] border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm leading-6 text-emerald-700">
                                    <div>{successMessage}</div>
                                    {successMeta ? (
                                        <div className="mt-1 text-xs text-emerald-600">
                                            留言编号 #{successMeta.id}，提交时间 {formatSubmittedAt(successMeta.createdAt)}
                                        </div>
                                    ) : null}
                                </div>
                            ) : null}

                            <div className="flex flex-wrap items-center justify-between gap-3 pt-2">
                                <div className="text-xs leading-6 text-slate-500">提交后将进入需求池，后续按系统和优先级统一评估。</div>
                                <button
                                    className={[
                                        "inline-flex h-11 min-w-[132px] items-center justify-center rounded-[1.1rem] px-5 text-sm font-semibold transition duration-200 motion-reduce:transition-none",
                                        submitting
                                            ? "cursor-wait bg-slate-300 text-white"
                                            : "cursor-pointer bg-[linear-gradient(135deg,#0369a1,#0ea5e9)] text-white shadow-[0_16px_34px_rgba(14,165,233,0.26)] hover:translate-y-[-1px] hover:brightness-[1.02]",
                                    ].join(" ")}
                                    disabled={submitting}
                                    type="submit"
                                >
                                    {submitting ? "提交中..." : "提交留言"}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            </section>
        </div>
    );
}
