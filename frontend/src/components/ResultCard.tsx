import type { TaskStatus } from "./TaskStatusCard";

export interface ResultCardData {
    fileName: string;
    resultCount: number;
    status: TaskStatus;
    updatedAt?: string;
    error?: string;
    downloadHref?: string;
}

interface ResultCardProps {
    data: ResultCardData;
}

export default function ResultCard({ data }: ResultCardProps) {
    const canDownload = data.status === "success" && Boolean(data.fileName) && Boolean(data.downloadHref);

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-center justify-between gap-4">
                <div>
                    <p className="text-[0.72rem] font-medium uppercase tracking-[0.22em] text-slate-400">
                        Result
                    </p>
                    <h3 className="mt-2 text-[1.65rem] font-semibold tracking-[-0.045em] text-slate-950">
                        导出结果
                    </h3>
                </div>

                <span className="rounded-full bg-white/85 px-3 py-1.5 text-sm font-medium text-slate-500 shadow-[0_8px_20px_rgba(15,23,42,0.05)]">
                    {data.status === "success" ? "可下载" : "待生成"}
                </span>
            </div>

            <div className="mt-5 flex flex-1 flex-col rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(246,248,253,0.8))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)]">
                <div className="grid gap-3 sm:grid-cols-[1fr_auto]">
                    <div className="rounded-[1.15rem] bg-white/92 px-4 py-3 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">文件</div>
                        <div className="mt-2 break-all text-sm font-medium text-slate-900">
                            {data.fileName || "等待生成"}
                        </div>
                    </div>

                    <div className="rounded-[1.15rem] bg-[linear-gradient(135deg,rgba(239,246,255,0.96),rgba(245,243,255,0.88))] px-4 py-3 shadow-[0_14px_28px_rgba(88,123,255,0.08)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">条数</div>
                        <div className="mt-2 text-[1.75rem] font-semibold tracking-[-0.05em] text-slate-950">
                            {data.resultCount}
                        </div>
                    </div>
                </div>

                <div className="mt-4 rounded-[1.15rem] bg-white/92 px-4 py-3 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                    <div className="text-xs uppercase tracking-[0.16em] text-slate-400">更新时间</div>
                    <div className="mt-2 text-sm font-medium text-slate-900">
                        {data.updatedAt || "待更新"}
                    </div>
                </div>

                {data.error && (
                    <div className="mt-4 rounded-[1.15rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                        {data.error}
                    </div>
                )}

                {canDownload ? (
                    <a
                        className="mt-5 inline-flex h-11 cursor-pointer items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-5 text-sm font-medium text-white shadow-[0_18px_38px_rgba(59,130,246,0.22)] transition hover:brightness-[1.04]"
                        href={data.downloadHref}
                    >
                        下载文件
                    </a>
                ) : (
                    <button
                        className="mt-5 inline-flex h-11 cursor-not-allowed items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-5 text-sm font-medium text-white opacity-45"
                        disabled
                        type="button"
                    >
                        下载文件
                    </button>
                )}
            </div>
        </div>
    );
}
