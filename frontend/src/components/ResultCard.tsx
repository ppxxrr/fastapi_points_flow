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
    const statusTone =
        data.status === "success"
            ? "bg-emerald-100 text-emerald-700"
            : data.status === "failed"
              ? "bg-rose-100 text-rose-700"
              : data.status === "running"
                ? "bg-blue-100 text-blue-700"
                : data.status === "pending"
                  ? "bg-amber-100 text-amber-700"
                  : "bg-slate-100 text-slate-600";

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Result</p>
                    <h3 className="mt-2 text-[1.95rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        导出结果
                    </h3>
                </div>
                <span className={["rounded-full px-3 py-1.5 text-sm font-medium", statusTone].join(" ")}>
                    {data.status.toUpperCase()}
                </span>
            </div>

            <div className="mt-6 flex flex-1 flex-col rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.84),rgba(245,247,252,0.78))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
                <div className="rounded-[1.2rem] bg-white/92 px-4 py-4 shadow-[0_12px_24px_rgba(15,23,42,0.04)]">
                    <div className="text-sm text-slate-400">文件名</div>
                    <div className="mt-2 break-all text-sm font-medium text-slate-900">
                        {data.fileName || "任务完成后将在这里展示导出文件名"}
                    </div>
                </div>

                <div className="mt-4 rounded-[1.2rem] bg-[linear-gradient(135deg,rgba(239,246,255,0.96),rgba(245,243,255,0.86))] px-4 py-4 shadow-[0_16px_30px_rgba(88,123,255,0.08)]">
                    <div className="text-sm text-slate-400">结果条数</div>
                    <div className="mt-2 text-3xl font-semibold tracking-[-0.04em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        {data.resultCount}
                    </div>
                </div>

                <div className="mt-4 rounded-[1.2rem] bg-white/92 px-4 py-4 shadow-[0_12px_24px_rgba(15,23,42,0.04)]">
                    <div className="text-sm text-slate-400">更新时间</div>
                    <div className="mt-2 text-sm font-medium text-slate-900">
                        {data.updatedAt || "等待任务执行后更新"}
                    </div>
                </div>

                {data.error && (
                    <div className="mt-4 rounded-[1.2rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.9))] px-4 py-4 text-sm leading-6 text-rose-700">
                        {data.error}
                    </div>
                )}

                {canDownload ? (
                    <a
                        className="mt-auto inline-flex h-11 cursor-pointer items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 text-sm font-medium text-white shadow-[0_20px_40px_rgba(59,130,246,0.22)] transition hover:brightness-[1.04]"
                        href={data.downloadHref}
                    >
                        下载 Excel 文件
                    </a>
                ) : (
                    <button
                        className="mt-auto inline-flex h-11 w-full cursor-not-allowed items-center justify-center rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 text-sm font-medium text-white opacity-45"
                        disabled
                        type="button"
                    >
                        下载 Excel 文件
                    </button>
                )}
            </div>
        </div>
    );
}
