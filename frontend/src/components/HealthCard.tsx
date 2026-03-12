export interface HealthCardData {
    status: "online" | "degraded" | "offline";
    checkedAt: string;
    apiBase: string;
    note: string;
    loading?: boolean;
    error?: string;
}


interface HealthCardProps {
    data: HealthCardData;
}


const toneMap = {
    online: {
        label: "服务在线",
        badge: "bg-emerald-100 text-emerald-700",
        dot: "bg-emerald-500",
    },
    degraded: {
        label: "部分异常",
        badge: "bg-amber-100 text-amber-700",
        dot: "bg-amber-500",
    },
    offline: {
        label: "服务离线",
        badge: "bg-rose-100 text-rose-700",
        dot: "bg-rose-500",
    },
};


export default function HealthCard({ data }: HealthCardProps) {
    const tone = toneMap[data.status];

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">System</p>
                    <h3 className="mt-2 text-[1.85rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        健康检查
                    </h3>
                </div>
                <span className={["rounded-full px-3 py-1.5 text-sm font-medium", tone.badge].join(" ")}>
                    {data.loading ? "检测中" : tone.label}
                </span>
            </div>

            <div className="mt-6 flex-1 rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.84),rgba(245,247,252,0.78))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
                <div className="flex items-center gap-3 rounded-[1.2rem] bg-white/92 px-4 py-4 shadow-[0_12px_24px_rgba(15,23,42,0.04)]">
                    <span className={["h-3 w-3 rounded-full", tone.dot].join(" ")} />
                    <div>
                        <div className="text-sm font-medium text-slate-900">FastAPI 状态</div>
                        <div className="text-sm text-slate-500">{data.status.toUpperCase()}</div>
                    </div>
                </div>

                <div className="mt-5 space-y-3">
                    <div className="rounded-[1.15rem] bg-white/92 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-sm text-slate-400">检查时间</div>
                        <div className="mt-1 text-sm font-medium text-slate-900">{data.checkedAt}</div>
                    </div>
                    <div className="rounded-[1.15rem] bg-white/92 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-sm text-slate-400">API 基础路径</div>
                        <div className="mt-1 text-sm font-medium text-slate-900">{data.apiBase}</div>
                    </div>
                    <div className="rounded-[1.15rem] bg-white/92 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-sm text-slate-400">当前说明</div>
                        <div className="mt-1 text-sm leading-6 text-slate-900">{data.note}</div>
                    </div>
                    {data.error && (
                        <div className="rounded-[1.15rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.9))] px-4 py-3">
                            <div className="text-sm text-rose-500">错误信息</div>
                            <div className="mt-1 text-sm leading-6 text-rose-700">{data.error}</div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
