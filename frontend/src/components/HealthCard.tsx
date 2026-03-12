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
        label: "在线",
        badge: "bg-emerald-100 text-emerald-700",
        dot: "bg-emerald-500",
    },
    degraded: {
        label: "异常",
        badge: "bg-amber-100 text-amber-700",
        dot: "bg-amber-500",
    },
    offline: {
        label: "离线",
        badge: "bg-rose-100 text-rose-700",
        dot: "bg-rose-500",
    },
};

export default function HealthCard({ data }: HealthCardProps) {
    const tone = toneMap[data.status];

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-center justify-between gap-4">
                <div>
                    <p className="text-[0.72rem] font-medium uppercase tracking-[0.22em] text-slate-400">
                        Service
                    </p>
                    <h3 className="mt-2 text-[1.45rem] font-semibold tracking-[-0.04em] text-slate-950">
                        系统状态
                    </h3>
                </div>

                <span className={["rounded-full px-3 py-1.5 text-sm font-medium", tone.badge].join(" ")}>
                    {data.loading ? "检测中" : tone.label}
                </span>
            </div>

            <div className="mt-5 rounded-[1.45rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(246,248,253,0.8))] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)]">
                <div className="flex items-center gap-3 rounded-[1.1rem] bg-white/92 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                    <span className={["h-2.5 w-2.5 rounded-full", tone.dot].join(" ")} />
                    <div className="text-sm font-medium text-slate-900">FastAPI</div>
                </div>

                <div className="mt-3 rounded-[1.1rem] bg-white/92 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                    <div className="text-xs uppercase tracking-[0.16em] text-slate-400">检查时间</div>
                    <div className="mt-2 text-sm font-medium text-slate-900">{data.checkedAt}</div>
                </div>

                {data.error && (
                    <div className="mt-3 rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                        {data.error}
                    </div>
                )}
            </div>
        </div>
    );
}
