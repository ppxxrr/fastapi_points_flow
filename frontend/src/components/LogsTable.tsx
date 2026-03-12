export interface TaskLogItem {
    time: string;
    level: string;
    message: string;
}


interface LogsTableProps {
    logs: TaskLogItem[];
}


function levelTone(level: string) {
    const normalized = level.toUpperCase();
    if (normalized === "SUCCESS") {
        return "bg-emerald-100 text-emerald-700";
    }
    if (normalized === "ERROR") {
        return "bg-rose-100 text-rose-700";
    }
    if (normalized === "WARN" || normalized === "WARNING") {
        return "bg-amber-100 text-amber-700";
    }
    return "bg-blue-100 text-blue-700";
}


export default function LogsTable({ logs }: LogsTableProps) {
    return (
        <div className="flex h-full flex-col">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Latest Task Logs</p>
                    <h3 className="mt-2 text-[1.95rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        最新任务日志
                    </h3>
                </div>
                <div className="rounded-[1.15rem] border border-slate-200/90 bg-white/90 px-4 py-2 text-sm text-slate-500 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                    共 {logs.length} 条日志
                </div>
            </div>

            <div className="mt-5 overflow-hidden rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(248,250,255,0.88))] shadow-[0_18px_38px_rgba(15,23,42,0.05)]">
                <div className="grid grid-cols-[160px_110px_1fr] gap-4 border-b border-slate-200/90 bg-white/80 px-5 py-4 text-sm font-medium text-slate-500 backdrop-blur-sm">
                    <div>时间</div>
                    <div>级别</div>
                    <div>消息</div>
                </div>

                <div className="max-h-[420px] overflow-y-auto">
                    {logs.length === 0 ? (
                        <div className="px-5 py-10 text-center text-sm text-slate-500">
                            暂无任务日志，提交导出任务后将在这里看到关键执行过程。
                        </div>
                    ) : (
                        logs.map((log, index) => (
                            <div
                                key={`${log.time}-${index}`}
                                className="grid grid-cols-[160px_110px_1fr] gap-4 border-b border-slate-100/90 px-5 py-4 text-sm last:border-b-0 hover:bg-white/72"
                            >
                                <div className="text-slate-500">{log.time}</div>
                                <div>
                                    <span className={["rounded-full px-2.5 py-1 text-xs font-medium", levelTone(log.level)].join(" ")}>
                                        {log.level}
                                    </span>
                                </div>
                                <div className="leading-6 text-slate-800">{log.message}</div>
                            </div>
                        ))
                    )}
                </div>
            </div>
        </div>
    );
}
