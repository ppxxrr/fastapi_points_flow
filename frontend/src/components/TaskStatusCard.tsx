export type TaskStatus = "idle" | "pending" | "running" | "success" | "failed";


export interface TaskStatusData {
    taskId: string;
    type: string;
    status: TaskStatus;
    createdAt: string;
    updatedAt: string;
    params: {
        username: string;
        startDate: string;
        endDate: string;
    };
    resultCount: number;
    error?: string;
}


interface TaskStatusCardProps {
    task: TaskStatusData;
}


const statusMap: Record<TaskStatus, { label: string; badge: string; progress: string; width: string }> = {
    idle: {
        label: "未创建",
        badge: "bg-slate-100 text-slate-600",
        progress: "等待填写任务参数。",
        width: "w-[12%]",
    },
    pending: {
        label: "待执行",
        badge: "bg-amber-100 text-amber-700",
        progress: "任务已提交，等待进入后台执行队列。",
        width: "w-[36%]",
    },
    running: {
        label: "执行中",
        badge: "bg-blue-100 text-blue-700",
        progress: "正在抓取积分流水并准备导出 Excel。",
        width: "w-[68%]",
    },
    success: {
        label: "已完成",
        badge: "bg-emerald-100 text-emerald-700",
        progress: "任务执行完成，可进入结果区下载文件。",
        width: "w-full",
    },
    failed: {
        label: "失败",
        badge: "bg-rose-100 text-rose-700",
        progress: "执行已中断，请查看日志与错误信息。",
        width: "w-[88%]",
    },
};


export default function TaskStatusCard({ task }: TaskStatusCardProps) {
    const statusMeta = statusMap[task.status];

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-start justify-between gap-4">
                <div>
                    <p className="text-xs uppercase tracking-[0.2em] text-slate-400">Current Task</p>
                    <h3 className="mt-2 text-[1.95rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        当前任务状态
                    </h3>
                </div>
                <span className={["rounded-full px-3 py-1.5 text-sm font-medium", statusMeta.badge].join(" ")}>
                    {statusMeta.label}
                </span>
            </div>

            <div className="mt-6 rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.84),rgba(245,247,252,0.78))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]">
                <div className="text-xs uppercase tracking-[0.18em] text-slate-400">Task ID</div>
                <div className="mt-2 break-all text-sm font-medium text-slate-900">{task.taskId}</div>

                <div className="mt-5 grid grid-cols-2 gap-4 text-sm">
                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">任务类型</div>
                        <div className="mt-1 font-medium text-slate-900">{task.type}</div>
                    </div>
                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">结果条数</div>
                        <div className="mt-1 font-medium text-slate-900">{task.resultCount || 0}</div>
                    </div>
                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">创建时间</div>
                        <div className="mt-1 font-medium text-slate-900">{task.createdAt}</div>
                    </div>
                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">更新时间</div>
                        <div className="mt-1 font-medium text-slate-900">{task.updatedAt}</div>
                    </div>
                </div>

                <div className="mt-5 rounded-[1.35rem] bg-[linear-gradient(135deg,rgba(239,246,255,0.96),rgba(245,243,255,0.86))] p-4 shadow-[0_16px_30px_rgba(88,123,255,0.08)]">
                    <div className="flex items-center justify-between text-sm">
                        <span className="font-medium text-slate-900">执行进度</span>
                        <span className="text-slate-500">{statusMeta.label}</span>
                    </div>
                    <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-white/75">
                        <div className={["h-full rounded-full bg-gradient-to-r from-blue-500 to-violet-500", statusMeta.width].join(" ")} />
                    </div>
                    <p className="mt-3 text-sm leading-6 text-slate-600">{statusMeta.progress}</p>
                </div>

                <div className="mt-5 space-y-3 text-sm">
                    <div className="rounded-[1.2rem] border border-slate-200/90 bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">任务参数</div>
                        <div className="mt-2 space-y-1 text-slate-900">
                            <div>账号：{task.params.username || "未填写"}</div>
                            <div>开始日期：{task.params.startDate || "未设置"}</div>
                            <div>结束日期：{task.params.endDate || "未设置"}</div>
                        </div>
                    </div>

                    <div className="rounded-[1.2rem] border border-slate-200/90 bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-slate-400">错误信息</div>
                        <div className="mt-2 text-slate-900">{task.error || "当前无错误"}</div>
                    </div>
                </div>
            </div>
        </div>
    );
}
