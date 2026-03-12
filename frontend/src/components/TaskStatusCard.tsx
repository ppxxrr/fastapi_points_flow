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

const statusMap: Record<
    TaskStatus,
    { label: string; badge: string; bar: string; percent: string }
> = {
    idle: {
        label: "未开始",
        badge: "bg-slate-100 text-slate-600",
        bar: "from-slate-300 to-slate-400",
        percent: "8%",
    },
    pending: {
        label: "待执行",
        badge: "bg-amber-100 text-amber-700",
        bar: "from-amber-400 to-orange-400",
        percent: "34%",
    },
    running: {
        label: "执行中",
        badge: "bg-blue-100 text-blue-700",
        bar: "from-sky-500 to-indigo-500",
        percent: "68%",
    },
    success: {
        label: "已完成",
        badge: "bg-emerald-100 text-emerald-700",
        bar: "from-emerald-400 to-teal-500",
        percent: "100%",
    },
    failed: {
        label: "失败",
        badge: "bg-rose-100 text-rose-700",
        bar: "from-rose-400 to-pink-500",
        percent: "100%",
    },
};

function renderTaskId(taskId: string) {
    if (!taskId || taskId === "等待创建") {
        return "等待创建";
    }
    return taskId;
}

export default function TaskStatusCard({ task }: TaskStatusCardProps) {
    const meta = statusMap[task.status];

    return (
        <div className="flex h-full flex-col">
            <div className="flex items-center justify-between gap-4">
                <div>
                    <p className="text-[0.72rem] font-medium uppercase tracking-[0.22em] text-slate-400">
                        Progress
                    </p>
                    <h3 className="mt-2 text-[1.65rem] font-semibold tracking-[-0.045em] text-slate-950">
                        执行进度
                    </h3>
                </div>

                <span className={["rounded-full px-3 py-1.5 text-sm font-medium", meta.badge].join(" ")}>
                    {meta.label}
                </span>
            </div>

            <div className="mt-5 rounded-[1.6rem] border border-slate-200/90 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(246,248,253,0.8))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)]">
                <div className="flex items-center justify-between text-sm">
                    <span className="font-medium text-slate-900">任务状态</span>
                    <span className="text-slate-500">{meta.label}</span>
                </div>

                <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-white/85">
                    <div
                        className={`h-full rounded-full bg-gradient-to-r ${meta.bar}`}
                        style={{ width: meta.percent }}
                    />
                </div>

                <div className="mt-5 grid gap-3 sm:grid-cols-2">
                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Task ID</div>
                        <div className="mt-2 break-all text-sm font-medium text-slate-900">
                            {renderTaskId(task.taskId)}
                        </div>
                    </div>

                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">结果条数</div>
                        <div className="mt-2 text-[1.55rem] font-semibold tracking-[-0.04em] text-slate-950">
                            {task.resultCount || 0}
                        </div>
                    </div>

                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">创建时间</div>
                        <div className="mt-2 text-sm font-medium text-slate-900">
                            {task.createdAt || "待创建"}
                        </div>
                    </div>

                    <div className="rounded-[1.15rem] bg-white/90 px-4 py-3 shadow-[0_10px_20px_rgba(15,23,42,0.04)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">更新时间</div>
                        <div className="mt-2 text-sm font-medium text-slate-900">
                            {task.updatedAt || "待更新"}
                        </div>
                    </div>
                </div>

                {task.error && (
                    <div className="mt-4 rounded-[1.15rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                        {task.error}
                    </div>
                )}
            </div>
        </div>
    );
}
