interface TopbarProps {
    operatorName: string;
    currentTaskId?: string;
    onLogout: () => void;
}

export default function Topbar({ operatorName, currentTaskId, onLogout }: TopbarProps) {
    return (
        <header className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
            <div>
                <p className="text-xs uppercase tracking-[0.28em] text-slate-400">Points Flow Admin</p>
                <h1 className="mt-3 text-[2.55rem] font-semibold tracking-[-0.06em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                    欢迎回来，{operatorName}
                </h1>
                <p className="mt-3 max-w-2xl text-base text-slate-600" style={{ fontFamily: "'Fira Sans', sans-serif" }}>
                    提交导出任务、追踪执行状态、阅读实时日志，并在任务完成后下载积分流水 Excel 文件。
                </p>
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                <div className="flex h-14 min-w-[280px] items-center gap-3 rounded-[1.3rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(247,249,255,0.84))] px-4 shadow-[0_18px_35px_rgba(76,108,180,0.08)]">
                    <svg className="h-5 w-5 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                        <path d="M21 21l-4.3-4.3M10.8 18a7.2 7.2 0 100-14.4 7.2 7.2 0 000 14.4z" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                    <input
                        className="w-full bg-transparent text-sm text-slate-700 outline-none placeholder:text-slate-400"
                        placeholder="按任务 ID 或导出文件名查找"
                    />
                </div>

                <div className="flex items-center gap-3">
                    <div className="rounded-[1.25rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(247,249,255,0.84))] px-4 py-3 text-sm shadow-[0_18px_35px_rgba(76,108,180,0.08)]">
                        <div className="text-xs uppercase tracking-[0.16em] text-slate-400">当前任务</div>
                        <div className="mt-1 font-medium text-slate-900">{currentTaskId || "等待创建任务"}</div>
                    </div>

                    <div className="flex items-center gap-3 rounded-[1.25rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(247,249,255,0.84))] px-3 py-2.5 shadow-[0_18px_35px_rgba(76,108,180,0.08)]">
                        <div className="flex h-10 w-10 items-center justify-center rounded-[1rem] bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500 text-sm font-semibold text-white shadow-[0_14px_28px_rgba(88,123,255,0.25)]">
                            {operatorName.slice(0, 1).toUpperCase()}
                        </div>
                        <div>
                            <div className="text-sm font-medium text-slate-900">{operatorName}</div>
                            <div className="text-xs text-slate-500">当前已登录 ICSP 用户</div>
                        </div>
                    </div>

                    <button
                        className="inline-flex h-12 cursor-pointer items-center justify-center rounded-[1.25rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.88),rgba(247,249,255,0.84))] px-4 text-sm font-medium text-slate-700 shadow-[0_18px_35px_rgba(76,108,180,0.08)] transition hover:text-slate-950"
                        onClick={onLogout}
                        type="button"
                    >
                        退出登录
                    </button>
                </div>
            </div>
        </header>
    );
}
