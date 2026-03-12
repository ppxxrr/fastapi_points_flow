import type { ReactNode } from "react";


export type SidebarSection = "create" | "status" | "logs" | "results" | "health";


interface SidebarProps {
    active: SidebarSection;
    operatorName: string;
    onNavigate: (section: SidebarSection) => void;
    onLogout: () => void;
}


interface NavItem {
    key: SidebarSection;
    label: string;
    hint: string;
    icon: ReactNode;
}


function NavIcon(props: { path: string }) {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d={props.path} strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}


const navItems: NavItem[] = [
    {
        key: "create",
        label: "积分流水导出",
        hint: "创建新任务",
        icon: <NavIcon path="M12 5v14M5 12h14" />,
    },
    {
        key: "status",
        label: "当前任务状态",
        hint: "执行进度",
        icon: <NavIcon path="M12 8v4l2.5 2.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />,
    },
    {
        key: "logs",
        label: "最新任务日志",
        hint: "过程回放",
        icon: <NavIcon path="M8 6h10M8 12h10M8 18h6M4 6h.01M4 12h.01M4 18h.01" />,
    },
    {
        key: "results",
        label: "导出结果",
        hint: "Excel 下载",
        icon: <NavIcon path="M12 3v12m0 0l4-4m-4 4l-4-4M5 21h14" />,
    },
    {
        key: "health",
        label: "系统健康检查",
        hint: "服务可用性",
        icon: <NavIcon path="M4 13h4l2-5 4 10 2-5h4" />,
    },
];


export default function Sidebar({
    active,
    operatorName,
    onNavigate,
    onLogout,
}: SidebarProps) {
    return (
        <aside className="relative hidden border-r border-white/75 bg-[linear-gradient(180deg,rgba(244,248,255,0.98),rgba(236,242,252,0.9))] lg:flex">
            <div className="pointer-events-none absolute inset-y-0 right-0 w-px bg-white/80" />

            <div className="flex w-20 flex-col items-center justify-between border-r border-white/70 bg-white/18 px-3 py-6">
                <div className="flex flex-col items-center gap-4">
                    <div className="flex h-12 w-12 items-center justify-center rounded-[1.1rem] bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500 text-white shadow-[0_20px_42px_rgba(88,123,255,0.28)] ring-1 ring-white/70">
                        <svg className="h-6 w-6" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                            <path d="M13 2L4 14h7l-1 8 10-13h-7l0-7z" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                    </div>

                    <div className="space-y-3">
                        {navItems.map((item) => {
                            const selected = item.key === active;
                            return (
                                <button
                                    key={item.key}
                                    className={[
                                        "flex h-12 w-12 cursor-pointer items-center justify-center rounded-[1.05rem] transition",
                                        selected
                                            ? "bg-white text-blue-600 shadow-[0_16px_30px_rgba(76,108,180,0.16)]"
                                            : "text-slate-500 hover:bg-white/75 hover:text-slate-900",
                                    ].join(" ")}
                                    onClick={() => onNavigate(item.key)}
                                    type="button"
                                >
                                    {item.icon}
                                </button>
                            );
                        })}
                    </div>
                </div>

                <button
                    className="flex h-12 w-12 cursor-pointer items-center justify-center rounded-[1.05rem] text-slate-500 transition hover:bg-white/75 hover:text-slate-900"
                    onClick={onLogout}
                    type="button"
                >
                    <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                        <path d="M15 18l5-6-5-6" strokeLinecap="round" strokeLinejoin="round" />
                        <path d="M20 12H9" strokeLinecap="round" strokeLinejoin="round" />
                        <path d="M9 5v14" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </button>
            </div>

            <div className="flex w-[320px] flex-col px-6 py-7">
                <div className="mb-8 flex items-start justify-between">
                    <div>
                        <p className="text-xs uppercase tracking-[0.22em] text-slate-400">Points Flow</p>
                        <h2 className="mt-2 text-[2.05rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                            控制台
                        </h2>
                        <p className="mt-2 max-w-[13rem] text-sm leading-6 text-slate-500">
                            积分流水导出任务、状态轮询与结果下载都集中在这里。
                        </p>
                    </div>
                    <span className="rounded-full border border-white/80 bg-white/80 px-3 py-1 text-xs font-medium text-slate-500 shadow-[0_10px_20px_rgba(15,23,42,0.05)]">
                        {operatorName}
                    </span>
                </div>

                <nav className="space-y-3">
                    {navItems.map((item) => {
                        const selected = item.key === active;
                        return (
                            <button
                                key={item.key}
                                className={[
                                    "flex w-full cursor-pointer items-start gap-3 rounded-[1.35rem] px-4 py-4 text-left transition",
                                    selected
                                        ? "bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(247,249,255,0.92))] text-slate-950 shadow-[0_20px_38px_rgba(76,108,180,0.12)]"
                                        : "text-slate-600 hover:bg-white/72 hover:text-slate-950",
                                ].join(" ")}
                                onClick={() => onNavigate(item.key)}
                                type="button"
                            >
                                <div
                                    className={[
                                        "flex h-10 w-10 items-center justify-center rounded-[1rem] transition",
                                        selected
                                            ? "bg-blue-50 text-blue-600 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)]"
                                            : "bg-white/70 text-slate-400",
                                    ].join(" ")}
                                >
                                    {item.icon}
                                </div>
                                <div>
                                    <div className="text-base font-medium">{item.label}</div>
                                    <div className="mt-1 text-sm text-slate-500">{item.hint}</div>
                                </div>
                            </button>
                        );
                    })}
                </nav>

                <div className="mt-auto rounded-[1.6rem] border border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.86),rgba(245,248,255,0.78))] p-4 shadow-[0_18px_35px_rgba(76,108,180,0.08)]">
                    <p className="text-xs uppercase tracking-[0.18em] text-slate-400">工作提示</p>
                    <p className="mt-3 text-sm leading-6 text-slate-600">
                        任务提交后会在右侧主内容区持续轮询，完成后可直接下载积分流水 Excel。
                    </p>
                </div>
            </div>
        </aside>
    );
}
