import type { ReactNode } from "react";

interface SidebarProps {
    collapsed: boolean;
    onToggleCollapse: () => void;
}

function AppIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path
                d="M12 3l6 3.5v5.5c0 4.5-2.7 7.9-6 9-3.3-1.1-6-4.5-6-9V6.5L12 3z"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
            <path d="M9.5 12.5l1.7 1.7 3.3-4.4" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function CollapseIcon({ collapsed }: { collapsed: boolean }) {
    return (
        <svg className="h-4.5 w-4.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            {collapsed ? (
                <path d="M9 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round" />
            ) : (
                <path d="M15 6l-6 6 6 6" strokeLinecap="round" strokeLinejoin="round" />
            )}
        </svg>
    );
}

function NavButton({
    collapsed,
    label,
    icon,
}: {
    collapsed: boolean;
    label: string;
    icon: ReactNode;
}) {
    return (
        <button
            className={[
                "flex w-full cursor-pointer items-center rounded-[1.35rem] border border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(247,249,255,0.9))] text-left text-slate-900 shadow-[0_18px_38px_rgba(76,108,180,0.1)] transition duration-300",
                collapsed ? "justify-center px-0 py-3.5" : "gap-3 px-4 py-4",
            ].join(" ")}
            type="button"
        >
            <div className="flex h-10 w-10 items-center justify-center rounded-[1rem] bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500 text-white shadow-[0_14px_28px_rgba(88,123,255,0.22)]">
                {icon}
            </div>
            {!collapsed && (
                <div className="min-w-0">
                    <div className="text-[1rem] font-medium tracking-[-0.02em]">{label}</div>
                </div>
            )}
        </button>
    );
}

export default function Sidebar({ collapsed, onToggleCollapse }: SidebarProps) {
    return (
        <aside
            className={[
                "relative hidden shrink-0 border-r border-white/75 bg-[linear-gradient(180deg,rgba(244,248,255,0.98),rgba(236,242,252,0.9))] transition-[width] duration-300 lg:flex lg:flex-col",
                collapsed ? "w-[96px]" : "w-[264px]",
            ].join(" ")}
        >
            <div className="pointer-events-none absolute inset-y-0 right-0 w-px bg-white/80" />

            <div className="flex items-center justify-between px-5 pb-4 pt-5">
                {!collapsed && (
                    <div>
                        <p className="text-[0.68rem] font-medium uppercase tracking-[0.24em] text-slate-400">
                            Smart Workbench
                        </p>
                        <div className="mt-2 text-[1.35rem] font-semibold tracking-[-0.04em] text-slate-950">
                            控制台
                        </div>
                    </div>
                )}

                <button
                    className={[
                        "inline-flex h-10 w-10 cursor-pointer items-center justify-center rounded-[1rem] border border-white/80 bg-white/78 text-slate-500 shadow-[0_12px_28px_rgba(76,108,180,0.08)] transition hover:text-slate-900",
                        collapsed ? "mx-auto" : "",
                    ].join(" ")}
                    onClick={onToggleCollapse}
                    type="button"
                >
                    <CollapseIcon collapsed={collapsed} />
                </button>
            </div>

            <div className={["px-4", collapsed ? "pt-4" : "pt-2"].join(" ")}>
                <NavButton collapsed={collapsed} icon={<AppIcon />} label="积分导出" />
            </div>

            <div className="mt-auto px-4 pb-5">
                <div
                    className={[
                        "rounded-[1.4rem] border border-white/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.82),rgba(245,248,255,0.76))] shadow-[0_14px_28px_rgba(76,108,180,0.07)] transition duration-300",
                        collapsed ? "px-0 py-3 text-center" : "px-4 py-4",
                    ].join(" ")}
                >
                    {collapsed ? (
                        <div className="mx-auto h-2.5 w-2.5 rounded-full bg-emerald-400" />
                    ) : (
                        <>
                            <div className="text-[0.7rem] font-medium uppercase tracking-[0.2em] text-slate-400">
                                Program
                            </div>
                            <div className="mt-2 text-sm font-medium text-slate-900">积分导出</div>
                        </>
                    )}
                </div>
            </div>
        </aside>
    );
}
