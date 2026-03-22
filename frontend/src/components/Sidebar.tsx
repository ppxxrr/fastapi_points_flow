import type { ReactNode } from "react";

export interface SidebarNavItem {
    key: string;
    label: string;
    icon: ReactNode;
    active: boolean;
    onClick: () => void;
}

interface SidebarProps {
    collapsed: boolean;
    onToggleCollapse: () => void;
    items: SidebarNavItem[];
    bottomItems?: SidebarNavItem[];
}

function ExportIcon() {
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

function AdminIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path
                d="M10.2 4.55a1 1 0 011.6 0l.77 1.03a1 1 0 001.08.34l1.24-.37a1 1 0 011.13.47l.62 1.14a1 1 0 00.9.53h1.28a1 1 0 01.8 1.38l-.52 1.17a1 1 0 00.14 1.12l.82 1a1 1 0 010 1.28l-.82 1a1 1 0 00-.14 1.12l.52 1.17a1 1 0 01-.8 1.38h-1.28a1 1 0 00-.9.53l-.62 1.14a1 1 0 01-1.13.47l-1.24-.37a1 1 0 00-1.08.34l-.77 1.03a1 1 0 01-1.6 0l-.77-1.03a1 1 0 00-1.08-.34l-1.24.37a1 1 0 01-1.13-.47l-.62-1.14a1 1 0 00-.9-.53H4.96a1 1 0 01-.8-1.38l.52-1.17a1 1 0 00-.14-1.12l-.82-1a1 1 0 010-1.28l.82-1a1 1 0 00.14-1.12l-.52-1.17a1 1 0 01.8-1.38h1.28a1 1 0 00.9-.53l.62-1.14a1 1 0 011.13-.47l1.24.37a1 1 0 001.08-.34l.77-1.03z"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
            <circle cx="12" cy="12" r="3" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function ToolsIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M14 6l4 4" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M5 19l6.5-6.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M11.5 12.5l2-2a3 3 0 114.24 4.24l-2 2" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8.5 15.5l-1 1a2 2 0 01-2.83-2.83l1-1" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function BiIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M5 19V9" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M12 19V5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M19 19v-7" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M4 19h16" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function ReconciliationIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M5 12h14" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8 8h8" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8 16h8" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M4 5h16v14H4z" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function LayoutIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M4 5h16v14H4z" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M9 5v14" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M15 12h5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M4 10h5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function MessageIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M6 18l-2 2V6a2 2 0 012-2h12a2 2 0 012 2v10a2 2 0 01-2 2H6z" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8 8h8" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8 12h6" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
    );
}

function ToolsIcon() {
    return (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M14 6l4 4" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M5 19l6.5-6.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M11.5 12.5l2-2a3 3 0 114.24 4.24l-2 2" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M8.5 15.5l-1 1a2 2 0 01-2.83-2.83l1-1" strokeLinecap="round" strokeLinejoin="round" />
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
    active,
    onClick,
}: {
    collapsed: boolean;
    label: string;
    icon: ReactNode;
    active: boolean;
    onClick: () => void;
}) {
    return (
        <button
            className={[
                "flex w-full cursor-pointer items-center rounded-[1.35rem] border text-left shadow-[0_18px_38px_rgba(76,108,180,0.1)] transition duration-300",
                active
                    ? "border-blue-100/90 bg-[linear-gradient(180deg,rgba(239,246,255,0.98),rgba(243,244,255,0.94))] text-slate-950"
                    : "border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(247,249,255,0.9))] text-slate-900",
                collapsed ? "justify-center px-0 py-3.5" : "gap-3 px-4 py-4",
            ].join(" ")}
            onClick={onClick}
            type="button"
        >
            <div
                className={[
                    "flex h-10 w-10 items-center justify-center rounded-[1rem] text-white shadow-[0_14px_28px_rgba(88,123,255,0.22)]",
                    active
                        ? "bg-gradient-to-br from-slate-900 via-blue-700 to-cyan-500"
                        : "bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500",
                ].join(" ")}
            >
                {icon}
            </div>
            {!collapsed ? <div className="text-[1rem] font-medium tracking-[-0.02em]">{label}</div> : null}
        </button>
    );
}

export { AdminIcon, BiIcon, ExportIcon, LayoutIcon, MessageIcon, ReconciliationIcon, ToolsIcon };

export default function Sidebar({ collapsed, onToggleCollapse, items, bottomItems = [] }: SidebarProps) {
    return (
        <aside
            className={[
                "relative hidden shrink-0 border-r border-white/75 bg-[linear-gradient(180deg,rgba(244,248,255,0.98),rgba(236,242,252,0.9))] transition-[width] duration-300 lg:flex lg:flex-col",
                collapsed ? "w-[96px]" : "w-[264px]",
            ].join(" ")}
        >
            <div className="pointer-events-none absolute inset-y-0 right-0 w-px bg-white/80" />

            <div className="flex justify-end px-5 pb-4 pt-5">
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

            <div className="flex min-h-0 flex-1 flex-col">
                <div className={["space-y-3 px-4", collapsed ? "pt-4" : "pt-2"].join(" ")}>
                    {items.map((item) => (
                        <NavButton
                            key={item.key}
                            active={item.active}
                            collapsed={collapsed}
                            icon={item.icon}
                            label={item.label}
                            onClick={item.onClick}
                        />
                    ))}
                </div>

                {bottomItems.length ? (
                    <div className={["mt-auto px-4 pb-5", collapsed ? "pt-4" : "pt-6"].join(" ")}>
                        <div className="h-px bg-gradient-to-r from-transparent via-slate-200/80 to-transparent" />
                        <div className="pt-4">
                            {bottomItems.map((item) => (
                                <NavButton
                                    key={item.key}
                                    active={item.active}
                                    collapsed={collapsed}
                                    icon={item.icon}
                                    label={item.label}
                                    onClick={item.onClick}
                                />
                            ))}
                        </div>
                    </div>
                ) : null}
            </div>
        </aside>
    );
}
