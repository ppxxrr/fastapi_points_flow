interface TopbarProps {
    operatorName?: string;
    isAuthenticated: boolean;
    onLogout?: () => void;
    onLogin?: () => void;
    title?: string;
    subtitle?: string;
}

export default function Topbar({
    operatorName,
    isAuthenticated,
    onLogout,
    onLogin,
    title,
}: TopbarProps) {
    const normalizedTitle = (title || "").trim();
    const hasTitle = Boolean(normalizedTitle) && normalizedTitle !== "小工具";

    return (
        <header className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
            <div>{hasTitle ? <h1 className="text-[2.1rem] font-semibold tracking-[-0.055em] text-slate-950">{normalizedTitle}</h1> : null}</div>

            <div className="flex items-center gap-3 self-start">
                {isAuthenticated ? (
                    <>
                        <div className="flex h-11 w-11 items-center justify-center rounded-[1.1rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(247,249,255,0.84))] shadow-[0_16px_32px_rgba(76,108,180,0.08)]">
                            <div className="relative flex h-8 w-8 items-center justify-center rounded-full bg-gradient-to-br from-sky-500 via-blue-500 to-violet-500 text-xs font-semibold text-white">
                                {(operatorName || "G").slice(0, 1).toUpperCase()}
                                <span className="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full border border-white bg-emerald-400" />
                            </div>
                        </div>

                        <button
                            className="inline-flex h-11 cursor-pointer items-center justify-center gap-2 rounded-[1.1rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(247,249,255,0.84))] px-4 text-sm font-medium text-slate-700 shadow-[0_16px_32px_rgba(76,108,180,0.08)] transition hover:text-slate-950"
                            onClick={onLogout}
                            type="button"
                        >
                            <svg className="h-4.5 w-4.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                                <path d="M15 18l5-6-5-6" strokeLinecap="round" strokeLinejoin="round" />
                                <path d="M20 12H9" strokeLinecap="round" strokeLinejoin="round" />
                                <path d="M9 5v14" strokeLinecap="round" strokeLinejoin="round" />
                            </svg>
                            <span>退出</span>
                        </button>
                    </>
                ) : (
                    <button
                        className="inline-flex h-11 min-w-[92px] shrink-0 cursor-pointer items-center justify-center gap-2 whitespace-nowrap rounded-[1.1rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 px-4 text-sm font-semibold text-white shadow-[0_16px_32px_rgba(79,113,255,0.2)] transition hover:brightness-[1.04]"
                        onClick={onLogin}
                        type="button"
                    >
                        <svg className="h-4.5 w-4.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                            <path d="M9 5v14" strokeLinecap="round" strokeLinejoin="round" />
                            <path d="M20 12H9" strokeLinecap="round" strokeLinejoin="round" />
                            <path d="M15 6l5 6-5 6" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                        <span>登录</span>
                    </button>
                )}
            </div>
        </header>
    );
}
