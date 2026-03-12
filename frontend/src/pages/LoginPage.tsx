import { type FormEvent, useState } from "react";

import { useAuth } from "../auth/AuthContext";


function FieldIcon({ kind }: { kind: "user" | "lock" }) {
    if (kind === "lock") {
        return (
            <svg className="h-5 w-5 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                <path d="M8 10V7.8a4 4 0 118 0V10" strokeLinecap="round" strokeLinejoin="round" />
                <rect x="5" y="10" width="14" height="10" rx="3" />
            </svg>
        );
    }

    return (
        <svg className="h-5 w-5 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
            <path d="M20 21a8 8 0 10-16 0" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx="12" cy="8" r="4" />
        </svg>
    );
}


function BrandMark() {
    return (
        <div className="flex h-14 w-14 items-center justify-center rounded-[1.2rem] bg-gradient-to-br from-sky-400 via-blue-500 to-violet-500 shadow-[0_18px_48px_rgba(88,123,255,0.28)] ring-1 ring-white/65">
            <svg className="h-7 w-7 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                <path d="M12 3l6 3.5v5.5c0 4.5-2.7 7.9-6 9-3.3-1.1-6-4.5-6-9V6.5L12 3z" />
                <path d="M9.5 12.5l1.7 1.7 3.3-4.4" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
        </div>
    );
}


export default function LoginPage() {
    const { login, isLoggingIn, loginError, clearLoginError } = useAuth();
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");

    async function handleSubmit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const ok = await login(username, password);
        if (ok) {
            setPassword("");
        }
    }

    return (
        <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[radial-gradient(circle_at_top_left,_rgba(103,168,255,0.16),_transparent_30%),radial-gradient(circle_at_82%_18%,_rgba(129,140,248,0.15),_transparent_26%),linear-gradient(180deg,_#fbfdff_0%,_#eef4ff_100%)] px-6 py-8">
            <div className="pointer-events-none absolute -left-24 top-8 h-[23rem] w-[23rem] rounded-full border-[24px] border-sky-300/28 blur-[2px]" />
            <div className="pointer-events-none absolute right-[-130px] top-[-40px] h-[28rem] w-[28rem] rounded-full border-[50px] border-violet-300/16 blur-[3px]" />
            <div className="pointer-events-none absolute bottom-[-140px] left-[-90px] h-[22rem] w-[22rem] rounded-full bg-gradient-to-br from-sky-200/42 to-violet-200/22 blur-3xl" />
            <div className="pointer-events-none absolute bottom-[-120px] right-[-60px] h-[18rem] w-[18rem] rounded-full bg-gradient-to-br from-blue-300/22 to-violet-300/18 blur-3xl" />
            <div className="pointer-events-none absolute left-1/4 top-[34%] h-20 w-20 rounded-full bg-sky-300/20 blur-2xl" />
            <div className="pointer-events-none absolute right-[18%] top-[19%] h-10 w-10 rounded-full bg-violet-400/25 blur-md" />

            <div className="relative w-full max-w-[27rem] rounded-[2rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(246,249,255,0.78))] px-8 py-7 shadow-[0_34px_100px_rgba(71,93,160,0.16)] backdrop-blur-[24px]">
                <div className="pointer-events-none absolute inset-0 rounded-[2rem] bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.12),_transparent_28%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.07),_transparent_28%)]" />

                <div className="relative mb-6 flex flex-col items-center text-center">
                    <BrandMark />
                    <div className="mt-4 inline-flex items-center rounded-full border border-blue-100 bg-white/85 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-blue-600 shadow-[0_8px_20px_rgba(129,140,248,0.1)]">
                        Smart Workbench
                    </div>
                    <h1 className="mt-4 text-[2rem] font-semibold tracking-[-0.05em] text-slate-950">
                        智能工作台
                    </h1>
                </div>

                <form className="relative space-y-4" onSubmit={handleSubmit}>
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-800">账号</label>
                        <div className="relative">
                            <div className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2">
                                <FieldIcon kind="user" />
                            </div>
                            <input
                                className="h-11 w-full rounded-[1.05rem] border border-slate-200/80 bg-white/90 pl-11 pr-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                                placeholder="请输入账号"
                                value={username}
                                onChange={(event) => {
                                    clearLoginError();
                                    setUsername(event.target.value);
                                }}
                                required
                            />
                        </div>
                    </div>

                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-800">密码</label>
                        <div className="relative">
                            <div className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2">
                                <FieldIcon kind="lock" />
                            </div>
                            <input
                                className="h-11 w-full rounded-[1.05rem] border border-slate-200/80 bg-white/90 pl-11 pr-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                                placeholder="请输入密码"
                                type="password"
                                value={password}
                                onChange={(event) => {
                                    clearLoginError();
                                    setPassword(event.target.value);
                                }}
                                required
                            />
                        </div>
                    </div>

                    {loginError && (
                        <div className="rounded-[1.05rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.9))] px-4 py-3 text-sm leading-6 text-rose-700">
                            {loginError}
                        </div>
                    )}

                    <button
                        className="h-11 w-full cursor-pointer rounded-[1.05rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 text-sm font-medium text-white shadow-[0_18px_40px_rgba(79,113,255,0.28)] transition hover:brightness-[1.04] focus:outline-none focus:ring-4 focus:ring-blue-200 disabled:cursor-not-allowed disabled:opacity-60"
                        disabled={isLoggingIn}
                        type="submit"
                    >
                        {isLoggingIn ? "登录中..." : "登录"}
                    </button>
                </form>
            </div>
        </div>
    );
}
