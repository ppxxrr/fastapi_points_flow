import { type FormEvent, useState } from "react";

import { useAuth } from "../auth/AuthContext";
import LoginBackground from "../components/LoginBackground";

function BrandMark() {
    return (
        <div className="flex h-14 w-14 items-center justify-center rounded-[1.2rem] bg-gradient-to-br from-sky-400 via-blue-500 to-violet-500 shadow-[0_20px_52px_rgba(88,123,255,0.28)] ring-1 ring-white/65">
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
        <div className="relative flex min-h-screen items-center justify-center overflow-hidden px-6 py-10">
            <LoginBackground />

            <div className="relative w-full max-w-[31rem]">
                <div className="pointer-events-none absolute inset-x-6 inset-y-6 rounded-[2.5rem] bg-[radial-gradient(circle,rgba(96,165,250,0.24),rgba(129,140,248,0.12),transparent_72%)] blur-[58px]" />

                <div className="relative mx-auto flex min-h-[27.75rem] w-full max-w-[26.8rem] flex-col rounded-[2.35rem] border border-white/55 bg-[linear-gradient(180deg,rgba(255,255,255,0.6),rgba(247,249,255,0.38))] px-7 py-7 shadow-[0_34px_88px_rgba(71,93,160,0.12)] backdrop-blur-[46px]">
                    <div className="pointer-events-none absolute inset-0 rounded-[2.35rem] bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.14),_transparent_28%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.08),_transparent_32%)]" />

                    <div className="relative flex flex-col items-center text-center">
                        <BrandMark />
                        <div className="mt-4 inline-flex items-center rounded-full border border-blue-100/65 bg-white/58 px-3.5 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.24em] text-blue-600 shadow-[0_8px_20px_rgba(129,140,248,0.08)]">
                            Smart Workbench
                        </div>
                        <h1 className="mt-4 text-[2.08rem] font-semibold tracking-[-0.055em] text-slate-950">
                            智能工作台
                        </h1>
                    </div>

                    <form className="relative mt-7 flex flex-1 flex-col justify-between" onSubmit={handleSubmit}>
                        <div className="space-y-4.5">
                            <div className="grid grid-cols-[3.5rem_minmax(0,1fr)] items-center gap-3.5">
                                <label className="text-sm font-medium text-slate-800">账号</label>
                                <input
                                    className="h-[2.95rem] w-full rounded-[1.1rem] border border-slate-200/70 bg-white/76 px-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white/92 focus:ring-4 focus:ring-blue-100"
                                    placeholder="请输入账号"
                                    value={username}
                                    onChange={(event) => {
                                        clearLoginError();
                                        setUsername(event.target.value);
                                    }}
                                    required
                                />
                            </div>

                            <div className="grid grid-cols-[3.5rem_minmax(0,1fr)] items-center gap-3.5">
                                <label className="text-sm font-medium text-slate-800">密码</label>
                                <input
                                    className="h-[2.95rem] w-full rounded-[1.1rem] border border-slate-200/70 bg-white/76 px-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white/92 focus:ring-4 focus:ring-blue-100"
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

                            {loginError && (
                                <div className="rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                                    {loginError}
                                </div>
                            )}
                        </div>

                        <button
                            className="mt-6 h-[3.35rem] w-full cursor-pointer rounded-[1.2rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 text-[0.95rem] font-semibold text-white shadow-[0_22px_48px_rgba(79,113,255,0.3)] transition hover:brightness-[1.04] focus:outline-none focus:ring-4 focus:ring-blue-200 disabled:cursor-not-allowed disabled:opacity-60"
                            disabled={isLoggingIn}
                            type="submit"
                        >
                            {isLoggingIn ? "登录中..." : "登录"}
                        </button>
                    </form>
                </div>
            </div>
        </div>
    );
}
