import { type FormEvent, useState } from "react";

import { useAuth } from "../auth/AuthContext";

function BrandMark() {
    return (
        <div className="flex h-16 w-16 items-center justify-center overflow-hidden rounded-[1.25rem] border border-white/80 bg-white shadow-[0_20px_52px_rgba(88,123,255,0.18)] ring-1 ring-white/70">
            <img
                alt="智能工作台"
                className="h-full w-full object-cover"
                src="/railin.png"
            />
        </div>
    );
}

interface LoginFormPanelProps {
    title?: string;
    subtitle?: string;
    onSuccess?: () => void;
    compact?: boolean;
}

export default function LoginFormPanel({
    title = "智能工作台",
    onSuccess,
    compact = false,
}: LoginFormPanelProps) {
    const { login, isLoggingIn, loginError, clearLoginError } = useAuth();
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");

    async function handleSubmit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const ok = await login(username, password);
        if (!ok) {
            return;
        }
        setPassword("");
        onSuccess?.();
    }

    return (
        <div
            className={[
                "relative mx-auto flex w-full flex-col rounded-[2.1rem] border border-white/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.82),rgba(247,249,255,0.7))] px-7 py-7 shadow-[0_34px_88px_rgba(71,93,160,0.12)] backdrop-blur-[32px]",
                compact ? "max-w-[26rem]" : "min-h-[27.75rem] max-w-[26.8rem]",
            ].join(" ")}
        >
            <div className="pointer-events-none absolute inset-0 rounded-[2.1rem] bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.13),_transparent_28%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.07),_transparent_32%)]" />

            <div className="relative flex flex-col items-center text-center">
                <BrandMark />
                <h2 className="mt-4 text-[2rem] font-semibold tracking-[-0.055em] text-slate-950">{title}</h2>
            </div>

            <form
                className={["relative mt-7 flex flex-1 flex-col", compact ? "gap-6" : "justify-between"].join(" ")}
                onSubmit={handleSubmit}
            >
                <div className="space-y-5">
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

                    {loginError ? (
                        <div className="rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                            {loginError}
                        </div>
                    ) : null}
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
    );
}
