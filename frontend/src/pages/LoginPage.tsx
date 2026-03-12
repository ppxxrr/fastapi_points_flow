import { type FormEvent, useEffect, useState } from "react";

import { getApiErrorMessage } from "../api/client";
import { getHealthStatus } from "../api/pointsFlow";
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
        <div className="flex h-16 w-16 items-center justify-center rounded-[1.4rem] bg-gradient-to-br from-sky-400 via-blue-500 to-violet-500 shadow-[0_22px_55px_rgba(88,123,255,0.34)] ring-1 ring-white/60">
            <svg className="h-8 w-8 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
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
    const [healthState, setHealthState] = useState<{
        label: string;
        badgeClass: string;
        message: string;
    }>({
        label: "检测中",
        badgeClass: "bg-slate-100 text-slate-600",
        message: "正在检查 FastAPI 服务状态。",
    });

    useEffect(() => {
        let active = true;

        async function loadHealth() {
            try {
                const response = await getHealthStatus();
                if (!active) {
                    return;
                }
                setHealthState({
                    label: response.status === "ok" ? "FastAPI 在线" : "状态异常",
                    badgeClass:
                        response.status === "ok"
                            ? "bg-emerald-100 text-emerald-700"
                            : "bg-amber-100 text-amber-700",
                    message:
                        response.status === "ok"
                            ? "后端服务可用。登录成功后会进入控制台，并使用当前 ICSP 身份发起导出任务。"
                            : `健康检查返回异常状态：${response.status}`,
                });
            } catch (error) {
                if (!active) {
                    return;
                }
                setHealthState({
                    label: "检查失败",
                    badgeClass: "bg-rose-100 text-rose-700",
                    message: getApiErrorMessage(error),
                });
            }
        }

        void loadHealth();
        return () => {
            active = false;
        };
    }, []);

    async function handleSubmit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const ok = await login(username, password);
        if (ok) {
            setPassword("");
        }
    }

    return (
        <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[radial-gradient(circle_at_top_left,_rgba(103,168,255,0.18),_transparent_32%),radial-gradient(circle_at_82%_20%,_rgba(129,140,248,0.16),_transparent_28%),linear-gradient(180deg,_#fafcff_0%,_#eef4ff_100%)] px-6 py-10">
            <div className="pointer-events-none absolute -left-20 top-10 h-[24rem] w-[24rem] rounded-full border-[26px] border-sky-300/35 blur-[2px]" />
            <div className="pointer-events-none absolute right-[-140px] top-[-30px] h-[30rem] w-[30rem] rounded-full border-[54px] border-violet-300/18 blur-[3px]" />
            <div className="pointer-events-none absolute bottom-[-140px] left-[-120px] h-[24rem] w-[24rem] rounded-full bg-gradient-to-br from-sky-200/55 to-violet-200/28 blur-3xl" />
            <div className="pointer-events-none absolute bottom-[-130px] right-[-60px] h-[20rem] w-[20rem] rounded-full bg-gradient-to-br from-blue-300/28 to-violet-300/24 blur-3xl" />
            <div className="pointer-events-none absolute left-1/4 top-[34%] h-24 w-24 rounded-full bg-sky-300/25 blur-2xl" />
            <div className="pointer-events-none absolute right-[18%] top-[19%] h-12 w-12 rounded-full bg-violet-400/30 blur-md" />
            <div className="pointer-events-none absolute inset-x-0 bottom-0 h-48 bg-[linear-gradient(180deg,transparent,rgba(255,255,255,0.7))]" />

            <div className="relative w-full max-w-[30rem] rounded-[2.2rem] border border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(246,249,255,0.78))] p-8 shadow-[0_40px_120px_rgba(71,93,160,0.18)] backdrop-blur-[26px] sm:p-9">
                <div className="pointer-events-none absolute inset-0 rounded-[2.2rem] bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.12),_transparent_30%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.08),_transparent_30%)]" />

                <div className="relative mb-8 flex flex-col items-center text-center">
                    <BrandMark />
                    <div className="mt-6 inline-flex items-center rounded-full border border-blue-100 bg-white/80 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.26em] text-blue-600 shadow-[0_10px_25px_rgba(129,140,248,0.12)]">
                        Points Flow Console
                    </div>
                    <h1 className="mt-5 text-[2.4rem] font-semibold tracking-[-0.05em] text-slate-950" style={{ fontFamily: "'Fira Code', monospace" }}>
                        积分流水导出
                    </h1>
                    <p className="mt-3 max-w-sm text-sm leading-6 text-slate-600" style={{ fontFamily: "'Fira Sans', sans-serif" }}>
                        使用 ICSP 账号密码登录。只有通过 ICSP 真实校验后，才能进入积分流水导出控制台。
                    </p>
                </div>

                <form className="relative space-y-5" onSubmit={handleSubmit}>
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-slate-800">账号</label>
                        <div className="relative">
                            <div className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2">
                                <FieldIcon kind="user" />
                            </div>
                            <input
                                className="h-12 w-full rounded-[1.15rem] border border-slate-200/80 bg-white/88 pl-11 pr-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                                placeholder="请输入 ICSP 用户名"
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
                        <div className="flex items-center justify-between">
                            <label className="text-sm font-medium text-slate-800">密码</label>
                            <span className="text-xs text-slate-500">仅用于 ICSP 登录校验</span>
                        </div>
                        <div className="relative">
                            <div className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2">
                                <FieldIcon kind="lock" />
                            </div>
                            <input
                                className="h-12 w-full rounded-[1.15rem] border border-slate-200/80 bg-white/88 pl-11 pr-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-blue-400 focus:bg-white focus:ring-4 focus:ring-blue-100"
                                placeholder="请输入 ICSP 密码"
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
                        <div className="rounded-[1.15rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.9))] px-4 py-3 text-sm leading-6 text-rose-700">
                            {loginError}
                        </div>
                    )}

                    <div className="rounded-[1.35rem] border border-blue-100/80 bg-[linear-gradient(135deg,rgba(240,247,255,0.95),rgba(244,242,255,0.82))] px-4 py-3 text-sm text-slate-700 shadow-[0_12px_30px_rgba(95,127,193,0.08)]">
                        <div className="flex items-center justify-between">
                            <span className="font-medium text-slate-900">服务状态</span>
                            <span className={["rounded-full px-2.5 py-1 text-xs font-medium", healthState.badgeClass].join(" ")}>
                                {healthState.label}
                            </span>
                        </div>
                        <p className="mt-2 leading-6 text-slate-600">{healthState.message}</p>
                    </div>

                    <button
                        className="h-12 w-full cursor-pointer rounded-[1.15rem] bg-gradient-to-r from-sky-500 via-blue-600 to-violet-500 text-sm font-medium text-white shadow-[0_22px_45px_rgba(79,113,255,0.34)] transition hover:brightness-[1.04] focus:outline-none focus:ring-4 focus:ring-blue-200 disabled:cursor-not-allowed disabled:opacity-60"
                        disabled={isLoggingIn}
                        type="submit"
                    >
                        {isLoggingIn ? "正在校验 ICSP 登录..." : "登录并进入任务控制台"}
                    </button>
                </form>

                <div className="relative mt-7 border-t border-slate-200/80 pt-5 text-center text-sm text-slate-500">
                    登录成功后，FastAPI 会保存服务端登录态，后续导出任务默认复用当前已登录的 ICSP 身份。
                </div>
            </div>
        </div>
    );
}
