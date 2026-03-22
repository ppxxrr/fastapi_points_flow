import { useEffect } from "react";

import LoginFormPanel from "./LoginFormPanel";

interface LoginDialogProps {
    open: boolean;
    onClose: () => void;
}

export default function LoginDialog({ open, onClose }: LoginDialogProps) {
    useEffect(() => {
        if (!open) {
            return;
        }

        function onKeyDown(event: KeyboardEvent) {
            if (event.key === "Escape") {
                onClose();
            }
        }

        window.addEventListener("keydown", onKeyDown);
        return () => window.removeEventListener("keydown", onKeyDown);
    }, [onClose, open]);

    if (!open) {
        return null;
    }

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/26 px-4 py-8 backdrop-blur-[4px]">
            <div className="absolute inset-0" onClick={onClose} />

            <div className="relative w-full max-w-[29rem]">
                <button
                    className="absolute right-3 top-3 z-10 inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/80 bg-white/88 text-slate-500 shadow-[0_14px_28px_rgba(15,23,42,0.08)] transition hover:text-slate-900"
                    onClick={onClose}
                    type="button"
                >
                    <svg className="h-4.5 w-4.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9">
                        <path d="M6 6l12 12" strokeLinecap="round" strokeLinejoin="round" />
                        <path d="M18 6L6 18" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </button>

                <LoginFormPanel compact onSuccess={onClose} title="登录" subtitle="登录后可访问完整后台功能" />
            </div>
        </div>
    );
}
