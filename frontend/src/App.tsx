import { useAuth } from "./auth/AuthContext";
import DashboardPage from "./pages/DashboardPage";
import LoginPage from "./pages/LoginPage";

export default function App() {
    const { isBootstrapping, user, logout } = useAuth();

    if (isBootstrapping) {
        return (
            <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[radial-gradient(circle_at_top_left,_rgba(103,168,255,0.18),_transparent_32%),radial-gradient(circle_at_82%_20%,_rgba(129,140,248,0.16),_transparent_28%),linear-gradient(180deg,_#fafcff_0%,_#eef4ff_100%)]">
                <div className="rounded-[2rem] border border-white/80 bg-white/80 px-8 py-7 text-center shadow-[0_30px_90px_rgba(71,93,160,0.16)] backdrop-blur-2xl">
                    <div className="mx-auto h-10 w-10 animate-spin rounded-full border-2 border-blue-200 border-t-blue-600" />
                    <div className="mt-4 text-sm font-medium text-slate-700">正在恢复登录态...</div>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-[#f6f8fd] text-slate-950">
            {user ? <DashboardPage currentUser={user} onLogout={logout} /> : <LoginPage />}
        </div>
    );
}
