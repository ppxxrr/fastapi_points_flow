import LoginBackground from "../components/LoginBackground";
import LoginFormPanel from "../components/LoginFormPanel";

export default function LoginPage() {
    return (
        <div className="relative flex min-h-screen items-center justify-center overflow-hidden px-6 py-10">
            <LoginBackground />

            <div className="relative w-full max-w-[31rem]">
                <div className="pointer-events-none absolute inset-x-6 inset-y-6 rounded-[2.5rem] bg-[radial-gradient(circle,rgba(96,165,250,0.2),rgba(129,140,248,0.1),transparent_72%)] blur-[58px]" />
                <LoginFormPanel />
            </div>
        </div>
    );
}
