import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

function normalizeBase(value: string) {
    if (!value) {
        return "/";
    }

    if (value === "/") {
        return value;
    }

    return value.endsWith("/") ? value : `${value}/`;
}

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, process.cwd(), "");
    const publicBase = normalizeBase(env.VITE_PUBLIC_BASE || "/");
    const proxyTarget = env.VITE_PROXY_TARGET || "http://127.0.0.1:8000";

    return {
        base: publicBase,
        plugins: [react()],
        server: {
            host: "0.0.0.0",
            port: 5173,
            proxy: {
                "/api": {
                    target: proxyTarget,
                    changeOrigin: true,
                },
                "/health": {
                    target: proxyTarget,
                    changeOrigin: true,
                },
            },
        },
        preview: {
            host: "0.0.0.0",
            port: 4173,
        },
        build: {
            outDir: "dist",
            emptyOutDir: true,
            sourcemap: false,
        },
    };
});
