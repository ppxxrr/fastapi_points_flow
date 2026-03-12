import { apiRequest } from "./client";


export interface AuthUser {
    username: string;
    display_name: string;
    user_id: string;
    user_code: string;
    created_at: string;
    updated_at: string;
}


export interface LoginPayload {
    username: string;
    password: string;
}


export async function loginWithICSP(payload: LoginPayload) {
    return apiRequest<AuthUser>("/api/auth/login", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
}


export async function logoutFromICSP() {
    return apiRequest<{ success: boolean }>("/api/auth/logout", {
        method: "POST",
    });
}


export async function getCurrentUser() {
    return apiRequest<AuthUser>("/api/auth/me");
}
