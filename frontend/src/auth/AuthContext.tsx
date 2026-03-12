import {
    createContext,
    useContext,
    useEffect,
    useMemo,
    useState,
    type ReactNode,
} from "react";

import { getApiErrorMessage, isUnauthorizedError } from "../api/client";
import { getCurrentUser, loginWithICSP, logoutFromICSP, type AuthUser } from "../api/auth";


interface AuthContextValue {
    user: AuthUser | null;
    isAuthenticated: boolean;
    isBootstrapping: boolean;
    isLoggingIn: boolean;
    loginError: string;
    login: (username: string, password: string) => Promise<boolean>;
    logout: () => Promise<void>;
    refreshUser: () => Promise<void>;
    clearLoginError: () => void;
}


const AuthContext = createContext<AuthContextValue | null>(null);


export function AuthProvider({ children }: { children: ReactNode }) {
    const [user, setUser] = useState<AuthUser | null>(null);
    const [isBootstrapping, setIsBootstrapping] = useState(true);
    const [isLoggingIn, setIsLoggingIn] = useState(false);
    const [loginError, setLoginError] = useState("");

    useEffect(() => {
        let active = true;

        async function bootstrap() {
            try {
                const nextUser = await getCurrentUser();
                if (!active) {
                    return;
                }
                setUser(nextUser);
                setLoginError("");
            } catch (error) {
                if (!active) {
                    return;
                }
                setUser(null);
                if (!isUnauthorizedError(error)) {
                    setLoginError(getApiErrorMessage(error));
                }
            } finally {
                if (active) {
                    setIsBootstrapping(false);
                }
            }
        }

        void bootstrap();
        return () => {
            active = false;
        };
    }, []);

    async function login(username: string, password: string) {
        setIsLoggingIn(true);
        setLoginError("");
        try {
            const nextUser = await loginWithICSP({ username, password });
            setUser(nextUser);
            return true;
        } catch (error) {
            setUser(null);
            setLoginError(getApiErrorMessage(error));
            return false;
        } finally {
            setIsLoggingIn(false);
        }
    }

    async function logout() {
        try {
            await logoutFromICSP();
        } catch {
            // Ignore logout transport errors and clear local state anyway.
        } finally {
            setUser(null);
            setLoginError("");
        }
    }

    async function refreshUser() {
        try {
            const nextUser = await getCurrentUser();
            setUser(nextUser);
            setLoginError("");
        } catch (error) {
            if (isUnauthorizedError(error)) {
                setUser(null);
                setLoginError("");
                return;
            }
            setLoginError(getApiErrorMessage(error));
            throw error;
        }
    }

    const value = useMemo<AuthContextValue>(
        () => ({
            user,
            isAuthenticated: Boolean(user),
            isBootstrapping,
            isLoggingIn,
            loginError,
            login,
            logout,
            refreshUser,
            clearLoginError: () => setLoginError(""),
        }),
        [isBootstrapping, isLoggingIn, loginError, user],
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}


export function useAuth() {
    const context = useContext(AuthContext);
    if (!context) {
        throw new Error("useAuth must be used within AuthProvider");
    }
    return context;
}
