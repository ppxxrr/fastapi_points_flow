import { useEffect, useRef, useState } from "react";

import { type AuthUser } from "../api/auth";
import { getApiErrorMessage, isUnauthorizedError } from "../api/client";
import {
    buildPointsFlowDownloadUrl,
    createPointsFlowTask,
    getPointsFlowTask,
    shouldPollTask,
    type PointsFlowTask,
} from "../api/pointsFlow";
import LoginDialog from "../components/LoginDialog";
import Sidebar, {
    AdminIcon,
    BiIcon,
    ExportIcon,
    LayoutIcon,
    MessageIcon,
    ReconciliationIcon,
    ToolsIcon,
} from "../components/Sidebar";
import TaskForm, { type TaskFormValues } from "../components/TaskForm";
import Topbar from "../components/Topbar";
import BiPage from "./BiPage";
import DeviceLayoutPage from "./DeviceLayoutPage";
import ManagementPage from "./ManagementPage";
import MessageBoardPage from "./MessageBoardPage";
import ReconciliationPage from "./ReconciliationPage";
import ToolsPage from "./ToolsPage";

interface DashboardPageProps {
    currentUser: AuthUser | null;
    onLogout: () => Promise<void> | void;
}

type DashboardView = "tools" | "export" | "management" | "bi" | "reconciliation" | "device-layout" | "message-board";
type SimpleTaskStatus = "idle" | "pending" | "running" | "success" | "failed";

const DEFAULT_GUEST_VIEW: DashboardView = "tools";
const DEFAULT_AUTH_VIEW: DashboardView = "export";
const POLL_INTERVAL_MS = 3000;

function recentTaskKey(username: string) {
    return `points_flow_recent_task_id:${username}`;
}

function dashboardViewKey(username: string) {
    return `dashboard_active_view:${username}`;
}

function parseDashboardView(hash: string): DashboardView | null {
    const normalized = hash.replace(/^#/, "");
    if (
        normalized === "tools" ||
        normalized === "export" ||
        normalized === "management" ||
        normalized === "bi" ||
        normalized === "reconciliation" ||
        normalized === "device-layout" ||
        normalized === "message-board"
    ) {
        return normalized;
    }
    return null;
}

function isGuestAccessibleView(view: DashboardView) {
    return view === "tools" || view === "message-board";
}

function formatApiTime(value?: string | null) {
    if (!value) {
        return "";
    }

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }

    return parsed.toLocaleString("zh-CN");
}

function getTaskStatus(task: PointsFlowTask | null): SimpleTaskStatus {
    if (!task) {
        return "idle";
    }
    return task.status;
}

function getTaskProgress(task: PointsFlowTask | null): { label: string; percent: number; bar: string } {
    const status = getTaskStatus(task);
    if (status === "pending") {
        return { label: "等待执行", percent: 20, bar: "from-amber-400 to-orange-400" };
    }
    if (status === "running") {
        return { label: "导出中", percent: 68, bar: "from-sky-500 to-indigo-500" };
    }
    if (status === "success") {
        return { label: "已完成", percent: 100, bar: "from-emerald-400 to-teal-500" };
    }
    if (status === "failed") {
        return { label: "失败", percent: 100, bar: "from-rose-400 to-pink-500" };
    }
    return { label: "未开始", percent: 0, bar: "from-slate-300 to-slate-400" };
}

function getStatusPill(status: SimpleTaskStatus) {
    if (status === "success") {
        return { label: "已完成", className: "bg-emerald-100 text-emerald-700" };
    }
    if (status === "failed") {
        return { label: "失败", className: "bg-rose-100 text-rose-700" };
    }
    if (status === "running") {
        return { label: "导出中", className: "bg-blue-100 text-blue-700" };
    }
    if (status === "pending") {
        return { label: "等待执行", className: "bg-amber-100 text-amber-700" };
    }
    return { label: "未开始", className: "bg-slate-100 text-slate-600" };
}

function triggerDownload(url: string) {
    const link = document.createElement("a");
    link.href = url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    document.body.appendChild(link);
    link.click();
    link.remove();
}

export default function DashboardPage({ currentUser, onLogout }: DashboardPageProps) {
    const downloadedPointsFlowUrlsRef = useRef(new Set<string>());
    const [sidebarCollapsed, setSidebarCollapsed] = useState(true);
    const [taskRecord, setTaskRecord] = useState<PointsFlowTask | null>(null);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [isPolling, setIsPolling] = useState(false);
    const [submitError, setSubmitError] = useState("");
    const [pollError, setPollError] = useState("");
    const [isLoginDialogOpen, setIsLoginDialogOpen] = useState(false);
    const [activeView, setActiveView] = useState<DashboardView>(() => {
        if (typeof window === "undefined") {
            return DEFAULT_GUEST_VIEW;
        }
        return parseDashboardView(window.location.hash) || DEFAULT_GUEST_VIEW;
    });

    const isAuthenticated = Boolean(currentUser);
    const operatorName = currentUser?.display_name || currentUser?.username || "";
    const taskStatus = getTaskStatus(taskRecord);
    const progressMeta = getTaskProgress(taskRecord);
    const statusPill = getStatusPill(taskStatus);
    const resultCount = taskRecord?.result_count || 0;
    const updatedAt = formatApiTime(taskRecord?.updated_at);
    const downloadHref = taskRecord?.result_file ? buildPointsFlowDownloadUrl(taskRecord.result_file) : "";

    useEffect(() => {
        if (currentUser) {
            setIsLoginDialogOpen(false);
        }
    }, [currentUser]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        function handleHashChange() {
            const parsed = parseDashboardView(window.location.hash);
            if (!parsed) {
                return;
            }
            if (!currentUser && !isGuestAccessibleView(parsed)) {
                setActiveView(DEFAULT_GUEST_VIEW);
                if (window.location.hash !== `#${DEFAULT_GUEST_VIEW}`) {
                    window.location.hash = DEFAULT_GUEST_VIEW;
                }
                return;
            }
            setActiveView(parsed);
        }

        window.addEventListener("hashchange", handleHashChange);
        return () => window.removeEventListener("hashchange", handleHashChange);
    }, [currentUser]);

    useEffect(() => {
        let active = true;

        if (!currentUser) {
            downloadedPointsFlowUrlsRef.current.clear();
            setTaskRecord(null);
            setSubmitError("");
            setPollError("");
            setIsPolling(false);
            const nextGuestView =
                typeof window !== "undefined"
                    ? (() => {
                          const parsed = parseDashboardView(window.location.hash);
                          return parsed && isGuestAccessibleView(parsed) ? parsed : DEFAULT_GUEST_VIEW;
                      })()
                    : DEFAULT_GUEST_VIEW;
            setActiveView(nextGuestView);
            if (typeof window !== "undefined" && window.location.hash !== `#${nextGuestView}`) {
                window.location.hash = nextGuestView;
            }
            return () => {
                active = false;
            };
        }

        const currentUsername = currentUser.username;

        async function restoreRecentTask() {
            const taskId = window.localStorage.getItem(recentTaskKey(currentUsername));
            if (!taskId) {
                return;
            }

            try {
                const restoredTask = await getPointsFlowTask(taskId);
                if (!active) {
                    return;
                }
                if (restoredTask.status === "success" && restoredTask.result_file) {
                    downloadedPointsFlowUrlsRef.current.add(buildPointsFlowDownloadUrl(restoredTask.result_file));
                }
                setTaskRecord(restoredTask);
                setPollError("");
            } catch (error) {
                if (!active) {
                    return;
                }
                if (isUnauthorizedError(error)) {
                    await onLogout();
                    return;
                }
                window.localStorage.removeItem(recentTaskKey(currentUsername));
                setPollError(getApiErrorMessage(error));
            }
        }

        const hashView = parseDashboardView(window.location.hash);
        if (hashView) {
            setActiveView(hashView);
        } else {
            const storedView = window.localStorage.getItem(dashboardViewKey(currentUsername));
            if (
                storedView === "tools" ||
                storedView === "management" ||
                storedView === "export" ||
                storedView === "bi" ||
                storedView === "reconciliation" ||
                storedView === "device-layout" ||
                storedView === "message-board"
            ) {
                setActiveView(storedView);
            } else {
                setActiveView(DEFAULT_AUTH_VIEW);
            }
        }

        void restoreRecentTask();
        return () => {
            active = false;
        };
    }, [currentUser, onLogout]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        if (!currentUser) {
            if (!isGuestAccessibleView(activeView)) {
                setActiveView(DEFAULT_GUEST_VIEW);
                return;
            }
            if (window.location.hash !== `#${activeView}`) {
                window.location.hash = activeView;
            }
            return;
        }

        window.localStorage.setItem(dashboardViewKey(currentUser.username), activeView);
        if (window.location.hash !== `#${activeView}`) {
            window.location.hash = activeView;
        }
    }, [activeView, currentUser]);

    useEffect(() => {
        if (!currentUser || !shouldPollTask(taskRecord)) {
            setIsPolling(false);
            return;
        }

        let active = true;
        setIsPolling(true);

        const timer = window.setInterval(async () => {
            if (!taskRecord || !currentUser) {
                return;
            }

            try {
                const nextTask = await getPointsFlowTask(taskRecord.task_id);
                if (!active) {
                    return;
                }
                setTaskRecord(nextTask);
                window.localStorage.setItem(recentTaskKey(currentUser.username), nextTask.task_id);
                setPollError("");
            } catch (error) {
                if (!active) {
                    return;
                }
                if (isUnauthorizedError(error)) {
                    window.clearInterval(timer);
                    setIsPolling(false);
                    await onLogout();
                    return;
                }
                setPollError(getApiErrorMessage(error));
            }
        }, POLL_INTERVAL_MS);

        return () => {
            active = false;
            setIsPolling(false);
            window.clearInterval(timer);
        };
    }, [currentUser, onLogout, taskRecord]);

    useEffect(() => {
        if (taskStatus !== "success" || !downloadHref) {
            return;
        }
        if (downloadedPointsFlowUrlsRef.current.has(downloadHref)) {
            return;
        }
        downloadedPointsFlowUrlsRef.current.add(downloadHref);
        triggerDownload(downloadHref);
    }, [downloadHref, taskStatus]);

    async function handleCreateTask(values: TaskFormValues) {
        if (!currentUser) {
            setIsLoginDialogOpen(true);
            return;
        }

        if (values.startDate > values.endDate) {
            setSubmitError("开始日期不能晚于结束日期。");
            return;
        }

        setIsSubmitting(true);
        setSubmitError("");
        setPollError("");

        try {
            downloadedPointsFlowUrlsRef.current.clear();
            const createdTask = await createPointsFlowTask({
                start_date: values.startDate,
                end_date: values.endDate,
            });
            setTaskRecord(createdTask);
            window.localStorage.setItem(recentTaskKey(currentUser.username), createdTask.task_id);
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setSubmitError(getApiErrorMessage(error));
        } finally {
            setIsSubmitting(false);
        }
    }

    const navigationItems = isAuthenticated
        ? [
              {
                  key: "tools",
                  label: "小工具",
                  icon: <ToolsIcon />,
                  active: activeView === "tools",
                  onClick: () => setActiveView("tools"),
              },
              {
                  key: "export",
                  label: "会员工具",
                  icon: <ExportIcon />,
                  active: activeView === "export",
                  onClick: () => setActiveView("export"),
              },
              {
                  key: "message-board",
                  label: "留言板",
                  icon: <MessageIcon />,
                  active: activeView === "message-board",
                  onClick: () => setActiveView("message-board"),
              },
              {
                  key: "reconciliation",
                  label: "对账核销",
                  icon: <ReconciliationIcon />,
                  active: activeView === "reconciliation",
                  onClick: () => setActiveView("reconciliation"),
              },
              {
                  key: "device-layout",
                  label: "设备布局",
                  icon: <LayoutIcon />,
                  active: activeView === "device-layout",
                  onClick: () => setActiveView("device-layout"),
              },
              {
                  key: "bi",
                  label: "BI",
                  icon: <BiIcon />,
                  active: activeView === "bi",
                  onClick: () => setActiveView("bi"),
              },
          ]
        : [
              {
                  key: "tools",
                  label: "小工具",
                  icon: <ToolsIcon />,
                  active: activeView === "tools",
                  onClick: () => setActiveView("tools"),
              },
              {
                  key: "message-board",
                  label: "留言板",
                  icon: <MessageIcon />,
                  active: activeView === "message-board",
                  onClick: () => setActiveView("message-board"),
              },
          ];

    const bottomNavigationItems = isAuthenticated
        ? [
              {
                  key: "management",
                  label: "管理",
                  icon: <AdminIcon />,
                  active: activeView === "management",
                  onClick: () => setActiveView("management"),
              },
          ]
        : [];

    const pageTitle =
        activeView === "management"
            ? "管理"
            : activeView === "reconciliation"
              ? "对账核销工具"
              : activeView === "device-layout"
                ? "设备布局"
              : activeView === "bi"
                ? "BI"
                : activeView === "message-board"
                  ? "留言板"
                  : activeView === "export"
                    ? "会员工具"
                    : "小工具";

    return (
        <div className="relative min-h-screen overflow-hidden bg-[radial-gradient(circle_at_top,_rgba(59,130,246,0.13),_transparent_26%),radial-gradient(circle_at_86%_10%,_rgba(124,58,237,0.11),_transparent_24%),linear-gradient(180deg,_#eef3fb_0%,_#f8fbff_100%)] p-4 lg:p-6">
            <div className="pointer-events-none absolute left-[-120px] top-[-80px] h-[26rem] w-[26rem] rounded-full bg-sky-300/16 blur-3xl" />
            <div className="pointer-events-none absolute right-[-140px] top-[8%] h-[24rem] w-[24rem] rounded-full bg-violet-300/16 blur-3xl" />
            <div className="pointer-events-none absolute bottom-[-160px] left-[18%] h-[22rem] w-[22rem] rounded-full bg-blue-200/24 blur-3xl" />

            <div className="relative mx-auto flex min-h-[calc(100vh-2rem)] max-w-[1580px] overflow-hidden rounded-[2.2rem] border border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.58),rgba(246,249,255,0.74))] shadow-[0_40px_120px_rgba(49,74,137,0.14)] backdrop-blur-[24px]">
                <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.08),_transparent_24%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.06),_transparent_24%)]" />

                <Sidebar
                    collapsed={sidebarCollapsed}
                    items={navigationItems}
                    bottomItems={bottomNavigationItems}
                    onToggleCollapse={() => setSidebarCollapsed((current) => !current)}
                />

                <main className="relative flex-1 overflow-hidden bg-[linear-gradient(180deg,rgba(255,255,255,0.3),rgba(241,245,255,0.66))] px-5 py-5 lg:px-7 lg:py-6">
                    <Topbar
                        isAuthenticated={isAuthenticated}
                        operatorName={operatorName}
                        onLogin={() => setIsLoginDialogOpen(true)}
                        onLogout={() => void onLogout()}
                        title={pageTitle}
                    />

                    {activeView === "management" ? (
                        <div className="mt-6">
                            <ManagementPage onLogout={onLogout} />
                        </div>
                    ) : activeView === "reconciliation" ? (
                        <div className="mt-6">
                            <ReconciliationPage onLogout={onLogout} />
                        </div>
                    ) : activeView === "bi" ? (
                        <div className="mt-6">
                            <BiPage onLogout={onLogout} />
                        </div>
                    ) : activeView === "device-layout" ? (
                        <div className="mt-6">
                            <DeviceLayoutPage onLogout={onLogout} />
                        </div>
                    ) : activeView === "message-board" ? (
                        <div className="mt-6">
                            <MessageBoardPage />
                        </div>
                    ) : activeView === "export" ? (
                        <section className="mt-6 rounded-[1.9rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-6 shadow-[0_20px_50px_rgba(15,23,42,0.06)] backdrop-blur-xl">
                            <div className="mb-6">
                                <h3 className="text-[1.45rem] font-semibold tracking-[-0.04em] text-slate-950">
                                    会员积分流水导出
                                </h3>
                            </div>

                            <TaskForm
                                defaultValues={{
                                    startDate: taskRecord?.params.start_date || "",
                                    endDate: taskRecord?.params.end_date || "",
                                }}
                                errorMessage={submitError || pollError}
                                onSubmit={handleCreateTask}
                                progress={progressMeta}
                                resultCount={resultCount}
                                statusClassName={statusPill.className}
                                statusLabel={statusPill.label}
                                submitting={isSubmitting}
                                updatedAt={updatedAt}
                            />

                            <div className="mt-5 flex flex-wrap items-center gap-3">
                                {taskRecord?.error ? (
                                    <div className="rounded-[1.1rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,244,247,0.98),rgba(255,240,244,0.92))] px-4 py-3 text-sm leading-6 text-rose-700">
                                        {taskRecord.error}
                                    </div>
                                ) : null}
                            </div>
                        </section>
                    ) : (
                        <div className="mt-6">
                            <ToolsPage />
                        </div>
                    )}
                </main>
            </div>

            <LoginDialog onClose={() => setIsLoginDialogOpen(false)} open={isLoginDialogOpen} />
        </div>
    );
}
