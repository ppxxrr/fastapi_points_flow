import { useEffect, useMemo, useState } from "react";

import { type AuthUser } from "../api/auth";
import { getApiErrorMessage, isUnauthorizedError } from "../api/client";
import {
    buildPointsFlowDownloadUrl,
    createPointsFlowTask,
    getHealthStatus,
    getPointsFlowTask,
    shouldPollTask,
    type PointsFlowTask,
} from "../api/pointsFlow";
import HealthCard, { type HealthCardData } from "../components/HealthCard";
import LogsTable, { type TaskLogItem } from "../components/LogsTable";
import ResultCard, { type ResultCardData } from "../components/ResultCard";
import Sidebar, { type SidebarSection } from "../components/Sidebar";
import TaskForm, { type TaskFormValues } from "../components/TaskForm";
import TaskStatusCard, { type TaskStatusData } from "../components/TaskStatusCard";
import Topbar from "../components/Topbar";


interface DashboardPageProps {
    currentUser: AuthUser;
    onLogout: () => Promise<void> | void;
}


const POLL_INTERVAL_MS = 3000;


function recentTaskKey(username: string) {
    return `points_flow_recent_task_id:${username}`;
}


function makeDisplayTime() {
    return new Date().toLocaleString("zh-CN");
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


function toUiTaskStatus(
    task: PointsFlowTask | null,
    fallbackUsername: string,
    fallbackStartDate: string,
    fallbackEndDate: string,
): TaskStatusData {
    if (!task) {
        return {
            taskId: "等待创建",
            type: "积分流水导出",
            status: "idle",
            createdAt: "",
            updatedAt: "",
            params: {
                username: fallbackUsername,
                startDate: fallbackStartDate,
                endDate: fallbackEndDate,
            },
            resultCount: 0,
            error: "",
        };
    }

    return {
        taskId: task.task_id,
        type: task.type,
        status: task.status,
        createdAt: formatApiTime(task.created_at),
        updatedAt: formatApiTime(task.updated_at),
        params: {
            username: task.params.username,
            startDate: task.params.start_date,
            endDate: task.params.end_date,
        },
        resultCount: task.result_count || 0,
        error: task.error || "",
    };
}


export default function DashboardPage({ currentUser, onLogout }: DashboardPageProps) {
    const [initialNow] = useState(() => makeDisplayTime());
    const [activeSection, setActiveSection] = useState<SidebarSection>("create");
    const [taskRecord, setTaskRecord] = useState<PointsFlowTask | null>(null);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [isPolling, setIsPolling] = useState(false);
    const [submitError, setSubmitError] = useState("");
    const [pollError, setPollError] = useState("");
    const [health, setHealth] = useState<HealthCardData>({
        status: "degraded",
        checkedAt: initialNow,
        apiBase: "/api",
        note: "正在检查 FastAPI 服务健康状态。",
        loading: true,
        error: "",
    });

    const operatorName = currentUser.display_name || currentUser.username;

    const task = useMemo(
        () => toUiTaskStatus(taskRecord, currentUser.username, "", ""),
        [currentUser.username, taskRecord],
    );

    const logs = useMemo<TaskLogItem[]>(() => {
        if (!taskRecord) {
            return [
                {
                    time: initialNow,
                    level: "INFO",
                    message: "控制台已加载，等待创建积分流水导出任务。",
                },
            ];
        }

        return [...taskRecord.logs]
            .map((log) => ({
                time: formatApiTime(log.time),
                level: log.level,
                message: log.message,
            }))
            .reverse();
    }, [initialNow, taskRecord]);

    const result = useMemo<ResultCardData>(
        () => ({
            fileName: taskRecord?.result_file || "",
            resultCount: taskRecord?.result_count || 0,
            status: taskRecord?.status || "idle",
            updatedAt: formatApiTime(taskRecord?.updated_at),
            error: taskRecord?.error || "",
            downloadHref: taskRecord?.result_file
                ? buildPointsFlowDownloadUrl(taskRecord.result_file)
                : undefined,
        }),
        [taskRecord],
    );

    useEffect(() => {
        let active = true;

        async function restoreRecentTask() {
            const taskId = window.localStorage.getItem(recentTaskKey(currentUser.username));
            if (!taskId) {
                return;
            }

            try {
                const restoredTask = await getPointsFlowTask(taskId);
                if (!active) {
                    return;
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
                window.localStorage.removeItem(recentTaskKey(currentUser.username));
                setPollError(getApiErrorMessage(error));
            }
        }

        void restoreRecentTask();
        return () => {
            active = false;
        };
    }, [currentUser.username, onLogout]);

    useEffect(() => {
        let active = true;

        async function loadHealth() {
            try {
                const response = await getHealthStatus();
                if (!active) {
                    return;
                }
                setHealth({
                    status: response.status === "ok" ? "online" : "degraded",
                    checkedAt: makeDisplayTime(),
                    apiBase: "/api",
                    note:
                        response.status === "ok"
                            ? "FastAPI 服务可用，前端已接入真实健康检查接口。"
                            : `健康检查返回异常状态：${response.status}`,
                    loading: false,
                    error: "",
                });
            } catch (error) {
                if (!active) {
                    return;
                }
                setHealth({
                    status: "offline",
                    checkedAt: makeDisplayTime(),
                    apiBase: "/api",
                    note: "无法连接健康检查接口，请确认 FastAPI 服务是否正常启动。",
                    loading: false,
                    error: getApiErrorMessage(error),
                });
            }
        }

        void loadHealth();
        const timer = window.setInterval(() => {
            void loadHealth();
        }, 30000);

        return () => {
            active = false;
            window.clearInterval(timer);
        };
    }, []);

    useEffect(() => {
        if (!shouldPollTask(taskRecord)) {
            setIsPolling(false);
            return;
        }

        let active = true;
        setIsPolling(true);

        const timer = window.setInterval(async () => {
            if (!taskRecord) {
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
    }, [currentUser.username, onLogout, taskRecord]);

    async function handleCreateTask(values: TaskFormValues) {
        if (values.startDate > values.endDate) {
            setSubmitError("开始日期不能晚于结束日期。");
            return;
        }

        setIsSubmitting(true);
        setSubmitError("");
        setPollError("");

        try {
            const createdTask = await createPointsFlowTask({
                start_date: values.startDate,
                end_date: values.endDate,
            });
            setTaskRecord(createdTask);
            window.localStorage.setItem(recentTaskKey(currentUser.username), createdTask.task_id);
            setActiveSection("status");
        } catch (error) {
            if (isUnauthorizedError(error)) {
                await onLogout();
                return;
            }
            setSubmitError(getApiErrorMessage(error));
            setActiveSection("create");
        } finally {
            setIsSubmitting(false);
        }
    }

    function sectionWrap(target: SidebarSection) {
        return activeSection === target
            ? "border border-blue-100/90 ring-2 ring-blue-200/80 shadow-[0_26px_60px_rgba(79,113,255,0.14)]"
            : "border border-white/80 shadow-[0_18px_42px_rgba(15,23,42,0.06)]";
    }

    return (
        <div className="relative min-h-screen overflow-hidden bg-[radial-gradient(circle_at_top,_rgba(59,130,246,0.14),_transparent_26%),radial-gradient(circle_at_88%_12%,_rgba(124,58,237,0.14),_transparent_24%),linear-gradient(180deg,_#eef3fb_0%,_#f8fbff_100%)] p-4 lg:p-6">
            <div className="pointer-events-none absolute left-[-120px] top-[-80px] h-[26rem] w-[26rem] rounded-full bg-sky-300/16 blur-3xl" />
            <div className="pointer-events-none absolute right-[-140px] top-[10%] h-[24rem] w-[24rem] rounded-full bg-violet-300/16 blur-3xl" />
            <div className="pointer-events-none absolute bottom-[-180px] left-[18%] h-[24rem] w-[24rem] rounded-full bg-blue-200/24 blur-3xl" />

            <div className="relative mx-auto flex min-h-[calc(100vh-2rem)] max-w-[1640px] overflow-hidden rounded-[2.2rem] border border-white/75 bg-[linear-gradient(180deg,rgba(255,255,255,0.56),rgba(246,249,255,0.72))] shadow-[0_40px_120px_rgba(49,74,137,0.14)] backdrop-blur-[24px]">
                <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,_rgba(129,140,248,0.08),_transparent_24%),radial-gradient(circle_at_bottom_left,_rgba(56,189,248,0.06),_transparent_24%)]" />

                <Sidebar
                    active={activeSection}
                    operatorName={operatorName}
                    onLogout={() => void onLogout()}
                    onNavigate={setActiveSection}
                />

                <main className="relative flex-1 overflow-hidden bg-[linear-gradient(180deg,rgba(255,255,255,0.28),rgba(241,245,255,0.66))] px-5 py-5 lg:px-7 lg:py-6">
                    <Topbar
                        operatorName={operatorName}
                        currentTaskId={task.taskId !== "等待创建" ? task.taskId : undefined}
                        onLogout={() => void onLogout()}
                    />

                    {(submitError || pollError) && (
                        <div className="mt-5 rounded-[1.45rem] border border-rose-200/90 bg-[linear-gradient(135deg,rgba(255,245,247,0.98),rgba(255,241,244,0.9))] px-5 py-4 text-sm text-rose-700 shadow-[0_16px_35px_rgba(244,63,94,0.08)]">
                            <div className="font-medium text-rose-900">接口调用异常</div>
                            <div className="mt-1 leading-6">{submitError || pollError}</div>
                        </div>
                    )}

                    {isPolling && (
                        <div className="mt-5 flex items-center gap-3 rounded-[1.35rem] border border-blue-100/90 bg-[linear-gradient(135deg,rgba(239,246,255,0.96),rgba(245,243,255,0.88))] px-5 py-4 text-sm text-blue-700 shadow-[0_16px_35px_rgba(88,123,255,0.08)]">
                            <div className="h-2.5 w-2.5 animate-pulse rounded-full bg-blue-500" />
                            正在每 3 秒轮询任务状态，任务完成后会自动停止。
                        </div>
                    )}

                    <div className="mt-6 grid gap-5 xl:grid-cols-[1.3fr_0.95fr_0.9fr]">
                        <div className={["rounded-[1.85rem] bg-[linear-gradient(180deg,rgba(255,255,255,0.9),rgba(247,249,255,0.8))] p-6 backdrop-blur-xl", sectionWrap("create")].join(" ")}>
                            <TaskForm
                                username={currentUser.username}
                                displayName={currentUser.display_name}
                                defaultValues={{
                                    startDate: task.params.startDate,
                                    endDate: task.params.endDate,
                                }}
                                errorMessage={submitError}
                                onSubmit={handleCreateTask}
                                submitting={isSubmitting}
                            />
                        </div>

                        <div className={["rounded-[1.85rem] bg-[linear-gradient(180deg,rgba(255,255,255,0.92),rgba(247,249,255,0.82))] p-6 backdrop-blur-xl", sectionWrap("status")].join(" ")}>
                            <TaskStatusCard task={task} />
                        </div>

                        <div className={["rounded-[1.85rem] bg-[linear-gradient(180deg,rgba(255,255,255,0.92),rgba(247,249,255,0.82))] p-6 backdrop-blur-xl", sectionWrap("health")].join(" ")}>
                            <HealthCard data={health} />
                        </div>
                    </div>

                    <div className="mt-5 grid gap-5 xl:grid-cols-[1.55fr_0.85fr]">
                        <div className={["rounded-[1.85rem] bg-[linear-gradient(180deg,rgba(255,255,255,0.92),rgba(247,249,255,0.84))] p-6 backdrop-blur-xl", sectionWrap("logs")].join(" ")}>
                            <LogsTable logs={logs} />
                        </div>

                        <div className={["rounded-[1.85rem] bg-[linear-gradient(180deg,rgba(255,255,255,0.92),rgba(247,249,255,0.84))] p-6 backdrop-blur-xl", sectionWrap("results")].join(" ")}>
                            <ResultCard data={result} />
                        </div>
                    </div>
                </main>
            </div>
        </div>
    );
}
