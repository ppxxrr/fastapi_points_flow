import { useEffect, useMemo, useRef, useState } from "react";

import { getApiErrorMessage, isUnauthorizedError } from "../api/client";
import {
    buildDeviceLayoutExportUrl,
    buildDeviceLayoutMapUrl,
    getDeviceLayoutConfig,
    getDeviceLayoutPoints,
    importDeviceLayoutPoints,
    saveDeviceLayoutPoints,
    type DeviceLayoutConfigResponse,
    type DeviceLayoutFloor,
    type DeviceLayoutPoint,
    type DeviceLayoutPointType,
} from "../api/deviceLayout";

interface DeviceLayoutPageProps {
    onLogout: () => Promise<void> | void;
}

const FLOOR_EXIT_DURATION_MS = 220;

function clamp(value: number) {
    if (value < 0) {
        return 0;
    }
    if (value > 1) {
        return 1;
    }
    return Math.round(value * 1_000_000) / 1_000_000;
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

function StatusBanner({ tone, message }: { tone: "error" | "success" | "info"; message: string }) {
    const toneClass =
        tone === "error"
            ? "border-rose-200 bg-rose-50 text-rose-700"
            : tone === "success"
              ? "border-emerald-200 bg-emerald-50 text-emerald-700"
              : "border-sky-200 bg-sky-50 text-sky-700";
    return <div className={`rounded-[1.15rem] border px-4 py-3 text-sm ${toneClass}`}>{message}</div>;
}

function EmptyPanel({ title, description }: { title: string; description: string }) {
    return (
        <div className="flex h-full min-h-[240px] items-center justify-center rounded-[1.5rem] border border-dashed border-slate-200 bg-white/80 px-6 py-8 text-center">
            <div>
                <div className="text-base font-medium text-slate-900">{title}</div>
                <div className="mt-2 text-sm leading-6 text-slate-500">{description}</div>
            </div>
        </div>
    );
}

function PointChip({
    point,
    draggable,
    onDragEnd,
    onDragStart,
}: {
    point: DeviceLayoutPoint;
    draggable: boolean;
    onDragEnd: () => void;
    onDragStart: (pointCode: string) => void;
}) {
    const placed = point.x_ratio !== null && point.y_ratio !== null;

    return (
        <button
            className={[
                "flex cursor-grab items-center gap-2 rounded-[1rem] border px-3 py-2 text-left text-sm shadow-[0_10px_18px_rgba(15,23,42,0.04)] transition active:cursor-grabbing",
                placed
                    ? "border-cyan-100/80 bg-[linear-gradient(135deg,rgba(239,246,255,0.98),rgba(236,254,255,0.96))] text-slate-800 shadow-[0_14px_24px_rgba(14,116,144,0.08)]"
                    : "border-slate-200 bg-white/92 text-slate-700",
            ].join(" ")}
            draggable={draggable}
            onDragEnd={onDragEnd}
            onDragStart={() => onDragStart(point.point_code)}
            title={point.point_name}
            type="button"
        >
            <span
                className={[
                    "h-2.5 w-2.5 rounded-full",
                    placed ? "bg-cyan-500 shadow-[0_0_0_4px_rgba(6,182,212,0.14)]" : "bg-slate-300",
                ].join(" ")}
            />
            <span className="max-w-[16rem] truncate">{point.point_name}</span>
        </button>
    );
}

export default function DeviceLayoutPage({ onLogout }: DeviceLayoutPageProps) {
    const importInputRef = useRef<HTMLInputElement | null>(null);
    const mapRef = useRef<HTMLDivElement | null>(null);
    const floorSwitchTimeoutRef = useRef<number | null>(null);
    const floorEnterTimeoutRef = useRef<number | null>(null);

    const [config, setConfig] = useState<DeviceLayoutConfigResponse | null>(null);
    const [selectedType, setSelectedType] = useState("");
    const [selectedFloorCode, setSelectedFloorCode] = useState("");
    const [renderedFloorCode, setRenderedFloorCode] = useState("");
    const [points, setPoints] = useState<DeviceLayoutPoint[]>([]);
    const [loadingConfig, setLoadingConfig] = useState(true);
    const [loadingPoints, setLoadingPoints] = useState(false);
    const [saving, setSaving] = useState(false);
    const [importing, setImporting] = useState(false);
    const [draggingPointCode, setDraggingPointCode] = useState("");
    const [isDragOverMap, setIsDragOverMap] = useState(false);
    const [dirty, setDirty] = useState(false);
    const [error, setError] = useState("");
    const [message, setMessage] = useState("");
    const [prefersReducedMotion, setPrefersReducedMotion] = useState(false);
    const [floorTransitionStage, setFloorTransitionStage] = useState<"idle" | "exit" | "enter">("idle");

    function clearFloorTransitionHandles() {
        if (floorSwitchTimeoutRef.current !== null) {
            window.clearTimeout(floorSwitchTimeoutRef.current);
            floorSwitchTimeoutRef.current = null;
        }
        if (floorEnterTimeoutRef.current !== null) {
            window.clearTimeout(floorEnterTimeoutRef.current);
            floorEnterTimeoutRef.current = null;
        }
    }

    useEffect(() => {
        if (typeof window === "undefined" || !window.matchMedia) {
            return;
        }
        const mediaQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
        const applyPreference = () => setPrefersReducedMotion(mediaQuery.matches);
        applyPreference();

        if (typeof mediaQuery.addEventListener === "function") {
            mediaQuery.addEventListener("change", applyPreference);
            return () => mediaQuery.removeEventListener("change", applyPreference);
        }

        mediaQuery.addListener(applyPreference);
        return () => mediaQuery.removeListener(applyPreference);
    }, []);

    useEffect(() => {
        let active = true;

        async function loadConfig() {
            setLoadingConfig(true);
            try {
                const response = await getDeviceLayoutConfig();
                if (!active) {
                    return;
                }
                setConfig(response);
                setSelectedType(response.default_point_type);
                setSelectedFloorCode(response.default_floor_code);
                setRenderedFloorCode(response.default_floor_code);
                setError("");
            } catch (requestError) {
                if (!active) {
                    return;
                }
                if (isUnauthorizedError(requestError)) {
                    await onLogout();
                    return;
                }
                setError(getApiErrorMessage(requestError));
            } finally {
                if (active) {
                    setLoadingConfig(false);
                }
            }
        }

        void loadConfig();
        return () => {
            active = false;
        };
    }, [onLogout]);

    useEffect(() => {
        if (!selectedType) {
            return;
        }
        let active = true;

        async function loadPoints() {
            setLoadingPoints(true);
            try {
                const response = await getDeviceLayoutPoints(selectedType);
                if (!active) {
                    return;
                }
                setPoints(response.points);
                setDirty(false);
                setError("");
            } catch (requestError) {
                if (!active) {
                    return;
                }
                if (isUnauthorizedError(requestError)) {
                    await onLogout();
                    return;
                }
                setError(getApiErrorMessage(requestError));
            } finally {
                if (active) {
                    setLoadingPoints(false);
                }
            }
        }

        void loadPoints();
        return () => {
            active = false;
        };
    }, [selectedType, onLogout]);

    useEffect(() => {
        if (!selectedFloorCode) {
            return;
        }
        if (!renderedFloorCode) {
            setRenderedFloorCode(selectedFloorCode);
            setFloorTransitionStage("idle");
            return;
        }
        if (renderedFloorCode === selectedFloorCode) {
            return;
        }
        clearFloorTransitionHandles();

        if (prefersReducedMotion) {
            setRenderedFloorCode(selectedFloorCode);
            setFloorTransitionStage("idle");
            return;
        }

        setFloorTransitionStage("exit");
        floorSwitchTimeoutRef.current = window.setTimeout(() => {
            floorSwitchTimeoutRef.current = null;
            setRenderedFloorCode(selectedFloorCode);
            setFloorTransitionStage("enter");
            floorEnterTimeoutRef.current = window.setTimeout(() => {
                floorEnterTimeoutRef.current = null;
                setFloorTransitionStage("idle");
            }, 260);
        }, FLOOR_EXIT_DURATION_MS);

        return () => {
            clearFloorTransitionHandles();
        };
    }, [prefersReducedMotion, selectedFloorCode]);

    useEffect(() => {
        return () => {
            clearFloorTransitionHandles();
        };
    }, []);

    const pointTypes = config?.point_types || [];
    const floors = config?.floors || [];
    const activeFloorCode = renderedFloorCode || selectedFloorCode;
    const displayedFloor =
        floors.find((item) => item.code === activeFloorCode) || floors[0] || null;
    const isFloorSwitching = floorTransitionStage !== "idle";
    const floorMotionClass = prefersReducedMotion
        ? ""
        : floorTransitionStage === "exit"
          ? "opacity-0 translate-y-2 scale-[0.996] blur-[2px]"
          : floorTransitionStage === "enter"
            ? "opacity-0 -translate-y-2 scale-[1.004] blur-[3px]"
            : "opacity-100 translate-y-0 scale-100 blur-0";

    const floorPoints = useMemo(
        () =>
            points
                .filter((item) => item.floor_code === activeFloorCode)
                .sort((left, right) => left.point_name.localeCompare(right.point_name, "zh-CN")),
        [activeFloorCode, points],
    );

    const placedFloorPoints = useMemo(
        () => floorPoints.filter((item) => item.x_ratio !== null && item.y_ratio !== null),
        [floorPoints],
    );

    const pendingFloorPoints = useMemo(
        () => floorPoints.filter((item) => item.x_ratio === null || item.y_ratio === null),
        [floorPoints],
    );

    const selectedPointTypeMeta =
        pointTypes.find((item) => item.key === selectedType) || null;

    function updatePointPosition(pointCode: string, nextX: number, nextY: number) {
        setPoints((current) =>
            current.map((item) =>
                item.point_code === pointCode
                    ? {
                          ...item,
                          floor_code: activeFloorCode,
                          x_ratio: nextX,
                          y_ratio: nextY,
                      }
                    : item,
            ),
        );
        setDirty(true);
        setMessage("点位坐标已更新，点击保存后生效。");
        setError("");
    }

    function handleDropOnMap(event: React.DragEvent<HTMLDivElement>) {
        event.preventDefault();
        setIsDragOverMap(false);
        if (!draggingPointCode || !mapRef.current) {
            return;
        }
        const rect = mapRef.current.getBoundingClientRect();
        if (!rect.width || !rect.height) {
            return;
        }
        const xRatio = clamp((event.clientX - rect.left) / rect.width);
        const yRatio = clamp((event.clientY - rect.top) / rect.height);
        updatePointPosition(draggingPointCode, xRatio, yRatio);
        setDraggingPointCode("");
    }

    async function handleSave() {
        setSaving(true);
        setMessage("");
        setError("");
        try {
            const response = await saveDeviceLayoutPoints({
                point_type: selectedType,
                points: points.map((item) => ({
                    point_code: item.point_code,
                    point_name: item.point_name,
                    floor_code: item.floor_code,
                    x_ratio: item.x_ratio,
                    y_ratio: item.y_ratio,
                })),
            });
            setDirty(false);
            setMessage(`已保存 ${response.saved_count} 个点位坐标。`);
        } catch (requestError) {
            if (isUnauthorizedError(requestError)) {
                await onLogout();
                return;
            }
            setError(getApiErrorMessage(requestError));
        } finally {
            setSaving(false);
        }
    }

    async function handleImport(file: File) {
        setImporting(true);
        setMessage("");
        setError("");
        try {
            const response = await importDeviceLayoutPoints({
                pointType: selectedType,
                file,
            });
            const refreshed = await getDeviceLayoutPoints(selectedType);
            setPoints(refreshed.points);
            setDirty(false);
            setMessage(
                `导入完成：处理 ${response.total_rows} 行，新增 ${response.created_count}，更新 ${response.updated_count}，跳过 ${response.skipped_count}。`,
            );
        } catch (requestError) {
            if (isUnauthorizedError(requestError)) {
                await onLogout();
                return;
            }
            setError(getApiErrorMessage(requestError));
        } finally {
            setImporting(false);
            if (importInputRef.current) {
                importInputRef.current.value = "";
            }
        }
    }

    if (loadingConfig) {
        return (
            <div className="rounded-[1.8rem] border border-white/80 bg-white/88 px-6 py-8 shadow-[0_20px_50px_rgba(15,23,42,0.06)]">
                <div className="text-sm text-slate-500">正在加载设备布局配置...</div>
            </div>
        );
    }

    return (
        <div className="space-y-5">
            <section className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-5 shadow-[0_18px_42px_rgba(15,23,42,0.06)] backdrop-blur-xl">
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex flex-wrap gap-3">
                        {pointTypes.map((item: DeviceLayoutPointType) => (
                            <button
                                key={item.key}
                                className={[
                                    "inline-flex h-11 items-center justify-center rounded-[1rem] px-4 text-sm font-medium transition",
                                    selectedType === item.key
                                        ? "bg-slate-950 text-white shadow-[0_16px_30px_rgba(15,23,42,0.16)]"
                                        : "border border-white/80 bg-white/90 text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)]",
                                ].join(" ")}
                                onClick={() => {
                                    setSelectedType(item.key);
                                    setMessage("");
                                    setError("");
                                }}
                                type="button"
                            >
                                {item.label}
                            </button>
                        ))}
                    </div>

                    <div className="flex flex-wrap gap-3">
                        <button
                            className="inline-flex h-11 items-center justify-center rounded-[1rem] bg-[linear-gradient(135deg,#2563eb,#1d4ed8)] px-5 text-sm font-medium text-white shadow-[0_16px_30px_rgba(37,99,235,0.22)] transition disabled:cursor-not-allowed disabled:opacity-60"
                            disabled={saving || loadingPoints || !selectedType}
                            onClick={() => void handleSave()}
                            type="button"
                        >
                            {saving ? "保存中..." : "保存"}
                        </button>
                        <button
                            className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-5 text-sm font-medium text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] transition hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-60"
                            disabled={!selectedType}
                            onClick={() => triggerDownload(buildDeviceLayoutExportUrl(selectedType))}
                            type="button"
                        >
                            导出
                        </button>
                        <button
                            className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/92 px-5 text-sm font-medium text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] transition hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-60"
                            disabled={importing || !selectedType}
                            onClick={() => importInputRef.current?.click()}
                            type="button"
                        >
                            {importing ? "导入中..." : "导入"}
                        </button>
                        <input
                            ref={importInputRef}
                            accept=".xlsx,.xlsm"
                            className="hidden"
                            onChange={(event) => {
                                const file = event.target.files?.[0];
                                if (file) {
                                    void handleImport(file);
                                }
                            }}
                            type="file"
                        />
                    </div>
                </div>

                <div className="mt-5 space-y-3">
                    {error ? <StatusBanner message={error} tone="error" /> : null}
                    {message ? (
                        <StatusBanner message={message} tone={dirty ? "info" : "success"} />
                    ) : null}
                    <div className="flex flex-wrap items-center gap-3 text-sm text-slate-500">
                        <span>当前类型：{selectedPointTypeMeta?.label || "-"}</span>
                        <span>当前楼层：{displayedFloor?.label || "-"}</span>
                        <span>本层点位：{floorPoints.length}</span>
                        <span>已定位：{placedFloorPoints.length}</span>
                        <span>待定位：{pendingFloorPoints.length}</span>
                        {isFloorSwitching ? <span className="text-sky-700">楼层切换中...</span> : null}
                        {dirty ? <span className="text-sky-700">坐标已修改，尚未保存</span> : null}
                    </div>
                </div>
            </section>

            <section className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-5 shadow-[0_18px_42px_rgba(15,23,42,0.06)] backdrop-blur-xl">
                {displayedFloor ? (
                    <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr),84px]">
                        <div className="min-w-0">
                            <div
                                ref={mapRef}
                                className={[
                                    "relative overflow-hidden rounded-[1.7rem] border border-white/80 bg-[radial-gradient(circle_at_top,_rgba(59,130,246,0.08),transparent_36%),linear-gradient(180deg,rgba(255,255,255,0.95),rgba(242,247,255,0.9))] shadow-[0_18px_38px_rgba(15,23,42,0.06)]",
                                    isDragOverMap ? "ring-2 ring-sky-400/70 ring-offset-2 ring-offset-transparent" : "",
                                ].join(" ")}
                                onDragLeave={() => setIsDragOverMap(false)}
                                onDragOver={(event) => {
                                    event.preventDefault();
                                    setIsDragOverMap(true);
                                }}
                                onDrop={handleDropOnMap}
                                style={{ aspectRatio: `${displayedFloor.image_width} / ${displayedFloor.image_height}` }}
                            >
                                <div
                                    className={[
                                        "relative h-full w-full transition-all duration-[420ms] ease-[cubic-bezier(0.22,1,0.36,1)] will-change-transform",
                                        floorMotionClass,
                                    ].join(" ")}
                                >
                                    <img
                                        alt={`${displayedFloor.label} 楼层地图`}
                                        className="h-full w-full select-none object-fill"
                                        draggable={false}
                                        src={buildDeviceLayoutMapUrl(displayedFloor.code)}
                                    />

                                    <div className="pointer-events-none absolute inset-x-0 top-0 flex items-center px-4 py-4">
                                        <div className="rounded-full bg-slate-950/78 px-3 py-1 text-xs font-medium uppercase tracking-[0.18em] text-white">
                                            {displayedFloor.label}
                                        </div>
                                    </div>

                                    {placedFloorPoints.map((point) => (
                                        <button
                                            key={point.point_code}
                                            className="group absolute cursor-grab active:cursor-grabbing transition-transform duration-200 hover:-translate-y-0.5"
                                            draggable={!isFloorSwitching}
                                            onDragEnd={() => setDraggingPointCode("")}
                                            onDragStart={() => setDraggingPointCode(point.point_code)}
                                            style={{
                                                left: `${(point.x_ratio || 0) * 100}%`,
                                                top: `${(point.y_ratio || 0) * 100}%`,
                                                transform: "translate(-50%, -100%)",
                                            }}
                                            title={point.point_name}
                                            type="button"
                                        >
                                            <span className="inline-flex items-center gap-2 rounded-full border border-cyan-100/22 bg-[linear-gradient(135deg,rgba(30,64,175,0.96),rgba(14,116,144,0.94))] px-3 py-2 text-xs font-semibold text-white shadow-[0_16px_30px_rgba(8,47,73,0.24)] transition duration-200 group-hover:-translate-y-0.5 group-hover:shadow-[0_20px_36px_rgba(8,47,73,0.3)]">
                                                <span className="h-2.5 w-2.5 rounded-full bg-amber-200 shadow-[0_0_0_3px_rgba(253,230,138,0.2)]" />
                                                <span className="max-w-[13rem] truncate">{point.point_name}</span>
                                            </span>
                                        </button>
                                    ))}

                                    {!loadingPoints && floorPoints.length === 0 ? (
                                        <div className="absolute inset-0 p-5">
                                            <EmptyPanel
                                                title="当前楼层暂无点位"
                                                description="切换到其他楼层查看，或导入当前点位类型的模板数据。"
                                            />
                                        </div>
                                    ) : null}
                                </div>

                                {loadingPoints ? (
                                    <div className="absolute inset-0 flex items-center justify-center bg-white/46 backdrop-blur-[2px]">
                                        <div className="rounded-full bg-slate-950/78 px-4 py-2 text-sm text-white">正在加载点位...</div>
                                    </div>
                                ) : null}
                                {isFloorSwitching ? (
                                    <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_50%_20%,rgba(255,255,255,0.36),transparent_48%),linear-gradient(180deg,rgba(255,255,255,0.14),rgba(255,255,255,0.04))] opacity-100 transition-opacity duration-[420ms]" />
                                ) : null}
                            </div>
                        </div>

                        <div className="flex flex-col gap-3">
                            {floors.map((floor: DeviceLayoutFloor) => (
                                <button
                                    key={floor.code}
                                    className={[
                                        "group relative inline-flex h-14 items-center justify-center overflow-hidden rounded-[1.15rem] border text-sm font-semibold transition-all duration-[260ms] ease-[cubic-bezier(0.22,1,0.36,1)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300/70",
                                        selectedFloorCode === floor.code
                                            ? "border-slate-950 bg-slate-950 text-white shadow-[0_18px_30px_rgba(15,23,42,0.18)] -translate-y-0.5"
                                            : "border-white/80 bg-white/92 text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.05)] hover:-translate-y-0.5 hover:border-white/90 hover:text-slate-950 hover:shadow-[0_16px_28px_rgba(15,23,42,0.08)]",
                                    ].join(" ")}
                                    onClick={() => setSelectedFloorCode(floor.code)}
                                    type="button"
                                >
                                    <span
                                        className={[
                                            "absolute inset-0 rounded-[1.15rem] bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.18),transparent_58%)] transition-opacity duration-[260ms]",
                                            selectedFloorCode === floor.code ? "opacity-100" : "opacity-0 group-hover:opacity-100",
                                        ].join(" ")}
                                    />
                                    <span
                                        className={[
                                            "absolute left-1/2 top-1.5 h-[3px] w-8 -translate-x-1/2 rounded-full transition-all duration-[260ms]",
                                            selectedFloorCode === floor.code
                                                ? "bg-white/60 opacity-100 scale-100"
                                                : "bg-slate-300 opacity-0 scale-75 group-hover:opacity-70 group-hover:scale-100",
                                        ].join(" ")}
                                    />
                                    <span
                                        className={[
                                            "relative z-10 tracking-[0.02em] transition-transform duration-[260ms]",
                                            selectedFloorCode === floor.code ? "scale-[1.02]" : "group-hover:scale-[1.02]",
                                        ].join(" ")}
                                    >
                                        {floor.label}
                                    </span>
                                </button>
                            ))}
                        </div>
                    </div>
                ) : (
                    <EmptyPanel title="楼层配置缺失" description="当前未读取到楼层地图配置。" />
                )}

                <div className="mt-5 rounded-[1.55rem] border border-white/80 bg-white/84 p-4 shadow-[0_14px_30px_rgba(15,23,42,0.04)]">
                    <div className="text-sm text-slate-500">当前楼层 {displayedFloor?.label || "-"} 共 {floorPoints.length} 个点位</div>

                    <div
                        className={[
                            "mt-4 grid gap-3 transition-all duration-[420ms] ease-[cubic-bezier(0.22,1,0.36,1)] sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4",
                            floorMotionClass,
                        ].join(" ")}
                    >
                        {floorPoints.map((point) => (
                            <PointChip
                                key={point.point_code}
                                draggable={!isFloorSwitching}
                                onDragEnd={() => setDraggingPointCode("")}
                                onDragStart={(pointCode) => setDraggingPointCode(pointCode)}
                                point={point}
                            />
                        ))}
                    </div>

                    {!loadingPoints && floorPoints.length === 0 ? (
                        <div className="mt-4 rounded-[1.1rem] border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
                            当前点位类型在该楼层暂无点位。你可以切换楼层，或导入模板后再调整坐标。
                        </div>
                    ) : null}
                </div>
            </section>
        </div>
    );
}
