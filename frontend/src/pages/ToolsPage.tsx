import { useEffect, useMemo, useRef, useState, type ChangeEvent, type DragEvent } from "react";

import { getApiErrorMessage } from "../api/client";
import {
    buildAssetUrl,
    getK2PrintJob,
    getPptConverterJob,
    mergePdfEditorPages,
    processBankInfoFiles,
    startK2PrintExport,
    startPptConverter,
    uploadBankInfoFiles,
    uploadPdfEditorFiles,
    uploadPptConverterFiles,
    type BankInfoUploadFile,
    type K2PrintJobStatusResponse,
    type PdfEditorPage,
    type PptConverterJobStatusResponse,
    type PptConverterUploadFile,
} from "../api/tools";

type DropIndicator = { targetIndex: number; position: "before" | "after" } | null;

const btn =
    "inline-flex h-10 items-center justify-center rounded-[1rem] px-4 text-sm font-medium transition motion-reduce:transition-none whitespace-nowrap";
const panel =
    "rounded-[1.6rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.95),rgba(246,249,255,0.88))] p-5 shadow-[0_18px_42px_rgba(15,23,42,0.06)] backdrop-blur-xl";
const progressPanel =
    "w-full rounded-[1.1rem] border border-blue-100/80 bg-[linear-gradient(180deg,rgba(239,246,255,0.95),rgba(255,255,255,0.92))] px-4 py-3 shadow-[0_14px_28px_rgba(37,99,235,0.08)]";

function ActionButton({
    label,
    onClick,
    disabled,
    tone = "primary",
}: {
    label: string;
    onClick?: () => void;
    disabled?: boolean;
    tone?: "primary" | "secondary" | "danger";
}) {
    const toneClass =
        tone === "primary"
            ? "bg-[linear-gradient(135deg,#1d4ed8,#2563eb)] text-white shadow-[0_14px_28px_rgba(37,99,235,0.22)]"
            : tone === "danger"
              ? "bg-[linear-gradient(135deg,#ef4444,#dc2626)] text-white shadow-[0_14px_28px_rgba(220,38,38,0.22)]"
              : "border border-white/80 bg-white/92 text-slate-700 shadow-[0_10px_22px_rgba(15,23,42,0.06)]";

    return (
        <button
            className={`${btn} ${toneClass} ${disabled ? "cursor-not-allowed opacity-50" : "cursor-pointer"}`}
            disabled={disabled}
            onClick={onClick}
            type="button"
        >
            {label}
        </button>
    );
}

function NoticePill({ text }: { text: string }) {
    return (
        <div className="inline-flex h-10 items-center rounded-[1rem] border border-emerald-200 bg-emerald-50 px-4 text-sm font-medium text-emerald-700 shadow-[0_10px_18px_rgba(16,185,129,0.08)]">
            {text}
        </div>
    );
}

function ErrorPill({ text }: { text: string }) {
    return (
        <div className="inline-flex h-10 items-center rounded-[1rem] border border-rose-200 bg-rose-50 px-4 text-sm font-medium text-rose-700 shadow-[0_10px_18px_rgba(244,63,94,0.08)]">
            {text}
        </div>
    );
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

function moveItem<T>(items: T[], from: number, to: number) {
    if (to < 0 || to >= items.length || from === to) return items;
    const next = [...items];
    const [picked] = next.splice(from, 1);
    next.splice(to, 0, picked);
    return next;
}

function reorderPages(pages: PdfEditorPage[], draggedPageId: string, indicator: DropIndicator) {
    if (!indicator) return pages;
    const sourceIndex = pages.findIndex((item) => item.page_id === draggedPageId);
    if (sourceIndex < 0) return pages;

    const next = [...pages];
    const [dragged] = next.splice(sourceIndex, 1);
    const rawInsertIndex = indicator.position === "before" ? indicator.targetIndex : indicator.targetIndex + 1;
    const insertIndex = sourceIndex < rawInsertIndex ? rawInsertIndex - 1 : rawInsertIndex;
    next.splice(Math.max(0, Math.min(insertIndex, next.length)), 0, dragged);
    return next;
}

function resolveK2Progress(job: K2PrintJobStatusResponse | null) {
    if (!job) {
        return { percent: 0, label: "等待开始" };
    }

    const stagePercent: Record<string, number> = {
        queued: 8,
        resolve_workflow_url: 18,
        workflow_url_resolved: 34,
        load_live_cookies: 52,
        keepalive: 66,
        export_pdf: 88,
        success: 100,
        failed: 100,
    };

    const stageLabel: Record<string, string> = {
        queued: "排队中",
        resolve_workflow_url: "解析流程",
        workflow_url_resolved: "流程就绪",
        load_live_cookies: "载入会话",
        keepalive: "保持登录",
        export_pdf: "导出 PDF",
        success: "已完成",
        failed: "失败",
    };

    return {
        percent: stagePercent[job.stage] ?? (job.status === "success" ? 100 : 12),
        label: stageLabel[job.stage] ?? job.stage,
    };
}

export default function ToolsPage() {
    const editorInputRef = useRef<HTMLInputElement | null>(null);
    const bankInputRef = useRef<HTMLInputElement | null>(null);
    const pptInputRef = useRef<HTMLInputElement | null>(null);
    const downloadedPptUrlsRef = useRef(new Set<string>());
    const downloadedK2UrlsRef = useRef(new Set<string>());

    const [editorSessionId, setEditorSessionId] = useState<string | null>(null);
    const [editorPages, setEditorPages] = useState<PdfEditorPage[]>([]);
    const [editorUploading, setEditorUploading] = useState(false);
    const [editorMerging, setEditorMerging] = useState(false);
    const [editorError, setEditorError] = useState("");
    const [editorNotice, setEditorNotice] = useState("");
    const [draggedPageId, setDraggedPageId] = useState<string | null>(null);
    const [dropIndicator, setDropIndicator] = useState<DropIndicator>(null);

    const [bankSessionId, setBankSessionId] = useState<string | null>(null);
    const [bankFiles, setBankFiles] = useState<BankInfoUploadFile[]>([]);
    const [bankUploading, setBankUploading] = useState(false);
    const [bankProcessing, setBankProcessing] = useState(false);
    const [bankError, setBankError] = useState("");
    const [bankNotice, setBankNotice] = useState("");

    const [pptSessionId, setPptSessionId] = useState<string | null>(null);
    const [pptFiles, setPptFiles] = useState<PptConverterUploadFile[]>([]);
    const [pptUploading, setPptUploading] = useState(false);
    const [pptProcessing, setPptProcessing] = useState(false);
    const [pptError, setPptError] = useState("");
    const [pptNotice, setPptNotice] = useState("");
    const [pptJob, setPptJob] = useState<PptConverterJobStatusResponse | null>(null);

    const [k2No, setK2No] = useState("");
    const [k2Submitting, setK2Submitting] = useState(false);
    const [k2Error, setK2Error] = useState("");
    const [k2Job, setK2Job] = useState<K2PrintJobStatusResponse | null>(null);

    const pptTotalSlides = useMemo(
        () => pptJob?.total_slides ?? pptFiles.reduce((sum, item) => sum + item.slide_count, 0),
        [pptFiles, pptJob],
    );
    const k2Progress = useMemo(() => resolveK2Progress(k2Job), [k2Job]);

    useEffect(() => {
        if (!pptJob?.job_id || !["queued", "running"].includes(pptJob.status)) return;
        const timer = window.setInterval(async () => {
            try {
                const next = await getPptConverterJob(pptJob.job_id);
                setPptJob(next);
                if (next.status === "success" || next.status === "failed") {
                    setPptProcessing(false);
                }
            } catch (error) {
                setPptProcessing(false);
                setPptError(getApiErrorMessage(error));
                window.clearInterval(timer);
            }
        }, 1200);
        return () => window.clearInterval(timer);
    }, [pptJob]);

    useEffect(() => {
        if (!k2Job?.job_id || !["queued", "running"].includes(k2Job.status)) return;
        const timer = window.setInterval(async () => {
            try {
                const next = await getK2PrintJob(k2Job.job_id);
                setK2Job(next);
                if (next.status === "success" || next.status === "failed") {
                    setK2Submitting(false);
                }
            } catch (error) {
                setK2Submitting(false);
                setK2Error(getApiErrorMessage(error));
                window.clearInterval(timer);
            }
        }, 1200);
        return () => window.clearInterval(timer);
    }, [k2Job]);

    useEffect(() => {
        if (!pptJob || pptJob.status !== "success") return;
        pptJob.files
            .filter((item) => item.success && item.download_url)
            .forEach((item, index) => {
                const url = buildAssetUrl(item.download_url!);
                if (downloadedPptUrlsRef.current.has(url)) return;
                downloadedPptUrlsRef.current.add(url);
                window.setTimeout(() => triggerDownload(url), index * 180);
            });
    }, [pptJob]);

    useEffect(() => {
        if (!k2Job || k2Job.status !== "success" || !k2Job.download_url) return;
        const url = buildAssetUrl(k2Job.download_url);
        if (downloadedK2UrlsRef.current.has(url)) return;
        downloadedK2UrlsRef.current.add(url);
        triggerDownload(url);
    }, [k2Job]);

    async function handleEditorUpload(event: ChangeEvent<HTMLInputElement>) {
        const files = Array.from(event.target.files || []).filter((file) => file.name.toLowerCase().endsWith(".pdf"));
        event.target.value = "";
        if (!files.length) return;

        setEditorUploading(true);
        setEditorError("");
        setEditorNotice("");
        try {
            const response = await uploadPdfEditorFiles(files, editorSessionId);
            setEditorSessionId(response.session_id);
            setEditorPages((current) => [...current, ...response.documents.flatMap((item) => item.pages)]);
            setEditorNotice(`已上传 ${response.total_pages} 页`);
        } catch (error) {
            setEditorError(getApiErrorMessage(error));
        } finally {
            setEditorUploading(false);
        }
    }

    async function handleMergeDownload() {
        if (!editorSessionId || editorPages.length === 0) {
            setEditorError("请先上传 PDF");
            return;
        }

        setEditorMerging(true);
        setEditorError("");
        try {
            const response = await mergePdfEditorPages(
                editorSessionId,
                editorPages.map((page) => ({
                    file_id: page.file_id,
                    page_index: page.page_index,
                })),
            );
            triggerDownload(buildAssetUrl(response.download_url));
        } catch (error) {
            setEditorError(getApiErrorMessage(error));
        } finally {
            setEditorMerging(false);
        }
    }

    async function handleBankUpload(event: ChangeEvent<HTMLInputElement>) {
        const files = Array.from(event.target.files || []).filter((file) => file.name.toLowerCase().endsWith(".pdf"));
        event.target.value = "";
        if (!files.length) return;

        setBankUploading(true);
        setBankError("");
        setBankNotice("");
        try {
            const response = await uploadBankInfoFiles(files, bankSessionId);
            setBankSessionId(response.session_id);
            setBankFiles((current) => {
                const map = new Map(current.map((item) => [item.file_id, item]));
                response.files.forEach((item) => map.set(item.file_id, item));
                return Array.from(map.values());
            });
            setBankNotice(`已上传 ${response.files.length} 个文件`);
        } catch (error) {
            setBankError(getApiErrorMessage(error));
        } finally {
            setBankUploading(false);
        }
    }

    async function handleBankProcess() {
        if (!bankSessionId || bankFiles.length === 0) {
            setBankError("请先上传 PDF");
            return;
        }

        setBankProcessing(true);
        setBankError("");
        setBankNotice("");
        try {
            const response = await processBankInfoFiles(bankSessionId);
            response.files
                .filter((item) => item.success && item.download_url)
                .forEach((item, index) => window.setTimeout(() => triggerDownload(buildAssetUrl(item.download_url!)), index * 180));

            if (response.success_count > 0) {
                setBankNotice(`开始下载 ${response.success_count} 个 PDF`);
            }
            if (response.success_count === 0) {
                setBankError("上传文件不是正确Ole账单");
            } else if (response.failed_count > 0) {
                setBankError("部分文件不是正确Ole账单");
            }
        } catch (error) {
            setBankError(getApiErrorMessage(error));
        } finally {
            setBankProcessing(false);
        }
    }

    async function handlePptUpload(event: ChangeEvent<HTMLInputElement>) {
        const files = Array.from(event.target.files || []).filter((file) => file.name.toLowerCase().endsWith(".pptx"));
        event.target.value = "";
        if (!files.length) return;

        setPptUploading(true);
        setPptError("");
        setPptNotice("");
        downloadedPptUrlsRef.current.clear();
        try {
            const response = await uploadPptConverterFiles(files);
            setPptSessionId(response.session_id);
            setPptFiles(response.files);
            setPptNotice(`已上传 ${response.files.length} 个文件`);
            setPptJob(null);
        } catch (error) {
            setPptError(getApiErrorMessage(error));
        } finally {
            setPptUploading(false);
        }
    }

    async function handlePptProcess() {
        if (!pptSessionId || pptFiles.length === 0) {
            setPptError("请先上传 PPTX");
            return;
        }

        setPptProcessing(true);
        setPptError("");
        setPptNotice("");
        downloadedPptUrlsRef.current.clear();
        try {
            const response = await startPptConverter(pptSessionId);
            setPptJob({
                session_id: response.session_id,
                job_id: response.job_id,
                status: response.status,
                total_files: pptFiles.length,
                processed_files: 0,
                total_slides: pptTotalSlides,
                processed_slides: 0,
                progress_percent: 0,
                current_file_name: pptFiles[0]?.file_name || null,
                started_at: null,
                finished_at: null,
                error: null,
                files: [],
            });
            setPptNotice("开始转换");
        } catch (error) {
            setPptProcessing(false);
            setPptError(getApiErrorMessage(error));
        }
    }

    async function handleK2Export() {
        if (!k2No.trim()) {
            setK2Error("请输入 K2 号");
            return;
        }

        setK2Submitting(true);
        setK2Error("");
        downloadedK2UrlsRef.current.clear();
        try {
            const response = await startK2PrintExport(k2No.trim());
            setK2Job({
                job_id: response.job_id,
                k2_no: k2No.trim(),
                status: response.status,
                stage: "queued",
                started_at: null,
                finished_at: null,
                resolved_workflow_url: null,
                resolved_print_url: null,
                download_url: null,
                error: null,
                logs: [],
            });
        } catch (error) {
            setK2Submitting(false);
            setK2Error(getApiErrorMessage(error));
        }
    }

    return (
        <div className="space-y-5">
            <section className={panel}>
                <h3 className="text-[1.25rem] font-semibold tracking-[-0.04em] text-slate-950">PDF 编辑</h3>
                <div className="mt-4 flex w-full items-start justify-between gap-4">
                    <input
                        ref={editorInputRef}
                        accept="application/pdf"
                        className="hidden"
                        multiple
                        onChange={handleEditorUpload}
                        type="file"
                    />
                    <div className="flex flex-wrap items-center gap-3">
                        <ActionButton
                            disabled={editorUploading}
                            label={editorUploading ? "上传中.." : "上传 PDF"}
                            onClick={() => editorInputRef.current?.click()}
                            tone="secondary"
                        />
                        <ActionButton
                            disabled={editorMerging || editorPages.length === 0}
                            label={editorMerging ? "处理中.." : "合并后下载"}
                            onClick={() => void handleMergeDownload()}
                        />
                        {editorNotice ? <NoticePill text={editorNotice} /> : null}
                    </div>
                    <ActionButton
                        disabled={editorPages.length === 0}
                        label="清空页面"
                        onClick={() => {
                            setEditorSessionId(null);
                            setEditorPages([]);
                            setEditorError("");
                            setEditorNotice("");
                            setDraggedPageId(null);
                            setDropIndicator(null);
                            if (editorInputRef.current) editorInputRef.current.value = "";
                        }}
                        tone="danger"
                    />
                </div>
                {editorError ? (
                    <div className="mt-4 rounded-[1rem] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                        {editorError}
                    </div>
                ) : null}
                <div className="mt-5 flex flex-wrap gap-3">
                    {editorPages.map((page, index) => {
                        const showLeftGuide = dropIndicator?.targetIndex === index && dropIndicator.position === "before";
                        const showRightGuide = dropIndicator?.targetIndex === index && dropIndicator.position === "after";

                        return (
                            <article
                                key={`${page.page_id}-${index}`}
                                className={`relative w-[9.25rem] overflow-hidden rounded-[1.2rem] border bg-white/92 shadow-[0_12px_24px_rgba(15,23,42,0.05)] ${draggedPageId === page.page_id ? "border-blue-300/90 opacity-60" : "border-slate-200/90"}`}
                                draggable
                                onDragEnd={() => {
                                    setDraggedPageId(null);
                                    setDropIndicator(null);
                                }}
                                onDragOver={(event: DragEvent<HTMLElement>) => {
                                    event.preventDefault();
                                    const rect = event.currentTarget.getBoundingClientRect();
                                    setDropIndicator({
                                        targetIndex: index,
                                        position: event.clientX - rect.left < rect.width / 2 ? "before" : "after",
                                    });
                                }}
                                onDragStart={(event: DragEvent<HTMLElement>) => {
                                    setDraggedPageId(page.page_id);
                                    event.dataTransfer.effectAllowed = "move";
                                    event.dataTransfer.setData("text/plain", page.page_id);
                                }}
                                onDrop={(event: DragEvent<HTMLElement>) => {
                                    event.preventDefault();
                                    const rect = event.currentTarget.getBoundingClientRect();
                                    const position = event.clientX - rect.left < rect.width / 2 ? "before" : "after";
                                    const dragId = draggedPageId || event.dataTransfer.getData("text/plain");
                                    if (dragId) {
                                        setEditorPages((current) =>
                                            reorderPages(current, dragId, { targetIndex: index, position }),
                                        );
                                    }
                                    setDraggedPageId(null);
                                    setDropIndicator(null);
                                }}
                            >
                                {showLeftGuide ? <div className="pointer-events-none absolute inset-y-2 left-0 z-10 w-[4px] rounded-full bg-blue-500 shadow-[0_0_0_3px_rgba(191,219,254,0.85)]" /> : null}
                                {showRightGuide ? <div className="pointer-events-none absolute inset-y-2 right-0 z-10 w-[4px] rounded-full bg-blue-500 shadow-[0_0_0_3px_rgba(191,219,254,0.85)]" /> : null}
                                <div className="aspect-[210/297] bg-slate-100">
                                    <img alt={`${page.source_file_name} 第 ${page.page_number} 页`} className="h-full w-full object-contain" loading="lazy" src={buildAssetUrl(page.thumbnail_url)} />
                                </div>
                                <div className="space-y-2 px-3 py-3">
                                    <div className="flex items-start justify-between gap-2">
                                        <div className="min-w-0">
                                            <div className="truncate whitespace-nowrap text-[0.83rem] font-medium text-slate-900">{page.source_file_name}</div>
                                            <div className="mt-1 text-[0.72rem] text-slate-500">{page.page_number}</div>
                                        </div>
                                        <div className="inline-flex h-8 w-8 cursor-grab items-center justify-center rounded-[0.9rem] border border-blue-100/90 bg-[linear-gradient(180deg,rgba(239,246,255,0.96),rgba(224,242,254,0.9))] text-blue-600 shadow-[0_10px_22px_rgba(37,99,235,0.12)] active:cursor-grabbing">
                                            <svg aria-hidden="true" className="h-4.5 w-4.5" viewBox="0 0 24 24" fill="currentColor"><circle cx="8" cy="8" r="2.1" /><circle cx="16" cy="8" r="2.1" /><circle cx="8" cy="16" r="2.1" /><circle cx="16" cy="16" r="2.1" /></svg>
                                        </div>
                                    </div>
                                    <div className="flex items-center justify-end gap-2">
                                        <button className="inline-flex h-8 w-8 items-center justify-center rounded-[0.9rem] border border-slate-200/90 bg-white text-slate-700 disabled:opacity-40" disabled={index === 0} onClick={() => setEditorPages((current) => moveItem(current, index, index - 1))} type="button"><svg className="h-3.5 w-3.5" viewBox="0 0 20 20" fill="currentColor"><path d="M10 4.25l5.75 8.5H4.25L10 4.25z" /></svg></button>
                                        <button className="inline-flex h-8 w-8 items-center justify-center rounded-[0.9rem] border border-slate-200/90 bg-white text-slate-700 disabled:opacity-40" disabled={index === editorPages.length - 1} onClick={() => setEditorPages((current) => moveItem(current, index, index + 1))} type="button"><svg className="h-3.5 w-3.5" viewBox="0 0 20 20" fill="currentColor"><path d="M10 15.75l-5.75-8.5h11.5L10 15.75z" /></svg></button>
                                        <button className="inline-flex h-8 w-8 items-center justify-center" onClick={() => setEditorPages((current) => current.filter((_, currentIndex) => currentIndex !== index))} type="button"><span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-[linear-gradient(180deg,#fb7185,#e11d48)] text-white shadow-[0_10px_22px_rgba(225,29,72,0.28)]"><svg className="h-3.5 w-3.5" viewBox="0 0 20 20" fill="none"><path d="M5 10h10" stroke="currentColor" strokeLinecap="round" strokeWidth="2.4" /></svg></span></button>
                                    </div>
                                </div>
                            </article>
                        );
                    })}
                </div>
            </section>

            <div className="grid gap-5 lg:grid-cols-2">
                <section className={panel}>
                    <h3 className="text-[1.25rem] font-semibold tracking-[-0.04em] text-slate-950">三分屏PPT转换</h3>
                    <div className="mt-4 flex h-full flex-col gap-4">
                        <input ref={pptInputRef} accept=".pptx,application/vnd.openxmlformats-officedocument.presentationml.presentation" className="hidden" multiple onChange={handlePptUpload} type="file" />
                        <div className="flex flex-wrap items-center gap-3">
                            <ActionButton disabled={pptUploading} label={pptUploading ? "上传中.." : "上传PPT"} onClick={() => pptInputRef.current?.click()} tone="secondary" />
                            <ActionButton disabled={pptProcessing || pptFiles.length === 0} label={pptProcessing ? "转换中.." : "转换后下载"} onClick={() => void handlePptProcess()} />
                            {pptNotice ? <NoticePill text={pptNotice} /> : null}
                        </div>
                        <div className={progressPanel}>
                            <div className="flex items-center justify-between gap-3"><span className="text-sm font-medium text-slate-900">处理页数</span><span className="text-sm font-semibold text-blue-700">{pptJob?.processed_slides ?? 0} / {pptTotalSlides}</span></div>
                            <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-blue-100/80"><div className="h-full rounded-full bg-[linear-gradient(90deg,#38bdf8,#2563eb)] transition-[width] duration-300" style={{ width: `${Math.max(0, Math.min(pptJob?.progress_percent ?? 0, 100))}%` }} /></div>
                        </div>
                    </div>
                    {pptError ? <div className="mt-4 rounded-[1rem] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{pptError}</div> : null}
                </section>

                <section className={panel}>
                    <h3 className="text-[1.25rem] font-semibold tracking-[-0.04em] text-slate-950">K2流程完整审批记录导出</h3>
                    <div className="mt-4 flex h-full flex-col gap-4">
                        <div className="flex flex-wrap items-center gap-3">
                            <label className="text-sm font-medium text-slate-700" htmlFor="k2-no-input">K2 号</label>
                            <input className="h-10 w-[15ch] rounded-[1rem] border border-slate-200/90 bg-white/92 px-4 text-sm text-slate-900 shadow-[0_10px_22px_rgba(15,23,42,0.06)] outline-none transition focus:border-blue-300 focus:ring-4 focus:ring-blue-100" id="k2-no-input" onChange={(event) => setK2No(event.target.value)} placeholder="请输入" type="text" value={k2No} />
                            <ActionButton disabled={k2Submitting} label={k2Submitting ? "处理中.." : "导出为 PDF"} onClick={() => void handleK2Export()} />
                        </div>
                        <div className={progressPanel}>
                            <div className="flex items-center justify-between gap-3">
                                <span className="text-sm font-medium text-slate-900">处理进度</span>
                                <span className={`text-sm font-semibold ${k2Job?.status === "failed" ? "text-rose-600" : k2Job?.status === "success" ? "text-emerald-600" : "text-blue-700"}`}>{k2Progress.percent}%</span>
                            </div>
                            <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-blue-100/80">
                                <div className={`h-full rounded-full transition-[width] duration-300 ${k2Job?.status === "failed" ? "bg-[linear-gradient(90deg,#fb7185,#e11d48)]" : k2Job?.status === "success" ? "bg-[linear-gradient(90deg,#34d399,#059669)]" : "bg-[linear-gradient(90deg,#38bdf8,#2563eb)]"}`} style={{ width: `${Math.max(0, Math.min(k2Progress.percent, 100))}%` }} />
                            </div>
                        </div>
                    </div>
                    {k2Error ? <div className="mt-4 rounded-[1rem] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{k2Error}</div> : null}
                    {k2Job?.error ? <div className="mt-4 rounded-[1rem] border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">{k2Job.error}</div> : null}
                </section>

                <section className={panel}>
                    <h3 className="text-[1.25rem] font-semibold tracking-[-0.04em] text-slate-950">Ole账单修改银行名称</h3>
                    <div className="mt-4 flex flex-wrap items-center gap-3">
                        <input ref={bankInputRef} accept="application/pdf" className="hidden" multiple onChange={handleBankUpload} type="file" />
                        <ActionButton disabled={bankUploading} label={bankUploading ? "上传中.." : "上传"} onClick={() => bankInputRef.current?.click()} tone="secondary" />
                        <ActionButton disabled={bankProcessing || bankFiles.length === 0} label={bankProcessing ? "处理中.." : "修改后下载"} onClick={() => void handleBankProcess()} />
                        {bankNotice ? <NoticePill text={bankNotice} /> : null}
                        {bankError ? <ErrorPill text={bankError} /> : null}
                    </div>
                </section>
            </div>
        </div>
    );
}
