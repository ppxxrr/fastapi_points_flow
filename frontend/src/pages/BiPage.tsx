import { useMemo, useState, type ReactNode } from "react";
import {
    Area,
    AreaChart,
    Bar,
    BarChart,
    CartesianGrid,
    Cell,
    ComposedChart,
    Funnel,
    FunnelChart,
    Legend,
    Line,
    LineChart,
    Pie,
    PieChart,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from "recharts";

import {
    getBiDashboard,
    type BiDashboardResponse,
    type BiDailySeriesItem,
    type BiPassengerTrendBlock,
} from "../api/admin";
import { getApiErrorMessage, isUnauthorizedError } from "../api/client";

interface BiPageProps {
    onLogout: () => Promise<void> | void;
}

type BiMode = "daily" | "range";
type BiCategory = "policy" | "regular" | "passenger" | "exception";

const CHART_COLORS = ["#2563eb", "#0f766e", "#f97316", "#7c3aed", "#ef4444", "#14b8a6", "#8b5cf6", "#0891b2"];

const SERIES_LABELS: Record<string, string> = {
    parking_count: "停车记录",
    trade_count: "停车交易",
    point_flow_count: "积分流水",
    trade_amount_yuan: "停车交易金额（元）",
    consume_amount_yuan: "消费金额（元）",
    amount_yuan: "金额（元）",
    count: "记录数",
    value: "数值",
    parking_members: "停车会员",
    trade_members: "交易会员",
    point_members: "积分会员",
    actual_receivable_yuan: "新规应收（元）",
    simulated_old_policy_yuan: "旧规模拟应收（元）",
    receivable_uplift_yuan: "增收差额（元）",
    recorded_fee_yuan: "记录实收（元）",
    point_earned_count: "产生积分停车次数",
    point_bonus_triggered_count: "享受+2小时优惠次数",
    point_bonus_saved_yuan: "积分加成释放金额（元）",
    before_share_pct: "变更前占比（%）",
    after_share_pct: "变更后占比（%）",
};

const BUSINESS_LABELS: Record<string, string> = {
    "car:parking:cashier": "停车收费",
    "car:parking:refund": "停车退款",
    "car:parking:prepay": "停车预付",
    "car:parking:recharge": "停车充值",
};

const COUNT_LABELS = new Set([
    "停车记录",
    "停车交易",
    "积分流水",
    "记录数",
    "笔数",
    "停车会员",
    "交易会员",
    "积分会员",
    "三域手机号交集",
    "三域会员交集",
    "产生积分停车次数",
    "享受+2小时优惠次数",
]);

const CATEGORY_META: Record<
    BiCategory,
    {
        label: string;
        shortLabel: string;
        tone: "blue" | "slate" | "amber";
    }
> = {
    policy: {
        label: "权益变更分析",
        shortLabel: "权益变更分析",
        tone: "blue",
    },
    regular: {
        label: "常规分析",
        shortLabel: "常规分析",
        tone: "slate",
    },
    passenger: {
        label: "客流分析",
        shortLabel: "客流分析",
        tone: "slate",
    },
    exception: {
        label: "异常分析",
        shortLabel: "异常分析",
        tone: "amber",
    },
};

function normalizeNumber(value: number) {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return Math.round(value * 100) / 100;
}

function formatInteger(value: number) {
    return Math.round(value || 0).toLocaleString("zh-CN");
}

function formatFlexibleDecimal(value: number) {
    const normalized = normalizeNumber(value || 0);
    const fractionDigits = Number.isInteger(normalized) ? 0 : Number.isInteger(normalized * 10) ? 1 : 2;
    return normalized.toLocaleString("zh-CN", {
        minimumFractionDigits: fractionDigits,
        maximumFractionDigits: fractionDigits,
    });
}

function formatMoney(value: number) {
    return `${formatFlexibleDecimal(value)} 元`;
}

function formatPercent(value: number) {
    return `${formatFlexibleDecimal(value)}%`;
}

function formatMetricText(value: string | number) {
    if (typeof value === "number") {
        return formatFlexibleDecimal(value);
    }
    const text = String(value ?? "").trim();
    if (!text) {
        return "0";
    }
    if (text.endsWith("%")) {
        return text;
    }
    if (text.endsWith("元")) {
        const parsed = Number.parseFloat(text.replace(/元/g, "").replace(/,/g, "").trim());
        return Number.isFinite(parsed) ? `${formatFlexibleDecimal(parsed)} 元` : text;
    }
    const parsed = Number.parseFloat(text.replace(/,/g, ""));
    if (Number.isFinite(parsed) && /^-?\d+(\.\d+)?$/.test(text.replace(/,/g, ""))) {
        return formatFlexibleDecimal(parsed);
    }
    return translateLabel(text);
}

function translateLabel(value: string) {
    return SERIES_LABELS[value] || BUSINESS_LABELS[value] || value;
}

function isMoneyLabel(label: string) {
    return label.includes("金额") || label.includes("元");
}

function isCountLabel(label: string) {
    return COUNT_LABELS.has(label) || label.includes("数") || label.includes("笔数") || label.includes("辆次");
}

function formatChartValue(value: number | string, kind: "count" | "money" = "count") {
    const numeric = typeof value === "number" ? value : Number.parseFloat(String(value));
    if (!Number.isFinite(numeric)) {
        return String(value ?? "");
    }
    return kind === "money" ? formatFlexibleDecimal(numeric) : formatInteger(numeric);
}

function formatDateAxisLabel(value: string) {
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        const [, month, day] = value.split("-");
        return `${month}-${day}`;
    }
    return value;
}

function tooltipFormatter(value: number | string, name: string) {
    const label = translateLabel(String(name));
    const kind = isMoneyLabel(label) ? "money" : "count";
    return [formatChartValue(value, kind), label];
}

function legendFormatter(value: string) {
    return translateLabel(String(value));
}

function yesterdayString() {
    const now = new Date();
    now.setDate(now.getDate() - 1);
    return now.toISOString().slice(0, 10);
}

function dateBefore(days: number) {
    const now = new Date();
    now.setDate(now.getDate() - days);
    return now.toISOString().slice(0, 10);
}

function Section({
    title,
    subtitle,
    children,
}: {
    title: string;
    subtitle?: string;
    children: ReactNode;
}) {
    return (
        <section className="rounded-[1.85rem] border border-white/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.94),rgba(247,249,255,0.86))] p-5 shadow-[0_18px_42px_rgba(15,23,42,0.06)] backdrop-blur-xl">
            <div className="mb-5">
                <h3 className="text-[1.4rem] font-semibold tracking-[-0.045em] text-slate-950">{title}</h3>
                {subtitle ? <div className="mt-1 text-sm text-slate-500">{subtitle}</div> : null}
            </div>
            {children}
        </section>
    );
}

function CategoryHeader({
    title,
    subtitle,
    tone,
}: {
    title: string;
    subtitle?: string;
    tone: "blue" | "slate" | "amber";
}) {
    const toneClass =
        tone === "blue"
            ? "border-blue-100/90 bg-[linear-gradient(135deg,rgba(239,246,255,0.95),rgba(247,250,255,0.88))]"
            : tone === "amber"
              ? "border-amber-100/90 bg-[linear-gradient(135deg,rgba(255,251,235,0.95),rgba(255,247,237,0.9))]"
              : "border-slate-200/90 bg-[linear-gradient(135deg,rgba(248,250,252,0.95),rgba(255,255,255,0.88))]";

    return (
        <div className={`rounded-[1.55rem] border px-5 py-4 shadow-[0_14px_30px_rgba(15,23,42,0.04)] ${toneClass}`}>
            <div className="text-[1.08rem] font-semibold tracking-[-0.03em] text-slate-950">{title}</div>
            {subtitle ? <div className="mt-1 text-sm leading-6 text-slate-500">{subtitle}</div> : null}
        </div>
    );
}

function StatCard({
    label,
    value,
    note,
}: {
    label: string;
    value: string;
    note?: string;
}) {
    return (
        <div className="rounded-[1.2rem] border border-white/80 bg-white/88 px-4 py-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
            <div className="text-xs uppercase tracking-[0.18em] text-slate-400">{label}</div>
            <div className="mt-2 text-[1.7rem] font-semibold tracking-[-0.05em] text-slate-950">{value}</div>
            {note ? <div className="mt-1 text-sm text-slate-500">{note}</div> : null}
        </div>
    );
}

function EmptyState({ message }: { message: string }) {
    return (
        <div className="rounded-[1.2rem] border border-dashed border-slate-200 bg-white/70 px-4 py-10 text-center text-sm text-slate-500">
            {message}
        </div>
    );
}

function Heatmap({ rows }: { rows: BiDashboardResponse["hourly_distribution"] }) {
    const maxValue = Math.max(...rows.map((item) => item.parking_count + item.trade_count + item.point_flow_count), 1);

    return (
        <div className="grid grid-cols-6 gap-3 lg:grid-cols-12">
            {rows.map((item) => {
                const value = item.parking_count + item.trade_count + item.point_flow_count;
                const opacity = 0.18 + (value / maxValue) * 0.82;
                return (
                    <div
                        key={item.hour}
                        className="rounded-[1rem] border border-white/80 px-3 py-3 shadow-[0_10px_18px_rgba(15,23,42,0.03)]"
                        style={{ backgroundColor: `rgba(37,99,235,${Math.min(opacity, 1)})` }}
                    >
                        <div className="text-xs font-medium text-white/90">{item.hour}</div>
                        <div className="mt-3 text-lg font-semibold text-white">{formatInteger(value)}</div>
                        <div className="mt-2 text-[11px] leading-5 text-white/90">
                            停车 {formatInteger(item.parking_count)}
                            <br />
                            交易 {formatInteger(item.trade_count)}
                            <br />
                            积分 {formatInteger(item.point_flow_count)}
                        </div>
                    </div>
                );
            })}
        </div>
    );
}

function PassengerTrendChart({
    block,
    periodLabel,
}: {
    block: BiPassengerTrendBlock;
    periodLabel: string;
}) {
    const currentYear = block.current_year || "当年";
    const previousYear = block.previous_year || "同期";
    const rows = block.daily_compare || [];
    const hasData = rows.some((item) => item.current_value > 0 || item.previous_value > 0);
    const currentDotRadius = rows.length >= 75 ? 2.4 : rows.length >= 45 ? 3.2 : 5;
    const currentActiveDotRadius = rows.length >= 75 ? 3.2 : rows.length >= 45 ? 4.2 : 6;
    const previousDotRadius = rows.length >= 75 ? 2.2 : rows.length >= 45 ? 3 : 4.5;
    const previousActiveDotRadius = rows.length >= 75 ? 3 : rows.length >= 45 ? 4 : 5.5;
    const dotStrokeWidth = rows.length >= 75 ? 1.8 : rows.length >= 45 ? 2.2 : 3;

    if (!hasData) {
        return <EmptyState message="当前筛选范围没有可展示的客流同期趋势数据。" />;
    }

    return (
        <div className="rounded-[1.6rem] border border-[#ebe7e2] bg-[#fcfbf8] p-4 shadow-[0_18px_36px_rgba(15,23,42,0.05)]">
            <div className="mb-3 text-center">
                <div className="text-[2rem] font-semibold tracking-[-0.05em] text-slate-800">
                    {block.title}
                    <br />
                    <span className="text-[1.1rem] font-medium tracking-[-0.03em] text-slate-600">({periodLabel})</span>
                </div>
            </div>

            <div className="relative h-[520px] w-full">
                <ResponsiveContainer>
                    <LineChart data={rows} margin={{ top: 22, right: 28, left: 10, bottom: 30 }}>
                        <CartesianGrid stroke="#ddd7d1" strokeDasharray="0" vertical={true} />
                        <XAxis
                            dataKey="label"
                            angle={-48}
                            dy={18}
                            height={66}
                            interval={Math.max(Math.floor(rows.length / 9), 0)}
                            stroke="#6b7280"
                            tick={{ fill: "#374151", fontSize: 12 }}
                        />
                        <YAxis
                            stroke="#6b7280"
                            tick={{ fill: "#374151", fontSize: 12 }}
                            tickFormatter={(value) => formatChartValue(value, "count")}
                            width={84}
                        />
                        <Tooltip
                            formatter={(value, name) => [
                                formatChartValue(Array.isArray(value) ? value[0] : value, "count"),
                                String(name),
                            ]}
                            labelFormatter={(label, payload) => {
                                const row = payload?.[0]?.payload as { current_date?: string; previous_date?: string } | undefined;
                                if (!row) {
                                    return String(label);
                                }
                                return `${label} | ${currentYear}: ${row.current_date} / ${previousYear}: ${row.previous_date}`;
                            }}
                        />
                        <Legend
                            verticalAlign="top"
                            align="right"
                            wrapperStyle={{
                                top: 12,
                                right: 16,
                                border: "1px solid #d8d4cf",
                                borderRadius: 16,
                                padding: "12px 16px",
                                backgroundColor: "rgba(255,255,255,0.92)",
                                boxShadow: "0 10px 24px rgba(15,23,42,0.12)",
                            }}
                        />
                        <Line
                            type="monotone"
                            dataKey="current_value"
                            name={`${currentYear}年客流量`}
                            stroke="#e34656"
                            strokeWidth={3.6}
                            dot={{ r: currentDotRadius, fill: "#ffffff", stroke: "#e34656", strokeWidth: dotStrokeWidth }}
                            activeDot={{ r: currentActiveDotRadius, fill: "#ffffff", stroke: "#e34656", strokeWidth: dotStrokeWidth }}
                        />
                        <Line
                            type="monotone"
                            dataKey="previous_value"
                            name={`${previousYear}年客流量`}
                            stroke="#8d949c"
                            strokeWidth={3.2}
                            strokeDasharray="10 7"
                            dot={{ r: previousDotRadius, fill: "#ffffff", stroke: "#8d949c", strokeWidth: dotStrokeWidth }}
                            activeDot={{ r: previousActiveDotRadius, fill: "#ffffff", stroke: "#8d949c", strokeWidth: dotStrokeWidth }}
                        />
                    </LineChart>
                </ResponsiveContainer>

                <div className="pointer-events-none absolute bottom-5 left-5 rounded-[1.1rem] border border-[#ddd7d1] bg-[rgba(255,255,255,0.92)] px-4 py-3 text-[0.82rem] leading-7 text-slate-700 shadow-[0_14px_26px_rgba(15,23,42,0.08)]">
                    <div className="font-semibold text-slate-900">关键统计：</div>
                    <div className="whitespace-nowrap">{currentYear}年平均（红色）：{formatInteger(block.summary.current_avg)} 人次</div>
                    <div className="whitespace-nowrap">{previousYear}年平均（灰色）：{formatInteger(block.summary.previous_avg)} 人次</div>
                    <div className="whitespace-nowrap">差异率：{formatPercent(block.summary.diff_rate_pct)}</div>
                </div>
            </div>
        </div>
    );
}

function buildRangeSummary(series: BiDailySeriesItem[], label: string): BiDailySeriesItem[] {
    if (series.length === 0) {
        return [];
    }

    const summary = series.reduce<BiDailySeriesItem>(
        (acc, item) => ({
            date: label,
            parking_count: acc.parking_count + item.parking_count,
            parking_fee_yuan: acc.parking_fee_yuan + item.parking_fee_yuan,
            matched_mobile_count: acc.matched_mobile_count + item.matched_mobile_count,
            matched_member_count: acc.matched_member_count + item.matched_member_count,
            trade_count: acc.trade_count + item.trade_count,
            trade_amount_yuan: acc.trade_amount_yuan + item.trade_amount_yuan,
            trade_discount_yuan: acc.trade_discount_yuan + item.trade_discount_yuan,
            point_flow_count: acc.point_flow_count + item.point_flow_count,
            consume_amount_yuan: acc.consume_amount_yuan + item.consume_amount_yuan,
            positive_points: acc.positive_points + item.positive_points,
            negative_points: acc.negative_points + item.negative_points,
        }),
        {
            date: label,
            parking_count: 0,
            parking_fee_yuan: 0,
            matched_mobile_count: 0,
            matched_member_count: 0,
            trade_count: 0,
            trade_amount_yuan: 0,
            trade_discount_yuan: 0,
            point_flow_count: 0,
            consume_amount_yuan: 0,
            positive_points: 0,
            negative_points: 0,
        },
    );

    return [summary];
}

export default function BiPage({ onLogout }: BiPageProps) {
    const [mode, setMode] = useState<BiMode>("daily");
    const [activeCategory, setActiveCategory] = useState<BiCategory>("policy");
    const [startDate, setStartDate] = useState(yesterdayString());
    const [endDate, setEndDate] = useState(yesterdayString());
    const [dashboard, setDashboard] = useState<BiDashboardResponse | null>(null);
    const [loading, setLoading] = useState(false);
    const [hasQueried, setHasQueried] = useState(false);
    const [queriedCategory, setQueriedCategory] = useState<BiCategory | null>(null);
    const [categoryNeedsQuery, setCategoryNeedsQuery] = useState(false);
    const [error, setError] = useState("");

    async function loadDashboard(nextStartDate: string, nextEndDate: string, nextMode: BiMode, nextCategory: BiCategory) {
        setLoading(true);
        setHasQueried(true);
        setError("");
        try {
            const response = await getBiDashboard({
                startDate: nextStartDate,
                endDate: nextEndDate,
                mode: nextMode,
                category: nextCategory,
            });
            setDashboard(response);
            return true;
        } catch (requestError) {
            if (isUnauthorizedError(requestError)) {
                await onLogout();
                return false;
            }
            setError(`BI 数据加载失败：${getApiErrorMessage(requestError)}`);
            return false;
        } finally {
            setLoading(false);
        }
    }

    async function handleQuery(nextMode = mode, nextStartDate = startDate, nextEndDate = endDate) {
        const normalizedStart = nextStartDate <= nextEndDate ? nextStartDate : nextEndDate;
        const normalizedEnd = nextStartDate <= nextEndDate ? nextEndDate : nextStartDate;
        setStartDate(normalizedStart);
        setEndDate(normalizedEnd);
        setMode(nextMode);
        const succeeded = await loadDashboard(normalizedStart, normalizedEnd, nextMode, activeCategory);
        if (succeeded) {
            setQueriedCategory(activeCategory);
            setCategoryNeedsQuery(false);
        }
    }

    function handleCategoryChange(nextCategory: BiCategory) {
        if (nextCategory === activeCategory) {
            return;
        }
        setActiveCategory(nextCategory);
        setCategoryNeedsQuery(true);
        setError("");
    }

    const summaryCards = useMemo(() => {
        if (!dashboard) {
            return [];
        }

        return [
            {
                label: "停车记录",
                value: formatInteger(dashboard.summary.parking.parking_count || 0),
                note: `车牌 ${formatInteger(dashboard.summary.parking.plate_count || 0)} / 手机 ${formatInteger(dashboard.summary.parking.mobile_count || 0)}`,
            },
            {
                label: "停车交易",
                value: formatMoney(dashboard.summary.trade.actual_value_yuan || 0),
                note: `交易笔数 ${formatInteger(dashboard.summary.trade.trade_count || 0)}`,
            },
            {
                label: "积分流水",
                value: formatMoney(dashboard.summary.point_flow.consume_amount_yuan || 0),
                note: `流水 ${formatInteger(dashboard.summary.point_flow.flow_count || 0)} 笔`,
            },
            {
                label: "拉通手机号",
                value: formatInteger(dashboard.summary.linked_mobile_count),
                note: `拉通会员 ${formatInteger(dashboard.summary.linked_member_count)}`,
            },
        ];
    }, [dashboard]);

    const dailySeries = dashboard?.daily_series || [];
    const appliedMode = dashboard?.mode || mode;
    const hasPendingFilters = Boolean(
        categoryNeedsQuery ||
            (dashboard &&
                (dashboard.mode !== mode ||
                    dashboard.period.start_date !== startDate ||
                    dashboard.period.end_date !== endDate)),
    );
    const chartSeries =
        appliedMode === "range" && dashboard
            ? buildRangeSummary(dailySeries, `${dashboard.period.start_date} 至 ${dashboard.period.end_date}`)
            : dailySeries;

    const plazaRanking = dashboard?.plaza_ranking || [];
    const levelDistribution = dashboard?.level_distribution || [];
    const durationBuckets = dashboard?.parking_duration_buckets || [];
    const hourlyDistribution = dashboard?.hourly_distribution || [];
    const channelDistribution = dashboard?.payment_channel_distribution || [];
    const businessDistribution = dashboard?.trade_business_distribution || [];
    const passengerAnalysis = dashboard?.passenger_analysis;
    const passengerPeriodLabel = passengerAnalysis?.period_label || "";
    const passengerRuiyin = passengerAnalysis?.ruiyin;
    const funnelData = dashboard?.linkage_funnel || [];
    const validationMetrics = dashboard?.validation_metrics || [];
    const policyImpact = dashboard?.policy_impact;
    const policyImpactSummary = policyImpact?.summary;
    const policyImpactDaily = policyImpact?.daily || [];
    const policyImpactByLevel = policyImpact?.by_member_level || [];
    const crossDaySummary = policyImpact?.cross_day_summary;
    const durationShift = dashboard?.duration_shift;
    const durationShiftSummary = durationShift?.summary || [];
    const durationShiftDistributions = durationShift?.distributions || [];
    const pointsLeverage = dashboard?.points_leverage;
    const pointsLeverageSummary = pointsLeverage?.summary;
    const pointsLeverageFunnel = pointsLeverage?.funnel || [];
    const pointsLeverageByLevel = pointsLeverage?.by_member_level || [];

    const policyImpactCards = useMemo(() => {
        if (!policyImpactSummary) {
            return [];
        }
        return [
            {
                label: "新规应收",
                value: formatMoney(policyImpactSummary.actual_receivable_yuan),
                note: `旧规模拟 ${formatMoney(policyImpactSummary.simulated_old_policy_yuan)}`,
            },
            {
                label: "增收差额",
                value: formatMoney(policyImpactSummary.receivable_uplift_yuan),
                note: `记录实收 ${formatMoney(policyImpactSummary.recorded_fee_yuan)}`,
            },
            {
                label: "零元转付费",
                value: formatInteger(policyImpactSummary.zero_to_paid_count),
                note: `占比 ${formatPercent(policyImpactSummary.zero_to_paid_rate_pct)}`,
            },
            {
                label: "跨天停车",
                value: formatInteger(policyImpactSummary.cross_day_count),
                note: `占比 ${formatPercent(policyImpactSummary.cross_day_rate_pct)}`,
            },
        ];
    }, [policyImpactSummary]);

    const durationShiftCharts = useMemo(() => {
        return durationShiftSummary.map((summaryItem) => ({
            memberLevel: summaryItem.member_level,
            beforeCount: summaryItem.before_count,
            afterCount: summaryItem.after_count,
            beforeAvgHours: summaryItem.before_avg_hours,
            afterAvgHours: summaryItem.after_avg_hours,
            beforeAvgReceivable: summaryItem.before_avg_receivable_yuan,
            afterAvgReceivable: summaryItem.after_avg_receivable_yuan,
            rows: durationShiftDistributions.filter((item) => item.member_level === summaryItem.member_level),
        }));
    }, [durationShiftDistributions, durationShiftSummary]);

    const pointsLeverageCards = useMemo(() => {
        if (!pointsLeverageSummary) {
            return [];
        }
        return [
            {
                label: "总停车次数",
                value: formatInteger(pointsLeverageSummary.total_parking_count),
                note: `产生积分 ${formatInteger(pointsLeverageSummary.point_earned_count)}`,
            },
            {
                label: "触发 +2h",
                value: formatInteger(pointsLeverageSummary.point_bonus_triggered_count),
                note: `触发率 ${formatPercent(pointsLeverageSummary.point_bonus_trigger_rate_pct)}`,
            },
            {
                label: "积分转化率",
                value: formatPercent(pointsLeverageSummary.leverage_conversion_rate_pct),
                note: `积分停车占比 ${formatPercent(pointsLeverageSummary.point_earned_rate_pct)}`,
            },
            {
                label: "释放减免金额",
                value: formatMoney(pointsLeverageSummary.point_bonus_saved_yuan),
                note: "仅统计新规样本",
            },
        ];
    }, [pointsLeverageSummary]);

    const noData =
        hasQueried &&
        !loading &&
        dashboard &&
        dailySeries.length === 0 &&
        plazaRanking.length === 0 &&
        levelDistribution.length === 0 &&
        durationBuckets.length === 0 &&
        hourlyDistribution.length === 0 &&
        policyImpactDaily.length === 0 &&
        pointsLeverageFunnel.length === 0;
    const canShowCurrentCategory = Boolean(dashboard && queriedCategory === activeCategory && !categoryNeedsQuery);
    const queryHintMessage = !hasQueried
        ? "请先设置日期范围、统计方式和分类，再点击“查询”加载当前分类数据。"
        : categoryNeedsQuery
          ? `已切换到“${CATEGORY_META[activeCategory].shortLabel}”，请点击“查询”后再显示该分类图表。`
          : "";

    return (
        <div className="space-y-5">
            <Section
                title="BI 看板"
                subtitle="聚合停车、停车交易、消费积分与会员等级，验证购物中心与停车会员系统的数据拉通情况。"
            >
                <div className="mb-5 flex flex-wrap gap-3">
                    {(Object.entries(CATEGORY_META) as Array<[BiCategory, (typeof CATEGORY_META)[BiCategory]]>).map(([key, meta]) => (
                        <button
                            key={key}
                            className={[
                                "inline-flex h-12 min-w-[172px] items-center justify-center rounded-[1.15rem] border px-4 text-center transition",
                                activeCategory === key
                                    ? "border-slate-950 bg-slate-950 text-white shadow-[0_16px_30px_rgba(15,23,42,0.16)]"
                                    : "border-white/80 bg-white/92 text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)] hover:text-slate-950",
                            ].join(" ")}
                            onClick={() => handleCategoryChange(key)}
                            type="button"
                        >
                            <span className="text-sm font-semibold tracking-[-0.02em]">{meta.label}</span>
                        </button>
                    ))}
                </div>

                <div className="flex flex-wrap items-end gap-3">
                    <div className="min-w-[168px]">
                        <div className="mb-1 text-sm font-medium text-slate-600">开始日期</div>
                        <input
                            className="h-11 w-full rounded-[1rem] border border-white/80 bg-white/90 px-4 text-sm text-slate-900 shadow-[0_10px_20px_rgba(15,23,42,0.04)] outline-none"
                            onChange={(event) => setStartDate(event.target.value)}
                            type="date"
                            value={startDate}
                        />
                    </div>
                    <div className="min-w-[168px]">
                        <div className="mb-1 text-sm font-medium text-slate-600">结束日期</div>
                        <input
                            className="h-11 w-full rounded-[1rem] border border-white/80 bg-white/90 px-4 text-sm text-slate-900 shadow-[0_10px_20px_rgba(15,23,42,0.04)] outline-none"
                            onChange={(event) => setEndDate(event.target.value)}
                            type="date"
                            value={endDate}
                        />
                    </div>

                    <button
                        className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/90 px-4 text-sm font-medium text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)] transition hover:text-slate-950"
                        onClick={() => {
                            setStartDate(yesterdayString());
                            setEndDate(yesterdayString());
                        }}
                        type="button"
                    >
                        昨日
                    </button>
                    <button
                        className="inline-flex h-11 items-center justify-center rounded-[1rem] border border-white/80 bg-white/90 px-4 text-sm font-medium text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)] transition hover:text-slate-950"
                        onClick={() => {
                            setStartDate(dateBefore(7));
                            setEndDate(yesterdayString());
                        }}
                        type="button"
                    >
                        近7日
                    </button>
                    <button
                        className="inline-flex h-11 items-center justify-center rounded-[1rem] bg-[linear-gradient(135deg,#2563eb,#1d4ed8)] px-5 text-sm font-medium text-white shadow-[0_16px_30px_rgba(37,99,235,0.22)]"
                        onClick={() => void handleQuery(mode, startDate, endDate)}
                        type="button"
                    >
                        {loading ? "查询中..." : "查询"}
                    </button>

                    <div className="ml-auto flex flex-wrap gap-2">
                        <button
                            className={[
                                "inline-flex h-11 items-center justify-center rounded-[1rem] px-4 text-sm font-medium transition",
                                mode === "daily"
                                    ? "bg-slate-950 text-white shadow-[0_16px_30px_rgba(15,23,42,0.16)]"
                                    : "border border-white/80 bg-white/90 text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)]",
                            ].join(" ")}
                            onClick={() => setMode("daily")}
                            type="button"
                        >
                            按日统计
                        </button>
                        <button
                            className={[
                                "inline-flex h-11 items-center justify-center rounded-[1rem] px-4 text-sm font-medium transition",
                                mode === "range"
                                    ? "bg-slate-950 text-white shadow-[0_16px_30px_rgba(15,23,42,0.16)]"
                                    : "border border-white/80 bg-white/90 text-slate-700 shadow-[0_10px_20px_rgba(15,23,42,0.04)]",
                            ].join(" ")}
                            onClick={() => setMode("range")}
                            type="button"
                        >
                            按时段统计
                        </button>
                    </div>
                </div>
            </Section>

            {error ? (
                <div className="rounded-[1.4rem] border border-rose-200 bg-[linear-gradient(135deg,rgba(255,245,247,0.98),rgba(255,241,244,0.92))] px-5 py-4 text-sm text-rose-700">
                    {error}
                </div>
            ) : null}

            {hasPendingFilters ? (
                <div className="rounded-[1.4rem] border border-sky-200 bg-[linear-gradient(135deg,rgba(239,246,255,0.98),rgba(243,248,255,0.92))] px-5 py-4 text-sm text-sky-700">
                    日期范围、统计方式或当前分类已调整，点击“查询”后再刷新当前分类内容。
                </div>
            ) : null}

            {!loading && !error && queryHintMessage ? (
                <EmptyState message={queryHintMessage} />
            ) : null}

            {canShowCurrentCategory ? (
                <>
                    {activeCategory === "policy" ? (
                        <>
                            <CategoryHeader
                                title={CATEGORY_META.policy.label}
                                tone={CATEGORY_META.policy.tone}
                            />

                            <Section
                        title="政策性增收对比"
                        subtitle="仅针对新规生效后的停车记录，对比实际新规应收与旧规则模拟应收，并单独揭示跨天停车的自然日拆分影响。"
                    >
                        {!policyImpactSummary || policyImpactSummary.parking_count === 0 ? (
                            <EmptyState message="当前时间范围没有可展示的政策影响样本。" />
                        ) : (
                            <div className="space-y-5">
                                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                                    {policyImpactCards.map((item) => (
                                        <StatCard key={item.label} label={item.label} value={item.value} note={item.note} />
                                    ))}
                                </div>

                                <div className="grid gap-4 xl:grid-cols-[1.2fr,0.8fr]">
                                    <div className="rounded-[1.2rem] border border-white/80 bg-white/88 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                                        {policyImpactDaily.length === 0 ? (
                                            <EmptyState message="当前时间范围没有按日政策影响数据。" />
                                        ) : (
                                            <div className="h-[340px] w-full">
                                                <ResponsiveContainer>
                                                    <ComposedChart data={policyImpactDaily}>
                                                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                                        <XAxis dataKey="date" stroke="#64748b" tickFormatter={formatDateAxisLabel} />
                                                        <YAxis yAxisId="left" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                                        <YAxis yAxisId="right" orientation="right" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                                        <Tooltip formatter={tooltipFormatter} labelFormatter={(label) => `日期：${label}`} />
                                                        <Legend formatter={legendFormatter} />
                                                        <Bar
                                                            yAxisId="left"
                                                            dataKey="simulated_old_policy_yuan"
                                                            name="旧规模拟应收（元）"
                                                            fill="#94a3b8"
                                                            radius={[6, 6, 0, 0]}
                                                        />
                                                        <Bar
                                                            yAxisId="left"
                                                            dataKey="actual_receivable_yuan"
                                                            name="新规应收（元）"
                                                            fill="#2563eb"
                                                            radius={[6, 6, 0, 0]}
                                                        />
                                                        <Line
                                                            yAxisId="right"
                                                            type="monotone"
                                                            dataKey="receivable_uplift_yuan"
                                                            name="增收差额（元）"
                                                            stroke="#f97316"
                                                            strokeWidth={2.4}
                                                            dot={false}
                                                        />
                                                    </ComposedChart>
                                                </ResponsiveContainer>
                                            </div>
                                        )}
                                    </div>

                                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
                                        <StatCard
                                            label="实收增长率"
                                            value={formatPercent(policyImpactSummary.realized_growth_rate_pct)}
                                            note="记录实收对比旧规模拟应收"
                                        />
                                        <StatCard
                                            label="积分释放金额"
                                            value={formatMoney(policyImpactSummary.point_bonus_saved_yuan)}
                                            note="新规样本中因积分 +2 小时减少的应收"
                                        />
                                        <StatCard
                                            label="跨天平均计费时长"
                                            value={formatFlexibleDecimal(crossDaySummary?.avg_cross_day_billed_hours || 0)}
                                            note={`平均跨 ${formatFlexibleDecimal(crossDaySummary?.avg_cross_day_day_count || 0)} 天`}
                                        />
                                        <StatCard
                                            label="跨天拆分影响"
                                            value={formatMoney(crossDaySummary?.cross_day_refinement_delta_yuan || 0)}
                                            note="自然日拆分后相对整单单次封顶的差额"
                                        />
                                    </div>
                                </div>

                                <div className="rounded-[1.2rem] border border-white/80 bg-white/88 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                                    {policyImpactByLevel.length === 0 ? (
                                        <EmptyState message="当前时间范围没有会员等级层面的政策影响数据。" />
                                    ) : (
                                        <div className="h-[360px] w-full">
                                            <ResponsiveContainer>
                                                <BarChart data={policyImpactByLevel}>
                                                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                                    <XAxis dataKey="member_level" stroke="#64748b" />
                                                    <YAxis yAxisId="left" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                                    <YAxis yAxisId="right" orientation="right" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                                    <Tooltip formatter={tooltipFormatter} />
                                                    <Legend formatter={legendFormatter} />
                                                    <Bar yAxisId="left" dataKey="actual_receivable_yuan" name="新规应收（元）" fill="#2563eb" radius={[6, 6, 0, 0]} />
                                                    <Bar yAxisId="left" dataKey="simulated_old_policy_yuan" name="旧规模拟应收（元）" fill="#94a3b8" radius={[6, 6, 0, 0]} />
                                                    <Line
                                                        yAxisId="right"
                                                        type="monotone"
                                                        dataKey="zero_to_paid_count"
                                                        name="零元转付费"
                                                        stroke="#f97316"
                                                        strokeWidth={2.4}
                                                    />
                                                </BarChart>
                                            </ResponsiveContainer>
                                        </div>
                                    )}
                                </div>
                            </div>
                        )}
                    </Section>

            <Section
                title="会员等级时长分布偏移"
                subtitle="对比 2026-03-03 前后普卡与银卡的停车时长分桶结构，评估权益收紧后是否出现明显停留时长收缩。"
            >
                {durationShiftCharts.length === 0 || durationShiftCharts.every((item) => item.beforeCount === 0 && item.afterCount === 0) ? (
                    <EmptyState message="当前时间范围没有可展示的时长偏移样本。" />
                ) : (
                    <div className="grid gap-4 xl:grid-cols-2">
                        {durationShiftCharts.map((item) => (
                            <div
                                key={item.memberLevel}
                                className="rounded-[1.2rem] border border-white/80 bg-white/88 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]"
                            >
                                <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
                                    <div>
                                        <div className="text-sm font-semibold text-slate-950">{item.memberLevel}</div>
                                        <div className="mt-1 text-sm text-slate-500">
                                            变更前均时 {formatFlexibleDecimal(item.beforeAvgHours)} 小时 / 变更后均时 {formatFlexibleDecimal(item.afterAvgHours)} 小时
                                        </div>
                                    </div>
                                    <div className="text-right text-xs text-slate-500">
                                        变更前样本 {formatInteger(item.beforeCount)} 笔
                                        <br />
                                        变更后样本 {formatInteger(item.afterCount)} 笔
                                    </div>
                                </div>
                                {item.rows.length === 0 ? (
                                    <EmptyState message="该等级当前时间范围没有分布数据。" />
                                ) : (
                                    <div className="h-[300px] w-full">
                                        <ResponsiveContainer>
                                            <BarChart data={item.rows}>
                                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                                <XAxis dataKey="duration_bucket" stroke="#64748b" />
                                                <YAxis stroke="#64748b" tickFormatter={(value) => `${formatFlexibleDecimal(Number(value))}%`} />
                                                <Tooltip
                                                    formatter={(value, name) => [
                                                        `${formatFlexibleDecimal(Number(value))}%`,
                                                        translateLabel(String(name)),
                                                    ]}
                                                />
                                                <Legend formatter={legendFormatter} />
                                                <Bar dataKey="before_share_pct" name="变更前占比（%）" fill="#94a3b8" radius={[6, 6, 0, 0]} />
                                                <Bar dataKey="after_share_pct" name="变更后占比（%）" fill="#2563eb" radius={[6, 6, 0, 0]} />
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                )}
            </Section>

                            <Section title="积分加成杠杆率" subtitle="仅统计新规样本，观察积分入账对 +2 小时停车减免的触发效果和会员消费联动效率。">
                {!pointsLeverageSummary || pointsLeverageSummary.total_parking_count === 0 ? (
                    <EmptyState message="当前时间范围没有可展示的积分杠杆样本。" />
                ) : (
                    <div className="space-y-5">
                        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                            {pointsLeverageCards.map((item) => (
                                <StatCard key={item.label} label={item.label} value={item.value} note={item.note} />
                            ))}
                        </div>

                        <div className="grid gap-4 xl:grid-cols-[0.9fr,1.1fr]">
                            <div className="rounded-[1.2rem] border border-white/80 bg-white/88 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                                {pointsLeverageFunnel.length === 0 ? (
                                    <EmptyState message="当前时间范围没有积分杠杆漏斗数据。" />
                                ) : (
                                    <div className="h-[320px] w-full">
                                        <ResponsiveContainer>
                                            <FunnelChart>
                                                <Tooltip formatter={tooltipFormatter} />
                                                <Funnel dataKey="value" data={pointsLeverageFunnel} isAnimationActive={false}>
                                                    {pointsLeverageFunnel.map((entry, index) => (
                                                        <Cell key={entry.name} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                                    ))}
                                                </Funnel>
                                            </FunnelChart>
                                        </ResponsiveContainer>
                                    </div>
                                )}
                            </div>

                            <div className="rounded-[1.2rem] border border-white/80 bg-white/88 p-4 shadow-[0_10px_22px_rgba(15,23,42,0.04)]">
                                {pointsLeverageByLevel.length === 0 ? (
                                    <EmptyState message="当前时间范围没有会员等级杠杆数据。" />
                                ) : (
                                    <div className="h-[320px] w-full">
                                        <ResponsiveContainer>
                                            <ComposedChart data={pointsLeverageByLevel}>
                                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                                <XAxis dataKey="member_level" stroke="#64748b" />
                                                <YAxis yAxisId="left" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                                <YAxis yAxisId="right" orientation="right" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                                <Tooltip formatter={tooltipFormatter} />
                                                <Legend formatter={legendFormatter} />
                                                <Bar yAxisId="left" dataKey="point_earned_count" name="产生积分停车次数" fill="#7c3aed" radius={[6, 6, 0, 0]} />
                                                <Bar
                                                    yAxisId="left"
                                                    dataKey="point_bonus_triggered_count"
                                                    name="享受+2小时优惠次数"
                                                    fill="#14b8a6"
                                                    radius={[6, 6, 0, 0]}
                                                />
                                                <Line
                                                    yAxisId="right"
                                                    type="monotone"
                                                    dataKey="point_bonus_saved_yuan"
                                                    name="积分加成释放金额（元）"
                                                    stroke="#f97316"
                                                    strokeWidth={2.4}
                                                    dot={false}
                                                />
                                            </ComposedChart>
                                        </ResponsiveContainer>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}
            </Section>
                        </>
                    ) : null}

                    {activeCategory === "regular" ? (
                        <>
                            <CategoryHeader
                                title={CATEGORY_META.regular.label}
                                tone={CATEGORY_META.regular.tone}
                            />

                            <Section
                                title="核心指标"
                                subtitle={`${dashboard?.period.start_date || startDate} 至 ${dashboard?.period.end_date || endDate}`}
                            >
                        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                            {summaryCards.map((item) => (
                                <StatCard key={item.label} label={item.label} value={item.value} note={item.note} />
                            ))}
                        </div>
                    </Section>

            <Section
                title={appliedMode === "daily" ? "按日趋势总览" : "时段汇总总览"}
                subtitle="统一观察停车记录、停车交易、消费金额与积分变化。"
            >
                {chartSeries.length === 0 ? (
                    <EmptyState message="当前时间范围没有可展示的数据。" />
                ) : (
                    <div className="h-[360px] w-full">
                        <ResponsiveContainer>
                            <ComposedChart data={chartSeries}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                <XAxis dataKey="date" stroke="#64748b" tickFormatter={formatDateAxisLabel} />
                                <YAxis yAxisId="left" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                <YAxis yAxisId="right" orientation="right" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                <Tooltip formatter={tooltipFormatter} labelFormatter={(label) => `日期：${label}`} />
                                <Legend formatter={legendFormatter} />
                                <Bar yAxisId="left" dataKey="parking_count" name="停车记录" fill="#2563eb" radius={[6, 6, 0, 0]} />
                                <Bar yAxisId="left" dataKey="trade_count" name="停车交易" fill="#14b8a6" radius={[6, 6, 0, 0]} />
                                <Line yAxisId="left" type="monotone" dataKey="point_flow_count" name="积分流水" stroke="#7c3aed" strokeWidth={2.2} dot={false} />
                                <Area yAxisId="right" type="monotone" dataKey="trade_amount_yuan" name="停车交易金额(元)" fill="rgba(249,115,22,0.18)" stroke="#f97316" />
                                <Area yAxisId="right" type="monotone" dataKey="consume_amount_yuan" name="消费金额(元)" fill="rgba(124,58,237,0.14)" stroke="#7c3aed" />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="系统拉通漏斗" subtitle="验证停车、交易、积分三域在手机号和会员层面的贯通程度。">
                {funnelData.length === 0 ? (
                    <EmptyState message="暂无漏斗数据。" />
                ) : (
                    <div className="h-[360px] w-full">
                        <ResponsiveContainer>
                            <FunnelChart>
                                <Tooltip formatter={tooltipFormatter} />
                                <Funnel dataKey="value" data={funnelData} isAnimationActive={false}>
                                    {funnelData.map((entry, index) => (
                                        <Cell key={entry.name} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                    ))}
                                </Funnel>
                            </FunnelChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="购物中心联动排行" subtitle="以购物中心或停车场名称为维度，对比停车流量、交易金额和积分消费贡献。">
                {plazaRanking.length === 0 ? (
                    <EmptyState message="当前时间范围没有购物中心联动数据。" />
                ) : (
                    <div className="h-[420px] w-full">
                        <ResponsiveContainer>
                            <BarChart data={plazaRanking} layout="vertical" margin={{ left: 32, right: 16 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                <XAxis type="number" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                <YAxis dataKey="plaza_name" type="category" width={180} stroke="#64748b" />
                                <Tooltip formatter={tooltipFormatter} />
                                <Legend formatter={legendFormatter} />
                                <Bar dataKey="parking_count" name="停车记录" fill="#2563eb" radius={[0, 6, 6, 0]} />
                                <Bar dataKey="trade_count" name="停车交易" fill="#14b8a6" radius={[0, 6, 6, 0]} />
                                <Bar dataKey="point_flow_count" name="积分流水" fill="#7c3aed" radius={[0, 6, 6, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="会员等级分布" subtitle="统计在选定时间范围内出现在停车、交易、积分数据中的会员当前等级结构。">
                {levelDistribution.length === 0 ? (
                    <EmptyState message="当前时间范围内暂无可统计的会员等级数据。" />
                ) : (
                    <div className="h-[360px] w-full">
                        <ResponsiveContainer>
                            <BarChart data={levelDistribution}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                <XAxis dataKey="level_name" stroke="#64748b" />
                                <YAxis stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                <Tooltip formatter={tooltipFormatter} />
                                <Legend formatter={legendFormatter} />
                                <Bar dataKey="parking_members" stackId="a" name="停车会员" fill="#2563eb" />
                                <Bar dataKey="trade_members" stackId="a" name="交易会员" fill="#14b8a6" />
                                <Bar dataKey="point_members" stackId="a" name="积分会员" fill="#7c3aed" />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="停车时长分布" subtitle="验证停车记录是否覆盖正常的停车时长结构。">
                {durationBuckets.length === 0 ? (
                    <EmptyState message="当前时间范围没有停车时长数据。" />
                ) : (
                    <div className="h-[320px] w-full">
                        <ResponsiveContainer>
                            <BarChart data={durationBuckets}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                <XAxis dataKey="bucket" stroke="#64748b" />
                                <YAxis stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                <Tooltip formatter={tooltipFormatter} />
                                <Bar dataKey="count" name="记录数" fill="#0f766e" radius={[8, 8, 0, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="时段热力分布" subtitle="展示一天 24 小时内停车、交易、积分记录的活跃程度。">
                {hourlyDistribution.length === 0 ? <EmptyState message="暂无时段数据。" /> : <Heatmap rows={hourlyDistribution} />}
            </Section>

            <Section title="支付渠道分布" subtitle="验证停车交易渠道构成与金额结构。">
                {channelDistribution.length === 0 ? (
                    <EmptyState message="当前时间范围没有交易渠道数据。" />
                ) : (
                    <div className="h-[360px] w-full">
                        <ResponsiveContainer>
                            <PieChart>
                                <Tooltip formatter={tooltipFormatter} />
                                <Legend formatter={legendFormatter} />
                                <Pie data={channelDistribution} dataKey="count" nameKey="name" innerRadius={70} outerRadius={120} paddingAngle={2}>
                                    {channelDistribution.map((item, index) => (
                                        <Cell key={item.name} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                                    ))}
                                </Pie>
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

            <Section title="交易业务类型分布" subtitle="展示停车交易业务类型的笔数和金额贡献。">
                {businessDistribution.length === 0 ? (
                    <EmptyState message="当前时间范围没有业务类型数据。" />
                ) : (
                    <div className="h-[360px] w-full">
                        <ResponsiveContainer>
                            <ComposedChart data={businessDistribution}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                <XAxis dataKey="name" stroke="#64748b" tickFormatter={translateLabel} />
                                <YAxis yAxisId="left" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "count")} />
                                <YAxis yAxisId="right" orientation="right" stroke="#64748b" tickFormatter={(value) => formatChartValue(value, "money")} />
                                <Tooltip formatter={tooltipFormatter} />
                                <Legend formatter={legendFormatter} />
                                <Bar yAxisId="left" dataKey="count" name="笔数" fill="#2563eb" radius={[6, 6, 0, 0]} />
                                <Area yAxisId="right" type="monotone" dataKey="amount_yuan" name="金额(元)" fill="rgba(249,115,22,0.18)" stroke="#f97316" />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </Section>

                            <Section title="数据验证明细" subtitle="把关键贯通率和金额校验直接列出来，方便人工核对。">
                <div className="overflow-hidden rounded-[1.35rem] border border-slate-200/90 bg-white/88">
                    <div className="grid grid-cols-[220px_160px_1fr] gap-3 border-b border-slate-200/90 px-4 py-3 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">
                        <div>指标</div>
                        <div>结果</div>
                        <div>说明</div>
                    </div>
                    <div className="max-h-[26rem] overflow-y-auto">
                        {validationMetrics.map((item) => (
                            <div
                                key={item.metric}
                                className="grid grid-cols-[220px_160px_1fr] gap-3 border-b border-slate-100/90 px-4 py-3 text-sm last:border-b-0"
                            >
                                <div className="font-medium text-slate-900">{item.metric}</div>
                                <div className="text-slate-700">{formatMetricText(item.value)}</div>
                                <div className="text-slate-500">{item.description}</div>
                            </div>
                        ))}
                    </div>
                </div>
            </Section>
                        </>
                    ) : null}

                    {activeCategory === "passenger" ? (
                        <>
                            <CategoryHeader
                                title={CATEGORY_META.passenger.label}
                                tone={CATEGORY_META.passenger.tone}
                            />

                            <Section title="客流趋势同比">
                                <div className="space-y-5">
                                    {passengerRuiyin ? (
                                        <PassengerTrendChart block={passengerRuiyin} periodLabel={passengerPeriodLabel} />
                                    ) : (
                                        <EmptyState message="当前范围暂无睿印客流趋势数据。" />
                                    )}
                                </div>
                            </Section>
                        </>
                    ) : null}

                    {activeCategory === "exception" ? (
                        <>
                            <CategoryHeader
                                title={CATEGORY_META.exception.label}
                                tone={CATEGORY_META.exception.tone}
                            />

                            <Section title="待新增内容" subtitle="该分类当前留空，后续在这里补充异常规则、异常趋势和异常样本明细。">
                                <EmptyState message="异常分析模块暂未启用。" />
                            </Section>
                        </>
                    ) : null}
                </>
            ) : null}

            {loading && !canShowCurrentCategory ? <EmptyState message="正在加载当前分类数据..." /> : null}
            {!loading && canShowCurrentCategory && activeCategory !== "passenger" && activeCategory !== "exception" && noData ? (
                <EmptyState message="当前筛选条件下没有可展示的数据。" />
            ) : null}
        </div>
    );
}
