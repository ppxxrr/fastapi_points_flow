from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal
import math
from typing import Any, Iterable

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models.legacy_flow import TrafficNodeDailyFlow
from app.models.member import MemberAccount, MemberProfile
from app.models.parking import ParkingRecord
from app.models.parking_policy import ParkingPolicyDim
from app.models.parking_trade import ParkingTradeRecord
from app.models.point_flow import MemberPointFlow
from app.services.parking_trade_service import extract_plate_no


TRADE_BUSINESS_LABELS = {
    "car:parking:cashier": "停车收费",
    "car:parking:refund": "停车退款",
    "car:parking:prepay": "停车预付",
    "car:parking:recharge": "停车充值",
}

PARKING_RULE_V1 = "PARKING_RULE_V1"
PARKING_RULE_V2 = "PARKING_RULE_V2"
POLICY_CHANGE_DATETIME = datetime(2026, 3, 3, 0, 0, 0)
IMPACT_DURATION_BUCKETS = (
    (1, "1小时内"),
    (2, "1-2小时"),
    (4, "2-4小时"),
    (8, "4-8小时"),
    (999999, "8小时以上"),
)
BEHAVIOR_SHIFT_LEVELS = ("普卡", "银卡")

FALLBACK_POLICY_RULES = {
    PARKING_RULE_V1: {
        "非会员": {"base_free_hours": 0, "is_diamond_full_free": False},
        "普卡": {"base_free_hours": 3, "is_diamond_full_free": False},
        "银卡": {"base_free_hours": 3, "is_diamond_full_free": False},
        "金卡": {"base_free_hours": 3, "is_diamond_full_free": False},
        "钻石卡": {"base_free_hours": 0, "is_diamond_full_free": True},
    },
    PARKING_RULE_V2: {
        "非会员": {"base_free_hours": 0, "is_diamond_full_free": False},
        "普卡": {"base_free_hours": 1, "is_diamond_full_free": False},
        "银卡": {"base_free_hours": 2, "is_diamond_full_free": False},
        "金卡": {"base_free_hours": 3, "is_diamond_full_free": False},
        "钻石卡": {"base_free_hours": 0, "is_diamond_full_free": True},
    },
}


@dataclass(frozen=True)
class BiDateRange:
    start_date: date
    end_date: date

    @property
    def start_datetime(self) -> datetime:
        return datetime.combine(self.start_date, dt_time.min)

    @property
    def end_datetime(self) -> datetime:
        return datetime.combine(self.end_date, dt_time.max.replace(microsecond=0))

    def iter_dates(self) -> list[date]:
        current = self.start_date
        values: list[date] = []
        while current <= self.end_date:
            values.append(current)
            current += timedelta(days=1)
        return values


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _clean_key(value: Any, fallback: str = "未标注") -> str:
    text = str(value or "").strip()
    return text or fallback


def _normalize_identifier(value: Any) -> str:
    text = str(value or "").strip()
    return text.upper() if text else ""


def _normalize_member_level(level_name: Any, member_id: Any = None) -> str:
    text = str(level_name or "").strip()
    if not text:
        return "普卡" if str(member_id or "").strip() else "非会员"
    if any(keyword in text for keyword in ("钻石", "黑钻", "钻卡")):
        return "钻石卡"
    if any(keyword in text for keyword in ("金卡", "黄金")):
        return "金卡"
    if any(keyword in text for keyword in ("银卡", "白银")):
        return "银卡"
    if any(keyword in text for keyword in ("普卡", "普通")):
        return "普卡"
    return "普卡" if str(member_id or "").strip() else "非会员"


def _shift_date_to_previous_year(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


def _translate_trade_business_name(value: Any) -> str:
    key = str(value or "").strip()
    if not key:
        return "未标注"
    return TRADE_BUSINESS_LABELS.get(key, key)


def _is_invalid_point_flow_row(row: Any) -> bool:
    point_operate = str(getattr(row, "point_operate", "") or "").strip().upper()
    change_type_name = str(getattr(row, "change_type_name", "") or "").strip()
    return point_operate == "EXPENSE" or "无效" in change_type_name


def _batch_values(values: Iterable[str], size: int = 1000) -> Iterable[list[str]]:
    batch: list[str] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _resolve_duration_bucket(duration_hours: int) -> str:
    for threshold, label in IMPACT_DURATION_BUCKETS:
        if duration_hours <= threshold:
            return label
    return IMPACT_DURATION_BUCKETS[-1][1]


class BiAnalyticsService:
    def __init__(self, db: Session):
        self.db = db

    def build_dashboard(self, *, start_date: date, end_date: date, mode: str, category: str = "regular") -> dict[str, Any]:
        date_range = BiDateRange(start_date=start_date, end_date=end_date)
        payload = self._build_base_payload(start_date=start_date, end_date=end_date, mode=mode, category=category)

        if category == "exception":
            return payload

        if category == "passenger":
            payload["passenger_analysis"] = self._build_passenger_analysis(date_range)
            return payload

        parking_dt = func.coalesce(ParkingRecord.exit_time, ParkingRecord.entry_time)
        trade_dt = func.coalesce(
            ParkingTradeRecord.result_time,
            ParkingTradeRecord.pay_time,
            ParkingTradeRecord.create_time,
        )
        point_dt = func.coalesce(MemberPointFlow.consume_time, MemberPointFlow.create_time)

        parking_rows = self._load_parking_rows(date_range, parking_dt)
        point_rows = self._load_point_rows(date_range, point_dt)
        policy_context = self._load_policy_context()
        point_bonus_index = self._build_point_bonus_index(point_rows)
        parking_level_by_member = self._build_member_level_map_from_parking_rows(parking_rows)

        if category == "policy":
            policy_analysis = self._build_policy_impact_analysis(
                parking_rows=parking_rows,
                point_bonus_index=point_bonus_index,
                policy_context=policy_context,
            )
            payload["policy_impact"] = policy_analysis["policy_impact"]
            payload["duration_shift"] = self._build_duration_shift(policy_analysis["rows"])
            payload["points_leverage"] = self._build_points_leverage(policy_analysis["after_policy_rows"])
            return payload

        trade_rows = self._load_trade_rows(date_range, trade_dt)
        parking_summary, parking_mobiles, parking_member_ids, parking_stats = self._aggregate_parking(
            parking_rows,
            date_range,
        )
        trade_summary, trade_mobiles, trade_member_ids, trade_stats = self._aggregate_trade(
            trade_rows,
            parking_rows,
            date_range,
        )
        point_summary, point_mobiles, point_member_ids, point_stats = self._aggregate_point(point_rows, date_range)
        level_distribution = self._build_level_distribution(
            parking_member_ids=parking_member_ids,
            trade_member_ids=trade_member_ids,
            point_member_ids=point_member_ids,
            known_level_by_member=parking_level_by_member,
        )
        policy_analysis = self._build_policy_impact_analysis(
            parking_rows=parking_rows,
            point_bonus_index=point_bonus_index,
            policy_context=policy_context,
        )
        duration_shift = self._build_duration_shift(policy_analysis["rows"])
        points_leverage = self._build_points_leverage(policy_analysis["after_policy_rows"])
        daily_series = list(parking_stats["daily"].values())
        for item in daily_series:
            trade_item = trade_stats["daily"].get(item["date"])
            if trade_item:
                item["trade_count"] = trade_item["trade_count"]
                item["trade_amount_yuan"] = trade_item["trade_amount_yuan"]
                item["trade_discount_yuan"] = trade_item["trade_discount_yuan"]
            point_item = point_stats["daily"].get(item["date"])
            if point_item:
                item["point_flow_count"] = point_item["point_flow_count"]
                item["consume_amount_yuan"] = point_item["consume_amount_yuan"]
                item["positive_points"] = point_item["positive_points"]
                item["negative_points"] = point_item["negative_points"]

        plaza_ranking = self._merge_plaza_ranking(
            parking_stats["plaza"],
            trade_stats["plaza"],
            point_stats["plaza"],
        )

        payload.update(
            {
                "summary": {
                    "parking": parking_summary,
                    "trade": trade_summary,
                    "point_flow": point_summary,
                    "linked_mobile_count": len(parking_mobiles | trade_mobiles | point_mobiles),
                    "linked_member_count": len(parking_member_ids | trade_member_ids | point_member_ids),
                },
                "daily_series": daily_series,
                "plaza_ranking": plaza_ranking,
                "level_distribution": level_distribution,
                "parking_duration_buckets": parking_stats["duration_buckets"],
                "hourly_distribution": self._merge_hourly_distribution(
                    parking_stats["hourly"],
                    trade_stats["hourly"],
                    point_stats["hourly"],
                ),
                "payment_channel_distribution": trade_stats["channels"],
                "trade_business_distribution": trade_stats["businesses"],
                "linkage_funnel": self._build_linkage_funnel(
                    parking_summary=parking_summary,
                    parking_mobiles=parking_mobiles,
                    parking_member_ids=parking_member_ids,
                    trade_mobiles=trade_mobiles,
                    trade_member_ids=trade_member_ids,
                    point_mobiles=point_mobiles,
                    point_member_ids=point_member_ids,
                ),
                "validation_metrics": self._build_validation_metrics(
                    parking_summary=parking_summary,
                    trade_summary=trade_summary,
                    point_summary=point_summary,
                    parking_mobiles=parking_mobiles,
                    parking_member_ids=parking_member_ids,
                    trade_mobiles=trade_mobiles,
                    trade_member_ids=trade_member_ids,
                    point_mobiles=point_mobiles,
                    point_member_ids=point_member_ids,
                    policy_impact_summary=policy_analysis["policy_impact"]["summary"],
                    points_leverage_summary=points_leverage["summary"],
                ),
            }
        )
        return payload

    def _build_base_payload(self, *, start_date: date, end_date: date, mode: str, category: str) -> dict[str, Any]:
        return {
            "mode": mode,
            "category": category,
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "day_count": len(BiDateRange(start_date=start_date, end_date=end_date).iter_dates()),
            },
            "summary": {
                "parking": {},
                "trade": {},
                "point_flow": {},
                "linked_mobile_count": 0,
                "linked_member_count": 0,
            },
            "daily_series": [],
            "plaza_ranking": [],
            "level_distribution": [],
            "parking_duration_buckets": [],
            "hourly_distribution": [],
            "payment_channel_distribution": [],
            "trade_business_distribution": [],
            "passenger_analysis": None,
            "policy_impact": None,
            "duration_shift": None,
            "points_leverage": None,
            "linkage_funnel": [],
            "validation_metrics": [],
        }

    def _load_parking_rows(self, date_range: BiDateRange, parking_dt: Any) -> list[Any]:
        time_filter = or_(
            ParkingRecord.exit_time.between(date_range.start_datetime, date_range.end_datetime),
            and_(
                ParkingRecord.exit_time.is_(None),
                ParkingRecord.entry_time.between(date_range.start_datetime, date_range.end_datetime),
            ),
        )
        stmt = (
            select(
                parking_dt.label("biz_datetime"),
                ParkingRecord.entry_time,
                ParkingRecord.exit_time,
                ParkingRecord.plate_no,
                ParkingRecord.mobile_no,
                ParkingRecord.member_id,
                ParkingRecord.total_fee_cent,
                ParkingRecord.parking_duration_seconds,
                ParkingRecord.plaza_name,
                MemberAccount.level_name.label("member_level_name"),
            )
            .outerjoin(MemberAccount, MemberAccount.member_id == ParkingRecord.member_id)
            .where(time_filter)
            .execution_options(stream_results=True)
        )
        return list(self.db.execute(stmt))

    def _load_trade_rows(self, date_range: BiDateRange, trade_dt: Any) -> list[Any]:
        time_filter = or_(
            ParkingTradeRecord.result_time.between(date_range.start_datetime, date_range.end_datetime),
            and_(
                ParkingTradeRecord.result_time.is_(None),
                ParkingTradeRecord.pay_time.between(date_range.start_datetime, date_range.end_datetime),
            ),
            and_(
                ParkingTradeRecord.result_time.is_(None),
                ParkingTradeRecord.pay_time.is_(None),
                ParkingTradeRecord.create_time.between(date_range.start_datetime, date_range.end_datetime),
            ),
        )
        stmt = (
            select(
                trade_dt.label("biz_datetime"),
                ParkingTradeRecord.mobile_no,
                ParkingTradeRecord.plate_no,
                ParkingTradeRecord.subject,
                ParkingTradeRecord.body,
                ParkingTradeRecord.actual_value_cent,
                ParkingTradeRecord.discount_cent,
                ParkingTradeRecord.fee_cent,
                ParkingTradeRecord.channel_name,
                ParkingTradeRecord.business,
                ParkingTradeRecord.plaza_name,
            )
            .where(time_filter)
            .execution_options(stream_results=True)
        )
        return list(self.db.execute(stmt))

    def _load_point_rows(self, date_range: BiDateRange, point_dt: Any) -> list[Any]:
        time_filter = or_(
            MemberPointFlow.consume_time.between(date_range.start_datetime, date_range.end_datetime),
            and_(
                MemberPointFlow.consume_time.is_(None),
                MemberPointFlow.create_time.between(date_range.start_datetime, date_range.end_datetime),
            ),
        )
        stmt = (
            select(
                point_dt.label("biz_datetime"),
                MemberPointFlow.mobile_no,
                MemberPointFlow.member_id,
                MemberPointFlow.consume_amount,
                MemberPointFlow.signed_change_points,
                MemberPointFlow.plaza_name,
                MemberPointFlow.point_operate,
                MemberPointFlow.change_type_name,
            )
            .where(time_filter)
            .execution_options(stream_results=True)
        )
        return list(self.db.execute(stmt))

    def _load_policy_context(self) -> dict[str, Any]:
        rows = self.db.execute(
            select(
                ParkingPolicyDim.version_id,
                ParkingPolicyDim.start_date,
                ParkingPolicyDim.end_date,
                ParkingPolicyDim.member_level,
                ParkingPolicyDim.base_free_hours,
                ParkingPolicyDim.is_diamond_full_free,
            )
        ).all()

        rules: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        ranges: dict[str, dict[str, Any]] = {}
        for row in rows:
            version_id = str(row.version_id or "").strip()
            if not version_id:
                continue
            member_level = _normalize_member_level(row.member_level)
            rules[version_id][member_level] = {
                "base_free_hours": max(_to_int(row.base_free_hours), 0),
                "is_diamond_full_free": bool(row.is_diamond_full_free),
            }

            range_item = ranges.get(version_id)
            if range_item is None:
                ranges[version_id] = {"start_date": row.start_date, "end_date": row.end_date}
                continue
            if row.start_date and (range_item["start_date"] is None or row.start_date < range_item["start_date"]):
                range_item["start_date"] = row.start_date
            if range_item["end_date"] is None or row.end_date is None:
                range_item["end_date"] = None
            elif row.end_date > range_item["end_date"]:
                range_item["end_date"] = row.end_date

        default_ranges = {
            PARKING_RULE_V1: {
                "start_date": datetime(2000, 1, 1, 0, 0, 0),
                "end_date": POLICY_CHANGE_DATETIME - timedelta(seconds=1),
            },
            PARKING_RULE_V2: {
                "start_date": POLICY_CHANGE_DATETIME,
                "end_date": None,
            },
        }
        for version_id, fallback_rules in FALLBACK_POLICY_RULES.items():
            rules[version_id].update({key: value for key, value in fallback_rules.items() if key not in rules[version_id]})
            ranges.setdefault(version_id, default_ranges[version_id])

        return {"rules": dict(rules), "ranges": ranges}

    def _build_point_bonus_index(self, point_rows: list[Any]) -> dict[str, set[tuple[str, str]]]:
        member_keys: set[tuple[str, str]] = set()
        mobile_keys: set[tuple[str, str]] = set()
        for row in point_rows:
            if _to_float(row.signed_change_points) <= 0:
                continue
            biz_datetime = row.biz_datetime
            if biz_datetime is None:
                continue
            date_key = biz_datetime.date().isoformat()
            member_id = _clean_key(row.member_id, "")
            mobile_no = _clean_key(row.mobile_no, "")
            if member_id:
                member_keys.add((member_id, date_key))
            if mobile_no:
                mobile_keys.add((mobile_no, date_key))
        return {"member_keys": member_keys, "mobile_keys": mobile_keys}

    def _resolve_policy_version(self, exit_time: datetime, policy_context: dict[str, Any]) -> str:
        ranges = policy_context.get("ranges", {})
        for version_id, range_item in sorted(
            ranges.items(),
            key=lambda item: item[1]["start_date"] or datetime.min,
            reverse=True,
        ):
            start_date = range_item.get("start_date")
            end_date = range_item.get("end_date")
            if start_date and exit_time < start_date:
                continue
            if end_date and exit_time > end_date:
                continue
            return version_id
        return PARKING_RULE_V2 if exit_time >= POLICY_CHANGE_DATETIME else PARKING_RULE_V1

    def _resolve_policy_rule(
        self,
        version_id: str,
        member_level: str,
        policy_context: dict[str, Any],
    ) -> dict[str, Any]:
        rules = policy_context.get("rules", {})
        version_rules = rules.get(version_id) or FALLBACK_POLICY_RULES.get(version_id, {})
        return version_rules.get(member_level) or version_rules.get("非会员") or {
            "base_free_hours": 0,
            "is_diamond_full_free": False,
        }

    def _resolve_duration_seconds(self, row: Any) -> int:
        duration_seconds = max(_to_int(getattr(row, "parking_duration_seconds", None)), 0)
        if duration_seconds > 0:
            return duration_seconds
        entry_time = getattr(row, "entry_time", None)
        exit_time = getattr(row, "exit_time", None)
        if entry_time is None or exit_time is None:
            return 0
        return max(int((exit_time - entry_time).total_seconds()), 0)

    def _split_duration_seconds_by_day(
        self,
        *,
        entry_time: datetime | None,
        exit_time: datetime | None,
        duration_seconds: int,
    ) -> list[dict[str, Any]]:
        if duration_seconds <= 0:
            return []
        if entry_time is None or exit_time is None or exit_time <= entry_time:
            if exit_time is None:
                return []
            return [
                {
                    "date": exit_time.date().isoformat(),
                    "seconds": duration_seconds,
                    "billed_hours": int(math.ceil(duration_seconds / 3600)),
                }
            ]

        segments: list[dict[str, Any]] = []
        cursor = entry_time
        while cursor < exit_time:
            next_day = datetime.combine(cursor.date() + timedelta(days=1), dt_time.min)
            segment_end = min(exit_time, next_day)
            segment_seconds = max(int((segment_end - cursor).total_seconds()), 0)
            if segment_seconds > 0:
                segments.append(
                    {
                        "date": cursor.date().isoformat(),
                        "seconds": segment_seconds,
                        "billed_hours": int(math.ceil(segment_seconds / 3600)),
                    }
                )
            cursor = segment_end

        actual_total = sum(item["seconds"] for item in segments)
        delta_seconds = duration_seconds - actual_total
        if segments and delta_seconds != 0:
            segments[-1]["seconds"] = max(0, segments[-1]["seconds"] + delta_seconds)
            segments[-1]["billed_hours"] = int(math.ceil(segments[-1]["seconds"] / 3600)) if segments[-1]["seconds"] > 0 else 0
        return segments

    def _calculate_flat_policy_fee(self, billed_hours: int, total_free_hours: int, is_full_free: bool) -> float:
        if billed_hours <= 0 or is_full_free:
            return 0.0
        if total_free_hours >= 1:
            return round(min(60.0, max(0, billed_hours - total_free_hours) * 5.0), 2)
        return round(min(60.0, 10.0 + max(0, billed_hours - 1) * 5.0), 2)

    def _calculate_policy_fee(
        self,
        *,
        entry_time: datetime | None,
        exit_time: datetime | None,
        duration_seconds: int,
        total_free_hours: int,
        is_full_free: bool,
    ) -> dict[str, Any]:
        if is_full_free:
            return {
                "fee_yuan": 0.0,
                "billed_hours": int(math.ceil(duration_seconds / 3600)) if duration_seconds > 0 else 0,
                "is_cross_day": bool(entry_time and exit_time and entry_time.date() != exit_time.date()),
                "day_count": 1 if duration_seconds > 0 else 0,
                "charged_day_count": 0,
            }

        segments = self._split_duration_seconds_by_day(
            entry_time=entry_time,
            exit_time=exit_time,
            duration_seconds=duration_seconds,
        )
        if not segments:
            return {"fee_yuan": 0.0, "billed_hours": 0, "is_cross_day": False, "day_count": 0, "charged_day_count": 0}

        billed_hours = sum(item["billed_hours"] for item in segments)
        if len(segments) == 1:
            return {
                "fee_yuan": self._calculate_flat_policy_fee(billed_hours, total_free_hours, False),
                "billed_hours": billed_hours,
                "is_cross_day": False,
                "day_count": 1,
                "charged_day_count": 1 if billed_hours > total_free_hours else 0,
            }

        remaining_free_hours = max(total_free_hours, 0)
        first_paid_hour_pending = remaining_free_hours < 1
        total_fee = 0.0
        charged_day_count = 0

        for segment in segments:
            day_hours = segment["billed_hours"]
            free_applied = min(remaining_free_hours, day_hours)
            remaining_free_hours -= free_applied
            paid_hours = max(day_hours - free_applied, 0)
            if paid_hours <= 0:
                continue

            charged_day_count += 1
            if first_paid_hour_pending:
                day_fee = min(60.0, 10.0 + max(0, paid_hours - 1) * 5.0)
                first_paid_hour_pending = False
            else:
                day_fee = min(60.0, paid_hours * 5.0)
            total_fee += day_fee

        return {
            "fee_yuan": round(total_fee, 2),
            "billed_hours": billed_hours,
            "is_cross_day": True,
            "day_count": len(segments),
            "charged_day_count": charged_day_count,
        }

    def _build_passenger_analysis(self, date_range: BiDateRange) -> dict[str, Any]:
        previous_dates = [_shift_date_to_previous_year(item) for item in date_range.iter_dates()]
        previous_range = BiDateRange(start_date=previous_dates[0], end_date=previous_dates[-1])

        ruiyin_current = self._load_daily_flow_totals(
            model=TrafficNodeDailyFlow,
            date_column=TrafficNodeDailyFlow.business_date,
            value_column=TrafficNodeDailyFlow.passenger_flow,
            date_range=date_range,
        )
        ruiyin_previous = self._load_daily_flow_totals(
            model=TrafficNodeDailyFlow,
            date_column=TrafficNodeDailyFlow.business_date,
            value_column=TrafficNodeDailyFlow.passenger_flow,
            date_range=previous_range,
        )

        return {
            "period_label": self._format_passenger_period_label(date_range),
            "ruiyin": self._build_passenger_compare_block(
                title="睿印客流趋势对比",
                current_map=ruiyin_current,
                previous_map=ruiyin_previous,
                current_dates=date_range.iter_dates(),
                previous_dates=previous_dates,
            ),
        }

    def _load_daily_flow_totals(
        self,
        *,
        model: Any,
        date_column: Any,
        value_column: Any,
        date_range: BiDateRange,
        filters: tuple[Any, ...] = (),
    ) -> dict[date, int]:
        rows = self.db.execute(
            select(
                date_column,
                func.coalesce(func.sum(value_column), 0).label("total_value"),
            )
            .select_from(model)
            .where(
                date_column >= date_range.start_date,
                date_column <= date_range.end_date,
                *filters,
            )
            .group_by(date_column)
            .order_by(date_column.asc())
        ).all()
        return {row[0]: int(row[1] or 0) for row in rows if row[0] is not None}

    def _build_passenger_compare_block(
        self,
        *,
        title: str,
        current_map: dict[date, int],
        previous_map: dict[date, int],
        current_dates: list[date],
        previous_dates: list[date],
    ) -> dict[str, Any]:
        series: list[dict[str, Any]] = []
        current_values: list[int] = []
        previous_values: list[int] = []

        for current_date, previous_date in zip(current_dates, previous_dates, strict=False):
            current_value = int(current_map.get(current_date, 0))
            previous_value = int(previous_map.get(previous_date, 0))
            current_values.append(current_value)
            previous_values.append(previous_value)
            series.append(
                {
                    "label": current_date.strftime("%m-%d"),
                    "current_date": current_date.isoformat(),
                    "previous_date": previous_date.isoformat(),
                    "current_value": current_value,
                    "previous_value": previous_value,
                }
            )

        current_total = sum(current_values)
        previous_total = sum(previous_values)
        day_count = len(current_dates) or 1
        current_avg = round(current_total / day_count, 2)
        previous_avg = round(previous_total / day_count, 2)
        diff_rate_pct = round(((current_avg - previous_avg) / previous_avg) * 100, 2) if previous_avg else 0.0

        return {
            "title": title,
            "current_year": current_dates[0].year if current_dates else None,
            "previous_year": previous_dates[0].year if previous_dates else None,
            "summary": {
                "current_total": current_total,
                "previous_total": previous_total,
                "current_avg": current_avg,
                "previous_avg": previous_avg,
                "diff_rate_pct": diff_rate_pct,
                "current_peak": max(current_values) if current_values else 0,
                "previous_peak": max(previous_values) if previous_values else 0,
            },
            "daily_compare": series,
        }

    def _format_passenger_period_label(self, date_range: BiDateRange) -> str:
        start_label = f"{date_range.start_date.month}月{date_range.start_date.day}日"
        end_label = f"{date_range.end_date.month}月{date_range.end_date.day}日"
        return f"{start_label}-{end_label}"

    def _build_policy_impact_analysis(
        self,
        *,
        parking_rows: list[Any],
        point_bonus_index: dict[str, set[tuple[str, str]]],
        policy_context: dict[str, Any],
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        member_keys = point_bonus_index.get("member_keys", set())
        mobile_keys = point_bonus_index.get("mobile_keys", set())

        for row in parking_rows:
            exit_time = getattr(row, "exit_time", None)
            if exit_time is None:
                continue
            entry_time = getattr(row, "entry_time", None)
            duration_seconds = self._resolve_duration_seconds(row)
            billed_hours = int(math.ceil(duration_seconds / 3600)) if duration_seconds > 0 else 0
            date_key = exit_time.date().isoformat()
            member_id = _clean_key(row.member_id, "")
            mobile_no = _clean_key(row.mobile_no, "")
            member_level = _normalize_member_level(getattr(row, "member_level_name", None), member_id)
            actual_version_id = self._resolve_policy_version(exit_time, policy_context)
            simulated_version_id = PARKING_RULE_V2 if actual_version_id == PARKING_RULE_V1 else PARKING_RULE_V1
            actual_rule = self._resolve_policy_rule(actual_version_id, member_level, policy_context)
            simulated_rule = self._resolve_policy_rule(simulated_version_id, member_level, policy_context)
            has_points = (member_id, date_key) in member_keys or (mobile_no, date_key) in mobile_keys
            point_bonus_hours = 2 if has_points else 0
            actual_total_free_hours = max(_to_int(actual_rule["base_free_hours"]) + point_bonus_hours, 0)
            simulated_total_free_hours = max(_to_int(simulated_rule["base_free_hours"]) + point_bonus_hours, 0)

            actual_calc = self._calculate_policy_fee(
                entry_time=entry_time,
                exit_time=exit_time,
                duration_seconds=duration_seconds,
                total_free_hours=actual_total_free_hours,
                is_full_free=bool(actual_rule["is_diamond_full_free"]),
            )
            simulated_calc = self._calculate_policy_fee(
                entry_time=entry_time,
                exit_time=exit_time,
                duration_seconds=duration_seconds,
                total_free_hours=simulated_total_free_hours,
                is_full_free=bool(simulated_rule["is_diamond_full_free"]),
            )
            actual_without_points = self._calculate_policy_fee(
                entry_time=entry_time,
                exit_time=exit_time,
                duration_seconds=duration_seconds,
                total_free_hours=max(_to_int(actual_rule["base_free_hours"]), 0),
                is_full_free=bool(actual_rule["is_diamond_full_free"]),
            )
            flat_actual_fee = self._calculate_flat_policy_fee(
                billed_hours,
                actual_total_free_hours,
                bool(actual_rule["is_diamond_full_free"]),
            )

            recorded_fee_yuan = round(_to_float(row.total_fee_cent) / 100, 2)
            actual_receivable_yuan = actual_calc["fee_yuan"]
            simulated_receivable_yuan = simulated_calc["fee_yuan"]
            point_bonus_saved_yuan = max(round(actual_without_points["fee_yuan"] - actual_receivable_yuan, 2), 0.0)

            rows.append(
                {
                    "exit_date": date_key,
                    "plaza_name": _clean_key(row.plaza_name),
                    "member_level": member_level,
                    "member_id": member_id,
                    "mobile_no": mobile_no,
                    "actual_version_id": actual_version_id,
                    "simulated_version_id": simulated_version_id,
                    "duration_seconds": duration_seconds,
                    "stay_duration_hours": round(duration_seconds / 3600, 2) if duration_seconds > 0 else 0.0,
                    "billed_hours": actual_calc["billed_hours"],
                    "duration_bucket": _resolve_duration_bucket(actual_calc["billed_hours"]),
                    "has_points": has_points,
                    "point_bonus_hours": point_bonus_hours,
                    "point_bonus_saved_yuan": point_bonus_saved_yuan,
                    "recorded_fee_yuan": recorded_fee_yuan,
                    "actual_receivable_yuan": actual_receivable_yuan,
                    "simulated_receivable_yuan": simulated_receivable_yuan,
                    "receivable_delta_yuan": round(actual_receivable_yuan - simulated_receivable_yuan, 2),
                    "recorded_vs_simulated_delta_yuan": round(recorded_fee_yuan - simulated_receivable_yuan, 2),
                    "is_cross_day": actual_calc["is_cross_day"],
                    "cross_day_day_count": actual_calc["day_count"],
                    "cross_day_charge_day_count": actual_calc["charged_day_count"],
                    "cross_day_refinement_delta_yuan": round(actual_receivable_yuan - flat_actual_fee, 2),
                    "zero_to_paid": simulated_receivable_yuan <= 0 and actual_receivable_yuan > 0,
                }
            )

        after_policy_rows = [item for item in rows if item["actual_version_id"] == PARKING_RULE_V2]
        daily_map: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "date": "",
                "parking_count": 0,
                "actual_receivable_yuan": 0.0,
                "simulated_old_policy_yuan": 0.0,
                "receivable_uplift_yuan": 0.0,
                "recorded_fee_yuan": 0.0,
                "zero_to_paid_count": 0,
                "cross_day_count": 0,
            }
        )
        level_map: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "member_level": "",
                "parking_count": 0,
                "actual_receivable_yuan": 0.0,
                "simulated_old_policy_yuan": 0.0,
                "receivable_uplift_yuan": 0.0,
                "recorded_fee_yuan": 0.0,
                "zero_to_paid_count": 0,
                "cross_day_count": 0,
                "avg_stay_duration_hours": 0.0,
                "_stay_duration_hours_sum": 0.0,
            }
        )

        for item in after_policy_rows:
            daily_item = daily_map[item["exit_date"]]
            daily_item["date"] = item["exit_date"]
            daily_item["parking_count"] += 1
            daily_item["actual_receivable_yuan"] += item["actual_receivable_yuan"]
            daily_item["simulated_old_policy_yuan"] += item["simulated_receivable_yuan"]
            daily_item["receivable_uplift_yuan"] += item["receivable_delta_yuan"]
            daily_item["recorded_fee_yuan"] += item["recorded_fee_yuan"]
            daily_item["zero_to_paid_count"] += 1 if item["zero_to_paid"] else 0
            daily_item["cross_day_count"] += 1 if item["is_cross_day"] else 0

            level_item = level_map[item["member_level"]]
            level_item["member_level"] = item["member_level"]
            level_item["parking_count"] += 1
            level_item["actual_receivable_yuan"] += item["actual_receivable_yuan"]
            level_item["simulated_old_policy_yuan"] += item["simulated_receivable_yuan"]
            level_item["receivable_uplift_yuan"] += item["receivable_delta_yuan"]
            level_item["recorded_fee_yuan"] += item["recorded_fee_yuan"]
            level_item["zero_to_paid_count"] += 1 if item["zero_to_paid"] else 0
            level_item["cross_day_count"] += 1 if item["is_cross_day"] else 0
            level_item["_stay_duration_hours_sum"] += item["stay_duration_hours"]

        simulated_old_total = sum(item["simulated_receivable_yuan"] for item in after_policy_rows)
        recorded_total = sum(item["recorded_fee_yuan"] for item in after_policy_rows)
        cross_day_rows = [item for item in after_policy_rows if item["is_cross_day"]]

        by_member_level: list[dict[str, Any]] = []
        for item in level_map.values():
            parking_count = item["parking_count"]
            simulated_total = item["simulated_old_policy_yuan"]
            item["actual_receivable_yuan"] = round(item["actual_receivable_yuan"], 2)
            item["simulated_old_policy_yuan"] = round(simulated_total, 2)
            item["receivable_uplift_yuan"] = round(item["receivable_uplift_yuan"], 2)
            item["recorded_fee_yuan"] = round(item["recorded_fee_yuan"], 2)
            item["zero_to_paid_rate_pct"] = round(item["zero_to_paid_count"] / parking_count * 100, 2) if parking_count else 0.0
            item["cross_day_rate_pct"] = round(item["cross_day_count"] / parking_count * 100, 2) if parking_count else 0.0
            item["avg_stay_duration_hours"] = round(item["_stay_duration_hours_sum"] / parking_count, 2) if parking_count else 0.0
            item["realized_growth_rate_pct"] = round(
                (item["recorded_fee_yuan"] - simulated_total) / simulated_total * 100,
                2,
            ) if simulated_total else 0.0
            item.pop("_stay_duration_hours_sum", None)
            by_member_level.append(item)
        by_member_level.sort(key=lambda item: (item["receivable_uplift_yuan"], item["actual_receivable_yuan"]), reverse=True)

        daily = list(daily_map.values())
        for item in daily:
            parking_count = item["parking_count"]
            item["actual_receivable_yuan"] = round(item["actual_receivable_yuan"], 2)
            item["simulated_old_policy_yuan"] = round(item["simulated_old_policy_yuan"], 2)
            item["receivable_uplift_yuan"] = round(item["receivable_uplift_yuan"], 2)
            item["recorded_fee_yuan"] = round(item["recorded_fee_yuan"], 2)
            item["zero_to_paid_rate_pct"] = round(item["zero_to_paid_count"] / parking_count * 100, 2) if parking_count else 0.0
            item["cross_day_rate_pct"] = round(item["cross_day_count"] / parking_count * 100, 2) if parking_count else 0.0
        daily.sort(key=lambda item: item["date"])

        policy_impact = {
            "summary": {
                "parking_count": len(after_policy_rows),
                "actual_receivable_yuan": round(sum(item["actual_receivable_yuan"] for item in after_policy_rows), 2),
                "simulated_old_policy_yuan": round(simulated_old_total, 2),
                "receivable_uplift_yuan": round(sum(item["receivable_delta_yuan"] for item in after_policy_rows), 2),
                "recorded_fee_yuan": round(recorded_total, 2),
                "realized_growth_rate_pct": round((recorded_total - simulated_old_total) / simulated_old_total * 100, 2)
                if simulated_old_total
                else 0.0,
                "zero_to_paid_count": sum(1 for item in after_policy_rows if item["zero_to_paid"]),
                "zero_to_paid_rate_pct": round(
                    sum(1 for item in after_policy_rows if item["zero_to_paid"]) / len(after_policy_rows) * 100,
                    2,
                )
                if after_policy_rows
                else 0.0,
                "cross_day_count": len(cross_day_rows),
                "cross_day_rate_pct": round(len(cross_day_rows) / len(after_policy_rows) * 100, 2)
                if after_policy_rows
                else 0.0,
                "point_bonus_saved_yuan": round(sum(item["point_bonus_saved_yuan"] for item in after_policy_rows), 2),
            },
            "daily": daily,
            "by_member_level": by_member_level,
            "cross_day_summary": {
                "cross_day_count": len(cross_day_rows),
                "avg_cross_day_billed_hours": round(
                    sum(item["billed_hours"] for item in cross_day_rows) / len(cross_day_rows),
                    2,
                )
                if cross_day_rows
                else 0.0,
                "avg_cross_day_day_count": round(
                    sum(item["cross_day_day_count"] for item in cross_day_rows) / len(cross_day_rows),
                    2,
                )
                if cross_day_rows
                else 0.0,
                "cross_day_actual_receivable_yuan": round(
                    sum(item["actual_receivable_yuan"] for item in cross_day_rows),
                    2,
                ),
                "cross_day_simulated_old_policy_yuan": round(
                    sum(item["simulated_receivable_yuan"] for item in cross_day_rows),
                    2,
                ),
                "cross_day_refinement_delta_yuan": round(
                    sum(item["cross_day_refinement_delta_yuan"] for item in cross_day_rows),
                    2,
                ),
            },
        }
        return {"rows": rows, "after_policy_rows": after_policy_rows, "policy_impact": policy_impact}

    def _build_duration_shift(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        summary: list[dict[str, Any]] = []
        distributions: list[dict[str, Any]] = []

        for member_level in BEHAVIOR_SHIFT_LEVELS:
            before_rows = [item for item in rows if item["member_level"] == member_level and item["actual_version_id"] == PARKING_RULE_V1]
            after_rows = [item for item in rows if item["member_level"] == member_level and item["actual_version_id"] == PARKING_RULE_V2]
            before_count = len(before_rows)
            after_count = len(after_rows)

            summary.append(
                {
                    "member_level": member_level,
                    "before_count": before_count,
                    "after_count": after_count,
                    "before_avg_hours": round(sum(item["stay_duration_hours"] for item in before_rows) / before_count, 2)
                    if before_count
                    else 0.0,
                    "after_avg_hours": round(sum(item["stay_duration_hours"] for item in after_rows) / after_count, 2)
                    if after_count
                    else 0.0,
                    "before_avg_receivable_yuan": round(sum(item["actual_receivable_yuan"] for item in before_rows) / before_count, 2)
                    if before_count
                    else 0.0,
                    "after_avg_receivable_yuan": round(sum(item["actual_receivable_yuan"] for item in after_rows) / after_count, 2)
                    if after_count
                    else 0.0,
                }
            )

            for _, bucket in IMPACT_DURATION_BUCKETS:
                before_bucket_count = sum(1 for item in before_rows if item["duration_bucket"] == bucket)
                after_bucket_count = sum(1 for item in after_rows if item["duration_bucket"] == bucket)
                distributions.append(
                    {
                        "member_level": member_level,
                        "duration_bucket": bucket,
                        "before_count": before_bucket_count,
                        "after_count": after_bucket_count,
                        "before_share_pct": round(before_bucket_count / before_count * 100, 2) if before_count else 0.0,
                        "after_share_pct": round(after_bucket_count / after_count * 100, 2) if after_count else 0.0,
                    }
                )

        return {"summary": summary, "distributions": distributions}

    def _build_points_leverage(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        total_count = len(rows)
        point_earned_count = sum(1 for item in rows if item["has_points"])
        point_bonus_triggered_count = sum(1 for item in rows if item["point_bonus_saved_yuan"] > 0)

        by_member_level_map: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "member_level": "",
                "parking_count": 0,
                "point_earned_count": 0,
                "point_bonus_triggered_count": 0,
                "point_bonus_saved_yuan": 0.0,
                "_stay_duration_hours_sum": 0.0,
            }
        )
        for item in rows:
            bucket = by_member_level_map[item["member_level"]]
            bucket["member_level"] = item["member_level"]
            bucket["parking_count"] += 1
            bucket["point_earned_count"] += 1 if item["has_points"] else 0
            bucket["point_bonus_triggered_count"] += 1 if item["point_bonus_saved_yuan"] > 0 else 0
            bucket["point_bonus_saved_yuan"] += item["point_bonus_saved_yuan"]
            bucket["_stay_duration_hours_sum"] += item["stay_duration_hours"]

        by_member_level: list[dict[str, Any]] = []
        for item in by_member_level_map.values():
            parking_count = item["parking_count"]
            point_earned_count_item = item["point_earned_count"]
            point_bonus_triggered_count_item = item["point_bonus_triggered_count"]
            item["point_bonus_saved_yuan"] = round(item["point_bonus_saved_yuan"], 2)
            item["point_earned_rate_pct"] = round(point_earned_count_item / parking_count * 100, 2) if parking_count else 0.0
            item["point_bonus_trigger_rate_pct"] = round(
                point_bonus_triggered_count_item / parking_count * 100,
                2,
            ) if parking_count else 0.0
            item["leverage_conversion_rate_pct"] = round(
                point_bonus_triggered_count_item / point_earned_count_item * 100,
                2,
            ) if point_earned_count_item else 0.0
            item["avg_stay_duration_hours"] = round(item["_stay_duration_hours_sum"] / parking_count, 2) if parking_count else 0.0
            item.pop("_stay_duration_hours_sum", None)
            by_member_level.append(item)
        by_member_level.sort(key=lambda item: (item["point_bonus_triggered_count"], item["point_bonus_saved_yuan"]), reverse=True)

        return {
            "summary": {
                "total_parking_count": total_count,
                "point_earned_count": point_earned_count,
                "point_bonus_triggered_count": point_bonus_triggered_count,
                "point_earned_rate_pct": round(point_earned_count / total_count * 100, 2) if total_count else 0.0,
                "point_bonus_trigger_rate_pct": round(point_bonus_triggered_count / total_count * 100, 2) if total_count else 0.0,
                "leverage_conversion_rate_pct": round(point_bonus_triggered_count / point_earned_count * 100, 2)
                if point_earned_count
                else 0.0,
                "point_bonus_saved_yuan": round(sum(item["point_bonus_saved_yuan"] for item in rows), 2),
            },
            "funnel": [
                {"name": "总停车次数", "value": total_count},
                {"name": "产生积分停车次数", "value": point_earned_count},
                {"name": "享受+2小时优惠次数", "value": point_bonus_triggered_count},
            ],
            "by_member_level": by_member_level,
        }

    def _aggregate_parking(self, rows: list[Any], date_range: BiDateRange) -> tuple[dict[str, Any], set[str], set[str], dict[str, Any]]:
        plate_set: set[str] = set()
        mobile_set: set[str] = set()
        member_set: set[str] = set()
        total_fee_cent = 0.0
        total_duration_seconds = 0.0

        daily = {
            current.isoformat(): {
                "date": current.isoformat(),
                "parking_count": 0,
                "parking_fee_yuan": 0.0,
                "matched_mobile_count": 0,
                "matched_member_count": 0,
                "trade_count": 0,
                "trade_amount_yuan": 0.0,
                "trade_discount_yuan": 0.0,
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "positive_points": 0.0,
                "negative_points": 0.0,
            }
            for current in date_range.iter_dates()
        }
        daily_mobile: dict[str, set[str]] = defaultdict(set)
        daily_member: dict[str, set[str]] = defaultdict(set)
        hourly: dict[int, int] = defaultdict(int)
        plaza_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "plaza_name": "未标注",
                "parking_count": 0,
                "trade_count": 0,
                "trade_amount_yuan": 0.0,
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "matched_member_count": 0,
                "_member_ids": set(),
            }
        )
        duration_counts = {
            "30分钟内": 0,
            "30-60分钟": 0,
            "1-2小时": 0,
            "2-4小时": 0,
            "4小时以上": 0,
        }

        for row in rows:
            biz_datetime = row.biz_datetime
            if biz_datetime is None:
                continue
            date_key = biz_datetime.date().isoformat()
            hour = biz_datetime.hour
            plate_no = _clean_key(row.plate_no, "")
            mobile_no = _clean_key(row.mobile_no, "")
            member_id = _clean_key(row.member_id, "")
            plaza_name = _clean_key(row.plaza_name)
            fee_cent = _to_float(row.total_fee_cent)
            duration_seconds = _to_float(row.parking_duration_seconds)

            daily[date_key]["parking_count"] += 1
            daily[date_key]["parking_fee_yuan"] += round(fee_cent / 100, 2)
            hourly[hour] += 1
            total_fee_cent += fee_cent
            total_duration_seconds += duration_seconds

            if plate_no:
                plate_set.add(plate_no)
            if mobile_no:
                mobile_set.add(mobile_no)
                daily_mobile[date_key].add(mobile_no)
            if member_id:
                member_set.add(member_id)
                daily_member[date_key].add(member_id)
                plaza_stats[plaza_name]["_member_ids"].add(member_id)

            plaza_stats[plaza_name]["plaza_name"] = plaza_name
            plaza_stats[plaza_name]["parking_count"] += 1

            if duration_seconds < 1800:
                duration_counts["30分钟内"] += 1
            elif duration_seconds < 3600:
                duration_counts["30-60分钟"] += 1
            elif duration_seconds < 7200:
                duration_counts["1-2小时"] += 1
            elif duration_seconds < 14400:
                duration_counts["2-4小时"] += 1
            else:
                duration_counts["4小时以上"] += 1

        for date_key, values in daily.items():
            values["matched_mobile_count"] = len(daily_mobile.get(date_key, set()))
            values["matched_member_count"] = len(daily_member.get(date_key, set()))

        for plaza_name, values in plaza_stats.items():
            values["matched_member_count"] = len(values.pop("_member_ids"))

        summary = {
            "parking_count": len(rows),
            "plate_count": len(plate_set),
            "mobile_count": len(mobile_set),
            "member_count": len(member_set),
            "total_fee_yuan": round(total_fee_cent / 100, 2),
            "avg_duration_minutes": round((total_duration_seconds / len(rows) / 60), 1) if rows else 0.0,
        }

        stats = {
            "daily": daily,
            "hourly": hourly,
            "plaza": plaza_stats,
            "duration_buckets": [{"bucket": key, "count": value} for key, value in duration_counts.items()],
        }
        return summary, mobile_set, member_set, stats

    def _aggregate_trade(
        self,
        rows: list[Any],
        parking_rows: list[Any],
        date_range: BiDateRange,
    ) -> tuple[dict[str, Any], set[str], set[str], dict[str, Any]]:
        direct_mobile_set: set[str] = set()
        linked_mobile_set: set[str] = set()
        linked_member_set: set[str] = set()
        plate_set: set[str] = set()
        total_actual_cent = 0.0
        total_discount_cent = 0.0
        total_fee_cent = 0.0

        parking_mobile_counts: dict[str, Counter[str]] = defaultdict(Counter)
        parking_member_counts: dict[str, Counter[str]] = defaultdict(Counter)
        for parking_row in parking_rows:
            plate_no = _normalize_identifier(parking_row.plate_no)
            if not plate_no:
                continue
            mobile_no = _clean_key(parking_row.mobile_no, "")
            member_id = _clean_key(parking_row.member_id, "")
            if mobile_no:
                parking_mobile_counts[plate_no][mobile_no] += 1
            if member_id:
                parking_member_counts[plate_no][member_id] += 1

        daily = {current.isoformat(): {"trade_count": 0, "trade_amount_yuan": 0.0, "trade_discount_yuan": 0.0} for current in date_range.iter_dates()}
        hourly: dict[int, int] = defaultdict(int)
        channel_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "未标注", "count": 0, "amount_yuan": 0.0})
        business_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "未标注", "count": 0, "amount_yuan": 0.0})
        plaza_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "plaza_name": "未标注",
                "parking_count": 0,
                "trade_count": 0,
                "trade_amount_yuan": 0.0,
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "matched_member_count": 0,
                "_member_ids": set(),
            }
        )

        for row in rows:
            biz_datetime = row.biz_datetime
            if biz_datetime is None:
                continue
            date_key = biz_datetime.date().isoformat()
            actual_value_cent = _to_float(row.actual_value_cent)
            discount_cent = _to_float(row.discount_cent)
            fee_cent = _to_float(row.fee_cent)
            mobile_no = _clean_key(row.mobile_no, "")
            plate_no = _normalize_identifier(row.plate_no) or _normalize_identifier(
                extract_plate_no({"subject": row.subject, "body": row.body})
            )
            plaza_name = _clean_key(row.plaza_name)
            channel_name = _clean_key(row.channel_name)
            business_name = _translate_trade_business_name(row.business)

            daily[date_key]["trade_count"] += 1
            daily[date_key]["trade_amount_yuan"] += round(actual_value_cent / 100, 2)
            daily[date_key]["trade_discount_yuan"] += round(discount_cent / 100, 2)
            hourly[biz_datetime.hour] += 1

            total_actual_cent += actual_value_cent
            total_discount_cent += discount_cent
            total_fee_cent += fee_cent

            if mobile_no:
                direct_mobile_set.add(mobile_no)
                linked_mobile_set.add(mobile_no)
            if plate_no:
                plate_set.add(plate_no)
                mobile_counter = parking_mobile_counts.get(plate_no)
                member_counter = parking_member_counts.get(plate_no)
                if mobile_counter:
                    linked_mobile_set.add(mobile_counter.most_common(1)[0][0])
                if member_counter:
                    member_id = member_counter.most_common(1)[0][0]
                    linked_member_set.add(member_id)
                    plaza_stats[plaza_name]["_member_ids"].add(member_id)

            channel_stats[channel_name]["name"] = channel_name
            channel_stats[channel_name]["count"] += 1
            channel_stats[channel_name]["amount_yuan"] += round(actual_value_cent / 100, 2)

            business_stats[business_name]["name"] = business_name
            business_stats[business_name]["count"] += 1
            business_stats[business_name]["amount_yuan"] += round(actual_value_cent / 100, 2)

            plaza_stats[plaza_name]["plaza_name"] = plaza_name
            plaza_stats[plaza_name]["trade_count"] += 1
            plaza_stats[plaza_name]["trade_amount_yuan"] += round(actual_value_cent / 100, 2)

        linked_member_set |= self._resolve_member_ids_by_mobile(linked_mobile_set)

        for plaza_name, values in plaza_stats.items():
            values["matched_member_count"] = len(values.pop("_member_ids"))

        summary = {
            "trade_count": len(rows),
            "mobile_count": len(linked_mobile_set),
            "direct_mobile_count": len(direct_mobile_set),
            "plate_count": len(plate_set),
            "actual_value_yuan": round(total_actual_cent / 100, 2),
            "discount_yuan": round(total_discount_cent / 100, 2),
            "fee_yuan": round(total_fee_cent / 100, 2),
        }
        stats = {
            "daily": daily,
            "hourly": hourly,
            "channels": sorted(channel_stats.values(), key=lambda item: item["count"], reverse=True),
            "businesses": sorted(business_stats.values(), key=lambda item: item["count"], reverse=True),
            "plaza": plaza_stats,
        }
        return summary, linked_mobile_set, linked_member_set, stats

    def _aggregate_point(self, rows: list[Any], date_range: BiDateRange) -> tuple[dict[str, Any], set[str], set[str], dict[str, Any]]:
        mobile_set: set[str] = set()
        member_set: set[str] = set()
        consume_amount = 0.0
        signed_points = 0.0
        positive_points = 0.0
        negative_points = 0.0
        valid_row_count = 0

        daily = {
            current.isoformat(): {
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "positive_points": 0.0,
                "negative_points": 0.0,
            }
            for current in date_range.iter_dates()
        }
        hourly: dict[int, int] = defaultdict(int)
        plaza_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "plaza_name": "未标注",
                "parking_count": 0,
                "trade_count": 0,
                "trade_amount_yuan": 0.0,
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "matched_member_count": 0,
            }
        )

        for row in rows:
            biz_datetime = row.biz_datetime
            if biz_datetime is None:
                continue
            if _is_invalid_point_flow_row(row):
                continue
            date_key = biz_datetime.date().isoformat()
            plaza_name = _clean_key(row.plaza_name)
            mobile_no = _clean_key(row.mobile_no, "")
            member_id = _clean_key(row.member_id, "")
            consume = _to_float(row.consume_amount)
            signed = _to_float(row.signed_change_points)
            valid_row_count += 1

            daily[date_key]["point_flow_count"] += 1
            daily[date_key]["consume_amount_yuan"] += consume
            hourly[biz_datetime.hour] += 1
            consume_amount += consume
            signed_points += signed

            if signed > 0:
                positive_points += signed
                daily[date_key]["positive_points"] += signed
            elif signed < 0:
                negative_points += -signed
                daily[date_key]["negative_points"] += -signed

            if mobile_no:
                mobile_set.add(mobile_no)
            if member_id:
                member_set.add(member_id)

            plaza_stats[plaza_name]["plaza_name"] = plaza_name
            plaza_stats[plaza_name]["point_flow_count"] += 1
            plaza_stats[plaza_name]["consume_amount_yuan"] += consume

        summary = {
            "flow_count": valid_row_count,
            "member_count": len(member_set),
            "mobile_count": len(mobile_set),
            "consume_amount_yuan": round(consume_amount, 2),
            "signed_points": round(signed_points, 2),
            "positive_points": round(positive_points, 2),
            "negative_points": round(negative_points, 2),
        }
        stats = {"daily": daily, "hourly": hourly, "plaza": plaza_stats}
        return summary, mobile_set, member_set, stats

    def _resolve_member_ids_by_mobile(self, mobiles: set[str]) -> set[str]:
        if not mobiles:
            return set()
        member_ids: set[str] = set()
        for batch in _batch_values(sorted(mobiles), size=1000):
            rows = self.db.execute(
                select(MemberProfile.mobile_no, MemberProfile.member_id).where(
                    MemberProfile.mobile_no.in_(batch),
                    MemberProfile.member_id.is_not(None),
                    MemberProfile.member_id != "",
                )
            ).all()
            for _, member_id in rows:
                if member_id:
                    member_ids.add(str(member_id).strip())
        return member_ids

    def _build_member_level_map_from_parking_rows(self, rows: list[Any]) -> dict[str, str]:
        level_by_member: dict[str, str] = {}
        for row in rows:
            member_id = _clean_key(getattr(row, "member_id", None), "")
            if not member_id or member_id in level_by_member:
                continue
            level_by_member[member_id] = _clean_key(getattr(row, "member_level_name", None), "未分级")
        return level_by_member

    def _build_level_distribution(
        self,
        *,
        parking_member_ids: set[str],
        trade_member_ids: set[str],
        point_member_ids: set[str],
        known_level_by_member: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        all_member_ids = parking_member_ids | trade_member_ids | point_member_ids
        if not all_member_ids:
            return []

        level_by_member = dict(known_level_by_member or {})
        missing_member_ids = sorted(member_id for member_id in all_member_ids if member_id not in level_by_member)

        accounts: list[Any] = []
        for batch in _batch_values(missing_member_ids, size=1000):
            accounts.extend(
                self.db.execute(
                    select(MemberAccount.member_id, MemberAccount.level_name).where(MemberAccount.member_id.in_(batch))
                ).all()
            )

        for member_id, level_name in accounts:
            if member_id:
                level_by_member[str(member_id).strip()] = _clean_key(level_name, "未分级")
        buckets: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "level_name": "未分级",
                "member_count": 0,
                "parking_members": 0,
                "trade_members": 0,
                "point_members": 0,
            }
        )

        for member_ids, field in (
            (parking_member_ids, "parking_members"),
            (trade_member_ids, "trade_members"),
            (point_member_ids, "point_members"),
        ):
            for member_id in member_ids:
                level_name = level_by_member.get(member_id, "未分级")
                buckets[level_name]["level_name"] = level_name
                buckets[level_name][field] += 1

        for member_id in all_member_ids:
            level_name = level_by_member.get(member_id, "未分级")
            buckets[level_name]["level_name"] = level_name
            buckets[level_name]["member_count"] += 1

        values = list(buckets.values())
        values.sort(key=lambda item: item["member_count"], reverse=True)
        return values

    def _merge_hourly_distribution(
        self,
        parking_hourly: dict[int, int],
        trade_hourly: dict[int, int],
        point_hourly: dict[int, int],
    ) -> list[dict[str, Any]]:
        return [
            {
                "hour": f"{hour:02d}:00",
                "parking_count": parking_hourly.get(hour, 0),
                "trade_count": trade_hourly.get(hour, 0),
                "point_flow_count": point_hourly.get(hour, 0),
            }
            for hour in range(24)
        ]

    def _merge_plaza_ranking(
        self,
        parking_plaza: dict[str, dict[str, Any]],
        trade_plaza: dict[str, dict[str, Any]],
        point_plaza: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ranking: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "plaza_name": "未标注",
                "parking_count": 0,
                "trade_count": 0,
                "trade_amount_yuan": 0.0,
                "point_flow_count": 0,
                "consume_amount_yuan": 0.0,
                "matched_member_count": 0,
            }
        )
        for source in (parking_plaza, trade_plaza, point_plaza):
            for key, row in source.items():
                ranking[key]["plaza_name"] = row.get("plaza_name", key)
                ranking[key]["parking_count"] += row.get("parking_count", 0)
                ranking[key]["trade_count"] += row.get("trade_count", 0)
                ranking[key]["trade_amount_yuan"] += row.get("trade_amount_yuan", 0.0)
                ranking[key]["point_flow_count"] += row.get("point_flow_count", 0)
                ranking[key]["consume_amount_yuan"] += row.get("consume_amount_yuan", 0.0)
                ranking[key]["matched_member_count"] += row.get("matched_member_count", 0)

        values = list(ranking.values())
        values.sort(
            key=lambda item: (
                item["parking_count"] + item["trade_count"] + item["point_flow_count"],
                item["trade_amount_yuan"] + item["consume_amount_yuan"],
            ),
            reverse=True,
        )
        return values[:12]

    def _build_linkage_funnel(
        self,
        *,
        parking_summary: dict[str, Any],
        parking_mobiles: set[str],
        parking_member_ids: set[str],
        trade_mobiles: set[str],
        trade_member_ids: set[str],
        point_mobiles: set[str],
        point_member_ids: set[str],
    ) -> list[dict[str, Any]]:
        return [
            {"name": "停车记录", "value": parking_summary["parking_count"]},
            {"name": "停车关联手机号", "value": parking_summary["mobile_count"]},
            {"name": "停车关联会员", "value": parking_summary["member_count"]},
            {"name": "停车交易拉通手机号", "value": len(trade_mobiles)},
            {"name": "同期积分会员", "value": len(point_member_ids)},
            {"name": "三域手机号交集", "value": len(parking_mobiles & trade_mobiles & point_mobiles)},
            {"name": "三域会员交集", "value": len(parking_member_ids & trade_member_ids & point_member_ids)},
        ]

    def _build_validation_metrics(
        self,
        *,
        parking_summary: dict[str, Any],
        trade_summary: dict[str, Any],
        point_summary: dict[str, Any],
        parking_mobiles: set[str],
        parking_member_ids: set[str],
        trade_mobiles: set[str],
        trade_member_ids: set[str],
        point_mobiles: set[str],
        point_member_ids: set[str],
        policy_impact_summary: dict[str, Any],
        points_leverage_summary: dict[str, Any],
    ) -> list[dict[str, Any]]:
        def pct(numerator: int, denominator: int) -> str:
            if denominator <= 0:
                return "0.00%"
            return f"{(numerator / denominator) * 100:.2f}%"

        def count_text(value: int) -> str:
            return f"{float(value):.2f}"

        mobile_all = parking_mobiles | trade_mobiles | point_mobiles
        member_all = parking_member_ids | trade_member_ids | point_member_ids
        effective_parking_fee_yuan = parking_summary["total_fee_yuan"]
        parking_fee_description = "时间范围内停车收费金额合计。"
        if effective_parking_fee_yuan <= 0 and trade_summary["actual_value_yuan"] > 0:
            effective_parking_fee_yuan = trade_summary["actual_value_yuan"]
            parking_fee_description = "停车记录源表收费字段为空，当前按停车交易实付金额作为收费校验口径。"

        return [
            {
                "metric": "停车记录手机号覆盖率",
                "value": pct(parking_summary["mobile_count"], parking_summary["parking_count"]),
                "description": "停车记录中已匹配手机号的占比。",
            },
            {
                "metric": "停车记录会员覆盖率",
                "value": pct(parking_summary["member_count"], parking_summary["parking_count"]),
                "description": "停车记录中已关联会员 ID 的占比。",
            },
            {
                "metric": "交易手机号与停车交集率",
                "value": pct(len(parking_mobiles & trade_mobiles), len(trade_mobiles)),
                "description": "同期停车交易手机号中（缺失手机号时按车牌回补），同时出现在停车数据中的比例。",
            },
            {
                "metric": "积分手机号与停车交集率",
                "value": pct(len(parking_mobiles & point_mobiles), len(point_mobiles)),
                "description": "同期积分手机号中，同时出现在停车数据中的比例。",
            },
            {
                "metric": "三域手机号交集数",
                "value": count_text(len(parking_mobiles & trade_mobiles & point_mobiles)),
                "description": f"停车、交易、积分三域共覆盖手机号 {count_text(len(mobile_all))} 个；交易域优先使用直接手机号，缺失时按车牌回补。",
            },
            {
                "metric": "三域会员交集数",
                "value": count_text(len(parking_member_ids & trade_member_ids & point_member_ids)),
                "description": f"停车、交易、积分三域共覆盖会员 {count_text(len(member_all))} 个；交易域会员按手机号和车牌映射回补。",
            },
            {
                "metric": "停车交易实付金额",
                "value": f"{trade_summary['actual_value_yuan']:.2f} 元",
                "description": "时间范围内停车交易实际支付金额合计。",
            },
            {
                "metric": "会员消费金额",
                "value": f"{point_summary['consume_amount_yuan']:.2f} 元",
                "description": "时间范围内积分流水关联的消费金额合计。",
            },
            {
                "metric": "停车收费金额",
                "value": f"{effective_parking_fee_yuan:.2f} 元",
                "description": parking_fee_description,
            },
            {
                "metric": "新规增收测算",
                "value": f"{policy_impact_summary['receivable_uplift_yuan']:.2f} 元",
                "description": "仅统计新规生效后的停车记录，比较新规应收与旧规则模拟应收之间的差额。",
            },
            {
                "metric": "零元转付费占比",
                "value": f"{policy_impact_summary['zero_to_paid_rate_pct']:.2f}%",
                "description": "旧规下原本可 0 元离场、在新规下变成付费离场的停车记录占比。",
            },
            {
                "metric": "跨天停车占比",
                "value": f"{policy_impact_summary['cross_day_rate_pct']:.2f}%",
                "description": "新规样本中跨自然日停车的占比，跨天记录已按自然日拆分并分别应用每日封顶。",
            },
            {
                "metric": "积分杠杆触发率",
                "value": f"{points_leverage_summary['point_bonus_trigger_rate_pct']:.2f}%",
                "description": "新规样本中，因当天有积分入账而真正触发额外 2 小时停车减免的停车记录占比。",
            },
            {
                "metric": "积分杠杆转化率",
                "value": f"{points_leverage_summary['leverage_conversion_rate_pct']:.2f}%",
                "description": "在当天已有积分入账的停车记录里，最终形成 +2 小时停车减免的转化率。",
            },
        ]
