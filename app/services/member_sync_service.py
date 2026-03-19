from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.member import (
    MemberAccount,
    MemberLevelChangeLog,
    MemberLevelDict,
    MemberLevelPeriod,
    MemberProfile,
    MemberProfileAttr,
)
from app.services.icsp_client import ICSPClient


def clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_datetime_value(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, (int, float)):
        timestamp_value = float(value)
        if timestamp_value > 1_000_000_000_000:
            timestamp_value /= 1000
        return datetime.fromtimestamp(timestamp_value, tz=timezone.utc).replace(tzinfo=None)

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_datetime_value(int(text))

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_bool_value(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "是"}:
        return True
    if text in {"0", "false", "no", "n", "off", "否"}:
        return False
    return None


def parse_decimal_value(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def extract_member_detail(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if clean_str(payload.get("memberId")):
        return payload

    for key in ("data", "result"):
        nested = payload.get(key)
        if isinstance(nested, dict) and clean_str(nested.get("memberId")):
            return nested
    return payload


def extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    if clean_str(payload.get("memberId")):
        return [payload]

    for key in ("rows", "list", "resultList", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    nested = payload.get("data")
    if isinstance(nested, list):
        return [item for item in nested if isinstance(item, dict)]
    if isinstance(nested, dict):
        return extract_rows(nested)
    return []


def build_attr_code(item: dict[str, Any], index: int) -> str:
    for key in ("metaKey", "metaCode", "code", "key", "itemCode", "name", "metaName", "label"):
        value = clean_str(item.get(key))
        if value:
            return value
    return f"meta_{index}"


def build_level_event_key(member_id: str, row: dict[str, Any], parsed_update_time: datetime | None) -> str:
    fingerprint = {
        "member_id": member_id,
        "pre_level_no": clean_str(row.get("preLevelNo")),
        "level_no": clean_str(row.get("levelNo")),
        "pre_level_name": clean_str(row.get("preLevelName")),
        "level_name": clean_str(row.get("levelName")),
        "update_time": parsed_update_time.isoformat() if parsed_update_time else clean_str(row.get("updateTime")),
        "opt_type": clean_str(row.get("optType")),
        "update_by": clean_str(row.get("updateBy")),
        "remark": clean_str(row.get("remark")),
    }
    encoded = json.dumps(fingerprint, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(slots=True)
class MemberSyncSummary:
    member_id: str
    profile_upserted: bool = False
    account_upserted: bool = False
    attr_count: int = 0
    level_dict_upserts: int = 0
    change_logs_inserted: int = 0
    change_logs_updated: int = 0
    periods_rebuilt: int = 0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def merge(self, other: "MemberSyncSummary") -> "MemberSyncSummary":
        self.profile_upserted = self.profile_upserted or other.profile_upserted
        self.account_upserted = self.account_upserted or other.account_upserted
        self.attr_count = max(self.attr_count, other.attr_count)
        self.level_dict_upserts += other.level_dict_upserts
        self.change_logs_inserted += other.change_logs_inserted
        self.change_logs_updated += other.change_logs_updated
        self.periods_rebuilt += other.periods_rebuilt
        self.warnings.extend(other.warnings)
        return self


@dataclass(slots=True)
class MemberFetchSyncResult:
    requested_mobile_no: str | None = None
    matched_member_ids: list[str] = field(default_factory=list)
    summaries: list[MemberSyncSummary] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_mobile_no": self.requested_mobile_no,
            "matched_member_ids": self.matched_member_ids,
            "summaries": [summary.to_dict() for summary in self.summaries],
            "warnings": self.warnings,
        }


class MemberSyncService:
    def __init__(self, db: Session):
        self.db = db
        self._level_cache: dict[tuple[str, str], MemberLevelDict] = {}
        self._level_upsert_count = 0

    def sync_member_bundle(
        self,
        member_detail_payload: dict[str, Any],
        level_history_payload: list[dict[str, Any]] | dict[str, Any] | None = None,
        *,
        commit: bool = True,
    ) -> MemberSyncSummary:
        detail_summary = self.sync_member_detail(member_detail_payload, commit=False)
        if level_history_payload is not None:
            history_summary = self.sync_member_level_history(
                level_history_payload,
                member_id=detail_summary.member_id,
                commit=False,
            )
            detail_summary.merge(history_summary)

        if commit:
            self.db.commit()
        return detail_summary

    def sync_member_detail(self, payload: dict[str, Any], *, commit: bool = True) -> MemberSyncSummary:
        starting_level_upserts = self._level_upsert_count
        member_data = extract_member_detail(payload)
        member_id = clean_str(member_data.get("memberId"))
        if not member_id:
            raise ValueError("member detail payload missing memberId")

        summary = MemberSyncSummary(member_id=member_id)

        profile = self.db.get(MemberProfile, member_id)
        if profile is None:
            profile = MemberProfile(member_id=member_id)
            self.db.add(profile)

        profile.member_name = clean_str(member_data.get("memberName"))
        profile.mobile_no = clean_str(member_data.get("mobileNo"))
        profile.email = clean_str(member_data.get("email"))
        profile.avatar_url = clean_str(member_data.get("avatarUrl")) or clean_str(member_data.get("headImgUrl"))
        profile.sex = clean_str(member_data.get("sex"))
        profile.reg_date = parse_datetime_value(member_data.get("regDate")) or parse_datetime_value(member_data.get("regTime"))
        profile.birthday = parse_datetime_value(member_data.get("birthday"))
        profile.fav_plaza_code = clean_str(member_data.get("favPlazaCode"))
        profile.fav_plaza_name = clean_str(member_data.get("favPlazaName"))
        profile.fav_plaza_bu_id = clean_str(member_data.get("favPlazaBuId"))
        profile.belong_plaza_code = clean_str(member_data.get("belongPlazaCode"))
        profile.belong_plaza_name = clean_str(member_data.get("belongPlazaName"))
        profile.belong_plaza_bu_id = clean_str(member_data.get("belongPlazaBuId"))
        profile.reg_plaza_code = clean_str(member_data.get("regPlazaCode"))
        profile.reg_plaza_name = clean_str(member_data.get("regPlazaName"))
        profile.reg_plaza_bu_id = clean_str(member_data.get("regPlazaBuId"))
        profile.expanding_channel = clean_str(member_data.get("expandingChannel")) or clean_str(
            member_data.get("expandingChannelCode")
        )
        profile.expanding_channel_desc = clean_str(member_data.get("expandingChannelDesc"))
        profile.card_mark = clean_str(member_data.get("cardMark"))
        profile.raw_json = member_data
        summary.profile_upserted = True

        # Ensure the parent row exists before any child row can be flushed in the same
        # transaction. This avoids MySQL FK failures on member_account/member_profile.
        self.db.flush()

        current_level = self._ensure_level_dict(
            level_id=clean_str(member_data.get("levelId")),
            level_no=clean_str(member_data.get("levelNo")),
            level_name=clean_str(member_data.get("levelName")),
            level_bit_value=clean_str(member_data.get("levelBitValue")),
            raw_json={
                "levelId": member_data.get("levelId"),
                "levelNo": member_data.get("levelNo"),
                "levelName": member_data.get("levelName"),
                "levelBitValue": member_data.get("levelBitValue"),
            },
        )

        account = self.db.scalar(select(MemberAccount).where(MemberAccount.member_id == member_id))
        if account is None:
            account = MemberAccount(member_id=member_id)
            self.db.add(account)

        account.current_level_dict_id = current_level.id if current_level else None
        account.level_id = clean_str(member_data.get("levelId"))
        account.level_name = clean_str(member_data.get("levelName"))
        account.level_bit_value = clean_str(member_data.get("levelBitValue"))
        account.level_validity_begin = parse_datetime_value(member_data.get("levelValidityBegin"))
        account.level_validity_end = parse_datetime_value(member_data.get("levelValidityEnd"))
        account.level_long_effective_flag = parse_bool_value(member_data.get("levelLongEffectiveFlag"))
        account.staff_flag = parse_bool_value(member_data.get("staffFlag"))
        if account.staff_flag is None:
            account.staff_flag = parse_bool_value(member_data.get("employeeMark"))
        account.level_no_down_flag = parse_bool_value(member_data.get("levelNoDownFlag"))
        if account.level_no_down_flag is None:
            account.level_no_down_flag = parse_bool_value(member_data.get("upDownWhiteList"))
        account.member_status = clean_str(member_data.get("memberStatus"))
        account.member_status_desc = clean_str(member_data.get("memberStatusDesc"))
        account.growth_add_up = parse_decimal_value(member_data.get("growthAddUp"))
        account.growth_balance = parse_decimal_value(member_data.get("growthBalance"))
        account.point_balance = parse_decimal_value(member_data.get("pointBalance"))
        account.member_activate = parse_bool_value(member_data.get("memberActivate"))
        account.member_abnormal = parse_bool_value(member_data.get("memberAbnormal"))
        account.raw_json = member_data
        summary.account_upserted = True

        self.db.execute(delete(MemberProfileAttr).where(MemberProfileAttr.member_id == member_id))
        meta_items = member_data.get("metaValueList") or []
        if not isinstance(meta_items, list):
            meta_items = []
        for index, item in enumerate(meta_items, start=1):
            if isinstance(item, dict):
                attr_code = build_attr_code(item, index)
                attr_name = (
                    clean_str(item.get("metaName"))
                    or clean_str(item.get("name"))
                    or clean_str(item.get("label"))
                    or attr_code
                )
                attr_value = (
                    clean_str(item.get("metaValue"))
                    or clean_str(item.get("value"))
                    or clean_str(item.get("itemValue"))
                    or clean_str(item.get("valueDesc"))
                )
                raw_json = item
            else:
                attr_code = f"meta_{index}"
                attr_name = attr_code
                attr_value = clean_str(item)
                raw_json = {"value": item}

            self.db.add(
                MemberProfileAttr(
                    member_id=member_id,
                    attr_code=attr_code,
                    attr_name=attr_name,
                    attr_value=attr_value,
                    display_order=index,
                    raw_json=raw_json,
                )
            )
            summary.attr_count += 1

        self.db.flush()
        summary.level_dict_upserts = self._level_upsert_count - starting_level_upserts

        if commit:
            self.db.commit()
        return summary

    def sync_member_level_history(
        self,
        payload: list[dict[str, Any]] | dict[str, Any],
        *,
        member_id: str | None = None,
        commit: bool = True,
    ) -> MemberSyncSummary:
        starting_level_upserts = self._level_upsert_count
        rows = extract_rows(payload)
        effective_member_id = member_id or ""
        summary = MemberSyncSummary(member_id=effective_member_id)
        affected_member_ids: set[str] = set()

        for row in rows:
            current_member_id = clean_str(row.get("memberId")) or clean_str(member_id)
            if not current_member_id:
                summary.warnings.append("Skipped a level history row because memberId is missing.")
                continue

            if not summary.member_id:
                summary.member_id = current_member_id
            affected_member_ids.add(current_member_id)

            parsed_update_time = parse_datetime_value(row.get("updateTime"))
            if row.get("updateTime") not in (None, "") and parsed_update_time is None:
                summary.warnings.append(
                    f"member_id={current_member_id} has an invalid updateTime: {row.get('updateTime')!r}"
                )

            level_dict = self._ensure_level_dict(
                level_id=None,
                level_no=clean_str(row.get("levelNo")),
                level_name=clean_str(row.get("levelName")),
                level_bit_value=None,
                raw_json={
                    "levelNo": row.get("levelNo"),
                    "levelName": row.get("levelName"),
                },
            )
            event_key = build_level_event_key(current_member_id, row, parsed_update_time)

            change_log = self.db.scalar(
                select(MemberLevelChangeLog).where(MemberLevelChangeLog.event_key == event_key)
            )
            if change_log is None:
                change_log = MemberLevelChangeLog(event_key=event_key, member_id=current_member_id)
                self.db.add(change_log)
                summary.change_logs_inserted += 1
            else:
                summary.change_logs_updated += 1

            change_log.level_dict_id = level_dict.id if level_dict else None
            change_log.pre_level_no = clean_str(row.get("preLevelNo"))
            change_log.level_no = clean_str(row.get("levelNo"))
            change_log.pre_level_name = clean_str(row.get("preLevelName"))
            change_log.level_name = clean_str(row.get("levelName"))
            change_log.remark = clean_str(row.get("remark"))
            change_log.update_time = parsed_update_time
            change_log.opt_type = clean_str(row.get("optType"))
            change_log.update_by = clean_str(row.get("updateBy"))
            change_log.update_name = clean_str(row.get("updateName"))
            change_log.raw_json = row

        self.db.flush()
        summary.level_dict_upserts = self._level_upsert_count - starting_level_upserts

        for affected_member_id in affected_member_ids:
            periods = self.refresh_member_level_periods(affected_member_id, commit=False, summary=summary)
            summary.periods_rebuilt += periods

        if commit:
            self.db.commit()
        return summary

    def refresh_member_level_periods(
        self,
        member_id: str,
        *,
        commit: bool = True,
        summary: MemberSyncSummary | None = None,
    ) -> int:
        history_logs = list(
            self.db.scalars(
                select(MemberLevelChangeLog)
                .where(MemberLevelChangeLog.member_id == member_id)
                .order_by(MemberLevelChangeLog.update_time.asc(), MemberLevelChangeLog.id.asc())
            )
        )

        valid_logs: list[MemberLevelChangeLog] = []
        for log in history_logs:
            if log.update_time is None:
                if summary is not None:
                    summary.warnings.append(
                        f"member_id={member_id} skipped a level change row because update_time is empty."
                    )
                continue

            if valid_logs and valid_logs[-1].update_time == log.update_time:
                valid_logs[-1] = log
                continue

            if valid_logs:
                same_level_no = (valid_logs[-1].level_no or "") == (log.level_no or "")
                same_level_name = (valid_logs[-1].level_name or "") == (log.level_name or "")
                if same_level_no and same_level_name:
                    continue

            valid_logs.append(log)

        self.db.execute(delete(MemberLevelPeriod).where(MemberLevelPeriod.member_id == member_id))

        if not valid_logs:
            fallback_count = self._build_fallback_current_period(member_id=member_id, summary=summary)
            if commit:
                self.db.commit()
            return fallback_count

        created = 0
        for index, log in enumerate(valid_logs):
            next_log = valid_logs[index + 1] if index + 1 < len(valid_logs) else None
            self.db.add(
                MemberLevelPeriod(
                    member_id=member_id,
                    level_dict_id=log.level_dict_id,
                    source_change_log_id=log.id,
                    level_no=log.level_no,
                    level_name=log.level_name,
                    valid_from=log.update_time,
                    valid_to=next_log.update_time if next_log else None,
                )
            )
            created += 1

        self.db.flush()
        if commit:
            self.db.commit()
        return created

    def _build_fallback_current_period(
        self,
        *,
        member_id: str,
        summary: MemberSyncSummary | None = None,
    ) -> int:
        account = self.db.scalar(select(MemberAccount).where(MemberAccount.member_id == member_id))
        if account is None or not account.level_name:
            return 0

        valid_from = account.level_validity_begin or account.updated_at
        if valid_from is None:
            return 0

        self.db.add(
            MemberLevelPeriod(
                member_id=member_id,
                level_dict_id=account.current_level_dict_id,
                source_change_log_id=None,
                level_no=None,
                level_name=account.level_name,
                valid_from=valid_from,
                valid_to=account.level_validity_end,
            )
        )
        self.db.flush()
        if summary is not None:
            summary.warnings.append(
                f"member_id={member_id} has no valid level history timestamps; "
                "generated one fallback period from member_account."
            )
        return 1

    def _ensure_level_dict(
        self,
        *,
        level_id: str | None,
        level_no: str | None,
        level_name: str | None,
        level_bit_value: str | None,
        raw_json: dict[str, Any] | None,
    ) -> MemberLevelDict | None:
        lookup_candidates = [
            ("level_id", level_id or ""),
            ("level_no", level_no or ""),
            ("level_name", level_name or ""),
        ]
        for cache_key in lookup_candidates:
            if cache_key[1] and cache_key in self._level_cache:
                return self._level_cache[cache_key]

        level: MemberLevelDict | None = None
        if level_id:
            level = self.db.scalar(select(MemberLevelDict).where(MemberLevelDict.level_id == level_id))
        if level is None and level_no:
            level = self.db.scalar(select(MemberLevelDict).where(MemberLevelDict.level_no == level_no))
        if level is None and level_name:
            level = self.db.scalar(select(MemberLevelDict).where(MemberLevelDict.level_name == level_name))

        if level is None and not any((level_id, level_no, level_name)):
            return None

        if level is None:
            level = MemberLevelDict()
            self.db.add(level)
            self._level_upsert_count += 1

        level.level_id = level.level_id or level_id
        level.level_no = level.level_no or level_no
        level.level_name = level.level_name or level_name
        if level_bit_value:
            level.level_bit_value = level_bit_value
        if raw_json:
            level.raw_json = raw_json

        self.db.flush()

        for cache_key in lookup_candidates:
            if cache_key[1]:
                self._level_cache[cache_key] = level
        return level


class ICSPMemberSyncService:
    def __init__(self, db: Session, icsp_client: ICSPClient):
        self.db = db
        self.icsp_client = icsp_client
        self.db_sync = MemberSyncService(db)

    def sync_member_by_mobile(self, mobile_no: str, *, commit: bool = True) -> MemberFetchSyncResult:
        result = MemberFetchSyncResult(requested_mobile_no=mobile_no)
        member_rows = self.icsp_client.query_members_by_mobile(mobile_no)
        if not member_rows:
            result.warnings.append(f"No member was found for mobile {mobile_no}.")
            return result

        seen_member_ids: set[str] = set()
        for row in member_rows:
            member_id = clean_str(row.get("memberId"))
            if not member_id or member_id in seen_member_ids:
                continue
            seen_member_ids.add(member_id)
            result.matched_member_ids.append(member_id)
            result.summaries.append(self._sync_single_member(member_id=member_id, member_list_row=row, commit=False))

        if commit:
            self.db.commit()
        return result

    def sync_member_by_member_id(
        self,
        member_id: str,
        *,
        member_list_row: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> MemberSyncSummary:
        summary = self._sync_single_member(member_id=member_id, member_list_row=member_list_row, commit=False)
        if commit:
            self.db.commit()
        return summary

    def rebuild_member_level_period(self, member_id: str, *, commit: bool = True) -> int:
        rebuilt_count = self.db_sync.refresh_member_level_periods(member_id, commit=False)
        if commit:
            self.db.commit()
        return rebuilt_count

    def _sync_single_member(
        self,
        *,
        member_id: str,
        member_list_row: dict[str, Any] | None,
        commit: bool,
    ) -> MemberSyncSummary:
        base_info = self.icsp_client.get_member_base_info(member_id)
        merged_detail = self._merge_member_detail(base_info=base_info, member_list_row=member_list_row)
        level_timeline = self.icsp_client.get_member_level_timeline(member_id)
        return self.db_sync.sync_member_bundle(
            member_detail_payload=merged_detail,
            level_history_payload=level_timeline,
            commit=commit,
        )

    @staticmethod
    def _merge_member_detail(
        *,
        base_info: dict[str, Any],
        member_list_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        detail = dict(member_list_row or {})
        detail.update(base_info or {})

        if "avatarUrl" not in detail and member_list_row:
            head_img_url = member_list_row.get("headImgUrl")
            if head_img_url:
                detail["avatarUrl"] = head_img_url

        if "regDate" not in detail and member_list_row and member_list_row.get("regTime") is not None:
            detail["regDate"] = member_list_row.get("regTime")

        if "expandingChannel" not in detail and member_list_row:
            expanding_channel_code = member_list_row.get("expandingChannelCode")
            if expanding_channel_code is not None:
                detail["expandingChannel"] = expanding_channel_code

        if member_list_row:
            detail["queryPageListRaw"] = member_list_row
        return detail
