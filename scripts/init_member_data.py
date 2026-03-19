from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import func, select


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.models.member import (
    MemberAccount,
    MemberLevelChangeLog,
    MemberLevelPeriod,
    MemberProfile,
    MemberProfileAttr,
)
from app.services.member_sync_service import MemberSyncService


SAMPLE_MEMBER_DETAIL: dict[str, Any] = {
    "memberId": "10000001",
    "memberName": "张三",
    "mobileNo": "13800138000",
    "email": "zhangsan@example.com",
    "avatarUrl": "https://example.com/avatar/10000001.png",
    "levelId": "L003",
    "levelName": "金卡",
    "levelBitValue": "4",
    "levelValidityBegin": 1735689600000,
    "levelValidityEnd": 1767225599000,
    "levelLongEffectiveFlag": 0,
    "staffFlag": 0,
    "levelNoDownFlag": 1,
    "memberStatus": "NORMAL",
    "memberStatusDesc": "正常",
    "favPlazaCode": "P001",
    "favPlazaName": "浦东广场",
    "favPlazaBuId": "901",
    "belongPlazaName": "浦东广场",
    "belongPlazaCode": "P001",
    "belongPlazaBuId": "901",
    "regPlazaName": "徐汇广场",
    "regPlazaCode": "P002",
    "regPlazaBuId": "902",
    "expandingChannel": "WX",
    "expandingChannelDesc": "微信注册",
    "sex": "F",
    "regDate": "2024-05-20 10:30:00",
    "birthday": "1992-08-16",
    "growthAddUp": 1260,
    "growthBalance": 360,
    "pointBalance": 8800,
    "memberActivate": 1,
    "metaValueList": [
        {"metaKey": "marital_status", "metaName": "婚否", "metaValue": "已婚"},
        {"metaKey": "children_status", "metaName": "子女状况", "metaValue": "二孩"},
    ],
    "cardMark": "实体卡",
    "memberAbnormal": 0,
}

SAMPLE_LEVEL_HISTORY: list[dict[str, Any]] = [
    {
        "memberId": "10000001",
        "preLevelNo": "L001",
        "levelNo": "L002",
        "preLevelName": "普卡",
        "levelName": "银卡",
        "remark": "升级到银卡",
        "updateTime": "2024-06-01 09:00:00",
        "optType": "UPGRADE",
        "updateBy": "system",
        "updateName": "系统任务",
    },
    {
        "memberId": "10000001",
        "preLevelNo": "L002",
        "levelNo": "L003",
        "preLevelName": "银卡",
        "levelName": "金卡",
        "remark": "升级到金卡",
        "updateTime": "2024-12-15 15:30:00",
        "optType": "UPGRADE",
        "updateBy": "system",
        "updateName": "系统任务",
    },
]


def load_json_file(file_path: str | None) -> Any:
    if not file_path:
        return None
    with open(file_path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialize member info sample data.")
    parser.add_argument("--detail-file", help="Path to member detail JSON file.")
    parser.add_argument("--history-file", help="Path to member level history JSON file.")
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create tables directly from ORM metadata for local smoke testing.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.create_tables:
        Base.metadata.create_all(bind=engine)

    member_detail = load_json_file(args.detail_file) or SAMPLE_MEMBER_DETAIL
    level_history = load_json_file(args.history_file) or SAMPLE_LEVEL_HISTORY

    with SessionLocal() as session:
        service = MemberSyncService(session)
        summary = service.sync_member_bundle(
            member_detail_payload=member_detail,
            level_history_payload=level_history,
            commit=True,
        )

        profile = session.get(MemberProfile, summary.member_id)
        account = session.scalar(select(MemberAccount).where(MemberAccount.member_id == summary.member_id))
        attr_count = session.scalar(
            select(func.count()).select_from(MemberProfileAttr).where(MemberProfileAttr.member_id == summary.member_id)
        )
        change_log_count = session.scalar(
            select(func.count())
            .select_from(MemberLevelChangeLog)
            .where(MemberLevelChangeLog.member_id == summary.member_id)
        )
        period_rows = list(
            session.scalars(
                select(MemberLevelPeriod)
                .where(MemberLevelPeriod.member_id == summary.member_id)
                .order_by(MemberLevelPeriod.valid_from.asc())
            )
        )

    print("=== Member Sync Summary ===")
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
    print("\n=== Verification ===")
    print(f"member_id: {summary.member_id}")
    print(f"profile_exists: {profile is not None}")
    print(f"account_level: {account.level_name if account else None}")
    print(f"profile_attr_count: {attr_count}")
    print(f"level_change_log_count: {change_log_count}")
    print(f"level_period_count: {len(period_rows)}")
    print("level_periods:")
    for row in period_rows:
        print(
            f"  - level={row.level_name}, valid_from={row.valid_from}, valid_to={row.valid_to}"
        )


if __name__ == "__main__":
    main()
