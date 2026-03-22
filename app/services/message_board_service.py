from __future__ import annotations

from datetime import date

from sqlalchemy.orm import Session

from app.models.message_board import MessageBoardEntry


def _clean_text(value: str) -> str:
    return " ".join(value.split()).strip()


class MessageBoardService:
    def __init__(self, db: Session):
        self.db = db

    def create_entry(
        self,
        *,
        request_name: str,
        detail: str,
        system_name: str,
        expected_completion_date: date | None,
    ) -> MessageBoardEntry:
        normalized_request_name = _clean_text(request_name)
        normalized_system_name = _clean_text(system_name)
        normalized_detail = detail.strip()

        if len(normalized_request_name) < 2:
            raise ValueError("需求名称至少需要 2 个字符。")
        if len(normalized_system_name) < 2:
            raise ValueError("系统名称至少需要 2 个字符。")
        if len(normalized_detail) < 10:
            raise ValueError("详细描述至少需要 10 个字符。")

        entry = MessageBoardEntry(
            request_name=normalized_request_name,
            detail=normalized_detail,
            system_name=normalized_system_name,
            expected_completion_date=expected_completion_date,
            status="new",
        )
        self.db.add(entry)
        self.db.commit()
        self.db.refresh(entry)
        return entry
