from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.db.session import SessionLocal
from app.schemas import MessageBoardEntryCreateRequest, MessageBoardEntryCreateResponse
from app.services.message_board_service import MessageBoardService
from app.utils.error_text import normalize_error_text


router = APIRouter(prefix="/api/message-board", tags=["message-board"])


@router.post("/entries", response_model=MessageBoardEntryCreateResponse, status_code=status.HTTP_201_CREATED)
def create_message_board_entry(payload: MessageBoardEntryCreateRequest) -> MessageBoardEntryCreateResponse:
    try:
        with SessionLocal() as session:
            service = MessageBoardService(session)
            entry = service.create_entry(
                request_name=payload.request_name,
                detail=payload.detail,
                system_name=payload.system_name,
                expected_completion_date=payload.expected_completion_date,
            )
        return MessageBoardEntryCreateResponse(
            id=entry.id,
            status=entry.status,
            created_at=entry.created_at.isoformat(sep=" ", timespec="seconds"),
            message="留言已提交，我们会将其纳入需求评估。",
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=normalize_error_text(exc)) from exc
