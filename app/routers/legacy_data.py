from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.db.session import SessionLocal
from app.services.legacy_flow_service import LegacyFlowService, RAILINLI_RECEIVER_TOKEN
from app.utils.railinli_security import verify_railinli_signature


router = APIRouter(tags=["legacy-data"])


class RailinliUploadRequest(BaseModel):
    token: str = Field(..., min_length=1)
    probe_id: str = Field(..., min_length=1, max_length=64)
    probe_name: str | None = Field(default=None, max_length=255)
    entry_count: int = Field(..., ge=0)
    date: date


@router.post("/upload")
async def upload_railinli_probe_daily_flow(request: Request) -> dict[str, str]:
    body = await request.body()
    timestamp = request.headers.get("X-Railinli-Timestamp", "").strip()
    nonce = request.headers.get("X-Railinli-Nonce", "").strip()
    signature = request.headers.get("X-Railinli-Signature", "").strip()
    verified, error = verify_railinli_signature(
        timestamp=timestamp,
        nonce=nonce,
        body=body,
        signature=signature,
    )
    if not verified:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error or "signature invalid")

    try:
        payload = RailinliUploadRequest.model_validate_json(body)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"payload invalid: {exc}") from exc

    if payload.token != RAILINLI_RECEIVER_TOKEN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="token invalid")

    with SessionLocal() as session:
        LegacyFlowService(session).upsert_railinli_upload(
            probe_id=payload.probe_id.strip(),
            probe_name=payload.probe_name.strip() if payload.probe_name else None,
            entry_count=payload.entry_count,
            business_date=payload.date,
            raw_payload=payload.model_dump(mode="json"),
        )
    return {"status": "success", "message": "uploaded"}
