from __future__ import annotations

import hashlib
import hmac
import os
import threading
import time


RAILINLI_SHARED_SECRET = os.getenv(
    "RAILINLI_SHARED_SECRET",
    "railinli-shared-secret-20260322-v1-4f3c9d2b8a7e6c5f",
)
RAILINLI_SIGNATURE_TTL_SECONDS = int(os.getenv("RAILINLI_SIGNATURE_TTL_SECONDS", "300"))
_NONCE_CACHE_LOCK = threading.Lock()
_NONCE_CACHE: dict[str, float] = {}


def build_railinli_signature(*, timestamp: str, nonce: str, body: bytes) -> str:
    message = timestamp.encode("utf-8") + b"\n" + nonce.encode("utf-8") + b"\n" + body
    digest = hmac.new(
        RAILINLI_SHARED_SECRET.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()
    return digest


def verify_railinli_signature(*, timestamp: str, nonce: str, body: bytes, signature: str) -> tuple[bool, str | None]:
    try:
        timestamp_value = int(timestamp)
    except (TypeError, ValueError):
        return False, "timestamp invalid"

    now = int(time.time())
    if abs(now - timestamp_value) > RAILINLI_SIGNATURE_TTL_SECONDS:
        return False, "timestamp expired"

    if not nonce or len(nonce) < 8:
        return False, "nonce invalid"

    if _is_replayed_nonce(nonce=nonce, timestamp_value=timestamp_value):
        return False, "nonce replayed"

    expected = build_railinli_signature(timestamp=timestamp, nonce=nonce, body=body)
    if not hmac.compare_digest(expected, signature or ""):
        return False, "signature invalid"
    return True, None


def _is_replayed_nonce(*, nonce: str, timestamp_value: int) -> bool:
    now = time.time()
    expires_at = now + RAILINLI_SIGNATURE_TTL_SECONDS
    with _NONCE_CACHE_LOCK:
        expired = [key for key, value in _NONCE_CACHE.items() if value <= now]
        for key in expired:
            _NONCE_CACHE.pop(key, None)
        if nonce in _NONCE_CACHE:
            return True
        _NONCE_CACHE[nonce] = expires_at
    return False
