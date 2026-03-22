from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock

import requests


CAPTCHA_TTL_SECONDS = 300


@dataclass(slots=True)
class ParkingCaptchaChallenge:
    project_key: str
    captcha_uuid: str
    cookies: dict[str, str]
    created_at: float


class ParkingCaptchaChallengeStore:
    def __init__(self) -> None:
        self._items: dict[str, ParkingCaptchaChallenge] = {}
        self._lock = Lock()

    def save(self, *, project_key: str, captcha_uuid: str, cookies: dict[str, str]) -> None:
        with self._lock:
            self._cleanup_locked()
            self._items[captcha_uuid] = ParkingCaptchaChallenge(
                project_key=project_key,
                captcha_uuid=captcha_uuid,
                cookies=dict(cookies),
                created_at=time.time(),
            )

    def get(self, captcha_uuid: str) -> ParkingCaptchaChallenge | None:
        with self._lock:
            self._cleanup_locked()
            return self._items.get(captcha_uuid)

    def _cleanup_locked(self) -> None:
        now = time.time()
        expired = [key for key, item in self._items.items() if now - item.created_at > CAPTCHA_TTL_SECONDS]
        for key in expired:
            self._items.pop(key, None)


parking_captcha_challenge_store = ParkingCaptchaChallengeStore()


def export_captcha_session_cookies(session: requests.Session) -> dict[str, str]:
    return requests.utils.dict_from_cookiejar(session.cookies)
