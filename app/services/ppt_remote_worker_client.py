from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from fastapi import HTTPException, status


class PptRemoteWorkerClient:
    def __init__(self) -> None:
        self.base_url = os.getenv("PPT_REMOTE_WORKER_URL", "").strip().rstrip("/")
        self.token = os.getenv("PPT_REMOTE_WORKER_TOKEN", "").strip()
        self.timeout = int(os.getenv("PPT_REMOTE_WORKER_TIMEOUT", "300"))

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.token:
            headers["X-PPT-Worker-Token"] = self.token
        return headers

    def start_job(self, uploads: list[Path]) -> dict[str, Any]:
        if not self.enabled:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="PPT remote worker is not configured")

        file_handles = []
        files: list[tuple[str, tuple[str, Any, str]]] = []
        try:
            for path in uploads:
                handle = path.open("rb")
                file_handles.append(handle)
                files.append(
                    (
                        "files",
                        (
                            path.name,
                            handle,
                            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        ),
                    )
                )
            response = requests.post(
                f"{self.base_url}/api/ppt/jobs",
                files=files,
                headers=self._headers(),
                timeout=self.timeout,
            )
        finally:
            for handle in file_handles:
                handle.close()

        payload = self._parse_payload(response)
        if not response.ok:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=self._extract_detail(payload) or "Failed to create PPT job on remote worker",
            )
        return payload

    def get_job(self, job_id: str) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/api/ppt/jobs/{job_id}",
            headers=self._headers(),
            timeout=self.timeout,
        )
        payload = self._parse_payload(response)
        if not response.ok:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=self._extract_detail(payload) or "Failed to query PPT job on remote worker",
            )
        return payload

    def download_file(self, job_id: str, file_name: str, target_path: Path) -> None:
        response = requests.get(
            f"{self.base_url}/api/ppt/jobs/{job_id}/downloads/{file_name}",
            headers=self._headers(),
            timeout=self.timeout,
            stream=True,
        )
        if not response.ok:
            payload = self._parse_payload(response)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=self._extract_detail(payload) or "Failed to download PPT result from remote worker",
            )

        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("wb") as buffer:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    buffer.write(chunk)

    def _parse_payload(self, response: requests.Response) -> Any:
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            try:
                return response.json()
            except Exception:
                return None
        return response.text

    def _extract_detail(self, payload: Any) -> str:
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str):
                return detail
        if isinstance(payload, str):
            return payload.strip()
        return ""
