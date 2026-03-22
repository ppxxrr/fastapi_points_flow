from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable

from app.services.parking_api_sync_service import (
    PARKING_MANAGEMENT_ORIGIN,
    PARKING_MERCHANT_ID,
    PARKING_USE_LIVE_CHROME_AUTH,
    REQUEST_TIMEOUT,
    RateLimiter,
    _build_encoded_url,
    _clean_text,
    _noop_logger,
    _utc_range_for_local_date,
    ParkingApiClient,
)


LoggerCallback = Callable[[str, str], None] | None

PAYMENT_TRADE_ENDPOINT = "https://mapi.4pyun.com/rest/2.0/payment/trade/list"
PAYMENT_TRADE_REFERER_URL = os.getenv(
    "PARKING_TRADE_REFERER_URL",
    "https://mch.4pyun.com/trading-center/trade/payment",
)
PAYMENT_TRADE_PAGE_SIZE = int(os.getenv("PARKING_TRADE_PAGE_SIZE", "99"))
PAYMENT_TRADE_RATE_PER_SEC = float(os.getenv("PARKING_TRADE_RATE_PER_SEC", "5"))
PAYMENT_TRADE_WORKERS = int(os.getenv("PARKING_TRADE_WORKERS", "4"))


@dataclass(slots=True)
class ParkingTradeApiRequestStats:
    target_date: str
    fetched_rows: int = 0
    total_pages: int = 0
    total_count: int = 0
    duplicate_trade_id_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_date": self.target_date,
            "fetched_rows": self.fetched_rows,
            "total_pages": self.total_pages,
            "total_count": self.total_count,
            "duplicate_trade_id_count": self.duplicate_trade_id_count,
        }


class ParkingTradeApiClient(ParkingApiClient):
    def __init__(self, logger: LoggerCallback = None) -> None:
        super().__init__(logger=logger)
        self._trade_stats: dict[date, ParkingTradeApiRequestStats] = {}

    def fetch_rows_for_dates(
        self,
        target_dates: set[date],
    ) -> dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]]:
        authorization = self.ensure_authorization()
        if self._auth_source in {"chrome", "chrome_profile_storage"}:
            self._start_keepalive_if_needed()

        grouped: dict[date, list[tuple[dict[str, Any], str | Path | None, int | None]]] = {}
        for target_date in sorted(target_dates):
            rows = self._fetch_single_date_rows(target_date, authorization)
            virtual_path = Path(f"parking_trade_api_{target_date.isoformat()}.virtual")
            grouped[target_date] = [(row, virtual_path, index) for index, row in enumerate(rows, start=1)]
        return grouped

    def get_stats(self) -> dict[str, Any]:
        return {value.isoformat(): stats.to_dict() for value, stats in self._trade_stats.items()}

    def _fetch_single_date_rows(self, target_date: date, authorization: str) -> list[dict[str, Any]]:
        limiter = RateLimiter(PAYMENT_TRADE_RATE_PER_SEC)
        start_time, end_time = _utc_range_for_local_date(target_date)
        base_params = {
            "merchant": PARKING_MERCHANT_ID,
            "start_time": start_time,
            "end_time": end_time,
            "sort_by": "result_time",
            "sort_direction": "DESC",
        }
        headers = self._build_headers(authorization=authorization, referer=PAYMENT_TRADE_REFERER_URL)
        first_header, first_rows = self._request_page(
            endpoint=PAYMENT_TRADE_ENDPOINT,
            headers=headers,
            params=base_params,
            page_index=1,
            page_size=PAYMENT_TRADE_PAGE_SIZE,
            limiter=limiter,
            label=f"parking-trade:{target_date.isoformat()}",
        )
        total_count = int(first_header.get("total_count") or 0)
        total_pages = int(first_header.get("page_count") or 0) or max(1, math.ceil(total_count / PAYMENT_TRADE_PAGE_SIZE))
        all_rows = list(first_rows)
        if total_pages > 1:
            for page_index in range(2, total_pages + 1):
                _, page_rows = self._request_page(
                    endpoint=PAYMENT_TRADE_ENDPOINT,
                    headers=headers,
                    params=base_params,
                    page_index=page_index,
                    page_size=PAYMENT_TRADE_PAGE_SIZE,
                    limiter=limiter,
                    label=f"parking-trade:{target_date.isoformat()}",
                )
                all_rows.extend(page_rows)

        duplicate_trade_ids = 0
        seen: set[str] = set()
        converted: list[dict[str, Any]] = []
        for row in sorted(all_rows, key=lambda item: (_clean_text(item.get("result_time")), _clean_text(item.get("id")))):
            trade_id = _clean_text(row.get("id"))
            if trade_id:
                if trade_id in seen:
                    duplicate_trade_ids += 1
                    continue
                seen.add(trade_id)
            converted.append(dict(row))

        stats = ParkingTradeApiRequestStats(
            target_date=target_date.isoformat(),
            fetched_rows=len(converted),
            total_pages=total_pages,
            total_count=total_count,
            duplicate_trade_id_count=duplicate_trade_ids,
        )
        self._trade_stats[target_date] = stats
        self.logger(
            "INFO",
            f"parking trade api fetched {target_date.isoformat()} rows={stats.fetched_rows} total_count={stats.total_count} pages={stats.total_pages} duplicates={stats.duplicate_trade_id_count}",
        )
        return converted

    def _build_headers(self, *, authorization: str, referer: str) -> dict[str, str]:
        return {
            "accept": "application/json, text/plain, */*",
            "authorization": authorization,
            "content-type": "application/x-www-form-urlencoded",
            "origin": PARKING_MANAGEMENT_ORIGIN,
            "referer": referer,
            "user-agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
        }
