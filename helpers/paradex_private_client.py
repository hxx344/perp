"""Small private REST client for Paradex endpoints we don't want to add to paradex-py.

Currently used for algo history fallbacks so TWAP final fills (when OPEN list becomes empty)
can still update the dashboard.

We intentionally keep this wrapper minimal and requests-based.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class ParadexPrivateClientConfig:
    base_url: str
    jwt_token: str
    timeout_seconds: float = 10.0


class ParadexPrivateClient:
    def __init__(self, cfg: ParadexPrivateClientConfig):
        self._cfg = cfg

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cfg.jwt_token}",
            "Accept": "application/json",
        }

    def fetch_algo_orders_history(
        self,
        *,
        status: Optional[str] = None,
        market: Optional[str] = None,
        side: Optional[str] = None,
        algo_type: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /v1/algo/orders-history

        Returns JSON dict like {"results": [...], "next": "..."} (shape per API).
        """

        url = f"{self._cfg.base_url.rstrip('/')}/v1/algo/orders-history"
        params: Dict[str, Any] = {}
        if status:
            params["status"] = status
        if market:
            params["market"] = market
        if side:
            params["side"] = side
        if algo_type:
            params["algo_type"] = algo_type
        if start is not None:
            params["start"] = int(start)
        if end is not None:
            params["end"] = int(end)
        if cursor:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = int(limit)

        resp = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=self._cfg.timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {"results": data}
