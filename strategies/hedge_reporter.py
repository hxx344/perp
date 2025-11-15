from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Dict, Optional

import aiohttp


class HedgeMetricsReporter:
    """Async helper that pushes hedging metrics to the coordinator service."""

    def __init__(self, base_url: str, *, timeout: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=self._timeout)
                self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def report(
        self,
        *,
        position: Decimal,
        total_cycles: int,
        cumulative_pnl: Decimal,
        cumulative_volume: Decimal,
    ) -> None:
        session = await self._ensure_session()
        url = f"{self._base_url}/update"
        payload: Dict[str, Any] = {
            "position": str(position),
            "total_cycles": total_cycles,
            "cumulative_pnl": str(cumulative_pnl),
            "cumulative_volume": str(cumulative_volume),
        }

        try:
            async with session.post(url, json=payload) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise RuntimeError(f"Coordinator update failed: HTTP {response.status} {text}")
        except Exception as exc:
            raise RuntimeError(f"Failed to report hedge metrics: {exc}") from exc

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
