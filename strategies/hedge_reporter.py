from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, Dict, Optional

import aiohttp


class HedgeMetricsReporter:
    """Async helper that pushes hedging metrics to the coordinator service."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 5.0,
        agent_id: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._agent_id = (agent_id or "").strip() or None
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        username = (auth_username or "").strip()
        password = (auth_password or "").strip()
        self._basic_auth: Optional[aiohttp.BasicAuth]
        if username or password:
            self._basic_auth = aiohttp.BasicAuth(username, password, encoding="utf-8")
        else:
            self._basic_auth = None

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
        agent_id: Optional[str] = None,
        available_balance: Optional[Decimal] = None,
        total_account_value: Optional[Decimal] = None,
    ) -> None:
        session = await self._ensure_session()
        url = f"{self._base_url}/update"
        payload: Dict[str, Any] = {
            "position": str(position),
            "total_cycles": total_cycles,
            "cumulative_pnl": str(cumulative_pnl),
            "cumulative_volume": str(cumulative_volume),
        }

        agent_identifier = (agent_id or self._agent_id or "").strip()
        if agent_identifier:
            payload["agent_id"] = agent_identifier

        if available_balance is not None:
            payload["available_balance"] = str(available_balance)

        if total_account_value is not None:
            payload["total_account_value"] = str(total_account_value)

        try:
            async with session.post(url, json=payload, auth=self._basic_auth) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise RuntimeError(f"Coordinator update failed: HTTP {response.status} {text}")
        except Exception as exc:
            raise RuntimeError(f"Failed to report hedge metrics: {exc}") from exc

    async def fetch_control(self, *, agent_id: Optional[str] = None) -> Dict[str, Any]:
        session = await self._ensure_session()
        url = f"{self._base_url}/control"
        identifier = (agent_id or self._agent_id or "").strip()
        params = {"agent_id": identifier} if identifier else None

        try:
            async with session.get(url, params=params, auth=self._basic_auth) as response:
                raw_text = await response.text()

                if response.status == 401:
                    raise RuntimeError(
                        "Coordinator control request unauthorized (check dashboard credentials)"
                    )

                if response.status >= 400:
                    raise RuntimeError(
                        f"Coordinator control request failed: HTTP {response.status} {raw_text}"
                    )

                try:
                    payload = json.loads(raw_text)
                except json.JSONDecodeError as exc:
                    preview = raw_text[:200]
                    raise RuntimeError(
                        f"Coordinator control response not valid JSON: {preview}"
                    ) from exc

                if not isinstance(payload, dict):
                    raise RuntimeError(
                        f"Unexpected coordinator control payload type: {type(payload).__name__}"
                    )

                return payload
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch coordinator control state: {exc}") from exc

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
