"""Backpack public bookTicker WS cache (L1 bid/ask).

This module is intentionally standalone to avoid adding large/new classes into
`hedge_coordinator.py` (which is huge and easy to break by indentation mistakes).

Public stream:
- wss://ws.backpack.exchange
- subscribe message: {"method": "SUBSCRIBE", "params": ["bookTicker.<symbol>"]}
- payload (wrapped): {"stream": "bookTicker.<symbol>", "data": {"s":..., "b":..., "B":..., "a":..., "A":...}}

We only maintain L1 (bid/ask + qty) per symbol.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Set, Tuple

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None  # type: ignore


@dataclass
class BookTickerQuote:
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    ts: float


class BackpackBookTickerWS:
    """Best-effort WS cache for Backpack `bookTicker.<symbol>`."""

    ws_url = "wss://ws.backpack.exchange"

    def __init__(self) -> None:
        if websockets is None:
            raise RuntimeError("websockets package not available")
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._lock = asyncio.Lock()
        self._want: Set[str] = set()
        self._subscribed: Set[str] = set()
        self._quotes: Dict[str, BookTickerQuote] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="bp_bookticker_ws")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        if task:
            task.cancel()
            with suppress(Exception):
                await task
        async with self._lock:
            ws = self._ws
            self._ws = None
            self._subscribed.clear()
        if ws:
            with suppress(Exception):
                await ws.close()

    def last_update_ts(self, symbol: str) -> Optional[float]:
        q = self._quotes.get(symbol)
        return q.ts if q else None

    async def ensure_symbol(self, symbol: str) -> None:
        sym = str(symbol or "").strip()
        if not sym:
            return
        async with self._lock:
            self._want.add(sym)
            ws = self._ws
            if not ws:
                return
            if sym in self._subscribed:
                return
            msg = {"method": "SUBSCRIBE", "params": [f"bookTicker.{sym}"]}
            await ws.send(json.dumps(msg))
            self._subscribed.add(sym)

    async def get_quote(self, symbol: str) -> Optional[Tuple[Decimal, Decimal, Decimal, Decimal]]:
        sym = str(symbol or "").strip()
        if not sym:
            return None
        # Fire-and-forget subscription in the background task.
        with suppress(Exception):
            await self.ensure_symbol(sym)
        q = self._quotes.get(sym)
        if not q:
            return None
        return (q.bid, q.bid_qty, q.ask, q.ask_qty)

    async def _resubscribe_all(self) -> None:
        async with self._lock:
            ws = self._ws
            want = set(self._want)
            self._subscribed.clear()
        if not ws:
            return
        for sym in want:
            with suppress(Exception):
                msg = {"method": "SUBSCRIBE", "params": [f"bookTicker.{sym}"]}
                await ws.send(json.dumps(msg))
            async with self._lock:
                self._subscribed.add(sym)

    async def _run(self) -> None:
        assert websockets is not None
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=50, ping_timeout=20) as ws:  # type: ignore[attr-defined]
                    async with self._lock:
                        self._ws = ws
                        self._subscribed.clear()
                    backoff = 1.0

                    # Re-subscribe already requested symbols.
                    await self._resubscribe_all()

                    while not self._stop.is_set():
                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        stream = msg.get("stream")
                        data = msg.get("data")
                        if not isinstance(stream, str) or not isinstance(data, dict):
                            continue
                        if not stream.startswith("bookTicker."):
                            continue

                        try:
                            sym = str(data.get("s") or "").strip()
                            if not sym:
                                continue
                            bid = Decimal(str(data.get("b") or "0"))
                            bid_qty = Decimal(str(data.get("B") or "0"))
                            ask = Decimal(str(data.get("a") or "0"))
                            ask_qty = Decimal(str(data.get("A") or "0"))
                        except Exception:
                            continue

                        self._quotes[sym] = BookTickerQuote(
                            bid=bid,
                            bid_qty=bid_qty,
                            ask=ask,
                            ask_qty=ask_qty,
                            ts=time.time(),
                        )

            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(min(10.0, backoff))
                backoff = min(10.0, backoff * 2)
            finally:
                async with self._lock:
                    self._ws = None
