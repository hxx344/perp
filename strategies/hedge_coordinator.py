#!/usr/bin/env python3
"""Minimal coordination service for the Aster–Lighter hedging loop.

This module exposes a very small HTTP API that the hedging executor can use to
publish run-state metrics (current position, cumulative PnL/volume, cycle count).
A lightweight dashboard served from ``/dashboard`` visualises these values.

Usage
-----

.. code-block:: bash

    python strategies/hedge_coordinator.py --host 0.0.0.0 --port 8899

The hedging bot can then be started with ``--coordinator-url http://host:8899``
so that it will POST metrics to ``/update`` after every cycle and during
shutdown.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web

BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "hedge_dashboard.html"

LOGGER = logging.getLogger("hedge.coordinator")


@dataclass
class HedgeState:
    """Shared mutable state tracked by the coordinator."""

    position: Decimal = Decimal("0")
    total_cycles: int = 0
    cumulative_pnl: Decimal = Decimal("0")
    cumulative_volume: Decimal = Decimal("0")
    last_update_ts: float = field(default_factory=time.time)

    def serialize(self) -> Dict[str, Any]:
        return {
            "position": str(self.position),
            "total_cycles": self.total_cycles,
            "cumulative_pnl": str(self.cumulative_pnl),
            "cumulative_volume": str(self.cumulative_volume),
            "last_update_ts": self.last_update_ts,
        }


class HedgeCoordinator:
    """aiohttp based coordinator that stores the latest hedging metrics."""

    def __init__(self) -> None:
        self._state = HedgeState()
        self._lock = asyncio.Lock()

    async def update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        position_raw = payload.get("position")
        cycles_raw = payload.get("total_cycles")
        pnl_raw = payload.get("cumulative_pnl")
        volume_raw = payload.get("cumulative_volume")

        async with self._lock:
            try:
                if position_raw is not None:
                    self._state.position = Decimal(str(position_raw))
            except Exception:
                LOGGER.warning("Invalid position payload: %s", position_raw)
            try:
                if cycles_raw is not None:
                    self._state.total_cycles = int(cycles_raw)
            except Exception:
                LOGGER.warning("Invalid cycle payload: %s", cycles_raw)
            try:
                if pnl_raw is not None:
                    self._state.cumulative_pnl = Decimal(str(pnl_raw))
            except Exception:
                LOGGER.warning("Invalid pnl payload: %s", pnl_raw)
            try:
                if volume_raw is not None:
                    self._state.cumulative_volume = Decimal(str(volume_raw))
            except Exception:
                LOGGER.warning("Invalid volume payload: %s", volume_raw)

            self._state.last_update_ts = time.time()
            return self._state.serialize()

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return self._state.serialize()


class CoordinatorApp:
    def __init__(self) -> None:
        self._coordinator = HedgeCoordinator()
        self._app = web.Application()
        self._app.add_routes(
            [
                web.get("/", self.handle_dashboard_redirect),
                web.get("/dashboard", self.handle_dashboard),
                web.get("/metrics", self.handle_metrics),
                web.post("/update", self.handle_update),
            ]
        )

    @property
    def app(self) -> web.Application:
        return self._app

    async def handle_dashboard_redirect(self, request: web.Request) -> web.Response:
        raise web.HTTPFound("/dashboard")

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        if not DASHBOARD_PATH.exists():
            raise web.HTTPNotFound(text="dashboard asset missing; ensure hedge_dashboard.html exists")
        return web.FileResponse(path=DASHBOARD_PATH)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        payload = await self._coordinator.snapshot()
        return web.json_response(payload)

    async def handle_update(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="update payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="update payload must be an object")

        state = await self._coordinator.update(body)
        return web.json_response(state)


async def _run_app(args: argparse.Namespace) -> None:
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    coordinator_app = CoordinatorApp()
    runner = web.AppRunner(coordinator_app.app)
    await runner.setup()

    site = web.TCPSite(runner, host=args.host, port=args.port)
    await site.start()

    LOGGER.info("hedge coordinator listening on %s:%s", args.host, args.port)

    stop_event = asyncio.Event()

    def _shutdown_handler() -> None:
        LOGGER.info("received termination signal; shutting down coordinator")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown_handler)

    await stop_event.wait()

    await runner.cleanup()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the hedging metrics coordinator")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address for the HTTP server")
    parser.add_argument("--port", type=int, default=8899, help="Port for the HTTP server")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run_app(args))
    except KeyboardInterrupt:
        LOGGER.warning("Coordinator interrupted by user")


if __name__ == "__main__":
    main()
