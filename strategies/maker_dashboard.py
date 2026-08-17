"""Local read-only dashboard for the Robinhood Lighter maker.

The dashboard deliberately has no mutation endpoints.  It is a small aiohttp
server that exposes the maker's already-assembled telemetry snapshot and a
static operational view for an operator on the same host.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aiohttp import web


SnapshotFactory = Callable[[], Dict[str, Any]]


class MarketMakerDashboard:
    """Serve a maker snapshot and the bundled dashboard page."""

    def __init__(
        self,
        snapshot_factory: SnapshotFactory,
        *,
        host: str = "127.0.0.1",
        port: int = 8788,
    ) -> None:
        self._snapshot_factory = snapshot_factory
        self.host = host
        self.port = int(port)
        self.bound_port: Optional[int] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._page_path = Path(__file__).with_name("maker_dashboard.html")

    @property
    def running(self) -> bool:
        return self._runner is not None

    async def start(self) -> None:
        if self.running:
            return

        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/dashboard", self._handle_index)
        app.router.add_get("/api/snapshot", self._handle_snapshot)
        app.router.add_get("/api/healthz", self._handle_health)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        try:
            await site.start()
        except Exception:
            await runner.cleanup()
            raise

        self._runner = runner
        self._site = site
        sockets = getattr(site, "_server", None)
        socket_list = getattr(sockets, "sockets", None) if sockets is not None else None
        self.bound_port = (
            int(socket_list[0].getsockname()[1])
            if socket_list
            else (self.port if self.port > 0 else None)
        )

    async def stop(self) -> None:
        runner = self._runner
        self._runner = None
        self._site = None
        self.bound_port = None
        if runner is not None:
            await runner.cleanup()

    async def _handle_index(self, _request: web.Request) -> web.StreamResponse:
        try:
            html = self._page_path.read_text(encoding="utf-8")
        except OSError as exc:
            return web.Response(status=500, text=f"dashboard page unavailable: {exc}")
        return web.Response(
            text=html,
            content_type="text/html",
            headers={"Cache-Control": "no-store"},
        )

    async def _handle_snapshot(self, _request: web.Request) -> web.Response:
        try:
            snapshot = self._snapshot_factory()
            # Serialize here so an accidental Decimal/enum in a future field
            # cannot produce a half-valid HTTP response.
            body = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            return web.json_response(
                {"ok": False, "error": str(exc)},
                status=503,
                headers={"Cache-Control": "no-store"},
            )
        return web.Response(
            text=body,
            content_type="application/json",
            headers={"Cache-Control": "no-store"},
        )

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response(
            {"ok": True, "dashboard": "running"},
            headers={"Cache-Control": "no-store"},
        )
