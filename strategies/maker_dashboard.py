"""Local read-only dashboard for the Robinhood Lighter maker.

The dashboard deliberately has no mutation endpoints.  It is a small aiohttp
server that exposes the maker's already-assembled telemetry snapshot and a
static operational view for an operator on the same host.  Keep it bound to
loopback and put an authenticated HTTPS reverse proxy in front of it when
remote access is required; this process must never serve trading credentials.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aiohttp import web


SnapshotFactory = Callable[[], Dict[str, Any]]


# These headers are applied to every response, including framework-generated
# errors.  TLS and authentication belong at the public reverse proxy (see the
# dashboard deployment guide), while the local listener remains HTTP/loopback.
_SECURITY_HEADERS = {
    "Cache-Control": "no-store, max-age=0",
    "Pragma": "no-cache",
    "Content-Security-Policy": (
        "default-src 'none'; base-uri 'none'; object-src 'none'; "
        "frame-ancestors 'none'; form-action 'none'; "
        "script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; font-src 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
    "Cross-Origin-Resource-Policy": "same-origin",
}


async def _add_security_headers(
    _request: web.Request,
    response: web.StreamResponse,
) -> None:
    """Apply defense-in-depth headers without changing proxy TLS ownership."""

    for name, value in _SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)


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

        if self.host.strip().casefold() not in {"127.0.0.1", "::1"}:
            raise RuntimeError(
                "The dashboard only serves loopback HTTP; use an authenticated "
                "HTTPS reverse proxy for remote access"
            )

        app = web.Application()
        # on_response_prepare also covers aiohttp's generated 4xx/5xx pages.
        app.on_response_prepare.append(_add_security_headers)
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
        )

    async def _handle_snapshot(self, _request: web.Request) -> web.Response:
        try:
            snapshot = self._snapshot_factory()
            # Serialize here so an accidental Decimal/enum in a future field
            # cannot produce a half-valid HTTP response.
            body = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        except Exception:  # pragma: no cover - defensive HTTP boundary
            return web.json_response(
                {"ok": False, "error": "dashboard snapshot unavailable"},
                status=503,
            )
        return web.Response(
            text=body,
            content_type="application/json",
        )

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response(
            {"ok": True, "dashboard": "running"},
        )
