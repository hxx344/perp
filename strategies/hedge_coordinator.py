#!/usr/bin/env python3
"""Minimal coordination service for the Aster–Lighter hedging loop.

This module exposes a very small HTTP API that the hedging executor can use to
publish run-state metrics (current position, cumulative PnL/volume, cycle count).
A lightweight dashboard served from ``/dashboard`` visualises these values.

Usage
-----

.. code-block:: bash

    python strategies/hedge_coordinator.py --host 0.0.0.0 --port 8899

Optional HTTP Basic authentication for the dashboard can be enabled with::

    python strategies/hedge_coordinator.py --dashboard-username admin --dashboard-password secret

The hedging bot can then be started with ``--coordinator-url http://host:8899``
so that it will POST metrics to ``/update`` after every cycle and during
shutdown.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
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

    agent_id: Optional[str] = None
    position: Decimal = Decimal("0")
    total_cycles: int = 0
    cumulative_pnl: Decimal = Decimal("0")
    cumulative_volume: Decimal = Decimal("0")
    available_balance: Decimal = Decimal("0")
    account_value: Decimal = Decimal("0")
    instrument: Optional[str] = None
    depths: Dict[str, int] = field(default_factory=dict)
    last_update_ts: float = field(default_factory=time.time)

    def update_from_payload(self, payload: Dict[str, Any]) -> None:
        position_raw = payload.get("position")
        cycles_raw = payload.get("total_cycles")
        pnl_raw = payload.get("cumulative_pnl")
        volume_raw = payload.get("cumulative_volume")
        available_raw = payload.get("available_balance")
        account_value_raw = payload.get("total_account_value")
        if account_value_raw is None:
            account_value_raw = payload.get("total_asset_value")
        instrument_raw = payload.get("instrument") or payload.get("instrument_label")
        depths_raw = payload.get("depths")
        maker_depth_raw = payload.get("maker_depth")

        if position_raw is not None:
            try:
                self.position = Decimal(str(position_raw))
            except Exception:
                LOGGER.warning("Invalid position payload: %s", position_raw)

        if cycles_raw is not None:
            try:
                self.total_cycles = int(cycles_raw)
            except Exception:
                LOGGER.warning("Invalid cycle payload: %s", cycles_raw)

        if pnl_raw is not None:
            try:
                self.cumulative_pnl = Decimal(str(pnl_raw))
            except Exception:
                LOGGER.warning("Invalid pnl payload: %s", pnl_raw)

        if volume_raw is not None:
            try:
                self.cumulative_volume = Decimal(str(volume_raw))
            except Exception:
                LOGGER.warning("Invalid volume payload: %s", volume_raw)

        if available_raw is not None:
            try:
                self.available_balance = Decimal(str(available_raw))
            except Exception:
                LOGGER.warning("Invalid available balance payload: %s", available_raw)

        if account_value_raw is not None:
            try:
                self.account_value = Decimal(str(account_value_raw))
            except Exception:
                LOGGER.warning("Invalid account value payload: %s", account_value_raw)

        if instrument_raw is not None:
            try:
                text = str(instrument_raw).strip()
            except Exception:
                text = ""
            if text:
                self.instrument = text

        updated_depths: Dict[str, int] = {}
        if isinstance(depths_raw, dict):
            for key, value in depths_raw.items():
                try:
                    updated_depths[str(key)] = int(value)
                except Exception:
                    continue
        elif maker_depth_raw is not None:
            try:
                updated_depths["maker"] = int(maker_depth_raw)
            except Exception:
                pass

        if updated_depths:
            self.depths = updated_depths

        self.last_update_ts = time.time()

    def serialize(self) -> Dict[str, Any]:
        payload = {
            "position": str(self.position),
            "total_cycles": self.total_cycles,
            "cumulative_pnl": str(self.cumulative_pnl),
            "cumulative_volume": str(self.cumulative_volume),
            "available_balance": str(self.available_balance),
            "total_account_value": str(self.account_value),
            "instrument": self.instrument,
            "depths": self.depths,
            "last_update_ts": self.last_update_ts,
        }
        if self.agent_id is not None:
            payload["agent_id"] = self.agent_id
        return payload

    @classmethod
    def aggregate(cls, states: Dict[str, "HedgeState"]) -> "HedgeState":
        aggregate = cls(agent_id="aggregate")
        aggregate.last_update_ts = 0.0
        instruments: set[str] = set()
        for state in states.values():
            aggregate.position += state.position
            aggregate.total_cycles += int(state.total_cycles)
            aggregate.cumulative_pnl += state.cumulative_pnl
            aggregate.cumulative_volume += state.cumulative_volume
            aggregate.available_balance += state.available_balance
            aggregate.account_value += state.account_value
            if state.instrument:
                instruments.add(state.instrument)
            if state.last_update_ts > aggregate.last_update_ts:
                aggregate.last_update_ts = state.last_update_ts

        if instruments:
            aggregate.instrument = ", ".join(sorted(instruments))

        if aggregate.last_update_ts == 0.0:
            aggregate.last_update_ts = time.time()
        return aggregate


class HedgeCoordinator:
    """aiohttp based coordinator that stores the latest hedging metrics."""

    def __init__(self) -> None:
        self._states: Dict[str, HedgeState] = {}
        self._lock = asyncio.Lock()
        self._eviction_seconds = 6 * 3600  # prune entries idle for 6 hours
        self._stale_warning_seconds = 5 * 60  # tag agents as stale after 5 minutes
        self._last_agent_id: Optional[str] = None
        self._controls: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _normalize_agent_id(raw: Any) -> str:
        if raw is None:
            return "default"
        try:
            text = str(raw).strip()
        except Exception:
            text = ""
        if not text:
            return "default"
        if len(text) > 120:
            text = text[:120]
        return text

    def _prune_stale(self, now: float) -> None:
        if self._eviction_seconds <= 0:
            return
        to_remove = [
            agent_id
            for agent_id, state in self._states.items()
            if now - state.last_update_ts > self._eviction_seconds
        ]
        for agent_id in to_remove:
            LOGGER.info("Pruning stale agent '%s' after %.0f seconds of inactivity", agent_id, self._eviction_seconds)
            self._states.pop(agent_id, None)
            self._controls.pop(agent_id, None)

    def _ensure_control(self, agent_id: str) -> Dict[str, Any]:
        control = self._controls.get(agent_id)
        if control is None:
            control = {"paused": False, "updated_at": None}
            self._controls[agent_id] = control
        return control

    def _serialize_control(self, agent_id: str, control: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "agent_id": agent_id,
            "paused": bool(control.get("paused", False)),
            "updated_at": control.get("updated_at"),
        }

    def _controls_snapshot(self) -> Dict[str, Any]:
        controls_payload = {
            agent_id: self._serialize_control(agent_id, control)
            for agent_id, control in self._controls.items()
        }
        paused_agents = [agent_id for agent_id, control in controls_payload.items() if control.get("paused")]
        return {
            "controls": controls_payload,
            "paused_agents": paused_agents,
        }

    async def set_agent_paused(self, agent_id: str, paused: bool) -> Dict[str, Any]:
        normalized = self._normalize_agent_id(agent_id)
        async with self._lock:
            control = self._ensure_control(normalized)
            control["paused"] = bool(paused)
            control["updated_at"] = time.time()
            LOGGER.info("Coordinator control update: agent=%s paused=%s", normalized, control["paused"])
            snapshot = self._controls_snapshot()
            snapshot["agent"] = self._serialize_control(normalized, control)
            return snapshot

    async def toggle_agent_pause(self, agent_id: str) -> Dict[str, Any]:
        normalized = self._normalize_agent_id(agent_id)
        async with self._lock:
            control = self._ensure_control(normalized)
            control["paused"] = not bool(control.get("paused", False))
            control["updated_at"] = time.time()
            LOGGER.info("Coordinator control toggle: agent=%s paused=%s", normalized, control["paused"])
            snapshot = self._controls_snapshot()
            snapshot["agent"] = self._serialize_control(normalized, control)
            return snapshot

    async def control_snapshot(self, agent_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            snapshot = self._controls_snapshot()
            if agent_id is not None:
                normalized = self._normalize_agent_id(agent_id)
                control = self._ensure_control(normalized)
                snapshot["agent"] = self._serialize_control(normalized, control)
            return snapshot

    def _build_snapshot(self, now: float) -> Dict[str, Any]:
        aggregate = HedgeState.aggregate(self._states)
        agents_payload = {agent_id: state.serialize() for agent_id, state in self._states.items()}

        for agent_id, payload in agents_payload.items():
            control = self._controls.get(agent_id)
            if control is not None:
                payload["paused"] = bool(control.get("paused", False))

        snapshot = aggregate.serialize()
        snapshot.pop("agent_id", None)
        snapshot["agents"] = agents_payload
        snapshot["agent_count"] = len(agents_payload)
        snapshot["instruments"] = sorted({state.instrument for state in self._states.values() if state.instrument})
        snapshot["stale_agents"] = [
            agent_id
            for agent_id, state in self._states.items()
            if now - state.last_update_ts > self._stale_warning_seconds
        ]
        snapshot["last_agent_id"] = self._last_agent_id
        controls_snapshot = self._controls_snapshot()
        snapshot.update(controls_snapshot)
        return snapshot

    async def update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            agent_id = self._normalize_agent_id(payload.get("agent_id"))
            state = self._states.get(agent_id)
            if state is None:
                state = HedgeState(agent_id=agent_id)
                self._states[agent_id] = state

            state.update_from_payload(payload)

            now = time.time()
            self._prune_stale(now)
            self._last_agent_id = agent_id
            snapshot = self._build_snapshot(now)
            return snapshot

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            now = time.time()
            self._prune_stale(now)
            return self._build_snapshot(now)


class CoordinatorApp:
    def __init__(
        self,
        *,
        dashboard_username: Optional[str] = None,
        dashboard_password: Optional[str] = None,
    ) -> None:
        self._coordinator = HedgeCoordinator()
        self._dashboard_username = (dashboard_username or "").strip()
        self._dashboard_password = (dashboard_password or "").strip()
        self._app = web.Application()
        self._app.add_routes(
            [
                web.get("/", self.handle_dashboard_redirect),
                web.get("/dashboard", self.handle_dashboard),
                web.get("/metrics", self.handle_metrics),
                web.post("/update", self.handle_update),
                web.get("/control", self.handle_control_get),
                web.post("/control", self.handle_control_update),
            ]
        )

    @property
    def app(self) -> web.Application:
        return self._app

    async def handle_dashboard_redirect(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        raise web.HTTPFound("/dashboard")

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        if not DASHBOARD_PATH.exists():
            raise web.HTTPNotFound(text="dashboard asset missing; ensure hedge_dashboard.html exists")
        return web.FileResponse(path=DASHBOARD_PATH)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
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

    async def handle_control_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        agent_id = request.rel_url.query.get("agent_id")
        snapshot = await self._coordinator.control_snapshot(agent_id)
        return web.json_response(snapshot)

    async def handle_control_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="control payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="control payload must be an object")

        agent_id_raw = body.get("agent_id")
        if agent_id_raw is None:
            raise web.HTTPBadRequest(text="agent_id is required")

        action_raw = body.get("action")
        paused_flag = body.get("paused")

        if action_raw is not None:
            action = str(action_raw).strip().lower()
            if action == "pause":
                snapshot = await self._coordinator.set_agent_paused(agent_id_raw, True)
            elif action in {"resume", "unpause"}:
                snapshot = await self._coordinator.set_agent_paused(agent_id_raw, False)
            elif action == "toggle":
                snapshot = await self._coordinator.toggle_agent_pause(agent_id_raw)
            else:
                raise web.HTTPBadRequest(text="invalid control action")
        elif paused_flag is not None:
            snapshot = await self._coordinator.set_agent_paused(agent_id_raw, bool(paused_flag))
        else:
            raise web.HTTPBadRequest(text="control payload requires 'action' or 'paused'")

        return web.json_response(snapshot)

    def _enforce_dashboard_auth(self, request: web.Request) -> None:
        username = self._dashboard_username
        password = self._dashboard_password

        if not username and not password:
            return

        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Hedge Dashboard"'})

        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Hedge Dashboard"'})

        provided_username, _, provided_password = decoded.partition(":")
        if username != provided_username or password != provided_password:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Hedge Dashboard"'})


async def _run_app(args: argparse.Namespace) -> None:
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    coordinator_app = CoordinatorApp(
        dashboard_username=args.dashboard_username,
        dashboard_password=args.dashboard_password,
    )

    if (args.dashboard_username or "") or (args.dashboard_password or ""):
        LOGGER.info("Dashboard authentication enabled; protected endpoints require HTTP Basic credentials")
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
    parser.add_argument(
        "--dashboard-username",
        help="Optional username required to access the dashboard (enables HTTP Basic auth)",
    )
    parser.add_argument(
        "--dashboard-password",
        help="Optional password required to access the dashboard (enables HTTP Basic auth)",
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
