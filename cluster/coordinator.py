"""Async coordination service for multi-VPS market-making cluster.

This module exposes a lightweight HTTP API (via aiohttp) that keeps track of
agent registrations, net exposures, risk thresholds, and run/pause/reverse
commands.  The logic follows the high-level design discussed with the user:

* One "primary" VPS runs the main single-sided strategy (e.g. only posting bids).
* Three hedge VPSes run the opposite side to offset risk.
* Position reports from every VPS are aggregated. When the *cluster-wide* net
  exposure breaches ``global_exposure_limit``, all sides are paused until the
  exposure decays below ``global_resume_ratio`` × limit.
* When the primary side accumulates inventory above ``cycle_inventory_cap`` the
  service initiates a cooldown window and then flips the quoting sides.
* Quantity instructions remain randomised within configured ranges; the
  coordinator simply forwards the ranges to agents for local sampling.

The service keeps all state in memory; a production deployment should add
durable storage if long-lived restart tolerance is required.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Literal, Optional

from aiohttp import web

BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "dashboard.html"
from pydantic import BaseModel, Field, validator


LOGGER = logging.getLogger("cluster.coordinator")




Direction = Literal["long", "short"]
Side = Literal["buy", "sell"]
AgentRole = Literal["primary", "hedge"]


def _to_decimal(value: Any, *, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


class QuantityRange(BaseModel):
    """Inclusive quantity range used for random sampling by agents."""

    minimum: Decimal = Field(..., gt=Decimal("0"))
    maximum: Decimal = Field(..., gt=Decimal("0"))

    @validator("maximum")
    def _validate_bounds(cls, v: Decimal, values: Dict[str, Any]) -> Decimal:  # noqa: D401,N805
        min_value = values.get("minimum")
        if min_value is not None and v < min_value:
            raise ValueError("maximum must be >= minimum")
        return v

    def as_payload(self) -> Dict[str, str]:
        return {"min": str(self.minimum), "max": str(self.maximum)}


class ClusterConfig(BaseModel):
    """Runtime configuration for the cluster coordinator."""

    global_exposure_limit: Decimal = Field(..., gt=Decimal("0"))
    global_resume_ratio: float = Field(0.7, ge=0.0, le=1.0)
    cycle_inventory_cap: Decimal = Field(..., gt=Decimal("0"))
    cooldown_seconds: float = Field(60.0, gt=0.0)
    initial_primary_direction: Direction = "long"
    primary_quantity_range: QuantityRange
    hedge_quantity_range: QuantityRange
    flatten_tolerance: Decimal = Field(default=Decimal("0.01"), ge=Decimal("0"))
    dashboard_username: Optional[str] = None
    dashboard_password: Optional[str] = None

    def resume_threshold(self) -> Decimal:
        return self.global_exposure_limit * Decimal(str(self.global_resume_ratio))


class AgentCommand(BaseModel):
    action: Literal["WAIT", "RUN", "PAUSE", "FLATTEN"]
    side: Optional[Side] = None
    quantity: Optional[Dict[str, str]] = None
    reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    issued_at: float = Field(default_factory=time.time)

    def as_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "action": self.action,
            "issued_at": self.issued_at,
        }
        if self.side is not None:
            payload["side"] = self.side
        if self.quantity is not None:
            payload["quantity"] = self.quantity
        if self.reason:
            payload["reason"] = self.reason
        if self.metadata is not None:
            payload["metadata"] = self.metadata
        return payload


@dataclass
class AgentStatus:
    """Bookkeeping for each registered agent."""

    vps_id: str
    role: AgentRole
    last_position: Decimal = Decimal("0")
    last_report_ts: float = field(default_factory=lambda: 0.0)
    pending_command: Optional[AgentCommand] = None
    current_mode: Literal["RUN", "PAUSE", "WAIT", "FLATTEN"] = "WAIT"

    def queue_command(self, command: AgentCommand) -> None:
        self.pending_command = command
        self.current_mode = command.action

    def next_command(self) -> AgentCommand:
        if self.pending_command is None:
            return AgentCommand(action="WAIT")
        command = self.pending_command
        self.pending_command = None
        return command


class ClusterState:
    """Mutable state machine implementing the coordination policy."""

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self._lock = asyncio.Lock()
        self.agents: Dict[str, AgentStatus] = {}
        self.primary_direction: Direction = config.initial_primary_direction
        self.phase: Literal["initial", "running", "hedge_only", "cooldown", "flatten"] = "initial"
        self.cooldown_ends_at: Optional[float] = None
        self._last_global_reason: Optional[str] = None
        self._hedge_only_active_role: Optional[AgentRole] = None
        self._flatten_active: bool = False
        self._flatten_started_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Registration & basic inspection
    async def register_agent(self, vps_id: str, role: Optional[AgentRole]) -> Dict[str, Any]:
        async with self._lock:
            if vps_id in self.agents:
                agent = self.agents[vps_id]
            else:
                assigned_role = role or self._assign_role()
                agent = AgentStatus(vps_id=vps_id, role=assigned_role)
                self.agents[vps_id] = agent
            LOGGER.info("Agent %s registered as %s", vps_id, agent.role)
            self._maybe_start_running()
            return self._agent_snapshot(agent)

    def _assign_role(self) -> AgentRole:
        primaries = sum(1 for agent in self.agents.values() if agent.role == "primary")
        hedges = sum(1 for agent in self.agents.values() if agent.role == "hedge")
        if primaries == 0:
            return "primary"
        if hedges < 3:
            return "hedge"
        return "hedge" if hedges <= primaries else "primary"

    def _agent_snapshot(self, agent: AgentStatus) -> Dict[str, Any]:
        return {
            "vps_id": agent.vps_id,
            "role": agent.role,
            "current_mode": agent.current_mode,
            "primary_direction": self.primary_direction,
            "phase": self.phase,
            "last_position": str(agent.last_position),
            "last_report_ts": agent.last_report_ts,
        }

    async def get_cluster_status(self) -> Dict[str, Any]:
        async with self._lock:
            net_exposure = self._net_exposure()
            primary_exposure = self._primary_exposure()
            return {
                "phase": self.phase,
                "primary_direction": self.primary_direction,
                "cooldown_ends_at": self.cooldown_ends_at,
                "net_exposure": str(net_exposure),
                "primary_exposure": str(primary_exposure),
                "global_limit": str(self.config.global_exposure_limit),
                "resume_threshold": str(self.config.resume_threshold()),
                "cycle_inventory_cap": str(self.config.cycle_inventory_cap),
                "flatten_active": self._flatten_active,
                "flatten_started_at": self._flatten_started_at,
                "flatten_tolerance": str(self.config.flatten_tolerance),
                "agents": [self._agent_snapshot(agent) for agent in self.agents.values()],
                "last_pause_reason": self._last_global_reason,
            }

    # ------------------------------------------------------------------
    # Metrics updates & state machine evaluation
    async def update_metrics(self, vps_id: str, net_position: Decimal, timestamp: float) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self.agents:
                raise web.HTTPNotFound(text=f"agent '{vps_id}' not registered")
            agent = self.agents[vps_id]
            agent.last_position = net_position
            agent.last_report_ts = timestamp
            LOGGER.debug("Metrics update %s pos=%s", vps_id, net_position)
            self._evaluate_state()
            return {
                "phase": self.phase,
                "net_exposure": str(self._net_exposure()),
            }

    # ------------------------------------------------------------------
    async def get_next_command(self, vps_id: str) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self.agents:
                raise web.HTTPNotFound(text=f"agent '{vps_id}' not registered")
            command = self.agents[vps_id].next_command()
            LOGGER.debug("Dispatch command to %s: %s", vps_id, command.action)
            return command.as_payload()

    # ------------------------------------------------------------------
    # Core evaluation helpers
    def _maybe_start_running(self) -> None:
        if self.phase != "initial":
            return
        has_primary = any(agent.role == "primary" for agent in self.agents.values())
        has_hedge = any(agent.role == "hedge" for agent in self.agents.values())
        if has_primary and has_hedge:
            LOGGER.info("Sufficient agents registered; moving to running phase")
            self.phase = "running"
            self._broadcast_run(reason="initial start")

    def _evaluate_state(self) -> None:
        now = time.time()

        if self.phase == "flatten":
            if self._flatten_active and self._all_agents_flat():
                LOGGER.info("Emergency flatten complete within tolerance %s", self.config.flatten_tolerance)
                self._complete_flatten()
            return

        if self.phase == "cooldown":
            if self.cooldown_ends_at is not None and now >= self.cooldown_ends_at:
                LOGGER.info("Cooldown complete; flipping sides")
                self._reverse_primary_direction()
                self.phase = "running"
                self.cooldown_ends_at = None
                self._broadcast_run(reason="cycle flip complete")
            return

        net_exposure = self._net_exposure()
        abs_net = abs(net_exposure)

        if self.phase != "hedge_only" and abs_net >= self.config.global_exposure_limit:
            reason = (
                f"global exposure {abs_net} exceeds limit {self.config.global_exposure_limit}"
            )
            LOGGER.warning(reason)
            self.phase = "hedge_only"
            self._enforce_hedge_only(net_exposure=net_exposure, reason=reason)
            return

        if self.phase == "hedge_only":
            resume_threshold = self.config.resume_threshold()
            if abs_net <= resume_threshold:
                LOGGER.info("Exposure %s within resume threshold; resuming primary quoting", abs_net)
                self.phase = "running"
                self._last_global_reason = None
                self._hedge_only_active_role = None
                self._broadcast_run(reason="resume")
            else:
                desired_role = self._role_to_reduce_net_exposure(net_exposure)
                if self._hedge_only_active_role != desired_role:
                    switch_reason = (
                        f"net exposure {net_exposure} now requires {desired_role} agents to run"
                    )
                    LOGGER.info(switch_reason)
                    self._enforce_hedge_only(net_exposure=net_exposure, reason=switch_reason)
            return

        # Running phase: check cycle inventory cap
        if self.phase == "running":
            primary_exposure = self._primary_exposure()
            primary_abs = abs(primary_exposure)
            if primary_abs >= self.config.cycle_inventory_cap and self._direction_matches_exposure(primary_exposure):
                reason = (
                    f"primary exposure {primary_abs} reached cycle cap {self.config.cycle_inventory_cap}"
                )
                LOGGER.warning(reason)
                self.phase = "cooldown"
                self.cooldown_ends_at = now + self.config.cooldown_seconds
                self._last_global_reason = reason
                self._hedge_only_active_role = None
                self._broadcast_pause(reason=reason)
            elif primary_abs >= self.config.cycle_inventory_cap:
                LOGGER.info(
                    "primary exposure %s exceeds cap but new direction '%s' will reduce inventory; allowing run",
                    primary_exposure,
                    self.primary_direction,
                )

    def _broadcast_run(self, *, reason: Optional[str]) -> None:
        for agent in self.agents.values():
            side = self._side_for_agent(agent.role)
            quantity_range = (
                self.config.primary_quantity_range if agent.role == "primary" else self.config.hedge_quantity_range
            )
            agent.queue_command(
                AgentCommand(
                    action="RUN",
                    side=side,
                    quantity=quantity_range.as_payload(),
                    reason=reason,
                )
            )

    def _broadcast_pause(self, *, reason: str) -> None:
        for agent in self.agents.values():
            agent.queue_command(AgentCommand(action="PAUSE", reason=reason))

    def _broadcast_flatten(self, *, reason: str) -> None:
        for agent in self.agents.values():
            agent.queue_command(
                AgentCommand(
                    action="FLATTEN",
                    reason=reason,
                    metadata={
                        "tolerance": str(self.config.flatten_tolerance),
                        "price_offset_ticks": 2,
                    },
                )
            )

    async def trigger_emergency_flatten(self, *, reason: str) -> Dict[str, Any]:
        async with self._lock:
            if self.phase == "flatten" and self._flatten_active:
                return {
                    "phase": self.phase,
                    "flatten_active": self._flatten_active,
                    "reason": self._last_global_reason,
                }

            self.phase = "flatten"
            self._flatten_active = True
            self._flatten_started_at = time.time()
            self._last_global_reason = reason
            self._hedge_only_active_role = None
            self.cooldown_ends_at = None
            LOGGER.warning("Emergency flatten triggered: %s", reason)
            self._broadcast_flatten(reason=reason)

            return {
                "phase": self.phase,
                "flatten_active": self._flatten_active,
                "reason": self._last_global_reason,
            }

    def _enforce_hedge_only(self, *, net_exposure: Decimal, reason: str) -> None:
        active_role = self._role_to_reduce_net_exposure(net_exposure)
        self._hedge_only_active_role = active_role
        self._last_global_reason = reason

        for agent in self.agents.values():
            if agent.role != active_role:
                agent.queue_command(AgentCommand(action="PAUSE", reason=reason))
                continue

            side = self._side_for_agent(agent.role)
            quantity_range = (
                self.config.primary_quantity_range
                if agent.role == "primary"
                else self.config.hedge_quantity_range
            )
            agent.queue_command(
                AgentCommand(
                    action="RUN",
                    side=side,
                    quantity=quantity_range.as_payload(),
                    reason=reason,
                )
            )

    def _reverse_primary_direction(self) -> None:
        self.primary_direction = "short" if self.primary_direction == "long" else "long"
        LOGGER.info("New primary direction: %s", self.primary_direction)

    def _side_for_agent(self, role: AgentRole) -> Side:
        # Primary follows primary_direction, hedge is opposite.
        desired = self.primary_direction
        if role == "hedge":
            desired = "short" if self.primary_direction == "long" else "long"
        return "buy" if desired == "long" else "sell"

    def _net_exposure(self) -> Decimal:
        return sum((agent.last_position for agent in self.agents.values()), Decimal("0"))

    def _primary_exposure(self) -> Decimal:
        return sum(
            (agent.last_position for agent in self.agents.values() if agent.role == "primary"),
            Decimal("0"),
        )

    def _role_to_reduce_net_exposure(self, net_exposure: Decimal) -> AgentRole:
        if net_exposure >= 0:
            return "hedge" if self.primary_direction == "long" else "primary"
        return "primary" if self.primary_direction == "long" else "hedge"

    def _direction_matches_exposure(self, primary_exposure: Decimal) -> bool:
        if primary_exposure == 0:
            return False
        if primary_exposure > 0:
            return self.primary_direction == "long"
        return self.primary_direction == "short"

    def _all_agents_flat(self) -> bool:
        tolerance = self.config.flatten_tolerance
        return all(abs(agent.last_position) <= tolerance for agent in self.agents.values())

    def _complete_flatten(self) -> None:
        if not self._flatten_active:
            return
        self._flatten_active = False
        self._flatten_started_at = None
        self._hedge_only_active_role = None
        reason = "emergency flatten complete; manual resume recommended"
        self._last_global_reason = reason
        self.phase = "running"
        self._broadcast_pause(reason=reason)


# ----------------------------------------------------------------------
# HTTP handlers


class CoordinatorApp:
    def __init__(self, state: ClusterState) -> None:
        self.state = state
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/register", self.handle_register),
                web.post("/metrics", self.handle_metrics),
                web.get("/command", self.handle_command),
                web.get("/status", self.handle_status),
                web.get("/dashboard", self.handle_dashboard),
                web.post("/flatten", self.handle_flatten),
            ]
        )

    async def handle_register(self, request: web.Request) -> web.Response:
        data = await request.json()
        vps_id = data.get("vps_id")
        if not vps_id:
            raise web.HTTPBadRequest(text="vps_id required")
        role = data.get("role")
        role_value: Optional[AgentRole]
        if role is None:
            role_value = None
        elif role in ("primary", "hedge"):
            role_value = role  # type: ignore[assignment]
        else:
            raise web.HTTPBadRequest(text="role must be 'primary' or 'hedge'")
        snapshot = await self.state.register_agent(vps_id=vps_id, role=role_value)
        return web.json_response(snapshot)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        data = await request.json()
        vps_id = data.get("vps_id")
        if not vps_id:
            raise web.HTTPBadRequest(text="vps_id required")
        net_position = _to_decimal(data.get("net_position"))
        timestamp = float(data.get("timestamp", time.time()))
        result = await self.state.update_metrics(vps_id=vps_id, net_position=net_position, timestamp=timestamp)
        return web.json_response(result)

    async def handle_command(self, request: web.Request) -> web.Response:
        vps_id = request.query.get("vps_id")
        if not vps_id:
            raise web.HTTPBadRequest(text="vps_id required")
        command = await self.state.get_next_command(vps_id=vps_id)
        return web.json_response(command)

    async def handle_status(self, request: web.Request) -> web.Response:
        status = await self.state.get_cluster_status()
        return web.json_response(status)

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            html = DASHBOARD_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise web.HTTPNotFound(text="dashboard asset missing; ensure cluster/dashboard.html exists")
        return web.Response(text=html, content_type="text/html")

    async def handle_flatten(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        requested_reason = payload.get("reason") if isinstance(payload, dict) else None
        remote = request.remote or "dashboard"
        reason = requested_reason or f"Emergency flatten triggered by {remote}"
        result = await self.state.trigger_emergency_flatten(reason=reason)
        return web.json_response(result)

    def _enforce_dashboard_auth(self, request: web.Request) -> None:
        username = self.state.config.dashboard_username
        password = self.state.config.dashboard_password
        if not username and not password:
            return

        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Cluster Dashboard"'})

        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Cluster Dashboard"'})

        provided_username, _, provided_password = decoded.partition(":")
        if (username or "") != provided_username or (password or "") != provided_password:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Cluster Dashboard"'})


def load_config(path: str) -> ClusterConfig:
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return ClusterConfig.parse_obj(data)


async def _start_app(config: ClusterConfig, host: str, port: int) -> None:
    state = ClusterState(config)
    coordinator_app = CoordinatorApp(state)
    runner = web.AppRunner(coordinator_app.app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    LOGGER.info("Starting coordinator on %s:%s", host, port)
    await site.start()
    # Run forever until cancelled.
    while True:
        await asyncio.sleep(3600)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run cluster coordinator service")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")

    config = load_config(args.config)
    try:
        asyncio.run(_start_app(config, args.host, args.port))
    except KeyboardInterrupt:
        LOGGER.info("Coordinator interrupted; shutting down")


if __name__ == "__main__":
    main()
