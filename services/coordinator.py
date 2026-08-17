"""Lightweight coordination service for single-node Lighter grid bots.

This module offers an HTTP API compatible with the trimmed-down requirements:

* Register each VPS and track its most recent metrics (position, trading volume,
  balances).
* Allow operators to pause/resume all registered bots from a central dashboard.
* Provide a JSON status endpoint for automation as well as a small web dashboard.

The implementation reuses the aiohttp stack similar to the one in
``perp-dex-tools`` but omits hedge/primary role logic and global hedging rules.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hmac
import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional

from aiohttp import web

LOGGER = logging.getLogger("services.coordinator")
BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "dashboard.html"

Action = Literal["RUN", "PAUSE"]


def _to_decimal(value: Any, *, default: str = "0") -> Decimal:
    """Best-effort parsing for decimal-like payload values."""

    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


@dataclass
class VPSState:
    """Persistent metrics for a registered VPS."""

    vps_id: str
    display_name: Optional[str] = None
    position: Decimal = Decimal("0")
    position_symbol: Optional[str] = None
    position_value: Optional[Decimal] = None
    position_direction: Optional[str] = None
    active_close_amount: Decimal = Decimal("0")
    trading_volume: Decimal = Decimal("0")
    balance: Decimal = Decimal("0")
    total_value: Optional[Decimal] = None
    last_report_ts: float = field(default_factory=lambda: 0.0)
    alerts: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    manual_balance_preview: Optional[Dict[str, Any]] = None

    def as_payload(self) -> Dict[str, Any]:
        return {
            "vps_id": self.vps_id,
            "display_name": self.display_name,
            "position": str(self.position),
            "position_symbol": self.position_symbol,
            "position_value": str(self.position_value) if self.position_value is not None else None,
            "position_direction": self.position_direction,
            "active_close_amount": str(self.active_close_amount),
            "trading_volume": str(self.trading_volume),
            "balance": str(self.balance),
            "total_value": str(self.total_value) if self.total_value is not None else None,
            "last_report_ts": self.last_report_ts,
            "alerts": [dict(alert) for alert in self.alerts.values()],
            "manual_balance_preview": self.manual_balance_preview,
        }


class CoordinatorState:
    """In-memory state machine for the simplified coordinator."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._agents: Dict[str, VPSState] = {}
        self._mode: Action = "RUN"
        self._reason: Optional[str] = None
        self._last_transition: float = time.time()
        self._agent_overrides: Dict[str, Dict[str, Any]] = {}
        self._agent_actions: Dict[str, List[Dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Registration & inspection helpers
    async def register(self, *, vps_id: str, display_name: Optional[str]) -> Dict[str, Any]:
        async with self._lock:
            state = self._agents.get(vps_id)
            if state is None:
                state = VPSState(vps_id=vps_id, display_name=display_name)
                self._agents[vps_id] = state
                LOGGER.info("Registered new VPS %s", vps_id)
            else:
                if display_name:
                    state.display_name = display_name
                LOGGER.debug("VPS %s re-registered", vps_id)
            return {
                "mode": self._mode,
                "reason": self._reason,
                "issued_at": self._last_transition,
                "agent": state.as_payload(),
            }

    async def update_metrics(
        self,
        *,
        vps_id: str,
        position: Decimal,
        position_symbol: Optional[str],
        position_value: Optional[Decimal],
        position_direction: Optional[str] = None,
        active_close_amount: Optional[Decimal] = None,
        trading_volume: Decimal,
        balance: Decimal,
        total_value: Optional[Decimal],
        timestamp: float,
        manual_balance_preview: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self._agents:
                raise web.HTTPNotFound(text=f"vps '{vps_id}' is not registered")
            state = self._agents[vps_id]
            state.position = position
            state.position_symbol = position_symbol
            state.position_value = position_value
            if isinstance(position_direction, str):
                normalized_direction = position_direction.strip().lower()
                if normalized_direction in {"long", "short", "flat"}:
                    state.position_direction = normalized_direction
                else:
                    state.position_direction = None
            else:
                state.position_direction = None
            if active_close_amount is not None:
                state.active_close_amount = active_close_amount
            else:
                state.active_close_amount = Decimal("0")
            state.trading_volume = trading_volume
            state.balance = balance
            state.total_value = total_value
            state.last_report_ts = timestamp
            state.manual_balance_preview = (
                manual_balance_preview if isinstance(manual_balance_preview, dict) else None
            )
            LOGGER.debug(
                "Metrics update %s pos=%s vol=%s balance=%s",
                vps_id,
                position,
                trading_volume,
                balance,
            )
            return self._status_locked()

    async def set_alert(
        self,
        *,
        vps_id: str,
        alert_type: str,
        severity: str,
        message: Optional[str],
        details: Dict[str, Any],
        timestamp: float,
    ) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self._agents:
                raise web.HTTPNotFound(text=f"vps '{vps_id}' is not registered")
            state = self._agents[vps_id]
            normalized_type = alert_type or "general"
            normalized_severity = severity.lower()
            record: Dict[str, Any] = {
                "type": normalized_type,
                "severity": normalized_severity,
                "message": message,
                "timestamp": timestamp,
            }
            if details:
                record["details"] = {
                    key: str(value)
                    for key, value in details.items()
                    if value is not None
                }

            if normalized_severity in {"resolved", "clear"}:
                state.alerts.pop(normalized_type, None)
                LOGGER.info("Cleared alert %s for %s", normalized_type, vps_id)
            else:
                state.alerts[normalized_type] = record
                LOGGER.warning(
                    "Alert %s (%s) recorded for %s: %s",
                    normalized_type,
                    normalized_severity,
                    vps_id,
                    message,
                )
            return self._status_locked()

    async def enqueue_action(
        self,
        *,
        action_type: str,
        reason: Optional[str],
        target_vps_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        async with self._lock:
            targets = list(target_vps_ids) if target_vps_ids else list(self._agents.keys())
            if not targets:
                raise web.HTTPBadRequest(text="no registered agents to target")
            now = time.time()
            queued: List[str] = []
            for vps_id in targets:
                if vps_id not in self._agents:
                    LOGGER.warning("Attempted to queue action for unknown VPS %s", vps_id)
                    continue
                payload = {
                    "type": action_type.upper(),
                    "reason": reason,
                    "timestamp": now,
                }
                queue = self._agent_actions.setdefault(vps_id, [])
                queue.append(payload)
                if len(queue) > 20:
                    del queue[:-20]
                queued.append(vps_id)
            if queued:
                LOGGER.warning(
                    "Queued action %s for: %s",
                    action_type,
                    ", ".join(queued),
                )
            return self._status_locked()

    async def next_command(self, *, vps_id: str) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self._agents:
                raise web.HTTPNotFound(text=f"vps '{vps_id}' is not registered")
            override = self._agent_overrides.get(vps_id)
            actions = self._agent_actions.get(vps_id, [])
            if actions:
                self._agent_actions[vps_id] = []
            return {
                "action": override.get("mode") if override else self._mode,
                "reason": override.get("reason") if override else self._reason,
                "issued_at": override.get("issued_at") if override else self._last_transition,
                "scope": "agent" if override else "global",
                "actions": actions,
            }

    async def set_mode(
        self,
        *,
        mode: Action,
        reason: Optional[str],
        target_vps_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        async with self._lock:
            now = time.time()
            if target_vps_ids is None:
                if mode == self._mode and reason == self._reason:
                    return self._status_locked()
                self._mode = mode
                self._reason = reason
                self._last_transition = now
                self._agent_overrides.clear()
                LOGGER.warning("Global mode switched to %s: %s", mode, reason)
                return self._status_locked()

            updated_targets = []
            for vps_id in target_vps_ids:
                if vps_id not in self._agents:
                    LOGGER.warning("Attempted to set mode for unknown VPS %s", vps_id)
                    continue
                if mode == self._mode:
                    if vps_id in self._agent_overrides:
                        del self._agent_overrides[vps_id]
                        updated_targets.append(vps_id)
                else:
                    self._agent_overrides[vps_id] = {
                        "mode": mode,
                        "reason": reason,
                        "issued_at": now,
                    }
                    updated_targets.append(vps_id)

            if updated_targets:
                LOGGER.warning(
                    "Override mode %s applied to: %s",
                    mode,
                    ", ".join(updated_targets),
                )
            return self._status_locked()

    async def status(self) -> Dict[str, Any]:
        async with self._lock:
            return self._status_locked()

    # ------------------------------------------------------------------
    def _status_locked(self) -> Dict[str, Any]:
        totals = {
            "position": Decimal("0"),
            "position_value": Decimal("0"),
            "trading_volume": Decimal("0"),
            "balance": Decimal("0"),
            "total_value": Decimal("0"),
            "has_position_value": False,
            "has_total_value": False,
            "alerts": {"warning": 0, "critical": 0},
            "active_close_amount": Decimal("0"),
        }
        total_long = Decimal("0")
        total_short = Decimal("0")
        agents_snapshot = []
        for agent in self._agents.values():
            payload = agent.as_payload()
            override = self._agent_overrides.get(agent.vps_id)
            payload.update(
                {
                    "command": override.get("mode") if override else self._mode,
                    "command_reason": override.get("reason") if override else self._reason,
                    "command_scope": "agent" if override else "global",
                    "command_issued_at": override.get("issued_at") if override else self._last_transition,
                }
            )
            agents_snapshot.append(payload)
            totals["position"] += agent.position
            if agent.position_value is not None:
                totals["position_value"] += agent.position_value
                totals["has_position_value"] = True
            totals["trading_volume"] += agent.trading_volume
            totals["balance"] += agent.balance
            if agent.total_value is not None:
                totals["total_value"] += agent.total_value
                totals["has_total_value"] = True
            totals["active_close_amount"] += agent.active_close_amount
            if agent.position_direction == "long":
                total_long += abs(agent.position)
            elif agent.position_direction == "short":
                total_short += abs(agent.position)
            for alert in payload.get("alerts", []):
                severity = str(alert.get("severity", "")).lower()
                if severity in totals["alerts"]:
                    totals["alerts"][severity] += 1

        if total_long > total_short:
            position_direction = "long"
        elif total_short > total_long:
            position_direction = "short"
        else:
            position_total = totals["position"]
            if position_total > 0:
                position_direction = "long"
            elif position_total < 0:
                position_direction = "short"
            else:
                position_direction = "flat"

        return {
            "mode": self._mode,
            "reason": self._reason,
            "issued_at": self._last_transition,
            "agents": agents_snapshot,
            "totals": {
                "position": str(totals["position"]),
                "position_value": str(totals["position_value"]) if totals["has_position_value"] else None,
                "trading_volume": str(totals["trading_volume"]),
                "balance": str(totals["balance"]),
                "total_value": str(totals["total_value"]) if totals["has_total_value"] else None,
                "alerts": totals["alerts"],
                "position_direction": position_direction,
                "active_close_amount": str(totals["active_close_amount"]),
            },
        }


class CoordinatorApp:
    """aiohttp application exposing the coordinator API and dashboard."""

    def __init__(
        self,
        *,
        state: CoordinatorState,
        dashboard_user: Optional[str],
        dashboard_password: Optional[str],
    ) -> None:
        self.state = state
        self.dashboard_user = dashboard_user
        self.dashboard_password = dashboard_password
        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/healthz", self.handle_healthz),
                web.post("/register", self.handle_register),
                web.post("/metrics", self.handle_metrics),
                web.get("/command", self.handle_command),
                web.get("/status", self.handle_status),
                web.post("/alerts", self.handle_alert),
                web.post("/manual_pause", self.handle_manual_pause),
                web.post("/manual_resume", self.handle_manual_resume),
                web.post("/manual_balance", self.handle_manual_balance),
                web.get("/dashboard", self.handle_dashboard),
            ]
        )

    # ------------------------------------------------------------------
    async def handle_healthz(self, _: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def handle_register(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        vps_id = payload.get("vps_id")
        if not vps_id or not isinstance(vps_id, str):
            raise web.HTTPBadRequest(text="vps_id required")
        display_name = payload.get("display_name")
        if display_name is not None and not isinstance(display_name, str):
            raise web.HTTPBadRequest(text="display_name must be string")
        snapshot = await self.state.register(vps_id=vps_id, display_name=display_name)
        return web.json_response(snapshot)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        vps_id = payload.get("vps_id")
        if not vps_id or not isinstance(vps_id, str):
            raise web.HTTPBadRequest(text="vps_id required")

        position = _to_decimal(payload.get("position"))
        position_symbol = payload.get("position_symbol")
        if position_symbol is not None and not isinstance(position_symbol, str):
            raise web.HTTPBadRequest(text="position_symbol must be string when provided")
        position_direction_raw = payload.get("position_direction")
        if position_direction_raw is not None and not isinstance(position_direction_raw, str):
            position_direction_raw = str(position_direction_raw)
        trading_volume = _to_decimal(payload.get("trading_volume"))
        balance = _to_decimal(payload.get("balance"))
        total_value_raw = payload.get("total_value")
        total_value = _to_decimal(total_value_raw) if total_value_raw is not None else None
        position_value_raw = payload.get("position_value")
        position_value = _to_decimal(position_value_raw) if position_value_raw is not None else None
        timestamp = float(payload.get("timestamp", time.time()))
        preview_payload = payload.get("manual_balance_preview")
        if not isinstance(preview_payload, dict):
            preview_payload = None
        active_close_raw = payload.get("active_close_amount")
        active_close_amount = _to_decimal(active_close_raw) if active_close_raw is not None else Decimal("0")

        status = await self.state.update_metrics(
            vps_id=vps_id,
            position=position,
            position_symbol=position_symbol,
            position_value=position_value,
            position_direction=position_direction_raw,
            active_close_amount=active_close_amount,
            trading_volume=trading_volume,
            balance=balance,
            total_value=total_value,
            timestamp=timestamp,
            manual_balance_preview=preview_payload,
        )
        return web.json_response(status)

    async def handle_command(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        vps_id = request.query.get("vps_id")
        if not vps_id:
            raise web.HTTPBadRequest(text="vps_id query parameter required")
        command = await self.state.next_command(vps_id=vps_id)
        return web.json_response(command)

    async def handle_status(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        snapshot = await self.state.status()
        return web.json_response(snapshot)

    async def handle_alert(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        vps_id = payload.get("vps_id")
        if not vps_id or not isinstance(vps_id, str):
            raise web.HTTPBadRequest(text="vps_id required")
        alert_type = payload.get("type")
        if alert_type is None or not isinstance(alert_type, str) or not alert_type.strip():
            alert_type = "general"
        severity = payload.get("severity")
        if severity is None or not isinstance(severity, str) or not severity.strip():
            severity = "info"
        message = payload.get("message") if isinstance(payload.get("message"), str) else None
        timestamp_raw = payload.get("timestamp")
        try:
            timestamp = float(timestamp_raw) if timestamp_raw is not None else time.time()
        except (TypeError, ValueError):
            timestamp = time.time()
        detail_keys = [
            "position",
            "active_closing_amount",
            "difference",
            "symbol",
        ]
        details: Dict[str, Any] = {key: payload.get(key) for key in detail_keys}
        status = await self.state.set_alert(
            vps_id=vps_id,
            alert_type=alert_type,
            severity=severity,
            message=message,
            details=details,
            timestamp=timestamp,
        )
        return web.json_response(status)

    async def handle_manual_pause(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        targets = self._extract_target_vps(payload)
        default_reason = "Manual pause (selected)" if targets else "Manual pause triggered"
        reason_text = reason if isinstance(reason, str) and reason.strip() else default_reason
        status = await self.state.set_mode(mode="PAUSE", reason=reason_text, target_vps_ids=targets)
        return web.json_response(status)

    async def handle_manual_resume(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        targets = self._extract_target_vps(payload)
        default_reason = "Manual resume (selected)" if targets else "Manual resume triggered"
        reason_text = reason if isinstance(reason, str) and reason.strip() else default_reason
        status = await self.state.set_mode(mode="RUN", reason=reason_text, target_vps_ids=targets)
        return web.json_response(status)

    async def handle_manual_balance(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        targets = self._extract_target_vps(payload)
        default_reason = "Manual balance (selected)" if targets else "Manual balance triggered"
        reason_text = reason if isinstance(reason, str) and reason.strip() else default_reason
        status = await self.state.enqueue_action(
            action_type="MANUAL_BALANCE",
            reason=reason_text,
            target_vps_ids=targets,
        )
        return web.json_response(status)

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        try:
            html = DASHBOARD_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise web.HTTPNotFound(text="dashboard asset missing; ensure dashboard.html exists")
        return web.Response(text=html, content_type="text/html")

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_target_vps(payload: Dict[str, Any]) -> Optional[List[str]]:
        if not isinstance(payload, dict):
            return None
        candidates: List[str] = []
        single = payload.get("vps_id")
        if isinstance(single, str) and single.strip():
            candidates.append(single.strip())
        multiple = payload.get("vps_ids")
        if isinstance(multiple, list):
            for item in multiple:
                if isinstance(item, str) and item.strip():
                    candidates.append(item.strip())
        unique = list(dict.fromkeys(candidates))
        return unique or None

    def _require_dashboard_auth(self, request: web.Request) -> None:
        if not self.dashboard_user and not self.dashboard_password:
            return
        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})
        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})
        username, _, password = decoded.partition(":")
        expected_username = self.dashboard_user or ""
        expected_password = self.dashboard_password or ""
        if not (
            hmac.compare_digest(username, expected_username)
            and hmac.compare_digest(password, expected_password)
        ):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})


async def _start_app(host: str, port: int, *, user: Optional[str], password: Optional[str]) -> None:
    state = CoordinatorState()
    app_wrapper = CoordinatorApp(state=state, dashboard_user=user, dashboard_password=password)
    runner = web.AppRunner(app_wrapper.app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    LOGGER.info("Starting coordinator on %s:%s", host, port)
    await site.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


def _validate_bind_security(
    host: str,
    *,
    user: Optional[str],
    password: Optional[str],
) -> None:
    has_user = bool((user or "").strip())
    has_password = bool(password)
    if has_user != has_password:
        raise ValueError("coordinator username and password must be configured together")

    normalized_host = (host or "").strip().lower()
    loopback_hosts = {"127.0.0.1", "localhost", "::1"}
    if normalized_host not in loopback_hosts and not (has_user and has_password):
        raise ValueError("non-loopback coordinator binds require authentication")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run single-cluster coordination service")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="TCP port to bind (default: 8787)")
    parser.add_argument(
        "--dashboard-user",
        default=os.getenv("COORDINATOR_USER"),
        help="Basic auth username for dashboard/API (or COORDINATOR_USER)",
    )
    parser.add_argument(
        "--dashboard-password",
        default=os.getenv("COORDINATOR_PASSWORD"),
        help="Basic auth password for dashboard/API (or COORDINATOR_PASSWORD)",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        _validate_bind_security(
            args.host,
            user=args.dashboard_user,
            password=args.dashboard_password,
        )
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )

    try:
        asyncio.run(
            _start_app(
                args.host,
                args.port,
                user=args.dashboard_user,
                password=args.dashboard_password,
            )
        )
    except KeyboardInterrupt:
        LOGGER.info("Coordinator interrupted; shutting down")


if __name__ == "__main__":
    main()
