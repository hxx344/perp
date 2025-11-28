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
import copy
import logging
import os
import secrets
import signal
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, cast
from urllib.parse import quote_plus

from aiohttp import ClientSession, ClientTimeout, web

try:
    from .grvt_adjustments import AdjustmentAction, GrvtAdjustmentManager
except ImportError:  # pragma: no cover - script execution path
    from grvt_adjustments import AdjustmentAction, GrvtAdjustmentManager

BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "hedge_dashboard.html"
LOGIN_TEMPLATE = """<!DOCTYPE html>
<html lang=\"zh-CN\">
    <head>
        <meta charset=\"utf-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>Hedge Dashboard Login</title>
        <style>
            :root {
                color-scheme: dark;
                font-family: \"Segoe UI\", system-ui, -apple-system, sans-serif;
                background: #0b0d13;
                color: #f2f4f8;
            }
            body {
                margin: 0;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 2rem;
            }
            form {
                background: #161a22;
                border-radius: 16px;
                padding: 2rem;
                width: min(400px, 100%);
                box-shadow: 0 25px 45px rgba(0,0,0,.45);
            }
            h1 {
                margin: 0 0 1.5rem;
                font-size: 1.5rem;
                text-align: center;
                letter-spacing: .05em;
            }
            label {
                display: block;
                margin-bottom: .5rem;
                color: #9ca5bd;
                font-size: .85rem;
                letter-spacing: .05em;
            }
            input {
                width: 100%;
                padding: .8rem 1rem;
                border-radius: 10px;
                border: 1px solid rgba(255,255,255,.08);
                background: rgba(255,255,255,.05);
                color: inherit;
                margin-bottom: 1.25rem;
                font-size: 1rem;
            }
            button {
                width: 100%;
                padding: .85rem 1rem;
                border-radius: 10px;
                border: none;
                font-weight: 600;
                font-size: 1rem;
                cursor: pointer;
                background: linear-gradient(120deg,#4f9cff,#7c4dff);
                color: #fff;
                transition: opacity .2s ease;
            }
            button:hover {
                opacity: .9;
            }
            .error {
                margin: 0 0 1.25rem;
                padding: .75rem 1rem;
                border-radius: 10px;
                background: rgba(255,87,126,.15);
                color: #ffb4c4;
                font-size: .9rem;
                text-align: center;
            }
            .hint {
                margin-top: 1rem;
                font-size: .8rem;
                color: #8a94a6;
                text-align: center;
            }
        </style>
    </head>
    <body>
        <form method=\"post\" action=\"/login\">
            <h1>Dashboard 登录</h1>
            {error_block}
            <label for=\"username\">用户名</label>
            <input id=\"username\" name=\"username\" autocomplete=\"username\" required />
            <label for=\"password\">密码</label>
            <input id=\"password\" type=\"password\" name=\"password\" autocomplete=\"current-password\" required />
            <button type=\"submit\">登录</button>
            <p class=\"hint\">如需关闭认证，可省略 --dashboard-username/--dashboard-password</p>
        </form>
    </body>
</html>
"""

LOGGER = logging.getLogger("hedge.coordinator")
MAX_SPREAD_HISTORY = 600
MAX_STRATEGY_EVENTS = 400
MAX_STRATEGY_TRADES = 200


class BarkNotifier:
    def __init__(self, url_template: str, timeout: float = 10.0, append_payload: bool = True) -> None:
        self._template = (url_template or "").strip()
        self._timeout = max(timeout, 1.0)
        self._append_payload = bool(append_payload)

    async def send(self, *, title: str, body: str) -> None:
        if not self._template:
            LOGGER.warning("Bark notifier missing push URL; skipping alert")
            return

        safe_title = quote_plus(title or "Alert")
        safe_body = quote_plus(body or "")
        if "{title}" in self._template or "{body}" in self._template:
            endpoint = self._template.replace("{title}", safe_title).replace("{body}", safe_body)
        elif self._append_payload:
            endpoint = f"{self._template.rstrip('/')}/{safe_title}/{safe_body}"
        else:
            endpoint = self._template

        timeout = ClientTimeout(total=self._timeout)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(endpoint) as response:
                    if response.status >= 400:
                        body_text = await response.text()
                        LOGGER.warning(
                            "Bark push failed (HTTP %s): %s",
                            response.status,
                            body_text[:200],
                        )
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Bark push request failed: %s", exc)


@dataclass(frozen=True)
class RiskAlertInfo:
    key: str
    agent_id: str
    account_label: str
    ratio: float
    loss_value: Decimal
    base_value: Decimal
    base_label: str


@dataclass(frozen=True)
class AutoBalanceConfig:
    agent_a: str
    agent_b: str
    threshold_ratio: float
    min_transfer: Decimal
    max_transfer: Optional[Decimal]
    cooldown_seconds: float
    currency: str = "USDT"
    use_available_equity: bool = False


@dataclass(frozen=True)
class AutoBalanceMeasurement:
    agent_a: str
    agent_b: str
    equity_a: Decimal
    equity_b: Decimal
    difference: Decimal
    ratio: float
    source_agent: str
    target_agent: str
    transfer_amount: Decimal

    def as_payload(self) -> Dict[str, Any]:
        def _fmt(value: Decimal) -> str:
            try:
                return format(value, "f")
            except Exception:
                return str(value)

        return {
            "agent_a": self.agent_a,
            "agent_b": self.agent_b,
            "equity_a": _fmt(self.equity_a),
            "equity_b": _fmt(self.equity_b),
            "difference": _fmt(self.difference),
            "ratio": self.ratio,
            "source_agent": self.source_agent,
            "target_agent": self.target_agent,
            "transfer_amount": _fmt(self.transfer_amount),
        }


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
    runtime_seconds: float = 0.0
    spread_metrics: Optional[Dict[str, Any]] = None
    strategy_metrics: Optional[Dict[str, Any]] = None
    grvt_accounts: Optional[Dict[str, Any]] = None

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
        runtime_raw = payload.get("runtime_seconds") or payload.get("runtime")
        spread_metrics_raw = payload.get("spread_metrics")
        strategy_metrics_raw = payload.get("strategy_metrics")
        grvt_accounts_raw = payload.get("grvt_accounts")

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

        if runtime_raw is not None:
            try:
                runtime_value = float(runtime_raw)
                if runtime_value >= 0:
                    self.runtime_seconds = runtime_value
            except (TypeError, ValueError):
                LOGGER.warning("Invalid runtime payload: %s", runtime_raw)

        if isinstance(spread_metrics_raw, dict):
            normalized_spread = copy.deepcopy(spread_metrics_raw)
            history = normalized_spread.get("history")
            if isinstance(history, list) and len(history) > MAX_SPREAD_HISTORY:
                normalized_spread["history"] = history[-MAX_SPREAD_HISTORY:]
            self.spread_metrics = normalized_spread

        if isinstance(strategy_metrics_raw, dict):
            normalized_strategy = copy.deepcopy(strategy_metrics_raw)
            recent_events = normalized_strategy.get("recent_events")
            if isinstance(recent_events, list) and len(recent_events) > MAX_STRATEGY_EVENTS:
                normalized_strategy["recent_events"] = recent_events[-MAX_STRATEGY_EVENTS:]
            recent_trades = normalized_strategy.get("recent_trades")
            if isinstance(recent_trades, list) and len(recent_trades) > MAX_STRATEGY_TRADES:
                normalized_strategy["recent_trades"] = recent_trades[-MAX_STRATEGY_TRADES:]
            self.strategy_metrics = normalized_strategy

        if isinstance(grvt_accounts_raw, dict):
            normalized_grvt = copy.deepcopy(grvt_accounts_raw)
            accounts_block = normalized_grvt.get("accounts")
            if isinstance(accounts_block, list) and len(accounts_block) > 50:
                normalized_grvt["accounts"] = accounts_block[:50]
            self.grvt_accounts = normalized_grvt

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
            "runtime_seconds": self.runtime_seconds,
        }
        if self.agent_id is not None:
            payload["agent_id"] = self.agent_id
        if self.spread_metrics is not None:
            payload["spread_metrics"] = copy.deepcopy(self.spread_metrics)
        if self.strategy_metrics is not None:
            payload["strategy_metrics"] = copy.deepcopy(self.strategy_metrics)
        if self.grvt_accounts is not None:
            payload["grvt_accounts"] = copy.deepcopy(self.grvt_accounts)
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
            if state.runtime_seconds > aggregate.runtime_seconds:
                aggregate.runtime_seconds = state.runtime_seconds
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

    def __init__(
        self,
        *,
        bark_notifier: Optional[BarkNotifier] = None,
        risk_alert_threshold: Optional[float] = None,
        risk_alert_reset: Optional[float] = None,
        risk_alert_cooldown: float = 900.0,
    ) -> None:
        self._states: Dict[str, HedgeState] = {}
        self._lock = asyncio.Lock()
        self._eviction_seconds = 6 * 3600  # prune entries idle for 6 hours
        self._stale_warning_seconds = 5 * 60  # tag agents as stale after 5 minutes
        self._last_agent_id: Optional[str] = None
        self._controls: Dict[str, Dict[str, Any]] = {}
        self._default_paused: bool = False
        self._bark_notifier = bark_notifier
        self._risk_alert_threshold = risk_alert_threshold if risk_alert_threshold and risk_alert_threshold > 0 else None
        if self._risk_alert_threshold is not None:
            if risk_alert_reset is not None and 0 < risk_alert_reset < self._risk_alert_threshold:
                self._risk_alert_reset = risk_alert_reset
            else:
                self._risk_alert_reset = self._risk_alert_threshold * 0.7
        else:
            self._risk_alert_reset = None
        self._risk_alert_cooldown = max(0.0, risk_alert_cooldown)
        self._risk_alert_active: Dict[str, bool] = {}
        self._risk_alert_last_ts: Dict[str, float] = {}

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
            control = {"paused": self._default_paused, "updated_at": time.time() if self._default_paused else None}
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
            "default_paused": self._default_paused,
        }

    async def set_all_paused(self, paused: bool) -> Dict[str, Any]:
        async with self._lock:
            self._default_paused = bool(paused)
            now = time.time()
            for control in self._controls.values():
                control["paused"] = bool(paused)
                control["updated_at"] = now
            snapshot = self._controls_snapshot()
            snapshot.update({"scope": "all", "paused": bool(paused)})
            return snapshot

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

        spread_map = {
            agent_id: copy.deepcopy(state.spread_metrics)
            for agent_id, state in self._states.items()
            if state.spread_metrics
        }
        if spread_map:
            snapshot["spread_metrics"] = spread_map
        strategy_map = {
            agent_id: copy.deepcopy(state.strategy_metrics)
            for agent_id, state in self._states.items()
            if state.strategy_metrics
        }
        if strategy_map:
            snapshot["strategy_metrics"] = strategy_map
        grvt_map = {
            agent_id: copy.deepcopy(state.grvt_accounts)
            for agent_id, state in self._states.items()
            if state.grvt_accounts
        }
        if grvt_map:
            snapshot["grvt_accounts"] = grvt_map
        return snapshot

    async def update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        alerts: Sequence[RiskAlertInfo] = []
        async with self._lock:
            agent_id = self._normalize_agent_id(payload.get("agent_id"))
            state = self._states.get(agent_id)
            if state is None:
                state = HedgeState(agent_id=agent_id)
                self._states[agent_id] = state

            state.update_from_payload(payload)
            alerts = self._prepare_risk_alerts(agent_id, state.grvt_accounts)

            now = time.time()
            self._prune_stale(now)
            self._last_agent_id = agent_id
            snapshot = self._build_snapshot(now)

        if alerts:
            self._schedule_risk_alerts(alerts)
        return snapshot

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            now = time.time()
            self._prune_stale(now)
            return self._build_snapshot(now)

    async def list_agent_ids(self) -> List[str]:
        async with self._lock:
            return list(self._states.keys())

    async def get_agent_transfer_defaults(self, agent_id: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_agent_id(agent_id)
        async with self._lock:
            state = self._states.get(normalized)
            if not state:
                return None
            grvt_block = state.grvt_accounts
            if not isinstance(grvt_block, dict):
                return None
            defaults = grvt_block.get("transfer_defaults")
            if isinstance(defaults, dict):
                return copy.deepcopy(defaults)
            return None

    def _prepare_risk_alerts(self, agent_id: str, grvt_payload: Optional[Dict[str, Any]]) -> Sequence[RiskAlertInfo]:
        if self._bark_notifier is None or self._risk_alert_threshold is None:
            return []
        if not isinstance(grvt_payload, dict):
            return []

        threshold = self._risk_alert_threshold
        reset_ratio = self._risk_alert_reset if self._risk_alert_reset is not None else threshold * 0.7
        summary = grvt_payload.get("summary") if isinstance(grvt_payload.get("summary"), dict) else None
        now = time.time()
        alerts: List[RiskAlertInfo] = []
        seen_keys: set[str] = set()

        for entry_key, label, account_payload in self._flatten_grvt_accounts(agent_id, grvt_payload):
            seen_keys.add(entry_key)
            total_value = self._decimal_from(account_payload.get("total_pnl"))
            if total_value is None:
                total_value = self._decimal_from(account_payload.get("total"))
            if total_value is None:
                self._risk_alert_active.pop(entry_key, None)
                continue

            base_value, base_label = self._select_base_value(account_payload, summary)
            ratio = self._compute_risk_ratio(total_value, base_value)

            if ratio is None:
                self._risk_alert_active.pop(entry_key, None)
                continue

            if ratio >= threshold:
                already_active = self._risk_alert_active.get(entry_key, False)
                last_ts = self._risk_alert_last_ts.get(entry_key, 0.0)
                if not already_active and (now - last_ts) >= self._risk_alert_cooldown:
                    alerts.append(
                        RiskAlertInfo(
                            key=entry_key,
                            agent_id=agent_id,
                            account_label=label,
                            ratio=ratio,
                            loss_value=abs(total_value),
                            base_value=base_value if base_value is not None else Decimal("0"),
                            base_label=base_label or "wallet",
                        )
                    )
                    self._risk_alert_active[entry_key] = True
                    self._risk_alert_last_ts[entry_key] = now
                elif already_active:
                    # keep active flag but no new alert
                    pass
            else:
                if ratio < reset_ratio:
                    self._risk_alert_active.pop(entry_key, None)

        agent_prefix = f"{agent_id}:"
        stale_keys = [
            key
            for key in self._risk_alert_active
            if key.startswith(agent_prefix) and key not in seen_keys
        ]
        for key in stale_keys:
            self._risk_alert_active.pop(key, None)
            self._risk_alert_last_ts.pop(key, None)

        return alerts

    def _flatten_grvt_accounts(
        self, agent_id: str, payload: Dict[str, Any]
    ) -> List[tuple[str, str, Dict[str, Any]]]:
        entries: List[tuple[str, str, Dict[str, Any]]] = []
        accounts = payload.get("accounts")
        if isinstance(accounts, list) and accounts:
            for idx, account in enumerate(accounts):
                if not isinstance(account, dict):
                    continue
                label_raw = account.get("name")
                try:
                    label = str(label_raw).strip()
                except Exception:
                    label = ""
                if not label:
                    label = f"Account {idx + 1}"
                entries.append((f"{agent_id}:{label}", label, account))
        else:
            summary = payload.get("summary")
            if isinstance(summary, dict):
                entries.append((f"{agent_id}:summary", "Summary", summary))
        return entries

    @staticmethod
    def _select_base_value(
        primary: Dict[str, Any], fallback: Optional[Dict[str, Any]] = None
    ) -> tuple[Optional[Decimal], str]:
        for source in (primary, fallback or {}):
            if not isinstance(source, dict):
                continue
            for field in ("equity", "available_equity", "balance", "available_balance"):
                value = HedgeCoordinator._decimal_from(source.get(field))
                if value is not None and value > 0:
                    label = "equity" if field in {"equity", "available_equity"} else "wallet"
                    return value, label
        return None, ""

    @staticmethod
    def _compute_risk_ratio(total_value: Decimal, base_value: Optional[Decimal]) -> Optional[float]:
        if total_value is None or total_value >= 0:
            return None
        if base_value is None or base_value <= 0:
            return None
        try:
            ratio = (-total_value) / base_value
            if ratio <= 0:
                return None
            return float(ratio)
        except Exception:
            return None

    @staticmethod
    def _decimal_from(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        try:
            return format(value, ",.2f")
        except Exception:
            return str(value)

    def _schedule_risk_alerts(self, alerts: Sequence[RiskAlertInfo]) -> None:
        if not alerts or self._bark_notifier is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            LOGGER.warning("Unable to schedule Bark alert; event loop not running")
            return
        for alert in alerts:
            loop.create_task(self._send_risk_alert(alert))

    async def _send_risk_alert(self, alert: RiskAlertInfo) -> None:
        if self._bark_notifier is None:
            return
        title = f"GRVT Risk {alert.ratio * 100:.1f}%"
        base_label = alert.base_label or "wallet"
        body = (
            f"{alert.account_label} ({alert.agent_id}) loss {self._format_decimal(alert.loss_value)} "
            f"/ {base_label} {self._format_decimal(alert.base_value)}"
        )
        await self._bark_notifier.send(title=title, body=body)


class CoordinatorApp:
    def __init__(
        self,
        *,
        dashboard_username: Optional[str] = None,
        dashboard_password: Optional[str] = None,
        dashboard_session_ttl: float = 12 * 3600,
        bark_notifier: Optional[BarkNotifier] = None,
        risk_alert_threshold: Optional[float] = None,
        risk_alert_reset: Optional[float] = None,
        risk_alert_cooldown: float = 900.0,
    ) -> None:
        self._coordinator = HedgeCoordinator(
            bark_notifier=bark_notifier,
            risk_alert_threshold=risk_alert_threshold,
            risk_alert_reset=risk_alert_reset,
            risk_alert_cooldown=risk_alert_cooldown,
        )
        self._adjustments = GrvtAdjustmentManager()
        self._dashboard_username = (dashboard_username or "").strip()
        self._dashboard_password = (dashboard_password or "").strip()
        self._session_cookie: str = "hedge_session"
        self._session_ttl: float = max(float(dashboard_session_ttl), 300.0)
        self._sessions: Dict[str, float] = {}
        self._auto_balance_cfg: Optional[AutoBalanceConfig] = None
        self._auto_balance_lock = asyncio.Lock()
        self._auto_balance_cooldown_until: Optional[float] = None
        self._auto_balance_status: Dict[str, Any] = {
            "enabled": False,
            "config": None,
            "last_request_id": None,
            "last_action_at": None,
            "last_error": None,
            "last_transfer_amount": None,
            "last_direction": None,
            "cooldown_until": None,
            "cooldown_active": False,
            "measurement": None,
        }
        self._app = web.Application()
        self._app.add_routes(
            [
                web.get("/", self.handle_dashboard_redirect),
                web.get("/login", self.handle_login_form),
                web.post("/login", self.handle_login_submit),
                web.post("/logout", self.handle_logout),
                web.get("/dashboard", self.handle_dashboard),
                web.get("/metrics", self.handle_metrics),
                web.post("/update", self.handle_update),
                web.get("/control", self.handle_control_get),
                web.post("/control", self.handle_control_update),
                web.get("/auto_balance/config", self.handle_auto_balance_get),
                web.post("/auto_balance/config", self.handle_auto_balance_update),
                web.get("/grvt/adjustments", self.handle_grvt_adjustments),
                web.post("/grvt/adjust", self.handle_grvt_adjust),
                web.post("/grvt/transfer", self.handle_grvt_transfer),
                web.post("/grvt/adjust/ack", self.handle_grvt_adjust_ack),
            ]
        )

    @property
    def app(self) -> web.Application:
        return self._app

    async def handle_dashboard_redirect(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request, redirect_on_fail=True)
        raise web.HTTPFound("/dashboard")

    async def handle_dashboard(self, request: web.Request) -> web.StreamResponse:
        self._enforce_dashboard_auth(request, redirect_on_fail=True)
        if not DASHBOARD_PATH.exists():
            raise web.HTTPNotFound(text="dashboard asset missing; ensure hedge_dashboard.html exists")
        return web.FileResponse(path=DASHBOARD_PATH)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = await self._coordinator.snapshot()
        payload["grvt_adjustments"] = await self._adjustments.summary()
        payload["auto_balance"] = self._auto_balance_status_snapshot()
        return web.json_response(payload)

    async def handle_update(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="update payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="update payload must be an object")

        state = await self._coordinator.update(body)
        await self._maybe_auto_balance(state)
        return web.json_response(state)

    async def handle_control_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        agent_id = request.rel_url.query.get("agent_id")
        snapshot = await self._coordinator.control_snapshot(agent_id)
        if agent_id:
            pending = await self._adjustments.pending_for_agent(agent_id)
            if pending:
                agent_block = snapshot.get("agent")
                if isinstance(agent_block, dict):
                    agent_block["pending_adjustments"] = pending
        return web.json_response(snapshot)

    async def handle_control_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="control payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="control payload must be an object")

        action_raw = body.get("action")
        paused_flag = body.get("paused")
        scope_raw = body.get("scope")
        scope = str(scope_raw).strip().lower() if isinstance(scope_raw, str) else None

        if scope == "all":
            if action_raw is None and paused_flag is None:
                raise web.HTTPBadRequest(text="control payload requires action or paused flag for scope 'all'")

            if action_raw is not None:
                action = str(action_raw).strip().lower()
                if action == "pause":
                    snapshot = await self._coordinator.set_all_paused(True)
                elif action in {"resume", "unpause"}:
                    snapshot = await self._coordinator.set_all_paused(False)
                else:
                    raise web.HTTPBadRequest(text="invalid control action for scope 'all'")
            else:
                snapshot = await self._coordinator.set_all_paused(bool(paused_flag))

            return web.json_response(snapshot)

        agent_id_raw = body.get("agent_id")
        if agent_id_raw is None:
            raise web.HTTPBadRequest(text="agent_id is required")

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

    async def handle_auto_balance_get(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        payload = {
            "config": self._auto_balance_config_as_payload(),
            "status": self._auto_balance_status_snapshot(),
        }
        return web.json_response(payload)

    async def handle_auto_balance_update(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="auto balance payload must be JSON")
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="auto balance payload must be an object")

        action_raw = body.get("action")
        action = str(action_raw).strip().lower() if isinstance(action_raw, str) else None
        enabled_flag = body.get("enabled")
        if enabled_flag is False or action == "disable":
            self._update_auto_balance_config(None)
            return web.json_response({
                "config": None,
                "status": self._auto_balance_status_snapshot(),
            })

        try:
            config = self._parse_auto_balance_config(body)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        self._update_auto_balance_config(config)
        return web.json_response({
            "config": self._auto_balance_config_as_payload(),
            "status": self._auto_balance_status_snapshot(),
        })

    async def handle_grvt_adjustments(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        summary = await self._adjustments.summary()
        return web.json_response(summary)

    async def handle_grvt_adjust(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="adjustment payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="adjustment payload must be an object")

        action_raw = (body.get("action") or "").strip().lower()
        if action_raw not in {"add", "reduce"}:
            raise web.HTTPBadRequest(text="action must be 'add' or 'reduce'")

        magnitude_raw = body.get("magnitude", 1)
        try:
            magnitude = float(magnitude_raw)
        except (TypeError, ValueError):
            raise web.HTTPBadRequest(text="magnitude must be numeric")

        agent_ids_raw = body.get("agent_ids")
        if agent_ids_raw is None:
            agent_ids = await self._coordinator.list_agent_ids()
        elif isinstance(agent_ids_raw, list):
            agent_ids = [str(agent).strip() for agent in agent_ids_raw if str(agent).strip()]
        else:
            raise web.HTTPBadRequest(text="agent_ids must be an array of strings")

        if not agent_ids:
            raise web.HTTPBadRequest(text="No agent IDs available for adjustment; ensure bots are reporting metrics")

        symbols_raw = body.get("symbols")
        if symbols_raw is None and body.get("symbol"):
            symbols_raw = [body.get("symbol")]

        if symbols_raw is None:
            symbols: Optional[List[str]] = None
        elif isinstance(symbols_raw, list):
            normalized_symbols: List[str] = []
            for item in symbols_raw:
                value = self._normalize_symbol(item)
                if value and value not in normalized_symbols:
                    normalized_symbols.append(value)
            symbols = normalized_symbols
        else:
            raise web.HTTPBadRequest(text="symbols must be an array of strings")

        created_by = request.remote or "dashboard"
        action = cast(AdjustmentAction, action_raw)
        try:
            payload = await self._adjustments.create_request(
                action=action,
                magnitude=magnitude,
                agent_ids=agent_ids,
                symbols=symbols,
                created_by=created_by,
            )
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        return web.json_response({"request": payload})

    async def handle_grvt_transfer(self, request: web.Request) -> web.Response:
        self._enforce_dashboard_auth(request)
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="transfer payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="transfer payload must be an object")

        payload = await self._process_transfer_request(body, created_by=request.remote or "dashboard")
        return web.json_response({"request": payload})

    async def _process_transfer_request(self, body: Dict[str, Any], *, created_by: str) -> Dict[str, Any]:
        def _clean_required(value: Any, field: str) -> str:
            try:
                text = str(value).strip()
            except Exception:
                text = ""
            if not text:
                raise web.HTTPBadRequest(text=f"{field} is required")
            return text

        def _clean_optional(value: Any) -> Optional[str]:
            if value is None:
                return None
            try:
                text = str(value).strip()
            except Exception:
                return None
            return text or None

        amount_raw = body.get("num_tokens") or body.get("amount")
        try:
            amount = Decimal(str(amount_raw))
        except Exception:
            raise web.HTTPBadRequest(text="num_tokens must be numeric")
        if amount <= 0:
            raise web.HTTPBadRequest(text="num_tokens must be positive")

        currency = (body.get("currency") or "USDT")
        currency_clean = _clean_required(currency, "currency").upper()

        agent_ids_raw = body.get("agent_ids")
        if agent_ids_raw is None:
            agent_ids = await self._coordinator.list_agent_ids()
        elif isinstance(agent_ids_raw, list):
            agent_ids = [
                str(agent).strip()
                for agent in agent_ids_raw
                if str(agent).strip()
            ]
        else:
            raise web.HTTPBadRequest(text="agent_ids must be an array of strings")
        if not agent_ids:
            raise web.HTTPBadRequest(text="No agent IDs available for transfer request")
        if len(agent_ids) > 1:
            raise web.HTTPBadRequest(text="Transfers currently support a single source agent per request")

        source_agent_id = agent_ids[0]
        target_agent_id = _clean_optional(
            body.get("target_agent_id")
            or body.get("destination_agent_id")
            or body.get("to_agent_id")
            or body.get("target_agent")
        )

        transfer_defaults = await self._coordinator.get_agent_transfer_defaults(source_agent_id)
        target_defaults: Optional[Dict[str, Any]] = None
        if target_agent_id:
            target_defaults = await self._coordinator.get_agent_transfer_defaults(target_agent_id)
            if target_defaults is None:
                raise web.HTTPBadRequest(text=f"target_agent_id {target_agent_id} has no transfer defaults")

        metadata = body.get("transfer_metadata") or {}
        if metadata and not isinstance(metadata, dict):
            raise web.HTTPBadRequest(text="transfer_metadata must be an object if provided")
        metadata = dict(metadata or {})
        if body.get("reason") and not metadata.get("reason"):
            metadata["reason"] = str(body["reason"])
        metadata.setdefault("agent_id", source_agent_id)
        if transfer_defaults and transfer_defaults.get("agent_label"):
            metadata.setdefault("agent_label", str(transfer_defaults["agent_label"]))
        if target_agent_id:
            metadata.setdefault("target_agent_id", target_agent_id)
            if target_defaults and target_defaults.get("agent_label"):
                metadata.setdefault("target_agent_label", str(target_defaults["agent_label"]))

        requested_direction = _clean_optional(body.get("direction"))
        default_direction = _clean_optional((transfer_defaults or {}).get("direction"))
        direction = (requested_direction or default_direction or "main_to_main").lower()
        allowed_directions = {"sub_to_main", "main_to_sub", "main_to_main"}
        if direction not in allowed_directions:
            raise web.HTTPBadRequest(text=f"Unsupported direction '{direction}'")
        if direction == "main_to_main":
            if not target_agent_id:
                raise web.HTTPBadRequest(text="target_agent_id is required for main_to_main transfers")
            if target_agent_id == source_agent_id:
                raise web.HTTPBadRequest(text="target_agent_id must differ from the source for main_to_main transfers")
        metadata.setdefault("direction", direction)

        routes_block = transfer_defaults.get("routes") if isinstance(transfer_defaults, dict) else None
        route_candidate = routes_block.get(direction) if isinstance(routes_block, dict) else None

        def _route_from_defaults() -> Optional[Dict[str, str]]:
            if not isinstance(transfer_defaults, dict):
                return None
            if isinstance(route_candidate, dict):
                return route_candidate
            main_account = _clean_optional(transfer_defaults.get("main_account_id"))
            trading_sub = _clean_optional(transfer_defaults.get("sub_account_id"))
            main_sub = _clean_optional(transfer_defaults.get("main_sub_account_id")) or "0"
            if not main_account:
                return None
            if direction == "sub_to_main":
                if not trading_sub:
                    return None
                return {
                    "from_account_id": main_account,
                    "from_sub_account_id": trading_sub,
                    "to_account_id": main_account,
                    "to_sub_account_id": main_sub,
                }
            if direction == "main_to_sub":
                if not trading_sub:
                    return None
                return {
                    "from_account_id": main_account,
                    "from_sub_account_id": main_sub,
                    "to_account_id": main_account,
                    "to_sub_account_id": trading_sub,
                }
            if direction == "main_to_main":
                return {
                    "from_account_id": main_account,
                    "from_sub_account_id": main_sub,
                    "to_account_id": main_account,
                    "to_sub_account_id": main_sub,
                }
            return None

        route_values = _route_from_defaults()

        def _select_from_defaults(defaults: Optional[Dict[str, Any]], keys: Sequence[str]) -> Optional[str]:
            if not isinstance(defaults, dict):
                return None
            for key in keys:
                candidate = _clean_optional(defaults.get(key))
                if candidate:
                    return candidate
            return None

        if direction == "main_to_main" and target_defaults:
            if route_values is None:
                route_values = {}
            target_account = _select_from_defaults(target_defaults, ["main_account_id", "to_account_id"])
            target_sub = _select_from_defaults(target_defaults, ["main_sub_account_id", "to_sub_account_id"]) or "0"
            if target_account:
                route_values["to_account_id"] = target_account
            if target_sub:
                route_values["to_sub_account_id"] = target_sub

        transfer_payload: Dict[str, Any] = {
            "currency": currency_clean,
            "num_tokens": format(amount, "f"),
            "direction": direction,
        }

        user_fields = {
            "from_account_id": _clean_optional(body.get("from_account_id")),
            "from_sub_account_id": _clean_optional(body.get("from_sub_account_id")),
            "to_account_id": _clean_optional(body.get("to_account_id")),
            "to_sub_account_id": _clean_optional(body.get("to_sub_account_id")),
        }

        fallback_map = {
            "from_account_id": ["main_account_id", "from_account_id"],
            "from_sub_account_id": ["sub_account_id", "from_sub_account_id"],
            "to_account_id": ["main_account_id", "to_account_id"],
            "to_sub_account_id": ["main_sub_account_id", "to_sub_account_id"],
        }
        if direction == "main_to_sub":
            fallback_map["from_sub_account_id"] = ["main_sub_account_id", "from_sub_account_id", "sub_account_id"]
            fallback_map["to_sub_account_id"] = ["sub_account_id", "to_sub_account_id", "main_sub_account_id"]
        if direction == "main_to_main":
            fallback_map["from_sub_account_id"] = ["main_sub_account_id", "from_sub_account_id", "sub_account_id"]

        target_fallback_map = {
            "to_account_id": ["main_account_id", "to_account_id"],
            "to_sub_account_id": ["main_sub_account_id", "to_sub_account_id"],
        }

        for field, provided in user_fields.items():
            if provided:
                transfer_payload[field] = provided
                continue
            candidate = _clean_optional(route_values.get(field)) if route_values else None
            if not candidate and direction == "main_to_main" and field in target_fallback_map and target_defaults:
                candidate = _select_from_defaults(target_defaults, target_fallback_map[field])
            if not candidate and isinstance(transfer_defaults, dict):
                for key in fallback_map.get(field, []):
                    candidate = _clean_optional(transfer_defaults.get(key))
                    if candidate:
                        break
            if candidate:
                transfer_payload[field] = candidate

        missing_fields = [field for field in ("from_account_id", "from_sub_account_id", "to_account_id", "to_sub_account_id") if not transfer_payload.get(field)]
        if missing_fields:
            raise web.HTTPBadRequest(text=f"Missing transfer fields: {', '.join(missing_fields)}")

        transfer_type = body.get("transfer_type") or ((transfer_defaults or {}).get("transfer_type"))
        if transfer_type:
            transfer_payload["transfer_type"] = str(transfer_type).strip()
        if metadata:
            transfer_payload["transfer_metadata"] = metadata

        payload = await self._adjustments.create_request(
            action="transfer",
            magnitude=float(amount),
            agent_ids=agent_ids,
            symbols=None,
            created_by=created_by,
            payload=transfer_payload,
        )
        return payload

    def _auto_balance_status_snapshot(self) -> Dict[str, Any]:
        snapshot = copy.deepcopy(self._auto_balance_status)
        snapshot["enabled"] = bool(self._auto_balance_cfg)
        snapshot["config"] = self._auto_balance_config_as_payload()
        return snapshot

    async def _maybe_auto_balance(self, snapshot: Dict[str, Any]) -> None:
        if not self._auto_balance_cfg:
            return
        async with self._auto_balance_lock:
            cfg = self._auto_balance_cfg
            measurement = self._compute_auto_balance_measurement(snapshot)
            self._auto_balance_status["measurement"] = measurement.as_payload() if measurement else None
            now = time.time()
            cooldown_until = self._auto_balance_cooldown_until
            if cooldown_until is not None and now < cooldown_until:
                self._auto_balance_status["cooldown_until"] = cooldown_until
                self._auto_balance_status["cooldown_active"] = True
                return
            self._auto_balance_status["cooldown_active"] = False
            self._auto_balance_status["cooldown_until"] = cooldown_until
            if not measurement:
                return
            if measurement.ratio < cfg.threshold_ratio:
                return
            requested_amount = measurement.transfer_amount
            if requested_amount < cfg.min_transfer:
                return
            if cfg.max_transfer is not None and requested_amount > cfg.max_transfer:
                requested_amount = cfg.max_transfer
            if requested_amount < cfg.min_transfer:
                return

            measurement_payload = measurement.as_payload()
            metadata = {
                "auto_balance": {
                    "ratio": measurement_payload["ratio"],
                    "threshold": cfg.threshold_ratio,
                    "difference": measurement_payload["difference"],
                    "agent_a_equity": measurement_payload["equity_a"],
                    "agent_b_equity": measurement_payload["equity_b"],
                    "computed_amount": measurement_payload["transfer_amount"],
                    "requested_amount": self._decimal_to_str(requested_amount),
                    "currency": cfg.currency,
                    "source_agent": measurement.source_agent,
                    "target_agent": measurement.target_agent,
                    "snapshot_ts": now,
                }
            }
            reason = f"auto_balance {measurement.source_agent}->{measurement.target_agent}"
            request_body = {
                "agent_ids": [measurement.source_agent],
                "target_agent_id": measurement.target_agent,
                "currency": cfg.currency,
                "num_tokens": self._decimal_to_str(requested_amount),
                "direction": "main_to_main",
                "transfer_metadata": metadata,
                "reason": reason,
            }
            try:
                response = await self._process_transfer_request(request_body, created_by="auto_balance")
            except web.HTTPError as exc:
                error_text = getattr(exc, "text", None) or str(exc)
                self._auto_balance_status["last_error"] = error_text
                if cfg.cooldown_seconds > 0:
                    self._auto_balance_cooldown_until = now + cfg.cooldown_seconds
                else:
                    self._auto_balance_cooldown_until = None
                self._auto_balance_status["cooldown_until"] = self._auto_balance_cooldown_until
                self._auto_balance_status["cooldown_active"] = (
                    self._auto_balance_cooldown_until is not None and now < self._auto_balance_cooldown_until
                )
                LOGGER.warning("Auto balance transfer failed: %s", error_text)
                return

            self._auto_balance_status["last_error"] = None
            self._auto_balance_status["last_request_id"] = response.get("request_id")
            self._auto_balance_status["last_action_at"] = now
            self._auto_balance_status["last_transfer_amount"] = self._decimal_to_str(requested_amount)
            self._auto_balance_status["last_direction"] = {
                "from": measurement.source_agent,
                "to": measurement.target_agent,
            }
            if cfg.cooldown_seconds > 0:
                self._auto_balance_cooldown_until = now + cfg.cooldown_seconds
            else:
                self._auto_balance_cooldown_until = None
            self._auto_balance_status["cooldown_until"] = self._auto_balance_cooldown_until
            self._auto_balance_status["cooldown_active"] = (
                self._auto_balance_cooldown_until is not None and now < self._auto_balance_cooldown_until
            )
            LOGGER.info(
                "Auto balance transfer: %s -> %s %s %s (ratio %.2f%%)",
                measurement.source_agent,
                measurement.target_agent,
                self._decimal_to_str(requested_amount),
                cfg.currency,
                measurement.ratio * 100,
            )

    def _compute_auto_balance_measurement(self, snapshot: Dict[str, Any]) -> Optional[AutoBalanceMeasurement]:
        if not self._auto_balance_cfg:
            return None
        agents_block = snapshot.get("agents")
        if not isinstance(agents_block, dict):
            return None
        entry_a = agents_block.get(self._auto_balance_cfg.agent_a)
        entry_b = agents_block.get(self._auto_balance_cfg.agent_b)
        if not isinstance(entry_a, dict) or not isinstance(entry_b, dict):
            return None
        equity_a = self._extract_equity_from_agent(entry_a, prefer_available=self._auto_balance_cfg.use_available_equity)
        equity_b = self._extract_equity_from_agent(entry_b, prefer_available=self._auto_balance_cfg.use_available_equity)
        if equity_a is None or equity_b is None:
            return None
        if equity_a <= 0 or equity_b <= 0:
            return None
        difference = abs(equity_a - equity_b)
        if difference <= 0:
            return None
        max_equity = equity_a if equity_a >= equity_b else equity_b
        if max_equity <= 0:
            return None
        ratio = float(difference / max_equity)
        source_agent = self._auto_balance_cfg.agent_a if equity_a >= equity_b else self._auto_balance_cfg.agent_b
        target_agent = self._auto_balance_cfg.agent_b if source_agent == self._auto_balance_cfg.agent_a else self._auto_balance_cfg.agent_a
        transfer_amount = difference / Decimal("2")
        if transfer_amount <= 0:
            return None
        return AutoBalanceMeasurement(
            agent_a=self._auto_balance_cfg.agent_a,
            agent_b=self._auto_balance_cfg.agent_b,
            equity_a=equity_a,
            equity_b=equity_b,
            difference=difference,
            ratio=ratio,
            source_agent=source_agent,
            target_agent=target_agent,
            transfer_amount=transfer_amount,
        )

    def _extract_equity_from_agent(self, agent_payload: Dict[str, Any], *, prefer_available: bool) -> Optional[Decimal]:
        grvt_block = agent_payload.get("grvt_accounts")
        if not isinstance(grvt_block, dict):
            return None
        summary = grvt_block.get("summary")
        if not isinstance(summary, dict):
            return None
        if prefer_available:
            fields = ("available_equity", "available_balance", "equity", "balance")
        else:
            fields = ("equity", "balance", "available_equity", "available_balance")
        for field in fields:
            value = HedgeCoordinator._decimal_from(summary.get(field))
            if value is not None:
                return value
        return None

    @staticmethod
    def _decimal_to_str(value: Decimal) -> str:
        try:
            return format(value, "f")
        except Exception:
            return str(value)

    def _auto_balance_config_as_payload(self) -> Optional[Dict[str, Any]]:
        cfg = self._auto_balance_cfg
        if not cfg:
            return None
        payload: Dict[str, Any] = {
            "agent_a": cfg.agent_a,
            "agent_b": cfg.agent_b,
            "threshold_ratio": cfg.threshold_ratio,
            "threshold_percent": cfg.threshold_ratio * 100.0,
            "min_transfer": self._decimal_to_str(cfg.min_transfer),
            "currency": cfg.currency,
            "cooldown_seconds": cfg.cooldown_seconds,
            "use_available_equity": cfg.use_available_equity,
        }
        if cfg.max_transfer is not None:
            payload["max_transfer"] = self._decimal_to_str(cfg.max_transfer)
        else:
            payload["max_transfer"] = None
        return payload

    def _update_auto_balance_config(self, config: Optional[AutoBalanceConfig]) -> None:
        self._auto_balance_cfg = config
        self._auto_balance_cooldown_until = None
        self._auto_balance_status["enabled"] = bool(config)
        self._auto_balance_status["config"] = self._auto_balance_config_as_payload()
        self._auto_balance_status["cooldown_until"] = None
        self._auto_balance_status["cooldown_active"] = False
        self._auto_balance_status["measurement"] = None
        self._auto_balance_status["last_error"] = None
        self._auto_balance_status["last_request_id"] = None
        self._auto_balance_status["last_action_at"] = None
        self._auto_balance_status["last_transfer_amount"] = None
        self._auto_balance_status["last_direction"] = None
        if not config:
            LOGGER.info("Auto balance disabled via dashboard")
        else:
            try:
                min_text = self._decimal_to_str(config.min_transfer)
            except Exception:
                min_text = str(config.min_transfer)
            LOGGER.info(
                "Auto balance configured via dashboard: %s ↔ %s (threshold %.2f%%, min %s %s)",
                config.agent_a,
                config.agent_b,
                config.threshold_ratio * 100,
                min_text,
                config.currency,
            )

    def _parse_auto_balance_config(self, body: Dict[str, Any]) -> AutoBalanceConfig:
        agent_a = HedgeCoordinator._normalize_agent_id(body.get("agent_a"))
        agent_b = HedgeCoordinator._normalize_agent_id(body.get("agent_b") or body.get("target_agent"))
        if not agent_a or not agent_b:
            raise ValueError("agent_a and agent_b are required")
        if agent_a == agent_b:
            raise ValueError("agent_a and agent_b must differ")

        threshold = body.get("threshold_ratio")
        if threshold is None:
            percent_raw = body.get("threshold_percent")
            if percent_raw is not None:
                try:
                    threshold = float(percent_raw) / 100.0
                except (TypeError, ValueError):
                    raise ValueError("threshold_percent must be numeric")
        if threshold is None:
            raise ValueError("threshold_ratio is required")
        try:
            threshold_value = float(threshold)
        except (TypeError, ValueError):
            raise ValueError("threshold_ratio must be numeric")
        if threshold_value <= 0:
            raise ValueError("threshold_ratio must be positive")

        min_transfer_raw = body.get("min_transfer") or body.get("minimum_transfer")
        if min_transfer_raw is None:
            raise ValueError("min_transfer is required")
        try:
            min_transfer = Decimal(str(min_transfer_raw))
        except Exception:
            raise ValueError("min_transfer must be numeric")
        if min_transfer <= 0:
            raise ValueError("min_transfer must be positive")

        max_transfer_raw = body.get("max_transfer")
        max_transfer: Optional[Decimal] = None
        if max_transfer_raw is not None and str(max_transfer_raw).strip() != "":
            try:
                max_transfer_candidate = Decimal(str(max_transfer_raw))
            except Exception:
                raise ValueError("max_transfer must be numeric if provided")
            if max_transfer_candidate > 0:
                max_transfer = max_transfer_candidate

        cooldown_raw = body.get("cooldown_seconds")
        if cooldown_raw is None:
            cooldown_seconds = 900.0
        else:
            try:
                cooldown_seconds = float(cooldown_raw)
            except (TypeError, ValueError):
                raise ValueError("cooldown_seconds must be numeric")
            if cooldown_seconds < 0:
                cooldown_seconds = 0.0

        currency_raw = body.get("currency") or "USDT"
        currency = str(currency_raw).strip().upper() or "USDT"
        use_available = self._interpret_bool(body.get("use_available_equity"))

        return AutoBalanceConfig(
            agent_a=agent_a,
            agent_b=agent_b,
            threshold_ratio=threshold_value,
            min_transfer=min_transfer,
            max_transfer=max_transfer,
            cooldown_seconds=cooldown_seconds,
            currency=currency,
            use_available_equity=use_available,
        )

    @staticmethod
    def _interpret_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"1", "true", "yes", "on"}:
                return True
            if text in {"0", "false", "no", "off"}:
                return False
        if value is None:
            return False
        return bool(value)

    async def handle_grvt_adjust_ack(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="ack payload must be JSON")

        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="ack payload must be an object")

        request_id = (body.get("request_id") or "").strip()
        agent_id = (body.get("agent_id") or "").strip()
        status = body.get("status") or "acknowledged"
        note = body.get("note")

        if not request_id:
            raise web.HTTPBadRequest(text="request_id is required")
        if not agent_id:
            raise web.HTTPBadRequest(text="agent_id is required")

        try:
            payload = await self._adjustments.acknowledge(
                request_id=request_id,
                agent_id=agent_id,
                status=status,
                note=(note if isinstance(note, str) else None),
            )
        except KeyError as exc:
            raise web.HTTPNotFound(text=str(exc))
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc))

        return web.json_response({"request": payload})

    @staticmethod
    def _normalize_symbol(symbol: Any) -> str:
        try:
            text = str(symbol).strip()
        except Exception:
            return ""
        text = text.upper()
        return text[:80] if text else ""

    def _credentials_configured(self) -> bool:
        return bool(self._dashboard_username or self._dashboard_password)

    def _validate_password(self, username: str, password: str) -> bool:
        expected_username = self._dashboard_username
        expected_password = self._dashboard_password
        if not self._credentials_configured():
            return True
        if expected_username and username != expected_username:
            return False
        if expected_password and password != expected_password:
            return False
        return True

    def _validate_basic_header(self, request: web.Request) -> bool:
        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return False
        provided_username, _, provided_password = decoded.partition(":")
        return self._validate_password(provided_username, provided_password)

    def _validate_session(self, request: web.Request) -> bool:
        token = request.cookies.get(self._session_cookie)
        if not token:
            return False
        expires_at = self._sessions.get(token)
        if not expires_at:
            return False
        now = time.time()
        if expires_at < now:
            self._sessions.pop(token, None)
            return False
        self._sessions[token] = now + self._session_ttl
        return True

    def _issue_session(self) -> str:
        token = secrets.token_urlsafe(32)
        self._sessions[token] = time.time() + self._session_ttl
        return token

    def _invalidate_session(self, token: Optional[str]) -> None:
        if token:
            self._sessions.pop(token, None)

    def _enforce_dashboard_auth(self, request: web.Request, *, redirect_on_fail: bool = False) -> None:
        if not self._credentials_configured():
            return
        if self._validate_session(request):
            return
        if self._validate_basic_header(request):
            return
        if redirect_on_fail and request.method == "GET":
            raise web.HTTPFound("/login")
        raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Hedge Dashboard"'})

    def _render_login_page(self, error: bool = False) -> web.Response:
        error_block = ""
        if error:
            error_block = '<div class="error">用户名或密码不正确</div>'
        html = LOGIN_TEMPLATE.replace("{error_block}", error_block)
        return web.Response(text=html, content_type="text/html")

    async def handle_login_form(self, request: web.Request) -> web.Response:
        if not self._credentials_configured():
            raise web.HTTPFound("/dashboard")
        if self._validate_session(request):
            raise web.HTTPFound("/dashboard")
        error = request.rel_url.query.get("error") == "1"
        return self._render_login_page(error)

    async def handle_login_submit(self, request: web.Request) -> web.Response:
        if not self._credentials_configured():
            raise web.HTTPFound("/dashboard")
        data = await request.post()
        username_raw = data.get("username")
        password_raw = data.get("password")
        username = str(username_raw) if username_raw is not None else ""
        password = str(password_raw) if password_raw is not None else ""
        username = username.strip()
        password = password.strip()
        if not self._validate_password(username, password):
            raise web.HTTPFound("/login?error=1")
        token = self._issue_session()
        response = web.HTTPFound("/dashboard")
        response.set_cookie(
            self._session_cookie,
            token,
            max_age=int(self._session_ttl),
            httponly=True,
            secure=False,
            samesite="Lax",
        )
        return response

    async def handle_logout(self, request: web.Request) -> web.Response:
        if not self._credentials_configured():
            raise web.HTTPFound("/dashboard")
        self._enforce_dashboard_auth(request)
        token = request.cookies.get(self._session_cookie)
        self._invalidate_session(token)
        response = web.HTTPFound("/login")
        response.del_cookie(self._session_cookie)
        return response
async def _run_app(args: argparse.Namespace) -> None:
    log_level_name = args.log_level
    if getattr(args, "quiet", False):
        log_level_name = "WARNING"
    logging.basicConfig(level=getattr(logging, (log_level_name or "INFO").upper(), logging.INFO))

    risk_threshold = args.risk_alert_threshold if args.risk_alert_threshold and args.risk_alert_threshold > 0 else None
    risk_reset = None
    if risk_threshold and args.risk_alert_reset and args.risk_alert_reset > 0:
        risk_reset = min(args.risk_alert_reset, risk_threshold)
    bark_notifier: Optional[BarkNotifier] = None
    bark_url = (args.bark_url or "").strip()
    if bark_url:
        bark_notifier = BarkNotifier(
            bark_url,
            timeout=max(args.bark_timeout, 1.0),
            append_payload=bool(args.bark_append_title_body),
        )
        if risk_threshold:
            LOGGER.info(
                "Bark notifications enabled; risk alerts fire at %.1f%%",
                risk_threshold * 100,
            )
        if not args.bark_append_title_body:
            LOGGER.info("Bark notifier configured to skip automatic title/body appending")
    elif risk_threshold:
        LOGGER.warning(
            "Risk alert threshold %.1f%% configured without --bark-url; alerts disabled.",
            risk_threshold * 100,
        )

    coordinator_app = CoordinatorApp(
        dashboard_username=args.dashboard_username,
        dashboard_password=args.dashboard_password,
        dashboard_session_ttl=args.dashboard_session_ttl,
        bark_notifier=bark_notifier,
        risk_alert_threshold=risk_threshold,
        risk_alert_reset=risk_reset,
        risk_alert_cooldown=max(args.risk_alert_cooldown, 0.0),
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
    def _env_float(name: str, default: float) -> float:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    def _env_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        text = raw.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    parser = argparse.ArgumentParser(description="Run the hedging metrics coordinator")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address for the HTTP server")
    parser.add_argument("--port", type=int, default=8899, help="Port for the HTTP server")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Shortcut for --log-level WARNING to suppress informational logs",
    )
    parser.add_argument(
        "--dashboard-username",
        help="Optional username required to access the dashboard (enables HTTP Basic auth)",
    )
    parser.add_argument(
        "--dashboard-password",
        help="Optional password required to access the dashboard (enables HTTP Basic auth)",
    )
    parser.add_argument(
        "--dashboard-session-ttl",
        type=float,
        default=_env_float("DASHBOARD_SESSION_TTL", 12 * 3600),
        help="Seconds that a dashboard login session remains valid (default 12h).",
    )
    parser.add_argument(
        "--risk-alert-threshold",
        type=float,
        default=_env_float("RISK_ALERT_THRESHOLD", 0.30),
        help="Risk ratio trigger (e.g. 0.3 == 30%%). Set <= 0 to disable alerts.",
    )
    parser.add_argument(
        "--risk-alert-reset",
        type=float,
        default=_env_float("RISK_ALERT_RESET", 0.20),
        help="Ratio below which alerts reset (defaults to ~70%% of threshold).",
    )
    parser.add_argument(
        "--risk-alert-cooldown",
        type=float,
        default=_env_float("RISK_ALERT_COOLDOWN", 900.0),
        help="Seconds to wait before re-alerting the same account.",
    )
    parser.add_argument(
        "--bark-url",
        default=os.getenv("BARK_URL"),
        help="Full Bark push URL or template (supports {title} and {body} placeholders).",
    )
    parser.add_argument(
        "--bark-append-title-body",
        default=_env_bool("BARK_APPEND_TITLE_BODY", True),
        action=argparse.BooleanOptionalAction,
        help="Append encoded title/body when the template lacks placeholders (use --no-bark-append-title-body to disable).",
    )
    parser.add_argument(
        "--bark-timeout",
        type=float,
        default=_env_float("BARK_TIMEOUT", 10.0),
        help="HTTP timeout in seconds for Bark push requests.",
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
